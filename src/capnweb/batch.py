"""HTTP Batch RPC transport and session helpers.

This module provides HTTP batch RPC support, allowing multiple RPC calls to be
batched into a single HTTP POST request/response. This is useful for:
- Environments without WebSocket support
- Serverless functions (single request/response)
- Reducing connection overhead

Based on the TypeScript reference implementation in batch.ts.

Batch lifecycle (one batch per session):
    A batch session performs exactly ONE HTTP round trip. Issue all calls
    first, then await results — the batch is flushed when the first result is
    awaited (i.e. when the first ``pull`` message is emitted). Calls issued
    after the flush are silently ignored (matching TS batch.ts:22-29) and
    their promises fail with ``BatchEndError`` when the response is exhausted.
    Stubs obtained from one batch are NOT usable in another batch/session:
    one stub = one HTTP request. Create a new session for each batch.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from capnweb import _json
from capnweb.rpc_session import BidirectionalSession, RpcSessionOptions
from capnweb.stubs import RpcStub

if TYPE_CHECKING:
    from aiohttp import ClientSession
    from aiohttp.web import Request, Response


SendBatchFunc = Callable[[list[str]], Awaitable[list[str]]]

# Default bound on how long the batch server waits for the request batch to
# be fully processed + drained. Overridable via RpcSessionConfig.drain_timeout
# (Python-only extension). On expiry the server FAILS LOUDLY — it never
# returns a silently-truncated 200.
DEFAULT_DRAIN_TIMEOUT_SECONDS = 30.0


class BatchEndError(Exception):
    """Raised when the batch has ended."""


def _is_pull_message(message: str) -> bool:
    """Detect a wire-level ``["pull", id]`` message (canonical JSON array)."""
    if not message.lstrip().startswith('["pull"'):
        return False
    try:
        parsed = _json.loads(message)
    except ValueError:
        return False
    return isinstance(parsed, list) and bool(parsed) and parsed[0] == "pull"


class BatchClientTransport:
    """In-memory transport for HTTP batch RPC client.

    Collects outgoing messages until the batch flushes, then serves incoming
    messages from the response.

    Flush timing: TS uses a ``setTimeout(0)`` macrotask (batch.ts:50-67).
    Python instead flushes when the first ``pull`` message is sent — the
    first pull means the application has started awaiting results, so
    queueing is done. This removes the flush race entirely: any number of
    ``await``s may occur between session creation and issuing calls. The
    flush task is scheduled AFTER the writer queue finishes draining the
    already-queued messages, so concurrently-issued pulls in the same event
    loop turn are included.
    """

    __slots__ = (
        "_aborted",
        "_batch_sent",
        "_batch_to_receive",
        "_batch_to_send",
        "_flush_task",
        "_receive_index",
        "_send_batch",
    )

    def __init__(self, send_batch: SendBatchFunc) -> None:
        self._send_batch = send_batch
        self._batch_to_send: list[str] | None = []
        self._batch_to_receive: list[str] | None = None
        self._batch_sent = asyncio.Event()
        self._aborted: Exception | None = None
        self._receive_index = 0
        self._flush_task: asyncio.Task | None = None

    async def send(self, message: str) -> None:
        """Queue a message to be sent in the batch.

        Messages sent after the batch flushed are silently ignored
        (batch.ts:22-29); the eventual receive() exhaustion propagates
        ``BatchEndError`` to anything still waiting.
        """
        if self._batch_to_send is None or self._aborted:
            # Batch already flushed (or aborted): ignore further messages.
            return
        self._batch_to_send.append(message)

        if self._flush_task is None and _is_pull_message(message):
            # First pull observed: the application is awaiting a result, so
            # the batch is complete. Flush in a retained task; by the time it
            # runs, the writer loop has drained every already-queued message
            # (the writer only suspends on an empty queue).
            self._flush_task = asyncio.create_task(self._flush())

    async def receive(self) -> str:
        """Receive a message from the batch response."""
        if not self._batch_sent.is_set():
            await self._batch_sent.wait()

        if self._aborted:
            raise self._aborted

        if self._batch_to_receive is None:
            raise RuntimeError("Batch not yet received")

        if self._receive_index < len(self._batch_to_receive):
            msg = self._batch_to_receive[self._receive_index]
            self._receive_index += 1
            return msg

        # No more messages - signal end of batch
        raise BatchEndError("Batch RPC request ended.")

    def abort(self, reason: Exception) -> None:
        """Abort the transport."""
        self._aborted = reason
        self._batch_sent.set()

    async def _flush(self) -> None:
        """Send the collected batch over HTTP and stage the response."""
        # Yield once more so anything scheduled in the same turn (e.g. a
        # second concurrently-awaited pull) lands in the batch first.
        await asyncio.sleep(0)

        if self._aborted:
            self._batch_sent.set()
            return

        try:
            batch = self._batch_to_send or []
            self._batch_to_send = None  # Flushed: ignore further sends
            self._batch_to_receive = await self._send_batch(batch)
        except Exception as e:
            self._aborted = e
        finally:
            self._batch_sent.set()


class BatchServerTransport:
    """In-memory transport for HTTP batch RPC server.

    Receives messages from the request batch and collects responses.
    """

    __slots__ = (
        "_aborted",
        "_all_received",
        "_batch_to_receive",
        "_batch_to_send",
        "_receive_index"
    )

    def __init__(self, batch: list[str]) -> None:
        self._batch_to_receive = batch
        self._batch_to_send: list[str] = []
        self._receive_index = 0
        self._all_received = asyncio.Event()
        self._aborted: Exception | None = None

    async def send(self, message: str) -> None:
        """Queue a message for the response."""
        self._batch_to_send.append(message)

    async def receive(self) -> str:
        """Get the next message from the request batch."""
        if self._receive_index < len(self._batch_to_receive):
            msg = self._batch_to_receive[self._receive_index]
            self._receive_index += 1
            return msg

        # No more messages
        self._all_received.set()
        # Return a future that never resolves (session will drain)
        await asyncio.Event().wait()
        raise RuntimeError("Unreachable")

    def abort(self, reason: Exception) -> None:
        """Abort the transport."""
        self._aborted = reason
        self._all_received.set()

    async def when_all_received(self) -> None:
        """Wait until all request messages have been received."""
        await self._all_received.wait()
        if self._aborted:
            raise self._aborted

    def get_response_body(self) -> str:
        """Get the response body (newline-separated messages)."""
        return "\n".join(self._batch_to_send)


async def new_http_batch_rpc_session(
    url: str,
    *,
    http_client: ClientSession | None = None,
    headers: dict[str, str] | None = None,
    options: RpcSessionOptions | None = None,
) -> RpcStub:
    """Start an HTTP batch RPC session as a client.

    This creates a session that batches RPC calls into a single HTTP POST
    request. ONE batch per session: issue all calls before (or while)
    awaiting the first result; the batch flushes when the first result is
    awaited. Stubs/promises from this batch are unusable afterwards — create
    a new session for the next batch.

    Args:
        url: The URL to POST the batch to
        http_client: Optional aiohttp ClientSession to use
        headers: Optional HTTP headers for the batch request (auth headers
            are the primary batch-RPC auth mechanism; TS achieves the same by
            passing a ``Request`` object, batch.ts:70-90)
        options: Optional RPC session options

    Returns:
        An RpcStub for the remote main object

    Example:
        ```python
        async with aiohttp.ClientSession() as client:
            api = await new_http_batch_rpc_session(
                "https://api.example.com/rpc",
                http_client=client,
                headers={"Authorization": "Bearer ..."},
            )
            # These calls are batched into ONE request; the batch is sent
            # when the first result is awaited.
            p1 = api.method1()
            p2 = api.method2()
            r1, r2 = await asyncio.gather(p1, p2)
        ```
    """
    stub, _session = _new_http_batch_rpc_session_with_session(
        url, http_client=http_client, headers=headers, options=options
    )
    return stub


def _new_http_batch_rpc_session_with_session(
    url: str,
    *,
    http_client: ClientSession | None = None,
    headers: dict[str, str] | None = None,
    options: RpcSessionOptions | None = None,
) -> tuple[RpcStub, BidirectionalSession]:
    """Internal: like new_http_batch_rpc_session but also returns the session.

    Callers that manage many batches in one process (e.g. UnifiedClient)
    should ``await session.stop()`` when done to reap the session's writer
    task promptly.
    """
    import aiohttp

    own_client = http_client is None

    async def send_batch(batch: list[str]) -> list[str]:
        client = aiohttp.ClientSession() if own_client else http_client
        try:
            body = "\n".join(batch)
            async with client.post(url, data=body, headers=headers) as response:
                if not response.ok:
                    text = await response.text()
                    raise RuntimeError(
                        f"RPC request failed: {response.status} {response.reason} - {text}"
                    )
                text = await response.text()
                return text.split("\n") if text else []
        finally:
            if own_client:
                await client.close()

    transport = BatchClientTransport(send_batch)
    session = BidirectionalSession(transport, None, options)
    session.start()

    # Wrap the main hook in an RpcStub for the user
    return RpcStub(session.get_main_stub()), session


async def new_http_batch_rpc_response(
    request_body: str,
    local_main: Any,
    options: RpcSessionOptions | None = None,
) -> str:
    """Handle an HTTP batch RPC request on the server side.

    This processes a batch of RPC messages and returns the response body.
    The underlying session is ALWAYS stopped before returning (no leaked
    read-loop/writer tasks), and a batch that fails to complete within the
    drain timeout raises instead of returning a silently-truncated 200
    (TS never truncates, batch.ts:139-164).

    Args:
        request_body: The request body (newline-separated messages)
        local_main: The main object to expose to the client
        options: Optional RPC session options. The Python-only
            ``drain_timeout`` extension bounds the wait (default 30s;
            ``None`` disables the bound).

    Returns:
        The response body (newline-separated messages)

    Raises:
        TimeoutError: If the batch did not finish within ``drain_timeout``.
        Exception: If the session aborted while processing the batch.

    Example:
        ```python
        @app.post("/rpc")
        async def handle_rpc(request: Request):
            body = await request.text()
            response_body = await new_http_batch_rpc_response(body, MyApi())
            return Response(text=response_body)
        ```
    """
    # Filter empty messages
    batch = [msg for msg in request_body.split("\n") if msg.strip()] if request_body else []

    # If no messages, return empty response
    if not batch:
        return ""

    transport = BatchServerTransport(batch)
    session = BidirectionalSession(transport, local_main, options)

    # Start the session's read loop
    session.start()

    drain_timeout = getattr(options, "drain_timeout", DEFAULT_DRAIN_TIMEOUT_SECONDS)

    try:
        async def process() -> None:
            # Wait for all messages to be received, then for all pending
            # operations to complete (server->client pipelining is allowed,
            # but the app must not await client responses — batch.ts:151-156).
            await transport.when_all_received()
            await session.drain()

        if drain_timeout is None:
            await process()
        else:
            try:
                await asyncio.wait_for(process(), timeout=drain_timeout)
            except TimeoutError:
                # Explicit error path: a truncated 200 is the worst outcome —
                # the client would hang or mis-resolve. Fail loudly instead.
                raise TimeoutError(
                    f"HTTP batch RPC did not complete within {drain_timeout}s "
                    "(pending calls or unresolved pulls); refusing to return "
                    "a truncated response"
                ) from None

        return transport.get_response_body()
    finally:
        await session.stop()


async def aiohttp_batch_rpc_handler(
    request: Request,
    local_main: Any,
    options: RpcSessionOptions | None = None,
    *,
    headers: dict[str, str] | None = None,
) -> Response:
    """AIOHTTP handler for HTTP batch RPC (standalone batch endpoint).

    NOTE: This standalone handler deliberately does NOT set
    ``Access-Control-Allow-Origin`` (matching TS ``newHttpBatchRpcResponse``,
    batch.ts:139-164). Cross-origin exposure is only appropriate for the
    combined endpoint — use :func:`aiohttp_rpc_handler` for that, and read
    its security warning.

    Args:
        request: The aiohttp Request object
        local_main: The main object to expose to the client
        options: Optional RPC session options
        headers: Optional extra response headers (analog of the TS
            ``nodeHttpBatchRpcResponse`` headers option, batch.ts:174-204)

    Returns:
        An aiohttp Response object (500 with a plain-text error if the batch
        failed or timed out — never a truncated 200)

    Example:
        ```python
        from aiohttp import web
        from capnweb.batch import aiohttp_batch_rpc_handler

        async def rpc_handler(request):
            return await aiohttp_batch_rpc_handler(request, MyApi())

        app = web.Application()
        app.router.add_post('/rpc', rpc_handler)
        ```
    """
    from aiohttp import web

    if request.method != "POST":
        return web.Response(
            text="This endpoint only accepts POST requests.",
            status=405
        )

    body = await request.text()
    try:
        response_body = await new_http_batch_rpc_response(body, local_main, options)
    except TimeoutError as e:
        return web.Response(text=str(e), status=500)

    response = web.Response(text=response_body)
    if headers:
        response.headers.update(headers)
    return response


async def aiohttp_rpc_handler(
    request: Request,
    local_main: Any,
    options: RpcSessionOptions | None = None,
) -> Response:
    """Unified aiohttp RPC endpoint: POST -> batch, WS upgrade -> WebSocket.

    Python analog of TS ``newWorkersRpcResponse`` (index.ts:147-161): routes
    POST requests to HTTP batch RPC, WebSocket upgrades to a WebSocket RPC
    session, and returns 400 for anything else.

    SECURITY WARNING (mirrors index.ts:141-146): this handler accepts
    cross-origin requests — it is the ONLY place that sets
    ``Access-Control-Allow-Origin: *`` on batch responses. Since the same API
    is exposed over WebSocket (which always allows cross-origin), the API
    must already be safe for cross-origin use (e.g. in-band authorization).
    If that's not the case, validate the ``Origin`` header before calling
    this, or use :func:`aiohttp_batch_rpc_handler` /
    :func:`capnweb.ws_session.handle_websocket_rpc` directly.
    """
    from aiohttp import web

    if request.method == "POST":
        response = await aiohttp_batch_rpc_handler(request, local_main, options)
        # See the security warning above (index.ts:150-154).
        response.headers["Access-Control-Allow-Origin"] = "*"
        return response

    if request.headers.get("Upgrade", "").lower() == "websocket":
        from capnweb.ws_session import handle_websocket_rpc
        return await handle_websocket_rpc(request, local_main, options)

    return web.Response(
        text="This endpoint only accepts POST or WebSocket requests.",
        status=400,
    )


async def fastapi_batch_rpc_handler(
    request_body: str,
    local_main: Any,
    options: RpcSessionOptions | None = None,
) -> str:
    """FastAPI handler for HTTP batch RPC.

    This is a convenience function for use with FastAPI.

    Args:
        request_body: The request body as a string
        local_main: The main object to expose to the client
        options: Optional RPC session options

    Returns:
        The response body as a string

    Example:
        ```python
        from fastapi import FastAPI, Request
        from fastapi.responses import PlainTextResponse
        from capnweb.batch import fastapi_batch_rpc_handler

        app = FastAPI()

        @app.post("/rpc")
        async def rpc_endpoint(request: Request):
            body = await request.body()
            response = await fastapi_batch_rpc_handler(
                body.decode(), MyApi()
            )
            return PlainTextResponse(response)
        ```
    """
    return await new_http_batch_rpc_response(request_body, local_main, options)
