"""WebSocket RPC sessions using BidirectionalSession.

Public surface (mirrors TS websocket.ts / index.ts):

- ``new_websocket_rpc_session(ws_or_url, local_main=None, options=None)`` —
  one-shot constructor returning the peer's main capability as an
  ``RpcStub`` (TS ``newWebSocketRpcSession``, websocket.ts:10-19). Accepts a
  ``ws(s)://`` URL or an already-open aiohttp WebSocket. Python deviation:
  the function is async because connecting is awaitable in asyncio.
- ``handle_websocket_rpc(request, local_main)`` — aiohttp server handler,
  analog of ``newWorkersWebSocketRpcResponse``.
- ``WebSocketRpcClient`` / ``WebSocketRpcServer`` — lifecycle-managed
  wrappers (Python-only additions, kept per matrix §5.5).
- ``wait_closed(session)`` — event-driven session-lifetime wait used by both
  server handlers (no polling).

Disposal: disposing the returned main stub shuts the session down and closes
the WebSocket (close code 3000 + reason on abort paths).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

import aiohttp
from aiohttp import web

from capnweb.rpc_session import BidirectionalSession, RpcSessionOptions, RpcTransport
from capnweb.stubs import RpcStub
from capnweb.ws_transport import WebSocketTransport

logger = logging.getLogger(__name__)

# Compatibility aliases: the canonical transport wraps either side's open WS.
# (The push-style feed_message server transport lives in ws_transport.py.)
WebSocketClientTransport = WebSocketTransport
WebSocketServerTransport = WebSocketTransport


async def wait_closed(session: BidirectionalSession) -> None:
    """Wait until the session ends (aborts or is shut down). Event-driven.

    This is the public replacement for both the old ``ws.closed`` 0.1s poll
    loop and direct ``session._abort_event`` reach-ins. If the session class
    grows a native ``wait_closed()`` (blocker filed for B1/A1), this helper
    defers to it.
    """
    native = getattr(session, "wait_closed", None)
    if native is not None:
        await native()
        return
    # Fallback: the session has no public completion signal yet.
    await session._abort_event.wait()  # noqa: SLF001 (single sanctioned reach-in)


async def new_websocket_rpc_session(
    ws_or_url: str | aiohttp.ClientWebSocketResponse | web.WebSocketResponse,
    local_main: Any | None = None,
    options: RpcSessionOptions | None = None,
    *,
    heartbeat: float | None = None,
) -> RpcStub:
    """Start a WebSocket RPC session; return the peer's main stub.

    Mirrors TS ``newWebSocketRpcSession(webSocket | url, localMain?, options?)``
    (websocket.ts:10-19). Accepts either a ``ws://``/``wss://`` URL or an
    already-open aiohttp WebSocket (client or server side).

    Disposing the returned stub (``stub.dispose()`` / ``with stub:``) shuts
    the session down and closes the WebSocket.

    Args:
        ws_or_url: URL to connect to, or an open aiohttp WebSocket.
        local_main: Optional local capability to expose to the peer.
        options: Optional session configuration.
        heartbeat: Optional aiohttp WS heartbeat interval in seconds
            (Python-only; only used when connecting from a URL).

    Returns:
        RpcStub for the peer's main capability.
    """
    if isinstance(ws_or_url, str):
        http_session = aiohttp.ClientSession()
        try:
            ws = await http_session.ws_connect(ws_or_url, heartbeat=heartbeat)
        except BaseException:
            await http_session.close()
            raise
        transport = WebSocketTransport(ws, owned_http_session=http_session)
    else:
        transport = WebSocketTransport(ws_or_url)

    session = BidirectionalSession(
        transport=transport,
        local_main=local_main,
        options=options,
    )
    session.start()
    return RpcStub(session.get_main_stub())


class WebSocketRpcClient:
    """WebSocket RPC client using BidirectionalSession.

    This provides full bidirectional RPC support:
    - Client can call server methods
    - Server can call client methods (if local_main is provided)
    - Proper release message handling
    - Connection death callbacks

    Example:
        ```python
        async with WebSocketRpcClient("ws://localhost:8080/rpc") as client:
            api = client.get_main_stub()
            result = await api.greet("World")
            print(result)  # "Hello, World!"
        ```
    """

    def __init__(
        self,
        url: str,
        local_main: Any | None = None,
        options: RpcSessionOptions | None = None,
        *,
        heartbeat: float | None = None,
    ) -> None:
        """Initialize the client.

        Args:
            url: WebSocket URL to connect to
            local_main: Optional local capability to expose to server
            options: Optional session configuration
            heartbeat: Optional aiohttp WS heartbeat interval in seconds
                (Python-only extension; aiohttp sends pings and drops dead
                connections)
        """
        self.url = url
        self._local_main = local_main
        self._options = options
        self._heartbeat = heartbeat
        self._http_session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._transport: WebSocketTransport | None = None
        self._session: BidirectionalSession | None = None

    async def __aenter__(self) -> "WebSocketRpcClient":
        """Connect to the server."""
        self._http_session = aiohttp.ClientSession()
        self._ws = await self._http_session.ws_connect(
            self.url, heartbeat=self._heartbeat
        )

        self._transport = WebSocketTransport(self._ws)
        self._session = BidirectionalSession(
            transport=self._transport,
            local_main=self._local_main,
            options=self._options,
        )
        self._session.start()

        return self

    async def __aexit__(self, *args: object) -> None:
        """Disconnect from the server."""
        await self.close()

    async def close(self) -> None:
        """Close the connection."""
        if self._session:
            # Pre-abort via the public shutdown() so stop() does not try to
            # flush an abort frame over a possibly-dead socket (TS sends no
            # abort on graceful teardown either; also avoids a writer-queue
            # join deadlock when the send fails — blocker filed for A1/B1).
            self._session.shutdown()
            await self._session.stop()
            self._session = None
        if self._transport:
            # Await any abort-close task the transport spawned, then close.
            await self._transport.wait_closed()
            self._transport = None
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._http_session:
            await self._http_session.close()
            self._http_session = None

    def get_main_stub(self) -> RpcStub:
        """Get the server's main capability as an RpcStub."""
        if self._session is None:
            raise RuntimeError("Not connected")
        return RpcStub(self._session.get_main_stub())

    # TS name for the same thing (rpc.ts getRemoteMain).
    get_remote_main = get_main_stub

    async def wait_closed(self) -> None:
        """Wait until the underlying session ends (event-driven)."""
        if self._session is not None:
            await wait_closed(self._session)

    async def drain(self) -> None:
        """Wait for all pending operations to complete."""
        if self._session:
            await self._session.drain()

    def get_stats(self) -> dict[str, int]:
        """Get session statistics."""
        if self._session:
            return self._session.get_stats()
        return {"imports": 0, "exports": 0}


async def handle_websocket_rpc(
    request: web.Request,
    local_main: Any,
    options: RpcSessionOptions | None = None,
    *,
    heartbeat: float | None = None,
) -> web.WebSocketResponse:
    """Handle a WebSocket RPC connection on the server side.

    This function should be called from an aiohttp route handler.

    Args:
        request: The aiohttp request
        local_main: The main capability to expose to clients
        options: Optional session configuration
        heartbeat: Optional aiohttp WS heartbeat interval in seconds
            (Python-only extension)

    Returns:
        The WebSocket response

    Example:
        ```python
        async def websocket_handler(request):
            return await handle_websocket_rpc(request, MyService())

        app.router.add_get("/rpc", websocket_handler)
        ```
    """
    ws = web.WebSocketResponse(heartbeat=heartbeat)
    await ws.prepare(request)

    transport = WebSocketTransport(ws)
    session = BidirectionalSession(
        transport=transport,
        local_main=local_main,
        options=options,
    )
    session.start()

    try:
        # Keep the connection alive until the session ends (event-driven).
        await wait_closed(session)
    except Exception as e:
        logger.debug("WebSocket session ended: %s", e)
    finally:
        await session.stop()
        await transport.wait_closed()

    return ws


class WebSocketRpcServer:
    """WebSocket RPC server using BidirectionalSession.

    This provides a simple server that handles WebSocket RPC connections.

    Example:
        ```python
        server = WebSocketRpcServer(MyService(), host="localhost", port=8080)
        await server.start()
        # ... server is running ...
        await server.stop()
        ```

    Per-connection capabilities: pass ``local_main_factory`` to create a fresh
    main capability for every connection (Bun ``createMain`` analog,
    bun.ts:39-44). When provided it takes precedence over ``local_main``.
    """

    def __init__(
        self,
        local_main: Any | None = None,
        host: str = "localhost",
        port: int = 8080,
        path: str = "/rpc",
        options: RpcSessionOptions | None = None,
        *,
        local_main_factory: Callable[[], Any] | None = None,
        heartbeat: float | None = None,
    ) -> None:
        """Initialize the server.

        Args:
            local_main: The main capability shared by all connections
                (ignored when local_main_factory is provided)
            host: Host to bind to
            port: Port to bind to
            path: URL path for WebSocket endpoint
            options: Optional session configuration
            local_main_factory: Zero-arg factory creating a fresh main
                capability per connection
            heartbeat: Optional aiohttp WS heartbeat interval in seconds
                (Python-only extension)
        """
        if local_main is None and local_main_factory is None:
            raise ValueError("Provide local_main or local_main_factory")
        self._local_main = local_main
        self._local_main_factory = local_main_factory
        self._host = host
        self._port = port
        self._path = path
        self._options = options
        self._heartbeat = heartbeat
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._sessions: list[BidirectionalSession] = []

    @classmethod
    def from_config(cls, config: Any, local_main: Any | None = None) -> "WebSocketRpcServer":
        """Build a server from a ``WebSocketServerConfig``."""
        return cls(
            local_main=local_main,
            host=config.host,
            port=config.port,
            path=config.path,
            options=config.options,
            local_main_factory=config.local_main_factory,
        )

    async def start(self) -> None:
        """Start the server."""
        self._app = web.Application()
        self._app.router.add_get(self._path, self._handle_ws)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        self._site = web.TCPSite(self._runner, self._host, self._port)
        await self._site.start()

        logger.info("WebSocket RPC server started on ws://%s:%d%s",
                   self._host, self._port, self._path)

    async def stop(self) -> None:
        """Stop the server."""
        # Stop all sessions. Pre-abort via shutdown() so stop() never tries
        # to flush an abort frame over a socket the client may already have
        # closed (avoids a writer-queue join deadlock; blocker filed for
        # A1/B1).
        for session in list(self._sessions):
            session.shutdown()
            await session.stop()
        self._sessions.clear()

        if self._site:
            await self._site.stop()
            self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self._app = None

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        """Handle incoming WebSocket connection."""
        ws = web.WebSocketResponse(heartbeat=self._heartbeat)
        await ws.prepare(request)

        if self._local_main_factory is not None:
            local_main = self._local_main_factory()
        else:
            local_main = self._local_main

        transport = WebSocketTransport(ws)
        session = BidirectionalSession(
            transport=transport,
            local_main=local_main,
            options=self._options,
        )
        self._sessions.append(session)
        session.start()

        try:
            # Keep the connection alive until the session ends (event-driven;
            # replaces the old ws.closed 0.1s poll loop).
            await wait_closed(session)
        except Exception as e:
            logger.debug("WebSocket session ended: %s", e)
        finally:
            await session.stop()
            await transport.wait_closed()
            if session in self._sessions:
                self._sessions.remove(session)

        return ws
