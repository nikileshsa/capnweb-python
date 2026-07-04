"""WebSocket transports for BidirectionalSession.

This is the ONE WebSocket transport module (matrix Part 5 §5.3 "Duplicate
transport implementations"). It provides:

- ``WebSocketTransport`` — the canonical transport, wrapping an already-open
  aiohttp WebSocket (client- or server-side; both expose the same
  ``send_str``/``receive``/``close`` API). Mirrors TS ``websocket.ts:45-145``:
  ``abort()`` closes the socket with close code **3000** and the abort reason
  as the close message (websocket.ts:120-133), and the close task is retained
  on the instance — never a fire-and-forget ``create_task``.
- ``WebSocketConnectingClientTransport`` — URL-based variant with an explicit
  ``connect()`` step (Python has no lazy sync-constructor sockets). Exported
  under its historical name ``WebSocketClientTransport``.
- ``WebSocketServerTransport`` — push-style ``feed_message`` transport, the
  Python analog of Bun's ``dispatchMessage``/``dispatchClose``/
  ``dispatchError`` pattern (bun.ts:101-123). Kept for integrations that own
  the WS read loop themselves.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import aiohttp

if TYPE_CHECKING:
    from aiohttp import ClientWebSocketResponse

# WS close frames cap the reason at 123 bytes (RFC 6455 §5.5).
_MAX_CLOSE_REASON_BYTES = 123

# Close code used when the RPC session aborts, matching TS websocket.ts:127.
ABORT_CLOSE_CODE = 3000


def _close_reason_bytes(reason: object) -> bytes:
    """Encode an abort reason as a WS close message, truncated safely."""
    raw = str(reason).encode("utf-8")
    if len(raw) <= _MAX_CLOSE_REASON_BYTES:
        return raw
    # Truncate without splitting a UTF-8 code point.
    return raw[:_MAX_CLOSE_REASON_BYTES].decode("utf-8", errors="ignore").encode("utf-8")


class WebSocketTransport:
    """Canonical WebSocket transport over an already-open aiohttp WebSocket.

    Works with both ``aiohttp.ClientWebSocketResponse`` (client side) and
    ``aiohttp.web.WebSocketResponse`` (server side) — they expose the same
    ``send_str`` / ``receive`` / ``close`` surface.

    Args:
        ws: The open WebSocket.
        owned_http_session: Optional ``aiohttp.ClientSession`` that this
            transport owns and must close when the connection ends (used by
            ``new_websocket_rpc_session(url)`` where nothing else holds it).
    """

    __slots__ = ('_ws', '_closed', '_close_task', '_owned_http_session')

    def __init__(
        self,
        ws: "ClientWebSocketResponse | aiohttp.web.WebSocketResponse",
        *,
        owned_http_session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._ws = ws
        self._closed = False
        self._close_task: asyncio.Task | None = None
        self._owned_http_session = owned_http_session

    async def send(self, message: str) -> None:
        """Send a message to the peer (one RPC message per WS frame)."""
        if self._closed:
            raise ConnectionError("WebSocket is closed")
        await self._ws.send_str(message)

    async def receive(self) -> str:
        """Receive a message from the peer."""
        if self._closed:
            raise ConnectionError("WebSocket is closed")

        msg = await self._ws.receive()

        if msg.type == aiohttp.WSMsgType.TEXT:
            return msg.data
        elif msg.type == aiohttp.WSMsgType.BINARY:
            return msg.data.decode("utf-8")
        elif msg.type in (
            aiohttp.WSMsgType.CLOSE,
            aiohttp.WSMsgType.CLOSING,
            aiohttp.WSMsgType.CLOSED,
        ):
            self._closed = True
            raise ConnectionError("WebSocket closed")
        elif msg.type == aiohttp.WSMsgType.ERROR:
            self._closed = True
            raise ConnectionError(f"WebSocket error: {self._ws.exception()}")
        else:
            raise ValueError(f"Unexpected message type: {msg.type}")

    def abort(self, reason: Exception) -> None:
        """Abort: close the WS with code 3000 + reason (websocket.ts:120-133).

        The close coroutine runs in a task RETAINED on the instance so it can
        neither be garbage-collected before running nor silently dropped.
        """
        if self._closed:
            return
        self._closed = True
        self._close_task = asyncio.create_task(
            self._do_close(ABORT_CLOSE_CODE, _close_reason_bytes(reason))
        )

    async def close(self, code: int = 1000, message: bytes = b"") -> None:
        """Gracefully close the WebSocket (and any owned HTTP session)."""
        self._closed = True
        await self._do_close(code, message)

    async def wait_closed(self) -> None:
        """Await any in-flight abort-close task (idempotent)."""
        if self._close_task is not None:
            try:
                await self._close_task
            except asyncio.CancelledError:
                pass

    async def _do_close(self, code: int, message: bytes) -> None:
        try:
            await self._ws.close(code=code, message=message)
        except Exception:
            pass
        finally:
            if self._owned_http_session is not None:
                session, self._owned_http_session = self._owned_http_session, None
                try:
                    await session.close()
                except Exception:
                    pass


class WebSocketConnectingClientTransport(WebSocketTransport):
    """URL-based client transport with an explicit ``connect()`` step.

    Python deviation from TS's lazy sync-constructor WebSocket: connecting is
    an awaitable operation here, so this transport must be ``connect()``ed
    before use.
    """

    __slots__ = ('url',)

    def __init__(self, url: str) -> None:
        super().__init__(None)  # type: ignore[arg-type]
        self.url = url

    async def connect(self, *, heartbeat: float | None = None) -> None:
        """Connect to the WebSocket server."""
        self._owned_http_session = aiohttp.ClientSession()
        self._ws = await self._owned_http_session.ws_connect(
            self.url, heartbeat=heartbeat
        )

    async def send(self, message: str) -> None:
        if self._ws is None:
            raise ConnectionError("WebSocket not connected")
        await super().send(message)

    async def receive(self) -> str:
        if self._ws is None:
            raise ConnectionError("WebSocket not connected")
        return await super().receive()

    def abort(self, reason: Exception) -> None:
        if self._ws is None:
            self._closed = True
            return
        super().abort(reason)

    async def close(self, code: int = 1000, message: bytes = b"") -> None:
        self._closed = True
        if self._ws is not None:
            await self._do_close(code, message)
            self._ws = None  # type: ignore[assignment]
        elif self._owned_http_session is not None:
            session, self._owned_http_session = self._owned_http_session, None
            await session.close()


# Historical name kept for existing importers (tests, downstream code).
WebSocketClientTransport = WebSocketConnectingClientTransport


class WebSocketServerTransport:
    """Push-style server transport (Bun ``dispatch*`` analog, bun.ts:101-123).

    The server handler owns the WS read loop and feeds inbound messages via
    ``feed_message`` / ``set_error`` / ``set_closed``.
    """

    __slots__ = ('_ws', '_closed', '_close_task', '_receive_queue', '_error')

    def __init__(
        self,
        ws: "aiohttp.web.WebSocketResponse",
    ) -> None:
        self._ws = ws
        self._closed = False
        self._close_task: asyncio.Task | None = None
        self._receive_queue: asyncio.Queue[str] = asyncio.Queue()
        self._error: Exception | None = None

    def feed_message(self, message: str) -> None:
        """Feed a message received by the server handler."""
        self._receive_queue.put_nowait(message)

    def set_error(self, error: Exception) -> None:
        """Signal that an error occurred."""
        self._error = error
        self._closed = True

    def set_closed(self) -> None:
        """Signal that the connection was closed."""
        self._closed = True

    async def send(self, message: str) -> None:
        """Send a message to the client."""
        if self._closed:
            raise ConnectionError("WebSocket closed")
        await self._ws.send_str(message)

    async def receive(self) -> str:
        """Receive a message from the client."""
        if self._error:
            raise self._error
        if self._closed and self._receive_queue.empty():
            raise ConnectionError("WebSocket closed")

        return await self._receive_queue.get()

    def abort(self, reason: Exception) -> None:
        """Abort: close with code 3000 + reason, task retained."""
        if self._closed and self._close_task is not None:
            return
        self._closed = True
        self._close_task = asyncio.create_task(
            self._do_close(ABORT_CLOSE_CODE, _close_reason_bytes(reason))
        )

    async def wait_closed(self) -> None:
        """Await any in-flight abort-close task (idempotent)."""
        if self._close_task is not None:
            try:
                await self._close_task
            except asyncio.CancelledError:
                pass

    async def _do_close(self, code: int, message: bytes) -> None:
        try:
            await self._ws.close(code=code, message=message)
        except Exception:
            pass
