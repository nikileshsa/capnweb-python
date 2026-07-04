"""Unified RPC Client with production-grade session management.

This module provides a unified client that uses the appropriate session type
based on the configured transport:
- HTTP Batch: one-batch-per-request sessions via ``new_http_batch_rpc_session``
- WebSocket: BidirectionalSession for full bidirectional RPC
- WebTransport: must be requested explicitly (never inferred from the URL);
  session integration is pending

Transport selection is explicit via ``ClientConfig.transport``; ``"auto"``
selects by URL scheme only (ws/wss -> WebSocket, http/https -> HTTP batch).

All session types support:
- Proper release message sending
- Reference counting
- onBroken callbacks (via ``stub.on_rpc_broken``)
- drain() for graceful shutdown
- getStats() for debugging
"""

from __future__ import annotations

from typing import Any, Self

import aiohttp

from capnweb.config import ClientConfig, RpcSessionConfig
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcStub
from capnweb.ws_transport import WebSocketTransport

# Backwards compatibility aliases
UnifiedClientConfig = ClientConfig
WebSocketClientTransport = WebSocketTransport


class UnifiedClient:
    """Production-grade RPC client with explicit transport selection.

    Features:
    - Proper release message sending (prevents memory leaks)
    - Reference counting for capabilities
    - onBroken callbacks for connection death handling
    - drain() for graceful shutdown
    - getStats() for debugging
    - Server can call back to client (bidirectional, WebSocket only)

    Example:
        ```python
        async with UnifiedClient(ClientConfig(url="ws://localhost:8080/rpc")) as client:
            api = client.get_main_stub()
            result = await api.greet("World")
            print(result)  # "Hello, World!"
        ```
    """

    def __init__(self, config: UnifiedClientConfig) -> None:
        self.config = config
        self._http_session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._transport: WebSocketTransport | None = None
        self._session: BidirectionalSession | None = None

        mode = getattr(config, "transport", "auto")
        if mode == "auto":
            if config.url.startswith(("ws://", "wss://")):
                mode = "websocket"
            else:
                mode = "http-batch"
        self._transport_mode = mode
        if mode == "websocket" and not config.url.startswith(("ws://", "wss://")):
            raise ValueError(f"WebSocket transport requires a ws:// or wss:// URL, got {config.url}")
        if mode == "http-batch" and not config.url.startswith(("http://", "https://")):
            raise ValueError(f"HTTP batch transport requires an http(s):// URL, got {config.url}")

    async def __aenter__(self) -> Self:
        """Connect to the server."""
        if self._transport_mode == "websocket":
            await self._connect_websocket()
        elif self._transport_mode == "webtransport":
            await self._connect_webtransport()
        else:
            # HTTP batch - a fresh one-shot session is created per batch;
            # share one aiohttp client for connection reuse.
            self._http_session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args: object) -> None:
        """Disconnect from the server."""
        await self.close()

    async def _connect_websocket(self) -> None:
        """Connect via WebSocket with BidirectionalSession."""
        self._http_session = aiohttp.ClientSession()
        self._ws = await self._http_session.ws_connect(
            self.config.url, heartbeat=getattr(self.config, "heartbeat", None)
        )

        options = self.config.options
        self._transport = WebSocketTransport(self._ws)
        self._session = BidirectionalSession(
            transport=self._transport,
            local_main=self.config.local_main,
            options=options,
        )
        self._session.start()

    async def _connect_webtransport(self) -> None:
        """Connect via WebTransport with BidirectionalSession."""
        # WebTransport requires aioquic
        try:
            from capnweb.webtransport import WebTransportClient  # noqa: F401
        except ImportError as e:
            raise RuntimeError("WebTransport requires aioquic: pip install aioquic") from e

        raise NotImplementedError(
            "WebTransport integration with BidirectionalSession pending"
        )

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
            await self._transport.wait_closed()
            self._transport = None
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._http_session:
            await self._http_session.close()
            self._http_session = None

    def get_main_stub(self) -> RpcStub:
        """Get the server's main capability as an RpcStub.

        Returns:
            An RpcStub for the server's main capability
        """
        if self._session:
            return RpcStub(self._session.get_main_stub())
        raise RuntimeError("Not connected or using HTTP batch mode")

    # TS name for the same thing (rpc.ts getRemoteMain).
    get_remote_main = get_main_stub

    async def new_batch(self) -> RpcStub:
        """Start a one-shot HTTP batch session and return its main stub.

        HTTP-batch mode only. Each batch is one HTTP round trip: issue calls
        on the returned stub, then await results (the batch flushes on the
        first await). Stubs from one batch are unusable in the next — call
        ``new_batch()`` again for the next round trip.
        """
        if self._transport_mode != "http-batch" or not self._http_session:
            msg = "new_batch() requires the http-batch transport"
            raise RuntimeError(msg)

        from capnweb.batch import new_http_batch_rpc_session

        return await new_http_batch_rpc_session(
            self.config.url,
            http_client=self._http_session,
            options=self.config.options or RpcSessionConfig(),
        )

    async def drain(self) -> None:
        """Wait for all pending operations to complete."""
        if self._session:
            await self._session.drain()

    def get_stats(self) -> dict[str, int]:
        """Get session statistics."""
        if self._session:
            return self._session.get_stats()
        return {"imports": 0, "exports": 0}

    @property
    def is_bidirectional(self) -> bool:
        """Check if this client supports bidirectional RPC."""
        return self._session is not None
