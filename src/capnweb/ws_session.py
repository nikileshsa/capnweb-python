"""WebSocket session using BidirectionalSession.

This module provides a WebSocket-based RPC session that uses the production-grade
BidirectionalSession for full bidirectional RPC support.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp
from aiohttp import web

from capnweb.rpc_session import BidirectionalSession, RpcSessionOptions, RpcTransport

logger = logging.getLogger(__name__)

# Constants
SESSION_POLL_INTERVAL_SECONDS = 0.1


class WebSocketClientTransport(RpcTransport):
    """WebSocket transport for client-side connections."""
    __slots__ = ('_ws', '_closed')
    
    def __init__(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self._ws = ws
        self._closed = False
    
    async def send(self, message: str) -> None:
        """Send a message to the server."""
        if self._closed:
            raise ConnectionError("WebSocket is closed")
        await self._ws.send_str(message)
    
    async def receive(self) -> str:
        """Receive a message from the server."""
        if self._closed:
            raise ConnectionError("WebSocket is closed")
        
        msg = await self._ws.receive()
        
        if msg.type == aiohttp.WSMsgType.TEXT:
            return msg.data
        elif msg.type == aiohttp.WSMsgType.BINARY:
            return msg.data.decode("utf-8")
        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
            self._closed = True
            raise ConnectionError("WebSocket closed")
        elif msg.type == aiohttp.WSMsgType.ERROR:
            self._closed = True
            raise ConnectionError(f"WebSocket error: {self._ws.exception()}")
        else:
            raise ValueError(f"Unexpected message type: {msg.type}")
    
    def abort(self, reason: Exception) -> None:
        """Signal that the session has failed."""
        self._closed = True
        # Close WebSocket in background
        asyncio.create_task(self._close_ws())
    
    async def _close_ws(self) -> None:
        """Close the WebSocket connection."""
        try:
            await self._ws.close()
        except Exception:
            pass


class WebSocketServerTransport(RpcTransport):
    """WebSocket transport for server-side connections."""
    __slots__ = ('_ws', '_closed')
    
    def __init__(self, ws: web.WebSocketResponse) -> None:
        self._ws = ws
        self._closed = False
    
    async def send(self, message: str) -> None:
        """Send a message to the client."""
        if self._closed:
            raise ConnectionError("WebSocket is closed")
        await self._ws.send_str(message)
    
    async def receive(self) -> str:
        """Receive a message from the client."""
        if self._closed:
            raise ConnectionError("WebSocket is closed")
        
        msg = await self._ws.receive()
        
        if msg.type == aiohttp.WSMsgType.TEXT:
            return msg.data
        elif msg.type == aiohttp.WSMsgType.BINARY:
            return msg.data.decode("utf-8")
        elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
            self._closed = True
            raise ConnectionError("WebSocket closed")
        elif msg.type == aiohttp.WSMsgType.ERROR:
            self._closed = True
            raise ConnectionError(f"WebSocket error: {self._ws.exception()}")
        else:
            raise ValueError(f"Unexpected message type: {msg.type}")
    
    def abort(self, reason: Exception) -> None:
        """Signal that the session has failed."""
        self._closed = True
        asyncio.create_task(self._close_ws())
    
    async def _close_ws(self) -> None:
        """Close the WebSocket connection."""
        try:
            await self._ws.close()
        except Exception:
            pass


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
            result = await client.call(0, "greet", ["World"])
            print(result)  # "Hello, World!"
        ```
    """
    
    def __init__(
        self,
        url: str,
        local_main: Any | None = None,
        options: RpcSessionOptions | None = None,
    ) -> None:
        """Initialize the client.
        
        Args:
            url: WebSocket URL to connect to
            local_main: Optional local capability to expose to server
            options: Optional session configuration
        """
        self.url = url
        self._local_main = local_main
        self._options = options
        self._http_session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: BidirectionalSession | None = None
    
    async def __aenter__(self) -> "WebSocketRpcClient":
        """Connect to the server."""
        self._http_session = aiohttp.ClientSession()
        self._ws = await self._http_session.ws_connect(self.url)
        
        transport = WebSocketClientTransport(self._ws)
        self._session = BidirectionalSession(
            transport=transport,
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
            await self._session.stop()
            self._session = None
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._http_session:
            await self._http_session.close()
            self._http_session = None
    
    def get_main_stub(self) -> Any:
        """Get the server's main capability as an RpcStub.
        
        Returns:
            An RpcStub for the server's main capability
        """
        if self._session is None:
            raise RuntimeError("Not connected")
        
        from capnweb.stubs import RpcStub
        return RpcStub(self._session.get_main_stub())
    
    async def call(
        self,
        cap_id: int,
        method: str,
        args: list[Any],
    ) -> Any:
        """Call a method on a remote capability.
        
        Args:
            cap_id: The capability ID (0 for main)
            method: The method name
            args: Arguments to pass
            
        Returns:
            The result of the call
        """
        if self._session is None:
            raise RuntimeError("Not connected")
        
        from capnweb.payload import RpcPayload
        
        args_payload = RpcPayload.from_app_params(args)
        hook = self._session.send_call(cap_id, [method], args_payload)  # sync
        payload = await hook.pull()
        return payload.value
    
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
) -> web.WebSocketResponse:
    """Handle a WebSocket RPC connection on the server side.
    
    This function should be called from an aiohttp route handler.
    
    Args:
        request: The aiohttp request
        local_main: The main capability to expose to clients
        options: Optional session configuration
        
    Returns:
        The WebSocket response
        
    Example:
        ```python
        async def websocket_handler(request):
            return await handle_websocket_rpc(request, MyService())
        
        app.router.add_get("/rpc", websocket_handler)
        ```
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    transport = WebSocketServerTransport(ws)
    session = BidirectionalSession(
        transport=transport,
        local_main=local_main,
        options=options,
    )
    session.start()
    
    try:
        # Keep connection alive until WebSocket is closed or session aborts
        # We wait on the abort event rather than drain() because drain() returns
        # immediately when there are no pending operations.
        await session._abort_event.wait()
    except Exception as e:
        logger.debug("WebSocket session ended: %s", e)
    finally:
        await session.stop()
    
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
    """
    
    def __init__(
        self,
        local_main: Any,
        host: str = "localhost",
        port: int = 8080,
        path: str = "/rpc",
        options: RpcSessionOptions | None = None,
    ) -> None:
        """Initialize the server.
        
        Args:
            local_main: The main capability to expose to clients
            host: Host to bind to
            port: Port to bind to
            path: URL path for WebSocket endpoint
            options: Optional session configuration
        """
        self._local_main = local_main
        self._host = host
        self._port = port
        self._path = path
        self._options = options
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._sessions: list[BidirectionalSession] = []
    
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
        # Stop all sessions
        for session in self._sessions:
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
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        transport = WebSocketServerTransport(ws)
        session = BidirectionalSession(
            transport=transport,
            local_main=self._local_main,
            options=self._options,
        )
        self._sessions.append(session)
        session.start()
        
        try:
            # Keep connection alive until closed
            while not ws.closed:
                await asyncio.sleep(SESSION_POLL_INTERVAL_SECONDS)
        except Exception as e:
            logger.debug("WebSocket session ended: %s", e)
        finally:
            await session.stop()
            if session in self._sessions:
                self._sessions.remove(session)
        
        return ws
