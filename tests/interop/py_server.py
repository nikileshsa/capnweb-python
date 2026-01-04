#!/usr/bin/env python3
"""Python test server for interop testing.

This server exposes a TestTarget that matches the TypeScript implementation
for cross-language protocol compliance testing.

Usage: python tests/interop/py_server.py <port>
"""

import asyncio
import sys
from aiohttp import web

# Add src to path for imports
sys.path.insert(0, str(__file__).replace('/tests/interop/py_server.py', '/src'))

from capnweb.ws_session import handle_websocket_rpc, WebSocketServerTransport
from capnweb.batch import aiohttp_batch_rpc_handler
from capnweb.rpc_session import BidirectionalSession

# Import test target
from test_target import TestTarget


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    """Handle WebSocket RPC connections."""
    return await handle_websocket_rpc(request, TestTarget())


async def batch_handler(request: web.Request) -> web.Response:
    """Handle HTTP batch RPC requests."""
    return await aiohttp_batch_rpc_handler(request, TestTarget())


async def main(port: int) -> None:
    """Start the server."""
    app = web.Application()
    
    # Route based on request type
    async def unified_handler(request: web.Request) -> web.StreamResponse:
        if request.headers.get('Upgrade', '').lower() == 'websocket':
            return await websocket_handler(request)
        elif request.method == 'POST':
            response = await batch_handler(request)
            response.headers['Access-Control-Allow-Origin'] = '*'
            return response
        else:
            return web.Response(
                text="This endpoint only accepts POST or WebSocket requests.",
                status=400
            )
    
    app.router.add_route('*', '/', unified_handler)
    app.router.add_route('*', '/rpc', unified_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', port)
    await site.start()
    
    print(f"Python interop server listening on port {port}")
    print(f"WebSocket: ws://localhost:{port}/rpc")
    print(f"HTTP Batch: http://localhost:{port}/")
    
    # Keep running until interrupted
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()


if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9200
    try:
        asyncio.run(main(port))
    except KeyboardInterrupt:
        print("Shutting down...")
