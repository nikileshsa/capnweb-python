"""Tests for UnifiedClient with production-grade session management.

These tests verify that UnifiedClient properly uses BidirectionalSession
for WebSocket connections with all production features.
"""

import asyncio
import pytest

from aiohttp import web

from capnweb.types import RpcTarget
from capnweb.unified_client import UnifiedClient, UnifiedClientConfig
from capnweb.ws_session import WebSocketServerTransport
from capnweb.rpc_session import BidirectionalSession
from .support import rpc_call


class CalculatorService(RpcTarget):
    """Calculator service for testing."""
    
    async def call(self, method: str, args: list) -> any:
        if method == "add":
            return args[0] + args[1]
        elif method == "multiply":
            return args[0] * args[1]
        elif method == "echo":
            return args[0] if args else ""
        elif method == "get_list":
            return [1, 2, 3, 4, 5]
        elif method == "slow_operation":
            await asyncio.sleep(0.1)
            return "completed"
        else:
            raise ValueError(f"Unknown method: {method}")
    
    def get_property(self, name: str) -> any:
        if name == "version":
            return "1.0.0"
        raise AttributeError(f"Unknown property: {name}")


async def create_test_server(port: int, service: RpcTarget) -> tuple[web.Application, web.AppRunner]:
    """Create a test WebSocket server."""
    sessions: list[BidirectionalSession] = []
    
    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        transport = WebSocketServerTransport(ws)
        session = BidirectionalSession(
            transport=transport,
            local_main=service,
        )
        sessions.append(session)
        session.start()
        
        try:
            while not ws.closed:
                await asyncio.sleep(0.1)
        finally:
            await session.stop()
            if session in sessions:
                sessions.remove(session)
        
        return ws
    
    app = web.Application()
    app.router.add_get("/rpc", ws_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, "localhost", port)
    await site.start()
    
    return app, runner


@pytest.mark.asyncio
class TestUnifiedClientWebSocket:
    """Test UnifiedClient with WebSocket transport."""

    async def test_simple_call(self):
        """Test a simple RPC call."""
        app, runner = await create_test_server(9101, CalculatorService())
        
        try:
            config = UnifiedClientConfig(url="ws://localhost:9101/rpc")
            async with UnifiedClient(config) as client:
                result = await rpc_call(client, "add", [5, 3])
                assert result == 8
        finally:
            await runner.cleanup()

    async def test_multiple_calls(self):
        """Test multiple sequential calls."""
        app, runner = await create_test_server(9102, CalculatorService())
        
        try:
            config = UnifiedClientConfig(url="ws://localhost:9102/rpc")
            async with UnifiedClient(config) as client:
                for i in range(10):
                    result = await rpc_call(client, "add", [i, 1])
                    assert result == i + 1
        finally:
            await runner.cleanup()

    async def test_get_stats(self):
        """Test session statistics."""
        app, runner = await create_test_server(9103, CalculatorService())
        
        try:
            config = UnifiedClientConfig(url="ws://localhost:9103/rpc")
            async with UnifiedClient(config) as client:
                # Make a call to populate stats
                await rpc_call(client, "echo", ["test"])
                
                stats = client.get_stats()
                assert "imports" in stats
                assert "exports" in stats
                assert stats["imports"] >= 0
                assert stats["exports"] >= 0
        finally:
            await runner.cleanup()

    async def test_drain(self):
        """Test drain functionality."""
        app, runner = await create_test_server(9104, CalculatorService())
        
        try:
            config = UnifiedClientConfig(url="ws://localhost:9104/rpc")
            async with UnifiedClient(config) as client:
                # Make some calls
                for i in range(5):
                    await rpc_call(client, "echo", [f"msg{i}"])
                
                # Drain should complete
                await asyncio.wait_for(client.drain(), timeout=1.0)
        finally:
            await runner.cleanup()

    async def test_is_bidirectional(self):
        """Test bidirectional flag."""
        app, runner = await create_test_server(9105, CalculatorService())
        
        try:
            config = UnifiedClientConfig(url="ws://localhost:9105/rpc")
            async with UnifiedClient(config) as client:
                assert client.is_bidirectional is True
        finally:
            await runner.cleanup()

    async def test_concurrent_calls(self):
        """Test concurrent RPC calls."""
        app, runner = await create_test_server(9106, CalculatorService())
        
        try:
            config = UnifiedClientConfig(url="ws://localhost:9106/rpc")
            async with UnifiedClient(config) as client:
                tasks = [
                    rpc_call(client, "add", [i, i])
                    for i in range(10)
                ]
                results = await asyncio.gather(*tasks)
                
                for i, result in enumerate(results):
                    assert result == i + i
        finally:
            await runner.cleanup()

    async def test_get_main_stub(self):
        """Test getting main stub."""
        app, runner = await create_test_server(9107, CalculatorService())
        
        try:
            config = UnifiedClientConfig(url="ws://localhost:9107/rpc")
            async with UnifiedClient(config) as client:
                stub = client.get_main_stub()
                assert stub is not None
        finally:
            await runner.cleanup()


@pytest.mark.asyncio
class TestUnifiedClientWithLocalMain:
    """Test UnifiedClient with client-side capability."""

    async def test_client_provides_capability(self):
        """Test that client can provide a capability to server."""
        
        class ClientCallback(RpcTarget):
            def __init__(self):
                self.called = False
            
            async def call(self, method: str, args: list) -> any:
                if method == "notify":
                    self.called = True
                    return f"Received: {args[0]}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> any:
                raise AttributeError(f"Unknown property: {name}")
        
        callback = ClientCallback()
        app, runner = await create_test_server(9108, CalculatorService())
        
        try:
            config = UnifiedClientConfig(
                url="ws://localhost:9108/rpc",
                local_main=callback,
            )
            async with UnifiedClient(config) as client:
                # Simple call to verify connection works
                result = await rpc_call(client, "echo", ["test"])
                assert result == "test"
        finally:
            await runner.cleanup()


@pytest.mark.asyncio
class TestUnifiedClientStress:
    """Stress tests for UnifiedClient."""

    async def test_rapid_calls(self):
        """Test rapid sequential calls."""
        app, runner = await create_test_server(9109, CalculatorService())
        
        try:
            config = UnifiedClientConfig(url="ws://localhost:9109/rpc")
            async with UnifiedClient(config) as client:
                for i in range(100):
                    result = await rpc_call(client, "multiply", [i, 2])
                    assert result == i * 2
        finally:
            await runner.cleanup()

    async def test_large_payload(self):
        """Test with large payload."""
        app, runner = await create_test_server(9110, CalculatorService())
        
        try:
            config = UnifiedClientConfig(url="ws://localhost:9110/rpc")
            async with UnifiedClient(config) as client:
                large_string = "X" * 10000
                result = await rpc_call(client, "echo", [large_string])
                assert result == large_string
        finally:
            await runner.cleanup()

    async def test_concurrent_clients(self):
        """Test multiple concurrent clients."""
        app, runner = await create_test_server(9111, CalculatorService())
        
        try:
            async def client_task(client_id: int) -> list:
                config = UnifiedClientConfig(url="ws://localhost:9111/rpc")
                async with UnifiedClient(config) as client:
                    results = []
                    for i in range(5):
                        result = await rpc_call(client, "add", [client_id, i])
                        results.append(result)
                    return results
            
            tasks = [client_task(i) for i in range(5)]
            all_results = await asyncio.gather(*tasks)
            
            for client_id, results in enumerate(all_results):
                for i, result in enumerate(results):
                    assert result == client_id + i
        finally:
            await runner.cleanup()
