"""Tests for WebSocket RPC session using BidirectionalSession.

These tests verify the full bidirectional RPC functionality without mocks.
"""

import asyncio
import pytest

from capnweb.payload import RpcPayload
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from .support import rpc_call


class EchoService(RpcTarget):
    """Simple echo service for testing."""
    
    async def call(self, method: str, args: list) -> any:
        if method == "echo":
            return args[0] if args else ""
        elif method == "add":
            return args[0] + args[1]
        elif method == "greet":
            return f"Hello, {args[0]}!"
        elif method == "get_list":
            return [1, 2, 3, 4, 5]
        elif method == "slow":
            await asyncio.sleep(0.1)
            return "done"
        else:
            raise ValueError(f"Unknown method: {method}")
    
    def get_property(self, name: str) -> any:
        if name == "name":
            return "EchoService"
        raise AttributeError(f"Unknown property: {name}")


@pytest.mark.asyncio
class TestWebSocketRpcBasic:
    """Basic WebSocket RPC tests."""

    async def test_simple_call(self):
        """Test a simple RPC call."""
        server = WebSocketRpcServer(EchoService(), port=9001)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9001/rpc") as client:
                result = await rpc_call(client, "echo", ["Hello"])
                assert result == "Hello"
        finally:
            await server.stop()

    async def test_add_call(self):
        """Test RPC call with multiple arguments."""
        server = WebSocketRpcServer(EchoService(), port=9002)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9002/rpc") as client:
                result = await rpc_call(client, "add", [5, 3])
                assert result == 8
        finally:
            await server.stop()

    async def test_greet_call(self):
        """Test RPC call with string manipulation."""
        server = WebSocketRpcServer(EchoService(), port=9003)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9003/rpc") as client:
                result = await rpc_call(client, "greet", ["World"])
                assert result == "Hello, World!"
        finally:
            await server.stop()

    async def test_multiple_calls(self):
        """Test multiple sequential RPC calls."""
        server = WebSocketRpcServer(EchoService(), port=9004)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9004/rpc") as client:
                for i in range(10):
                    result = await rpc_call(client, "echo", [f"Message {i}"])
                    assert result == f"Message {i}"
        finally:
            await server.stop()

    async def test_get_stats(self):
        """Test session statistics."""
        server = WebSocketRpcServer(EchoService(), port=9005)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9005/rpc") as client:
                stats = client.get_stats()
                assert "imports" in stats
                assert "exports" in stats
                assert stats["imports"] >= 0
                assert stats["exports"] >= 0
        finally:
            await server.stop()


@pytest.mark.asyncio
class TestWebSocketRpcConcurrent:
    """Concurrent WebSocket RPC tests."""

    async def test_concurrent_calls(self):
        """Test multiple concurrent RPC calls."""
        server = WebSocketRpcServer(EchoService(), port=9006)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9006/rpc") as client:
                # Make 10 concurrent calls
                tasks = [
                    rpc_call(client, "echo", [f"Concurrent {i}"])
                    for i in range(10)
                ]
                results = await asyncio.gather(*tasks)
                
                for i, result in enumerate(results):
                    assert result == f"Concurrent {i}"
        finally:
            await server.stop()

    async def test_concurrent_clients(self):
        """Test multiple concurrent clients."""
        server = WebSocketRpcServer(EchoService(), port=9007)
        await server.start()
        
        try:
            async def client_task(client_id: int) -> list:
                async with WebSocketRpcClient("ws://localhost:9007/rpc") as client:
                    results = []
                    for i in range(5):
                        result = await rpc_call(client, "echo", [f"Client{client_id}-{i}"])
                        results.append(result)
                    return results
            
            # Run 5 clients concurrently
            tasks = [client_task(i) for i in range(5)]
            all_results = await asyncio.gather(*tasks)
            
            for client_id, results in enumerate(all_results):
                for i, result in enumerate(results):
                    assert result == f"Client{client_id}-{i}"
        finally:
            await server.stop()


@pytest.mark.asyncio
class TestWebSocketRpcStress:
    """Stress tests for WebSocket RPC."""

    async def test_rapid_calls(self):
        """Test rapid sequential calls."""
        server = WebSocketRpcServer(EchoService(), port=9008)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9008/rpc") as client:
                for i in range(100):
                    result = await rpc_call(client, "add", [i, 1])
                    assert result == i + 1
        finally:
            await server.stop()

    async def test_large_payload(self):
        """Test with large payload."""
        server = WebSocketRpcServer(EchoService(), port=9009)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9009/rpc") as client:
                large_string = "X" * 10000
                result = await rpc_call(client, "echo", [large_string])
                assert result == large_string
        finally:
            await server.stop()


@pytest.mark.asyncio
class TestWebSocketRpcBidirectional:
    """Bidirectional RPC tests (server calling client)."""

    async def test_client_provides_capability(self):
        """Test server calling back to client-provided capability."""
        
        class CallbackService(RpcTarget):
            """Service that calls back to client."""
            
            def __init__(self):
                self.client_callback: RpcStub | None = None
            
            async def call(self, method: str, args: list) -> any:
                if method == "register_callback":
                    self.client_callback = args[0]
                    return "registered"
                elif method == "trigger_callback":
                    if self.client_callback is None:
                        return "no callback"
                    hook = (
                        self.client_callback._hook
                        if isinstance(self.client_callback, RpcStub)
                        else self.client_callback
                    )
                    result_hook = hook.call(["notify"], RpcPayload.owned(["ping"]))
                    result = await result_hook.pull()
                    return result.value
                elif method == "echo":
                    return args[0]
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> any:
                raise AttributeError(f"Unknown property: {name}")
        
        server = WebSocketRpcServer(CallbackService(), port=9010)
        await server.start()
        
        try:
            # Client with its own capability
            class ClientCallback(RpcTarget):
                def __init__(self):
                    self.notifications: list[str] = []

                async def call(self, method: str, args: list) -> any:
                    if method == "notify":
                        self.notifications.append(args[0])
                        return f"Got: {args[0]}"
                    raise ValueError(f"Unknown method: {method}")
                
                def get_property(self, name: str) -> any:
                    raise AttributeError(f"Unknown property: {name}")
            
            local = ClientCallback()

            async with WebSocketRpcClient(
                "ws://localhost:9010/rpc",
                local_main=local,
            ) as client:
                assert client._session is not None

                # Send the client's local main capability to the server as a stub
                callback_stub = RpcStub(client._session.get_export(0).dup())
                result = await rpc_call(client, "register_callback", [callback_stub])
                assert result == "registered"

                # Now have server call back into the client via the stored capability
                result = await rpc_call(client, "trigger_callback", [])
                assert result == "Got: ping"
                assert local.notifications == ["ping"]

                # Simple call to verify connection works
                result = await rpc_call(client, "echo", ["test"])
                assert result == "test"
        finally:
            await server.stop()


@pytest.mark.asyncio
class TestWebSocketRpcDrain:
    """Tests for drain() functionality."""

    async def test_drain_empty(self):
        """Test drain with no pending operations."""
        server = WebSocketRpcServer(EchoService(), port=9011)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9011/rpc") as client:
                # Should complete immediately
                await asyncio.wait_for(client.drain(), timeout=1.0)
        finally:
            await server.stop()

    async def test_drain_after_calls(self):
        """Test drain after making calls."""
        server = WebSocketRpcServer(EchoService(), port=9012)
        await server.start()
        
        try:
            async with WebSocketRpcClient("ws://localhost:9012/rpc") as client:
                # Make some calls
                for i in range(5):
                    await rpc_call(client, "echo", [f"msg{i}"])
                
                # Drain should complete
                await asyncio.wait_for(client.drain(), timeout=1.0)
        finally:
            await server.stop()
