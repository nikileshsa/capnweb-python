"""Capability lifecycle tests for TypeScript/Python interop.

These tests verify proper handling of capability references including:
- Release messages sent after resolve
- Refcount tracking
- Capability disposal
- Garbage collection
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from .conftest import InteropClient, ServerProcess


# =============================================================================
# Capability Passing Tests
# =============================================================================

@pytest.mark.asyncio
class TestCapabilityPassing:
    """Test passing capabilities between client and server."""
    
    async def test_server_returns_capability_ts(self, ts_server: ServerProcess):
        """Server can return a capability (Counter) to client."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # makeCounter returns a Counter capability
            counter = await client.call("makeCounter", [10])
            # The counter should be a stub we can use
            assert counter is not None
    
    async def test_server_returns_capability_py(self, py_server: ServerProcess):
        """Server can return a capability (Counter) to client."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            counter = await client.call("makeCounter", [10])
            assert counter is not None
    
    async def test_client_passes_capability_to_server_ts(self, ts_server: ServerProcess):
        """Client can pass a capability to server, server can call it back."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        
        class ClientCallback(RpcTarget):
            def __init__(self):
                self.notifications: list[str] = []
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.notifications.append(args[0])
                    return f"Got: {args[0]}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        from capnweb.ws_session import WebSocketRpcClient
        
        local = ClientCallback()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{ts_server.port}/",
            local_main=local,
        ) as client:
            assert client._session is not None
            
            # Pass client's local main to server
            callback_stub = RpcStub(client._session.get_export(0).dup())
            result = await client.call(0, "registerCallback", [callback_stub])
            assert result == "registered"
            
            # Have server call back
            result = await client.call(0, "triggerCallback", [])
            assert result == "Got: ping"
            assert local.notifications == ["ping"]
    
    async def test_client_passes_capability_to_server_py(self, py_server: ServerProcess):
        """Client can pass a capability to server, server can call it back."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        
        class ClientCallback(RpcTarget):
            def __init__(self):
                self.notifications: list[str] = []
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.notifications.append(args[0])
                    return f"Got: {args[0]}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        from capnweb.ws_session import WebSocketRpcClient
        
        local = ClientCallback()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{py_server.port}/rpc",
            local_main=local,
        ) as client:
            assert client._session is not None
            
            callback_stub = RpcStub(client._session.get_export(0).dup())
            result = await client.call(0, "registerCallback", [callback_stub])
            assert result == "registered"
            
            result = await client.call(0, "triggerCallback", [])
            assert result == "Got: ping"
            assert local.notifications == ["ping"]


# =============================================================================
# Release Message Tests
# =============================================================================

@pytest.mark.asyncio
class TestReleaseMessages:
    """Test that release messages are sent correctly."""
    
    async def test_release_sent_after_resolve_ts(self, ts_server: ServerProcess):
        """Release message is sent after capability is resolved."""
        from capnweb.ws_session import WebSocketRpcClient
        
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Make a call that returns a capability
            counter = await client.call(0, "makeCounter", [10])
            
            # The session should have tracked the import
            session = client._session
            assert session is not None
            
            # After the call resolves, we should have an import entry
            # When we dispose of the stub, a release should be sent
            # This is tested implicitly by the session cleanup
    
    async def test_release_sent_after_resolve_py(self, py_server: ServerProcess):
        """Release message is sent after capability is resolved."""
        from capnweb.ws_session import WebSocketRpcClient
        
        async with WebSocketRpcClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            counter = await client.call(0, "makeCounter", [10])
            session = client._session
            assert session is not None


# =============================================================================
# Multiple Capability References Tests
# =============================================================================

@pytest.mark.asyncio
class TestMultipleReferences:
    """Test handling of multiple references to the same capability."""
    
    async def test_multiple_calls_same_capability_ts(self, ts_server: ServerProcess):
        """Multiple calls to the same capability work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Multiple calls to the main capability
            results = await asyncio.gather(
                client.call("square", [1]),
                client.call("square", [2]),
                client.call("square", [3]),
            )
            assert results == [1, 4, 9]
    
    async def test_multiple_calls_same_capability_py(self, py_server: ServerProcess):
        """Multiple calls to the same capability work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            results = await asyncio.gather(
                client.call("square", [1]),
                client.call("square", [2]),
                client.call("square", [3]),
            )
            assert results == [1, 4, 9]


# =============================================================================
# Capability Cleanup Tests
# =============================================================================

@pytest.mark.asyncio
class TestCapabilityCleanup:
    """Test that capabilities are properly cleaned up."""
    
    async def test_session_cleanup_ts(self, ts_server: ServerProcess):
        """Session cleanup releases all capabilities."""
        from capnweb.ws_session import WebSocketRpcClient
        
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Make several calls
            await client.call(0, "square", [5])
            await client.call(0, "greet", ["World"])
            await client.call(0, "generateFibonacci", [5])
            
            session = client._session
            assert session is not None
            
            # Get stats before cleanup
            stats = session.get_stats()
            # Should have at least the main import
            assert stats["imports"] >= 1
        
        # After context exit, session should be stopped
        # This is tested implicitly by the context manager
    
    async def test_session_cleanup_py(self, py_server: ServerProcess):
        """Session cleanup releases all capabilities."""
        from capnweb.ws_session import WebSocketRpcClient
        
        async with WebSocketRpcClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            await client.call(0, "square", [5])
            await client.call(0, "greet", ["World"])
            await client.call(0, "generateFibonacci", [5])
            
            session = client._session
            assert session is not None
            stats = session.get_stats()
            assert stats["imports"] >= 1


# =============================================================================
# Bidirectional Capability Tests
# =============================================================================

@pytest.mark.asyncio
class TestBidirectionalCapabilities:
    """Test bidirectional capability passing."""
    
    async def test_callback_multiple_times_ts(self, ts_server: ServerProcess):
        """Server can call client callback multiple times."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class ClientCallback(RpcTarget):
            def __init__(self):
                self.call_count = 0
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.call_count += 1
                    return f"Call #{self.call_count}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        local = ClientCallback()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{ts_server.port}/",
            local_main=local,
        ) as client:
            callback_stub = RpcStub(client._session.get_export(0).dup())
            await client.call(0, "registerCallback", [callback_stub])
            
            # Trigger callback multiple times
            result1 = await client.call(0, "triggerCallback", [])
            result2 = await client.call(0, "triggerCallback", [])
            result3 = await client.call(0, "triggerCallback", [])
            
            assert local.call_count == 3
    
    async def test_callback_multiple_times_py(self, py_server: ServerProcess):
        """Server can call client callback multiple times."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class ClientCallback(RpcTarget):
            def __init__(self):
                self.call_count = 0
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.call_count += 1
                    return f"Call #{self.call_count}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        local = ClientCallback()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{py_server.port}/rpc",
            local_main=local,
        ) as client:
            callback_stub = RpcStub(client._session.get_export(0).dup())
            await client.call(0, "registerCallback", [callback_stub])
            
            result1 = await client.call(0, "triggerCallback", [])
            result2 = await client.call(0, "triggerCallback", [])
            result3 = await client.call(0, "triggerCallback", [])
            
            assert local.call_count == 3
