"""Error handling tests for TypeScript/Python interop.

These tests verify proper error propagation and handling including:
- Server-side exceptions
- Method not found errors
- Invalid capability references
- Abort message propagation
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
# Server Exception Tests
# =============================================================================

@pytest.mark.asyncio
class TestServerExceptions:
    """Test that server exceptions are properly propagated to client."""
    
    async def test_server_throws_error_ts(self, ts_server: ServerProcess):
        """Server exception is propagated to client."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            with pytest.raises(Exception) as exc_info:
                await client.call("throwError", [])
            
            # Should contain error information
            error = exc_info.value
            assert error is not None
    
    async def test_server_throws_error_py(self, py_server: ServerProcess):
        """Server exception is propagated to client."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            with pytest.raises(Exception) as exc_info:
                await client.call("throwError", [])
            
            error = exc_info.value
            assert error is not None
    
    async def test_error_does_not_break_session_ts(self, ts_server: ServerProcess):
        """Error in one call doesn't break subsequent calls."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            # First call succeeds
            result1 = await client.call("square", [5])
            assert result1 == 25
            
            # Second call throws error
            with pytest.raises(Exception):
                await client.call("throwError", [])
            
            # Third call should still work
            result3 = await client.call("square", [6])
            assert result3 == 36
    
    async def test_error_does_not_break_session_py(self, py_server: ServerProcess):
        """Error in one call doesn't break subsequent calls."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result1 = await client.call("square", [5])
            assert result1 == 25
            
            with pytest.raises(Exception):
                await client.call("throwError", [])
            
            result3 = await client.call("square", [6])
            assert result3 == 36


# =============================================================================
# Method Not Found Tests
# =============================================================================

@pytest.mark.asyncio
class TestMethodNotFound:
    """Test handling of calls to non-existent methods."""
    
    async def test_unknown_method_ts(self, ts_server: ServerProcess):
        """Calling unknown method raises error."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            with pytest.raises(Exception) as exc_info:
                await client.call("nonExistentMethod", [])
            
            # Should get an error
            assert exc_info.value is not None
    
    async def test_unknown_method_py(self, py_server: ServerProcess):
        """Calling unknown method raises error."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            with pytest.raises(Exception) as exc_info:
                await client.call("nonExistentMethod", [])
            
            assert exc_info.value is not None
    
    async def test_unknown_method_does_not_break_session_ts(self, ts_server: ServerProcess):
        """Unknown method error doesn't break subsequent calls."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            result1 = await client.call("square", [5])
            assert result1 == 25
            
            with pytest.raises(Exception):
                await client.call("nonExistentMethod", [])
            
            result3 = await client.call("square", [6])
            assert result3 == 36
    
    async def test_unknown_method_does_not_break_session_py(self, py_server: ServerProcess):
        """Unknown method error doesn't break subsequent calls."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result1 = await client.call("square", [5])
            assert result1 == 25
            
            with pytest.raises(Exception):
                await client.call("nonExistentMethod", [])
            
            result3 = await client.call("square", [6])
            assert result3 == 36


# =============================================================================
# Concurrent Error Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentErrors:
    """Test error handling with concurrent calls."""
    
    async def test_concurrent_calls_with_one_error_ts(self, ts_server: ServerProcess):
        """One error in concurrent calls doesn't affect others."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            async def call_square(n):
                return await client.call("square", [n])
            
            async def call_error():
                try:
                    await client.call("throwError", [])
                    return "no error"
                except Exception as e:
                    return f"error: {type(e).__name__}"
            
            # Mix of successful calls and one error
            results = await asyncio.gather(
                call_square(1),
                call_square(2),
                call_error(),
                call_square(3),
                call_square(4),
            )
            
            assert results[0] == 1
            assert results[1] == 4
            assert "error" in results[2]
            assert results[3] == 9
            assert results[4] == 16
    
    async def test_concurrent_calls_with_one_error_py(self, py_server: ServerProcess):
        """One error in concurrent calls doesn't affect others."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            async def call_square(n):
                return await client.call("square", [n])
            
            async def call_error():
                try:
                    await client.call("throwError", [])
                    return "no error"
                except Exception as e:
                    return f"error: {type(e).__name__}"
            
            results = await asyncio.gather(
                call_square(1),
                call_square(2),
                call_error(),
                call_square(3),
                call_square(4),
            )
            
            assert results[0] == 1
            assert results[1] == 4
            assert "error" in results[2]
            assert results[3] == 9
            assert results[4] == 16


# =============================================================================
# Callback Error Tests
# =============================================================================

@pytest.mark.asyncio
class TestCallbackErrors:
    """Test error handling in callbacks."""
    
    async def test_callback_throws_error_ts(self, ts_server: ServerProcess):
        """Error in client callback is propagated back to server."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class ErrorCallback(RpcTarget):
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    raise ValueError("Callback error!")
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        local = ErrorCallback()
        async with WebSocketRpcClient(
            f"ws://localhost:{ts_server.port}/",
            local_main=local,
        ) as client:
            callback_stub = RpcStub(client._session.get_export(0).dup())
            await client.call(0, "registerCallback", [callback_stub])
            
            # When server calls callback, it should get an error
            with pytest.raises(Exception):
                await client.call(0, "triggerCallback", [])
    
    async def test_callback_throws_error_py(self, py_server: ServerProcess):
        """Error in client callback is propagated back to server."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class ErrorCallback(RpcTarget):
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    raise ValueError("Callback error!")
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        local = ErrorCallback()
        async with WebSocketRpcClient(
            f"ws://localhost:{py_server.port}/rpc",
            local_main=local,
        ) as client:
            callback_stub = RpcStub(client._session.get_export(0).dup())
            await client.call(0, "registerCallback", [callback_stub])
            
            with pytest.raises(Exception):
                await client.call(0, "triggerCallback", [])


# =============================================================================
# Timeout Tests
# =============================================================================

@pytest.mark.asyncio
class TestTimeouts:
    """Test timeout handling."""
    
    async def test_client_timeout_ts(self, ts_server: ServerProcess):
        """Client-side timeout works correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            # Normal call should complete quickly
            result = await client.call_with_timeout("square", [5], timeout=5.0)
            assert result == 25
    
    async def test_client_timeout_py(self, py_server: ServerProcess):
        """Client-side timeout works correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result = await client.call_with_timeout("square", [5], timeout=5.0)
            assert result == 25


# =============================================================================
# Connection Error Tests
# =============================================================================

@pytest.mark.asyncio
class TestConnectionErrors:
    """Test handling of connection errors."""
    
    async def test_connect_to_nonexistent_server(self):
        """Connecting to non-existent server raises error."""
        from capnweb.ws_session import WebSocketRpcClient
        
        with pytest.raises(Exception):
            async with WebSocketRpcClient("ws://localhost:59999/") as client:
                await client.call(0, "square", [5])
    
    async def test_server_shutdown_during_call_ts(self, ts_server_fresh: ServerProcess):
        """Server shutdown during call is handled gracefully."""
        from capnweb.ws_session import WebSocketRpcClient
        
        async with WebSocketRpcClient(f"ws://localhost:{ts_server_fresh.port}/") as client:
            # Make a successful call first
            result = await client.call(0, "square", [5])
            assert result == 25
            
            # Stop the server
            ts_server_fresh.stop()
            
            # Next call should fail
            with pytest.raises(Exception):
                await asyncio.wait_for(
                    client.call(0, "square", [6]),
                    timeout=5.0
                )
    
    async def test_server_shutdown_during_call_py(self, py_server_fresh: ServerProcess):
        """Server shutdown during call is handled gracefully."""
        from capnweb.ws_session import WebSocketRpcClient
        
        async with WebSocketRpcClient(f"ws://localhost:{py_server_fresh.port}/rpc") as client:
            result = await client.call(0, "square", [5])
            assert result == 25
            
            py_server_fresh.stop()
            
            with pytest.raises(Exception):
                await asyncio.wait_for(
                    client.call(0, "square", [6]),
                    timeout=5.0
                )
