"""Connection lifecycle tests for Cap'n Web RPC.

Tests for connection establishment, clean shutdown, and error recovery.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from .conftest import ServerProcess, InteropClient


# =============================================================================
# Clean Shutdown Tests
# =============================================================================

@pytest.mark.asyncio
class TestCleanShutdown:
    """Test graceful connection shutdown."""
    
    async def test_client_closes_cleanly_ts(self, ts_server: ServerProcess):
        """Client can close connection cleanly after calls."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("square", [5])
            assert result == 25
        # Connection should be closed cleanly here
    
    async def test_client_closes_cleanly_py(self, py_server: ServerProcess):
        """Client can close connection cleanly after calls."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("square", [5])
            assert result == 25
    
    async def test_client_closes_without_calls_ts(self, ts_server: ServerProcess):
        """Client can close connection without making any calls."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            pass  # Just connect and disconnect
    
    async def test_client_closes_without_calls_py(self, py_server: ServerProcess):
        """Client can close connection without making any calls."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            pass
    
    async def test_multiple_sessions_sequential_ts(self, ts_server: ServerProcess):
        """Multiple sequential sessions work correctly."""
        for i in range(5):
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                result = await client.call("square", [i])
                assert result == i * i
    
    async def test_multiple_sessions_sequential_py(self, py_server: ServerProcess):
        """Multiple sequential sessions work correctly."""
        for i in range(5):
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                result = await client.call("square", [i])
                assert result == i * i


# =============================================================================
# Connection State Tests
# =============================================================================

@pytest.mark.asyncio
class TestConnectionState:
    """Test connection state handling."""
    
    async def test_call_after_close_fails_ts(self, ts_server: ServerProcess):
        """Calling after connection close raises error."""
        client = InteropClient(f"ws://127.0.0.1:{ts_server.port}/")
        await client.__aenter__()
        
        result = await client.call("square", [5])
        assert result == 25
        
        await client.__aexit__(None, None, None)
        
        # Call after close should fail
        with pytest.raises(Exception):
            await client.call("square", [5])
    
    async def test_call_after_close_fails_py(self, py_server: ServerProcess):
        """Calling after connection close raises error."""
        client = InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc")
        await client.__aenter__()
        
        result = await client.call("square", [5])
        assert result == 25
        
        await client.__aexit__(None, None, None)
        
        with pytest.raises(Exception):
            await client.call("square", [5])
    
    async def test_double_close_safe_ts(self, ts_server: ServerProcess):
        """Closing connection twice doesn't raise error."""
        client = InteropClient(f"ws://127.0.0.1:{ts_server.port}/")
        await client.__aenter__()
        await client.__aexit__(None, None, None)
        await client.__aexit__(None, None, None)  # Should not raise
    
    async def test_double_close_safe_py(self, py_server: ServerProcess):
        """Closing connection twice doesn't raise error."""
        client = InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc")
        await client.__aenter__()
        await client.__aexit__(None, None, None)
        await client.__aexit__(None, None, None)


# =============================================================================
# Reconnection Tests
# =============================================================================

@pytest.mark.asyncio
class TestReconnection:
    """Test reconnection after disconnect."""
    
    async def test_reconnect_after_close_ts(self, ts_server: ServerProcess):
        """Can reconnect after closing connection."""
        # First connection
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("square", [5])
            assert result == 25
        
        # Second connection (reconnect)
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("square", [6])
            assert result == 36
    
    async def test_reconnect_after_close_py(self, py_server: ServerProcess):
        """Can reconnect after closing connection."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("square", [5])
            assert result == 25
        
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("square", [6])
            assert result == 36
    
    async def test_many_reconnects_ts(self, ts_server: ServerProcess):
        """Many reconnections work correctly."""
        for i in range(10):
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                result = await client.call("add", [i, 1])
                assert result == i + 1
    
    async def test_many_reconnects_py(self, py_server: ServerProcess):
        """Many reconnections work correctly."""
        for i in range(10):
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                result = await client.call("add", [i, 1])
                assert result == i + 1


# =============================================================================
# Concurrent Connection Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentConnections:
    """Test multiple concurrent connections."""
    
    async def test_multiple_concurrent_clients_ts(self, ts_server: ServerProcess):
        """Multiple clients can connect concurrently."""
        async def client_task(client_id: int) -> int:
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                result = await client.call("square", [client_id])
                return result
        
        tasks = [client_task(i) for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        assert results == [0, 1, 4, 9, 16]
    
    async def test_multiple_concurrent_clients_py(self, py_server: ServerProcess):
        """Multiple clients can connect concurrently."""
        async def client_task(client_id: int) -> int:
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                result = await client.call("square", [client_id])
                return result
        
        tasks = [client_task(i) for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        assert results == [0, 1, 4, 9, 16]
    
    async def test_concurrent_clients_different_calls_ts(self, ts_server: ServerProcess):
        """Concurrent clients making different calls work."""
        async def square_client(n: int) -> int:
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                return await client.call("square", [n])
        
        async def add_client(a: int, b: int) -> int:
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                return await client.call("add", [a, b])
        
        tasks = [
            square_client(5),
            add_client(3, 4),
            square_client(6),
            add_client(10, 20),
        ]
        results = await asyncio.gather(*tasks)
        
        assert results == [25, 7, 36, 30]
    
    async def test_concurrent_clients_different_calls_py(self, py_server: ServerProcess):
        """Concurrent clients making different calls work."""
        async def square_client(n: int) -> int:
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                return await client.call("square", [n])
        
        async def add_client(a: int, b: int) -> int:
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                return await client.call("add", [a, b])
        
        tasks = [
            square_client(5),
            add_client(3, 4),
            square_client(6),
            add_client(10, 20),
        ]
        results = await asyncio.gather(*tasks)
        
        assert results == [25, 7, 36, 30]


# =============================================================================
# Error Recovery Tests
# =============================================================================

@pytest.mark.asyncio
class TestErrorRecovery:
    """Test recovery after errors."""
    
    async def test_call_after_error_ts(self, ts_server: ServerProcess):
        """Can make calls after an error."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # First call fails
            try:
                await client.call("throwError", ["test"])
            except Exception:
                pass
            
            # Second call should still work
            result = await client.call("square", [5])
            assert result == 25
    
    async def test_call_after_error_py(self, py_server: ServerProcess):
        """Can make calls after an error."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            try:
                await client.call("throwError", ["test"])
            except Exception:
                pass
            
            result = await client.call("square", [5])
            assert result == 25
    
    async def test_multiple_errors_then_success_ts(self, ts_server: ServerProcess):
        """Multiple errors followed by success works."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            for _ in range(3):
                try:
                    await client.call("throwError", ["test"])
                except Exception:
                    pass
            
            result = await client.call("add", [10, 20])
            assert result == 30
    
    async def test_multiple_errors_then_success_py(self, py_server: ServerProcess):
        """Multiple errors followed by success works."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            for _ in range(3):
                try:
                    await client.call("throwError", ["test"])
                except Exception:
                    pass
            
            result = await client.call("add", [10, 20])
            assert result == 30


# =============================================================================
# Abrupt Disconnect Tests
# =============================================================================

@pytest.mark.asyncio
class TestAbruptDisconnect:
    """Test handling of abrupt disconnections."""
    
    async def test_connect_to_invalid_port(self):
        """Connecting to invalid port fails gracefully."""
        with pytest.raises(Exception):
            async with InteropClient("ws://127.0.0.1:59999/") as client:
                await client.call("square", [5])
    
    async def test_session_handles_close_gracefully_ts(self, ts_server: ServerProcess):
        """Session handles close without pending calls gracefully."""
        client = InteropClient(f"ws://127.0.0.1:{ts_server.port}/")
        await client.__aenter__()
        
        # Make a call to ensure connection is established
        result = await client.call("square", [5])
        assert result == 25
        
        # Close should work cleanly
        await client.__aexit__(None, None, None)
    
    async def test_session_handles_close_gracefully_py(self, py_server: ServerProcess):
        """Session handles close without pending calls gracefully."""
        client = InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc")
        await client.__aenter__()
        
        result = await client.call("square", [5])
        assert result == 25
        
        await client.__aexit__(None, None, None)
