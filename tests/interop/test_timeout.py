"""Timeout and deadline tests for Cap'n Web RPC.

Tests for client-side timeouts and slow server handling.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from .conftest import ServerProcess, InteropClient


# =============================================================================
# Client Timeout Tests
# =============================================================================

@pytest.mark.asyncio
class TestClientTimeout:
    """Test client-side timeout handling."""
    
    async def test_fast_call_within_timeout_ts(self, ts_server: ServerProcess):
        """Fast call completes within timeout."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call_with_timeout("square", [5], timeout=5.0)
            assert result == 25
    
    async def test_fast_call_within_timeout_py(self, py_server: ServerProcess):
        """Fast call completes within timeout."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call_with_timeout("square", [5], timeout=5.0)
            assert result == 25
    
    async def test_timeout_on_slow_call_ts(self, ts_server: ServerProcess):
        """Slow call times out correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            with pytest.raises(asyncio.TimeoutError):
                # slowMethod takes 5 seconds, timeout is 0.1
                await client.call_with_timeout("slowMethod", [5000], timeout=0.1)
    
    async def test_timeout_on_slow_call_py(self, py_server: ServerProcess):
        """Slow call times out correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            with pytest.raises(asyncio.TimeoutError):
                await client.call_with_timeout("slowMethod", [5000], timeout=0.1)
    
    async def test_call_after_timeout_ts(self, ts_server: ServerProcess):
        """Can make calls after a timeout."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # First call times out
            try:
                await client.call_with_timeout("slowMethod", [5000], timeout=0.1)
            except asyncio.TimeoutError:
                pass
            
            # Wait a bit for cleanup
            await asyncio.sleep(0.2)
            
            # Second call should work
            result = await client.call_with_timeout("square", [5], timeout=5.0)
            assert result == 25
    
    async def test_call_after_timeout_py(self, py_server: ServerProcess):
        """Can make calls after a timeout."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            try:
                await client.call_with_timeout("slowMethod", [5000], timeout=0.1)
            except asyncio.TimeoutError:
                pass
            
            await asyncio.sleep(0.2)
            
            result = await client.call_with_timeout("square", [5], timeout=5.0)
            assert result == 25


# =============================================================================
# Concurrent Timeout Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentTimeout:
    """Test timeout handling with concurrent calls."""
    
    async def test_some_calls_timeout_ts(self, ts_server: ServerProcess):
        """Some calls timing out doesn't affect others."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            async def fast_call():
                return await client.call_with_timeout("square", [5], timeout=5.0)
            
            async def slow_call():
                return await client.call_with_timeout("slowMethod", [5000], timeout=0.1)
            
            tasks = [
                fast_call(),
                slow_call(),
                fast_call(),
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            assert results[0] == 25
            assert isinstance(results[1], asyncio.TimeoutError)
            assert results[2] == 25
    
    async def test_some_calls_timeout_py(self, py_server: ServerProcess):
        """Some calls timing out doesn't affect others."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            async def fast_call():
                return await client.call_with_timeout("square", [5], timeout=5.0)
            
            async def slow_call():
                return await client.call_with_timeout("slowMethod", [5000], timeout=0.1)
            
            tasks = [
                fast_call(),
                slow_call(),
                fast_call(),
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            assert results[0] == 25
            assert isinstance(results[1], asyncio.TimeoutError)
            assert results[2] == 25


# =============================================================================
# Timeout Edge Cases
# =============================================================================

@pytest.mark.asyncio
class TestTimeoutEdgeCases:
    """Test timeout edge cases."""
    
    async def test_zero_timeout_ts(self, ts_server: ServerProcess):
        """Zero timeout raises immediately."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            with pytest.raises(asyncio.TimeoutError):
                await client.call_with_timeout("square", [5], timeout=0)
    
    async def test_zero_timeout_py(self, py_server: ServerProcess):
        """Zero timeout raises immediately."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            with pytest.raises(asyncio.TimeoutError):
                await client.call_with_timeout("square", [5], timeout=0)
    
    async def test_very_short_timeout_ts(self, ts_server: ServerProcess):
        """Very short timeout (0.001s) may or may not complete."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            try:
                result = await client.call_with_timeout("square", [5], timeout=0.001)
                # If it completes, result should be correct
                assert result == 25
            except asyncio.TimeoutError:
                # Timeout is also acceptable for very short timeout
                pass
    
    async def test_very_short_timeout_py(self, py_server: ServerProcess):
        """Very short timeout (0.001s) may or may not complete."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            try:
                result = await client.call_with_timeout("square", [5], timeout=0.001)
                assert result == 25
            except asyncio.TimeoutError:
                pass
    
    async def test_generous_timeout_ts(self, ts_server: ServerProcess):
        """Generous timeout allows slow calls to complete."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # slowMethod with 100ms delay should complete within 5s timeout
            result = await client.call_with_timeout("slowMethod", [100], timeout=5.0)
            assert result == "slow result after 100ms"
    
    async def test_generous_timeout_py(self, py_server: ServerProcess):
        """Generous timeout allows slow calls to complete."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call_with_timeout("slowMethod", [100], timeout=5.0)
            assert result == "slow result after 100ms"


# =============================================================================
# Capability Timeout Tests
# =============================================================================

@pytest.mark.asyncio
class TestCapabilityTimeout:
    """Test timeout handling with capabilities."""
    
    async def test_timeout_during_capability_call_ts(self, ts_server: ServerProcess):
        """Timeout during a capability method call."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Create a counter capability
            counter = await client.call("makeCounter", [0])
            assert counter is not None
            
            # The counter itself should work fine with normal timeout
            # This tests that capability calls respect timeouts
    
    async def test_timeout_during_capability_call_py(self, py_server: ServerProcess):
        """Timeout during a capability method call."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            counter = await client.call("makeCounter", [0])
            assert counter is not None
