"""Concurrency tests for Cap'n Web RPC.

Inspired by grpclib's test_outbound_streams_limit, these tests verify:
1. Concurrent calls work correctly
2. Both sides can call simultaneously
3. Responses match requests
4. High throughput scenarios work
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from .conftest import ServerProcess, InteropClient


# =============================================================================
# Concurrent Calls Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentCalls:
    """Test concurrent call handling."""
    
    async def test_10_concurrent_calls_ts(self, ts_server: ServerProcess):
        """10 simultaneous calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = [client.call("square", [i]) for i in range(10)]
            results = await asyncio.gather(*tasks)
            
            assert len(results) == 10
            for i, result in enumerate(results):
                assert result == i * i
    
    async def test_10_concurrent_calls_py(self, py_server: ServerProcess):
        """10 simultaneous calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [client.call("square", [i]) for i in range(10)]
            results = await asyncio.gather(*tasks)
            
            assert len(results) == 10
            for i, result in enumerate(results):
                assert result == i * i
    
    async def test_50_concurrent_calls_ts(self, ts_server: ServerProcess):
        """50 simultaneous calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = [client.call("add", [i, i + 1]) for i in range(50)]
            results = await asyncio.gather(*tasks)
            
            assert len(results) == 50
            for i, result in enumerate(results):
                assert result == i + (i + 1)
    
    async def test_50_concurrent_calls_py(self, py_server: ServerProcess):
        """50 simultaneous calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [client.call("add", [i, i + 1]) for i in range(50)]
            results = await asyncio.gather(*tasks)
            
            assert len(results) == 50
            for i, result in enumerate(results):
                assert result == i + (i + 1)
    
    async def test_mixed_methods_concurrent_ts(self, ts_server: ServerProcess):
        """Concurrent calls to different methods work."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = [
                client.call("square", [5]),
                client.call("add", [3, 4]),
                client.call("greet", ["World"]),
                client.call("echo", ["test"]),
                client.call("square", [10]),
            ]
            results = await asyncio.gather(*tasks)
            
            assert results[0] == 25
            assert results[1] == 7
            assert results[2] == "Hello, World!"
            assert results[3] == "test"
            assert results[4] == 100
    
    async def test_mixed_methods_concurrent_py(self, py_server: ServerProcess):
        """Concurrent calls to different methods work."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [
                client.call("square", [5]),
                client.call("add", [3, 4]),
                client.call("greet", ["World"]),
                client.call("echo", ["test"]),
                client.call("square", [10]),
            ]
            results = await asyncio.gather(*tasks)
            
            assert results[0] == 25
            assert results[1] == 7
            assert results[2] == "Hello, World!"
            assert results[3] == "test"
            assert results[4] == 100


# =============================================================================
# Call Ordering Tests
# =============================================================================

@pytest.mark.asyncio
class TestCallOrdering:
    """Test that responses match requests correctly."""
    
    async def test_responses_match_requests_ts(self, ts_server: ServerProcess):
        """Each response matches its corresponding request."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Create tasks with unique identifiable results
            tasks = []
            expected = []
            for i in range(20):
                tasks.append(client.call("add", [i * 1000, i]))
                expected.append(i * 1000 + i)
            
            results = await asyncio.gather(*tasks)
            
            assert results == expected
    
    async def test_responses_match_requests_py(self, py_server: ServerProcess):
        """Each response matches its corresponding request."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = []
            expected = []
            for i in range(20):
                tasks.append(client.call("add", [i * 1000, i]))
                expected.append(i * 1000 + i)
            
            results = await asyncio.gather(*tasks)
            
            assert results == expected
    
    async def test_interleaved_calls_ts(self, ts_server: ServerProcess):
        """Interleaved calls and awaits work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Start some calls
            task1 = asyncio.create_task(client.call("square", [5]))
            task2 = asyncio.create_task(client.call("square", [6]))
            
            # Start more before first completes
            task3 = asyncio.create_task(client.call("square", [7]))
            
            # Await in different order
            result3 = await task3
            result1 = await task1
            result2 = await task2
            
            assert result1 == 25
            assert result2 == 36
            assert result3 == 49
    
    async def test_interleaved_calls_py(self, py_server: ServerProcess):
        """Interleaved calls and awaits work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            task1 = asyncio.create_task(client.call("square", [5]))
            task2 = asyncio.create_task(client.call("square", [6]))
            task3 = asyncio.create_task(client.call("square", [7]))
            
            result3 = await task3
            result1 = await task1
            result2 = await task2
            
            assert result1 == 25
            assert result2 == 36
            assert result3 == 49


# =============================================================================
# High Throughput Tests
# =============================================================================

@pytest.mark.asyncio
class TestHighThroughput:
    """Test high throughput scenarios."""
    
    async def test_100_sequential_calls_ts(self, ts_server: ServerProcess):
        """100 sequential calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            for i in range(100):
                result = await client.call("square", [i])
                assert result == i * i
    
    async def test_100_sequential_calls_py(self, py_server: ServerProcess):
        """100 sequential calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            for i in range(100):
                result = await client.call("square", [i])
                assert result == i * i
    
    async def test_100_concurrent_calls_ts(self, ts_server: ServerProcess):
        """100 concurrent calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = [client.call("square", [i]) for i in range(100)]
            results = await asyncio.gather(*tasks)
            
            assert len(results) == 100
            for i, result in enumerate(results):
                assert result == i * i
    
    async def test_100_concurrent_calls_py(self, py_server: ServerProcess):
        """100 concurrent calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [client.call("square", [i]) for i in range(100)]
            results = await asyncio.gather(*tasks)
            
            assert len(results) == 100
            for i, result in enumerate(results):
                assert result == i * i
    
    async def test_batched_concurrent_calls_ts(self, ts_server: ServerProcess):
        """Multiple batches of concurrent calls work."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            for batch in range(5):
                tasks = [client.call("add", [batch * 10 + i, 1]) for i in range(10)]
                results = await asyncio.gather(*tasks)
                
                for i, result in enumerate(results):
                    assert result == batch * 10 + i + 1
    
    async def test_batched_concurrent_calls_py(self, py_server: ServerProcess):
        """Multiple batches of concurrent calls work."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            for batch in range(5):
                tasks = [client.call("add", [batch * 10 + i, 1]) for i in range(10)]
                results = await asyncio.gather(*tasks)
                
                for i, result in enumerate(results):
                    assert result == batch * 10 + i + 1


# =============================================================================
# Concurrent Error Handling Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentErrors:
    """Test error handling in concurrent scenarios."""
    
    async def test_some_calls_fail_ts(self, ts_server: ServerProcess):
        """Some calls failing doesn't affect others."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = [
                client.call("square", [5]),
                client.call("throwError", ["error1"]),
                client.call("square", [6]),
                client.call("throwError", ["error2"]),
                client.call("square", [7]),
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            assert results[0] == 25
            assert isinstance(results[1], Exception)
            assert results[2] == 36
            assert isinstance(results[3], Exception)
            assert results[4] == 49
    
    async def test_some_calls_fail_py(self, py_server: ServerProcess):
        """Some calls failing doesn't affect others."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [
                client.call("square", [5]),
                client.call("throwError", ["error1"]),
                client.call("square", [6]),
                client.call("throwError", ["error2"]),
                client.call("square", [7]),
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            assert results[0] == 25
            assert isinstance(results[1], Exception)
            assert results[2] == 36
            assert isinstance(results[3], Exception)
            assert results[4] == 49
    
    async def test_all_calls_fail_ts(self, ts_server: ServerProcess):
        """All calls failing is handled correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = [
                client.call("throwError", [f"error{i}"])
                for i in range(5)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            assert all(isinstance(r, Exception) for r in results)
    
    async def test_all_calls_fail_py(self, py_server: ServerProcess):
        """All calls failing is handled correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [
                client.call("throwError", [f"error{i}"])
                for i in range(5)
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            assert all(isinstance(r, Exception) for r in results)


# =============================================================================
# Concurrent Capability Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentCapabilities:
    """Test concurrent capability operations."""
    
    async def test_concurrent_capability_creation_ts(self, ts_server: ServerProcess):
        """Creating multiple capabilities concurrently works."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = [client.call("makeCounter", [i]) for i in range(5)]
            counters = await asyncio.gather(*tasks)
            
            assert len(counters) == 5
            assert all(c is not None for c in counters)
    
    async def test_concurrent_capability_creation_py(self, py_server: ServerProcess):
        """Creating multiple capabilities concurrently works."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [client.call("makeCounter", [i]) for i in range(5)]
            counters = await asyncio.gather(*tasks)
            
            assert len(counters) == 5
            assert all(c is not None for c in counters)


# =============================================================================
# Concurrent Bidirectional Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentBidirectional:
    """Test bidirectional concurrent operations."""
    
    async def test_concurrent_different_methods_ts(self, ts_server: ServerProcess):
        """Concurrent calls to different methods work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Mix of different operations running concurrently
            tasks = [
                client.call("square", [5]),
                client.call("add", [10, 20]),
                client.call("greet", ["World"]),
                client.call("echo", [{"key": "value"}]),
                client.call("square", [7]),
            ]
            results = await asyncio.gather(*tasks)
            
            assert results[0] == 25
            assert results[1] == 30
            assert results[2] == "Hello, World!"
            assert results[3] == {"key": "value"}
            assert results[4] == 49
    
    async def test_concurrent_different_methods_py(self, py_server: ServerProcess):
        """Concurrent calls to different methods work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [
                client.call("square", [5]),
                client.call("add", [10, 20]),
                client.call("greet", ["World"]),
                client.call("echo", [{"key": "value"}]),
                client.call("square", [7]),
            ]
            results = await asyncio.gather(*tasks)
            
            assert results[0] == 25
            assert results[1] == 30
            assert results[2] == "Hello, World!"
            assert results[3] == {"key": "value"}
            assert results[4] == 49
    
    async def test_rapid_fire_calls_ts(self, ts_server: ServerProcess):
        """Rapid fire calls without waiting for responses."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Start many calls without awaiting
            tasks = []
            for i in range(30):
                tasks.append(asyncio.create_task(client.call("square", [i])))
            
            # Now await all
            results = await asyncio.gather(*tasks)
            
            for i, result in enumerate(results):
                assert result == i * i
    
    async def test_rapid_fire_calls_py(self, py_server: ServerProcess):
        """Rapid fire calls without waiting for responses."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = []
            for i in range(30):
                tasks.append(asyncio.create_task(client.call("square", [i])))
            
            results = await asyncio.gather(*tasks)
            
            for i, result in enumerate(results):
                assert result == i * i
