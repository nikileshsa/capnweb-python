"""Stress and performance tests for Cap'n Web RPC.

These tests verify the system handles high load scenarios:
1. High throughput (many calls per second)
2. Large payloads
3. Many concurrent connections
4. Sustained load over time
5. Burst traffic patterns
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from .conftest import ServerProcess, InteropClient


# =============================================================================
# High Throughput Tests
# =============================================================================

@pytest.mark.asyncio
class TestHighThroughput:
    """Test high throughput scenarios."""
    
    async def test_500_sequential_calls_ts(self, ts_server: ServerProcess):
        """500 sequential calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            start = time.time()
            for i in range(500):
                result = await client.call("square", [i % 100])
                assert result == (i % 100) ** 2
            elapsed = time.time() - start
            
            # Should complete in reasonable time (< 30s)
            assert elapsed < 30, f"500 calls took {elapsed:.2f}s"
    
    async def test_500_sequential_calls_py(self, py_server: ServerProcess):
        """500 sequential calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            start = time.time()
            for i in range(500):
                result = await client.call("square", [i % 100])
                assert result == (i % 100) ** 2
            elapsed = time.time() - start
            
            assert elapsed < 30, f"500 calls took {elapsed:.2f}s"
    
    async def test_200_concurrent_calls_ts(self, ts_server: ServerProcess):
        """200 concurrent calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            start = time.time()
            tasks = [client.call("add", [i, i + 1]) for i in range(200)]
            results = await asyncio.gather(*tasks)
            elapsed = time.time() - start
            
            assert len(results) == 200
            for i, result in enumerate(results):
                assert result == i + (i + 1)
            
            # Should complete quickly (< 10s)
            assert elapsed < 10, f"200 concurrent calls took {elapsed:.2f}s"
    
    async def test_200_concurrent_calls_py(self, py_server: ServerProcess):
        """200 concurrent calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            start = time.time()
            tasks = [client.call("add", [i, i + 1]) for i in range(200)]
            results = await asyncio.gather(*tasks)
            elapsed = time.time() - start
            
            assert len(results) == 200
            for i, result in enumerate(results):
                assert result == i + (i + 1)
            
            assert elapsed < 10, f"200 concurrent calls took {elapsed:.2f}s"


# =============================================================================
# Large Payload Tests
# =============================================================================

@pytest.mark.asyncio
class TestLargePayloads:
    """Test large payload handling."""
    
    async def test_1mb_string_ts(self, ts_server: ServerProcess):
        """1MB string payload round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            large_string = "x" * (1024 * 1024)  # 1MB
            result = await client.call("echo", [large_string])
            assert result == large_string
    
    async def test_1mb_string_py(self, py_server: ServerProcess):
        """1MB string payload round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            large_string = "x" * (1024 * 1024)  # 1MB
            result = await client.call("echo", [large_string])
            assert result == large_string
    
    async def test_large_array_ts(self, ts_server: ServerProcess):
        """Large array (10000 elements) round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            large_array = list(range(10000))
            result = await client.call("echo", [large_array])
            assert result == large_array
    
    async def test_large_array_py(self, py_server: ServerProcess):
        """Large array (10000 elements) round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            large_array = list(range(10000))
            result = await client.call("echo", [large_array])
            assert result == large_array
    
    async def test_large_nested_object_ts(self, ts_server: ServerProcess):
        """Large nested object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Create object with 1000 keys
            large_obj = {f"key_{i}": {"value": i, "nested": {"data": f"item_{i}"}} for i in range(1000)}
            result = await client.call("echo", [large_obj])
            assert result == large_obj
    
    async def test_large_nested_object_py(self, py_server: ServerProcess):
        """Large nested object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            large_obj = {f"key_{i}": {"value": i, "nested": {"data": f"item_{i}"}} for i in range(1000)}
            result = await client.call("echo", [large_obj])
            assert result == large_obj


# =============================================================================
# Many Connections Tests
# =============================================================================

@pytest.mark.asyncio
class TestManyConnections:
    """Test many concurrent connections."""
    
    async def test_20_concurrent_connections_ts(self, ts_server: ServerProcess):
        """20 concurrent connections work correctly."""
        async def client_task(client_id: int) -> int:
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                result = await client.call("square", [client_id])
                return result
        
        tasks = [client_task(i) for i in range(20)]
        results = await asyncio.gather(*tasks)
        
        for i, result in enumerate(results):
            assert result == i * i
    
    async def test_20_concurrent_connections_py(self, py_server: ServerProcess):
        """20 concurrent connections work correctly."""
        async def client_task(client_id: int) -> int:
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                result = await client.call("square", [client_id])
                return result
        
        tasks = [client_task(i) for i in range(20)]
        results = await asyncio.gather(*tasks)
        
        for i, result in enumerate(results):
            assert result == i * i
    
    async def test_connections_with_multiple_calls_ts(self, ts_server: ServerProcess):
        """Multiple connections each making multiple calls."""
        async def client_task(client_id: int) -> list[int]:
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                results = []
                for j in range(10):
                    result = await client.call("add", [client_id, j])
                    results.append(result)
                return results
        
        tasks = [client_task(i) for i in range(10)]
        all_results = await asyncio.gather(*tasks)
        
        for i, results in enumerate(all_results):
            for j, result in enumerate(results):
                assert result == i + j
    
    async def test_connections_with_multiple_calls_py(self, py_server: ServerProcess):
        """Multiple connections each making multiple calls."""
        async def client_task(client_id: int) -> list[int]:
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                results = []
                for j in range(10):
                    result = await client.call("add", [client_id, j])
                    results.append(result)
                return results
        
        tasks = [client_task(i) for i in range(10)]
        all_results = await asyncio.gather(*tasks)
        
        for i, results in enumerate(all_results):
            for j, result in enumerate(results):
                assert result == i + j


# =============================================================================
# Sustained Load Tests
# =============================================================================

@pytest.mark.asyncio
class TestSustainedLoad:
    """Test sustained load over time."""
    
    async def test_sustained_calls_5_seconds_ts(self, ts_server: ServerProcess):
        """Sustained calls for 5 seconds work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            start = time.time()
            call_count = 0
            
            while time.time() - start < 5:
                result = await client.call("square", [call_count % 100])
                assert result == (call_count % 100) ** 2
                call_count += 1
            
            # Should have made many calls
            assert call_count > 100, f"Only made {call_count} calls in 5 seconds"
    
    async def test_sustained_calls_5_seconds_py(self, py_server: ServerProcess):
        """Sustained calls for 5 seconds work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            start = time.time()
            call_count = 0
            
            while time.time() - start < 5:
                result = await client.call("square", [call_count % 100])
                assert result == (call_count % 100) ** 2
                call_count += 1
            
            assert call_count > 100, f"Only made {call_count} calls in 5 seconds"
    
    async def test_sustained_concurrent_calls_ts(self, ts_server: ServerProcess):
        """Sustained concurrent calls for 3 seconds."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            start = time.time()
            batch_count = 0
            
            while time.time() - start < 3:
                tasks = [client.call("add", [batch_count, i]) for i in range(10)]
                results = await asyncio.gather(*tasks)
                
                for i, result in enumerate(results):
                    assert result == batch_count + i
                batch_count += 1
            
            assert batch_count > 10, f"Only completed {batch_count} batches"
    
    async def test_sustained_concurrent_calls_py(self, py_server: ServerProcess):
        """Sustained concurrent calls for 3 seconds."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            start = time.time()
            batch_count = 0
            
            while time.time() - start < 3:
                tasks = [client.call("add", [batch_count, i]) for i in range(10)]
                results = await asyncio.gather(*tasks)
                
                for i, result in enumerate(results):
                    assert result == batch_count + i
                batch_count += 1
            
            assert batch_count > 10, f"Only completed {batch_count} batches"


# =============================================================================
# Burst Traffic Tests
# =============================================================================

@pytest.mark.asyncio
class TestBurstTraffic:
    """Test burst traffic patterns."""
    
    async def test_burst_then_idle_ts(self, ts_server: ServerProcess):
        """Burst of calls, then idle, then more calls."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Burst 1
            tasks = [client.call("square", [i]) for i in range(50)]
            results = await asyncio.gather(*tasks)
            assert all(results[i] == i * i for i in range(50))
            
            # Idle
            await asyncio.sleep(0.5)
            
            # Burst 2
            tasks = [client.call("add", [i, 100]) for i in range(50)]
            results = await asyncio.gather(*tasks)
            assert all(results[i] == i + 100 for i in range(50))
    
    async def test_burst_then_idle_py(self, py_server: ServerProcess):
        """Burst of calls, then idle, then more calls."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            # Burst 1
            tasks = [client.call("square", [i]) for i in range(50)]
            results = await asyncio.gather(*tasks)
            assert all(results[i] == i * i for i in range(50))
            
            # Idle
            await asyncio.sleep(0.5)
            
            # Burst 2
            tasks = [client.call("add", [i, 100]) for i in range(50)]
            results = await asyncio.gather(*tasks)
            assert all(results[i] == i + 100 for i in range(50))
    
    async def test_alternating_bursts_ts(self, ts_server: ServerProcess):
        """Alternating bursts and single calls."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            for round_num in range(5):
                # Burst
                tasks = [client.call("square", [i]) for i in range(20)]
                results = await asyncio.gather(*tasks)
                assert len(results) == 20
                
                # Single call
                result = await client.call("add", [round_num, 1])
                assert result == round_num + 1
    
    async def test_alternating_bursts_py(self, py_server: ServerProcess):
        """Alternating bursts and single calls."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            for round_num in range(5):
                # Burst
                tasks = [client.call("square", [i]) for i in range(20)]
                results = await asyncio.gather(*tasks)
                assert len(results) == 20
                
                # Single call
                result = await client.call("add", [round_num, 1])
                assert result == round_num + 1


# =============================================================================
# Mixed Workload Tests
# =============================================================================

@pytest.mark.asyncio
class TestMixedWorkload:
    """Test mixed workload scenarios."""
    
    async def test_mixed_call_types_ts(self, ts_server: ServerProcess):
        """Mix of different call types under load."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = []
            
            # Mix of different operations
            for i in range(50):
                if i % 4 == 0:
                    tasks.append(client.call("square", [i]))
                elif i % 4 == 1:
                    tasks.append(client.call("add", [i, i + 1]))
                elif i % 4 == 2:
                    tasks.append(client.call("greet", [f"User{i}"]))
                else:
                    tasks.append(client.call("echo", [{"id": i}]))
            
            results = await asyncio.gather(*tasks)
            assert len(results) == 50
    
    async def test_mixed_call_types_py(self, py_server: ServerProcess):
        """Mix of different call types under load."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = []
            
            for i in range(50):
                if i % 4 == 0:
                    tasks.append(client.call("square", [i]))
                elif i % 4 == 1:
                    tasks.append(client.call("add", [i, i + 1]))
                elif i % 4 == 2:
                    tasks.append(client.call("greet", [f"User{i}"]))
                else:
                    tasks.append(client.call("echo", [{"id": i}]))
            
            results = await asyncio.gather(*tasks)
            assert len(results) == 50
    
    async def test_errors_under_load_ts(self, ts_server: ServerProcess):
        """Some errors mixed with successful calls under load."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = []
            
            for i in range(30):
                if i % 5 == 0:
                    # This will fail
                    tasks.append(client.call("throwError", [f"error{i}"]))
                else:
                    tasks.append(client.call("square", [i]))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successes and failures
            successes = sum(1 for r in results if not isinstance(r, Exception))
            failures = sum(1 for r in results if isinstance(r, Exception))
            
            assert successes == 24  # 30 - 6 errors
            assert failures == 6
    
    async def test_errors_under_load_py(self, py_server: ServerProcess):
        """Some errors mixed with successful calls under load."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = []
            
            for i in range(30):
                if i % 5 == 0:
                    tasks.append(client.call("throwError", [f"error{i}"]))
                else:
                    tasks.append(client.call("square", [i]))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            successes = sum(1 for r in results if not isinstance(r, Exception))
            failures = sum(1 for r in results if isinstance(r, Exception))
            
            assert successes == 24
            assert failures == 6
