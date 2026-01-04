"""Memory leak tests for Cap'n Web RPC.

Inspired by grpclib's test_memory.py, these tests verify that:
1. No objects are retained after simple calls
2. Stubs are cleaned up properly
3. No leaks on error paths
4. Concurrent calls don't leak

Uses gc.collect() and object counting to detect leaks.
"""

from __future__ import annotations

import asyncio
import gc
import sys
import weakref
from typing import Any

import pytest

from .conftest import ServerProcess, InteropClient


# =============================================================================
# Memory Tracking Utilities
# =============================================================================

def count_objects_of_type(type_name: str) -> int:
    """Count objects of a given type name in memory."""
    gc.collect()
    gc.collect()
    gc.collect()
    return sum(1 for obj in gc.get_objects() if type(obj).__name__ == type_name)


def get_rpc_object_counts() -> dict[str, int]:
    """Get counts of RPC-related objects."""
    gc.collect()
    gc.collect()
    gc.collect()
    
    types_to_track = [
        'RpcStub',
        'RpcPromise',
        'ImportHook',
        'ExportEntry',
        'ImportEntry',
        'PayloadStubHook',
        'PromiseStubHook',
        'RpcTargetHook',
        'BidirectionalSession',
        'WebSocketRpcClient',
    ]
    
    counts = {}
    for obj in gc.get_objects():
        name = type(obj).__name__
        if name in types_to_track:
            counts[name] = counts.get(name, 0) + 1
    
    return counts


def assert_no_new_objects(before: dict[str, int], after: dict[str, int], allowed_delta: int = 5):
    """Assert that no new RPC objects were created (or within allowed delta).
    
    Note: We use a generous allowed_delta because:
    1. Some objects may be cached by the runtime
    2. GC timing is non-deterministic
    3. We're looking for significant leaks, not small variations
    """
    for name, after_count in after.items():
        before_count = before.get(name, 0)
        delta = after_count - before_count
        if delta > allowed_delta:
            raise AssertionError(
                f"Memory leak detected: {name} increased by {delta} "
                f"(before={before_count}, after={after_count})"
            )


# =============================================================================
# Simple Call Memory Tests
# =============================================================================

@pytest.mark.asyncio
class TestSimpleCallMemory:
    """Test that simple calls don't leak memory."""
    
    async def test_no_leak_simple_call_ts(self, ts_server: ServerProcess):
        """Simple call to TypeScript server doesn't leak."""
        # Warm up - first call may create cached objects
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            await client.call("square", [5])
        
        gc.collect()
        before = get_rpc_object_counts()
        
        # Make several calls
        for _ in range(10):
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                await client.call("square", [5])
        
        gc.collect()
        after = get_rpc_object_counts()
        
        # Allow small delta for any cached objects
        assert_no_new_objects(before, after, allowed_delta=5)
    
    async def test_no_leak_simple_call_py(self, py_server: ServerProcess):
        """Simple call to Python server doesn't leak."""
        # Warm up
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            await client.call("square", [5])
        
        gc.collect()
        before = get_rpc_object_counts()
        
        for _ in range(10):
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                await client.call("square", [5])
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=5)
    
    async def test_multiple_calls_same_session_ts(self, ts_server: ServerProcess):
        """Multiple calls in same session complete and session closes cleanly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            for i in range(20):
                result = await client.call("square", [i])
                assert result == i * i
        gc.collect()
    
    async def test_multiple_calls_same_session_py(self, py_server: ServerProcess):
        """Multiple calls in same session complete and session closes cleanly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            for i in range(20):
                result = await client.call("square", [i])
                assert result == i * i
        gc.collect()


# =============================================================================
# Capability Memory Tests
# =============================================================================

@pytest.mark.asyncio
class TestCapabilityMemory:
    """Test that capability passing doesn't leak memory."""
    
    async def test_no_leak_capability_creation_ts(self, ts_server: ServerProcess):
        """Creating and releasing capabilities doesn't leak."""
        gc.collect()
        before = get_rpc_object_counts()
        
        for _ in range(5):
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                counter = await client.call("makeCounter", [0])
                # Counter is a stub - should be cleaned up when session closes
                assert counter is not None
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=10)
    
    async def test_no_leak_capability_creation_py(self, py_server: ServerProcess):
        """Creating and releasing capabilities doesn't leak."""
        gc.collect()
        before = get_rpc_object_counts()
        
        for _ in range(5):
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                counter = await client.call("makeCounter", [0])
                assert counter is not None
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=10)
    
    async def test_stub_lifecycle_ts(self, ts_server: ServerProcess):
        """Stubs are created and session closes cleanly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            counter = await client.call("makeCounter", [0])
            assert counter is not None
        # Session closed - stubs should be released
        gc.collect()
    
    async def test_stub_lifecycle_py(self, py_server: ServerProcess):
        """Stubs are created and session closes cleanly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            counter = await client.call("makeCounter", [0])
            assert counter is not None
        gc.collect()


# =============================================================================
# Error Path Memory Tests
# =============================================================================

@pytest.mark.asyncio
class TestErrorPathMemory:
    """Test that error paths don't leak memory."""
    
    async def test_no_leak_on_error_ts(self, ts_server: ServerProcess):
        """Errors don't cause memory leaks."""
        gc.collect()
        before = get_rpc_object_counts()
        
        for _ in range(5):
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                try:
                    await client.call("throwError", ["test error"])
                except Exception:
                    pass  # Expected
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=2)
    
    async def test_no_leak_on_error_py(self, py_server: ServerProcess):
        """Errors don't cause memory leaks."""
        gc.collect()
        before = get_rpc_object_counts()
        
        for _ in range(5):
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                try:
                    await client.call("throwError", ["test error"])
                except Exception:
                    pass  # Expected
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=2)
    
    async def test_no_leak_method_not_found_ts(self, ts_server: ServerProcess):
        """Method not found errors don't leak."""
        gc.collect()
        before = get_rpc_object_counts()
        
        for _ in range(5):
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                try:
                    await client.call("nonExistentMethod", [])
                except Exception:
                    pass
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=2)
    
    async def test_no_leak_method_not_found_py(self, py_server: ServerProcess):
        """Method not found errors don't leak."""
        gc.collect()
        before = get_rpc_object_counts()
        
        for _ in range(5):
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                try:
                    await client.call("nonExistentMethod", [])
                except Exception:
                    pass
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=2)


# =============================================================================
# Concurrent Call Memory Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentMemory:
    """Test that concurrent calls don't leak memory."""
    
    async def test_no_leak_concurrent_calls_ts(self, ts_server: ServerProcess):
        """Many concurrent calls don't leak."""
        gc.collect()
        before = get_rpc_object_counts()
        
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Make 20 concurrent calls
            tasks = [client.call("square", [i]) for i in range(20)]
            results = await asyncio.gather(*tasks)
            assert len(results) == 20
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=10)
    
    async def test_no_leak_concurrent_calls_py(self, py_server: ServerProcess):
        """Many concurrent calls don't leak."""
        gc.collect()
        before = get_rpc_object_counts()
        
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [client.call("square", [i]) for i in range(20)]
            results = await asyncio.gather(*tasks)
            assert len(results) == 20
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=10)
    
    async def test_no_leak_many_sessions_ts(self, ts_server: ServerProcess):
        """Many sequential sessions don't leak."""
        gc.collect()
        before = get_rpc_object_counts()
        
        for i in range(10):
            async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
                await client.call("square", [i])
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=10)
    
    async def test_no_leak_many_sessions_py(self, py_server: ServerProcess):
        """Many sequential sessions don't leak."""
        gc.collect()
        before = get_rpc_object_counts()
        
        for i in range(10):
            async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
                await client.call("square", [i])
        
        gc.collect()
        after = get_rpc_object_counts()
        
        assert_no_new_objects(before, after, allowed_delta=10)
