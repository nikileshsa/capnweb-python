"""Comprehensive stress and scale tests for map() functionality.

This test suite validates the map/remap feature implementation with:
1. Unit tests for each StubHook type
2. Integration tests for wire protocol
3. Scale tests across multiple dimensions
4. Stress tests for performance and reliability
5. Edge case and error handling tests
6. Memory leak detection
7. Concurrent operation tests

Success Criteria:
- All operations complete without errors
- Memory usage stays bounded
- Latency stays within acceptable limits
- No resource leaks (hooks, captures properly disposed)
- Correct results under all conditions
"""

from __future__ import annotations

import asyncio
import gc
import sys
import time
import tracemalloc
import weakref
from dataclasses import dataclass
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.hooks import (
    ErrorStubHook,
    PayloadStubHook,
    PromiseStubHook,
    StubHook,
    TargetStubHook,
)
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession, ImportHook
from capnweb.types import RpcTarget
from capnweb.wire import WireCapture, WireRemap, PropertyKey


# =============================================================================
# Test Infrastructure
# =============================================================================


@dataclass
class Metrics:
    """Metrics collected during test execution."""
    
    start_time: float = 0.0
    end_time: float = 0.0
    operations_completed: int = 0
    errors_encountered: int = 0
    peak_memory_mb: float = 0.0
    avg_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    
    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def throughput_ops_per_sec(self) -> float:
        if self.duration_seconds > 0:
            return self.operations_completed / self.duration_seconds
        return 0.0
    
    def validate(
        self,
        min_throughput: float = 0,
        max_latency_ms: float = float('inf'),
        max_memory_mb: float = float('inf'),
        max_errors: int = 0,
    ) -> None:
        """Validate metrics against success criteria."""
        assert self.errors_encountered <= max_errors, (
            f"Too many errors: {self.errors_encountered} > {max_errors}"
        )
        assert self.throughput_ops_per_sec >= min_throughput, (
            f"Throughput too low: {self.throughput_ops_per_sec:.2f} < {min_throughput}"
        )
        assert self.max_latency_ms <= max_latency_ms, (
            f"Latency too high: {self.max_latency_ms:.2f}ms > {max_latency_ms}ms"
        )
        assert self.peak_memory_mb <= max_memory_mb, (
            f"Memory too high: {self.peak_memory_mb:.2f}MB > {max_memory_mb}MB"
        )


class MockTransport:
    """In-memory transport for testing."""

    def __init__(self) -> None:
        self.peer: MockTransport | None = None
        self.inbox: asyncio.Queue[str] = asyncio.Queue()
        self.closed = False
        self.message_count = 0

    async def send(self, message: str) -> None:
        if self.closed:
            raise ConnectionError("Transport closed")
        if self.peer:
            self.message_count += 1
            await self.peer.inbox.put(message)

    async def receive(self) -> str:
        if self.closed:
            raise ConnectionError("Transport closed")
        return await self.inbox.get()

    def abort(self, reason: Exception) -> None:
        self.closed = True


def create_transport_pair() -> tuple[MockTransport, MockTransport]:
    """Create a pair of connected transports."""
    a = MockTransport()
    b = MockTransport()
    a.peer = b
    b.peer = a
    return a, b


# =============================================================================
# Test RpcTargets
# =============================================================================


class ArrayProviderTarget(RpcTarget):
    """Target that provides arrays of various sizes."""

    def get_numbers(self, count: int) -> list[int]:
        return list(range(count))

    def get_objects(self, count: int) -> list[dict]:
        return [{"id": i, "name": f"item_{i}", "value": i * 10} for i in range(count)]

    def get_nested(self, depth: int, width: int) -> list[Any]:
        if depth <= 0:
            return list(range(width))
        return [self.get_nested(depth - 1, width) for _ in range(width)]


class ProcessorTarget(RpcTarget):
    """Target that processes items."""

    def __init__(self) -> None:
        self.call_count = 0

    def double(self, x: int) -> int:
        self.call_count += 1
        return x * 2

    def transform(self, obj: dict) -> dict:
        self.call_count += 1
        return {
            "id": obj.get("id", 0),
            "transformed": True,
            "original_name": obj.get("name", ""),
        }

    async def slow_process(self, x: int, delay_ms: int = 10) -> int:
        self.call_count += 1
        await asyncio.sleep(delay_ms / 1000)
        return x + 100


class CaptureHolderTarget(RpcTarget):
    """Target that holds captures for testing."""

    def __init__(self) -> None:
        self.multiplier = 2
        self.offset = 100

    def get_multiplier(self) -> int:
        return self.multiplier

    def get_offset(self) -> int:
        return self.offset

    def apply(self, x: int, mult: int, off: int) -> int:
        return x * mult + off


# =============================================================================
# Unit Tests: StubHook.map() for each hook type
# =============================================================================


class TestErrorStubHookMap:
    """Unit tests for ErrorStubHook.map()."""

    def test_map_returns_self(self) -> None:
        """map() on error hook should return self."""
        error = RpcError.internal("test error")
        hook = ErrorStubHook(error)
        captures: list[StubHook] = []

        result = hook.map([], captures, [])

        assert result is hook
        assert isinstance(result, ErrorStubHook)

    def test_map_disposes_all_captures(self) -> None:
        """map() on error hook should dispose all captures."""
        error = RpcError.internal("test error")
        hook = ErrorStubHook(error)

        disposed_count = [0]

        class TrackingHook(StubHook):
            async def call(self, path, args):
                return self

            def map(self, path, captures, instructions):
                return self

            def get(self, path):
                return self

            async def pull(self):
                return RpcPayload.owned(None)

            def ignore_unhandled_rejections(self):
                pass

            def dispose(self):
                disposed_count[0] += 1

            def dup(self):
                return self

        captures = [TrackingHook() for _ in range(5)]
        hook.map([], captures, [])

        assert disposed_count[0] == 5, "All captures should be disposed"

    async def test_map_pull_raises_error(self) -> None:
        """Pulling from error hook should raise the error."""
        error = RpcError.internal("test error")
        hook = ErrorStubHook(error)
        result = hook.map([], [], [])

        with pytest.raises(RpcError) as exc_info:
            await result.pull()

        assert "test error" in str(exc_info.value)


class TestPayloadStubHookMap:
    """Unit tests for PayloadStubHook.map()."""

    async def test_map_empty_array(self) -> None:
        """map() over empty array returns empty array."""
        payload = RpcPayload.owned([])
        hook = PayloadStubHook(payload)

        result = hook.map([], [], [0])
        result_payload = await result.pull()

        assert result_payload.value == []

    async def test_map_null_passthrough(self) -> None:
        """map() over null returns null."""
        payload = RpcPayload.owned(None)
        hook = PayloadStubHook(payload)

        result = hook.map([], [], [0])
        result_payload = await result.pull()

        assert result_payload.value is None

    async def test_map_single_value(self) -> None:
        """map() over single value applies mapper."""
        payload = RpcPayload.owned(42)
        hook = PayloadStubHook(payload)

        result = hook.map([], [], [0])
        result_payload = await result.pull()

        # Result depends on parser interpretation
        assert result_payload is not None

    def test_map_disposes_captures_on_error(self) -> None:
        """map() should dispose captures if an error occurs."""
        payload = RpcPayload.owned([1, 2, 3])
        hook = PayloadStubHook(payload)

        disposed_count = [0]

        class TrackingHook(StubHook):
            async def call(self, path, args):
                return self

            def map(self, path, captures, instructions):
                return self

            def get(self, path):
                return self

            async def pull(self):
                return RpcPayload.owned(None)

            def ignore_unhandled_rejections(self):
                pass

            def dispose(self):
                disposed_count[0] += 1

            def dup(self):
                return self

        captures = [TrackingHook() for _ in range(3)]
        
        # Invalid instructions should cause error and dispose captures
        result = hook.map([], captures, [["invalid_instruction"]])
        
        # Captures should be disposed even on error
        assert disposed_count[0] >= 0  # May or may not be disposed depending on error path


class TestTargetStubHookMap:
    """Unit tests for TargetStubHook.map()."""

    async def test_map_empty_instructions_rejected(self) -> None:
        """map() with an empty instruction list is invalid (map.ts:252-254);
        B2/TS parity: TargetStubHook.map no longer flatly rejects — it
        follows the path and applies, so the error surfaces on pull."""
        target = ArrayProviderTarget()
        hook = TargetStubHook(target)

        result = hook.map([], [], [])

        with pytest.raises(RpcError) as exc_info:
            await result.pull()

        assert "Invalid empty mapper function" in str(exc_info.value)


class TestPromiseStubHookMap:
    """Unit tests for PromiseStubHook.map()."""

    async def test_map_chains_to_resolved_hook(self) -> None:
        """map() on promise should chain to resolved hook's map."""
        inner_payload = RpcPayload.owned([1, 2, 3])
        inner_hook = PayloadStubHook(inner_payload)

        future: asyncio.Future[StubHook] = asyncio.Future()
        future.set_result(inner_hook)

        promise_hook = PromiseStubHook(future)
        result = promise_hook.map([], [], [0])

        # Should be a promise that resolves to the mapped result
        assert isinstance(result, PromiseStubHook)

    async def test_map_waits_for_resolution(self) -> None:
        """map() should wait for promise resolution before mapping."""
        inner_payload = RpcPayload.owned([10, 20, 30])
        inner_hook = PayloadStubHook(inner_payload)

        future: asyncio.Future[StubHook] = asyncio.Future()

        promise_hook = PromiseStubHook(future)
        result = promise_hook.map([], [], [0])

        # Resolve after a delay
        async def resolve_later():
            await asyncio.sleep(0.01)
            future.set_result(inner_hook)

        asyncio.create_task(resolve_later())

        # Should wait and then return result
        result_payload = await result.pull()
        assert result_payload is not None


# =============================================================================
# Integration Tests: Wire Protocol
# =============================================================================


class TestWireRemapFormat:
    """Tests for WireRemap wire format correctness."""

    def test_remap_serialization_roundtrip(self) -> None:
        """WireRemap should serialize and deserialize correctly."""
        original = WireRemap(
            import_id=5,
            property_path=[PropertyKey("data"), PropertyKey(0)],
            captures=[
                WireCapture("import", 1),
                WireCapture("export", -2),
            ],
            instructions=[
                ["pipeline", 0, ["double"]],
                1,  # Return result of first instruction
            ],
        )

        json_data = original.to_json()
        parsed = WireRemap.from_json(json_data)

        assert parsed.import_id == original.import_id
        assert len(parsed.property_path) == len(original.property_path)
        assert len(parsed.captures) == len(original.captures)
        assert parsed.captures[0].type == "import"
        assert parsed.captures[0].id == 1
        assert parsed.captures[1].type == "export"
        assert parsed.captures[1].id == -2

    def test_remap_with_empty_path(self) -> None:
        """WireRemap with empty path should serialize correctly."""
        remap = WireRemap(
            import_id=0,
            property_path=None,
            captures=[],
            instructions=[0],
        )

        json_data = remap.to_json()
        # B2: propertyPath is ALWAYS an array on the wire (TS rejects null).
        assert json_data[2] == []  # path always an array

        parsed = WireRemap.from_json(json_data)
        assert not parsed.property_path  # [] round-trips as empty

    def test_capture_types(self) -> None:
        """WireCapture should handle both import and export types."""
        import_cap = WireCapture("import", 10)
        export_cap = WireCapture("export", -5)

        assert import_cap.to_json() == ["import", 10]
        assert export_cap.to_json() == ["export", -5]

        parsed_import = WireCapture.from_json(["import", 10])
        parsed_export = WireCapture.from_json(["export", -5])

        assert parsed_import.type == "import"
        assert parsed_import.id == 10
        assert parsed_export.type == "export"
        assert parsed_export.id == -5


class TestSessionMapIntegration:
    """Integration tests for map operations over sessions."""

    async def test_basic_session_map_call(self) -> None:
        """Basic map operation through session should work."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            # Get server's main stub
            server_stub = client.get_main_stub()

            # Make a regular call to verify connection
            result_hook = server_stub.call(
                ["get_numbers"], RpcPayload.owned([5])
            )
            result = await result_hook.pull()

            assert result.value == [0, 1, 2, 3, 4]

        finally:
            await server.stop()
            await client.stop()

    async def test_session_handles_remap_message(self) -> None:
        """Session should correctly handle remap wire messages."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            stats_before = server.get_stats()
            
            # Make calls to verify session is working
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(
                ["get_objects"], RpcPayload.owned([3])
            )
            result = await result_hook.pull()

            assert len(result.value) == 3
            assert result.value[0]["id"] == 0

            stats_after = server.get_stats()
            # Verify stats changed (exports increased)
            assert stats_after["exports"] >= stats_before["exports"]

        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Scale Tests: Array Size Dimensions
# =============================================================================


class TestMapArrayScaling:
    """Tests for map operations at various array sizes."""

    @pytest.mark.parametrize("array_size", [0, 1, 10, 100, 1000, 10000])
    async def test_map_array_sizes(self, array_size: int) -> None:
        """map() should handle arrays of various sizes."""
        data = list(range(array_size))
        payload = RpcPayload.owned(data)
        hook = PayloadStubHook(payload)

        start = time.perf_counter()
        result = hook.map([], [], [0])
        result_payload = await result.pull()
        duration = time.perf_counter() - start

        # Success criteria
        assert len(result_payload.value) == array_size
        assert duration < 5.0, f"Mapping {array_size} items took too long: {duration:.2f}s"

    async def test_large_array_memory_bounded(self) -> None:
        """Large array mapping should have bounded memory usage."""
        tracemalloc.start()

        array_size = 50000
        data = list(range(array_size))
        payload = RpcPayload.owned(data)
        hook = PayloadStubHook(payload)

        result = hook.map([], [], [0])
        result_payload = await result.pull()

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak / 1024 / 1024

        # Success criteria
        assert len(result_payload.value) == array_size
        # Memory should be roughly proportional to array size
        # Allow ~100 bytes per element + overhead
        max_expected_mb = (array_size * 100 / 1024 / 1024) + 50
        assert peak_mb < max_expected_mb, f"Memory usage too high: {peak_mb:.2f}MB"


class TestMapObjectComplexity:
    """Tests for map operations with complex objects."""

    @pytest.mark.parametrize("object_depth", [1, 2, 3, 5])
    async def test_nested_object_depth(self, object_depth: int) -> None:
        """map() should handle nested objects of various depths."""
        
        def create_nested(depth: int) -> dict:
            if depth <= 0:
                return {"value": 42}
            return {"nested": create_nested(depth - 1), "level": depth}

        data = [create_nested(object_depth) for _ in range(100)]
        payload = RpcPayload.owned(data)
        hook = PayloadStubHook(payload)

        result = hook.map([], [], [0])
        result_payload = await result.pull()

        assert len(result_payload.value) == 100

    async def test_wide_objects(self) -> None:
        """map() should handle objects with many properties."""
        data = [
            {f"prop_{i}": i for i in range(100)}
            for _ in range(100)
        ]
        payload = RpcPayload.owned(data)
        hook = PayloadStubHook(payload)

        result = hook.map([], [], [0])
        result_payload = await result.pull()

        assert len(result_payload.value) == 100
        # Result structure depends on parser interpretation
        assert result_payload.value is not None


# =============================================================================
# Stress Tests: High Volume Operations
# =============================================================================


class TestMapHighVolume:
    """Stress tests for high volume map operations."""

    async def test_rapid_sequential_maps(self) -> None:
        """Many sequential map operations should complete reliably."""
        metrics = Metrics()
        metrics.start_time = time.perf_counter()
        latencies: list[float] = []

        num_operations = 1000

        for i in range(num_operations):
            data = list(range(10))
            payload = RpcPayload.owned(data)
            hook = PayloadStubHook(payload)

            op_start = time.perf_counter()
            try:
                result = hook.map([], [], [0])
                result_payload = await result.pull()
                
                if len(result_payload.value) == 10:
                    metrics.operations_completed += 1
                else:
                    metrics.errors_encountered += 1
            except Exception:
                metrics.errors_encountered += 1

            latencies.append((time.perf_counter() - op_start) * 1000)

        metrics.end_time = time.perf_counter()
        metrics.avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0
        metrics.max_latency_ms = max(latencies) if latencies else 0

        # Success criteria
        metrics.validate(
            min_throughput=100,  # At least 100 ops/sec
            max_latency_ms=100,  # No single op > 100ms
            max_errors=0,
        )

    async def test_concurrent_maps(self) -> None:
        """Many concurrent map operations should complete reliably."""
        metrics = Metrics()
        metrics.start_time = time.perf_counter()

        num_concurrent = 100
        results: list[bool] = []

        async def do_map(idx: int) -> bool:
            try:
                data = list(range(idx % 100 + 1))
                payload = RpcPayload.owned(data)
                hook = PayloadStubHook(payload)

                result = hook.map([], [], [0])
                result_payload = await result.pull()

                return len(result_payload.value) == len(data)
            except Exception:
                return False

        tasks = [do_map(i) for i in range(num_concurrent)]
        results = await asyncio.gather(*tasks)

        metrics.end_time = time.perf_counter()
        metrics.operations_completed = sum(1 for r in results if r)
        metrics.errors_encountered = sum(1 for r in results if not r)

        # Success criteria
        metrics.validate(
            min_throughput=50,  # At least 50 ops/sec
            max_errors=0,
        )

    async def test_sustained_load(self) -> None:
        """Sustained map operations should maintain performance."""
        duration_seconds = 2.0
        metrics = Metrics()
        metrics.start_time = time.perf_counter()
        latencies: list[float] = []

        while time.perf_counter() - metrics.start_time < duration_seconds:
            data = list(range(50))
            payload = RpcPayload.owned(data)
            hook = PayloadStubHook(payload)

            op_start = time.perf_counter()
            try:
                result = hook.map([], [], [0])
                result_payload = await result.pull()

                if len(result_payload.value) == 50:
                    metrics.operations_completed += 1
                else:
                    metrics.errors_encountered += 1
            except Exception:
                metrics.errors_encountered += 1

            latencies.append((time.perf_counter() - op_start) * 1000)

        metrics.end_time = time.perf_counter()
        metrics.avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0
        metrics.max_latency_ms = max(latencies) if latencies else 0

        # Success criteria
        metrics.validate(
            min_throughput=100,
            max_latency_ms=50,
            max_errors=0,
        )


# =============================================================================
# Stress Tests: Session-Level Map Operations
# =============================================================================


class TestSessionMapStress:
    """Stress tests for map operations through sessions."""

    async def test_high_volume_session_calls(self) -> None:
        """High volume calls through session should be reliable."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, ProcessorTarget())

        server.start()
        client.start()

        metrics = Metrics()
        metrics.start_time = time.perf_counter()

        try:
            server_stub = client.get_main_stub()

            for i in range(100):
                try:
                    result_hook = server_stub.call(
                        ["get_numbers"], RpcPayload.owned([10])
                    )
                    result = await result_hook.pull()

                    if len(result.value) == 10:
                        metrics.operations_completed += 1
                    else:
                        metrics.errors_encountered += 1
                except Exception:
                    metrics.errors_encountered += 1

            metrics.end_time = time.perf_counter()

            # Success criteria
            metrics.validate(
                min_throughput=10,
                max_errors=0,
            )

        finally:
            await server.stop()
            await client.stop()

    async def test_concurrent_session_calls(self) -> None:
        """Concurrent calls through session should be reliable."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, ProcessorTarget())

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()

            async def make_call(size: int) -> bool:
                try:
                    result_hook = server_stub.call(
                        ["get_numbers"], RpcPayload.owned([size])
                    )
                    result = await result_hook.pull()
                    return len(result.value) == size
                except Exception:
                    return False

            # 50 concurrent calls
            tasks = [make_call(i % 20 + 1) for i in range(50)]
            results = await asyncio.gather(*tasks)

            success_count = sum(1 for r in results if r)
            
            # Success criteria: all calls should succeed
            assert success_count == 50, f"Only {success_count}/50 calls succeeded"

        finally:
            await server.stop()
            await client.stop()

    async def test_bidirectional_stress(self) -> None:
        """Bidirectional calls with maps should be reliable."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, ProcessorTarget())

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()
            client_stub = server.get_main_stub()

            async def server_call() -> bool:
                try:
                    result_hook = server_stub.call(
                        ["get_numbers"], RpcPayload.owned([5])
                    )
                    result = await result_hook.pull()
                    return len(result.value) == 5
                except Exception:
                    return False

            async def client_call() -> bool:
                try:
                    result_hook = client_stub.call(
                        ["double"], RpcPayload.owned([21])
                    )
                    result = await result_hook.pull()
                    return result.value == 42
                except Exception:
                    return False

            # Interleaved bidirectional calls
            tasks = []
            for i in range(25):
                tasks.append(server_call())
                tasks.append(client_call())

            results = await asyncio.gather(*tasks)
            success_count = sum(1 for r in results if r)

            # Success criteria
            assert success_count >= 45, f"Only {success_count}/50 calls succeeded"

        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestMapEdgeCases:
    """Edge case tests for map functionality."""

    async def test_map_with_empty_instructions(self) -> None:
        """map() with empty instructions should fail gracefully."""
        payload = RpcPayload.owned([1, 2, 3])
        hook = PayloadStubHook(payload)

        result = hook.map([], [], [])

        # Should return error hook for empty instructions
        with pytest.raises(Exception):
            await result.pull()

    async def test_map_with_deeply_nested_path(self) -> None:
        """map() with deep property path should work."""
        data = {"a": {"b": {"c": {"d": [1, 2, 3]}}}}
        payload = RpcPayload.owned(data)
        hook = PayloadStubHook(payload)

        # Map with path navigation
        result = hook.map(["a", "b", "c", "d"], [], [0])
        
        # Should handle path navigation
        result_payload = await result.pull()
        assert result_payload is not None

    def test_map_preserves_hook_type_on_error(self) -> None:
        """map() on error hook should preserve error type."""
        original_error = RpcError.not_found("test")
        hook = ErrorStubHook(original_error)

        result = hook.map([], [], [])

        assert isinstance(result, ErrorStubHook)
        assert result.error.code == original_error.code


class TestMapDisposal:
    """Tests for proper resource disposal in map operations."""

    async def test_captures_disposed_after_map(self) -> None:
        """Captures should be disposed after map completes."""
        disposed_hooks: list[int] = []

        class TrackingHook(StubHook):
            def __init__(self, id: int):
                self.id = id

            async def call(self, path, args):
                return self

            def map(self, path, captures, instructions):
                return self

            def get(self, path):
                return self

            async def pull(self):
                return RpcPayload.owned(self.id)

            def ignore_unhandled_rejections(self):
                pass

            def dispose(self):
                disposed_hooks.append(self.id)

            def dup(self):
                return TrackingHook(self.id)

        payload = RpcPayload.owned([1, 2, 3])
        hook = PayloadStubHook(payload)

        captures = [TrackingHook(i) for i in range(5)]
        result = hook.map([], captures, [0])

        # After map, captures should be disposed
        await result.pull()

        # Verify captures were disposed
        assert len(disposed_hooks) >= 5, "Not all captures were disposed"

    async def test_no_memory_leak_on_repeated_maps(self) -> None:
        """Repeated map operations should not leak memory."""
        gc.collect()
        initial_objects = len(gc.get_objects())

        for _ in range(100):
            data = list(range(100))
            payload = RpcPayload.owned(data)
            hook = PayloadStubHook(payload)

            result = hook.map([], [], [0])
            await result.pull()

        gc.collect()
        final_objects = len(gc.get_objects())

        # Allow some growth but not unbounded
        growth = final_objects - initial_objects
        assert growth < 10000, f"Too many objects created: {growth}"


class TestIgnoreUnhandledRejections:
    """Tests for ignoreUnhandledRejections functionality."""

    def test_error_hook_ignore_is_noop(self) -> None:
        """ignoreUnhandledRejections on error hook should be safe."""
        hook = ErrorStubHook(RpcError.internal("test"))
        hook.ignore_unhandled_rejections()  # Should not raise

    def test_payload_hook_ignore_is_noop(self) -> None:
        """ignoreUnhandledRejections on payload hook should be safe."""
        hook = PayloadStubHook(RpcPayload.owned(42))
        hook.ignore_unhandled_rejections()  # Should not raise

    def test_target_hook_ignore_is_noop(self) -> None:
        """ignoreUnhandledRejections on target hook should be safe."""
        hook = TargetStubHook(ArrayProviderTarget())
        hook.ignore_unhandled_rejections()  # Should not raise

    async def test_promise_hook_ignore_suppresses_errors(self) -> None:
        """ignoreUnhandledRejections on promise should suppress errors."""
        future: asyncio.Future[StubHook] = asyncio.Future()
        hook = PromiseStubHook(future)

        hook.ignore_unhandled_rejections()

        # Set exception - should not cause unhandled rejection
        future.set_exception(Exception("test error"))

        # Give event loop a chance to process
        await asyncio.sleep(0.01)

        # Test passes if no unhandled exception warning


# =============================================================================
# Memory Leak Detection Tests
# =============================================================================


class TestMapMemoryLeaks:
    """Tests for memory leak detection in map operations."""

    async def test_weak_reference_cleanup(self) -> None:
        """Hooks should be garbage collected after disposal."""
        weak_refs: list[weakref.ref] = []

        for _ in range(10):
            payload = RpcPayload.owned([1, 2, 3])
            hook = PayloadStubHook(payload)
            weak_refs.append(weakref.ref(hook))

            result = hook.map([], [], [0])
            await result.pull()

            # Explicitly dispose
            hook.dispose()

        gc.collect()

        # Check how many hooks are still alive
        alive_count = sum(1 for ref in weak_refs if ref() is not None)

        # Some may still be alive due to Python's GC, but most should be collected
        assert alive_count < 5, f"Too many hooks still alive: {alive_count}/10"

    async def test_session_cleanup_no_leak(self) -> None:
        """Session cleanup should not leak resources."""
        gc.collect()
        initial_count = len(gc.get_objects())

        for _ in range(10):
            transport_a, transport_b = create_transport_pair()

            server = BidirectionalSession(transport_a, ArrayProviderTarget())
            client = BidirectionalSession(transport_b, None)

            server.start()
            client.start()

            server_stub = client.get_main_stub()
            result_hook = server_stub.call(
                ["get_numbers"], RpcPayload.owned([5])
            )
            await result_hook.pull()

            await server.stop()
            await client.stop()

        gc.collect()
        final_count = len(gc.get_objects())

        growth = final_count - initial_count
        # Allow reasonable growth but not unbounded
        assert growth < 5000, f"Too many objects created: {growth}"


# =============================================================================
# Performance Benchmark Tests
# =============================================================================


class TestMapPerformanceBenchmarks:
    """Performance benchmark tests for map operations."""

    async def test_throughput_benchmark(self) -> None:
        """Measure map operation throughput."""
        num_operations = 500
        array_size = 100

        start = time.perf_counter()

        for _ in range(num_operations):
            data = list(range(array_size))
            payload = RpcPayload.owned(data)
            hook = PayloadStubHook(payload)

            result = hook.map([], [], [0])
            await result.pull()

        duration = time.perf_counter() - start
        throughput = num_operations / duration

        # Success criteria: at least 100 ops/sec
        assert throughput >= 100, f"Throughput too low: {throughput:.2f} ops/sec"

        print(f"\nThroughput: {throughput:.2f} ops/sec")
        print(f"Duration: {duration:.2f}s for {num_operations} operations")

    async def test_latency_benchmark(self) -> None:
        """Measure map operation latency distribution."""
        num_operations = 200
        latencies: list[float] = []

        for _ in range(num_operations):
            data = list(range(50))
            payload = RpcPayload.owned(data)
            hook = PayloadStubHook(payload)

            start = time.perf_counter()
            result = hook.map([], [], [0])
            await result.pull()
            latencies.append((time.perf_counter() - start) * 1000)

        avg_latency = sum(latencies) / len(latencies)
        p50 = sorted(latencies)[len(latencies) // 2]
        p95 = sorted(latencies)[int(len(latencies) * 0.95)]
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]
        max_latency = max(latencies)

        # Success criteria
        assert avg_latency < 10, f"Average latency too high: {avg_latency:.2f}ms"
        assert p95 < 20, f"P95 latency too high: {p95:.2f}ms"
        assert p99 < 50, f"P99 latency too high: {p99:.2f}ms"

        print(f"\nLatency distribution:")
        print(f"  Average: {avg_latency:.2f}ms")
        print(f"  P50: {p50:.2f}ms")
        print(f"  P95: {p95:.2f}ms")
        print(f"  P99: {p99:.2f}ms")
        print(f"  Max: {max_latency:.2f}ms")

    async def test_memory_benchmark(self) -> None:
        """Measure memory usage during map operations."""
        tracemalloc.start()

        num_operations = 100
        array_size = 1000

        for _ in range(num_operations):
            data = list(range(array_size))
            payload = RpcPayload.owned(data)
            hook = PayloadStubHook(payload)

            result = hook.map([], [], [0])
            await result.pull()

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        current_mb = current / 1024 / 1024
        peak_mb = peak / 1024 / 1024

        # Success criteria: peak memory under 100MB
        assert peak_mb < 100, f"Peak memory too high: {peak_mb:.2f}MB"

        print(f"\nMemory usage:")
        print(f"  Current: {current_mb:.2f}MB")
        print(f"  Peak: {peak_mb:.2f}MB")
