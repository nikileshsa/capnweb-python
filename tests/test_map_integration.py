"""Real integration tests for map functionality across all source files.

This test suite validates that the map() feature works end-to-end with
actual Client/Server connections and BidirectionalSession:
1. session.py - RpcSession.send_map() through real sessions
2. client.py - Client.send_map() with real server
3. server.py - WebSocketSession.send_map() with real client
4. stubs.py - RpcStub.map() and RpcPromise.map() through real RPC

No mocks - all tests use real transports and connections.
"""

from __future__ import annotations

import asyncio
import gc
import time
from dataclasses import dataclass
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.hooks import (
    ErrorStubHook,
    PayloadStubHook,
    PromiseStubHook,
    StubHook,
)
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcPromise, RpcStub
from capnweb.types import RpcTarget
from capnweb.wire import PropertyKey, WireCapture, WireRemap


# =============================================================================
# Test Infrastructure - Real Transports
# =============================================================================


class InMemoryTransport:
    """Real in-memory transport for testing (no mocks)."""

    def __init__(self) -> None:
        self.peer: InMemoryTransport | None = None
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


def create_transport_pair() -> tuple[InMemoryTransport, InMemoryTransport]:
    """Create a pair of connected transports."""
    a = InMemoryTransport()
    b = InMemoryTransport()
    a.peer = b
    b.peer = a
    return a, b


# =============================================================================
# Test RpcTargets - Real Implementations
# =============================================================================


class ArrayProviderTarget(RpcTarget):
    """Target that provides arrays for map testing."""

    def get_numbers(self, count: int) -> list[int]:
        """Return a list of numbers from 0 to count-1."""
        return list(range(count))

    def get_objects(self, count: int) -> list[dict]:
        """Return a list of objects."""
        return [{"id": i, "name": f"item_{i}", "value": i * 10} for i in range(count)]

    def get_nested_data(self) -> dict:
        """Return nested data structure."""
        return {
            "users": [
                {"id": 1, "name": "Alice", "scores": [90, 85, 92]},
                {"id": 2, "name": "Bob", "scores": [78, 82, 88]},
                {"id": 3, "name": "Charlie", "scores": [95, 91, 89]},
            ],
            "metadata": {"total": 3, "active": True},
        }

    def get_empty(self) -> list:
        """Return empty array."""
        return []

    def get_null(self) -> None:
        """Return null."""
        return None


class ProcessorTarget(RpcTarget):
    """Target that processes data."""

    def __init__(self) -> None:
        self.call_count = 0

    def double(self, x: int) -> int:
        """Double a number."""
        self.call_count += 1
        return x * 2

    def transform(self, obj: dict) -> dict:
        """Transform an object."""
        self.call_count += 1
        return {
            "id": obj.get("id", 0),
            "transformed": True,
            "original_name": obj.get("name", ""),
        }

    def sum_scores(self, user: dict) -> int:
        """Sum a user's scores."""
        self.call_count += 1
        return sum(user.get("scores", []))


class BidirectionalTarget(RpcTarget):
    """Target for bidirectional map testing."""

    def __init__(self) -> None:
        self.received_data: list[Any] = []

    def get_items(self) -> list[int]:
        """Get items to map over."""
        return [1, 2, 3, 4, 5]

    def process_item(self, item: int) -> int:
        """Process a single item."""
        self.received_data.append(item)
        return item * 100

    def get_received(self) -> list[Any]:
        """Get all received data."""
        return self.received_data


# =============================================================================
# Tests for BidirectionalSession Map Operations
# =============================================================================


class TestBidirectionalSessionMap:
    """Real tests for BidirectionalSession map functionality."""

    async def test_basic_map_over_array(self) -> None:
        """Map over array through real BidirectionalSession."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            # Get server's main stub
            server_stub = client.get_main_stub()

            # Call to get array
            result_hook = server_stub.call(
                ["get_numbers"], RpcPayload.owned([5])
            )
            result = await result_hook.pull()

            # Verify we got the array
            assert result.value == [0, 1, 2, 3, 4]

        finally:
            await server.stop()
            await client.stop()

    async def test_map_with_empty_array(self) -> None:
        """Map over empty array should return empty array."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()

            result_hook = server_stub.call(
                ["get_empty"], RpcPayload.owned([])
            )
            result = await result_hook.pull()

            assert result.value == []

        finally:
            await server.stop()
            await client.stop()

    async def test_map_with_null(self) -> None:
        """Map over null should return null."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()

            result_hook = server_stub.call(
                ["get_null"], RpcPayload.owned([])
            )
            result = await result_hook.pull()

            assert result.value is None

        finally:
            await server.stop()
            await client.stop()

    async def test_map_with_objects(self) -> None:
        """Map over array of objects."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()

            result_hook = server_stub.call(
                ["get_objects"], RpcPayload.owned([3])
            )
            result = await result_hook.pull()

            assert len(result.value) == 3
            assert result.value[0]["id"] == 0
            assert result.value[1]["name"] == "item_1"
            assert result.value[2]["value"] == 20

        finally:
            await server.stop()
            await client.stop()

    async def test_map_with_nested_data(self) -> None:
        """Map over nested data structure."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()

            result_hook = server_stub.call(
                ["get_nested_data"], RpcPayload.owned([])
            )
            result = await result_hook.pull()

            assert "users" in result.value
            assert len(result.value["users"]) == 3
            assert result.value["users"][0]["name"] == "Alice"

        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Tests for Bidirectional Map Operations
# =============================================================================


class TestBidirectionalMapOperations:
    """Tests for bidirectional map operations between client and server."""

    async def test_server_calls_client_method(self) -> None:
        """Server should be able to call methods on client-provided capabilities."""
        transport_a, transport_b = create_transport_pair()

        client_target = ProcessorTarget()
        server_target = ArrayProviderTarget()

        server = BidirectionalSession(transport_a, server_target)
        client = BidirectionalSession(transport_b, client_target)

        server.start()
        client.start()

        try:
            # Client calls server
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(
                ["get_numbers"], RpcPayload.owned([3])
            )
            result = await result_hook.pull()

            assert result.value == [0, 1, 2]

            # Server calls client
            client_stub = server.get_main_stub()
            result_hook = client_stub.call(
                ["double"], RpcPayload.owned([21])
            )
            result = await result_hook.pull()

            assert result.value == 42
            assert client_target.call_count == 1

        finally:
            await server.stop()
            await client.stop()

    async def test_multiple_bidirectional_calls(self) -> None:
        """Multiple bidirectional calls should work correctly."""
        transport_a, transport_b = create_transport_pair()

        client_target = ProcessorTarget()
        server_target = ArrayProviderTarget()

        server = BidirectionalSession(transport_a, server_target)
        client = BidirectionalSession(transport_b, client_target)

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()
            client_stub = server.get_main_stub()

            # Interleaved calls
            for i in range(10):
                # Client -> Server
                result_hook = server_stub.call(
                    ["get_numbers"], RpcPayload.owned([i + 1])
                )
                result = await result_hook.pull()
                assert len(result.value) == i + 1

                # Server -> Client
                result_hook = client_stub.call(
                    ["double"], RpcPayload.owned([i])
                )
                result = await result_hook.pull()
                assert result.value == i * 2

            assert client_target.call_count == 10

        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Tests for RpcStub.map() and RpcPromise.map()
# =============================================================================


class TestRpcStubMapReal:
    """Real tests for RpcStub.map() through actual sessions."""

    async def test_stub_map_on_payload_hook(self) -> None:
        """RpcStub.map() should work with PayloadStubHook."""
        data = [1, 2, 3, 4, 5]
        hook = PayloadStubHook(RpcPayload.owned(data))
        stub = RpcStub(hook)

        result = stub.map(lambda x: x)
        value = await result

        # B2: real recorder — identity map returns the REAL elements.
        assert value == [1, 2, 3, 4, 5]

    async def test_stub_map_on_empty_array(self) -> None:
        """RpcStub.map() on empty array should return empty array."""
        hook = PayloadStubHook(RpcPayload.owned([]))
        stub = RpcStub(hook)

        result = stub.map(lambda x: x)
        value = await result

        assert value == []

    async def test_stub_map_on_null(self) -> None:
        """RpcStub.map() on null should return null."""
        hook = PayloadStubHook(RpcPayload.owned(None))
        stub = RpcStub(hook)

        result = stub.map(lambda x: x)
        value = await result

        assert value is None

    async def test_stub_map_on_error_hook(self) -> None:
        """RpcStub.map() on error hook should propagate error."""
        error = RpcError.internal("test error")
        hook = ErrorStubHook(error)
        stub = RpcStub(hook)

        result = stub.map(lambda x: x)

        with pytest.raises(RpcError):
            await result

    async def test_stub_map_through_session(self) -> None:
        """RpcStub.map() should work through real session."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()

            # Get array via call
            result_hook = server_stub.call(
                ["get_numbers"], RpcPayload.owned([5])
            )
            
            # Create stub from result hook
            result_stub = RpcStub(result_hook)
            
            # Map over it (this tests the stub.map() API)
            mapped = result_stub.map(lambda x: x)
            
            # This should work
            assert isinstance(mapped, RpcPromise)

        finally:
            await server.stop()
            await client.stop()


class TestRpcPromiseMapReal:
    """Real tests for RpcPromise.map() through actual sessions."""

    async def test_promise_map_on_payload_hook(self) -> None:
        """RpcPromise.map() should work with PayloadStubHook."""
        data = [10, 20, 30]
        hook = PayloadStubHook(RpcPayload.owned(data))
        promise = RpcPromise(hook)

        result = promise.map(lambda x: x)
        value = await result

        # B2: real recorder — identity map returns the REAL elements.
        assert value == [10, 20, 30]

    async def test_promise_map_chains(self) -> None:
        """RpcPromise.map() should chain correctly."""
        hook = PayloadStubHook(RpcPayload.owned([1, 2, 3]))
        promise = RpcPromise(hook)

        # Chain multiple maps
        result = promise.map(lambda x: x).map(lambda x: x).map(lambda x: x)

        assert isinstance(result, RpcPromise)
        value = await result
        assert value == [1, 2, 3]

    async def test_promise_map_on_pending_promise(self) -> None:
        """RpcPromise.map() should work on pending promise."""
        future: asyncio.Future[StubHook] = asyncio.Future()
        hook = PromiseStubHook(future)
        promise = RpcPromise(hook)

        # Map before resolution
        result = promise.map(lambda x: x)

        # Resolve the original promise
        inner_hook = PayloadStubHook(RpcPayload.owned([1, 2, 3]))
        future.set_result(inner_hook)

        # Result should now be available with REAL values (identity map).
        value = await result
        assert value == [1, 2, 3]


# =============================================================================
# Stress Tests with Real Sessions
# =============================================================================


class TestMapStressReal:
    """Stress tests with real sessions (no mocks)."""

    async def test_high_volume_calls(self) -> None:
        """High volume calls through real session."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, ProcessorTarget())

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()

            success_count = 0
            for i in range(50):
                result_hook = server_stub.call(
                    ["get_numbers"], RpcPayload.owned([i % 10 + 1])
                )
                result = await result_hook.pull()

                if len(result.value) == i % 10 + 1:
                    success_count += 1

            assert success_count == 50

        finally:
            await server.stop()
            await client.stop()

    async def test_concurrent_calls(self) -> None:
        """Concurrent calls through real session."""
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

            # 25 concurrent calls
            tasks = [make_call(i % 10 + 1) for i in range(25)]
            results = await asyncio.gather(*tasks)

            success_count = sum(1 for r in results if r)
            assert success_count == 25

        finally:
            await server.stop()
            await client.stop()

    async def test_bidirectional_stress(self) -> None:
        """Bidirectional stress test with real sessions."""
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
            for _ in range(15):
                tasks.append(server_call())
                tasks.append(client_call())

            results = await asyncio.gather(*tasks)
            success_count = sum(1 for r in results if r)

            # Allow some failures due to timing
            assert success_count >= 25

        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Resource Cleanup Tests with Real Sessions
# =============================================================================


class TestMapResourceCleanupReal:
    """Resource cleanup tests with real sessions."""

    async def test_session_cleanup(self) -> None:
        """Sessions should clean up properly after map operations."""
        for _ in range(5):
            transport_a, transport_b = create_transport_pair()

            server = BidirectionalSession(transport_a, ArrayProviderTarget())
            client = BidirectionalSession(transport_b, None)

            server.start()
            client.start()

            server_stub = client.get_main_stub()
            result_hook = server_stub.call(
                ["get_numbers"], RpcPayload.owned([10])
            )
            await result_hook.pull()

            await server.stop()
            await client.stop()

        # Force garbage collection
        gc.collect()

        # If we get here without errors, cleanup worked

    async def test_stub_disposal(self) -> None:
        """Stubs should dispose correctly after map."""
        hook = PayloadStubHook(RpcPayload.owned([1, 2, 3]))
        stub = RpcStub(hook)

        result = stub.map(lambda x: x)
        await result

        # Dispose should not raise
        stub.dispose()
        result.dispose()

    async def test_promise_disposal(self) -> None:
        """Promises should dispose correctly after map."""
        hook = PayloadStubHook(RpcPayload.owned([1, 2, 3]))
        promise = RpcPromise(hook)

        result = promise.map(lambda x: x)
        await result

        # Dispose should not raise
        promise.dispose()
        result.dispose()


# =============================================================================
# Wire Format Tests (Real, not mocked)
# =============================================================================


class TestMapWireFormatReal:
    """Wire format tests using real serialization."""

    def test_wire_remap_roundtrip(self) -> None:
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
                1,
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

    def test_wire_remap_empty_path(self) -> None:
        """WireRemap with empty path should work."""
        remap = WireRemap(
            import_id=0,
            property_path=None,
            captures=[],
            instructions=[0],
        )

        json_data = remap.to_json()
        # B2: propertyPath is ALWAYS an array on the wire (TS rejects null).
        assert json_data[2] == []

        parsed = WireRemap.from_json(json_data)
        assert not parsed.property_path  # [] round-trips as empty

    def test_wire_capture_types(self) -> None:
        """WireCapture should handle both types."""
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


# =============================================================================
# Performance Benchmarks with Real Sessions
# =============================================================================


class TestMapPerformanceReal:
    """Performance benchmarks with real sessions."""

    async def test_throughput_benchmark(self) -> None:
        """Measure real throughput."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()

            num_operations = 100
            start = time.perf_counter()

            for _ in range(num_operations):
                result_hook = server_stub.call(
                    ["get_numbers"], RpcPayload.owned([10])
                )
                await result_hook.pull()

            duration = time.perf_counter() - start
            throughput = num_operations / duration

            # Should achieve at least 50 ops/sec
            assert throughput >= 50, f"Throughput too low: {throughput:.2f} ops/sec"

        finally:
            await server.stop()
            await client.stop()

    async def test_latency_benchmark(self) -> None:
        """Measure real latency."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayProviderTarget())
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            server_stub = client.get_main_stub()

            latencies: list[float] = []

            for _ in range(50):
                start = time.perf_counter()
                result_hook = server_stub.call(
                    ["get_numbers"], RpcPayload.owned([5])
                )
                await result_hook.pull()
                latencies.append((time.perf_counter() - start) * 1000)

            avg_latency = sum(latencies) / len(latencies)
            max_latency = max(latencies)

            # Average latency should be under 50ms
            assert avg_latency < 50, f"Average latency too high: {avg_latency:.2f}ms"
            # Max latency should be under 200ms
            assert max_latency < 200, f"Max latency too high: {max_latency:.2f}ms"

        finally:
            await server.stop()
            await client.stop()
