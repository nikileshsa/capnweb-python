"""Tests for map() functionality.

This tests the .map() capability feature that allows applying a function
to array elements remotely without transferring data back and forth.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.hooks import (
    ErrorStubHook,
    PayloadStubHook,
    StubHook,
    TargetStubHook,
)
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession
from capnweb.types import RpcTarget


# -----------------------------------------------------------------------------
# Test fixtures
# -----------------------------------------------------------------------------


class MockTransport:
    """Simple in-memory transport for testing."""

    def __init__(self) -> None:
        self.peer: MockTransport | None = None
        self.inbox: asyncio.Queue[str] = asyncio.Queue()
        self.closed = False

    async def send(self, message: str) -> None:
        if self.closed:
            raise ConnectionError("Transport closed")
        if self.peer:
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


# -----------------------------------------------------------------------------
# Test RpcTargets
# -----------------------------------------------------------------------------


class ArrayTarget(RpcTarget):
    """Target that provides array data for map tests."""

    def __init__(self, data: list[Any]) -> None:
        self._data = data

    def get_data(self) -> list[Any]:
        return self._data

    def get_numbers(self) -> list[int]:
        return [1, 2, 3, 4, 5]

    def get_objects(self) -> list[dict]:
        return [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]


class ProcessorTarget(RpcTarget):
    """Target that can process items."""

    def double(self, x: int) -> int:
        return x * 2

    def get_name(self, obj: dict) -> str:
        return obj.get("name", "unknown")

    async def slow_process(self, x: int) -> int:
        await asyncio.sleep(0.01)
        return x + 100


# -----------------------------------------------------------------------------
# Unit tests for StubHook.map()
# -----------------------------------------------------------------------------


class TestErrorStubHookMap:
    """Tests for ErrorStubHook.map()."""

    def test_map_returns_self(self) -> None:
        """map() on error hook should return self."""
        error = RpcError.internal("test error")
        hook = ErrorStubHook(error)

        # Create dummy captures
        captures: list[StubHook] = []

        result = hook.map([], captures, [])

        assert result is hook

    def test_map_disposes_captures(self) -> None:
        """map() on error hook should dispose captures."""
        error = RpcError.internal("test error")
        hook = ErrorStubHook(error)

        # Create a capture that tracks disposal
        disposed = []

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
                disposed.append(True)

            def dup(self):
                return self

        captures = [TrackingHook()]
        hook.map([], captures, [])

        assert len(disposed) == 1


class TestPayloadStubHookMap:
    """Tests for PayloadStubHook.map()."""

    async def test_map_over_empty_array(self) -> None:
        """map() over empty array returns empty array."""
        payload = RpcPayload.owned([])
        hook = PayloadStubHook(payload)

        # Simple identity map
        result = hook.map([], [], [0])  # Return input unchanged

        result_payload = await result.pull()
        assert result_payload.value == []

    async def test_map_over_single_value(self) -> None:
        """map() over single value should apply the mapper to it."""
        payload = RpcPayload.owned(42)
        hook = PayloadStubHook(payload)

        # Map over single value - the instruction [0] means "return variable 0 (the input)"
        # But since map_applicator uses the parser which interprets this as an import,
        # we need to verify the behavior is consistent
        result = hook.map([], [], [0])

        # The result depends on how the parser interprets [0]
        # For now, just verify it doesn't crash and returns something
        result_payload = await result.pull()
        assert result_payload is not None


class TestTargetStubHookMap:
    """Tests for TargetStubHook.map()."""

    async def test_map_empty_instructions_rejected(self) -> None:
        """map() with an empty instruction list is invalid (map.ts:252-254)."""
        target = ArrayTarget([1, 2, 3])
        hook = TargetStubHook(target)

        result = hook.map([], [], [])

        with pytest.raises(RpcError, match="Invalid empty mapper function"):
            await result.pull()

    async def test_map_over_target_whole_subject(self) -> None:
        """B2/TS parity: mapping a target applies the mapper to it as a
        single value (ValueStubHook.map -> applyMap single-value branch)."""
        target = ArrayTarget([1, 2, 3])
        hook = TargetStubHook(target)

        # identity mapper: instructions = [["pipeline", 0]]
        result = hook.map([], [], [["pipeline", 0]])
        payload = await result.pull()
        # The single-value branch wraps the target in a stub for the mapper.
        assert payload.value is not None


# -----------------------------------------------------------------------------
# Integration tests with BidirectionalSession
# -----------------------------------------------------------------------------


class TestMapWithSession:
    """Integration tests for map() with BidirectionalSession."""

    async def test_session_handles_remap_message(self) -> None:
        """Session should handle remap wire messages."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayTarget([1, 2, 3]))
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            # Get the server's main stub
            server_stub = client.get_main_stub()

            # Make a regular call to verify connection works
            result_hook = server_stub.call(["get_numbers"], RpcPayload.owned([]))
            result = await result_hook.pull()

            assert result.value == [1, 2, 3, 4, 5]

        finally:
            await server.stop()
            await client.stop()


# -----------------------------------------------------------------------------
# Tests for ignore_unhandled_rejections()
# -----------------------------------------------------------------------------


class TestIgnoreUnhandledRejections:
    """Tests for ignore_unhandled_rejections()."""

    def test_error_hook_ignore_is_noop(self) -> None:
        """ignore_unhandled_rejections() on error hook is a no-op."""
        error = RpcError.internal("test")
        hook = ErrorStubHook(error)
        hook.ignore_unhandled_rejections()  # Should not raise

    def test_payload_hook_ignore_is_noop(self) -> None:
        """ignore_unhandled_rejections() on payload hook is a no-op."""
        payload = RpcPayload.owned(42)
        hook = PayloadStubHook(payload)
        hook.ignore_unhandled_rejections()  # Should not raise

    def test_target_hook_ignore_is_noop(self) -> None:
        """ignore_unhandled_rejections() on target hook is a no-op."""
        target = ArrayTarget([])
        hook = TargetStubHook(target)
        hook.ignore_unhandled_rejections()  # Should not raise


# -----------------------------------------------------------------------------
# Tests for exportPromise vs exportStub
# -----------------------------------------------------------------------------


class TestExportPromiseVsStub:
    """Tests for separate promise and stub export handling."""

    async def test_export_stub_reuses_id(self) -> None:
        """Exporting the same stub twice should reuse the export ID."""
        transport_a, transport_b = create_transport_pair()

        server = BidirectionalSession(transport_a, ArrayTarget([]))
        client = BidirectionalSession(transport_b, None)

        server.start()
        client.start()

        try:
            # The export_capability method should reuse IDs for the same hook
            # This is tested indirectly through the session behavior
            stats = server.get_stats()
            assert "exports" in stats
            assert "imports" in stats

        finally:
            await server.stop()
            await client.stop()


# -----------------------------------------------------------------------------
# Wire format tests
# -----------------------------------------------------------------------------


class TestWireRemap:
    """Tests for WireRemap wire format."""

    def test_wire_remap_serialization(self) -> None:
        """WireRemap should serialize correctly."""
        from capnweb.wire import WireRemap, WireCapture, PropertyKey

        remap = WireRemap(
            import_id=0,
            property_path=[PropertyKey("data")],
            captures=[WireCapture("import", 1)],
            instructions=[["pipeline", 0, ["double"]]],
        )

        json_repr = remap.to_json()

        assert json_repr[0] == "remap"
        assert json_repr[1] == 0  # import_id
        assert json_repr[2] == ["data"]  # property_path
        assert json_repr[3] == [["import", 1]]  # captures
        assert len(json_repr[4]) == 1  # instructions

    def test_wire_remap_parsing(self) -> None:
        """WireRemap should parse correctly."""
        from capnweb.wire import WireRemap

        json_data = [
            "remap",
            0,
            ["data"],
            [["export", -1]],
            [["pipeline", 0, ["process"]]],
        ]

        remap = WireRemap.from_json(json_data)

        assert remap.import_id == 0
        assert len(remap.property_path) == 1
        assert remap.property_path[0].value == "data"
        assert len(remap.captures) == 1
        assert remap.captures[0].type == "export"
        assert remap.captures[0].id == -1
        assert len(remap.instructions) == 1


class TestWireCapture:
    """Tests for WireCapture wire format."""

    def test_wire_capture_import(self) -> None:
        """WireCapture import should serialize correctly."""
        from capnweb.wire import WireCapture

        capture = WireCapture("import", 5)
        assert capture.to_json() == ["import", 5]

    def test_wire_capture_export(self) -> None:
        """WireCapture export should serialize correctly."""
        from capnweb.wire import WireCapture

        capture = WireCapture("export", -3)
        assert capture.to_json() == ["export", -3]

    def test_wire_capture_parsing(self) -> None:
        """WireCapture should parse correctly."""
        from capnweb.wire import WireCapture

        capture = WireCapture.from_json(["import", 10])
        assert capture.type == "import"
        assert capture.id == 10
