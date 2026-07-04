"""B2 stream — client-side .map() recorder tests (map.ts:8-233 port).

Pins: contextvar call interception (core.ts:326-341), lazy path accumulation
on RpcPromise (fused pipeline instructions), pushCall arg unwrap
(map.ts:79-86), capture dedup + negative indices, nested recording,
recorder-scoped Exporter restrictions, sync misuse throws at the .map() call
site, and the ``[]``-not-``null`` propertyPath wire rule (matrix 04 row 12).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.hooks import PayloadStubHook, StubHook, TargetStubHook
from capnweb.map_builder import MapBuilder, send_map
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcPromise, RpcStub, with_call_interceptor
from capnweb.types import RpcTarget


class RecordingSubject(StubHook):
    """Subject hook that records the map() dispatch."""

    def __init__(self) -> None:
        self.recorded: tuple[list, list, list] | None = None

    def call(self, path, args):  # pragma: no cover - not used
        raise AssertionError("unexpected call")

    def get(self, path):  # pragma: no cover - not used
        raise AssertionError("unexpected get")

    def map(self, path, captures, instructions):
        self.recorded = (path, captures, instructions)
        return PayloadStubHook(RpcPayload.owned("map-result"))

    async def pull(self):  # pragma: no cover - not used
        raise AssertionError("unexpected pull")

    def ignore_unhandled_rejections(self) -> None:
        pass

    def dispose(self) -> None:
        pass

    def dup(self):
        return self


def record(func, path: list | None = None) -> tuple[list, list, list]:
    """Run send_map against a recording subject; return (path, caps, instrs)."""
    subject = RecordingSubject()
    promise = send_map(subject, path or [], func)
    assert isinstance(promise, RpcPromise)
    assert subject.recorded is not None
    return subject.recorded


class TestRecorderInstructions:
    def test_identity_mapper_records_pipeline_zero(self) -> None:
        _path, captures, instructions = record(lambda x: x)
        assert captures == []
        # The final (only) instruction is the devaluated input placeholder.
        assert instructions == [["pipeline", 0]]

    def test_fused_path_call_records_single_instruction(self) -> None:
        """x.a.b(args) must fuse into ONE ["pipeline", 0, ["a","b"], args]
        instruction (TS lazy pathIfPromise; matrix 04 row 3)."""
        _path, captures, instructions = record(lambda x: x.a.b(5))
        assert captures == []
        assert instructions == [
            ["pipeline", 0, ["a", "b"], [5]],  # the call
            ["pipeline", 1],                    # return value = result of #1
        ]

    def test_push_call_args_not_double_escaped(self) -> None:
        """map.ts:79-86 unwrap hack: instruction args are UN-escaped."""
        _path, _captures, instructions = record(lambda x: x.f([1, 2]))
        # args must be [[1,2]]-the-literal-array-escape, NOT [[[1,2]]]
        assert instructions[0] == ["pipeline", 0, ["f"], [[[1, 2]]]]

    def test_property_return_devaluates_inline_path(self) -> None:
        """Returning a bare property promise devaluates as
        ["pipeline", 0, ["prop"]] WITHOUT an extra instruction (TS
        unwrapStubAndPath on lazy paths)."""
        _path, _captures, instructions = record(lambda x: x.prop)
        assert instructions == [["pipeline", 0, ["prop"]]]

    def test_captured_stub_gets_negative_index_and_dedupes(self) -> None:
        capture_hook = TargetStubHook(lambda v: v)
        stub = RpcStub(capture_hook)

        def mapper(x):
            a = stub.inc(x)
            b = stub.dec(x)
            return [a, b]

        _path, captures, instructions = record(mapper)
        # One deduped capture despite two calls.
        assert len(captures) == 1
        assert instructions[0][:2] == ["pipeline", -1]
        assert instructions[1][:2] == ["pipeline", -1]
        # Final instruction is the escaped array of the two results.
        assert instructions[2] == [[["pipeline", 1], ["pipeline", 2]]]

    def test_mixed_structure_return(self) -> None:
        capture_hook = TargetStubHook(lambda v: v)
        stub = RpcStub(capture_hook)

        def mapper(x):
            val = stub.compute(x)
            return {"value": val, "orig": x, "n": 42}

        _path, captures, instructions = record(mapper)
        assert len(captures) == 1
        assert instructions[0] == ["pipeline", -1, ["compute"], [["pipeline", 0]]]
        assert instructions[1] == {
            "value": ["pipeline", 1],
            "orig": ["pipeline", 0],
            "n": 42,
        }

    def test_nested_map_records_remap_instruction(self) -> None:
        def mapper(x):
            return x.items.map(lambda y: y.name)

        _path, captures, instructions = record(mapper)
        assert captures == []
        # Nested remap over placeholder path ["items"]:
        # inner instructions: [["pipeline", 0, ["name"]]]
        nested = instructions[0]
        assert nested[0] == "remap"
        assert nested[1] == 0  # subject = outer input placeholder
        assert nested[2] == ["items"]
        assert nested[3] == []  # no captures pulled from the parent
        assert nested[4] == [["pipeline", 0, ["name"]]]
        # Mapper return value = nested remap's result variable.
        assert instructions[1] == ["pipeline", 1]

    def test_nested_map_captures_chain_through_parent(self) -> None:
        capture_hook = TargetStubHook(lambda v: v)
        stub = RpcStub(capture_hook)

        def mapper(x):
            return x.items.map(lambda y: stub.f(y))

        _path, captures, instructions = record(mapper)
        # The inner capture of `stub` chains to the parent (map.ts:105-108).
        assert len(captures) == 1
        nested = instructions[0]
        assert nested[0] == "remap"
        assert nested[3] == [["import", -1]]  # parent capture index
        assert nested[4][0][:2] == ["pipeline", -1]


class TestRecorderMisuse:
    def test_async_callback_rejected(self) -> None:
        subject = RecordingSubject()

        async def mapper(x):  # noqa: RUF029
            return x

        with pytest.raises(RpcError, match=r"map\(\) callbacks cannot be async"):
            send_map(subject, [], mapper)

    def test_callback_exception_propagates_synchronously(self) -> None:
        subject = RecordingSubject()

        def mapper(x):
            raise ValueError("mapper exploded")

        with pytest.raises(ValueError, match="mapper exploded"):
            send_map(subject, [], mapper)
        assert subject.recorded is None

    async def test_awaiting_placeholder_raises_placeholder_error(self) -> None:
        subject = RecordingSubject()
        seen: list[Exception] = []

        def mapper(x):
            try:
                # Placeholders cannot be pulled (map.ts:221-224); the pull
                # coroutine raises as soon as it runs.
                coro = x._hook.pull()
                try:
                    coro.send(None)
                except StopIteration:  # pragma: no cover
                    pass
            except Exception as e:
                seen.append(e)
                raise
            return x

        with pytest.raises(RpcError, match="abstract placeholder"):
            send_map(subject, [], mapper)
        assert seen and "abstract placeholder" in str(seen[0])

    def test_raw_rpc_target_in_mapper_rejected(self) -> None:
        subject = RecordingSubject()

        class Inline(RpcTarget):
            async def call(self, method, args):  # pragma: no cover
                return None

        def mapper(x):
            return x.consume(Inline())

        with pytest.raises(
            RpcError,
            match="Can't construct an RpcTarget or RPC callback inside a mapper",
        ):
            send_map(subject, [], mapper)

    def test_readable_stream_in_mapper_rejected(self) -> None:
        from capnweb.streams import RpcReadableStream

        async def gen():  # pragma: no cover
            yield b"x"

        stream = RpcReadableStream(gen())
        subject = RecordingSubject()

        def mapper(x):
            return x.consume(stream)

        with pytest.raises(RpcError, match="Cannot send ReadableStream inside a mapper"):
            send_map(subject, [], mapper)

    def test_existing_stub_is_fine(self) -> None:
        stub = RpcStub(PayloadStubHook(RpcPayload.owned("cap")))

        def mapper(x):
            return x.consume(stub)

        _path, captures, instructions = record(mapper)
        assert len(captures) == 1
        assert instructions[0] == ["pipeline", 0, ["consume"], [["import", -1]]]


class TestCallInterceptor:
    def test_interceptor_scoped_and_restored(self) -> None:
        calls: list[tuple] = []

        def interceptor(hook, path, params):
            calls.append((hook, list(path)))
            return PayloadStubHook(RpcPayload.owned("intercepted"))

        stub = RpcStub(PayloadStubHook(RpcPayload.owned(lambda: "real")))
        result = with_call_interceptor(interceptor, lambda: stub.method(1))
        assert isinstance(result, RpcPromise)
        assert calls and calls[0][1] == ["method"]

        # Outside the scope, calls hit the hook again.
        outside = stub.noop(2)
        assert isinstance(outside, RpcPromise)
        assert len(calls) == 1

    def test_interceptor_restored_on_exception(self) -> None:
        def interceptor(hook, path, params):  # pragma: no cover
            raise AssertionError("should not be reached")

        with pytest.raises(RuntimeError, match="boom"):
            with_call_interceptor(
                interceptor, lambda: (_ for _ in ()).throw(RuntimeError("boom"))
            )

        # Interceptor must be uninstalled after the exception.
        stub = RpcStub(PayloadStubHook(RpcPayload.owned(lambda: "x")))
        assert isinstance(stub.anything(), RpcPromise)


class TestBuilderExporterSurface:
    def test_builder_registration_is_scoped(self) -> None:
        import capnweb.map_builder as mb

        assert mb._current_map_builder is None
        subject = RecordingSubject()
        send_map(subject, [], lambda x: x)
        assert mb._current_map_builder is None

    def test_builder_unregisters_on_callback_error(self) -> None:
        import capnweb.map_builder as mb

        subject = RecordingSubject()
        with pytest.raises(ValueError):
            send_map(subject, [], lambda x: (_ for _ in ()).throw(ValueError()))
        assert mb._current_map_builder is None

    def test_capture_dups_hook(self) -> None:
        """push_get/push_call capture hook.dup() — ownership per row 6."""

        class CountingHook(RecordingSubject):
            def __init__(self) -> None:
                super().__init__()
                self.dups = 0

            def dup(self):
                self.dups += 1
                return self

        capture = CountingHook()
        stub = RpcStub.__new__(RpcStub)  # bypass coercion; raw hook stub
        object.__setattr__(stub, "_hook", capture)

        subject = RecordingSubject()
        send_map(subject, [], lambda x: stub.f(x))
        assert capture.dups >= 1
