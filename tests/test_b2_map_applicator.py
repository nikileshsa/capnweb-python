"""B2 stream — server-side map/remap evaluation tests (map.ts:237-351 port).

Pins: the ONE Parser/Evaluator path for mapper instructions
(serialize.ts:826-951), the mapper index space (negative -> captures, 0 ->
input, positive -> prior results), call-argument re-wrap semantics, nested
remap with child capture scope, and the SECURITY hard-fail on
``export``/``promise`` tags inside instructions (matrix 04 row 15 —
index-aliasing).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.hooks import PayloadStubHook, TargetStubHook
from capnweb.map_applicator import MapApplicator, apply_map_locally
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcPromise, RpcStub
from capnweb.types import RpcTarget, Undefined


class Doubler(RpcTarget):
    """Capture target: double(x) -> 2x, record calls."""

    def __init__(self) -> None:
        self.calls: list[Any] = []

    async def call(self, method: str, args: list) -> Any:
        self.calls.append((method, list(args)))
        if method == "double":
            return args[0] * 2
        if method == "add":
            return args[0] + args[1]
        if method == "makeList":
            return list(range(args[0]))
        raise RpcError.not_found(method)

    async def get_property(self, name: str) -> Any:
        raise AttributeError(name)


def map_hook(input_value: Any, captures: list, instructions: list):
    """Dispatch a map through the hook layer (like a received remap)."""
    return PayloadStubHook(RpcPayload.owned(input_value)).map(
        [], captures, instructions
    )


async def run_map(
    input_value: Any,
    captures: list,
    instructions: list,
) -> Any:
    """hook.map + pull, returning the delivered value."""
    from capnweb.stubs import deliver_payload_in_place

    payload = await map_hook(input_value, captures, instructions).pull()
    await deliver_payload_in_place(payload)
    return payload.value


class TestMapperIndexSpace:
    async def test_literal_final_instruction(self) -> None:
        assert await run_map([1, 2, 3], [], [42]) == [42, 42, 42]

    async def test_input_reference(self) -> None:
        assert await run_map([1, 2, 3], [], [["pipeline", 0]]) == [1, 2, 3]

    async def test_input_property_path(self) -> None:
        data = [{"v": 10}, {"v": 20}]
        assert await run_map(data, [], [["pipeline", 0, ["v"]]]) == [10, 20]

    async def test_capture_call_with_input_arg(self) -> None:
        doubler = Doubler()
        captures = [TargetStubHook(doubler)]
        result = await run_map(
            [1, 2, 3], captures,
            [
                ["pipeline", -1, ["double"], [["pipeline", 0]]],
                ["pipeline", 1],
            ],
        )
        assert result == [2, 4, 6]
        assert [a for m, a in doubler.calls] == [[1], [2], [3]]

    async def test_positive_index_chains_previous_results(self) -> None:
        doubler = Doubler()
        captures = [TargetStubHook(doubler)]
        result = await run_map(
            [1, 2], captures,
            [
                ["pipeline", -1, ["double"], [["pipeline", 0]]],   # 1: 2x
                ["pipeline", -1, ["double"], [["pipeline", 1]]],   # 2: 4x
                ["pipeline", 2],
            ],
        )
        assert result == [4, 8]

    async def test_call_args_rewrap(self) -> None:
        """Instruction args are UN-escaped; the evaluator re-wraps them
        (serialize.ts:898-900): add(x, 10)."""
        doubler = Doubler()
        captures = [TargetStubHook(doubler)]
        result = await run_map(
            [1, 2], captures,
            [
                ["pipeline", -1, ["add"], [["pipeline", 0], 10]],
                ["pipeline", 1],
            ],
        )
        assert result == [11, 12]

    async def test_structured_final_value(self) -> None:
        doubler = Doubler()
        captures = [TargetStubHook(doubler)]
        result = await run_map(
            [3], captures,
            [
                ["pipeline", -1, ["double"], [["pipeline", 0]]],
                {"orig": ["pipeline", 0], "doubled": ["pipeline", 1], "k": 7},
            ],
        )
        assert result == [{"orig": 3, "doubled": 6, "k": 7}]

    async def test_out_of_range_index_fails(self) -> None:
        hook = map_hook([1], [], [["pipeline", 5]])
        with pytest.raises(Exception, match="no such entry on exports table: 5"):
            await hook.pull()

    async def test_out_of_range_capture_fails(self) -> None:
        hook = map_hook([1], [], [["pipeline", -2]])
        with pytest.raises(Exception, match="no such entry on exports table: -2"):
            await hook.pull()


class TestElementSemantics:
    async def test_null_passthrough(self) -> None:
        assert await run_map(None, [], [42]) is None

    async def test_undefined_passthrough(self) -> None:
        assert await run_map(Undefined, [], [42]) is Undefined

    async def test_single_value_applied_once(self) -> None:
        doubler = Doubler()
        captures = [TargetStubHook(doubler)]
        result = await run_map(
            21, captures,
            [["pipeline", -1, ["double"], [["pipeline", 0]]], ["pipeline", 1]],
        )
        assert result == 42
        assert len(doubler.calls) == 1

    async def test_empty_array(self) -> None:
        assert await run_map([], [], [["pipeline", 0]]) == []

    async def test_rpc_promise_input_rejected(self) -> None:
        promise = RpcPromise(PayloadStubHook(RpcPayload.owned([1])))
        hook = map_hook([promise], [], [0])
        # A promise ELEMENT is deep-copied per element; the whole-value case:
        hook = PayloadStubHook(RpcPayload.owned(None)).map([], [], [0])
        del hook
        hook = map_hook(promise, [], [0])
        with pytest.raises(RpcError, match=r"applyMap\(\) can't be called on RpcPromise"):
            await hook.pull()

    async def test_empty_instructions_rejected(self) -> None:
        hook = map_hook([1, 2], [], [])
        with pytest.raises(RpcError, match="Invalid empty mapper function"):
            await hook.pull()

    async def test_captures_disposed_after_apply(self) -> None:
        class DisposeTracking(PayloadStubHook):
            def __init__(self) -> None:
                super().__init__(RpcPayload.owned("cap"))
                self.disposed = False

            def dispose(self) -> None:
                self.disposed = True
                super().dispose()

        cap = DisposeTracking()
        await run_map([1], [cap], [["pipeline", 0]])
        assert cap.disposed


class TestExportTagSecurity:
    """Matrix 04 row 15 — mapper instructions cannot reference exports."""

    async def test_export_tag_hard_fails(self) -> None:
        hook = map_hook([1], [], [["export", 1]])
        with pytest.raises(RpcError, match="A mapper function cannot refer to exports"):
            await hook.pull()

    async def test_promise_tag_hard_fails(self) -> None:
        hook = map_hook([1], [], [["promise", 0]])
        with pytest.raises(RpcError, match="A mapper function cannot refer to exports"):
            await hook.pull()

    async def test_export_tag_does_not_alias_variable_index(self) -> None:
        """ADVERSARIAL: ["export", 1] must NOT resolve variable 1 (the old
        import_capability implementation silently aliased indices)."""
        doubler = Doubler()
        captures = [TargetStubHook(doubler)]
        hook = map_hook(
            [5], captures,
            [
                ["pipeline", -1, ["double"], [["pipeline", 0]]],  # variable 1
                ["export", 1],  # attacker hopes this aliases variable 1
            ],
        )
        with pytest.raises(RpcError, match="A mapper function cannot refer to exports"):
            await hook.pull()

    async def test_export_tag_in_capture_slot_of_nested_remap_fails(self) -> None:
        hook = map_hook(
            [[1, 2]], [],
            [["remap", 0, [], [["export", 3]], [["pipeline", 0]]]],
        )
        with pytest.raises(RpcError, match="A mapper function cannot refer to exports"):
            await hook.pull()

    def test_get_pipe_readable_hard_fails(self) -> None:
        applicator = MapApplicator([], PayloadStubHook(RpcPayload.owned(1)))
        with pytest.raises(RpcError, match="cannot use pipe readables"):
            applicator.get_pipe_readable(1)


class TestNestedRemap:
    async def test_nested_remap_over_input_property(self) -> None:
        data = [{"items": [1, 2]}, {"items": [3]}]
        # mapper: x => x.items.map(y => y)  ==>
        # instructions: [["remap", 0, ["items"], [], [["pipeline", 0]]],
        #                ["pipeline", 1]]
        result = await run_map(
            data, [],
            [
                ["remap", 0, ["items"], [], [["pipeline", 0]]],
                ["pipeline", 1],
            ],
        )
        assert result == [[1, 2], [3]]

    async def test_nested_remap_with_parent_capture(self) -> None:
        doubler = Doubler()
        captures = [TargetStubHook(doubler)]
        # mapper: x => x.items.map(y => cap.double(y))
        result = await run_map(
            [{"items": [1, 2]}, {"items": [10]}], captures,
            [
                ["remap", 0, ["items"], [["import", -1]],
                 [["pipeline", -1, ["double"], [["pipeline", 0]]],
                  ["pipeline", 1]]],
                ["pipeline", 1],
            ],
        )
        assert result == [[2, 4], [20]]

    async def test_nested_remap_null_path_lenient(self) -> None:
        """A null propertyPath in a received remap is tolerated (pre-B2
        Python senders emitted it)."""
        result = await run_map(
            [[1, 2]], [],
            [["remap", 0, None, [], [["pipeline", 0]]], ["pipeline", 1]],
        )
        assert result == [[1, 2]]
