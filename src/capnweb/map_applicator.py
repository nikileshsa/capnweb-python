"""MapApplicator — server-side execution of mapper instructions.

Port of map.ts:237-351. A received ``["remap", ...]`` carries the recorded
body of a ``.map()`` callback; this module evaluates those instructions once
per input element, in the mapper's own index space:

* negative n  -> captures[-n-1] (stubs the sender captured)
* 0           -> the input element
* positive n  -> the result of instructions[n-1]
* the last instruction is the mapper's return value

Evaluation reuses the ONE Parser/Evaluator (parser.py) with the applicator
as the Importer (C-IMPORTER): ``get_export`` resolves mapper indices, while
``export``/``promise`` tags inside instructions HARD-FAIL — a mapper cannot
reference exports (map.ts:279-286; security: index-aliasing, matrix 04
row 15).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from capnweb.error import RpcError
from capnweb.payload import RpcPayload

if TYPE_CHECKING:
    from capnweb.hooks import StubHook


class MapApplicator:
    """Applies map instructions to a single input element (map.ts:237-299).

    Implements the C-IMPORTER surface with mapper index semantics.
    """

    def __init__(self, captures: list["StubHook"], input_hook: "StubHook") -> None:
        """Initialize the applicator.

        Args:
            captures: External stubs used in the mapper function (borrowed —
                the applyMap caller owns and disposes them)
            input_hook: The input element wrapped in a hook (owned)
        """
        self.captures = captures
        # Variables: index 0 is input, positive indices are instruction results
        self.variables: list["StubHook"] = [input_hook]

    def dispose(self) -> None:
        """Dispose all variables (map.ts:244-248)."""
        variables = self.variables
        self.variables = []
        for var in variables:
            var.dispose()

    def apply(self, instructions: list[Any]) -> RpcPayload:
        """Evaluate the instructions; the last one is the return value.

        Args:
            instructions: Raw JSON expressions in mapper index space

        Returns:
            The result payload

        Raises:
            RpcError: If the instructions are invalid
        """
        from capnweb.hooks import PayloadStubHook

        if not isinstance(instructions, list) or not instructions:
            raise RpcError("Error", "Invalid empty mapper function.")

        # Evaluate all instructions except the last (which is the return
        # value); each intermediate result becomes a variable.
        for instruction in instructions[:-1]:
            payload = self._evaluate_instruction(instruction)

            # The payload almost always contains a single stub; as an
            # optimization, unwrap it and store the hook directly
            # (map.ts:256-268 unwrapStubNoProperties).
            hook = _unwrap_single_stub(payload)
            if hook is not None:
                self.variables.append(hook)
            else:
                self.variables.append(PayloadStubHook(payload))

        # Evaluate the final instruction (the mapper's return value).
        return self._evaluate_instruction(instructions[-1])

    def _evaluate_instruction(self, instruction: Any) -> RpcPayload:
        """Evaluate one instruction with the applicator as Importer."""
        from capnweb.parser import Parser

        parser = Parser(importer=self)
        return parser.parse(instruction)

    # -----------------------------------------------------------------------
    # C-IMPORTER protocol (mapper index semantics, map.ts:279-298)
    # -----------------------------------------------------------------------

    def get_export(self, idx: int) -> "StubHook | None":
        """Resolve a mapper-space index (map.ts:288-294).

        Negative indices are captures; 0 and positive indices are variables
        (0 = the input element). Out-of-range indices return None so the
        evaluator raises the TS 'no such entry on exports table' error.
        """
        if idx < 0:
            capture_idx = -idx - 1
            if capture_idx >= len(self.captures):
                return None
            return self.captures[capture_idx]
        if idx >= len(self.variables):
            return None
        return self.variables[idx]

    def import_capability(self, import_id: int) -> "StubHook":
        """HARD FAIL: ``["export", id]`` inside mapper instructions.

        In session context an "export" tag means "the sender is exporting a
        new capability"; inside a mapper it would alias the sender-chosen
        index into OUR tables — a capability-confusion hazard. TS throws
        (map.ts:279-283); so do we (matrix 04 row 15).
        """
        raise RpcError("Error", "A mapper function cannot refer to exports.")

    def create_promise_hook(self, promise_id: int) -> "StubHook":
        """HARD FAIL: ``["promise", id]`` inside mapper instructions
        (map.ts:284-286)."""
        raise RpcError("Error", "A mapper function cannot refer to exports.")

    def get_pipe_readable(self, export_id: int) -> Any:
        """HARD FAIL: pipes cannot appear inside mapper instructions
        (map.ts:296-298)."""
        raise RpcError("Error", "A mapper function cannot use pipe readables.")


def _unwrap_single_stub(payload: RpcPayload) -> "StubHook | None":
    """Extract the hook when the payload IS a single stub/root promise.

    Python port of ``unwrapStubNoProperties`` applied to instruction results
    (map.ts:259-266). Property promises (pending path) are not unwrapped.
    Ownership: the hook is taken from the payload, which is then dropped
    WITHOUT disposal (it contains nothing else).
    """
    from capnweb.stubs import RpcPromise, RpcStub

    value = payload.value
    if isinstance(value, RpcStub):
        return value._hook
    if isinstance(value, RpcPromise) and not value._path:
        return value._raw_hook
    return None


def apply_map_to_element(
    input_value: Any,
    parent: object | None,
    owner: RpcPayload | None,
    captures: list["StubHook"],
    instructions: list[Any],
) -> RpcPayload:
    """Apply map instructions to a single element (map.ts:301-313).

    Args:
        input_value: The element to map over
        parent: Parent object (for RpcTarget handling)
        owner: Owner payload (for RpcTarget handling)
        captures: External stubs used in the mapper (borrowed)
        instructions: The mapper instructions (raw JSON)

    Returns:
        The mapped result payload
    """
    from capnweb.hooks import PayloadStubHook

    # Create a hook for the input
    input_payload = RpcPayload.deep_copy_from(input_value, parent, owner)
    input_hook = PayloadStubHook(input_payload)

    # Apply the instructions
    applicator = MapApplicator(captures, input_hook)
    try:
        return applicator.apply(instructions)
    finally:
        applicator.dispose()


def apply_map_locally(
    input_value: Any,
    parent: object | None,
    owner: RpcPayload | None,
    captures: list["StubHook"],
    instructions: list[Any],
) -> "StubHook":
    """Apply a map operation to a local value (mapImpl.applyMap,
    map.ts:315-351).

    Semantics per element kind: an RpcPromise input is a caller bug; arrays
    map per-element (disposing partial results on failure); null/None and
    the Undefined sentinel pass through untouched; any other single value is
    mapped once. Captures are ALWAYS disposed on the way out.

    Args:
        input_value: The value to map over (usually an array)
        parent: The containing object, if any
        owner: The owner payload
        captures: External stubs used in the mapper (ownership taken)
        instructions: The mapper instructions (raw JSON)

    Returns:
        A StubHook containing the mapped results
    """
    from capnweb.hooks import PayloadStubHook
    from capnweb.stubs import RpcPromise
    from capnweb.types import Undefined

    try:
        if isinstance(input_value, RpcPromise):
            # The caller is responsible for making sure the input is not a
            # promise, since we can't know if it resolves to an array later.
            raise RpcError("Error", "applyMap() can't be called on RpcPromise")

        if isinstance(input_value, list):
            # Map over array elements
            payloads: list[RpcPayload] = []
            try:
                for elem in input_value:
                    payloads.append(
                        apply_map_to_element(
                            elem, input_value, owner, captures, instructions
                        )
                    )
            except Exception:
                # Dispose already-created payloads on error
                for p in payloads:
                    p.dispose()
                raise

            # Combine into array payload; from_array re-parents element-root
            # promises onto the combined array natively (payload.py).
            result = RpcPayload.from_array(payloads)
        elif input_value is None or input_value is Undefined:
            # null/undefined pass through (map.ts:337-338)
            result = RpcPayload.from_app_return(input_value)
        else:
            # Single value - apply map to it
            result = apply_map_to_element(
                input_value, parent, owner, captures, instructions
            )

        return PayloadStubHook(result)

    finally:
        # Dispose captures (map.ts:346-350)
        for cap in captures:
            cap.dispose()
