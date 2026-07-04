"""MapBuilder — the client-side .map() recorder (port of map.ts:8-233).

``send_map(hook, path, func)`` runs the user's mapper callback exactly once,
synchronously, against a recording placeholder promise. Every RPC call the
callback makes is intercepted (via ``stubs.with_call_interceptor``, the
Python ``doCall`` swap of core.ts:326-341) and compiled into a wire
instruction; the callback's return value is devaluated with the builder as
the Exporter, so references to placeholders and captured stubs become
``["pipeline", idx, path?]`` / ``["import", idx]`` forms in the mapper's own
index space:

    negative n -> captures[-n-1],  0 -> map input,  positive n -> result of
    instructions[n-1];  the last instruction is the mapper's return value.

Example:
    ```python
    result = await stub.getData().map(lambda x: x.process(counter.dup()))
    # records: [["pipeline", 0, ["process"], [["import", -1]]],
    #           ["pipeline", 1]]
    ```
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable

from capnweb.error import RpcError
from capnweb.hooks import StubHook
from capnweb.payload import RpcPayload

# Type alias for map instructions
MapInstruction = list[Any]


# The currently-recording builder (TS module global `currentMapBuilder`,
# map.ts:8). Recording is strictly synchronous — mapper callbacks cannot
# await — so a module global matches TS semantics; the call interceptor
# itself is contextvar-scoped in stubs.py.
_current_map_builder: "MapBuilder | None" = None


_PLACEHOLDER_USE_ERROR = (
    "Attempted to use an abstract placeholder from a mapper function. "
    "Please make sure your map function has no side effects."
)


def _placeholder_use_error() -> RpcError:
    """The TS throwMapperBuilderUseError (map.ts:181-185)."""
    return RpcError("Error", _PLACEHOLDER_USE_ERROR)


def _reject_new_targets(value: Any) -> None:
    """Reject RAW RpcTargets/callables in recorder values (row 8 scoping).

    TS's Devaluator sees raw targets and routes them to
    ``MapBuilder.exportStub`` which throws (map.ts:120-136). Python wraps
    raw targets into stubs during payload deep copy — BEFORE the serializer
    could tell them apart from legitimately captured stubs — so the recorder
    walks the raw value first. Pre-existing ``RpcStub``/``RpcPromise``
    instances are fine (they become captures via ``get_import``).
    """
    from capnweb.stubs import RpcPromise, RpcStub
    from capnweb.types import RpcTarget

    if isinstance(value, (RpcStub, RpcPromise)) or value is None:
        return
    if isinstance(value, RpcTarget) or (
        callable(value) and not isinstance(value, type)
    ):
        raise RpcError(
            "Error",
            "Can't construct an RpcTarget or RPC callback inside a mapper "
            "function. Try creating a new RpcStub outside the callback "
            "first, then using it inside the callback.",
        )
    if isinstance(value, (list, tuple)):
        for item in value:
            _reject_new_targets(item)
    elif isinstance(value, dict):
        for item in value.values():
            _reject_new_targets(item)


class MapVariableHook(StubHook):
    """A hook representing a variable in a map function (map.ts:188-233).

    A placeholder tracking which instruction result it refers to. It cannot
    be pulled or used outside the recording context.
    """

    def __init__(self, mapper: "MapBuilder", idx: int) -> None:
        self.mapper = mapper
        self.idx = idx

    def dup(self) -> "MapVariableHook":
        """Nothing to dispose, so dup() can return the same hook (map.ts:194)."""
        return self

    def dispose(self) -> None:
        """Nothing to dispose for variables."""

    def get(self, path: list[str | int]) -> StubHook:
        """Get a property — records a pipeline instruction (map.ts:197-208).

        This IS invoked during serialization (devaluating a placeholder
        property), so it must work while a builder is registered.
        """
        if not path:
            # Cannot be pulled and dispose() is a no-op, so the same hook can
            # represent the empty-path get.
            return self

        if _current_map_builder is not None:
            return _current_map_builder.push_get(self, path)

        raise _placeholder_use_error()

    def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Can't be called; all calls are intercepted (map.ts:211-214)."""
        raise _placeholder_use_error()

    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        """Can't be called; all map()s are intercepted (map.ts:216-219)."""
        raise _placeholder_use_error()

    async def pull(self) -> RpcPayload:
        """Map functions cannot await (map.ts:221-224)."""
        raise _placeholder_use_error()

    def ignore_unhandled_rejections(self) -> None:
        """Probably never called but whatever (map.ts:226-228)."""

    def on_broken(self, callback: Any) -> None:
        """Placeholders have no failure channel (map.ts:230-232)."""
        raise _placeholder_use_error()


class MapBuilder:
    """Records the body of a ``.map()`` callback (map.ts:17-155).

    Implements the C-EXPORTER protocol with recorder semantics: capability
    references devaluate into capture indices instead of session exports.

    Usage:
        ```python
        builder = MapBuilder(subject_hook, path)
        try:
            input_var = builder.make_input()
            # ... execute map callback with input_var ...
            result_hook = builder.make_output(result_payload)
        finally:
            builder.unregister()
        ```
    """

    def __init__(self, subject: StubHook, path: list[str | int]) -> None:
        """Initialize the map builder and register it as current.

        Args:
            subject: The hook to map over
            path: Property path to the array
        """
        global _current_map_builder

        # Context: for a nested map, the subject is captured from the parent
        # builder and captures are parent indices; for a root map, the
        # subject is the hook itself and captures are hooks (map.ts:25-43).
        self.parent: MapBuilder | None = _current_map_builder
        self.path = list(path)
        if self.parent is not None:
            self.captures: list[Any] = []  # parent capture indices (int)
            self.subject: Any = self.parent.capture(subject)
        else:
            self.captures = []  # owned StubHooks
            self.subject = subject

        self._capture_map: dict[int, int] = {}  # id(hook) -> capture index
        self._instructions: list[MapInstruction] = []

        _current_map_builder = self

    def unregister(self) -> None:
        """Unregister this builder from the recording context (map.ts:45-47)."""
        global _current_map_builder
        _current_map_builder = self.parent

    def make_input(self) -> MapVariableHook:
        """Create the hook representing the map input (index 0)."""
        return MapVariableHook(self, 0)

    def make_output(self, result: RpcPayload) -> StubHook:
        """Finalize the recording and return the result hook (map.ts:53-75).

        The devaluated result is the FINAL instruction (the mapper's return
        value). A root builder dispatches ``subject.map(...)``; a nested
        builder appends a ``["remap", ...]`` instruction to its parent.
        """
        from capnweb.serializer import Serializer

        try:
            _reject_new_targets(result.value)
            # Devaluate the RAW value (TS Devaluator.devaluate on the
            # un-copied payload, map.ts:56): a deep copy would materialize
            # placeholder property paths via hook.get() OUTSIDE this
            # builder's registration, corrupting the recording.
            serializer = Serializer(exporter=self)
            devalued = serializer.serialize(result.value)
        finally:
            result.dispose()

        # The result is the final instruction.
        self._instructions.append(devalued)

        if self.parent is not None:
            # Nested map: emit a remap instruction into the parent recording
            # (map.ts:65-71).
            self.parent._instructions.append([
                "remap",
                self.subject,
                self.path,
                [["import", cap] for cap in self.captures],
                self._instructions,
            ])
            return MapVariableHook(self.parent, len(self.parent._instructions))

        # Root map: dispatch to the subject hook.
        return self.subject.map(self.path, self.captures, self._instructions)

    def push_call(
        self,
        hook: StubHook,
        path: list[str | int],
        params: RpcPayload,
    ) -> StubHook:
        """Record a call instruction; returns a variable for the result.

        Port of map.ts:77-86 including the arg-unwrap HACK: the devaluator
        escapes the args array as ``[[...]]``; instruction args must be
        UN-escaped (matching sendCall and the evaluator's re-wrap
        ``evaluate([args])``, serialize.ts:898-900).

        Args:
            hook: The hook being called
            path: Property path + method name
            params: Arguments payload

        Returns:
            A MapVariableHook for the call's result
        """
        from capnweb.serializer import Serializer

        _reject_new_targets(params.value)
        # Devaluate the RAW args (no deep copy — see make_output).
        serializer = Serializer(exporter=self)
        devalued = serializer.serialize(params.value)
        # HACK (map.ts:79-81): args are an array, so the devaluator wrapped
        # them in a second array. Unwrap.
        devalued = devalued[0]

        subject = self.capture(hook.dup())
        self._instructions.append(["pipeline", subject, list(path), devalued])
        return MapVariableHook(self, len(self._instructions))

    def push_get(self, hook: StubHook, path: list[str | int]) -> StubHook:
        """Record a property-get instruction (map.ts:88-92).

        Args:
            hook: The hook being read
            path: Property path

        Returns:
            A MapVariableHook for the property's value
        """
        subject = self.capture(hook.dup())
        self._instructions.append(["pipeline", subject, list(path)])
        return MapVariableHook(self, len(self._instructions))

    def capture(self, hook: StubHook) -> int:
        """Capture a hook into the mapper's index space (map.ts:94-115).

        Own placeholders return their variable index; external hooks are
        deduplicated and assigned negative capture indices (-1, -2, ...);
        nested builders capture through their parent.

        Args:
            hook: The hook to capture

        Returns:
            The mapper-space index for the hook
        """
        if isinstance(hook, MapVariableHook) and hook.mapper is self:
            # Already one of our own variables.
            return hook.idx

        hook_id = id(hook)
        existing = self._capture_map.get(hook_id)
        if existing is not None:
            return existing

        if self.parent is not None:
            parent_idx = self.parent.capture(hook)
            self.captures.append(parent_idx)
        else:
            self.captures.append(hook)

        result = -len(self.captures)
        self._capture_map[hook_id] = result
        return result

    # -----------------------------------------------------------------------
    # C-EXPORTER protocol (recorder semantics, map.ts:117-155)
    # -----------------------------------------------------------------------

    def export_capability(self, stub: Any) -> int:
        """Reject NEW RpcTargets/callbacks constructed inside the mapper.

        Existing stubs never reach here — the serializer probes
        ``get_import`` first, which captures them (map.ts:120-136). Only a
        genuinely-new target (wrapped into a fresh stub during payload deep
        copy) falls through to this error.
        """
        raise RpcError(
            "Error",
            "Can't construct an RpcTarget or RPC callback inside a mapper "
            "function. Try creating a new RpcStub outside the callback "
            "first, then using it inside the callback.",
        )

    def export_promise(self, stub: Any) -> int:
        """Same restriction as export_capability (map.ts:137-139)."""
        return self.export_capability(stub)

    def get_import(self, hook: StubHook) -> int | None:
        """Every hook devaluated inside a mapper is a capture (map.ts:140-142).

        Non-placeholder hooks are captured as a dup(): the devaluated stub
        stays owned by the application, while the captures list transfers
        ownership to ``sendMap`` (which exports or releases them).
        """
        if isinstance(hook, MapVariableHook):
            return self.capture(hook)
        return self.capture(hook.dup())

    def unexport(self, ids: list[Any]) -> None:
        """No-op: a failed recording is cooked anyway (map.ts:144-146)."""

    def create_pipe(self, readable: Any, guard_hook: Any = None) -> int:
        """Streams cannot cross into mapper instructions (map.ts:148-150)."""
        raise RpcError(
            "Error", "Cannot send ReadableStream inside a mapper function."
        )

    def on_send_error(self, error: Exception) -> Exception | None:
        """No error rewriting inside a recording (map.ts:152-154)."""
        return None


def send_map(
    subject: StubHook,
    path: list[str | int],
    func: Callable[[Any], Any],
) -> "RpcPromise":
    """Record a mapper callback and dispatch the map (mapImpl.sendMap,
    map.ts:157-179).

    Runs ``func`` exactly once, synchronously, under the call interceptor.
    Misuse (throwing callbacks, placeholder abuse, async callbacks) raises
    synchronously out of the ``.map()`` call site.

    Args:
        subject: The hook to map over
        path: Property path to the array (derived from the promise chain)
        func: The mapper callback

    Returns:
        An RpcPromise for the mapped result

    Raises:
        RpcError: If the callback is async or misuses placeholders.
    """
    from capnweb.stubs import RpcPromise, RpcStub, with_call_interceptor

    builder = MapBuilder(subject, path)
    try:
        result = with_call_interceptor(
            builder.push_call,
            lambda: func(RpcPromise(builder.make_input(), [])),
        )
    finally:
        builder.unregister()

    # Detect misuse: map callbacks cannot be async (map.ts:168-176). Handle
    # plain coroutines, async generators, and other awaitables — but NOT
    # RpcPromise/RpcStub, which are awaitable placeholders and the normal
    # currency of a recording (TS checks `instanceof Promise`, which an
    # RpcPromise is not).
    if asyncio.iscoroutine(result):
        result.close()  # squelch the never-awaited warning
        raise RpcError("Error", "RPC map() callbacks cannot be async.")
    if inspect.isasyncgen(result):
        raise RpcError("Error", "RPC map() callbacks cannot be async.")
    if not isinstance(result, (RpcPromise, RpcStub)) and inspect.isawaitable(result):
        raise RpcError("Error", "RPC map() callbacks cannot be async.")

    result_payload = RpcPayload.from_app_return(result)
    return RpcPromise(builder.make_output(result_payload), [])


def build_map(
    subject: StubHook,
    path: list[str | int],
    func: Callable[[Any], Any],
) -> StubHook:
    """Like :func:`send_map` but returns the raw result StubHook."""
    promise = send_map(subject, path, func)
    return promise._raw_hook
