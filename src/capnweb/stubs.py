"""User-facing RPC stub and promise classes.

These classes provide the Pythonic interface to RPC capabilities. They are
thin wrappers around StubHook instances and use Python's magic methods to
provide a natural, Proxy-like API.

Two TS-parity mechanisms live here (parity stream B2):

* **Call interception** (core.ts:326-341): every stub/promise invocation
  routes through the module-level ``_do_call`` indirection, swappable within
  a synchronous scope via ``with_call_interceptor``. The map recorder uses
  this to compile calls made inside ``.map()`` callbacks into instructions.
* **Lazy path accumulation** (core.ts:358-380 proxy ``get`` trap): property
  access on a stub/promise does NOT touch the hook — it returns a new
  ``RpcPromise`` carrying ``(hook, path)``. ``hook.get(path)`` happens only
  on await / dup / devaluation, so ``x.a.b(args)`` compiles to a single
  fused ``["pipeline", id, ["a","b"], args]`` exactly like TS.
"""

from __future__ import annotations

import contextvars
from typing import TYPE_CHECKING, Any, Callable, Self

from capnweb.payload import RpcPayload

if TYPE_CHECKING:
    from capnweb.hooks import StubHook
    from capnweb.rpc_session import BidirectionalSession


# ---------------------------------------------------------------------------
# Call interception (core.ts:326-341)
# ---------------------------------------------------------------------------

# interceptor(hook, path, params) -> StubHook
CallInterceptor = Callable[["StubHook", list, RpcPayload], "StubHook"]

_call_interceptor: contextvars.ContextVar[CallInterceptor | None] = (
    contextvars.ContextVar("capnweb_call_interceptor", default=None)
)


def _do_call(hook: "StubHook", path: list, params: RpcPayload) -> "StubHook":
    """Dispatch a stub call, honoring any installed interceptor.

    Mirror of the TS module-global ``doCall`` (core.ts:329-332). ALL calls
    made through :class:`RpcStub`/:class:`RpcPromise` route through here so
    the map recorder can intercept them.
    """
    interceptor = _call_interceptor.get()
    if interceptor is None:
        return hook.call(path, params)
    return interceptor(hook, path, params)


def with_call_interceptor(interceptor: CallInterceptor, callback: Callable[[], Any]) -> Any:
    """Run ``callback`` with all stub calls routed through ``interceptor``.

    Synchronous-scoped like TS ``withCallInterceptor`` (core.ts:334-341);
    implemented with a contextvar for asyncio safety (recording itself is
    strictly synchronous — mapper callbacks cannot await).
    """
    token = _call_interceptor.set(interceptor)
    try:
        return callback()
    finally:
        _call_interceptor.reset(token)


# ---------------------------------------------------------------------------
# Delivery-time promise substitution (TS RpcPayload.deliverResolve/deliverTo)
# ---------------------------------------------------------------------------


async def _pull_and_deliver_value(hook: "StubHook") -> Any:
    """Pull a hook and return its value with all embedded promises resolved."""
    payload = await hook.pull()
    await deliver_payload_in_place(payload)
    return payload.value


async def deliver_payload_in_place(payload: RpcPayload) -> None:
    """Resolve every promise embedded in ``payload`` and splice the values in.

    Python port of the TS delivery machinery (core.ts:1122-1163
    ``deliverTo``/``deliverRpcPromiseTo``): before a payload reaches
    application code, all embedded ``RpcPromise``s must be replaced by their
    resolutions — this is what makes pipelined references inside values work.

    Like TS, the promises stay on ``payload.promises`` for disposal: each
    promise still owns its resolution payload, which owns the stubs the
    spliced value contains.
    """
    if payload.delivered:
        return
    payload.delivered = True

    # Delivery-blocking substitutions first (Blob collection, remap results).
    await payload.substitute_promises()

    for parent, key, promise in list(payload.promises):
        # _hook materializes property promises into real hooks (TS deepCopy
        # guarantees the same before delivery).
        value = await _pull_and_deliver_value(promise._hook)
        _splice_value(payload, parent, key, value)


def _splice_value(payload: RpcPayload, parent: Any, key: Any, value: Any) -> None:
    """Replace a promise placeholder at (parent, key) with its resolution.

    Root promises are always tracked with their owning RpcPayload as parent
    (from_array/deep_copy_from/ensure_deep_copied all agree), so there is no
    None-parent case.
    """
    if parent is payload or isinstance(parent, RpcPayload):
        parent.value = value
    elif isinstance(parent, list):
        if isinstance(key, int) and 0 <= key < len(parent):
            parent[key] = value
    elif isinstance(parent, dict):
        parent[key] = value


def _coerce_to_hook(value: Any) -> "StubHook":
    """Coerce an RpcStub constructor argument into a StubHook.

    Mirrors TS ``new RpcStub(value)`` (core.ts:451-476): a StubHook passes
    through; an RpcTarget or callable becomes a TargetStubHook; any other
    value is adopted with "return" semantics into a PayloadStubHook.
    """
    from capnweb.hooks import PayloadStubHook, StubHook, TargetStubHook
    from capnweb.types import RpcTarget as RpcTargetType

    if isinstance(value, StubHook):
        return value
    if isinstance(value, (RpcStub, RpcPromise)):
        # Adopt the other stub's/promise's hook with a fresh reference.
        return value._hook.dup()
    if isinstance(value, RpcTargetType) or callable(value):
        return TargetStubHook(value)
    return PayloadStubHook(RpcPayload.from_app_return(value))


class RpcStub:
    """A reference to an RPC capability (stub).

    This class wraps a StubHook and provides a Pythonic interface using
    magic methods. It acts like a Proxy in TypeScript - property access
    and method calls are delegated to the hook.

    Example:
        ```python
        # Get a property - returns a promise
        user_id = stub.user.id

        # Call a method - returns a promise
        result = stub.calculate(5, 3)

        # Await the promise
        value = await result
        ```
    """
    __slots__ = ('_hook',)

    def __init__(self, hook: StubHook | Any) -> None:
        """Initialize with a hook, an RpcTarget/callable, or a plain value.

        Like TS ``new RpcStub(value)``: applications may construct a stub
        explicitly without an RPC connection by passing an ``RpcTarget``, a
        callable, or any serializable value (core.ts:451-476). Internal code
        passes a ``StubHook`` directly.

        Args:
            hook: The StubHook backing this stub, or a local value to wrap
        """
        # Use object.__setattr__ to avoid triggering __setattr__
        object.__setattr__(self, "_hook", _coerce_to_hook(hook))

    def __getattr__(self, name: str) -> RpcPromise:
        """Access a property, returning a promise for the value.

        LAZY (core.ts:358-361): no hook call happens here — the returned
        promise carries ``(hook, [name])`` and only resolves the path on
        await / dup / serialization.

        Args:
            name: The property name

        Returns:
            An RpcPromise for the property value
        """
        if name.startswith("_"):
            # Avoid infinite recursion for private attrs
            msg = f"'{type(self).__name__}' object has no attribute '{name}'"
            raise AttributeError(msg)

        return RpcPromise(self._hook, [name], _borrowed=True)

    def __call__(self, *args: Any, **kwargs: Any) -> RpcPromise:
        """Call the stub as a function.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments (not yet supported)

        Returns:
            An RpcPromise that will resolve to the call result
        """
        if kwargs:
            msg = "Keyword arguments not yet supported in RPC calls"
            raise NotImplementedError(msg)

        # Package arguments into a payload
        args_payload = RpcPayload.from_app_params(list(args))

        # Call through the (interceptable) doCall indirection, SYNCHRONOUSLY
        # (empty path = call the stub itself), like the TS proxy apply trap.
        result_hook = _do_call(self._hook, [], args_payload)

        return RpcPromise(result_hook)

    def dispose(self) -> None:
        """Dispose this stub, releasing resources.

        After calling dispose, the stub should not be used anymore.
        (Python spelling of TS ``stub[Symbol.dispose]()``.)
        """
        self._hook.dispose()

    def dup(self) -> "RpcStub":
        """Duplicate this stub (core.ts:491-507, README:378-380).

        The underlying target is disposed only when ALL duplicates (and the
        original) have been disposed — hooks refcount internally. Use this to
        keep a capability alive past the disposal of the stub it arrived on.

        Returns:
            A new, independently-disposable RpcStub for the same capability.
        """
        return RpcStub(self._hook.dup())

    def on_rpc_broken(self, callback: Callable[[Exception], None]) -> None:
        """Register a callback for when the backing connection is broken.

        Python spelling of TS ``stub.onRpcBroken(cb)`` (types.d.ts:57). The
        callback fires (with the abort reason) when the session backing this
        capability dies. For local (non-RPC) stubs this never fires.
        """
        self._hook.on_broken(callback)

    def map(self, func: Callable[["RpcPromise"], Any]) -> "RpcPromise":
        """Apply a mapper function to array elements without transferring data.

        TS signature (core.ts:511-514): ONE argument — the callback. The
        callback runs exactly once, synchronously, against a recording
        placeholder; the calls it makes are compiled into instructions and
        executed remotely per element.

        Args:
            func: A function taking an RpcPromise placeholder and returning
                the mapped value. It cannot be async and must be side-effect
                free.

        Returns:
            An RpcPromise that will resolve to the mapped array.

        Example:
            ```python
            result = await stub.getData().map(lambda x: x.double())
            ```
        """
        from capnweb.map_builder import send_map

        return send_map(self._hook, [], func)

    def __enter__(self) -> Self:
        """Enter sync context manager (Python spelling of TS ``using``)."""
        return self

    def __exit__(self, *args: object) -> None:
        """Exit sync context manager, disposing the stub."""
        self.dispose()

    async def __aenter__(self) -> Self:
        """Enter async context manager."""
        return self

    async def __aexit__(self, *args: object) -> None:
        """Exit async context manager, disposing the stub."""
        self.dispose()

    def __repr__(self) -> str:
        """Return a readable representation."""
        return f"RpcStub({self._hook!r})"


class RpcPromise:
    """A promise for an RPC value.

    Like TS ``RpcPromise`` this carries ``(hook, pathIfPromise)``: property
    access extends the path WITHOUT touching the hook (lazy accumulation,
    core.ts:373-380); the ``hook.get(path)`` happens only when the promise
    is awaited, duplicated, or serialized. This is what fuses ``x.a.b(...)``
    into a single pipeline instruction.

    Example:
        ```python
        # Chain operations before awaiting
        promise = stub.user.profile.getName()

        # Await to get the final value
        name = await promise

        # Or use as async context manager
        async with stub.user.profile.getName() as name:
            print(name)
        ```
    """
    __slots__ = ('_raw_hook', '_path', '_hook_cache', '_borrowed_hook')

    def __init__(
        self,
        hook: StubHook,
        path: list[str | int] | None = None,
        *,
        _borrowed: bool = False,
    ) -> None:
        """Initialize with a hook and an optional pending property path.

        Args:
            hook: The StubHook backing this promise
            path: Lazily accumulated property path (``pathIfPromise``).
                Empty/None = root promise (owns the hook); non-empty =
                property promise (borrows the parent's hook, like TS
                property promises which have no disposer).
            _borrowed: Internal — hook is borrowed from a parent stub.
        """
        object.__setattr__(self, "_raw_hook", hook)
        object.__setattr__(self, "_path", list(path) if path else [])
        object.__setattr__(self, "_hook_cache", None)
        object.__setattr__(self, "_borrowed_hook", _borrowed)

    # -- hook materialization ------------------------------------------------

    @property
    def _hook(self) -> "StubHook":
        """The hook for this promise's VALUE.

        Root promises return the underlying hook directly. Property promises
        materialize ``hook.get(path)`` on first access and cache it — the
        Python analog of TS resolving ``pathIfPromise`` at use time
        (pullPromise, core.ts:619-630). Consumers that need the raw
        (hook, path) pair — the serializer, the recorder — read
        ``_raw_hook``/``_path`` instead and never trigger materialization.
        """
        cache = self._hook_cache
        if cache is not None:
            return cache
        if self._path:
            cache = self._raw_hook.get(list(self._path))
            object.__setattr__(self, "_hook_cache", cache)
            return cache
        return self._raw_hook

    @_hook.setter
    def _hook(self, value: Any) -> None:
        # RpcPayload's take-ownership deep copy does ``promise._hook = None``
        # after extracting the hook; support that by clearing all state.
        object.__setattr__(self, "_raw_hook", value)
        object.__setattr__(self, "_path", [])
        object.__setattr__(self, "_hook_cache", None)
        object.__setattr__(self, "_borrowed_hook", False)

    def __getattr__(self, name: str) -> RpcPromise:
        """Access a property on the promised value, returning a new promise.

        This enables chaining: ``promise.user.id``. LAZY: extends the path
        without any hook call (core.ts:373-380).

        Args:
            name: The property name

        Returns:
            A new RpcPromise for the property
        """
        if name.startswith("_"):
            msg = f"'{type(self).__name__}' object has no attribute '{name}'"
            raise AttributeError(msg)

        return RpcPromise(
            self._raw_hook, [*self._path, name], _borrowed=True
        )

    def __call__(self, *args: Any, **kwargs: Any) -> RpcPromise:
        """Call the promised value as a function, returning a new promise.

        This enables chaining: ``promise.getUser(123).getName()``. The
        accumulated path is passed WHOLE to a single call (TS proxy apply:
        ``doCall(hook, pathIfPromise || [], args)``), producing one fused
        pipeline instruction.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments (not yet supported)

        Returns:
            A new RpcPromise for the call result
        """
        if kwargs:
            msg = "Keyword arguments not yet supported in RPC calls"
            raise NotImplementedError(msg)

        args_payload = RpcPayload.from_app_params(list(args))

        # Route through the (interceptable) doCall indirection, synchronously.
        result_hook = _do_call(self._raw_hook, list(self._path), args_payload)

        return RpcPromise(result_hook)

    def __await__(self):
        """Make this promise awaitable.

        Materializes the accumulated path (one ``hook.get(path)``), pulls,
        and — like TS ``deliverResolve`` — substitutes every promise embedded
        in the payload before handing the value to the application.
        """

        async def resolve():
            payload = await self._hook.pull()
            await deliver_payload_in_place(payload)
            return payload.value

        return resolve().__await__()

    def dispose(self) -> None:
        """Dispose this promise, canceling it if pending.

        Root promises dispose their hook. Property promises have no disposer
        in TS (the proxy hides ``Symbol.dispose``); Python goes one better
        and disposes the transiently materialized ``get(path)`` hook if one
        was created, leaving the parent's hook untouched.
        """
        cache = self._hook_cache
        if cache is not None:
            object.__setattr__(self, "_hook_cache", None)
            cache.dispose()
        if not self._path and not self._borrowed_hook:
            raw = self._raw_hook
            if raw is not None:
                raw.dispose()

    def dup(self) -> "RpcStub":
        """Duplicate this promise as a stub (core.ts:491-507).

        Like TS, ``dup()`` on a promise "stub-ifies" it: the result is an
        immediately usable ``RpcStub``. For property promises the pending
        path is resolved into the new hook (``hook.get(path)``,
        core.ts:495-499). The capability stays alive until all duplicates
        are disposed.
        """
        if self._path:
            return RpcStub(self._raw_hook.get(list(self._path)))
        return RpcStub(self._raw_hook.dup())

    def on_rpc_broken(self, callback: Callable[[Exception], None]) -> None:
        """Register a callback for when the backing connection is broken.

        Python spelling of TS ``promise.onRpcBroken(cb)`` — registered on the
        underlying hook (core.ts:509-511). If the promise rejects (e.g.
        because the session aborted), the callback receives the error.
        """
        self._raw_hook.on_broken(callback)

    def map(self, func: Callable[["RpcPromise"], Any]) -> "RpcPromise":
        """Apply a mapper function to array elements without transferring data.

        TS signature (core.ts:511-514): ONE argument. The property path is
        derived from the promise chain itself (``stub.data.map(f)`` maps
        over the ``data`` property — no explicit path parameter).

        Args:
            func: A function taking an RpcPromise placeholder and returning
                the mapped value. Cannot be async; must be side-effect free.

        Returns:
            An RpcPromise that will resolve to the mapped array.

        Example:
            ```python
            values = await stub.data.map(lambda x: x.transform())
            ```
        """
        from capnweb.map_builder import send_map

        return send_map(self._raw_hook, list(self._path), func)

    async def __aenter__(self) -> Any:
        """Enter async context manager, awaiting the value."""
        return await self

    async def __aexit__(self, *args: object) -> None:
        """Exit async context manager, disposing the promise."""
        self.dispose()

    def __repr__(self) -> str:
        """Return a readable representation."""
        return f"RpcPromise({self._raw_hook!r}, path={self._path!r})"


def get_remote_main(session: "BidirectionalSession") -> RpcStub:
    """Get the peer's main capability as an ``RpcStub``.

    Public analog of TS ``RpcSession.getRemoteMain()`` (rpc.ts:1089-1105).
    Prefer this over ``session.get_main_stub()`` (which returns a raw hook).
    Disposing the returned stub shuts the session down, like TS
    (rpc.ts:506-510).
    """
    native = getattr(session, "get_remote_main", None)
    if native is not None:
        result = native()
        if isinstance(result, RpcStub):
            return result
    return RpcStub(session.get_main_stub())


def create_stub(target: "RpcTarget") -> RpcStub:
    """Create an RpcStub from an RpcTarget.

    This is the public API for creating stubs from local capabilities.
    Use this when you need to pass a local object as a callback or
    capability to a remote peer.

    Args:
        target: An RpcTarget implementation

    Returns:
        An RpcStub wrapping the target

    Example:
        ```python
        class MyCallback(RpcTarget):
            async def call(self, method: str, args: list) -> Any:
                if method == "onMessage":
                    print(f"Received: {args[0]}")
                    return None
                raise RpcError.not_found(f"Method '{method}' not found")

        # Create a stub to pass to the server
        callback_stub = create_stub(MyCallback())
        await server.join("alice", callback_stub)
        ```
    """
    from capnweb.types import RpcTarget as RpcTargetType

    if not isinstance(target, RpcTargetType):
        raise TypeError(f"Expected RpcTarget, got {type(target).__name__}")

    return RpcStub(target)
