"""StubHook hierarchy for decentralized RPC capability management.

This module implements the hook pattern from the TypeScript reference implementation.
Each StubHook represents the backing implementation of an RPC-able reference.

Instead of a monolithic evaluator, different hook types handle different scenarios:
- ErrorStubHook: Holds an error
- PayloadStubHook: Wraps locally-resolved data
- TargetStubHook: Wraps a local RpcTarget object
- PromiseStubHook: Wraps a future that will resolve to another hook

Note: Remote capability handling (ImportHook) is in rpc_session.py.
"""

from __future__ import annotations

import asyncio
import inspect
from abc import ABC, abstractmethod
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

from capnweb.error import RpcError
from capnweb.payload import RpcPayload

if TYPE_CHECKING:
    from capnweb.types import RpcTarget


async def invoke_callable(target: Any, args: list[Any]) -> Any:
    """Invoke a callable (sync or async) with arguments.

    Args:
        target: The callable to invoke
        args: Arguments to pass (as a list)

    Returns:
        The result of the call
    """
    if inspect.iscoroutinefunction(target):
        return await target(*args)
    return target(*args)


@dataclass
class FollowPathResult:
    """Result of :func:`follow_path` (TS FollowPathResult, core.ts:1478-1485).

    Exactly one of two shapes:
    * ``hook`` is set: the walk hit an embedded stub/promise; the caller must
      delegate the operation to ``hook`` with ``remaining_path``.
    * ``hook`` is None: the walk ended on a plain ``value`` (with its
      ``parent`` container and owning payload, when known).
    """

    hook: "StubHook | None" = None
    remaining_path: list[Any] | None = None
    value: Any = None
    parent: Any = None
    owner: Any = None


def follow_path(
    value: Any, parent: Any, path: list[str | int], owner: Any
) -> FollowPathResult:
    """Walk a property path through a plain value tree (core.ts:1487-1560).

    Delegates to embedded stub/promise hooks MID-PATH: if a path element
    lands on an ``RpcStub``/``RpcPromise``, the walk stops and returns the
    underlying hook plus the not-yet-consumed remainder of the path (with a
    promise's own pending path prepended, like TS ``pathIfPromise.concat``).

    Security: dangerous keys (Python dunders + JS Object.prototype names)
    resolve to None instead of being looked up, mirroring the TS
    ``part in Object.prototype`` guard.
    """
    from capnweb.parser import DANGEROUS_KEYS
    from capnweb.stubs import RpcPromise, RpcStub

    for i, part in enumerate(path):
        # Delegate to an embedded capability before consuming `part`.
        if isinstance(value, RpcStub):
            return FollowPathResult(
                hook=value._hook, remaining_path=list(path[i:])
            )
        if isinstance(value, RpcPromise):
            return FollowPathResult(
                hook=value._raw_hook,
                remaining_path=[*value._path, *path[i:]],
            )

        parent = value
        if isinstance(part, str) and part in DANGEROUS_KEYS:
            # Don't allow probing dangerous properties over RPC
            # (core.ts:1493-1502).
            value = None
        elif isinstance(part, int) and not isinstance(part, bool):
            value = value[part]  # sequence index (IndexError/TypeError raise)
        elif isinstance(value, dict):
            value = value[part]  # KeyError raises
        else:
            value = getattr(value, part)

    # Path fully consumed. A final stub/promise is returned as a VALUE (not
    # delegated): TS's followPath loop also only delegates mid-path — a
    # trailing stub is deep-copied by get(), applied to by map(), and made
    # callable by the proxy for call() (each operation decides).
    return FollowPathResult(value=value, parent=parent, owner=owner)


class StubHook(ABC):
    """Abstract base class for all stub hook implementations.

    A StubHook represents the backing implementation of an RPC capability.
    It knows how to handle calls, property access, promise resolution, etc.

    This is the core of the decentralized architecture - each hook type
    implements these methods according to its specific semantics.
    """

    @abstractmethod
    def call(self, path: list[str | int], args: RpcPayload) -> "StubHook":
        """Call a method through this hook (synchronous).

        This method is synchronous to ensure messages are queued before
        batch transports send their requests. This matches TypeScript's
        StubHook.call() behavior.

        Args:
            path: Property path to navigate before calling (e.g., ["user", "profile", "getName"])
            args: Arguments wrapped in RpcPayload

        Returns:
            A new StubHook representing the result
        """
        ...

    @abstractmethod
    def map(
        self,
        path: list[str | int],
        captures: list["StubHook"],
        instructions: list[Any],
    ) -> "StubHook":
        """Apply a map operation.

        This allows applying a function to array elements remotely without
        transferring data back and forth.

        Args:
            path: Property path to the array to map over
            captures: External stubs used in the mapper function
            instructions: JSON-serializable instructions describing the mapper

        Returns:
            A new StubHook representing the mapped result
        """
        ...

    @abstractmethod
    def get(self, path: list[str | int]) -> "StubHook":
        """Get a property through this hook.

        Args:
            path: Property path to navigate (e.g., ["user", "id"])

        Returns:
            A new StubHook representing the property value
        """
        ...

    @abstractmethod
    async def pull(self) -> RpcPayload:
        """Pull the final value from this hook.

        This is what happens when you await a promise. It resolves the
        value (possibly waiting for network I/O) and returns the payload.

        Returns:
            The resolved payload

        Raises:
            RpcError: If the capability is in an error state
        """
        ...

    @abstractmethod
    def ignore_unhandled_rejections(self) -> None:
        """Prevent this stub from generating unhandled rejection events.

        Called to prevent spurious rejection errors when a promise throws
        before the client gets a chance to pull it or use it in a pipeline.
        """
        ...

    @abstractmethod
    def dispose(self) -> None:
        """Dispose this hook, releasing any resources.

        This decrements reference counts, sends release messages for remote
        capabilities, and cleans up state.
        """
        ...

    @abstractmethod
    def dup(self) -> Self:
        """Duplicate this hook (increment reference count).

        This is used when copying payloads to ensure proper refcounting.

        Returns:
            A new StubHook sharing the same underlying resource
        """
        ...

    def on_broken(self, callback: Any) -> None:
        """Register callback for when connection breaks.

        Default implementation does nothing. Override in subclasses that
        represent remote capabilities.
        """
        pass

    def stream(
        self, path: list[str | int], args: RpcPayload
    ) -> tuple[Any, int | None]:
        """Dispatch a streaming call (C-STREAM; core.ts:216-231 default).

        Default: delegate to ``call()`` + ``pull()`` and return
        ``size=None``, which tells the caller this is a local call and
        writes must be serialized (awaited one at a time). Hooks backed by
        the wire (ImportHook, WritableStreamHook) override this to send a
        ``stream`` message and report the frame size for flow control.

        Returns:
            (awaitable completing when the write is delivered, size or None)
        """
        result_hook = self.call(path, args)

        async def run() -> None:
            payload = await result_hook.pull()
            payload.dispose()

        return run(), None


@dataclass
class ErrorStubHook(StubHook):
    """A hook that holds an error.

    All operations on this hook either return itself or raise the error.
    This is useful for representing failed promises or broken capabilities.
    The original error is preserved verbatim (never re-wrapped), so wire
    serialization stays faithful to what actually failed.
    """
    __slots__ = ('error',)
    error: Exception

    def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Always returns self (errors propagate through chains)."""
        return self

    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        """Always returns self (errors propagate through chains)."""
        # Dispose captures since we're not using them
        for cap in captures:
            cap.dispose()
        return self

    def get(self, path: list[str | int]) -> StubHook:
        """Always returns self (errors propagate through chains)."""
        return self

    async def pull(self) -> RpcPayload:
        """Raises the error."""
        raise self.error

    def ignore_unhandled_rejections(self) -> None:
        """Nothing to do for errors."""
        pass

    def dispose(self) -> None:
        """Nothing to dispose for errors."""

    def dup(self) -> Self:
        """Errors can be freely shared."""
        return self

    def on_broken(self, callback: Any) -> None:
        """Call the callback immediately with the error."""
        try:
            callback(self.error)
        except Exception:
            pass  # Treat as unhandled rejection


class PayloadStubHook(StubHook):
    """A hook that wraps locally-resolved data.

    This represents a capability that has already been resolved to a local
    value. Method calls and property access navigate through the payload's
    object tree.
    """
    __slots__ = ('payload',)

    def __init__(self, payload: RpcPayload) -> None:
        """Initialize with a payload.

        Args:
            payload: The payload this hook wraps
        """
        self.payload = payload
        # Ensure payload is owned before use
        self.payload.ensure_deep_copied()

    def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Navigate the path and call as a function (synchronous dispatch).

        Port of TS ``ValueStubHook.call`` (core.ts:1628-1648): the walk
        delegates to embedded stub hooks mid-path; a local function is
        invoked via deliverCall semantics — any promises embedded in the
        args are resolved and substituted before the function runs.

        Args:
            path: Property path to navigate
            args: Arguments to pass to the function

        Returns:
            A new hook with the result
        """
        from capnweb.stubs import RpcPromise, RpcStub

        try:
            result = follow_path(self.payload.value, None, path, self.payload)
        except Exception as e:
            return ErrorStubHook(self._navigation_error(path, e))

        if result.hook is not None:
            return result.hook.call(result.remaining_path or [], args)

        target = result.value
        # A trailing stub/promise value: the TS proxy makes stubs callable
        # (apply trap -> doCall); Python delegates through the hook.
        if isinstance(target, RpcStub):
            return target._hook.call([], args)
        if isinstance(target, RpcPromise):
            return target._raw_hook.call(list(target._path), args)

        if not callable(target):
            joined = ".".join(str(p) for p in path)
            error = RpcError.bad_request(f"'{joined}' is not a function.")
            return ErrorStubHook(error)

        args.ensure_deep_copied()

        if args.promises or args.substitutions or inspect.iscoroutinefunction(target):
            # deliverCall (core.ts:1173-1205): promises in params must be
            # resolved and substituted before the function is invoked.
            async def call_async():
                try:
                    from capnweb.stubs import deliver_payload_in_place

                    await deliver_payload_in_place(args)
                    result = await invoke_callable(
                        target,
                        args.value if isinstance(args.value, list) else [args.value],
                    )
                    return PayloadStubHook(RpcPayload.owned(result))
                except Exception as e:
                    if isinstance(e, RpcError):
                        return ErrorStubHook(e)  # explicit signal — preserve
                    error = RpcError.wrap_internal(f"Call failed: {e}")
                    return ErrorStubHook(error)

            future: asyncio.Future[StubHook] = asyncio.ensure_future(call_async())
            return PromiseStubHook(future)

        # Synchronous callable with no embedded promises: invoke immediately
        # (e-order; TS deliverCall's synchronous fast path).
        try:
            result = (
                target(*args.value)
                if isinstance(args.value, list)
                else target(args.value)
            )
            return PayloadStubHook(RpcPayload.owned(result))
        except Exception as e:
            if isinstance(e, RpcError):
                return ErrorStubHook(e)  # explicit signal — preserve message
            error = RpcError.wrap_internal(f"Call failed: {e}")
            return ErrorStubHook(error)

    def get(self, path: list[str | int]) -> StubHook:
        """Navigate the path and return the property.

        Port of TS ``ValueStubHook.get`` (core.ts:1675-1706): delegates to
        embedded stub hooks mid-path; otherwise deep-copies the reached
        value (dup()ing embedded stubs) so the returned hook owns its copy.

        Args:
            path: Property path to navigate

        Returns:
            A new hook with the property value
        """
        try:
            result = follow_path(self.payload.value, None, path, self.payload)
            if result.hook is not None:
                return result.hook.get(result.remaining_path or [])
            return PayloadStubHook(
                RpcPayload.deep_copy_from(result.value, result.parent, result.owner)
            )
        except Exception as e:
            return ErrorStubHook(self._navigation_error(path, e))

    @staticmethod
    def _navigation_error(path: list[str | int], exc: Exception) -> Exception:
        """Adapt a navigation failure, preserving deliberate errors."""
        if isinstance(exc, RpcError):
            return exc
        if isinstance(exc, (KeyError, IndexError, AttributeError, TypeError)):
            return RpcError.not_found(f"Property {path} not found: {exc}")
        return exc

    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        """Apply a map operation locally.

        Port of TS ``ValueStubHook.map`` (core.ts:1650-1673): follows the
        path (delegating to embedded stub hooks mid-path, with capture
        disposal on navigation failure) and applies the mapper locally.
        Errors are preserved verbatim in the ErrorStubHook — never re-wrapped.
        """
        try:
            try:
                result = follow_path(self.payload.value, None, path, self.payload)
            except Exception:
                # We took ownership of the captures; dispose them
                # (core.ts:1652-1661).
                for cap in captures:
                    cap.dispose()
                raise

            if result.hook is not None:
                return result.hook.map(
                    result.remaining_path or [], captures, instructions
                )

            from capnweb.map_applicator import apply_map_locally
            return apply_map_locally(
                result.value, result.parent, result.owner, captures, instructions
            )
        except Exception as e:
            return ErrorStubHook(e)

    async def pull(self) -> RpcPayload:
        """Return the payload directly (already resolved)."""
        return self.payload

    def ignore_unhandled_rejections(self) -> None:
        """Nothing to do for already-resolved payloads."""
        pass

    def dispose(self) -> None:
        """Dispose the payload."""
        self.payload.dispose()

    def dup(self) -> "PayloadStubHook":
        """Duplicate by deep-copying the payload (core.ts:1739-1750).

        Each duplicate owns its own payload copy (with all embedded stubs
        dup()ed), so disposing one duplicate never double-disposes stubs
        reachable from the other.
        """
        return PayloadStubHook(
            RpcPayload.deep_copy_from(self.payload.value, None, self.payload)
        )

    def on_broken(self, callback: Any) -> None:
        """Forward to a single-stub payload's hook (core.ts:1772-1783).

        If the payload IS a single stub, onRpcBroken forwards to it;
        otherwise local payloads never break, so this is a no-op.
        """
        from capnweb.stubs import RpcStub
        if isinstance(self.payload.value, RpcStub):
            self.payload.value.on_rpc_broken(callback)


class TargetStubHook(StubHook):
    """A hook that wraps a local RpcTarget object.

    This represents a local capability provided by the application. It
    delegates method calls to the actual Python object.
    """
    __slots__ = ('target', 'ref_count')

    def __init__(self, target: "RpcTarget", ref_count: int = 1) -> None:
        self.target = target
        self.ref_count = ref_count  # For disposal tracking

    async def _navigate_to_target(self, property_path: list[str | int]) -> Any:
        """Navigate through properties to reach the target object.

        Args:
            property_path: List of properties to navigate

        Returns:
            The target object after navigation

        Raises:
            RpcError: If navigation fails
        """
        current_obj = self.target
        for prop in property_path:
            try:
                prop_value = await current_obj.get_property(str(prop))
                current_obj = prop_value
            except Exception as e:
                if isinstance(e, RpcError):
                    raise
                msg = f"Property navigation failed at path {property_path}: {e}"
                raise RpcError.not_found(msg) from e
        return current_obj

    async def _invoke_method(
        self, target: Any, method_name: str, args: RpcPayload
    ) -> Any:
        """Invoke a method on the target object.

        Args:
            target: The target object
            method_name: Name of the method to call
            args: Arguments for the method

        Returns:
            The method result

        Raises:
            RpcError: If the method call fails
        """
        # If target is an RpcTarget, use its call method
        if hasattr(target, "call") and callable(target.call):
            return await target.call(  # type: ignore[misc]
                method_name,
                args.value if isinstance(args.value, list) else [args.value],
            )

        # Empty method name = function call (invoke target directly if callable)
        if not method_name:
            if callable(target):
                if inspect.iscoroutinefunction(target):
                    return (
                        await target(*args.value)
                        if isinstance(args.value, list)
                        else await target(args.value)
                    )
                return (
                    target(*args.value) if isinstance(args.value, list) else target(args.value)
                )
            msg = "Target is not callable as a function"
            raise RpcError.bad_request(msg)

        # Otherwise, try to call the method directly on the object
        method = getattr(target, method_name)
        if not callable(method):
            msg = f"Method {method_name} is not callable"
            raise RpcError.bad_request(msg)

        # Handle async and sync methods
        if inspect.iscoroutinefunction(method):
            return (
                await method(*args.value)
                if isinstance(args.value, list)
                else await method(args.value)
            )

        return (
            method(*args.value) if isinstance(args.value, list) else method(args.value)
        )

    def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Call a method on the target (synchronous).

        Args:
            path: Property path (last element is method name)
            args: Arguments for the call

        Returns:
            A new hook with the result (may be a PromiseStubHook for async methods)
        """
        args.ensure_deep_copied()

        # Wrap the async call in a PromiseStubHook
        async def do_call():
            # deliverCall parity (core.ts:1173-1205): promises embedded in
            # the params are resolved and substituted before the target's
            # method runs — this is what makes pipelined references inside
            # call arguments work.
            if args.promises or args.substitutions:
                from capnweb.stubs import deliver_payload_in_place

                try:
                    await deliver_payload_in_place(args)
                except Exception as e:
                    if isinstance(e, RpcError):
                        return ErrorStubHook(e)
                    return ErrorStubHook(
                        RpcError.wrap_internal(f"Argument resolution failed: {e}")
                    )

            # Determine method name and target object
            if not path:
                # Empty path = function call (invoke target directly)
                method_name = ""
                current_target = self.target
            elif len(path) == 1:
                method_name = str(path[0])
                current_target = self.target
            else:
                property_path = path[:-1]
                method_name = str(path[-1])
                try:
                    current_target = await self._navigate_to_target(property_path)
                except RpcError as e:
                    return ErrorStubHook(e)

            # Invoke the method
            try:
                result = await self._invoke_method(current_target, method_name, args)
                return PayloadStubHook(RpcPayload.from_app_return(result))
            except RpcError as e:
                return ErrorStubHook(e)
            except Exception as e:
                # F6: unexpected app exception — flag as internal-origin so the
                # serializer redacts its free-text message (which may embed a
                # filesystem path/secret) before it reaches an untrusted peer.
                error = RpcError.wrap_internal(f"Target call failed: {e}")
                return ErrorStubHook(error)

        future: asyncio.Future[StubHook] = asyncio.ensure_future(do_call())
        return PromiseStubHook(future)

    async def _follow_from_target(
        self, path: list[str | int]
    ) -> "FollowPathResult":
        """Resolve a property path starting at the RpcTarget.

        The first hop goes through ``target.get_property`` (the Python
        RpcTarget protocol); nested RpcTargets keep using ``get_property``;
        plain values/containers and embedded stubs are walked by
        :func:`follow_path`. Descending into an RpcTarget clears the owner,
        like TS (core.ts:1536-1547).
        """
        from capnweb.types import RpcTarget as RpcTargetType

        value: Any = self.target
        for i, part in enumerate(path):
            if isinstance(value, RpcTargetType) and hasattr(value, "get_property"):
                value = await value.get_property(str(part))
                continue
            # Non-target value: hand the rest of the walk to follow_path.
            return follow_path(value, None, list(path[i:]), None)
        return follow_path(value, None, [], None)

    def get(self, path: list[str | int]) -> StubHook:
        """Get a property from the target.

        Supports full property paths (TS ValueStubHook.get + followPath):
        chained ``get_property`` for nested targets, plain-container hops,
        and mid-path delegation to embedded stubs.

        Args:
            path: Property path

        Returns:
            A new hook with the property value
        """
        if not path:
            # TS ValueStubHook.get (core.ts:1679-1685): a TargetStubHook
            # never backs a promise, so an empty-path get is a protocol
            # misuse.
            return ErrorStubHook(
                RpcError.bad_request("Can't dup an RpcTarget stub as a promise.")
            )

        async def get_property_async():
            try:
                result = await self._follow_from_target(path)
                if result.hook is not None:
                    return result.hook.get(result.remaining_path or [])
                return PayloadStubHook(RpcPayload.from_app_return(result.value))
            except Exception as e:
                if isinstance(e, RpcError):
                    return ErrorStubHook(e)
                error = RpcError.wrap_internal(f"Property access failed: {e}")
                return ErrorStubHook(error)

        # Return a promise hook that will resolve to the property
        future: asyncio.Future[StubHook] = asyncio.ensure_future(
            get_property_async()
        )
        return PromiseStubHook(future)

    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        """Map over a property of the target (TS ValueStubHook.map).

        ``stub_to_rpc_target.some_array_prop`` IS mappable: the path is
        followed into the target's value (delegating to embedded stub hooks
        mid-path) and the mapper is applied locally. Original errors are
        preserved — no re-wrap (core.ts:1650-1673).
        """
        async def do_map():
            try:
                try:
                    result = await self._follow_from_target(path)
                except Exception:
                    for cap in captures:
                        cap.dispose()
                    raise

                if result.hook is not None:
                    return result.hook.map(
                        result.remaining_path or [], captures, instructions
                    )

                from capnweb.map_applicator import apply_map_locally
                return apply_map_locally(
                    result.value, result.parent, result.owner,
                    captures, instructions,
                )
            except Exception as e:
                return ErrorStubHook(e)

        return PromiseStubHook(asyncio.ensure_future(do_map()))

    async def pull(self) -> RpcPayload:
        """Targets can't be pulled directly."""

        msg = "Cannot pull a target object"
        raise RpcError.bad_request(msg)

    def ignore_unhandled_rejections(self) -> None:
        """Nothing to do for targets."""
        pass

    def dispose(self) -> None:
        """Decrement reference count and notify target if disposable."""
        self.ref_count -= 1

        # Notify target when refcount reaches 0 if it implements disposal
        if (
            self.ref_count == 0
            and hasattr(self.target, "dispose")
            and callable(self.target.dispose)
        ):
            # Ignore disposal errors - best effort cleanup
            with suppress(Exception):
                self.target.dispose()

    def dup(self) -> Self:
        """Increment reference count."""
        self.ref_count += 1
        return self


@dataclass
class PromiseStubHook(StubHook):
    """A hook wrapping a future that will resolve to another hook.

    This represents a promise - a value that will be available in the future.
    Operations on this hook create chained promises.
    """
    __slots__ = ('future',)
    future: asyncio.Future[StubHook]

    def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Wait for the promise to resolve, then call on the result (synchronous).

        Args:
            path: Property path + method name
            args: Arguments

        Returns:
            A new PromiseStubHook for the chained result
        """

        async def chained_call():
            resolved_hook = await self.future
            # resolved_hook.call() is now synchronous, returns StubHook
            return resolved_hook.call(path, args)

        chained_future: asyncio.Future[StubHook] = asyncio.ensure_future(chained_call())
        return PromiseStubHook(chained_future)

    def get(self, path: list[str | int]) -> StubHook:
        """Wait for the promise to resolve, then get property on the result.

        Args:
            path: Property path

        Returns:
            A new PromiseStubHook for the chained result
        """

        async def chained_get():
            resolved_hook = await self.future
            return resolved_hook.get(path)

        chained_future: asyncio.Future[StubHook] = asyncio.ensure_future(chained_get())
        return PromiseStubHook(chained_future)

    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        """Wait for the promise to resolve, then map on the result."""

        async def chained_map():
            resolved_hook = await self.future
            return resolved_hook.map(path, captures, instructions)

        chained_future: asyncio.Future[StubHook] = asyncio.ensure_future(chained_map())
        return PromiseStubHook(chained_future)

    async def pull(self) -> RpcPayload:
        """Wait for the promise to resolve, then pull from the result.

        Returns:
            The final payload
        """
        resolved_hook = await self.future
        return await resolved_hook.pull()

    def ignore_unhandled_rejections(self) -> None:
        """Suppress unhandled rejection errors for this promise."""
        # Add an exception handler that does nothing
        def _ignore_exception(future: asyncio.Future) -> None:
            try:
                future.exception()
            except (asyncio.CancelledError, asyncio.InvalidStateError):
                pass

        if not self.future.done():
            self.future.add_done_callback(_ignore_exception)

    def dispose(self) -> None:
        """Dispose the resolution — WITHOUT canceling pending work.

        TS semantics (core.ts:1984-1994): disposing a promise hook defers
        the disposal to the eventual resolution. It must NOT cancel the
        in-flight work, because chained hooks (created via get/call/map
        before the dispose) share the same future — the mapper applicator
        relies on this when it disposes intermediate variables whose results
        still feed later instructions.
        """
        def _dispose_result(future: asyncio.Future) -> None:
            if future.cancelled():
                return
            if future.exception() is not None:
                return  # nothing to dispose
            future.result().dispose()

        if self.future.done():
            _dispose_result(self.future)
        else:
            self.future.add_done_callback(_dispose_result)

    def dup(self) -> "PromiseStubHook":
        """Duplicate: dup the resolved hook once available (core.ts:1952-1957).

        The duplicate holds its OWN reference: when the shared future
        resolves, the resulting hook is dup()ed for this duplicate, so each
        of the two hooks can be disposed independently without
        double-disposing the resolution.
        """
        async def dup_resolved() -> StubHook:
            resolved_hook = await asyncio.shield(self.future)
            return resolved_hook.dup()

        return PromiseStubHook(asyncio.ensure_future(dup_resolved()))

    def stream(
        self, path: list[str | int], args: RpcPayload
    ) -> tuple[Any, int | None]:
        """Await resolution, then re-dispatch the stream (core.ts:1924-1934).

        Args are deep-copied before parking; no size is reported — the safe
        default is serialized writes.
        """
        args.ensure_deep_copied()
        future = self.future

        async def run_after_resolve() -> None:
            resolved = await future
            awaitable, _size = resolved.stream(path, args)
            await awaitable

        return run_after_resolve(), None

    def on_broken(self, callback: Any) -> None:
        """Forward onBroken to the resolution (core.ts:1996-2004).

        If the promise rejects, the callback receives the error instead.
        """
        def _forward(future: asyncio.Future[StubHook]) -> None:
            if future.cancelled():
                return
            err = future.exception()
            if err is not None:
                try:
                    callback(err)
                except Exception:
                    pass
            else:
                future.result().on_broken(callback)

        if self.future.done():
            _forward(self.future)
        else:
            self.future.add_done_callback(_forward)
