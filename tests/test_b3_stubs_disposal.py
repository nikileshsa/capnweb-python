"""B3 tests: stub/promise public API — dup(), on_rpc_broken(), polymorphic
constructor, sync context managers, get_remote_main (matrix Part 5 §5.2/§5.6,
core.ts:451-520).
"""

from __future__ import annotations

import asyncio

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
from capnweb.stubs import RpcPromise, RpcStub, create_stub, get_remote_main
from capnweb.types import RpcTarget


class DisposableTarget(RpcTarget):
    """RpcTarget that counts dispose() calls."""

    def __init__(self) -> None:
        self.disposed = 0

    async def call(self, method: str, args: list) -> object:
        if method == "echo":
            return args[0]
        raise ValueError(f"Unknown method: {method}")

    async def get_property(self, name: str) -> object:
        # Attribute access on a local stub goes through get_property; return
        # a callable so `stub.echo(...)` works (lazy path accumulation on
        # RpcPromise is B2's stream).
        if name == "echo":
            return lambda x: x
        if name == "name":
            return "disposable"
        raise AttributeError(name)

    def dispose(self) -> None:
        self.disposed += 1


class TestRpcStubDup:
    """stub.dup() — core.ts:491-507, README:378-380."""

    def test_dup_returns_new_stub(self) -> None:
        target = DisposableTarget()
        stub = RpcStub(TargetStubHook(target))
        dup = stub.dup()
        assert isinstance(dup, RpcStub)
        assert dup is not stub

    def test_target_disposed_only_after_all_dups_disposed(self) -> None:
        target = DisposableTarget()
        stub = RpcStub(TargetStubHook(target))
        dup = stub.dup()

        stub.dispose()
        assert target.disposed == 0  # dup still holds a reference

        dup.dispose()
        assert target.disposed == 1

    def test_dup_of_dup(self) -> None:
        target = DisposableTarget()
        stub = RpcStub(TargetStubHook(target))
        d1 = stub.dup()
        d2 = d1.dup()
        stub.dispose()
        d1.dispose()
        assert target.disposed == 0
        d2.dispose()
        assert target.disposed == 1

    async def test_dup_is_usable_after_original_disposed(self) -> None:
        target = DisposableTarget()
        stub = RpcStub(TargetStubHook(target))
        dup = stub.dup()
        stub.dispose()

        result = await dup.echo("hello")
        assert result == "hello"
        dup.dispose()


class TestRpcPromiseDup:
    """promise.dup() stub-ifies (core.ts:495-503)."""

    async def test_promise_dup_returns_stub(self) -> None:
        target = DisposableTarget()
        stub = RpcStub(TargetStubHook(target))
        promise = stub.echo("x")
        assert isinstance(promise, RpcPromise)

        dup = promise.dup()
        assert isinstance(dup, RpcStub)
        assert not isinstance(dup, RpcPromise)
        # Original promise still awaitable.
        assert await promise == "x"

    async def test_promise_stub_hook_dup_is_independent(self) -> None:
        """Disposing the dup must not dispose the original's resolution."""
        target = DisposableTarget()
        future: asyncio.Future[StubHook] = asyncio.get_event_loop().create_future()
        original = PromiseStubHook(future)
        dup = original.dup()

        future.set_result(TargetStubHook(target))
        # Let the dup's chained (shielded) future resolve.
        for _ in range(3):
            await asyncio.sleep(0)

        dup.dispose()
        await asyncio.sleep(0)
        assert target.disposed == 0  # original still holds a reference

        original.dispose()
        await asyncio.sleep(0)
        assert target.disposed == 1


class TestPayloadStubHookDup:
    """PayloadStubHook.dup deep-copies (core.ts:1739-1750)."""

    def test_dup_does_not_double_dispose_embedded_stubs(self) -> None:
        target = DisposableTarget()
        inner = RpcStub(TargetStubHook(target))
        payload = RpcPayload.from_app_return({"cap": inner})
        hook = PayloadStubHook(payload)

        dup = hook.dup()
        hook.dispose()
        assert target.disposed == 0  # dup's deep copy dup()ed the stub
        dup.dispose()
        assert target.disposed == 1


class TestOnRpcBroken:
    """stub.on_rpc_broken(cb) — types.d.ts:57."""

    def test_error_hook_fires_immediately(self) -> None:
        err = RpcError.internal("broken")
        stub = RpcStub(ErrorStubHook(err))
        seen: list[Exception] = []
        stub.on_rpc_broken(seen.append)
        assert seen == [err]

    def test_local_target_never_fires(self) -> None:
        stub = RpcStub(TargetStubHook(DisposableTarget()))
        seen: list[Exception] = []
        stub.on_rpc_broken(seen.append)
        assert seen == []

    def test_payload_hook_forwards_to_single_stub_value(self) -> None:
        """core.ts:1772-1783: payload that IS a stub forwards onRpcBroken."""
        err = RpcError.internal("inner broken")
        inner = RpcStub(ErrorStubHook(err))
        hook = PayloadStubHook(RpcPayload.owned(inner))
        seen: list[Exception] = []
        hook.on_broken(seen.append)
        assert seen == [err]

    async def test_promise_hook_forwards_after_resolution(self) -> None:
        """core.ts:1996-2004."""
        err = RpcError.internal("resolved to error")
        future: asyncio.Future[StubHook] = asyncio.get_event_loop().create_future()
        promise = RpcPromise(PromiseStubHook(future))
        seen: list[Exception] = []
        promise.on_rpc_broken(seen.append)
        assert seen == []

        future.set_result(ErrorStubHook(err))
        await asyncio.sleep(0)
        assert seen == [err]

    async def test_promise_hook_rejection_calls_callback(self) -> None:
        err = RpcError.internal("rejected")
        future: asyncio.Future[StubHook] = asyncio.get_event_loop().create_future()
        hook = PromiseStubHook(future)
        hook.ignore_unhandled_rejections()
        promise = RpcPromise(hook)
        seen: list[Exception] = []
        promise.on_rpc_broken(seen.append)

        future.set_exception(err)
        await asyncio.sleep(0)
        assert seen == [err]


class TestPolymorphicConstructor:
    """RpcStub(value) — core.ts:451-476, index.ts:38-40."""

    async def test_construct_from_rpc_target(self) -> None:
        target = DisposableTarget()
        stub = RpcStub(target)
        assert await stub.echo("hi") == "hi"

    async def test_construct_from_plain_value(self) -> None:
        stub = RpcStub({"a": 1, "b": [1, 2, 3]})
        assert await stub.a == 1

    async def test_construct_from_callable(self) -> None:
        stub = RpcStub(lambda x: x * 2)
        assert await stub(21) == 42

    def test_construct_from_hook_passes_through(self) -> None:
        hook = TargetStubHook(DisposableTarget())
        stub = RpcStub(hook)
        assert stub._hook is hook

    def test_construct_from_stub_dups(self) -> None:
        target = DisposableTarget()
        original = RpcStub(target)
        copy = RpcStub(original)
        original.dispose()
        assert target.disposed == 0
        copy.dispose()
        assert target.disposed == 1

    def test_create_stub_still_requires_rpc_target(self) -> None:
        with pytest.raises(TypeError, match="Expected RpcTarget"):
            create_stub(42)  # type: ignore[arg-type]


class TestSyncContextManager:
    """`with stub:` == TS `using` (§5.6)."""

    def test_sync_with_disposes(self) -> None:
        target = DisposableTarget()
        with RpcStub(target) as stub:
            assert isinstance(stub, RpcStub)
            assert target.disposed == 0
        assert target.disposed == 1

    async def test_async_with_still_disposes(self) -> None:
        target = DisposableTarget()
        async with RpcStub(target):
            assert target.disposed == 0
        assert target.disposed == 1

    async def test_promise_async_with_awaits_then_disposes(self) -> None:
        """Python-only sugar: `async with promise as value` (§5.6 item 3)."""
        target = DisposableTarget()
        stub = RpcStub(target)
        async with stub.echo("v") as value:
            assert value == "v"


class TestGetRemoteMain:
    """get_remote_main(session) -> RpcStub (rpc.ts:1089-1105)."""

    async def test_returns_stub_over_real_session(self) -> None:
        from capnweb.batch import BatchServerTransport
        from capnweb.rpc_session import BidirectionalSession

        session = BidirectionalSession(BatchServerTransport([]), None)
        stub = get_remote_main(session)
        assert isinstance(stub, RpcStub)
        # Disposing the main stub shuts the session down (rpc.ts:506-510).
        stub.dispose()
        assert session._abort_reason is not None
        await session.stop()
