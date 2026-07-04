"""A1 stream — session core P1 parity tests.

Pins: sync send_map on the shared import-ID counter (rpc.ts:824-848),
promise-ID-reuse guard (rpc.ts:655-668), pull-on-non-promise guard +
non-promise dup (rpc.ts:371-387), MainImportHook shutdown (rpc.ts:412-427),
chained-promise resolve skip (rpc.ts:575-597), arbitrary-expression push
(protocol.md:73,81-83), unified pull-count accounting (rpc.ts:464,601-632),
promise imports never send pull (importPromise pulling=true).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.hooks import ErrorStubHook, PayloadStubHook, PromiseStubHook
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession, ImportHook, MainImportHook
from capnweb.stubs import RpcPromise
from capnweb.types import RpcTarget


# =============================================================================
# Harness (kept self-contained: the two `tests` packages in this repo make
# cross-test-module imports fragile).
# =============================================================================

class ScriptedTransport:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.aborted: Exception | None = None
        self._incoming: asyncio.Queue[str | BaseException] = asyncio.Queue()

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def receive(self) -> str:
        item = await self._incoming.get()
        if isinstance(item, BaseException):
            raise item
        return item

    def abort(self, reason: Exception) -> None:
        self.aborted = reason

    def inject(self, frame: str) -> None:
        self._incoming.put_nowait(frame)


def sent_messages(transport: ScriptedTransport) -> list[list[Any]]:
    messages: list[list[Any]] = []
    for frame in transport.sent:
        for line in frame.split("\n"):
            if line.strip():
                messages.append(json.loads(line))
    return messages


def messages_of(transport: ScriptedTransport, msg_type: str) -> list[list[Any]]:
    return [m for m in sent_messages(transport) if m[0] == msg_type]


async def wait_until(cond, timeout: float = 5.0, msg: str = "") -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not cond():
        if loop.time() > deadline:
            raise AssertionError(f"timed out waiting for: {msg or cond}")
        await asyncio.sleep(0.01)


async def make_session(
    local_main: Any | None = None,
) -> tuple[ScriptedTransport, BidirectionalSession]:
    transport = ScriptedTransport()
    session = BidirectionalSession(transport, local_main=local_main)
    session.start()
    await asyncio.sleep(0)
    return transport, session


class EchoTarget(RpcTarget):
    def echo(self, value: Any) -> Any:
        return value


# =============================================================================
# send_map: synchronous, shared import-ID counter, writer-queue routed
# =============================================================================

class TestSendMap:
    async def test_send_map_is_synchronous_and_shares_import_counter(self) -> None:
        transport, session = await make_session()
        try:
            h1 = session.send_call(0, ["m"], RpcPayload.owned([]))
            mh = session.send_map(0, [], [], [["instr"]])
            h3 = session.send_call(0, ["m"], RpcPayload.owned([]))

            assert isinstance(mh, ImportHook), "send_map must be sync like sendCall"
            assert h1.import_id == 1
            assert mh.import_id == 2
            assert h3.import_id == 3

            await wait_until(
                lambda: len(messages_of(transport, "push")) == 3,
                msg="three pushes on the wire",
            )
            pushes = messages_of(transport, "push")
            assert pushes[0][1][0] == "pipeline"
            assert pushes[1][1][0] == "remap"
            assert pushes[2][1][0] == "pipeline"
        finally:
            await session.stop()

    async def test_stub_map_hook_is_import_hook_not_promise_wrapper(self) -> None:
        """ImportHook.map/PipelineHook.map return the send_map ImportHook
        directly now that send_map is sync."""
        transport, session = await make_session()
        try:
            main = session.get_main_stub()
            result = main.map([], [], [["instr"]])
            assert isinstance(result, ImportHook)
        finally:
            await session.stop()


# =============================================================================
# Promise imports: reuse guard; never send pull (importPromise pulling=true)
# =============================================================================

class TestPromiseImports:
    async def test_promise_id_reuse_returns_error_hook(self) -> None:
        transport, session = await make_session()
        try:
            session.send_call(0, ["m"], RpcPayload.owned([]))  # import 1 exists
            hook = session.create_promise_hook(1)
            assert isinstance(hook, ErrorStubHook)
        finally:
            await session.stop()

    async def test_promise_import_never_sends_pull(self) -> None:
        """A ["promise", id] embedded in a resolve is auto-resolved by the
        sender; awaiting it must not emit a pull message."""
        transport, session = await make_session()
        try:
            call_hook = session.send_call(0, ["m"], RpcPayload.owned([]))
            pull_task = asyncio.create_task(call_hook.pull())
            await wait_until(
                lambda: [1] in [m[1:] for m in messages_of(transport, "pull")],
                msg="pull for import 1",
            )

            transport.inject('["resolve", 1, ["promise", -3]]')
            # B2/TS delivery parity: promises are never delivered to the
            # application — delivery blocks until the embedded promise
            # resolves and is substituted in place (deliverResolve).
            await asyncio.sleep(0.1)
            assert not pull_task.done(), \
                "delivery must wait for the embedded promise resolution"
            assert [-3] not in [m[1:] for m in messages_of(transport, "pull")], \
                "promise imports are already-pulling; no pull message allowed"

            transport.inject('["resolve", -3, "later"]')
            payload = await asyncio.wait_for(pull_task, timeout=2.0)
            assert payload.value == "later", \
                "embedded promise must be substituted before delivery"
        finally:
            await session.stop()


# =============================================================================
# Pull guard + dup semantics (rpc.ts:371-387)
# =============================================================================

class TestPullGuardAndDup:
    async def test_pull_on_non_promise_hook_raises(self) -> None:
        transport, session = await make_session()
        try:
            hook = session.import_capability(5)
            with pytest.raises(RpcError) as exc_info:
                await hook.pull()
            assert "not a promise" in str(exc_info.value)
        finally:
            await session.stop()

    async def test_dup_is_always_non_promise_and_single_increment(self) -> None:
        transport, session = await make_session()
        try:
            hook = session.send_call(0, ["m"], RpcPayload.owned([]))
            entry = hook._entry
            assert entry is not None and entry.local_refcount == 1

            dup = hook.dup()
            assert dup.is_promise is False
            assert entry.local_refcount == 2, "dup must increment exactly once"

            dup.dispose()
            assert entry.local_refcount == 1
        finally:
            await session.stop()


# =============================================================================
# Main stub disposal => session shutdown (rpc.ts:412-427)
# =============================================================================

class TestMainHookShutdown:
    async def test_main_hook_dispose_shuts_down_session(self) -> None:
        transport, session = await make_session()
        main = session.get_main_stub()
        assert isinstance(main, MainImportHook)

        main.dispose()
        await asyncio.sleep(0.05)
        assert session._abort_reason is not None
        assert "disposing the main stub" in str(session._abort_reason)
        # shutdown() never sends an abort message (rpc.ts:506-510)
        assert messages_of(transport, "abort") == []

        main.dispose()  # idempotent


# =============================================================================
# Chained-promise resolve skip (rpc.ts:575-597)
# =============================================================================

class ChainTarget(RpcTarget):
    def __init__(self) -> None:
        self._tasks: list[asyncio.Task] = []

    def chain(self) -> Any:
        future: asyncio.Future = asyncio.get_event_loop().create_future()

        async def later() -> None:
            await asyncio.sleep(0.05)
            future.set_result(PayloadStubHook(RpcPayload.owned(42)))

        self._tasks.append(asyncio.ensure_future(later()))
        return RpcPromise(PromiseStubHook(future))


class TestChainedResolveSkip:
    async def test_promise_valued_result_resolves_to_final_value(self) -> None:
        transport, session = await make_session(ChainTarget())
        try:
            transport.inject('["push", ["pipeline", 0, ["chain"], []]]')
            transport.inject('["pull", 1]')

            await wait_until(
                lambda: len(messages_of(transport, "resolve")) >= 1,
                msg="resolve for chained promise",
            )
            resolves = messages_of(transport, "resolve")
            assert resolves == [["resolve", 1, 42]], (
                "intermediate promise must be skipped; the final value is "
                f"resolved directly, got {resolves}"
            )
        finally:
            await session.stop()


# =============================================================================
# Arbitrary-expression push (protocol.md:73,81-83)
# =============================================================================

class TestArbitraryPush:
    async def test_push_literal_array(self) -> None:
        transport, session = await make_session(EchoTarget())
        try:
            transport.inject('["push", [[1, 2, 3]]]')
            transport.inject('["pull", 1]')
            await wait_until(
                lambda: len(messages_of(transport, "resolve")) >= 1,
                msg="resolve of literal push",
            )
            assert messages_of(transport, "resolve") == [["resolve", 1, [[1, 2, 3]]]]
        finally:
            await session.stop()

    async def test_push_literal_object_then_pipeline_into_it(self) -> None:
        """Plain-data pushes join the same export ID space and can be
        pipelined into by later pushes."""
        transport, session = await make_session(EchoTarget())
        try:
            transport.inject('["push", {"greeting": "hello"}]')  # export 1
            transport.inject('["pull", 1]')
            await wait_until(
                lambda: len(messages_of(transport, "resolve")) >= 1,
                msg="resolve of object push",
            )
            assert messages_of(transport, "resolve")[0] == [
                "resolve", 1, {"greeting": "hello"}
            ]
            assert session._abort_reason is None
        finally:
            await session.stop()


# =============================================================================
# Unified pull-count accounting
# =============================================================================

class SlowTarget(RpcTarget):
    async def slow(self) -> str:
        await asyncio.sleep(0.2)
        return "done"


class TestExporterContract:
    """C-EXPORTER surface on the live session (A2 -> A1 handoff)."""

    async def test_unexport_releases_like_peer_release(self) -> None:
        transport, session = await make_session(EchoTarget())
        try:
            extra = EchoTarget()
            export_id = session.export_target(extra)
            assert export_id in session._exports

            session.unexport([export_id])

            assert export_id not in session._exports
            assert id(extra) not in session._target_exports
        finally:
            await session.stop()

    async def test_create_pipe_rejects_non_stream_and_sends_pipe_frame(self) -> None:
        """B1 landed: create_pipe type-checks its input, and a real
        RpcReadableStream produces a ["pipe"] frame + tracked pump task
        (rpc.ts:684-705)."""
        from capnweb.streams import ReadableStreamGuardHook, RpcReadableStream

        transport, session = await make_session()
        try:
            with pytest.raises(TypeError):
                session.create_pipe(None, None)

            async def gen():
                yield 1

            stream = RpcReadableStream(gen())
            guard = ReadableStreamGuardHook.create(stream)
            import_id = session.create_pipe(stream, guard)
            assert import_id >= 1
            await wait_until(
                lambda: any(m[0] == "pipe" for m in sent_messages(transport)),
                msg="pipe frame sent",
            )
        finally:
            await session.stop()

    async def test_on_send_error_redacts_reject(self) -> None:
        from capnweb.config import RpcSessionConfig

        class SecretTarget(RpcTarget):
            def leak(self) -> None:
                raise RpcError.permission_denied("super secret detail")

        options = RpcSessionConfig(
            on_send_error=lambda e: RpcError.internal("redacted")
        )
        transport = ScriptedTransport()
        session = BidirectionalSession(
            transport, local_main=SecretTarget(), options=options
        )
        session.start()
        await asyncio.sleep(0)
        try:
            transport.inject('["push", ["pipeline", 0, ["leak"], []]]')
            transport.inject('["pull", 1]')
            await wait_until(
                lambda: len(messages_of(transport, "reject")) >= 1,
                msg="reject for redacted error",
            )
            reject = messages_of(transport, "reject")[0]
            assert reject[2][1] == "internal"
            assert reject[2][2] == "redacted"
            assert "secret" not in json.dumps(reject)
        finally:
            await session.stop()

    async def test_reject_carries_wire_faithful_error_name(self) -> None:
        class NamedErrorTarget(RpcTarget):
            def typed(self) -> None:
                raise RpcError("TypeError", "bad type")

        transport, session = await make_session(NamedErrorTarget())
        try:
            transport.inject('["push", ["pipeline", 0, ["typed"], []]]')
            transport.inject('["pull", 1]')
            await wait_until(
                lambda: len(messages_of(transport, "reject")) >= 1,
                msg="reject with JS error name",
            )
            reject = messages_of(transport, "reject")[0]
            assert reject[2][:3] == ["error", "TypeError", "bad type"]
        finally:
            await session.stop()


class TestPullCount:
    async def test_repeated_pulls_count_once(self) -> None:
        transport, session = await make_session(SlowTarget())
        try:
            transport.inject('["push", ["pipeline", 0, ["slow"], []]]')
            transport.inject('["pull", 1]')
            transport.inject('["pull", 1]')
            transport.inject('["pull", 1]')
            await wait_until(lambda: session._pull_count > 0, msg="pull registered")
            assert session._pull_count == 1

            await session.drain()
            assert session._pull_count == 0
            assert len(messages_of(transport, "resolve")) == 1
        finally:
            await session.stop()
