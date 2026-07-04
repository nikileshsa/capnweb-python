"""Phase C parity tests: encoding levels, in-process pipe sessions,
onRpcBroken ordering, bootstrap error hook, multi-waiter drain, payload
tracking fold-ins and the serializer promise-export fallback leak fix.

TS references: rpc.ts:50-88,471-491 (encoding levels), rpc.ts:217-237
(onBroken re-registration ordering hack), rpc.ts:1089-1096 (bootstrap
ErrorStubHook), messageport.ts (queue-pair transport analog).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.hooks import PayloadStubHook, PromiseStubHook, StubHook
from capnweb.inprocess import InProcessPipeTransport, new_pipe_rpc_session_pair
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession
from capnweb.serializer import Serializer
from capnweb.stubs import RpcPromise, RpcStub, deliver_payload_in_place
from capnweb.types import RpcTarget


async def within(awaitable: Any, timeout: float = 5.0) -> Any:
    return await asyncio.wait_for(awaitable, timeout)


class Counter(RpcTarget):
    def __init__(self) -> None:
        self.n = 0

    async def call(self, method: str, args: list[Any]) -> Any:
        if method == "increment":
            self.n += args[0] if args else 1
            return self.n
        if method == "value":
            return self.n
        raise RpcError.not_found(method)

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(name)


class MainService(RpcTarget):
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "echo":
                return args[0]
            case "add":
                return args[0] + args[1]
            case "bytes_back":
                return b"\x00\x01\xfe" + bytes(args[0])
            case "fail":
                raise RpcError("TypeError", "deliberate failure")
            case "counter":
                return Counter()
            case "slow":
                await asyncio.sleep(args[0])
                return "done"
            case _:
                raise RpcError.not_found(method)

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(name)


def _sessions_of(stub_a: RpcStub, stub_b: RpcStub) -> tuple[Any, Any]:
    return stub_a._hook.session, stub_b._hook.session


async def _teardown(*stubs: RpcStub) -> None:
    sessions = {s._hook.session for s in stubs}
    for stub in stubs:
        stub.dispose()
    for session in sessions:
        await session.stop()


# =============================================================================
# new_pipe_rpc_session_pair (MessagePort analog)
# =============================================================================


class TestPipeSessionPair:
    async def test_bidirectional_calls(self) -> None:
        a_main = MainService()
        stub_b, stub_a = new_pipe_rpc_session_pair(a_main, MainService())
        # stub_b = A's view of B's main; stub_a = B's view of A's main.
        try:
            assert await within(stub_b.echo("hi")) == "hi"
            assert await within(stub_a.add(2, 3)) == 5
        finally:
            await _teardown(stub_b, stub_a)

    async def test_capability_passing_and_pipelining(self) -> None:
        stub_b, stub_a = new_pipe_rpc_session_pair(None, MainService())
        try:
            counter = await within(stub_b.counter())
            assert await within(counter.increment(5)) == 5
            assert await within(counter.increment(2)) == 7
        finally:
            await _teardown(stub_b, stub_a)

    async def test_dispose_main_stub_closes_both_sessions(self) -> None:
        stub_b, stub_a = new_pipe_rpc_session_pair(MainService(), MainService())
        session_a, session_b = _sessions_of(stub_b, stub_a)
        stub_b.dispose()  # A shuts down; close sentinel reaches B
        await within(session_a.wait_closed())
        await within(session_b.wait_closed())
        await session_a.stop()
        await session_b.stop()

    async def test_errors_round_trip(self) -> None:
        stub_b, stub_a = new_pipe_rpc_session_pair(None, MainService())
        try:
            with pytest.raises(RpcError) as exc_info:
                await within(stub_b.fail())
            assert exc_info.value.name == "TypeError"
            assert "deliberate failure" in exc_info.value.message
        finally:
            await _teardown(stub_b, stub_a)


# =============================================================================
# Encoding levels (rpc.ts:50-88, 471-491)
# =============================================================================


class _RecordingPipeTransport(InProcessPipeTransport):
    """Records every frame it sends, for wire-shape assertions."""

    __slots__ = ("sent",)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.sent: list[Any] = []

    async def send(self, message: Any) -> None:
        self.sent.append(message)
        await super().send(message)


def _recording_pair(
    encoding_level: str,
    local_main_a: Any = None,
    local_main_b: Any = None,
) -> tuple[RpcStub, RpcStub, _RecordingPipeTransport, _RecordingPipeTransport]:
    q_ab: asyncio.Queue[Any] = asyncio.Queue()
    q_ba: asyncio.Queue[Any] = asyncio.Queue()
    ta = _RecordingPipeTransport(q_ab, q_ba, encoding_level)
    tb = _RecordingPipeTransport(q_ba, q_ab, encoding_level)
    sa = BidirectionalSession(ta, local_main_a)
    sb = BidirectionalSession(tb, local_main_b)
    sa.start()
    sb.start()
    return sa.get_remote_main(), sb.get_remote_main(), ta, tb


class TestEncodingLevels:
    async def test_json_compatible_loopback_end_to_end(self) -> None:
        """A jsonCompatible transport carries value TREES, never JSON text,
        and a full session works over it (calls, errors, capabilities)."""
        stub_b, stub_a, ta, tb = _recording_pair(
            "jsonCompatible", None, MainService()
        )
        try:
            assert await within(stub_b.echo({"k": [1, 2, 3]})) == {"k": [1, 2, 3]}
            # Bytes still encode as base64 strings at this level.
            result = await within(stub_b.bytes_back(b"\x10"))
            assert result == b"\x00\x01\xfe\x10"
            # Capabilities work.
            counter = await within(stub_b.counter())
            assert await within(counter.increment()) == 1
            # Errors round trip.
            with pytest.raises(RpcError, match="deliberate failure"):
                await within(stub_b.fail())

            assert ta.sent and tb.sent
            for frame in ta.sent + tb.sent:
                assert isinstance(frame, list), frame  # trees, not strings
        finally:
            await _teardown(stub_b, stub_a)

    async def test_json_compatible_with_bytes_keeps_bytes_raw(self) -> None:
        stub_b, stub_a, ta, tb = _recording_pair(
            "jsonCompatibleWithBytes", None, MainService()
        )
        try:
            payload = b"\x00\xff" * 10
            result = await within(stub_b.bytes_back(payload))
            assert result == b"\x00\x01\xfe" + payload

            def find_bytes(node: Any) -> bool:
                if isinstance(node, list):
                    if (
                        len(node) == 2
                        and node[0] == "bytes"
                        and isinstance(node[1], bytes)
                    ):
                        return True
                    return any(find_bytes(item) for item in node)
                if isinstance(node, dict):
                    return any(find_bytes(v) for v in node.values())
                return False

            # Both directions carried raw ["bytes", <bytes>] — no base64.
            assert any(find_bytes(f) for f in ta.sent)
            assert any(find_bytes(f) for f in tb.sent)
        finally:
            await _teardown(stub_b, stub_a)

    async def test_unknown_encoding_level_rejected(self) -> None:
        """Strict rejection: a stale/unknown level name must fail loudly at
        construction, never silently corrupt the wire (rpc.ts:481-491)."""
        q1: asyncio.Queue[Any] = asyncio.Queue()
        q2: asyncio.Queue[Any] = asyncio.Queue()
        transport = InProcessPipeTransport(q1, q2, "jsonCompatable")  # typo'd
        with pytest.raises(TypeError, match="Unknown transport encodingLevel"):
            BidirectionalSession(transport)

    async def test_structured_clonable_rejected_as_unsupported(self) -> None:
        q1: asyncio.Queue[Any] = asyncio.Queue()
        q2: asyncio.Queue[Any] = asyncio.Queue()
        transport = InProcessPipeTransport(q1, q2, "structuredClonable")
        with pytest.raises(NotImplementedError, match="structuredClonable"):
            BidirectionalSession(transport)

    async def test_none_encoding_level_means_string(self) -> None:
        """A present-but-None attribute is the default string level
        (rpc.ts:479-484: present-but-undefined is not a custom encoding)."""
        q1: asyncio.Queue[Any] = asyncio.Queue()
        q2: asyncio.Queue[Any] = asyncio.Queue()
        transport = InProcessPipeTransport(q1, q2, "string")
        transport.encoding_level = None  # type: ignore[assignment]
        session = BidirectionalSession(transport)
        assert session._encoding_level == "string"

    async def test_malformed_tree_from_custom_transport_aborts(self) -> None:
        """Deserialization hardening holds on the tree path too: garbage
        frames abort the session (never silently coerced)."""
        stub_b, stub_a, _ta, _tb = _recording_pair(
            "jsonCompatible", None, MainService()
        )
        session_a, _session_b = _sessions_of(stub_b, stub_a)
        try:
            assert await within(stub_b.echo(1)) == 1
            # Inject a malformed frame directly into A's receive queue.
            session_a.transport._in.put_nowait({"not": "a message"})
            await within(session_a.wait_closed())
            assert session_a._abort_reason is not None
        finally:
            await _teardown(stub_b, stub_a)


# =============================================================================
# Bootstrap ErrorStubHook at export 0 (rpc.ts:1089-1096)
# =============================================================================


class TestNoMainObject:
    async def test_calls_on_absent_main_reject_without_abort(self) -> None:
        stub_b, stub_a = new_pipe_rpc_session_pair(MainService(), None)
        # B has no main: A's calls on it must REJECT with the TS message,
        # and the session must survive (reject, not abort).
        session_a, session_b = _sessions_of(stub_b, stub_a)
        try:
            with pytest.raises(RpcError, match="This connection has no main object."):
                await within(stub_b.anything())
            # Session alive: B can still call A's (present) main.
            assert await within(stub_a.echo("still alive")) == "still alive"
            assert session_a._abort_reason is None
            assert session_b._abort_reason is None
        finally:
            await _teardown(stub_b, stub_a)


# =============================================================================
# onRpcBroken ordering across resolve (rpc.ts:217-237)
# =============================================================================


class TestOnBrokenOrdering:
    async def test_three_callbacks_preserve_registration_order(self) -> None:
        """cb1 and cb3 are registered on an unresolved import that resolves
        to another capability on the SAME session (re-registering them); cb2
        sits on a different import in between. Order must stay 1, 2, 3."""
        stub_b, stub_a = new_pipe_rpc_session_pair(None, MainService())
        session_a, _ = _sessions_of(stub_b, stub_a)
        fired: list[str] = []
        try:
            p1 = stub_b.counter()
            p1.on_rpc_broken(lambda e: fired.append("cb1"))
            p2 = stub_b.counter()
            p2.on_rpc_broken(lambda e: fired.append("cb2"))
            p1.on_rpc_broken(lambda e: fired.append("cb3"))

            # Resolve p1: transfer re-registers cb1/cb3 via the resolution
            # (a same-session capability); the hack must keep their ORIGINAL
            # slots instead of pushing them behind cb2.
            counter1 = await within(p1)
            assert isinstance(counter1, RpcStub)

            session_a._abort(RpcError.internal("boom"), send_abort=False)
            assert fired == ["cb1", "cb2", "cb3"]
        finally:
            await _teardown(stub_b, stub_a)

    async def test_callback_indices_are_never_reused(self) -> None:
        """Monotonic registration indices: freeing a middle slot must not
        let a later registration clobber an existing one (the old len()-based
        allocator did exactly that)."""
        q1: asyncio.Queue[Any] = asyncio.Queue()
        q2: asyncio.Queue[Any] = asyncio.Queue()
        session = BidirectionalSession(InProcessPipeTransport(q1, q2))
        i0 = session.register_on_broken_callback(lambda e: None)
        i1 = session.register_on_broken_callback(lambda e: None)
        i2 = session.register_on_broken_callback(lambda e: None)
        session.remove_on_broken_callback(i1)
        i3 = session.register_on_broken_callback(lambda e: None)
        assert len({i0, i1, i2, i3}) == 4
        assert i3 > i2


# =============================================================================
# Multi-waiter drain
# =============================================================================


class TestMultiWaiterDrain:
    async def test_concurrent_drains_all_complete(self) -> None:
        stub_b, stub_a = new_pipe_rpc_session_pair(None, MainService())
        _, session_b = _sessions_of(stub_b, stub_a)
        try:
            call_task = asyncio.ensure_future(within(stub_b.slow(0.2)))
            # Let the push+pull reach B so its pull_count goes positive.
            for _ in range(50):
                if session_b._pull_count > 0:
                    break
                await asyncio.sleep(0.01)
            assert session_b._pull_count > 0

            d1 = asyncio.ensure_future(session_b.drain())
            d2 = asyncio.ensure_future(session_b.drain())
            d3 = asyncio.ensure_future(session_b.drain())
            await within(asyncio.gather(d1, d2, d3))
            assert await within(call_task) == "done"
        finally:
            await _teardown(stub_b, stub_a)

    async def test_drain_waiters_rejected_on_abort(self) -> None:
        stub_b, stub_a = new_pipe_rpc_session_pair(None, MainService())
        _, session_b = _sessions_of(stub_b, stub_a)
        try:
            call_task = asyncio.ensure_future(within(stub_b.slow(30)))
            for _ in range(50):
                if session_b._pull_count > 0:
                    break
                await asyncio.sleep(0.01)

            d1 = asyncio.ensure_future(session_b.drain())
            d2 = asyncio.ensure_future(session_b.drain())
            await asyncio.sleep(0.05)
            session_b._abort(RpcError.internal("die"), send_abort=False)
            for d in (d1, d2):
                with pytest.raises(Exception):
                    await within(d)
            call_task.cancel()
            with pytest.raises(BaseException):
                await call_task
        finally:
            await _teardown(stub_b, stub_a)


# =============================================================================
# payload.py fold-ins (B2 -> C handoff, 2026-07-05)
# =============================================================================


def _resolved_promise(value: Any) -> RpcPromise:
    fut: asyncio.Future[StubHook] = asyncio.get_event_loop().create_future()
    fut.set_result(PayloadStubHook(RpcPayload.owned(value)))
    return RpcPromise(PromiseStubHook(fut))


class TestPayloadTrackingFoldIns:
    async def test_from_array_reparents_root_promises(self) -> None:
        p1 = RpcPayload.deep_copy_from(_resolved_promise(41))
        p2 = RpcPayload.deep_copy_from(_resolved_promise(42))
        combined = RpcPayload.from_array([p1, p2])
        # Tracking entries must point at the combined array slots.
        assert [(parent is combined.value, key) for parent, key, _ in combined.promises] == [
            (True, 0),
            (True, 1),
        ]
        await deliver_payload_in_place(combined)
        assert combined.value == [41, 42]
        combined.dispose()

    async def test_deep_copy_from_tracks_root_promise_with_payload_parent(self) -> None:
        payload = RpcPayload.deep_copy_from(_resolved_promise("root"))
        assert len(payload.promises) == 1
        parent, key, _promise = payload.promises[0]
        assert parent is payload
        assert key == "value"
        await deliver_payload_in_place(payload)
        assert payload.value == "root"
        payload.dispose()

    async def test_delivered_flag_prevents_double_delivery(self) -> None:
        payload = RpcPayload.deep_copy_from(_resolved_promise("x"))
        assert payload.delivered is False
        await deliver_payload_in_place(payload)
        assert payload.delivered is True
        # Second delivery is a no-op (no re-splice, no re-await).
        await deliver_payload_in_place(payload)
        assert payload.value == "x"
        payload.dispose()


# =============================================================================
# Serializer promise-export fallback (one-ref leak fix)
# =============================================================================


class _CountingHook(StubHook):
    """A hook that refcounts dup/dispose and resolves get(path) to itself."""

    def __init__(self) -> None:
        self.refs = 1
        self.gets: list[list[Any]] = []

    def call(self, path, args):  # pragma: no cover - unused
        raise NotImplementedError

    def map(self, path, captures, instructions):  # pragma: no cover - unused
        raise NotImplementedError

    def get(self, path):
        self.gets.append(list(path))
        self.refs += 1
        return self

    async def pull(self):  # pragma: no cover - unused
        raise NotImplementedError

    def ignore_unhandled_rejections(self) -> None:
        pass

    def dispose(self) -> None:
        self.refs -= 1

    def dup(self):
        self.refs += 1
        return self


class _MinimalPromiseExporter:
    """Exotic third-party exporter: has export_promise but NOT
    export_promise_hook (the fallback path)."""

    def __init__(self) -> None:
        self.exported: list[StubHook] = []

    def export_capability(self, stub):  # pragma: no cover - unused
        raise NotImplementedError

    def export_promise(self, stub) -> int:
        # TS exportPromise semantics via the wrapper: dup and adopt.
        self.exported.append(stub._hook.dup())
        return -1

    def get_import(self, hook):
        return None

    def unexport(self, ids) -> None:
        for _ in ids:
            hook = self.exported.pop()
            hook.dispose()

    def create_pipe(self, readable, guard_hook):  # pragma: no cover - unused
        raise NotImplementedError

    def on_send_error(self, error):
        return None


class TestPromiseExportFallbackLeak:
    def test_fallback_drops_temp_ref(self) -> None:
        base = _CountingHook()
        exporter = _MinimalPromiseExporter()
        promise = RpcPromise(base, ["a", "b"], _borrowed=True)

        wire = Serializer(exporter=exporter).serialize(promise)
        assert wire == ["promise", -1]

        # get(path) materialized one ref; export_promise dup'd one; the
        # serializer's TEMP wrapper ref must have been released. Exactly the
        # export-table ref (and the caller's original) remain.
        assert base.gets == [["a", "b"]]
        assert base.refs == 2  # original + export table (temp gone)

        exporter.unexport([-1])
        assert base.refs == 1  # only the caller's original remains
