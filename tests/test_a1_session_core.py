"""A1 stream — session core & dispatch parity tests (P0 set).

Pins the TypeScript reference behavior from upstream capnweb rpc.ts
(readLoop rpc.ts:929-1056, ensureResolvingExport rpc.ts:569-634,
release lifecycle rpc.ts:547-561/857-862, import cancellation
rpc.ts:254-261/395-403, abort rpc.ts:864-927/1045-1050) against
BidirectionalSession.

Matrix: docs/architecture/capnweb-parity/01-wire-protocol-session-core.md
"""

from __future__ import annotations

import asyncio
import inspect
import json
from typing import Any

import pytest

import capnweb.rpc_session as rpc_session_mod
from capnweb.error import ErrorCode, RpcError
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession, ExportEntry
from capnweb.types import RpcTarget
from capnweb.wire import WirePull


# =============================================================================
# Harness: a transport where the test plays the remote peer with raw frames.
# =============================================================================

class ScriptedTransport:
    """Transport whose remote peer is the test: inject raw frames, read sent."""

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
        """Inject a raw frame as if received from the peer."""
        self._incoming.put_nowait(frame)

    def fail(self, exc: BaseException) -> None:
        """Make the next receive() raise, simulating transport failure."""
        self._incoming.put_nowait(exc)


def sent_messages(transport: ScriptedTransport) -> list[list[Any]]:
    """All sent wire messages, parsed. Splits newline-joined frames leniently
    so assertions still work while the old (divergent) framing is in place."""
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


class EchoTarget(RpcTarget):
    def echo(self, value: Any) -> Any:
        return value

    async def slow(self) -> str:
        await asyncio.sleep(0.25)
        return "done"


async def make_session(
    local_main: Any | None = None,
) -> tuple[ScriptedTransport, BidirectionalSession]:
    transport = ScriptedTransport()
    session = BidirectionalSession(transport, local_main=local_main)
    session.start()
    await asyncio.sleep(0)  # let loops spin up
    return transport, session


# =============================================================================
# Item 1: synchronous arrival-ordered export-ID assignment (rpc.ts:929-1056)
# =============================================================================

class TestArrivalOrderedDispatch:
    async def test_export_ids_follow_wire_arrival_order(self) -> None:
        """The killer regression: a mixed frame [pull-for-slow-export, push1]
        followed by [push2] must assign push1's export ID BEFORE push2's.

        With the old task-per-frame dispatch the inline pull await suspends
        the first frame's task and push2 steals the next export ID.
        """
        transport, session = await make_session(EchoTarget())
        try:
            transport.inject('["push", ["pipeline", 0, ["slow"], []]]')  # export 1
            # Mixed frame (lenient receive): pull the slow export, then push "a".
            transport.inject('["pull", 1]\n["push", ["pipeline", 0, ["echo"], ["a"]]]')  # export 2
            transport.inject('["push", ["pipeline", 0, ["echo"], ["b"]]]')  # export 3
            transport.inject('["pull", 2]')
            transport.inject('["pull", 3]')

            await wait_until(
                lambda: len(messages_of(transport, "resolve")) >= 2,
                msg="resolves for the two echo pushes",
            )
            resolves = {m[1]: m[2] for m in messages_of(transport, "resolve")}
            assert resolves[2] == "a", f"export 2 must be push1 (echo a), got {resolves}"
            assert resolves[3] == "b", f"export 3 must be push2 (echo b), got {resolves}"

            await wait_until(
                lambda: 1 in {m[1] for m in messages_of(transport, "resolve")},
                msg="slow export resolve",
            )
            resolves = {m[1]: m[2] for m in messages_of(transport, "resolve")}
            assert resolves[1] == "done"
        finally:
            await session.stop()

    async def test_pull_is_idempotent_one_resolve_per_export(self) -> None:
        """ensureResolvingExport is memoized (rpc.ts:569-575): repeated pull
        messages for one export produce exactly one resolve."""
        transport, session = await make_session(EchoTarget())
        try:
            transport.inject('["push", ["pipeline", 0, ["slow"], []]]')
            transport.inject('["pull", 1]')
            transport.inject('["pull", 1]')
            transport.inject('["pull", 1]')

            await wait_until(
                lambda: len(messages_of(transport, "resolve")) >= 1,
                msg="first resolve",
            )
            await asyncio.sleep(0.2)  # give any duplicate resolves time to appear
            assert len(messages_of(transport, "resolve")) == 1
        finally:
            await session.stop()

    async def test_pull_unknown_export_aborts(self) -> None:
        """Unknown export ID in pull is a protocol error -> session abort
        (rpc.ts:570-572,1054), not a 30s wait + reject."""
        transport, session = await make_session(EchoTarget())
        transport.inject('["pull", 99]')

        await wait_until(
            lambda: session._abort_reason is not None,
            timeout=2.0,
            msg="session abort on unknown export pull",
        )
        await wait_until(
            lambda: len(messages_of(transport, "abort")) >= 1,
            msg="abort message sent to peer",
        )
        await session.stop()

    async def test_poll_constants_deleted(self) -> None:
        """The 0.1s cancel-poll and the 30s export-wait hack are gone."""
        assert not hasattr(rpc_session_mod, "READ_LOOP_TIMEOUT_SECONDS")
        assert not hasattr(rpc_session_mod, "EXPORT_WAIT_TIMEOUT_SECONDS")


# =============================================================================
# Item 6: unknown/malformed message => abort (rpc.ts:1054)
# =============================================================================

class TestBadMessageAborts:
    async def test_unknown_message_type_aborts(self) -> None:
        transport, session = await make_session(EchoTarget())
        transport.inject('["frobnicate", 1]')
        await wait_until(
            lambda: session._abort_reason is not None,
            timeout=2.0,
            msg="abort on unknown message",
        )
        assert len(messages_of(transport, "abort")) >= 1
        await session.stop()

    async def test_malformed_json_aborts(self) -> None:
        transport, session = await make_session(EchoTarget())
        transport.inject("this is not json")
        await wait_until(
            lambda: session._abort_reason is not None,
            timeout=2.0,
            msg="abort on malformed frame",
        )
        await session.stop()

    async def test_stream_message_dispatches_with_auto_release(self) -> None:
        """B1 landed: ["stream", expr] is auto-pulled (a resolve goes out
        with no pull received) and auto-released (the export table returns
        to baseline with no release received). rpc.ts:966-984."""
        transport, session = await make_session(EchoTarget())
        try:
            baseline_exports = len(session._exports)
            transport.inject('["stream", ["pipeline", 0, ["echo"], ["x"]]]')
            await wait_until(
                lambda: len(messages_of(transport, "resolve")) >= 1,
                msg="auto-pulled resolve for the stream message",
            )
            assert session._abort_reason is None
            resolve = messages_of(transport, "resolve")[0]
            assert resolve[1] == 1 and resolve[2] == "x"
            await wait_until(
                lambda: len(session._exports) == baseline_exports,
                msg="stream export auto-released after resolve",
            )
        finally:
            await session.stop()

    async def test_pipe_message_creates_pipe_export(self) -> None:
        """B1 landed: ["pipe"] creates an export with a stashed,
        consume-once readable end (rpc.ts:986-993, 674-682)."""
        transport, session = await make_session(EchoTarget())
        try:
            transport.inject('["pipe"]')
            await wait_until(
                lambda: 1 in session._exports,
                msg="pipe export created at the next arrival-ordered ID",
            )
            assert session._abort_reason is None
            entry = session._exports[1]
            assert entry.pipe_readable is not None
            readable = session.get_pipe_readable(1)
            assert readable is not None
            with pytest.raises(ValueError):
                session.get_pipe_readable(1)  # consume-once
        finally:
            await session.stop()


# =============================================================================
# Item 2: import cancellation on dispose (rpc.ts:254-261, 395-403)
# =============================================================================

class TestImportCancellation:
    async def test_dispose_unresolved_import_rejects_pull_and_releases(self) -> None:
        transport, session = await make_session()
        try:
            hook = session.send_call(0, ["never"], RpcPayload.owned([]))
            pull_task = asyncio.create_task(hook.pull())
            await wait_until(
                lambda: len(messages_of(transport, "pull")) >= 1,
                msg="pull sent",
            )

            hook.dispose()

            await wait_until(
                lambda: [1, 1] in [m[1:] for m in messages_of(transport, "release")],
                msg="release for canceled import",
            )
            with pytest.raises(RpcError) as exc_info:
                await asyncio.wait_for(pull_task, timeout=1.0)
            assert "disposed" in str(exc_info.value)
            assert 1 not in session._imports
        finally:
            await session.stop()

    async def test_dispose_without_pull_still_releases(self) -> None:
        transport, session = await make_session()
        try:
            hook = session.send_call(0, ["never"], RpcPayload.owned([]))
            hook.dispose()
            await wait_until(
                lambda: len(messages_of(transport, "release")) >= 1,
                msg="release sent on dispose",
            )
            assert 1 not in session._imports
        finally:
            await session.stop()


# =============================================================================
# Item 3: release lifecycle (rpc.ts:547-561, 857-862)
# =============================================================================

class TestReleaseLifecycle:
    async def test_send_release_deletes_import_entry(self) -> None:
        """sendRelease deletes the table slot immediately; hooks keep working
        through their cached entry reference (the TS model)."""
        transport, session = await make_session()
        try:
            hook = session.send_call(0, ["m"], RpcPayload.owned([]))
            assert 1 in session._imports
            transport.inject('["resolve", 1, "hi"]')

            await wait_until(
                lambda: len(messages_of(transport, "release")) >= 1,
                msg="release after resolve",
            )
            assert 1 not in session._imports, "import slot must be freed on release send"
            payload = await hook.pull()
            assert payload.value == "hi", "hook must still reach the resolution via its entry"
        finally:
            await session.stop()

    async def test_release_unknown_export_aborts(self) -> None:
        transport, session = await make_session(EchoTarget())
        transport.inject('["release", 42, 1]')
        await wait_until(
            lambda: session._abort_reason is not None,
            timeout=2.0,
            msg="abort on unknown release id",
        )
        await session.stop()

    async def test_release_negative_refcount_aborts(self) -> None:
        transport, session = await make_session(EchoTarget())
        transport.inject('["push", ["pipeline", 0, ["echo"], ["x"]]]')  # export 1, refcount 1
        transport.inject('["release", 1, 2]')  # would go negative
        await wait_until(
            lambda: session._abort_reason is not None,
            timeout=2.0,
            msg="abort on negative refcount",
        )
        await session.stop()

    async def test_release_cleans_reverse_maps_for_target_export(self) -> None:
        transport, session = await make_session(EchoTarget())
        try:
            extra = EchoTarget()
            export_id = session.export_target(extra)
            assert session._target_exports[id(extra)] == export_id

            transport.inject(f'["release", {export_id}, 1]')
            await wait_until(
                lambda: export_id not in session._exports,
                msg="export removed on release",
            )
            assert id(extra) not in session._target_exports

            # Re-export must get a fresh ID cleanly (no stale-map KeyError).
            new_id = session.export_target(extra)
            assert new_id != export_id
            assert new_id in session._exports
        finally:
            await session.stop()

    async def test_release_cleans_reverse_exports_for_hook_export(self) -> None:
        transport, session = await make_session(EchoTarget())
        try:
            from capnweb.hooks import PayloadStubHook

            hook = PayloadStubHook(RpcPayload.owned({"k": 1}))
            export_id = session._export_hook(hook)
            transport.inject(f'["release", {export_id}, 1]')
            await wait_until(
                lambda: export_id not in session._exports,
                msg="export removed on release",
            )
            assert id(hook) not in session._reverse_exports
        finally:
            await session.stop()


# =============================================================================
# Item 4: reject releases; unknown-import resolve evaluates + disposes
# (rpc.ts:1005-1033)
# =============================================================================

class TestRejectAndUnknownResolve:
    async def test_reject_sends_release_and_deletes_entry(self) -> None:
        transport, session = await make_session()
        try:
            hook = session.send_call(0, ["m"], RpcPayload.owned([]))
            transport.inject('["reject", 1, ["error", "bad_request", "nope"]]')

            await wait_until(
                lambda: [1, 1] in [m[1:] for m in messages_of(transport, "release")],
                msg="release after reject",
            )
            assert 1 not in session._imports

            with pytest.raises(RpcError) as exc_info:
                await hook.pull()
            assert exc_info.value.code == ErrorCode.BAD_REQUEST
            assert "nope" in exc_info.value.message
        finally:
            await session.stop()

    async def test_unknown_import_resolve_disposes_embedded_stubs(self) -> None:
        """Resolve for an unknown (already released) import is still parsed
        and disposed so embedded ["export", id] stubs release (rpc.ts:1024-1028)."""
        transport, session = await make_session()
        try:
            transport.inject('["resolve", 7, ["export", 5]]')
            await wait_until(
                lambda: [5, 1] in [m[1:] for m in messages_of(transport, "release")],
                msg="release for stub embedded in orphaned resolve",
            )
        finally:
            await session.stop()

    async def test_unknown_import_reject_is_ignored(self) -> None:
        """TS only evaluates orphaned *resolve* expressions; an orphaned reject
        is dropped without aborting (rpc.ts:1020-1028)."""
        transport, session = await make_session()
        try:
            transport.inject('["reject", 7, ["error", "internal", "late"]]')
            await asyncio.sleep(0.1)
            assert session._abort_reason is None
        finally:
            await session.stop()


# =============================================================================
# Item 5: incoming abort — no echo, parsed reason, resolved imports untouched
# (rpc.ts:1045-1050, 263-276)
# =============================================================================

class TestIncomingAbort:
    async def test_incoming_abort_uses_reason_and_never_echoes(self) -> None:
        transport, session = await make_session()
        hook = session.send_call(0, ["m"], RpcPayload.owned([]))
        pull_task = asyncio.create_task(hook.pull())
        await asyncio.sleep(0.05)

        transport.inject('["abort", ["error", "internal", "peer went away"]]')
        await wait_until(
            lambda: session._abort_reason is not None,
            msg="abort applied",
        )
        assert "peer went away" in str(session._abort_reason)
        await asyncio.sleep(0.1)  # give any (buggy) echo time to flush
        assert messages_of(transport, "abort") == [], "must never echo abort back"

        with pytest.raises(Exception) as exc_info:
            await asyncio.wait_for(pull_task, timeout=1.0)
        assert "peer went away" in str(exc_info.value)

    async def test_abort_leaves_resolved_imports_intact(self) -> None:
        """TS abort() only rejects unresolved entries; already-resolved
        resolutions stay usable (rpc.ts:263-276, divergence (a))."""
        transport, session = await make_session()
        hook = session.send_call(0, ["m"], RpcPayload.owned([]))
        transport.inject('["resolve", 1, "kept"]')
        await wait_until(
            lambda: hook._entry is not None and hook._entry.resolution is not None,
            msg="resolution recorded",
        )
        entry = hook._entry

        transport.inject('["abort", ["error", "internal", "bye"]]')
        await wait_until(lambda: session._abort_reason is not None, msg="aborted")

        payload = await entry.resolution.pull()
        assert payload.value == "kept"


# =============================================================================
# Item 8: read loop — no cancel-poll; connection errors abort loudly
# =============================================================================

class TestReadLoopFailure:
    async def test_connection_error_aborts_and_fires_on_broken(self) -> None:
        transport, session = await make_session()
        errors: list[Exception] = []
        main = session.get_main_stub()
        main.on_broken(errors.append)

        hook = session.send_call(0, ["m"], RpcPayload.owned([]))
        pull_task = asyncio.create_task(hook.pull())
        await asyncio.sleep(0.05)

        transport.fail(ConnectionError("socket died"))
        await wait_until(
            lambda: session._abort_reason is not None,
            timeout=2.0,
            msg="abort on transport failure",
        )
        assert isinstance(session._abort_reason, ConnectionError)
        assert errors, "on_broken callbacks must fire"
        with pytest.raises(Exception):
            await asyncio.wait_for(pull_task, timeout=1.0)
        # Broken transport: never try to send an abort message over it.
        assert messages_of(transport, "abort") == []


# =============================================================================
# Item 7 + 9: one message per frame; all sends via the single-writer queue
# =============================================================================

class TestFraming:
    async def test_send_sync_enqueues_one_message_per_frame(self) -> None:
        transport, session = await make_session()
        try:
            session._send_sync([WirePull(1), WirePull(2)])
            await wait_until(lambda: len(transport.sent) >= 2, msg="two frames")
            assert len(transport.sent) == 2
            for frame in transport.sent:
                assert "\n" not in frame
                json.loads(frame)  # every frame is exactly one JSON message
        finally:
            await session.stop()

    async def test_responses_are_single_message_frames(self) -> None:
        transport, session = await make_session(EchoTarget())
        try:
            transport.inject(
                '["push", ["pipeline", 0, ["echo"], ["a"]]]\n'
                '["push", ["pipeline", 0, ["echo"], ["b"]]]\n'
                '["pull", 1]\n["pull", 2]'
            )
            await wait_until(
                lambda: len(messages_of(transport, "resolve")) >= 2,
                msg="both resolves",
            )
            for frame in transport.sent:
                assert "\n" not in frame, "one message per outbound frame (C-FRAME)"
        finally:
            await session.stop()

    async def test_no_writer_queue_bypass_left_in_source(self) -> None:
        """Item 9: every send goes through the serialized writer queue —
        the awaited direct-send bypass is deleted outright."""
        src = inspect.getsource(rpc_session_mod)
        assert "await self._send(" not in src

    async def test_drain_flushes_writer_queue(self) -> None:
        """After drain() returns, resolves must already be on the transport
        (the HTTP batch server reads the response right after drain)."""
        transport, session = await make_session(EchoTarget())
        try:
            transport.inject('["push", ["pipeline", 0, ["echo"], ["x"]]]')
            transport.inject('["pull", 1]')
            await wait_until(lambda: session._pull_count > 0 or
                             len(messages_of(transport, "resolve")) >= 1,
                             msg="pull registered")
            await session.drain()
            assert len(messages_of(transport, "resolve")) == 1
        finally:
            await session.stop()


# =============================================================================
# C-STREAM structural fields (frozen contract, populated by B1)
# =============================================================================

class TestExportEntryContract:
    def test_export_entry_has_stream_fields(self) -> None:
        from capnweb.hooks import PayloadStubHook

        entry = ExportEntry(hook=PayloadStubHook(RpcPayload.owned(1)))
        assert entry.auto_release is False
        assert entry.pipe_readable is None
