"""B1: streams subsystem loopback tests.

Two BidirectionalSessions joined by in-memory transports exercise every
end/error/cancel path from the design doc (03-streams.md §1.5) plus the
hook-level behaviors of streams.ts:20-119 and 443-520:

* readable in return value / in params (pipe + stream writes + close)
* empty stream (immediate close)
* writable proxy: ordering, close-fails-with-write-error, release-without-
  close abort rule (protocol.md:259/261)
* receiver cancellation and sender abort, both surfaced across the wire
* unconsumed stream disposal (guard hook) cancels the source
* stream-message bookkeeping: no pull/release frames, tables at baseline
* session abort mid-stream: parked writers unblock, pump tasks reaped,
  un-closed local writables aborted
* Blob roundtrip incl. delivery-blocking substitution
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession
from capnweb.streams import (
    ReadableStreamGuardHook,
    RpcReadableStream,
    RpcWritableStream,
    WritableStreamHook,
)
from capnweb.stubs import RpcPromise
from capnweb.types import Blob, RpcTarget

TIMEOUT = 5.0


async def within(awaitable: Any, timeout: float = TIMEOUT) -> Any:
    return await asyncio.wait_for(awaitable, timeout)


class RecordingTransport:
    """In-memory transport that records every frame it sends."""

    def __init__(self) -> None:
        self.peer: "RecordingTransport | None" = None
        self.inbox: asyncio.Queue[str] = asyncio.Queue()
        self.sent: list[str] = []
        self.closed = False

    async def send(self, message: str) -> None:
        if self.closed:
            raise ConnectionError("transport closed")
        self.sent.append(message)
        if self.peer is not None and not self.peer.closed:
            await self.peer.inbox.put(message)

    async def receive(self) -> str:
        if self.closed:
            raise ConnectionError("transport closed")
        return await self.inbox.get()

    def abort(self, reason: Exception) -> None:
        self.closed = True

    def sent_of(self, msg_type: str) -> list[list[Any]]:
        return [
            json.loads(f) for f in self.sent if json.loads(f)[0] == msg_type
        ]


def make_pair(
    main_a: Any | None = None, main_b: Any | None = None
) -> tuple[BidirectionalSession, BidirectionalSession, RecordingTransport, RecordingTransport]:
    ta, tb = RecordingTransport(), RecordingTransport()
    ta.peer, tb.peer = tb, ta
    sa = BidirectionalSession(ta, local_main=main_a)
    sb = BidirectionalSession(tb, local_main=main_b)
    sa.start()
    sb.start()
    return sa, sb, ta, tb


class CollectorSink:
    """Local writable sink recording everything that happens to it."""

    def __init__(self, fail_on_chunk: Any = None) -> None:
        self.chunks: list[Any] = []
        self.closed = False
        self.abort_reason: Exception | None = None
        self.fail_on_chunk = fail_on_chunk

    async def write(self, chunk: Any) -> None:
        if self.fail_on_chunk is not None and chunk == self.fail_on_chunk:
            raise RpcError("Error", "sink write failed")
        self.chunks.append(chunk)

    async def close(self) -> None:
        self.closed = True

    async def abort(self, reason: Any = None) -> None:
        self.abort_reason = (
            reason if isinstance(reason, Exception) else RpcError("Error", str(reason))
        )


class StuckSink:
    """Sink whose writes block until released (or aborted)."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.chunks: list[Any] = []
        self.closed = False
        self.abort_reason: Exception | None = None

    async def write(self, chunk: Any) -> None:
        await self.release.wait()
        self.chunks.append(chunk)

    async def close(self) -> None:
        self.closed = True

    async def abort(self, reason: Any = None) -> None:
        self.abort_reason = (
            reason if isinstance(reason, Exception) else RpcError("Error", str(reason))
        )
        self.release.set()


class StreamTarget(RpcTarget):
    """Server-side target exercising every stream direction."""

    def __init__(self) -> None:
        self.received: list[Any] = []
        self.sink = CollectorSink()
        self.source_finalized = asyncio.Event()
        self.source_error: BaseException | None = None

    def echo(self, value: Any) -> Any:
        return value

    def make_stream(self, chunks: list[Any]) -> RpcReadableStream:
        async def gen():
            for chunk in chunks:
                yield chunk

        return RpcReadableStream(gen())

    def make_tracked_stream(self, count: int) -> RpcReadableStream:
        async def gen():
            try:
                for i in range(count):
                    yield i
            except BaseException as e:  # includes GeneratorExit
                self.source_error = e
                raise
            finally:
                self.source_finalized.set()

        return RpcReadableStream(gen())

    def make_error_stream(self, ok_chunks: int) -> RpcReadableStream:
        async def gen():
            for i in range(ok_chunks):
                yield i
            raise RpcError("Error", "source exploded")

        return RpcReadableStream(gen())

    async def consume_stream(self, stream: RpcReadableStream) -> int:
        count = 0
        async for chunk in stream:
            self.received.append(chunk)
            count += 1
        return count

    def make_writable(self) -> RpcWritableStream:
        return RpcWritableStream(self.sink)

    def make_stuck_writable(self) -> RpcWritableStream:
        self.sink = StuckSink()
        return RpcWritableStream(self.sink)

    def make_failing_writable(self, fail_on: Any) -> RpcWritableStream:
        self.sink = CollectorSink(fail_on_chunk=fail_on)
        return RpcWritableStream(self.sink)

    def sink_state(self) -> dict[str, Any]:
        return {
            "chunks": self.sink.chunks,
            "closed": self.sink.closed,
            "abort": str(self.sink.abort_reason) if self.sink.abort_reason else None,
        }

    async def read_request(self, req: Any) -> dict[str, Any]:
        from capnweb.types import Request

        assert isinstance(req, Request)
        body = req.body
        if isinstance(body, RpcReadableStream):
            data = b"".join([bytes(c) async for c in body])
        else:
            data = body
        return {
            "method": req.method,
            "url": req.url,
            "body": data.decode() if isinstance(data, bytes) else data,
            "duplex": req.extensions.get("duplex"),
        }

    def echo_blob(self, blob: Blob) -> dict[str, Any]:
        assert isinstance(blob, Blob), f"expected Blob, got {type(blob)}"
        return {"type": blob.type, "size": blob.size, "text": blob.data.decode()}

    def make_blob(self, text: str, content_type: str) -> Blob:
        return Blob(content_type, text.encode())


async def call_main(
    session: BidirectionalSession, method: str, args: list[Any]
) -> tuple[Any, Any]:
    """Call a method on the peer's main capability; returns (value, hook)."""
    hook = session.send_call(0, [method], RpcPayload.from_app_params(args))
    payload = await within(hook.pull())
    return payload.value, hook


async def stopped(*sessions: BidirectionalSession) -> None:
    for s in sessions:
        await s.stop()


# =============================================================================
# §1.1 readable in return value / §4 items 1-2 analog (loopback)
# =============================================================================

class TestReadableStreams:
    async def test_readable_in_return_value(self) -> None:
        sa, sb, ta, tb = make_pair(main_b=StreamTarget())
        try:
            value, _hook = await call_main(sa, "make_stream", [[1, "two", {"three": 3}]])
            assert isinstance(value, RpcReadableStream)
            chunks = [c async for c in value]
            assert chunks == [1, "two", {"three": 3}]
            # Clean end: further reads keep raising StopAsyncIteration.
            with pytest.raises(StopAsyncIteration):
                await within(value.read())
        finally:
            await stopped(sa, sb)

    async def test_empty_readable_immediate_close(self) -> None:
        sa, sb, ta, tb = make_pair(main_b=StreamTarget())
        try:
            value, _hook = await call_main(sa, "make_stream", [[]])
            assert isinstance(value, RpcReadableStream)
            chunks = [c async for c in value]
            assert chunks == []
        finally:
            await stopped(sa, sb)

    async def test_readable_in_params(self) -> None:
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            async def gen():
                for i in range(5):
                    yield {"i": i}

            value, _hook = await call_main(
                sa, "consume_stream", [RpcReadableStream(gen())]
            )
            assert value == 5
            assert target.received == [{"i": i} for i in range(5)]
        finally:
            await stopped(sa, sb)

    async def test_pipe_frame_precedes_push(self) -> None:
        """Early-flow property (protocol.md:121): the ["pipe"] frame must be
        sent before the push that carries ["readable", id]."""
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            async def gen():
                yield 1

            await call_main(sa, "consume_stream", [RpcReadableStream(gen())])
            kinds = [json.loads(f)[0] for f in ta.sent]
            assert "pipe" in kinds and "push" in kinds
            assert kinds.index("pipe") < kinds.index("push")
        finally:
            await stopped(sa, sb)

    async def test_bytes_chunks_roundtrip(self) -> None:
        sa, sb, ta, tb = make_pair(main_b=StreamTarget())
        try:
            payloads = [b"\x00\x01\x02", b"x" * 1000]
            value, _hook = await call_main(sa, "make_stream", [payloads])
            chunks = [c async for c in value]
            assert chunks == payloads
        finally:
            await stopped(sa, sb)

    async def test_bare_async_generator_does_not_auto_stream(self) -> None:
        """§3.4 recommendation: bare async generators are NOT auto-streamed;
        sending one is a serialization error, not silent streaming."""
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            async def gen():
                yield 1

            with pytest.raises(TypeError, match="Cannot serialize value"):
                sa.send_call(
                    0, ["consume_stream"], RpcPayload.from_app_params([gen()])
                )
            assert not target.received
        finally:
            await stopped(sa, sb)

    async def test_large_single_chunk_exceeding_initial_window(self) -> None:
        """>256 KiB single chunk: exceeds INITIAL_WINDOW in one write."""
        big = bytes(range(256)) * 1200  # 300 KiB
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            async def gen():
                yield big

            value, _hook = await call_main(sa, "consume_stream", [RpcReadableStream(gen())])
            assert value == 1
            assert target.received == [big]
        finally:
            await stopped(sa, sb)


# =============================================================================
# §1.2 ["writable"] proxy + protocol.md:259/261 rules
# =============================================================================

class TestWritableStreams:
    async def test_writable_proxy_ordering_and_close(self) -> None:
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            writable, hook = await call_main(sa, "make_writable", [])
            assert isinstance(writable, RpcWritableStream)
            for i in range(10):
                await within(writable.write(i))
            await within(writable.close())
            state, _ = await call_main(sa, "sink_state", [])
            assert state["chunks"] == list(range(10))
            assert state["closed"] is True
            assert state["abort"] is None
        finally:
            await stopped(sa, sb)

    async def test_writable_as_async_context_manager(self) -> None:
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            writable, _hook = await call_main(sa, "make_writable", [])
            async with writable as w:
                await within(w.write("a"))
                await within(w.write("b"))
            state, _ = await call_main(sa, "sink_state", [])
            assert state["chunks"] == ["a", "b"] and state["closed"] is True
        finally:
            await stopped(sa, sb)

    async def test_close_surfaces_mid_stream_write_error(self) -> None:
        """protocol.md:261: writes may be pipelined; if any write failed,
        close() fails with that error."""
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            writable, _hook = await call_main(sa, "make_failing_writable", ["bad"])
            await within(writable.write("ok-1"))
            await within(writable.write("bad"))   # fails on the sink, later
            await within(writable.write("ok-2"))  # processed after the failure
            with pytest.raises(Exception) as exc_info:
                await within(writable.close())
            assert "sink write failed" in str(exc_info.value)
            state, _ = await call_main(sa, "sink_state", [])
            assert state["chunks"] == ["ok-1"]  # error latched: ok-2 rejected
        finally:
            await stopped(sa, sb)

    async def test_write_after_error_raises_immediately(self) -> None:
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            writable, _hook = await call_main(sa, "make_failing_writable", ["bad"])
            await within(writable.write("bad"))
            # Wait for the rejection to come back and latch.
            for _ in range(100):
                if writable._pending_error is not None:
                    break
                await asyncio.sleep(0.01)
            with pytest.raises(Exception, match="sink write failed"):
                await within(writable.write("after"))
        finally:
            await stopped(sa, sb)

    async def test_release_without_close_aborts_sink(self) -> None:
        """protocol.md:259 / streams.ts:100-113: releasing a ["writable"]
        export without close() aborts the underlying stream."""
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            writable, hook = await call_main(sa, "make_writable", [])
            await within(writable.write("one"))
            # Drop the result without close(): dispose the call result hook,
            # whose payload owns the proxy's import ref.
            hook.dispose()
            for _ in range(200):
                if target.sink.abort_reason is not None:
                    break
                await asyncio.sleep(0.01)
            assert target.sink.abort_reason is not None
            assert "disposed without calling close()" in str(target.sink.abort_reason)
            assert target.sink.chunks == ["one"]
        finally:
            await stopped(sa, sb)

    async def test_abort_propagates_reason(self) -> None:
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            writable, _hook = await call_main(sa, "make_writable", [])
            await within(writable.write("x"))
            await within(writable.abort(RpcError("Error", "client changed its mind")))
            for _ in range(200):
                if target.sink.abort_reason is not None:
                    break
                await asyncio.sleep(0.01)
            assert "client changed its mind" in str(target.sink.abort_reason)
            # Writes after abort fail locally.
            with pytest.raises(Exception):
                await within(writable.write("y"))
        finally:
            await stopped(sa, sb)

    async def test_exported_writable_is_locked_locally(self) -> None:
        """Serializing an RpcWritableStream locks it (TS getWriter())."""
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_a=StreamTarget(), main_b=target)
        try:
            sink = CollectorSink()
            local = RpcWritableStream(sink)
            # Send it to the peer inside a call.
            hook = sa.send_call(0, ["echo"], RpcPayload.from_app_params([local]))
            await within(hook.pull())
            with pytest.raises(Exception, match="locked"):
                await within(local.write("nope"))
        finally:
            await stopped(sa, sb)


# =============================================================================
# §1.5 cancellation / abort paths
# =============================================================================

class TestCancellationAndAbort:
    async def test_receiver_cancellation_reaches_source(self) -> None:
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            value, _hook = await call_main(sa, "make_tracked_stream", [1000])
            assert isinstance(value, RpcReadableStream)
            got = [await within(value.read()) for _ in range(2)]
            assert got == [0, 1]
            await within(value.cancel(RpcError("Error", "had enough")))
            # The cancellation travels via write rejections; the server pump
            # then closes the source generator.
            await within(target.source_finalized.wait())
            # Session stays healthy for subsequent calls.
            echoed, _ = await call_main(sa, "echo", ["still alive"])
            assert echoed == "still alive"
        finally:
            await stopped(sa, sb)

    async def test_sender_abort_errors_receiver(self) -> None:
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            value, _hook = await call_main(sa, "make_error_stream", [2])
            chunks: list[Any] = []

            async def consume() -> None:
                async for chunk in value:
                    chunks.append(chunk)

            with pytest.raises(Exception) as exc_info:
                await within(consume())
            # WHATWG semantics: an abort discards still-buffered chunks, so
            # the reader observes a PREFIX of the sent chunks, then the error.
            assert chunks == [0, 1][: len(chunks)]
            assert "source exploded" in str(exc_info.value)
        finally:
            await stopped(sa, sb)

    async def test_unconsumed_stream_disposal_cancels_source(self) -> None:
        """§4 item 7 analog: dispose the payload without reading; the guard
        hook cancels the source and both tables return to baseline."""
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            base_a = sa.get_stats()
            base_b = sb.get_stats()
            value, hook = await call_main(sa, "make_tracked_stream", [50])
            assert isinstance(value, RpcReadableStream)
            hook.dispose()  # dispose result without consuming the stream
            await within(target.source_finalized.wait())
            # Tables drain back to baseline on both peers.
            for _ in range(300):
                if sa.get_stats() == base_a and sb.get_stats() == base_b:
                    break
                await asyncio.sleep(0.01)
            assert sa.get_stats() == base_a, (sa.get_stats(), base_a)
            assert sb.get_stats() == base_b, (sb.get_stats(), base_b)
        finally:
            await stopped(sa, sb)

    async def test_session_abort_mid_stream_unblocks_parked_writer(self) -> None:
        """§4 item 10 analog: parked writers must unblock with the abort
        error (no hung write), pump tasks reaped, local writables aborted."""
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            writable, _hook = await call_main(sa, "make_stuck_writable", [])
            # Server sink never acks, so in-flight bytes only grow. Frame
            # size for a 100 KiB bytes chunk is ~137 KB (base64), so the
            # first write stays under INITIAL_WINDOW and the second parks.
            big = b"z" * (100 * 1024)
            first = asyncio.ensure_future(writable.write(big))
            await asyncio.sleep(0.05)
            assert first.done(), "first write must not block (window not full)"

            parked = asyncio.ensure_future(writable.write(big))
            await asyncio.sleep(0.05)
            assert not parked.done(), "second write should be parked on the window"

            sa._abort(RpcError.internal("test abort"), send_abort=False)
            with pytest.raises(Exception):
                await within(parked)
            # Pump tasks reaped after stop().
            await stopped(sa, sb)
            assert not sa._pump_tasks
            assert not sb._pump_tasks
        finally:
            await stopped(sa, sb)

    async def test_session_abort_aborts_unclosed_local_writables(self) -> None:
        """_abort's export-dispose sweep must abort un-closed local sinks
        (WritableStreamHook.dispose, protocol.md:259)."""
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            writable, _hook = await call_main(sa, "make_writable", [])
            await within(writable.write("x"))
            await asyncio.sleep(0.05)
            sb._abort(RpcError.internal("server died"), send_abort=False)
            for _ in range(200):
                if target.sink.abort_reason is not None:
                    break
                await asyncio.sleep(0.01)
            assert target.sink.abort_reason is not None
        finally:
            await stopped(sa, sb)


# =============================================================================
# §4 item 9 analog: stream-message bookkeeping
# =============================================================================

class TestBookkeeping:
    async def test_no_pull_or_release_for_stream_writes(self) -> None:
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            base_a = sa.get_stats()
            base_b = sb.get_stats()

            async def gen():
                for i in range(20):
                    yield i

            value, hook = await call_main(sa, "consume_stream", [RpcReadableStream(gen())])
            assert value == 20

            stream_frames = ta.sent_of("stream")
            assert len(stream_frames) == 21  # 20 writes + close
            # Stream import ids implied by wire order: collect them and
            # assert no pull/release ever names them.
            pulls = {m[1] for m in ta.sent_of("pull")}
            # The pipe write ids are never pulled: only the main call is.
            assert len(pulls) <= 1
            # Releases: exactly one for the pipe import itself (proxy close),
            # plus the main-call resolve release; none per stream write.
            releases = ta.sent_of("release")
            assert len(releases) <= 2

            hook.dispose()
            for _ in range(300):
                if sa.get_stats() == base_a and sb.get_stats() == base_b:
                    break
                await asyncio.sleep(0.01)
            assert sa.get_stats() == base_a
            assert sb.get_stats() == base_b
        finally:
            await stopped(sa, sb)

    async def test_stream_import_entries_purged_after_resolve(self) -> None:
        """remote_refcount=0 stream imports are deleted manually on
        resolution (rpc.ts:810-819)."""
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            async def gen():
                yield "only"

            imports_before = set(sa._imports)
            value, hook = await call_main(sa, "consume_stream", [RpcReadableStream(gen())])
            assert value == 1
            hook.dispose()
            for _ in range(300):
                extra = set(sa._imports) - imports_before
                if not extra:
                    break
                await asyncio.sleep(0.01)
            assert set(sa._imports) - imports_before == set()
        finally:
            await stopped(sa, sb)


# =============================================================================
# Blob (D5, matrix Part 2 row 11)
# =============================================================================

class TestBlob:
    async def test_blob_type_validation(self) -> None:
        with pytest.raises(TypeError):
            Blob(123, b"x")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            Blob("text/plain", "not bytes")  # type: ignore[arg-type]
        blob = Blob("text/plain", bytearray(b"abc"))
        assert blob.data == b"abc" and blob.size == 3

    async def test_blob_stream_chunks(self) -> None:
        blob = Blob("application/octet-stream", b"a" * (70 * 1024))
        chunks = [c async for c in blob.stream()]
        assert b"".join(chunks) == blob.data
        assert len(chunks) == 2  # 64 KiB chunking

    async def test_blob_in_params_delivery_awaits_content(self) -> None:
        target = StreamTarget()
        sa, sb, ta, tb = make_pair(main_b=target)
        try:
            blob = Blob("text/plain", "hello blob".encode())
            value, _hook = await call_main(sa, "echo_blob", [blob])
            assert value == {"type": "text/plain", "size": 10, "text": "hello blob"}
        finally:
            await stopped(sa, sb)

    async def test_blob_in_return_value(self) -> None:
        sa, sb, ta, tb = make_pair(main_b=StreamTarget())
        try:
            value, _hook = await call_main(sa, "make_blob", ["röundtrip", "text/plain;charset=utf-8"])
            assert isinstance(value, Blob)
            assert value.type == "text/plain;charset=utf-8"
            assert value.data.decode() == "röundtrip"
        finally:
            await stopped(sa, sb)

    async def test_large_blob_roundtrip(self) -> None:
        sa, sb, ta, tb = make_pair(main_b=StreamTarget())
        try:
            text = "0123456789" * 20000  # 200 KB -> multiple pipe chunks
            value, _hook = await call_main(sa, "echo_blob", [Blob("t/x", text.encode())])
            assert value["size"] == 200000
        finally:
            await stopped(sa, sb)

    async def test_blob_wire_form(self) -> None:
        sa, sb, ta, tb = make_pair(main_b=StreamTarget())
        try:
            await call_main(sa, "echo_blob", [Blob("text/plain", b"x")])
            # The push args must contain ["blob", type, ["readable", id]]
            # and a ["pipe"] frame must have preceded the push.
            def find_blob(node: Any) -> list[Any] | None:
                if isinstance(node, list):
                    if node and node[0] == "blob":
                        return node
                    for item in node:
                        found = find_blob(item)
                        if found is not None:
                            return found
                return None

            blob_expr = next(
                (b for m in ta.sent_of("push") if (b := find_blob(m)) is not None),
                None,
            )
            assert blob_expr is not None, ta.sent
            assert blob_expr[1] == "text/plain"
            assert isinstance(blob_expr[2], list) and blob_expr[2][0] == "readable"
            kinds = [json.loads(f)[0] for f in ta.sent]
            assert kinds.index("pipe") < kinds.index("push")
        finally:
            await stopped(sa, sb)


# =============================================================================
# Request/Response stream bodies (unblocked by B1; serialize.ts:745-757)
# =============================================================================

class TestHttpStreamBodies:
    async def test_request_with_stream_body(self) -> None:
        from capnweb.types import Request

        sa, sb, ta, tb = make_pair(main_b=StreamTarget())
        try:
            async def gen():
                yield b"part one, "
                yield b"part two"

            req = Request(
                url="https://example.test/upload",
                method="POST",
                body=RpcReadableStream(gen()),
            )
            value, _hook = await call_main(sa, "read_request", [req])
            assert value == {
                "method": "POST",
                "url": "https://example.test/upload",
                "body": "part one, part two",
                "duplex": "half",  # emitted automatically for stream bodies
            }
        finally:
            await stopped(sa, sb)


# =============================================================================
# Hook-level unit tests (streams.ts:20-119, 443-520 parity)
# =============================================================================

class TestWritableStreamHook:
    async def test_unknown_method_rejected(self) -> None:
        sink = CollectorSink()
        hook = WritableStreamHook.create(sink)
        result = hook.call(["frobnicate"], RpcPayload.from_app_params([]))
        with pytest.raises(Exception, match="Unknown WritableStream method"):
            await within(result.pull())
        hook.dispose()

    async def test_deep_path_rejected(self) -> None:
        sink = CollectorSink()
        hook = WritableStreamHook.create(sink)
        result = hook.call(["a", "b"], RpcPayload.from_app_params([]))
        with pytest.raises(Exception, match="direct method calls"):
            await within(result.pull())
        hook.dispose()

    async def test_get_map_pull_error(self) -> None:
        sink = CollectorSink()
        hook = WritableStreamHook.create(sink)
        with pytest.raises(Exception, match="Cannot pull"):
            await within(hook.pull())
        get_hook = hook.get(["x"])
        with pytest.raises(Exception, match="properties"):
            await within(get_hook.pull())
        map_hook = hook.map([], [], [0])
        with pytest.raises(Exception, match="map"):
            await within(map_hook.pull())
        hook.dispose()

    async def test_dispose_without_close_aborts(self) -> None:
        sink = CollectorSink()
        hook = WritableStreamHook.create(sink)
        write = hook.call(["write"], RpcPayload.from_app_params(["c"]))
        await within(write.pull())
        hook.dispose()
        await asyncio.sleep(0.05)
        assert sink.abort_reason is not None
        assert "disposed without calling close()" in str(sink.abort_reason)

    async def test_dispose_after_close_does_not_abort(self) -> None:
        sink = CollectorSink()
        hook = WritableStreamHook.create(sink)
        close = hook.call(["close"], RpcPayload.from_app_params([]))
        await within(close.pull())
        hook.dispose()
        await asyncio.sleep(0.05)
        assert sink.abort_reason is None
        assert sink.closed is True

    async def test_dup_shares_refcount(self) -> None:
        sink = CollectorSink()
        hook = WritableStreamHook.create(sink)
        dup = hook.dup()
        hook.dispose()
        await asyncio.sleep(0.02)
        assert sink.abort_reason is None  # dup still holds a ref
        dup.dispose()
        await asyncio.sleep(0.05)
        assert sink.abort_reason is not None

    async def test_use_after_dispose_raises(self) -> None:
        sink = CollectorSink()
        hook = WritableStreamHook.create(sink)
        hook.dispose()
        result = hook.call(["write"], RpcPayload.from_app_params(["x"]))
        with pytest.raises(Exception, match="after it was disposed"):
            await within(result.pull())

    async def test_writes_processed_in_order(self) -> None:
        """Delivered write() coroutines resolve in arrival order (§3.4)."""
        order: list[int] = []

        class SlowFirstSink:
            async def write(self, chunk: int) -> None:
                if chunk == 0:
                    await asyncio.sleep(0.05)
                order.append(chunk)

        hook = WritableStreamHook.create(SlowFirstSink())
        results = [
            hook.call(["write"], RpcPayload.from_app_params([i])) for i in range(5)
        ]
        for r in results:
            await within(r.pull())
        assert order == [0, 1, 2, 3, 4]
        hook.call(["close"], RpcPayload.from_app_params([]))
        hook.dispose()


class TestReadableStreamGuardHook:
    async def test_dispose_cancels_unconsumed(self) -> None:
        class Source:
            def __init__(self) -> None:
                self.aclosed = asyncio.Event()

            def __aiter__(self) -> "Source":
                return self

            async def __anext__(self) -> int:
                return 1

            async def aclose(self) -> None:
                self.aclosed.set()

        source = Source()
        stream = RpcReadableStream(source)
        guard = ReadableStreamGuardHook.create(stream)
        guard.dispose()
        await within(source.aclosed.wait())
        assert stream._cancelled is not None
        assert "disposed without being consumed" in str(stream._cancelled)

    async def test_dispose_skips_consuming_stream(self) -> None:
        async def gen():
            yield 1
            yield 2

        stream = RpcReadableStream(gen())
        guard = ReadableStreamGuardHook.create(stream)
        assert await within(stream.read()) == 1  # consuming
        guard.dispose()
        await asyncio.sleep(0.02)
        assert stream._cancelled is None
        assert await within(stream.read()) == 2

    async def test_dup_and_double_dispose(self) -> None:
        async def gen():
            yield 1

        stream = RpcReadableStream(gen())
        guard = ReadableStreamGuardHook.create(stream)
        dup = guard.dup()
        guard.dispose()
        await asyncio.sleep(0.02)
        assert stream._cancelled is None  # dup keeps it alive
        dup.dispose()
        await asyncio.sleep(0.02)
        assert stream._cancelled is not None
        with pytest.raises(Exception, match="after it was disposed"):
            dup.dup()

    async def test_operations_error(self) -> None:
        async def gen():
            yield 1

        stream = RpcReadableStream(gen())
        guard = ReadableStreamGuardHook.create(stream)
        call_result = guard.call([], RpcPayload.from_app_params([]))
        with pytest.raises(Exception, match="Cannot call methods"):
            await within(call_result.pull())
        with pytest.raises(Exception, match="Cannot pull"):
            await within(guard.pull())
        guard.dispose()


class TestRpcStreamClasses:
    async def test_readable_requires_async_iterable(self) -> None:
        with pytest.raises(TypeError):
            RpcReadableStream([1, 2, 3])  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            RpcReadableStream(iter([1]))  # type: ignore[arg-type]

        async def gen():
            yield 1

        # AsyncIterator and AsyncIterable both accepted.
        assert isinstance(RpcReadableStream(gen()), RpcReadableStream)

        class Iterable_:
            def __aiter__(self):
                return gen()

        s = RpcReadableStream(Iterable_())
        assert [c async for c in s] == [1]

    async def test_read_raises_cancel_reason(self) -> None:
        async def gen():
            yield 1

        s = RpcReadableStream(gen())
        await s.cancel(RpcError("Error", "nope"))
        with pytest.raises(Exception, match="nope"):
            await within(s.read())

    async def test_writable_requires_write_method(self) -> None:
        with pytest.raises(TypeError):
            RpcWritableStream(object())

    async def test_local_writable_sequentializes_and_latches(self) -> None:
        sink = CollectorSink(fail_on_chunk="bad")
        w = RpcWritableStream(sink)
        await within(w.write("a"))
        with pytest.raises(Exception, match="sink write failed"):
            await within(w.write("bad"))
        with pytest.raises(Exception, match="sink write failed"):
            await within(w.close())

    async def test_local_writable_context_manager_abort_on_error(self) -> None:
        sink = CollectorSink()
        with pytest.raises(ValueError):
            async with RpcWritableStream(sink) as w:
                await w.write("x")
                raise ValueError("app error")
        assert sink.abort_reason is not None
        assert sink.closed is False


class TestStandaloneCodecs:
    def test_standalone_serialize_rejects_streams(self) -> None:
        from capnweb.serializer import serialize

        async def gen():
            yield 1

        with pytest.raises(Exception, match="without an RPC session"):
            serialize(RpcReadableStream(gen()))
        with pytest.raises(Exception, match="without an RPC session"):
            serialize(Blob("t", b"x"))
        with pytest.raises(Exception, match="without an RPC session"):
            serialize(RpcWritableStream(CollectorSink()))

    def test_standalone_deserialize_rejects_streams(self) -> None:
        from capnweb.parser import deserialize

        with pytest.raises(Exception, match="without an RPC session"):
            deserialize('["readable", 1]')
        with pytest.raises(Exception, match="without an RPC session"):
            deserialize('["writable", 1]')
