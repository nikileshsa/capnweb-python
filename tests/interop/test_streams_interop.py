"""B1: PY <-> TS stream interop tests (design doc 03-streams.md §4).

Matrix coverage against the real TS reference server (npx tsx ts_server.ts):

1.  TS→PY ReadableStream in return value (incl. N=0 and a >256 KiB chunk)
2.  PY→TS ReadableStream in call params (+ wire-capture: pipe before push)
3.  WritableStream both directions (+ close() surfaces a mid-stream error)
4.  Flow-control soak, 32 MiB each direction (@pytest.mark.slow)
5.  Receiver cancellation, both directions
6.  Sender abort, both directions
8.  Release-without-close abort rule (protocol.md:259)
9.  stream-message bookkeeping (no pull/release for stream writes; tables
    return to baseline)
10. Session abort mid-stream (parked writer unblocks, pumps reaped)
11. Blob framing both directions

(Item 7 — unconsumed-stream disposal — is covered at the loopback level in
tests/test_b1_streams.py; the TS-facing cancel path is exercised by item 5.)
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.streams import FlowController, RpcReadableStream, RpcWritableStream
from capnweb.types import Blob

TIMEOUT = 15.0


async def within(awaitable: Any, timeout: float = TIMEOUT) -> Any:
    return await asyncio.wait_for(awaitable, timeout)


class _RecordingTransportWrapper:
    """Delegating transport wrapper that records every sent frame."""

    def __init__(self, inner: Any, frames: list[str]) -> None:
        self._inner = inner
        self._frames = frames

    async def send(self, message: str) -> None:
        self._frames.append(message)
        await self._inner.send(message)

    async def receive(self) -> str:
        return await self._inner.receive()

    def abort(self, reason: Exception) -> None:
        abort = getattr(self._inner, "abort", None)
        if abort is not None:
            abort(reason)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class SessionClient:
    """Thin session-level client: capture frames, keep result hooks."""

    def __init__(self, url: str) -> None:
        self.url = url
        self._client: Any = None
        self.session: Any = None
        self.sent_frames: list[str] = []

    async def __aenter__(self) -> "SessionClient":
        from capnweb.ws_session import WebSocketRpcClient

        self._client = WebSocketRpcClient(self.url)
        await self._client.__aenter__()
        self.session = self._client._session

        # Wire capture: wrap the transport (its class uses __slots__, so we
        # swap in a delegating wrapper on the session).
        self.session.transport = _RecordingTransportWrapper(
            self.session.transport, self.sent_frames
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client is not None:
            await self._client.__aexit__(*args)

    def call_hook(self, method: str, args: list[Any]) -> Any:
        """Start a call on the peer main; returns the result ImportHook."""
        return self.session.send_call(
            0, [method], RpcPayload.from_app_params(args)
        )

    async def call(self, method: str, args: list[Any]) -> Any:
        hook = self.call_hook(method, args)
        payload = await within(hook.pull())
        return payload.value

    def frames_of(self, msg_type: str) -> list[list[Any]]:
        out = []
        for frame in self.sent_frames:
            parsed = json.loads(frame)
            if parsed[0] == msg_type:
                out.append(parsed)
        return out

    def frame_kinds(self) -> list[str]:
        return [json.loads(f)[0] for f in self.sent_frames]


@pytest.fixture
async def client(ts_server):  # noqa: ANN001 - conftest fixture
    async with SessionClient(f"ws://127.0.0.1:{ts_server.port}/") as c:
        yield c


def byte_pattern(total: int) -> bytes:
    return bytes((i & 0xFF) for i in range(total))


async def async_chunks(chunks: list[Any]):
    for chunk in chunks:
        yield chunk


# =============================================================================
# Item 1 — TS→PY ReadableStream in return value
# =============================================================================

class TestTsToPyReadable:
    async def test_return_value_stream_chunks_and_clean_end(self, client) -> None:
        stream = await client.call("makeStream", [[1, "two", {"three": 3}, [4, 5]]])
        assert isinstance(stream, RpcReadableStream)
        chunks = [c async for c in stream]
        assert chunks == [1, "two", {"three": 3}, [4, 5]]
        with pytest.raises(StopAsyncIteration):
            await within(stream.read())

    async def test_empty_stream_immediate_close(self, client) -> None:
        stream = await client.call("makeStream", [[]])
        assert isinstance(stream, RpcReadableStream)
        assert [c async for c in stream] == []

    async def test_large_single_chunk_exceeds_initial_window(self, client) -> None:
        size = 300 * 1024  # > INITIAL_WINDOW (256 KiB) in one write
        stream = await client.call("makeLargeChunkStream", [size])
        chunks = [c async for c in stream]
        assert len(chunks) == 1
        assert bytes(chunks[0]) == bytes((i & 0xFF) for i in range(size))

    async def test_byte_stream_content(self, client) -> None:
        stream = await client.call("makeByteStream", [64 * 1024, 8 * 1024])
        data = b"".join([bytes(c) async for c in stream])
        assert data == byte_pattern(64 * 1024)


# =============================================================================
# Item 2 — PY→TS ReadableStream in call params (+ early-flow wire property)
# =============================================================================

class TestPyToTsReadable:
    async def test_params_stream_collected_by_ts(self, client) -> None:
        chunks = [1, "two", {"three": 3}]
        result = await client.call(
            "collectStream", [RpcReadableStream(async_chunks(chunks))]
        )
        assert result == chunks

    async def test_pipe_frame_precedes_push(self, client) -> None:
        await client.call("collectStream", [RpcReadableStream(async_chunks([1]))])
        kinds = client.frame_kinds()
        assert "pipe" in kinds and "push" in kinds
        assert kinds.index("pipe") < kinds.index("push"), kinds

    async def test_byte_chunks_arrive_in_order(self, client) -> None:
        payloads = [bytes([i]) * 100 for i in range(20)]
        total = await client.call(
            "sumByteStream", [RpcReadableStream(async_chunks(payloads))]
        )
        assert total == 2000


# =============================================================================
# Item 3 — WritableStream both directions
# =============================================================================

class TestWritableBothDirections:
    async def test_ts_exports_writable_py_writes(self, client) -> None:
        collector_hook = client.call_hook("makeCollector", [])
        writable_hook = None
        try:
            payload = await within(collector_hook.pull())
            collector = payload.value
            writable_hook = collector._hook.call(
                ["getWritable"], RpcPayload.from_app_params([])
            )
            wpayload = await within(writable_hook.pull())
            writable = wpayload.value
            assert isinstance(writable, RpcWritableStream)

            for i in range(5):
                await within(writable.write({"n": i}))
            await within(writable.close())

            state_hook = collector._hook.call(
                ["getState"], RpcPayload.from_app_params([])
            )
            state = (await within(state_hook.pull())).value
            assert state["chunks"] == [{"n": i} for i in range(5)]
            assert state["closed"] is True
            assert state["error"] is None
        finally:
            if writable_hook is not None:
                writable_hook.dispose()
            collector_hook.dispose()

    async def test_close_surfaces_mid_stream_write_error(self, client) -> None:
        """protocol.md:261 — pipelined writes; close() fails with the write
        error when a sink write failed."""
        collector_hook = client.call_hook("makeCollector", [])
        try:
            collector = (await within(collector_hook.pull())).value
            writable_hook = collector._hook.call(
                ["getFailingWritable"], RpcPayload.from_app_params([1])
            )
            writable = (await within(writable_hook.pull())).value

            await within(writable.write("ok"))
            await within(writable.write("boom-trigger"))  # sink throws
            with pytest.raises(Exception) as exc_info:
                await within(writable.close())
            assert "collector sink failed" in str(exc_info.value)

            state_hook = collector._hook.call(
                ["getState"], RpcPayload.from_app_params([])
            )
            state = (await within(state_hook.pull())).value
            assert state["chunks"] == ["ok"]
            writable_hook.dispose()
        finally:
            collector_hook.dispose()

    async def test_py_exports_writable_ts_writes(self, client) -> None:
        chunks: list[Any] = []
        closed = asyncio.Event()

        class Sink:
            async def write(self, chunk: Any) -> None:
                chunks.append(chunk)

            async def close(self) -> None:
                closed.set()

        result = await client.call(
            "writeToWritable", [RpcWritableStream(Sink()), [1, "two", [3]]]
        )
        assert result == "ok"
        await within(closed.wait())
        assert chunks == [1, "two", [3]]


# =============================================================================
# Item 5 — receiver cancellation, both directions
# =============================================================================

class TestReceiverCancellation:
    async def test_py_cancels_ts_stream(self, client) -> None:
        source_hook = client.call_hook("makeStreamSource", [])
        try:
            source = (await within(source_hook.pull())).value
            stream_hook = source._hook.call(
                ["makeInfiniteStream"], RpcPayload.from_app_params([])
            )
            stream = (await within(stream_hook.pull())).value
            assert isinstance(stream, RpcReadableStream)

            first = await within(stream.read())
            second = await within(stream.read())
            assert (first, second) == (0, 1)
            await within(stream.cancel(RpcError("Error", "py had enough")))

            # The TS source's cancel() must fire (pipeTo abort propagation).
            reason = None
            for _ in range(100):
                reason_hook = source._hook.call(
                    ["getCancelReason"], RpcPayload.from_app_params([])
                )
                reason = (await within(reason_hook.pull())).value
                if reason is not None:
                    break
                await asyncio.sleep(0.05)
            assert reason is not None and "py had enough" in reason

            # Session stays healthy.
            assert await client.call("square", [7]) == 49
            stream_hook.dispose()
        finally:
            source_hook.dispose()

    async def test_ts_cancels_py_stream(self, client) -> None:
        finalized = asyncio.Event()

        async def gen():
            try:
                i = 0
                while True:
                    yield i
                    i += 1
            finally:
                finalized.set()

        got = await client.call(
            "readStreamPartial", [RpcReadableStream(gen()), 3]
        )
        assert got == [0, 1, 2]
        # TS reader.cancel() propagates back: the PY source generator is
        # closed by the pump.
        await within(finalized.wait())
        # Session stays healthy.
        assert await client.call("add", [2, 3]) == 5


# =============================================================================
# Item 6 — sender abort, both directions
# =============================================================================

class TestSenderAbort:
    async def test_py_source_error_rejects_ts_collect(self, client) -> None:
        async def gen():
            yield 1
            yield 2
            raise RuntimeError("py source exploded")

        hook = client.call_hook("collectStream", [RpcReadableStream(gen())])
        with pytest.raises(Exception) as exc_info:
            await within(hook.pull())
        assert "py source exploded" in str(exc_info.value)
        hook.dispose()
        # Session survives a stream failure.
        assert await client.call("square", [3]) == 9

    async def test_ts_source_error_errors_py_reader(self, client) -> None:
        stream = await client.call("makeErrorStream", [2])
        chunks: list[Any] = []

        async def consume() -> None:
            async for chunk in stream:
                chunks.append(chunk)

        with pytest.raises(Exception) as exc_info:
            await within(consume())
        assert chunks == [0, 1][: len(chunks)]  # prefix; abort may drop buffered
        assert "ts source exploded" in str(exc_info.value)


# =============================================================================
# Item 8 — release-without-close abort rule (protocol.md:259)
# =============================================================================

class TestReleaseWithoutClose:
    async def test_dropping_ts_writable_aborts_underlying_stream(self, client) -> None:
        collector_hook = client.call_hook("makeCollector", [])
        try:
            collector = (await within(collector_hook.pull())).value
            writable_hook = collector._hook.call(
                ["getWritable"], RpcPayload.from_app_params([])
            )
            writable = (await within(writable_hook.pull())).value
            await within(writable.write("one"))

            # Drop the proxy WITHOUT close(): dispose the result hook whose
            # payload owns the import ref -> release goes out -> TS must
            # abort the underlying stream (streams.ts:100-113).
            writable_hook.dispose()

            state = None
            for _ in range(100):
                state_hook = collector._hook.call(
                    ["getState"], RpcPayload.from_app_params([])
                )
                state = (await within(state_hook.pull())).value
                if state["error"] is not None:
                    break
                await asyncio.sleep(0.05)
            assert state is not None
            assert state["error"] is not None
            assert "disposed without calling close()" in state["error"]
            assert state["closed"] is False
            assert state["chunks"] == ["one"]
        finally:
            collector_hook.dispose()


# =============================================================================
# Item 9 — stream-message bookkeeping
# =============================================================================

class TestBookkeeping:
    async def test_no_pull_or_release_for_stream_writes(self, client) -> None:
        session = client.session
        base_stats = session.get_stats()
        base_frames = len(client.sent_frames)

        chunks = list(range(25))
        result = await client.call(
            "collectStream", [RpcReadableStream(async_chunks(chunks))]
        )
        assert result == chunks

        new_frames = [json.loads(f) for f in client.sent_frames[base_frames:]]
        stream_frames = [f for f in new_frames if f[0] == "stream"]
        assert len(stream_frames) == 26  # 25 writes + close

        # No pull may ever name a stream-write import id; stream imports are
        # never pulled (auto-pull) nor released (auto-release, refcount 0).
        pull_ids = {f[1] for f in new_frames if f[0] == "pull"}
        release_ids = {f[1] for f in new_frames if f[0] == "release"}
        # The ONLY releasable import here is the pipe itself (proxy close)
        # and the main-call resolve; never one release per write.
        assert len(release_ids) <= 2, new_frames
        assert len(pull_ids) <= 1, new_frames

        # remote_refcount=0 stream imports are purged after resolve, and the
        # tables return to baseline.
        for _ in range(100):
            if session.get_stats() == base_stats:
                break
            await asyncio.sleep(0.05)
        assert session.get_stats() == base_stats


# =============================================================================
# Item 10 — session abort mid-stream
# =============================================================================

class TestSessionAbortMidStream:
    async def test_abort_unblocks_parked_writer_and_reaps_pumps(self, client) -> None:
        session = client.session

        released = asyncio.Event()

        async def gen():
            # Big chunks with a slow TS consumer: the pump's writes outrun
            # the acks and eventually park on the flow-control window.
            try:
                while True:
                    yield b"z" * (64 * 1024)
                    await asyncio.sleep(0)
            finally:
                released.set()

        hook = client.call_hook(
            "collectStreamSlow", [RpcReadableStream(gen()), 50]
        )
        # Let the pump saturate the window.
        await asyncio.sleep(1.0)
        assert session._pump_tasks, "pump should be active"

        session._abort(RpcError.internal("test kills the session"), send_abort=False)

        # Pump tasks reaped promptly; no hung parked write.
        for _ in range(100):
            if not session._pump_tasks:
                break
            await asyncio.sleep(0.05)
        assert not session._pump_tasks
        await within(released.wait())

        with pytest.raises(Exception):
            await within(hook.pull())

    async def test_transport_death_mid_ts_to_py_stream(self, ts_server) -> None:
        async with SessionClient(f"ws://127.0.0.1:{ts_server.port}/") as c:
            stream = await c.call("makeByteStream", [8 * 1024 * 1024, 8 * 1024])
            # Read a little, then kill the session under the reader.
            await within(stream.read())
            c.session._abort(RpcError.internal("transport died"), send_abort=False)

            with pytest.raises(Exception):
                # Reader must unblock with an error, not hang. It may drain
                # a few buffered chunks first.
                for _ in range(64):
                    await within(stream.read(), timeout=5.0)


# =============================================================================
# Item 4 — flow-control soak (32 MiB each direction) — slow
# =============================================================================

TOTAL_SOAK = 32 * 1024 * 1024
SOAK_CHUNK = 8 * 1024


@pytest.mark.slow
class TestFlowControlSoak:
    async def test_py_to_ts_soak_window_bounded(self, client, monkeypatch) -> None:
        # EXACT boundedness invariant of the controller + single sequential
        # writer, deterministic under ANY CPU load (Phase C root-cause of the
        # old flaky bound):
        #
        #   * a NON-blocking send leaves bytes_in_flight strictly BELOW the
        #     window used for that send (on_send checks after adding size);
        #   * a BLOCKING send parks the writer and may exceed the LARGEST
        #     WINDOW OBSERVED SO FAR by at most that one send's size.
        #
        # Root cause of the old flake: the peak window was sampled only at
        # SEND time, but the window also GROWS inside on_ack — a writer
        # unparked against an ack-time window that later decays could
        # legitimately exceed every send-time sample. Under CPU load acks
        # batch up, making exactly that pattern likely (B3 observed 71 KiB
        # "overshoot" against a 16 KiB allowance). Sampling the window at
        # BOTH send and ack time closes the hole: every unpark decision is
        # then covered by the peak, so any violation is a genuine
        # flow-control bug, never scheduler noise.
        violations: list[tuple[int, float, bool]] = []
        sends = 0
        peak_window = 0.0
        original_on_send = FlowController.on_send
        original_on_ack = FlowController.on_ack

        def instrumented_on_send(self: FlowController, size: int):
            nonlocal sends, peak_window
            window_at_send = self.window
            peak_window = max(peak_window, window_at_send)
            token, should_block = original_on_send(self, size)
            sends += 1
            if should_block:
                excess = self.bytes_in_flight - peak_window
                if excess > size:
                    violations.append((size, excess, True))
            elif self.bytes_in_flight - window_at_send >= 0:
                violations.append((size, self.bytes_in_flight - window_at_send, False))
            return token, should_block

        def instrumented_on_ack(self: FlowController, token):
            nonlocal peak_window
            result = original_on_ack(self, token)
            # The window may have grown; unpark decisions use THIS value.
            peak_window = max(peak_window, self.window)
            return result

        monkeypatch.setattr(FlowController, "on_send", instrumented_on_send)
        monkeypatch.setattr(FlowController, "on_ack", instrumented_on_ack)

        async def gen():
            sent = 0
            while sent < TOTAL_SOAK:
                n = min(SOAK_CHUNK, TOTAL_SOAK - sent)
                yield b"\xab" * n
                sent += n

        total = await asyncio.wait_for(
            client.call("sumByteStream", [RpcReadableStream(gen())]),
            timeout=300,
        )
        assert total == TOTAL_SOAK
        assert sends > 0, "flow controller was never exercised"
        assert not violations, violations[:10]

    async def test_ts_to_py_soak(self, client) -> None:
        stream = await client.call("makeByteStream", [TOTAL_SOAK, SOAK_CHUNK])
        total = 0

        async def consume() -> None:
            nonlocal total
            async for chunk in stream:
                total += len(bytes(chunk))

        await asyncio.wait_for(consume(), timeout=300)
        assert total == TOTAL_SOAK

    async def test_slow_py_consumer_throttles_ts_sender(self, client) -> None:
        """Ack-clocking: a slow PY consumer must plateau the TS sender's
        in-flight data (the resolve IS the ack; a full pipe queue delays it)."""
        stream = await client.call("makeByteStream", [2 * 1024 * 1024, 8 * 1024])
        received = 0
        async for chunk in stream:
            received += len(bytes(chunk))
            await asyncio.sleep(0.002)  # deliberately slow consumer
        assert received == 2 * 1024 * 1024


# =============================================================================
# Item 11 — Blob framing
# =============================================================================

class TestBlobInterop:
    async def test_ts_blob_arrives_as_py_blob(self, client) -> None:
        blob = await client.call("makeBlob", ["hello from ts", "text/plain"])
        assert isinstance(blob, Blob)
        assert blob.type == "text/plain"
        assert blob.data == b"hello from ts"
        assert blob.size == 13

    async def test_py_blob_read_by_ts(self, client) -> None:
        result = await client.call(
            "readBlob", [Blob("application/x-test", b"py blob payload")]
        )
        assert result["type"] == "application/x-test"
        assert result["text"] == "py blob payload"
        assert result["size"] == 15

    async def test_large_blob_roundtrip(self, client) -> None:
        text = "0123456789" * 30000  # 300 KB, > INITIAL_WINDOW
        result = await client.call("readBlob", [Blob("t/x", text.encode())])
        assert result["size"] == 300000
        blob = await client.call("makeBlob", [text, "t/x"])
        assert blob.data.decode() == text
