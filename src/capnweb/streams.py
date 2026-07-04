"""Streams subsystem: RpcReadableStream / RpcWritableStream / flow control.

Python port of the Cap'n Web stream support (upstream PR #132), parity
stream B1. TS source of truth:

* ``streams.ts`` (whole file): WritableStreamStubHook (:20-119),
  FlowController (:166-307), createWritableStreamFromHook (:312-425),
  ReadableStreamStubHook (:443-520).
* ``rpc.ts``: sendStream (:786-822), createPipe (:684-705),
  getPipeReadable (:674-682), estimateEncodedSize (:95-163),
  stream/pipe dispatch (:966-993).
* ``serialize.ts``: writable/readable devaluate (:499-519), evaluate
  (:977-1000); protocol.md:103-121, 249-269.

Python type mapping (locked decision D5): no WHATWG streams here —
``RpcReadableStream`` is an async iterator, ``RpcWritableStream`` an async
writer object with ``write``/``close``/``abort`` and async-context-manager
support.

Policy (design doc 03-streams.md §3.4): bare async generators do NOT
auto-stream; the sending application must wrap them in ``RpcReadableStream``
explicitly. Likewise arbitrary sink objects must be wrapped in
``RpcWritableStream`` to be sent as ``["writable", id]``.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterable, AsyncIterator, Awaitable, Callable

from capnweb.error import RpcError
from capnweb.hooks import (
    ErrorStubHook,
    PayloadStubHook,
    PromiseStubHook,
    StubHook,
)
from capnweb.payload import RpcPayload

__all__ = [
    "DECAY_FACTOR",
    "INITIAL_WINDOW",
    "MAX_WINDOW",
    "MIN_WINDOW",
    "STARTUP_EXIT_ROUNDS",
    "STARTUP_GROWTH_FACTOR",
    "STEADY_GROWTH_FACTOR",
    "FlowController",
    "ReadableStreamGuardHook",
    "RpcReadableStream",
    "RpcWritableStream",
    "SendToken",
    "WritableStreamHook",
    "estimate_encoded_size",
    "hook_stream",
]

# ---------------------------------------------------------------------------
# Background task bookkeeping (gate: no create_task without a retained ref)
# ---------------------------------------------------------------------------

_background_tasks: set[asyncio.Task[Any]] = set()


def _spawn_background(coro: Awaitable[Any]) -> asyncio.Task[Any] | None:
    """Schedule a fire-and-forget coroutine with a retained reference.

    Exceptions are consumed (these are best-effort cleanup actions like
    ``abort``/``cancel`` on disposal, whose failures TS also ignores).
    Returns None (closing the coroutine) if no event loop is running.
    """
    try:
        task = asyncio.ensure_future(coro)
    except RuntimeError:
        coro.close()  # type: ignore[attr-defined]
        return None
    _background_tasks.add(task)

    def _done(t: asyncio.Task[Any]) -> None:
        _background_tasks.discard(t)
        if not t.cancelled():
            t.exception()  # consume, per TS `.catch(() => {})`

    task.add_done_callback(_done)
    return task


# ---------------------------------------------------------------------------
# estimate_encoded_size — port of rpc.ts:95-163
# ---------------------------------------------------------------------------

ESTIMATED_OBJECT_OVERHEAD = 16
ESTIMATED_ENTRY_OVERHEAD = 8
ESTIMATED_BINARY_OVERHEAD = 16
MAX_ESTIMATE_DEPTH = 64


def _estimate_string_size(value: str) -> int:
    # Bias high: UTF-8 uses up to 3 bytes for BMP code points (rpc.ts:95-99).
    return 2 + len(value) * 3


def estimate_encoded_size(
    value: Any, _seen: set[int] | None = None, _depth: int = 0
) -> int:
    """Rough encoded-size estimate for flow control (rpc.ts:101-163).

    Fallback for when the exact serialized frame length is unavailable.
    Cycle-safe: containers are deduped by identity across the whole
    traversal (shared substructure under-counts slightly, which is fine for
    a flow-control estimate that otherwise biases high).
    """
    if _depth >= MAX_ESTIMATE_DEPTH:
        return ESTIMATED_ENTRY_OVERHEAD

    if isinstance(value, str):
        return _estimate_string_size(value)
    if isinstance(value, bool):
        return 8
    if isinstance(value, (int, float)):
        return 16
    if value is None:
        return 8
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ESTIMATED_BINARY_OVERHEAD + len(value)
    # Blob (defined in types.py; avoid import cycle with a duck check).
    blob_data = getattr(value, "data", None)
    if isinstance(blob_data, bytes) and type(value).__name__ == "Blob":
        return ESTIMATED_BINARY_OVERHEAD + len(blob_data)
    if isinstance(value, datetime):
        return 16

    if isinstance(value, (list, tuple, dict)) or isinstance(value, BaseException):
        # `_seen` is only ever added to, never removed — dedupe by object
        # identity across the entire traversal (rpc.ts:118-127).
        if _seen is None:
            _seen = set()
        if id(value) in _seen:
            return ESTIMATED_ENTRY_OVERHEAD
        _seen.add(id(value))

        if isinstance(value, (list, tuple)):
            size = ESTIMATED_OBJECT_OVERHEAD
            for item in value:
                size += ESTIMATED_ENTRY_OVERHEAD + estimate_encoded_size(
                    item, _seen, _depth + 1
                )
            return size

        if isinstance(value, BaseException):
            name = getattr(value, "name", type(value).__name__)
            message = getattr(value, "message", str(value))
            stack = getattr(value, "stack", None) or ""
            size = (
                ESTIMATED_OBJECT_OVERHEAD
                + _estimate_string_size(str(name))
                + _estimate_string_size(str(message))
                + _estimate_string_size(str(stack))
            )
            for key, val in (getattr(value, "properties", None) or {}).items():
                size += (
                    ESTIMATED_ENTRY_OVERHEAD
                    + _estimate_string_size(key)
                    + estimate_encoded_size(val, _seen, _depth + 1)
                )
            return size

        size = ESTIMATED_OBJECT_OVERHEAD
        for key, val in value.items():
            size += (
                ESTIMATED_ENTRY_OVERHEAD
                + _estimate_string_size(str(key))
                + estimate_encoded_size(val, _seen, _depth + 1)
            )
        return size

    return 16


# ---------------------------------------------------------------------------
# FlowController — numerically-identical port of streams.ts:166-307
# ---------------------------------------------------------------------------

# Flow control constants (streams.ts:139-151). Both peers run the algorithm
# independently, so these MUST match TS exactly.
INITIAL_WINDOW = 256 * 1024
MAX_WINDOW = 1024 * 1024 * 1024
MIN_WINDOW = 64 * 1024
STARTUP_GROWTH_FACTOR = 2
STEADY_GROWTH_FACTOR = 1.25
DECAY_FACTOR = 0.90
STARTUP_EXIT_ROUNDS = 3


@dataclass(slots=True)
class SendToken:
    """Send-time snapshot passed back to on_ack (streams.ts:155-162)."""

    sent_time: float
    size: int
    delivered_at_send: int
    delivered_time_at_send: float
    window_at_send: float
    window_full_at_send: bool


class FlowController:
    """BDP-based dynamic flow control for stream writes (streams.ts:166-307).

    Pure computation — no asyncio inside. Time comes from an injectable
    clock (defaults to ``time.monotonic``; units cancel in
    ``bandwidth * min_rtt`` so seconds work as well as TS's milliseconds).
    """

    __slots__ = (
        "window",
        "bytes_in_flight",
        "in_startup_phase",
        "_now",
        "_delivered",
        "_delivered_time",
        "_first_ack_time",
        "_first_ack_delivered",
        "_min_rtt",
        "_rounds_without_increase",
        "_last_round_window",
        "_round_start_time",
    )

    def __init__(self, now: Callable[[], float] | None = None) -> None:
        self._now = now if now is not None else time.monotonic
        # The current window size in bytes; sender blocks when
        # bytes_in_flight >= window.
        self.window: float = INITIAL_WINDOW
        # Total bytes currently in flight (sent but not yet acked).
        self.bytes_in_flight: int = 0
        # Whether we're still in the startup phase.
        self.in_startup_phase: bool = True

        # ----- BDP estimation state -----
        self._delivered = 0
        self._delivered_time = 0.0
        self._first_ack_time = 0.0
        self._first_ack_delivered = 0
        self._min_rtt = math.inf
        self._rounds_without_increase = 0
        self._last_round_window = 0.0
        self._round_start_time = 0.0

    def on_send(self, size: int) -> tuple[SendToken, bool]:
        """Register a write of ``size`` bytes about to be sent.

        Returns ``(token, should_block)`` — the token must be passed to
        on_ack/on_error when the write settles (streams.ts:199-212).
        """
        self.bytes_in_flight += size

        token = SendToken(
            sent_time=self._now(),
            size=size,
            delivered_at_send=self._delivered,
            delivered_time_at_send=self._delivered_time,
            window_at_send=self.window,
            window_full_at_send=self.bytes_in_flight >= self.window,
        )
        return token, token.window_full_at_send

    def on_error(self, token: SendToken) -> None:
        """A previously-sent write failed: restore bytes_in_flight only
        (streams.ts:216-218)."""
        self.bytes_in_flight -= token.size

    def on_ack(self, token: SendToken) -> bool:
        """An ack arrived: update BDP estimates and the window
        (streams.ts:222-306). Returns whether a blocked sender should
        unblock."""
        ack_time = self._now()

        # Update delivery tracking metrics.
        self._delivered += token.size
        self._delivered_time = ack_time
        self.bytes_in_flight -= token.size

        # Update RTT estimate.
        rtt = ack_time - token.sent_time
        self._min_rtt = min(self._min_rtt, rtt)

        # Update bandwidth estimate and window.
        if self._first_ack_time == 0:
            # Very first ack: can't estimate bandwidth yet — we need the
            # interval between acks (streams.ts:235-239).
            self._first_ack_time = ack_time
            self._first_ack_delivered = self._delivered
        else:
            if token.delivered_time_at_send == 0:
                # Sent before any acks had been received, but wasn't the very
                # first write: estimate starting from the first ack.
                base_time = self._first_ack_time
                base_delivered = self._first_ack_delivered
            else:
                base_time = token.delivered_time_at_send
                base_delivered = token.delivered_at_send

            interval = ack_time - base_time
            bytes_delivered = self._delivered - base_delivered
            bandwidth = bytes_delivered / interval

            growth_factor = (
                STARTUP_GROWTH_FACTOR if self.in_startup_phase else STEADY_GROWTH_FACTOR
            )

            # Target = BDP plus growth headroom to probe for more bandwidth.
            new_window = bandwidth * self._min_rtt * growth_factor

            # Collar: grow by at most growth_factor per RTT.
            new_window = min(new_window, token.window_at_send * growth_factor)

            if token.window_full_at_send:
                # Don't allow the window to shrink too quickly.
                new_window = max(new_window, token.window_at_send * DECAY_FACTOR)
            else:
                # App-limited: never shrink at all. Clamp to self.window (not
                # window_at_send) so previous shrinkage isn't undone
                # (streams.ts:275-281).
                new_window = max(new_window, self.window)

            # Clamp to min/max.
            self.window = max(min(new_window, MAX_WINDOW), MIN_WINDOW)

            # Startup exit detection (streams.ts:287-302).
            if self.in_startup_phase and token.sent_time >= self._round_start_time:
                if self.window > self._last_round_window * STEADY_GROWTH_FACTOR:
                    self._rounds_without_increase = 0
                else:
                    self._rounds_without_increase += 1
                    if self._rounds_without_increase >= STARTUP_EXIT_ROUNDS:
                        self.in_startup_phase = False

                self._round_start_time = ack_time
                self._last_round_window = self.window

        return self.bytes_in_flight < self.window


# ---------------------------------------------------------------------------
# hook_stream — StubHook.stream() semantics without owning hooks.py
# ---------------------------------------------------------------------------


def hook_stream(
    hook: StubHook, path: list[str | int], args: RpcPayload
) -> tuple[Awaitable[None], int | None]:
    """Dispatch a streaming call on any hook (core.ts:216-231 semantics).

    Thin alias for ``hook.stream(path, args)``: B3 folded the default
    implementation into the ``StubHook`` base class (call()+pull(),
    size=None ⇒ serialized writes) with overrides on ImportHook (wire
    ``stream`` message + frame size), WritableStreamHook (vtable write) and
    PromiseStubHook (deep-copy, await, re-dispatch; core.ts:1924-1934), so
    the old getattr fallback machinery here is gone (Phase C sweep of the
    2026-07-04 B1→B3 handoff).
    """
    return hook.stream(path, args)


# ---------------------------------------------------------------------------
# _PipeChannel — the Python analogue of an identity TransformStream
# ---------------------------------------------------------------------------

# Receiver-side buffering only: the *wire* backpressure is the sender's
# FlowController; this queue just decouples the read loop from the consumer.
# `await put()` inside a delivered write() delays that write's resolve —
# which IS the flow-control ack — so a slow consumer throttles the remote
# sender with no extra machinery (design doc §3.1).
PIPE_BUFFER_CHUNKS = 16


class _EndOfStream(Exception):
    """Internal sentinel: clean end of a pipe channel."""


class _PipeChannel:
    """Bounded FIFO with close/error semantics, mirroring an identity
    TransformStream: ``fail()`` drops buffered chunks and wakes both sides,
    like WHATWG abort/cancel."""

    __slots__ = ("_buffer", "_maxsize", "_closed", "_error", "_getters", "_putters")

    def __init__(self, maxsize: int = PIPE_BUFFER_CHUNKS) -> None:
        self._buffer: deque[Any] = deque()
        self._maxsize = maxsize
        self._closed = False
        self._error: Exception | None = None
        self._getters: deque[asyncio.Future[Any]] = deque()
        self._putters: deque[tuple[asyncio.Future[None], Any]] = deque()

    @property
    def broken(self) -> Exception | None:
        return self._error

    async def put(self, item: Any) -> None:
        if self._error is not None:
            raise self._error
        if self._closed:
            raise RpcError("Error", "Cannot write to a closed stream.")

        # Direct handoff to a waiting reader.
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                getter.set_result(item)
                return

        if len(self._buffer) < self._maxsize:
            self._buffer.append(item)
            return

        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._putters.append((fut, item))
        try:
            await fut
        except asyncio.CancelledError:
            with suppress(ValueError):
                self._putters.remove((fut, item))
            raise

    async def get(self) -> Any:
        if self._buffer:
            item = self._buffer.popleft()
            self._wake_one_putter()
            return item
        if self._error is not None:
            raise self._error
        if self._closed:
            raise _EndOfStream()

        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._getters.append(fut)
        try:
            return await fut
        except asyncio.CancelledError:
            with suppress(ValueError):
                self._getters.remove(fut)
            raise

    def _wake_one_putter(self) -> None:
        while self._putters and len(self._buffer) < self._maxsize:
            fut, item = self._putters.popleft()
            if not fut.done():
                self._buffer.append(item)
                fut.set_result(None)

    def close(self) -> None:
        """Clean end (writer close). Buffered chunks stay readable."""
        if self._closed or self._error is not None:
            return
        self._closed = True
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                getter.set_exception(_EndOfStream())
        # Order-chained writes mean no put can still be pending at close; if
        # one is (peer bug), fail it loudly.
        while self._putters:
            fut, _item = self._putters.popleft()
            if not fut.done():
                fut.set_exception(
                    RpcError("Error", "Cannot write to a closed stream.")
                )

    def fail(self, error: Exception) -> None:
        """Error the channel (writer abort or reader cancel).

        Buffered chunks are discarded and both sides wake with the error,
        mirroring WHATWG TransformStream abort/cancel semantics.
        """
        if self._error is not None:
            return
        self._error = error
        self._buffer.clear()
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                getter.set_exception(error)
        while self._putters:
            fut, _item = self._putters.popleft()
            if not fut.done():
                fut.set_exception(error)


class _ChannelSink:
    """Writable sink feeding a _PipeChannel (the ["pipe"] receive side)."""

    __slots__ = ("_channel",)

    def __init__(self, channel: _PipeChannel) -> None:
        self._channel = channel

    async def write(self, chunk: Any = None) -> None:
        await self._channel.put(chunk)

    async def close(self) -> None:
        self._channel.close()

    async def abort(self, reason: Any = None) -> None:
        self._channel.fail(_to_exception(reason, "WritableStream was aborted"))


def _to_exception(reason: Any, default_message: str) -> Exception:
    if isinstance(reason, Exception):
        return reason
    if reason is None:
        return RpcError("Error", default_message)
    # Error expressions delivered over RPC arrive as RpcStub(ErrorStubHook);
    # unwrap them so abort reasons stay real exceptions.
    hook = getattr(reason, "_hook", None)
    error = getattr(hook, "error", None)
    if isinstance(error, Exception):
        return error
    return RpcError("Error", str(reason))


# ---------------------------------------------------------------------------
# RpcReadableStream
# ---------------------------------------------------------------------------


class RpcReadableStream:
    """A readable stream of RPC values (async iterator).

    Two roles:

    * **Sending**: wrap any ``AsyncIterator``/``AsyncIterable`` and place the
      wrapper in RPC params or a return value — it is serialized as a pipe +
      ``["readable", id]``. Bare async generators are deliberately NOT
      auto-streamed (§3.4): wrap explicitly.
    * **Receiving**: what the application gets when the peer sends a
      ReadableStream; iterate it (``async for`` / ``read()``) or ``cancel()``.
    """

    __slots__ = (
        "_source",
        "_iterator",
        "_channel",
        "_consuming",
        "_cancelled",
        "_done",
    )

    def __init__(self, source: AsyncIterator[Any] | AsyncIterable[Any]) -> None:
        if isinstance(source, RpcReadableStream):
            raise TypeError("RpcReadableStream is already a stream; don't re-wrap it.")
        if not hasattr(source, "__anext__") and not hasattr(source, "__aiter__"):
            raise TypeError(
                "RpcReadableStream requires an AsyncIterator or AsyncIterable, "
                f"got {type(source).__name__}. (Sync iterables and generator "
                "functions are not accepted; pass an async iterator.)"
            )
        self._source: Any = source
        self._iterator: AsyncIterator[Any] | None = None
        self._channel: _PipeChannel | None = None
        self._consuming = False
        self._cancelled: Exception | None = None
        self._done = False

    @classmethod
    def _for_pipe(cls, channel: _PipeChannel) -> "RpcReadableStream":
        """Internal: readable end of a ["pipe"] (receive side)."""
        stream = cls.__new__(cls)
        stream._source = None
        stream._iterator = None
        stream._channel = channel
        stream._consuming = False
        stream._cancelled = None
        stream._done = False
        return stream

    # -- consumption ---------------------------------------------------------

    def __aiter__(self) -> "RpcReadableStream":
        return self

    async def __anext__(self) -> Any:
        return await self.read()

    async def read(self) -> Any:
        """Read the next chunk.

        Raises StopAsyncIteration on clean end; raises the abort/cancel
        error if the stream failed.
        """
        self._consuming = True
        if self._cancelled is not None:
            raise self._cancelled
        if self._done:
            raise StopAsyncIteration

        if self._channel is not None:
            try:
                return await self._channel.get()
            except _EndOfStream:
                self._done = True
                raise StopAsyncIteration from None

        try:
            return await self._get_iterator().__anext__()
        except StopAsyncIteration:
            self._done = True
            raise

    def _get_iterator(self) -> AsyncIterator[Any]:
        if self._iterator is None:
            source = self._source
            if hasattr(source, "__anext__"):
                self._iterator = source
            else:
                self._iterator = source.__aiter__()
        return self._iterator

    # -- cancellation / disposal ----------------------------------------------

    async def cancel(self, reason: Exception | None = None) -> None:
        """Receiver-side cancellation (WHATWG ReadableStream.cancel analog).

        For pipe-backed streams the error propagates to the remote writer:
        its next ``write()`` rejects with this reason (carried in the reject
        of the in-flight ``["stream"]`` message — no dedicated cancel frame,
        design doc §1.5).
        """
        if self._cancelled is not None or self._done:
            return
        error = _to_exception(reason, "ReadableStream was canceled")
        self._cancelled = error
        if self._channel is not None:
            self._channel.fail(error)
            return
        # Source mode: close the underlying async generator/iterator.
        iterator = self._iterator if self._iterator is not None else self._source
        aclose = getattr(iterator, "aclose", None)
        if aclose is not None:
            with suppress(Exception):
                await aclose()

    def _acquire_for_pump(self) -> None:
        """Lock for pumping (TS pipeTo lock analog): guard-hook disposal
        must not cancel a stream that is being pumped."""
        self._consuming = True

    def _dispose_unconsumed(self) -> None:
        """Called by ReadableStreamGuardHook at refcount 0 (streams.ts:491-515).

        Cancels the stream only if nobody has started consuming it — if
        someone is consuming (or pumping), they own full consumption.
        """
        if self._consuming or self._cancelled is not None or self._done:
            return
        _spawn_background(
            self.cancel(
                RpcError(
                    "Error",
                    "ReadableStream RPC stub was disposed without being consumed",
                )
            )
        )

    def __repr__(self) -> str:
        kind = "pipe" if self._channel is not None else "source"
        return f"RpcReadableStream({kind}, consuming={self._consuming})"


# ---------------------------------------------------------------------------
# RpcWritableStream
# ---------------------------------------------------------------------------


class RpcWritableStream:
    """A writable stream of RPC values.

    Two roles:

    * **Proxy** (``["writable", id]`` received from the peer): ``write()``
      forwards over RPC with FlowController backpressure — the numerically
      faithful port of createWritableStreamFromHook (streams.ts:312-425).
    * **Local** (application-constructed around a sink object exposing
      ``async write(chunk)`` and optionally ``async close()`` /
      ``async abort(reason)``): sendable to the peer as ``["writable", id]``.

    ``write()`` calls must be sequential; an internal ``asyncio.Lock``
    enforces this (the TS design parks at most ONE writer because WHATWG
    serializes write() calls — design doc §3.3).
    """

    __slots__ = (
        "_hook",
        "_sink",
        "_lock",
        "_fc",
        "_pending_error",
        "_hook_disposed",
        "_window_future",
        "_write_tasks",
        "_locked_for_export",
        "_closed",
    )

    def __init__(self, sink: Any) -> None:
        if not callable(getattr(sink, "write", None)):
            raise TypeError(
                "RpcWritableStream sink must have an async write(chunk) method, "
                f"got {type(sink).__name__}"
            )
        self._sink: Any = sink
        self._hook: StubHook | None = None
        self._init_common()

    @classmethod
    def _from_hook(cls, hook: StubHook) -> "RpcWritableStream":
        """Internal: proxy over a remote hook (streams.ts:312)."""
        stream = cls.__new__(cls)
        stream._sink = None
        stream._hook = hook
        stream._init_common()
        return stream

    def _init_common(self) -> None:
        self._lock = asyncio.Lock()
        self._fc = FlowController()
        self._pending_error: Exception | None = None
        self._hook_disposed = False
        self._window_future: asyncio.Future[None] | None = None
        self._write_tasks: set[asyncio.Task[Any]] = set()
        self._locked_for_export = False
        self._closed = False

    # -- public API ------------------------------------------------------------

    async def write(self, chunk: Any) -> None:
        """Write one chunk. Applies flow control; raises any latched error."""
        async with self._lock:
            self._check_usable()
            if self._pending_error is not None:
                raise self._pending_error
            if self._hook is not None:
                await self._write_remote(chunk)
            else:
                await self._write_local(chunk)

    async def close(self) -> None:
        """Close the stream; prefers a latched write error over the close
        error (streams.ts:387-407)."""
        async with self._lock:
            self._check_usable()
            self._closed = True
            if self._hook is not None:
                await self._close_remote()
            else:
                await self._close_local()

    async def abort(self, reason: Exception | None = None) -> None:
        """Abort the stream (streams.ts:409-423).

        Deliberately NOT under the write lock: abort must be able to
        interrupt a write parked on backpressure.
        """
        self._check_usable()
        await self._abort_internal(reason)

    async def __aenter__(self) -> "RpcWritableStream":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc is not None:
            await self.abort(exc if isinstance(exc, Exception) else None)
        else:
            await self.close()

    def _check_usable(self) -> None:
        if self._locked_for_export:
            raise RpcError(
                "Error",
                "This RpcWritableStream was sent over RPC and is locked; the "
                "remote peer now owns it.",
            )

    # -- proxy (remote) implementation ------------------------------------------

    async def _write_remote(self, chunk: Any) -> None:
        assert self._hook is not None
        payload = RpcPayload.from_app_params([chunk])
        awaitable, size = hook_stream(self._hook, ["write"], payload)

        if size is None:
            # Local call — await directly to serialize writes; latch errors
            # (streams.ts:339-347).
            try:
                await awaitable
            except Exception as err:
                if self._pending_error is None:
                    self._pending_error = err
                raise
            return

        # Remote call — window-based flow control (streams.ts:348-384).
        token, should_block = self._fc.on_send(size)
        task = asyncio.ensure_future(awaitable)
        self._write_tasks.add(task)
        task.add_done_callback(
            lambda t, tok=token: self._on_write_settled(t, tok)
        )

        if should_block:
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[None] = loop.create_future()
            self._window_future = fut
            await fut

    def _on_write_settled(self, task: asyncio.Task[Any], token: SendToken) -> None:
        self._write_tasks.discard(task)
        err: Exception | None
        if task.cancelled():
            err = RpcError("Error", "stream write was cancelled")
        else:
            err = task.exception()  # type: ignore[assignment]

        if err is None:
            has_capacity = self._fc.on_ack(token)
            if has_capacity and self._window_future is not None:
                fut = self._window_future
                self._window_future = None
                if not fut.done():
                    fut.set_result(None)
        else:
            self._fc.on_error(token)
            if self._pending_error is None:
                self._pending_error = err
                self._dispose_hook()
            # Unblock any write parked on backpressure — reject it so the
            # stream finishes erroring instead of hanging (streams.ts:368-374).
            if self._window_future is not None:
                fut = self._window_future
                self._window_future = None
                if not fut.done():
                    fut.set_exception(err)

    async def _close_remote(self) -> None:
        assert self._hook is not None
        if self._pending_error is not None:
            self._dispose_hook()
            raise self._pending_error

        # Send close() WITHOUT awaiting pending writes: the protocol
        # guarantees in-order processing and that close() fails if any prior
        # write failed (protocol.md:261).
        awaitable, _size = hook_stream(
            self._hook, ["close"], RpcPayload.from_app_params([])
        )
        try:
            await awaitable
        except Exception as err:
            # Prefer a latched write error: the close error is likely just a
            # consequence ("can't close errored stream") (streams.ts:399-403).
            raise (self._pending_error or err) from None
        finally:
            self._dispose_hook()

    async def _abort_internal(self, reason: Exception | None) -> None:
        self._closed = True
        if self._hook is not None:
            if self._pending_error is not None:
                return
            error = _to_exception(reason, "WritableStream was aborted")
            self._pending_error = error
            if self._window_future is not None:
                fut = self._window_future
                self._window_future = None
                if not fut.done():
                    fut.set_exception(error)
                    # The parked writer may already be gone (e.g. cancelled);
                    # don't let the rejection go unretrieved.
                    fut.exception()
            awaitable, _size = hook_stream(
                self._hook, ["abort"], RpcPayload.from_app_params([reason])
            )
            try:
                await awaitable
            except Exception:
                pass
            finally:
                self._dispose_hook()
        else:
            if self._pending_error is not None:
                return
            self._pending_error = _to_exception(reason, "WritableStream was aborted")
            sink_abort = getattr(self._sink, "abort", None)
            if sink_abort is not None:
                with suppress(Exception):
                    await sink_abort(reason)

    def _dispose_hook(self) -> None:
        if not self._hook_disposed and self._hook is not None:
            self._hook_disposed = True
            self._hook.dispose()

    # -- local (sink) implementation ---------------------------------------------

    async def _write_local(self, chunk: Any) -> None:
        try:
            await self._sink.write(chunk)
        except Exception as err:
            if self._pending_error is None:
                self._pending_error = err
            raise

    async def _close_local(self) -> None:
        if self._pending_error is not None:
            raise self._pending_error
        sink_close = getattr(self._sink, "close", None)
        if sink_close is not None:
            await sink_close()

    # -- export support (serializer) ---------------------------------------------

    def _lock_for_export(self) -> "_WritableExportSink":
        """Lock this stream for export (TS getWriter() lock analog)."""
        if self._locked_for_export:
            raise RpcError(
                "Error", "Cannot serialize a WritableStream that is already locked."
            )
        self._locked_for_export = True
        return _WritableExportSink(self)

    def __repr__(self) -> str:
        kind = "proxy" if self._hook is not None else "local"
        return f"RpcWritableStream({kind}, closed={self._closed})"


class _WritableExportSink:
    """Sink adapter used when an RpcWritableStream is exported: routes the
    peer's write/close/abort through the stream's internal implementation
    (bypassing the export lock, which only blocks the local app)."""

    __slots__ = ("_stream",)

    def __init__(self, stream: RpcWritableStream) -> None:
        self._stream = stream

    async def write(self, chunk: Any = None) -> None:
        stream = self._stream
        async with stream._lock:
            if stream._pending_error is not None:
                raise stream._pending_error
            if stream._hook is not None:
                await stream._write_remote(chunk)
            else:
                await stream._write_local(chunk)

    async def close(self) -> None:
        stream = self._stream
        async with stream._lock:
            stream._closed = True
            if stream._hook is not None:
                await stream._close_remote()
            else:
                await stream._close_local()

    async def abort(self, reason: Any = None) -> None:
        await self._stream._abort_internal(
            reason if isinstance(reason, Exception) else _to_exception(
                reason, "WritableStream was aborted"
            )
        )


# ---------------------------------------------------------------------------
# WritableStreamHook — export-side hook (streams.ts:20-119)
# ---------------------------------------------------------------------------


class _BoxedWriterState:
    """Shared state for all WritableStreamHooks pointing at one sink
    (streams.ts:14-18), plus the WHATWG writer semantics TS gets for free:
    an op chain (writes processed strictly in order) and an error latch (a
    failed write errors the stream; close() then fails with that error —
    protocol.md:261)."""

    __slots__ = ("refcount", "sink", "closed", "error", "tail")

    def __init__(self, sink: Any) -> None:
        self.refcount = 1
        self.sink = sink
        self.closed = False
        self.error: Exception | None = None
        self.tail: asyncio.Future[Any] | None = None


class WritableStreamHook(StubHook):
    """Wraps a local writable sink for export (streams.ts:20-119).

    Accepts exactly three method calls — write(chunk) / close() /
    abort(reason?) — mirroring WritableStreamDefaultWriter. Disposal without
    close() aborts the underlying stream (protocol.md:259).
    """

    __slots__ = ("_state",)

    def __init__(
        self,
        state: _BoxedWriterState,
        dup_from: "WritableStreamHook | None" = None,
    ) -> None:
        self._state: _BoxedWriterState | None = state
        if dup_from is not None:
            state.refcount += 1

    @classmethod
    def create(cls, target: Any) -> "WritableStreamHook":
        """Create a hook around an RpcWritableStream (locking it, like TS
        getWriter()) or a raw sink object."""
        if isinstance(target, RpcWritableStream):
            sink: Any = target._lock_for_export()
        else:
            sink = target
        return cls(_BoxedWriterState(sink))

    def _get_state(self) -> _BoxedWriterState:
        if self._state is not None:
            return self._state
        raise RpcError(
            "Error", "Attempted to use a WritableStreamHook after it was disposed."
        )

    def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        try:
            state = self._get_state()

            if len(path) != 1 or not isinstance(path[0], str):
                raise RpcError(
                    "Error", "WritableStream stub only supports direct method calls"
                )

            method = path[0]
            if method not in ("write", "close", "abort"):
                args.dispose()
                raise RpcError("Error", f"Unknown WritableStream method: {method}")

            if method in ("close", "abort"):
                state.closed = True

            args.ensure_deep_copied()
            call_args = (
                list(args.value) if isinstance(args.value, list) else [args.value]
            )

            prev_tail = state.tail

            async def run() -> StubHook:
                # WHATWG writer semantics: ops run strictly in order.
                if prev_tail is not None:
                    with suppress(Exception):
                        await prev_tail
                if method == "abort":
                    reason = call_args[0] if call_args else None
                    sink_abort = getattr(state.sink, "abort", None)
                    result = await sink_abort(reason) if sink_abort else None
                    return PayloadStubHook(RpcPayload.from_app_return(result))
                if state.error is not None:
                    # Errored stream: writes reject; close() fails with the
                    # original write error (protocol.md:261).
                    raise state.error
                if method == "write":
                    chunk = call_args[0] if call_args else None
                    try:
                        result = await state.sink.write(chunk)
                    except Exception as err:
                        state.error = err
                        raise
                else:  # close
                    sink_close = getattr(state.sink, "close", None)
                    result = await sink_close() if sink_close else None
                return PayloadStubHook(RpcPayload.from_app_return(result))

            future: asyncio.Future[StubHook] = asyncio.ensure_future(run())
            state.tail = future
            return PromiseStubHook(future)
        except Exception as err:
            return ErrorStubHook(_as_rpc_error(err))

    def stream(
        self, path: list[str | int], args: RpcPayload
    ) -> tuple[Awaitable[None], int | None]:
        """Local streaming call: no size (caller serializes writes)."""
        result_hook = self.call(path, args)

        async def run() -> None:
            payload = await result_hook.pull()
            payload.dispose()

        return run(), None

    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        for cap in captures:
            cap.dispose()
        return ErrorStubHook(RpcError("Error", "Cannot use map() on a WritableStream"))

    def get(self, path: list[str | int]) -> StubHook:
        return ErrorStubHook(
            RpcError("Error", "Cannot access properties on a WritableStream stub")
        )

    def dup(self) -> "WritableStreamHook":
        state = self._get_state()
        return WritableStreamHook(state, dup_from=self)

    async def pull(self) -> RpcPayload:
        raise RpcError("Error", "Cannot pull a WritableStream stub")

    def ignore_unhandled_rejections(self) -> None:
        pass

    def dispose(self) -> None:
        state = self._state
        self._state = None
        if state is not None:
            state.refcount -= 1
            if state.refcount == 0:
                if not state.closed:
                    # Abort the stream if not cleanly closed (protocol.md:259,
                    # streams.ts:100-113).
                    state.closed = True
                    sink_abort = getattr(state.sink, "abort", None)
                    if sink_abort is not None:
                        _spawn_background(
                            sink_abort(
                                RpcError(
                                    "Error",
                                    "WritableStream RPC stub was disposed "
                                    "without calling close()",
                                )
                            )
                        )

    def on_broken(self, callback: Any) -> None:
        # WritableStream stubs don't have a "broken" state; the caller
        # notices when write/close/abort fails (streams.ts:115-118).
        pass


def _as_rpc_error(err: Exception) -> RpcError:
    if isinstance(err, RpcError):
        return err
    return RpcError("Error", str(err))


# ---------------------------------------------------------------------------
# ReadableStreamGuardHook — disposal tracking only (streams.ts:443-520)
# ---------------------------------------------------------------------------


class _BoxedReadableState:
    __slots__ = ("refcount", "stream", "canceled")

    def __init__(self, stream: RpcReadableStream) -> None:
        self.refcount = 1
        self.stream = stream
        self.canceled = False


class ReadableStreamGuardHook(StubHook):
    """Disposal-tracking hook for an RpcReadableStream (streams.ts:443-520).

    Exists solely to live in payload hook tracking so an unconsumed stream
    is canceled when the payload is disposed. All RPC operations error.
    """

    __slots__ = ("_state",)

    def __init__(
        self,
        state: _BoxedReadableState,
        dup_from: "ReadableStreamGuardHook | None" = None,
    ) -> None:
        self._state: _BoxedReadableState | None = state
        if dup_from is not None:
            state.refcount += 1

    @classmethod
    def create(cls, stream: RpcReadableStream) -> "ReadableStreamGuardHook":
        return cls(_BoxedReadableState(stream))

    def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        args.dispose()
        return ErrorStubHook(
            RpcError("Error", "Cannot call methods on a ReadableStream stub")
        )

    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        for cap in captures:
            cap.dispose()
        return ErrorStubHook(RpcError("Error", "Cannot use map() on a ReadableStream"))

    def get(self, path: list[str | int]) -> StubHook:
        return ErrorStubHook(
            RpcError("Error", "Cannot access properties on a ReadableStream stub")
        )

    def dup(self) -> "ReadableStreamGuardHook":
        state = self._state
        if state is None:
            raise RpcError(
                "Error",
                "Attempted to dup a ReadableStreamGuardHook after it was disposed.",
            )
        return ReadableStreamGuardHook(state, dup_from=self)

    async def pull(self) -> RpcPayload:
        raise RpcError("Error", "Cannot pull a ReadableStream stub")

    def ignore_unhandled_rejections(self) -> None:
        pass

    def dispose(self) -> None:
        state = self._state
        self._state = None
        if state is not None:
            state.refcount -= 1
            if state.refcount == 0 and not state.canceled:
                state.canceled = True
                # If someone is consuming/pumping the stream, they own full
                # consumption; only cancel truly-unconsumed streams
                # (streams.ts:499-511).
                state.stream._dispose_unconsumed()

    def on_broken(self, callback: Any) -> None:
        pass
