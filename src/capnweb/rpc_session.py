"""Unified RPC Session for bidirectional communication.

This module implements a symmetric RPC session that can handle both client and server
roles. It follows the TypeScript reference implementation pattern where:

1. Both sides use the same session class
2. A single read loop processes all incoming messages
3. Push creates an export on the receiver, import on the sender
4. Resolve/reject messages resolve pending imports

Key insight from TypeScript:
- Positive IDs for imports we initiate (things we're waiting for)
- Negative IDs for exports we initiate (things we're providing)

Dispatch model (locked decision D2, port of rpc.ts:929-1056):
- Protocol accounting (frame parse, export-ID assignment, table mutations,
  resolve/reject/release/abort handling) happens SYNCHRONOUSLY in wire-arrival
  order inside the read loop. Export IDs therefore always match the peer's
  wire order.
- Only application-level call execution runs asynchronously, behind
  promise-backed hooks (TargetStubHook.call returns a PromiseStubHook
  synchronously).
- Pull handling never blocks the read loop: ensureResolvingExport
  (rpc.ts:569-634) memoizes an idempotent per-export resolve task.

Framing (locked decision D3, contract C-FRAME):
- One RPC message per transport frame on send. Receive stays lenient and
  accepts newline-joined frames (HTTP batch transport framing).

Production features:
- reverseExports map for O(1) capability lookup
- sendRelease deletes the import table slot (rpc.ts:857-862)
- onBroken callbacks for connection death handling
- pullCount tracking and drain() for graceful shutdown
- Proper abort message to peer
- RpcSessionOptions for error redaction
- Serialized single-writer send queue + bounded pull timeout (hardening fixes)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Protocol

from capnweb.error import RpcError
from capnweb.hooks import (
    ErrorStubHook,
    PayloadStubHook,
    StubHook,
    TargetStubHook,
)
from capnweb.payload import RpcPayload
from capnweb.serializer import Serializer
from capnweb.wire import (
    PropertyKey,
    WireAbort,
    WireCapture,
    WireError,
    WirePipe,
    WirePipeline,
    WirePull,
    WirePush,
    WireReject,
    WireRelease,
    WireRemap,
    WireResolve,
    WireStream,
    parse_wire_batch,
    parse_wire_message_tree,
    serialize_wire_message,
)

logger = logging.getLogger(__name__)

# Bound on teardown flush waits (stop() must never hang; B2 session fix).
STOP_FLUSH_TIMEOUT = 5.0

# Transport encoding levels (rpc.ts:50-88, serialize.ts:28-29). A transport
# opts into a custom encoding by exposing an ``encoding_level`` attribute:
#
# - "string" (default): messages are JSON strings; the session performs the
#   final stringify/parse.
# - "jsonCompatible": messages are JSON-compatible value trees; the transport
#   handles final serialization (e.g. an in-process queue pair needs none).
# - "jsonCompatibleWithBytes": like "jsonCompatible" but ``bytes`` values are
#   left raw inside ``["bytes", ...]`` forms.
# - "structuredClonable": REJECTED — structured clone is a JS-host feature
#   with no Python analog; failing loudly beats silently mis-encoding.
#
# Anything else raises TypeError at session construction (rpc.ts:481-491).
_STRING_LEVEL = "string"
_SUPPORTED_ENCODING_LEVELS = frozenset(
    {"string", "jsonCompatible", "jsonCompatibleWithBytes"}
)
_KNOWN_ENCODING_LEVELS = _SUPPORTED_ENCODING_LEVELS | {"structuredClonable"}


def _read_encoding_level(transport: Any) -> str:
    """Validate and return a transport's encoding level (rpc.ts:475-491).

    A present-but-None attribute (e.g. an uninitialized field) is treated as
    the default string level rather than mis-routed down the custom-encoding
    path; any unrecognized value is rejected loudly instead of silently
    corrupting the wire.
    """
    raw = getattr(transport, "encoding_level", None)
    if raw is None:
        return _STRING_LEVEL
    if raw not in _KNOWN_ENCODING_LEVELS:
        msg = f"Unknown transport encodingLevel: {raw}"
        raise TypeError(msg)
    if raw == "structuredClonable":
        msg = (
            "The 'structuredClonable' encoding level is a JS-host feature "
            "(structured clone) with no Python analog; use 'jsonCompatible' "
            "or 'jsonCompatibleWithBytes' for in-process transports."
        )
        raise NotImplementedError(msg)
    return raw


def _mark_retrieved(future: asyncio.Future[Any]) -> None:
    """Read a future's exception so asyncio doesn't log 'never retrieved'.

    Awaiting callers still observe the exception normally.
    """
    try:
        future.exception()
    except (asyncio.CancelledError, asyncio.InvalidStateError):
        pass


def _fail_future(future: asyncio.Future[Any] | None, error: Exception) -> None:
    """Set an exception on a pending future without GC-time log noise."""
    if future is not None and not future.done():
        future.set_exception(error)
        _mark_retrieved(future)


class RpcTransport(Protocol):
    """Interface for bidirectional message transport.

    Implement this for WebSocket, HTTP batch, etc.
    """

    async def send(self, message: str) -> None:
        """Send a message to the peer."""
        ...

    async def receive(self) -> str:
        """Receive a message from the peer. Raises on disconnect."""
        ...

    def abort(self, reason: Exception) -> None:
        """Signal that the session has failed."""
        ...


class ExportEntry:
    """Entry in the exports table (capabilities we provide).

    ``auto_release`` and ``pipe_readable`` are the C-STREAM structural
    fields (frozen contract): ``auto_release=True`` on exports created by
    ``["stream"]`` messages (released with refcount 1 once resolve/reject is
    sent); ``pipe_readable`` holds the consume-once readable end of a
    ``["pipe"]`` export until ``["readable", id]`` claims it.
    """
    __slots__ = (
        'hook', 'refcount', 'pull_task', 'auto_release', 'pipe_readable',
        'reverse_key',
    )

    def __init__(
        self,
        hook: StubHook,
        refcount: int = 1,
        pull_task: asyncio.Task[None] | None = None,
        auto_release: bool = False,
        pipe_readable: Any | None = None,
        reverse_key: int | None = None,
    ) -> None:
        self.hook = hook
        self.refcount = refcount
        self.pull_task = pull_task  # Memoized resolve task (ensureResolvingExport)
        self.auto_release = auto_release
        self.pipe_readable = pipe_readable
        # Key into session._reverse_exports used when this entry was deduped
        # (id() of the hook as passed to the exporter, which may be a
        # different object than the stored dup). Cleaned up on release.
        self.reverse_key = reverse_key


# Import Pydantic config - keep old class for backwards compatibility
from capnweb.config import RpcSessionConfig

# Backwards compatibility alias
RpcSessionOptions = RpcSessionConfig


class ImportEntry:
    """Entry in the imports table (capabilities we're waiting for).

    Mirrors TS ImportTableEntry (rpc.ts:180-296). Hooks hold the entry object
    directly; the table slot is freed as soon as a release is sent
    (rpc.ts:857-862), so the entry outlives its table slot.
    """
    __slots__ = (
        'import_id', 'session', 'resolution', 'pending_pull', 'pulling',
        'local_refcount', 'remote_refcount', 'on_broken_callbacks'
    )

    def __init__(
        self,
        import_id: int,
        session: "BidirectionalSession",
        resolution: StubHook | None = None,
        pending_pull: asyncio.Future[StubHook] | None = None,
        pulling: bool = False,
        local_refcount: int = 0,
        remote_refcount: int = 1,
        on_broken_callbacks: list[int] | None = None,
    ) -> None:
        self.import_id = import_id
        self.session = session
        self.resolution = resolution
        self.pending_pull = pending_pull
        # True once a pull is in flight OR the peer auto-resolves this import
        # (["promise", id] entries are born pulling and must never send a
        # pull message). The waiter future is created lazily on first await
        # so promise imports can be parsed outside a running event loop.
        self.pulling = pulling or pending_pull is not None
        self.local_refcount = local_refcount  # Start at 0, incremented by ImportHook
        self.remote_refcount = remote_refcount
        self.on_broken_callbacks = on_broken_callbacks  # Indices into session callbacks

    def resolve(self, hook: StubHook) -> None:
        """Resolve this import with a hook (rpc.ts:198-242).

        Rejects also route through here with an ErrorStubHook so that a
        reject sends the release message too (rpc.ts:1013-1019).
        """
        if self.local_refcount == 0:
            # Already disposed (canceled): ignore the resolution and don't
            # send a redundant release (rpc.ts:207-211).
            hook.dispose()
            return

        self.resolution = hook
        self._send_release()  # Release remote reference now that we have resolution

        # Transfer onBroken callbacks to the resolution, preserving original
        # registration order (rpc.ts:217-237): if re-registering on the
        # resolution lands the callback right back on THIS session (appended
        # at the end), delete the NEW registration and keep the original slot
        # — when the connection dies, callbacks must fire in the order they
        # were first registered, not get pushed to the back of the line.
        if self.on_broken_callbacks:
            for idx in self.on_broken_callbacks:
                callback = self.session.get_on_broken_callback(idx)
                if callback:
                    end_index = self.session.peek_next_on_broken_index()
                    hook.on_broken(callback)
                    if self.session.get_on_broken_callback(end_index) is callback:
                        # on_broken() just re-registered the callback on this
                        # same session: drop the new slot, keep the original.
                        self.session.remove_on_broken_callback(end_index)
                    else:
                        # Registered elsewhere (or fired immediately): the
                        # original slot on this session is now stale.
                        self.session.remove_on_broken_callback(idx)
            self.on_broken_callbacks = None

        if self.pending_pull and not self.pending_pull.done():
            self.pending_pull.set_result(hook)

    def reject(self, error: Exception) -> None:
        """Abort this import with an error (TS ImportTableEntry.abort,
        rpc.ts:263-276). Already-resolved entries are left untouched."""
        if self.resolution is not None:
            return
        self.resolution = ErrorStubHook(error)
        self.on_broken_callbacks = None  # Session will call all callbacks
        _fail_future(self.pending_pull, error)

    def dispose(self) -> None:
        """Dispose this import entry (rpc.ts:254-261).

        For an unresolved entry this is a cancellation: reject any pending
        pull and send a release so the peer stops computing.
        """
        if self.resolution is not None:
            self.resolution.dispose()
        else:
            self.reject(
                RpcError.internal(
                    "RPC was canceled because the RpcPromise was disposed."
                )
            )
            self._send_release()

    def _send_release(self) -> None:
        """Send release message to peer."""
        if self.remote_refcount > 0:
            self.session.send_release(self.import_id, self.remote_refcount)
            self.remote_refcount = 0

    def on_broken(self, callback: Callable[[Exception], None]) -> None:
        """Register callback for when connection breaks."""
        if self.resolution:
            self.resolution.on_broken(callback)
        else:
            idx = self.session.register_on_broken_callback(callback)
            if self.on_broken_callbacks is None:
                self.on_broken_callbacks = []
            self.on_broken_callbacks.append(idx)


class BidirectionalSession:
    """A symmetric RPC session supporting bidirectional communication.

    This class can be used on both client and server sides. Each side:
    - Exports capabilities (things it provides)
    - Imports capabilities (things it receives from peer)
    - Runs a message loop to process incoming messages

    The key insight is that when we send a "push" (call), we create an import
    entry to track the pending result. When we receive a "push", we create an
    export entry for the result.
    """

    def __init__(
        self,
        transport: RpcTransport,
        local_main: Any | None = None,
        options: RpcSessionOptions | None = None,
    ) -> None:
        """Initialize the session.

        Args:
            transport: The message transport
            local_main: Optional local main capability to export as ID 0
            options: Optional session configuration
        """
        self.transport = transport
        self._options = options or RpcSessionOptions()

        # Encoding level from the transport (defaults to "string";
        # rpc.ts:471-491). Unknown levels raise at construction.
        self._encoding_level = _read_encoding_level(transport)

        # Export table: export_id -> ExportEntry
        # Positive IDs are assigned by peer's pushes
        # Negative IDs are assigned by us when we export
        self._exports: dict[int, ExportEntry] = {}
        self._reverse_exports: dict[int, int] = {}  # hook id -> export_id for O(1) lookup
        self._target_exports: dict[int, int] = {}  # target object id -> export_id for RpcTarget dedup
        self._next_export_id = -1  # We use negative IDs for our exports

        # Import table: import_id -> ImportEntry
        # Positive IDs are assigned by our pushes (sequential)
        self._imports: dict[int, ImportEntry] = {}

        # Track push sequence from peer (they assign sequential positive IDs).
        # Mutated ONLY synchronously in wire-arrival order (D2).
        self._peer_push_count = 0

        # Single shared counter for every import-creating message we send
        # (call/map/stream/pipe) — rpc.ts uses `imports.length` for all of
        # them (C-STREAM).
        self._next_import_id = 1

        # Abort state
        self._abort_reason: Exception | None = None
        self._abort_event = asyncio.Event()

        # Message loop task
        self._read_loop_task: asyncio.Task | None = None

        # Serialized outbound writer (hardening fix P1): all fire-and-forget
        # sync sends go through one FIFO queue drained by a single long-lived
        # writer task. This fixes (a) message LOSS — the old code used
        # `asyncio.create_task(...)` without keeping a reference, so the send
        # coroutine could be garbage-collected before it ran, silently dropping
        # a push/pull and hanging the peer forever; and (b) ORDERING — two
        # independent send tasks could invert on the wire under backpressure.
        # Frames are JSON strings at the "string" level and value trees on
        # custom-encoding transports; None is the stop sentinel.
        self._send_queue: asyncio.Queue[Any] = asyncio.Queue()
        self._writer_task: asyncio.Task | None = None
        self._send_lock = asyncio.Lock()

        # Best-effort abort-message flush task (kept referenced so it can't be
        # GC'd before running).
        self._abort_send_task: asyncio.Task | None = None

        # Bounded wait for pulls (hardening fix P0). A lost/mis-routed resolve
        # must surface as an error, never a permanent hang. Sourced from
        # RpcSessionConfig.pull_timeout ONLY (the old transport-attribute
        # override was dropped in Phase C per the 2026-07-04 B3 handoff).
        self._pull_timeout: float | None = (
            self._options.pull_timeout
            if self._options.pull_timeout is not None
            else 120.0
        )

        # onBroken callbacks: sparse dict of index -> callback. Indices come
        # from a monotonic counter (never len()) so a slot freed in the middle
        # can't be reused and clobber a later registration; iteration in
        # index order = registration order (TS sparse array, rpc.ts:466-468).
        self._on_broken_callbacks: dict[int, Callable[[Exception], None]] = {}
        self._next_on_broken_idx = 0

        # Pull count for drain() - how many promises peer expects us to resolve.
        # Incremented exactly once per export, inside _ensure_resolving_export.
        self._pull_count = 0
        # drain() waiters — a LIST so any number of concurrent drain() calls
        # all complete (TS overwrites its single onBatchDone; Phase C
        # multi-waiter fix).
        self._drain_waiters: list[asyncio.Future[None]] = []

        # Tracked background send tasks for the no-writer fallback path
        # (session used without start()); also gathered by drain().
        self._pending_push_tasks: set[asyncio.Task] = set()

        # Pipe pump tasks (C-STREAM): one per outgoing ReadableStream,
        # created by create_pipe. Cancelled on _abort, awaited in stop().
        self._pump_tasks: set[asyncio.Task] = set()

        # Register local main as export 0. When there is no local main, the
        # bootstrap is an ErrorStubHook so the peer's calls on it REJECT with
        # a clear message instead of aborting the whole session
        # (rpc.ts:1089-1096).
        if local_main is not None:
            from capnweb.types import RpcTarget as RpcTargetType

            if isinstance(local_main, RpcTargetType):
                # It's an RpcTarget - track by target id for deduplication
                hook = TargetStubHook(local_main)
                self._target_exports[id(local_main)] = 0
            else:
                hook = PayloadStubHook(RpcPayload.from_app_return(local_main))
        else:
            hook = ErrorStubHook(
                RpcError("Error", "This connection has no main object.")
            )
        self._exports[0] = ExportEntry(hook=hook)

        # Import 0 is the peer's main capability
        self._imports[0] = ImportEntry(import_id=0, session=self)

    def start(self) -> None:
        """Start the message read loop and the serialized outbound writer."""
        if self._read_loop_task is None:
            self._read_loop_task = asyncio.create_task(self._read_loop())
        if self._writer_task is None:
            self._writer_task = asyncio.create_task(self._writer_loop())

    async def _writer_loop(self) -> None:
        """Single consumer of the outbound queue — guarantees FIFO send order
        and keeps a strong reference so no send is lost to GC.

        On ANY exit (stop sentinel, send failure, cancellation) the queue is
        drained with ``task_done()`` for every leftover item, so
        ``queue.join()`` waiters (drain(), the abort-frame flush task) can
        never deadlock on frames the dead writer will no longer consume
        (B3's stop() deadlock report, 2026-07-04).
        """
        try:
            while True:
                frame = await self._send_queue.get()
                try:
                    if frame is None:  # stop sentinel
                        return
                    async with self._send_lock:
                        await self.transport.send(frame)
                except Exception as e:
                    self._abort(e, send_abort=False)
                    return
                finally:
                    self._send_queue.task_done()
        finally:
            self._drain_send_queue()

    def _drain_send_queue(self) -> None:
        """Consume (and drop) everything left in the send queue.

        Only called once the writer is exiting: the transport is dead or the
        session is stopping, so the frames can never be sent anyway. Marks
        each as done so join() completes.
        """
        while True:
            try:
                self._send_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            else:
                self._send_queue.task_done()

    async def stop(self) -> None:
        """Stop the session. NEVER hangs: every wait is bounded.

        Robust regardless of writer state (B3's deadlock report,
        2026-07-04): if the writer already exited on a send failure, the
        stop sentinel is consumed by the writer's exit drain and the
        abort-flush task's join() is time-bounded.
        """
        self._abort(RpcError.internal("Session stopped"))
        # Reap pipe pump tasks (cancelled by _abort).
        if self._pump_tasks:
            await asyncio.gather(
                *list(self._pump_tasks), return_exceptions=True
            )
        if self._writer_task:
            self._send_queue.put_nowait(None)  # drain + stop the writer
            if not self._writer_task.done():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._writer_task), STOP_FLUSH_TIMEOUT
                    )
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    self._writer_task.cancel()
            # If the writer had already exited (e.g. send failure), or its
            # exit drain raced our sentinel, consume the leftovers ourselves
            # so join() waiters don't hang. Idempotent.
            self._drain_send_queue()
        if self._read_loop_task:
            self._read_loop_task.cancel()
            try:
                await self._read_loop_task
            except asyncio.CancelledError:
                pass
        if self._abort_send_task:
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._abort_send_task), STOP_FLUSH_TIMEOUT
                )
            except (asyncio.CancelledError, asyncio.TimeoutError):
                self._abort_send_task.cancel()

    def get_main_stub(self) -> StubHook:
        """Get a hook for the peer's main capability.

        Disposing the returned hook shuts down the whole session, matching
        TS RpcMainHook (rpc.ts:412-427).
        """
        return MainImportHook(self)

    def get_remote_main(self) -> "RpcStub":
        """Get the peer's main capability as an RpcStub.

        Native port of TS ``RpcSession.getRemoteMain()`` (rpc.ts:1089-1105).
        Disposing the returned stub shuts the session down.
        """
        from capnweb.stubs import RpcStub

        return RpcStub(self.get_main_stub())

    async def wait_closed(self) -> None:
        """Wait until the session ends (aborts or is shut down).

        Event-driven — the native completion signal the module-level
        ``ws_session.wait_closed`` helper defers to (B3 handoff,
        2026-07-04).
        """
        await self._abort_event.wait()

    async def drain(self) -> None:
        """Wait for all pending operations to complete.

        This is useful for graceful shutdown - wait for all promises
        that the peer is expecting us to resolve, then make sure every
        queued outbound frame actually reached the transport (the HTTP
        batch server snapshots the response right after drain()).
        """
        if self._abort_reason:
            raise self._abort_reason

        # Wait for tracked background send tasks (no-writer fallback path)
        if self._pending_push_tasks:
            await asyncio.gather(*self._pending_push_tasks, return_exceptions=True)

        # Wait for all pending pulls to complete. Multi-waiter: every
        # concurrent drain() gets its own future (TS overwrites its single
        # onBatchDone slot, losing earlier waiters — Phase C fix).
        if self._pull_count > 0:
            waiter: asyncio.Future[None] = (
                asyncio.get_running_loop().create_future()
            )
            self._drain_waiters.append(waiter)
            await waiter

        # Flush the writer queue so callers can observe the sent frames.
        if self._writer_task is not None and not self._writer_task.done():
            await self._send_queue.join()

    def get_stats(self) -> dict[str, int]:
        """Get statistics about the session.

        Returns:
            Dict with 'imports' and 'exports' counts
        """
        return {
            "imports": len(self._imports),
            "exports": len(self._exports),
        }

    def shutdown(self) -> None:
        """Shutdown the session (called when main stub is disposed)."""
        self._abort(RpcError.internal("RPC session was shut down by disposing the main stub"), send_abort=False)

    # -------------------------------------------------------------------------
    # Callback management (for ImportEntry)
    # -------------------------------------------------------------------------

    def register_on_broken_callback(self, callback: Callable[[Exception], None]) -> int:
        """Register an onBroken callback and return its index.

        Indices come from a monotonic counter (TS appends to a sparse array):
        a slot freed by the resolve-transfer path is never reused, so index
        order is exactly registration order.
        """
        idx = self._next_on_broken_idx
        self._next_on_broken_idx += 1
        self._on_broken_callbacks[idx] = callback
        return idx

    def peek_next_on_broken_index(self) -> int:
        """The index the NEXT registration would get (TS ``array.length``,
        used by the resolve-transfer ordering hack, rpc.ts:221-236)."""
        return self._next_on_broken_idx

    def get_on_broken_callback(self, idx: int) -> Callable[[Exception], None] | None:
        """Get an onBroken callback by index."""
        return self._on_broken_callbacks.get(idx)

    def remove_on_broken_callback(self, idx: int) -> None:
        """Remove an onBroken callback by index."""
        self._on_broken_callbacks.pop(idx, None)

    def send_release(self, import_id: int, refcount: int) -> None:
        """Send a release message to peer (public wrapper)."""
        self._send_release(import_id, refcount)

    # -------------------------------------------------------------------------
    # Sending messages
    # -------------------------------------------------------------------------

    def send_call(
        self,
        target_id: int,
        path: list[str | int],
        args: RpcPayload | None = None,
    ) -> "ImportHook":
        """Send a call to the peer and return a hook for the result.

        This method is SYNCHRONOUS to ensure messages are queued before
        the batch is sent. This matches TypeScript's sendCall behavior.

        Args:
            target_id: The import ID to call on
            path: Property path + method name
            args: Optional arguments

        Returns:
            An ImportHook that will resolve when the peer responds
        """
        if self._abort_reason:
            raise self._abort_reason

        # Create pipeline expression
        path_keys = [PropertyKey(p) for p in path]

        # Serialize args if provided
        # NOTE: Per TypeScript behavior, pipeline args are sent UN-ESCAPED on the wire.
        # The serializer wraps arrays as [[...]], but TypeScript unwraps this for pipeline
        # args (see rpc.ts line 539). We match this by unwrapping after serialization.
        serialized_args = None
        if args is not None:
            serializer = Serializer(exporter=self, encoding_level=self._encoding_level)
            serialized_args = serializer.serialize_payload(args)
            # Unwrap the escaped array: [[5]] -> [5]
            if isinstance(serialized_args, list) and len(serialized_args) == 1:
                serialized_args = serialized_args[0]

        pipeline = WirePipeline(
            import_id=target_id,
            property_path=path_keys,
            args=serialized_args,
        )

        # Send push message SYNCHRONOUSLY (queue to transport)
        push = WirePush(pipeline)
        self._send_sync([push])

        # Allocate the next import ID from the single shared counter and
        # create the import entry for the result (rpc.ts:779-783).
        import_id = self._next_import_id
        self._next_import_id += 1
        entry = ImportEntry(import_id=import_id, session=self)
        self._imports[import_id] = entry

        # Pass entry directly to ImportHook (like TypeScript)
        return ImportHook.from_entry(self, entry, is_promise=True)

    def send_pull(self, import_id: int) -> None:
        """Send a pull request for an import (synchronous)."""
        if self._abort_reason:
            raise self._abort_reason

        pull = WirePull(import_id)
        self._send_sync([pull])

    def _serialize_frame(self, msg: Any) -> Any:
        """Encode one wire message per the transport's encoding level.

        "string": JSON text (the default wire format). Custom-encoding
        levels: the JSON-compatible value tree — the transport handles any
        final serialization itself (rpc.ts:707-745).
        """
        if self._encoding_level == _STRING_LEVEL:
            return serialize_wire_message(msg)
        return msg.to_json()

    def _send_sync(self, messages: list[Any]) -> None:
        """Queue messages to be sent to the peer (synchronous).

        Each message becomes its OWN transport frame (contract C-FRAME):
        newline batching exists only inside the HTTP batch transport, which
        implements it by joining the individually-sent frames itself.
        """
        if self._abort_reason:
            return  # Don't send after abort
        frames = [self._serialize_frame(msg) for msg in messages]
        self._send_frames_sync(frames)

    def _send_frames_sync(self, frames: list[Any]) -> None:
        """Queue pre-encoded frames for the writer (C-FRAME)."""
        if self._writer_task is not None:
            # Enqueue for the serialized writer. put_nowait is sync and the
            # single writer task drains in FIFO order with a strong reference,
            # so the send can neither be lost to GC nor reordered.
            for frame in frames:
                self._send_queue.put_nowait(frame)
        else:
            # Writer not started (session used without start()): fall back to a
            # tracked task so the reference is still held until completion.
            task = asyncio.ensure_future(self._send_frames(frames))
            self._pending_push_tasks.add(task)
            task.add_done_callback(self._pending_push_tasks.discard)

    async def _send_frames(self, frames: list[Any]) -> None:
        """Send frames directly to the transport (no-writer fallback)."""
        try:
            # Share the writer's lock so this can never interleave with the
            # serialized queue mid-sequence.
            async with self._send_lock:
                for frame in frames:
                    await self.transport.send(frame)
        except Exception as e:
            # If send fails, abort without trying to send abort message
            self._abort(e, send_abort=False)

    def _send_release(self, import_id: int, refcount: int) -> None:
        """Send a release message and free the import table slot immediately
        (rpc.ts:857-862). Hooks keep the resolved value reachable through
        their cached entry reference."""
        if self._abort_reason:
            return
        release = WireRelease(import_id, refcount)
        self._send_sync([release])  # serialized writer, no GC loss
        self._imports.pop(import_id, None)

    def send_stream(
        self,
        target_id: int,
        path: list[str | int],
        args: RpcPayload,
    ) -> tuple[Any, int]:
        """Send a ``["stream", ["pipeline", id, path, [args]]]`` write
        (port of rpc.ts:786-822, C-STREAM).

        Returns ``(awaitable, size)``:

        * ``awaitable`` resolves when the peer acks the write via
          resolve/reject (the ack that clocks the FlowController);
        * ``size`` is the byte length of the serialized frame (the message
          IS the transport frame here), falling back to the
          estimate_encoded_size port if the length is somehow unavailable.

        The import entry is created pre-pulling with ``remote_refcount=0``
        (the peer auto-releases the export after resolving — no release
        message must be sent) and ``local_refcount=1``; on resolution the
        payload is disposed and the entry deleted MANUALLY, since the normal
        release path is bypassed (rpc.ts:810-819).
        """
        if self._abort_reason:
            raise self._abort_reason


        path_keys = [PropertyKey(p) for p in path]
        serializer = Serializer(exporter=self, encoding_level=self._encoding_level)
        serialized_args = serializer.serialize_payload(args)
        # Pipeline args are sent UN-ESCAPED on the wire, like send_call
        # (rpc.ts:794-798 "HACK ... unwrap").
        if isinstance(serialized_args, list) and len(serialized_args) == 1:
            serialized_args = serialized_args[0]

        message = WireStream(
            WirePipeline(
                import_id=target_id,
                property_path=path_keys,
                args=serialized_args,
            )
        )
        frame = self._serialize_frame(message)
        self._send_frames_sync([frame])
        if self._encoding_level == _STRING_LEVEL:
            # The message IS the transport frame; its UTF-8 length is exact.
            size = len(frame.encode("utf-8"))
        else:
            # Custom-encoding transports may not report sizes; estimate like
            # TS (rpc.ts:799-801, estimateEncodedSize).
            from capnweb.streams import estimate_encoded_size

            size = estimate_encoded_size(frame)

        import_id = self._next_import_id
        self._next_import_id += 1
        entry = ImportEntry(
            import_id=import_id,
            session=self,
            pending_pull=asyncio.get_running_loop().create_future(),
            local_refcount=1,
            remote_refcount=0,
        )
        self._imports[import_id] = entry

        async def await_resolution() -> None:
            try:
                resolved_hook = await entry.pending_pull  # type: ignore[misc]
            except BaseException:
                self._imports.pop(import_id, None)
                raise
            try:
                payload = await resolved_hook.pull()
                payload.dispose()
            finally:
                self._imports.pop(import_id, None)

        return await_resolution(), size

    def create_pipe(self, readable: Any, guard_hook: StubHook) -> int:
        """Create a pipe: send ``["pipe"]``, allocate a non-promise import,
        and start pumping ``readable`` into it (port of rpc.ts:684-705,
        C-EXPORTER).

        Pumping starts immediately — before/concurrently with the message
        that will carry ``["readable", import_id]`` — so data flows without
        waiting for a round trip (protocol.md:121). ``guard_hook`` is
        disposed when the pump finishes.
        """
        if self._abort_reason:
            raise self._abort_reason

        from capnweb.streams import RpcReadableStream, RpcWritableStream

        if not isinstance(readable, RpcReadableStream):
            raise TypeError(
                "create_pipe requires an RpcReadableStream, got "
                f"{type(readable).__name__}"
            )

        self._send_sync([WirePipe()])

        import_id = self._next_import_id
        self._next_import_id += 1
        # The pipe import is NOT a promise — it's immediately usable as a
        # writable stream stub (rpc.ts:691-693).
        entry = ImportEntry(import_id=import_id, session=self)
        self._imports[import_id] = entry

        hook = ImportHook.from_entry(self, entry, is_promise=False)
        proxy = RpcWritableStream._from_hook(hook)

        # Lock the source now (TS pipeTo locks synchronously) so a
        # guard-hook disposal racing the pump start can't cancel it.
        readable._acquire_for_pump()

        task = asyncio.ensure_future(self._pump(readable, proxy, guard_hook))
        self._pump_tasks.add(task)
        task.add_done_callback(self._pump_tasks.discard)

        return import_id

    async def _pump(
        self,
        readable: Any,
        writable: Any,
        guard_hook: StubHook,
    ) -> None:
        """Pump a readable into a pipe's proxy writable (pipeTo analog,
        rpc.ts:696-701).

        Error routing matches pipeTo: a source error aborts the destination;
        a destination (write) error cancels the source. Pump errors are
        swallowed — the writable path owns error reporting.
        """
        from capnweb.streams import _spawn_background

        try:
            try:
                async for chunk in readable:
                    await writable.write(chunk)
                await writable.close()
            except asyncio.CancelledError:
                # Session shutdown: release the pipe import and let the
                # source generator clean itself up in the background.
                writable._dispose_hook()
                _spawn_background(
                    readable.cancel(
                        RpcError.internal("RPC session was shut down")
                    )
                )
                raise
            except Exception as err:
                # Cancel the source (no-op if the source itself errored) and
                # abort the destination (no-op if a write already errored).
                try:
                    await readable.cancel(err)
                except Exception:
                    pass
                try:
                    await writable.abort(err)
                except Exception:
                    pass
        finally:
            guard_hook.dispose()

    def get_pipe_readable(self, export_id: int) -> Any:
        """Retrieve the readable end of a pipe export — consume-once
        (port of rpc.ts:674-682, C-IMPORTER)."""
        entry = self._exports.get(export_id)
        if entry is None or entry.pipe_readable is None:
            raise ValueError(
                f"Export {export_id} is not a pipe or its readable end was "
                "already consumed."
            )
        readable = entry.pipe_readable
        entry.pipe_readable = None
        return readable

    def send_map(
        self,
        target_id: int,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> "ImportHook":
        """Send a map operation to the peer (synchronous, rpc.ts:824-848).

        Args:
            target_id: The import ID to map over
            path: Property path to the array
            captures: External stubs used in the mapper function
            instructions: The mapper instructions

        Returns:
            An ImportHook that will resolve to the mapped result
        """
        if self._abort_reason:
            # Dispose captures on abort
            for cap in captures:
                cap.dispose()
            raise self._abort_reason

        # Convert captures to wire format (rpc.ts:833-840). sendMap takes
        # ownership of the capture hooks: the export branch adopts them into
        # the export table; the import branch only needs the ID, so the
        # owned reference is released (keeps import refcounts balanced —
        # TS leaks these, see the map.ts:100 dedup TODO).
        wire_captures: list[WireCapture] = []
        for cap in captures:
            import_id = self.get_import(cap)
            if import_id is not None:
                wire_captures.append(WireCapture("import", import_id))
                cap.dispose()
            else:
                # Export the capture; exportStub takes ownership (no dup).
                export_id = self._export_hook_by_id(cap, id(cap), dup=False)
                wire_captures.append(WireCapture("export", export_id))

        # Create remap expression. The path is ALWAYS an array on the wire —
        # TS receivers hard-reject null (serialize.ts:906-912; matrix 04
        # row 12).
        path_keys = [PropertyKey(p) for p in path]

        remap = WireRemap(
            import_id=target_id,
            property_path=path_keys,
            captures=wire_captures,
            instructions=instructions,
        )

        # Send push message with remap through the writer queue, and allocate
        # the import ID from the same shared counter as send_call in the same
        # synchronous critical section (no reordering window).
        push = WirePush(remap)
        self._send_sync([push])

        import_id = self._next_import_id
        self._next_import_id += 1
        entry = ImportEntry(import_id=import_id, session=self)
        self._imports[import_id] = entry

        return ImportHook.from_entry(self, entry, is_promise=True)

    def _export_hook_by_id(self, hook: StubHook, hook_id: int, dup: bool = True) -> int:
        """Export a hook by its ID and return the export ID.

        Args:
            hook: The hook to export
            hook_id: The ID to use for deduplication (usually id(hook))
            dup: Whether to duplicate the hook (False for already-owned hooks)

        Returns:
            The export ID
        """
        # O(1) lookup using reverse map
        existing_id = self._reverse_exports.get(hook_id)
        if existing_id is not None:
            self._exports[existing_id].refcount += 1
            return existing_id

        # Allocate new negative export ID
        export_id = self._next_export_id
        self._next_export_id -= 1
        self._exports[export_id] = ExportEntry(
            hook=hook.dup() if dup else hook, reverse_key=hook_id
        )
        self._reverse_exports[hook_id] = export_id
        return export_id

    def _export_hook(self, hook: StubHook) -> int:
        """Export a hook and return its export ID."""
        return self._export_hook_by_id(hook, id(hook))

    # -------------------------------------------------------------------------
    # Exporter protocol (for serializing capabilities we send)
    # -------------------------------------------------------------------------

    def export_capability(self, stub: Any) -> int:
        """Export a capability (stub) and return its export ID.

        For stubs, we reuse existing export IDs if the same hook is exported again.
        """
        if self._abort_reason:
            raise self._abort_reason

        hook: StubHook = stub._hook
        return self._export_hook_by_id(hook, id(hook))

    def export_target(self, target: Any) -> int:
        """Export an RpcTarget directly and return its export ID.

        This wraps the RpcTarget in a TargetStubHook and exports it.
        Uses _target_exports map to deduplicate by target object ID.
        """
        if self._abort_reason:
            raise self._abort_reason

        target_id = id(target)

        # O(1) lookup using target exports map
        existing_id = self._target_exports.get(target_id)
        if existing_id is not None:
            self._exports[existing_id].refcount += 1
            return existing_id

        from capnweb.hooks import TargetStubHook

        # Wrap the target in a hook
        hook = TargetStubHook(target)

        # Allocate new negative export ID
        export_id = self._next_export_id
        self._next_export_id -= 1
        self._exports[export_id] = ExportEntry(hook=hook)
        self._target_exports[target_id] = export_id
        return export_id

    def export_promise(self, stub: Any) -> int:
        """Export a promise and return its export ID.

        Unlike stubs, promises always get a new ID because otherwise the
        recipient could miss the resolution. We also start auto-resolving
        the promise.
        """
        if self._abort_reason:
            raise self._abort_reason

        hook: StubHook = stub._hook

        # Promises always use a new ID
        export_id = self._next_export_id
        self._next_export_id -= 1
        self._exports[export_id] = ExportEntry(hook=hook.dup(), reverse_key=id(hook))
        self._reverse_exports[id(hook)] = export_id

        # Start auto-resolving the promise
        self._ensure_resolving_export(export_id)

        return export_id

    def export_promise_hook(self, hook: StubHook) -> int:
        """Export a promise hook, TAKING OWNERSHIP (TS exportPromise).

        Used by the Serializer when it materializes a promise's pending
        property path into a dedicated hook (serialize.ts:471-477): the
        fresh hook is owned by nobody else, so the export table adopts it
        directly instead of dup()ing a borrowed reference.
        """
        if self._abort_reason:
            raise self._abort_reason

        export_id = self._next_export_id
        self._next_export_id -= 1
        self._exports[export_id] = ExportEntry(hook=hook, reverse_key=id(hook))
        self._reverse_exports[id(hook)] = export_id
        self._ensure_resolving_export(export_id)
        return export_id

    def get_import(self, hook: StubHook) -> int | None:
        """Return the import ID if `hook` is a live import of THIS session
        (TS getImport, rpc.ts:634-641); None otherwise."""
        if (
            isinstance(hook, ImportHook)
            and hook.session is self
            and hook._entry is not None
        ):
            return hook._entry.import_id
        return None

    def unexport(self, ids: list[int]) -> None:
        """Roll back exports allocated during a failed serialization
        (C-EXPORTER; TS unexport, rpc.ts:540-544).

        Semantically identical to receiving a peer release with refcount 1
        for each ID.
        """
        for export_id in ids:
            self._release_export(export_id, 1)

    def on_send_error(self, error: Exception) -> Exception | None:
        """Error-redaction hook (C-EXPORTER), sourced from RpcSessionConfig.

        Called by the Serializer whenever an error is serialized for the
        wire; returning an Exception substitutes it for the original.
        """
        if self._options.on_send_error is not None:
            return self._options.on_send_error(error)
        return None

    @property
    def redact_internal_errors(self) -> bool:
        """F6: whether unexpected-exception text is redacted before the wire.

        Read by the Serializer (C-EXPORTER surface) via ``getattr``. Sourced
        from RpcSessionConfig; defaults True (secure) for the untrusted-peer
        control plane.
        """
        return self._options.redact_internal_errors

    def _new_parser(self, *, errors_as_values: bool = False) -> "Parser":
        """Construct a Parser threaded with this session's decode bounds.

        Centralizes the F4/F5 limits so every receive-path decode enforces the
        same configured ``max_array_len`` / ``max_blob_bytes`` (a peer can't
        find an un-bounded decode site).
        """
        from capnweb.parser import Parser

        return Parser(
            importer=self,
            errors_as_values=errors_as_values,
            max_array_len=self._options.max_array_len,
            max_blob_bytes=self._options.max_blob_bytes,
        )

    def _ensure_resolving_export(self, export_id: int) -> None:
        """Idempotently start resolving an export (rpc.ts:569-634).

        The resolve task is memoized on ExportEntry.pull_task so repeated
        pulls of one export produce exactly one resolve message, and pull
        handling never blocks the dispatch loop.

        Raises:
            RpcError: If the export ID is unknown (protocol error -> abort).
        """
        entry = self._exports.get(export_id)
        if entry is None:
            raise RpcError.not_found(f"no such export ID: {export_id}")
        if entry.pull_task is not None:
            return

        self._pull_count += 1
        entry.pull_task = asyncio.create_task(
            self._resolve_export(export_id, entry)
        )

    async def _resolve_export(self, export_id: int, entry: ExportEntry) -> None:
        """Pull an export's hook and send resolve/reject (rpc.ts:575-632)."""
        auto_release = entry.auto_release
        try:
            try:
                from capnweb.stubs import RpcPromise

                hook = entry.hook
                while True:
                    payload = await hook.pull()
                    value = payload.value
                    if isinstance(value, RpcPromise) and self.get_import(hook) is None:
                        # Optimization (rpc.ts:575-597): the resolution is just
                        # another promise not pointing back at the peer. An
                        # intermediate resolve message would carry no useful
                        # information; keep pulling the chain instead.
                        hook = value._hook
                        continue
                    break

                # Serialize and send resolve through the writer queue.
                # We don't transfer ownership of stubs in the payload since the
                # payload belongs to the hook, which sticks around to handle
                # pipelined requests.
                serializer = Serializer(exporter=self, encoding_level=self._encoding_level)
                serialized = serializer.serialize_payload(payload)
                self._send_sync([WireResolve(export_id, serialized)])
            except Exception as e:
                # Two-stage: reject with the app (or serialization) error; if
                # even that fails, abort the session (rpc.ts:602-632).
                try:
                    self._send_sync(
                        [WireReject(export_id, self._serialize_error_expression(e))]
                    )
                except Exception as e2:
                    self._abort(e2)
                    return
            if auto_release:
                # stream messages implicitly release after resolve (B1).
                self._release_export(export_id, 1)
        finally:
            self._pull_count -= 1
            if self._pull_count == 0 and self._drain_waiters:
                waiters, self._drain_waiters = self._drain_waiters, []
                for waiter in waiters:
                    if not waiter.done():
                        waiter.set_result(None)

    def _serialize_error_expression(self, error: Exception) -> Any:
        """Wire-faithful error expression via the one codec stack.

        The Serializer applies the on_send_error redaction hook (through the
        session's C-EXPORTER `on_send_error`), preserves JS error names and
        recursively encodes properties/cause (C-ERROR). Falls back to a
        minimal internal error if even that fails.
        """
        try:
            return Serializer(exporter=self, encoding_level=self._encoding_level).serialize(error)
        except Exception:
            logger.exception("Failed to serialize error for the wire")
            return WireError("internal", str(error))

    # -------------------------------------------------------------------------
    # Message handling (synchronous dispatch in wire-arrival order — D2)
    # -------------------------------------------------------------------------

    async def _read_loop(self) -> None:
        """Main message processing loop (port of rpc.ts:929-1056).

        receive() runs UNINTERRUPTED — no cancel-poll (the Python analog of
        upstream PR #168: a timed-out wait_for cancels an in-flight receive
        that may already hold a frame, silently dropping it). Abort cancels
        this task directly instead.

        Protocol accounting is synchronous, in wire-arrival order. Only
        application call execution runs behind promise-backed hooks.
        """
        from capnweb.batch import BatchEndError

        try:
            while self._abort_reason is None:
                try:
                    data = await self.transport.receive()
                except asyncio.CancelledError:
                    raise
                except (BatchEndError, ConnectionError, EOFError) as e:
                    # Transport gone / batch over: fail loudly so on_broken
                    # fires and pending pulls fail fast. Never try to send an
                    # abort message over the dead transport.
                    self._abort(e, send_abort=False)
                    return
                except Exception as e:
                    self._abort(e, send_abort=False)
                    return

                if self._abort_reason is not None:
                    break  # check again before processing

                try:
                    self._handle_frame(data)
                except Exception as e:
                    # Protocol error: abort the session and tell the peer.
                    logger.debug("Protocol error in read loop: %s", e)
                    self._abort(e)
                    return
        except asyncio.CancelledError:
            pass

    def _handle_frame(self, data: Any) -> None:
        """Parse and dispatch one inbound frame synchronously.

        At the "string" level receive stays lenient about newline-joined
        multi-message frames (the HTTP batch transport framing); sends are
        one message per frame. Custom-encoding transports deliver one
        already-decoded message tree per frame (rpc.ts:947); malformed trees
        abort exactly like malformed JSON.
        """
        # F3: reject an oversized frame BEFORE parsing it wholesale (a giant
        # string/blob/array frame would otherwise allocate its full size
        # during parse). Only the network-facing wire delivers a raw
        # str/bytes frame; O(1) length check, no encode pass. Custom-encoding
        # tree transports carry no raw frame and are bounded upstream.
        if isinstance(data, (str, bytes, bytearray)):
            if len(data) > self._options.max_message_bytes:
                raise RpcError.bad_request(
                    f"message size {len(data)} exceeds maximum "
                    f"({self._options.max_message_bytes} bytes)"
                )
        if self._encoding_level != _STRING_LEVEL:
            try:
                message = parse_wire_message_tree(data)
            except Exception as e:
                raise RpcError.bad_request(f"bad RPC message: {data!r}"[:280]) from e
            self._process_message(message)
            return
        try:
            messages = parse_wire_batch(data)
        except Exception as e:
            raise RpcError.bad_request(f"bad RPC message: {data[:256]!r}") from e
        for msg in messages:
            self._process_message(msg)

    def _process_message(self, msg: Any) -> None:
        """Process a single message synchronously (rpc.ts:946-1054).

        Raising here is a protocol error: the read loop aborts the session.
        push/stream/pipe all assign export IDs from the same arrival-ordered
        counter (C-STREAM; TS uses exports.length for all three).
        """
        match msg:
            case WirePush():
                self._handle_push(msg)

            case WireStream(expression):
                self._handle_stream(expression)

            case WirePipe():
                self._handle_pipe()

            case WirePull(import_id):
                # Kick off (or join) the memoized resolve task; never blocks.
                self._ensure_resolving_export(import_id)

            case WireResolve(export_id, value):
                self._handle_resolve(export_id, value)

            case WireReject(export_id, error):
                self._handle_reject(export_id, error)

            case WireRelease(import_id, refcount):
                self._release_export(import_id, refcount)

            case WireAbort(error):
                self._handle_peer_abort(error)

            case _:
                raise RpcError.bad_request(f"bad RPC message: {msg!r}")

    def _guard_export_capacity(self) -> None:
        """F1: bound the export table before creating a new entry.

        Counts LIVE entries — post-release deletes shrink the table, so a
        peer that grants and releases stays far under the cap; only a
        hoarding peer (pushes that never release) trips it. Raising here
        aborts the session via the read loop (fail-loud, no silent cap).
        """
        if len(self._exports) >= self._options.max_exports:
            raise RpcError.bad_request(
                f"export table limit exceeded "
                f"({self._options.max_exports} entries)"
            )

    def _handle_push(self, msg: WirePush) -> None:
        """Handle a push message synchronously (rpc.ts:951-964).

        The export ID is assigned in strict wire-arrival order BEFORE any
        awaiting happens; only the application call behind the resulting hook
        runs asynchronously.
        """
        self._guard_export_capacity()
        export_id = self._peer_push_count + 1
        self._peer_push_count += 1

        result_hook = self._evaluate_push_expression(msg.expression)

        # It's possible for a rejection to occur before the peer gets a chance
        # to send a "pull" or use the promise in a pipeline; don't treat that
        # as an unhandled rejection on our end.
        result_hook.ignore_unhandled_rejections()

        self._exports[export_id] = ExportEntry(hook=result_hook)

    def _handle_stream(self, expression: Any) -> None:
        """Handle a ["stream", expression] message (rpc.ts:966-984).

        Like push, but: refcount 1 with auto_release=True (once resolve/
        reject is sent, the export is implicitly released — the sender never
        sends release), and the export is immediately auto-pulled (the
        sender never sends pull). protocol.md:103-111.
        """
        self._guard_export_capacity()
        export_id = self._peer_push_count + 1
        self._peer_push_count += 1

        result_hook = self._evaluate_push_expression(expression)
        result_hook.ignore_unhandled_rejections()

        self._exports[export_id] = ExportEntry(
            hook=result_hook, refcount=1, auto_release=True
        )
        # Automatically pull since stream messages are always pulled.
        self._ensure_resolving_export(export_id)

    def _handle_pipe(self) -> None:
        """Handle a ["pipe"] message (rpc.ts:986-993).

        Builds the Python TransformStream equivalent: a bounded channel with
        an RpcReadableStream reading from it and a WritableStreamHook whose
        write() awaits the channel put — delaying the write's resolve (the
        flow-control ack) when the local consumer is slow. The writable end
        becomes the export; the readable end is stashed on the entry for a
        later, consume-once ["readable", importId] expression.
        """
        from capnweb.streams import (
            RpcReadableStream,
            WritableStreamHook,
            _ChannelSink,
            _PipeChannel,
        )

        self._guard_export_capacity()
        export_id = self._peer_push_count + 1
        self._peer_push_count += 1

        channel = _PipeChannel()
        readable = RpcReadableStream._for_pipe(channel)
        hook = WritableStreamHook.create(_ChannelSink(channel))
        self._exports[export_id] = ExportEntry(
            hook=hook, refcount=1, pipe_readable=readable
        )

    def _payload_hook_with_substitution(self, payload: RpcPayload) -> StubHook:
        """Wrap a parsed payload, resolving embedded promises before delivery.

        TS delivery semantics (core.ts:1122-1163, serialize.ts comment on
        "promise"): promises embedded in a value — ordinary ``["promise",
        id]`` imports, remap results, and delivery-blocking substitutions
        like Blob collection — are resolved and substituted in place before
        the value reaches application code.
        """
        if not payload.substitutions and not payload.promises:
            return PayloadStubHook(payload)

        from capnweb.hooks import PromiseStubHook
        from capnweb.stubs import deliver_payload_in_place

        async def substitute() -> StubHook:
            await deliver_payload_in_place(payload)
            return PayloadStubHook(payload)

        return PromiseStubHook(asyncio.ensure_future(substitute()))

    def _evaluate_push_expression(self, expression: Any) -> StubHook:
        """Evaluate a push subject into a StubHook, synchronously.

        Any expression is legal as a push subject (protocol.md:73,81-83):
        pipeline and remap map onto capability calls; everything else
        evaluates to plain data wrapped in a PayloadStubHook.

        Raises on protocol errors (e.g. unknown export ID), which aborts the
        session per TS readLoop semantics.
        """
        if isinstance(expression, WirePipeline):
            target_entry = self._exports.get(expression.import_id)
            if target_entry is None:
                raise RpcError.not_found(
                    f"no such entry on exports table: {expression.import_id}"
                )
            target_hook = target_entry.hook

            path: list[str | int] = [
                pk.value for pk in (expression.property_path or [])
            ]

            if expression.args is None:
                # ["pipeline", id, path] — property reference, not a call.
                return target_hook.get(path)

            # Parse args through Parser to convert ["export", id] into stubs.
            # Pipeline args are sent UN-ESCAPED on the wire; TypeScript wraps
            # them before evaluating: evaluate([args]) (serialize.ts:899).
            parser = self._new_parser()
            args_payload = parser.parse([expression.args])

            if args_payload.substitutions:
                # Args contain delivery-blocking promises (Blob collection):
                # deliver them in place, then dispatch. Export-ID assignment
                # already happened synchronously; only the app call defers,
                # exactly like any async application method. ONE delivery
                # path: stubs.deliver_payload_in_place (B2→C handoff,
                # 2026-07-05) — its `delivered` flag makes the hooks-level
                # deliverCall re-check a no-op.
                from capnweb.hooks import PromiseStubHook
                from capnweb.stubs import deliver_payload_in_place

                async def call_after_substitution() -> StubHook:
                    await deliver_payload_in_place(args_payload)
                    return target_hook.call(path, args_payload)

                return PromiseStubHook(
                    asyncio.ensure_future(call_after_substitution())
                )

            # Synchronous: application execution runs behind the hook.
            return target_hook.call(path, args_payload)

        if isinstance(expression, WireRemap):
            return self._evaluate_remap(expression)

        # Arbitrary expression (plain data, embedded capabilities, ...):
        # evaluate through the Parser into a payload-backed hook.
        parser = self._new_parser()
        payload = parser.parse(expression)
        return self._payload_hook_with_substitution(payload)

    def _evaluate_remap(self, remap: WireRemap) -> StubHook:
        """Evaluate a remap (map) expression synchronously.

        ONE evaluation path: delegates to ``Parser.evaluate_remap`` — the
        same TS-evaluator port (serialize.ts:904-950) used when a remap
        appears inside a value — with the session as Importer. Protocol
        errors raise, aborting the session like TS.
        """
        return self._new_parser().evaluate_remap(remap.to_json())

    def _handle_resolve(self, import_id: int, value: Any) -> None:
        """Handle a resolve message (rpc.ts:1005-1031)."""
        parser = self._new_parser()

        entry = self._imports.get(import_id)
        if entry is None:
            # Probably released already, so we don't care about the resolution
            # itself — but we must still evaluate it and immediately dispose it
            # so any stubs it contains get released (rpc.ts:1024-1028).
            payload = parser.parse(value)
            self._dispose_parsed_payload(payload)
            return

        payload = parser.parse(value)
        entry.resolve(self._payload_hook_with_substitution(payload))

    def _handle_reject(self, import_id: int, error: Any) -> None:
        """Handle a reject message (rpc.ts:1013-1019).

        Routed through entry.resolve() with an ErrorStubHook so a reject also
        sends the release message. Orphaned rejects are dropped (TS only
        evaluates orphaned resolve expressions — errors carry no stubs).
        """
        entry = self._imports.get(import_id)
        if entry is None:
            return

        entry.resolve(ErrorStubHook(self._wire_to_error(error)))

    def _wire_to_error(self, error: Any) -> Exception:
        """Convert a wire reject/abort expression into an Exception."""
        if isinstance(error, WireError):
            return RpcError.from_wire(
                error.error_type,
                error.message,
                data=error.data,
                stack=error.stack,
            )
        # General expression: evaluate it and use the value as the reason.
        try:
            payload = self._new_parser().parse(error)
            value = payload.value
        except Exception:
            value = error
        if isinstance(value, Exception):
            return value
        return RpcError.internal(str(value))

    def _dispose_parsed_payload(self, payload: RpcPayload) -> None:
        """Dispose a parsed payload including stubs embedded in its value.

        Parser output (RpcPayload.owned) doesn't populate the payload's stub
        tracking lists, so walk the value tree explicitly.
        """
        payload.dispose()
        self._dispose_embedded_stubs(payload.value)

    def _dispose_embedded_stubs(self, value: Any) -> None:
        from capnweb.stubs import RpcPromise, RpcStub

        if isinstance(value, (RpcStub, RpcPromise)):
            value._hook.dispose()
        elif isinstance(value, list):
            for item in value:
                self._dispose_embedded_stubs(item)
        elif isinstance(value, dict):
            for item in value.values():
                self._dispose_embedded_stubs(item)

    def _release_export(self, export_id: int, refcount: int) -> None:
        """Release an export (receive side, rpc.ts:547-561).

        Raises (-> session abort) on unknown IDs or refcounts that would go
        negative. At zero, frees the table slot, cleans the reverse maps and
        disposes the hook.
        """
        entry = self._exports.get(export_id)
        if entry is None:
            raise RpcError.bad_request(f"no such export ID: {export_id}")
        if entry.refcount < refcount:
            raise RpcError.bad_request(
                f"refcount would go negative: {entry.refcount} < {refcount}"
            )
        entry.refcount -= refcount
        if entry.refcount == 0:
            del self._exports[export_id]
            for key in (entry.reverse_key, id(entry.hook)):
                if key is not None and self._reverse_exports.get(key) == export_id:
                    del self._reverse_exports[key]
            target = getattr(entry.hook, "target", None)
            if target is not None and self._target_exports.get(id(target)) == export_id:
                del self._target_exports[id(target)]
            entry.hook.dispose()

    def _handle_peer_abort(self, error: Any) -> None:
        """Handle an incoming abort (rpc.ts:1045-1050).

        Uses the parsed expression as the abort reason and NEVER echoes an
        abort message back to the already-aborting peer.
        """
        self._abort(self._wire_to_error(error), send_abort=False)

    def _abort(self, reason: Exception, send_abort: bool = True) -> None:
        """Abort the session (rpc.ts:864-927).

        Args:
            reason: The error that caused the abort
            send_abort: Whether to try sending an abort message to peer
        """
        if self._abort_reason is not None:
            return

        # Cancel the read loop directly (no poll tick). Skip when we're being
        # called from inside the read loop itself — it checks _abort_reason
        # and returns on its own.
        if (
            self._read_loop_task is not None
            and not self._read_loop_task.done()
            and self._read_loop_task is not asyncio.current_task()
        ):
            self._read_loop_task.cancel()

        # Cancel pipe pump tasks; stop() awaits them. Parked stream writers
        # unblock via the import-entry rejection sweep below (their in-flight
        # write futures reject -> FlowController.on_error -> proxy errors).
        for pump_task in list(self._pump_tasks):
            if not pump_task.done():
                pump_task.cancel()

        # Best-effort abort message to the peer. Must be enqueued BEFORE
        # _abort_reason is set (the writer path drops post-abort sends);
        # the transport is aborted only after the frame has flushed.
        transport_abort_scheduled = False
        if send_abort:
            try:
                if not isinstance(reason, Exception):
                    reason = RpcError.internal(str(reason))
                frame = self._serialize_frame(
                    WireAbort(self._serialize_error_expression(reason))
                )
                if self._writer_task is not None and not self._writer_task.done():
                    self._send_queue.put_nowait(frame)
                    self._abort_send_task = asyncio.create_task(
                        self._abort_transport_after_flush(reason)
                    )
                else:
                    self._abort_send_task = asyncio.create_task(
                        self._send_abort_then_abort_transport(frame, reason)
                    )
                transport_abort_scheduled = True
            except Exception:
                transport_abort_scheduled = False

        self._abort_reason = reason
        self._abort_event.set()

        # Reject all drain() waiters
        if self._drain_waiters:
            waiters, self._drain_waiters = self._drain_waiters, []
            for waiter in waiters:
                _fail_future(waiter, reason)

        # Call transport abort handler now unless the abort-message flush
        # task will do it after sending.
        if not transport_abort_scheduled:
            self._abort_transport(reason)

        # Call all onBroken callbacks in ORIGINAL registration order (index
        # order — the resolve-transfer hack preserves original slots).
        for _idx, callback in sorted(self._on_broken_callbacks.items()):
            try:
                callback(reason)
            except Exception:
                pass  # Treat as unhandled rejection
        self._on_broken_callbacks.clear()

        # Reject all UNRESOLVED imports; already-resolved resolutions stay
        # intact and usable (rpc.ts:263-276). The table itself is not cleared
        # (TS keeps entries; all sends are dead anyway).
        for entry in list(self._imports.values()):
            entry.reject(reason)

        # Dispose all exports; cancel in-flight resolve tasks for promptness
        # (their sends would no-op post-abort anyway).
        for entry in list(self._exports.values()):
            if entry.pull_task is not None and not entry.pull_task.done():
                entry.pull_task.cancel()
            entry.hook.dispose()
        self._exports.clear()
        self._reverse_exports.clear()
        self._target_exports.clear()

    def _abort_transport(self, reason: Exception) -> None:
        if hasattr(self.transport, 'abort') and self.transport.abort:
            try:
                self.transport.abort(reason)
            except Exception:
                pass

    async def _abort_transport_after_flush(self, reason: Exception) -> None:
        """Let the writer flush the queued abort frame, then abort the transport.

        The join is BOUNDED: if the writer dies without consuming the abort
        frame (send failure during teardown races), we abort the transport
        anyway instead of hanging forever (B3 deadlock report, 2026-07-04).
        """
        try:
            await asyncio.wait_for(self._send_queue.join(), STOP_FLUSH_TIMEOUT)
        except Exception:
            pass
        self._abort_transport(reason)

    async def _send_abort_then_abort_transport(self, frame: Any, reason: Exception) -> None:
        """Send abort message (best-effort) and then abort the transport."""
        try:
            async with self._send_lock:
                await self.transport.send(frame)
        except Exception:
            pass
        finally:
            self._abort_transport(reason)

    # -------------------------------------------------------------------------
    # Importer protocol (for parsing capabilities we receive)
    # -------------------------------------------------------------------------

    def import_capability(self, import_id: int) -> StubHook:
        """Import a capability from the peer.

        This is called by the Parser when it encounters ["export", id] in
        the wire format. We create an ImportEntry if one doesn't exist.
        """
        entry = self._imports.get(import_id)
        if entry is None:
            self._guard_import_capacity()
            entry = ImportEntry(import_id=import_id, session=self)
            self._imports[import_id] = entry

        return ImportHook.from_entry(self, entry, is_promise=False)

    def _guard_import_capacity(self) -> None:
        """F2: bound the import table before creating a new entry.

        Counts LIVE entries. A single frame carrying many distinct
        ``["export", id]`` refs (import-table amplification) is capped here;
        raising aborts the session (parse disposes partial imports first).
        """
        if len(self._imports) >= self._options.max_imports:
            raise RpcError.bad_request(
                f"import table limit exceeded "
                f"({self._options.max_imports} entries)"
            )

    def create_promise_hook(self, promise_id: int) -> StubHook:
        """Create a promise hook for ["promise", id] (TS importPromise,
        rpc.ts:655-668).

        Reusing an existing export ID for a promise is a peer bug: return an
        ErrorStubHook. The entry is created in the already-pulling state —
        promises embedded in values are auto-resolved by the sender, so no
        pull message is ever sent for them.
        """
        if promise_id in self._imports:
            return ErrorStubHook(RpcError.bad_request(
                "Bug in RPC system: The peer sent a promise reusing an "
                "existing export ID."
            ))

        self._guard_import_capacity()  # F2: bound import-table growth
        entry = ImportEntry(
            import_id=promise_id,
            session=self,
            pulling=True,  # auto-resolved by the sender; never send a pull
        )
        self._imports[promise_id] = entry
        return ImportHook.from_entry(self, entry, is_promise=True)

    def get_export(self, export_id: int) -> StubHook | None:
        """Get an export by ID (for remap - sender passing our object back).

        When we receive ["import", id] or ["remap", id, ...] in an expression,
        the id refers to our export table. The sender is passing back an object
        we previously exported to them.

        Args:
            export_id: The export ID to look up

        Returns:
            The StubHook for this export, or None if not found
        """
        entry = self._exports.get(export_id)
        if entry is None:
            return None
        return entry.hook


class ImportHook(StubHook):
    """A hook representing an imported capability from the peer.

    Like TypeScript's RpcImportHook, this holds a direct reference to the
    ImportEntry, not just the import_id. This is important because the entry
    is removed from _imports as soon as a release is sent, but we still need
    access to the resolution.
    """
    __slots__ = ('session', 'import_id', 'is_promise', '_disposed', '_entry')

    def __init__(
        self,
        session: BidirectionalSession,
        import_id: int,
        is_promise: bool = False,
    ) -> None:
        self.session = session
        self.import_id = import_id
        self.is_promise = is_promise
        self._disposed = False
        self._entry: ImportEntry | None = None

        # Try to get existing entry and increment refcount (like TypeScript)
        entry = session._imports.get(import_id)
        if entry is not None:
            entry.local_refcount += 1
            self._entry = entry

    @classmethod
    def from_entry(
        cls,
        session: BidirectionalSession,
        entry: ImportEntry,
        is_promise: bool = False,
    ) -> "ImportHook":
        """Create an ImportHook from an existing entry.

        This is the preferred way to create ImportHook when you already have
        the entry (e.g., from send_call). It ensures the entry reference is
        captured and refcount is incremented.
        """
        hook = cls.__new__(cls)
        hook.session = session
        hook.import_id = entry.import_id
        hook.is_promise = is_promise
        hook._disposed = False
        hook._entry = entry
        entry.local_refcount += 1
        return hook

    def _get_entry(self) -> ImportEntry:
        """Get the import entry.

        If we already have a cached entry, return it. Otherwise, look up
        in the session's imports table and cache it.
        """
        if self._entry is not None:
            return self._entry

        entry = self.session._imports.get(self.import_id)
        if entry is None:
            # Create new entry if it doesn't exist
            entry = ImportEntry(import_id=self.import_id, session=self.session)
            self.session._imports[self.import_id] = entry

        # Increment local refcount and cache
        entry.local_refcount += 1
        self._entry = entry
        return entry

    def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Call a method on this import (synchronous).

        This is synchronous to ensure messages are queued before the batch
        is sent, matching TypeScript's behavior.
        """
        # Check if session is aborted
        if self.session._abort_reason:
            raise self.session._abort_reason

        entry = self._get_entry()

        # If already resolved, delegate to resolution
        if entry.resolution:
            return entry.resolution.call(path, args)

        # Send call to peer (synchronous)
        return self.session.send_call(self.import_id, path, args)

    def stream(
        self, path: list[str | int], args: RpcPayload
    ) -> tuple[Any, int | None]:
        """Streaming call (rpc.ts:335-341, C-STREAM).

        Unresolved: route through session.send_stream — one ["stream"]
        frame whose resolve doubles as the flow-control ack; returns the
        frame's byte size for the FlowController. Resolved: delegate to the
        resolution's stream semantics.
        """
        if self.session._abort_reason:
            raise self.session._abort_reason

        entry = self._get_entry()

        if entry.resolution:
            from capnweb.streams import hook_stream

            return hook_stream(entry.resolution, path, args)

        return self.session.send_stream(self.import_id, path, args)

    def get(self, path: list[str | int]) -> StubHook:
        """Get a property on this import (rpc.ts:362-369).

        Unresolved: sends a property-reference push ``["pipeline", id,
        path]`` (no args — TS sendCall without args) and returns the new
        promise import. Callers reach here only on await/dup/devaluation —
        RpcPromise accumulates paths lazily — so exactly one fused push goes
        out per materialized chain.
        """
        entry = self._get_entry()
        if entry.resolution:
            return entry.resolution.get(path)

        return self.session.send_call(self.import_id, path)

    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        """Send a map operation to the peer (synchronous, like TS)."""
        try:
            entry = self._get_entry()
        except Exception:
            for cap in captures:
                cap.dispose()
            raise

        # If already resolved, delegate to resolution
        if entry.resolution:
            return entry.resolution.map(path, captures, instructions)

        return self.session.send_map(self.import_id, path, captures, instructions)

    async def pull(self) -> RpcPayload:
        """Pull the value of this import."""
        entry = self._get_entry()

        if not self.is_promise:
            raise RpcError.bad_request(
                "Can't pull this hook because it's not a promise hook."
            )

        if entry.resolution:
            return await entry.resolution.pull()

        # Need to wait for resolution. The waiter future is created lazily
        # (promise imports are born `pulling` with no future yet).
        if entry.pending_pull is None:
            entry.pending_pull = asyncio.get_running_loop().create_future()
            if not entry.pulling:
                entry.pulling = True
                self.session.send_pull(self.import_id)  # sync

        # Bounded wait (hardening fix P0): a lost or mis-routed resolve must
        # surface as an error, not a permanent hang.
        timeout = self.session._pull_timeout
        try:
            if timeout is None:
                resolved_hook = await entry.pending_pull
            else:
                resolved_hook = await asyncio.wait_for(
                    asyncio.shield(entry.pending_pull), timeout
                )
        except asyncio.TimeoutError as e:
            raise RpcError.internal(
                f"pull timed out after {timeout}s waiting for import "
                f"{self.import_id} to resolve (peer sent no resolve/reject)"
            ) from e
        return await resolved_hook.pull()

    def ignore_unhandled_rejections(self) -> None:
        """Nothing to do - rejections are handled by the session."""
        pass

    def dispose(self) -> None:
        """Dispose this hook (rpc.ts:395-403).

        At local refcount zero the ENTRY is disposed, which for an unresolved
        import cancels it: rejects the pending pull and sends a release so
        the peer stops computing.
        """
        if self._disposed:
            return
        self._disposed = True

        entry = self._entry
        self._entry = None
        if entry is not None:
            entry.local_refcount -= 1
            if entry.local_refcount <= 0:
                entry.dispose()

    def dup(self) -> "ImportHook":
        """Duplicate this hook. Dups are always non-promise (rpc.ts:371-373)."""
        return ImportHook.from_entry(self.session, self._get_entry(), is_promise=False)

    def on_broken(self, callback: Any) -> None:
        """Register callback for when connection breaks."""
        entry = self._get_entry()
        entry.on_broken(callback)


class MainImportHook(ImportHook):
    """Hook for import 0 (the peer's main capability).

    Disposing it shuts the whole session down, matching TS RpcMainHook
    (rpc.ts:412-427).
    """
    __slots__ = ()

    def __init__(self, session: BidirectionalSession) -> None:
        super().__init__(session, 0, is_promise=False)

    def dispose(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        self._entry = None
        self.session.shutdown()


# NOTE: the old PipelineHook (a lazy path-carrying wrapper around an import)
# was DELETED in parity stream B2: RpcPromise now accumulates paths lazily
# itself (TS pathIfPromise), so property access never needed a hook at all,
# and materialization goes through ImportHook.get -> session.send_call
# (property-reference push), exactly like TS RpcImportHook.get.
