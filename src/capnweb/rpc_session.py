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

Production features:
- reverseExports map for O(1) capability lookup
- sendRelease on resolve to prevent memory leaks
- onBroken callbacks for connection death handling
- pullCount tracking and drain() for graceful shutdown
- Proper abort message to peer
- Async events instead of polling
- RpcSessionOptions for error redaction
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from capnweb.error import RpcError
from capnweb.hooks import (
    ErrorStubHook,
    PayloadStubHook,
    StubHook,
    TargetStubHook,
)
from capnweb.payload import RpcPayload
from capnweb.wire import (
    PropertyKey,
    WireCapture,
    WireError,
    WirePipeline,
    WirePush,
    WirePull,
    WireReject,
    WireRemap,
    WireResolve,
    WireRelease,
    WireAbort,
    parse_wire_batch,
    serialize_wire_batch,
)

logger = logging.getLogger(__name__)

# Constants
READ_LOOP_TIMEOUT_SECONDS = 0.1
EXPORT_WAIT_TIMEOUT_SECONDS = 30.0


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
    """Entry in the exports table (capabilities we provide)."""
    __slots__ = ('hook', 'refcount', 'pull_task')
    
    def __init__(
        self,
        hook: StubHook,
        refcount: int = 1,
        pull_task: asyncio.Task[None] | None = None,
    ) -> None:
        self.hook = hook
        self.refcount = refcount
        self.pull_task = pull_task  # For auto-resolving promises


# Import Pydantic config - keep old class for backwards compatibility
from capnweb.config import RpcSessionConfig

# Backwards compatibility alias
RpcSessionOptions = RpcSessionConfig


class ImportEntry:
    """Entry in the imports table (capabilities we're waiting for)."""
    __slots__ = (
        'import_id', 'session', 'resolution', 'pending_pull',
        'local_refcount', 'remote_refcount', 'on_broken_callbacks'
    )
    
    def __init__(
        self,
        import_id: int,
        session: "BidirectionalSession",
        resolution: StubHook | None = None,
        pending_pull: asyncio.Future[StubHook] | None = None,
        local_refcount: int = 0,
        remote_refcount: int = 1,
        on_broken_callbacks: list[int] | None = None,
    ) -> None:
        self.import_id = import_id
        self.session = session
        self.resolution = resolution
        self.pending_pull = pending_pull
        self.local_refcount = local_refcount  # Start at 0, incremented by ImportHook
        self.remote_refcount = remote_refcount
        self.on_broken_callbacks = on_broken_callbacks  # Indices into session callbacks
    
    def resolve(self, hook: StubHook) -> None:
        """Resolve this import with a hook."""
        if self.local_refcount == 0:
            # Already disposed, ignore resolution
            hook.dispose()
            return
        
        self.resolution = hook
        self._send_release()  # Release remote reference now that we have resolution
        
        # Transfer onBroken callbacks to the resolution
        if self.on_broken_callbacks:
            for idx in self.on_broken_callbacks:
                callback = self.session.get_on_broken_callback(idx)
                if callback:
                    hook.on_broken(callback)
                    self.session.remove_on_broken_callback(idx)
            self.on_broken_callbacks = None
        
        if self.pending_pull and not self.pending_pull.done():
            self.pending_pull.set_result(hook)
    
    def reject(self, error: Exception) -> None:
        """Reject this import with an error."""
        self.resolution = ErrorStubHook(error)
        self.on_broken_callbacks = None  # Session will call all callbacks
        if self.pending_pull and not self.pending_pull.done():
            self.pending_pull.set_exception(error)
    
    def dispose(self) -> None:
        """Dispose this import entry."""
        if self.resolution:
            self.resolution.dispose()
        else:
            self.reject(RpcError.internal("RPC was canceled because the RpcPromise was disposed."))
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
        
        # Track push sequence from peer (they assign sequential positive IDs)
        self._peer_push_count = 0
        
        # Track our push sequence (peer will assign these as export IDs)
        self._our_push_count = 0
        
        # Abort state
        self._abort_reason: Exception | None = None
        self._abort_event = asyncio.Event()
        
        # Message loop task
        self._read_loop_task: asyncio.Task | None = None
        
        # onBroken callbacks: sparse dict of index -> callback
        self._on_broken_callbacks: dict[int, Callable[[Exception], None]] = {}
        
        # Pull count for drain() - how many promises peer expects us to resolve
        self._pull_count = 0
        self._on_batch_done: asyncio.Future[None] | None = None
        
        # Pending push tasks for drain() - track background push handlers
        self._pending_push_tasks: set[asyncio.Task] = set()
        
        # Pending export events for async waiting (replaces polling)
        self._export_events: dict[int, asyncio.Event] = {}
        
        # Register local main as export 0
        if local_main is not None:
            if hasattr(local_main, 'call') and hasattr(local_main, 'get_property'):
                # It's an RpcTarget - track by target id for deduplication
                hook = TargetStubHook(local_main)
                self._target_exports[id(local_main)] = 0
            else:
                hook = PayloadStubHook(RpcPayload.from_app_return(local_main))
            self._exports[0] = ExportEntry(hook=hook)
        
        # Import 0 is the peer's main capability
        self._imports[0] = ImportEntry(import_id=0, session=self)
    
    def start(self) -> None:
        """Start the message read loop."""
        if self._read_loop_task is None:
            self._read_loop_task = asyncio.create_task(self._read_loop())
    
    async def stop(self) -> None:
        """Stop the session."""
        self._abort(RpcError.internal("Session stopped"))
        if self._read_loop_task:
            self._read_loop_task.cancel()
            try:
                await self._read_loop_task
            except asyncio.CancelledError:
                pass
    
    def get_main_stub(self) -> StubHook:
        """Get a hook for the peer's main capability."""
        return ImportHook(self, 0)
    
    async def drain(self) -> None:
        """Wait for all pending operations to complete.
        
        This is useful for graceful shutdown - wait for all promises
        that the peer is expecting us to resolve, and all push handlers
        to finish processing.
        """
        if self._abort_reason:
            raise self._abort_reason
        
        # Wait for all pending push tasks to complete
        if self._pending_push_tasks:
            await asyncio.gather(*self._pending_push_tasks, return_exceptions=True)
        
        # Wait for all pending pulls to complete
        if self._pull_count > 0:
            self._on_batch_done = asyncio.get_event_loop().create_future()
            await self._on_batch_done
    
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
        """Register an onBroken callback and return its index."""
        idx = len(self._on_broken_callbacks)
        self._on_broken_callbacks[idx] = callback
        return idx
    
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
            from capnweb.serializer import Serializer
            serializer = Serializer(exporter=self)
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
        
        # Create import entry for the result
        # The peer will assign export ID = their push count + 1
        # We track this with _our_push_count
        self._our_push_count += 1
        import_id = self._our_push_count  # This matches peer's export ID
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
    
    def _send_sync(self, messages: list[Any]) -> None:
        """Queue messages to be sent to the peer (synchronous).
        
        This queues messages to the transport without awaiting.
        The transport will send them when the batch is flushed.
        """
        if self._abort_reason:
            return  # Don't send after abort
        batch = serialize_wire_batch(messages)
        # Queue to transport synchronously - transport.send() should not block
        # for batch transports (it just queues the message)
        asyncio.create_task(self._send_async(batch))
    
    async def _send_async(self, batch: str) -> None:
        """Actually send the batch to the transport."""
        try:
            await self.transport.send(batch)
        except Exception as e:
            # If send fails, abort without trying to send abort message
            self._abort(e, send_abort=False)
    
    async def _send(self, messages: list[Any]) -> None:
        """Send messages to the peer (async version)."""
        if self._abort_reason:
            return  # Don't send after abort
        batch = serialize_wire_batch(messages)
        try:
            await self.transport.send(batch)
        except Exception as e:
            # If send fails, abort without trying to send abort message
            self._abort(e, send_abort=False)
    
    def _send_release(self, import_id: int, refcount: int) -> None:
        """Send a release message to peer (sync, fire-and-forget).
        
        Note: Unlike TypeScript, we don't delete the entry from _imports here.
        The entry is kept until all local references (ImportHooks) are disposed.
        This allows UnifiedClient.call() to find resolved entries by import_id.
        """
        if self._abort_reason:
            return
        release = WireRelease(import_id, refcount)
        asyncio.create_task(self._send([release]))
        # Don't delete entry here - keep it for local lookups
        # Entry will be cleaned up when all ImportHooks are disposed
    
    async def send_map(
        self,
        target_id: int,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> ImportHook:
        """Send a map operation to the peer.
        
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
        
        # Convert captures to wire format
        wire_captures: list[WireCapture] = []
        for cap in captures:
            # Check if this is one of our imports
            if isinstance(cap, ImportHook) and cap.session is self:
                wire_captures.append(WireCapture("import", cap.import_id))
            else:
                # Export the capture
                export_id = self._export_hook(cap)
                wire_captures.append(WireCapture("export", export_id))
        
        # Create remap expression
        path_keys = [PropertyKey(p) for p in path] if path else None
        
        remap = WireRemap(
            import_id=target_id,
            property_path=path_keys,
            captures=wire_captures,
            instructions=instructions,
        )
        
        # Send push message with remap
        push = WirePush(remap)
        await self._send([push])
        
        # Create import entry for the result
        self._our_push_count += 1
        import_id = self._our_push_count
        entry = ImportEntry(import_id=import_id, session=self)
        self._imports[import_id] = entry
        
        return ImportHook(self, import_id)
    
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
        self._exports[export_id] = ExportEntry(hook=hook.dup() if dup else hook)
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
        self._exports[export_id] = ExportEntry(hook=hook.dup())
        self._reverse_exports[id(hook)] = export_id
        
        # Start auto-resolving the promise
        self._ensure_resolving_export(export_id)
        
        return export_id
    
    def _ensure_resolving_export(self, export_id: int) -> None:
        """Ensure we're resolving an export (for promises).
        
        This starts a background task to pull the promise and send
        the resolution to the peer.
        """
        entry = self._exports.get(export_id)
        if entry is None or entry.pull_task is not None:
            return
        
        async def resolve_export() -> None:
            self._pull_count += 1
            try:
                payload = await entry.hook.pull()
                
                # Serialize and send resolve
                from capnweb.serializer import Serializer
                serializer = Serializer(exporter=self)
                serialized = serializer.serialize_payload(payload)
                
                resolve_msg = WireResolve(export_id, serialized)
                await self._send([resolve_msg])
                
            except RpcError as e:
                # Send reject with proper error code
                error = WireError(str(e.code.value), e.message)
                reject_msg = WireReject(export_id, error)
                await self._send([reject_msg])
            except Exception as e:
                # Send reject with internal error for unexpected exceptions
                logger.exception("Error resolving export %d", export_id)
                error = WireError("internal", str(e))
                reject_msg = WireReject(export_id, error)
                await self._send([reject_msg])
            finally:
                self._pull_count -= 1
                if self._pull_count == 0 and self._on_batch_done:
                    self._on_batch_done.set_result(None)
                    self._on_batch_done = None
        
        entry.pull_task = asyncio.create_task(resolve_export())
    
    # -------------------------------------------------------------------------
    # Message handling
    # -------------------------------------------------------------------------
    
    async def _read_loop(self) -> None:
        """Main message processing loop.
        
        This loop must never block on anything other than receiving messages,
        otherwise nested calls (server calling client while processing client call)
        will deadlock.
        """
        from capnweb.batch import BatchEndError
        
        try:
            while not self._abort_reason:
                try:
                    # Use wait_for with a short timeout to allow checking abort
                    message = await asyncio.wait_for(
                        self.transport.receive(),
                        timeout=READ_LOOP_TIMEOUT_SECONDS
                    )
                    # Handle message in background to avoid blocking
                    asyncio.create_task(self._handle_message_safe(message))
                except asyncio.TimeoutError:
                    # Check abort and continue
                    continue
                except asyncio.CancelledError:
                    break
                except BatchEndError:
                    # Normal end of batch session - not an error
                    break
                except ConnectionError:
                    # Normal connection close (e.g., WebSocket closed)
                    break
                except Exception as e:
                    if not self._abort_reason:
                        logger.exception("Error in read loop")
                        self._abort(e)
                    break
                        
        except asyncio.CancelledError:
            pass
    
    async def _handle_message_safe(self, data: str) -> None:
        """Handle message with error catching."""
        try:
            await self._handle_message(data)
        except Exception as e:
            logger.exception("Error handling message")
            self._abort(e)
    
    async def _handle_message(self, data: str) -> None:
        """Handle an incoming message batch."""
        messages = parse_wire_batch(data)
        responses: list[Any] = []
        
        for msg in messages:
            response = await self._process_message(msg)
            if response:
                if isinstance(response, list):
                    responses.extend(response)
                else:
                    responses.append(response)
        
        if responses:
            await self._send(responses)
    
    async def _process_message(self, msg: Any) -> Any | None:
        """Process a single message and return optional response."""
        match msg:
            case WirePush():
                # Handle push in background to avoid blocking read loop
                # This allows nested calls (server calling client while processing client call)
                task = asyncio.create_task(self._handle_push_async(msg))
                self._pending_push_tasks.add(task)
                task.add_done_callback(self._pending_push_tasks.discard)
                return None
            
            case WirePull():
                return await self._handle_pull(msg.import_id)
            
            case WireResolve(export_id, value):
                self._handle_resolve(export_id, value)
                return None
            
            case WireReject(export_id, error):
                self._handle_reject(export_id, error)
                return None
            
            case WireRelease(import_id, refcount):
                self._handle_release(import_id, refcount)
                return None
            
            case WireAbort(error):
                self._abort(RpcError.internal(f"Peer aborted: {error}"))
                return None
            
            case _:
                logger.warning(f"Unknown message type: {type(msg)}")
                return None
    
    async def _handle_push_async(self, msg: WirePush) -> None:
        """Handle push in background and send response directly."""
        response = await self._handle_push(msg)
        if response:
            await self._send([response])
    
    async def _handle_push(self, msg: WirePush) -> Any | None:
        """Handle a push message (peer is calling us).
        
        When we receive a push, we:
        1. Evaluate the pipeline or remap expression
        2. Store the result as an export (peer will pull it later)
        """
        # Assign sequential export ID for this push result
        export_id = self._peer_push_count + 1
        self._peer_push_count += 1
        
        try:
            expression = msg.expression
            
            # Handle remap (map operation)
            if isinstance(expression, WireRemap):
                return await self._handle_remap(export_id, expression)
            
            if not isinstance(expression, WirePipeline):
                error = WireError("bad_request", "Expected pipeline or remap expression")
                return WireReject(export_id, error)
            
            # Get target from our exports
            target_entry = self._exports.get(expression.import_id)
            if target_entry is None:
                error = WireError("not_found", f"Capability {expression.import_id} not found")
                return WireReject(export_id, error)
            
            target_hook = target_entry.hook
            
            # Parse args through Parser to convert ["export", id] into RpcStub
            # NOTE: Per TypeScript behavior, pipeline args are sent UN-ESCAPED on the wire.
            # TypeScript wraps them before parsing: evaluate([args]) (see serialize.ts line 442).
            # We match this by wrapping the args before parsing.
            from capnweb.parser import Parser
            parser = Parser(importer=self)
            raw_args = expression.args if expression.args is not None else []
            # Wrap args to match TypeScript's evaluate([args]) behavior
            args_payload = parser.parse([raw_args])
            
            # Extract path
            path: list[str | int] = [
                str(pk.value) for pk in (expression.property_path or [])
            ]
            
            # Execute the call (synchronous - returns StubHook directly)
            result_hook = target_hook.call(path, args_payload)
            
            # Ignore unhandled rejections for the result
            result_hook.ignore_unhandled_rejections()
            
            # Store result as export for later pull
            self._exports[export_id] = ExportEntry(hook=result_hook)
            
            # Signal any waiting pull handlers
            if export_id in self._export_events:
                self._export_events[export_id].set()
            
            return None  # Result will be pulled
            
        except RpcError as e:
            error = WireError(str(e.code.value), e.message)
            return WireReject(export_id, error)
        except Exception as e:
            logger.exception("Error handling push")
            error = WireError("internal", str(e))
            return WireReject(export_id, error)
    
    async def _handle_remap(self, export_id: int, remap: WireRemap) -> Any | None:
        """Handle a remap (map) expression.
        
        Args:
            export_id: The export ID for the result
            remap: The remap expression
            
        Returns:
            None on success, WireReject on error
        """
        try:
            # Get target from our exports
            target_entry = self._exports.get(remap.import_id)
            if target_entry is None:
                error = WireError("not_found", f"Capability {remap.import_id} not found")
                return WireReject(export_id, error)
            
            target_hook = target_entry.hook
            
            # Resolve captures
            # When sender sends ["import", id]: sender's import = our export
            # When sender sends ["export", id]: sender's export = our import
            captures: list[StubHook] = []
            for capture in remap.captures:
                if capture.type == "import":
                    # Sender's import = our export (we exported it to them)
                    exp_entry = self._exports.get(capture.id)
                    if exp_entry:
                        captures.append(exp_entry.hook.dup())
                    else:
                        error = WireError("not_found", f"Export {capture.id} not found")
                        # Dispose already-created captures
                        for cap in captures:
                            cap.dispose()
                        return WireReject(export_id, error)
                else:  # export
                    # Sender's export = our import (they exported it to us)
                    imp_entry = self._imports.get(capture.id)
                    if imp_entry and imp_entry.resolution:
                        captures.append(imp_entry.resolution.dup())
                    else:
                        captures.append(ImportHook(self, capture.id))
            
            # Extract path
            path: list[str | int] = [
                pk.value for pk in (remap.property_path or [])
            ]
            
            # Execute the map operation
            result_hook = target_hook.map(path, captures, remap.instructions)
            
            # Ignore unhandled rejections
            result_hook.ignore_unhandled_rejections()
            
            # Store result as export
            self._exports[export_id] = ExportEntry(hook=result_hook)
            
            # Signal any waiting pull handlers
            if export_id in self._export_events:
                self._export_events[export_id].set()
            
            return None
            
        except RpcError as e:
            error = WireError(str(e.code.value), e.message)
            return WireReject(export_id, error)
        except Exception as e:
            logger.exception("Error handling remap")
            error = WireError("internal", str(e))
            return WireReject(export_id, error)
    
    async def _handle_pull(self, export_id: int) -> Any:
        """Handle a pull request (peer wants a result).
        
        If the export doesn't exist yet (push still processing), wait for it
        using async events instead of polling.
        """
        self._pull_count += 1
        try:
            # Wait for export to exist using async event
            entry = self._exports.get(export_id)
            if entry is None:
                # Create event and wait for export to be created
                if export_id not in self._export_events:
                    self._export_events[export_id] = asyncio.Event()
                
                try:
                    await asyncio.wait_for(
                        self._export_events[export_id].wait(),
                        timeout=EXPORT_WAIT_TIMEOUT_SECONDS
                    )
                except asyncio.TimeoutError:
                    error = WireError("timeout", f"Export {export_id} not created in time")
                    return WireReject(export_id, error)
                finally:
                    self._export_events.pop(export_id, None)
                
                entry = self._exports.get(export_id)
            
            if entry is None:
                error = WireError("not_found", f"Export {export_id} not found")
                return WireReject(export_id, error)
            
            payload = await entry.hook.pull()
            
            from capnweb.serializer import Serializer
            serializer = Serializer(exporter=self)
            serialized = serializer.serialize_payload(payload)
            
            return WireResolve(export_id, serialized)
            
        except RpcError as e:
            error = WireError(str(e.code.value), e.message)
            return WireReject(export_id, error)
        except Exception as e:
            logger.exception("Error handling pull")
            error = WireError("internal", str(e))
            return WireReject(export_id, error)
        finally:
            self._pull_count -= 1
            if self._pull_count == 0 and self._on_batch_done and not self._on_batch_done.done():
                self._on_batch_done.set_result(None)
                self._on_batch_done = None
    
    def _handle_resolve(self, import_id: int, value: Any) -> None:
        """Handle a resolve message (peer resolved our import)."""
        entry = self._imports.get(import_id)
        if entry is None:
            # Already released, ignore
            return
        
        from capnweb.parser import Parser
        parser = Parser(importer=self)
        payload = parser.parse(value)
        hook = PayloadStubHook(payload)
        entry.resolve(hook)
    
    def _handle_reject(self, import_id: int, error: Any) -> None:
        """Handle a reject message (peer rejected our import)."""
        entry = self._imports.get(import_id)
        if entry is None:
            return
        
        if isinstance(error, WireError):
            # Preserve the original error code from the wire
            rpc_error = RpcError.from_wire(error.error_type, error.message)
        else:
            rpc_error = RpcError.internal(str(error))
        entry.reject(rpc_error)
    
    def _handle_release(self, export_id: int, refcount: int) -> None:
        """Handle a release message (peer released our export)."""
        entry = self._exports.get(export_id)
        if entry is None:
            return
        
        entry.refcount -= refcount
        if entry.refcount <= 0:
            entry.hook.dispose()
            del self._exports[export_id]
    
    def _abort(self, reason: Exception, send_abort: bool = True) -> None:
        """Abort the session.
        
        Args:
            reason: The error that caused the abort
            send_abort: Whether to try sending an abort message to peer
        """
        if self._abort_reason is not None:
            return

        # Try to send abort message to peer.
        # Spec intent: send ["abort", error] and then terminate the session.
        # We therefore avoid aborting/closing the transport until after the
        # best-effort abort message send has run.
        if send_abort:
            try:
                error_msg = str(reason) if reason else "Unknown error"
                abort_msg = WireAbort(WireError("abort", error_msg))
                asyncio.create_task(self._send_abort_then_abort_transport(abort_msg, reason))
            except Exception:
                # Fall back to immediate transport abort below
                send_abort = False
        
        self._abort_reason = reason
        self._abort_event.set()
        
        # Reject batch done future
        if self._on_batch_done and not self._on_batch_done.done():
            self._on_batch_done.set_exception(reason)
        
        # Call transport abort handler.
        # If we scheduled send_abort_then_abort_transport, it will abort the
        # transport after sending; otherwise abort immediately.
        if not send_abort and hasattr(self.transport, 'abort') and self.transport.abort:
            try:
                self.transport.abort(reason)
            except Exception:
                pass
        
        # Call all onBroken callbacks
        for callback in list(self._on_broken_callbacks.values()):
            try:
                callback(reason)
            except Exception:
                pass  # Treat as unhandled rejection
        self._on_broken_callbacks.clear()
        
        # Reject all pending imports
        for entry in list(self._imports.values()):
            if entry.resolution is None:
                entry.reject(reason)
        
        # Dispose all exports
        for entry in list(self._exports.values()):
            entry.hook.dispose()
        
        # Clear export events
        for event in self._export_events.values():
            event.set()  # Unblock any waiting handlers
        self._export_events.clear()
    
    async def _send_abort_then_abort_transport(self, abort_msg: WireAbort, reason: Exception) -> None:
        """Send abort message (best-effort) and then abort the transport."""
        try:
            batch = serialize_wire_batch([abort_msg])
            await self.transport.send(batch)
        except Exception:
            pass
        finally:
            if hasattr(self.transport, 'abort') and self.transport.abort:
                try:
                    self.transport.abort(reason)
                except Exception:
                    pass
    
    # -------------------------------------------------------------------------
    # Importer protocol (for parsing capabilities we receive)
    # -------------------------------------------------------------------------
    
    def import_capability(self, import_id: int) -> StubHook:
        """Import a capability from the peer.
        
        This is called by the Parser when it encounters ["export", id] in
        the wire format. We create an ImportEntry if one doesn't exist.
        """
        # Create entry if it doesn't exist
        if import_id not in self._imports:
            entry = ImportEntry(import_id=import_id, session=self)
            self._imports[import_id] = entry
        
        return ImportHook.from_entry(self, self._imports[import_id])
    
    def create_promise_hook(self, promise_id: int) -> StubHook:
        """Create a promise hook."""
        return ImportHook(self, promise_id, is_promise=True)

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
    may be removed from _imports after resolve, but we still need access to
    the resolution.
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
    
    def get(self, path: list[str | int]) -> StubHook:
        """Get a property on this import.
        
        Returns a PipelineHook that will send a pipelined call when used.
        """
        entry = self._get_entry()
        if entry.resolution:
            return entry.resolution.get(path)
        
        # Return a pipeline hook that chains the property access
        return PipelineHook(self.session, self.import_id, path)
    
    def map(
        self,
        path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        """Send a map operation to the peer."""
        entry = self._get_entry()
        
        # If already resolved, delegate to resolution
        if entry.resolution:
            return entry.resolution.map(path, captures, instructions)
        
        # Send map to peer - this needs to be async but we return a promise hook
        async def do_map() -> StubHook:
            return await self.session.send_map(self.import_id, path, captures, instructions)
        
        from capnweb.hooks import PromiseStubHook
        future: asyncio.Future[StubHook] = asyncio.ensure_future(do_map())
        return PromiseStubHook(future)
    
    async def pull(self) -> RpcPayload:
        """Pull the value of this import."""
        entry = self._get_entry()
        
        if entry.resolution:
            return await entry.resolution.pull()
        
        # Need to wait for resolution
        if entry.pending_pull is None:
            entry.pending_pull = asyncio.Future()
            self.session.send_pull(self.import_id)  # sync
        
        resolved_hook = await entry.pending_pull
        return await resolved_hook.pull()
    
    def ignore_unhandled_rejections(self) -> None:
        """Nothing to do - rejections are handled by the session."""
        pass
    
    def dispose(self) -> None:
        """Dispose this hook."""
        if self._disposed:
            return
        self._disposed = True
        
        entry = self.session._imports.get(self.import_id)
        if entry:
            entry.local_refcount -= 1
            if entry.local_refcount <= 0 and entry.resolution:
                entry.resolution.dispose()
    
    def dup(self) -> "ImportHook":
        """Duplicate this hook."""
        entry = self._get_entry()
        entry.local_refcount += 1
        return ImportHook(self.session, self.import_id, self.is_promise)
    
    def on_broken(self, callback: Any) -> None:
        """Register callback for when connection breaks."""
        entry = self._get_entry()
        entry.on_broken(callback)


class PipelineHook(StubHook):
    """A hook representing a pipelined property access on an import.
    
    This allows chaining property access and method calls on unresolved imports,
    sending them as pipelined calls to the peer.
    """
    __slots__ = ('session', 'import_id', 'path', '_disposed')
    
    def __init__(
        self,
        session: BidirectionalSession,
        import_id: int,
        path: list[str | int],
    ) -> None:
        self.session = session
        self.import_id = import_id
        self.path = path
        self._disposed = False
    
    def call(self, extra_path: list[str | int], args: RpcPayload) -> StubHook:
        """Call a method on this pipelined property (synchronous)."""
        if self.session._abort_reason:
            raise self.session._abort_reason
        
        # Combine paths
        full_path = self.path + extra_path
        
        # Send call to peer with full path (synchronous)
        return self.session.send_call(self.import_id, full_path, args)
    
    def get(self, extra_path: list[str | int]) -> StubHook:
        """Get a property on this pipelined property."""
        # Chain the path
        return PipelineHook(self.session, self.import_id, self.path + extra_path)
    
    def map(
        self,
        extra_path: list[str | int],
        captures: list[StubHook],
        instructions: list[Any],
    ) -> StubHook:
        """Send a map operation."""
        full_path = self.path + extra_path
        
        async def do_map() -> StubHook:
            return await self.session.send_map(self.import_id, full_path, captures, instructions)
        
        from capnweb.hooks import PromiseStubHook
        future: asyncio.Future[StubHook] = asyncio.ensure_future(do_map())
        return PromiseStubHook(future)
    
    async def pull(self) -> RpcPayload:
        """Pull the value - sends a call with no args to get the property value."""
        # For property access, we need to send a pull for the pipelined path
        # This is done by sending a call with empty args (send_call is sync)
        result_hook = self.session.send_call(self.import_id, self.path, RpcPayload.owned([]))
        return await result_hook.pull()
    
    def ignore_unhandled_rejections(self) -> None:
        """Nothing to do."""
        pass
    
    def dispose(self) -> None:
        """Dispose this hook."""
        self._disposed = True
    
    def dup(self) -> "PipelineHook":
        """Duplicate this hook."""
        return PipelineHook(self.session, self.import_id, self.path.copy())
    
    def on_broken(self, callback: Any) -> None:
        """Register callback for when connection breaks."""
        self.session.on_broken(callback)
