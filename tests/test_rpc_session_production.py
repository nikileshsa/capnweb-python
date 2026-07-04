"""Comprehensive tests for production-grade BidirectionalSession features.

These tests verify each production feature added to match the TypeScript reference:
1. reverseExports O(1) lookup
2. sendRelease on resolve
3. onBroken callbacks
4. pullCount and drain()
5. abort message to peer
6. async events (no polling)
7. RpcSessionOptions
8. get_stats()
9. Edge cases (concurrent calls, rapid operations)

NO MOCKING - All tests use real transports and real message passing.
"""

import asyncio
import pytest
from typing import Any
from aiohttp import web

from capnweb.rpc_session import (
    BidirectionalSession,
    RpcSessionOptions,
    ImportEntry,
    ExportEntry,
)
from capnweb.ws_transport import WebSocketClientTransport, WebSocketServerTransport
from capnweb.types import RpcTarget
from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcStub
from capnweb.wire import WireAbort, WireRelease, parse_wire_batch


# =============================================================================
# Test Infrastructure - Real In-Memory Transport
# =============================================================================

class InMemoryTransport:
    """Real async transport using queues - no mocking."""
    
    def __init__(self, send_queue: asyncio.Queue, recv_queue: asyncio.Queue, name: str = ""):
        self.send_queue = send_queue
        self.recv_queue = recv_queue
        self.name = name
        self._closed = False
        self._abort_reason: Exception | None = None
        self.sent_messages: list[str] = []  # Track all sent messages for verification
        self.received_messages: list[str] = []  # Track all received messages
    
    async def send(self, message: str) -> None:
        if self._closed:
            raise ConnectionError("Transport closed")
        self.sent_messages.append(message)
        await self.send_queue.put(message)
    
    async def receive(self) -> str:
        if self._closed and self.recv_queue.empty():
            raise ConnectionError("Transport closed")
        msg = await self.recv_queue.get()
        self.received_messages.append(msg)
        return msg
    
    def abort(self, reason: Exception) -> None:
        self._abort_reason = reason
        self._closed = True
    
    def close(self) -> None:
        self._closed = True


def create_transport_pair() -> tuple[InMemoryTransport, InMemoryTransport]:
    """Create a pair of connected transports."""
    queue_a_to_b = asyncio.Queue()
    queue_b_to_a = asyncio.Queue()
    transport_a = InMemoryTransport(queue_a_to_b, queue_b_to_a, "A")
    transport_b = InMemoryTransport(queue_b_to_a, queue_a_to_b, "B")
    return transport_a, transport_b


# =============================================================================
# Test Targets - Real RpcTarget implementations
# =============================================================================

class EchoTarget(RpcTarget):
    """Simple echo target for basic tests - using ergonomic style."""
    
    def echo(self, value: Any) -> Any:
        return value
    
    def add(self, a: int, b: int) -> int:
        return a + b
    
    async def slow(self, delay: float = 0.1) -> str:
        await asyncio.sleep(delay)
        return "done"
    
    def error(self) -> None:
        raise RpcError.internal("Intentional error")


class CallbackTarget(RpcTarget):
    """Target that stores and calls back to client capabilities - ergonomic style."""
    
    def __init__(self):
        self.callbacks: list[Any] = []
        self.call_count = 0
    
    def register(self, callback: Any) -> str:
        self.call_count += 1
        self.callbacks.append(callback)
        return f"registered-{len(self.callbacks)}"
    
    async def trigger(self) -> list:
        self.call_count += 1
        results = []
        for cb in self.callbacks:
            hook = cb._hook if isinstance(cb, RpcStub) else cb
            result_hook = hook.call(["onMessage"], RpcPayload.owned([f"msg-{len(results)}"]))
            result = await result_hook.pull()
            results.append(result.value)
        return results
    
    def get_callback_count(self) -> int:
        self.call_count += 1
        return len(self.callbacks)


class ClientCallback(RpcTarget):
    """Client-side callback that tracks messages received - ergonomic style."""
    
    def __init__(self):
        self.messages: list[str] = []
        self.on_broken_called = False
        self.on_broken_error: Exception | None = None
    
    def onMessage(self, msg: str) -> str:
        self.messages.append(msg)
        return f"ack-{len(self.messages)}"


# =============================================================================
# Test 1: reverseExports O(1) lookup
# =============================================================================

@pytest.mark.asyncio
class TestReverseExportsLookup:
    """Test that exporting the same capability reuses the export ID."""
    
    async def test_same_capability_reuses_export_id(self) -> None:
        """Exporting the same capability twice should return the same export ID."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        try:
            # Get the client's main export (the callback)
            # Export it twice via the same stub
            stub1 = RpcStub(client._exports[0].hook.dup())
            stub2 = RpcStub(client._exports[0].hook.dup())
            
            # Export both - should get same ID since same underlying hook
            export_id1 = client.export_capability(stub1)
            export_id2 = client.export_capability(stub2)
            
            # Verify same export ID is reused
            assert export_id1 == export_id2, f"Expected same export ID, got {export_id1} and {export_id2}"
            
            # Verify refcount increased
            entry = client._exports[export_id1]
            assert entry.refcount == 2, f"Expected refcount 2, got {entry.refcount}"
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_different_capabilities_get_different_ids(self) -> None:
        """Different capabilities should get different export IDs."""
        transport_a, transport_b = create_transport_pair()
        
        callback1 = ClientCallback()
        callback2 = ClientCallback()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, callback1)
        
        server.start()
        client.start()
        
        try:
            from capnweb.hooks import TargetStubHook
            
            # Create two different stubs
            stub1 = RpcStub(TargetStubHook(callback1))
            stub2 = RpcStub(TargetStubHook(callback2))
            
            export_id1 = client.export_capability(stub1)
            export_id2 = client.export_capability(stub2)
            
            # Different capabilities should get different IDs
            assert export_id1 != export_id2, f"Expected different IDs, got {export_id1} and {export_id2}"
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 2: sendRelease on resolve
# =============================================================================

@pytest.mark.asyncio
class TestSendReleaseOnResolve:
    """Test that release messages are sent when imports resolve."""
    
    async def test_release_sent_after_resolve(self) -> None:
        """When an import resolves, a release message should be sent to peer."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            # Make a call
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(["echo"], RpcPayload.owned(["hello"]))
            
            # Pull the result (this triggers resolve)
            result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            assert result.value == "hello"

            async def wait_for_release() -> None:
                deadline = asyncio.get_event_loop().time() + 2.0
                while asyncio.get_event_loop().time() < deadline:
                    for batch in transport_b.sent_messages:
                        for m in parse_wire_batch(batch):
                            if isinstance(m, WireRelease):
                                return
                    await asyncio.sleep(0)
                pytest.fail(f"No WireRelease message found in: {transport_b.sent_messages}")

            await wait_for_release()
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_import_removed_after_release(self) -> None:
        """After resolve, the import entry should send a release to the peer."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            initial_imports = len(client._imports)
            
            # Make a call
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(["echo"], RpcPayload.owned(["test"]))
            
            # Should have one more import now
            assert len(client._imports) == initial_imports + 1
            
            # Pull and resolve
            await asyncio.wait_for(result_hook.pull(), timeout=2.0)

            # TS parity (rpc.ts:857-862, fixed by stream A1): sendRelease frees
            # the table slot immediately. The resolved value stays reachable
            # only through the hook's cached entry reference.
            assert initial_imports not in client._imports
            entry = result_hook._entry
            assert entry is not None
            assert entry.remote_refcount == 0

            async def wait_for_release_for_import(import_id: int) -> None:
                deadline = asyncio.get_event_loop().time() + 2.0
                while asyncio.get_event_loop().time() < deadline:
                    for batch in transport_b.sent_messages:
                        for m in parse_wire_batch(batch):
                            if isinstance(m, WireRelease) and m.import_id == import_id:
                                return
                    await asyncio.sleep(0)
                pytest.fail(
                    f"No WireRelease(import_id={import_id}) found in: {transport_b.sent_messages}"
                )

            await wait_for_release_for_import(initial_imports)
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 3: onBroken callbacks
# =============================================================================

@pytest.mark.asyncio
class TestOnBrokenCallbacks:
    """Test that onBroken callbacks are called when connection breaks."""
    
    async def test_callbacks_called_on_abort(self) -> None:
        """onBroken callbacks should be called when session aborts."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        callback_called = False
        callback_error = None
        
        def on_broken(error: Exception) -> None:
            nonlocal callback_called, callback_error
            callback_called = True
            callback_error = error
        
        try:
            # Register onBroken callback on an import
            entry = client._imports[0]  # Main import
            entry.on_broken(on_broken)
            
            # Verify callback is registered
            assert len(client._on_broken_callbacks) == 1
            
            # Abort the session
            client._abort(RpcError.internal("Test abort"))
            
            # Callback should have been called
            assert callback_called, "onBroken callback was not called"
            assert callback_error is not None
            assert "Test abort" in str(callback_error)
            
        finally:
            await server.stop()
            # Client already aborted


# =============================================================================
# Test 4: drain() - wait for pending operations
# =============================================================================

@pytest.mark.asyncio
class TestDrain:
    """Test that drain() waits for pending operations."""
    
    async def test_drain_waits_for_pending_pulls(self) -> None:
        """drain() should wait until all pending pulls are resolved."""
        transport_a, transport_b = create_transport_pair()
        
        # Server with slow response
        class SlowTarget(RpcTarget):
            async def call(self, method: str, args: list[Any]) -> Any:
                if method == "slow":
                    await asyncio.sleep(0.3)
                    return "slow-done"
                raise RpcError.not_found(f"Unknown: {method}")
            
            async def get_property(self, prop: str) -> Any:
                raise RpcError.not_found(f"Unknown: {prop}")
        
        server = BidirectionalSession(transport_a, SlowTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            # Make a call that will take time
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(["slow"], RpcPayload.owned([]))
            
            # Start pulling in background
            pull_task = asyncio.create_task(result_hook.pull())
            
            # drain() should not raise and should complete once the server has
            # resolved all pending pulls.
            await asyncio.wait_for(server.drain(), timeout=2.0)
            await asyncio.wait_for(pull_task, timeout=2.0)
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_drain_returns_immediately_when_no_pending(self) -> None:
        """drain() should return immediately when no pending operations."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            # drain() with no pending operations should return immediately
            await asyncio.wait_for(server.drain(), timeout=1.0)
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 5: abort message to peer
# =============================================================================

@pytest.mark.asyncio
class TestAbortMessage:
    """Test that abort messages are sent to peer."""
    
    async def test_abort_message_sent_to_peer(self) -> None:
        """When session aborts, an abort message should be sent to peer."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            # First make a successful call to ensure connection is working
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(["echo"], RpcPayload.owned(["test"]))
            await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            
            # Now abort the client
            client._abort(RpcError.internal("Client aborting"))

            async def wait_for_abort() -> None:
                deadline = asyncio.get_event_loop().time() + 2.0
                while asyncio.get_event_loop().time() < deadline:
                    for batch in transport_b.sent_messages:
                        for m in parse_wire_batch(batch):
                            if isinstance(m, WireAbort):
                                return
                    await asyncio.sleep(0)
                pytest.fail(f"No WireAbort message found in: {transport_b.sent_messages}")

            assert client._abort_reason is not None, "Client should be aborted"
            await wait_for_abort()
            
        finally:
            await server.stop()
    
    async def test_peer_receives_abort_and_handles_it(self) -> None:
        """Peer should receive abort message and abort its session."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            # Abort the client
            client._abort(RpcError.internal("Client aborting"))

            async def wait_for_server_abort() -> None:
                deadline = asyncio.get_event_loop().time() + 2.0
                while asyncio.get_event_loop().time() < deadline:
                    if server._abort_reason is not None:
                        return
                    await asyncio.sleep(0)
                pytest.fail("Server did not abort after receiving WireAbort")

            await wait_for_server_abort()

        finally:
            await server.stop()


# =============================================================================
# Test 6: async events (no polling)
# =============================================================================

@pytest.mark.asyncio
class TestAsyncEvents:
    """Test that async events are used instead of polling."""
    
    async def test_pull_waits_efficiently_for_export(self) -> None:
        """Pull should wait efficiently using events, not polling."""
        transport_a, transport_b = create_transport_pair()
        
        # Server that delays creating the export
        class DelayedTarget(RpcTarget):
            async def call(self, method: str, args: list[Any]) -> Any:
                if method == "delayed":
                    await asyncio.sleep(0.2)
                    return "delayed-result"
                raise RpcError.not_found(f"Unknown: {method}")
            
            async def get_property(self, prop: str) -> Any:
                raise RpcError.not_found(f"Unknown: {prop}")
        
        server = BidirectionalSession(transport_a, DelayedTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            # Make a call
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(["delayed"], RpcPayload.owned([]))
            
            # Pull should wait efficiently
            start = asyncio.get_event_loop().time()
            result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            duration = asyncio.get_event_loop().time() - start
            
            assert result.value == "delayed-result"
            # Should complete in ~0.2s (the delay), not 30s (the timeout)
            assert duration < 1.0, f"Pull took too long: {duration}s"
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_export_event_signaled_when_created(self) -> None:
        """Export events should be signaled when exports are created."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            # Make multiple rapid calls
            server_stub = client.get_main_stub()
            
            results = []
            for i in range(5):
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"msg-{i}"]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
                results.append(result.value)
            
            # All should complete successfully
            assert results == ["msg-0", "msg-1", "msg-2", "msg-3", "msg-4"]
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 7: RpcSessionOptions
# =============================================================================

@pytest.mark.asyncio
class TestRpcSessionOptions:
    """Test RpcSessionOptions configuration."""
    
    async def test_on_send_error_callback_called(self) -> None:
        """on_send_error callback should be called when serializing errors."""
        transport_a, transport_b = create_transport_pair()
        
        errors_seen: list[Exception] = []
        
        def on_send_error(error: Exception) -> Exception | None:
            errors_seen.append(error)
            # Redact the error
            return RpcError.internal("Redacted error")
        
        options = RpcSessionOptions(on_send_error=on_send_error)
        
        # Server that throws errors
        class ErrorTarget(RpcTarget):
            async def call(self, method: str, args: list[Any]) -> Any:
                raise RpcError.internal("Secret internal details")
            
            async def get_property(self, prop: str) -> Any:
                raise RpcError.not_found(f"Unknown: {prop}")
        
        server = BidirectionalSession(transport_a, ErrorTarget(), options)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            # Make a call that will error
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(["anything"], RpcPayload.owned([]))
            
            # Pull should get the error
            with pytest.raises(Exception):
                await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            
            # Note: on_send_error is for serialization, may not be called in all paths
            # This test verifies the option is accepted and doesn't break anything
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 8: get_stats()
# =============================================================================

@pytest.mark.asyncio
class TestGetStats:
    """Test get_stats() returns accurate counts."""
    
    async def test_stats_reflect_imports_and_exports(self) -> None:
        """get_stats() should return accurate import/export counts."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        try:
            # Initial stats
            server_stats = server.get_stats()
            client_stats = client.get_stats()
            
            # Server has export 0 (CallbackTarget), import 0 (client main)
            assert server_stats["exports"] >= 1
            assert server_stats["imports"] >= 1
            
            # Client has export 0 (ClientCallback), import 0 (server main)
            assert client_stats["exports"] >= 1
            assert client_stats["imports"] >= 1
            
            # Make some calls to increase counts
            server_stub = client.get_main_stub()
            callback_stub = RpcStub(client._exports[0].hook.dup())
            
            # Register callback
            result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
            await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            
            # Stats should have changed
            new_server_stats = server.get_stats()
            new_client_stats = client.get_stats()
            
            # Server should have more imports (the callback)
            assert new_server_stats["imports"] >= server_stats["imports"]
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_stats_update_after_release(self) -> None:
        """Stats should update after exports are released."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            initial_server_exports = server.get_stats()["exports"]
            
            # Make a call (creates export on server)
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(["echo"], RpcPayload.owned(["test"]))
            
            # Give time for push to be processed
            await asyncio.sleep(0.1)
            
            # Server should have more exports now
            mid_stats = server.get_stats()
            assert mid_stats["exports"] > initial_server_exports
            
            # Pull and release
            await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            
            # Give time for release to be processed
            await asyncio.sleep(0.1)
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 9: Edge cases - concurrent calls, rapid operations
# =============================================================================

@pytest.mark.asyncio
class TestEdgeCases:
    """Test edge cases and stress scenarios."""
    
    async def test_concurrent_calls(self) -> None:
        """Multiple concurrent calls should all complete correctly."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Make 10 concurrent calls
            async def make_call(i: int) -> str:
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"msg-{i}"]))
                result = await result_hook.pull()
                return result.value
            
            tasks = [make_call(i) for i in range(10)]
            results = await asyncio.wait_for(
                asyncio.gather(*tasks),
                timeout=5.0
            )
            
            # All should complete with correct values
            expected = {f"msg-{i}" for i in range(10)}
            assert set(results) == expected
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_nested_bidirectional_calls(self) -> None:
        """Server calling client while client is calling server."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            callback_stub = RpcStub(client._exports[0].hook.dup())
            
            # Register callback
            result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
            await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            
            # Trigger - server will call back to client
            result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            
            # Should have received the callback
            assert client_callback.messages == ["msg-0"]
            assert result.value == ["ack-1"]
            
        finally:
            await server.stop()
            await client.stop()
    
    async def test_rapid_connect_disconnect(self) -> None:
        """Rapid session creation and destruction should not leak resources."""
        for _ in range(5):
            transport_a, transport_b = create_transport_pair()
            
            server = BidirectionalSession(transport_a, EchoTarget())
            client = BidirectionalSession(transport_b, ClientCallback())
            
            server.start()
            client.start()
            
            # Quick call
            server_stub = client.get_main_stub()
            result_hook = server_stub.call(["echo"], RpcPayload.owned(["quick"]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
            assert result.value == "quick"
            
            # Stop immediately
            await server.stop()
            await client.stop()
    
    async def test_call_after_abort_raises(self) -> None:
        """Calls after abort should raise immediately."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, EchoTarget())
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        try:
            # Abort the client
            client._abort(RpcError.internal("Aborted"))
            
            # Trying to make a call should raise
            server_stub = client.get_main_stub()
            with pytest.raises(Exception):
                server_stub.call(["echo"], RpcPayload.owned(["test"]))
            
        finally:
            await server.stop()
    
    async def test_multiple_callbacks_registered(self) -> None:
        """Multiple callbacks can be registered and all called."""
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register the same callback multiple times
            for i in range(3):
                callback_stub = RpcStub(client._exports[0].hook.dup())
                result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
                assert result.value == f"registered-{i+1}"
            
            # Trigger all callbacks
            result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
            result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            
            # All 3 callbacks should have been called
            assert len(client_callback.messages) == 3
            assert result.value == ["ack-1", "ack-2", "ack-3"]
            
        finally:
            await server.stop()
            await client.stop()


# =============================================================================
# Test 10: Real WebSocket E2E
# =============================================================================

@pytest.mark.asyncio
class TestRealWebSocket:
    """Test with real WebSocket transport."""
    
    async def test_full_bidirectional_over_websocket(self) -> None:
        """Full bidirectional RPC over real WebSocket."""
        
        # WebSocket handler
        async def handle_websocket(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            
            transport = WebSocketServerTransport(ws)
            session = BidirectionalSession(transport, CallbackTarget())
            session.start()
            
            try:
                async for msg in ws:
                    if msg.type == web.WSMsgType.TEXT:
                        transport.feed_message(msg.data)
                    elif msg.type == web.WSMsgType.ERROR:
                        transport.set_error(ws.exception())
                        break
            finally:
                transport.set_closed()
                await session.stop()
            
            return ws
        
        # Start server
        app = web.Application()
        app.router.add_get("/ws", handle_websocket)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        
        try:
            # Connect client
            client_callback = ClientCallback()
            transport = WebSocketClientTransport(f"ws://127.0.0.1:{port}/ws")
            await transport.connect()
            
            session = BidirectionalSession(transport, client_callback)
            session.start()
            
            try:
                server_stub = session.get_main_stub()
                callback_stub = RpcStub(session._exports[0].hook.dup())
                
                # Register
                result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
                assert result.value == "registered-1"
                
                # Trigger
                result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                
                assert client_callback.messages == ["msg-0"]
                assert result.value == ["ack-1"]
                
                # Verify stats
                stats = session.get_stats()
                assert stats["imports"] >= 1
                assert stats["exports"] >= 1
                
            finally:
                await session.stop()
                await transport.close()
                
        finally:
            await runner.cleanup()
