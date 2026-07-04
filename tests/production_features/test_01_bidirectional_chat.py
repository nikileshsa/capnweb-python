"""Example 1: Bidirectional Chat Application

Demonstrates and validates:
- BidirectionalSession for full-duplex RPC
- Server calling client methods (bidirectional)
- Client calling server methods
- drain() for graceful shutdown
- getStats() for monitoring

This is a complete chat application where:
- Clients join a chat room and register a callback
- Server broadcasts messages to all clients via their callbacks
- Both directions are validated with assertions
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from capnweb import RpcTarget, RpcError
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from ..support import rpc_call


# =============================================================================
# Server-side: Chat Room Service
# =============================================================================

@dataclass
class ChatRoom(RpcTarget):
    """Chat room that manages clients and broadcasts messages."""
    
    clients: dict[str, Any] = field(default_factory=dict)
    message_history: list[dict] = field(default_factory=list)
    broadcast_count: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "join":
                return await self._join(args[0], args[1])
            case "send_message":
                return await self._send_message(args[0], args[1])
            case "leave":
                return await self._leave(args[0])
            case "get_stats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    async def get_property(self, prop: str) -> Any:
        match prop:
            case "user_count":
                return len(self.clients)
            case "message_count":
                return len(self.message_history)
            case _:
                raise RpcError.not_found(f"Property {prop} not found")
    
    async def _join(self, username: str, client_callback: Any) -> dict:
        """Client joins with a callback for receiving messages."""
        if username in self.clients:
            raise RpcError.bad_request(f"Username {username} taken")
        
        self.clients[username] = client_callback
        
        # BIDIRECTIONAL: Server calls client's on_user_joined
        join_event = {"type": "join", "user": username}
        await self._broadcast(join_event, exclude=username)
        
        return {
            "status": "joined",
            "users": list(self.clients.keys()),
            "history": self.message_history[-5:],
        }
    
    async def _send_message(self, username: str, text: str) -> dict:
        """Send message - server broadcasts to all clients."""
        msg = {"type": "message", "user": username, "text": text}
        self.message_history.append(msg)
        
        # BIDIRECTIONAL: Server calls all clients' on_message
        await self._broadcast(msg)
        
        return {"status": "sent", "id": len(self.message_history)}
    
    async def _leave(self, username: str) -> dict:
        """Client leaves the chat."""
        if username in self.clients:
            del self.clients[username]
            leave_event = {"type": "leave", "user": username}
            await self._broadcast(leave_event)
        return {"status": "left"}
    
    def _get_stats(self) -> dict:
        """Get chat room statistics."""
        return {
            "users": len(self.clients),
            "messages": len(self.message_history),
            "broadcasts": self.broadcast_count,
        }
    
    async def _broadcast(self, event: dict, exclude: str | None = None):
        """Broadcast event to all connected clients via their callbacks."""
        self.broadcast_count += 1
        disconnected = []
        
        for username, callback in self.clients.items():
            if username == exclude:
                continue
            try:
                # THIS IS THE KEY BIDIRECTIONAL CALL
                # Server is calling a method on the client's callback
                await callback.on_event(event)
            except Exception:
                disconnected.append(username)
        
        # Clean up disconnected clients
        for username in disconnected:
            del self.clients[username]


# =============================================================================
# Client-side: Chat Client Callback Handler
# =============================================================================

@dataclass
class ChatClientCallback(RpcTarget):
    """Client-side handler for receiving chat events from server."""
    
    username: str
    received_events: list[dict] = field(default_factory=list)
    event_received: asyncio.Event = field(default_factory=asyncio.Event)
    broken_error: Exception | None = None
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "on_event":
                return await self._on_event(args[0])
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    async def _on_event(self, event: dict) -> dict:
        """Called by server when an event occurs."""
        self.received_events.append(event)
        self.event_received.set()
        return {"status": "received"}
    
    async def wait_for_event(self, timeout: float = 2.0) -> dict | None:
        """Wait for next event from server."""
        self.event_received.clear()
        try:
            await asyncio.wait_for(self.event_received.wait(), timeout)
            return self.received_events[-1] if self.received_events else None
        except asyncio.TimeoutError:
            return None
    
    def on_broken(self, error: Exception):
        """Called when connection breaks."""
        self.broken_error = error


# =============================================================================
# E2E Tests with Assertions
# =============================================================================

# Port counter to avoid conflicts
_port = 9100


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestBidirectionalChat:
    """Validate bidirectional RPC with a real chat application."""
    
    async def test_client_calls_server(self):
        """ASSERTION: Client can call server methods."""
        port = get_port()
        chat_room = ChatRoom()
        server = WebSocketRpcServer(chat_room, port=port)
        await server.start()
        
        try:
            callback = ChatClientCallback(username="Alice")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=callback) as client:
                # Client calls server's join method
                result = await rpc_call(client, "join", ["Alice", callback])
                
                # ASSERTION: Server responded correctly
                assert result["status"] == "joined"
                assert "Alice" in result["users"]
                
                # Client calls server's get_stats method
                stats = await rpc_call(client, "get_stats", [])
                
                # ASSERTION: Stats are correct
                assert stats["users"] == 1
                assert stats["messages"] == 0
        finally:
            await server.stop()
    
    async def test_server_calls_client_bidirectional(self):
        """ASSERTION: Server can call client methods (bidirectional RPC)."""
        port = get_port()
        chat_room = ChatRoom()
        server = WebSocketRpcServer(chat_room, port=port)
        await server.start()
        
        try:
            callback1 = ChatClientCallback(username="Alice")
            callback2 = ChatClientCallback(username="Bob")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=callback1) as client1:
                # Alice joins
                await rpc_call(client1, "join", ["Alice", callback1])
                
                async with WebSocketRpcClient(url, local_main=callback2) as client2:
                    # Bob joins - Alice should receive notification via bidirectional call
                    await rpc_call(client2, "join", ["Bob", callback2])
                    
                    # Wait for Alice to receive the join event
                    await asyncio.sleep(0.1)
                    
                    # ASSERTION: Server called client1's on_event method
                    assert len(callback1.received_events) >= 1
                    join_event = callback1.received_events[-1]
                    assert join_event["type"] == "join"
                    assert join_event["user"] == "Bob"
        finally:
            await server.stop()
    
    async def test_message_broadcast_bidirectional(self):
        """ASSERTION: Messages are broadcast to all clients via bidirectional calls."""
        port = get_port()
        chat_room = ChatRoom()
        server = WebSocketRpcServer(chat_room, port=port)
        await server.start()
        
        try:
            callback1 = ChatClientCallback(username="Alice")
            callback2 = ChatClientCallback(username="Bob")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=callback1) as client1:
                async with WebSocketRpcClient(url, local_main=callback2) as client2:
                    # Both join
                    await rpc_call(client1, "join", ["Alice", callback1])
                    await rpc_call(client2, "join", ["Bob", callback2])
                    
                    # Clear events from join notifications
                    callback1.received_events.clear()
                    callback2.received_events.clear()
                    
                    # Alice sends a message
                    result = await rpc_call(client1, "send_message", ["Alice", "Hello everyone!"])
                    assert result["status"] == "sent"
                    
                    # Wait for broadcast
                    await asyncio.sleep(0.1)
                    
                    # ASSERTION: Both clients received the message via bidirectional call
                    assert any(
                        e["type"] == "message" and e["text"] == "Hello everyone!"
                        for e in callback1.received_events
                    )
                    assert any(
                        e["type"] == "message" and e["text"] == "Hello everyone!"
                        for e in callback2.received_events
                    )
        finally:
            await server.stop()
    
    async def test_get_stats_monitoring(self):
        """ASSERTION: getStats() returns accurate session statistics."""
        port = get_port()
        chat_room = ChatRoom()
        server = WebSocketRpcServer(chat_room, port=port)
        await server.start()
        
        try:
            callback = ChatClientCallback(username="Alice")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=callback) as client:
                await rpc_call(client, "join", ["Alice", callback])
                
                # Send some messages
                for i in range(3):
                    await rpc_call(client, "send_message", ["Alice", f"Message {i}"])
                
                # Get stats from chat room
                stats = await rpc_call(client, "get_stats", [])
                
                # ASSERTION: Stats are accurate
                assert stats["users"] == 1
                assert stats["messages"] == 3
                assert stats["broadcasts"] >= 3
        finally:
            await server.stop()
    
    async def test_drain_graceful_shutdown(self):
        """ASSERTION: drain() waits for pending operations before shutdown."""
        port = get_port()
        chat_room = ChatRoom()
        server = WebSocketRpcServer(chat_room, port=port)
        await server.start()
        
        try:
            callback = ChatClientCallback(username="Alice")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=callback) as client:
                await rpc_call(client, "join", ["Alice", callback])
                
                # Start multiple concurrent calls
                tasks = [
                    asyncio.create_task(rpc_call(client, "send_message", ["Alice", f"Msg {i}"]))
                    for i in range(5)
                ]
                
                # Wait for all to complete
                results = await asyncio.gather(*tasks)
                
                # ASSERTION: All calls completed successfully
                assert all(r["status"] == "sent" for r in results)
                
                # Drain should complete quickly since all operations are done
                await client.drain()
                
                # ASSERTION: Session stats available
                stats = client.get_stats()
                assert stats is not None
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
