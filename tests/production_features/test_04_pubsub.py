"""Example 4: Pub/Sub with Bidirectional Callbacks

Demonstrates:
- Subscribe/unsubscribe pattern with callbacks
- Reference counting for subscriptions
- Multiple subscribers receiving events
- Stats tracking
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from capnweb import RpcTarget, RpcError
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from ..support import rpc_call


@dataclass
class PubSubService(RpcTarget):
    """Pub/Sub service with topic subscriptions."""
    
    topics: dict[str, list[Any]] = field(default_factory=dict)
    message_count: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "subscribe":
                return self._subscribe(args[0], args[1])
            case "unsubscribe":
                return self._unsubscribe(args[0], args[1])
            case "publish":
                return await self._publish(args[0], args[1])
            case "get_stats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    def _subscribe(self, topic: str, callback: Any) -> dict:
        if topic not in self.topics:
            self.topics[topic] = []
        self.topics[topic].append(callback)
        return {"topic": topic, "subscribers": len(self.topics[topic])}
    
    def _unsubscribe(self, topic: str, callback: Any) -> dict:
        if topic not in self.topics:
            return {"topic": topic, "removed": False}
        # Note: In real impl would need proper callback identity
        if self.topics[topic]:
            self.topics[topic].pop()
            return {"topic": topic, "removed": True}
        return {"topic": topic, "removed": False}
    
    async def _publish(self, topic: str, message: Any) -> dict:
        if topic not in self.topics:
            return {"topic": topic, "delivered": 0}
        
        self.message_count += 1
        delivered = 0
        for callback in self.topics[topic]:
            try:
                await callback.on_message(topic, message)
                delivered += 1
            except Exception:
                pass
        return {"topic": topic, "delivered": delivered}
    
    def _get_stats(self) -> dict:
        return {
            "topics": len(self.topics),
            "total_subscribers": sum(len(subs) for subs in self.topics.values()),
            "messages_published": self.message_count,
        }


@dataclass
class Subscriber(RpcTarget):
    """Client-side subscriber callback."""
    
    name: str = "default"
    received: list = field(default_factory=list)
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "on_message":
                self.received.append({"topic": args[0], "message": args[1]})
                return {"status": "received"}
            case _:
                raise RpcError.not_found(f"Method {method} not found")


_port = 9400


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestPubSub:
    
    async def test_subscribe_and_receive(self):
        """ASSERTION: Subscribers receive published messages."""
        port = get_port()
        pubsub = PubSubService()
        server = WebSocketRpcServer(pubsub, port=port)
        await server.start()
        
        try:
            subscriber = Subscriber(name="sub1")
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=subscriber) as client:
                result = await rpc_call(client, "subscribe", ["news", subscriber])
                assert result["subscribers"] == 1
                
                await rpc_call(client, "publish", ["news", {"headline": "Hello"}])
                await asyncio.sleep(0.1)
                
                assert len(subscriber.received) == 1
                assert subscriber.received[0]["message"]["headline"] == "Hello"
        finally:
            await server.stop()
    
    async def test_multiple_subscribers(self):
        """ASSERTION: Multiple subscribers all receive messages."""
        port = get_port()
        pubsub = PubSubService()
        server = WebSocketRpcServer(pubsub, port=port)
        await server.start()
        
        try:
            sub1 = Subscriber(name="sub1")
            sub2 = Subscriber(name="sub2")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=sub1) as client1:
                async with WebSocketRpcClient(url, local_main=sub2) as client2:
                    await rpc_call(client1, "subscribe", ["events", sub1])
                    await rpc_call(client2, "subscribe", ["events", sub2])
                    
                    result = await rpc_call(client1, "publish", ["events", {"type": "test"}])
                    assert result["delivered"] == 2
                    
                    await asyncio.sleep(0.1)
                    assert len(sub1.received) == 1
                    assert len(sub2.received) == 1
        finally:
            await server.stop()
    
    async def test_unsubscribe(self):
        """ASSERTION: Unsubscribed clients don't receive messages."""
        port = get_port()
        pubsub = PubSubService()
        server = WebSocketRpcServer(pubsub, port=port)
        await server.start()
        
        try:
            subscriber = Subscriber(name="sub1")
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=subscriber) as client:
                await rpc_call(client, "subscribe", ["alerts", subscriber])
                await rpc_call(client, "unsubscribe", ["alerts", subscriber])
                
                result = await rpc_call(client, "publish", ["alerts", {"alert": "test"}])
                assert result["delivered"] == 0
        finally:
            await server.stop()
    
    async def test_stats_tracking(self):
        """ASSERTION: Stats are tracked correctly."""
        port = get_port()
        pubsub = PubSubService()
        server = WebSocketRpcServer(pubsub, port=port)
        await server.start()
        
        try:
            sub1 = Subscriber(name="sub1")
            sub2 = Subscriber(name="sub2")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=sub1) as client1:
                async with WebSocketRpcClient(url, local_main=sub2) as client2:
                    await rpc_call(client1, "subscribe", ["topic1", sub1])
                    await rpc_call(client2, "subscribe", ["topic2", sub2])
                    await rpc_call(client1, "publish", ["topic1", {}])
                    await rpc_call(client2, "publish", ["topic2", {}])
                    
                    stats = await rpc_call(client1, "get_stats", [])
                    assert stats["topics"] == 2
                    assert stats["total_subscribers"] == 2
                    assert stats["messages_published"] == 2
        finally:
            await server.stop()
    
    async def test_multiple_topics(self):
        """ASSERTION: Subscribers can subscribe to multiple topics."""
        port = get_port()
        pubsub = PubSubService()
        server = WebSocketRpcServer(pubsub, port=port)
        await server.start()
        
        try:
            subscriber = Subscriber(name="multi")
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=subscriber) as client:
                await rpc_call(client, "subscribe", ["sports", subscriber])
                await rpc_call(client, "subscribe", ["weather", subscriber])
                
                await rpc_call(client, "publish", ["sports", {"score": "1-0"}])
                await rpc_call(client, "publish", ["weather", {"temp": 72}])
                await asyncio.sleep(0.1)
                
                assert len(subscriber.received) == 2
                topics = [r["topic"] for r in subscriber.received]
                assert "sports" in topics
                assert "weather" in topics
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
