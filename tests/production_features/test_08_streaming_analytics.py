"""Example 8: Streaming Analytics with Event Processing

Demonstrates:
- Stream creation and event pushing
- Batch processing
- Concurrent streams
- High throughput handling
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
class StreamingAnalytics(RpcTarget):
    """Streaming analytics service."""
    
    streams: dict[str, list] = field(default_factory=dict)
    event_count: int = 0
    batch_count: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "create_stream":
                return self._create_stream(args[0])
            case "push_event":
                return self._push_event(args[0], args[1])
            case "push_batch":
                return self._push_batch(args[0], args[1])
            case "get_stream":
                return self._get_stream(args[0])
            case "get_stats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    def _create_stream(self, stream_id: str) -> dict:
        if stream_id in self.streams:
            raise RpcError.bad_request(f"Stream {stream_id} already exists")
        self.streams[stream_id] = []
        return {"stream_id": stream_id, "created": True}
    
    def _push_event(self, stream_id: str, event: Any) -> dict:
        if stream_id not in self.streams:
            raise RpcError.not_found(f"Stream {stream_id} not found")
        self.streams[stream_id].append(event)
        self.event_count += 1
        return {"stream_id": stream_id, "event_count": len(self.streams[stream_id])}
    
    def _push_batch(self, stream_id: str, events: list) -> dict:
        if stream_id not in self.streams:
            raise RpcError.not_found(f"Stream {stream_id} not found")
        self.streams[stream_id].extend(events)
        self.event_count += len(events)
        self.batch_count += 1
        return {"stream_id": stream_id, "added": len(events), "total": len(self.streams[stream_id])}
    
    def _get_stream(self, stream_id: str) -> dict:
        if stream_id not in self.streams:
            raise RpcError.not_found(f"Stream {stream_id} not found")
        return {"stream_id": stream_id, "events": self.streams[stream_id]}
    
    def _get_stats(self) -> dict:
        return {
            "streams": len(self.streams),
            "total_events": self.event_count,
            "batches": self.batch_count,
        }


_port = 9800


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestStreamingAnalytics:
    
    async def test_create_stream_and_push_event(self):
        """ASSERTION: Streams can be created and events pushed."""
        port = get_port()
        analytics = StreamingAnalytics()
        server = WebSocketRpcServer(analytics, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                result = await rpc_call(client, "create_stream", ["events"])
                assert result["created"] == True
                
                result = await rpc_call(client, "push_event", ["events", {"type": "click", "x": 100}])
                assert result["event_count"] == 1
                
                stream = await rpc_call(client, "get_stream", ["events"])
                assert len(stream["events"]) == 1
        finally:
            await server.stop()
    
    async def test_batch_processing(self):
        """ASSERTION: Batch event processing works correctly."""
        port = get_port()
        analytics = StreamingAnalytics()
        server = WebSocketRpcServer(analytics, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "create_stream", ["batch_stream"])
                
                events = [{"id": i, "value": i * 10} for i in range(100)]
                result = await rpc_call(client, "push_batch", ["batch_stream", events])
                assert result["added"] == 100
                assert result["total"] == 100
        finally:
            await server.stop()
    
    async def test_concurrent_streams(self):
        """ASSERTION: Multiple concurrent streams work correctly."""
        port = get_port()
        analytics = StreamingAnalytics()
        server = WebSocketRpcServer(analytics, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                # Create multiple streams
                for i in range(5):
                    await rpc_call(client, "create_stream", [f"stream_{i}"])
                
                # Push events concurrently
                tasks = [
                    asyncio.create_task(rpc_call(client, "push_event", [f"stream_{i % 5}", {"i": i}]))
                    for i in range(20)
                ]
                await asyncio.gather(*tasks)
                
                stats = await rpc_call(client, "get_stats", [])
                assert stats["streams"] == 5
                assert stats["total_events"] == 20
        finally:
            await server.stop()
    
    async def test_high_throughput(self):
        """ASSERTION: High throughput event processing works."""
        port = get_port()
        analytics = StreamingAnalytics()
        server = WebSocketRpcServer(analytics, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "create_stream", ["high_volume"])
                
                # Push 10 batches of 100 events each
                for batch in range(10):
                    events = [{"batch": batch, "idx": i} for i in range(100)]
                    await rpc_call(client, "push_batch", ["high_volume", events])
                
                stream = await rpc_call(client, "get_stream", ["high_volume"])
                assert len(stream["events"]) == 1000
        finally:
            await server.stop()
    
    async def test_stats_tracking(self):
        """ASSERTION: Stats are tracked correctly."""
        port = get_port()
        analytics = StreamingAnalytics()
        server = WebSocketRpcServer(analytics, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "create_stream", ["s1"])
                await rpc_call(client, "create_stream", ["s2"])
                
                await rpc_call(client, "push_event", ["s1", {}])
                await rpc_call(client, "push_event", ["s1", {}])
                await rpc_call(client, "push_batch", ["s2", [{}, {}, {}]])
                
                stats = await rpc_call(client, "get_stats", [])
                assert stats["streams"] == 2
                assert stats["total_events"] == 5
                assert stats["batches"] == 1
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
