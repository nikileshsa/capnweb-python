"""Example 7: Distributed Cache with Reference Counting

Demonstrates:
- Put/get operations
- Reference counting for cache entries
- Eviction on zero refs
- Stats tracking
- Concurrent access
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from capnweb import RpcTarget, RpcError
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from ..support import rpc_call


@dataclass
class CacheEntry:
    """Cache entry with reference counting."""
    value: Any
    refs: int = 1


@dataclass
class DistributedCache(RpcTarget):
    """Distributed cache with reference counting."""
    
    entries: dict[str, CacheEntry] = field(default_factory=dict)
    hits: int = 0
    misses: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "put":
                return self._put(args[0], args[1])
            case "get":
                return self._get(args[0])
            case "acquire":
                return self._acquire(args[0])
            case "release":
                return self._release(args[0])
            case "get_stats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    def _put(self, key: str, value: Any) -> dict:
        if key in self.entries:
            self.entries[key].value = value
            self.entries[key].refs += 1
        else:
            self.entries[key] = CacheEntry(value=value, refs=1)
        return {"key": key, "refs": self.entries[key].refs}
    
    def _get(self, key: str) -> dict:
        if key not in self.entries:
            self.misses += 1
            raise RpcError.not_found(f"Key {key} not found")
        self.hits += 1
        return {"key": key, "value": self.entries[key].value}
    
    def _acquire(self, key: str) -> dict:
        if key not in self.entries:
            raise RpcError.not_found(f"Key {key} not found")
        self.entries[key].refs += 1
        return {"key": key, "refs": self.entries[key].refs}
    
    def _release(self, key: str) -> dict:
        if key not in self.entries:
            return {"key": key, "evicted": False}
        self.entries[key].refs -= 1
        if self.entries[key].refs <= 0:
            del self.entries[key]
            return {"key": key, "evicted": True}
        return {"key": key, "evicted": False, "refs": self.entries[key].refs}
    
    def _get_stats(self) -> dict:
        return {
            "entries": len(self.entries),
            "hits": self.hits,
            "misses": self.misses,
            "total_refs": sum(e.refs for e in self.entries.values()),
        }


_port = 9700


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestDistributedCache:
    
    async def test_put_and_get(self):
        """ASSERTION: Put and get operations work correctly."""
        port = get_port()
        cache = DistributedCache()
        server = WebSocketRpcServer(cache, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                result = await rpc_call(client, "put", ["key1", {"data": "value1"}])
                assert result["key"] == "key1"
                
                result = await rpc_call(client, "get", ["key1"])
                assert result["value"]["data"] == "value1"
        finally:
            await server.stop()
    
    async def test_reference_counting(self):
        """ASSERTION: Reference counting works correctly."""
        port = get_port()
        cache = DistributedCache()
        server = WebSocketRpcServer(cache, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "put", ["key1", "value1"])
                result = await rpc_call(client, "acquire", ["key1"])
                assert result["refs"] == 2
                result = await rpc_call(client, "acquire", ["key1"])
                assert result["refs"] == 3
        finally:
            await server.stop()
    
    async def test_eviction_on_zero_refs(self):
        """ASSERTION: Entries are evicted when refs reach zero."""
        port = get_port()
        cache = DistributedCache()
        server = WebSocketRpcServer(cache, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "put", ["key1", "value1"])
                result = await rpc_call(client, "release", ["key1"])
                assert result["evicted"] == True
                stats = await rpc_call(client, "get_stats", [])
                assert stats["entries"] == 0
        finally:
            await server.stop()
    
    async def test_stats_tracking(self):
        """ASSERTION: Stats are tracked correctly."""
        port = get_port()
        cache = DistributedCache()
        server = WebSocketRpcServer(cache, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "put", ["key1", "value1"])
                await rpc_call(client, "put", ["key2", "value2"])
                await rpc_call(client, "get", ["key1"])
                await rpc_call(client, "get", ["key2"])
                try:
                    await rpc_call(client, "get", ["nonexistent"])
                except Exception:
                    pass
                stats = await rpc_call(client, "get_stats", [])
                assert stats["entries"] == 2
                assert stats["hits"] == 2
                assert stats["misses"] == 1
        finally:
            await server.stop()
    
    async def test_concurrent_access(self):
        """ASSERTION: Concurrent access works correctly."""
        port = get_port()
        cache = DistributedCache()
        server = WebSocketRpcServer(cache, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                tasks = [
                    asyncio.create_task(rpc_call(client, "put", [f"key{i}", f"value{i}"]))
                    for i in range(10)
                ]
                await asyncio.gather(*tasks)
                stats = await rpc_call(client, "get_stats", [])
                assert stats["entries"] == 10
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
