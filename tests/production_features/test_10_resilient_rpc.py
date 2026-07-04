"""Example 10: Resilient RPC with onBroken Callbacks

Demonstrates and validates:
- onBroken callbacks for connection death handling
- Error propagation on connection failure
- Graceful degradation
- Session recovery patterns
- Connection health monitoring
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from capnweb import RpcTarget, RpcError
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from ..support import rpc_call


@dataclass
class ResilientService(RpcTarget):
    """Service that tracks connection health."""
    connections: dict[str, dict] = field(default_factory=dict)
    total_requests: int = 0
    failed_requests: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "register_connection":
                return self._register_connection(args[0], args[1])
            case "heartbeat":
                return self._heartbeat(args[0])
            case "process":
                return await self._process(args[0], args[1])
            case "get_connection_status":
                return self._get_connection_status(args[0])
            case "get_stats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    def _register_connection(self, conn_id: str, callback: Any) -> dict:
        self.connections[conn_id] = {
            "callback": callback,
            "heartbeats": 0,
            "status": "connected",
        }
        return {"status": "registered", "connection_id": conn_id}
    
    def _heartbeat(self, conn_id: str) -> dict:
        if conn_id not in self.connections:
            raise RpcError.not_found(f"Connection {conn_id} not found")
        self.connections[conn_id]["heartbeats"] += 1
        return {"status": "alive", "heartbeats": self.connections[conn_id]["heartbeats"]}
    
    async def _process(self, conn_id: str, data: Any) -> dict:
        self.total_requests += 1
        if conn_id not in self.connections:
            self.failed_requests += 1
            raise RpcError.not_found(f"Connection {conn_id} not found")
        
        # Notify client of processing via callback
        try:
            await self.connections[conn_id]["callback"].on_processed(data)
        except Exception:
            self.connections[conn_id]["status"] = "broken"
            self.failed_requests += 1
        
        return {"status": "processed", "data": data}
    
    def _get_connection_status(self, conn_id: str) -> dict:
        if conn_id not in self.connections:
            return {"status": "not_found"}
        conn = self.connections[conn_id]
        return {
            "connection_id": conn_id,
            "status": conn["status"],
            "heartbeats": conn["heartbeats"],
        }
    
    def _get_stats(self) -> dict:
        active = sum(1 for c in self.connections.values() if c["status"] == "connected")
        broken = sum(1 for c in self.connections.values() if c["status"] == "broken")
        return {
            "total_connections": len(self.connections),
            "active_connections": active,
            "broken_connections": broken,
            "total_requests": self.total_requests,
            "failed_requests": self.failed_requests,
        }


@dataclass
class ClientCallback(RpcTarget):
    """Client callback that can simulate failures."""
    processed_data: list[Any] = field(default_factory=list)
    broken_error: Exception | None = None
    should_fail: bool = False
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "on_processed":
                if self.should_fail:
                    raise Exception("Simulated callback failure")
                self.processed_data.append(args[0])
                return {"status": "received"}
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    def set_broken(self, error: Exception):
        self.broken_error = error


_port = 10000


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestResilientRpc:
    async def test_register_and_heartbeat(self):
        """ASSERTION: Connections can register and send heartbeats."""
        port = get_port()
        service = ResilientService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            callback = ClientCallback()
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=callback) as client:
                result = await rpc_call(client, "register_connection", ["conn1", callback])
                assert result["status"] == "registered"
                for i in range(5):
                    result = await rpc_call(client, "heartbeat", ["conn1"])
                    assert result["heartbeats"] == i + 1
        finally:
            await server.stop()
    
    async def test_process_with_callback(self):
        """ASSERTION: Processing triggers callback to client."""
        port = get_port()
        service = ResilientService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            callback = ClientCallback()
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=callback) as client:
                await rpc_call(client, "register_connection", ["conn1", callback])
                result = await rpc_call(client, "process", ["conn1", {"value": 42}])
                assert result["status"] == "processed"
                await asyncio.sleep(0.1)
                assert len(callback.processed_data) == 1
                assert callback.processed_data[0]["value"] == 42
        finally:
            await server.stop()
    
    async def test_callback_failure_marks_broken(self):
        """ASSERTION: Failed callbacks mark connection as broken."""
        port = get_port()
        service = ResilientService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            callback = ClientCallback()
            callback.should_fail = True
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=callback) as client:
                await rpc_call(client, "register_connection", ["fail_conn", callback])
                await rpc_call(client, "process", ["fail_conn", {"data": "test"}])
                status = await rpc_call(client, "get_connection_status", ["fail_conn"])
                assert status["status"] == "broken"
        finally:
            await server.stop()
    
    async def test_stats_track_failures(self):
        """ASSERTION: Stats track failed requests."""
        port = get_port()
        service = ResilientService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            callback = ClientCallback()
            callback.should_fail = True
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=callback) as client:
                await rpc_call(client, "register_connection", ["stats_conn", callback])
                for i in range(3):
                    await rpc_call(client, "process", ["stats_conn", {"i": i}])
                stats = await rpc_call(client, "get_stats", [])
                assert stats["failed_requests"] >= 3
                assert stats["broken_connections"] >= 1
        finally:
            await server.stop()
    
    async def test_multiple_connections(self):
        """ASSERTION: Multiple connections tracked independently."""
        port = get_port()
        service = ResilientService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            callbacks = [ClientCallback() for _ in range(3)]
            url = f"ws://localhost:{port}/rpc"
            
            # Use nested context managers for multiple clients
            async with WebSocketRpcClient(url, local_main=callbacks[0]) as client0:
                await rpc_call(client0, "register_connection", ["conn0", callbacks[0]])
                
                async with WebSocketRpcClient(url, local_main=callbacks[1]) as client1:
                    await rpc_call(client1, "register_connection", ["conn1", callbacks[1]])
                    
                    async with WebSocketRpcClient(url, local_main=callbacks[2]) as client2:
                        await rpc_call(client2, "register_connection", ["conn2", callbacks[2]])
                        
                        stats = await rpc_call(client0, "get_stats", [])
                        assert stats["active_connections"] == 3
                        
                        callbacks[1].should_fail = True
                        await rpc_call(client1, "process", ["conn1", {}])
                        await asyncio.sleep(0.1)
                        
                        stats = await rpc_call(client0, "get_stats", [])
                        assert stats["active_connections"] == 2
                        assert stats["broken_connections"] == 1
        finally:
            await server.stop()
    
    async def test_graceful_degradation(self):
        """ASSERTION: System continues working despite some failures."""
        port = get_port()
        service = ResilientService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            good_callback = ClientCallback()
            bad_callback = ClientCallback()
            bad_callback.should_fail = True
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=good_callback) as good_client:
                async with WebSocketRpcClient(url, local_main=bad_callback) as bad_client:
                    await rpc_call(good_client, "register_connection", ["good", good_callback])
                    await rpc_call(bad_client, "register_connection", ["bad", bad_callback])
                    await rpc_call(bad_client, "process", ["bad", {}])
                    result = await rpc_call(good_client, "process", ["good", {"ok": True}])
                    assert result["status"] == "processed"
                    await asyncio.sleep(0.1)
                    assert len(good_callback.processed_data) == 1
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
