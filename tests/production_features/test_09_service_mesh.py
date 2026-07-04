"""Example 9: Service Mesh with Service Discovery

Demonstrates:
- Service registration and discovery
- Call routing through registry
- Multiple services
- Stats tracking
- Service not found handling
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from capnweb import RpcTarget, RpcError
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from ..support import rpc_call


@dataclass
class ServiceRegistry(RpcTarget):
    """Service registry for service mesh."""
    
    services: dict[str, Any] = field(default_factory=dict)
    call_count: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "register":
                return self._register(args[0], args[1])
            case "discover":
                return self._discover(args[0])
            case "call_service":
                return await self._call_service(args[0], args[1], args[2])
            case "list_services":
                return self._list_services()
            case "get_stats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    def _register(self, name: str, service: Any) -> dict:
        self.services[name] = service
        return {"name": name, "registered": True}
    
    def _discover(self, name: str) -> dict:
        if name not in self.services:
            raise RpcError.not_found(f"Service {name} not found")
        return {"name": name, "found": True}
    
    async def _call_service(self, name: str, method: str, args: list) -> dict:
        if name not in self.services:
            raise RpcError.not_found(f"Service {name} not found")
        
        self.call_count += 1
        service = self.services[name]
        result = await service.invoke(method, args)
        return {"service": name, "method": method, "result": result}
    
    def _list_services(self) -> dict:
        return {"services": list(self.services.keys())}
    
    def _get_stats(self) -> dict:
        return {
            "registered_services": len(self.services),
            "total_calls": self.call_count,
        }


@dataclass
class MicroService(RpcTarget):
    """A simple microservice that can be registered."""
    
    name: str = "service"
    invocations: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "invoke":
                return await self._invoke(args[0], args[1])
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    async def _invoke(self, method: str, args: list) -> Any:
        self.invocations += 1
        if method == "echo":
            return {"echo": args[0] if args else None}
        elif method == "add":
            return {"sum": sum(args)}
        elif method == "info":
            return {"name": self.name, "invocations": self.invocations}
        else:
            raise RpcError.not_found(f"Unknown method: {method}")


_port = 9900


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestServiceMesh:
    
    async def test_register_and_discover_service(self):
        """ASSERTION: Services can be registered and discovered."""
        port = get_port()
        registry = ServiceRegistry()
        server = WebSocketRpcServer(registry, port=port)
        await server.start()
        
        try:
            service = MicroService(name="echo_service")
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=service) as client:
                result = await rpc_call(client, "register", ["echo", service])
                assert result["registered"] == True
                
                result = await rpc_call(client, "discover", ["echo"])
                assert result["found"] == True
        finally:
            await server.stop()
    
    async def test_call_through_registry(self):
        """ASSERTION: Services can be called through the registry."""
        port = get_port()
        registry = ServiceRegistry()
        server = WebSocketRpcServer(registry, port=port)
        await server.start()
        
        try:
            service = MicroService(name="math_service")
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=service) as client:
                await rpc_call(client, "register", ["math", service])
                
                result = await rpc_call(client, "call_service", ["math", "add", [1, 2, 3, 4, 5]])
                assert result["result"]["sum"] == 15
        finally:
            await server.stop()
    
    async def test_multiple_services(self):
        """ASSERTION: Multiple services can be registered."""
        port = get_port()
        registry = ServiceRegistry()
        server = WebSocketRpcServer(registry, port=port)
        await server.start()
        
        try:
            svc1 = MicroService(name="service1")
            svc2 = MicroService(name="service2")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=svc1) as client1:
                async with WebSocketRpcClient(url, local_main=svc2) as client2:
                    await rpc_call(client1, "register", ["svc1", svc1])
                    await rpc_call(client2, "register", ["svc2", svc2])
                    
                    services = await rpc_call(client1, "list_services", [])
                    assert "svc1" in services["services"]
                    assert "svc2" in services["services"]
        finally:
            await server.stop()
    
    async def test_service_stats(self):
        """ASSERTION: Service stats are tracked correctly."""
        port = get_port()
        registry = ServiceRegistry()
        server = WebSocketRpcServer(registry, port=port)
        await server.start()
        
        try:
            service = MicroService(name="stats_service")
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=service) as client:
                await rpc_call(client, "register", ["stats", service])
                
                await rpc_call(client, "call_service", ["stats", "echo", ["test"]])
                await rpc_call(client, "call_service", ["stats", "echo", ["test2"]])
                
                stats = await rpc_call(client, "get_stats", [])
                assert stats["registered_services"] == 1
                assert stats["total_calls"] == 2
        finally:
            await server.stop()
    
    async def test_service_not_found(self):
        """ASSERTION: Service not found raises appropriate error."""
        port = get_port()
        registry = ServiceRegistry()
        server = WebSocketRpcServer(registry, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                with pytest.raises(Exception):
                    await rpc_call(client, "discover", ["nonexistent"])
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
