"""Example 3: Data Pipeline with Transformations

Demonstrates:
- Dataset creation and transformation
- Chained transformations
- Filter and aggregate operations
- Pipeline statistics tracking
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from capnweb import RpcTarget, RpcError
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from ..support import rpc_call


@dataclass
class DataPipeline(RpcTarget):
    """Data pipeline service for batch processing."""
    
    datasets: dict[str, list] = field(default_factory=dict)
    transform_count: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "create_dataset":
                return self._create_dataset(args[0], args[1])
            case "transform":
                return self._transform(args[0], args[1])
            case "filter":
                return self._filter(args[0], args[1])
            case "aggregate":
                return self._aggregate(args[0], args[1])
            case "get_stats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    def _create_dataset(self, name: str, data: list) -> dict:
        self.datasets[name] = data
        return {"name": name, "size": len(data)}
    
    def _transform(self, name: str, operation: str) -> dict:
        if name not in self.datasets:
            raise RpcError.not_found(f"Dataset {name} not found")
        data = self.datasets[name]
        self.transform_count += 1
        
        if operation == "double":
            result = [x * 2 for x in data]
            new_name = f"{name}_double"
        elif operation == "square":
            result = [x * x for x in data]
            new_name = f"{name}_square"
        else:
            raise RpcError.bad_request(f"Unknown operation: {operation}")
        
        self.datasets[new_name] = result
        return {"name": new_name, "data": result}
    
    def _filter(self, name: str, predicate: str) -> dict:
        if name not in self.datasets:
            raise RpcError.not_found(f"Dataset {name} not found")
        data = self.datasets[name]
        
        if predicate == "even":
            result = [x for x in data if x % 2 == 0]
        elif predicate == "odd":
            result = [x for x in data if x % 2 != 0]
        else:
            raise RpcError.bad_request(f"Unknown predicate: {predicate}")
        
        new_name = f"{name}_{predicate}"
        self.datasets[new_name] = result
        return {"name": new_name, "data": result}
    
    def _aggregate(self, name: str, operation: str) -> dict:
        if name not in self.datasets:
            raise RpcError.not_found(f"Dataset {name} not found")
        data = self.datasets[name]
        
        if operation == "sum":
            result = sum(data)
        elif operation == "avg":
            result = sum(data) / len(data) if data else 0
        elif operation == "min":
            result = min(data) if data else None
        elif operation == "max":
            result = max(data) if data else None
        else:
            raise RpcError.bad_request(f"Unknown operation: {operation}")
        
        return {"operation": operation, "result": result}
    
    def _get_stats(self) -> dict:
        return {
            "datasets": len(self.datasets),
            "transforms": self.transform_count,
            "total_items": sum(len(d) for d in self.datasets.values()),
        }


_port = 9300


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestDataPipeline:
    
    async def test_create_and_transform_dataset(self):
        """ASSERTION: Datasets can be created and transformed."""
        port = get_port()
        pipeline = DataPipeline()
        server = WebSocketRpcServer(pipeline, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                result = await rpc_call(client, "create_dataset", ["numbers", [1, 2, 3, 4, 5]])
                assert result["name"] == "numbers"
                assert result["size"] == 5
                
                result = await rpc_call(client, "transform", ["numbers", "double"])
                assert result["data"] == [2, 4, 6, 8, 10]
        finally:
            await server.stop()
    
    async def test_chained_transformations(self):
        """ASSERTION: Multiple transformations can be chained."""
        port = get_port()
        pipeline = DataPipeline()
        server = WebSocketRpcServer(pipeline, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "create_dataset", ["data", [1, 2, 3, 4, 5]])
                await rpc_call(client, "transform", ["data", "double"])
                result = await rpc_call(client, "transform", ["data_double", "square"])
                assert result["data"] == [4, 16, 36, 64, 100]
        finally:
            await server.stop()
    
    async def test_filter_and_aggregate(self):
        """ASSERTION: Filter and aggregate operations work correctly."""
        port = get_port()
        pipeline = DataPipeline()
        server = WebSocketRpcServer(pipeline, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "create_dataset", ["mixed", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]])
                filter_result = await rpc_call(client, "filter", ["mixed", "even"])
                assert filter_result["data"] == [2, 4, 6, 8, 10]
                
                agg_result = await rpc_call(client, "aggregate", ["mixed_even", "sum"])
                assert agg_result["result"] == 30
        finally:
            await server.stop()
    
    async def test_aggregate_operations(self):
        """ASSERTION: All aggregate operations work correctly."""
        port = get_port()
        pipeline = DataPipeline()
        server = WebSocketRpcServer(pipeline, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "create_dataset", ["nums", [1, 2, 3, 4, 5]])
                
                sum_result = await rpc_call(client, "aggregate", ["nums", "sum"])
                assert sum_result["result"] == 15
                
                avg_result = await rpc_call(client, "aggregate", ["nums", "avg"])
                assert avg_result["result"] == 3.0
                
                min_result = await rpc_call(client, "aggregate", ["nums", "min"])
                assert min_result["result"] == 1
                
                max_result = await rpc_call(client, "aggregate", ["nums", "max"])
                assert max_result["result"] == 5
        finally:
            await server.stop()
    
    async def test_pipeline_stats_tracking(self):
        """ASSERTION: Pipeline stats are tracked correctly."""
        port = get_port()
        pipeline = DataPipeline()
        server = WebSocketRpcServer(pipeline, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "create_dataset", ["a", [1, 2, 3]])
                await rpc_call(client, "create_dataset", ["b", [4, 5, 6]])
                await rpc_call(client, "transform", ["a", "double"])
                await rpc_call(client, "transform", ["b", "square"])
                
                stats = await rpc_call(client, "get_stats", [])
                assert stats["datasets"] == 4
                assert stats["transforms"] == 2
                assert stats["total_items"] == 12
        finally:
            await server.stop()
    
    async def test_concurrent_pipeline_operations(self):
        """ASSERTION: Concurrent operations work correctly."""
        port = get_port()
        pipeline = DataPipeline()
        server = WebSocketRpcServer(pipeline, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                tasks = [
                    asyncio.create_task(rpc_call(client, "create_dataset", [f"ds{i}", list(range(i, i+5))]))
                    for i in range(5)
                ]
                results = await asyncio.gather(*tasks)
                assert len(results) == 5
                
                stats = await rpc_call(client, "get_stats", [])
                assert stats["datasets"] == 5
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
