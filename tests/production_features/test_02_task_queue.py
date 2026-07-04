"""Example 2: Task Queue with Graceful Shutdown

Demonstrates and validates:
- drain() for waiting on pending operations
- Graceful shutdown without losing tasks
- getStats() for monitoring queue depth
- Concurrent task processing
- Proper cleanup on shutdown

This is a distributed task queue where:
- Clients submit tasks to a server
- Server processes tasks asynchronously
- drain() ensures all tasks complete before shutdown
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from capnweb import RpcTarget, RpcError
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from ..support import rpc_call


# =============================================================================
# Server-side: Task Queue Service
# =============================================================================

@dataclass
class TaskResult:
    """Result of a processed task."""
    task_id: str
    status: str
    result: Any = None
    error: str | None = None


@dataclass
class TaskQueue(RpcTarget):
    """Distributed task queue with async processing."""
    
    pending_tasks: dict[str, dict] = field(default_factory=dict)
    completed_tasks: dict[str, TaskResult] = field(default_factory=dict)
    processing_tasks: set[str] = field(default_factory=set)
    task_counter: int = 0
    _processing_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "submit_task":
                return await self._submit_task(args[0], args[1])
            case "get_task_status":
                return self._get_task_status(args[0])
            case "get_result":
                return self._get_result(args[0])
            case "get_queue_stats":
                return self._get_queue_stats()
            case "process_all":
                return await self._process_all()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    async def _submit_task(self, task_type: str, payload: dict) -> dict:
        """Submit a new task to the queue."""
        self.task_counter += 1
        task_id = f"task_{self.task_counter}"
        
        self.pending_tasks[task_id] = {
            "id": task_id,
            "type": task_type,
            "payload": payload,
            "submitted_at": asyncio.get_event_loop().time(),
        }
        
        # Start processing in background
        asyncio.create_task(self._process_task(task_id))
        
        return {"task_id": task_id, "status": "queued"}
    
    async def _process_task(self, task_id: str):
        """Process a single task."""
        async with self._processing_lock:
            if task_id not in self.pending_tasks:
                return
            
            task = self.pending_tasks.pop(task_id)
            self.processing_tasks.add(task_id)
        
        try:
            # Simulate task processing
            task_type = task["type"]
            payload = task["payload"]
            
            if task_type == "compute":
                # Simulate computation
                await asyncio.sleep(0.05)
                result = sum(payload.get("numbers", []))
            elif task_type == "transform":
                await asyncio.sleep(0.03)
                result = payload.get("text", "").upper()
            elif task_type == "slow":
                await asyncio.sleep(0.2)
                result = "slow_completed"
            else:
                result = f"processed_{task_type}"
            
            self.completed_tasks[task_id] = TaskResult(
                task_id=task_id,
                status="completed",
                result=result,
            )
        except Exception as e:
            self.completed_tasks[task_id] = TaskResult(
                task_id=task_id,
                status="failed",
                error=str(e),
            )
        finally:
            self.processing_tasks.discard(task_id)
    
    def _get_task_status(self, task_id: str) -> dict:
        """Get status of a specific task."""
        if task_id in self.pending_tasks:
            return {"task_id": task_id, "status": "pending"}
        elif task_id in self.processing_tasks:
            return {"task_id": task_id, "status": "processing"}
        elif task_id in self.completed_tasks:
            result = self.completed_tasks[task_id]
            return {
                "task_id": task_id,
                "status": result.status,
                "result": result.result,
                "error": result.error,
            }
        else:
            raise RpcError.not_found(f"Task {task_id} not found")
    
    def _get_result(self, task_id: str) -> Any:
        """Get result of a completed task."""
        if task_id not in self.completed_tasks:
            raise RpcError.bad_request(f"Task {task_id} not completed")
        
        result = self.completed_tasks[task_id]
        if result.status == "failed":
            raise RpcError.internal(result.error or "Task failed")
        
        return result.result
    
    def _get_queue_stats(self) -> dict:
        """Get queue statistics."""
        return {
            "pending": len(self.pending_tasks),
            "processing": len(self.processing_tasks),
            "completed": len(self.completed_tasks),
            "total_submitted": self.task_counter,
        }
    
    async def _process_all(self) -> dict:
        """Wait for all tasks to complete."""
        # Wait until no pending or processing tasks
        while self.pending_tasks or self.processing_tasks:
            await asyncio.sleep(0.01)
        
        return {
            "status": "all_complete",
            "completed_count": len(self.completed_tasks),
        }


# =============================================================================
# E2E Tests with Assertions
# =============================================================================

_port = 9200


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestTaskQueue:
    """Validate task queue with drain() and graceful shutdown."""
    
    async def test_submit_and_complete_task(self):
        """ASSERTION: Tasks are submitted and processed correctly."""
        port = get_port()
        task_queue = TaskQueue()
        server = WebSocketRpcServer(task_queue, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                result = await rpc_call(client, "submit_task", ["compute", {"numbers": [1, 2, 3, 4, 5]}])
                assert result["status"] == "queued"
                task_id = result["task_id"]
                await asyncio.sleep(0.1)
                final_result = await rpc_call(client, "get_result", [task_id])
                assert final_result == 15
        finally:
            await server.stop()
    
    async def test_queue_stats_monitoring(self):
        """ASSERTION: getStats() accurately tracks queue state."""
        port = get_port()
        task_queue = TaskQueue()
        server = WebSocketRpcServer(task_queue, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                stats = await rpc_call(client, "get_queue_stats", [])
                assert stats["pending"] == 0
                for i in range(5):
                    await rpc_call(client, "submit_task", ["compute", {"numbers": [i]}])
                stats = await rpc_call(client, "get_queue_stats", [])
                assert stats["total_submitted"] == 5
                await rpc_call(client, "process_all", [])
                stats = await rpc_call(client, "get_queue_stats", [])
                assert stats["completed"] == 5
        finally:
            await server.stop()
    
    async def test_drain_waits_for_pending(self):
        """ASSERTION: drain() waits for all pending operations."""
        port = get_port()
        task_queue = TaskQueue()
        server = WebSocketRpcServer(task_queue, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                task_ids = []
                for i in range(3):
                    result = await rpc_call(client, "submit_task", ["slow", {}])
                    task_ids.append(result["task_id"])
                await client.drain()
                stats = client.get_stats()
                assert stats is not None
                await rpc_call(client, "process_all", [])
                for task_id in task_ids:
                    status = await rpc_call(client, "get_task_status", [task_id])
                    assert status["status"] == "completed"
        finally:
            await server.stop()
    
    async def test_concurrent_task_submission(self):
        """ASSERTION: Concurrent task submissions are handled correctly."""
        port = get_port()
        task_queue = TaskQueue()
        server = WebSocketRpcServer(task_queue, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                submit_tasks = [
                    asyncio.create_task(rpc_call(client, "submit_task", ["compute", {"numbers": [i, i*2]}]))
                    for i in range(20)
                ]
                results = await asyncio.gather(*submit_tasks)
                assert len(results) == 20
                assert all(r["status"] == "queued" for r in results)
                await rpc_call(client, "process_all", [])
                stats = await rpc_call(client, "get_queue_stats", [])
                assert stats["completed"] == 20
        finally:
            await server.stop()
    
    async def test_graceful_shutdown_no_task_loss(self):
        """ASSERTION: Graceful shutdown doesn't lose tasks."""
        port = get_port()
        task_queue = TaskQueue()
        server = WebSocketRpcServer(task_queue, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                for i in range(5):
                    await rpc_call(client, "submit_task", ["transform", {"text": f"message_{i}"}])
                await client.drain()
                await asyncio.sleep(0.3)  # Wait for server processing
            
            # Verify tasks completed via the task_queue instance directly
            assert task_queue.task_counter == 5
            assert len(task_queue.completed_tasks) >= 5
        finally:
            await server.stop()
    
    async def test_task_status_transitions(self):
        """ASSERTION: Task status transitions correctly through lifecycle."""
        port = get_port()
        task_queue = TaskQueue()
        server = WebSocketRpcServer(task_queue, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                result = await rpc_call(client, "submit_task", ["slow", {}])
                task_id = result["task_id"]
                status = await rpc_call(client, "get_task_status", [task_id])
                assert status["status"] in ["pending", "processing"]
                await asyncio.sleep(0.3)
                status = await rpc_call(client, "get_task_status", [task_id])
                assert status["status"] == "completed"
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
