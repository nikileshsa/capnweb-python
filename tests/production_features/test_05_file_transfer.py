"""Example 5: File Transfer with Progress Callbacks

Demonstrates:
- Upload/download with progress callbacks
- Chunked transfer
- Concurrent transfers
- Cancel operations
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from capnweb import RpcTarget, RpcError
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from ..support import rpc_call


@dataclass
class FileTransferService(RpcTarget):
    """File transfer service with progress tracking."""
    
    files: dict[str, bytes] = field(default_factory=dict)
    transfers: dict[str, dict] = field(default_factory=dict)
    transfer_id: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "upload":
                return await self._upload(args[0], args[1], args[2] if len(args) > 2 else None)
            case "download":
                return await self._download(args[0], args[1] if len(args) > 1 else None)
            case "list_files":
                return self._list_files()
            case "delete":
                return self._delete(args[0])
            case "get_stats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    async def _upload(self, filename: str, data: str, callback: Any = None) -> dict:
        self.transfer_id += 1
        tid = f"upload_{self.transfer_id}"
        self.transfers[tid] = {"type": "upload", "status": "in_progress"}
        
        # Simulate chunked upload with progress
        chunks = [data[i:i+10] for i in range(0, len(data), 10)]
        for i, chunk in enumerate(chunks):
            if callback:
                try:
                    await callback.on_progress(tid, (i + 1) / len(chunks) * 100)
                except Exception:
                    pass
            await asyncio.sleep(0.01)
        
        self.files[filename] = data.encode()
        self.transfers[tid]["status"] = "completed"
        return {"filename": filename, "size": len(data), "transfer_id": tid}
    
    async def _download(self, filename: str, callback: Any = None) -> dict:
        if filename not in self.files:
            raise RpcError.not_found(f"File {filename} not found")
        
        self.transfer_id += 1
        tid = f"download_{self.transfer_id}"
        self.transfers[tid] = {"type": "download", "status": "in_progress"}
        
        data = self.files[filename].decode()
        chunks = [data[i:i+10] for i in range(0, len(data), 10)]
        
        for i, chunk in enumerate(chunks):
            if callback:
                try:
                    await callback.on_progress(tid, (i + 1) / len(chunks) * 100)
                except Exception:
                    pass
            await asyncio.sleep(0.01)
        
        self.transfers[tid]["status"] = "completed"
        return {"filename": filename, "data": data, "transfer_id": tid}
    
    def _list_files(self) -> dict:
        return {"files": [{"name": k, "size": len(v)} for k, v in self.files.items()]}
    
    def _delete(self, filename: str) -> dict:
        if filename not in self.files:
            raise RpcError.not_found(f"File {filename} not found")
        del self.files[filename]
        return {"deleted": filename}
    
    def _get_stats(self) -> dict:
        return {
            "files": len(self.files),
            "total_size": sum(len(v) for v in self.files.values()),
            "transfers": len(self.transfers),
        }


@dataclass
class TransferCallback(RpcTarget):
    """Client-side transfer progress callback."""
    
    progress_updates: list = field(default_factory=list)
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "on_progress":
                self.progress_updates.append({"transfer_id": args[0], "progress": args[1]})
                return {"status": "ack"}
            case _:
                raise RpcError.not_found(f"Method {method} not found")


_port = 9500


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestFileTransfer:
    
    async def test_upload_with_progress(self):
        """ASSERTION: Upload works with progress callbacks."""
        port = get_port()
        service = FileTransferService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            callback = TransferCallback()
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=callback) as client:
                result = await rpc_call(client, "upload", ["test.txt", "Hello World Data!", callback])
                assert result["filename"] == "test.txt"
                assert result["size"] == 17
                
                await asyncio.sleep(0.1)
                assert len(callback.progress_updates) > 0
                assert callback.progress_updates[-1]["progress"] == 100
        finally:
            await server.stop()
    
    async def test_download_with_progress(self):
        """ASSERTION: Download works with progress callbacks."""
        port = get_port()
        service = FileTransferService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            callback = TransferCallback()
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=callback) as client:
                await rpc_call(client, "upload", ["data.txt", "Some file content here"])
                
                callback.progress_updates.clear()
                result = await rpc_call(client, "download", ["data.txt", callback])
                assert result["data"] == "Some file content here"
                
                await asyncio.sleep(0.1)
                assert len(callback.progress_updates) > 0
        finally:
            await server.stop()
    
    async def test_file_listing(self):
        """ASSERTION: File listing works correctly."""
        port = get_port()
        service = FileTransferService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "upload", ["file1.txt", "content1"])
                await rpc_call(client, "upload", ["file2.txt", "content2"])
                
                result = await rpc_call(client, "list_files", [])
                assert len(result["files"]) == 2
                names = [f["name"] for f in result["files"]]
                assert "file1.txt" in names
                assert "file2.txt" in names
        finally:
            await server.stop()
    
    async def test_delete_file(self):
        """ASSERTION: File deletion works correctly."""
        port = get_port()
        service = FileTransferService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                await rpc_call(client, "upload", ["todelete.txt", "delete me"])
                result = await rpc_call(client, "delete", ["todelete.txt"])
                assert result["deleted"] == "todelete.txt"
                
                listing = await rpc_call(client, "list_files", [])
                assert len(listing["files"]) == 0
        finally:
            await server.stop()
    
    async def test_concurrent_transfers(self):
        """ASSERTION: Concurrent transfers work correctly."""
        port = get_port()
        service = FileTransferService()
        server = WebSocketRpcServer(service, port=port)
        await server.start()
        
        try:
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url) as client:
                tasks = [
                    asyncio.create_task(rpc_call(client, "upload", [f"file{i}.txt", f"content{i}"]))
                    for i in range(5)
                ]
                results = await asyncio.gather(*tasks)
                assert len(results) == 5
                
                stats = await rpc_call(client, "get_stats", [])
                assert stats["files"] == 5
                assert stats["transfers"] == 5
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
