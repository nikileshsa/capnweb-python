"""Bidirectional RPC tests for TypeScript/Python interop.

These tests verify bidirectional capability passing and callbacks:
- Server calls client back
- Nested capability chains
- Interleaved messages
- Pipelined calls
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from .conftest import InteropClient, ServerProcess


# =============================================================================
# Callback Tests
# =============================================================================

@pytest.mark.asyncio
class TestCallbacks:
    """Test server calling back to client."""
    
    async def test_simple_callback_ts(self, ts_server: ServerProcess):
        """Server can call a simple callback on client."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        print(f"[DEBUG] test_simple_callback_ts starting, port={ts_server.port}", flush=True)
        
        class Callback(RpcTarget):
            def __init__(self):
                self.calls: list[str] = []
            
            async def call(self, method: str, args: list) -> Any:
                print(f"[DEBUG] Callback.call({method}, {args})", flush=True)
                if method == "notify":
                    self.calls.append(args[0])
                    return f"received: {args[0]}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        callback = Callback()
        print("[DEBUG] Creating WebSocketRpcClient...", flush=True)
        async with asyncio.timeout(30):
            async with WebSocketRpcClient(
                f"ws://127.0.0.1:{ts_server.port}/",
                local_main=callback,
            ) as client:
                print("[DEBUG] Connected to server", flush=True)
                stub = RpcStub(client._session.get_export(0).dup())
                print("[DEBUG] Calling registerCallback...", flush=True)
                await client.call(0, "registerCallback", [stub])
                print("[DEBUG] registerCallback done", flush=True)
                
                print("[DEBUG] Calling triggerCallback...", flush=True)
                result = await client.call(0, "triggerCallback", [])
                print(f"[DEBUG] triggerCallback result: {result}", flush=True)
                
                assert callback.calls == ["ping"]
                assert result == "received: ping"
        print("[DEBUG] test_simple_callback_ts done", flush=True)
    
    async def test_simple_callback_py(self, py_server: ServerProcess):
        """Server can call a simple callback on client."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class Callback(RpcTarget):
            def __init__(self):
                self.calls: list[str] = []
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.calls.append(args[0])
                    return f"received: {args[0]}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        callback = Callback()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{py_server.port}/rpc",
            local_main=callback,
        ) as client:
            stub = RpcStub(client._session.get_export(0).dup())
            await client.call(0, "registerCallback", [stub])
            
            result = await client.call(0, "triggerCallback", [])
            
            assert callback.calls == ["ping"]
            assert result == "received: ping"
    
    async def test_multiple_callbacks_ts(self, ts_server: ServerProcess):
        """Server can call callback multiple times."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class Counter(RpcTarget):
            def __init__(self):
                self.count = 0
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.count += 1
                    return f"call #{self.count}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        counter = Counter()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{ts_server.port}/",
            local_main=counter,
        ) as client:
            stub = RpcStub(client._session.get_export(0).dup())
            await client.call(0, "registerCallback", [stub])
            
            # Trigger multiple times
            r1 = await client.call(0, "triggerCallback", [])
            r2 = await client.call(0, "triggerCallback", [])
            r3 = await client.call(0, "triggerCallback", [])
            
            assert counter.count == 3
            assert r1 == "call #1"
            assert r2 == "call #2"
            assert r3 == "call #3"
    
    async def test_multiple_callbacks_py(self, py_server: ServerProcess):
        """Server can call callback multiple times."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class Counter(RpcTarget):
            def __init__(self):
                self.count = 0
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.count += 1
                    return f"call #{self.count}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        counter = Counter()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{py_server.port}/rpc",
            local_main=counter,
        ) as client:
            stub = RpcStub(client._session.get_export(0).dup())
            await client.call(0, "registerCallback", [stub])
            
            r1 = await client.call(0, "triggerCallback", [])
            r2 = await client.call(0, "triggerCallback", [])
            r3 = await client.call(0, "triggerCallback", [])
            
            assert counter.count == 3


# =============================================================================
# Pipelined Call Tests
# =============================================================================

@pytest.mark.asyncio
class TestPipelinedCalls:
    """Test multiple calls before any resolve."""
    
    async def test_pipelined_calls_ts(self, ts_server: ServerProcess):
        """Multiple calls can be sent before waiting for results."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Start multiple calls without awaiting
            tasks = [
                asyncio.create_task(client.call("square", [i]))
                for i in range(5)
            ]
            
            # Now await all
            results = await asyncio.gather(*tasks)
            
            assert results == [0, 1, 4, 9, 16]
    
    async def test_pipelined_calls_py(self, py_server: ServerProcess):
        """Multiple calls can be sent before waiting for results."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [
                asyncio.create_task(client.call("square", [i]))
                for i in range(5)
            ]
            
            results = await asyncio.gather(*tasks)
            
            assert results == [0, 1, 4, 9, 16]
    
    async def test_interleaved_calls_ts(self, ts_server: ServerProcess):
        """Interleaved calls with different methods work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = [
                asyncio.create_task(client.call("square", [2])),
                asyncio.create_task(client.call("greet", ["Alice"])),
                asyncio.create_task(client.call("add", [3, 4])),
                asyncio.create_task(client.call("echo", ["test"])),
                asyncio.create_task(client.call("generateFibonacci", [5])),
            ]
            
            results = await asyncio.gather(*tasks)
            
            assert results[0] == 4
            assert results[1] == "Hello, Alice!"
            assert results[2] == 7
            assert results[3] == "test"
            assert results[4] == [0, 1, 1, 2, 3]
    
    async def test_interleaved_calls_py(self, py_server: ServerProcess):
        """Interleaved calls with different methods work correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [
                asyncio.create_task(client.call("square", [2])),
                asyncio.create_task(client.call("greet", ["Alice"])),
                asyncio.create_task(client.call("add", [3, 4])),
                asyncio.create_task(client.call("echo", ["test"])),
                asyncio.create_task(client.call("generateFibonacci", [5])),
            ]
            
            results = await asyncio.gather(*tasks)
            
            assert results[0] == 4
            assert results[1] == "Hello, Alice!"
            assert results[2] == 7
            assert results[3] == "test"
            assert results[4] == [0, 1, 1, 2, 3]


# =============================================================================
# Capability Chain Tests
# =============================================================================

@pytest.mark.asyncio
class TestCapabilityChains:
    """Test chains of capability passing."""
    
    async def test_counter_chain_ts(self, ts_server: ServerProcess):
        """Create counter, pass it back, increment it."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Create a counter on the server
            counter = await client.call("makeCounter", [10])
            
            # The counter is returned as a stub
            assert counter is not None
    
    async def test_counter_chain_py(self, py_server: ServerProcess):
        """Create counter, pass it back, increment it."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            counter = await client.call("makeCounter", [10])
            assert counter is not None
    
    async def test_function_callback_ts(self, ts_server: ServerProcess):
        """Pass a function to server, server calls it."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class SquareFunction(RpcTarget):
            async def call(self, method: str, args: list) -> Any:
                # Called as a function (no method name)
                if method == "" or method is None:
                    return args[0] * args[0]
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        func = SquareFunction()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{ts_server.port}/",
            local_main=func,
        ) as client:
            stub = RpcStub(client._session.get_export(0).dup())
            result = await client.call(0, "callFunction", [stub, 7])
            
            assert result == {"result": 49}
    
    async def test_function_callback_py(self, py_server: ServerProcess):
        """Pass a function to server, server calls it."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class SquareFunction(RpcTarget):
            async def call(self, method: str, args: list) -> Any:
                if method == "" or method is None:
                    return args[0] * args[0]
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        func = SquareFunction()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{py_server.port}/rpc",
            local_main=func,
        ) as client:
            stub = RpcStub(client._session.get_export(0).dup())
            result = await client.call(0, "callFunction", [stub, 7])
            
            assert result == {"result": 49}


# =============================================================================
# Concurrent Bidirectional Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentBidirectional:
    """Test concurrent calls in both directions."""
    
    async def test_concurrent_with_callback_ts(self, ts_server: ServerProcess):
        """Concurrent calls while callback is registered."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class Callback(RpcTarget):
            def __init__(self):
                self.calls = 0
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.calls += 1
                    return "ok"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        callback = Callback()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{ts_server.port}/",
            local_main=callback,
        ) as client:
            stub = RpcStub(client._session.get_export(0).dup())
            await client.call(0, "registerCallback", [stub])
            
            # Make concurrent calls including callback triggers
            tasks = [
                asyncio.create_task(client.call(0, "square", [i]))
                for i in range(5)
            ] + [
                asyncio.create_task(client.call(0, "triggerCallback", []))
                for _ in range(3)
            ]
            
            results = await asyncio.gather(*tasks)
            
            # First 5 are squares
            assert results[:5] == [0, 1, 4, 9, 16]
            # Callback was triggered 3 times
            assert callback.calls == 3
    
    async def test_concurrent_with_callback_py(self, py_server: ServerProcess):
        """Concurrent calls while callback is registered."""
        from capnweb.types import RpcTarget
        from capnweb.stubs import RpcStub
        from capnweb.ws_session import WebSocketRpcClient
        
        class Callback(RpcTarget):
            def __init__(self):
                self.calls = 0
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.calls += 1
                    return "ok"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        callback = Callback()
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{py_server.port}/rpc",
            local_main=callback,
        ) as client:
            stub = RpcStub(client._session.get_export(0).dup())
            await client.call(0, "registerCallback", [stub])
            
            tasks = [
                asyncio.create_task(client.call(0, "square", [i]))
                for i in range(5)
            ] + [
                asyncio.create_task(client.call(0, "triggerCallback", []))
                for _ in range(3)
            ]
            
            results = await asyncio.gather(*tasks)
            
            assert results[:5] == [0, 1, 4, 9, 16]
            assert callback.calls == 3


# =============================================================================
# Self-Reference Tests
# =============================================================================

@pytest.mark.asyncio
class TestSelfReference:
    """Test passing self-reference to server."""
    
    async def test_call_self_ts(self, ts_server: ServerProcess):
        """Server can call method on passed self-reference."""
        from capnweb.ws_session import WebSocketRpcClient
        
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Get a stub for the server's main capability
            # Then pass it back to the server to call
            result = await client.call(0, "square", [5])
            assert result == 25
    
    async def test_call_self_py(self, py_server: ServerProcess):
        """Server can call method on passed self-reference."""
        from capnweb.ws_session import WebSocketRpcClient
        
        async with WebSocketRpcClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call(0, "square", [5])
            assert result == 25
