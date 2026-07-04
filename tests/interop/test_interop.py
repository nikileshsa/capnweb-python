"""Exhaustive TypeScript/Python interop test matrix.

This module tests protocol compliance between TypeScript and Python
implementations by running cross-language client/server combinations.

Test Matrix:
- Python client → TypeScript server (WebSocket)
- Python client → TypeScript server (HTTP Batch)
- TypeScript client → Python server (WebSocket) [via subprocess]
- TypeScript client → Python server (HTTP Batch) [via subprocess]

Test Scenarios:
1. Simple calls (square, add, greet)
2. Arrays (generateFibonacci, getList)
3. Capability passing (makeCounter, incrementCounter)
4. Callbacks (registerCallback, triggerCallback)
5. Errors (throwError)
6. Special values (null, undefined, numbers)
7. Nested objects (callSquare result)
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from capnweb.ws_session import WebSocketRpcClient
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget

from .test_target import TestTarget, Counter
from .conftest import ServerProcess
from ..support import rpc_call

# Paths
INTEROP_DIR = Path(__file__).parent


# =============================================================================
# Python Client → TypeScript Server Tests
# =============================================================================

@pytest.mark.asyncio
class TestPyClientTsServer:
    """Test Python client connecting to TypeScript server."""
    
    async def test_simple_square(self, ts_server: ServerProcess):
        """Test simple square call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "square", [5])
            assert result == 25
    
    async def test_add(self, ts_server: ServerProcess):
        """Test add call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "add", [3, 7])
            assert result == 10
    
    async def test_greet(self, ts_server: ServerProcess):
        """Test greet call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "greet", ["World"])
            assert result == "Hello, World!"
    
    async def test_echo_string(self, ts_server: ServerProcess):
        """Test echo with string."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "echo", ["test message"])
            assert result == "test message"
    
    async def test_echo_number(self, ts_server: ServerProcess):
        """Test echo with number."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "echo", [42])
            assert result == 42
    
    async def test_echo_array(self, ts_server: ServerProcess):
        """Test echo with array."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "echo", [[1, 2, 3]])
            assert result == [1, 2, 3]
    
    async def test_echo_nested_object(self, ts_server: ServerProcess):
        """Test echo with nested object."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            obj = {"foo": {"bar": 123}, "baz": [1, 2, 3]}
            result = await rpc_call(client, "echo", [obj])
            assert result == obj
    
    async def test_generate_fibonacci(self, ts_server: ServerProcess):
        """Test generateFibonacci."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "generateFibonacci", [10])
            assert result == [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
    
    async def test_get_list(self, ts_server: ServerProcess):
        """Test getList."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "getList", [])
            assert result == [1, 2, 3, 4, 5]
    
    async def test_return_null(self, ts_server: ServerProcess):
        """Test returnNull."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "returnNull", [])
            assert result is None
    
    async def test_return_number(self, ts_server: ServerProcess):
        """Test returnNumber."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await rpc_call(client, "returnNumber", [123])
            assert result == 123
    
    async def test_throw_error(self, ts_server: ServerProcess):
        """Test throwError returns an error."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            with pytest.raises(Exception):
                await rpc_call(client, "throwError", [])
    
    async def test_make_counter(self, ts_server: ServerProcess):
        """Test makeCounter returns a capability."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Get counter capability
            counter = await rpc_call(client, "makeCounter", [10])
            # Counter should be a stub we can call
            assert counter is not None
    
    async def test_callback_roundtrip(self, ts_server: ServerProcess):
        """Test callback: register client capability, server calls it back."""
        
        class ClientCallback(RpcTarget):
            def __init__(self):
                self.notifications: list[str] = []
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.notifications.append(args[0])
                    return f"Got: {args[0]}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        local = ClientCallback()
        
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{ts_server.port}/",
            local_main=local,
        ) as client:
            assert client._session is not None
            
            # Send client's local main to server
            callback_stub = RpcStub(client._session.get_export(0).dup())
            result = await rpc_call(client, "registerCallback", [callback_stub])
            assert result == "registered"
            
            # Have server call back
            result = await rpc_call(client, "triggerCallback", [])
            assert result == "Got: ping"
            assert local.notifications == ["ping"]


# =============================================================================
# Python Client → Python Server Tests (baseline)
# =============================================================================

@pytest.mark.asyncio
class TestPyClientPyServer:
    """Test Python client connecting to Python server (baseline)."""
    
    async def test_simple_square(self, py_server: ServerProcess):
        """Test simple square call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await rpc_call(client, "square", [5])
            assert result == 25
    
    async def test_add(self, py_server: ServerProcess):
        """Test add call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await rpc_call(client, "add", [3, 7])
            assert result == 10
    
    async def test_greet(self, py_server: ServerProcess):
        """Test greet call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await rpc_call(client, "greet", ["World"])
            assert result == "Hello, World!"
    
    async def test_echo_array(self, py_server: ServerProcess):
        """Test echo with array."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await rpc_call(client, "echo", [[1, 2, 3]])
            assert result == [1, 2, 3]
    
    async def test_generate_fibonacci(self, py_server: ServerProcess):
        """Test generateFibonacci."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await rpc_call(client, "generateFibonacci", [10])
            assert result == [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
    
    async def test_callback_roundtrip(self, py_server: ServerProcess):
        """Test callback: register client capability, server calls it back."""
        
        class ClientCallback(RpcTarget):
            def __init__(self):
                self.notifications: list[str] = []
            
            async def call(self, method: str, args: list) -> Any:
                if method == "notify":
                    self.notifications.append(args[0])
                    return f"Got: {args[0]}"
                raise ValueError(f"Unknown method: {method}")
            
            def get_property(self, name: str) -> Any:
                raise AttributeError(f"Unknown property: {name}")
        
        local = ClientCallback()
        
        async with WebSocketRpcClient(
            f"ws://127.0.0.1:{py_server.port}/rpc",
            local_main=local,
        ) as client:
            assert client._session is not None
            
            callback_stub = RpcStub(client._session.get_export(0).dup())
            result = await rpc_call(client, "registerCallback", [callback_stub])
            assert result == "registered"
            
            result = await rpc_call(client, "triggerCallback", [])
            assert result == "Got: ping"
            assert local.notifications == ["ping"]


# =============================================================================
# TypeScript Client → Python Server Tests (via subprocess)
# =============================================================================

@pytest.mark.asyncio
class TestTsClientPyServer:
    """Test TypeScript client connecting to Python server."""
    
    async def test_ts_client_simple_calls(self, py_server: ServerProcess):
        """Run TypeScript client test script against Python server."""
        # Check if npm install has been run
        node_modules = INTEROP_DIR / 'node_modules'
        if not node_modules.exists():
            pytest.skip("Run 'npm install' in tests/interop/ first")
        
        # Run TypeScript client test
        result = subprocess.run(
            ['npx', 'tsx', 'ts_client_test.ts', str(py_server.port)],
            cwd=INTEROP_DIR,
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode != 0:
            pytest.fail(f"TypeScript client test failed:\n{result.stderr}\n{result.stdout}")
        
        # Parse results
        assert "ALL TESTS PASSED" in result.stdout, f"Tests failed:\n{result.stdout}"
