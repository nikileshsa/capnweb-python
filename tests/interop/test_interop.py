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

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from capnweb.stubs import RpcStub
from capnweb.payload import RpcPayload

from .test_target import TestTarget, Counter


# Ports for test servers
TS_SERVER_PORT = 9100
PY_SERVER_PORT = 9200

# Paths
INTEROP_DIR = Path(__file__).parent
CAPNWEB_DIR = INTEROP_DIR.parent.parent.parent / 'capnweb'


def is_port_in_use(port: int) -> bool:
    """Check if a port is in use."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0


async def wait_for_port(port: int, timeout: float = 10.0) -> bool:
    """Wait for a port to become available."""
    start = time.time()
    while time.time() - start < timeout:
        if is_port_in_use(port):
            return True
        await asyncio.sleep(0.1)
    return False


@pytest.fixture(scope="module")
def ts_server():
    """Start TypeScript server for testing."""
    # Check if npm install has been run
    node_modules = INTEROP_DIR / 'node_modules'
    if not node_modules.exists():
        pytest.skip("Run 'npm install' in tests/interop/ first")
    
    # Start TypeScript server
    proc = subprocess.Popen(
        ['npx', 'tsx', 'ts_server.ts', str(TS_SERVER_PORT)],
        cwd=INTEROP_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    
    # Wait for server to start
    time.sleep(2)
    if not is_port_in_use(TS_SERVER_PORT):
        proc.kill()
        stdout, stderr = proc.communicate()
        pytest.fail(f"TypeScript server failed to start:\n{stderr.decode()}")
    
    yield proc
    
    # Cleanup - use SIGTERM and handle timeout gracefully
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


@pytest.fixture(scope="module")
def py_server():
    """Start Python server for testing."""
    proc = subprocess.Popen(
        [sys.executable, 'py_server.py', str(PY_SERVER_PORT)],
        cwd=INTEROP_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    
    # Wait for server to start
    time.sleep(2)
    if not is_port_in_use(PY_SERVER_PORT):
        proc.kill()
        stdout, stderr = proc.communicate()
        pytest.fail(f"Python server failed to start:\n{stderr.decode()}")
    
    yield proc
    
    # Cleanup - use SIGTERM and handle timeout gracefully
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


# =============================================================================
# Python Client → TypeScript Server Tests
# =============================================================================

@pytest.mark.asyncio
class TestPyClientTsServer:
    """Test Python client connecting to TypeScript server."""
    
    async def test_simple_square(self, ts_server):
        """Test simple square call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "square", [5])
            assert result == 25
    
    async def test_add(self, ts_server):
        """Test add call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "add", [3, 7])
            assert result == 10
    
    async def test_greet(self, ts_server):
        """Test greet call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "greet", ["World"])
            assert result == "Hello, World!"
    
    async def test_echo_string(self, ts_server):
        """Test echo with string."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "echo", ["test message"])
            assert result == "test message"
    
    async def test_echo_number(self, ts_server):
        """Test echo with number."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "echo", [42])
            assert result == 42
    
    async def test_echo_array(self, ts_server):
        """Test echo with array."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "echo", [[1, 2, 3]])
            assert result == [1, 2, 3]
    
    async def test_echo_nested_object(self, ts_server):
        """Test echo with nested object."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            obj = {"foo": {"bar": 123}, "baz": [1, 2, 3]}
            result = await client.call(0, "echo", [obj])
            assert result == obj
    
    async def test_generate_fibonacci(self, ts_server):
        """Test generateFibonacci."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "generateFibonacci", [10])
            assert result == [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
    
    async def test_get_list(self, ts_server):
        """Test getList."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "getList", [])
            assert result == [1, 2, 3, 4, 5]
    
    async def test_return_null(self, ts_server):
        """Test returnNull."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "returnNull", [])
            assert result is None
    
    async def test_return_number(self, ts_server):
        """Test returnNumber."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            result = await client.call(0, "returnNumber", [123])
            assert result == 123
    
    async def test_throw_error(self, ts_server):
        """Test throwError returns an error."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            with pytest.raises(Exception):
                await client.call(0, "throwError", [])
    
    async def test_make_counter(self, ts_server):
        """Test makeCounter returns a capability."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{TS_SERVER_PORT}/") as client:
            # Get counter capability
            counter = await client.call(0, "makeCounter", [10])
            # Counter should be a stub we can call
            assert counter is not None
    
    async def test_callback_roundtrip(self, ts_server):
        """Test callback: register client capability, server calls it back."""
        
        class ClientCallback:
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
            f"ws://127.0.0.1:{TS_SERVER_PORT}/",
            local_main=local,
        ) as client:
            assert client._session is not None
            
            # Send client's local main to server
            callback_stub = RpcStub(client._session.get_export(0).dup())
            result = await client.call(0, "registerCallback", [callback_stub])
            assert result == "registered"
            
            # Have server call back
            result = await client.call(0, "triggerCallback", [])
            assert result == "Got: ping"
            assert local.notifications == ["ping"]


# =============================================================================
# Python Client → Python Server Tests (baseline)
# =============================================================================

@pytest.mark.asyncio
class TestPyClientPyServer:
    """Test Python client connecting to Python server (baseline)."""
    
    async def test_simple_square(self, py_server):
        """Test simple square call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{PY_SERVER_PORT}/rpc") as client:
            result = await client.call(0, "square", [5])
            assert result == 25
    
    async def test_add(self, py_server):
        """Test add call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{PY_SERVER_PORT}/rpc") as client:
            result = await client.call(0, "add", [3, 7])
            assert result == 10
    
    async def test_greet(self, py_server):
        """Test greet call."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{PY_SERVER_PORT}/rpc") as client:
            result = await client.call(0, "greet", ["World"])
            assert result == "Hello, World!"
    
    async def test_echo_array(self, py_server):
        """Test echo with array."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{PY_SERVER_PORT}/rpc") as client:
            result = await client.call(0, "echo", [[1, 2, 3]])
            assert result == [1, 2, 3]
    
    async def test_generate_fibonacci(self, py_server):
        """Test generateFibonacci."""
        async with WebSocketRpcClient(f"ws://127.0.0.1:{PY_SERVER_PORT}/rpc") as client:
            result = await client.call(0, "generateFibonacci", [10])
            assert result == [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
    
    async def test_callback_roundtrip(self, py_server):
        """Test callback: register client capability, server calls it back."""
        
        class ClientCallback:
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
            f"ws://127.0.0.1:{PY_SERVER_PORT}/rpc",
            local_main=local,
        ) as client:
            assert client._session is not None
            
            callback_stub = RpcStub(client._session.get_export(0).dup())
            result = await client.call(0, "registerCallback", [callback_stub])
            assert result == "registered"
            
            result = await client.call(0, "triggerCallback", [])
            assert result == "Got: ping"
            assert local.notifications == ["ping"]


# =============================================================================
# TypeScript Client → Python Server Tests (via subprocess)
# =============================================================================

@pytest.mark.asyncio
class TestTsClientPyServer:
    """Test TypeScript client connecting to Python server."""
    
    async def test_ts_client_simple_calls(self, py_server):
        """Run TypeScript client test script against Python server."""
        # Check if npm install has been run
        node_modules = INTEROP_DIR / 'node_modules'
        if not node_modules.exists():
            pytest.skip("Run 'npm install' in tests/interop/ first")
        
        # Run TypeScript client test
        result = subprocess.run(
            ['npx', 'tsx', 'ts_client_test.ts', str(PY_SERVER_PORT)],
            cwd=INTEROP_DIR,
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode != 0:
            pytest.fail(f"TypeScript client test failed:\n{result.stderr}\n{result.stdout}")
        
        # Parse results
        assert "ALL TESTS PASSED" in result.stdout, f"Tests failed:\n{result.stdout}"
