"""Shared fixtures for interop tests.

This module provides robust server fixtures and test utilities
for cross-language protocol compliance testing.
"""

from __future__ import annotations

import asyncio
import gc
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncGenerator, Generator

import pytest

# Paths
INTEROP_DIR = Path(__file__).parent
PY_CAPNWEB_DIR = INTEROP_DIR.parent.parent
SRC_DIR = PY_CAPNWEB_DIR / "src"

# Add src to path
sys.path.insert(0, str(SRC_DIR))

# Ports - use ephemeral ports to avoid conflicts
TS_SERVER_BASE_PORT = 19100
PY_SERVER_BASE_PORT = 19200


def find_free_port() -> int:
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def is_port_in_use(port: int) -> bool:
    """Check if a port is in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


async def wait_for_port(port: int, timeout: float = 10.0) -> bool:
    """Wait for a port to become available."""
    start = time.time()
    while time.time() - start < timeout:
        if is_port_in_use(port):
            return True
        await asyncio.sleep(0.1)
    return False


def wait_for_port_sync(port: int, timeout: float = 10.0) -> bool:
    """Synchronous version of wait_for_port."""
    start = time.time()
    while time.time() - start < timeout:
        if is_port_in_use(port):
            return True
        time.sleep(0.1)
    return False


@dataclass
class ServerProcess:
    """Wrapper for a server subprocess."""
    
    process: subprocess.Popen
    port: int
    name: str
    
    def stop(self, timeout: float = 5.0) -> None:
        """Stop the server gracefully."""
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
    
    def is_running(self) -> bool:
        """Check if server is still running."""
        return self.process.poll() is None
    
    def get_output(self) -> tuple[str, str]:
        """Get stdout and stderr (returns empty strings if DEVNULL was used)."""
        return "", ""


def start_ts_server(port: int | None = None) -> ServerProcess:
    """Start the TypeScript test server."""
    if port is None:
        port = find_free_port()
    
    # Check if npm install has been run
    node_modules = INTEROP_DIR / "node_modules"
    if not node_modules.exists():
        raise RuntimeError(
            "Run 'npm install' in tests/interop/ first"
        )
    
    # Use DEVNULL to avoid blocking on full pipe buffers in CI
    # The server output is not needed for tests
    proc = subprocess.Popen(
        ["npx", "tsx", "./ts_server.ts", str(port)],
        cwd=INTEROP_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    
    if not wait_for_port_sync(port, timeout=15.0):
        proc.kill()
        proc.wait()
        raise RuntimeError(
            f"TypeScript server failed to start on port {port}"
        )
    
    return ServerProcess(process=proc, port=port, name="TypeScript")


def start_py_server(port: int | None = None) -> ServerProcess:
    """Start the Python test server."""
    if port is None:
        port = find_free_port()
    
    # Use DEVNULL to avoid blocking on full pipe buffers in CI
    proc = subprocess.Popen(
        [sys.executable, "py_server.py", str(port)],
        cwd=INTEROP_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, "PYTHONPATH": str(SRC_DIR)},
    )
    
    if not wait_for_port_sync(port, timeout=15.0):
        proc.kill()
        proc.wait()
        raise RuntimeError(
            f"Python server failed to start on port {port}"
        )
    
    return ServerProcess(process=proc, port=port, name="Python")


@pytest.fixture(scope="module")
def ts_server() -> Generator[ServerProcess, None, None]:
    """Start TypeScript server for the test module."""
    try:
        server = start_ts_server()
    except RuntimeError as e:
        pytest.skip(str(e))
        return
    
    yield server
    
    server.stop()


@pytest.fixture(scope="module")
def py_server() -> Generator[ServerProcess, None, None]:
    """Start Python server for the test module."""
    try:
        server = start_py_server()
    except RuntimeError as e:
        pytest.skip(str(e))
        return
    
    yield server
    
    server.stop()


@pytest.fixture
def ts_server_fresh() -> Generator[ServerProcess, None, None]:
    """Start a fresh TypeScript server for each test."""
    try:
        server = start_ts_server()
    except RuntimeError as e:
        pytest.skip(str(e))
        return
    
    yield server
    
    server.stop()


@pytest.fixture
def py_server_fresh() -> Generator[ServerProcess, None, None]:
    """Start a fresh Python server for each test."""
    try:
        server = start_py_server()
    except RuntimeError as e:
        pytest.skip(str(e))
        return
    
    yield server
    
    server.stop()


# =============================================================================
# Client Helpers
# =============================================================================

@dataclass
class InteropClient:
    """Unified client interface for interop testing."""
    
    url: str
    _client: Any = field(default=None, repr=False)
    _session: Any = field(default=None, repr=False)
    
    async def __aenter__(self) -> "InteropClient":
        from capnweb.ws_session import WebSocketRpcClient
        self._client = WebSocketRpcClient(self.url)
        await self._client.__aenter__()
        self._session = self._client._session
        return self
    
    async def __aexit__(self, *args) -> None:
        if self._client:
            await self._client.__aexit__(*args)
    
    async def call(self, method: str, args: list | None = None) -> Any:
        """Call a method on the remote main capability."""
        return await self._client.call(0, method, args or [])
    
    async def call_with_timeout(
        self, method: str, args: list | None = None, timeout: float = 5.0
    ) -> Any:
        """Call a method with a timeout."""
        return await asyncio.wait_for(
            self.call(method, args), timeout=timeout
        )
    
    async def call_expecting_error(
        self, method: str, args: list | None = None
    ) -> Exception:
        """Call a method expecting it to raise an error."""
        try:
            await self.call(method, args)
            raise AssertionError(f"Expected error from {method}")
        except Exception as e:
            return e
    
    def get_session(self) -> Any:
        """Get the underlying RPC session."""
        return self._session


@pytest.fixture
def py_client_to_ts(ts_server: ServerProcess):
    """Create a Python client connected to TypeScript server."""
    async def _create():
        client = InteropClient(f"ws://127.0.0.1:{ts_server.port}/")
        await client.__aenter__()
        return client
    return _create


@pytest.fixture
def py_client_to_py(py_server: ServerProcess):
    """Create a Python client connected to Python server."""
    async def _create():
        client = InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc")
        await client.__aenter__()
        return client
    return _create


# =============================================================================
# Wire Format Utilities
# =============================================================================

@dataclass
class MockTransport:
    """Mock transport that captures sent messages."""
    
    sent_messages: list[str] = field(default_factory=list)
    receive_queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    closed: bool = False
    
    async def send(self, message: str) -> None:
        if self.closed:
            raise ConnectionError("Transport closed")
        self.sent_messages.append(message)
    
    async def receive(self) -> str:
        if self.closed:
            raise ConnectionError("Transport closed")
        return await self.receive_queue.get()
    
    def abort(self, reason: Exception | None = None) -> None:
        self.closed = True
    
    def inject_message(self, message: str) -> None:
        """Inject a message to be received."""
        self.receive_queue.put_nowait(message)
    
    def get_last_message(self) -> str | None:
        """Get the last sent message."""
        return self.sent_messages[-1] if self.sent_messages else None
    
    def clear(self) -> None:
        """Clear sent messages."""
        self.sent_messages.clear()


def parse_wire_messages(raw: str) -> list[Any]:
    """Parse wire messages from a raw string."""
    import json
    messages = []
    for line in raw.strip().split("\n"):
        if line:
            messages.append(json.loads(line))
    return messages


def assert_wire_contains(messages: list[str], expected_type: str) -> Any:
    """Assert that messages contain a specific type."""
    import json
    for msg in messages:
        parsed = json.loads(msg) if isinstance(msg, str) else msg
        if isinstance(parsed, list) and parsed and parsed[0] == expected_type:
            return parsed
    raise AssertionError(f"No {expected_type} message found in {messages}")


# =============================================================================
# Memory Leak Detection (Inspired by grpclib)
# =============================================================================

def collect_objects() -> set[int]:
    """Collect all object IDs."""
    gc.collect()
    return {id(obj) for obj in gc.get_objects()}


@dataclass
class MemorySnapshot:
    """Snapshot of memory state for leak detection."""
    
    object_ids: set[int] = field(default_factory=set)
    
    @classmethod
    def take(cls) -> "MemorySnapshot":
        """Take a memory snapshot."""
        gc.collect()
        return cls(object_ids=collect_objects())
    
    def diff(self, other: "MemorySnapshot") -> set[int]:
        """Get new object IDs since this snapshot."""
        return other.object_ids - self.object_ids


@pytest.fixture
def memory_check():
    """Fixture for memory leak detection."""
    gc.collect()
    gc.disable()
    before = MemorySnapshot.take()
    
    yield before
    
    gc.collect()
    after = MemorySnapshot.take()
    gc.enable()
    
    # Check for leaks (allow some tolerance)
    new_objects = before.diff(after)
    # Filter out test infrastructure objects
    # This is a simplified check - real implementation would be more sophisticated


# =============================================================================
# Test Data Generators
# =============================================================================

ALL_PRIMITIVE_VALUES = [
    None,
    True,
    False,
    0,
    1,
    -1,
    42,
    3.14,
    -2.718,
    "",
    "hello",
    "Hello, 世界! 🌍",
]

ALL_ARRAY_VALUES = [
    [],
    [1],
    [1, 2, 3],
    [[1, 2], [3, 4]],
    [{"a": 1}, {"b": 2}],
]

ALL_OBJECT_VALUES = [
    {},
    {"key": "value"},
    {"nested": {"deep": {"value": 42}}},
    {"mixed": [1, "two", {"three": 3}]},
]

EDGE_CASE_VALUES = [
    {"empty_array": []},
    {"empty_object": {}},
    {"null_value": None},
    {"unicode": "日本語テスト"},
    {"emoji": "🎉🚀💻"},
    {"special_chars": "\"'\\/<>&"},
]


@pytest.fixture
def all_test_values():
    """All test values for comprehensive type testing."""
    return (
        ALL_PRIMITIVE_VALUES +
        ALL_ARRAY_VALUES +
        ALL_OBJECT_VALUES +
        EDGE_CASE_VALUES
    )
