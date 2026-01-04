# API Reference

Complete reference for all public APIs in Cap'n Web Python.

## Table of Contents

- [Client](#client)
- [Server](#server)
- [RPC Targets](#rpc-targets)
- [Capabilities](#capabilities)
- [Errors](#errors)
- [Pipelining](#pipelining)
- [Transports](#transports)

## Client

### ClientConfig

Configuration for the RPC client (Pydantic model).

```python
class ClientConfig(BaseModel):
    url: str                     # RPC endpoint URL (ws://, wss://, http://, https://)
    timeout: float = 30.0        # Request timeout in seconds (must be > 0)
    local_main: Any | None = None  # Optional capability to expose to server
    options: RpcSessionConfig | None = None  # Optional session configuration
```

**Example:**
```python
from capnweb.config import ClientConfig

config = ClientConfig(
    url="ws://localhost:8080/rpc",
    timeout=10.0
)
```

### WebSocketRpcClient

Main client class for making RPC calls over WebSocket.

```python
class WebSocketRpcClient:
    def __init__(
        self,
        url: str,
        local_main: Any | None = None,
        options: RpcSessionOptions | None = None,
    )
```

#### Methods

##### `async call(cap_id: int, method: str, args: list) -> Any`

Make a single RPC call.

**Parameters:**
- `cap_id`: ID of the capability to call (usually 0 for main)
- `method`: Name of the method to call
- `args`: List of arguments to pass

**Returns:** The method's return value

**Raises:** `RpcError` on failure

**Example:**
```python
from capnweb.ws_session import WebSocketRpcClient

async with WebSocketRpcClient("ws://localhost:8080/rpc") as client:
    result = await client.call(0, "add", [5, 3])
    print(result)  # 8
```

##### `get_main_stub() -> RpcStub`

Get the server's main capability as an RpcStub.

**Example:**
```python
async with WebSocketRpcClient("ws://localhost:8080/rpc") as client:
    stub = client.get_main_stub()
    result = await stub.add(5, 3)
```

##### `async close() -> None`

Close the connection and clean up resources.

##### Context Manager Support

```python
async with WebSocketRpcClient(url) as client:
    # Client automatically connected and closed
    result = await client.call(0, "method", [])
```

## Server

### WebSocketServerConfig

Configuration for the WebSocket RPC server (Pydantic model).

```python
class WebSocketServerConfig(BaseModel):
    host: str = "0.0.0.0"                # Host to bind to
    port: int = 8080                     # Port to bind to (1-65535)
    path: str = "/rpc"                   # WebSocket endpoint path
    local_main_factory: Callable[[], Any] | None = None  # Per-connection capability factory
    options: RpcSessionConfig | None = None  # Optional session configuration
```

**Example:**
```python
from capnweb.config import WebSocketServerConfig

config = WebSocketServerConfig(
    host="0.0.0.0",
    port=8080,
    path="/rpc",
    local_main_factory=lambda: MyService()
)
```

### RpcSessionConfig

Session-level configuration options.

```python
class RpcSessionConfig(BaseModel):
    on_send_error: Callable[[Exception], Exception | None] | None = None
```

**Use case:** Redact sensitive information from error stack traces before sending.

### BidirectionalSession

Core session class for bidirectional RPC.

```python
class BidirectionalSession:
    def __init__(
        self,
        transport: Transport,
        local_main: Any | None = None,
        options: RpcSessionOptions | None = None,
    )
```

#### Methods

##### `start() -> None`

Start the session's read loop.

##### `async stop() -> None`

Stop the session gracefully.

##### `async drain() -> None`

Wait for all pending operations to complete.

##### `get_main_stub() -> StubHook`

Get the peer's main capability as a StubHook.

## RPC Targets

### RpcTarget Base Class

Base class for objects that can be exposed as RPC capabilities.

```python
class RpcTarget(ABC):
    async def call(self, method: str, args: list[Any]) -> Any:
        """Handle method calls. Default dispatches to public methods."""

    async def get_property(self, prop: str) -> Any:
        """Handle property access. Default returns public attributes."""
```

#### Ergonomic Style (Recommended)

Just define public methods - they're automatically exposed:

```python
class Calculator(RpcTarget):
    def add(self, a: int, b: int) -> int:
        return a + b

    def multiply(self, a: int, b: int) -> int:
        return a * b

    async def fetch_data(self, id: int) -> dict:
        # Async methods are also supported
        return {"id": id, "data": "..."}
```

#### Explicit Style (Custom Dispatch)

Override `call()` for custom dispatch logic:

```python
class Calculator(RpcTarget):
    async def call(self, method: str, args: list) -> int:
        match method:
            case "add":
                return args[0] + args[1]
            case "multiply":
                return args[0] * args[1]
            case _:
                raise RpcError.not_found(f"Method {method} not found")
```

### Reserved Methods

These methods are never exposed as RPC endpoints:
- `call`, `get_property`, `dispose`
- Methods starting with `_`
- Python special methods (`__init__`, `__str__`, etc.)

### Optional: dispose()

If your target needs cleanup, implement a `dispose()` method:

```python
class DatabaseService(RpcTarget):
    def __init__(self, connection):
        self.conn = connection

    def query(self, sql: str) -> list:
        return self.conn.execute(sql).fetchall()

    def dispose(self):
        """Called when capability is released."""
        self.conn.close()
```

## Capabilities

### RpcStub

Represents a remote capability (returned from RPC calls).

```python
class RpcStub:
    __slots__ = ('_hook',)

    def __getattr__(self, name: str) -> RpcPromise:
        """Access properties, returns a promise."""

    def __call__(self, *args) -> RpcPromise:
        """Call as a function."""

    def map(self, mapper: Callable, path: list | None = None) -> RpcPromise:
        """Apply a mapper function to array elements remotely."""

    def dispose() -> None:
        """Release the capability."""
```

**Example:**
```python
user = await client.call(0, "getUser", ["alice"])
name = await user.name           # Property access
greeting = await user.greet()    # Method call
```

#### Context Manager Support

```python
async with stub as s:
    result = await s.process()
# Automatically disposed on exit
```

#### Methods

##### `dispose() -> None`

Release the capability and clean up resources.

##### `map(mapper, path=None) -> RpcPromise`

Apply a mapper function to array elements without transferring data.

**Example:**
```python
# Map over an array on the server
result = await stub.data.map(lambda x: x.double())
```

### RpcPromise

Represents a future value from an RPC call.

```python
class RpcPromise:
    def __await__(self):
        """Make the promise awaitable."""

    def __getattr__(self, name: str) -> RpcPromise:
        """Access properties on the future value."""
```

**Example:**
```python
# Create promise (doesn't block)
promise = stub.getUser("alice")

# Chain operations (still doesn't block)
name_promise = promise.name

# Await to get final value (blocks)
name = await name_promise
```

## Errors

### ErrorCode

Standard error codes.

```python
class ErrorCode(Enum):
    BAD_REQUEST = "bad_request"           # Invalid request
    NOT_FOUND = "not_found"               # Resource not found
    CAP_REVOKED = "cap_revoked"           # Capability revoked
    PERMISSION_DENIED = "permission_denied"  # Access denied
    CANCELED = "canceled"                 # Operation canceled
    INTERNAL = "internal"                 # Internal server error
```

### RpcError

Structured RPC error with error code and optional data.

```python
class RpcError(Exception):
    code: ErrorCode
    message: str
    data: Any | None
```

#### Factory Methods

```python
# Create errors using factory methods
raise RpcError.bad_request("Invalid input")
raise RpcError.not_found("User not found")
raise RpcError.permission_denied("Access denied")
raise RpcError.internal("Database connection failed")

# With custom data
raise RpcError.bad_request(
    "Validation failed",
    data={"field": "email", "reason": "invalid format"}
)
```

#### Handling Errors

```python
try:
    result = await client.call(0, "method", [args])
except RpcError as e:
    match e.code:
        case ErrorCode.NOT_FOUND:
            print(f"Not found: {e.message}")
        case ErrorCode.PERMISSION_DENIED:
            print(f"Access denied: {e.message}")
        case _:
            print(f"Error {e.code}: {e.message}")

    if e.data:
        print(f"Additional data: {e.data}")
```

## Pipelining

### PipelineBatch

Batch multiple RPC calls into a single network round-trip.

```python
class PipelineBatch:
    def __init__(self, client: Client, capability_id: int)
```

#### Methods

##### `call(method: str, args: list) -> PipelinePromise`

Queue a method call (doesn't execute yet).

**Parameters:**
- `method`: Method name to call
- `args`: Arguments to pass

**Returns:** A `PipelinePromise` that can be awaited later

**Example:**
```python
batch = PipelineBatch(client, capability_id=0)
call1 = batch.call("method1", [arg1])
call2 = batch.call("method2", [arg2])
```

##### `async execute() -> None`

Execute all queued calls in a single network request.

**Example:**
```python
batch = PipelineBatch(client, capability_id=0)

# Queue calls
promise1 = batch.call("add", [10, 20])
promise2 = batch.call("multiply", [5, 6])

# Execute batch (single network request)
await batch.execute()

# Get results
result1 = await promise1  # 30
result2 = await promise2  # 30
```

### PipelinePromise

Promise returned by `PipelineBatch.call()`.

```python
class PipelinePromise:
    def __await__(self):
        """Await to get the final result."""

    def __getattr__(self, name: str) -> PipelinePromise:
        """Chain property access."""
```

**Example:**
```python
batch = PipelineBatch(client, 0)

# Queue call and chain property access
user_promise = batch.call("getUser", ["alice"])
name_promise = user_promise.name  # Chained access

# Execute
await batch.execute()

# Get result
name = await name_promise  # "alice"
```

## Transports

### HttpBatchTransport

HTTP-based batch transport (default).

**Features:**
- Simple request/response
- Automatic batching
- Works with any HTTP server

**URL format:** `http://host:port/path` or `https://host:port/path`

**Example:**
```python
config = ClientConfig(url="http://localhost:8080/rpc/batch")
```

### WebSocketTransport

WebSocket-based transport for persistent connections.

**Features:**
- Persistent connection
- Lower latency for multiple calls
- Server can push updates

**URL format:** `ws://host:port/path` or `wss://host:port/path`

**Example:**
```python
config = ClientConfig(url="ws://localhost:8080/rpc/ws")
```

## Utility Classes

### RpcPayload

Wraps data with ownership semantics (internal use).

```python
class RpcPayload:
    @classmethod
    def from_app_params(cls, value: Any) -> RpcPayload:
        """Create from application parameters (will be copied)."""

    @classmethod
    def from_app_return(cls, value: Any) -> RpcPayload:
        """Create from application return value (ownership transferred)."""

    @classmethod
    def owned(cls, value: Any) -> RpcPayload:
        """Create from already-owned data (already copied)."""
```

**Note:** Usually you don't need to work with `RpcPayload` directly - the framework handles it.

## Type Hints

All public APIs are fully type-hinted. For best results, use a type checker:

```bash
# With pyrefly (recommended)
pyrefly check

# With mypy
mypy src/
```

**Example with type hints:**
```python
from capnweb.client import Client, ClientConfig
from capnweb.error import RpcError

async def get_user(client: Client, user_id: str) -> dict[str, Any]:
    """Fetch user data from the server."""
    try:
        user: dict[str, Any] = await client.call(0, "getUser", [user_id])
        return user
    except RpcError as e:
        print(f"Error fetching user: {e}")
        raise
```

## Next Steps

- **[Quickstart Guide](quickstart.md)** - Get started quickly
- **[Architecture Guide](architecture.md)** - Understand the internals
- **[Advanced Topics](advanced.md)** - Resume tokens, bidirectional RPC
