# Cap'n Web Python - Quickstart Guide

Get up and running with Cap'n Web Python in 5 minutes.

## What is Cap'n Web?

Cap'n Web is a capability-based RPC protocol that enables:
- **Type-safe remote procedure calls** between Python services
- **Promise pipelining** to batch multiple dependent calls into one round-trip
- **Bidirectional RPC** where both client and server can export capabilities
- **Multiple transports** (HTTP batch, WebSocket, WebTransport)

## Installation

```bash
# Clone the repository
git clone https://github.com/nikileshsa/capnweb-python.git
cd capnweb-python

# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

## Example 1: Calculator (HTTP Batch)

The simplest way to get started. Great for stateless request/response RPC.

### Server (`server.py`)

```python
import asyncio
from typing import Any
from aiohttp import web
from capnweb import RpcTarget, RpcError, aiohttp_batch_rpc_handler

class Calculator(RpcTarget):
    """A simple calculator service."""

    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "add":
                return args[0] + args[1]
            case "subtract":
                return args[0] - args[1]
            case "multiply":
                return args[0] * args[1]
            case "divide":
                if args[1] == 0:
                    raise RpcError.bad_request("Division by zero")
                return args[0] / args[1]
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")

async def main():
    calculator = Calculator()
    
    async def rpc_handler(request: web.Request) -> web.Response:
        return await aiohttp_batch_rpc_handler(request, calculator)

    app = web.Application()
    app.router.add_post("/rpc/batch", rpc_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 8080).start()

    print("🧮 Calculator server running on http://127.0.0.1:8080/rpc/batch")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
```

### Client (`client.py`)

```python
import asyncio
import aiohttp
from capnweb.batch import new_http_batch_rpc_session

async def main():
    url = "http://127.0.0.1:8080/rpc/batch"

    async with aiohttp.ClientSession() as http:
        # Each call creates a new batch session
        stub = await new_http_batch_rpc_session(url, http_client=http)
        result = await stub.add(5, 3)
        print(f"5 + 3 = {result}")  # Output: 8

        stub = await new_http_batch_rpc_session(url, http_client=http)
        result = await stub.multiply(7, 6)
        print(f"7 × 6 = {result}")  # Output: 42

        # Error handling
        try:
            stub = await new_http_batch_rpc_session(url, http_client=http)
            await stub.divide(10, 0)
        except Exception as e:
            print(f"Error: {e}")  # bad_request: Division by zero

if __name__ == "__main__":
    asyncio.run(main())
```

### Run It

```bash
# Terminal 1 - Start server
uv run python server.py

# Terminal 2 - Run client
uv run python client.py
```

**Expected output:**
```
5 + 3 = 8
7 × 6 = 42
Error: bad_request: Division by zero
```

---

## Example 2: Chat (WebSocket with Bidirectional RPC)

For real-time applications where the server needs to push messages to clients.

### Server (`server.py`)

```python
import asyncio
from typing import Any
from aiohttp import web
from capnweb import RpcTarget, RpcError, BidirectionalSession, RpcStub
from capnweb.ws_transport import WebSocketServerTransport

# Store connected clients
clients: dict[str, RpcStub] = {}

class ChatServer(RpcTarget):
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "join":
                username, callback = args[0], args[1]
                clients[username] = callback
                return {"message": f"Welcome {username}!", "users": list(clients.keys())}
            case "sendMessage":
                username, text = args[0], args[1]
                # Broadcast to all clients (server calling client!)
                for name, client in clients.items():
                    await client.onMessage({"from": username, "text": text})
                return {"status": "sent"}
            case "leave":
                username = args[0]
                clients.pop(username, None)
                return {"message": f"Goodbye {username}!"}
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")

chat_server = ChatServer()

async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    transport = WebSocketServerTransport(ws)
    session = BidirectionalSession(transport, chat_server)
    session.start()
    
    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            transport.feed_message(msg.data)
    
    transport.set_closed()
    return ws

async def main():
    app = web.Application()
    app.router.add_get("/rpc/ws", ws_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 8080).start()
    
    print("💬 Chat server running on ws://127.0.0.1:8080/rpc/ws")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
```

### Client (`client.py`)

```python
import asyncio
from typing import Any
from capnweb import RpcTarget, RpcError, RpcStub, BidirectionalSession, create_stub
from capnweb.ws_transport import WebSocketClientTransport

class ChatCallback(RpcTarget):
    """Receives messages from server."""
    
    async def call(self, method: str, args: list[Any]) -> Any:
        if method == "onMessage":
            msg = args[0]
            print(f"📨 {msg['from']}: {msg['text']}")
            return None
        raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")

async def main():
    # Connect to server
    transport = WebSocketClientTransport("ws://127.0.0.1:8080/rpc/ws")
    await transport.connect()
    
    callback = ChatCallback()
    session = BidirectionalSession(transport, callback)
    session.start()
    
    server = RpcStub(session.get_main_stub())
    
    # Create a stub from our callback to pass to server
    callback_stub = create_stub(callback)
    
    # Join the chat
    result = await server.join("Alice", callback_stub)
    print(f"Joined: {result}")
    
    # Send a message (will be broadcast back to us)
    await server.sendMessage("Alice", "Hello everyone!")
    
    await asyncio.sleep(0.5)  # Wait for broadcast
    
    # Leave
    await server.leave("Alice")
    
    await session.stop()
    await transport.close()

if __name__ == "__main__":
    asyncio.run(main())
```

### Run It

```bash
# Terminal 1 - Start server
uv run python server.py

# Terminal 2 - Run client
uv run python client.py
```

**Expected output:**
```
Joined: {'message': 'Welcome Alice!', 'users': ['Alice']}
📨 Alice: Hello everyone!
```

---

## Example 3: Capability Security (Bank Account)

Demonstrates capability attenuation - creating read-only views of objects.

### Server (`server.py`)

```python
import asyncio
from typing import Any
from aiohttp import web
from capnweb import RpcTarget, RpcError, BidirectionalSession
from capnweb.ws_transport import WebSocketServerTransport

class BankAccount(RpcTarget):
    def __init__(self, balance: float = 0):
        self._balance = balance

    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "getBalance":
                return self._balance
            case "deposit":
                self._balance += args[0]
                return {"newBalance": self._balance}
            case "withdraw":
                if args[0] > self._balance:
                    raise RpcError.bad_request("Insufficient funds")
                self._balance -= args[0]
                return {"newBalance": self._balance}
            case "createReadOnlyView":
                # Return an attenuated capability!
                return ReadOnlyAccount(self)
            case _:
                raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")

class ReadOnlyAccount(RpcTarget):
    """Attenuated capability - can only read, not modify."""
    
    def __init__(self, account: BankAccount):
        self._account = account

    async def call(self, method: str, args: list[Any]) -> Any:
        if method == "getBalance":
            return self._account._balance
        raise RpcError.permission_denied(f"Read-only account cannot '{method}'")

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")

class BankGateway(RpcTarget):
    async def call(self, method: str, args: list[Any]) -> Any:
        if method == "createAccount":
            return BankAccount(balance=args[0])
        raise RpcError.not_found(f"Method '{method}' not found")

    async def get_property(self, name: str) -> Any:
        raise RpcError.not_found(f"Property '{name}' not found")

gateway = BankGateway()

async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    transport = WebSocketServerTransport(ws)
    session = BidirectionalSession(transport, gateway)
    session.start()
    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            transport.feed_message(msg.data)
    transport.set_closed()
    return ws

async def main():
    app = web.Application()
    app.router.add_get("/rpc/ws", ws_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 8080).start()
    print("🏦 Bank server running on ws://127.0.0.1:8080/rpc/ws")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
```

### Client (`client.py`)

```python
import asyncio
from capnweb import UnifiedClient, UnifiedClientConfig, RpcError

async def main():
    config = UnifiedClientConfig(url="ws://127.0.0.1:8080/rpc/ws")
    
    async with UnifiedClient(config) as client:
        main_stub = client.get_main_stub()
        
        # Create account with $1000
        account = await main_stub.createAccount(1000.0)
        print(f"Balance: ${await account.getBalance()}")
        
        # Deposit and withdraw
        await account.deposit(500.0)
        print(f"After deposit: ${await account.getBalance()}")
        
        await account.withdraw(200.0)
        print(f"After withdraw: ${await account.getBalance()}")
        
        # Create read-only view
        readonly = await account.createReadOnlyView()
        print(f"Read-only balance: ${await readonly.getBalance()}")
        
        # Try to deposit on read-only (should fail!)
        try:
            await readonly.deposit(100.0)
        except RpcError as e:
            print(f"Blocked: {e.code}")  # permission_denied

if __name__ == "__main__":
    asyncio.run(main())
```

### Run It

```bash
# Terminal 1
uv run python server.py

# Terminal 2
uv run python client.py
```

**Expected output:**
```
Balance: $1000.0
After deposit: $1500.0
After withdraw: $1300.0
Read-only balance: $1300.0
Blocked: permission_denied
```

---

## Error Handling

Cap'n Web has structured error handling with proper error codes:

```python
from capnweb import RpcError

# Server side - raise structured errors
async def call(self, method: str, args: list) -> Any:
    if method == "divide":
        if args[1] == 0:
            raise RpcError.bad_request("Division by zero")
        return args[0] / args[1]
    raise RpcError.not_found(f"Method {method} not found")

# Client side - catch errors
try:
    result = await stub.divide(10, 0)
except RpcError as e:
    print(f"Error: {e.code} - {e.message}")
    # Error: bad_request - Division by zero
```

**Available error codes:**
- `RpcError.not_found(msg)` - Method/property doesn't exist
- `RpcError.bad_request(msg)` - Invalid arguments
- `RpcError.permission_denied(msg)` - Not authorized
- `RpcError.internal(msg)` - Server error

---

## Next Steps

- **[Examples](../examples/)** - See all 10 working examples
- **[Architecture Guide](architecture.md)** - Understand the hook-based architecture
- **[API Reference](api-reference.md)** - Detailed API documentation
- **[Wire Format](WIRE_FORMAT.md)** - Protocol details

## Getting Help

- **GitHub Issues**: https://github.com/nikileshsa/capnweb-python/issues
- **Examples**: See the `examples/` directory for 10 complete examples
- **Tests**: The test suite (744 tests) has many usage examples
