# Cap'n Web Python Documentation

Welcome to the Cap'n Web Python documentation!

## Getting Started

- **[Quickstart Guide](quickstart.md)** - Get up and running in 5 minutes
- **[Installation](#installation)** - How to install the library
- **[Examples](../examples/)** - 10 working code examples

## Core Documentation

- **[API Reference](api-reference.md)** - Complete API documentation
- **[Architecture Guide](architecture.md)** - Understand the hook-based architecture
- **[Wire Format](WIRE_FORMAT.md)** - Protocol wire format details

## Installation

### Using uv (Recommended)

```bash
git clone https://github.com/nikileshsa/capnweb-python.git
cd capnweb-python
uv sync
```

### Using pip

```bash
git clone https://github.com/nikileshsa/capnweb-python.git
cd capnweb-python
pip install -e .
```

## Quick Example

### Server (HTTP Batch)

```python
import asyncio
from aiohttp import web
from capnweb import RpcTarget, RpcError, aiohttp_batch_rpc_handler

class Calculator(RpcTarget):
    async def call(self, method: str, args: list):
        match method:
            case "add": return args[0] + args[1]
            case _: raise RpcError.not_found(f"{method} not found")

    async def get_property(self, name: str):
        raise RpcError.not_found(f"{name} not found")

async def main():
    calc = Calculator()
    app = web.Application()
    app.router.add_post("/rpc/batch", lambda req: aiohttp_batch_rpc_handler(req, calc))
    
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 8080).start()
    print("Server running on http://127.0.0.1:8080/rpc/batch")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
```

### Client

```python
import asyncio
import aiohttp
from capnweb.batch import new_http_batch_rpc_session

async def main():
    url = "http://127.0.0.1:8080/rpc/batch"
    async with aiohttp.ClientSession() as http:
        stub = await new_http_batch_rpc_session(url, http_client=http)
        result = await stub.add(5, 3)
        print(f"5 + 3 = {result}")  # Output: 8

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

## What is Cap'n Web?

Cap'n Web is a capability-based RPC protocol that provides:

- **Type-safe RPC** - Fully type-hinted Python APIs
- **Promise Pipelining** - Batch multiple dependent calls into one round-trip
- **Bidirectional RPC** - Both client and server can export capabilities
- **Multiple Transports** - HTTP batch, WebSocket, WebTransport/HTTP/3
- **Structured Errors** - Rich error types with proper code propagation
- **Reference Counting** - Automatic resource cleanup

## Documentation Structure

### For Users

1. Start with the **[Quickstart Guide](quickstart.md)**
2. Review the **[API Reference](api-reference.md)** for details
3. Explore **[Examples](../examples/)** for real-world usage

### For Contributors

1. Understand the **[Architecture Guide](architecture.md)**
2. Check the **[TODO](../TODO.md)** for open tasks
3. Read the **[Changelog](../CHANGES.md)** for recent changes

## Features

### Implemented ✅

- HTTP batch transport (client & server)
- WebSocket transport (full bidirectional RPC)
- WebTransport/HTTP/3 support
- Capability dispatch and method calls
- Hook-based architecture (ValueCodec, CapabilityCodec)
- Promise pipelining
- Bidirectional RPC (peer-to-peer)
- TypeScript interoperability
- Structured error handling with code propagation
- Reference counting and resource management
- **744 tests, 70% coverage**

## Examples

| Example | Transport | Description |
|---------|-----------|-------------|
| `calculator/` | HTTP Batch | Simple RPC calculator |
| `batch-pipelining/` | HTTP Batch | Promise pipelining |
| `peer-to-peer/` | HTTP Batch | Bidirectional RPC |
| `chat/` | WebSocket | Real-time chat |
| `task-queue/` | WebSocket | Distributed tasks |
| `collab-docs/` | WebSocket | Collaborative editor |
| `capability-security/` | WebSocket | Capability attenuation |
| `microservices/` | WebSocket | Service mesh |
| `actor-system/` | WebSocket | Supervisor/worker |
| `webtransport/` | HTTP/3 | WebTransport demo |

## Protocol Compliance

This implementation follows the [Cap'n Web protocol specification](https://github.com/cloudflare/capnweb/blob/main/protocol.md).

**Compatibility:** 100% with TypeScript reference implementation

## Community

- **GitHub**: https://github.com/nikileshsa/capnweb-python
- **Issues**: https://github.com/nikileshsa/capnweb-python/issues

## License

Dual-licensed under MIT or Apache-2.0, at your option.
