# Cap'n Web Python

[![CI](https://github.com/nikileshsa/capnweb-python/actions/workflows/ci.yml/badge.svg)](https://github.com/nikileshsa/capnweb-python/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-green.svg)](LICENSE)

> ⚠️ **Beta Version** - This library is under active development and not yet production-ready. APIs may change. Use at your own risk in production environments.

A complete Python implementation of the [Cap'n Web protocol](https://github.com/cloudflare/capnweb) - a capability-based RPC system with promise pipelining, structured errors, and multiple transport support.

## What's in the Box

**Core Features:**
- **Capability-based security** - Unforgeable object references with explicit disposal
- **Promise pipelining** - Batch multiple dependent calls into single round-trips
- **Multiple transports** - HTTP Batch, WebSocket, and WebTransport/HTTP/3
- **Type-safe** - Full type hints compatible with pyright/mypy
- **Async/await** - Built on Python's asyncio
- **Bidirectional RPC** - Full peer-to-peer capability passing
- **100% Interoperable** - Fully compatible with TypeScript reference implementation

**Status:**
- Full wire parity with the TypeScript reference at **v0.9.0** (all 8
  message types incl. `stream`/`pipe`, flow control, Blob/Headers/Request/
  Response, encoding levels, wire-faithful errors, `.map()` both directions)
- 1000+ tests: unit + golden-wire conformance fixtures generated from the TS
  `serialize()`/`deserialize()` + a FULL live interop suite against the real
  TS server + hypothesis property tests
- Clean hook-based architecture (same shape as the TS runtime)

## Why Use Cap'n Web?

**Traditional RPC has problems:**
- No security model (anyone can call anything)
- No resource management (memory leaks)
- Poor performance (round-trip per call)

**Cap'n Web solves these:**
- **Security**: Capabilities are unforgeable - you can only call what you have a reference to
- **Resource Management**: Explicit disposal with reference counting prevents leaks
- **Performance**: Promise pipelining batches dependent calls into one round-trip
- **Flexibility**: Pass capabilities as arguments - the server decides who gets access

## Installation

```bash
# Clone and install from source
git clone https://github.com/nikileshsa/capnweb-python.git
cd capnweb-python
uv sync

# For WebTransport support (optional):
uv pip install aioquic
```

## Quick Start

**Server (HTTP Batch):**
```python
import asyncio
from aiohttp import web
from capnweb import RpcTarget, RpcError, aiohttp_batch_rpc_handler

class Calculator(RpcTarget):
    async def call(self, method: str, args: list) -> any:
        match method:
            case "add": return args[0] + args[1]
            case "multiply": return args[0] * args[1]
            case _: raise RpcError.not_found(f"Unknown method: {method}")

    async def get_property(self, name: str) -> any:
        raise RpcError.not_found(f"Property '{name}' not found")

async def main():
    calc = Calculator()
    app = web.Application()
    app.router.add_post("/rpc/batch", lambda req: aiohttp_batch_rpc_handler(req, calc))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 8080).start()
    await asyncio.Event().wait()

asyncio.run(main())
```

**Client:**
```python
import asyncio
import aiohttp
from capnweb.batch import new_http_batch_rpc_session

async def main():
    async with aiohttp.ClientSession() as http:
        stub = await new_http_batch_rpc_session(
            "http://localhost:8080/rpc/batch", 
            http_client=http
        )
        result = await stub.add(5, 3)
        print(f"5 + 3 = {result}")  # Output: 8

asyncio.run(main())
```

**Capability Passing** (bidirectional RPC):
```python
# Server returns a capability, client calls methods on it directly
account = await main_stub.createAccount(1000.0)
balance = await account.getBalance()  # Pythonic API!
await account.deposit(500.0)
```

## TypeScript ↔ Python API mapping

The upstream TypeScript runtime (`cloudflare/capnweb`) is the canonical
implementation; this table maps its public surface to the Python spelling.

| TypeScript (capnweb) | Python (this package) | Notes |
|---|---|---|
| `newWebSocketRpcSession(wsOrUrl, localMain?, opts?)` | `await new_websocket_rpc_session(ws_or_url, local_main, options)` | Async (connecting is awaitable in asyncio). Accepts a URL or an open aiohttp WebSocket. Returns the peer's main `RpcStub`. |
| `newHttpBatchRpcSession(urlOrRequest, opts?)` | `await new_http_batch_rpc_session(url, headers=..., options=...)` | `headers=` replaces passing a `Request` (auth headers). One batch per session. |
| `newHttpBatchRpcResponse(request, localMain, opts?)` | `await new_http_batch_rpc_response(body_str, local_main, options)` | Framework-neutral: body-string in/out. Raises instead of truncating on timeout. |
| `nodeHttpBatchRpcResponse(req, res, main, opts & {headers})` | `aiohttp_batch_rpc_handler(request, local_main, headers=...)` / `fastapi_batch_rpc_handler` | Standalone batch endpoints. Do NOT set CORS headers. |
| `newWorkersRpcResponse(request, localMain)` | `aiohttp_rpc_handler(request, local_main, options)` | Unified endpoint: POST → batch, WS upgrade → WebSocket, else 400. The ONLY place that sets `Access-Control-Allow-Origin: *` — read its security warning. |
| `newWorkersWebSocketRpcResponse(...)` | `handle_websocket_rpc(request, local_main, options)` | aiohttp server handler. |
| `newMessagePortRpcSession(port, localMain?)` | `new_pipe_rpc_session_pair(main_a, main_b, encoding_level=...)` | In-process queue-pair sessions (MessagePort analog; `None` close sentinel). Python has no structured clone, so the levels are `"string"` (default) / `"jsonCompatible"` / `"jsonCompatibleWithBytes"`. |
| `RpcTransportWithCustomEncoding` (`encodingLevel`) | transport attribute `encoding_level` | `"jsonCompatible"`/`"jsonCompatibleWithBytes"` skip the JSON stringify/parse; unknown levels raise `TypeError` at session construction; `"structuredClonable"` raises `NotImplementedError` (JS-host feature). |
| `new RpcSession(transport, localMain?, opts?)` | `RpcSession(transport, local_main, options)` (alias of `BidirectionalSession`) | Python deviation: call `session.start()` explicitly; `async with`-style lifecycle via `stop()`. |
| `session.getRemoteMain()` | `get_remote_main(session)` | Returns `RpcStub`. (`session.get_main_stub()` returns the raw hook.) |
| `session.getStats()` / `session.drain()` | `session.get_stats()` / `await session.drain()` | |
| `new RpcStub(value)` | `RpcStub(value)` | Polymorphic: hook, `RpcTarget`, callable, or plain value. |
| `stub.dup()` | `stub.dup()` | On a promise, `dup()` returns a plain `RpcStub` (stub-ifies), like TS. |
| `stub.onRpcBroken(cb)` | `stub.on_rpc_broken(cb)` | On stubs and promises. |
| `stub[Symbol.dispose]()` / `using stub = ...` | `stub.dispose()` / `with stub:` (or `async with stub:`) | See disposal idiom below. |
| `RpcTarget` + `[Symbol.dispose]` | `RpcTarget` + `def dispose(self)` | Called when the last dup of the last stub is disposed. |
| `RpcTransport` interface | `RpcTransport` Protocol (exported from `capnweb`) | `send`/`receive` async; `abort(reason)`. |
| `WebSocketTransport` | `WebSocketTransport` (`capnweb.ws_transport`) | Wraps an open aiohttp WS (client or server side). Abort closes with code 3000 + reason. |
| `RpcSessionOptions = { onSendError? }` | `RpcSessionConfig(on_send_error=...)` | Python-only extensions: `pull_timeout` (default 120s bound on promise pulls), `drain_timeout` (default 30s batch-server bound), `heartbeat=` kwargs on WS client/server (aiohttp ping keepalive). |

### `RpcSessionConfig` security limits (Python-only hardening)

Local-policy bounds enforced against an untrusted peer — the wire format is
unchanged; a peer that breaches a bound is aborted. Defaults sit well above
legitimate traffic (interop stays 459/0); tune per deployment.

| Field | Default | Closes | Effect on breach |
|-------|---------|--------|------------------|
| `max_exports` | `100_000` | F1 | Abort — export table (peer pushes) capped at N live entries |
| `max_imports` | `100_000` | F2 | Abort — import table (referenced caps) capped at N live entries |
| `max_message_bytes` | `16 MiB` | F3 | Abort — single inbound frame rejected before parse |
| `max_array_len` | `1_000_000` | F5 | Abort — decoded wire array wider than N rejected |
| `max_blob_bytes` | `64 MiB` | F4 | Error — streamed blob exceeding N bytes rejected |
| `redact_internal_errors` | `True` | F6 | Unexpected (non-`RpcError`) exception text replaced with `"internal error"` on the wire; type/name kept. Deliberate `RpcError` messages pass; `on_send_error` still takes precedence. |

Python-only additions (no TS equivalent, kept deliberately):
`WebSocketRpcClient` / `WebSocketRpcServer` (lifecycle-managed pair;
`local_main_factory=` gives each connection its own main capability),
`UnifiedClient` (explicit `transport=` selection: `"auto" | "websocket" |
"http-batch" | "webtransport"` — WebTransport is never inferred from the URL),
`wait_closed(session)` (event-driven session-lifetime wait for server
handlers).

Removed (Phase C): the positional `client.call(cap_id, method, args)` API on
`WebSocketRpcClient` / `UnifiedClient`. TS has no such API — the stub IS the
API. Use `get_main_stub()` and call methods on the stub; in HTTP-batch mode
use `UnifiedClient.new_batch()` (or `new_http_batch_rpc_session`).

## Disposal & resource management (the Python idiom)

Cap'n Web capabilities are resources: a stub holds a table entry on the peer
until it is disposed. TS uses `using` / `Symbol.dispose`; Python spells the
same model as follows — **never rely on `__del__`/GC**:

1. **`stub.dispose()`** is `stub[Symbol.dispose]()`. Disposing the **main**
   stub (the one returned by `new_websocket_rpc_session` /
   `get_remote_main`) shuts the whole session down.
2. **`with stub:`** (or `async with stub:`) is `using stub = ...` — disposal
   is synchronous, so the sync form is the closest analog.
3. **`async with promise as value:`** awaits the promise, then disposes it —
   Python-only sugar. (TS `using` disposes *without* awaiting; deliberate
   difference.)
4. **`stub.dup()`** escapes scope-bound disposal: the underlying capability
   lives until *all* duplicates are disposed (hooks refcount). `dup()` on a
   promise returns an immediately usable `RpcStub`.
5. **`stub.on_rpc_broken(cb)`** notifies you when the backing session dies —
   pair it with `pull_timeout` for robust hang-free clients.
6. **Server side:** give your `RpcTarget` a `def dispose(self)` — it runs
   when the last client reference is released.
7. **Sessions:** `async with WebSocketRpcClient(...)` /
   `await session.stop()` is the canonical lifecycle; session end implicitly
   disposes everything it holds.
8. **Batch sessions** are one-shot: all calls are batched until the first
   result is awaited, which sends the single HTTP request; calls issued
   after that fail with `BatchEndError`. Use a fresh
   `new_http_batch_rpc_session()` per batch.

## Current Status

**Transports:**
- ✅ HTTP Batch (stateless, pipelining)
- ✅ WebSocket (full bidirectional RPC with capability passing)
- ✅ WebTransport/HTTP/3 (requires aioquic)

**Protocol Features (v0.9.0 parity):**
- ✅ Wire protocol — all 8 message types (`push`/`pull`/`resolve`/`reject`/`release`/`abort`/`stream`/`pipe`)
- ✅ Promise pipelining with lazy path accumulation (one fused pipeline per chain)
- ✅ `.map()`/remap both directions (recorder + applicator, TS mapper-index semantics)
- ✅ Streams + BBR-style flow control (numerically identical to `streams.ts`)
- ✅ Blob / Headers / Request / Response, `Undefined`/`InvalidDate` sentinels
- ✅ Bidirectional RPC (full capability passing), reference counting & disposal
- ✅ Wire-faithful structured errors (name/message/stack/properties/cause, AggregateError)
- ✅ Transport encoding levels (`string` / `jsonCompatible` / `jsonCompatibleWithBytes`)
- ✅ One codec stack (Serializer/Parser) shared by sessions and the standalone `serialize`/`deserialize` helpers

**Conformance:**
- ✅ Golden-wire fixtures generated from the TS reference (byte-parity oracle)
- ✅ FULL live interop suite vs the real TS 0.9 server (incl. adversarial cases)
- ✅ Hypothesis property tests

## Documentation

- **[Quickstart Guide](docs/quickstart.md)** - Get started in 5 minutes
- **[Architecture Guide](docs/architecture.md)** - Understand the internals
- **[Wire Format](docs/WIRE_FORMAT.md)** - Wire-format parsing algorithm
- **[Examples](examples/)** - Working code examples
- **[PARITY.md](PARITY.md)** - Parity status vs the TypeScript reference

## Examples

**All examples tested and working:**

| Example | Transport | Description |
|---------|-----------|-------------|
| `calculator/` | HTTP Batch | Simple RPC calculator with error handling |
| `batch-pipelining/` | HTTP Batch | Promise pipelining demonstration |
| `peer-to-peer/` | HTTP Batch | Bidirectional RPC (Alice & Bob) |
| `chat/` | WebSocket | Real-time chat with callbacks |
| `task-queue/` | WebSocket | Distributed task queue with progress callbacks |
| `collab-docs/` | WebSocket | Collaborative document editor |
| `capability-security/` | WebSocket | Bank account with capability attenuation |
| `microservices/` | WebSocket | Service mesh with capability passing |
| `actor-system/` | WebSocket | Supervisor/worker pattern |
| `webtransport/` | HTTP/3 | WebTransport/QUIC demo |

Each example includes a README with running instructions.

## Transport Features

| Feature | HTTP Batch | WebSocket | WebTransport |
|---------|------------|-----------|---------------|
| Request/Response | ✅ | ✅ | ✅ |
| Bidirectional RPC | ✅ | ✅ | ✅ |
| Capability Passing | ✅ | ✅ | ✅ |
| Server Callbacks | ✅ | ✅ | ✅ |
| Persistent Connection | ❌ | ✅ | ✅ |
| Multiplexing | Manual | Auto | Native |

**WebTransport:**
- Requires `aioquic` library: `pip install capnweb[webtransport]`
- Best for high-performance, low-latency applications

## Development

```bash
# Clone and install
git clone https://github.com/nikileshsa/capnweb-python.git
cd capnweb-python
uv sync

# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=capnweb --cov-report=term-missing
```

## Protocol Compliance

This implementation follows the [Cap'n Web protocol specification](https://github.com/cloudflare/capnweb/blob/main/protocol.md).

**Interoperability:**
Designed to be compatible with the TypeScript reference implementation. Interop test suite available in `interop/` directory.

## Acknowledgments & Key Improvements from Original Fork

This project is based on [py-capnweb](https://github.com/abilian/py-capnweb) by Abilian SAS.

**Why a separate repo?** The original implementation had [several architectural issues](https://github.com/abilian/py-capnweb/issues/5) that required a major refactor to fix properly. Rather than attempting incremental patches, we rebuilt core components from scratch while preserving the overall design.

### Key Improvements from Original

**Architecture:**
- Refactored `ValueCodec` and `CapabilityCodec` architecture for cleaner wire format handling
- Introduced `BidirectionalSession` for full duplex RPC communication
- Added `WebSocketServerTransport` and `WebSocketClientTransport` for persistent connections
- Implemented proper capability table management (imports/exports/promises)

**Full Bidirectional Streaming:**
- Server can now call methods on client-provided callbacks
- Real-time push notifications from server to client
- Progress callbacks for long-running operations
- Symmetric RPC - both peers can export and import capabilities

**Error Handling:**
- Fixed error code propagation (errors preserve their original codes through the RPC chain)
- `RpcError.from_wire()` for proper wire-to-error conversion
- Structured errors with `bad_request`, `not_found`, `permission_denied`, `internal` codes

**Public API:**
- Added `create_stub()` factory for ergonomic capability creation from `RpcTarget`
- Exported `RpcStub`, `RpcPromise` for direct use
- Pythonic method calls: `await stub.method(args)` instead of `stub._hook.call()`

**Examples:**
- 10 comprehensive examples demonstrating all features
- All examples use public API patterns (no internal `_hook` access)
- Each example tested and verified working

**Testing:**
- 744 tests with 70% coverage
- Production feature tests (chat, task-queue, data pipeline, pub/sub, etc.)
- Stress tests for concurrent operations

## License

Dual-licensed under MIT or Apache-2.0, at your option.
