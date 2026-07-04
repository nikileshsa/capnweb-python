# TypeScript Parity Status

**Current status (2026-07): full-parity implementation of the Cap'n Web
v0.9.0 protocol** (reference: cloudflare/capnweb @ `8a9f19d`, v0.9.0+2).
Implemented, wire-faithful to the TypeScript runtime:

- All **8 message types**: push / pull / resolve / reject / release / abort /
  **stream / pipe** (one message per frame on framed transports; newline
  batching only inside the HTTP batch transport).
- **Streams & flow control**: `RpcReadableStream` / `RpcWritableStream`,
  `WritableStreamHook` / `ReadableStreamGuardHook`, and a numerically
  identical port of the BBR-style `FlowController` (streams.ts:166-307),
  including `estimateEncodedSize`.
- **Value types**: `Undefined` and `InvalidDate` sentinels, `Blob` (always
  piped), `Headers` / `Request` / `Response`, bigint auto-promotion, unpadded
  base64 bytes, `["date", null]`.
- **Wire-faithful errors** (`RpcError(name, message, stack, properties,
  cause)`, `ExceptionGroup` ⇄ `AggregateError`; the 6-code enum is a derived
  convenience only).
- **`.map()`/remap** both directions: client-side recorder (`MapBuilder`
  with capture index space) and server-side applicator (`MapApplicator`)
  with the TS mapper-index semantics and the export-aliasing hard-fail.
- **Encoding levels** on transports: `string` (default), `jsonCompatible`,
  `jsonCompatibleWithBytes`; unknown levels are rejected at session
  construction; `structuredClonable` is rejected as a JS-host-only feature.
- **Public API parity**: `RpcStub`/`RpcPromise` with lazy path accumulation,
  `dup()`, `on_rpc_broken()`, sync/async CM disposal, `RpcSession` alias,
  `new_websocket_rpc_session`, `new_http_batch_rpc_session(_response)`,
  `new_pipe_rpc_session_pair` (MessagePort analog), `serialize`/`deserialize`
  standalone helpers.

Conformance is enforced by committed golden-wire fixtures generated from the
TS `serialize()`/`deserialize()` (tests/interop/fixtures/golden_wire.json),
a FULL live interop suite against the real TS 0.9 server (`npx tsx`,
tests/interop/), and a hypothesis property suite.

Documented deliberate Python extensions (not upstream):
`RpcSessionConfig.pull_timeout` / `.drain_timeout` bounded waits, aiohttp
`heartbeat=` keepalive, `WebSocketRpcClient`/`WebSocketRpcServer`/
`UnifiedClient` lifecycle wrappers, the serialized single-writer send queue,
and the six legacy code-string error names remaining revivable
(Python↔Python `.code` round-trip).

## Provenance

Derived from abilian/py-capnweb, an independent Python reimplementation of
Cloudflare's Cap'n Web protocol (https://github.com/cloudflare/capnweb).
The v0.4-era codebase implemented the 6-message core
(push/pull/resolve/reject/release/abort); a 2026-07 parity program brought
it to full v0.9.0 parity. The TS runtime remains the canonical protocol
reference for any future drift catch-up.
