# Cap'n Web Python — Performance Baseline & Optimization Opportunities

**Status:** baseline measurement pass (no `src/` changes). This document is the
*before* evidence the optimization pass will be held to. Every number here is
reproducible with `make bench` on the pinned environment below.

**Mandate:** the vendored Python Cap'n Web must be the most performant Cap'n Web
implementation, *proven by benchmarks, not asserted*. This pass (a) establishes
where Python actually stands versus the canonical TypeScript runtime on the RPC
metrics that matter, (b) profiles the hot paths, and (c) produces a prioritized,
file:line-cited optimization list. It is intellectually honest: it reports where
TS is faster and why, and where our design (pipelining/batching) wins.

---

## 0. TL;DR

* **Codec (pure serialize/parse):** Python trails V8 by **2.4–2.8× on large
  (1 MiB) payloads**, **~6× on realistic nested RPC args**, and **15–45× on
  tiny scalars / 10k-int arrays** where CPython's per-call and per-element
  interpreter overhead dominates. Python is *faster than TS on error-value
  decode* (0.75×) because TS materializes real `Error` objects.
* **In-process RPC round-trip:** Python `155 µs` vs TS `9 µs` (~17×). But
  **~33 % of the Python time is the asyncio event loop + `kqueue` syscall on the
  loopback** (profiler-confirmed), not Cap'n Web code, and **JSON framing is
  only ~12 %** (jsonCompatible round-trip is `138 µs`). The library-attributable
  CPU gap is real but much smaller than the wall-clock ratio suggests.
* **Pipelining works and is the headline win:** a 5-deep dependent chain issued
  without intermediate awaits runs in `307 µs` vs `779 µs` for the same calls
  awaited sequentially — a **2.54× end-to-end speedup** from one batched flush.
  (TS's own pipelining win on the same workload is 1.82×.)
* **Streaming:** Python `~155 MB/s` vs TS `~4.1 GB/s` (~27×). The gap is structural
  — the "string" transport base64+JSON-encodes every 64 KiB chunk, while TS's
  MessagePort structured-clones raw bytes. This is the single largest gap and
  the clearest optimization target for byte-heavy streams.
* **Lifecycle bookkeeping is not a hotspot:** export grant+release is `495 ns`
  per cycle (~2.0 M/s), table stays bounded across 400 k cycles. A1's
  release-lifecycle work did not introduce a hot path.
* **Per-session memory:** ~22.9 KB per bidirectional loopback pair (two
  sessions).

The top optimization levers, in priority order, are in §7: **(1)** eliminate the
redundant wire→dataclass→JSON double-pass in the receive path, **(2)** fuse the
payload deep-copy into serialization, **(3)** replace `isinstance`-chains in the
codec hot loops with `type()` dispatch, **(4)** stream bytes without per-chunk
base64+JSON on custom transports, **(5)** avoid `parse_wire_batch`'s
strip/split on single-frame transports.

---

## 1. Environment (pinned)

| | |
|---|---|
| Machine | Apple **M4 Max**, 16 cores, 128 GiB RAM |
| OS | macOS (Darwin 25.2.0, arm64) |
| Python | **CPython 3.12.11** (the `uv`-pinned interpreter used by `make bench` and the profiler; the system `python3` is 3.14.6 but is *not* what the suite runs on) |
| Node | **v24.4.1**, V8 13.6.233.10 |
| Python pkg | `py-capnweb` @ vendored `src/` (this tree) |
| TS pkg | **`capnweb` 0.9.0** (`tests/interop/node_modules`, run via `npx tsx`) |
| Clock | `time.perf_counter_ns` (Py) / `process.hrtime.bigint()` (TS) — monotonic ns, no `Date.now` |

All timings: warmup iterations discarded, then N samples of an inner batch;
GC disabled during the timed window (Python); median / p95 / stddev reported.

---

## 2. How to run (reproducible)

```bash
cd packages/capnweb

make bench          # Python suite  -> benchmarks/results/python_baseline.json
make bench-ts       # TS reference  -> benchmarks/results/ts_baseline.json
make bench-profile  # cProfile the two heaviest workloads (codec + RPC)

# side-by-side table from the two JSON files:
uv run python -m benchmarks.compare

# subsets:
uv run python -m benchmarks.run_all serialize          # one group
cd benchmarks/ts_compare && npx tsx run_all.ts rpc      # one TS group
```

`benchmarks/ts_compare/node_modules` is a symlink to the interop install of
`capnweb` 0.9.0 + `tsx`, so no extra `npm install` is needed. The 11-minute
interop conformance suite is **not** invoked by any bench target.

### Harness design

* `benchmarks/_harness.py` / `ts_compare/harness.ts` — warmup + samples,
  median/p95/mean/stddev, ns/op derived from a timed inner batch (amortizes the
  clock's own cost). `bench_async` reuses one event loop across samples so
  session/transport state stays hot.
* `_payloads.py` / `payloads.ts` — the **same** canonical shapes on both sides.
* Workloads: `bench_serialize` (codec), `bench_rpc` (round-trip / pipelining /
  batch / fan-out), `bench_streams` (64 MiB pump), `bench_tables` (grant/release
  churn). `_targets.py` provides the shared `BenchService` and a `PipePair`
  that cleanly `stop()`s both sessions (avoids the task-leak noise that would
  otherwise perturb the fan-out memory number).

### Apples-to-apples caveats (read before trusting a ratio)

1. **Codec micro** is a fair comparison: both sides call the library's standalone
   `serialize()`/`deserialize()`, which is JSON-string in and out on both.
2. **RPC / stream transport is *not* symmetric.** Python's loopback pipe defaults
   to the `"string"` encoding level (JSON per frame — the realistic WebSocket
   wire). TS's `MessagePort` transport is `"structuredClonable"` (postMessage
   clones the value tree — **no** JSON per frame). To isolate this we also run
   Python at the `jsonCompatible` level (`roundtrip/add(2,3)_jsonCompatible`),
   the closest analog to structured clone. Compare against that for the fair
   head-to-head; against `"string"` for the realistic-wire cost.
3. **Event-loop cost differs.** Python `asyncio` hits `kqueue` on every await
   even in-process; Node's libuv MessagePort scheduling is cheaper. On a real
   network transport this floor is replaced by actual socket I/O on both sides.

---

## 3. Python baseline (median / p95)

### 3.1 Codec — serialize / deserialize / roundtrip

| workload | serialize | deserialize | roundtrip | wire bytes |
|---|--:|--:|--:|--:|
| small_scalar (`42`) | 1.00 µs | 1.62 µs | 2.79 µs | 2 |
| short_string | 694 ns | 1.62 µs | 2.46 µs | 13 |
| nested_object (realistic RPC arg) | 5.17 µs | 6.60 µs | 12.40 µs | 245 |
| large_array_10k_ints | 805 µs | 1.499 ms | 2.304 ms | 48,893 |
| large_string_1mb | 1.845 ms | 480 µs | 2.335 ms | 1,048,578 |
| bytes_blob_1mb (base64) | 3.151 ms | 1.878 ms | 5.085 ms | 1,398,114 |
| error_value | 1.85 µs | 2.22 µs | 4.40 µs | 51 |
| capability_payload (8 stubs, tree only) | 2.82 µs | — | — | — |

p95 tracks median within ~5 % on the micro shapes; stddev is <2 % of median on
the 1 MiB shapes (see JSON for exact figures).

### 3.2 End-to-end RPC (in-process loopback)

| workload | median | p95 | derived |
|---|--:|--:|--|
| roundtrip / add(2,3) — string | 155.9 µs | 167 µs | 6.4 k calls/s |
| roundtrip / add(2,3) — jsonCompatible | 137.8 µs | 140 µs | 7.3 k calls/s |
| roundtrip / echo(nested) | 170.7 µs | 185 µs | 5.9 k calls/s |
| pipeline / 5-deep **pipelined** | 307 µs | 314 µs | **2.54× vs sequential** |
| pipeline / 5-deep sequential | 779 µs | 860 µs | 5 round-trips |
| batch / 1000 calls (pipe) | 35.2 ms | 36 ms | **28.4 k calls/s** |
| batch / 1000 calls (HTTP loopback) | 32.5 ms | 33 ms | 30.8 k calls/s |
| fanout / 100 sessions × 20 calls | 99.5 ms | 114 ms | 20.1 k calls/s; **22.9 KB/pair** |

### 3.3 Streaming & bookkeeping

| workload | result |
|---|--|
| stream / 64 MiB loopback | **154 MB/s** (415 ms/64 MiB); FlowController `INITIAL_WINDOW=256 KiB`, `MAX=1 GiB`, `MIN=64 KiB` (BBR-style) |
| tables / grant+release cycle | **495 ns/cycle**, ~2.0 M/s; export table bounded (≤1 entry across 400 k cycles → no leak) |

---

## 4. Side-by-side vs TypeScript 0.9.0 (honest ratios)

**PY/TS > 1 means TS is faster.** TS is V8/JIT; Python is CPython/interpreted —
a language-level floor of several × is expected on interpreter-bound work.

### 4.1 Codec

| workload | Python | TS 0.9.0 | PY/TS |
|---|--:|--:|--:|
| serialize/small_scalar | 1.00 µs | 35 ns | 28.6× |
| serialize/short_string | 694 ns | 42 ns | 16.5× |
| serialize/nested_object | 5.17 µs | 871 ns | 5.9× |
| serialize/large_array_10k | 805 µs | 135 µs | 5.9× |
| serialize/large_string_1mb | 1.845 ms | 657 µs | 2.8× |
| serialize/bytes_blob_1mb | 3.151 ms | 1.336 ms | 2.36× |
| serialize/error_value | 1.85 µs | 368 ns | 5.0× |
| deserialize/small_scalar | 1.62 µs | 36 ns | 45× |
| deserialize/nested_object | 6.60 µs | 1.23 µs | 5.4× |
| deserialize/large_array_10k | 1.50 ms | ~100 µs | **~15×** |
| deserialize/large_string_1mb | 480 µs | 361 µs | **1.33×** |
| deserialize/bytes_blob_1mb | 1.878 ms | 582 µs | 3.2× |
| **deserialize/error_value** | 2.22 µs | 2.94 µs | **0.75× (Python faster)** |
| roundtrip/large_string_1mb | 2.335 ms | 1.006 ms | 2.3× |

**Reading it:** the gap is *smallest where the work is C-bound* (1 MiB string:
both are memcpy + `json`, so 1.3–2.8×) and *largest where CPython pays
per-element / per-call interpreter overhead* (10k-int array decode: 15×; tiny
scalars: 28–45×, because a 2-byte value still pays `json.loads` + `Parser()` +
`RpcPayload` allocation + recursive `_parse_value` + `payload.dispose()`).

### 4.2 RPC & stream

| workload | Python | TS 0.9.0 | PY/TS | notes |
|---|--:|--:|--:|--|
| roundtrip/add (string) | 155.9 µs | 9.13 µs | 17.1× | realistic JSON wire |
| roundtrip/add (jsonCompatible) | 137.8 µs | 9.13 µs | 15.1× | fair vs structured-clone |
| roundtrip/echo(nested) | 170.7 µs | 11.1 µs | 15.4× | |
| pipeline/5-deep pipelined | 307 µs | 23.3 µs | 13.2× | |
| pipeline/5-deep sequential | 779 µs | 42.3 µs | 18.4× | |
| batch/1000 calls | 35.2 ms | 6.80 ms | 5.2× | Python closes the gap under load |
| fanout/100×20 | 99.5 ms | 15.5 ms | 6.4× | Python closes the gap under load |
| stream/64 MiB | ~155 MB/s | ~4140 MB/s | **26.7×** | string base64+JSON vs structured clone |

**Where our design wins end-to-end:** pipelining and batching *narrow* the gap
(17× single call → 5.2× at 1000-call batch) because they amortize the fixed
per-await asyncio cost — the exact scenario Cap'n Web is built for. The Python
pipelining speedup (2.54×) is *larger* than TS's (1.82×) precisely because
Python's per-round-trip floor is higher, so collapsing 5 round-trips into 1
saves more. This is the honest headline: **on the metric Cap'n Web exists to
optimize — dependent call chains — the Python impl gets proportionally more
benefit, and under batch load it is within ~5× of V8.**

---

## 5. Profiler findings (cProfile; py-spy unavailable on this host)

### 5.1 Codec roundtrip (nested ×20k + array10k ×200) — 1.976 s total

| function | tottime | calls | note |
|---|--:|--:|--|
| `parser.py:205 _parse_value` | 0.745 s | 2.44 M | **#1 hot** — recursive decode |
| `serializer.py:241 _serialize_value` | 0.466 s | 2.44 M | recursive encode |
| `builtins.isinstance` | **0.307 s** | **5.96 M** | **~15 % of total**, all in the codec loops |
| `list.append` | 0.101 s | 2.10 M | escaped-array rebuild, one append/elem |
| `json encoder.iterencode` | 0.093 s | 20.2 k | JSON is only ~8 % combined |
| `json decoder.raw_decode` | 0.071 s | 20.2 k | |

**Takeaway:** JSON (`dumps`+`loads`) is a *minority* (~0.16 s / 1.98 s). The
Python tree-walk dominates, and `isinstance` — 5.96 M calls — is the single
biggest built-in cost. The decoder (`_parse_value`) is ~1.6× the encoder.

### 5.2 RPC roundtrip add(2,3) ×20k — 5.790 s total

| function | tottime | cumtime | note |
|---|--:|--:|--|
| `select.kqueue.control` | **1.841 s** | 1.841 s | **~33 %** — asyncio loopback syscall, not capnweb |
| `base_events._run_once` | 0.226 s | 5.842 s | event-loop driver |
| `_contextvars.Context.run` | 0.084 s | 3.564 s | per-callback context |
| `queues.Queue.get` | 0.124 s | 0.341 s | transport handoff |
| `rpc_session.py:458 _writer_loop` | 0.090 s | 0.538 s | |
| `wire.py:713 parse_wire_batch` | 0.051 s | 0.616 s | strip/split even for 1 frame |
| `rpc_session.py:1319 _process_message` | 0.058 s | 0.751 s | |
| `payload.py:317 _deep_copy_value` | 0.062 s | 0.077 s | 3rd tree-walk per call |
| `json loads`+`dumps`+enc/dec | ~0.30 s | | ~12 % of total wall |

**Takeaway:** a full third of in-process RPC latency is the asyncio scheduler +
`kqueue` on the loopback (a floor that a real socket transport replaces with I/O,
not a Cap'n Web inefficiency). Of the *library*-attributable time, wire parsing
(json.loads → `wire_expression_from_json` dataclass build → Parser re-dispatch),
send-side `json.dumps`, and the payload deep-copy are the movable parts.

### 5.3 Allocation hotspots (per RPC call)

The args tree is walked **at least three times** per call: `_deep_copy_value`
(ownership copy), `_serialize_value` (encode), and `_parse_value` (decode on the
peer) — plus `wire_expression_from_json` builds throwaway `WirePipeline`/
`WireError` dataclasses that `Parser` immediately converts back to JSON. Each
call also allocates a fresh `Serializer`, `Parser`, `RpcPayload`, `WirePipeline`,
and a `PropertyKey` list.

---

## 6. Structural inefficiencies spotted (evidence for §7)

* **Redundant double-pass on receive.** `parse_wire_message_tree`
  (`wire.py:618`) calls `wire_expression_from_json` (`wire.py:359`) which builds
  `WirePipeline`/`WireError`/`WireDate`/`WireRemap` dataclasses; then
  `Parser._parse_value` (`parser.py:224`) does `isinstance(value, (WireError,
  WireDate, WirePipeline, WireRemap))` and immediately calls `.to_json()` to turn
  them **back** into raw lists and re-dispatches. The dataclass layer is pure
  overhead on the value path.
* **`isinstance` chains in the decode hot loop.** `_parse_value`
  (`parser.py:224,228,231,258`) runs an `isinstance` cascade on every node — the
  wire-dataclass check (`:224`) fires on every value even on the JSON path where
  those types never occur. Encoder uses fast `type(v) is X` (`serializer.py:249+`);
  decoder does not.
* **Escaped-array rebuild one append at a time.** `parser.py:234-243` builds the
  result list with `.append` in a loop (2.1 M appends in the profile) instead of
  a comprehension.
* **`parse_wire_batch` strip/split per frame.** `wire.py:713-716` does
  `data.strip().split("\n")` on every inbound frame; on natively-framed
  transports (WS, pipe) there is always exactly one message, so the batch split
  is wasted work on the hot path.
* **Third tree-walk for ownership.** `payload.py:317 _deep_copy_value` re-walks
  the args tree with its own `isinstance` cascade before serialization re-walks
  it again.
* **Per-message object churn.** `send_call` (`rpc_session.py:648-694`) allocates a
  `PropertyKey` list + `WirePipeline` + `WirePush` per call; `_serialize_frame`
  → `serialize_wire_message` does a per-message `json.dumps`.
* **Per-chunk base64+JSON for byte streams.** On the `"string"` level every
  stream chunk is `["bytes", <base64>]`-encoded and JSON-stringified
  (`serializer.py:294-302`), decoded symmetrically — the ~27× stream gap.

---

## 7. Prioritized optimization opportunities

Ranked by **(estimated win × safety)**. "Safety" = low risk to correctness /
wire-compat. None of these require changing the wire format.

### P1 — Drop the wire→dataclass→JSON double-pass on the value path
* **Win:** ~10–20 % of RPC round-trip and resolve/push decode; removes a whole
  allocation+conversion layer. Helps: roundtrip, batch, fanout.
* **Where:** `wire.py:359 wire_expression_from_json` + `parser.py:224` (the
  `isinstance(... WireError/WireDate/WirePipeline/WireRemap ...)` normalize-back).
  Keep wire *message* framing (`WirePush` etc.); stop wrapping *expression*
  interiors in dataclasses that `Parser` only unwraps again.
* **Risk:** Medium — must preserve the `["pipeline"]`/`["remap"]` accept paths
  and the `property_path` `None`-vs-`[]` distinction (`wire.py:230-246`). Covered
  by `test_wire*`, `test_parser`, golden fixtures.
* **Pure-Python.**

### P2 — Fuse the payload deep-copy into serialization
* **Win:** eliminates one of three per-call tree-walks (`_deep_copy_value` was
  0.062 s tottime / 20 k calls in the profile); ~5–10 % of round-trip, more on
  large args. Helps: roundtrip, batch, large nested args.
* **Where:** `payload.py:278 ensure_deep_copied` / `:317 _deep_copy_value` vs
  `serializer.py:234 serialize_payload`. The serializer already produces a fresh
  tree and already tracks stubs/promises; the ownership copy can be done *during*
  the encode walk instead of before it.
* **Risk:** Medium-High — deep-copy also enforces ownership/refcount semantics
  (dup vs take, `obj._hook = None`); fusing must not drop a stub ref. Heavily
  covered by capability-lifecycle + disposal tests.
* **Pure-Python.**

### P3 — Replace `isinstance`-chains with `type()` dispatch in the decoder
* **Win:** `isinstance` was 5.96 M calls / 0.307 s (~15 %) in the codec profile;
  a `type(value)` fast-path + dict dispatch on the tag string could cut a large
  fraction. Helps: every decode — nested args, arrays, resolve values.
* **Where:** `parser.py:205 _parse_value` (mirror the encoder's `type(v) is X`
  ladder at `serializer.py:249+`), and hoist the wire-dataclass check out of the
  JSON path (see P1). Consider a `dict[str, handler]` for `_parse_tagged`
  (`parser.py:279`) instead of the `if tag == ...` cascade.
* **Risk:** Low — behavior-preserving refactor; exact-type semantics already the
  intended contract. Covered by `test_parser`, golden fixtures.
* **Pure-Python.**

### P4 — Byte-stream fast path without per-chunk base64+JSON
* **Win:** largest single gap (stream ~155 MB/s → potential multi-hundred MB/s).
  Helps: streaming, Blob transfer, large `bytes` payloads.
* **Where:** `serializer.py:294-302` (base64 per chunk) + the pipe pump
  (`streams.py` `_ChannelSink`/`RpcWritableStream.write`). On the
  `jsonCompatibleWithBytes` level the encoder already keeps bytes raw
  (`serializer.py:296-299`); wire the pipe/stream pump to use it on custom
  transports so chunks skip base64+JSON entirely. For the `"string"` WebSocket
  wire, base64 is mandated by the protocol — no change there.
* **Risk:** Low-Medium — only affects custom-encoding transports; wire-compat on
  `"string"` unchanged. Covered by `test_b1_streams`, `test_streams_interop`.
* **Pure-Python** (a C base64 is already used; the win is *avoiding* it).

### P5 — Skip `parse_wire_batch` strip/split on natively-framed transports
* **Win:** small but free (~2–4 % of round-trip); removes `strip().split("\n")`
  + list build per inbound frame. Helps: every WS/pipe receive.
* **Where:** `wire.py:713 parse_wire_batch` called from
  `rpc_session.py:1312 _handle_frame`. On the `"string"` level a frame is one
  message except inside the HTTP-batch transport; fast-path the single-line case
  (no newline → parse directly).
* **Risk:** Low — HTTP batch is the only multi-message framing (contract
  C-FRAME/D3). Covered by `test_batch*`, `test_wire`.
* **Pure-Python.**

### P6 — Reduce per-call object churn in `send_call`
* **Win:** ~3–8 % of round-trip; fewer allocations (`PropertyKey` list,
  `WirePipeline`, `WirePush`) and a per-call `Serializer` instance.
* **Where:** `rpc_session.py:648-694 send_call` (+ `_serialize_frame`
  `:714`). Reuse a per-session `Serializer`, avoid the `PropertyKey` wrapper for
  the common string path, and build the push list directly.
* **Risk:** Low-Medium — `Serializer` holds per-message `_exports`/`_stream_stubs`
  state that must be reset per call if reused. Covered by `test_rpc_session*`.
* **Pure-Python.**

### P7 — (Optional, high-effort) C-accelerate the codec tree-walk
* **Win:** could close most of the remaining codec gap on tiny/array shapes
  (15–45× → low single digits) where interpreter overhead is the whole story.
* **Where:** a Cython/Rust (`pyo3`) `_serialize_value`/`_parse_value` kernel
  behind the existing pure-Python fallback.
* **Risk:** High — build complexity, must stay byte-identical to golden fixtures,
  new failure surface. Only worth it if P1–P3 don't close enough of the gap.
* **Needs C-accel.**

### P8 — (Design note, not a quick win) asyncio round-trip floor
* **Observation:** ~33 % of in-process round-trip is `kqueue`/scheduler, not
  Cap'n Web. On real network transports this is replaced by socket I/O, so it is
  not "waste" — but for same-process privilege-separation seams a **synchronous
  in-process transport** (direct callback handoff, no `Queue`/loop round-trip)
  could bypass it. Explore only if same-process RPC latency becomes a target.
* **Risk:** High (changes the transport concurrency model). **Pure-Python.**

---

## 8. Verdict

The Python implementation is **correct and competitive on the axes Cap'n Web is
designed around** — promise pipelining (2.54× self-speedup, larger than TS's)
and batch throughput (within ~5× of V8 at 1000 calls) — while paying the
expected CPython interpreter tax on raw single-call and micro-serialize
throughput. The profiler shows the movable Python-side cost is concentrated in a
**redundant decode double-pass, a triplicate tree-walk per call, and
`isinstance`-heavy hot loops** — all pure-Python, wire-compatible fixes (P1–P6)
that should meaningfully close the RPC and codec gaps without touching the
protocol. The largest single gap (byte streaming, ~27×) is a transport-encoding
choice (base64+JSON vs structured clone), addressable on custom transports
(P4). These baseline numbers are the *before*; the optimization pass will be
measured against them with `make bench`.
