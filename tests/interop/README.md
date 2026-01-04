# Cap'n Web Interop Tests

This directory contains exhaustive interop tests between TypeScript and Python
implementations of the Cap'n Web RPC protocol.

## Test Matrix

| Client | Server | Transport | Status |
|--------|--------|-----------|--------|
| Python | TypeScript | WebSocket | ✓ |
| Python | TypeScript | HTTP Batch | Planned |
| Python | Python | WebSocket | ✓ (baseline) |
| TypeScript | Python | WebSocket | ✓ |
| TypeScript | Python | HTTP Batch | Planned |

## Test Scenarios

1. **Simple calls**: `square`, `add`, `greet`
2. **Arrays**: `generateFibonacci`, `getList`
3. **Capability passing**: `makeCounter`, `incrementCounter`
4. **Callbacks**: `registerCallback`, `triggerCallback`
5. **Errors**: `throwError`
6. **Special values**: `null`, `undefined`, numbers
7. **Nested objects**: echo with complex structures

---

## Long-Running Protocol Compatibility Test

### Intent

The `test_long_running.py` script provides a **5-minute continuous stress test** that
validates protocol compatibility between Python and TypeScript implementations. Unlike
quick unit tests, this test runs sustained bidirectional RPC traffic to catch:

- **Memory leaks** in capability management
- **Connection stability** over extended periods
- **Wire format edge cases** that only appear under load
- **Capability lifecycle issues** (creation, passing, release)

### What It Tests

#### 1. Wire Format Compatibility
Cycles through test cases verifying both implementations serialize/deserialize:
- Primitives: `null`, `true`, `false`, integers, floats, strings
- Collections: arrays, objects, nested structures, empty containers
- Mixed types: `[1, "two", 3.0, null]`

#### 2. Capability-Based Security Features
- **Capability creation**: Server creates `Counter` objects, returns them to client
- **Capability passing**: Client receives capability stubs it can call
- **Bidirectional callbacks**: Client registers callback, server invokes it

#### 3. Continuous Bidirectional RPC
- ~20 RPC calls/second for 5 minutes = ~6,000+ total calls
- Server→client callbacks every 10 calls
- Progress reports every 30 seconds

### Running the Test

```bash
# Install TypeScript dependencies first
cd tests/interop && npm install && cd ../..

# Quick test (30 seconds) - Python client -> Python server
uv run python tests/interop/test_long_running.py --duration 30 --server py --client py

# Quick test - Python client -> TypeScript server
uv run python tests/interop/test_long_running.py --duration 30 --server ts --client py

# Quick test - TypeScript client -> Python server
uv run python tests/interop/test_long_running.py --duration 30 --server py --client ts

# Full 5-minute test with all combinations
uv run python tests/interop/test_long_running.py --duration 300 --server both --client both
```

### Test Matrix

| Client | Server | Command |
|--------|--------|---------|
| Python | Python | `--server py --client py` |
| Python | TypeScript | `--server ts --client py` |
| TypeScript | Python | `--server py --client ts` |

### Expected Output

```
============================================================
Protocol Compatibility Test: Python client <-> TypeScript server
Duration: 300s
Server URL: ws://127.0.0.1:57305/
============================================================

[TypeScript] Callback registered with server
[Python->TypeScript] 30s: calls=580, callbacks=58, passed=580, failed=0
[Python->TypeScript] 60s: calls=1160, callbacks=116, passed=1160, failed=0
...

============================================================
Protocol Compatibility Test Complete: Python <-> TypeScript
  Duration: 300.0s
  Total calls: 5800
  Server callbacks: 580
  Protocol tests passed: 5800
  Protocol tests failed: 0
  Capability creates: 1160
  Capability calls: 1160
  Errors: 0
============================================================
```

### Success Criteria

- **Protocol tests failed: 0** - All wire format tests pass
- **Errors: 0** (or < 1% error rate for network issues)
- No hangs or crashes over the full duration

---

## Setup

### 1. Install TypeScript dependencies

```bash
cd tests/interop
npm install
```

### 2. Build capnweb (if not already built)

```bash
cd ../../../capnweb
npm install
npm run build
```

## Running Tests

### Run all interop tests

```bash
cd py-capnweb
uv run pytest tests/interop/ -v
```

### Run Python client → TypeScript server tests only

```bash
uv run pytest tests/interop/test_interop.py::TestPyClientTsServer -v
```

### Run TypeScript client → Python server tests only

```bash
uv run pytest tests/interop/test_interop.py::TestTsClientPyServer -v
```

### Manual testing

Start TypeScript server:
```bash
cd tests/interop
npx tsx ts_server.ts 9100
```

Start Python server:
```bash
cd tests/interop
python py_server.py 9200
```

Run TypeScript client against Python server:
```bash
cd tests/interop
npx tsx ts_client_test.ts 9200
```

## Protocol Compliance

These tests verify that both implementations:

1. **Wire format**: Arrays are escaped as `[[...]]`, special forms are validated
2. **Message types**: `push`, `pull`, `resolve`, `reject`, `release`, `abort`
3. **Capability passing**: Stubs serialize as `["export", id]`, parse as imports
4. **ID conventions**: Export IDs are negative, import IDs are positive, main is 0
5. **Error handling**: Errors serialize as `["error", type, message]`
6. **Release semantics**: `release` sent when imports are no longer needed

## Files

- `test_target.py` - Python TestTarget implementation
- `ts_server.ts` - TypeScript server with TestTarget
- `py_server.py` - Python server with TestTarget
- `ts_client_test.ts` - TypeScript client test script
- `test_interop.py` - Main pytest test file
- `test_long_running.py` - Long-running protocol compatibility test
- `package.json` - Node.js dependencies
- `tsconfig.json` - TypeScript configuration
