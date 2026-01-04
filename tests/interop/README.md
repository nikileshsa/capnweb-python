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
- `package.json` - Node.js dependencies
- `tsconfig.json` - TypeScript configuration
