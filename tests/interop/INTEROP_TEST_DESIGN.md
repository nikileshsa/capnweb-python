# Exhaustive Interop Test Design

Based on analysis of grpclib, trio, and wtransport test patterns, this document
outlines a comprehensive interop test matrix for TypeScript/Python protocol compliance.

## Test Categories (Inspired by grpclib)

### 1. Protocol Compliance Tests
Tests that verify wire format adherence to `protocol.md`.

| Test | Description | Priority |
|------|-------------|----------|
| `test_array_escaping` | Verify `[[1,2,3]]` wire format for arrays | Critical |
| `test_special_forms` | All special forms: export, import, pipeline, promise, error, bytes, date, bigint | Critical |
| `test_pipeline_args_format` | Args sent un-escaped per TypeScript behavior | Critical |
| `test_message_types` | push, pull, resolve, reject, release, abort | Critical |
| `test_id_sign_conventions` | Export IDs negative, import IDs positive, main=0 | Critical |
| `test_property_path_encoding` | String and integer path keys | High |
| `test_error_serialization` | Error type, message, stack, data fields | High |
| `test_bytes_base64` | Binary data as `["bytes", base64]` | High |
| `test_special_numbers` | `["inf"]`, `["-inf"]`, `["nan"]` | Medium |
| `test_undefined_vs_null` | `["undefined"]` vs `null` | Medium |

### 2. Streaming/Bidirectional Tests (Inspired by grpclib test_functional.py)

| Test | Description | Priority |
|------|-------------|----------|
| `test_unary_call` | Simple request → response | Critical |
| `test_pipelined_calls` | Multiple calls before any resolve | Critical |
| `test_nested_capability_passing` | A calls B, B calls C, C returns to A | High |
| `test_bidirectional_callbacks` | Server calls client, client calls server | High |
| `test_interleaved_messages` | Multiple concurrent calls with interleaved responses | High |
| `test_stream_of_capabilities` | Return array of stubs | Medium |

### 3. Error Handling Tests (Inspired by grpclib test_client_stream.py)

| Test | Description | Priority |
|------|-------------|----------|
| `test_method_not_found` | Call non-existent method | Critical |
| `test_server_throws_error` | Server method raises exception | Critical |
| `test_client_receives_reject` | Verify reject message handling | Critical |
| `test_abort_propagation` | Abort message sent to peer | High |
| `test_error_during_capability_call` | Error in passed capability | High |
| `test_invalid_capability_id` | Reference non-existent export | High |
| `test_malformed_message` | Invalid JSON or structure | Medium |
| `test_unknown_message_type` | Unrecognized top-level message | Medium |

### 4. Timeout/Deadline Tests (Inspired by grpclib test_client_stream.py)

| Test | Description | Priority |
|------|-------------|----------|
| `test_client_timeout` | Client-side timeout on slow server | High |
| `test_server_slow_response` | Server takes too long | High |
| `test_timeout_during_capability_call` | Timeout in callback | Medium |
| `test_deadline_propagation` | Deadline passed through pipeline | Medium |

### 5. Connection Lifecycle Tests (Inspired by grpclib test_protocol.py)

| Test | Description | Priority |
|------|-------------|----------|
| `test_clean_shutdown` | Graceful connection close | Critical |
| `test_abrupt_disconnect` | Connection drops mid-call | Critical |
| `test_reconnect_after_disconnect` | New session after failure | High |
| `test_server_initiated_close` | Server closes connection | High |
| `test_client_initiated_close` | Client closes connection | High |

### 6. Capability Lifecycle Tests (Inspired by grpclib test_stream_release)

| Test | Description | Priority |
|------|-------------|----------|
| `test_release_after_resolve` | Release sent when import resolved | Critical |
| `test_release_on_dispose` | Release sent when stub disposed | Critical |
| `test_refcount_tracking` | Multiple references to same capability | High |
| `test_release_before_resolve` | Cancel pending call | High |
| `test_capability_garbage_collection` | Stubs cleaned up properly | High |
| `test_double_release` | Idempotent release handling | Medium |

### 7. Memory Leak Tests (Inspired by grpclib test_memory.py)

| Test | Description | Priority |
|------|-------------|----------|
| `test_no_leak_simple_call` | No objects retained after call | High |
| `test_no_leak_capability_passing` | Stubs cleaned up | High |
| `test_no_leak_error_path` | No leak on error | High |
| `test_no_leak_concurrent_calls` | Many calls don't leak | Medium |

### 8. Concurrency Tests (Inspired by grpclib test_outbound_streams_limit)

| Test | Description | Priority |
|------|-------------|----------|
| `test_concurrent_calls` | 10+ simultaneous calls | High |
| `test_concurrent_bidirectional` | Both sides calling simultaneously | High |
| `test_call_ordering` | Responses match requests | High |
| `test_high_throughput` | 100+ calls in sequence | Medium |

### 9. Edge Cases (Inspired by trio test_run.py)

| Test | Description | Priority |
|------|-------------|----------|
| `test_empty_args` | Call with no arguments | High |
| `test_null_result` | Method returns null | High |
| `test_undefined_result` | Method returns undefined | High |
| `test_large_payload` | 1MB+ data transfer | Medium |
| `test_deeply_nested_object` | 10+ levels of nesting | Medium |
| `test_unicode_strings` | Non-ASCII characters | Medium |
| `test_empty_array` | `[]` as argument/result | Medium |
| `test_empty_object` | `{}` as argument/result | Medium |
| `test_circular_reference_prevention` | Detect/reject cycles | Medium |

### 10. Cross-Language Specific Tests

| Test | Description | Priority |
|------|-------------|----------|
| `test_py_client_ts_server_all_types` | All data types round-trip | Critical |
| `test_ts_client_py_server_all_types` | All data types round-trip | Critical |
| `test_py_client_ts_server_capability` | Capability passing works | Critical |
| `test_ts_client_py_server_capability` | Capability passing works | Critical |
| `test_py_client_ts_server_callback` | Server calls client back | Critical |
| `test_ts_client_py_server_callback` | Server calls client back | Critical |
| `test_mixed_language_pipeline` | Py→TS→Py or TS→Py→TS | High |

## Test Infrastructure Requirements

### 1. Server Fixtures
```python
@pytest.fixture(scope="module")
def ts_server():
    """Start TypeScript server, yield, cleanup"""
    
@pytest.fixture(scope="module")  
def py_server():
    """Start Python server, yield, cleanup"""
```

### 2. Client Helpers
```python
class InteropClient:
    """Unified client interface for both languages"""
    async def call(method, args) -> result
    async def call_with_timeout(method, args, timeout) -> result
    async def call_expecting_error(method, args) -> error
```

### 3. Test Utilities
```python
def assert_wire_message(transport, expected):
    """Verify exact wire format"""
    
def assert_no_memory_leak(before, after):
    """Compare object counts"""
    
async def wait_for_release(transport, timeout):
    """Wait for release message"""
```

## Implementation Priority

### Phase 1: Critical (Must Have)
- Protocol compliance tests
- Basic error handling
- Capability lifecycle (release)
- Clean shutdown

### Phase 2: High (Should Have)
- Bidirectional callbacks
- Timeout handling
- Concurrency tests
- Memory leak detection

### Phase 3: Medium (Nice to Have)
- Edge cases
- Large payloads
- Stress tests
- Performance benchmarks

## Test File Structure

```
tests/interop/
├── __init__.py
├── conftest.py              # Shared fixtures
├── test_target.py           # TestTarget implementation
├── ts_server.ts             # TypeScript server
├── py_server.py             # Python server
├── ts_client_test.ts        # TypeScript client tests
│
├── test_protocol_compliance.py   # Wire format tests
├── test_capability_lifecycle.py  # Release/refcount tests
├── test_error_handling.py        # Error propagation tests
├── test_bidirectional.py         # Callback tests
├── test_concurrency.py           # Concurrent call tests
├── test_edge_cases.py            # Edge case tests
├── test_memory.py                # Memory leak tests
└── test_stress.py                # Stress/performance tests
```

## Key Insights from Reference Projects

### From grpclib:
1. **Separate protocol tests from functional tests** - Test wire format independently
2. **Use stub transports** - Mock transport layer for unit tests
3. **Test error paths explicitly** - Every error condition has a test
4. **Memory leak detection** - gc.collect() before/after comparison
5. **Timeout tests with safety timeout** - Prevent test hangs

### From trio:
1. **Test cancellation paths** - What happens when operations are cancelled
2. **Test task crash propagation** - Errors propagate correctly
3. **Test interleaving** - Concurrent operations don't interfere
4. **Use weakref for leak detection** - Verify objects are collected

### From wtransport:
1. **Test both stream and datagram modes** - Different transport semantics
2. **Test connection establishment** - Handshake edge cases
3. **Test bidirectional streams** - Both sides can send/receive

## Next Steps

1. Implement `conftest.py` with robust server fixtures
2. Implement `test_protocol_compliance.py` (Phase 1)
3. Implement `test_capability_lifecycle.py` (Phase 1)
4. Implement `test_error_handling.py` (Phase 1)
5. Run tests, fix any protocol mismatches found
6. Proceed to Phase 2 tests
