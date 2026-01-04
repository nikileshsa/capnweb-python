"""Protocol compliance tests for TypeScript/Python interop.

These tests verify that both implementations adhere to the wire format
specified in protocol.md.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from .conftest import InteropClient, ServerProcess


# =============================================================================
# Array Escaping Tests
# =============================================================================

@pytest.mark.asyncio
class TestArrayEscaping:
    """Test that arrays are properly escaped as [[...]] on the wire."""
    
    async def test_empty_array_roundtrip_ts(self, ts_server: ServerProcess):
        """Empty array round-trips correctly through TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echo", [[]])
            assert result == []
    
    async def test_empty_array_roundtrip_py(self, py_server: ServerProcess):
        """Empty array round-trips correctly through Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echo", [[]])
            assert result == []
    
    async def test_simple_array_roundtrip_ts(self, ts_server: ServerProcess):
        """Simple array round-trips correctly through TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echo", [[1, 2, 3]])
            assert result == [1, 2, 3]
    
    async def test_simple_array_roundtrip_py(self, py_server: ServerProcess):
        """Simple array round-trips correctly through Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echo", [[1, 2, 3]])
            assert result == [1, 2, 3]
    
    async def test_nested_array_roundtrip_ts(self, ts_server: ServerProcess):
        """Nested array round-trips correctly through TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echo", [[[1, 2], [3, 4]]])
            assert result == [[1, 2], [3, 4]]
    
    async def test_nested_array_roundtrip_py(self, py_server: ServerProcess):
        """Nested array round-trips correctly through Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echo", [[[1, 2], [3, 4]]])
            assert result == [[1, 2], [3, 4]]
    
    async def test_array_in_object_ts(self, ts_server: ServerProcess):
        """Array inside object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            obj = {"items": [1, 2, 3], "nested": {"arr": [4, 5]}}
            result = await client.call("echo", [obj])
            assert result == obj
    
    async def test_array_in_object_py(self, py_server: ServerProcess):
        """Array inside object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            obj = {"items": [1, 2, 3], "nested": {"arr": [4, 5]}}
            result = await client.call("echo", [obj])
            assert result == obj


# =============================================================================
# Primitive Type Tests
# =============================================================================

@pytest.mark.asyncio
class TestPrimitiveTypes:
    """Test primitive type serialization/deserialization."""
    
    @pytest.mark.parametrize("value", [
        None,
        True,
        False,
        0,
        1,
        -1,
        42,
        -999999,
        3.14,
        -2.718,
    ])
    async def test_primitive_roundtrip_ts(self, ts_server: ServerProcess, value):
        """Primitive values round-trip correctly through TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echo", [value])
            if value is None:
                assert result is None
            else:
                assert result == value
    
    @pytest.mark.parametrize("value", [
        None,
        True,
        False,
        0,
        1,
        -1,
        42,
        -999999,
        3.14,
        -2.718,
    ])
    async def test_primitive_roundtrip_py(self, py_server: ServerProcess, value):
        """Primitive values round-trip correctly through Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echo", [value])
            if value is None:
                assert result is None
            else:
                assert result == value


# =============================================================================
# String Tests
# =============================================================================

@pytest.mark.asyncio
class TestStrings:
    """Test string serialization including unicode."""
    
    @pytest.mark.parametrize("value", [
        "",
        "hello",
        "Hello, World!",
        "line1\nline2",
        "tab\there",
        "quote\"here",
        "backslash\\here",
        "日本語",
        "中文测试",
        "🎉🚀💻🌍",
        "mixed: 日本語 and emoji 🎉",
        "special: <>&\"'",
    ])
    async def test_string_roundtrip_ts(self, ts_server: ServerProcess, value):
        """String values round-trip correctly through TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echo", [value])
            assert result == value
    
    @pytest.mark.parametrize("value", [
        "",
        "hello",
        "Hello, World!",
        "line1\nline2",
        "tab\there",
        "quote\"here",
        "backslash\\here",
        "日本語",
        "中文测试",
        "🎉🚀💻🌍",
        "mixed: 日本語 and emoji 🎉",
        "special: <>&\"'",
    ])
    async def test_string_roundtrip_py(self, py_server: ServerProcess, value):
        """String values round-trip correctly through Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echo", [value])
            assert result == value


# =============================================================================
# Object Tests
# =============================================================================

@pytest.mark.asyncio
class TestObjects:
    """Test object serialization."""
    
    async def test_empty_object_ts(self, ts_server: ServerProcess):
        """Empty object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echo", [{}])
            assert result == {}
    
    async def test_empty_object_py(self, py_server: ServerProcess):
        """Empty object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echo", [{}])
            assert result == {}
    
    async def test_simple_object_ts(self, ts_server: ServerProcess):
        """Simple object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            obj = {"key": "value", "number": 42}
            result = await client.call("echo", [obj])
            assert result == obj
    
    async def test_simple_object_py(self, py_server: ServerProcess):
        """Simple object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            obj = {"key": "value", "number": 42}
            result = await client.call("echo", [obj])
            assert result == obj
    
    async def test_deeply_nested_object_ts(self, ts_server: ServerProcess):
        """Deeply nested object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            obj = {
                "level1": {
                    "level2": {
                        "level3": {
                            "level4": {
                                "level5": {"value": "deep"}
                            }
                        }
                    }
                }
            }
            result = await client.call("echo", [obj])
            assert result == obj
    
    async def test_deeply_nested_object_py(self, py_server: ServerProcess):
        """Deeply nested object round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            obj = {
                "level1": {
                    "level2": {
                        "level3": {
                            "level4": {
                                "level5": {"value": "deep"}
                            }
                        }
                    }
                }
            }
            result = await client.call("echo", [obj])
            assert result == obj
    
    async def test_complex_mixed_object_ts(self, ts_server: ServerProcess):
        """Complex object with mixed types round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            obj = {
                "string": "hello",
                "number": 42,
                "float": 3.14,
                "bool": True,
                "null": None,
                "array": [1, 2, 3],
                "nested": {"a": 1, "b": [4, 5, 6]},
            }
            result = await client.call("echo", [obj])
            assert result == obj
    
    async def test_complex_mixed_object_py(self, py_server: ServerProcess):
        """Complex object with mixed types round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            obj = {
                "string": "hello",
                "number": 42,
                "float": 3.14,
                "bool": True,
                "null": None,
                "array": [1, 2, 3],
                "nested": {"a": 1, "b": [4, 5, 6]},
            }
            result = await client.call("echo", [obj])
            assert result == obj


# =============================================================================
# Method Call Tests
# =============================================================================

@pytest.mark.asyncio
class TestMethodCalls:
    """Test various method call patterns."""
    
    async def test_no_args_ts(self, ts_server: ServerProcess):
        """Method with no arguments works."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("returnNull", [])
            assert result is None
    
    async def test_no_args_py(self, py_server: ServerProcess):
        """Method with no arguments works."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("returnNull", [])
            assert result is None
    
    async def test_single_arg_ts(self, ts_server: ServerProcess):
        """Method with single argument works."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("square", [5])
            assert result == 25
    
    async def test_single_arg_py(self, py_server: ServerProcess):
        """Method with single argument works."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("square", [5])
            assert result == 25
    
    async def test_multiple_args_ts(self, ts_server: ServerProcess):
        """Method with multiple arguments works."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("add", [3, 7])
            assert result == 10
    
    async def test_multiple_args_py(self, py_server: ServerProcess):
        """Method with multiple arguments works."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("add", [3, 7])
            assert result == 10
    
    async def test_array_result_ts(self, ts_server: ServerProcess):
        """Method returning array works."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("generateFibonacci", [10])
            assert result == [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
    
    async def test_array_result_py(self, py_server: ServerProcess):
        """Method returning array works."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("generateFibonacci", [10])
            assert result == [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]


# =============================================================================
# Concurrent Call Tests
# =============================================================================

@pytest.mark.asyncio
class TestConcurrentCalls:
    """Test concurrent RPC calls."""
    
    async def test_concurrent_calls_ts(self, ts_server: ServerProcess):
        """Multiple concurrent calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            tasks = [
                client.call("square", [i])
                for i in range(10)
            ]
            results = await asyncio.gather(*tasks)
            expected = [i * i for i in range(10)]
            assert results == expected
    
    async def test_concurrent_calls_py(self, py_server: ServerProcess):
        """Multiple concurrent calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            tasks = [
                client.call("square", [i])
                for i in range(10)
            ]
            results = await asyncio.gather(*tasks)
            expected = [i * i for i in range(10)]
            assert results == expected
    
    async def test_many_sequential_calls_ts(self, ts_server: ServerProcess):
        """Many sequential calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            for i in range(50):
                result = await client.call("square", [i])
                assert result == i * i
    
    async def test_many_sequential_calls_py(self, py_server: ServerProcess):
        """Many sequential calls complete correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            for i in range(50):
                result = await client.call("square", [i])
                assert result == i * i


# =============================================================================
# Cross-Language Specific Tests
# =============================================================================

@pytest.mark.asyncio
class TestCrossLanguage:
    """Tests specifically for cross-language interop."""
    
    async def test_py_client_ts_server_all_types(self, ts_server: ServerProcess):
        """Python client can send/receive all types to TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Primitives
            assert await client.call("echo", [None]) is None
            assert await client.call("echo", [True]) is True
            assert await client.call("echo", [42]) == 42
            assert await client.call("echo", [3.14]) == 3.14
            assert await client.call("echo", ["hello"]) == "hello"
            
            # Arrays
            assert await client.call("echo", [[]]) == []
            assert await client.call("echo", [[1, 2, 3]]) == [1, 2, 3]
            
            # Objects
            assert await client.call("echo", [{}]) == {}
            obj = {"a": 1, "b": [2, 3]}
            assert await client.call("echo", [obj]) == obj
    
    async def test_py_client_py_server_all_types(self, py_server: ServerProcess):
        """Python client can send/receive all types to Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            # Primitives
            assert await client.call("echo", [None]) is None
            assert await client.call("echo", [True]) is True
            assert await client.call("echo", [42]) == 42
            assert await client.call("echo", [3.14]) == 3.14
            assert await client.call("echo", ["hello"]) == "hello"
            
            # Arrays
            assert await client.call("echo", [[]]) == []
            assert await client.call("echo", [[1, 2, 3]]) == [1, 2, 3]
            
            # Objects
            assert await client.call("echo", [{}]) == {}
            obj = {"a": 1, "b": [2, 3]}
            assert await client.call("echo", [obj]) == obj
