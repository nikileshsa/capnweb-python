"""Edge case tests for TypeScript/Python interop.

These tests verify handling of unusual but valid inputs including:
- Large payloads
- Deeply nested structures
- Empty values
- Special characters
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from .conftest import InteropClient, ServerProcess


# =============================================================================
# Empty Value Tests
# =============================================================================

@pytest.mark.asyncio
class TestEmptyValues:
    """Test handling of empty values."""
    
    async def test_empty_string_ts(self, ts_server: ServerProcess):
        """Empty string round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            result = await client.call("echo", [""])
            assert result == ""
    
    async def test_empty_string_py(self, py_server: ServerProcess):
        """Empty string round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result = await client.call("echo", [""])
            assert result == ""
    
    async def test_empty_array_ts(self, ts_server: ServerProcess):
        """Empty array round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            result = await client.call("echo", [[]])
            assert result == []
    
    async def test_empty_array_py(self, py_server: ServerProcess):
        """Empty array round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result = await client.call("echo", [[]])
            assert result == []
    
    async def test_empty_object_ts(self, ts_server: ServerProcess):
        """Empty object round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            result = await client.call("echo", [{}])
            assert result == {}
    
    async def test_empty_object_py(self, py_server: ServerProcess):
        """Empty object round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result = await client.call("echo", [{}])
            assert result == {}
    
    async def test_null_ts(self, ts_server: ServerProcess):
        """Null value round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            result = await client.call("echo", [None])
            assert result is None
    
    async def test_null_py(self, py_server: ServerProcess):
        """Null value round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result = await client.call("echo", [None])
            assert result is None


# =============================================================================
# Large Payload Tests
# =============================================================================

@pytest.mark.asyncio
class TestLargePayloads:
    """Test handling of large payloads."""
    
    async def test_large_string_ts(self, ts_server: ServerProcess):
        """Large string (10KB) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            large_string = "x" * 10000
            result = await client.call("echo", [large_string])
            assert result == large_string
    
    async def test_large_string_py(self, py_server: ServerProcess):
        """Large string (10KB) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            large_string = "x" * 10000
            result = await client.call("echo", [large_string])
            assert result == large_string
    
    async def test_large_array_ts(self, ts_server: ServerProcess):
        """Large array (1000 elements) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            large_array = list(range(1000))
            result = await client.call("echo", [large_array])
            assert result == large_array
    
    async def test_large_array_py(self, py_server: ServerProcess):
        """Large array (1000 elements) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            large_array = list(range(1000))
            result = await client.call("echo", [large_array])
            assert result == large_array
    
    async def test_large_object_ts(self, ts_server: ServerProcess):
        """Large object (100 keys) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            large_object = {f"key_{i}": i for i in range(100)}
            result = await client.call("echo", [large_object])
            assert result == large_object
    
    async def test_large_object_py(self, py_server: ServerProcess):
        """Large object (100 keys) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            large_object = {f"key_{i}": i for i in range(100)}
            result = await client.call("echo", [large_object])
            assert result == large_object


# =============================================================================
# Deeply Nested Structure Tests
# =============================================================================

@pytest.mark.asyncio
class TestDeeplyNested:
    """Test handling of deeply nested structures."""
    
    async def test_deeply_nested_object_ts(self, ts_server: ServerProcess):
        """Deeply nested object (10 levels) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            # Build nested object
            obj: dict = {"value": "deep"}
            for i in range(10):
                obj = {f"level_{i}": obj}
            
            result = await client.call("echo", [obj])
            assert result == obj
    
    async def test_deeply_nested_object_py(self, py_server: ServerProcess):
        """Deeply nested object (10 levels) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            obj: dict = {"value": "deep"}
            for i in range(10):
                obj = {f"level_{i}": obj}
            
            result = await client.call("echo", [obj])
            assert result == obj
    
    async def test_deeply_nested_array_ts(self, ts_server: ServerProcess):
        """Deeply nested array (10 levels) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            arr: list = ["deep"]
            for _ in range(10):
                arr = [arr]
            
            result = await client.call("echo", [arr])
            assert result == arr
    
    async def test_deeply_nested_array_py(self, py_server: ServerProcess):
        """Deeply nested array (10 levels) round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            arr: list = ["deep"]
            for _ in range(10):
                arr = [arr]
            
            result = await client.call("echo", [arr])
            assert result == arr


# =============================================================================
# Special Character Tests
# =============================================================================

@pytest.mark.asyncio
class TestSpecialCharacters:
    """Test handling of special characters."""
    
    @pytest.mark.parametrize("char,name", [
        ("\n", "newline"),
        ("\t", "tab"),
        ("\r", "carriage return"),
        ("\"", "double quote"),
        ("'", "single quote"),
        ("\\", "backslash"),
        ("/", "forward slash"),
        ("<", "less than"),
        (">", "greater than"),
        ("&", "ampersand"),
        ("\x00", "null byte"),
    ])
    async def test_special_char_ts(self, ts_server: ServerProcess, char: str, name: str):
        """Special character round-trips correctly through TypeScript server."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            test_string = f"before{char}after"
            result = await client.call("echo", [test_string])
            assert result == test_string, f"Failed for {name}"
    
    @pytest.mark.parametrize("char,name", [
        ("\n", "newline"),
        ("\t", "tab"),
        ("\r", "carriage return"),
        ("\"", "double quote"),
        ("'", "single quote"),
        ("\\", "backslash"),
        ("/", "forward slash"),
        ("<", "less than"),
        (">", "greater than"),
        ("&", "ampersand"),
        ("\x00", "null byte"),
    ])
    async def test_special_char_py(self, py_server: ServerProcess, char: str, name: str):
        """Special character round-trips correctly through Python server."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            test_string = f"before{char}after"
            result = await client.call("echo", [test_string])
            assert result == test_string, f"Failed for {name}"


# =============================================================================
# Unicode Tests
# =============================================================================

@pytest.mark.asyncio
class TestUnicode:
    """Test handling of unicode characters."""
    
    @pytest.mark.parametrize("text,name", [
        ("日本語", "Japanese"),
        ("中文", "Chinese"),
        ("한국어", "Korean"),
        ("العربية", "Arabic"),
        ("עברית", "Hebrew"),
        ("Ελληνικά", "Greek"),
        ("Русский", "Russian"),
        ("🎉🚀💻🌍", "Emoji"),
        ("👨‍👩‍👧‍👦", "Family emoji"),
        ("🏳️‍🌈", "Rainbow flag"),
    ])
    async def test_unicode_ts(self, ts_server: ServerProcess, text: str, name: str):
        """Unicode text round-trips correctly through TypeScript server."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            result = await client.call("echo", [text])
            assert result == text, f"Failed for {name}"
    
    @pytest.mark.parametrize("text,name", [
        ("日本語", "Japanese"),
        ("中文", "Chinese"),
        ("한국어", "Korean"),
        ("العربية", "Arabic"),
        ("עברית", "Hebrew"),
        ("Ελληνικά", "Greek"),
        ("Русский", "Russian"),
        ("🎉🚀💻🌍", "Emoji"),
        ("👨‍👩‍👧‍👦", "Family emoji"),
        ("🏳️‍🌈", "Rainbow flag"),
    ])
    async def test_unicode_py(self, py_server: ServerProcess, text: str, name: str):
        """Unicode text round-trips correctly through Python server."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result = await client.call("echo", [text])
            assert result == text, f"Failed for {name}"


# =============================================================================
# Numeric Edge Cases
# =============================================================================

@pytest.mark.asyncio
class TestNumericEdgeCases:
    """Test handling of numeric edge cases."""
    
    @pytest.mark.parametrize("value,name", [
        (0, "zero"),
        (-0.0, "negative zero"),
        (1e10, "large positive"),
        (-1e10, "large negative"),
        (1e-10, "small positive"),
        (-1e-10, "small negative"),
        (2**31 - 1, "max int32"),
        (-(2**31), "min int32"),
        (2**53 - 1, "max safe integer"),
        (-(2**53 - 1), "min safe integer"),
    ])
    async def test_numeric_ts(self, ts_server: ServerProcess, value, name: str):
        """Numeric value round-trips correctly through TypeScript server."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            result = await client.call("echo", [value])
            assert result == value, f"Failed for {name}"
    
    @pytest.mark.parametrize("value,name", [
        (0, "zero"),
        (-0.0, "negative zero"),
        (1e10, "large positive"),
        (-1e10, "large negative"),
        (1e-10, "small positive"),
        (-1e-10, "small negative"),
        (2**31 - 1, "max int32"),
        (-(2**31), "min int32"),
        (2**53 - 1, "max safe integer"),
        (-(2**53 - 1), "min safe integer"),
    ])
    async def test_numeric_py(self, py_server: ServerProcess, value, name: str):
        """Numeric value round-trips correctly through Python server."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result = await client.call("echo", [value])
            assert result == value, f"Failed for {name}"


# =============================================================================
# Boolean Edge Cases
# =============================================================================

@pytest.mark.asyncio
class TestBooleanEdgeCases:
    """Test handling of boolean values."""
    
    async def test_true_ts(self, ts_server: ServerProcess):
        """True value round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            result = await client.call("echo", [True])
            assert result is True
    
    async def test_true_py(self, py_server: ServerProcess):
        """True value round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result = await client.call("echo", [True])
            assert result is True
    
    async def test_false_ts(self, ts_server: ServerProcess):
        """False value round-trips correctly."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            result = await client.call("echo", [False])
            assert result is False
    
    async def test_false_py(self, py_server: ServerProcess):
        """False value round-trips correctly."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result = await client.call("echo", [False])
            assert result is False
    
    async def test_boolean_not_confused_with_int_ts(self, ts_server: ServerProcess):
        """Boolean values are not confused with integers."""
        async with InteropClient(f"ws://localhost:{ts_server.port}/") as client:
            # In some languages, True == 1 and False == 0
            # Make sure we preserve the type
            result_true = await client.call("echo", [True])
            result_one = await client.call("echo", [1])
            result_false = await client.call("echo", [False])
            result_zero = await client.call("echo", [0])
            
            assert result_true is True
            assert result_one == 1
            assert result_false is False
            assert result_zero == 0
    
    async def test_boolean_not_confused_with_int_py(self, py_server: ServerProcess):
        """Boolean values are not confused with integers."""
        async with InteropClient(f"ws://localhost:{py_server.port}/rpc") as client:
            result_true = await client.call("echo", [True])
            result_one = await client.call("echo", [1])
            result_false = await client.call("echo", [False])
            result_zero = await client.call("echo", [0])
            
            assert result_true is True
            assert result_one == 1
            assert result_false is False
            assert result_zero == 0
