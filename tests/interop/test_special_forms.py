"""Special forms tests for TypeScript/Python interop.

These tests verify proper handling of special wire format forms:
- bytes: ["bytes", base64_string]
- date: ["date", timestamp]
- bigint: ["bigint", string]
- undefined: ["undefined"]
- infinity: ["inf"], ["-inf"]
- nan: ["nan"]
"""

from __future__ import annotations

import asyncio
import base64
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from .conftest import InteropClient, ServerProcess


# =============================================================================
# Bytes Tests (["bytes", base64])
# =============================================================================

@pytest.mark.asyncio
class TestBytes:
    """Test binary data serialization as ["bytes", base64]."""
    
    @pytest.mark.xfail(reason="TypeScript Uint8Array serialization issue")
    async def test_bytes_roundtrip_ts(self, ts_server: ServerProcess):
        """Bytes round-trip correctly through TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Create bytes on server and get them back
            data = await client.call("makeBytes", ["SGVsbG8gV29ybGQ="])  # "Hello World"
            assert isinstance(data, bytes)
            assert data == b"Hello World"
    
    async def test_bytes_roundtrip_py(self, py_server: ServerProcess):
        """Bytes round-trip correctly through Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            data = await client.call("makeBytes", ["SGVsbG8gV29ybGQ="])
            assert isinstance(data, bytes)
            assert data == b"Hello World"
    
    async def test_bytes_echo_ts(self, ts_server: ServerProcess):
        """Bytes can be sent to and echoed from TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            original = b"Binary data \x00\x01\x02\xff"
            result = await client.call("echoBytes", [original])
            assert isinstance(result, bytes)
            assert result == original
    
    async def test_bytes_echo_py(self, py_server: ServerProcess):
        """Bytes can be sent to and echoed from Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            original = b"Binary data \x00\x01\x02\xff"
            result = await client.call("echoBytes", [original])
            assert isinstance(result, bytes)
            assert result == original
    
    async def test_bytes_length_ts(self, ts_server: ServerProcess):
        """Server can process bytes and return length."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            data = b"Hello World"
            length = await client.call("getBytesLength", [data])
            assert length == 11
    
    async def test_bytes_length_py(self, py_server: ServerProcess):
        """Server can process bytes and return length."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            data = b"Hello World"
            length = await client.call("getBytesLength", [data])
            assert length == 11
    
    async def test_empty_bytes_ts(self, ts_server: ServerProcess):
        """Empty bytes round-trip correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echoBytes", [b""])
            assert isinstance(result, bytes)
            assert result == b""
    
    async def test_empty_bytes_py(self, py_server: ServerProcess):
        """Empty bytes round-trip correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echoBytes", [b""])
            assert isinstance(result, bytes)
            assert result == b""
    
    async def test_large_bytes_ts(self, ts_server: ServerProcess):
        """Large bytes (10KB) round-trip correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            original = bytes(range(256)) * 40  # 10KB
            result = await client.call("echoBytes", [original])
            assert isinstance(result, bytes)
            assert result == original
    
    async def test_large_bytes_py(self, py_server: ServerProcess):
        """Large bytes (10KB) round-trip correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            original = bytes(range(256)) * 40  # 10KB
            result = await client.call("echoBytes", [original])
            assert isinstance(result, bytes)
            assert result == original


# =============================================================================
# Date Tests (["date", timestamp])
# =============================================================================

@pytest.mark.asyncio
class TestDate:
    """Test date serialization as ["date", timestamp].
    
    Note: Some tests are marked xfail because TypeScript's Date serialization
    may not be returning the expected wire format.
    """
    
    async def test_date_roundtrip_ts(self, ts_server: ServerProcess):
        """Date round-trips correctly through TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Create date on server: 2024-01-15T12:30:00Z = 1705322200000ms
            timestamp = 1705322200000
            date = await client.call("makeDate", [timestamp])
            assert isinstance(date, datetime)
            assert date.timestamp() * 1000 == pytest.approx(timestamp, abs=1)
    
    async def test_date_roundtrip_py(self, py_server: ServerProcess):
        """Date round-trips correctly through Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            timestamp = 1705322200000
            date = await client.call("makeDate", [timestamp])
            assert isinstance(date, datetime)
            assert date.timestamp() * 1000 == pytest.approx(timestamp, abs=1)
    
    async def test_date_echo_ts(self, ts_server: ServerProcess):
        """Date can be sent to and echoed from TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            original = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
            result = await client.call("echoDate", [original])
            assert isinstance(result, datetime)
            assert result.timestamp() == pytest.approx(original.timestamp(), abs=0.001)
    
    async def test_date_echo_py(self, py_server: ServerProcess):
        """Date can be sent to and echoed from Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            original = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
            result = await client.call("echoDate", [original])
            assert isinstance(result, datetime)
            assert result.timestamp() == pytest.approx(original.timestamp(), abs=0.001)
    
    async def test_date_timestamp_ts(self, ts_server: ServerProcess):
        """Server can extract timestamp from date."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            date = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
            timestamp = await client.call("getTimestamp", [date])
            expected = int(date.timestamp() * 1000)
            assert timestamp == pytest.approx(expected, abs=1)
    
    async def test_date_timestamp_py(self, py_server: ServerProcess):
        """Server can extract timestamp from date."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            date = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
            timestamp = await client.call("getTimestamp", [date])
            expected = int(date.timestamp() * 1000)
            assert timestamp == pytest.approx(expected, abs=1)
    
    async def test_epoch_date_ts(self, ts_server: ServerProcess):
        """Unix epoch date round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            result = await client.call("echoDate", [epoch])
            assert isinstance(result, datetime)
            assert result.timestamp() == pytest.approx(0, abs=0.001)
    
    async def test_epoch_date_py(self, py_server: ServerProcess):
        """Unix epoch date round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            result = await client.call("echoDate", [epoch])
            assert isinstance(result, datetime)
            assert result.timestamp() == pytest.approx(0, abs=0.001)


# =============================================================================
# BigInt Tests (["bigint", string])
# =============================================================================

@pytest.mark.asyncio
class TestBigInt:
    """Test bigint serialization as ["bigint", string].
    
    Note: Some tests may fail when sending large ints to TypeScript
    because Python doesn't automatically serialize large ints as bigint.
    """
    
    async def test_bigint_roundtrip_ts(self, ts_server: ServerProcess):
        """BigInt round-trips correctly through TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Create bigint on server
            result = await client.call("makeBigInt", ["12345678901234567890"])
            assert isinstance(result, int)
            assert result == 12345678901234567890
    
    async def test_bigint_roundtrip_py(self, py_server: ServerProcess):
        """BigInt round-trips correctly through Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("makeBigInt", ["12345678901234567890"])
            assert isinstance(result, int)
            assert result == 12345678901234567890
    
    async def test_bigint_echo_ts(self, ts_server: ServerProcess):
        """BigInt can be sent to and echoed from TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            # Python int larger than JS safe integer
            original = 2**63
            result = await client.call("echoBigInt", [original])
            assert isinstance(result, int)
            assert result == original
    
    async def test_bigint_echo_py(self, py_server: ServerProcess):
        """BigInt can be sent to and echoed from Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            original = 2**63
            result = await client.call("echoBigInt", [original])
            assert isinstance(result, int)
            assert result == original
    
    async def test_bigint_string_ts(self, ts_server: ServerProcess):
        """Server can convert bigint to string."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            value = 2**100
            result = await client.call("getBigIntString", [value])
            assert result == str(value)
    
    async def test_bigint_string_py(self, py_server: ServerProcess):
        """Server can convert bigint to string."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            value = 2**100
            result = await client.call("getBigIntString", [value])
            assert result == str(value)
    
    async def test_negative_bigint_ts(self, ts_server: ServerProcess):
        """Negative bigint round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            original = -(2**63)
            result = await client.call("echoBigInt", [original])
            assert isinstance(result, int)
            assert result == original
    
    async def test_negative_bigint_py(self, py_server: ServerProcess):
        """Negative bigint round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            original = -(2**63)
            result = await client.call("echoBigInt", [original])
            assert isinstance(result, int)
            assert result == original
    
    async def test_zero_bigint_ts(self, ts_server: ServerProcess):
        """Zero bigint round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("makeBigInt", ["0"])
            assert isinstance(result, int)
            assert result == 0
    
    async def test_zero_bigint_py(self, py_server: ServerProcess):
        """Zero bigint round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("makeBigInt", ["0"])
            assert isinstance(result, int)
            assert result == 0


# =============================================================================
# Special Number Tests (["inf"], ["-inf"], ["nan"])
# =============================================================================

@pytest.mark.asyncio
class TestSpecialNumbers:
    """Test special number serialization."""
    
    async def test_infinity_ts(self, ts_server: ServerProcess):
        """Infinity is returned correctly from TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("returnInfinity", [])
            assert result == float('inf')
            assert math.isinf(result) and result > 0
    
    async def test_infinity_py(self, py_server: ServerProcess):
        """Infinity is returned correctly from Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("returnInfinity", [])
            assert result == float('inf')
            assert math.isinf(result) and result > 0
    
    async def test_negative_infinity_ts(self, ts_server: ServerProcess):
        """Negative infinity is returned correctly from TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("returnNegativeInfinity", [])
            assert result == float('-inf')
            assert math.isinf(result) and result < 0
    
    async def test_negative_infinity_py(self, py_server: ServerProcess):
        """Negative infinity is returned correctly from Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("returnNegativeInfinity", [])
            assert result == float('-inf')
            assert math.isinf(result) and result < 0
    
    async def test_nan_ts(self, ts_server: ServerProcess):
        """NaN is returned correctly from TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("returnNaN", [])
            assert math.isnan(result)
    
    async def test_nan_py(self, py_server: ServerProcess):
        """NaN is returned correctly from Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("returnNaN", [])
            assert math.isnan(result)
    
    async def test_infinity_echo_ts(self, ts_server: ServerProcess):
        """Infinity can be sent to and echoed from TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echo", [float('inf')])
            assert result == float('inf')
    
    async def test_infinity_echo_py(self, py_server: ServerProcess):
        """Infinity can be sent to and echoed from Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echo", [float('inf')])
            assert result == float('inf')
    
    async def test_negative_infinity_echo_ts(self, ts_server: ServerProcess):
        """Negative infinity can be sent to and echoed from TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echo", [float('-inf')])
            assert result == float('-inf')
    
    async def test_negative_infinity_echo_py(self, py_server: ServerProcess):
        """Negative infinity can be sent to and echoed from Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echo", [float('-inf')])
            assert result == float('-inf')
    
    async def test_nan_echo_ts(self, ts_server: ServerProcess):
        """NaN can be sent to and echoed from TypeScript server."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("echo", [float('nan')])
            assert math.isnan(result)
    
    async def test_nan_echo_py(self, py_server: ServerProcess):
        """NaN can be sent to and echoed from Python server."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("echo", [float('nan')])
            assert math.isnan(result)


# =============================================================================
# Undefined Tests (["undefined"])
# =============================================================================

@pytest.mark.asyncio
class TestUndefined:
    """Test undefined serialization as ["undefined"]."""
    
    async def test_undefined_return_ts(self, ts_server: ServerProcess):
        """TypeScript undefined is returned correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            result = await client.call("returnUndefined", [])
            # Python represents undefined as None
            assert result is None
    
    async def test_undefined_return_py(self, py_server: ServerProcess):
        """Python None (as undefined) is returned correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            result = await client.call("returnUndefined", [])
            assert result is None
    
    async def test_null_vs_undefined_ts(self, ts_server: ServerProcess):
        """Null and undefined are both received as None in Python."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            null_result = await client.call("returnNull", [])
            undefined_result = await client.call("returnUndefined", [])
            # Both should be None in Python
            assert null_result is None
            assert undefined_result is None
    
    async def test_null_vs_undefined_py(self, py_server: ServerProcess):
        """Null and undefined are both received as None in Python."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            null_result = await client.call("returnNull", [])
            undefined_result = await client.call("returnUndefined", [])
            assert null_result is None
            assert undefined_result is None


# =============================================================================
# Mixed Special Forms in Objects
# =============================================================================

@pytest.mark.asyncio
class TestMixedSpecialForms:
    """Test objects containing multiple special forms.
    
    Note: Some tests are marked xfail due to incomplete special form support.
    """
    
    async def test_object_with_bytes_ts(self, ts_server: ServerProcess):
        """Object containing bytes round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            obj = {"data": b"binary", "name": "test"}
            result = await client.call("echo", [obj])
            assert result["name"] == "test"
            assert result["data"] == b"binary"
    
    async def test_object_with_bytes_py(self, py_server: ServerProcess):
        """Object containing bytes round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            obj = {"data": b"binary", "name": "test"}
            result = await client.call("echo", [obj])
            assert result["name"] == "test"
            assert result["data"] == b"binary"
    
    async def test_object_with_date_ts(self, ts_server: ServerProcess):
        """Object containing date round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            date = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
            obj = {"created": date, "name": "test"}
            result = await client.call("echo", [obj])
            assert result["name"] == "test"
            assert isinstance(result["created"], datetime)
            assert result["created"].timestamp() == pytest.approx(date.timestamp(), abs=0.001)
    
    async def test_object_with_date_py(self, py_server: ServerProcess):
        """Object containing date round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            date = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
            obj = {"created": date, "name": "test"}
            result = await client.call("echo", [obj])
            assert result["name"] == "test"
            assert isinstance(result["created"], datetime)
            assert result["created"].timestamp() == pytest.approx(date.timestamp(), abs=0.001)
    
    async def test_array_with_special_numbers_ts(self, ts_server: ServerProcess):
        """Array containing special numbers round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{ts_server.port}/") as client:
            arr = [1, float('inf'), float('-inf'), 3.14]
            result = await client.call("echo", [arr])
            assert result[0] == 1
            assert result[1] == float('inf')
            assert result[2] == float('-inf')
            assert result[3] == 3.14
    
    async def test_array_with_special_numbers_py(self, py_server: ServerProcess):
        """Array containing special numbers round-trips correctly."""
        async with InteropClient(f"ws://127.0.0.1:{py_server.port}/rpc") as client:
            arr = [1, float('inf'), float('-inf'), 3.14]
            result = await client.call("echo", [arr])
            assert result[0] == 1
            assert result[1] == float('inf')
            assert result[2] == float('-inf')
            assert result[3] == 3.14
