"""Tests for wire protocol implementation."""

import math

import pytest

from capnweb.wire import (
    PropertyKey,
    WireDate,
    WireError,
    WireExport,
    WireImport,
    WireMessage,
    WirePipeline,
    WirePromise,
    WirePull,
    WirePush,
    WireReject,
    WireRelease,
    WireResolve,
    parse_wire_batch,
    parse_wire_message,
    serialize_wire_batch,
    wire_expression_to_json,
)


class TestPropertyKey:
    """Tests for PropertyKey."""

    def test_string_key(self) -> None:
        """Test string property key."""
        key = PropertyKey("name")
        assert key.value == "name"
        assert key.to_json() == "name"

    def test_numeric_key(self) -> None:
        """Test numeric property key."""
        key = PropertyKey(42)
        assert key.value == 42
        assert key.to_json() == 42

    def test_from_json_string(self) -> None:
        """Test parsing string key from JSON."""
        key = PropertyKey.from_json("name")
        assert key.value == "name"

    def test_from_json_number(self) -> None:
        """Test parsing numeric key from JSON."""
        key = PropertyKey.from_json(42)
        assert key.value == 42

    def test_from_json_invalid(self) -> None:
        """Test parsing invalid key raises error."""
        with pytest.raises(ValueError):
            PropertyKey.from_json([1, 2, 3])


class TestWireExpressions:
    """Tests for wire expression types."""

    def test_wire_error(self) -> None:
        """Test WireError serialization."""
        error = WireError("TypeError", "Invalid type")
        json_arr = error.to_json()
        assert json_arr == ["error", "TypeError", "Invalid type"]

        # Test with stack
        error_with_stack = WireError("TypeError", "Invalid type", "stack trace")
        json_arr = error_with_stack.to_json()
        assert json_arr == ["error", "TypeError", "Invalid type", "stack trace"]

    def test_wire_import(self) -> None:
        """Test WireImport serialization."""
        import_expr = WireImport(42)
        json_arr = import_expr.to_json()
        assert json_arr == ["import", 42]

    def test_wire_export(self) -> None:
        """Test WireExport serialization."""
        export_expr = WireExport(-5)
        json_arr = export_expr.to_json()
        assert json_arr == ["export", -5]

    def test_wire_promise(self) -> None:
        """Test WirePromise serialization."""
        promise_expr = WirePromise(-3)
        json_arr = promise_expr.to_json()
        assert json_arr == ["promise", -3]

    def test_wire_date(self) -> None:
        """Test WireDate serialization."""
        date_expr = WireDate(1749342170815.0)
        json_arr = date_expr.to_json()
        assert json_arr == ["date", 1749342170815.0]

    def test_wire_pipeline(self) -> None:
        """Test WirePipeline serialization."""
        pipeline = WirePipeline(1, None, None)
        json_arr = pipeline.to_json()
        assert json_arr[0] == "pipeline"
        assert json_arr[1] == 1

        # Test with property path
        pipeline_with_path = WirePipeline(
            1, [PropertyKey("user"), PropertyKey("profile")], None
        )
        json_arr = pipeline_with_path.to_json()
        assert json_arr[2] == ["user", "profile"]


class TestWireMessages:
    """Tests for wire message types."""

    def test_wire_push(self) -> None:
        """Test WirePush message."""
        push = WirePush("test value")
        json_arr = push.to_json()
        assert json_arr == ["push", "test value"]

    def test_wire_pull(self) -> None:
        """Test WirePull message."""
        pull = WirePull(42)
        json_arr = pull.to_json()
        assert json_arr == ["pull", 42]

    def test_wire_resolve(self) -> None:
        """Test WireResolve message."""
        resolve = WireResolve(-5, "result")
        json_arr = resolve.to_json()
        assert json_arr == ["resolve", -5, "result"]

    def test_wire_reject(self) -> None:
        """Test WireReject message."""
        error = WireError("Error", "Failed")
        reject = WireReject(-5, error)
        json_arr = reject.to_json()
        assert json_arr[0] == "reject"
        assert json_arr[1] == -5

    def test_wire_release(self) -> None:
        """Test WireRelease message."""
        release = WireRelease(import_id=42, refcount=3)
        json_arr = release.to_json()
        assert json_arr == ["release", 42, 3]


class TestWireExpressionConversion:
    """Tests for wire expression conversion."""

    def test_literal_values(self) -> None:
        """Test conversion of literal values."""
        assert wire_expression_to_json(None) is None
        assert wire_expression_to_json(True) is True
        assert wire_expression_to_json(42) == 42
        assert wire_expression_to_json(math.pi) == math.pi
        assert wire_expression_to_json("hello") == "hello"

    def test_array_conversion(self) -> None:
        """Test array conversion.

        Per protocol.md, literal arrays on the wire must be escaped using [[...]].
        """
        arr = [[1, 2, "test", None]]
        result = wire_expression_to_json(arr)
        assert result == [[1, 2, "test", None]]

    def test_object_conversion(self) -> None:
        """Test object/dict conversion."""
        obj = {"name": "Alice", "age": 30}
        result = wire_expression_to_json(obj)
        assert result == {"name": "Alice", "age": 30}

    def test_special_form_conversion(self) -> None:
        """Test special form conversion."""
        import_expr = WireImport(42)
        result = wire_expression_to_json(import_expr)
        assert result == ["import", 42]


class TestMessageParsing:
    """Tests for message parsing."""

    def test_parse_push_message(self) -> None:
        """Test parsing push message."""
        json_str = '["push", "test value"]'
        msg = parse_wire_message(json_str)
        assert isinstance(msg, WirePush)
        assert msg.expression == "test value"

    def test_parse_pull_message(self) -> None:
        """Test parsing pull message."""
        json_str = '["pull", 42]'
        msg = parse_wire_message(json_str)
        assert isinstance(msg, WirePull)
        assert msg.import_id == 42

    def test_parse_resolve_message(self) -> None:
        """Test parsing resolve message."""
        json_str = '["resolve", -5, "result"]'
        msg = parse_wire_message(json_str)
        assert isinstance(msg, WireResolve)
        assert msg.export_id == -5
        assert msg.value == "result"

    def test_parse_release_message(self) -> None:
        """Test parsing release message."""
        json_str = '["release", 42, 3]'
        msg = parse_wire_message(json_str)
        assert isinstance(msg, WireRelease)
        assert msg.import_id == 42
        assert msg.refcount == 3

    def test_parse_invalid_message(self) -> None:
        """Test parsing invalid message raises error."""
        with pytest.raises(ValueError):
            parse_wire_message('["unknown_type", 42]')

    def test_parse_empty_message(self) -> None:
        """Test parsing empty message raises error."""
        with pytest.raises(ValueError):
            parse_wire_message("[]")


class TestBatchOperations:
    """Tests for batch message operations."""

    def test_serialize_batch(self) -> None:
        """Test serializing a batch of messages."""
        messages: list[WireMessage] = [
            WirePush("value1"),
            WirePull(42),
            WireRelease(1, 2),
        ]
        result = serialize_wire_batch(messages)

        lines = result.strip().split("\n")
        assert len(lines) == 3
        assert "push" in lines[0]
        assert "pull" in lines[1]
        assert "release" in lines[2]

    def test_parse_batch(self) -> None:
        """Test parsing a batch of messages."""
        batch_str = '["push", "value1"]\n["pull", 42]\n["release", 1, 2]'
        messages = parse_wire_batch(batch_str)

        assert len(messages) == 3
        assert isinstance(messages[0], WirePush)
        assert isinstance(messages[1], WirePull)
        assert isinstance(messages[2], WireRelease)

    def test_roundtrip_batch(self) -> None:
        """Test serialization and parsing roundtrip."""
        original: list[WireMessage] = [WirePush(123), WirePull(42)]
        serialized = serialize_wire_batch(original)
        parsed = parse_wire_batch(serialized)

        assert len(parsed) == len(original)
        assert isinstance(parsed[0], WirePush)
        assert isinstance(parsed[1], WirePull)
