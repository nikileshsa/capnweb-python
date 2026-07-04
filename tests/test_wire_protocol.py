"""Tests for wire protocol parsing and serialization."""

import json

import pytest

from capnweb.wire import (
    WireCapture,
    WireError,
    WireImport,
    WireRelease,
    WireRemap,
    parse_wire_message,
    serialize_wire_message,
    wire_expression_from_json,
    wire_expression_to_json,
)


class TestWireRelease:
    """Tests for WireRelease message with refcount."""

    def test_wire_release_serialization(self):
        """Test that WireRelease serializes to correct format."""
        release = WireRelease(import_id=42, refcount=3)
        result = release.to_json()
        assert result == ["release", 42, 3]

    def test_wire_release_parsing(self):
        """Test that WireRelease parses correctly."""
        msg = parse_wire_message('["release", 42, 3]')
        assert isinstance(msg, WireRelease)
        assert msg.import_id == 42
        assert msg.refcount == 3

    def test_wire_release_roundtrip(self):
        """Test that WireRelease survives serialization roundtrip."""
        original = WireRelease(import_id=7, refcount=2)
        serialized = serialize_wire_message(original)
        parsed = parse_wire_message(serialized)
        assert isinstance(parsed, WireRelease)
        assert parsed.import_id == original.import_id
        assert parsed.refcount == original.refcount

    def test_wire_release_invalid_format(self):
        """Test that invalid release message raises error."""
        with pytest.raises(ValueError, match="requires exactly 3 elements"):
            parse_wire_message('["release", 42]')  # Missing refcount


class TestWireRemap:
    """Tests for WireRemap expression."""

    def test_wire_remap_basic(self):
        """Test basic WireRemap serialization."""
        remap = WireRemap(
            import_id=1,
            property_path=None,
            captures=[],
            instructions=[42],
        )
        result = remap.to_json()
        # B2: propertyPath is ALWAYS an array on the wire (TS rejects null).
        assert result == ["remap", 1, [], [], [42]]

    def test_wire_remap_with_captures(self):
        """Test WireRemap with captures."""
        captures = [
            WireCapture("import", 1),
            WireCapture("export", -2),
        ]
        # B2: instructions are RAW JSON end-to-end (matrix 04 row 13).
        remap = WireRemap(
            import_id=5,
            property_path=None,
            captures=captures,
            instructions=[["import", -1], ["import", -2]],
        )
        result = remap.to_json()
        assert result == [
            "remap",
            5,
            [],
            [["import", 1], ["export", -2]],
            [["import", -1], ["import", -2]],
        ]

    def test_wire_remap_parsing(self):
        """Test parsing WireRemap from JSON."""
        # Remap is an expression, not a message, so use expression parser directly
        json_str = '["remap", 3, null, [["import", 1]], [[42]]]'
        data = json.loads(json_str)
        expr = wire_expression_from_json(data)

        assert isinstance(expr, WireRemap)
        assert expr.import_id == 3
        assert expr.property_path is None
        assert len(expr.captures) == 1
        assert expr.captures[0].type == "import"
        assert expr.captures[0].id == 1
        assert expr.instructions == [[42]]

    def test_wire_remap_roundtrip(self):
        """Test WireRemap serialization roundtrip."""
        original = WireRemap(
            import_id=10,
            property_path=None,
            captures=[WireCapture("import", 5)],
            instructions=["test", 123],
        )
        json_data = original.to_json()
        parsed = WireRemap.from_json(json_data)

        assert parsed.import_id == original.import_id
        # to_json normalizes a None path to [] (TS rejects null paths).
        assert parsed.property_path == []
        assert len(parsed.captures) == len(original.captures)
        assert parsed.captures[0].type == original.captures[0].type
        assert parsed.captures[0].id == original.captures[0].id
        assert parsed.instructions == original.instructions


class TestEscapedArrays:
    """Tests for escaped literal arrays [[...]]."""

    def test_escaped_array_parsing(self):
        """Test that [[...]] is NOT unwrapped by wire_expression_from_json.
        
        Escaped array unwrapping is done by Parser, not wire_expression_from_json.
        wire_expression_from_json only handles wire-level special forms.
        """
        # An escaped array [[...]] is left as-is by wire_expression_from_json
        escaped = [["error", "not really an error"]]

        # wire_expression_from_json does NOT unwrap - that's Parser's job
        parsed = wire_expression_from_json(escaped)
        assert parsed == [["error", "not really an error"]]
        assert isinstance(parsed, list)
        assert not isinstance(parsed, WireError)

    def test_normal_array_not_escaped(self):
        """Test that normal arrays are not escaped by default."""
        normal_array = [1, 2, 3]
        json_output = wire_expression_to_json(normal_array)
        assert json_output == [1, 2, 3]

    def test_nested_arrays_with_strings(self):
        """Test arrays with string elements that don't need escaping by default."""
        array = [123, "hello", 456]  # Doesn't start with string
        json_output = wire_expression_to_json(array)
        assert json_output == [123, "hello", 456]

    def test_escaped_array_roundtrip(self):
        """Test that escaped arrays are preserved by wire_expression_from_json.
        
        wire_expression_from_json does NOT unwrap escaped arrays - that's Parser's job.
        """
        # Start with an escaped array that looks like a special form
        escaped = [["import", 123, "foo"]]

        # wire_expression_from_json does NOT unwrap - leaves as-is
        parsed = wire_expression_from_json(escaped)
        assert parsed == [["import", 123, "foo"]]
        assert isinstance(parsed, list)

        # Serialize back - wire_expression_to_json doesn't escape
        serialized = wire_expression_to_json(parsed)
        assert serialized == [["import", 123, "foo"]]

    def test_actual_wire_expression_not_double_wrapped(self):
        """Test that actual wire expressions are not double-wrapped."""
        wire_import = WireImport(5)
        serialized = wire_expression_to_json(wire_import)
        assert serialized == ["import", 5]

        # When parsed, capability expressions are left as plain lists
        # (Parser is responsible for converting to RpcStub objects)
        parsed = wire_expression_from_json(serialized)
        assert parsed == ["import", 5]
        assert isinstance(parsed, list)


class TestWireCapture:
    """Tests for WireCapture used in remap."""

    def test_capture_import(self):
        """Test capture with import type."""
        capture = WireCapture("import", 42)
        assert capture.to_json() == ["import", 42]

    def test_capture_export(self):
        """Test capture with export type."""
        capture = WireCapture("export", -5)
        assert capture.to_json() == ["export", -5]

    def test_capture_parsing(self):
        """Test parsing captures."""
        import_cap = WireCapture.from_json(["import", 10])
        assert import_cap.type == "import"
        assert import_cap.id == 10

        export_cap = WireCapture.from_json(["export", -3])
        assert export_cap.type == "export"
        assert export_cap.id == -3

    def test_capture_invalid_type(self):
        """Test that invalid capture type raises error."""
        with pytest.raises(ValueError, match="Capture requires"):
            WireCapture.from_json(["invalid", 1])


class TestIntegration:
    """Integration tests for all wire protocol features."""

    def test_complex_message_with_all_features(self):
        """Test a complex message using multiple features."""
        # Create a complex expression with actual wire types
        complex_expr = {
            "user": WireImport(1),
            "action": "update",
            "mapper": WireRemap(
                import_id=2,
                property_path=None,
                captures=[WireCapture("import", 1)],
                instructions=[["pipeline", -1]],
            ),
        }

        # Serialize
        serialized = wire_expression_to_json(complex_expr)

        # Should be valid JSON
        json_str = json.dumps(serialized)
        reparsed = json.loads(json_str)

        # Deserialize
        result = wire_expression_from_json(reparsed)

        assert isinstance(result, dict)
        # P1 (perf): wire_expression_from_json classifies only the ROOT of an
        # expression; container interiors stay raw JSON for the Parser, which
        # materializes nested special forms itself (parser.py _parse_tagged).
        # So a NESTED remap/import/export/promise all stay plain lists here —
        # only a root-level tagged form becomes a wire dataclass.
        assert result["user"] == ["import", 1]
        assert result["action"] == "update"
        assert result["mapper"] == ["remap", 2, [], [["import", 1]], [["pipeline", -1]]]
