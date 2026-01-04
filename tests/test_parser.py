"""Tests for Parser - wire format to Python object conversion."""

import asyncio

import pytest

from capnweb.error import ErrorCode
from capnweb.hooks import ErrorStubHook, PayloadStubHook, TargetStubHook
from capnweb.parser import Parser
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcPromise, RpcStub
from capnweb.types import RpcTarget
from tests.conftest import create_transport_pair


class TestParserBasics:
    """Test basic parser functionality."""

    def test_parser_initialization(self):
        """Test that parser initializes with an importer."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        assert parser.importer is session

    def test_parse_primitives(self):
        """Test parsing primitive values."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        # None
        result = parser.parse(None)
        assert result.value is None

        # Boolean
        result = parser.parse(True)
        assert result.value is True

        # Integer
        result = parser.parse(42)
        assert result.value == 42

        # Float
        result = parser.parse(3.14)
        assert result.value == 3.14

        # String
        result = parser.parse("hello")
        assert result.value == "hello"

    def test_parse_array(self):
        """Test parsing regular arrays."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = [[1, 2, 3, "four", 5.0]]
        result = parser.parse(wire_value)

        assert result.value == [1, 2, 3, "four", 5.0]

    def test_parse_nested_array(self):
        """Test parsing nested arrays."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = [[[[1, 2]], [[3, 4]], [[5]]]]
        result = parser.parse(wire_value)

        assert result.value == [[1, 2], [3, 4], [5]]

    def test_parse_dict(self):
        """Test parsing dictionaries."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = {"name": "Alice", "age": 30, "active": True}
        result = parser.parse(wire_value)

        assert result.value == {"name": "Alice", "age": 30, "active": True}

    def test_parse_nested_dict(self):
        """Test parsing nested dictionaries."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = {
            "user": {"id": 123, "profile": {"name": "Bob", "email": "bob@example.com"}},
            "count": 5,
        }
        result = parser.parse(wire_value)

        assert result.value == wire_value

    def test_parse_mixed_structures(self):
        """Test parsing mixed nested structures."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = {
            "users": [[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]],
            "metadata": {"total": 2, "page": 1},
        }
        result = parser.parse(wire_value)

        assert result.value == {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            "metadata": {"total": 2, "page": 1},
        }


class TestParseExport:
    """Test parsing export expressions."""

    def test_parse_export_basic(self):
        """Test parsing a basic export expression."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        # ["export", 1]
        wire_value = ["export", 1]
        result = parser.parse(wire_value)

        # Should return an RpcStub
        assert isinstance(result.value, RpcStub)
        # Should have created an import in session
        assert 1 in session._imports

    def test_parse_export_in_dict(self):
        """Test parsing export nested in a dictionary."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = {"user": ["export", 5], "id": 123}
        result = parser.parse(wire_value)

        assert isinstance(result.value["user"], RpcStub)
        assert result.value["id"] == 123
        assert 5 in session._imports

    def test_parse_export_in_array(self):
        """Test parsing export nested in an array."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = [[["export", 1], ["export", 2], {"name": "test"}]]
        result = parser.parse(wire_value)

        assert isinstance(result.value[0], RpcStub)
        assert isinstance(result.value[1], RpcStub)
        assert result.value[2] == {"name": "test"}
        assert 1 in session._imports
        assert 2 in session._imports

    def test_parse_multiple_exports_same_id(self):
        """Test parsing same export ID multiple times returns same import."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = [[["export", 1], ["export", 1]]]
        parser.parse(wire_value)

        # Both should reference the same import hook (same ID)
        assert 1 in session._imports


class TestParsePromise:
    """Test parsing promise expressions."""

    def test_parse_promise_basic(self):
        """Test parsing a basic promise expression."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        # ["promise", 1]
        wire_value = ["promise", 1]
        result = parser.parse(wire_value)

        # Should return an RpcPromise
        assert isinstance(result.value, RpcPromise)

    def test_parse_promise_in_dict(self):
        """Test parsing promise nested in a dictionary."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = {"result": ["promise", 10], "status": "pending"}
        result = parser.parse(wire_value)

        assert isinstance(result.value["result"], RpcPromise)
        assert result.value["status"] == "pending"

    def test_parse_promise_in_array(self):
        """Test parsing promise nested in an array."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = [[["promise", 1], ["promise", 2]]]
        result = parser.parse(wire_value)

        assert isinstance(result.value[0], RpcPromise)
        assert isinstance(result.value[1], RpcPromise)


class TestParseError:
    """Test parsing error expressions."""

    def test_parse_error_basic(self):
        """Test parsing a basic error expression."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        # ["error", "not_found", "Resource not found"]
        wire_value = ["error", "not_found", "Resource not found"]
        result = parser.parse(wire_value)

        # Should return an RpcStub with ErrorStubHook
        assert isinstance(result.value, RpcStub)
        hook = result.value._hook
        assert isinstance(hook, ErrorStubHook)
        assert hook.error.code == ErrorCode.NOT_FOUND
        assert "Resource not found" in hook.error.message

    def test_parse_error_with_stack(self):
        """Test parsing error with stack trace."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = [
            "error",
            "internal",
            "Server error",
            "Traceback: line 1\nline 2",
        ]
        result = parser.parse(wire_value)

        hook = result.value._hook
        assert isinstance(hook, ErrorStubHook)
        assert hook.error.code == ErrorCode.INTERNAL
        assert "Server error" in hook.error.message

    def test_parse_error_with_data(self):
        """Test parsing error with custom data."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = [
            "error",
            "bad_request",
            "Invalid input",
            None,
            {"field": "email", "reason": "invalid format"},
        ]
        result = parser.parse(wire_value)

        hook = result.value._hook
        assert isinstance(hook, ErrorStubHook)
        assert hook.error.code == ErrorCode.BAD_REQUEST
        assert hook.error.data == {"field": "email", "reason": "invalid format"}

    def test_parse_error_unknown_type(self):
        """Test parsing error with unknown type defaults to INTERNAL."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = ["error", "unknown_error_type", "Some error"]
        result = parser.parse(wire_value)

        hook = result.value._hook
        assert isinstance(hook, ErrorStubHook)
        # Unknown types should default to INTERNAL
        assert hook.error.code == ErrorCode.INTERNAL

    def test_parse_error_in_dict(self):
        """Test parsing error nested in a dictionary."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = {
            "success": False,
            "error": ["error", "not_found", "User not found"],
        }
        result = parser.parse(wire_value)

        assert result.value["success"] is False
        assert isinstance(result.value["error"], RpcStub)
        hook = result.value["error"]._hook
        assert isinstance(hook, ErrorStubHook)


class TestParseInvalidExpressions:
    """Test parsing invalid or unsupported expressions."""

    def test_parse_import_expression_returns_error(self):
        """Test that import expressions in received data return error stub."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        # ["import", 1] should not appear in received data
        wire_value = ["import", 1]
        result = parser.parse(wire_value)

        # Should return an error stub
        assert isinstance(result.value, RpcStub)
        hook = result.value._hook
        assert isinstance(hook, ErrorStubHook)
        assert "should not appear in parse input" in hook.error.message

    def test_parse_pipeline_expression_returns_error(self):
        """Test that pipeline expressions in parse input return error stub."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        # ["pipeline", 0, ["method"], []] should not appear in parse input
        wire_value = ["pipeline", 0, ["method"], []]
        result = parser.parse(wire_value)

        # Should return an error stub
        assert isinstance(result.value, RpcStub)
        hook = result.value._hook
        assert isinstance(hook, ErrorStubHook)
        assert "should not appear in parse input" in hook.error.message


class TestParseEdgeCases:
    """Test edge cases and complex scenarios."""

    def test_parse_empty_array(self):
        """Test parsing an empty array."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        result = parser.parse([[]])
        assert result.value == []

    def test_parse_empty_dict(self):
        """Test parsing an empty dictionary."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        result = parser.parse({})
        assert result.value == {}

    def test_parse_array_with_single_string(self):
        """Test that array with single string is not treated as special form."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        # ["hello"] is a regular array, and must be escaped on the wire as [["hello"]]
        wire_value = [["hello"]]
        result = parser.parse(wire_value)

        assert result.value == ["hello"]

    def test_parse_mixed_special_forms(self):
        """Test parsing multiple special forms in same structure."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = {
            "capability": ["export", 1],
            "promise": ["promise", 2],
            "error": ["error", "not_found", "Not found"],
            "data": [[1, 2, 3]],
        }
        result = parser.parse(wire_value)

        assert isinstance(result.value["capability"], RpcStub)
        assert isinstance(result.value["promise"], RpcPromise)
        assert isinstance(result.value["error"], RpcStub)
        assert result.value["data"] == [1, 2, 3]

    def test_parse_deeply_nested_structure(self):
        """Test parsing deeply nested structures."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = {
            "level1": {
                "level2": {
                    "level3": {"level4": {"capability": ["export", 100], "value": 42}}
                }
            }
        }
        result = parser.parse(wire_value)

        nested_cap = result.value["level1"]["level2"]["level3"]["level4"]["capability"]
        assert isinstance(nested_cap, RpcStub)
        assert result.value["level1"]["level2"]["level3"]["level4"]["value"] == 42

    def test_parse_payload_value_method(self):
        """Test the convenience parse_payload_value method."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = {"test": "data"}
        result = parser.parse_payload_value(wire_value)

        assert isinstance(result, RpcPayload)
        assert result.value == {"test": "data"}


class TestParserIntegration:
    """Integration tests with BidirectionalSession."""

    def test_parse_and_resolve_export(self):
        """Test parsing export and using it."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        # Parse an export
        wire_value = ["export", 1]
        result = parser.parse(wire_value)

        # The stub should reference the imported capability
        assert isinstance(result.value, RpcStub)
        assert 1 in session._imports

    def test_parse_error_stub(self):
        """Test parsing error stub contains the error."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        parser = Parser(importer=session)

        wire_value = ["error", "not_found", "Test error"]
        result = parser.parse(wire_value)

        # Should be an RpcStub with ErrorStubHook
        assert isinstance(result.value, RpcStub)
        hook = result.value._hook
        assert isinstance(hook, ErrorStubHook)
        assert hook.error.code == ErrorCode.NOT_FOUND
        assert "Test error" in hook.error.message
