"""Tests for Serializer - Python object to wire format conversion."""

from capnweb.error import ErrorCode, RpcError
from capnweb.hooks import PayloadStubHook
from capnweb.payload import RpcPayload
from capnweb.serializer import Serializer
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcPromise, RpcStub
from tests.conftest import create_transport_pair


class TestSerializerPrimitives:
    """Test serialization of primitive types."""

    def test_serialize_none(self):
        """Test serializing None."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        result = serializer.serialize(None)
        assert result is None

    def test_serialize_bool(self):
        """Test serializing booleans."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        assert serializer.serialize(True) is True
        assert serializer.serialize(False) is False

    def test_serialize_int(self):
        """Test serializing integers."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        assert serializer.serialize(42) == 42
        assert serializer.serialize(0) == 0
        assert serializer.serialize(-123) == -123

    def test_serialize_float(self):
        """Test serializing floats."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        assert serializer.serialize(3.14) == 3.14
        assert serializer.serialize(0.0) == 0.0
        assert serializer.serialize(-2.5) == -2.5

    def test_serialize_string(self):
        """Test serializing strings."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        assert serializer.serialize("hello") == "hello"
        assert serializer.serialize("") == ""
        assert serializer.serialize("unicode: 你好") == "unicode: 你好"


class TestSerializerCollections:
    """Test serialization of collections."""

    def test_serialize_list(self):
        """Test serializing lists - arrays are escaped by wrapping in outer array."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        result = serializer.serialize([1, 2, 3])
        # Arrays are escaped as [[...]] to distinguish from wire expressions
        assert result == [[1, 2, 3]]

    def test_serialize_nested_list(self):
        """Test serializing nested lists - each level is escaped."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        result = serializer.serialize([[1, 2], [3, 4], [5]])
        # Outer array escaped, inner arrays also escaped
        assert result == [[[[1, 2]], [[3, 4]], [[5]]]]

    def test_serialize_dict(self):
        """Test serializing dictionaries."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        result = serializer.serialize({"name": "Alice", "age": 30})
        assert result == {"name": "Alice", "age": 30}

    def test_serialize_nested_dict(self):
        """Test serializing nested dictionaries."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        result = serializer.serialize({
            "user": {"id": 1, "profile": {"name": "Bob"}}
        })
        assert result == {"user": {"id": 1, "profile": {"name": "Bob"}}}

    def test_serialize_mixed_structures(self):
        """Test serializing mixed nested structures."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        value = {
            "users": [{"id": 1}, {"id": 2}],
            "metadata": {"total": 2},
        }
        result = serializer.serialize(value)
        # Arrays in dicts are escaped
        assert result == {
            "users": [[{"id": 1}, {"id": 2}]],
            "metadata": {"total": 2},
        }


class TestSerializerStubs:
    """Test serialization of RpcStub objects."""

    def test_serialize_stub(self):
        """Test serializing an RpcStub."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        # Create a stub with a PayloadStubHook
        hook = PayloadStubHook(RpcPayload.owned("test data"))
        stub = RpcStub(hook)

        result = serializer.serialize(stub)

        # Should be exported as ["export", id]
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == "export"
        assert isinstance(result[1], int)

    def test_serialize_stub_in_dict(self):
        """Test serializing stub nested in dictionary."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        hook = PayloadStubHook(RpcPayload.owned("data"))
        stub = RpcStub(hook)

        result = serializer.serialize({"capability": stub, "id": 123})

        assert result["id"] == 123
        assert isinstance(result["capability"], list)
        assert result["capability"][0] == "export"

    def test_serialize_stub_in_list(self):
        """Test serializing stub nested in list."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        hook = PayloadStubHook(RpcPayload.owned("data"))
        stub = RpcStub(hook)

        result = serializer.serialize([stub, "other", 42])

        # Result is escaped: [[stub_export, "other", 42]]
        assert isinstance(result, list)
        assert len(result) == 1  # Escaped outer array
        inner = result[0]
        assert isinstance(inner[0], list)
        assert inner[0][0] == "export"
        assert inner[1] == "other"
        assert inner[2] == 42


class TestSerializerPromises:
    """Test serialization of RpcPromise objects."""

    def test_serialize_promise(self):
        """Test serializing an RpcPromise."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        # Create a promise
        hook = session.create_promise_hook(1)
        promise = RpcPromise(hook)

        result = serializer.serialize(promise)

        # Per protocol.md, promises are exported as ["promise", id]
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == "promise"

    def test_serialize_promise_in_dict(self):
        """Test serializing promise nested in dictionary."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        hook = session.create_promise_hook(1)
        promise = RpcPromise(hook)

        result = serializer.serialize({"result": promise, "status": "pending"})

        assert result["status"] == "pending"
        assert isinstance(result["result"], list)
        assert result["result"][0] == "promise"


class TestSerializerEdgeCases:
    """Test edge cases in serialization."""

    def test_serialize_empty_list(self):
        """Test serializing empty list - empty arrays are also escaped."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        result = serializer.serialize([])
        # Empty array is escaped as [[]]
        assert result == [[]]

    def test_serialize_empty_dict(self):
        """Test serializing empty dictionary."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        result = serializer.serialize({})
        assert result == {}

    def test_serialize_deeply_nested(self):
        """Test serializing deeply nested structures."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        value = {
            "l1": {"l2": {"l3": {"l4": {"value": 42}}}}
        }
        result = serializer.serialize(value)
        assert result == value

    def test_serialize_list_starting_with_string(self):
        """Test that lists starting with wire expression keywords are escaped."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        # Regular list starting with string - escaped as [[...]]
        result = serializer.serialize(["export", 1, 2])

        # Should be escaped as [["export", 1, 2]]
        assert result == [["export", 1, 2]]

    def test_serialize_regular_list_escaped(self):
        """Test that all lists are escaped by wrapping in outer array."""
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        result = serializer.serialize([1, 2, 3])
        # All arrays are escaped as [[...]]
        assert result == [[1, 2, 3]]

    def test_serialize_bytes(self):
        """Test serializing bytes.

        Per protocol.md, bytes must be encoded as ["bytes", base64].
        """
        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)

        result = serializer.serialize(b"hello")
        assert isinstance(result, list)
        assert result[0] == "bytes"
        assert isinstance(result[1], str)


class TestSerializerRoundtrip:
    """Test serialization/parsing roundtrip."""

    def test_roundtrip_primitives(self):
        """Test roundtrip of primitive values."""
        from capnweb.parser import Parser

        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)
        parser = Parser(importer=session)

        values = [None, True, False, 42, 3.14, "hello", ""]
        for value in values:
            wire = serializer.serialize(value)
            result = parser.parse(wire)
            assert result.value == value

    def test_roundtrip_collections(self):
        """Test roundtrip of collections."""
        from capnweb.parser import Parser

        transport_a, transport_b = create_transport_pair()
        session = BidirectionalSession(transport_a, None)
        serializer = Serializer(exporter=session)
        parser = Parser(importer=session)

        values = [
            [1, 2, 3],
            {"a": 1, "b": 2},
            {"users": [{"id": 1}, {"id": 2}]},
        ]
        for value in values:
            wire = serializer.serialize(value)
            result = parser.parse(wire)
            assert result.value == value
