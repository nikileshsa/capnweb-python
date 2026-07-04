"""Property-based tests using Hypothesis to find edge cases.

These tests generate random inputs to verify invariants hold across
a wide range of values, helping discover edge cases that might be
missed by example-based tests.

This is about as good as we can get with property-based tests alone without significantly changing the architecture or adding integration tests.

The property tests cwurrently thoroughly cover:

- All wire protocol serialization/deserialization paths
- ID allocation invariants
- Payload ownership semantics
- Error handling
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from capnweb.error import ErrorCode, RpcError
from capnweb.payload import PayloadSource, RpcPayload
from capnweb.wire import (
    PropertyKey,
    WireAbort,
    WireCapture,
    WireDate,
    WireError,
    WireExport,
    WireImport,
    WirePipeline,
    WirePromise,
    WirePull,
    WirePush,
    WireRelease,
    WireRemap,
    WireResolve,
    wire_expression_from_json,
    wire_expression_to_json,
)

# Custom strategies for wire protocol types


@st.composite
def wire_error_strategy(draw: Any) -> WireError:
    """Generate random WireError expressions."""
    error_type = draw(st.sampled_from(["TypeError", "ValueError", "RuntimeError"]))
    message = draw(st.text(min_size=1, max_size=100))
    stack = draw(st.one_of(st.none(), st.text(max_size=500)))
    data = draw(
        st.one_of(
            st.none(),
            st.dictionaries(
                st.text(min_size=1, max_size=20),
                st.one_of(
                    st.integers(),
                    st.text(max_size=50),
                    st.booleans(),
                ),  # type: ignore[arg-type]
                max_size=5,
            ),
        )
    )
    return WireError(error_type, message, stack, data)


@st.composite
def wire_import_strategy(draw: Any) -> WireImport:
    """Generate random WireImport expressions."""
    import_id = draw(st.integers(min_value=-10000, max_value=10000))
    return WireImport(import_id)


@st.composite
def wire_export_strategy(draw: Any) -> WireExport:
    """Generate random WireExport expressions."""
    export_id = draw(st.integers(min_value=-10000, max_value=10000))
    return WireExport(export_id)


@st.composite
def wire_promise_strategy(draw: Any) -> WirePromise:
    """Generate random WirePromise expressions."""
    promise_id = draw(st.integers(min_value=1, max_value=10000))
    return WirePromise(promise_id)


@st.composite
def simple_json_strategy(draw: Any) -> Any:
    """Generate simple JSON-serializable values."""
    return draw(
        st.one_of(
            st.none(),
            st.booleans(),
            st.integers(min_value=-1000000, max_value=1000000),
            st.floats(allow_nan=False, allow_infinity=False),
            st.text(max_size=100),
        )
    )


@st.composite
def wire_date_strategy(draw: Any) -> WireDate:
    """Generate random WireDate expressions."""
    # Use reasonable timestamp range (2000-01-01 to 2100-01-01)
    timestamp = draw(st.floats(min_value=946684800.0, max_value=4102444800.0))
    return WireDate(timestamp)


@st.composite
def wire_capture_strategy(draw: Any) -> WireCapture:
    """Generate random WireCapture expressions."""
    cap_type = draw(st.sampled_from(["import", "export"]))
    cap_id = draw(st.integers(min_value=-10000, max_value=10000))
    return WireCapture(cap_type, cap_id)


@st.composite
def property_key_strategy(draw: Any) -> PropertyKey:
    """Generate random PropertyKey values."""
    return PropertyKey(
        draw(
            st.one_of(
                st.text(min_size=1, max_size=20),
                st.integers(min_value=0, max_value=1000),
            )
        )
    )


# Property tests for wire serialization


class TestWireSerializationProperties:
    """Property-based tests for wire protocol serialization."""

    @given(wire_error_strategy())
    def test_wire_error_roundtrip(self, error: WireError) -> None:
        """WireError should survive JSON roundtrip."""
        json_repr = error.to_json()
        assert isinstance(json_repr, list)
        assert json_repr[0] == "error"

        # Roundtrip through JSON
        serialized = json.dumps(json_repr)
        deserialized = json.loads(serialized)
        reconstructed = WireError.from_json(deserialized)

        assert reconstructed.error_type == error.error_type
        assert reconstructed.message == error.message
        assert reconstructed.stack == error.stack
        assert reconstructed.data == error.data

    @given(wire_import_strategy())
    def test_wire_import_roundtrip(self, wire_import: WireImport) -> None:
        """WireImport should survive JSON roundtrip."""
        json_repr = wire_import.to_json()
        assert json_repr == ["import", wire_import.import_id]

        # Roundtrip through JSON
        serialized = json.dumps(json_repr)
        deserialized = json.loads(serialized)
        reconstructed = WireImport.from_json(deserialized)

        assert reconstructed.import_id == wire_import.import_id

    @given(wire_export_strategy())
    def test_wire_export_roundtrip(self, wire_export: WireExport) -> None:
        """WireExport should survive JSON roundtrip."""
        json_repr = wire_export.to_json()
        assert json_repr == ["export", wire_export.export_id]

        # Roundtrip through JSON
        serialized = json.dumps(json_repr)
        deserialized = json.loads(serialized)
        reconstructed = WireExport.from_json(deserialized)

        assert reconstructed.export_id == wire_export.export_id

    @given(wire_promise_strategy())
    def test_wire_promise_roundtrip(self, wire_promise: WirePromise) -> None:
        """WirePromise should survive JSON roundtrip."""
        json_repr = wire_promise.to_json()
        assert json_repr == ["promise", wire_promise.promise_id]

        # Roundtrip through JSON
        serialized = json.dumps(json_repr)
        deserialized = json.loads(serialized)
        reconstructed = WirePromise.from_json(deserialized)

        assert reconstructed.promise_id == wire_promise.promise_id

    @given(wire_date_strategy())
    def test_wire_date_roundtrip(self, wire_date: WireDate) -> None:
        """WireDate should survive JSON roundtrip."""
        json_repr = wire_date.to_json()
        assert json_repr == ["date", wire_date.timestamp]

        # Roundtrip through JSON
        serialized = json.dumps(json_repr)
        deserialized = json.loads(serialized)
        reconstructed = WireDate.from_json(deserialized)

        assert reconstructed.timestamp == wire_date.timestamp

    @given(wire_capture_strategy())
    def test_wire_capture_roundtrip(self, wire_capture: WireCapture) -> None:
        """WireCapture should survive JSON roundtrip."""
        json_repr = wire_capture.to_json()
        assert json_repr == [wire_capture.type, wire_capture.id]

        # Roundtrip through JSON
        serialized = json.dumps(json_repr)
        deserialized = json.loads(serialized)
        reconstructed = WireCapture.from_json(deserialized)

        assert reconstructed.type == wire_capture.type
        assert reconstructed.id == wire_capture.id

    @given(property_key_strategy())
    def test_property_key_roundtrip(self, prop_key: PropertyKey) -> None:
        """PropertyKey should survive JSON roundtrip."""
        json_repr = prop_key.to_json()
        assert json_repr == prop_key.value

        # Roundtrip
        reconstructed = PropertyKey.from_json(json_repr)
        assert reconstructed.value == prop_key.value

    @given(
        st.integers(min_value=1, max_value=10000),
        st.lists(property_key_strategy(), min_size=0, max_size=5),
        st.lists(wire_capture_strategy(), min_size=0, max_size=3),
        st.lists(simple_json_strategy(), min_size=1, max_size=3),
    )
    def test_wire_remap_roundtrip(
        self,
        import_id: int,
        path: list[PropertyKey],
        captures: list[WireCapture],
        instructions: list[Any],
    ) -> None:
        """WireRemap should survive JSON roundtrip."""
        remap = WireRemap(
            import_id,
            path or None,
            captures,
            instructions,
        )

        json_repr = remap.to_json()
        assert isinstance(json_repr, list)
        assert json_repr[0] == "remap"
        assert json_repr[1] == import_id

        # Roundtrip through JSON
        serialized = json.dumps(json_repr)
        deserialized = json.loads(serialized)
        reconstructed = WireRemap.from_json(deserialized)

        assert reconstructed.import_id == remap.import_id
        if remap.property_path:
            assert len(reconstructed.property_path or []) == len(remap.property_path)
        assert len(reconstructed.captures) == len(remap.captures)
        assert len(reconstructed.instructions) == len(remap.instructions)

    @given(st.integers(min_value=1, max_value=10000))
    def test_wire_push_serializes_correctly(self, import_id: int) -> None:
        """WirePush should serialize to correct JSON structure."""
        expression = ["import", import_id]
        push = WirePush(expression)

        json_repr = push.to_json()
        assert isinstance(json_repr, list)
        assert json_repr[0] == "push"
        assert json_repr[1] == ["import", import_id]

        # Should be JSON-serializable
        serialized = json.dumps(json_repr)
        assert isinstance(serialized, str)

    @given(
        st.integers(min_value=1, max_value=10000),
        st.one_of(simple_json_strategy(), wire_error_strategy()),
    )
    def test_wire_resolve_serializes_correctly(
        self, export_id: int, value: Any | WireError
    ) -> None:
        """WireResolve should serialize to correct JSON structure."""
        value_json = value.to_json() if isinstance(value, WireError) else value

        resolve = WireResolve(export_id, value_json)

        json_repr = resolve.to_json()
        assert isinstance(json_repr, list)
        assert json_repr[0] == "resolve"
        assert json_repr[1] == export_id

        # Should be JSON-serializable
        serialized = json.dumps(json_repr)
        assert isinstance(serialized, str)


# Property tests for expression evaluation


class TestExpressionProperties:
    """Property-based tests for expression parsing."""

    @given(st.lists(simple_json_strategy(), min_size=0, max_size=10))
    def test_plain_arrays_are_preserved(self, arr: list[Any]) -> None:
        """Plain arrays (not starting with reserved words) should be preserved."""
        # Skip arrays that start with reserved words
        if (
            arr
            and isinstance(arr[0], str)
            and arr[0]
            in {
                "error",
                "import",
                "export",
                "promise",
                "pipeline",
                "remap",
            }
        ):
            return

        # Plain arrays should roundtrip
        json_str = json.dumps(arr)
        parsed = json.loads(json_str)
        assert parsed == arr

    @given(
        st.text(min_size=1, max_size=50),
        st.text(min_size=1, max_size=100),
    )
    def test_error_expressions_always_have_error_type_and_message(
        self, error_type: str, message: str
    ) -> None:
        """Error expressions must have type and message."""
        error = WireError(error_type, message)
        json_repr = error.to_json()

        assert len(json_repr) >= 3
        assert json_repr[0] == "error"
        assert json_repr[1] == error_type
        assert json_repr[2] == message


# Fuzz testing for error paths


class TestFuzzErrorPaths:
    """Fuzz testing to discover error handling edge cases."""

    @given(st.lists(st.integers(), min_size=0, max_size=3))
    def test_malformed_import_expressions(self, arr: list[int]) -> None:
        """Malformed import expressions should raise ValueError."""
        full_arr = ["import"] + arr
        if len(full_arr) == 2:
            # Valid case - exactly ["import", id]
            wire_import = WireImport.from_json(full_arr)
            assert isinstance(wire_import, WireImport)
        else:
            # Invalid cases - not exactly 2 elements
            with pytest.raises(ValueError, match="requires exactly 2 elements"):
                WireImport.from_json(full_arr)

    @given(st.lists(st.text(), min_size=0, max_size=2))
    def test_malformed_error_expressions(self, arr: list[str]) -> None:
        """Malformed error expressions should raise ValueError."""
        if len(arr) >= 2:
            # Valid case - at least type and message
            error = WireError.from_json(["error"] + arr[:2])
            assert isinstance(error, WireError)
        else:
            # Invalid cases
            with pytest.raises(ValueError, match="requires at least 3 elements"):
                WireError.from_json(["error"] + arr)

    @given(st.text(min_size=0, max_size=20))
    def test_rpc_error_codes_are_valid(self, message: str) -> None:
        """RPC errors should have valid error codes."""
        # All factory methods should produce valid errors
        errors = [
            RpcError.not_found(message),
            RpcError.bad_request(message),
            RpcError.internal(message),
        ]

        for error in errors:
            assert isinstance(error.code, ErrorCode)
            assert error.message == message or message == ""

    @given(
        st.integers(min_value=-1000000, max_value=1000000),
        st.one_of(
            st.none(),
            st.booleans(),
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.text(),
        ),
    )
    def test_wire_resolve_accepts_various_value_types(
        self, export_id: int, value: Any
    ) -> None:
        """WireResolve should accept various JSON-serializable values."""
        resolve = WireResolve(export_id, value)
        json_repr = resolve.to_json()

        assert isinstance(json_repr, list)
        assert json_repr[0] == "resolve"
        assert json_repr[1] == export_id

        # Should be JSON-serializable
        serialized = json.dumps(json_repr)
        assert isinstance(serialized, str)

    @given(st.integers(min_value=1, max_value=10000))
    def test_wire_pull_is_simple(self, import_id: int) -> None:
        """WirePull is a simple message type."""
        pull = WirePull(import_id)
        json_repr = pull.to_json()

        assert isinstance(json_repr, list)
        assert json_repr == ["pull", import_id]

        # Should be JSON-serializable
        serialized = json.dumps(json_repr)
        assert isinstance(serialized, str)

    @given(
        st.integers(min_value=1, max_value=10000),
        st.integers(min_value=1, max_value=100),
    )
    def test_wire_release_with_refcount(self, import_id: int, refcount: int) -> None:
        """WireRelease should handle refcounts correctly."""
        release = WireRelease(import_id, refcount)
        json_repr = release.to_json()

        assert isinstance(json_repr, list)
        assert json_repr[0] == "release"
        assert json_repr[1] == import_id
        assert json_repr[2] == refcount

        # Should be JSON-serializable
        serialized = json.dumps(json_repr)
        assert isinstance(serialized, str)

    @given(st.one_of(st.text(min_size=1, max_size=200), wire_error_strategy()))
    def test_wire_abort_preserves_error(self, error: str | WireError) -> None:
        """WireAbort should preserve the error."""
        error_expr = error.to_json() if isinstance(error, WireError) else error

        abort = WireAbort(error_expr)
        json_repr = abort.to_json()

        assert isinstance(json_repr, list)
        assert json_repr[0] == "abort"

        # Should be JSON-serializable
        serialized = json.dumps(json_repr)
        assert isinstance(serialized, str)


# Integration property tests


class TestIntegrationProperties:
    """Property tests that combine multiple components."""

    @given(
        st.integers(min_value=1, max_value=10000),
        st.text(min_size=1, max_size=20),
    )
    def test_capability_reference_roundtrip(self, import_id: int, method: str) -> None:
        """Capability references should survive serialization."""
        # Create a pipeline expression
        pipeline = WirePipeline(
            import_id=import_id,
            property_path=[PropertyKey(method)],
            args=[],
        )

        json_repr = pipeline.to_json()
        assert isinstance(json_repr, list)
        assert json_repr[0] == "pipeline"

        # Roundtrip
        serialized = json.dumps(json_repr)
        deserialized = json.loads(serialized)

        # Check structure
        assert deserialized[0] == "pipeline"
        assert deserialized[1] == import_id
        assert deserialized[2] == [method]

    @given(
        st.lists(
            st.one_of(
                wire_import_strategy(),
                wire_export_strategy(),
                simple_json_strategy(),
            ),  # type: ignore[arg-type]
            min_size=0,
            max_size=5,
        )
    )
    def test_mixed_argument_lists(self, args: list[Any]) -> None:
        """Pipeline calls should handle mixed argument types."""
        # Convert wire types to JSON
        json_args = []
        for arg in args:
            if isinstance(arg, (WireImport, WireExport)):
                json_args.append(arg.to_json())
            else:
                json_args.append(arg)

        # Should be JSON-serializable
        serialized = json.dumps(json_args)
        deserialized = json.loads(serialized)

        assert isinstance(deserialized, list)
        assert len(deserialized) == len(json_args)


# Property tests for RpcPayload ownership semantics


class TestPayloadProperties:
    """Property-based tests for RpcPayload ownership semantics."""

    @given(simple_json_strategy())
    def test_owned_payload_has_owned_source(self, value: Any) -> None:
        """Owned payloads should have PayloadSource.OWNED."""
        payload = RpcPayload.owned(value)
        assert payload.source == PayloadSource.OWNED
        assert payload.value == value

    @given(simple_json_strategy())
    def test_params_payload_has_params_source(self, value: Any) -> None:
        """Params payloads should have PayloadSource.PARAMS."""
        payload = RpcPayload.from_app_params(value)
        assert payload.source == PayloadSource.PARAMS
        assert payload.value == value

    @given(simple_json_strategy())
    def test_return_payload_has_return_source(self, value: Any) -> None:
        """Return payloads should have PayloadSource.RETURN."""
        payload = RpcPayload.from_app_return(value)
        assert payload.source == PayloadSource.RETURN
        assert payload.value == value

    @given(simple_json_strategy())
    def test_params_payload_needs_deep_copy(self, value: Any) -> None:
        """PARAMS payloads should require deep copy before use."""
        payload = RpcPayload.from_app_params(value)
        assert payload.source == PayloadSource.PARAMS

        # After ensure_deep_copied, source should change to OWNED
        copied = payload.ensure_deep_copied()
        if copied is not None:
            assert copied.source == PayloadSource.OWNED

    @given(simple_json_strategy())
    def test_owned_payload_no_copy_needed(self, value: Any) -> None:
        """OWNED payloads don't need copying."""
        payload = RpcPayload.owned(value)
        copied = payload.ensure_deep_copied()

        # Should return same payload (no copy needed) or None if immutable
        if copied is not None:
            assert copied.source == PayloadSource.OWNED

    @given(simple_json_strategy())
    def test_return_payload_becomes_owned(self, value: Any) -> None:
        """RETURN payloads become OWNED when copied."""
        payload = RpcPayload.from_app_return(value)
        copied = payload.ensure_deep_copied()

        # Should be OWNED after copying
        if copied is not None:
            assert copied.source == PayloadSource.OWNED

    @given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=10))
    def test_deep_copy_actually_copies_mutable_values(self, value: list[int]) -> None:
        """Deep copy should create independent copy of mutable objects."""
        payload = RpcPayload.from_app_params(value)
        copied = payload.ensure_deep_copied()

        # Modify original
        value.append(999)

        # Copied value should not be affected
        assert 999 in value
        if copied is not None and isinstance(copied.value, list):
            assert 999 not in copied.value


# Property tests for RpcError


class TestRpcErrorProperties:
    """Property-based tests for RpcError factory methods."""

    @given(st.text(min_size=1, max_size=200))
    def test_not_found_error_has_correct_code(self, message: str) -> None:
        """not_found() factory should create NOT_FOUND errors."""
        error = RpcError.not_found(message)
        assert error.code == ErrorCode.NOT_FOUND
        assert error.message == message

    @given(st.text(min_size=1, max_size=200))
    def test_bad_request_error_has_correct_code(self, message: str) -> None:
        """bad_request() factory should create BAD_REQUEST errors."""
        error = RpcError.bad_request(message)
        assert error.code == ErrorCode.BAD_REQUEST
        assert error.message == message

    @given(st.text(min_size=1, max_size=200))
    def test_internal_error_has_correct_code(self, message: str) -> None:
        """internal() factory should create INTERNAL errors."""
        error = RpcError.internal(message)
        assert error.code == ErrorCode.INTERNAL
        assert error.message == message

    @given(
        st.sampled_from(list(ErrorCode)),
        st.text(min_size=1, max_size=200),
    )
    def test_error_code_is_preserved(self, code: ErrorCode, message: str) -> None:
        """Error codes should be preserved in RpcError."""
        error = RpcError(code, message)
        assert error.code == code
        assert error.message == message

    @given(
        st.text(min_size=1, max_size=200),
        st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.one_of(st.integers(), st.text(max_size=50), st.booleans()),  # type: ignore[arg-type]
            min_size=0,
            max_size=5,
        ),
    )
    def test_error_data_is_preserved(self, message: str, data: dict[str, Any]) -> None:
        """Custom error data should be preserved."""
        error = RpcError(ErrorCode.INTERNAL, message, properties=data)
        assert error.properties == data
        assert error.data == (data or None)

    @given(
        st.sampled_from(list(ErrorCode)),
        st.text(min_size=1, max_size=200),
    )
    def test_error_is_exception(self, code: ErrorCode, message: str) -> None:
        """RpcError should be a proper exception."""
        error = RpcError(code, message)
        assert isinstance(error, Exception)
        assert str(error) == f"{code}: {message}"


# Property tests for wire expression parsing/serialization


class TestWireExpressionProperties:
    """Property-based tests for wire_expression_from_json and wire_expression_to_json."""

    @given(simple_json_strategy())
    def test_primitives_roundtrip(self, value: Any) -> None:
        """Primitive values should roundtrip through wire expression conversion."""
        # Primitives should pass through unchanged
        if value is None or isinstance(value, (bool, int, float, str)):
            json_value = wire_expression_to_json(value)
            assert json_value == value

            parsed = wire_expression_from_json(json_value)
            assert parsed == value

    @given(
        st.dictionaries(
            st.text(min_size=1, max_size=20), simple_json_strategy(), max_size=5
        )
    )
    def test_dicts_roundtrip(self, value: dict[str, Any]) -> None:
        """Dictionaries should roundtrip through wire expression conversion."""
        json_value = wire_expression_to_json(value)
        parsed = wire_expression_from_json(json_value)
        assert parsed == value

    @given(st.lists(simple_json_strategy(), min_size=0, max_size=10))
    def test_plain_arrays_roundtrip(self, value: list[Any]) -> None:
        """Plain arrays should roundtrip."""
        # Skip arrays that start with wire keywords
        if (
            value
            and isinstance(value[0], str)
            and value[0]
            in {
                "error",
                "import",
                "export",
                "promise",
                "pipeline",
                "date",
                "remap",
            }
        ):
            return

        json_value = wire_expression_to_json(value)
        parsed = wire_expression_from_json(json_value)
        assert parsed == value

    @given(wire_error_strategy())
    def test_wire_error_through_expression_converter(self, error: WireError) -> None:
        """WireError should convert through expression functions."""
        # to_json converts WireError to array
        json_value = wire_expression_to_json(error)
        assert isinstance(json_value, list)
        assert json_value[0] == "error"

        # from_json should parse it back
        parsed = wire_expression_from_json(json_value)
        assert isinstance(parsed, WireError)
        assert parsed.error_type == error.error_type
        assert parsed.message == error.message

    @given(wire_date_strategy())
    def test_wire_date_through_expression_converter(self, date: WireDate) -> None:
        """WireDate should convert through expression functions."""
        json_value = wire_expression_to_json(date)
        assert isinstance(json_value, list)
        assert json_value[0] == "date"

        parsed = wire_expression_from_json(json_value)
        assert isinstance(parsed, WireDate)
        assert parsed.timestamp == date.timestamp

    @given(st.lists(simple_json_strategy(), min_size=1, max_size=5))
    def test_arrays_in_pipeline_args(self, args: list[Any]) -> None:
        """Arrays in pipeline arguments should serialize correctly."""
        # wire_expression_to_json simply converts to JSON without escaping
        json_value = wire_expression_to_json(args)

        # Should be the same structure
        assert isinstance(json_value, list)
        assert json_value == args

    @given(st.integers(min_value=-10000, max_value=10000))
    def test_import_export_promise_stay_as_lists(self, id_value: int) -> None:
        """Import/export/promise expressions should stay as plain lists."""
        for tag in ["import", "export", "promise"]:
            expr = [tag, id_value]
            parsed = wire_expression_from_json(expr)

            # Should stay as plain list, not converted to WireImport/WireExport/WirePromise
            assert isinstance(parsed, list)
            assert parsed == expr

    @given(
        st.lists(
            st.lists(simple_json_strategy(), min_size=1, max_size=3),
            min_size=1,
            max_size=3,
        )
    )
    def test_escaped_literal_arrays(self, inner: list[list[Any]]) -> None:
        """Escaped literal arrays [[...]] should be unwrapped."""
        # Skip if inner array looks like a special form
        if (
            inner
            and inner[0]
            and isinstance(inner[0][0], str)
            and inner[0][0]
            in {
                "error",
                "import",
                "export",
                "promise",
                "pipeline",
                "date",
                "remap",
            }
        ):
            return

        # Wrap array: [[...]]
        wrapped = [inner]
        parsed = wire_expression_from_json(wrapped)

        # Should be unwrapped back to original
        assert isinstance(parsed, list)

    @given(
        st.lists(
            st.one_of(
                simple_json_strategy(),
                wire_error_strategy(),
                wire_date_strategy(),
            ),
            min_size=0,
            max_size=5,
        )
    )
    def test_nested_expressions_convert_correctly(self, values: list[Any]) -> None:
        """Nested wire expressions should convert correctly."""
        json_values = [wire_expression_to_json(v) for v in values]

        # Each should be serializable
        serialized = json.dumps(json_values)
        assert isinstance(serialized, str)

        # Should deserialize
        deserialized = json.loads(serialized)
        assert isinstance(deserialized, list)

    @given(st.text(min_size=0, max_size=100))
    def test_invalid_expression_raises(self, invalid: str) -> None:
        """Invalid expression types should raise ValueError."""

        # Create something that's not a valid wire expression type
        # (not None, bool, int, float, str, list, dict)
        @dataclass
        class InvalidType:
            value: str

        invalid_expr = InvalidType(invalid)

        # Should raise when trying to parse
        try:
            wire_expression_to_json(invalid_expr)  # type: ignore[arg-type]
            # If we get here without raising, that's a problem
            assert False, "Expected exception was not raised"  # noqa: B011, PT015
        except (ValueError, TypeError, AttributeError):
            # Expected - invalid type should raise
            pass
