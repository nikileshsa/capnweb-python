"""Tests for error types."""

import pytest

from capnweb.error import ErrorCode, RpcError


class TestErrorCode:
    """Tests for ErrorCode enum."""

    def test_error_codes(self) -> None:
        """Test all error code values."""
        assert str(ErrorCode.BAD_REQUEST) == "bad_request"
        assert str(ErrorCode.NOT_FOUND) == "not_found"
        assert str(ErrorCode.CAP_REVOKED) == "cap_revoked"
        assert str(ErrorCode.PERMISSION_DENIED) == "permission_denied"
        assert str(ErrorCode.CANCELED) == "canceled"
        assert str(ErrorCode.INTERNAL) == "internal"


class TestRpcError:
    """Tests for RpcError."""

    def test_basic_error(self) -> None:
        """Test basic error creation."""
        error = RpcError(ErrorCode.BAD_REQUEST, "Invalid input")
        assert error.code == ErrorCode.BAD_REQUEST
        assert error.message == "Invalid input"
        assert error.data is None

    def test_error_with_data(self) -> None:
        """Test error with additional properties (C-ERROR: the 3rd positional
        is now the stack slot; the props bag is the ``properties`` kwarg)."""
        data = {"field": "username"}
        error = RpcError(ErrorCode.BAD_REQUEST, "Invalid field", properties=data)
        assert error.data == data
        assert error.properties == data

    def test_bad_request_constructor(self) -> None:
        """Test bad_request convenience constructor."""
        error = RpcError.bad_request("Bad input")
        assert error.code == ErrorCode.BAD_REQUEST
        assert error.message == "Bad input"

    def test_not_found_constructor(self) -> None:
        """Test not_found convenience constructor."""
        error = RpcError.not_found("Resource not found")
        assert error.code == ErrorCode.NOT_FOUND
        assert error.message == "Resource not found"

    def test_cap_revoked_constructor(self) -> None:
        """Test cap_revoked convenience constructor."""
        error = RpcError.cap_revoked("Capability revoked")
        assert error.code == ErrorCode.CAP_REVOKED
        assert error.message == "Capability revoked"

    def test_permission_denied_constructor(self) -> None:
        """Test permission_denied convenience constructor."""
        error = RpcError.permission_denied("Access denied")
        assert error.code == ErrorCode.PERMISSION_DENIED
        assert error.message == "Access denied"

    def test_canceled_constructor(self) -> None:
        """Test canceled convenience constructor."""
        error = RpcError.canceled("Operation canceled")
        assert error.code == ErrorCode.CANCELED
        assert error.message == "Operation canceled"

    def test_internal_constructor(self) -> None:
        """Test internal convenience constructor."""
        error = RpcError.internal("Internal error")
        assert error.code == ErrorCode.INTERNAL
        assert error.message == "Internal error"

    def test_str_representation(self) -> None:
        """Test string representation."""
        error = RpcError.internal("Something went wrong")
        error_str = str(error)
        assert "internal" in error_str.lower()
        assert "Something went wrong" in error_str

    def test_exception_behavior(self) -> None:
        """Test that RpcError can be raised as an exception."""
        with pytest.raises(RpcError) as exc_info:
            msg = "Test error"
            raise RpcError.not_found(msg)

        error = exc_info.value
        assert error.code == ErrorCode.NOT_FOUND
        assert error.message == "Test error"
