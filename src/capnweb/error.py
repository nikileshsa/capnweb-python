"""Error types for Cap'n Web protocol.

Implements contract C-ERROR (capnweb-parity-plan.md):

    RpcError(name, message, stack=None, properties={}, cause=None)

``RpcError`` carries the wire-faithful JavaScript error surface:

* ``name``       -- the JS error class name ("Error", "TypeError", ...) or a
                    Python-legacy code string ("bad_request", ...). Emitted
                    verbatim at index 1 of ``["error", name, message, ...]``.
* ``message``    -- human-readable message (index 2).
* ``stack``      -- stack trace string. Only ever EMITTED when an
                    ``on_send_error`` hook deliberately attaches one
                    (serialize.ts:435-438); preserved on decode.
* ``properties`` -- own enumerable properties captured from the wire props bag
                    (5th element), each recursively decoded/encoded
                    (serialize.ts:396-440). Keys name/message/stack are never
                    present here.
* ``cause``      -- the JS ``cause`` slot, decoded from ``properties["cause"]``
                    and re-encoded into the props bag.

The 6-code ``ErrorCode`` enum is a DERIVED convenience (``.code``), not the
wire encoding (locked decision D4).
"""

from __future__ import annotations

from enum import Enum
from typing import Any

# JS error class names that survive revival verbatim, mirroring TS ERROR_TYPES
# (serialize.ts:82-85). Unknown names collapse to "Error" on decode, exactly
# like `new Error(msg)` does in the reference implementation.
JS_ERROR_NAMES: frozenset[str] = frozenset({
    "Error",
    "EvalError",
    "RangeError",
    "ReferenceError",
    "SyntaxError",
    "TypeError",
    "URIError",
    "AggregateError",
})


class ErrorCode(Enum):
    """Standard RPC error codes (Python-side convenience; derived from name)."""

    BAD_REQUEST = "bad_request"
    NOT_FOUND = "not_found"
    CAP_REVOKED = "cap_revoked"
    PERMISSION_DENIED = "permission_denied"
    CANCELED = "canceled"
    INTERNAL = "internal"

    def __str__(self) -> str:
        return self.value


# The 6 code strings double as legacy Python wire names; they must survive
# revival so error codes round-trip between Python peers (documented Python
# extension to the TS allowlist -- a TS peer would collapse them to "Error").
_CODE_NAMES: frozenset[str] = frozenset(c.value for c in ErrorCode)

# Names that survive decode verbatim; everything else collapses to "Error".
REVIVABLE_ERROR_NAMES: frozenset[str] = JS_ERROR_NAMES | _CODE_NAMES


class RpcError(Exception):
    """RPC error carrying the wire-faithful error surface (contract C-ERROR)."""

    __slots__ = ("name", "message", "stack", "properties", "cause",
                 "_internal_origin")

    def __init__(
        self,
        name: str | ErrorCode,
        message: str,
        stack: str | None = None,
        properties: dict[str, Any] | None = None,
        cause: Any = None,
    ) -> None:
        if isinstance(name, ErrorCode):
            name = name.value
        if not isinstance(name, str):
            raise TypeError(
                f"RpcError name must be a string, got {type(name).__name__}"
            )
        super().__init__(f"{name}: {message}")
        self.name = name
        self.message = message
        self.stack = stack
        self.properties: dict[str, Any] = properties if properties is not None else {}
        self.cause = cause
        # F6: marks an error that was auto-wrapped from an UNEXPECTED
        # (non-RpcError) application exception. The serializer redacts the
        # free-text message of such errors by default so internal detail
        # (paths/secrets) never reaches an untrusted peer. Deliberate
        # RpcError protocol signals leave this False and keep their message.
        self._internal_origin = False

    # -- derived conveniences ------------------------------------------------

    @property
    def code(self) -> ErrorCode:
        """Derived 6-code convenience; INTERNAL for non-code names."""
        try:
            return ErrorCode(self.name)
        except ValueError:
            return ErrorCode.INTERNAL

    @property
    def data(self) -> dict[str, Any] | None:
        """Legacy alias for ``properties`` (None when empty)."""
        return self.properties or None

    @property
    def errors(self) -> list[Any] | None:
        """AggregateError convenience: the ``errors`` list, if present."""
        errs = self.properties.get("errors")
        return errs if isinstance(errs, list) else None

    def __str__(self) -> str:
        return f"{self.name}: {self.message}"

    def __repr__(self) -> str:
        return (
            f"RpcError(name={self.name!r}, message={self.message!r}, "
            f"stack={self.stack!r}, properties={self.properties!r}, "
            f"cause={self.cause!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RpcError):
            return NotImplemented
        return (
            self.name == other.name
            and self.message == other.message
            and self.stack == other.stack
            and self.properties == other.properties
            and self.cause == other.cause
        )

    def __hash__(self) -> int:
        return hash((self.name, self.message, self.stack))

    # -- factories (Python-side convenience; name = code string) -------------

    @staticmethod
    def bad_request(message: str, data: dict[str, Any] | None = None) -> RpcError:
        """Create a BAD_REQUEST error."""
        return RpcError("bad_request", message, properties=data)

    @staticmethod
    def not_found(message: str, data: dict[str, Any] | None = None) -> RpcError:
        """Create a NOT_FOUND error."""
        return RpcError("not_found", message, properties=data)

    @staticmethod
    def cap_revoked(message: str, data: dict[str, Any] | None = None) -> RpcError:
        """Create a CAP_REVOKED error."""
        return RpcError("cap_revoked", message, properties=data)

    @staticmethod
    def permission_denied(
        message: str, data: dict[str, Any] | None = None
    ) -> RpcError:
        """Create a PERMISSION_DENIED error."""
        return RpcError("permission_denied", message, properties=data)

    @staticmethod
    def canceled(message: str, data: dict[str, Any] | None = None) -> RpcError:
        """Create a CANCELED error."""
        return RpcError("canceled", message, properties=data)

    @staticmethod
    def internal(message: str, data: dict[str, Any] | None = None) -> RpcError:
        """Create an INTERNAL error."""
        return RpcError("internal", message, properties=data)

    @staticmethod
    def wrap_internal(
        message: str, data: dict[str, Any] | None = None
    ) -> RpcError:
        """Create an INTERNAL error flagged as an auto-wrapped, UNEXPECTED
        application exception (F6).

        Identical to :meth:`internal` but sets ``_internal_origin`` so the
        serializer redacts the free-text message before it crosses the wire
        (when ``redact_internal_errors`` is on). Use this at the boundaries
        where a non-``RpcError`` exception is adapted into the RPC error
        surface; a deliberate ``RpcError`` raised by app code must NOT use it
        (its message is a protocol signal and is preserved).
        """
        err = RpcError("internal", message, properties=data)
        err._internal_origin = True
        return err

    @staticmethod
    def from_wire(
        code: str,
        message: str,
        data: dict[str, Any] | None = None,
        stack: str | None = None,
    ) -> RpcError:
        """Create an RpcError from wire-format values.

        The wire name is preserved verbatim (D4); ``.code`` derives from it.

        Args:
            code: Wire error name (e.g. "TypeError" or legacy "bad_request")
            message: Error message
            data: Optional error properties bag
            stack: Optional stack trace from the wire
        """
        return RpcError(code, message, stack=stack, properties=data)
