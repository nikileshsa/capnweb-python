"""Serializer (Devaluator) for converting Python objects to wire format.

This module replaces the old ExpressionEvaluator's serialization logic with
a cleaner, more explicit approach. The Serializer takes Python objects and,
with the help of an Exporter (the RpcSession), converts them to JSON-serializable
wire expressions.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Protocol

from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcPromise, RpcStub
from capnweb.wire import WireError, WireExport, WirePromise


class Exporter(Protocol):
    """Protocol for objects that can export capabilities.

    This is typically implemented by RpcSession (Client/Server).
    """

    def export_capability(self, stub: RpcStub | RpcPromise) -> int:
        """Export a capability and return its export ID.

        Args:
            stub: The RpcStub or RpcPromise to export

        Returns:
            The export ID assigned to this capability
        """
        ...


@dataclass
class Serializer:
    """Converts Python objects to wire format for RPC transmission.

    This class (called Devaluator in TypeScript) is responsible for:
    1. Taking Python objects and converting them to JSON-serializable structures
    2. Finding RpcStub and RpcPromise instances and exporting them
    3. Replacing stubs/promises with ["export", id] or ["promise", id] expressions
    4. Handling errors by converting them to ["error", ...] expressions

    The key difference from the old evaluator: this is a pure, stateless
    transformation. All state management happens in the RpcSession (Exporter).
    """

    exporter: Exporter

    def serialize(self, value: Any) -> Any:
        """Serialize a Python value to wire format.

        This is the main entry point. It recursively walks the object tree
        and converts it to a JSON-serializable structure.

        Args:
            value: The Python value to serialize (could be anything)

        Returns:
            A JSON-serializable wire expression
        """
        # Import here to avoid circular dependencies

        match value:
            case None | bool() | int() | float() | str():
                # Handle None and primitives
                return value

            case RpcError():
                # Handle RPC errors
                return self._serialize_error(value)

            case RpcStub():
                # Handle RPC stubs - export them
                export_id = self.exporter.export_capability(value)
                return WireExport(export_id).to_json()

            case RpcPromise():
                # Handle RPC promises - export them as promises
                export_id = self.exporter.export_capability(value)
                # Promises are exported with their promise ID

                return WirePromise(export_id).to_json()

            case list():
                # Handle lists - ESCAPE by wrapping in outer array
                # This matches TypeScript: return [result] to escape literal arrays
                # so they don't get confused with special forms like ["export", id]
                serialized = [self.serialize(item) for item in value]
                return [serialized]  # Wrap in outer array to escape

            case dict():
                # Handle dicts - validate keys are strings (JSON requirement)
                for key in value.keys():
                    if not isinstance(key, str):
                        raise TypeError(f"JSON object keys must be strings, got {type(key).__name__}")
                return {key: self.serialize(val) for key, val in value.items()}

            case RpcPayload():
                # Handle RpcPayload - serialize its value
                # Ensure it's owned first
                value.ensure_deep_copied()
                return self.serialize(value.value)

            case bytes() | bytearray() | memoryview():
                # Encode bytes-like values as ["bytes", base64]
                # per protocol.md.
                raw = bytes(value)
                encoded = base64.b64encode(raw).decode("ascii")
                return ["bytes", encoded]

            case _:
                # For other types, try to serialize as-is
                # (might fail at JSON encoding time)
                return value

    def _serialize_error(self, error: RpcError) -> list[Any]:
        """Serialize an RpcError to wire format.

        Args:
            error: The error to serialize

        Returns:
            A ["error", type, message, ...] array
        """
        wire_error = WireError(
            error_type=error.code.value,
            message=error.message,
            stack=None,  # Stack traces handled by security policy
            data=error.data,
        )
        return wire_error.to_json()

    def serialize_payload(self, payload: RpcPayload) -> Any:
        """Serialize an RpcPayload to wire format.

        This is a convenience method that ensures the payload is owned
        before serializing its value.

        Args:
            payload: The payload to serialize

        Returns:
            A JSON-serializable wire expression
        """
        payload.ensure_deep_copied()
        return self.serialize(payload.value)
