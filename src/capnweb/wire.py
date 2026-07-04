"""Wire protocol implementation for Cap'n Web.

Implements the JSON-based wire format as specified in the protocol:
https://github.com/cloudflare/capnweb/blob/main/protocol.md

## Architecture: Wire vs Parser Responsibilities

This module handles WIRE-LEVEL parsing only:
- Converts JSON strings to WireMessage objects (push, pull, resolve, etc.)
- Converts wire expressions to WireExpression dataclasses (WirePipeline, WireError, etc.)
- Does NOT unwrap escaped arrays [[...]] - that's Parser's job
- Does NOT convert ["export", id] to RpcStub - that's Parser's job

The Parser (parser.py) handles APPLICATION-LEVEL parsing:
- Unwraps escaped arrays: [[1,2,3]] → [1,2,3]
- Converts ["export", id] → RpcStub
- Converts ["promise", id] → RpcPromise
- Handles special values: bigint, date, bytes, etc.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from capnweb import _json

# Security: Maximum recursion depth to prevent stack overflow attacks
MAX_PARSE_DEPTH: Final[int] = 64


def is_int_not_bool(x: object) -> bool:
    """Check if x is an int but not a bool.
    
    In Python, bool is a subclass of int, so isinstance(True, int) returns True.
    This is dangerous in a capability protocol where True/False could alias IDs 1/0.
    """
    return isinstance(x, int) and not isinstance(x, bool)


@dataclass(frozen=True, slots=True)
class PropertyKey:
    """A property key, either string or numeric."""

    value: str | int

    def to_json(self) -> str | int:
        """Convert to JSON representation."""
        return self.value

    @staticmethod
    def from_json(value: Any) -> PropertyKey:
        """Parse from JSON value."""
        if isinstance(value, str):
            return PropertyKey(value)
        if is_int_not_bool(value):
            return PropertyKey(value)
        msg = f"Invalid property key: {value}"
        raise ValueError(msg)


# Wire Expressions


@dataclass(frozen=True, slots=True)
class WireError:
    """Error expression: ["error", type, message, stack?, data?]

    The data field allows encoding custom properties that have been added to the error,
    enabling richer error information to be transmitted across the RPC boundary.
    """

    error_type: str
    message: str
    stack: str | None = None
    data: dict[str, Any] | None = None

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        result: list[Any] = ["error", self.error_type, self.message]
        if self.stack is not None:
            result.append(self.stack)
            # If we have data but no stack, we need to add null for stack
            if self.data is not None:
                result.append(self.data)
        elif self.data is not None:
            # No stack but we have data - add null for stack position
            result.extend((None, self.data))
        return result

    @staticmethod
    def from_json(arr: list[Any]) -> WireError:
        """Parse from JSON array."""
        if len(arr) < 3:
            msg = "Error expression requires at least 3 elements"
            raise ValueError(msg)
        error_type = arr[1]
        message = arr[2]
        # Strict type validation for boundary objects
        if not isinstance(error_type, str):
            msg = f"Error type must be string, got {type(error_type).__name__}"
            raise ValueError(msg)
        if not isinstance(message, str):
            msg = f"Error message must be string, got {type(message).__name__}"
            raise ValueError(msg)
        # TS only uses the stack slot when it is a string (serialize.ts:691-693);
        # other values are ignored, not errors (the slot is normalized to null
        # when a props bag is present).
        stack = arr[3] if len(arr) > 3 and isinstance(arr[3], str) else None
        data: dict[str, Any] | None = None
        if len(arr) > 4:
            if not isinstance(arr[4], dict):
                # Malformed props bag is a hard error (serialize.ts:697-699).
                msg = f"Error properties must be an object, got {type(arr[4]).__name__}"
                raise ValueError(msg)
            data = arr[4]
        return WireError(error_type, message, stack, data)


@dataclass(frozen=True, slots=True)
class WireImport:
    """Import expression: ["import", id]"""

    import_id: int

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["import", self.import_id]

    @staticmethod
    def from_json(arr: list[Any]) -> WireImport:
        """Parse from JSON array."""
        if len(arr) != 2:
            msg = "Import expression requires exactly 2 elements"
            raise ValueError(msg)
        if not is_int_not_bool(arr[1]):
            msg = f"Import ID must be int, got {type(arr[1]).__name__}"
            raise ValueError(msg)
        return WireImport(arr[1])


@dataclass(frozen=True, slots=True)
class WireExport:
    """Export expression: ["export", id]"""

    export_id: int
    # NOTE: is_promise removed - in Cap'n Web, "promise" is a separate special form

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["export", self.export_id]

    @staticmethod
    def from_json(arr: list[Any]) -> WireExport:
        """Parse from JSON array."""
        if len(arr) != 2:
            msg = "Export expression requires exactly 2 elements"
            raise ValueError(msg)
        if not is_int_not_bool(arr[1]):
            msg = f"Export ID must be int, got {type(arr[1]).__name__}"
            raise ValueError(msg)
        return WireExport(arr[1])


@dataclass(frozen=True, slots=True)
class WirePromise:
    """Promise expression: ["promise", id]"""

    promise_id: int

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["promise", self.promise_id]

    @staticmethod
    def from_json(arr: list[Any]) -> WirePromise:
        """Parse from JSON array."""
        if len(arr) != 2:
            msg = "Promise expression requires exactly 2 elements"
            raise ValueError(msg)
        if not is_int_not_bool(arr[1]):
            msg = f"Promise ID must be int, got {type(arr[1]).__name__}"
            raise ValueError(msg)
        return WirePromise(arr[1])


@dataclass(frozen=True, slots=True)
class WirePipeline:
    """Pipeline expression: ["pipeline", import_id, property_path?, args?]"""

    import_id: int
    property_path: list[PropertyKey] | None = None
    args: WireExpression | None = None

    def to_json(self) -> list[Any]:
        """Convert to JSON array.
        
        Emits minimal form per spec:
        - ["pipeline", id] if no path and no args
        - ["pipeline", id, path] if path but no args
        - ["pipeline", id, path_or_null, args] if args present
        """
        result: list[Any] = ["pipeline", self.import_id]

        # Only add property_path if present or if args will follow
        if self.args is not None:
            # Args present - need placeholder for path if None
            if self.property_path is not None:
                result.append([pk.to_json() for pk in self.property_path])
            else:
                result.append(None)  # Placeholder only when args follow
            # Args are raw JSON - pass through without transformation
            result.append(self.args)
        elif self.property_path is not None:
            # Path present, no args - just add path
            result.append([pk.to_json() for pk in self.property_path])
        # else: no path, no args - minimal ["pipeline", id]

        return result

    @staticmethod
    def from_json(arr: list[Any]) -> WirePipeline:
        """Parse from JSON array."""
        if len(arr) < 2:
            msg = "Pipeline expression requires at least 2 elements"
            raise ValueError(msg)
        import_id = arr[1]
        if not is_int_not_bool(import_id):
            msg = f"Pipeline import_id must be int, got {type(import_id).__name__}"
            raise ValueError(msg)
        # Preserve [] vs None distinction:
        # - None means "no path provided" (slot absent or null)
        # - [] means "empty path" (explicitly provided empty list)
        property_path = None
        if len(arr) > 2:
            path_val = arr[2]
            if path_val is None:
                property_path = None
            elif isinstance(path_val, list):
                property_path = [PropertyKey.from_json(k) for k in path_val]
            else:
                msg = f"Pipeline property_path must be list or null, got {type(path_val).__name__}"
                raise ValueError(msg)
        # Args are raw JSON - pass through without transformation
        # Don't apply wire_expression_from_json to avoid unwrapping [[1,2,3]]
        args = arr[3] if len(arr) > 3 else None
        return WirePipeline(import_id, property_path, args)


@dataclass(frozen=True, slots=True)
class WireDate:
    """Date expression: ["date", timestamp]"""

    timestamp: float

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["date", self.timestamp]

    @staticmethod
    def from_json(arr: list[Any]) -> WireDate:
        """Parse from JSON array."""
        if len(arr) != 2:
            msg = "Date expression requires exactly 2 elements"
            raise ValueError(msg)
        return WireDate(arr[1])


@dataclass(frozen=True, slots=True)
class WireCapture:
    """Capture expression for remap: ["import", importId] or ["export", exportId]"""

    type: str  # "import" or "export"
    id: int

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return [self.type, self.id]

    @staticmethod
    def from_json(arr: list[Any]) -> WireCapture:
        """Parse from JSON array."""
        if len(arr) != 2 or arr[0] not in ("import", "export"):
            msg = "Capture requires ['import'|'export', id]"
            raise ValueError(msg)
        if not is_int_not_bool(arr[1]):
            msg = f"Capture ID must be int, got {type(arr[1]).__name__}"
            raise ValueError(msg)
        return WireCapture(arr[0], arr[1])


@dataclass(frozen=True, slots=True)
class WireRemap:
    """Remap expression: ["remap", importId, propertyPath, captures, instructions]"""

    import_id: int
    property_path: list[PropertyKey] | None
    captures: list[WireCapture]
    instructions: list[Any]  # RAW JSON expressions, passed through untouched

    def to_json(self) -> list[Any]:
        """Convert to JSON array.

        The propertyPath is ALWAYS emitted as an array — never null. TS
        receivers require ``value[2] instanceof Array`` and hard-reject null
        (serialize.ts:906-912; matrix 04 row 12). Instructions are raw JSON
        and pass through untouched (TS passes value[4] through,
        serialize.ts:944-946; matrix 04 row 13).
        """
        path_json = (
            [pk.to_json() for pk in self.property_path] if self.property_path else []
        )
        captures_json = [c.to_json() for c in self.captures]
        return ["remap", self.import_id, path_json, captures_json,
                list(self.instructions)]

    @staticmethod
    def from_json(arr: list[Any]) -> WireRemap:
        """Parse from JSON array.

        Instructions stay RAW JSON end-to-end (matrix 04 row 13): they are
        evaluated later inside the mapper's own index space, where
        ``["pipeline", ...]`` etc. mean something different than in a
        top-level expression. Receive stays lenient about a null path
        (Python <= B1 senders emitted it).
        """
        if len(arr) != 5:
            msg = "Remap expression requires exactly 5 elements"
            raise ValueError(msg)
        import_id = arr[1]
        if not is_int_not_bool(import_id):
            msg = f"Remap import_id must be int, got {type(import_id).__name__}"
            raise ValueError(msg)
        property_path = (
            [PropertyKey.from_json(pk) for pk in arr[2]] if arr[2] is not None else None
        )
        captures = [WireCapture.from_json(c) for c in arr[3]]
        return WireRemap(import_id, property_path, captures, arr[4])


# Wire expression type union
WireExpression = (
    None
    | bool
    | int
    | float
    | str
    | list[Any]
    | dict[str, Any]
    | WireError
    | WireImport
    | WireExport
    | WirePromise
    | WirePipeline
    | WireDate
    | WireRemap
)


def wire_expression_from_json(value: Any, *, _depth: int = 0) -> WireExpression:  # noqa: C901
    """Parse a wire expression from JSON.
    
    This function converts JSON wire format to Python wire expression types.
    It handles wire-level special forms (pipeline, remap, error, date) but
    leaves application-level forms (export, import, promise) as plain lists
    for the Parser to handle.
    
    IMPORTANT: This function does NOT unwrap escaped arrays [[...]].
    That's the Parser's responsibility (see parser.py).
    
    Args:
        value: JSON value to parse
        _depth: Internal recursion depth counter (do not pass externally)
        
    Returns:
        Parsed wire expression (may be WirePipeline, WireRemap, etc. or plain data)
        
    Raises:
        ValueError: If recursion depth exceeds MAX_PARSE_DEPTH (security protection)
    """
    # Security: Prevent stack overflow from deeply nested malicious payloads
    if _depth > MAX_PARSE_DEPTH:
        raise ValueError(
            f"Wire expression exceeds maximum depth ({MAX_PARSE_DEPTH}). "
            "Possible malicious payload or circular reference."
        )

    # Primitives pass through unchanged (no recursion needed)
    # NOTE: Using tuple form - isinstance(x, T1 | T2) raises TypeError in Python
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    # P1 (perf): classify only the ROOT of a wire expression; leave every
    # container INTERIOR as raw JSON. The Parser (parser.py) materializes
    # nested special forms directly from raw JSON via ``_parse_tagged`` — the
    # session only ever dispatches on the root form (push/stream pipeline &
    # remap, reject/abort error). Recursively wrapping interiors in wire
    # dataclasses here just produced objects the Parser immediately converted
    # back with ``.to_json()`` (the wire→dataclass→JSON double-pass). Dropping
    # that recursion removes a whole allocation+conversion layer from the
    # receive path without changing any decoded value or the wire format.
    if isinstance(value, dict):
        return value

    if isinstance(value, list):
        # Empty array - return as-is
        if not value:
            return value

        # Check for special forms (arrays starting with a string tag)
        if isinstance(value[0], str):
            tag = value[0]

            # Wire-level expressions that we convert to dataclasses (ROOT only).
            if tag == "error":
                if len(value) >= 3 and isinstance(value[1], str) and isinstance(value[2], str):
                    return WireError.from_json(value)

            elif tag == "pipeline":
                if len(value) >= 2 and isinstance(value[1], int):
                    return WirePipeline.from_json(value)

            elif tag == "date":
                if len(value) == 2 and isinstance(value[1], (int, float)):
                    return WireDate.from_json(value)

            elif tag == "remap":
                # value[2] (property_path) can be null or list
                if (len(value) == 5 and isinstance(value[1], int) and
                    (value[2] is None or isinstance(value[2], list)) and
                    isinstance(value[3], list) and isinstance(value[4], list)):
                    return WireRemap.from_json(value)

        # Everything else — application-level forms (export/import/promise),
        # other special values (bigint/bytes/undefined/inf/nan), escaped
        # arrays [[...]], plain arrays, and unknown tags — passes through
        # unchanged for the Parser to handle.
        return value

    msg = f"Invalid wire expression type: {type(value).__name__}"
    raise ValueError(msg)


def wire_expression_to_json(expr: WireExpression) -> Any:
    """Convert a wire expression to JSON.

    This function simply converts wire expressions to JSON-serializable format.
    It does NOT handle array escaping - that's the Serializer's responsibility.
    
    Args:
        expr: The expression to convert
    """
    match expr:
        case None | bool() | int() | float() | str():
            return expr

        case bytes() | bytearray() | memoryview():
            # Raw bytes appear inside ["bytes", ...] forms at the
            # "jsonCompatibleWithBytes" encoding level; the transport passes
            # them through natively (never JSON-stringified).
            return expr

        case dict():
            return {k: wire_expression_to_json(v) for k, v in expr.items()}

        case list():
            return [wire_expression_to_json(item) for item in expr]

        case (
            WireError()
            | WireImport()
            | WireExport()
            | WirePromise()
            | WirePipeline()
            | WireDate()
            | WireRemap()
        ):
            return expr.to_json()

        case _:
            msg = f"Invalid wire expression: {expr}"
            raise ValueError(msg)


# Wire Messages


@dataclass(frozen=True, slots=True)
class WirePush:
    """Push message: ["push", expression]"""

    expression: WireExpression

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["push", wire_expression_to_json(self.expression)]


@dataclass(frozen=True, slots=True)
class WirePull:
    """Pull message: ["pull", import_id]"""

    import_id: int

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["pull", self.import_id]


@dataclass(frozen=True, slots=True)
class WireResolve:
    """Resolve message: ["resolve", export_id, value]"""

    export_id: int
    value: WireExpression

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        # Value is already serialized - just convert to JSON
        return ["resolve", self.export_id, wire_expression_to_json(self.value)]


@dataclass(frozen=True, slots=True)
class WireReject:
    """Reject message: ["reject", export_id, error]"""

    export_id: int
    error: WireExpression

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["reject", self.export_id, wire_expression_to_json(self.error)]


@dataclass(frozen=True, slots=True)
class WireRelease:
    """Release message: ["release", importId, refcount]"""

    import_id: int
    refcount: int

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["release", self.import_id, self.refcount]


@dataclass(frozen=True, slots=True)
class WireAbort:
    """Abort message: ["abort", error]"""

    error: WireExpression

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["abort", wire_expression_to_json(self.error)]


@dataclass(frozen=True, slots=True)
class WireStream:
    """Stream message: ["stream", expression] (protocol.md:103-111).

    Like push, but: no pipelining on the result, auto-pulled, and
    auto-released once the recipient sends resolve/reject.
    """

    expression: WireExpression

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["stream", wire_expression_to_json(self.expression)]


@dataclass(frozen=True, slots=True)
class WirePipe:
    """Pipe message: ["pipe"] (protocol.md:113-121).

    Creates a pipe on the remote end; implicitly assigned the next
    sequential positive import ID (same counter as push/stream).
    """

    def to_json(self) -> list[Any]:
        """Convert to JSON array."""
        return ["pipe"]


WireMessage = (
    WirePush
    | WirePull
    | WireResolve
    | WireReject
    | WireRelease
    | WireAbort
    | WireStream
    | WirePipe
)


def parse_wire_message(data: str) -> WireMessage:
    """Parse a wire message from JSON string ("string" encoding level).

    Uses strict JSON parsing to reject non-standard constants (NaN, Infinity)
    which should be encoded as special forms in Cap'n Web.
    """
    # capnweb._json.loads is strict RFC-8259: NaN/Infinity are rejected
    # natively (they must be encoded as ["nan"]/["inf"] escape forms).
    return parse_wire_message_tree(_json.loads(data))


def parse_wire_message_tree(arr: Any) -> WireMessage:  # noqa: C901
    """Parse a wire message from an already-decoded JSON-compatible tree.

    Used directly by sessions on custom-encoding transports (encoding level
    "jsonCompatible"/"jsonCompatibleWithBytes", rpc.ts:947), where the
    transport hands over value trees instead of JSON strings. Applies exactly
    the same strict validation as the string path.
    """
    if not isinstance(arr, list) or not arr:
        msg = "Wire message must be a non-empty array"
        raise ValueError(msg)

    msg_type = arr[0]
    if not isinstance(msg_type, str):
        msg = "Message type must be a string"
        raise ValueError(msg)

    match msg_type:
        case "push":
            if len(arr) != 2:
                msg = "Push message requires exactly 2 elements"
                raise ValueError(msg)
            return WirePush(wire_expression_from_json(arr[1]))

        case "pull":
            if len(arr) != 2:
                msg = "Pull message requires exactly 2 elements"
                raise ValueError(msg)
            if not is_int_not_bool(arr[1]):
                msg = f"Pull import_id must be int, got {type(arr[1]).__name__}"
                raise ValueError(msg)
            return WirePull(arr[1])

        case "resolve":
            if len(arr) != 3:
                msg = "Resolve message requires exactly 3 elements"
                raise ValueError(msg)
            if not is_int_not_bool(arr[1]):
                msg = f"Resolve export_id must be int, got {type(arr[1]).__name__}"
                raise ValueError(msg)
            return WireResolve(arr[1], wire_expression_from_json(arr[2]))

        case "reject":
            if len(arr) != 3:
                msg = "Reject message requires exactly 3 elements"
                raise ValueError(msg)
            if not is_int_not_bool(arr[1]):
                msg = f"Reject export_id must be int, got {type(arr[1]).__name__}"
                raise ValueError(msg)
            return WireReject(arr[1], wire_expression_from_json(arr[2]))

        case "release":
            if len(arr) != 3:
                msg = "Release message requires exactly 3 elements"
                raise ValueError(msg)
            if not is_int_not_bool(arr[1]):
                msg = f"Release import_id must be int, got {type(arr[1]).__name__}"
                raise ValueError(msg)
            if not is_int_not_bool(arr[2]):
                msg = f"Release refcount must be int, got {type(arr[2]).__name__}"
                raise ValueError(msg)
            return WireRelease(arr[1], arr[2])

        case "abort":
            if len(arr) != 2:
                msg = "Abort message requires exactly 2 elements"
                raise ValueError(msg)
            return WireAbort(wire_expression_from_json(arr[1]))

        case "stream":
            if len(arr) != 2:
                msg = "Stream message requires exactly 2 elements"
                raise ValueError(msg)
            return WireStream(wire_expression_from_json(arr[1]))

        case "pipe":
            if len(arr) != 1:
                msg = "Pipe message requires exactly 1 element"
                raise ValueError(msg)
            return WirePipe()

        case _:
            msg = f"Unknown message type: {msg_type}"
            raise ValueError(msg)


def serialize_wire_message(msg: WireMessage) -> str:
    """Serialize a wire message to JSON string.
    
    Uses the orjson-backed codec (capnweb._json): compact, raw-UTF-8, strict —
    byte-identical to TS JSON.stringify. Cap'n Web encodes NaN/Infinity via
    escape arrays like ["nan"], so raw constants never reach the encoder.
    """
    return _json.dumps(msg.to_json())


def parse_wire_batch(data: str) -> list[WireMessage]:
    """Parse a batch of newline-delimited wire messages.

    P5 (perf): natively-framed transports (WebSocket, in-process pipe) deliver
    exactly one message per frame — no embedded newline. Fast-path that common
    case to skip the ``split("\\n")`` allocation and the per-line re-``strip``.
    Only the HTTP-batch transport ever packs multiple newline-delimited
    messages into one frame (contract C-FRAME/D3), which still takes the split
    path below.
    """
    if "\n" not in data:
        stripped = data.strip()
        if not stripped:
            return []
        return [parse_wire_message(stripped)]
    lines = data.strip().split("\n")
    return [parse_wire_message(line) for line in lines if line.strip()]


def serialize_wire_batch(messages: list[WireMessage]) -> str:
    """Serialize a batch of wire messages to newline-delimited JSON."""
    return "\n".join(serialize_wire_message(msg) for msg in messages)
