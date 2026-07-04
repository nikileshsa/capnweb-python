"""Serializer (Devaluator) for converting Python objects to wire format.

This is the ONE serialization stack (locked decision D1): the session path
and the standalone ``capnweb.serialize()`` helper both go through
``Serializer``. It mirrors the TS ``Devaluator`` (serialize.ts:91-535) +
``typeForRpc`` (core.ts:51-152):

* exact-type matching (subclasses of serializable types are rejected, like
  TS's exact-prototype switch), tuples serialize as escaped arrays like lists;
* depth limit 64 with the TS error message;
* unsupported types raise ``TypeError("Cannot serialize value: ...")``;
* ints outside +/-2^53 auto-promote to ``["bigint", str]`` (locked Python
  mapping, matrix 02 row 8 -- Python has a single int type);
* naive datetimes are treated as UTC; ``InvalidDate`` emits ``["date", null]``;
* bytes emit UNPADDED base64 (serialize.ts:227);
* errors emit the wire-faithful ``["error", name, message, stack?, props?]``
  forms byte-exactly per serialize.ts:396-440, with recursive property
  devaluation, per-property drop-on-unserializable + export rollback, and
  ``on_send_error`` rewrite support;
* stubs probe ``Exporter.get_import(hook)`` first and emit ``["import", id]``
  back-references; promises route through ``export_promise`` and emit
  ``["pipeline", importId, path?]`` when pointing back at the peer;
* every export is tracked and rolled back via ``Exporter.unexport(ids)`` if
  serialization fails midway (serialize.ts:108-127).
"""

from __future__ import annotations

import base64
import math
from datetime import UTC, datetime
from typing import Any, Protocol

from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.streams import (
    ReadableStreamGuardHook,
    RpcReadableStream,
    RpcWritableStream,
    WritableStreamHook,
)
from capnweb.stubs import RpcPromise, RpcStub
from capnweb.types import (
    Blob,
    Headers,
    InvalidDate,
    Request,
    Response,
    RpcTarget,
    Undefined,
)

# JavaScript's Number.MAX_SAFE_INTEGER = 2^53 - 1
JS_MAX_SAFE_INTEGER = 9007199254740991
JS_MIN_SAFE_INTEGER = -9007199254740991

# TS Devaluator throws at depth >= 64 (serialize.ts:132-136).
MAX_SERIALIZATION_DEPTH = 64

_DEPTH_ERROR_MESSAGE = (
    "Serialization exceeded maximum allowed depth. "
    "(Does the message contain cycles?)"
)

# F6: generic message substituted for an UNEXPECTED exception's free text when
# ``redact_internal_errors`` is on, so internal detail (paths/secrets) can't
# reach an untrusted peer. The exception type/name is preserved separately.
_REDACTED_INTERNAL_MESSAGE = "internal error"

# Python exception class name -> natural JS error name. Everything else maps
# to "Error" (matrix 02 row 15). RpcError carries its own wire name.
_PY_EXC_JS_NAME: dict[str, str] = {
    "TypeError": "TypeError",
    "SyntaxError": "SyntaxError",
    "NameError": "ReferenceError",
    "RecursionError": "RangeError",
}

# Request init extension keys, in TS emission order (serialize.ts:297-317).
_REQUEST_EXT_ORDER = (
    "mode",
    "credentials",
    "referrer",
    "referrerPolicy",
    "keepalive",
    "cf",
    "encodeResponseBody",
)

# Response init extension keys, in TS emission order (serialize.ts:342-348).
_RESPONSE_EXT_ORDER = ("cf", "encodeBody")


class Exporter(Protocol):
    """Protocol for objects that can export capabilities (contract C-EXPORTER).

    Implemented by RpcSession (Client/Server); MapBuilder implements the same
    protocol with recorder semantics.
    """

    def export_capability(self, stub: RpcStub | RpcPromise) -> int:
        """Export a capability and return its export ID (dedupes by hook)."""
        ...

    def export_promise(self, stub: RpcStub | RpcPromise) -> int:
        """Export a promise; always allocates a FRESH export ID."""
        ...

    def get_import(self, hook: Any) -> int | None:
        """If ``hook`` points back at the peer, return its import ID."""
        ...

    def unexport(self, ids: list[int]) -> None:
        """Roll back exports made during a failed serialization."""
        ...

    def create_pipe(self, readable: Any, guard_hook: Any) -> int:
        """Create a pipe for a stream (parity stream B1)."""
        ...

    def on_send_error(self, error: RpcError) -> RpcError | None:
        """Optionally rewrite an error before it is serialized."""
        ...


class NullExporter:
    """Exporter that refuses all capability traffic (standalone encode).

    Mirrors the TS ``NullExporter`` (serialize.ts:50-66).
    """

    def export_capability(self, stub: RpcStub | RpcPromise) -> int:
        raise RuntimeError("Cannot serialize RPC stubs without an RPC session.")

    def export_promise(self, stub: RpcStub | RpcPromise) -> int:
        raise RuntimeError("Cannot serialize RPC stubs without an RPC session.")

    def get_import(self, hook: Any) -> int | None:
        return None

    def unexport(self, ids: list[int]) -> None:
        pass

    def create_pipe(self, readable: Any, guard_hook: Any) -> int:
        raise RuntimeError("Cannot create pipes without an RPC session.")

    def on_send_error(self, error: RpcError) -> RpcError | None:
        return None


_NULL_EXPORTER = NullExporter()


class Serializer:
    """Converts Python objects to wire format for RPC transmission.

    This class (called Devaluator in TypeScript) is a per-message, stateless
    transformation apart from the export-rollback tracking; all durable state
    lives in the RpcSession (Exporter).
    """

    __slots__ = ("_exports", "_stream_stubs", "encoding_level", "exporter")

    def __init__(self, exporter: Any, encoding_level: str = "string") -> None:
        self.exporter = exporter
        # Encoding level (serialize.ts:95-110): at "jsonCompatibleWithBytes"
        # bytes values stay raw inside ["bytes", ...] (no base64); all other
        # forms are identical to "string"/"jsonCompatible". The
        # "structuredClonable" level is rejected at session construction
        # (JS-host structured clone has no Python analog).
        self.encoding_level = encoding_level
        self._exports: list[int] = []
        # Per-message memo: id(RpcWritableStream) -> RpcStub(WritableStreamHook).
        # The same writable appearing twice in one message reuses one hook
        # (mirrors TS getHookForWritableStream memoization); our transient
        # stub reference is dropped after serialize() so the export table
        # holds the only durable refs.
        self._stream_stubs: dict[int, RpcStub] = {}

    # -- Exporter surface adapters --------------------------------------------
    # get_import / unexport / on_send_error are part of the frozen C-EXPORTER
    # contract but may not be implemented by every exporter yet (A1's session
    # lands them separately); missing methods degrade to safe no-ops.

    def _get_import(self, hook: Any) -> int | None:
        get_import = getattr(self.exporter, "get_import", None)
        if get_import is None:
            return None
        return get_import(hook)

    def _unexport(self, ids: list[int]) -> None:
        unexport = getattr(self.exporter, "unexport", None)
        if unexport is not None:
            try:
                unexport(ids)
            except Exception:
                # Probably a side effect of the original error; ignore it
                # (mirrors serialize.ts:117-121).
                pass

    def _on_send_error(self, error: RpcError) -> RpcError | None:
        hook = getattr(self.exporter, "on_send_error", None)
        if hook is None:
            return None
        rewritten = hook(error)
        return rewritten if isinstance(rewritten, RpcError) else None

    def _should_redact_internal(
        self, exc: BaseException, error: RpcError
    ) -> bool:
        """F6: decide whether an error's free-text message must be redacted.

        Redaction applies only when the exporter opts in
        (``redact_internal_errors``; the real session defaults it True, bare
        test exporters default it off so unit-level serialization stays
        transparent) AND the error was auto-wrapped from an UNEXPECTED
        (non-``RpcError``) application exception at a call boundary
        (``_internal_origin`` — set by :meth:`RpcError.wrap_internal` at
        hooks.py). This is the exact surface where server-side detail
        (filesystem paths, secrets embedded in an exception's ``str()``)
        would otherwise reach an untrusted peer.

        Deliberate ``RpcError`` protocol signals (raised by app code or by the
        library — e.g. "no such entry on exports table") are NOT redacted:
        their messages are diagnostics that echo protocol/peer state, not
        server secrets.
        """
        if not getattr(self.exporter, "redact_internal_errors", False):
            return False
        return getattr(exc, "_internal_origin", False) is True

    # -- public API ------------------------------------------------------------

    def serialize(self, value: Any) -> Any:
        """Serialize a Python value to wire format.

        Tracks all exports made during the walk; on failure they are rolled
        back via ``Exporter.unexport`` before the exception propagates
        (serialize.ts:108-127).
        """
        self._exports = []
        self._stream_stubs = {}
        try:
            return self._serialize_value(value, 0)
        except BaseException:
            if self._exports:
                self._unexport(self._exports)
            # NOTE (serialize.ts:123-125): this rollback only releases
            # exports. Pipes created via create_pipe (ReadableStreams and
            # Blobs) have already sent a ["pipe"] frame and started pumping;
            # there is no inverse on the Exporter interface, so they leak
            # until session shutdown. Same wart as TS.
            raise
        finally:
            # Drop our transient hook refs; on success the export-table dups
            # keep the sinks alive, on failure unexport() already released
            # them so this final dispose triggers the disposed-without-close
            # abort (streams.ts:100-113 parity).
            for stub in self._stream_stubs.values():
                stub.dispose()
            self._stream_stubs = {}

    def serialize_payload(self, payload: RpcPayload) -> Any:
        """Serialize an RpcPayload (ensures ownership first)."""
        payload.ensure_deep_copied()
        return self.serialize(payload.value)

    # -- implementation ----------------------------------------------------------

    def _serialize_value(self, value: Any, depth: int) -> Any:  # noqa: C901
        if depth >= MAX_SERIALIZATION_DEPTH:
            raise ValueError(_DEPTH_ERROR_MESSAGE)

        # Primitives. Exact-type checks mirror TS's exact-prototype rule
        # (core.ts:79-98); bool before int because bool subclasses int.
        if value is None:
            return None
        vtype = type(value)
        if vtype is bool or vtype is str:
            return value
        if vtype is float:
            if math.isnan(value):
                return ["nan"]
            if math.isinf(value):
                return ["inf"] if value > 0 else ["-inf"]
            return value
        if vtype is int:
            # Locked Python mapping (matrix 02 row 8): ints beyond the JS safe
            # range auto-promote to bigint so a JS peer never silently rounds.
            if value > JS_MAX_SAFE_INTEGER or value < JS_MIN_SAFE_INTEGER:
                return ["bigint", str(value)]
            return value

        if value is Undefined:
            return ["undefined"]
        if value is InvalidDate:
            return ["date", None]

        if isinstance(value, RpcStub) and not isinstance(value, RpcPromise):
            return self._serialize_stub(value)
        if isinstance(value, RpcPromise):
            return self._serialize_promise(value)

        if isinstance(value, BaseException):
            return self._serialize_error(value, depth)

        if vtype is list or vtype is tuple:
            # Literal arrays (and tuples -- locked tuple policy, matrix 02
            # row 5) are escaped by wrapping in an outer one-element array.
            return [[self._serialize_value(item, depth + 1) for item in value]]

        if vtype is dict:
            for key in value.keys():
                if not isinstance(key, str):
                    raise TypeError(
                        f"JSON object keys must be strings, got {type(key).__name__}"
                    )
            return {
                key: self._serialize_value(val, depth + 1)
                for key, val in value.items()
            }

        if vtype in (bytes, bytearray, memoryview):
            raw = bytes(value)
            if self.encoding_level == "jsonCompatibleWithBytes":
                # Keep bytes raw; the transport passes them through natively
                # (serialize.ts:206-210).
                return ["bytes", raw]
            # Unpadded base64, byte-identical to TS (serialize.ts:213, 227).
            encoded = base64.b64encode(raw).decode("ascii").rstrip("=")
            return ["bytes", encoded]

        if vtype is datetime:
            # Naive datetimes are UTC everywhere (locked policy, row 9).
            if value.tzinfo is None:
                value = value.replace(tzinfo=UTC)
            return ["date", round(value.timestamp() * 1000)]

        if isinstance(value, RpcWritableStream):
            return self._serialize_writable(value)

        if isinstance(value, RpcReadableStream):
            return self._serialize_readable(value)

        if vtype is Blob:
            return self._serialize_blob(value)

        if vtype is Headers:
            return ["headers", [[k, v] for k, v in value]]

        if vtype is Request:
            return self._serialize_request(value, depth)

        if vtype is Response:
            return self._serialize_response(value, depth)

        if isinstance(value, RpcPayload):
            value.ensure_deep_copied()
            return self._serialize_value(value.value, depth)

        if isinstance(value, RpcTarget) or callable(value):
            # Raw targets/functions must be wrapped into stubs by the payload
            # deep-copy before serialization (TS: "Can't serialize RPC stubs
            # in this context.", serialize.ts:452-453).
            raise TypeError("Can't serialize RPC stubs in this context.")

        # Unsupported type (core.ts "unsupported" -> serialize.ts:140-147).
        try:
            msg = f"Cannot serialize value: {value}"
        except Exception:
            msg = "Cannot serialize value: (couldn't stringify value)"
        raise TypeError(msg)

    # -- capabilities ------------------------------------------------------------

    def _serialize_stub(self, stub: RpcStub) -> list[Any]:
        hook = stub._hook
        import_id = self._get_import(hook)
        if import_id is not None:
            # Back-reference: the peer's own capability goes back as
            # ["import", id] -- no new export (serialize.ts:456-468).
            return ["import", import_id]
        export_id = self.exporter.export_capability(stub)
        self._exports.append(export_id)
        return ["export", export_id]

    def _serialize_promise(self, promise: RpcPromise) -> list[Any]:
        # TS unwrapStubAndPath (serialize.ts:455-467): probe get_import with
        # the RAW hook — the pending property path is emitted on the wire,
        # never resolved locally, so lazy pipelining stays lazy.
        hook = promise._raw_hook
        path = list(promise._path)
        import_id = self._get_import(hook)
        if import_id is not None:
            # Promise pointing back at the peer (or a recorder capture):
            # pure pipelining, no export (serialize.ts:459-465).
            if path:
                return ["pipeline", import_id, path]
            return ["pipeline", import_id]
        # Local promise: always a FRESH export via export_promise, never the
        # deduped stub path (serialize.ts:471-477, 530-531). A pending path
        # is resolved into a dedicated hook first (serialize.ts:471-473);
        # export_promise_hook takes ownership of it (TS exportPromise
        # semantics — the exporter owns the hook it's handed).
        if path:
            resolved = hook.get(path)
            export_hook = getattr(self.exporter, "export_promise_hook", None)
            if export_hook is not None:
                export_id = export_hook(resolved)
            else:
                # Fallback for exporters without the ownership-transfer
                # surface: export a wrapper (the exporter dups internally),
                # then dispose our temporary reference so the export table
                # holds the ONLY durable ref — no one-ref leak.
                temp = RpcPromise(resolved)
                try:
                    export_id = self.exporter.export_promise(temp)
                finally:
                    temp.dispose()
        else:
            export_id = self.exporter.export_promise(promise)
        self._exports.append(export_id)
        return ["promise", export_id]

    # -- streams (B1; serialize.ts:499-519, 358-369) ---------------------------------

    def _serialize_writable(self, stream: RpcWritableStream) -> list[Any]:
        """WritableStream -> export-table capability with a 3-method vtable
        (write/close/abort): ``["writable", exportId]`` (serialize.ts:499-506,
        528-536). Creating the hook locks the stream (TS getWriter())."""
        stub = self._stream_stubs.get(id(stream))
        if stub is None:
            hook = WritableStreamHook.create(stream)  # locks the stream
            stub = RpcStub(hook)
            self._stream_stubs[id(stream)] = stub
        export_id = self.exporter.export_capability(stub)
        self._exports.append(export_id)
        return ["writable", export_id]

    def _serialize_readable(self, stream: RpcReadableStream) -> list[Any]:
        """ReadableStream -> pipe side-effect + ``["readable", importId]``.

        WART (serialize.ts:123-125, 392-394, shared with TS): create_pipe
        SENDS the ["pipe"] frame and starts pumping DURING serialization.
        If serialization fails later in the walk, the export rollback cannot
        un-send the pipe — it leaks until session shutdown.
        """
        hook = ReadableStreamGuardHook.create(stream)
        import_id = self.exporter.create_pipe(stream, hook)
        return ["readable", import_id]

    def _serialize_blob(self, blob: Blob) -> list[Any]:
        """Blob -> ``["blob", type, ["readable", importId]]``.

        Blobs are ALWAYS streamed through a pipe — no inline fast path even
        for small blobs (serialize.ts:358-369; protocol.md:175-177), since
        the message must be serialized synchronously.
        """
        readable = blob.stream()
        hook = ReadableStreamGuardHook.create(readable)
        import_id = self.exporter.create_pipe(readable, hook)
        return ["blob", blob.type, ["readable", import_id]]

    # -- errors --------------------------------------------------------------------

    def _serialize_error(self, exc: BaseException, depth: int) -> list[Any]:
        """Serialize any exception per serialize.ts:372-440 (contract C-ERROR)."""
        error = self._as_rpc_error(exc)

        # F6: an ``on_send_error`` hook, if set, always runs and takes
        # precedence. Only when it declines to rewrite (returns None) does the
        # default redaction apply — replacing the free-text message of an
        # UNEXPECTED (non-RpcError) exception with a generic string so internal
        # detail (filesystem paths, secrets) never leaks to an untrusted peer.
        # The exception type/name is preserved; deliberate RpcError protocol
        # signals raised by app code keep their message.
        rewritten = self._on_send_error(error)
        if rewritten is not None:
            error = rewritten
        elif self._should_redact_internal(exc, error):
            error = RpcError(error.name, _REDACTED_INTERNAL_MESSAGE)

        props: dict[str, Any] = {}

        def capture_prop(key: str, val: Any) -> None:
            # Per-property drop-on-unserializable with export rollback
            # (serialize.ts:398-416): the error itself must always make it
            # through.
            exports_before = len(self._exports)
            try:
                props[key] = self._serialize_value(val, depth + 1)
            except Exception:
                props.pop(key, None)
                if len(self._exports) > exports_before:
                    tail = self._exports[exports_before:]
                    del self._exports[exports_before:]
                    self._unexport(tail)

        for key, val in error.properties.items():
            if key in ("name", "message", "stack"):
                continue
            capture_prop(key, val)
        if error.cause is not None:
            capture_prop("cause", error.cause)

        # Legacy 3/4-element forms when there are no extras; with props the
        # stack slot is normalized to null so props is always index 4
        # (serialize.ts:429-440). A stack is only emitted when on_send_error
        # deliberately attached one.
        result: list[Any] = ["error", error.name, error.message]
        if props:
            result.append(
                rewritten.stack if (rewritten is not None and rewritten.stack) else None
            )
            result.append(props)
        elif rewritten is not None and rewritten.stack:
            result.append(rewritten.stack)
        return result

    @staticmethod
    def _as_rpc_error(exc: BaseException) -> RpcError:
        """Adapt any Python exception to the wire error surface."""
        if isinstance(exc, RpcError):
            return exc
        if isinstance(exc, BaseExceptionGroup):
            # ExceptionGroup <-> AggregateError (locked decision D4).
            return RpcError(
                "AggregateError",
                exc.message,
                properties={"errors": list(exc.exceptions)},
                cause=exc.__cause__,
            )
        name = _PY_EXC_JS_NAME.get(type(exc).__name__, "Error")
        return RpcError(name, str(exc), cause=exc.__cause__)

    # -- HTTP types (D5; serialize.ts:235-355) ---------------------------------------

    def _serialize_request(self, req: Request, depth: int) -> list[Any]:
        init: dict[str, Any] = {}
        if req.method != "GET":
            init["method"] = req.method
        pairs = [[k, v] for k, v in req.headers]
        if pairs:
            init["headers"] = pairs
        if req.body is not None:
            init["body"] = self._serialize_value(req.body, depth + 1)
            if "duplex" in req.extensions:
                init["duplex"] = req.extensions["duplex"]
            elif isinstance(req.body, RpcReadableStream):
                # The Fetch spec (and Chrome) requires duplex when the body
                # is a stream (serialize.ts:257-262).
                init["duplex"] = "half"
        if req.cache != "default":
            init["cache"] = req.cache
        if req.redirect != "follow":
            init["redirect"] = req.redirect
        if req.integrity:
            init["integrity"] = req.integrity
        for key in _REQUEST_EXT_ORDER:
            if key in req.extensions:
                init[key] = req.extensions[key]
        for key, val in req.extensions.items():
            if key != "duplex" and key not in init:
                init[key] = val
        return ["request", req.url, init]

    def _serialize_response(self, resp: Response, depth: int) -> list[Any]:
        if resp.extensions.get("webSocket"):
            raise TypeError("Can't serialize a Response containing a webSocket.")
        body = self._serialize_value(resp.body, depth + 1)
        init: dict[str, Any] = {}
        if resp.status != 200:
            init["status"] = resp.status
        if resp.status_text:
            init["statusText"] = resp.status_text
        pairs = [[k, v] for k, v in resp.headers]
        if pairs:
            init["headers"] = pairs
        for key in _RESPONSE_EXT_ORDER:
            if key in resp.extensions:
                init[key] = resp.extensions[key]
        for key, val in resp.extensions.items():
            if key != "webSocket" and key not in init:
                init[key] = val
        return ["response", body, init]


def serialize(value: Any) -> str:
    """Serialize a value using Cap'n Web's underlying serialization.

    Standalone helper (serialize.ts:541-543): cannot serialize RPC stubs or
    streams, but supports all basic data types.
    """
    from capnweb import _json

    tree = Serializer(exporter=_NULL_EXPORTER).serialize(value)
    # Compact, raw-UTF-8, no NaN/Infinity literals — byte-identical to TS
    # JSON.stringify (see capnweb._json).
    return _json.dumps(tree)
