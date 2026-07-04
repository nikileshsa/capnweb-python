"""Parser (Evaluator) for converting wire format to Python objects.

This is the ONE deserialization stack (locked decision D1): the session path
and the standalone ``capnweb.deserialize()`` helper both go through
``Parser``. It mirrors the TS ``Evaluator`` (serialize.ts:607-1029):

* strict rejection of unknown tags, bare ``[]``, and malformed special forms
  with ``TypeError("unknown special value: ...")`` -- never silently
  delivered as plain lists;
* ``["date", null]`` decodes to the ``InvalidDate`` sentinel and
  ``["undefined"]`` to the ``Undefined`` sentinel (C-SENTINELS);
* errors revive wire-faithfully (name/message/stack/properties/cause), with
  recursive property evaluation, name/message/stack key filtering, unknown
  names collapsing to "Error" per the TS ERROR_TYPES allowlist, and malformed
  props bags rejected (serialize.ts:685-709);
* ``["pipeline", id, path?, args?]`` and the 4-element
  ``["import", id, path, args]`` call-coercion form are accepted in values
  (serialize.ts:826-903);
* dangerous object keys (Python dunders + the TS Object.prototype names) are
  dropped with traverse-then-drop semantics so embedded stubs still
  import+release (serialize.ts:1003-1023);
* hooks created during the parse are collected into the resulting RpcPayload
  and disposed if the parse fails midway (serialize.ts:613-622);
* ``["writable", id]`` evaluates to a proxy ``RpcWritableStream`` (flow
  controlled), ``["readable", id]`` to the consume-once pipe readable with a
  disposal guard hook, and ``["blob", type, readable]`` to a
  delivery-blocking promise that collects the pipe into a ``Blob``
  (parity stream B1; serialize.ts:809-824, 977-1000).
"""

from __future__ import annotations

import asyncio
import base64
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final, Protocol

from capnweb import _json
from capnweb.error import REVIVABLE_ERROR_NAMES, RpcError
from capnweb.hooks import ErrorStubHook, PayloadStubHook, PromiseStubHook
from capnweb.payload import RpcPayload
from capnweb.streams import (
    ReadableStreamGuardHook,
    RpcReadableStream,
    RpcWritableStream,
)
from capnweb.stubs import RpcPromise, RpcStub
from capnweb.types import Blob, Headers, InvalidDate, Request, Response, Undefined
from capnweb.wire import (
    MAX_PARSE_DEPTH,
    WireDate,
    WireError,
    WirePipeline,
    WireRemap,
)
from capnweb.wire import (
    is_int_not_bool as _is_int_not_bool,
)

if TYPE_CHECKING:
    from capnweb.hooks import StubHook

# Resource-exhaustion decode bounds (audit F4/F5). Defaults mirror
# RpcSessionConfig; the session threads its configured values into the
# Parser, while standalone decodes (NullImporter) use these directly. Kept
# here — not imported from config — to avoid an import cycle and to keep the
# pure-decode stack self-contained.
DEFAULT_MAX_ARRAY_LEN: Final[int] = 1_000_000
DEFAULT_MAX_BLOB_BYTES: Final[int] = 64 * 1024 * 1024

# Security: object keys dropped on decode (traverse-then-drop). The union of
# the Python pickle/attribute gadget surface and the TS Object.prototype
# membership test + "toJSON" (serialize.ts:1003-1023), so a Python middlebox
# can't launder a prototype-pollution payload to a JS peer (matrix 02 row 23).
DANGEROUS_KEYS: Final[frozenset[str]] = frozenset({
    # Python-threat-model dunders
    "__proto__", "__class__", "__dict__", "__slots__",
    "__getattr__", "__setattr__", "__delattr__",
    "__reduce__", "__reduce_ex__", "__getstate__", "__setstate__",
    # JS Object.prototype members + toJSON (TS evaluator drop set)
    "constructor", "toJSON", "toString", "valueOf", "hasOwnProperty",
    "isPrototypeOf", "propertyIsEnumerable", "toLocaleString",
    "__defineGetter__", "__defineSetter__", "__lookupGetter__",
    "__lookupSetter__",
})

_BIGINT_RE = re.compile(r"^-?[0-9]+$")


def _json_repr(value: Any) -> str:
    """Compact JSON repr for error messages, mirroring TS JSON.stringify."""
    try:
        return _json.dumps(value)
    except Exception:
        return repr(value)


class _Malformed(Exception):
    """Internal marker: tagged form failed validation (TS switch `break`)."""


class Importer(Protocol):
    """Protocol for objects that can import capabilities (contract C-IMPORTER).

    This is typically implemented by RpcSession (Client/Server).
    """

    def import_capability(self, import_id: int) -> StubHook:
        """Import a capability the peer exported to us."""
        ...

    def create_promise_hook(self, promise_id: int) -> StubHook:
        """Create a promise hook for a future value."""
        ...

    def get_export(self, export_id: int) -> StubHook | None:
        """Look up our own export (sender is passing our object back)."""
        ...

    def get_pipe_readable(self, export_id: int) -> Any:
        """Retrieve the readable end of a pipe (parity stream B1)."""
        ...


class NullImporter:
    """Importer that refuses all capability traffic (standalone decode).

    Mirrors the TS ``NullImporter`` (serialize.ts:558-571).
    """

    def import_capability(self, import_id: int) -> StubHook:
        raise RuntimeError("Cannot deserialize RPC stubs without an RPC session.")

    def create_promise_hook(self, promise_id: int) -> StubHook:
        raise RuntimeError("Cannot deserialize RPC stubs without an RPC session.")

    def get_export(self, export_id: int) -> StubHook | None:
        return None

    def get_pipe_readable(self, export_id: int) -> Any:
        raise RuntimeError("Cannot retrieve pipe readable without an RPC session.")


_NULL_IMPORTER = NullImporter()


class Parser:
    """Converts wire format to Python objects for RPC reception.

    This class (called Evaluator in TypeScript) collects every hook it
    creates into the resulting ``RpcPayload`` so a mid-parse failure disposes
    partially-imported capabilities instead of leaking peer-side refcounts.
    """

    def __init__(
        self,
        importer: Importer,
        *,
        errors_as_values: bool = False,
        max_array_len: int = DEFAULT_MAX_ARRAY_LEN,
        max_blob_bytes: int = DEFAULT_MAX_BLOB_BYTES,
    ) -> None:
        """Initialize with an importer.

        Args:
            importer: The RpcSession (or NullImporter) that manages IDs.
            errors_as_values: When True, ``["error", ...]`` decodes to a bare
                ``RpcError`` value (TS delivers Error objects); the session
                default wraps errors in ``RpcStub(ErrorStubHook)``.
            max_array_len: F5 — max element count of a single decoded wire
                array; a wider array aborts the decode (resource exhaustion).
                The session threads its configured value here.
            max_blob_bytes: F4 — max total bytes accumulated while collecting
                a streamed blob before the containing value is rejected.
        """
        self.importer = importer
        self._errors_as_values = errors_as_values
        self._max_array_len = max_array_len
        self._max_blob_bytes = max_blob_bytes
        self._stubs: list[RpcStub] = []
        self._promises: list[tuple[Any, str | int, RpcPromise]] = []
        # Delivery-blocking promises (Blob collection): the session awaits
        # and substitutes these before the value reaches application code.
        self._substitutions: list[tuple[Any, str | int, RpcPromise]] = []

    def parse(self, wire_value: Any) -> RpcPayload:
        """Parse a wire expression into a Python value wrapped in RpcPayload.

        On failure, every stub/promise imported before the exception is
        disposed (serialize.ts:613-622).

        Raises:
            ValueError / TypeError: malformed expression or depth exceeded.
        """
        self._stubs = []
        self._promises = []
        self._substitutions = []
        # P1/P3 (perf): wire dataclasses (WireError/WireDate/WirePipeline/
        # WireRemap) are produced by wire_expression_from_json ONLY at the root
        # of an expression — container interiors are handed over as raw JSON.
        # Normalize the root back to raw JSON here, ONCE, so the per-node hot
        # loop in _parse_value carries no wire-dataclass isinstance check.
        if isinstance(wire_value, (WireError, WireDate, WirePipeline, WireRemap)):
            wire_value = wire_value.to_json()
        payload = RpcPayload.owned(None)
        try:
            payload.value = self._parse_value(
                wire_value,
                depth=0,
                parent=payload,
                key="value",
                error_values=self._errors_as_values,
            )
        except BaseException:
            for stub in self._stubs:
                stub.dispose()
            for _parent, _key, promise in self._promises:
                promise.dispose()
            for _parent, _key, promise in self._substitutions:
                promise.dispose()
            self._stubs = []
            self._promises = []
            self._substitutions = []
            raise
        payload.stubs = self._stubs
        payload.promises = self._promises
        payload.substitutions = self._substitutions
        self._stubs = []
        self._promises = []
        self._substitutions = []
        return payload

    def parse_payload_value(self, wire_value: Any) -> RpcPayload:
        """Convenience alias for :meth:`parse`."""
        return self.parse(wire_value)

    # -- implementation --------------------------------------------------------

    def _parse_value(  # noqa: C901
        self,
        value: Any,
        *,
        depth: int,
        parent: Any,
        key: str | int,
        error_values: bool,
    ) -> Any:
        # Security: prevent stack overflow from deeply nested payloads.
        if depth > MAX_PARSE_DEPTH:
            raise ValueError(
                f"Expression exceeds maximum depth ({MAX_PARSE_DEPTH}). "
                "Possible malicious payload or circular reference."
            )

        # P3 (perf): fast identity checks for the exact primitive types
        # json.loads emits — these beat the isinstance tuple-scan on the hot
        # path and dominate real payloads. Wire dataclasses are normalized at
        # the root in parse(); interiors are always raw JSON (P1), so there is
        # deliberately NO per-node wire-dataclass check here anymore.
        vtype = type(value)
        if vtype is str or vtype is int or vtype is float or vtype is bool or value is None:
            return value

        if isinstance(value, list):
            # Escaped array: [[...]] -- unwrap and evaluate contents in place
            # (checked BEFORE the tag switch, serialize.ts:640-647).
            if len(value) == 1 and isinstance(value[0], list):
                inner = value[0]
                # F5: bound array width. O(1) check on the header length that
                # already drives the loop below — no extra tree-walk.
                if len(inner) > self._max_array_len:
                    raise ValueError(
                        f"Array length {len(inner)} exceeds maximum "
                        f"({self._max_array_len}). Possible malicious payload."
                    )
                result: list[Any] = []
                for i, item in enumerate(inner):
                    result.append(
                        self._parse_value(
                            item, depth=depth + 1, parent=result, key=i,
                            error_values=error_values,
                        )
                    )
                return result

            if value and isinstance(value[0], str):
                try:
                    return self._parse_tagged(
                        value, depth=depth, parent=parent, key=key,
                        error_values=error_values,
                    )
                except _Malformed:
                    pass

            # Bare [], [1,2], unknown tags, malformed forms: hard error,
            # exactly like TS (serialize.ts:1002).
            raise TypeError(f"unknown special value: {_json_repr(value)}")

        if isinstance(value, dict):
            result_dict: dict[str, Any] = {}
            for k, v in value.items():
                if k in DANGEROUS_KEYS:
                    # Traverse-then-drop: still evaluate the value so embedded
                    # stubs are imported (and thus releasable), then discard
                    # (serialize.ts:1003-1023).
                    self._parse_value(
                        v, depth=depth + 1, parent=result_dict, key=k,
                        error_values=error_values,
                    )
                    continue
                result_dict[k] = self._parse_value(
                    v, depth=depth + 1, parent=result_dict, key=k,
                    error_values=error_values,
                )
            return result_dict

        # Non-JSON Python objects pass through (already-hydrated values).
        return value

    def _parse_tagged(  # noqa: C901
        self,
        value: list[Any],
        *,
        depth: int,
        parent: Any,
        key: str | int,
        error_values: bool,
    ) -> Any:
        """Decode a tagged special form; raises _Malformed on shape errors."""
        tag = value[0]

        if tag == "bigint":
            if len(value) == 2 and isinstance(value[1], str):
                if not _BIGINT_RE.match(value[1]):
                    raise ValueError(f"Cannot convert {value[1]} to a bigint")
                return int(value[1])
            raise _Malformed

        if tag == "date":
            if len(value) == 2:
                if value[1] is None:
                    # PR #152: ["date", null] is the invalid-date sentinel.
                    return InvalidDate
                if isinstance(value[1], (int, float)) and not isinstance(value[1], bool):
                    return datetime.fromtimestamp(value[1] / 1000, tz=UTC)
            raise _Malformed

        if tag == "bytes":
            if len(value) == 2 and isinstance(value[1], str):
                b64 = value[1]
                pad = -len(b64) % 4
                if pad:
                    b64 += "=" * pad
                return base64.b64decode(b64, validate=True)
            if len(value) == 2 and isinstance(value[1], (bytes, bytearray, memoryview)):
                # Raw bytes: only producible by a custom-encoding transport
                # at the "jsonCompatibleWithBytes" level (serialize.ts:775-779).
                # JSON wire input can never contain Python bytes, so this adds
                # no laxity to the network-facing parse path.
                return bytes(value[1])
            raise _Malformed

        if tag == "error":
            err = self._parse_error(value, depth=depth)
            if error_values:
                return err
            return RpcStub(ErrorStubHook(err))

        if tag == "undefined":
            if len(value) == 1:
                return Undefined
            raise _Malformed

        # TS applies no length validation to these three (serialize.ts:716-721).
        if tag == "inf":
            return float("inf")
        if tag == "-inf":
            return float("-inf")
        if tag == "nan":
            return float("nan")

        if tag == "headers":
            # Only validate that the pairs param is an array; the Headers
            # constructor performs the per-pair type checks (serialize.ts:723-730).
            if len(value) == 2 and isinstance(value[1], list):
                return Headers(value[1])
            raise _Malformed

        if tag == "request":
            return self._parse_request(value, depth=depth, error_values=error_values)

        if tag == "response":
            return self._parse_response(
                value, depth=depth, parent=parent, key=key, error_values=error_values
            )

        if tag == "writable":
            # A WritableStream export from the sender: import it and build a
            # proxy that forwards writes with flow control
            # (serialize.ts:977-987). The hook is tracked for disposal — the
            # proxy and the payload share ONE import ref, exactly like TS
            # (ImportHook.dispose is idempotent).
            if len(value) == 2 and _is_int_not_bool(value[1]):
                hook = self.importer.import_capability(value[1])
                self._stubs.append(RpcStub(hook))
                return RpcWritableStream._from_hook(hook)
            raise _Malformed

        if tag == "readable":
            # The readable end of a pipe previously created by ["pipe"];
            # consume-once (serialize.ts:989-1000). The guard hook only
            # exists so an unconsumed stream is canceled when the payload
            # is disposed.
            if len(value) == 2 and _is_int_not_bool(value[1]):
                stream = self.importer.get_pipe_readable(value[1])
                self._stubs.append(RpcStub(ReadableStreamGuardHook.create(stream)))
                return stream
            raise _Malformed

        if tag == "blob":
            return self._parse_blob(
                value, depth=depth, parent=parent, key=key,
                error_values=error_values,
            )

        if tag in ("import", "pipeline"):
            return self._parse_import_or_pipeline(
                value, depth=depth, parent=parent, key=key
            )

        if tag == "remap":
            # ["remap", importId, propertyPath, captures, instructions]
            if (
                len(value) == 5
                and _is_int_not_bool(value[1])
                and (value[2] is None or isinstance(value[2], list))
                and isinstance(value[3], list)
                and isinstance(value[4], list)
            ):
                return self._parse_remap(value, parent=parent, key=key)
            raise _Malformed

        if tag in ("export", "promise"):
            if len(value) == 2 and _is_int_not_bool(value[1]):
                if tag == "promise":
                    return self._parse_promise(value, parent=parent, key=key)
                return self._parse_export(value)
            raise _Malformed

        # Unknown tag.
        raise _Malformed

    # -- capabilities ------------------------------------------------------------

    def _parse_export(self, wire_expr: list[Any]) -> Any:
        """["export", id]: the peer is exporting a capability; import it."""
        import_hook = self.importer.import_capability(wire_expr[1])
        stub = RpcStub(import_hook)
        self._stubs.append(stub)
        return stub

    def _parse_promise(
        self, wire_expr: list[Any], *, parent: Any, key: str | int
    ) -> Any:
        """["promise", id]: a promise that will be resolved later."""
        promise_hook = self.importer.create_promise_hook(wire_expr[1])
        promise = RpcPromise(promise_hook)
        self._promises.append((parent, key, promise))
        return promise

    def _parse_import_or_pipeline(
        self, wire_expr: list[Any], *, depth: int, parent: Any, key: str | int
    ) -> Any:
        """["import"|"pipeline", id, path?, args?] per serialize.ts:826-903.

        The id references OUR export table (the sender is passing our own
        object back). "pipeline" evaluates to a promise; "import" coerces the
        result to a stub. The optional path selects a property; the optional
        args make it a call.
        """
        if not (2 <= len(wire_expr) <= 4) or not _is_int_not_bool(wire_expr[1]):
            raise _Malformed

        hook = self.importer.get_export(wire_expr[1])
        if hook is None:
            raise ValueError(f"no such entry on exports table: {wire_expr[1]}")

        is_promise = wire_expr[0] == "pipeline"

        def wrap(result_hook: StubHook) -> Any:
            if is_promise:
                promise = RpcPromise(result_hook)
                self._promises.append((parent, key, promise))
                return promise
            stub = RpcStub(result_hook)
            self._stubs.append(stub)
            return stub

        if len(wire_expr) == 2:
            # Just referencing the export itself. hook.get([]) guarantees a
            # promise hook; dup() a stub hook.
            return wrap(hook.get([]) if is_promise else hook.dup())

        path = wire_expr[2]
        if not isinstance(path, list) or not all(
            isinstance(part, str) or _is_int_not_bool(part) for part in path
        ):
            raise _Malformed

        if len(wire_expr) == 3:
            return wrap(hook.get(list(path)))

        args = wire_expr[3]
        if not isinstance(args, list):
            raise _Malformed

        # Args build a separate payload via a fresh evaluator, wrapped like
        # TS's subEval.evaluate([args]).
        sub_parser = Parser(self.importer, errors_as_values=self._errors_as_values)
        args_payload = sub_parser.parse([args])
        return wrap(hook.call(list(path), args_payload))

    # -- blob (B1; serialize.ts:809-824) ---------------------------------------------

    def _parse_blob(
        self,
        wire_expr: list[Any],
        *,
        depth: int,
        parent: Any,
        key: str | int,
        error_values: bool,
    ) -> Any:
        """["blob", type, readableExpression] -> delivery-blocking promise.

        The wire format is strictly a pipe-backed stream (the encoder always
        streams blob bytes). Mirrors streamToBlobPromise: the payload
        delivery machinery awaits the promise and substitutes the real Blob
        before user code sees the value, so delivery waits for full content.
        """
        if len(wire_expr) != 3 or not isinstance(wire_expr[1], str):
            raise _Malformed
        content_type = wire_expr[1]
        content = self._parse_value(
            wire_expr[2], depth=depth + 1, parent=parent, key=key,
            error_values=error_values,
        )
        if not isinstance(content, RpcReadableStream):
            raise TypeError("Blob content must be serialized as a ReadableStream.")

        # We're committing to consume the stream: mark it so the guard hook
        # tracked above won't cancel it on payload disposal.
        content._acquire_for_pump()

        max_blob_bytes = self._max_blob_bytes

        async def collect() -> StubHook:
            parts: list[bytes] = []
            total = 0
            async for chunk in content:
                if isinstance(chunk, (bytes, bytearray, memoryview)):
                    chunk_bytes = bytes(chunk)
                    # F4: bound total accumulation. O(1) running-sum check —
                    # reject the containing value the moment the cap is passed
                    # so a large/unending blob can't pin memory.
                    total += len(chunk_bytes)
                    if total > max_blob_bytes:
                        raise ValueError(
                            f"Blob size exceeds maximum ({max_blob_bytes} "
                            "bytes). Possible malicious payload."
                        )
                    parts.append(chunk_bytes)
                else:
                    raise TypeError(
                        "Blob stream produced a non-bytes chunk: "
                        f"{type(chunk).__name__}"
                    )
            return PayloadStubHook(
                RpcPayload.owned(Blob(content_type, b"".join(parts)))
            )

        promise = RpcPromise(PromiseStubHook(asyncio.ensure_future(collect())))
        self._substitutions.append((parent, key, promise))
        return promise

    # -- errors --------------------------------------------------------------------

    def _parse_error(self, wire_expr: list[Any], *, depth: int) -> RpcError:
        """Revive ["error", name, message, stack?, props?] (contract C-ERROR)."""
        if not (
            len(wire_expr) >= 3
            and isinstance(wire_expr[1], str)
            and isinstance(wire_expr[2], str)
        ):
            raise _Malformed

        name = wire_expr[1]
        message = wire_expr[2]
        # TS assigns the stack only when it is a string (serialize.ts:691-693);
        # other values in the slot are ignored, not errors.
        stack = (
            wire_expr[3]
            if len(wire_expr) > 3 and isinstance(wire_expr[3], str)
            else None
        )

        properties: dict[str, Any] = {}
        cause: Any = None
        if len(wire_expr) >= 5:
            bag = wire_expr[4]
            # Malformed props bag (non-object) falls through to the
            # unknown-special-value throw, like TS (serialize.ts:697-699).
            if not isinstance(bag, dict):
                raise _Malformed
            for k, v in bag.items():
                if k in ("name", "message", "stack"):
                    continue
                # Prop values are recursively evaluated; nested errors are
                # delivered as RpcError VALUES (TS delivers Error objects).
                parsed = self._parse_value(
                    v, depth=depth + 1, parent=properties, key=k, error_values=True
                )
                if k == "cause":
                    cause = parsed
                else:
                    properties[k] = parsed

        # Revive via the ERROR_TYPES allowlist; unknown names collapse to
        # "Error" exactly like `new Error(msg)` (serialize.ts:685-690).
        # REVIVABLE_ERROR_NAMES additionally keeps the six Python legacy code
        # strings alive so Python<->Python error codes survive the hop.
        revived_name = name if name in REVIVABLE_ERROR_NAMES else "Error"
        return RpcError(
            revived_name, message, stack=stack, properties=properties, cause=cause
        )

    # -- HTTP types (D5; serialize.ts:732-807) ---------------------------------------

    def _parse_request(
        self, wire_expr: list[Any], *, depth: int, error_values: bool
    ) -> Request:
        if len(wire_expr) != 3 or not isinstance(wire_expr[1], str):
            raise _Malformed
        url = wire_expr[1]
        init = wire_expr[2]
        if not isinstance(init, dict):
            raise _Malformed

        body: Any = None
        if init.get("body"):
            body = self._parse_value(
                init["body"], depth=depth + 1, parent=init, key="body",
                error_values=error_values,
            )
            # Acceptable types: null | string | bytes | stream
            # (serialize.ts:745-757).
            if not (body is None or isinstance(body, (str, bytes, RpcReadableStream))):
                raise TypeError("Request body must be of type ReadableStream.")

        if init.get("signal"):
            # AbortSignal is not supported (serialize.ts:750-755 requires an
            # actual AbortSignal instance, which cannot exist here).
            raise TypeError("Request signal must be of type AbortSignal.")

        headers_val = init.get("headers")
        if headers_val is not None and not isinstance(headers_val, list):
            raise TypeError("Request headers must be serialized as an array of pairs.")
        headers = Headers(headers_val or [])

        method = init.get("method", "GET")
        if not isinstance(method, str):
            raise TypeError(f"Request method must be a string, got {method!r}")

        consumed = {"method", "headers", "body", "signal", "cache", "redirect", "integrity"}
        extensions = {k: v for k, v in init.items() if k not in consumed}
        return Request(
            url=url,
            method=method,
            headers=headers,
            body=body,
            redirect=init.get("redirect", "follow"),
            integrity=init.get("integrity", ""),
            cache=init.get("cache", "default"),
            extensions=extensions,
        )

    def _parse_response(
        self,
        wire_expr: list[Any],
        *,
        depth: int,
        parent: Any,
        key: str | int,
        error_values: bool,
    ) -> Response:
        if len(wire_expr) != 3:
            raise _Malformed

        body = self._parse_value(
            wire_expr[1], depth=depth + 1, parent=parent, key=key,
            error_values=error_values,
        )
        # Acceptable types: null | string | bytes | stream
        # (serialize.ts:838-846).
        if not (body is None or isinstance(body, (str, bytes, RpcReadableStream))):
            raise TypeError("Response body must be of type ReadableStream.")

        init = wire_expr[2]
        if not isinstance(init, dict):
            raise _Malformed

        if init.get("webSocket"):
            # Cloudflare Workers extension; not supported for serialization
            # (serialize.ts:794-798).
            raise TypeError("Can't deserialize a Response containing a webSocket.")

        headers_val = init.get("headers")
        if headers_val is not None and not isinstance(headers_val, list):
            raise TypeError("Request headers must be serialized as an array of pairs.")

        status = init.get("status", 200)
        if not _is_int_not_bool(status):
            raise TypeError(f"Response status must be an int, got {status!r}")
        status_text = init.get("statusText", "")
        if not isinstance(status_text, str):
            raise TypeError(f"Response statusText must be a string, got {status_text!r}")

        consumed = {"status", "statusText", "headers", "webSocket"}
        extensions = {k: v for k, v in init.items() if k not in consumed}
        return Response(
            body=body,
            status=status,
            status_text=status_text,
            headers=Headers(headers_val or []),
            extensions=extensions,
        )

    # -- remap (.map()) ----------------------------------------------------------

    def _parse_remap(
        self, wire_expr: list[Any], *, parent: Any, key: str | int
    ) -> Any:
        """["remap", importId, propertyPath, captures, instructions].

        The result promise is registered for SUBSTITUTION before delivery
        (TS pushes it onto ``this.promises`` and the delivery machinery
        resolves it in place, serialize.ts:940-946): the application must
        see the mapped values, never the promise.
        """
        result_hook = self.evaluate_remap(wire_expr)
        promise = RpcPromise(result_hook)
        self._substitutions.append((parent, key, promise))
        return promise

    def evaluate_remap(self, wire_expr: list[Any]) -> StubHook:
        """Evaluate a raw remap expression into its result hook.

        ONE evaluation path (TS Evaluator "remap" case, serialize.ts:905-950)
        shared by value decoding (:meth:`_parse_remap`) and the session's
        top-level push handling. Failure modes are TS-strict: protocol
        errors THROW (which aborts the session), no soft error stubs.

        Captures resolve against the importer:
        ``["import", id]`` = sender's import = our export (``get_export`` +
        dup); ``["export", id]`` = sender exports a new stub to us
        (``import_capability`` — which a MapApplicator importer hard-fails,
        matrix 04 row 15). A ``null`` propertyPath is leniently accepted as
        ``[]`` (pre-B2 Python senders emitted it).

        Instructions stay RAW JSON and are passed through untouched
        (serialize.ts:944-946).
        """
        import_id = wire_expr[1]
        raw_path = wire_expr[2] if wire_expr[2] is not None else []
        captures_wire = wire_expr[3]
        instructions = wire_expr[4]

        hook = self.importer.get_export(import_id)
        if hook is None:
            raise ValueError(f"no such entry on exports table: {import_id}")

        if not all(
            isinstance(p, str) or _is_int_not_bool(p) for p in raw_path
        ):
            raise TypeError(f"unknown special value: {_json_repr(wire_expr)}")

        captures: list[StubHook] = []
        try:
            for cap in captures_wire:
                if (
                    not isinstance(cap, list)
                    or len(cap) != 2
                    or cap[0] not in ("import", "export")
                    or not _is_int_not_bool(cap[1])
                ):
                    raise TypeError(f"unknown map capture: {_json_repr(cap)}")

                if cap[0] == "export":
                    captures.append(self.importer.import_capability(cap[1]))
                else:  # "import"
                    exp = self.importer.get_export(cap[1])
                    if exp is None:
                        raise ValueError(
                            f"no such entry on exports table: {cap[1]}"
                        )
                    captures.append(exp.dup())
        except BaseException:
            for cap in captures:
                cap.dispose()
            raise

        return hook.map(list(raw_path), captures, instructions)


def deserialize(value: str) -> Any:
    """Deserialize a value serialized using ``serialize()``.

    Standalone helper (serialize.ts:1034-1038): stubs/pipes cannot be
    materialized without a session and raise instead. Errors are delivered
    as ``RpcError`` values.
    """

    tree = _json.loads(value)
    payload = Parser(_NULL_IMPORTER, errors_as_values=True).parse(tree)
    payload.dispose()  # should be a no-op, but mirror the TS helper
    return payload.value
