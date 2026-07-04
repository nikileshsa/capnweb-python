"""A2 stream unit tests: serialization behaviors the golden fixtures can't pin.

Covers (matrix 02 rows in parens):
* export tracking + unexport() rollback on serialize failure (25)
* per-property drop-on-unserializable with export rollback in errors (16)
* on_send_error invocation + returned stack honored (17)
* get_import() back-reference emission for stubs and promises (18/19)
* promises routed through export_promise, never the stub dedupe path (19)
* ExceptionGroup <-> AggregateError mapping (15/16, D4)
* traverse-then-drop for dangerous keys with a stub-embedding payload (23)
* parser hook collection into the payload + dispose on mid-parse failure (26)
* ["pipeline", id, path?, args?] / ["import", id, path, args] acceptance (19)
* standalone capnweb.serialize()/deserialize() helpers (27)
* sentinel semantics, bigint policy, naive-datetime-as-UTC, unpadded base64
  (6/8/9/10) and the D5 Headers/Request/Response types (12-14)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest

import capnweb
from capnweb.error import ErrorCode, RpcError
from capnweb.hooks import StubHook
from capnweb.parser import DANGEROUS_KEYS, Parser, deserialize
from capnweb.payload import RpcPayload
from capnweb.serializer import Serializer, serialize
from capnweb.stubs import RpcPromise, RpcStub
from capnweb.types import Headers, InvalidDate, Request, Response, Undefined


# ---------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------


class FakeHook(StubHook):
    """Minimal recording hook."""

    def __init__(self, label: str = "hook") -> None:
        self.label = label
        self.disposed = 0
        self.dup_count = 0
        self.calls: list[tuple[str, Any]] = []

    def call(self, path, args):
        self.calls.append(("call", (path, args)))
        return FakeHook(f"{self.label}.call")

    def map(self, path, captures, instructions):
        self.calls.append(("map", (path, captures, instructions)))
        return FakeHook(f"{self.label}.map")

    def get(self, path):
        self.calls.append(("get", path))
        return FakeHook(f"{self.label}.get")

    async def pull(self) -> RpcPayload:
        return RpcPayload.owned(None)

    def ignore_unhandled_rejections(self) -> None:
        pass

    def dispose(self) -> None:
        self.disposed += 1

    def dup(self):
        self.dup_count += 1
        return self

    def on_broken(self, callback: Any) -> None:
        pass


class RecordingExporter:
    """Exporter recording export/unexport traffic (full C-EXPORTER surface)."""

    def __init__(self, import_ids: dict[int, int] | None = None) -> None:
        self.next_id = 1
        self.exported: list[tuple[str, int]] = []
        self.unexported: list[list[int]] = []
        self.send_errors: list[RpcError] = []
        self.rewrite: RpcError | None = None
        self._import_ids = import_ids or {}

    def export_capability(self, stub) -> int:
        eid = self.next_id
        self.next_id += 1
        self.exported.append(("stub", eid))
        return eid

    def export_promise(self, stub) -> int:
        eid = self.next_id
        self.next_id += 1
        self.exported.append(("promise", eid))
        return eid

    def get_import(self, hook) -> int | None:
        return self._import_ids.get(id(hook))

    def unexport(self, ids: list[int]) -> None:
        self.unexported.append(list(ids))

    def on_send_error(self, error: RpcError) -> RpcError | None:
        self.send_errors.append(error)
        return self.rewrite

    def create_pipe(self, readable, guard_hook) -> int:
        raise NotImplementedError


class RecordingImporter:
    """Importer recording created hooks (full C-IMPORTER surface)."""

    def __init__(self, exports: dict[int, StubHook] | None = None) -> None:
        self.imported: list[FakeHook] = []
        self.promise_hooks: list[FakeHook] = []
        self._exports = exports or {}

    def import_capability(self, import_id: int) -> StubHook:
        hook = FakeHook(f"import-{import_id}")
        self.imported.append(hook)
        return hook

    def create_promise_hook(self, promise_id: int) -> StubHook:
        hook = FakeHook(f"promise-{promise_id}")
        self.promise_hooks.append(hook)
        return hook

    def get_export(self, export_id: int) -> StubHook | None:
        return self._exports.get(export_id)

    def get_pipe_readable(self, export_id: int) -> Any:
        raise NotImplementedError


class Unserializable:
    def __str__(self) -> str:
        return "<unserializable>"


# ---------------------------------------------------------------------------
# export rollback (row 25)
# ---------------------------------------------------------------------------


class TestExportRollback:
    def test_unexport_called_on_mid_serialize_failure(self) -> None:
        exporter = RecordingExporter()
        stub = RpcStub(FakeHook())
        with pytest.raises(TypeError, match="Cannot serialize value"):
            Serializer(exporter=exporter).serialize(
                {"a": stub, "b": Unserializable()}
            )
        assert exporter.exported == [("stub", 1)]
        assert exporter.unexported == [[1]]

    def test_no_unexport_on_success(self) -> None:
        exporter = RecordingExporter()
        stub = RpcStub(FakeHook())
        result = Serializer(exporter=exporter).serialize({"a": stub})
        assert result == {"a": ["export", 1]}
        assert exporter.unexported == []

    def test_promise_exports_rolled_back_too(self) -> None:
        exporter = RecordingExporter()
        promise = RpcPromise(FakeHook())
        with pytest.raises(TypeError):
            Serializer(exporter=exporter).serialize([promise, Unserializable()])
        assert exporter.exported == [("promise", 1)]
        assert exporter.unexported == [[1]]


# ---------------------------------------------------------------------------
# error encoding: on_send_error, per-prop drop + rollback (rows 16/17)
# ---------------------------------------------------------------------------


class TestErrorEncoding:
    def test_on_send_error_invoked_and_stack_redacted_by_default(self) -> None:
        exporter = RecordingExporter()
        err = RpcError("Error", "boom", stack="secret stack")
        result = Serializer(exporter=exporter).serialize(err)
        # Stack is only emitted when on_send_error deliberately attaches one.
        assert result == ["error", "Error", "boom"]
        assert len(exporter.send_errors) == 1

    def test_on_send_error_rewrite_stack_honored(self) -> None:
        exporter = RecordingExporter()
        exporter.rewrite = RpcError("Error", "redacted", stack="deliberate stack")
        err = RpcError("TypeError", "boom")
        result = Serializer(exporter=exporter).serialize(err)
        assert result == ["error", "Error", "redacted", "deliberate stack"]

    def test_rewrite_stack_normalized_to_slot3_with_props(self) -> None:
        exporter = RecordingExporter()
        exporter.rewrite = RpcError(
            "Error", "redacted", stack="s", properties={"k": 1}
        )
        result = Serializer(exporter=exporter).serialize(RpcError("Error", "x"))
        assert result == ["error", "Error", "redacted", "s", {"k": 1}]

    def test_unserializable_prop_dropped_error_survives(self) -> None:
        exporter = RecordingExporter()
        err = RpcError(
            "Error", "boom", properties={"good": 1, "bad": Unserializable()}
        )
        result = Serializer(exporter=exporter).serialize(err)
        assert result == ["error", "Error", "boom", None, {"good": 1}]

    def test_prop_drop_rolls_back_partial_exports(self) -> None:
        exporter = RecordingExporter()
        stub = RpcStub(FakeHook())
        # The prop value exports a stub, then fails: the partial walk's
        # exports must be spliced off and unexported (serialize.ts:398-416).
        err = RpcError(
            "Error", "boom", properties={"bad": [stub, Unserializable()]}
        )
        result = Serializer(exporter=exporter).serialize(err)
        assert result == ["error", "Error", "boom"]
        assert exporter.unexported == [[1]]

    def test_all_props_unserializable_gives_legacy_3_form(self) -> None:
        result = Serializer(exporter=RecordingExporter()).serialize(
            RpcError("Error", "boom", properties={"bad": Unserializable()})
        )
        assert result == ["error", "Error", "boom"]

    def test_cause_captured_recursively(self) -> None:
        err = RpcError(
            "Error", "outer", cause=RpcError("TypeError", "inner")
        )
        result = Serializer(exporter=RecordingExporter()).serialize(err)
        assert result == [
            "error", "Error", "outer", None,
            {"cause": ["error", "TypeError", "inner"]},
        ]

    def test_name_message_stack_keys_filtered_from_props(self) -> None:
        err = RpcError(
            "Error", "boom",
            properties={"name": "X", "message": "Y", "stack": "Z", "keep": 1},
        )
        result = Serializer(exporter=RecordingExporter()).serialize(err)
        assert result == ["error", "Error", "boom", None, {"keep": 1}]

    def test_generic_python_exceptions_map_to_js_names(self) -> None:
        s = Serializer(exporter=RecordingExporter())
        assert s.serialize(TypeError("bad type")) == ["error", "TypeError", "bad type"]
        assert s.serialize(ValueError("nope")) == ["error", "Error", "nope"]
        assert Serializer(exporter=RecordingExporter()).serialize(
            RecursionError("deep")
        ) == ["error", "RangeError", "deep"]


class TestExceptionGroupAggregateError:
    def test_exception_group_encodes_as_aggregate_error(self) -> None:
        group = ExceptionGroup("many", [ValueError("e1"), TypeError("e2")])
        result = Serializer(exporter=RecordingExporter()).serialize(group)
        assert result == [
            "error", "AggregateError", "many", None,
            {"errors": [[["error", "Error", "e1"], ["error", "TypeError", "e2"]]]},
        ]

    def test_aggregate_error_decodes_with_errors_list(self) -> None:
        wire = (
            '["error","AggregateError","many",null,'
            '{"errors":[[["error","Error","e1"],["error","TypeError","e2"]]]}]'
        )
        err = deserialize(wire)
        assert isinstance(err, RpcError)
        assert err.name == "AggregateError"
        assert err.errors is not None
        assert [e.name for e in err.errors] == ["Error", "TypeError"]
        assert [e.message for e in err.errors] == ["e1", "e2"]


# ---------------------------------------------------------------------------
# get_import back-refs + export_promise routing (rows 18/19)
# ---------------------------------------------------------------------------


class TestBackReferences:
    def test_stub_pointing_at_peer_emits_import_backref(self) -> None:
        hook = FakeHook()
        exporter = RecordingExporter(import_ids={id(hook): 7})
        result = Serializer(exporter=exporter).serialize(RpcStub(hook))
        assert result == ["import", 7]
        assert exporter.exported == []  # no new export!

    def test_promise_pointing_at_peer_emits_pipeline(self) -> None:
        hook = FakeHook()
        exporter = RecordingExporter(import_ids={id(hook): 7})
        result = Serializer(exporter=exporter).serialize(RpcPromise(hook))
        assert result == ["pipeline", 7]
        assert exporter.exported == []

    def test_local_promise_routes_through_export_promise(self) -> None:
        exporter = RecordingExporter()
        result = Serializer(exporter=exporter).serialize(RpcPromise(FakeHook()))
        assert result == ["promise", 1]
        assert exporter.exported == [("promise", 1)]

    def test_local_stub_still_exports(self) -> None:
        exporter = RecordingExporter()
        result = Serializer(exporter=exporter).serialize(RpcStub(FakeHook()))
        assert result == ["export", 1]
        assert exporter.exported == [("stub", 1)]


# ---------------------------------------------------------------------------
# parser: pipeline/import forms in values (row 19)
# ---------------------------------------------------------------------------


class TestPipelineAcceptance:
    def test_pipeline_bare_reference(self) -> None:
        target = FakeHook("target")
        importer = RecordingImporter(exports={3: target})
        payload = Parser(importer).parse(["pipeline", 3])
        assert isinstance(payload.value, RpcPromise)
        assert target.calls == [("get", [])]

    def test_import_bare_reference_dups(self) -> None:
        target = FakeHook("target")
        importer = RecordingImporter(exports={3: target})
        payload = Parser(importer).parse(["import", 3])
        assert isinstance(payload.value, RpcStub)
        assert target.dup_count == 1

    def test_pipeline_with_path(self) -> None:
        target = FakeHook("target")
        importer = RecordingImporter(exports={3: target})
        payload = Parser(importer).parse(["pipeline", 3, ["a", 0]])
        assert isinstance(payload.value, RpcPromise)
        assert target.calls == [("get", ["a", 0])]

    def test_pipeline_with_args_calls(self) -> None:
        target = FakeHook("target")
        importer = RecordingImporter(exports={3: target})
        payload = Parser(importer).parse(["pipeline", 3, ["method"], [42, "x"]])
        assert isinstance(payload.value, RpcPromise)
        assert len(target.calls) == 1
        kind, (path, args) = target.calls[0]
        assert kind == "call"
        assert path == ["method"]
        assert isinstance(args, RpcPayload)
        assert args.value == [42, "x"]

    def test_import_call_coercion_form(self) -> None:
        # 4-element ["import", id, path, args] coerces the result to a stub
        # (serialize.ts:884-903).
        target = FakeHook("target")
        importer = RecordingImporter(exports={3: target})
        payload = Parser(importer).parse(["import", 3, ["m"], []])
        assert isinstance(payload.value, RpcStub)
        assert target.calls[0][0] == "call"

    def test_missing_export_raises(self) -> None:
        importer = RecordingImporter()
        with pytest.raises(ValueError, match="no such entry on exports table: 9"):
            Parser(importer).parse(["pipeline", 9])
        with pytest.raises(ValueError, match="no such entry on exports table: 9"):
            Parser(importer).parse(["import", 9])

    def test_malformed_pipeline_rejected(self) -> None:
        importer = RecordingImporter(exports={3: FakeHook()})
        with pytest.raises(TypeError, match="unknown special value"):
            Parser(importer).parse(["pipeline", 3, "not-a-path"])
        with pytest.raises(TypeError, match="unknown special value"):
            Parser(importer).parse(["pipeline", 3, [True]])
        with pytest.raises(TypeError, match="unknown special value"):
            Parser(importer).parse(["pipeline", 3, [], "args", "extra"])

    def test_bool_capture_id_rejected_in_remap(self) -> None:
        # matrix 02 row 22 nuance: bool must not alias capture IDs 0/1.
        # B2 tightened remap failures to TS-strict throws (the session
        # aborts on protocol errors instead of soft-failing).
        target = FakeHook("target")
        importer = RecordingImporter(exports={3: target})
        with pytest.raises(TypeError, match="unknown map capture"):
            Parser(importer).parse(["remap", 3, [], [["import", True]], []])
        assert importer.imported == []


# ---------------------------------------------------------------------------
# traverse-then-drop + payload hook collection (rows 23/26)
# ---------------------------------------------------------------------------


class TestDangerousKeys:
    def test_denylist_contains_ts_object_prototype_names(self) -> None:
        for key in (
            "constructor", "toJSON", "toString", "valueOf", "hasOwnProperty",
            "isPrototypeOf", "propertyIsEnumerable", "toLocaleString",
            "__defineGetter__", "__defineSetter__", "__lookupGetter__",
            "__lookupSetter__", "__proto__", "__class__", "__reduce__",
        ):
            assert key in DANGEROUS_KEYS

    def test_dropped_key_value_still_imported(self) -> None:
        # The dropped value must still be evaluated so embedded stubs are
        # imported (and therefore releasable) -- serialize.ts:1003-1023.
        importer = RecordingImporter()
        payload = Parser(importer).parse(
            {"__proto__": ["export", 0], "safe": 1}
        )
        assert payload.value == {"safe": 1}
        assert len(importer.imported) == 1
        # The dropped stub is owned by the payload: disposing the payload
        # releases the import.
        payload.dispose()
        assert importer.imported[0].disposed == 1

    def test_all_dangerous_keys_dropped(self) -> None:
        wire = {k: 1 for k in DANGEROUS_KEYS}
        wire["keep"] = 2
        payload = Parser(RecordingImporter()).parse(wire)
        assert payload.value == {"keep": 2}


class TestPayloadHookCollection:
    def test_hooks_collected_into_payload(self) -> None:
        importer = RecordingImporter()
        payload = Parser(importer).parse(
            {"a": ["export", 1], "b": ["promise", 2]}
        )
        assert len(payload.stubs) == 1
        assert len(payload.promises) == 1
        parent, key, promise = payload.promises[0]
        assert key == "b"
        assert isinstance(promise, RpcPromise)

    def test_hooks_disposed_on_mid_parse_failure(self) -> None:
        importer = RecordingImporter()
        with pytest.raises(TypeError, match="unknown special value"):
            Parser(importer).parse([[["export", 1], ["zzz", 1]]])
        assert len(importer.imported) == 1
        assert importer.imported[0].disposed == 1


# ---------------------------------------------------------------------------
# standalone helpers (row 27)
# ---------------------------------------------------------------------------


class TestStandaloneHelpers:
    def test_roundtrip_basic_types(self) -> None:
        value = {
            "s": "hello",
            "n": 42,
            "arr": [1, [2, 3]],
            "b": b"hi",
            "d": datetime(2024, 7, 3, 12, 0, 0, tzinfo=timezone.utc),
            "u": Undefined,
            "bad_date": InvalidDate,
            "big": 2**60,
        }
        out = capnweb.deserialize(capnweb.serialize(value))
        assert out == value

    def test_serialize_stub_raises(self) -> None:
        with pytest.raises(RuntimeError, match="without an RPC session"):
            serialize(RpcStub(FakeHook()))

    def test_deserialize_stub_raises(self) -> None:
        with pytest.raises(RuntimeError, match="without an RPC session"):
            deserialize('["export",0]')

    def test_deserialize_error_gives_rpc_error_value(self) -> None:
        err = deserialize('["error","TypeError","bad"]')
        assert isinstance(err, RpcError)
        assert err.name == "TypeError"
        assert err.code is ErrorCode.INTERNAL  # derived convenience

    def test_deserialize_rejects_nan_literal(self) -> None:
        # Non-standard JSON constants (NaN/Infinity) must be rejected — Cap'n
        # Web encodes those as ["nan"]/["inf"]. The orjson-backed codec rejects
        # them natively (JSONDecodeError, a ValueError subclass). Assert the
        # behavior, not the backend-specific message.
        with pytest.raises(ValueError):
            deserialize("NaN")
        with pytest.raises(ValueError):
            deserialize("Infinity")

    def test_serialize_depth_limit_message(self) -> None:
        v: Any = 42
        for _ in range(64):
            v = [v]
        with pytest.raises(ValueError, match="exceeded maximum allowed depth"):
            serialize(v)


# ---------------------------------------------------------------------------
# sentinels, numbers, dates, bytes (rows 6/8/9/10)
# ---------------------------------------------------------------------------


class TestSentinelsAndScalars:
    def test_undefined_singleton_semantics(self) -> None:
        assert not Undefined
        assert repr(Undefined) == "undefined"
        assert Undefined is type(Undefined)()

    def test_undefined_distinct_from_none(self) -> None:
        assert deserialize('["undefined"]') is Undefined
        assert deserialize("null") is None
        assert serialize(Undefined) == '["undefined"]'
        assert serialize(None) == "null"

    def test_invalid_date_roundtrip(self) -> None:
        assert deserialize('["date",null]') is InvalidDate
        assert serialize(InvalidDate) == '["date",null]'

    def test_naive_datetime_treated_as_utc(self) -> None:
        naive = datetime(2024, 7, 3, 12, 0, 0)
        aware = naive.replace(tzinfo=timezone.utc)
        assert serialize(naive) == serialize(aware)

    def test_bigint_policy(self) -> None:
        assert serialize(2**53 - 1) == "9007199254740991"
        assert serialize(2**53) == '["bigint","9007199254740992"]'
        assert serialize(-(2**53)) == '["bigint","-9007199254740992"]'
        assert deserialize('["bigint","123"]') == 123

    def test_bigint_rejects_non_decimal_strings(self) -> None:
        for bad in ("not-a-number", "1_0", " 12", "0x10", "1.5", ""):
            with pytest.raises((ValueError, TypeError)):
                deserialize(json.dumps(["bigint", bad]))

    def test_bytes_unpadded_emit_tolerant_decode(self) -> None:
        assert serialize(b"hello") == '["bytes","aGVsbG8"]'
        assert deserialize('["bytes","aGVsbG8"]') == b"hello"
        assert deserialize('["bytes","aGVsbG8="]') == b"hello"

    def test_bytes_invalid_base64_rejected(self) -> None:
        with pytest.raises(Exception):
            deserialize('["bytes","!!!!"]')

    def test_bytearray_and_memoryview_serialize(self) -> None:
        assert serialize(bytearray(b"hi")) == serialize(b"hi")
        assert serialize(memoryview(b"hi")) == serialize(b"hi")

    def test_unsupported_types_raise(self) -> None:
        class Custom:
            pass

        with pytest.raises(TypeError, match="Cannot serialize value"):
            serialize(Custom())
        with pytest.raises(TypeError, match="Can't serialize RPC stubs"):
            serialize(lambda: 1)
        with pytest.raises(TypeError):
            serialize({1: "non-string-key"})
        with pytest.raises(TypeError, match="Cannot serialize value"):
            serialize(set([1]))


# ---------------------------------------------------------------------------
# D5 types: Headers / Request / Response (rows 12-14)
# ---------------------------------------------------------------------------


class TestHeadersType:
    def test_case_insensitive_multimap(self) -> None:
        h = Headers()
        h.append("X-Multi", "a")
        h.append("x-multi", "b")
        h.append("Content-Type", "text/plain")
        assert h.get("X-MULTI") == "a, b"
        assert list(h) == [
            ("content-type", "text/plain"),
            ("x-multi", "a, b"),
        ]

    def test_wire_roundtrip(self) -> None:
        wire = '["headers",[["content-type","text/plain"],["x-multi","a, b"]]]'
        h = deserialize(wire)
        assert isinstance(h, Headers)
        assert serialize(h) == wire

    def test_invalid_pairs_rejected(self) -> None:
        with pytest.raises(TypeError):
            deserialize('["headers",[["only-one"]]]')
        with pytest.raises(TypeError):
            deserialize('["headers",[["k",5]]]')

    def test_non_array_pairs_param_rejected(self) -> None:
        with pytest.raises(TypeError, match="unknown special value"):
            deserialize('["headers",{"k":"v"}]')


class TestRequestType:
    def test_default_request_roundtrip(self) -> None:
        wire = '["request","https://example.com/",{}]'
        req = deserialize(wire)
        assert isinstance(req, Request)
        assert req.method == "GET"
        assert req.body is None
        assert serialize(req) == wire

    def test_string_body_and_headers(self) -> None:
        wire = (
            '["request","https://example.com/api",{"method":"POST",'
            '"headers":[["content-type","text/plain"]],"body":"hello"}]'
        )
        req = deserialize(wire)
        assert req.method == "POST"
        assert req.body == "hello"
        assert req.headers.get("content-type") == "text/plain"
        assert serialize(req) == wire

    def test_bytes_body(self) -> None:
        req = deserialize('["request","https://x/",{"body":["bytes","aGk"]}]')
        assert req.body == b"hi"

    def test_invalid_body_type_rejected(self) -> None:
        with pytest.raises(TypeError, match="Request body must be"):
            deserialize('["request","https://x/",{"body":[[1,2]]}]')

    def test_headers_must_be_pairs_array(self) -> None:
        with pytest.raises(TypeError, match="array of pairs"):
            deserialize('["request","https://x/",{"headers":{"k":"v"}}]')

    def test_signal_unsupported(self) -> None:
        with pytest.raises(TypeError, match="AbortSignal"):
            deserialize('["request","https://x/",{"signal":true}]')

    def test_stream_body_requires_session(self) -> None:
        # B1 landed: stream bodies decode through the pipe machinery, which
        # the standalone NullImporter refuses (like TS NullImporter).
        with pytest.raises(RuntimeError, match="without an RPC session"):
            deserialize('["request","https://x/",{"body":["readable",1]}]')


class TestResponseType:
    def test_default_response_roundtrip(self) -> None:
        wire = '["response",null,{}]'
        resp = deserialize(wire)
        assert isinstance(resp, Response)
        assert resp.status == 200
        assert serialize(resp) == wire

    def test_status_and_text(self) -> None:
        wire = '["response",null,{"status":404,"statusText":"Not Found"}]'
        resp = deserialize(wire)
        assert resp.status == 404
        assert resp.status_text == "Not Found"
        assert serialize(resp) == wire

    def test_websocket_rejected_both_directions(self) -> None:
        with pytest.raises(TypeError, match="webSocket"):
            deserialize('["response",null,{"webSocket":true}]')
        resp = Response(extensions={"webSocket": object()})
        with pytest.raises(TypeError, match="webSocket"):
            serialize(resp)

    def test_invalid_body_rejected(self) -> None:
        with pytest.raises(TypeError, match="Response body must be"):
            deserialize('["response",[[1]],{}]')


class TestStreamTagsRefused:
    def test_blob_readable_writable_require_session(self) -> None:
        # B1 landed: these decode to live streams inside a session; the
        # standalone helper still refuses them loudly (TS NullImporter).
        for wire in (
            '["blob","text/plain",["readable",1]]',
            '["readable",1]',
            '["writable",1]',
        ):
            with pytest.raises(RuntimeError, match="without an RPC session"):
                deserialize(wire)


# ---------------------------------------------------------------------------
# error decode details (rows 15/16)
# ---------------------------------------------------------------------------


class TestErrorDecode:
    def test_name_preserved_for_js_error_types(self) -> None:
        err = deserialize('["error","RangeError","out of range"]')
        assert err.name == "RangeError"

    def test_unknown_name_collapses_to_error(self) -> None:
        err = deserialize('["error","SomeCustomError","custom"]')
        assert err.name == "Error"

    def test_python_code_names_survive_for_code_derivation(self) -> None:
        err = deserialize('["error","not_found","missing"]')
        assert err.name == "not_found"
        assert err.code is ErrorCode.NOT_FOUND

    def test_stack_preserved_on_decode_never_reemitted(self) -> None:
        err = deserialize('["error","Error","x","stacktrace"]')
        assert err.stack == "stacktrace"
        assert serialize(err) == '["error","Error","x"]'

    def test_props_recursively_evaluated(self) -> None:
        err = deserialize(
            '["error","Error","x",null,{"when":["date",1720000000000],"blob":["bytes","aGk"]}]'
        )
        assert err.properties["when"] == datetime.fromtimestamp(
            1720000000, tz=timezone.utc
        )
        assert err.properties["blob"] == b"hi"

    def test_cause_exposed_as_attribute(self) -> None:
        err = deserialize(
            '["error","Error","outer",null,{"cause":["error","TypeError","inner"]}]'
        )
        assert isinstance(err.cause, RpcError)
        assert err.cause.name == "TypeError"
        assert "cause" not in err.properties

    def test_malformed_props_bag_rejected(self) -> None:
        with pytest.raises(TypeError, match="unknown special value"):
            deserialize('["error","Error","x",null,[["not","an","object"]]]')

    def test_name_message_stack_keys_skipped_on_decode(self) -> None:
        err = deserialize(
            '["error","Error","real",null,{"name":"Fake","message":"fake","keep":1}]'
        )
        assert err.name == "Error"
        assert err.message == "real"
        assert err.properties == {"keep": 1}
