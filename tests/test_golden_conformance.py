"""Golden wire-format conformance suite (Cap'n Web parity plan, stream S0).

Oracle: ``tests/interop/fixtures/golden_wire.json`` — generated from the
reference TypeScript implementation's standalone ``serialize()`` /
``deserialize()`` helpers by ``tests/interop/gen_golden_fixtures.ts``
(capnweb 0.9.0). Regenerate with ``npx tsx gen_golden_fixtures.ts`` from
``tests/interop/`` after upstream bumps.

Per-entry contract (see the generator header for full semantics):

* ``expect == "roundtrip"``   — Python must DECODE ``wire`` via the session
  parser path (``capnweb.parser.Parser`` with a null importer) and RE-ENCODE
  via ``capnweb.serializer.Serializer`` (null exporter) to something
  semantically equal to ``reencoded_wire`` (the TS canonical form). Where the
  gap matrices say byte parity is expected, the re-encoded wire string must
  match EXACTLY.
* ``expect == "decode_error"`` — the TS reference throws; Python must raise.
* ``expect == "encode_error"`` — the TS reference throws on encode; Python
  must reject the equivalent input (where a Python-equivalent input exists).

XFAIL CONVENTION (read this before editing)
-------------------------------------------
Rows the CURRENT Python stack is known to fail are listed in the
``*_XFAIL`` tables below and are marked
``pytest.mark.xfail(strict=False, reason="parity gap: <matrix row ref>")``.

* ``strict=False`` keeps the suite green today; when a stream (A1/A2/B1-B3)
  fixes a row it flips to XPASS instead of breaking the build.
* When your stream turns a row green, DELETE its entry from the table in the
  same change — that hardens the row into a strict assertion forever.
* Matrix row refs point into ``docs/architecture/capnweb-parity/``
  (e.g. "matrix 02 row 15" = 02-serialization-value-types.md, gap-table row 15).

``BYTE_PARITY_EXEMPT`` rows are NOT gaps: they document the locked Python
type-mapping policy (matrix 02 row 8 — a single ``int`` type means bigint-ness
of |n| <= 2^53 values cannot survive a Python hop; semantic equality still holds).
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, Callable

import pytest

from capnweb.hooks import ErrorStubHook
from capnweb.parser import NullImporter, Parser
from capnweb.serializer import NullExporter, Serializer
from capnweb.stubs import RpcStub

FIXTURE_PATH = (
    Path(__file__).parent / "interop" / "fixtures" / "golden_wire.json"
)

_FIXTURE_DOC = json.loads(FIXTURE_PATH.read_text())
ENTRIES: list[dict[str, Any]] = _FIXTURE_DOC["entries"]
_BY_NAME = {e["name"]: e for e in ENTRIES}


# ---------------------------------------------------------------------------
# xfail tables — one entry per currently-failing fixture row.
# Value = matrix row reference used in the xfail reason.
#
# A2 (serialization unification) turned every matrix-Part-2 row green; the
# tables are empty and every fixture row is a strict assertion. Add entries
# ONLY for newly-added fixture rows owned by not-yet-landed streams.
# ---------------------------------------------------------------------------

SEMANTIC_XFAIL: dict[str, str] = {}

# Rows where semantic decode is fine today but the emitted wire string is not
# byte-identical to the TS canonical form.
BYTE_PARITY_XFAIL: dict[str, str] = {
    **SEMANTIC_XFAIL,
}

# Locked Python type-mapping policy, NOT parity gaps (never expected to flip):
# Python has one int type, so bigint-ness inside +/-2^53 cannot round-trip and
# raw JSON numbers beyond +/-2^53 are deliberately promoted to ["bigint", ...]
# on emit (matrix 02 row 8: "the right Python mapping — document it").
BYTE_PARITY_EXEMPT: dict[str, str] = {
    "int_2pow53_as_number": "matrix 02 row 8 (int auto-promotion policy)",
    "int_neg_2pow53_as_number": "matrix 02 row 8 (int auto-promotion policy)",
    "bigint_negative": "matrix 02 row 8 (small bigint decodes to plain int)",
}

DECODE_ERROR_XFAIL: dict[str, str] = {}

ENCODE_ERROR_XFAIL: dict[str, str] = {}


def _deep_list(depth: int) -> Any:
    v: Any = 42
    for _ in range(depth):
        v = [v]
    return v


class _CustomClass:
    def __init__(self) -> None:
        self.x = 1


# Python-equivalent inputs for encode_error fixture rows.
ENCODE_ERROR_INPUTS: dict[str, Callable[[], Any]] = {
    "array_nest_depth64": lambda: _deep_list(64),
    "array_nest_depth65": lambda: _deep_list(65),
    "function_encode": lambda: (lambda: 1),
    "class_instance_encode": lambda: _CustomClass(),
}

# encode_error rows with no meaningful Python-equivalent input.
ENCODE_ERROR_SKIP: dict[str, str] = {
    "symbol_encode": "JS Symbol has no Python analog",
    "map_object_encode": (
        "JS Map's Python analog is dict, which is legitimately serializable"
    ),
    "request_post_body_headers": (
        "TS-platform stream-body limitation; Python Request type lands in B3 "
        "(matrix 02 row 13) and string bodies are covered by "
        "request_post_string_body_decode"
    ),
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _decode(wire: str) -> Any:
    """Decode a wire string through the session parser path."""
    return Parser(NullImporter()).parse(json.loads(wire)).value


def _normalize(value: Any) -> Any:
    """Unwrap decoded artifacts that cannot re-enter the Serializer directly.

    The session parser materializes ``["error", ...]`` as
    ``RpcStub(ErrorStubHook)``; for re-encoding we extract the underlying
    RpcError (which the Serializer handles).
    """
    if isinstance(value, RpcStub) and isinstance(value._hook, ErrorStubHook):
        return value._hook.error
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in value.items()}
    return value


def _encode(value: Any) -> str:
    """Encode a Python value through the session serializer path."""
    tree = Serializer(exporter=NullExporter()).serialize(value)
    # Match JSON.stringify: compact separators, raw (non-ascii-escaped) unicode.
    return json.dumps(tree, separators=(",", ":"), ensure_ascii=False)


def _b64_bytes(s: str) -> bytes:
    pad = -len(s) % 4
    return base64.b64decode(s + "=" * pad)


def _canon(node: Any) -> Any:
    """Canonicalize a wire TREE for semantic comparison.

    Collapses representational freedom the matrices explicitly allow:
    * ``["bytes", b64]`` compares by decoded bytes (padded vs unpadded emit);
    * ``["bigint", s]`` and raw JSON integers compare by numeric value
      (Python's single int type; matrix 02 row 8);
    * int-valued floats compare equal to ints.
    Everything else (tags, shapes, key sets) must match exactly.
    """
    if node is None:
        return ("null",)
    if isinstance(node, bool):
        return ("bool", node)
    if isinstance(node, (int, float)):
        if isinstance(node, float) and node.is_integer():
            node = int(node)
        return ("num", node)
    if isinstance(node, str):
        return ("str", node)
    if isinstance(node, list):
        if len(node) == 2 and node[0] == "bytes" and isinstance(node[1], str):
            return ("bytes", _b64_bytes(node[1]))
        if len(node) == 2 and node[0] == "bigint" and isinstance(node[1], str):
            return ("num", int(node[1]))
        return ("arr", tuple(_canon(v) for v in node))
    if isinstance(node, dict):
        return ("obj", tuple(sorted((k, _canon(v)) for k, v in node.items())))
    return ("opaque", repr(node))


def _params(
    predicate: Callable[[dict[str, Any]], bool],
    xfail_table: dict[str, str],
) -> list[Any]:
    params = []
    for entry in ENTRIES:
        if not predicate(entry):
            continue
        marks = []
        if entry["name"] in xfail_table:
            marks.append(
                pytest.mark.xfail(
                    strict=False,
                    reason=f"parity gap: {xfail_table[entry['name']]}",
                )
            )
        params.append(pytest.param(entry, id=entry["name"], marks=marks))
    return params


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_fixture_provenance() -> None:
    """The committed fixture must come from the parity target TS version."""
    assert _FIXTURE_DOC["capnweb_version"].startswith("0.9."), (
        "golden_wire.json must be regenerated against capnweb 0.9.x "
        f"(found {_FIXTURE_DOC['capnweb_version']})"
    )
    assert _FIXTURE_DOC["entry_count"] == len(ENTRIES)


@pytest.mark.parametrize(
    "entry",
    _params(lambda e: e["expect"] == "roundtrip", SEMANTIC_XFAIL),
)
def test_roundtrip_semantic(entry: dict[str, Any]) -> None:
    """Decode the TS wire, re-encode, compare semantically vs TS canonical."""
    decoded = _decode(entry["wire"])
    reencoded = _encode(_normalize(decoded))
    # Target: TS's own canonical re-encode when available, else the original
    # wire (reencode of some revived platform types throws in TS — see notes).
    target = entry.get("reencoded_wire", entry["wire"])
    assert _canon(json.loads(reencoded)) == _canon(json.loads(target)), (
        f"semantic mismatch\n  wire:      {entry['wire']}\n"
        f"  ts canon:  {target}\n  py emit:   {reencoded}\n"
        f"  notes:     {entry['notes']}"
    )


@pytest.mark.parametrize(
    "entry",
    _params(
        lambda e: (
            e["expect"] == "roundtrip"
            and "reencoded_wire" in e
            and e["name"] not in BYTE_PARITY_EXEMPT
        ),
        BYTE_PARITY_XFAIL,
    ),
)
def test_roundtrip_byte_parity(entry: dict[str, Any]) -> None:
    """Where deterministic, Python must emit the exact TS canonical string."""
    decoded = _decode(entry["wire"])
    reencoded = _encode(_normalize(decoded))
    assert reencoded == entry["reencoded_wire"], (
        f"byte parity mismatch\n  ts canon: {entry['reencoded_wire']}\n"
        f"  py emit:  {reencoded}\n  notes:    {entry['notes']}"
    )


@pytest.mark.parametrize(
    "entry",
    _params(lambda e: e["expect"] == "decode_error", DECODE_ERROR_XFAIL),
)
def test_decode_error(entry: dict[str, Any]) -> None:
    """The TS reference rejects this wire; the session parser must raise."""
    with pytest.raises(Exception):
        _decode(entry["wire"])


@pytest.mark.parametrize(
    "entry",
    _params(lambda e: e["expect"] == "encode_error", ENCODE_ERROR_XFAIL),
)
def test_encode_error(entry: dict[str, Any]) -> None:
    """The TS reference rejects this input on encode; Python must too."""
    name = entry["name"]
    if name in ENCODE_ERROR_SKIP:
        pytest.skip(ENCODE_ERROR_SKIP[name])
    factory = ENCODE_ERROR_INPUTS.get(name)
    assert factory is not None, (
        f"encode_error fixture row {name!r} has no Python-equivalent input; "
        "add it to ENCODE_ERROR_INPUTS or ENCODE_ERROR_SKIP"
    )
    with pytest.raises(Exception):
        _encode(factory())


def test_tuple_policy() -> None:
    """Tuples serialize as escaped arrays, exactly like lists (matrix 02
    row 5, locked A2 decision): an application 2-tuple like ("date", "x")
    must never leak onto the wire as an unescaped special form.
    """
    result = Serializer(exporter=NullExporter()).serialize((1, 2))
    assert result == [[1, 2]]
    nested = Serializer(exporter=NullExporter()).serialize(("date", "x"))
    assert nested == [["date", "x"]]
