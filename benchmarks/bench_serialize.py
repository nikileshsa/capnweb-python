"""Serialize + parse micro-benchmarks across payload shapes.

Measures the pure codec (no session, no transport):

* ``serialize``   — Python value -> Serializer tree -> ``json.dumps`` string.
* ``deserialize`` — string -> ``json.loads`` -> Parser -> Python value.
* ``roundtrip``   — the two composed (the real wire cost of a value).

Plus a capability-bearing shape measured Python-only (TS ``serialize()`` also
refuses stubs standalone), through the ``Serializer`` tree with a trivial
counting exporter, to expose the export-table bookkeeping cost.
"""

from __future__ import annotations

from typing import Any

from capnweb.serializer import Serializer, serialize
from capnweb.parser import deserialize
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget

from benchmarks._harness import Result, bench
from benchmarks._payloads import payloads


# Per-shape inner counts: cheap shapes need many inner ops to amortize the
# clock; the 1 MiB shapes are expensive so a few inner ops suffice.
_INNER = {
    "small_scalar": 20_000,
    "short_string": 20_000,
    "nested_object": 4_000,
    "large_array_10k_ints": 40,
    "large_string_1mb": 200,
    "bytes_blob_1mb": 200,
    "error_value": 8_000,
}


class _CountingExporter:
    """Minimal Exporter: hands out fresh export IDs, no real table."""

    def __init__(self) -> None:
        self.n = 0

    def export_capability(self, stub: Any) -> int:
        self.n += 1
        return self.n

    def export_promise(self, stub: Any) -> int:
        self.n += 1
        return self.n

    def get_import(self, hook: Any) -> int | None:
        return None

    def unexport(self, ids: list[int]) -> None:
        pass

    def on_send_error(self, error: Any) -> Any:
        return None


class _Echo(RpcTarget):
    async def call(self, method: str, args: list[Any]) -> Any:  # pragma: no cover
        return args

    async def get_property(self, name: str) -> Any:  # pragma: no cover
        return None


def run() -> list[Result]:
    results: list[Result] = []
    shapes = payloads()

    for name, value in shapes.items():
        inner = _INNER[name]
        wire = serialize(value)
        nbytes = len(wire.encode("utf-8"))

        results.append(
            bench(
                f"serialize/{name}", "codec.serialize",
                lambda v=value: serialize(v),
                inner=inner, bytes_=nbytes,
                extra={"wire_bytes": nbytes},
            )
        )
        results.append(
            bench(
                f"deserialize/{name}", "codec.deserialize",
                lambda w=wire: deserialize(w),
                inner=inner, bytes_=nbytes,
                extra={"wire_bytes": nbytes},
            )
        )
        results.append(
            bench(
                f"roundtrip/{name}", "codec.roundtrip",
                lambda v=value: deserialize(serialize(v)),
                inner=inner, bytes_=nbytes,
                extra={"wire_bytes": nbytes},
            )
        )

    # Capability-bearing payload: a value tree carrying 8 stubs. Python-only
    # (measures Serializer tree + export bookkeeping; no JSON, since stubs are
    # not JSON-encodable standalone). TS counterpart is N/A.
    stub_payload = {
        "caps": [RpcStub(_Echo()) for _ in range(8)],
        "meta": {"n": 8, "kind": "capability-bearing"},
    }

    def _serialize_caps() -> Any:
        ser = Serializer(exporter=_CountingExporter())
        return ser.serialize(stub_payload)

    results.append(
        bench(
            "serialize/capability_payload_8stubs", "codec.capability",
            _serialize_caps, inner=4_000,
            extra={"stubs": 8, "note": "python-only; tree only, no json"},
        )
    )
    return results


if __name__ == "__main__":
    from benchmarks._harness import summarize

    print(summarize(run()))
