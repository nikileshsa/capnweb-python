"""Centralized JSON codec for the Cap'n Web wire path.

Uses **orjson** (Rust, SIMD-accelerated) for the hot serialize/parse path.
``orjson`` is a required package dependency; import failure should be loud
because silently switching codecs changes the production performance envelope.
The codec:

* emit **compact** output (no inter-token spaces),
* emit **raw UTF-8** for non-ASCII (never ``\\uXXXX``),
* preserve dict **insertion order**,

so the bytes are identical to TypeScript ``JSON.stringify`` (the interop /
golden-fixture oracle); and it **rejects the non-standard JSON constants**
``NaN`` / ``Infinity`` / ``-Infinity`` on input — Cap'n Web encodes those as the
``["nan"]`` / ``["inf"]`` / ``["-inf"]`` escape forms, so a literal on the wire
is a protocol violation.

Invariant relied upon: values are always run through ``Serializer`` (which
converts float NaN/Inf to escape forms and forbids non-str dict keys) before
reaching :func:`dumps`, so the encoder never sees a raw NaN/Inf float or an
int-keyed dict. orjson would otherwise emit ``null`` for a stray NaN. That
cannot occur on the real path.
"""

from __future__ import annotations

from typing import Any

import orjson


def dumps(tree: Any) -> str:
    """Encode a JSON-compatible tree to a compact UTF-8 ``str``."""
    return orjson.dumps(tree).decode("utf-8")


def dumps_bytes(tree: Any) -> bytes:
    """Encode to UTF-8 ``bytes`` directly (skips the decode round-trip)."""
    return orjson.dumps(tree)


def loads(data: str | bytes) -> Any:
    """Strict RFC-8259 parse.

    Rejects NaN/Infinity/trailing commas natively by raising
    ``orjson.JSONDecodeError``, a ``ValueError`` subclass.
    """
    return orjson.loads(data)
