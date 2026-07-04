"""Canonical payload shapes shared by the Python and TS micro-benchmarks.

Keep these in lockstep with ``ts_compare/payloads.ts`` so the serialize/parse
numbers are apples-to-apples. Every shape is pure JSON-compatible data (no
stubs) so both the standalone ``serialize``/``deserialize`` helpers and
``JSON.parse``-based TS path can handle them.
"""

from __future__ import annotations

import base64
import os
from typing import Any


def _nested_object() -> dict[str, Any]:
    # A realistic RPC arg: a request-ish record with nesting and mixed types.
    return {
        "id": 12345,
        "name": "Ada Lovelace",
        "active": True,
        "score": 98.6,
        "tags": ["alpha", "beta", "gamma"],
        "address": {
            "street": "1 Analytical Engine Way",
            "city": "London",
            "geo": {"lat": 51.5074, "lng": -0.1278},
        },
        "roles": [{"k": "admin", "w": 5}, {"k": "user", "w": 1}],
    }


# 1 MiB of pseudo-random bytes -> base64 blob path.
_BLOB_RAW = os.urandom(1024 * 1024)


def payloads() -> dict[str, Any]:
    """Return the canonical {name: value} shapes."""
    return {
        "small_scalar": 42,
        "short_string": "hello world",
        "nested_object": _nested_object(),
        "large_array_10k_ints": list(range(10_000)),
        "large_string_1mb": "x" * (1024 * 1024),
        # bytes -> ["bytes", <unpadded base64>]; the 1 MiB blob path.
        "bytes_blob_1mb": _BLOB_RAW,
        # An error value round-trips through the ["error", ...] form.
        "error_value": ValueError("something went wrong at layer 7"),
    }


def blob_base64_len() -> int:
    return len(base64.b64encode(_BLOB_RAW).rstrip(b"="))
