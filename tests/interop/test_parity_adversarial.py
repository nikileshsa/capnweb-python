"""Adversarial interop tests against the real TypeScript server (stream S0).

These pin protocol behaviors the ordinary interop suite never exercises —
the "passing tests != conformance" gaps called out in the parity plan
(docs/architecture/capnweb-parity-plan.md, headline finding 1).

XFAIL CONVENTION (same as tests/test_golden_conformance.py):
rows the current Python stack is KNOWN to fail carry
``pytest.mark.xfail(strict=False, reason="parity gap: <matrix row ref>")``.
strict=False keeps the suite green today; the row flips to XPASS when the
owning stream fixes it, at which point the fix REMOVES the mark in the same
change. Matrix refs point into docs/architecture/capnweb-parity/.

Requires the TS interop server (spawned by conftest; capnweb 0.9.x in
tests/interop/node_modules).
"""

from __future__ import annotations

import asyncio

import pytest


async def test_map_whole_subject_matches_local_apply(py_client_to_ts) -> None:
    """.map() over a WHOLE subject (propertyPath []) against a real TS server.

    A conformant client compiles ``promise.map(f)`` into a remap whose
    propertyPath is ``[]`` (TS always sends an array, rpc.ts:842) and whose
    instructions encode ``f``. The result must equal applying ``f`` locally.

    B2 closed matrix 04 rows 1/12: the Python recorder now compiles the
    mapper for real and always emits an array path, so this passes against
    the canonical TS receiver (xfail removed).
    """
    client = await py_client_to_ts()
    try:
        main = client._client.get_main_stub()
        mapper = lambda x: x  # noqa: E731 — identity: recordable by a conformant recorder
        mapped = main.getList().map(mapper)
        result = await asyncio.wait_for(mapped, timeout=15)

        source = [1, 2, 3, 4, 5]  # what getList() returns
        assert result == [mapper(x) for x in source], (
            f"remap over whole subject returned {result!r}, expected "
            f"{[mapper(x) for x in source]!r}"
        )
    finally:
        await client.__aexit__(None, None, None)


async def test_error_name_round_trip(py_client_to_ts) -> None:
    """A TS-thrown TypeError must surface with wire-faithful name 'TypeError'.

    D4 (parity plan): RpcError carries a ``name`` attribute holding the wire
    name verbatim; the ErrorCode enum is a derived convenience only.
    """
    client = await py_client_to_ts()
    try:
        err = await client.call_expecting_error("throwTypeError")
        name = getattr(err, "name", None)
        assert name == "TypeError", (
            f"expected wire-faithful error name 'TypeError'; got name={name!r} "
            f"on {type(err).__name__}: {err}"
        )
    finally:
        await client.__aexit__(None, None, None)


async def test_unknown_tag_not_silently_delivered(py_client_to_ts) -> None:
    """A TS-sent tag Python doesn't implement must NOT arrive as a plain list.

    The TS server's makeHeaders() returns a Headers object, serialized by
    capnweb 0.9 as ``["headers", [[k, v], ...]]``. Conformant options for the
    Python side are (a) reject the message like TS does for unknown tags, or
    (b) decode it into a real Headers type (matrix 02 row 12). Delivering the
    raw tagged array as application data is wire corruption.
    """
    client = await py_client_to_ts()
    try:
        try:
            result = await client.call_with_timeout("makeHeaders", timeout=10)
        except Exception:
            # Hard rejection is conformant (matches TS unknown-tag behavior).
            return
        assert not (
            isinstance(result, list) and result and result[0] == "headers"
        ), (
            "['headers', ...] special form was silently delivered as a plain "
            f"list: {result!r}"
        )
        # If it wasn't mangled, it must be a structured mapping-like Headers
        # value, not a bare list.
        assert not isinstance(result, list), (
            f"expected a structured Headers value or an error, got {result!r}"
        )
    finally:
        await client.__aexit__(None, None, None)
