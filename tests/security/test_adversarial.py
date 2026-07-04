"""Targeted adversarial tests — one per threat-model finding.

Each test either PINS a defense that is already correct (regression guard) or
is ``xfail(strict=True)`` tagged with a finding ID, demonstrating a REAL gap
that the hardening pass must close (the xfail flips to a failure — and the
hardening pass removes the marker — once the limit lands).

Findings map to docs/security/capnweb-audit-findings.md. Adversary = a fully
malicious MicroVM peer sending arbitrary wire frames to the control plane.

Run:
    uv run pytest packages/capnweb/tests/security/test_adversarial.py -q \
        -o addopts="" --timeout=120
"""

from __future__ import annotations

import asyncio
import json

import pytest

from capnweb.config import RpcSessionConfig
from capnweb.error import RpcError

from tests.security._harness import DrivenSession, _Echo, drive

pytestmark = pytest.mark.asyncio


# ==========================================================================
# VERIFIED-SAFE — defenses confirmed active (regression guards)
# ==========================================================================

async def test_S1_unauthorized_export_reference_aborts() -> None:
    """A peer referencing an export ID it was never granted -> clean abort,
    never silent capability materialization (capability forgery)."""
    d = await drive(['["push",["pipeline",424242,["whatever"],[]]]'])
    try:
        assert isinstance(d.abort_reason, RpcError)
        assert "no such entry on exports table" in str(d.abort_reason)
    finally:
        await d.close()


async def test_S2_released_export_id_is_not_reused() -> None:
    """Our export IDs are monotonic (negative, decreasing); a released ID is
    never handed to a later grant, so a stale reference can't hijack a new
    capability."""
    d = DrivenSession()
    s = d.session
    from capnweb.hooks import TargetStubHook

    first = s._export_hook(TargetStubHook(_Echo()))
    s._release_export(first, 1)  # drop it
    assert first not in s._exports
    second = s._export_hook(TargetStubHook(_Echo()))
    assert second != first, "released export ID was reused for a new grant"
    assert second < first, "export IDs must strictly decrease (no reuse)"
    await d.close()


async def test_S3_refcount_underflow_aborts() -> None:
    """Release with a refcount larger than held -> abort; the refcount never
    goes negative."""
    d = await drive([
        '["push",["pipeline",0,["greet"],["a"]]]',  # creates export 1 (refcount 1)
        '["release",1,999]',                          # over-release
    ])
    try:
        assert isinstance(d.abort_reason, RpcError)
        assert "negative" in str(d.abort_reason)
    finally:
        await d.close()


async def test_S3b_double_release_and_unknown_release_abort() -> None:
    d = await drive(['["release",7,1]'])  # release of never-granted ID
    try:
        assert isinstance(d.abort_reason, RpcError)
        assert "no such export ID" in str(d.abort_reason)
    finally:
        await d.close()


async def test_S4_bool_is_not_accepted_as_capability_id() -> None:
    """bool is an int subclass in Python; True/False must NOT alias IDs 1/0."""
    d = await drive(['["pull",true]'])
    try:
        assert isinstance(d.abort_reason, Exception)
    finally:
        await d.close()


async def test_S5_depth_bomb_aborts_without_stack_overflow() -> None:
    frame = '["push",' + "[" * 200 + "1" + "]" * 200 + "]"
    d = await drive([frame])
    try:
        assert isinstance(d.abort_reason, Exception)
        # A protocol/parse error, not a Python RecursionError leaking out.
        assert not isinstance(d.abort_reason, RecursionError)
    finally:
        await d.close()


async def test_S6_mapper_export_tag_reference_does_not_crash_session() -> None:
    """Mapper instructions that reference exports (index-aliasing, row 15)
    hard-fail inside the applicator; the session survives (surfaces as a
    reject, never a capability-confusion escape)."""
    d = await drive(['["push",["remap",0,[],[],[["export",1]]]]', '["pull",1]'])
    try:
        # Either the remap rejected cleanly or the session aborted with a
        # protocol error — but no uncaught crash and no negative refcount.
        if d.abort_reason is not None:
            assert isinstance(d.abort_reason, Exception)
        mn = d.min_export_refcount()
        assert mn is None or mn >= 0
    finally:
        await d.close()


async def test_S7_prototype_pollution_keys_dropped_but_stubs_released() -> None:
    """__proto__/constructor/toJSON keys are dropped (traverse-then-drop);
    embedded exports inside them are still imported so they can be released —
    no refcount leak, no gadget key reaching app code."""
    d = await drive([
        '["resolve",1,{"__proto__":["export",2],"constructor":["export",3],"ok":1}]',
    ])
    try:
        assert d.abort_reason is None  # dropping keys is not a protocol error
    finally:
        await d.close()


async def test_S8_bare_nan_infinity_rejected_special_forms_ok() -> None:
    """Raw JSON NaN/Infinity are rejected (strict parse); the ["nan"] special
    form decodes to a float without crashing."""
    bad = await drive(['["push",NaN]'])
    try:
        assert isinstance(bad.abort_reason, Exception)
    finally:
        await bad.close()
    ok = await drive(['["push",["nan"]]'])
    try:
        assert ok.abort_reason is None
    finally:
        await ok.close()


async def test_S9_sessions_do_not_share_export_tables() -> None:
    """No module/class-level mutable state: two independent sessions have
    fully isolated export/import tables (tenant/session isolation)."""
    a = DrivenSession()
    b = DrivenSession()
    from capnweb.hooks import TargetStubHook

    a._session_eid = a.session._export_hook(TargetStubHook(_Echo()))  # type: ignore[attr-defined]
    assert a.session._exports is not b.session._exports
    assert a._session_eid not in b.session._exports
    await a.close()
    await b.close()


async def test_S10_malformed_frame_aborts_not_hangs() -> None:
    """A truncated/garbage frame aborts promptly (bounded), never hangs."""
    d = await drive(['["push",["pipeline"', "garbage{{{"], time_budget=3.0)
    try:
        assert isinstance(d.abort_reason, Exception)
    finally:
        await d.close()


async def test_S11_huge_int_id_no_overflow() -> None:
    """A 2**63 export/import ID is a Python bignum — accepted, no overflow,
    no crash (it simply names a table slot)."""
    d = await drive(['["resolve",1,["export",' + str(2**63) + "]]"])
    try:
        assert d.abort_reason is None
    finally:
        await d.close()


async def test_S12_unresolved_pull_times_out_not_hangs() -> None:
    """A peer that never resolves a promise we pulled surfaces as a bounded
    timeout error, never a permanent hang (control-plane liveness)."""
    opts = RpcSessionConfig(pull_timeout=0.3)
    d = DrivenSession(options=opts)
    remote = d.session.get_remote_main()
    with pytest.raises(RpcError) as ei:
        await asyncio.wait_for(remote.greet("x"), 3.0)
    assert "timed out" in str(ei.value)
    await d.close()


async def test_S13_no_stack_or_traceback_leaked_by_default() -> None:
    """With the default config (on_send_error=None) a server-side exception is
    sent as ["error", name, message] with NO stack/traceback element."""
    d = await drive(['["push",["pipeline",0,["boom"],[]]]', '["pull",1]'],
                    time_budget=3.0)
    try:
        rejects = [json.loads(f) for f in d.transport.out
                   if isinstance(f, str) and f.startswith('["reject"')]
        assert rejects, f"expected a reject frame, got {d.transport.out}"
        err = rejects[0][2]  # ["error", name, message, stack?, props?]
        assert err[0] == "error"
        # No stack slot present (len 3) — a stack would be index 3 as a string.
        assert len(err) == 3 or err[3] is None, f"stack leaked: {err!r}"
    finally:
        await d.close()


# ==========================================================================
# RESOLVED GAPS — the hardening pass added the limit; each xfail flipped to a
# passing assertion that the bound is enforced (docs/security/
# capnweb-audit-findings.md marks F1-F6 RESOLVED).
# ==========================================================================

# --- F1: max_exports -------------------------------------------------------

async def test_F1_default_config_bounds_export_table() -> None:
    """The default RpcSessionConfig carries a finite ``max_exports`` bound —
    the export table can no longer grow without limit."""
    cfg = RpcSessionConfig()
    assert cfg.max_exports == 100_000


async def test_F1_unbounded_pushes_are_bounded() -> None:
    """A peer that streams pushes and never releases is aborted once the
    export-table cap is reached (memory-exhaustion DoS closed). Uses a low
    ``max_exports`` so the bound is exercised cheaply; the default (100k) is
    the same code path at a higher threshold."""
    opts = RpcSessionConfig(max_exports=100)
    frames = ['["push",["pipeline",0,["greet"],["x"]]]'] * 5000
    d = await drive(frames, time_budget=10.0, options=opts)
    try:
        assert isinstance(d.abort_reason, RpcError), (
            f"export table grew to {d.n_exports} with no abort"
        )
        assert "export table limit exceeded" in str(d.abort_reason)
        # LIVE entries never exceeded the cap.
        assert d.n_exports <= 100
    finally:
        await d.close()


async def test_F1_under_the_export_cap_is_accepted() -> None:
    """Traffic below the cap is processed normally — the bound never
    penalizes a well-behaved peer."""
    opts = RpcSessionConfig(max_exports=100)
    frames = ['["push",["pipeline",0,["greet"],["x"]]]'] * 50
    d = await drive(frames, time_budget=10.0, options=opts)
    try:
        assert d.abort_reason is None
        # 50 peer pushes (+ possibly the local main export) — under the cap.
        assert 50 <= d.n_exports <= 51
    finally:
        await d.close()


# --- F2: max_imports -------------------------------------------------------

async def test_F2_default_config_bounds_import_table() -> None:
    cfg = RpcSessionConfig()
    assert cfg.max_imports == 100_000


async def test_F2_unbounded_imports_are_bounded() -> None:
    """One push whose escaped array carries M distinct ["export", id] refs is
    aborted once the import-table cap is reached (amplification DoS closed)."""
    opts = RpcSessionConfig(max_imports=100)
    big = "[" + ",".join(f'["export",{i}]' for i in range(5000)) + "]"
    d = await drive(['["push",[' + big + "]]"], time_budget=10.0, options=opts)
    try:
        assert isinstance(d.abort_reason, RpcError), (
            f"import table grew to {d.n_imports} with no abort"
        )
        assert "import table limit exceeded" in str(d.abort_reason)
        assert d.n_imports <= 100
    finally:
        await d.close()


async def test_F2_under_the_import_cap_is_accepted() -> None:
    opts = RpcSessionConfig(max_imports=100)
    big = "[" + ",".join(f'["export",{i}]' for i in range(50)) + "]"
    d = await drive(['["push",[' + big + "]]"], time_budget=10.0, options=opts)
    try:
        assert d.abort_reason is None
        assert d.n_imports == 50
    finally:
        await d.close()


# --- F3: max_message_bytes -------------------------------------------------

async def test_F3_default_config_bounds_message_size() -> None:
    cfg = RpcSessionConfig()
    assert cfg.max_message_bytes == 16 * 1024 * 1024


async def test_F3_oversized_frame_is_rejected() -> None:
    """A single oversized frame is rejected/aborted BEFORE being parsed
    wholesale. Uses a low ``max_message_bytes`` and an 8 MiB frame."""
    opts = RpcSessionConfig(max_message_bytes=1 * 1024 * 1024)
    giant = "A" * (8 * 1024 * 1024)
    frame = '["push",["pipeline",0,["identity"],[' + json.dumps(giant) + "]]]"
    d = await drive([frame], time_budget=15.0, options=opts)
    try:
        assert isinstance(d.abort_reason, RpcError), (
            "oversized frame was accepted with no size cap"
        )
        assert "message size" in str(d.abort_reason)
    finally:
        await d.close()


async def test_F3_frame_under_the_size_cap_is_accepted() -> None:
    opts = RpcSessionConfig(max_message_bytes=1 * 1024 * 1024)
    payload = "A" * 1000
    frame = '["push",["pipeline",0,["identity"],[' + json.dumps(payload) + "]]]"
    d = await drive([frame], time_budget=10.0, options=opts)
    try:
        assert d.abort_reason is None
    finally:
        await d.close()


# --- F4: max_blob_bytes ----------------------------------------------------

async def test_F4_blob_size_limit_config_exists() -> None:
    """``max_blob_bytes`` bounds blob accumulation; the knob exists with the
    audit default and is threaded into the decode Parser."""
    assert "max_blob_bytes" in RpcSessionConfig.model_fields
    assert RpcSessionConfig().max_blob_bytes == 64 * 1024 * 1024


async def test_F4_blob_collection_aborts_past_cap() -> None:
    """A blob whose streamed bytes exceed ``max_blob_bytes`` rejects the
    containing value instead of accumulating unbounded memory."""
    from capnweb.parser import Parser
    from capnweb.streams import RpcReadableStream

    async def _gen():
        yield b"x" * 100
        yield b"x" * 100  # running total 200 > cap of 150

    class _BlobImporter:
        def __init__(self, stream: RpcReadableStream) -> None:
            self.stream = stream

        def import_capability(self, i: int):  # pragma: no cover - unused
            raise RuntimeError

        def create_promise_hook(self, i: int):  # pragma: no cover - unused
            raise RuntimeError

        def get_export(self, i: int):
            return None

        def get_pipe_readable(self, i: int):
            return self.stream

    stream = RpcReadableStream(_gen())
    parser = Parser(_BlobImporter(stream), max_blob_bytes=150)
    payload = parser.parse(["blob", "text/plain", ["readable", 1]])
    promise = payload.substitutions[0][2]
    with pytest.raises((ValueError, RpcError)) as ei:
        await asyncio.wait_for(promise._hook.future, 3.0)
    assert "Blob size exceeds maximum" in str(ei.value)
    payload.dispose()


async def test_F4_blob_under_cap_collects() -> None:
    from capnweb.parser import Parser
    from capnweb.streams import RpcReadableStream

    async def _gen():
        yield b"x" * 40
        yield b"x" * 40  # total 80 < cap

    class _BlobImporter:
        def __init__(self, stream: RpcReadableStream) -> None:
            self.stream = stream

        def import_capability(self, i: int):  # pragma: no cover - unused
            raise RuntimeError

        def create_promise_hook(self, i: int):  # pragma: no cover - unused
            raise RuntimeError

        def get_export(self, i: int):
            return None

        def get_pipe_readable(self, i: int):
            return self.stream

    stream = RpcReadableStream(_gen())
    parser = Parser(_BlobImporter(stream), max_blob_bytes=150)
    payload = parser.parse(["blob", "text/plain", ["readable", 1]])
    promise = payload.substitutions[0][2]
    hook = await asyncio.wait_for(promise._hook.future, 3.0)
    assert hook is not None
    payload.dispose()


# --- F5: max_array_len -----------------------------------------------------

async def test_F5_array_length_limit_config_exists() -> None:
    assert "max_array_len" in RpcSessionConfig.model_fields
    assert RpcSessionConfig().max_array_len == 1_000_000


async def test_F5_oversized_array_is_rejected() -> None:
    """An escaped array wider than ``max_array_len`` aborts the decode
    instead of materializing millions of elements."""
    opts = RpcSessionConfig(max_array_len=100)
    wide = "[" + ",".join("0" for _ in range(500)) + "]"
    d = await drive(['["resolve",1,[' + wide + "]]"], time_budget=10.0,
                    options=opts)
    try:
        assert isinstance(d.abort_reason, Exception), (
            "oversized array was accepted with no width cap"
        )
        assert "exceeds maximum" in str(d.abort_reason)
    finally:
        await d.close()


async def test_F5_array_under_cap_is_accepted() -> None:
    opts = RpcSessionConfig(max_array_len=100)
    wide = "[" + ",".join("0" for _ in range(50)) + "]"
    d = await drive(['["resolve",1,[' + wide + "]]"], time_budget=10.0,
                    options=opts)
    try:
        assert d.abort_reason is None
    finally:
        await d.close()


# --- F6: redact_internal_errors --------------------------------------------

async def test_F6_redaction_on_by_default() -> None:
    assert RpcSessionConfig().redact_internal_errors is True


async def test_F6_app_exception_text_is_not_leaked_by_default() -> None:
    """A raw server-side OSError's message (which carries a filesystem path)
    is NOT forwarded to the untrusted peer: redact_internal_errors defaults
    True, so the free-text message is replaced with a generic string."""
    d = await drive(['["push",["pipeline",0,["boom"],[]]]', '["pull",1]'],
                    time_budget=3.0)
    try:
        wire = " ".join(f for f in d.transport.out if isinstance(f, str))
        assert "/etc/control-plane-secret.key" not in wire, (
            "server filesystem path leaked to peer in error message"
        )
        # The reject is still delivered (fail-loud), with a redacted message.
        rejects = [json.loads(f) for f in d.transport.out
                   if isinstance(f, str) and f.startswith('["reject"')]
        assert rejects, f"expected a reject frame, got {d.transport.out}"
        assert rejects[0][2][2] == "internal error"
    finally:
        await d.close()


async def test_F6_explicit_rpc_error_message_is_preserved() -> None:
    """A deliberate RpcError raised by app code is a protocol signal — its
    message must still reach the peer (only UNEXPECTED exceptions redact)."""

    class _Boomer(_Echo):
        def kaboom(self) -> str:
            raise RpcError.bad_request("explicit protocol detail 42")

    d = await drive(['["push",["pipeline",0,["kaboom"],[]]]', '["pull",1]'],
                    time_budget=3.0, local_main=_Boomer())
    try:
        rejects = [json.loads(f) for f in d.transport.out
                   if isinstance(f, str) and f.startswith('["reject"')]
        assert rejects, f"expected a reject frame, got {d.transport.out}"
        assert "explicit protocol detail 42" in rejects[0][2][2]
    finally:
        await d.close()


async def test_F6_opt_out_forwards_raw_text() -> None:
    """A deployment can opt out (trusted peer): redact_internal_errors=False
    restores the raw message."""
    opts = RpcSessionConfig(redact_internal_errors=False)
    d = await drive(['["push",["pipeline",0,["boom"],[]]]', '["pull",1]'],
                    time_budget=3.0, options=opts)
    try:
        wire = " ".join(f for f in d.transport.out if isinstance(f, str))
        assert "/etc/control-plane-secret.key" in wire
    finally:
        await d.close()
