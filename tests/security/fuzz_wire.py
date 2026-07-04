"""Structure-aware fuzzer for the Cap'n Web session receive path.

Drives BOTH the pure deserialization stack (``parse_wire_batch`` + ``Parser``)
and a real ``BidirectionalSession`` over a loopback transport with
hypothesis-generated wire frames — well-formed-but-adversarial AND malformed.

INVARIANTS asserted for every case (the control plane must survive a fully
malicious MicroVM peer):

* the read loop never lets an unhandled non-RpcError escape — the session
  either processes the frame or latches ``_abort_reason`` to an Exception;
* no case hangs — each run is wrapped in ``asyncio.wait_for`` with a tight
  bound;
* export refcounts never go negative;
* the session always ``stop()``s cleanly afterwards (no leaked deadlock).

Determinism: a dedicated ``capnweb-security`` hypothesis profile
(``derandomize=True``, fixed ``max_examples``) is registered and loaded so CI
runs are reproducible and bounded. Multi-MB / multi-million-element extremes
live in ``test_adversarial.py`` as single dedicated cases; here the seeds use
CI-reasonable sizes that still exercise the same code paths.

Run just this file:
    uv run pytest packages/capnweb/tests/security/fuzz_wire.py -q \
        -o addopts="" --timeout=120
"""

from __future__ import annotations

import asyncio
import json

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from capnweb.error import RpcError
from capnweb.parser import NullImporter, Parser
from capnweb.wire import parse_wire_batch

from tests.security._harness import drive

# --------------------------------------------------------------------------
# Deterministic, CI-bounded hypothesis profile.
# --------------------------------------------------------------------------
settings.register_profile(
    "capnweb-security",
    max_examples=120,
    deadline=None,  # per-case timing is bounded by asyncio.wait_for in-body
    derandomize=True,  # reproducible corpus across runs / CI
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
settings.load_profile("capnweb-security")


# --------------------------------------------------------------------------
# Wire-expression strategies: build both plausible and hostile trees.
# --------------------------------------------------------------------------
_ids = st.integers(min_value=-(2**63), max_value=2**63)
_prop_keys = st.one_of(st.text(max_size=8), st.integers(min_value=-5, max_value=100))
_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**63), max_value=2**63),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
    st.text(max_size=16),
)

# Tagged special forms — some well-formed, some deliberately wrong-arity so
# the malformed-form rejection path gets exercised too.
_tags = st.sampled_from(
    ["export", "import", "promise", "pipeline", "remap", "error", "bigint",
     "date", "bytes", "undefined", "inf", "-inf", "nan", "headers", "blob",
     "writable", "readable", "request", "response", "constructor", "bogus"],
)


@st.composite
def _tagged(draw: st.DrawFn, child: st.SearchStrategy) -> list:
    tag = draw(_tags)
    # Random arity 1..4 of arbitrary children after the tag.
    arity = draw(st.integers(min_value=0, max_value=3))
    return [tag, *[draw(child) for _ in range(arity)]]


def _expr() -> st.SearchStrategy:
    return st.recursive(
        st.one_of(_scalars, _ids),
        lambda children: st.one_of(
            st.lists(children, max_size=5),
            _tagged(children),
            # dict with dangerous + benign keys
            st.dictionaries(
                st.sampled_from(["__proto__", "constructor", "toJSON", "ok", "x"]),
                children, max_size=4,
            ),
        ),
        max_leaves=25,
    )


# Full wire MESSAGES (the frame the session read loop parses).
def _message() -> st.SearchStrategy:
    expr = _expr()
    return st.one_of(
        st.builds(lambda e: ["push", e], expr),
        st.builds(lambda e: ["stream", e], expr),
        st.just(["pipe"]),
        st.builds(lambda i: ["pull", i], _ids),
        st.builds(lambda i, v: ["resolve", i, v], _ids, expr),
        st.builds(lambda i, e: ["reject", i, e], _ids, expr),
        st.builds(lambda i, r: ["release", i, r], _ids, _ids),
        st.builds(lambda e: ["abort", e], expr),
        # Structurally-wrong messages (bad arity / bad type tag).
        st.builds(lambda t: [t], st.text(max_size=6)),
        expr,  # a bare expression that isn't a message at all
    )


def _frame(msg) -> str:
    try:
        return json.dumps(msg)
    except (TypeError, ValueError):
        return "null"


# --------------------------------------------------------------------------
# Fuzz target 1: pure deserialization stack (cheap, high volume).
# --------------------------------------------------------------------------
@given(msg=_message())
def test_fuzz_parser_never_crashes_uncaught(msg) -> None:
    """The wire parser + evaluator only ever raise ValueError/TypeError/
    RpcError (or succeed) — never an uncaught, unexpected exception type."""
    frame = _frame(msg)
    try:
        parsed = parse_wire_batch(frame)
    except (ValueError, TypeError, RpcError):
        return
    except RecursionError:  # depth guards should pre-empt, but never crash
        pytest.fail("RecursionError escaped wire parse (depth guard gap)")
    # Wire parse succeeded; evaluate the value payloads too.
    for m in parsed:
        expr = getattr(m, "expression", None) or getattr(m, "value", None)
        if expr is None:
            continue
        try:
            payload = Parser(NullImporter()).parse(expr)
            payload.dispose()
        except (ValueError, TypeError, RpcError, RuntimeError):
            pass
        except RecursionError:
            pytest.fail("RecursionError escaped Parser (depth guard gap)")


# --------------------------------------------------------------------------
# Fuzz target 2: stateful session receive path over a loopback transport.
# --------------------------------------------------------------------------
async def _drive_and_check(frames: list[str]) -> None:
    d = await drive(frames, time_budget=5.0)
    try:
        # Invariant A: session either processed or aborted with an Exception —
        # never a torn state.
        if d.abort_reason is not None:
            assert isinstance(d.abort_reason, Exception), (
                f"abort reason is not an Exception: {d.abort_reason!r}"
            )
        # Invariant B: export refcounts never negative.
        mn = d.min_export_refcount()
        assert mn is None or mn >= 0, f"negative export refcount: {mn}"
        # Invariant C: no runaway amplification from these bounded inputs.
        assert d.n_exports + d.n_imports < 200_000
    finally:
        await d.close()


# Nasty seeds required by the audit brief (CI-reasonable sizes; the
# multi-MB / multi-million extremes are dedicated cases in test_adversarial).
_SEED_FRAMES = [
    '["push",' + "[" * 65 + "1" + "]" * 65 + "]",             # depth 65 bomb
    '["push",["pipeline",0,["identity"],[' + json.dumps("A" * 100_000) + "]]]",  # big string
    '["resolve",1,[[' + ",".join("0" for _ in range(10_000)) + "]]]",  # wide array
    '["push",["pipeline",' + str(2**63) + ',["x"],[]]]",'[:-2],  # huge export id ref
    '["release",5,' + str(2**63) + "]",                        # huge refcount
    '["release",99,-1]',                                       # negative refcount
    '["release",42,1]',                                        # release unknown id
    '["resolve",1,["promise",3]]',                             # promise into import
    '["push",["remap",0,[],[["export",7]],[["export",7]]]]',   # remap w/ export capture+instr
    '["push",["pipeline",1,["a"],[["pipeline",1,["b"]]]]]',    # self-referential pipeline
    '["push",[["nan","nan","inf","-inf","nan"]]]',             # nan/inf flood
    '["resolve",1,{"__proto__":["export",2],"constructor":["export",3],"ok":1}]',  # dunder+exports
    '["push",[["bytes","!!!not-base64!!!"]]]',                 # truncated/invalid base64
    '["reject",1,["error","Error","x",null,{"a":{"b":{"c":{"d":1}}}}]]',  # deep error props
    '["push",["pipeline",true,["x"],[]]]',                     # bool-as-id confusion
    '["pull",false]',                                          # bool-as-id
    "not json at all {{{",                                     # non-JSON garbage
    "",                                                        # empty frame
    '["push"]',                                                # wrong arity
    '["totally-unknown-type",1,2,3]',                          # unknown message type
]


@pytest.mark.parametrize("seed", _SEED_FRAMES, ids=range(len(_SEED_FRAMES)))
def test_seed_frames_survive(seed: str) -> None:
    """Each hand-picked adversarial seed is processed or cleanly aborted."""
    asyncio.run(_drive_and_check([seed]))


@settings(max_examples=60, deadline=None, derandomize=True,
          suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
@given(frames=st.lists(_message().map(_frame), min_size=1, max_size=6))
def test_fuzz_session_survives(frames: list[str]) -> None:
    """A malicious peer streaming arbitrary frame sequences never crashes,
    hangs, or drives a refcount negative on the control-plane session.

    Sync test + ``asyncio.run`` per example gives each hypothesis case a
    fresh event loop, so sessions/tasks can't leak across examples.
    """
    asyncio.run(_drive_and_check(frames))
