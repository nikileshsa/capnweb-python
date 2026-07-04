"""End-to-end RPC benchmarks over the in-process loopback (queue) transport.

These measure the *pure library overhead* of a call: serialize args -> push
frame -> queue -> peer read loop -> dispatch to target -> resolve -> pull ->
deliver. No sockets, no kernel, no JSON-over-the-wire beyond the "string"
encoding level the default pipe uses (the realistic WebSocket wire format).

Workloads:

* ``roundtrip`` — one ``await stub.add(a, b)`` at a time (latency).
* ``pipeline_depth5`` — a 5-deep dependent chain issued WITHOUT intermediate
  awaits vs. 5 sequentially-awaited calls (quantifies the pipelining win).
* ``batch_1000`` — 1000 independent calls in one turn, then gather (calls/sec)
  over both the pipe and the real HTTP-batch codec (loopback).
* ``fanout`` — 100 concurrent sessions each doing N calls (aggregate + mem).
"""

from __future__ import annotations

import asyncio
import tracemalloc

from benchmarks._harness import Result, bench_async
from benchmarks._targets import BenchService, PipePair, loopback_batch_stub


# ---- round-trip latency --------------------------------------------------

def _bench_roundtrip(loop: asyncio.AbstractEventLoop) -> list[Result]:
    state: dict = {}

    async def setup():
        pair = PipePair(BenchService())
        state["pair"] = pair
        state["client"] = pair.client
        return pair

    async def teardown(_):
        await state["pair"].stop()

    async def one_call():
        await state["client"].add(2, 3)

    async def one_echo_nested():
        await state["client"].echo({"id": 1, "tags": [1, 2, 3], "ok": True})

    r1 = bench_async(
        "roundtrip/add(2,3)", "rpc.roundtrip", one_call,
        inner=200, samples=40, setup=setup, teardown=teardown, loop=loop,
    )
    r2 = bench_async(
        "roundtrip/echo(nested)", "rpc.roundtrip", one_echo_nested,
        inner=200, samples=40, setup=setup, teardown=teardown, loop=loop,
    )

    # jsonCompatible variant: skips the per-frame JSON stringify/parse, the
    # closest analog to TS's structured-clone MessagePort transport (fair
    # head-to-head with ts_compare/bench_rpc.ts).
    jc: dict = {}

    async def setup_jc():
        pair = PipePair(BenchService(), encoding_level="jsonCompatible")
        jc["pair"] = pair
        return pair

    async def teardown_jc(_):
        await jc["pair"].stop()

    async def one_call_jc():
        await jc["pair"].client.add(2, 3)

    r3 = bench_async(
        "roundtrip/add(2,3)_jsonCompatible", "rpc.roundtrip", one_call_jc,
        inner=200, samples=40, setup=setup_jc, teardown=teardown_jc, loop=loop,
    )
    return [r1, r2, r3]


# ---- pipelining vs sequential -------------------------------------------

def _bench_pipeline(loop: asyncio.AbstractEventLoop) -> list[Result]:
    state: dict = {}

    async def setup():
        pair = PipePair(BenchService())
        state["pair"] = pair
        state["client"] = pair.client
        return pair

    async def teardown(_):
        await state["pair"].stop()

    async def pipelined_chain():
        # 5-deep dependent chain: each call feeds the next, but we DON'T await
        # intermediate results — the promises pipeline to the server.
        c = state["client"]
        p = c.chain(0)
        p = c.chain(p.n)
        p = c.chain(p.n)
        p = c.chain(p.n)
        p = c.chain(p.n)
        await p

    async def sequential_chain():
        # Same 5 calls, but each awaited before issuing the next (classic
        # request/response — 5 full round-trips).
        c = state["client"]
        n = 0
        for _ in range(5):
            r = await c.chain(n)
            n = r["n"]

    r1 = bench_async(
        "pipeline/5deep_pipelined", "rpc.pipeline", pipelined_chain,
        inner=100, samples=40, setup=setup, teardown=teardown, loop=loop,
    )
    r2 = bench_async(
        "pipeline/5deep_sequential", "rpc.pipeline", sequential_chain,
        inner=100, samples=40, setup=setup, teardown=teardown, loop=loop,
    )
    return [r1, r2]


# ---- batch throughput ----------------------------------------------------

def _bench_batch(loop: asyncio.AbstractEventLoop) -> list[Result]:
    N = 1000
    state: dict = {}

    async def setup_pipe():
        pair = PipePair(BenchService())
        state["pair"] = pair
        state["client"] = pair.client
        return pair

    async def teardown_pipe(_):
        await state["pair"].stop()

    async def batch_pipe():
        c = state["client"]
        promises = [c.add(i, 1) for i in range(N)]
        await asyncio.gather(*promises)

    # inner=1 (each sample is 1000 calls); report ns per BATCH, then derive
    # calls/sec in extra.
    r_pipe = bench_async(
        "batch/1000_calls_pipe", "rpc.batch", batch_pipe,
        inner=1, samples=30, setup=setup_pipe, teardown=teardown_pipe, loop=loop,
    )
    r_pipe.extra["calls_per_sec"] = N * 1e9 / r_pipe.ns_median
    r_pipe.extra["n_calls"] = N

    async def batch_http():
        # Fresh loopback HTTP-batch session per batch (batch sessions are
        # single-shot). This is the real newline-framed batch codec path.
        stub, session = loopback_batch_stub(BenchService())
        promises = [stub.add(i, 1) for i in range(N)]
        await asyncio.gather(*promises)
        await session.stop()

    r_http = bench_async(
        "batch/1000_calls_http_loopback", "rpc.batch", batch_http,
        inner=1, samples=20, warmup=4, loop=loop,
    )
    r_http.extra["calls_per_sec"] = N * 1e9 / r_http.ns_median
    r_http.extra["n_calls"] = N
    return [r_pipe, r_http]


# ---- fan-out: 100 sessions x N calls ------------------------------------

def _bench_fanout(loop: asyncio.AbstractEventLoop) -> list[Result]:
    SESSIONS = 100
    CALLS = 20
    state: dict = {}

    async def setup():
        pairs = [PipePair(BenchService()) for _ in range(SESSIONS)]
        state["pairs"] = pairs
        return pairs

    async def teardown(_):
        for p in state["pairs"]:
            await p.stop()

    async def fanout():
        async def per_session(p):
            for i in range(CALLS):
                await p.client.add(i, 1)
        await asyncio.gather(*(per_session(p) for p in state["pairs"]))

    r = bench_async(
        "fanout/100sess_x20calls", "rpc.fanout", fanout,
        inner=1, samples=25, warmup=5, setup=setup, teardown=teardown, loop=loop,
    )
    total = SESSIONS * CALLS
    r.extra["calls_per_sec"] = total * 1e9 / r.ns_median
    r.extra["total_calls"] = total
    r.extra["sessions"] = SESSIONS

    # Per-session memory via tracemalloc: measure the retained delta of
    # standing up 100 idle sessions.
    def measure_mem():
        tracemalloc.start()
        base = tracemalloc.take_snapshot()
        pairs = [PipePair(BenchService()) for _ in range(SESSIONS)]
        after = tracemalloc.take_snapshot()
        stats = after.compare_to(base, "filename")
        total_bytes = sum(st.size_diff for st in stats)
        tracemalloc.stop()
        return total_bytes, pairs

    mem, mem_pairs = loop.run_until_complete(_run_sync(measure_mem))
    loop.run_until_complete(_stop_all(mem_pairs))
    r.extra["mem_per_session_bytes"] = int(mem / SESSIONS)
    r.extra["mem_total_bytes"] = int(mem)
    return [r]


async def _run_sync(fn):
    return fn()


async def _stop_all(pairs):
    for p in pairs:
        await p.stop()


def run() -> list[Result]:
    loop = asyncio.new_event_loop()
    try:
        results: list[Result] = []
        results += _bench_roundtrip(loop)
        results += _bench_pipeline(loop)
        results += _bench_batch(loop)
        results += _bench_fanout(loop)
        return results
    finally:
        loop.close()


if __name__ == "__main__":
    from benchmarks._harness import summarize

    res = run()
    print(summarize(res))
    for r in res:
        if r.extra:
            print(f"  {r.name}: {r.extra}")
