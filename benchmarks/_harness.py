"""Timing harness: warmup + many iterations + median/p95/stddev, perf_counter.

No pytest-benchmark dependency (async workloads dominate and it does not time
coroutines well). Everything here uses ``time.perf_counter_ns`` so there is no
``Date.now``/clock-resolution ambiguity, and every measurement warms up before
the timed run so the JIT-less CPython path, import caches, and event loop are
all hot.

Two measurement strategies:

* :func:`bench` — time a *batch* of ``inner`` operations per sample (amortizes
  the perf_counter call over ``inner`` ops so per-op cost isn't dominated by
  the clock's own overhead). Reports ns/op derived from batch time / inner.
* :func:`bench_async` — same, for coroutine factories, driven on one event
  loop that is reused across samples.

A :class:`Result` is JSON-serializable; :func:`summarize` renders the table.
"""

from __future__ import annotations

import asyncio
import gc
import statistics
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Awaitable


@dataclass
class Result:
    name: str
    group: str
    # Per-operation latency stats, nanoseconds.
    ns_median: float
    ns_p95: float
    ns_mean: float
    ns_stddev: float
    ns_min: float
    ops_per_sec: float
    samples: int
    inner: int
    bytes: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _stats(per_op_ns: list[float]) -> dict[str, float]:
    per_op_ns = sorted(per_op_ns)
    n = len(per_op_ns)
    p95 = per_op_ns[min(n - 1, int(round(0.95 * (n - 1))))]
    median = statistics.median(per_op_ns)
    return {
        "ns_median": median,
        "ns_p95": p95,
        "ns_mean": statistics.fmean(per_op_ns),
        "ns_stddev": statistics.pstdev(per_op_ns) if n > 1 else 0.0,
        "ns_min": per_op_ns[0],
        "ops_per_sec": 1e9 / median if median else 0.0,
    }


def bench(
    name: str,
    group: str,
    fn: Callable[[], Any],
    *,
    inner: int = 1,
    samples: int = 60,
    warmup: int = 10,
    bytes_: int = 0,
    extra: dict[str, Any] | None = None,
) -> Result:
    """Time ``fn`` called ``inner`` times per sample, ``samples`` samples.

    ``fn`` must be idempotent-enough to call repeatedly. The reported ns/op is
    (batch wall time / inner).
    """
    for _ in range(warmup):
        fn()

    gc.collect()
    gc_was = gc.isenabled()
    gc.disable()
    per_op_ns: list[float] = []
    try:
        for _ in range(samples):
            t0 = time.perf_counter_ns()
            for _ in range(inner):
                fn()
            t1 = time.perf_counter_ns()
            per_op_ns.append((t1 - t0) / inner)
    finally:
        if gc_was:
            gc.enable()

    s = _stats(per_op_ns)
    return Result(
        name=name, group=group, samples=samples, inner=inner,
        bytes=bytes_, extra=extra or {}, **s,
    )


def bench_async(
    name: str,
    group: str,
    factory: Callable[[], Awaitable[Any]],
    *,
    inner: int = 1,
    samples: int = 40,
    warmup: int = 8,
    setup: Callable[[], Awaitable[Any]] | None = None,
    teardown: Callable[[Any], Awaitable[None]] | None = None,
    bytes_: int = 0,
    extra: dict[str, Any] | None = None,
    loop: asyncio.AbstractEventLoop | None = None,
) -> Result:
    """Time an async ``factory`` (returns a fresh awaitable per call).

    ``inner`` awaitables are awaited sequentially per sample. If ``setup`` is
    given it runs once and its result is passed to ``teardown`` at the end;
    the harness itself creates/uses a single event loop across all samples so
    session/transport state stays hot.
    """
    own_loop = loop is None
    lp = loop or asyncio.new_event_loop()

    async def run_samples() -> list[float]:
        ctx = await setup() if setup else None
        for _ in range(warmup):
            await factory()
        gc.collect()
        gc_was = gc.isenabled()
        gc.disable()
        out: list[float] = []
        try:
            for _ in range(samples):
                t0 = time.perf_counter_ns()
                for _ in range(inner):
                    await factory()
                t1 = time.perf_counter_ns()
                out.append((t1 - t0) / inner)
        finally:
            if gc_was:
                gc.enable()
            if teardown:
                await teardown(ctx)
        return out

    try:
        per_op_ns = lp.run_until_complete(run_samples())
    finally:
        if own_loop:
            lp.close()

    s = _stats(per_op_ns)
    return Result(
        name=name, group=group, samples=samples, inner=inner,
        bytes=bytes_, extra=extra or {}, **s,
    )


def _fmt_ns(ns: float) -> str:
    if ns < 1_000:
        return f"{ns:,.0f} ns"
    if ns < 1_000_000:
        return f"{ns / 1e3:,.2f} us"
    return f"{ns / 1e6:,.3f} ms"


def summarize(results: list[Result]) -> str:
    lines: list[str] = []
    header = (
        f"{'workload':<40} {'median':>12} {'p95':>12} "
        f"{'stddev':>11} {'ops/sec':>14}"
    )
    cur_group = None
    for r in results:
        if r.group != cur_group:
            cur_group = r.group
            lines.append("")
            lines.append(f"== {cur_group} ==")
            lines.append(header)
            lines.append("-" * len(header))
        lines.append(
            f"{r.name:<40} {_fmt_ns(r.ns_median):>12} {_fmt_ns(r.ns_p95):>12} "
            f"{_fmt_ns(r.ns_stddev):>11} {r.ops_per_sec:>14,.0f}"
        )
    return "\n".join(lines)
