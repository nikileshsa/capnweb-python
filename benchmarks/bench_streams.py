"""Stream throughput over the loopback pipe + FlowController steady state.

A server method returns an ``RpcReadableStream`` that yields fixed-size byte
chunks; the client consumes it with ``async for``. This drives the full
``["pipe"]`` + flow-controlled write/ack machinery end-to-end in one process,
so the number is the CPU-bound ceiling (no network). We report MB/s and the
FlowController's steady-state window at the end of the pump.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from capnweb.streams import RpcReadableStream
from capnweb.types import RpcTarget

from benchmarks._harness import Result
from benchmarks._targets import PipePair


CHUNK = b"\xab" * (64 * 1024)  # 64 KiB chunks


class StreamService(RpcTarget):
    def __init__(self, total_bytes: int) -> None:
        self._total = total_bytes

    async def call(self, method: str, args: list[Any]) -> Any:
        if method == "produce":
            n_chunks = self._total // len(CHUNK)

            def gen():
                async def agen():
                    for _ in range(n_chunks):
                        yield CHUNK
                return agen()

            return RpcReadableStream(gen())
        raise Exception(f"StreamService: no method {method}")

    async def get_property(self, name: str) -> Any:
        raise Exception(f"no prop {name}")


def _observe_window() -> float | None:
    """Best-effort: read a representative steady-state window value.

    The FlowController lives inside the WritableStreamHook created per pipe;
    we can't reach it from here without touching src, so we sample the module
    default and report the tuning constants for the report instead.
    """
    from capnweb import streams

    return float(streams.INITIAL_WINDOW)


def run() -> list[Result]:
    loop = asyncio.new_event_loop()
    TOTAL = 64 * 1024 * 1024  # 64 MiB

    async def pump_once(encoding_level: str) -> float:
        pair = PipePair(StreamService(TOTAL), encoding_level=encoding_level)
        received = 0
        t0 = time.perf_counter_ns()
        stream = await pair.client.produce()
        async for chunk in stream:
            received += len(chunk)
        t1 = time.perf_counter_ns()
        await pair.stop()
        assert received == TOTAL, (received, TOTAL)
        return (t1 - t0)

    from benchmarks._harness import _stats

    def measure(name: str, encoding_level: str) -> Result:
        # Warmup (smaller): 1 full pump.
        loop.run_until_complete(pump_once(encoding_level))
        samples_ns: list[float] = []
        for _ in range(7):
            samples_ns.append(loop.run_until_complete(pump_once(encoding_level)))
        samples_ns.sort()
        median_ns = samples_ns[len(samples_ns) // 2]
        mbps = (TOTAL / (1024 * 1024)) / (median_ns / 1e9)
        s = _stats(samples_ns)
        # Note: here ns is per-PUMP (64 MiB), not per-op.
        return Result(
            name=name, group="stream.throughput",
            samples=len(samples_ns), inner=1, bytes=TOTAL,
            ns_median=s["ns_median"], ns_p95=s["ns_p95"], ns_mean=s["ns_mean"],
            ns_stddev=s["ns_stddev"], ns_min=s["ns_min"],
            ops_per_sec=0.0,
            extra={
                "MB_per_sec": round(mbps, 1),
                "MiB_total": TOTAL // (1024 * 1024),
                "chunk_bytes": len(CHUNK),
                "encoding_level": encoding_level,
                "initial_window_bytes": _observe_window(),
            },
        )

    try:
        results = [
            # Realistic WebSocket wire: every chunk is base64+JSON per protocol.
            measure("stream/64MiB_loopback", "string"),
            # P4: custom-encoding transport keeps bytes raw inside ["bytes", ...]
            # and skips the per-frame json.dumps — chunks bypass base64+JSON
            # entirely, the analog to TS's structured-clone MessagePort.
            measure("stream/64MiB_loopback_bytes", "jsonCompatibleWithBytes"),
        ]
    finally:
        loop.close()

    return results


if __name__ == "__main__":
    for r in run():
        print(f"{r.name}: {r.extra['MB_per_sec']} MB/s "
              f"(median {r.ns_median/1e6:.1f} ms/64MiB, "
              f"p95 {r.ns_p95/1e6:.1f} ms), window0={r.extra['initial_window_bytes']}")
