"""cProfile the two heaviest Python workloads: codec + RPC round-trip.

Emits cumulative + total-time top functions to stdout and, if ``py-spy`` is on
PATH, prints the command to capture a sampling profile (py-spy needs to attach
to a live process, so we document the invocation rather than shell out with
sudo from here).

    uv run python -m benchmarks.profile_hotpaths
"""

from __future__ import annotations

import asyncio
import cProfile
import io
import pstats

from capnweb.serializer import serialize
from capnweb.parser import deserialize

from benchmarks._payloads import payloads
from benchmarks._targets import BenchService, PipePair


def _profile(label: str, fn, *, restrict: int = 25) -> None:
    pr = cProfile.Profile()
    pr.enable()
    fn()
    pr.disable()
    for sort in ("tottime", "cumulative"):
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).strip_dirs().sort_stats(sort)
        ps.print_stats(restrict)
        print(f"\n===== {label} :: sort={sort} =====")
        print(s.getvalue())


def profile_codec() -> None:
    shapes = payloads()
    nested = shapes["nested_object"]
    arr = shapes["large_array_10k_ints"]

    def work():
        # Mix of the realistic-arg and large-array shapes; enough iterations
        # for the profiler to attribute time meaningfully.
        for _ in range(20_000):
            deserialize(serialize(nested))
        for _ in range(200):
            deserialize(serialize(arr))

    _profile("codec roundtrip (nested x20k + array10k x200)", work)


def profile_rpc() -> None:
    loop = asyncio.new_event_loop()

    async def setup():
        pair = PipePair(BenchService())
        return pair

    pair = loop.run_until_complete(setup())

    async def many_calls():
        for _ in range(20_000):
            await pair.client.add(2, 3)

    def work():
        loop.run_until_complete(many_calls())

    try:
        _profile("rpc roundtrip add(2,3) x20k", work)
    finally:
        loop.run_until_complete(pair.stop())
        loop.close()


if __name__ == "__main__":
    profile_codec()
    profile_rpc()
    import shutil

    if shutil.which("py-spy"):
        print("\n[py-spy available] To capture a sampling profile of a live run:")
        print("  uv run python -m benchmarks.bench_rpc &")
        print("  py-spy record -o rpc.svg --pid $! --duration 20")
    else:
        print("\n[py-spy not on PATH] install with `uv pip install py-spy` for "
              "a sampling flamegraph; cProfile results above are authoritative "
              "for hot-function attribution.")
