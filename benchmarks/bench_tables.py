"""Export/import table bookkeeping churn (A1 release-lifecycle hot-path check).

Proves the grant+release cycle is O(1) amortized and not a hotspot: repeatedly
export a capability, then release it, for 10k cycles, and time the steady-state
per-cycle cost. Uses the real ``BidirectionalSession`` export/release paths
(``export_capability`` -> ``_release_export``) but drives them directly so the
measurement isolates table bookkeeping from transport/dispatch.
"""

from __future__ import annotations

from capnweb.rpc_session import BidirectionalSession
from capnweb.inprocess import InProcessPipeTransport
import asyncio

from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget

from benchmarks._harness import Result, bench


class _T(RpcTarget):
    async def call(self, method, args):  # pragma: no cover
        return None

    async def get_property(self, name):  # pragma: no cover
        return None


def _make_session() -> BidirectionalSession:
    a: asyncio.Queue = asyncio.Queue()
    b: asyncio.Queue = asyncio.Queue()
    t = InProcessPipeTransport(a, b)
    return BidirectionalSession(t, None, None)  # not started: no loop needed


def run() -> list[Result]:
    session = _make_session()
    stub = RpcStub(_T())

    def grant_release_cycle():
        # export_capability assigns a fresh export ID + table entry; the peer
        # would later send ["release", id, refcount]; we invoke the receive
        # path directly to exercise the reverse-map cleanup.
        eid = session.export_capability(stub)
        session._release_export(eid, 1)

    r = bench(
        "tables/grant+release_cycle", "tables.churn",
        grant_release_cycle, inner=10_000, samples=40,
        extra={"note": "export_capability + _release_export per cycle"},
    )
    # Sanity: the export table must not have grown unbounded.
    r.extra["exports_table_size_after"] = len(session._exports)
    return [r]


if __name__ == "__main__":
    from benchmarks._harness import summarize

    res = run()
    print(summarize(res))
    for r in res:
        print(f"  {r.name}: {r.extra}")
