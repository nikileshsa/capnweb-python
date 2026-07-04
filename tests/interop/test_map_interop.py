"""B2 stream — map/remap interop against the real TypeScript peer.

Ports the TS map test scenarios (__tests__/index.test.ts:806-860,1459-1515)
in BOTH directions:

* PY client ``.map()`` vs TS server: the Python recorder emits the remap,
  the canonical TS evaluator executes it. Covers null/undefined/single-value
  maps, arrays with captured stubs + counters (including a PY-local counter
  the TS server calls back into), and nested maps.
* TS client remap vs PY server: driven through additive ``ts_server.ts``
  methods (mapArrayRemote & co.) that run a TS-side ``.map()`` over a
  promise pointing at a PY-hosted source target — the remap expression is
  emitted by the canonical TS recorder and evaluated by the Python receiver.

All assertions check REAL mapped values — never just "not None".
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from capnweb.error import RpcError
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget, Undefined


class PyCounter(RpcTarget):
    """Python-side counter the TS peer can call back into."""

    def __init__(self, initial: int = 0) -> None:
        self.count = initial

    async def call(self, method: str, args: list) -> Any:
        if method == "increment":
            self.count += args[0] if args else 1
            return self.count
        raise RpcError.not_found(f"Unknown method: {method}")

    async def get_property(self, name: str) -> Any:
        if name == "value":
            return self.count
        raise AttributeError(name)


class MapSource(RpcTarget):
    """Python-side source target for the TS-client remap direction."""

    async def call(self, method: str, args: list) -> Any:
        if method == "getNumbers":
            return [1, 2, 3, 4, 5]
        if method == "double":
            return args[0] * 2
        if method == "makeList":
            return list(range(args[0]))
        if method == "returnNull":
            return None
        if method == "returnNumber":
            return args[0]
        if method == "makeCounter":
            return PyCounter(args[0])
        raise RpcError.not_found(f"Unknown method: {method}")

    async def get_property(self, name: str) -> Any:
        raise AttributeError(name)


# =============================================================================
# Direction 1: PY client .map() vs real TS server
# =============================================================================


class TestPyClientMapsOverTsServer:
    async def test_map_on_null_undefined_and_single_values(self, py_client_to_ts) -> None:
        """Port of TS 'supports map() on nulls' (index.test.ts:1455-1480)."""
        client = await py_client_to_ts()
        try:
            main = client._client.get_main_stub()
            counter = await asyncio.wait_for(main.makeCounter(0), 10)

            # null passthrough: mapper NOT invoked.
            result = await asyncio.wait_for(
                main.returnNull().map(lambda _: counter.increment(123)), 10
            )
            assert result is None

            # undefined passthrough.
            result = await asyncio.wait_for(
                main.returnUndefined().map(lambda _: counter.increment(456)), 10
            )
            assert result is Undefined

            # Counter untouched so far.
            assert await asyncio.wait_for(counter.value, 10) == 0

            # Single (non-array) values map once, cumulatively.
            assert await asyncio.wait_for(
                main.returnNumber(2).map(lambda i: counter.increment(i)), 10
            ) == 2
            assert await asyncio.wait_for(
                main.returnNumber(4).map(lambda i: counter.increment(i)), 10
            ) == 6
        finally:
            await client.__aexit__(None, None, None)

    async def test_map_on_arrays_with_captured_stubs(self, py_client_to_ts) -> None:
        """Port of TS 'supports map() on arrays' (index.test.ts:1482-1502):
        per-element counters created inside the mapper, plus a PY-LOCAL
        outer counter the TS server calls back into per element."""
        client = await py_client_to_ts()
        try:
            main = client._client.get_main_stub()
            outer = PyCounter(0)
            outer_stub = RpcStub(outer)

            def mapper(i):
                counter = main.makeCounter(i)
                val = counter.increment(3)
                outer_stub.increment(1)
                return {"counter": counter, "val": val}

            fib = main.generateFibonacci(6)  # [0,1,1,2,3,5]
            counters = await asyncio.wait_for(fib.map(mapper), 15)

            assert [x["val"] for x in counters] == [3, 4, 4, 5, 6, 8]

            values = [
                await asyncio.wait_for(x["counter"].value, 10) for x in counters
            ]
            assert values == [3, 4, 4, 5, 6, 8]

            # The captured PY-side counter was bumped once per element by
            # the TS server (bidirectional callback inside a mapper).
            assert outer.count == 6
        finally:
            await client.__aexit__(None, None, None)

    async def test_nested_map(self, py_client_to_ts) -> None:
        """Port of TS 'supports nested map()' (index.test.ts:1504-1520)."""
        client = await py_client_to_ts()
        try:
            main = client._client.get_main_stub()
            fib = main.generateFibonacci(7)  # [0,1,1,2,3,5,8]
            result = await asyncio.wait_for(
                fib.map(
                    lambda i: main.generateFibonacci(i).map(
                        lambda j: main.generateFibonacci(j)
                    )
                ),
                30,
            )
            def fib_list(n: int) -> list[int]:
                out: list[int] = []
                a, b = 0, 1
                for _ in range(n):
                    out.append(a)
                    a, b = b, a + b
                return out

            expected = [
                [fib_list(j) for j in fib_list(i)] for i in fib_list(7)
            ]
            assert result == expected
        finally:
            await client.__aexit__(None, None, None)

    async def test_map_with_fused_pipeline_instruction(self, py_client_to_ts) -> None:
        """x.a.b(...) inside a mapper must record ONE fused instruction the
        TS evaluator accepts (lazy pathIfPromise parity)."""
        client = await py_client_to_ts()
        try:
            main = client._client.get_main_stub()
            fib = main.generateFibonacci(4)  # [0,1,1,2]
            # counter = makeCounter(i); then read .value via property map.
            result = await asyncio.wait_for(
                fib.map(lambda i: main.makeCounter(i).value), 15
            )
            assert result == [0, 1, 1, 2]
        finally:
            await client.__aexit__(None, None, None)


# =============================================================================
# Direction 2: TS client remap vs PY receiver
# =============================================================================


class TestTsRecorderRemapAgainstPyReceiver:
    """The TS server acts as the map CLIENT: its canonical recorder emits
    the remap over a promise pointing at a PY-hosted target, and the Python
    side evaluates it."""

    async def test_ts_remap_over_py_array(self, py_client_to_ts) -> None:
        client = await py_client_to_ts()
        try:
            result = await asyncio.wait_for(
                client.call("mapArrayRemote", [RpcStub(MapSource())]), 15
            )
            assert result == [2, 4, 6, 8, 10]
        finally:
            await client.__aexit__(None, None, None)

    async def test_ts_remap_over_py_null(self, py_client_to_ts) -> None:
        client = await py_client_to_ts()
        try:
            result = await asyncio.wait_for(
                client.call("mapNullRemote", [RpcStub(MapSource())]), 15
            )
            assert result is None
        finally:
            await client.__aexit__(None, None, None)

    async def test_ts_remap_over_py_single_value(self, py_client_to_ts) -> None:
        client = await py_client_to_ts()
        try:
            result = await asyncio.wait_for(
                client.call("mapSingleRemote", [RpcStub(MapSource()), 21]), 15
            )
            assert result == 42
        finally:
            await client.__aexit__(None, None, None)

    async def test_ts_nested_remap_over_py(self, py_client_to_ts) -> None:
        client = await py_client_to_ts()
        try:
            result = await asyncio.wait_for(
                client.call("mapNestedRemote", [RpcStub(MapSource())]), 30
            )
            # getNumbers() = [1..5]; makeList(x) = range(x); double each.
            assert result == [
                [0],
                [0, 2],
                [0, 2, 4],
                [0, 2, 4, 6],
                [0, 2, 4, 6, 8],
            ]
        finally:
            await client.__aexit__(None, None, None)

    async def test_ts_remap_with_captured_py_counter(self, py_client_to_ts) -> None:
        client = await py_client_to_ts()
        try:
            result = await asyncio.wait_for(
                client.call("mapCounterRemote", [RpcStub(MapSource())]), 15
            )
            # counter starts at 10; increments by 1..5 cumulatively.
            assert result == [11, 13, 16, 20, 25]
        finally:
            await client.__aexit__(None, None, None)
