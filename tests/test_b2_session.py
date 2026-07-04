"""B2 stream — session-level map/remap + lifecycle tests.

Pins: end-to-end .map() over a real session pair (real mapped results, not
literal-0), lazy-path pipelining on the wire (one fused push per chain),
remap propertyPath always [] on emit / lenient null accept, remap protocol
failures abort like TS, the stop() deadlock fix (writer dies on send
failure), native wait_closed()/get_remote_main(), and pull_timeout sourcing
from RpcSessionConfig.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from capnweb.config import RpcSessionConfig
from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.rpc_session import BidirectionalSession, ImportHook
from capnweb.stubs import RpcPromise, RpcStub, get_remote_main
from capnweb.types import RpcTarget


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class PairedTransport:
    def __init__(self) -> None:
        self.peer: "PairedTransport | None" = None
        self.inbox: asyncio.Queue[str] = asyncio.Queue()
        self.sent: list[str] = []
        self.closed = False

    async def send(self, message: str) -> None:
        if self.closed:
            raise ConnectionError("Transport closed")
        self.sent.append(message)
        if self.peer:
            await self.peer.inbox.put(message)

    async def receive(self) -> str:
        return await self.inbox.get()

    def abort(self, reason: Exception) -> None:
        self.closed = True


def transport_pair() -> tuple[PairedTransport, PairedTransport]:
    a, b = PairedTransport(), PairedTransport()
    a.peer, b.peer = b, a
    return a, b


def sent_messages(transport: PairedTransport) -> list[list[Any]]:
    out: list[list[Any]] = []
    for frame in transport.sent:
        for line in frame.split("\n"):
            if line.strip():
                out.append(json.loads(line))
    return out


class MapTarget(RpcTarget):
    """Server target in the TS TestTarget style."""

    def __init__(self) -> None:
        self.counters: list["Counter"] = []

    async def call(self, method: str, args: list) -> Any:
        if method == "getList":
            return [1, 2, 3, 4, 5]
        if method == "generateFibonacci":
            n = args[0]
            out: list[int] = []
            a, b = 0, 1
            for _ in range(n):
                out.append(a)
                a, b = b, a + b
            return out
        if method == "makeCounter":
            counter = Counter(args[0])
            self.counters.append(counter)
            return counter
        if method == "returnNull":
            return None
        if method == "returnNumber":
            return args[0]
        if method == "getData":
            return {"data": [10, 20, 30]}
        raise RpcError.not_found(method)

    async def get_property(self, name: str) -> Any:
        raise AttributeError(name)


class Counter(RpcTarget):
    def __init__(self, initial: int = 0) -> None:
        self.value_ = initial

    async def call(self, method: str, args: list) -> Any:
        if method == "increment":
            self.value_ += args[0] if args else 1
            return self.value_
        raise RpcError.not_found(method)

    async def get_property(self, name: str) -> Any:
        if name == "value":
            return self.value_
        raise AttributeError(name)


async def session_pair(
    server_main: Any,
) -> tuple[BidirectionalSession, BidirectionalSession, PairedTransport, PairedTransport]:
    ta, tb = transport_pair()
    client = BidirectionalSession(ta)
    server = BidirectionalSession(tb, local_main=server_main)
    client.start()
    server.start()
    await asyncio.sleep(0)
    return client, server, ta, tb


# ---------------------------------------------------------------------------
# End-to-end .map() over a real session pair
# ---------------------------------------------------------------------------


class TestMapEndToEnd:
    async def test_map_identity_returns_real_values(self) -> None:
        client, server, *_ = await session_pair(MapTarget())
        try:
            main = get_remote_main(client)
            result = await asyncio.wait_for(main.getList().map(lambda x: x), 5)
            assert result == [1, 2, 3, 4, 5]
        finally:
            await client.stop()
            await server.stop()

    async def test_map_with_captured_counter(self) -> None:
        """Port of the TS 'supports map() on arrays' counter scenario."""
        target = MapTarget()
        client, server, *_ = await session_pair(target)
        try:
            main = get_remote_main(client)
            outer = await asyncio.wait_for(main.makeCounter(0), 5)

            fib = main.generateFibonacci(6)
            result = await asyncio.wait_for(
                fib.map(lambda i: outer.increment(i)), 5
            )
            # fib(6) = [0,1,1,2,3,5]; running increments: 0,1,2,4,7,12
            assert result == [0, 1, 2, 4, 7, 12]
        finally:
            await client.stop()
            await server.stop()

    async def test_map_on_null_and_single_value(self) -> None:
        target = MapTarget()
        client, server, *_ = await session_pair(target)
        try:
            main = get_remote_main(client)
            counter = await asyncio.wait_for(main.makeCounter(0), 5)

            assert await asyncio.wait_for(
                main.returnNull().map(lambda _: counter.increment(123)), 5
            ) is None
            # Counter must NOT have been invoked for a null input.
            assert target.counters[0].value_ == 0

            assert await asyncio.wait_for(
                main.returnNumber(2).map(lambda i: counter.increment(i)), 5
            ) == 2
            assert await asyncio.wait_for(
                main.returnNumber(4).map(lambda i: counter.increment(i)), 5
            ) == 6
        finally:
            await client.stop()
            await server.stop()

    async def test_map_over_property_path(self) -> None:
        """stub.getData().data.map(f): path derived from the promise chain."""
        client, server, *_ = await session_pair(MapTarget())
        try:
            main = get_remote_main(client)
            result = await asyncio.wait_for(
                main.getData().data.map(lambda x: x), 5
            )
            assert result == [10, 20, 30]
        finally:
            await client.stop()
            await server.stop()

    async def test_map_returns_stubs(self) -> None:
        """Mapper returning capability-bearing structures (TS counters map)."""
        client, server, *_ = await session_pair(MapTarget())
        try:
            main = get_remote_main(client)
            fib = main.generateFibonacci(4)  # [0,1,1,2]
            counters = await asyncio.wait_for(
                fib.map(lambda i: main.makeCounter(i)), 5
            )
            assert len(counters) == 4
            values = [
                await asyncio.wait_for(c.value, 5) for c in counters
            ]
            assert values == [0, 1, 1, 2]
        finally:
            await client.stop()
            await server.stop()

    async def test_nested_map(self) -> None:
        """Port of the TS 'supports nested map()' scenario (n=5)."""
        client, server, *_ = await session_pair(MapTarget())
        try:
            main = get_remote_main(client)
            fib = main.generateFibonacci(5)  # [0,1,1,2,3]
            result = await asyncio.wait_for(
                fib.map(
                    lambda i: main.generateFibonacci(i).map(
                        lambda j: main.generateFibonacci(j)
                    )
                ),
                10,
            )
            assert result == [
                [],
                [[]],
                [[]],
                [[], [0]],
                [[], [0], [0]],
            ]
        finally:
            await client.stop()
            await server.stop()

    async def test_map_wire_shape(self) -> None:
        """The remap push carries [] path (never null) and raw instructions."""
        client, server, ta, _tb = await session_pair(MapTarget())
        try:
            main = get_remote_main(client)
            await asyncio.wait_for(main.getList().map(lambda x: x), 5)
            remaps = [
                m for m in sent_messages(ta)
                if m[0] == "push"
                and isinstance(m[1], list)
                and m[1] and m[1][0] == "remap"
            ]
            assert len(remaps) == 1
            remap = remaps[0][1]
            assert remap[2] == [], "propertyPath must be [] on the wire"
            assert remap[3] == []
            assert remap[4] == [["pipeline", 0]]
        finally:
            await client.stop()
            await server.stop()

    async def test_remap_null_path_still_accepted_on_receive(self) -> None:
        """Lenient receive: a pre-B2 Python peer may emit a null path."""
        client, server, ta, _tb = await session_pair(MapTarget())
        try:
            main = get_remote_main(client)
            # Prime an import for the list.
            list_promise = main.getList()
            hook = list_promise._raw_hook
            assert isinstance(hook, ImportHook)
            # Let the client's push flush to the server before injecting.
            await asyncio.sleep(0.1)
            # Manually inject a remap with null path over that import.
            frame = json.dumps(
                ["push", ["remap", hook.import_id, None, [], [["pipeline", 0]]]]
            )
            await ta.peer.inbox.put(frame)  # type: ignore[union-attr]
            # Pull the remap result: it is the NEXT peer-push export on the
            # server, i.e. our import; easiest end-to-end check is that the
            # session did NOT abort.
            await asyncio.sleep(0.2)
            assert server._abort_reason is None
            assert await asyncio.wait_for(list_promise, 5) == [1, 2, 3, 4, 5]
        finally:
            await client.stop()
            await server.stop()

    async def test_remap_unknown_target_aborts(self) -> None:
        """Protocol errors in remap evaluation abort the session (TS)."""
        client, server, *_ = await session_pair(MapTarget())
        try:
            frame = json.dumps(["push", ["remap", 99, [], [], [0]]])
            await server.transport.peer.inbox.put(frame)  # type: ignore
            # server receives it and must abort
            await asyncio.wait_for(server.wait_closed(), 5)
            assert "no such entry on exports table: 99" in str(server._abort_reason)
        finally:
            await client.stop()
            await server.stop()

    async def test_mapper_export_ref_rejects_result_not_session(self) -> None:
        """["export", n] INSIDE instructions is an app-level mapper error:
        the map result rejects; the session survives (matrix 04 row 15)."""
        client, server, *_ = await session_pair(MapTarget())
        try:
            main = get_remote_main(client)
            list_promise = main.getList()
            hook = list_promise._raw_hook
            assert isinstance(hook, ImportHook)
            mapped = hook.map([], [], [["export", 1]])
            with pytest.raises(RpcError, match="cannot refer to exports"):
                await asyncio.wait_for(RpcPromise(mapped), 5)
            assert server._abort_reason is None
            assert client._abort_reason is None
        finally:
            await client.stop()
            await server.stop()


class TestImportCallCoercion:
    """Row 21: ["import", id, path, args] call-coercion in general
    expressions (serialize.ts:826-902) — accepted as a push subject."""

    async def test_import_with_path_and_args_as_push_subject(self) -> None:
        client, server, ta, _tb = await session_pair(MapTarget())
        try:
            # Push 1 (client -> server): pipeline call getList (import 1).
            main = get_remote_main(client)
            list_promise = main.getList()
            await asyncio.sleep(0.05)

            # Push 2 (raw): 4-element "import" coercion calling our own
            # export 0 (returnNumber) — evaluates to a STUB, not a promise.
            frame = json.dumps(
                ["push", ["import", 0, ["returnNumber"], [7]]]
            )
            await ta.peer.inbox.put(frame)  # deliver to the server
            await asyncio.sleep(0.1)
            assert server._abort_reason is None

            # Pull import 2 from the client side: "import" coerces the call
            # result to a STUB (serialize.ts:884-901), so the delivered
            # value is a capability wrapping the result, not a bare value.
            hook = client.import_capability(2)
            from capnweb.rpc_session import ImportHook as IH
            assert isinstance(hook, IH)
            result = await asyncio.wait_for(RpcPromise(hook.get([])), 5)
            assert isinstance(result, RpcStub), (
                "4-element import must coerce the call result to a stub"
            )
            # And the session kept working normally around it.
            assert await asyncio.wait_for(list_promise, 5) == [1, 2, 3, 4, 5]
        finally:
            await client.stop()
            await server.stop()


# ---------------------------------------------------------------------------
# Lazy-path pipelining on the wire
# ---------------------------------------------------------------------------


class TestLazyPathWire:
    async def test_property_chain_fuses_into_one_push(self) -> None:
        client, server, ta, _tb = await session_pair(MapTarget())
        try:
            main = get_remote_main(client)
            data = main.getData()          # push 1 (call)
            chained = data.data            # NO push (lazy)
            await asyncio.sleep(0.05)
            pushes = [m for m in sent_messages(ta) if m[0] == "push"]
            assert len(pushes) == 1, "property access must not send traffic"

            value = await asyncio.wait_for(chained, 5)
            assert value == [10, 20, 30]
            pushes = [m for m in sent_messages(ta) if m[0] == "push"]
            assert len(pushes) == 2
            # The materialized get is ONE fused property-reference pipeline
            # (no args element — a get, not a call).
            assert pushes[1][1][2] == ["data"]
            assert len(pushes[1][1]) == 3
        finally:
            await client.stop()
            await server.stop()

    async def test_chained_call_paths_fuse(self) -> None:
        client, server, ta, _tb = await session_pair(MapTarget())
        try:
            main = get_remote_main(client)
            counter = main.makeCounter(10)
            # counter.increment(5): ONE push ["pipeline", id, ["increment"], [5]]
            result = await asyncio.wait_for(counter.increment(5), 5)
            assert result == 15
            pushes = [m for m in sent_messages(ta) if m[0] == "push"]
            call_push = pushes[1][1]
            assert call_push[2] == ["increment"]
            assert call_push[3] == [5]
        finally:
            await client.stop()
            await server.stop()


# ---------------------------------------------------------------------------
# Session lifecycle fixes (B2 asks from the blockers log)
# ---------------------------------------------------------------------------


class FailingTransport:
    """Transport whose send() always fails (peer socket gone)."""

    def __init__(self) -> None:
        self.aborted: Exception | None = None
        self._never = asyncio.Queue[str]()

    async def send(self, message: str) -> None:
        raise ConnectionError("peer went away")

    async def receive(self) -> str:
        return await self._never.get()

    def abort(self, reason: Exception) -> None:
        self.aborted = reason


class TestStopRobustness:
    async def test_stop_never_hangs_when_writer_dies_on_send(self) -> None:
        """B3's deadlock report (2026-07-04): the writer exits on a send
        failure without consuming the stop sentinel; stop() used to hang on
        the abort-flush join. Must now complete promptly."""
        transport = FailingTransport()
        session = BidirectionalSession(transport)
        session.start()
        await asyncio.sleep(0)

        # Queue a frame; the writer will die trying to send it.
        with pytest.raises(Exception):
            # send_call queues the push; the writer failure aborts async.
            session.send_call(0, ["m"], RpcPayload.owned([]))
            await asyncio.sleep(0.1)
            raise RuntimeError("force-exit context")  # noqa: TRY301

        # stop() must return quickly regardless of writer state.
        await asyncio.wait_for(session.stop(), timeout=3.0)

    async def test_stop_never_hangs_on_unaborted_session_with_dead_peer(self) -> None:
        """stop() on a NOT-yet-aborted session flushes an abort frame; if
        that send fails the whole teardown must still complete."""
        transport = FailingTransport()
        session = BidirectionalSession(transport)
        session.start()
        await asyncio.sleep(0)

        await asyncio.wait_for(session.stop(), timeout=3.0)

    async def test_repeated_stop_is_idempotent(self) -> None:
        transport = FailingTransport()
        session = BidirectionalSession(transport)
        session.start()
        await asyncio.sleep(0)
        await asyncio.wait_for(session.stop(), timeout=3.0)
        await asyncio.wait_for(session.stop(), timeout=3.0)


class TestNativeSessionSurface:
    async def test_wait_closed_native(self) -> None:
        client, server, *_ = await session_pair(MapTarget())
        waiter = asyncio.create_task(client.wait_closed())
        await asyncio.sleep(0.05)
        assert not waiter.done()
        await client.stop()
        await asyncio.wait_for(waiter, timeout=2.0)
        await server.stop()

    async def test_get_remote_main_native(self) -> None:
        client, server, *_ = await session_pair(MapTarget())
        try:
            stub = client.get_remote_main()
            assert isinstance(stub, RpcStub)
            assert await asyncio.wait_for(stub.returnNumber(9), 5) == 9
            # The module-level helper defers to the native method.
            helper_stub = get_remote_main(client)
            assert isinstance(helper_stub, RpcStub)
        finally:
            await client.stop()
            await server.stop()

    async def test_pull_timeout_from_config(self) -> None:
        ta, _tb = transport_pair()
        session = BidirectionalSession(
            ta, options=RpcSessionConfig(pull_timeout=7.5)
        )
        assert session._pull_timeout == 7.5

    async def test_pull_timeout_transport_attr_is_ignored(self) -> None:
        """Phase C dropped the transport-attribute override: the session
        reads RpcSessionConfig.pull_timeout ONLY (B3 handoff, 2026-07-04)."""
        ta, _tb = transport_pair()
        ta.pull_timeout = 3.0  # type: ignore[attr-defined]
        session = BidirectionalSession(
            ta, options=RpcSessionConfig(pull_timeout=7.5)
        )
        assert session._pull_timeout == 7.5

    async def test_pull_timeout_defaults_to_120(self) -> None:
        ta, _tb = transport_pair()
        session = BidirectionalSession(ta)
        assert session._pull_timeout == 120.0
