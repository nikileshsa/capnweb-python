"""Shared loopback harness for the Cap'n Web security suite.

Drives the REAL session receive path (``BidirectionalSession`` + its read
loop) with attacker-controlled wire frames over an in-memory transport, then
lets the test inspect the resulting protocol state (abort reason, table
sizes, refcounts) and the frames the session emitted back.

The adversary model: the peer is fully malicious code inside a MicroVM
sandbox dialing back to a trusted control plane. It may send ANY bytes / any
sequence of wire messages. The invariant we protect: the control-plane side
must never crash (unhandled non-RpcError escaping the read loop), never hang,
and never let a refcount go negative — it either processes the frame or
aborts the session cleanly.
"""

from __future__ import annotations

import asyncio
from typing import Any

from capnweb.rpc_session import BidirectionalSession
from capnweb.types import RpcTarget


class _Echo(RpcTarget):
    """Minimal local main so a peer's ``["pipeline", 0, ...]`` has a target."""

    def greet(self, name: Any = None) -> str:
        return f"hi {name}"

    def boom(self) -> str:  # deliberately raises to probe the error channel
        raise FileNotFoundError(2, "No such file or directory", "/etc/control-plane-secret.key")

    def identity(self, x: Any = None) -> Any:
        return x


class LoopbackTransport:
    """Feed frames in via ``inject``; collect outbound frames in ``out``.

    ``receive`` yields injected frames FIFO; a ``None`` sentinel raises
    ConnectionError (clean peer close). ``encoding_level='string'`` = the
    network-facing JSON wire path (the one a MicroVM peer speaks).
    """

    encoding_level = "string"

    def __init__(self) -> None:
        self.inq: asyncio.Queue[Any] = asyncio.Queue()
        self.out: list[Any] = []
        self.aborted: Exception | None = None

    async def send(self, message: Any) -> None:
        self.out.append(message)

    async def receive(self) -> Any:
        message = await self.inq.get()
        if message is None:
            raise ConnectionError("peer closed")
        return message

    def abort(self, reason: Exception) -> None:
        self.aborted = reason

    def inject(self, *frames: Any) -> None:
        for frame in frames:
            self.inq.put_nowait(frame)


class DrivenSession:
    """A started session + its loopback transport, for one test/example."""

    def __init__(self, local_main: Any | None = None, options: Any | None = None) -> None:
        self.transport = LoopbackTransport()
        self.session = BidirectionalSession(
            self.transport, local_main=local_main if local_main is not None else _Echo(),
            options=options,
        )
        self.session.start()

    def inject(self, *frames: Any) -> None:
        self.transport.inject(*frames)

    async def settle(self, quiet: float = 0.05) -> None:
        """Let the read loop drain injected frames (bounded by caller)."""
        # Poll until the inbound queue is empty and the loop has caught up, or
        # the session aborts. Bounded number of ticks so a stall can't hang.
        for _ in range(200):
            if self.session._abort_reason is not None:
                return
            if self.transport.inq.empty():
                await asyncio.sleep(quiet)
                if self.transport.inq.empty():
                    return
            await asyncio.sleep(0.005)

    @property
    def abort_reason(self) -> Exception | None:
        return self.session._abort_reason

    @property
    def n_exports(self) -> int:
        return len(self.session._exports)

    @property
    def n_imports(self) -> int:
        return len(self.session._imports)

    def min_export_refcount(self) -> int | None:
        return min((e.refcount for e in self.session._exports.values()), default=None)

    async def close(self) -> None:
        self.transport.inject(None)
        try:
            await asyncio.wait_for(self.session.stop(), 5)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            pass


async def drive(frames: list[Any], *, time_budget: float = 5.0,
                local_main: Any | None = None, options: Any | None = None) -> DrivenSession:
    """Inject ``frames`` into a fresh session and return it after settling.

    The whole run is bounded by ``time_budget`` so a hostile frame that would
    hang surfaces as ``asyncio.TimeoutError`` in the caller, not a stuck test.
    """
    d = DrivenSession(local_main=local_main, options=options)

    async def _run() -> None:
        d.inject(*frames)
        await d.settle()

    await asyncio.wait_for(_run(), time_budget)
    return d
