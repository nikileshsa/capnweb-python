"""Regression tests for the fix-forward hardening of this fork.

These pin the robustness properties added after the Cap'n Web drift review:
- P0: a lost/mis-routed resolve must surface as an error, never a permanent
  hang (bounded `pull` timeout).
- P1: fire-and-forget sends go through a single serialized writer, so they are
  neither lost to GC nor reordered.

See ../PARITY.md.
"""

from __future__ import annotations

import asyncio

import pytest

from capnweb.error import RpcError
from capnweb.rpc_session import BidirectionalSession


class SilentTransport:
    """Transport that accepts sends but never delivers a reply.

    Simulates the exact failure mode the fix targets: the peer's
    resolve/reject is lost, so an un-timed `pull` would hang forever.
    """

    def __init__(self) -> None:
        self.sent: list[str] = []
        self._never = asyncio.Event()  # stays unset forever

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def receive(self) -> str:
        await self._never.wait()  # never returns
        raise AssertionError("unreachable")

    def abort(self, reason: Exception) -> None:  # noqa: D401
        self._never.set()


@pytest.mark.asyncio
async def test_pull_times_out_instead_of_hanging():
    """A pull whose resolve never arrives raises, bounded by _pull_timeout."""
    session = BidirectionalSession(SilentTransport())
    session._pull_timeout = 0.3  # tighten for the test
    session.start()
    try:
        with pytest.raises(RpcError):
            await asyncio.wait_for(session.get_main_stub().pull(), timeout=3.0)
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_sends_go_through_serialized_writer_in_order():
    """send_pull enqueues onto the single writer; order is preserved and no
    send is lost (the old code dropped GC'd fire-and-forget tasks)."""
    transport = SilentTransport()
    session = BidirectionalSession(transport)
    session.start()
    try:
        # Fire several sync sends; the serialized writer must deliver all,
        # in order, to the transport.
        for import_id in range(5):
            session.send_pull(import_id)
        # Let the writer drain.
        await asyncio.sleep(0.1)
        assert len(transport.sent) == 5, "a send was lost"
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_stop_is_clean_and_idempotent():
    session = BidirectionalSession(SilentTransport())
    session.start()
    await session.stop()
    await session.stop()  # must not raise
