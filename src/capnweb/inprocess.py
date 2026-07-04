"""In-process RPC session pairs over queue-pair transports.

Python analog of TS ``newMessagePortRpcSession`` (messageport.ts): two
directly-connected sessions in one process, no sockets. Useful for tests,
same-process privilege separation seams, and as the reference implementation
of a custom-encoding transport.

Differences from the TS MessagePort transport, deliberately:

* The TS transport declares ``encodingLevel: "structuredClonable"`` because
  ``postMessage`` structured-clones natively. Python has no structured clone,
  so the default here is the plain ``"string"`` level; pass
  ``encoding_level="jsonCompatible"`` (or ``"jsonCompatibleWithBytes"``) to
  skip the JSON stringify/parse round-trip — the queues pass value trees
  through untouched.
* Close signaling matches MessagePort exactly: a ``None`` sentinel posted to
  the peer means "connection closed" (messageport.ts:31-35, 79-85).

Trust model: both ends live in the SAME process and trust domain (exactly
like a MessagePort pair), so the receive queues are unbounded just as the TS
MessagePort receive queue is; this transport must not be used to bridge
untrusted peers.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from capnweb.rpc_session import BidirectionalSession, RpcSessionOptions

if TYPE_CHECKING:
    from capnweb.stubs import RpcStub

__all__ = ["InProcessPipeTransport", "new_pipe_rpc_session_pair"]

# Close sentinel posted to the peer's queue (MessagePort posts `null`).
_CLOSE_SENTINEL = None


class InProcessPipeTransport:
    """One end of an in-process queue pair (MessagePortTransport analog).

    ``send`` posts frames to the peer's receive queue; ``abort`` posts the
    ``None`` close sentinel and latches the error locally so subsequent
    operations fail fast (messageport.ts:56-99).
    """

    __slots__ = ("_error", "_in", "_out", "encoding_level")

    def __init__(
        self,
        out_queue: asyncio.Queue[Any],
        in_queue: asyncio.Queue[Any],
        encoding_level: str = "string",
    ) -> None:
        self._out = out_queue
        self._in = in_queue
        self._error: Exception | None = None
        # Read by BidirectionalSession at construction (rpc.ts:471-491);
        # unknown values are rejected there.
        self.encoding_level = encoding_level

    async def send(self, message: Any) -> None:
        if self._error is not None:
            raise self._error
        self._out.put_nowait(message)

    async def receive(self) -> Any:
        if self._error is not None:
            raise self._error
        message = await self._in.get()
        if message is _CLOSE_SENTINEL:
            # Peer signaled close (MessagePort `null` sentinel).
            self._set_error(ConnectionError("Peer closed pipe connection."))
            raise self._error  # type: ignore[misc]  # set above
        return message

    def abort(self, reason: Exception) -> None:
        # Best-effort close signal to the peer before latching the error
        # (messageport.ts:79-92).
        with contextlib.suppress(Exception):
            self._out.put_nowait(_CLOSE_SENTINEL)
        self._set_error(reason)

    def _set_error(self, error: Exception) -> None:
        if self._error is None:
            self._error = error


def new_pipe_rpc_session_pair(
    local_main_a: Any | None = None,
    local_main_b: Any | None = None,
    *,
    options_a: RpcSessionOptions | None = None,
    options_b: RpcSessionOptions | None = None,
    encoding_level: str = "string",
) -> tuple[RpcStub, RpcStub]:
    """Create two directly-connected in-process RPC sessions.

    Returns ``(main_of_b_as_seen_by_a, main_of_a_as_seen_by_b)`` — each side
    gets the OTHER side's main capability as an ``RpcStub``, exactly like
    calling ``newMessagePortRpcSession`` on both ports of a MessageChannel.
    Disposing a returned stub shuts its session down and (via the close
    sentinel) tears down the peer session too.

    Args:
        local_main_a: Main capability exposed BY side A (seen by side B).
        local_main_b: Main capability exposed BY side B (seen by side A).
        options_a: Session options for side A.
        options_b: Session options for side B.
        encoding_level: ``"string"`` (default), ``"jsonCompatible"``, or
            ``"jsonCompatibleWithBytes"`` — non-string levels skip the JSON
            stringify/parse round-trip entirely.
    """
    a_to_b: asyncio.Queue[Any] = asyncio.Queue()
    b_to_a: asyncio.Queue[Any] = asyncio.Queue()

    transport_a = InProcessPipeTransport(a_to_b, b_to_a, encoding_level)
    transport_b = InProcessPipeTransport(b_to_a, a_to_b, encoding_level)

    session_a = BidirectionalSession(transport_a, local_main_a, options_a)
    session_b = BidirectionalSession(transport_b, local_main_b, options_b)
    session_a.start()
    session_b.start()

    return session_a.get_remote_main(), session_b.get_remote_main()
