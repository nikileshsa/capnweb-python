"""Shared RpcTarget servers + a loopback HTTP-batch helper for the benches."""

from __future__ import annotations

import asyncio
from typing import Any

from capnweb.batch import BatchClientTransport, new_http_batch_rpc_response
from capnweb.inprocess import InProcessPipeTransport
from capnweb.rpc_session import BidirectionalSession
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget


class BenchService(RpcTarget):
    """A minimal server exercising the shapes the benches call."""

    def __init__(self) -> None:
        self._counter = 0

    async def call(self, method: str, args: list[Any]) -> Any:
        if method == "add":
            return args[0] + args[1]
        if method == "echo":
            return args[0]
        if method == "noop":
            return None
        if method == "incr":
            self._counter += 1
            return self._counter
        if method == "get_object":
            # A pipelining root: returns a record the client drills into.
            return {"value": args[0], "nested": {"leaf": args[0] * 2}}
        if method == "chain":
            # Pipeline step: takes the previous result, returns next.
            return {"n": (args[0] if args else 0) + 1}
        raise Exception(f"BenchService: no method {method}")

    async def get_property(self, name: str) -> Any:
        if name == "value" or name == "nested" or name == "leaf" or name == "n":
            return None
        raise Exception(f"BenchService: no property {name}")


class PipePair:
    """A loopback session pair whose sessions can be cleanly stopped.

    Same wiring as ``new_pipe_rpc_session_pair`` but keeps the session handles
    so benchmarks can ``await stop()`` and avoid leaking writer/read tasks
    (which otherwise flood stderr with "Task was destroyed" at loop close and
    perturb the fan-out memory measurement).
    """

    __slots__ = ("client", "server_view", "session_a", "session_b")

    def __init__(
        self,
        server_main: RpcTarget | None,
        client_main: RpcTarget | None = None,
        *,
        encoding_level: str = "string",
    ) -> None:
        # "string" = JSON round-trip per frame (the realistic WebSocket wire).
        # "jsonCompatible" = value trees pass through untouched (the closest
        # analog to TS's structured-clone MessagePort — no JSON stringify).
        a_to_b: asyncio.Queue[Any] = asyncio.Queue()
        b_to_a: asyncio.Queue[Any] = asyncio.Queue()
        ta = InProcessPipeTransport(a_to_b, b_to_a, encoding_level)
        tb = InProcessPipeTransport(b_to_a, a_to_b, encoding_level)
        # side A exposes client_main; side B exposes server_main. A's remote
        # main is B's server, so `client` calls the server.
        self.session_a = BidirectionalSession(ta, client_main, None)
        self.session_b = BidirectionalSession(tb, server_main, None)
        self.session_a.start()
        self.session_b.start()
        self.client: RpcStub = self.session_a.get_remote_main()
        self.server_view: RpcStub = self.session_b.get_remote_main()

    async def stop(self) -> None:
        await self.session_a.stop()
        await self.session_b.stop()


def loopback_batch_stub(
    local_main: RpcTarget,
) -> tuple[RpcStub, BidirectionalSession]:
    """An HTTP-batch client whose transport routes in-process to the server
    handler (no sockets) — measures the full batch encode/dispatch/decode path
    without network noise.
    """

    async def send_batch(batch: list[str]) -> list[str]:
        body = "\n".join(batch)
        resp = await new_http_batch_rpc_response(body, local_main)
        return resp.split("\n") if resp else []

    transport = BatchClientTransport(send_batch)
    session = BidirectionalSession(transport, None, None)
    session.start()
    return RpcStub(session.get_main_stub()), session
