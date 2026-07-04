"""B3 tests: WebSocket transport semantics and entrypoints — close code
3000 + reason on abort (websocket.ts:120-133), retained close tasks,
new_websocket_rpc_session one-shot, event-driven wait_closed, per-connection
local_main_factory, UnifiedClient explicit transport selection
(matrix Part 5 §5.1/§5.3/§5.5).
"""

from __future__ import annotations

import asyncio
import warnings

import aiohttp
import pytest
from aiohttp import web

from capnweb.config import ClientConfig, RpcSessionConfig
from capnweb.stubs import RpcStub
from capnweb.types import RpcTarget
from capnweb.unified_client import UnifiedClient
from capnweb.ws_session import (
    WebSocketRpcClient,
    WebSocketRpcServer,
    handle_websocket_rpc,
    new_websocket_rpc_session,
    wait_closed,
)
from capnweb.ws_transport import (
    ABORT_CLOSE_CODE,
    WebSocketTransport,
)
from .support import rpc_call


class EchoService(RpcTarget):
    def __init__(self, tag: str = "echo") -> None:
        self.tag = tag

    async def call(self, method: str, args: list) -> object:
        if method == "echo":
            return args[0]
        if method == "whoami":
            return self.tag
        raise ValueError(f"Unknown method: {method}")

    async def get_property(self, name: str) -> object:
        raise AttributeError(name)


async def start_site(app: web.Application) -> tuple[web.AppRunner, int]:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, port


class TestAbortCloseSemantics:
    """abort() => close code 3000 + reason, task retained (websocket.ts:120-133)."""

    async def test_abort_closes_with_code_3000_and_reason(self) -> None:
        close_info: dict[str, object] = {}
        closed = asyncio.Event()

        async def ws_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                pass  # drain until close
            close_info["code"] = ws.close_code
            closed.set()
            return ws

        app = web.Application()
        app.router.add_get("/ws", ws_handler)
        runner, port = await start_site(app)
        try:
            async with aiohttp.ClientSession() as http:
                ws = await http.ws_connect(f"ws://127.0.0.1:{port}/ws")
                transport = WebSocketTransport(ws)

                transport.abort(RuntimeError("session exploded"))
                # The close task is retained and awaitable — not
                # fire-and-forget.
                assert transport._close_task is not None
                await transport.wait_closed()
                await asyncio.wait_for(closed.wait(), timeout=5)
                assert close_info["code"] == ABORT_CLOSE_CODE == 3000
        finally:
            await runner.cleanup()

    async def test_close_reason_truncated_to_ws_limit(self) -> None:
        from capnweb.ws_transport import _close_reason_bytes

        reason = "x" * 500
        encoded = _close_reason_bytes(RuntimeError(reason))
        assert len(encoded) <= 123

        # Multi-byte truncation must not split a code point.
        encoded = _close_reason_bytes("é" * 200)
        encoded.decode("utf-8")  # must not raise

    async def test_session_abort_reaches_peer_as_3000(self) -> None:
        """End-to-end: session-level abort closes the WS with 3000."""
        server_ws_close: dict[str, object] = {}
        closed = asyncio.Event()

        async def ws_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            # Send garbage to force the client session to abort.
            await ws.send_str("this is not a capnweb message")
            async for _msg in ws:
                pass
            server_ws_close["code"] = ws.close_code
            closed.set()
            return ws

        app = web.Application()
        app.router.add_get("/ws", ws_handler)
        runner, port = await start_site(app)
        try:
            stub = await new_websocket_rpc_session(f"ws://127.0.0.1:{port}/ws")
            try:
                await asyncio.wait_for(closed.wait(), timeout=5)
                assert server_ws_close["code"] == 3000
            finally:
                stub.dispose()
                await asyncio.sleep(0.05)
        finally:
            await runner.cleanup()


class TestNewWebSocketRpcSession:
    """new_websocket_rpc_session(ws_or_url) -> RpcStub (websocket.ts:10-19)."""

    async def test_from_url(self) -> None:
        port = _free_port()
        server = WebSocketRpcServer(EchoService(), host="127.0.0.1", port=port)
        await server.start()
        try:
            stub = await new_websocket_rpc_session(f"ws://127.0.0.1:{port}/rpc")
            assert isinstance(stub, RpcStub)
            try:
                assert await asyncio.wait_for(stub.echo("url"), timeout=5) == "url"
            finally:
                stub.dispose()
                await asyncio.sleep(0.05)
        finally:
            await server.stop()

    async def test_from_existing_websocket(self) -> None:
        port = _free_port()
        server = WebSocketRpcServer(EchoService(), host="127.0.0.1", port=port)
        await server.start()
        try:
            async with aiohttp.ClientSession() as http:
                ws = await http.ws_connect(f"ws://127.0.0.1:{port}/rpc")
                stub = await new_websocket_rpc_session(ws)
                try:
                    result = await asyncio.wait_for(stub.echo("existing"), timeout=5)
                    assert result == "existing"
                finally:
                    stub.dispose()
                    await asyncio.sleep(0.05)
        finally:
            await server.stop()

    async def test_dispose_main_stub_shuts_down_and_closes_socket(self) -> None:
        """§5.6: disposing the main stub tears the session down (rpc.ts:506)."""
        disconnected = asyncio.Event()

        async def ws_handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for _msg in ws:
                pass
            disconnected.set()
            return ws

        app = web.Application()
        app.router.add_get("/ws", ws_handler)
        runner, port = await start_site(app)
        try:
            stub = await new_websocket_rpc_session(f"ws://127.0.0.1:{port}/ws")
            stub.dispose()
            await asyncio.wait_for(disconnected.wait(), timeout=5)
        finally:
            await runner.cleanup()


class TestWaitClosedKeepalive:
    """Server handlers are event-driven — no ws.closed poll loop."""

    async def test_handler_returns_promptly_after_client_disconnect(self) -> None:
        handler_done = asyncio.Event()

        async def handler(request: web.Request) -> web.WebSocketResponse:
            ws = await handle_websocket_rpc(request, EchoService())
            handler_done.set()
            return ws

        app = web.Application()
        app.router.add_get("/rpc", handler)
        runner, port = await start_site(app)
        try:
            client = WebSocketRpcClient(f"ws://127.0.0.1:{port}/rpc")
            async with client:
                stub = client.get_main_stub()
                assert await asyncio.wait_for(stub.echo("hi"), timeout=5) == "hi"
            # Client closed: the server handler must finish promptly.
            await asyncio.wait_for(handler_done.wait(), timeout=5)
        finally:
            await runner.cleanup()

    async def test_wait_closed_helper_wakes_on_session_end(self) -> None:
        from capnweb.batch import BatchServerTransport
        from capnweb.rpc_session import BidirectionalSession

        session = BidirectionalSession(BatchServerTransport([]), None)
        waiter = asyncio.create_task(wait_closed(session))
        await asyncio.sleep(0.05)
        assert not waiter.done()

        session.shutdown()
        await asyncio.wait_for(waiter, timeout=2)
        await session.stop()


class TestPerConnectionMain:
    """local_main_factory creates a fresh main per connection (bun.ts:39-44)."""

    async def test_each_connection_gets_its_own_main(self) -> None:
        counter = {"n": 0}

        def factory() -> EchoService:
            counter["n"] += 1
            return EchoService(tag=f"conn-{counter['n']}")

        port = _free_port()
        server = WebSocketRpcServer(
            local_main_factory=factory, host="127.0.0.1", port=port
        )
        await server.start()
        try:
            tags = []
            for _ in range(2):
                async with WebSocketRpcClient(f"ws://127.0.0.1:{port}/rpc") as c:
                    stub = c.get_main_stub()
                    tags.append(await asyncio.wait_for(stub.whoami(), timeout=5))
            assert tags == ["conn-1", "conn-2"]
        finally:
            await server.stop()

    async def test_requires_some_main(self) -> None:
        with pytest.raises(ValueError, match="local_main"):
            WebSocketRpcServer()


class TestOnRpcBrokenOverWire:
    """stub.on_rpc_broken fires when the connection dies (P0 #3 e2e)."""

    async def test_callback_fires_on_server_death(self) -> None:
        port = _free_port()
        server = WebSocketRpcServer(EchoService(), host="127.0.0.1", port=port)
        await server.start()

        broken: list[Exception] = []
        client = WebSocketRpcClient(f"ws://127.0.0.1:{port}/rpc")
        async with client:
            stub = client.get_main_stub()
            stub.on_rpc_broken(broken.append)
            assert await asyncio.wait_for(stub.echo("pre"), timeout=5) == "pre"

            await server.stop()
            for _ in range(100):
                if broken:
                    break
                await asyncio.sleep(0.02)
            assert len(broken) == 1
            assert isinstance(broken[0], Exception)


class TestUnifiedClientTransportSelection:
    """Explicit transport enum; no URL heuristics for WebTransport."""

    async def test_auto_selects_websocket_for_ws_url(self) -> None:
        port = _free_port()
        server = WebSocketRpcServer(EchoService(), host="127.0.0.1", port=port)
        await server.start()
        try:
            config = ClientConfig(url=f"ws://127.0.0.1:{port}/rpc")
            async with UnifiedClient(config) as client:
                assert client.is_bidirectional
                stub = client.get_main_stub()
                assert await asyncio.wait_for(stub.echo("ws"), timeout=5) == "ws"
        finally:
            await server.stop()

    async def test_wt_like_urls_are_not_webtransport(self) -> None:
        """':4433' / '/wt' in an http URL must NOT trigger WebTransport."""
        for url in ("http://example.com:4433/rpc", "https://example.com/wt/rpc"):
            client = UnifiedClient(ClientConfig(url=url))
            assert client._transport_mode == "http-batch"

    async def test_explicit_webtransport_required(self) -> None:
        client = UnifiedClient(
            ClientConfig(url="https://example.com/rpc", transport="webtransport")
        )
        assert client._transport_mode == "webtransport"
        with pytest.raises((NotImplementedError, RuntimeError)):
            async with client:
                pass

    async def test_mismatched_transport_url_rejected(self) -> None:
        with pytest.raises(ValueError, match="ws://"):
            UnifiedClient(
                ClientConfig(url="http://example.com/rpc", transport="websocket")
            )
        with pytest.raises(ValueError, match="http"):
            UnifiedClient(
                ClientConfig(url="ws://example.com/rpc", transport="http-batch")
            )

    async def test_http_batch_mode_uses_one_shot_batch_sessions(self) -> None:
        from capnweb.batch import aiohttp_batch_rpc_handler

        api = EchoService()

        async def handler(request: web.Request) -> web.Response:
            return await aiohttp_batch_rpc_handler(request, api)

        app = web.Application()
        app.router.add_post("/rpc", handler)
        runner, port = await start_site(app)
        try:
            config = ClientConfig(url=f"http://127.0.0.1:{port}/rpc")
            async with UnifiedClient(config) as client:
                assert not client.is_bidirectional
                stub = await client.new_batch()
                with stub:
                    result = await asyncio.wait_for(
                        stub.echo("batched"), timeout=5
                    )
                assert result == "batched"
                # And again — batch sessions are one-shot.
                stub = await client.new_batch()
                with stub:
                    result = await asyncio.wait_for(stub.echo("again"), timeout=5)
                assert result == "again"
        finally:
            await runner.cleanup()

    async def test_positional_call_api_is_gone(self) -> None:
        """The deprecated client.call(cap_id, method, args) API was DELETED
        in Phase C (TS has no such API — the stub IS the API)."""
        assert not hasattr(WebSocketRpcClient, "call")
        assert not hasattr(WebSocketRpcClient, "_call")
        assert not hasattr(UnifiedClient, "call")
        assert not hasattr(UnifiedClient, "_call")


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
