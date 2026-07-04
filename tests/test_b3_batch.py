"""B3 tests: HTTP batch lifecycle — flush-on-first-pull client, server
session stop (no task leak), explicit timeout error path (no truncated 200),
CORS placement, unified aiohttp endpoint (matrix Part 5 §5.3 HTTP batch,
§5.1 newWorkersRpcResponse; TS batch.ts / index.ts:147-161).
"""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import ClientSession, web

from capnweb.batch import (
    BatchClientTransport,
    BatchEndError,
    aiohttp_batch_rpc_handler,
    aiohttp_rpc_handler,
    new_http_batch_rpc_response,
    new_http_batch_rpc_session,
)
from capnweb.config import RpcSessionConfig
from capnweb.types import RpcTarget


class SimpleApi(RpcTarget):
    def __init__(self) -> None:
        self.seen_headers: dict[str, str] = {}

    async def call(self, method: str, args: list) -> object:
        match method:
            case "echo":
                return args[0] if args else None
            case "add":
                return args[0] + args[1]
            case "hang":
                await asyncio.Event().wait()
                return None
            case _:
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


def batch_app(api: RpcTarget) -> web.Application:
    async def handler(request: web.Request) -> web.Response:
        return await aiohttp_batch_rpc_handler(request, api)

    app = web.Application()
    app.router.add_route("*", "/rpc", handler)
    return app


class TestBatchClientFlushTiming:
    """Flush happens on first pull, not on the first event-loop yield."""

    async def test_awaits_before_calls_do_not_flush_batch(self) -> None:
        """THE race regression: sleep between session creation and calls."""
        runner, port = await start_site(batch_app(SimpleApi()))
        try:
            stub = await new_http_batch_rpc_session(f"http://127.0.0.1:{port}/rpc")
            # Old behavior: sleep(0) flush => these yields sent an EMPTY
            # batch and the later call hung/failed.
            await asyncio.sleep(0.05)
            await asyncio.sleep(0)

            result = await asyncio.wait_for(stub.echo("late call"), timeout=5)
            assert result == "late call"
        finally:
            await runner.cleanup()

    async def test_multiple_calls_one_request(self) -> None:
        request_count = 0
        api = SimpleApi()

        async def counting_handler(request: web.Request) -> web.Response:
            nonlocal request_count
            request_count += 1
            return await aiohttp_batch_rpc_handler(request, api)

        app = web.Application()
        app.router.add_post("/rpc", counting_handler)
        runner, port = await start_site(app)
        try:
            stub = await new_http_batch_rpc_session(f"http://127.0.0.1:{port}/rpc")
            p1 = stub.echo("one")
            p2 = stub.add(2, 3)
            r1, r2 = await asyncio.wait_for(asyncio.gather(p1, p2), timeout=5)
            assert (r1, r2) == ("one", 5)
            assert request_count == 1
        finally:
            await runner.cleanup()

    async def test_transport_flushes_only_on_pull(self) -> None:
        flushed: list[list[str]] = []

        async def send_batch(batch: list[str]) -> list[str]:
            flushed.append(batch)
            return []

        transport = BatchClientTransport(send_batch)
        await transport.send(json.dumps(["push", ["pipeline", 0, ["m"], []]]))
        await asyncio.sleep(0.05)
        assert flushed == []  # pushes alone never flush

        await transport.send(json.dumps(["pull", 1]))
        await asyncio.sleep(0.05)
        assert len(flushed) == 1
        assert len(flushed[0]) == 2

    async def test_sends_after_flush_are_ignored(self) -> None:
        async def send_batch(batch: list[str]) -> list[str]:
            return []

        transport = BatchClientTransport(send_batch)
        await transport.send(json.dumps(["pull", 1]))
        await asyncio.sleep(0.05)
        await transport.send(json.dumps(["push", ["pipeline", 0, ["m"], []]]))
        with pytest.raises(BatchEndError):
            await transport.receive()

    async def test_one_batch_per_session(self) -> None:
        """Calls issued after the batch flushed fail; new session works."""
        runner, port = await start_site(batch_app(SimpleApi()))
        url = f"http://127.0.0.1:{port}/rpc"
        try:
            stub = await new_http_batch_rpc_session(url)
            assert await asyncio.wait_for(stub.echo("first"), timeout=5) == "first"

            # The batch is done; a second round trip on the same session
            # must fail (post-flush sends ignored; receive() exhausted).
            with pytest.raises(Exception):
                await asyncio.wait_for(stub.echo("second"), timeout=5)

            # A fresh session works.
            stub2 = await new_http_batch_rpc_session(url)
            assert await asyncio.wait_for(stub2.echo("second"), timeout=5) == "second"
        finally:
            await runner.cleanup()

    async def test_headers_kwarg_sent_with_request(self) -> None:
        seen: dict[str, str] = {}
        api = SimpleApi()

        async def handler(request: web.Request) -> web.Response:
            seen["authorization"] = request.headers.get("Authorization", "")
            return await aiohttp_batch_rpc_handler(request, api)

        app = web.Application()
        app.router.add_post("/rpc", handler)
        runner, port = await start_site(app)
        try:
            stub = await new_http_batch_rpc_session(
                f"http://127.0.0.1:{port}/rpc",
                headers={"Authorization": "Bearer sekrit"},
            )
            await asyncio.wait_for(stub.echo("x"), timeout=5)
            assert seen["authorization"] == "Bearer sekrit"
        finally:
            await runner.cleanup()


class TestBatchServerLifecycle:
    """new_http_batch_rpc_response stops its session; no truncation."""

    async def test_no_leaked_tasks_per_request(self) -> None:
        api = SimpleApi()
        push = json.dumps(["push", ["pipeline", 0, ["echo"], ["v"]]])
        pull = json.dumps(["pull", 1])
        body = f"{push}\n{pull}"

        # Warm up once so lazily-created singletons don't skew the count.
        await new_http_batch_rpc_response(body, api)
        await asyncio.sleep(0.05)

        baseline = len(asyncio.all_tasks())
        for _ in range(5):
            response = await new_http_batch_rpc_response(body, api)
            assert '"resolve"' in response or "resolve" in response
        await asyncio.sleep(0.05)
        assert len(asyncio.all_tasks()) <= baseline

    async def test_timeout_raises_instead_of_truncated_200(self) -> None:
        api = SimpleApi()
        push = json.dumps(["push", ["pipeline", 0, ["hang"], []]])
        pull = json.dumps(["pull", 1])
        options = RpcSessionConfig(drain_timeout=0.2)

        with pytest.raises(TimeoutError, match="refusing to return"):
            await new_http_batch_rpc_response(f"{push}\n{pull}", api, options)

    async def test_handler_returns_500_on_timeout(self) -> None:
        api = SimpleApi()
        options = RpcSessionConfig(drain_timeout=0.2)

        async def handler(request: web.Request) -> web.Response:
            return await aiohttp_batch_rpc_handler(request, api, options)

        app = web.Application()
        app.router.add_post("/rpc", handler)
        runner, port = await start_site(app)
        try:
            push = json.dumps(["push", ["pipeline", 0, ["hang"], []]])
            pull = json.dumps(["pull", 1])
            async with ClientSession() as client:
                async with client.post(
                    f"http://127.0.0.1:{port}/rpc", data=f"{push}\n{pull}"
                ) as resp:
                    assert resp.status == 500
                    assert "refusing to return" in await resp.text()
        finally:
            await runner.cleanup()

    async def test_empty_batch(self) -> None:
        assert await new_http_batch_rpc_response("", SimpleApi()) == ""


class TestCorsPlacement:
    """ACAO:* only on the unified endpoint (index.ts:147-161)."""

    async def test_standalone_batch_handler_sets_no_acao(self) -> None:
        runner, port = await start_site(batch_app(SimpleApi()))
        try:
            push = json.dumps(["push", ["pipeline", 0, ["echo"], ["x"]]])
            pull = json.dumps(["pull", 1])
            async with ClientSession() as client:
                async with client.post(
                    f"http://127.0.0.1:{port}/rpc", data=f"{push}\n{pull}"
                ) as resp:
                    assert resp.status == 200
                    assert "Access-Control-Allow-Origin" not in resp.headers
        finally:
            await runner.cleanup()

    async def test_standalone_batch_handler_extra_headers(self) -> None:
        api = SimpleApi()

        async def handler(request: web.Request) -> web.Response:
            return await aiohttp_batch_rpc_handler(
                request, api, headers={"X-Custom": "yes"}
            )

        app = web.Application()
        app.router.add_post("/rpc", handler)
        runner, port = await start_site(app)
        try:
            async with ClientSession() as client:
                async with client.post(f"http://127.0.0.1:{port}/rpc", data="") as resp:
                    assert resp.headers["X-Custom"] == "yes"
        finally:
            await runner.cleanup()

    async def test_non_post_returns_405(self) -> None:
        runner, port = await start_site(batch_app(SimpleApi()))
        try:
            async with ClientSession() as client:
                async with client.get(f"http://127.0.0.1:{port}/rpc") as resp:
                    assert resp.status == 405
        finally:
            await runner.cleanup()


class TestUnifiedRpcHandler:
    """aiohttp_rpc_handler: POST -> batch (+ACAO:*), WS -> session, else 400."""

    @staticmethod
    def unified_app(api: RpcTarget) -> web.Application:
        async def handler(request: web.Request) -> web.StreamResponse:
            return await aiohttp_rpc_handler(request, api)

        app = web.Application()
        app.router.add_route("*", "/rpc", handler)
        return app

    async def test_post_routes_to_batch_with_acao(self) -> None:
        runner, port = await start_site(self.unified_app(SimpleApi()))
        try:
            push = json.dumps(["push", ["pipeline", 0, ["echo"], ["cors"]]])
            pull = json.dumps(["pull", 1])
            async with ClientSession() as client:
                async with client.post(
                    f"http://127.0.0.1:{port}/rpc", data=f"{push}\n{pull}"
                ) as resp:
                    assert resp.status == 200
                    assert resp.headers["Access-Control-Allow-Origin"] == "*"
                    assert "cors" in await resp.text()
        finally:
            await runner.cleanup()

    async def test_websocket_upgrade_routes_to_ws_session(self) -> None:
        from capnweb.ws_session import new_websocket_rpc_session

        runner, port = await start_site(self.unified_app(SimpleApi()))
        try:
            stub = await new_websocket_rpc_session(f"ws://127.0.0.1:{port}/rpc")
            try:
                result = await asyncio.wait_for(stub.echo("over ws"), timeout=5)
                assert result == "over ws"
            finally:
                stub.dispose()
                await asyncio.sleep(0.05)
        finally:
            await runner.cleanup()

    async def test_other_requests_get_400(self) -> None:
        runner, port = await start_site(self.unified_app(SimpleApi()))
        try:
            async with ClientSession() as client:
                async with client.get(f"http://127.0.0.1:{port}/rpc") as resp:
                    assert resp.status == 400
                    assert "POST or WebSocket" in await resp.text()
        finally:
            await runner.cleanup()
