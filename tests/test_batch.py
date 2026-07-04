"""Tests for HTTP Batch RPC transport."""

import asyncio
import pytest

from capnweb.batch import (
    BatchClientTransport,
    BatchServerTransport,
    BatchEndError,
    new_http_batch_rpc_response,
)
from capnweb.rpc_session import BidirectionalSession
from capnweb.types import RpcTarget


class SimpleApi(RpcTarget):
    """Simple API for testing."""

    def __init__(self):
        self.call_count = 0

    async def call(self, method: str, args: list) -> any:
        self.call_count += 1
        match method:
            case "echo":
                return args[0] if args else None
            case "add":
                return args[0] + args[1]
            case "greet":
                name = args[0] if args else "World"
                return f"Hello, {name}!"
            case "getCallCount":
                return self.call_count
            case _:
                raise ValueError(f"Unknown method: {method}")

    async def get_property(self, name: str) -> any:
        if name == "callCount":
            return self.call_count
        raise AttributeError(f"Unknown property: {name}")


class TestBatchServerTransport:
    """Test BatchServerTransport."""

    def test_init(self):
        """Test transport initialization."""
        batch = ["msg1", "msg2", "msg3"]
        transport = BatchServerTransport(batch)
        assert transport._batch_to_receive == batch
        assert transport._batch_to_send == []
        assert transport._receive_index == 0

    @pytest.mark.asyncio
    async def test_receive_messages(self):
        """Test receiving messages from batch."""
        batch = ["msg1", "msg2"]
        transport = BatchServerTransport(batch)

        msg1 = await transport.receive()
        assert msg1 == "msg1"

        msg2 = await transport.receive()
        assert msg2 == "msg2"

    @pytest.mark.asyncio
    async def test_send_messages(self):
        """Test sending messages to response."""
        transport = BatchServerTransport([])

        await transport.send("response1")
        await transport.send("response2")

        assert transport._batch_to_send == ["response1", "response2"]

    def test_get_response_body(self):
        """Test getting response body."""
        transport = BatchServerTransport([])
        transport._batch_to_send = ["resp1", "resp2", "resp3"]

        body = transport.get_response_body()
        assert body == "resp1\nresp2\nresp3"

    def test_get_response_body_empty(self):
        """Test getting empty response body."""
        transport = BatchServerTransport([])
        body = transport.get_response_body()
        assert body == ""

    @pytest.mark.asyncio
    async def test_when_all_received(self):
        """Test waiting for all messages to be received."""
        batch = ["msg1"]
        transport = BatchServerTransport(batch)

        # Receive the message
        await transport.receive()

        # Create a task to wait for all received
        wait_task = asyncio.create_task(transport.when_all_received())

        # Trigger end of batch by trying to receive again
        receive_task = asyncio.create_task(transport.receive())

        # Wait for when_all_received to complete
        await asyncio.wait_for(wait_task, timeout=1.0)

        # Cancel the hanging receive task
        receive_task.cancel()
        try:
            await receive_task
        except asyncio.CancelledError:
            pass


class TestBatchClientTransport:
    """Test BatchClientTransport."""

    @pytest.mark.asyncio
    async def test_send_collects_messages(self):
        """Send collects messages; the batch flushes on the first pull.

        (B3 fix: the old sleep(0)-scheduled flush raced any caller await;
        now the first ``["pull", ...]`` message triggers the flush.)
        """
        received_batch = None

        async def mock_send_batch(batch: list[str]) -> list[str]:
            nonlocal received_batch
            received_batch = batch
            return ["resp1", "resp2"]

        transport = BatchClientTransport(mock_send_batch)

        # Send messages before batch is sent
        await transport.send("msg1")
        await transport.send("msg2")

        # No pull yet: an event-loop yield must NOT flush the batch.
        await asyncio.sleep(0.1)
        assert received_batch is None

        # The first pull flushes.
        await transport.send('["pull",1]')
        await asyncio.sleep(0.1)

        assert received_batch == ["msg1", "msg2", '["pull",1]']

    @pytest.mark.asyncio
    async def test_receive_after_batch_sent(self):
        """Test receiving responses after the batch flushed (pull-triggered)."""

        async def mock_send_batch(batch: list[str]) -> list[str]:
            return ["resp1", "resp2"]

        transport = BatchClientTransport(mock_send_batch)

        # Trigger the flush with a pull, then wait for it.
        await transport.send('["pull",1]')
        await asyncio.sleep(0.1)

        # Receive responses
        resp1 = await transport.receive()
        assert resp1 == "resp1"

        resp2 = await transport.receive()
        assert resp2 == "resp2"

    @pytest.mark.asyncio
    async def test_receive_raises_at_end(self):
        """Test that receive raises BatchEndError when batch ends."""

        async def mock_send_batch(batch: list[str]) -> list[str]:
            return ["resp1"]

        transport = BatchClientTransport(mock_send_batch)

        # Trigger the flush with a pull, then wait for it.
        await transport.send('["pull",1]')
        await asyncio.sleep(0.1)

        # Receive the one response
        await transport.receive()

        # Next receive should raise
        with pytest.raises(BatchEndError):
            await transport.receive()

    @pytest.mark.asyncio
    async def test_abort(self):
        """Test aborting the transport."""

        async def mock_send_batch(batch: list[str]) -> list[str]:
            await asyncio.sleep(10)  # Long delay
            return []

        transport = BatchClientTransport(mock_send_batch)

        # Abort immediately
        transport.abort(RuntimeError("Test abort"))

        # Receive should raise the abort error
        with pytest.raises(RuntimeError, match="Test abort"):
            await transport.receive()


class TestHttpBatchRpcResponse:
    """Test new_http_batch_rpc_response."""

    @pytest.mark.asyncio
    async def test_empty_batch(self):
        """Test handling empty batch."""
        api = SimpleApi()
        response = await new_http_batch_rpc_response("", api)
        assert response == ""

    @pytest.mark.asyncio
    async def test_single_push_no_pull(self):
        """Test single push without pull returns empty (no resolve sent)."""
        import json

        api = SimpleApi()

        # Create a push message for echo("hello") - no pull
        push_msg = json.dumps(["push", ["pipeline", 0, ["echo"], ["hello"]]])

        response = await new_http_batch_rpc_response(push_msg, api)

        # Without a pull, no resolve is sent
        assert response == ""

    @pytest.mark.asyncio
    async def test_roundtrip_echo(self):
        """Test roundtrip echo call."""
        import json

        api = SimpleApi()

        # Simulate client sending echo call
        push_msg = json.dumps(["push", ["pipeline", 0, ["echo"], ["test value"]]])
        pull_msg = json.dumps(["pull", 1])

        request_body = f"{push_msg}\n{pull_msg}"
        response = await new_http_batch_rpc_response(request_body, api)

        # Parse response
        lines = [l for l in response.split("\n") if l]
        assert len(lines) >= 1

        # Check that we got a resolve message
        found_resolve = False
        for line in lines:
            msg = json.loads(line)
            if msg[0] == "resolve":
                found_resolve = True
                # msg[2] should be the result
                assert msg[2] == "test value"
                break

        assert found_resolve, f"No resolve message found in: {lines}"
