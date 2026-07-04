"""Pytest configuration for all tests."""

import asyncio

import pytest

# Re-export aiohttp_server fixture for production feature tests
pytest_plugins = ["aiohttp.pytest_plugin"]


@pytest.fixture(autouse=True)
def _ensure_current_event_loop():
    """Guarantee an ambient event loop for every test (order-independence).

    Some sync tests use the deprecated ``asyncio.Future()`` /
    ``asyncio.get_event_loop()`` pattern, which needs a *current* loop in the
    policy. Other sync tests (e.g. the security fuzzer) run ``asyncio.run()``,
    which resets the policy's current loop to ``None`` on exit. Without this
    fixture, a sync test that happens to run after an ``asyncio.run()`` test
    raises "There is no current event loop" — a collection-order artifact,
    not a real defect. Ensuring a usable loop exists (only when none is set)
    makes the suite deterministic. It does not interfere with pytest-asyncio,
    which installs its own running loop for async tests.
    """
    policy = asyncio.get_event_loop_policy()
    loop = getattr(getattr(policy, "_local", None), "_loop", None)
    if loop is None or loop.is_closed():
        policy.set_event_loop(policy.new_event_loop())
    yield


class MockTransport:
    """In-memory transport for testing."""

    def __init__(self) -> None:
        self.peer: "MockTransport | None" = None
        self.inbox: asyncio.Queue[str] = asyncio.Queue()
        self.closed = False
        self.message_count = 0

    async def send(self, message: str) -> None:
        """Send message to peer."""
        if self.peer and not self.peer.closed:
            self.message_count += 1
            await self.peer.inbox.put(message)

    async def receive(self) -> str:
        """Receive message from inbox."""
        return await self.inbox.get()

    async def close(self) -> None:
        """Close the transport."""
        self.closed = True


def create_transport_pair() -> tuple[MockTransport, MockTransport]:
    """Create a pair of connected transports."""
    a = MockTransport()
    b = MockTransport()
    a.peer = b
    b.peer = a
    return a, b
