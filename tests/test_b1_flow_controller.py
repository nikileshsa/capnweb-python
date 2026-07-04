"""B1: FlowController unit tests — verbatim port of the TS suite.

Ports upstream __tests__/flow-control.test.ts (capnweb @ 8a9f19d) against the
Python FlowController (capnweb/streams.py), which must be numerically
identical to streams.ts:166-307 since both ends of a connection run the
algorithm independently.

Also covers the estimate_encoded_size fallback (rpc.ts:95-163 port).
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

from capnweb.error import RpcError
from capnweb.streams import (
    DECAY_FACTOR,
    INITIAL_WINDOW,
    MAX_WINDOW,
    MIN_WINDOW,
    STARTUP_EXIT_ROUNDS,
    STARTUP_GROWTH_FACTOR,
    STEADY_GROWTH_FACTOR,
    FlowController,
    SendToken,
    estimate_encoded_size,
)

# To emulate random-ish chunk sizes while keeping the test reproducible, we
# cycle through this list of sizes (flow-control.test.ts:10).
CHUNK_SIZES = [32 * 1024, 4 * 1024, 16000, 12345, 16, 9999, 4321, 8]


class StreamSimulator:
    """Simulates a stream with a fake clock and set RTT and bandwidth.

    Direct port of the TS StreamSimulator (flow-control.test.ts:14-98).
    """

    def __init__(self) -> None:
        # Default RTT of 100ms.
        self.rtt = 100
        # Default bandwidth of 10kB/ms = 10MB/s = 1MB/RTT. This is larger than
        # the initial window size of 256k, so the window should grow if
        # saturated.
        self.bandwidth = 10 * 1024
        # Current simulated time.
        self.t = 0.0
        # Are we currently blocked, according to the flow controller?
        self.blocked = False
        # The outgoing link is sending bytes until this time.
        self.link_occupied_until = 0.0
        # The flow controller itself, on the fake clock.
        self.fc = FlowController(now=lambda: self.t)
        # In-flight writes, in send order: (token, scheduled ack time).
        self.in_flight: list[tuple[SendToken, float]] = []

    @property
    def bdp(self) -> float:
        return self.rtt * self.bandwidth

    def send(self, size: int) -> None:
        self.link_occupied_until = (
            max(self.link_occupied_until, self.t) + size / self.bandwidth
        )
        token, should_block = self.fc.on_send(size)
        # ackTime = time when the chunk finishes writing out, plus 1 rtt.
        self.in_flight.append((token, self.link_occupied_until + self.rtt))
        self.blocked = should_block

    def fill_window(self, chunk_size: int) -> int:
        count = 0
        while not self.blocked:
            count += 1
            self.send(chunk_size)
        return count

    def wait_for_next_ack(self) -> None:
        if self.in_flight:
            token, ack_time = self.in_flight.pop(0)
            self.t = ack_time
            if self.fc.on_ack(token):
                self.blocked = False

    def saturate_for(self, duration: float) -> None:
        end_time = self.t + duration
        i = 0
        while self.t < end_time:
            if self.blocked:
                self.wait_for_next_ack()
            else:
                self.send(CHUNK_SIZES[i % len(CHUNK_SIZES)])
                i += 1
        while self.in_flight:
            self.wait_for_next_ack()


class TestFlowControllerConstants:
    def test_constants_match_ts(self):
        """streams.ts:139-151 — the two peers must agree numerically."""
        assert INITIAL_WINDOW == 256 * 1024
        assert MAX_WINDOW == 1024 * 1024 * 1024
        assert MIN_WINDOW == 64 * 1024
        assert STARTUP_GROWTH_FACTOR == 2
        assert STEADY_GROWTH_FACTOR == 1.25
        assert DECAY_FACTOR == 0.90
        assert STARTUP_EXIT_ROUNDS == 3


class TestFlowController:
    """Verbatim port of describe("FlowController") flow-control.test.ts:100-254."""

    def test_blocks_when_window_is_full(self):
        sim = StreamSimulator()
        initial_window = sim.fc.window  # 256KB

        count = sim.fill_window(64 * 1024)
        assert count == 4  # 4 * 64KB = 256KB = window
        assert sim.fc.bytes_in_flight == initial_window

    def test_unblocks_after_ack_frees_space(self):
        sim = StreamSimulator()

        sim.t = 0
        sim.send(64 * 1024)
        sim.t = 1
        sim.send(64 * 1024)
        sim.t = 2
        sim.send(64 * 1024)
        sim.t = 3
        assert sim.blocked is False
        sim.send(64 * 1024)
        assert sim.blocked is True

        # Ack only the first one.
        sim.wait_for_next_ack()
        assert sim.fc.bytes_in_flight == 192 * 1024  # 3 chunks still in flight

    def test_window_grows_during_startup(self):
        sim = StreamSimulator()

        initial_window = sim.fc.window  # 256KB
        sim.saturate_for(sim.rtt * 5)

        # During startup, window should quickly grow past the 1MB BDP.
        assert sim.fc.window > initial_window
        assert sim.fc.window > sim.bdp

    def test_exits_startup_after_window_growth_plateaus(self):
        sim = StreamSimulator()

        assert sim.fc.in_startup_phase is True
        sim.saturate_for(sim.rtt * 50)
        assert sim.fc.in_startup_phase is False

    def test_steady_state_window_converges_near_bdp(self):
        sim = StreamSimulator()

        sim.saturate_for(sim.rtt * 50)
        assert sim.fc.in_startup_phase is False

        # Window should have settled around 25% above BDP.
        assert sim.fc.window > sim.bdp * 1.2
        assert sim.fc.window < sim.bdp * 1.3

        sim.saturate_for(sim.rtt * 20)

        # Window should be pretty stable.
        assert sim.fc.window > sim.bdp * 1.2
        assert sim.fc.window < sim.bdp * 1.3

    def test_window_does_not_shrink_when_app_limited(self):
        sim = StreamSimulator()

        sim.saturate_for(sim.rtt * 50)
        window_after_startup = sim.fc.window

        # Repeatedly send small chunks that don't fill the window.
        for _ in range(50):
            sim.send(1024)
            sim.wait_for_next_ack()

        assert sim.fc.window == window_after_startup

    def test_window_shrinks_when_pipe_bandwidth_decreases(self):
        sim = StreamSimulator()

        sim.saturate_for(sim.rtt * 50)
        assert sim.fc.in_startup_phase is False
        assert sim.fc.window > sim.bdp

        # Simulate pipe bandwidth drop to 1/4 of original.
        sim.bandwidth /= 4
        sim.saturate_for(sim.rtt * 200)

        # The window should have decayed toward the new, lower BDP.
        assert sim.fc.window < sim.bdp * 2

    def test_on_error_restores_bytes_in_flight_without_changing_window(self):
        sim = StreamSimulator()

        token, _should_block = sim.fc.on_send(64 * 1024)
        assert sim.fc.bytes_in_flight == 64 * 1024

        window_before = sim.fc.window
        sim.fc.on_error(token)
        assert sim.fc.bytes_in_flight == 0
        assert sim.fc.window == window_before

    def test_growth_collar_limits_window_increase_per_rtt(self):
        sim = StreamSimulator()
        chunk_size = 64 * 1024

        # First round to get past the first-ack bootstrap.
        sim.fill_window(chunk_size)
        sim.wait_for_next_ack()

        window_before = sim.fc.window

        sim.fill_window(chunk_size)
        while sim.in_flight:
            sim.wait_for_next_ack()

        # In startup, window can at most double per RTT.
        assert sim.fc.window <= window_before * 2 + 1

    def test_minimum_window_is_enforced(self):
        sim = StreamSimulator()

        # Excessively low RTT and bandwidth, to create a low BDP.
        sim.rtt = 1
        sim.bandwidth = 1

        sim.saturate_for(10_000_000)

        assert sim.fc.window == 64 * 1024


class TestFlowControllerMisc:
    def test_default_clock_is_monotonic(self):
        fc = FlowController()
        token, should_block = fc.on_send(1)
        assert should_block is False
        assert token.sent_time > 0
        assert fc.on_ack(token) is True
        assert fc.bytes_in_flight == 0

    def test_should_block_exactly_at_window(self):
        fc = FlowController(now=lambda: 1.0)
        token, should_block = fc.on_send(INITIAL_WINDOW)
        assert should_block is True
        assert token.window_full_at_send is True
        fc.on_error(token)
        assert fc.bytes_in_flight == 0


class TestEstimateEncodedSize:
    """Port of the rpc.ts:95-163 estimator (fallback size source)."""

    def test_primitives(self):
        assert estimate_encoded_size("abc") == 2 + 3 * 3
        assert estimate_encoded_size(42) == 16
        assert estimate_encoded_size(1.5) == 16
        assert estimate_encoded_size(True) == 8
        assert estimate_encoded_size(None) == 8
        assert estimate_encoded_size(10**30) == 16  # bigint
        assert estimate_encoded_size(datetime.now(timezone.utc)) == 16

    def test_binary(self):
        assert estimate_encoded_size(b"\x00" * 100) == 16 + 100
        assert estimate_encoded_size(bytearray(10)) == 16 + 10
        assert estimate_encoded_size(memoryview(b"12345")) == 16 + 5

    def test_array_and_object(self):
        # [] = 16; each entry adds 8 + item estimate.
        assert estimate_encoded_size([]) == 16
        assert estimate_encoded_size([1, 2]) == 16 + 2 * (8 + 16)
        # {} = 16; each key adds 8 + string estimate + value estimate.
        assert estimate_encoded_size({}) == 16
        assert estimate_encoded_size({"a": 1}) == 16 + 8 + (2 + 3) + 16

    def test_error(self):
        err = RpcError("Error", "boom")
        est = estimate_encoded_size(err)
        assert est >= 16 + (2 + 3 * len("Error")) + (2 + 3 * len("boom"))

    def test_cycle_safe(self):
        a: list = []
        a.append(a)
        est = estimate_encoded_size(a)
        assert est > 0 and math.isfinite(est)
        d: dict = {}
        d["self"] = d
        assert estimate_encoded_size(d) > 0

    def test_shared_substructure_counted_once(self):
        shared = [1, 2, 3]
        est_shared = estimate_encoded_size([shared, shared])
        est_distinct = estimate_encoded_size([[1, 2, 3], [1, 2, 3]])
        assert est_shared < est_distinct

    def test_blob(self):
        from capnweb.types import Blob

        blob = Blob("text/plain", b"x" * 50)
        assert estimate_encoded_size(blob) == 16 + 50
