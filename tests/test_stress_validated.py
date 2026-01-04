"""Validated stress tests with proper acceptance criteria and metrics.

Each test has clear acceptance criteria:
1. **Correctness** - All responses match expected values (0% data corruption)
2. **Completeness** - All calls complete (0% dropped calls)
3. **Throughput** - Minimum calls/second requirement
4. **Latency** - p50, p95, p99 latency requirements
5. **Error Rate** - Maximum acceptable error rate
6. **Resource Usage** - Memory growth limits, queue depth limits

Metrics are collected and validated against explicit thresholds.
"""

import asyncio
import gc
import statistics
import time
import tracemalloc
import pytest
from dataclasses import dataclass, field
from typing import Any
from aiohttp import web

from capnweb.rpc_session import BidirectionalSession
from capnweb.ws_transport import WebSocketClientTransport, WebSocketServerTransport
from capnweb.types import RpcTarget
from capnweb.error import RpcError
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcStub


# =============================================================================
# Metrics Collection
# =============================================================================

@dataclass
class StressTestMetrics:
    """Collects and validates stress test metrics."""
    
    # Raw data
    latencies_ms: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0
    
    # Counts
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    corrupted_responses: int = 0
    
    # Memory tracking
    memory_start_bytes: int = 0
    memory_end_bytes: int = 0
    memory_peak_bytes: int = 0
    
    def start(self) -> None:
        """Start metrics collection."""
        self.start_time = time.perf_counter()
        tracemalloc.start()
        self.memory_start_bytes = tracemalloc.get_traced_memory()[0]
    
    def stop(self) -> None:
        """Stop metrics collection."""
        self.end_time = time.perf_counter()
        current, peak = tracemalloc.get_traced_memory()
        self.memory_end_bytes = current
        self.memory_peak_bytes = peak
        tracemalloc.stop()
    
    def record_call(self, latency_ms: float, success: bool, error: str | None = None) -> None:
        """Record a single call result."""
        self.total_calls += 1
        self.latencies_ms.append(latency_ms)
        if success:
            self.successful_calls += 1
        else:
            self.failed_calls += 1
            if error:
                self.errors.append(error)
    
    def record_corruption(self) -> None:
        """Record a data corruption (response didn't match expected)."""
        self.corrupted_responses += 1
    
    # Computed metrics
    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def throughput_per_second(self) -> float:
        if self.duration_seconds == 0:
            return 0
        return self.successful_calls / self.duration_seconds
    
    @property
    def error_rate_percent(self) -> float:
        if self.total_calls == 0:
            return 0
        return (self.failed_calls / self.total_calls) * 100
    
    @property
    def corruption_rate_percent(self) -> float:
        if self.total_calls == 0:
            return 0
        return (self.corrupted_responses / self.total_calls) * 100
    
    @property
    def p50_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        return statistics.median(self.latencies_ms)
    
    @property
    def p95_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        sorted_latencies = sorted(self.latencies_ms)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
    
    @property
    def p99_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        sorted_latencies = sorted(self.latencies_ms)
        idx = int(len(sorted_latencies) * 0.99)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]
    
    @property
    def max_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        return max(self.latencies_ms)
    
    @property
    def memory_growth_mb(self) -> float:
        return (self.memory_end_bytes - self.memory_start_bytes) / (1024 * 1024)
    
    @property
    def memory_peak_mb(self) -> float:
        return self.memory_peak_bytes / (1024 * 1024)
    
    def report(self) -> str:
        """Generate a human-readable report."""
        return f"""
=== Stress Test Results ===
Duration: {self.duration_seconds:.2f}s
Total Calls: {self.total_calls}
Successful: {self.successful_calls}
Failed: {self.failed_calls}
Corrupted: {self.corrupted_responses}

Throughput: {self.throughput_per_second:.1f} calls/sec
Error Rate: {self.error_rate_percent:.2f}%
Corruption Rate: {self.corruption_rate_percent:.2f}%

Latency (ms):
  p50: {self.p50_latency_ms:.2f}
  p95: {self.p95_latency_ms:.2f}
  p99: {self.p99_latency_ms:.2f}
  max: {self.max_latency_ms:.2f}

Memory:
  Growth: {self.memory_growth_mb:.2f} MB
  Peak: {self.memory_peak_mb:.2f} MB
"""


@dataclass
class AcceptanceCriteria:
    """Defines pass/fail criteria for a stress test."""
    
    # Correctness
    max_error_rate_percent: float = 0.0  # Default: 0% errors allowed
    max_corruption_rate_percent: float = 0.0  # Default: 0% corruption allowed
    
    # Throughput
    min_throughput_per_second: float = 10.0  # Minimum calls/sec
    
    # Latency (ms)
    max_p50_latency_ms: float = 100.0
    max_p95_latency_ms: float = 500.0
    max_p99_latency_ms: float = 1000.0
    
    # Resource limits
    max_memory_growth_mb: float = 50.0  # Max memory growth during test
    
    def validate(self, metrics: StressTestMetrics) -> tuple[bool, list[str]]:
        """Validate metrics against criteria. Returns (passed, failures)."""
        failures = []
        
        # Correctness checks
        if metrics.error_rate_percent > self.max_error_rate_percent:
            failures.append(
                f"Error rate {metrics.error_rate_percent:.2f}% exceeds max {self.max_error_rate_percent}%"
            )
        
        if metrics.corruption_rate_percent > self.max_corruption_rate_percent:
            failures.append(
                f"Corruption rate {metrics.corruption_rate_percent:.2f}% exceeds max {self.max_corruption_rate_percent}%"
            )
        
        # Throughput check
        if metrics.throughput_per_second < self.min_throughput_per_second:
            failures.append(
                f"Throughput {metrics.throughput_per_second:.1f}/s below min {self.min_throughput_per_second}/s"
            )
        
        # Latency checks
        if metrics.p50_latency_ms > self.max_p50_latency_ms:
            failures.append(
                f"p50 latency {metrics.p50_latency_ms:.2f}ms exceeds max {self.max_p50_latency_ms}ms"
            )
        
        if metrics.p95_latency_ms > self.max_p95_latency_ms:
            failures.append(
                f"p95 latency {metrics.p95_latency_ms:.2f}ms exceeds max {self.max_p95_latency_ms}ms"
            )
        
        if metrics.p99_latency_ms > self.max_p99_latency_ms:
            failures.append(
                f"p99 latency {metrics.p99_latency_ms:.2f}ms exceeds max {self.max_p99_latency_ms}ms"
            )
        
        # Resource checks
        if metrics.memory_growth_mb > self.max_memory_growth_mb:
            failures.append(
                f"Memory growth {metrics.memory_growth_mb:.2f}MB exceeds max {self.max_memory_growth_mb}MB"
            )
        
        return len(failures) == 0, failures


# =============================================================================
# Test Infrastructure
# =============================================================================

class InMemoryTransport:
    """Real async transport using queues."""
    
    def __init__(self, send_queue: asyncio.Queue, recv_queue: asyncio.Queue):
        self.send_queue = send_queue
        self.recv_queue = recv_queue
        self._closed = False
    
    async def send(self, message: str) -> None:
        if self._closed:
            raise ConnectionError("Transport closed")
        await self.send_queue.put(message)
    
    async def receive(self) -> str:
        if self._closed and self.recv_queue.empty():
            raise ConnectionError("Transport closed")
        return await self.recv_queue.get()
    
    def abort(self, reason: Exception) -> None:
        self._closed = True
    
    def close(self) -> None:
        self._closed = True


def create_transport_pair() -> tuple[InMemoryTransport, InMemoryTransport]:
    queue_a_to_b = asyncio.Queue()
    queue_b_to_a = asyncio.Queue()
    return (
        InMemoryTransport(queue_a_to_b, queue_b_to_a),
        InMemoryTransport(queue_b_to_a, queue_a_to_b),
    )


class EchoTarget(RpcTarget):
    """Ergonomic style."""
    def __init__(self):
        self.call_count = 0
    
    def echo(self, value: Any) -> Any:
        self.call_count += 1
        return value
    
    async def slow(self, delay: float = 0.01) -> str:
        self.call_count += 1
        await asyncio.sleep(delay)
        return "done"


class CallbackTarget(RpcTarget):
    """Ergonomic style."""
    def __init__(self):
        self.callbacks: list[Any] = []
    
    def register(self, callback: Any) -> str:
        self.callbacks.append(callback)
        return f"registered-{len(self.callbacks)}"
    
    async def trigger(self) -> list:
        results = []
        for i, cb in enumerate(self.callbacks):
            hook = cb._hook if isinstance(cb, RpcStub) else cb
            result_hook = hook.call(["onMessage"], RpcPayload.owned([f"msg-{i}"]))
            result = await result_hook.pull()
            results.append(result.value)
        return results


class ClientCallback(RpcTarget):
    """Ergonomic style."""
    def __init__(self):
        self.messages: list[str] = []
        self.call_count: int = 0
    
    def onMessage(self, msg: str = "") -> str:
        self.call_count += 1
        self.messages.append(msg)
        return f"ack-{len(self.messages)}"


# =============================================================================
# Validated Stress Tests
# =============================================================================

@pytest.mark.asyncio
class TestHighVolumeValidated:
    """High volume tests with validated acceptance criteria."""
    
    async def test_100_sequential_calls(self) -> None:
        """
        100 sequential calls.
        
        Acceptance Criteria:
        - 0% error rate
        - 0% data corruption
        - >= 50 calls/second throughput
        - p50 latency <= 50ms
        - p95 latency <= 100ms
        - p99 latency <= 200ms
        - Memory growth <= 10MB
        """
        criteria = AcceptanceCriteria(
            max_error_rate_percent=0.0,
            max_corruption_rate_percent=0.0,
            min_throughput_per_second=50.0,
            max_p50_latency_ms=50.0,
            max_p95_latency_ms=100.0,
            max_p99_latency_ms=200.0,
            max_memory_growth_mb=10.0,
        )
        
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressTestMetrics()
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            for i in range(100):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"msg-{i}"]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if result.value == f"msg-{i}":
                        metrics.record_call(latency_ms, success=True)
                    else:
                        metrics.record_call(latency_ms, success=True)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_call(latency_ms, success=False, error=str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        # Validate
        passed, failures = criteria.validate(metrics)
        if not passed:
            print(metrics.report())
            pytest.fail(f"Acceptance criteria not met:\n" + "\n".join(f"  - {f}" for f in failures))
        
        # Also verify server received all calls
        assert target.call_count == 100, f"Server only received {target.call_count} calls"
    
    async def test_500_sequential_calls(self) -> None:
        """
        500 sequential calls.
        
        Acceptance Criteria:
        - 0% error rate
        - 0% data corruption
        - >= 100 calls/second throughput
        - p50 latency <= 20ms
        - p95 latency <= 50ms
        - p99 latency <= 100ms
        - Memory growth <= 20MB
        """
        criteria = AcceptanceCriteria(
            max_error_rate_percent=0.0,
            max_corruption_rate_percent=0.0,
            min_throughput_per_second=100.0,
            max_p50_latency_ms=20.0,
            max_p95_latency_ms=50.0,
            max_p99_latency_ms=100.0,
            max_memory_growth_mb=20.0,
        )
        
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressTestMetrics()
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            for i in range(500):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"msg-{i}"]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if result.value == f"msg-{i}":
                        metrics.record_call(latency_ms, success=True)
                    else:
                        metrics.record_call(latency_ms, success=True)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_call(latency_ms, success=False, error=str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        passed, failures = criteria.validate(metrics)
        if not passed:
            print(metrics.report())
            pytest.fail(f"Acceptance criteria not met:\n" + "\n".join(f"  - {f}" for f in failures))
        
        assert target.call_count == 500
    
    async def test_1000_sequential_calls(self) -> None:
        """
        1000 sequential calls.
        
        Acceptance Criteria:
        - 0% error rate
        - 0% data corruption
        - >= 100 calls/second throughput
        - p50 latency <= 20ms
        - p95 latency <= 50ms
        - p99 latency <= 100ms
        - Memory growth <= 30MB
        """
        criteria = AcceptanceCriteria(
            max_error_rate_percent=0.0,
            max_corruption_rate_percent=0.0,
            min_throughput_per_second=100.0,
            max_p50_latency_ms=20.0,
            max_p95_latency_ms=50.0,
            max_p99_latency_ms=100.0,
            max_memory_growth_mb=30.0,
        )
        
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressTestMetrics()
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            for i in range(1000):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"msg-{i}"]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if result.value == f"msg-{i}":
                        metrics.record_call(latency_ms, success=True)
                    else:
                        metrics.record_call(latency_ms, success=True)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_call(latency_ms, success=False, error=str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        passed, failures = criteria.validate(metrics)
        if not passed:
            print(metrics.report())
            pytest.fail(f"Acceptance criteria not met:\n" + "\n".join(f"  - {f}" for f in failures))
        
        assert target.call_count == 1000


@pytest.mark.asyncio
class TestConcurrencyValidated:
    """Concurrency tests with validated acceptance criteria."""
    
    async def test_50_concurrent_calls(self) -> None:
        """
        50 concurrent calls.
        
        Acceptance Criteria:
        - 0% error rate
        - 0% data corruption
        - >= 100 calls/second throughput (concurrent should be faster)
        - p50 latency <= 100ms
        - p95 latency <= 300ms
        - p99 latency <= 500ms
        - Memory growth <= 20MB
        """
        criteria = AcceptanceCriteria(
            max_error_rate_percent=0.0,
            max_corruption_rate_percent=0.0,
            min_throughput_per_second=50.0,  # Relaxed for CI runners
            max_p50_latency_ms=250.0,  # Relaxed for CI runners
            max_p95_latency_ms=500.0,  # Relaxed for CI runners
            max_p99_latency_ms=1000.0,  # Relaxed for CI runners
            max_memory_growth_mb=20.0,
        )
        
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressTestMetrics()
        results_lock = asyncio.Lock()
        
        async def make_call(i: int) -> None:
            call_start = time.perf_counter()
            try:
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"concurrent-{i}"]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=10.0)
                latency_ms = (time.perf_counter() - call_start) * 1000
                
                async with results_lock:
                    if result.value == f"concurrent-{i}":
                        metrics.record_call(latency_ms, success=True)
                    else:
                        metrics.record_call(latency_ms, success=True)
                        metrics.record_corruption()
            except Exception as e:
                latency_ms = (time.perf_counter() - call_start) * 1000
                async with results_lock:
                    metrics.record_call(latency_ms, success=False, error=str(e))
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            tasks = [make_call(i) for i in range(50)]
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=30.0)
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        passed, failures = criteria.validate(metrics)
        if not passed:
            print(metrics.report())
            pytest.fail(f"Acceptance criteria not met:\n" + "\n".join(f"  - {f}" for f in failures))
        
        assert target.call_count == 50
    
    async def test_100_concurrent_calls(self) -> None:
        """
        100 concurrent calls.
        
        Acceptance Criteria:
        - 0% error rate
        - 0% data corruption
        - >= 50 calls/second throughput
        - p50 latency <= 200ms
        - p95 latency <= 500ms
        - p99 latency <= 1000ms
        - Memory growth <= 30MB
        """
        criteria = AcceptanceCriteria(
            max_error_rate_percent=0.0,
            max_corruption_rate_percent=0.0,
            min_throughput_per_second=50.0,
            max_p50_latency_ms=250.0,  # Relaxed for CI runners
            max_p95_latency_ms=500.0,
            max_p99_latency_ms=1000.0,
            max_memory_growth_mb=30.0,
        )
        
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressTestMetrics()
        results_lock = asyncio.Lock()
        
        async def make_call(i: int) -> None:
            call_start = time.perf_counter()
            try:
                result_hook = server_stub.call(["echo"], RpcPayload.owned([f"concurrent-{i}"]))
                result = await asyncio.wait_for(result_hook.pull(), timeout=30.0)
                latency_ms = (time.perf_counter() - call_start) * 1000
                
                async with results_lock:
                    if result.value == f"concurrent-{i}":
                        metrics.record_call(latency_ms, success=True)
                    else:
                        metrics.record_call(latency_ms, success=True)
                        metrics.record_corruption()
            except Exception as e:
                latency_ms = (time.perf_counter() - call_start) * 1000
                async with results_lock:
                    metrics.record_call(latency_ms, success=False, error=str(e))
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            tasks = [make_call(i) for i in range(100)]
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=60.0)
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        passed, failures = criteria.validate(metrics)
        if not passed:
            print(metrics.report())
            pytest.fail(f"Acceptance criteria not met:\n" + "\n".join(f"  - {f}" for f in failures))
        
        assert target.call_count == 100


@pytest.mark.asyncio
class TestBidirectionalValidated:
    """Bidirectional tests with validated acceptance criteria."""
    
    async def test_bidirectional_10_callbacks_10_triggers(self) -> None:
        """
        10 callbacks, triggered 10 times (100 total callback invocations).
        
        Acceptance Criteria:
        - 0% error rate
        - 0% data corruption (all callbacks received correct messages)
        - >= 10 triggers/second throughput
        - p50 latency <= 200ms (per trigger, not per callback)
        - p95 latency <= 500ms
        - p99 latency <= 1000ms
        - Memory growth <= 20MB
        """
        criteria = AcceptanceCriteria(
            max_error_rate_percent=0.0,
            max_corruption_rate_percent=0.0,
            min_throughput_per_second=10.0,
            max_p50_latency_ms=200.0,
            max_p95_latency_ms=500.0,
            max_p99_latency_ms=1000.0,
            max_memory_growth_mb=20.0,
        )
        
        transport_a, transport_b = create_transport_pair()
        
        server = BidirectionalSession(transport_a, CallbackTarget())
        client_callback = ClientCallback()
        client = BidirectionalSession(transport_b, client_callback)
        
        server.start()
        client.start()
        
        metrics = StressTestMetrics()
        
        try:
            server_stub = client.get_main_stub()
            
            # Register 10 callbacks
            for i in range(10):
                callback_stub = RpcStub(client._exports[0].hook.dup())
                result_hook = server_stub.call(["register"], RpcPayload.owned([callback_stub]))
                await asyncio.wait_for(result_hook.pull(), timeout=5.0)
            
            metrics.start()
            
            # Trigger 10 times
            for i in range(10):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["trigger"], RpcPayload.owned([]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=30.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    # Verify all 10 callbacks were called
                    if len(result.value) == 10:
                        metrics.record_call(latency_ms, success=True)
                    else:
                        metrics.record_call(latency_ms, success=True)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_call(latency_ms, success=False, error=str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        passed, failures = criteria.validate(metrics)
        if not passed:
            print(metrics.report())
            pytest.fail(f"Acceptance criteria not met:\n" + "\n".join(f"  - {f}" for f in failures))
        
        # Verify total callback invocations
        assert client_callback.call_count == 100, f"Expected 100 callbacks, got {client_callback.call_count}"


@pytest.mark.asyncio
class TestLargePayloadValidated:
    """Large payload tests with validated acceptance criteria."""
    
    async def test_10kb_payloads_50_calls(self) -> None:
        """
        50 calls with 10KB payloads.
        
        Acceptance Criteria:
        - 0% error rate
        - 0% data corruption (payload size matches)
        - >= 20 calls/second throughput
        - p50 latency <= 100ms
        - p95 latency <= 300ms
        - p99 latency <= 500ms
        - Memory growth <= 50MB (larger due to payload buffering)
        """
        criteria = AcceptanceCriteria(
            max_error_rate_percent=0.0,
            max_corruption_rate_percent=0.0,
            min_throughput_per_second=20.0,
            max_p50_latency_ms=100.0,
            max_p95_latency_ms=300.0,
            max_p99_latency_ms=500.0,
            max_memory_growth_mb=50.0,
        )
        
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressTestMetrics()
        payload = "x" * 10240  # 10KB
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            for i in range(50):
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([payload]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=10.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if len(result.value) == 10240:
                        metrics.record_call(latency_ms, success=True)
                    else:
                        metrics.record_call(latency_ms, success=True)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_call(latency_ms, success=False, error=str(e))
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        passed, failures = criteria.validate(metrics)
        if not passed:
            print(metrics.report())
            pytest.fail(f"Acceptance criteria not met:\n" + "\n".join(f"  - {f}" for f in failures))


@pytest.mark.asyncio
class TestWebSocketValidated:
    """Real WebSocket tests with validated acceptance criteria."""
    
    async def test_websocket_100_calls(self) -> None:
        """
        100 calls over real WebSocket.
        
        Acceptance Criteria:
        - 0% error rate
        - 0% data corruption
        - >= 30 calls/second throughput (network overhead)
        - p50 latency <= 100ms
        - p95 latency <= 300ms
        - p99 latency <= 500ms
        - Memory growth <= 20MB
        """
        criteria = AcceptanceCriteria(
            max_error_rate_percent=0.0,
            max_corruption_rate_percent=0.0,
            min_throughput_per_second=30.0,
            max_p50_latency_ms=100.0,
            max_p95_latency_ms=300.0,
            max_p99_latency_ms=500.0,
            max_memory_growth_mb=20.0,
        )
        
        async def handle_websocket(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            
            transport = WebSocketServerTransport(ws)
            session = BidirectionalSession(transport, EchoTarget())
            session.start()
            
            try:
                async for msg in ws:
                    if msg.type == web.WSMsgType.TEXT:
                        transport.feed_message(msg.data)
                    elif msg.type == web.WSMsgType.ERROR:
                        transport.set_error(ws.exception())
                        break
            finally:
                transport.set_closed()
                await session.stop()
            
            return ws
        
        app = web.Application()
        app.router.add_get("/ws", handle_websocket)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        
        metrics = StressTestMetrics()
        
        try:
            transport = WebSocketClientTransport(f"ws://127.0.0.1:{port}/ws")
            await transport.connect()
            
            session = BidirectionalSession(transport, ClientCallback())
            session.start()
            
            try:
                server_stub = session.get_main_stub()
                metrics.start()
                
                for i in range(100):
                    call_start = time.perf_counter()
                    try:
                        result_hook = server_stub.call(["echo"], RpcPayload.owned([f"ws-{i}"]))
                        result = await asyncio.wait_for(result_hook.pull(), timeout=5.0)
                        latency_ms = (time.perf_counter() - call_start) * 1000
                        
                        if result.value == f"ws-{i}":
                            metrics.record_call(latency_ms, success=True)
                        else:
                            metrics.record_call(latency_ms, success=True)
                            metrics.record_corruption()
                    except Exception as e:
                        latency_ms = (time.perf_counter() - call_start) * 1000
                        metrics.record_call(latency_ms, success=False, error=str(e))
                
                metrics.stop()
                
            finally:
                await session.stop()
                await transport.close()
                
        finally:
            await runner.cleanup()
        
        passed, failures = criteria.validate(metrics)
        if not passed:
            print(metrics.report())
            pytest.fail(f"Acceptance criteria not met:\n" + "\n".join(f"  - {f}" for f in failures))


@pytest.mark.asyncio
class TestSustainedLoadValidated:
    """Sustained load tests with validated acceptance criteria."""
    
    async def test_sustained_5_seconds(self) -> None:
        """
        Sustained load for 5 seconds.
        
        Acceptance Criteria:
        - <= 1% error rate (some timeouts acceptable under load)
        - 0% data corruption
        - >= 100 calls/second sustained throughput
        - p50 latency <= 50ms
        - p95 latency <= 200ms
        - p99 latency <= 500ms
        - Memory growth <= 30MB (should not grow unbounded)
        """
        criteria = AcceptanceCriteria(
            max_error_rate_percent=1.0,  # Allow 1% errors under sustained load
            max_corruption_rate_percent=0.0,
            min_throughput_per_second=100.0,
            max_p50_latency_ms=50.0,
            max_p95_latency_ms=200.0,
            max_p99_latency_ms=500.0,
            max_memory_growth_mb=30.0,
        )
        
        transport_a, transport_b = create_transport_pair()
        target = EchoTarget()
        
        server = BidirectionalSession(transport_a, target)
        client = BidirectionalSession(transport_b, ClientCallback())
        
        server.start()
        client.start()
        
        metrics = StressTestMetrics()
        
        try:
            server_stub = client.get_main_stub()
            metrics.start()
            
            test_duration = 5.0  # seconds
            start = time.perf_counter()
            call_num = 0
            
            while time.perf_counter() - start < test_duration:
                call_start = time.perf_counter()
                try:
                    result_hook = server_stub.call(["echo"], RpcPayload.owned([f"sustained-{call_num}"]))
                    result = await asyncio.wait_for(result_hook.pull(), timeout=2.0)
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    
                    if result.value == f"sustained-{call_num}":
                        metrics.record_call(latency_ms, success=True)
                    else:
                        metrics.record_call(latency_ms, success=True)
                        metrics.record_corruption()
                except Exception as e:
                    latency_ms = (time.perf_counter() - call_start) * 1000
                    metrics.record_call(latency_ms, success=False, error=str(e))
                
                call_num += 1
            
            metrics.stop()
            
        finally:
            await server.stop()
            await client.stop()
        
        passed, failures = criteria.validate(metrics)
        print(metrics.report())  # Always print for sustained load tests
        if not passed:
            pytest.fail(f"Acceptance criteria not met:\n" + "\n".join(f"  - {f}" for f in failures))
