"""Tests for StubHook implementations - the core of capability management."""

import asyncio

import pytest

from capnweb.error import RpcError
from capnweb.hooks import (
    ErrorStubHook,
    PayloadStubHook,
    PromiseStubHook,
    TargetStubHook,
)
from capnweb.payload import RpcPayload
from capnweb.types import RpcTarget


class SimpleTarget(RpcTarget):
    """Simple RPC target for testing."""

    def __init__(self, value: str = "test"):
        self.value = value
        self.disposed = False

    async def call(self, method: str, args: list) -> str:
        match method:
            case "getValue":
                return self.value
            case "echo":
                return args[0] if args else ""
            case "fail":
                msg = "Intentional failure"
                raise ValueError(msg)
            case _:
                msg = f"Method {method} not found"
                raise RpcError.not_found(msg)

    async def get_property(self, property: str) -> str:
        match property:
            case "value":
                return self.value
            case "missing":
                msg = f"Property {property} not found"
                raise RpcError.not_found(msg)
            case _:
                msg = f"Property {property} not found"
                raise RpcError.not_found(msg)

    def dispose(self):
        """Mark as disposed."""
        self.disposed = True


class TestErrorStubHook:
    """Test ErrorStubHook behavior."""

    @pytest.mark.asyncio
    async def test_error_hook_call_returns_self(self):
        """Test that calling an error hook returns itself."""
        error = RpcError.not_found("Test error")
        hook = ErrorStubHook(error)
        payload = RpcPayload.owned([])

        result = hook.call(["method"], payload)
        assert result is hook

    def test_error_hook_get_returns_self(self):
        """Test that getting property on error hook returns itself."""
        error = RpcError.not_found("Test error")
        hook = ErrorStubHook(error)

        result = hook.get(["property"])
        assert result is hook

    @pytest.mark.asyncio
    async def test_error_hook_pull_raises(self):
        """Test that pulling an error hook raises the error."""
        error = RpcError.not_found("Test error")
        hook = ErrorStubHook(error)

        with pytest.raises(RpcError, match="Test error"):
            await hook.pull()

    def test_error_hook_dispose(self):
        """Test that disposing error hook does nothing."""
        error = RpcError.not_found("Test error")
        hook = ErrorStubHook(error)

        # Should not raise
        hook.dispose()

    def test_error_hook_dup(self):
        """Test that duplicating error hook returns itself."""
        error = RpcError.not_found("Test error")
        hook = ErrorStubHook(error)

        dup = hook.dup()
        assert dup is hook


class TestPayloadStubHookCall:
    """Test PayloadStubHook call behavior."""

    @pytest.mark.asyncio
    async def test_call_async_callable(self):
        """Test calling an async function."""

        async def async_func(x, y):
            await asyncio.sleep(0.001)
            return x + y

        payload = RpcPayload.owned(async_func)
        hook = PayloadStubHook(payload)
        args = RpcPayload.owned([5, 3])

        result_hook = hook.call([], args)

        # Should return a PromiseStubHook
        assert isinstance(result_hook, PromiseStubHook)
        result_payload = await result_hook.pull()
        assert result_payload.value == 8

    @pytest.mark.asyncio
    async def test_call_async_callable_with_error(self):
        """Test calling an async function that raises an error."""

        async def failing_async():
            await asyncio.sleep(0.001)
            msg = "Async error"
            raise ValueError(msg)

        payload = RpcPayload.owned(failing_async)
        hook = PayloadStubHook(payload)
        args = RpcPayload.owned([])

        result_hook = hook.call([], args)

        # Should return a PromiseStubHook that resolves to ErrorStubHook
        assert isinstance(result_hook, PromiseStubHook)
        # The promise should resolve to an error
        resolved = await result_hook.future
        assert isinstance(resolved, ErrorStubHook)

    @pytest.mark.asyncio
    async def test_call_sync_callable_with_error(self):
        """Test calling a sync function that raises an error."""

        def failing_func():
            msg = "Sync error"
            raise ValueError(msg)

        payload = RpcPayload.owned(failing_func)
        hook = PayloadStubHook(payload)
        args = RpcPayload.owned([])

        result_hook = hook.call([], args)

        # Should return an ErrorStubHook
        assert isinstance(result_hook, ErrorStubHook)

    @pytest.mark.asyncio
    async def test_call_non_callable(self):
        """Test calling a non-callable value returns error."""
        payload = RpcPayload.owned({"not": "callable"})
        hook = PayloadStubHook(payload)
        args = RpcPayload.owned([])

        result_hook = hook.call([], args)

        assert isinstance(result_hook, ErrorStubHook)
        assert "not callable" in result_hook.error.message

    def test_get_missing_property(self):
        """Test getting a missing property returns error."""
        payload = RpcPayload.owned({"exists": "yes"})
        hook = PayloadStubHook(payload)

        result_hook = hook.get(["missing"])

        assert isinstance(result_hook, ErrorStubHook)
        assert "not found" in result_hook.error.message


class TestTargetStubHookNavigation:
    """Test TargetStubHook property navigation."""

    @pytest.mark.asyncio
    async def test_navigate_to_target_success(self):
        """Test successful navigation through properties."""

        class NestedTarget(RpcTarget):
            async def call(self, method: str, args: list):
                return "called"

            async def get_property(self, property: str):
                if property == "nested":
                    return SimpleTarget("nested_value")
                msg = f"Property {property} not found"
                raise RpcError.not_found(msg)

        target = NestedTarget()
        hook = TargetStubHook(target)
        args = RpcPayload.owned([])

        # Navigate through nested properties
        result_hook = hook.call(["nested", "getValue"], args)

        # Now returns PromiseStubHook since call is sync but work is async
        assert isinstance(result_hook, PromiseStubHook)
        result = await result_hook.pull()
        assert result.value == "nested_value"

    @pytest.mark.asyncio
    async def test_navigate_to_target_failure(self):
        """Test navigation failure returns error."""
        target = SimpleTarget()
        hook = TargetStubHook(target)
        args = RpcPayload.owned([])

        # Try to navigate through non-existent property
        result_hook = hook.call(["missing", "method"], args)

        # Now returns PromiseStubHook, error is in the resolved hook
        assert isinstance(result_hook, PromiseStubHook)
        resolved = await result_hook.future
        assert isinstance(resolved, ErrorStubHook)
        assert "not found" in resolved.error.message.lower()

    @pytest.mark.asyncio
    async def test_call_without_path(self):
        """Test calling target without method name goes through RpcTarget.call with empty method."""
        target = SimpleTarget()
        hook = TargetStubHook(target)
        args = RpcPayload.owned([])

        result_hook = hook.call([], args)

        # Now returns PromiseStubHook since RpcTarget.call is async
        assert isinstance(result_hook, PromiseStubHook)
        resolved = await result_hook.future
        # SimpleTarget.call raises RpcError.not_found for unknown methods (including empty)
        assert isinstance(resolved, ErrorStubHook)
        assert isinstance(resolved.error, RpcError)

    @pytest.mark.asyncio
    async def test_call_with_rpc_error(self):
        """Test that RpcError is returned as ErrorStubHook."""
        target = SimpleTarget()
        hook = TargetStubHook(target)
        args = RpcPayload.owned([])

        # Call a method that doesn't exist
        result_hook = hook.call(["nonexistent"], args)

        # Now returns PromiseStubHook, error is in the resolved hook
        assert isinstance(result_hook, PromiseStubHook)
        resolved = await result_hook.future
        assert isinstance(resolved, ErrorStubHook)
        assert isinstance(resolved.error, RpcError)

    @pytest.mark.asyncio
    async def test_call_with_non_rpc_error(self):
        """Test that non-RpcError exceptions become internal errors."""
        target = SimpleTarget()
        hook = TargetStubHook(target)
        args = RpcPayload.owned([])

        # Call a method that raises ValueError
        result_hook = hook.call(["fail"], args)

        # Now returns PromiseStubHook, error is in the resolved hook
        assert isinstance(result_hook, PromiseStubHook)
        resolved = await result_hook.future
        assert isinstance(resolved, ErrorStubHook)
        assert "Target call failed" in resolved.error.message


class TestTargetStubHookProperty:
    """Test TargetStubHook property access."""

    def test_get_property_error(self):
        """Test getting property that raises error."""
        target = SimpleTarget()
        hook = TargetStubHook(target)

        # Get property path (not simple case)
        result_hook = hook.get(["level1", "level2"])

        assert isinstance(result_hook, ErrorStubHook)
        assert "not yet supported" in result_hook.error.message

    @pytest.mark.asyncio
    async def test_pull_target_raises(self):
        """Test that pulling a target hook raises error."""
        target = SimpleTarget()
        hook = TargetStubHook(target)

        with pytest.raises(RpcError, match="Cannot pull a target"):
            await hook.pull()


class TestTargetStubHookDisposal:
    """Test TargetStubHook disposal and reference counting."""

    def test_dispose_decrements_refcount(self):
        """Test that dispose decrements reference count."""
        target = SimpleTarget()
        hook = TargetStubHook(target)

        assert hook.ref_count == 1
        hook.dispose()
        assert hook.ref_count == 0

    def test_dispose_calls_target_dispose(self):
        """Test that dispose calls target's dispose when refcount reaches 0."""
        target = SimpleTarget()
        hook = TargetStubHook(target)

        assert not target.disposed
        hook.dispose()
        assert target.disposed

    def test_dispose_with_multiple_refs(self):
        """Test that dispose with multiple refs doesn't call target dispose."""
        target = SimpleTarget()
        hook = TargetStubHook(target)

        # Duplicate to increase refcount
        hook2 = hook.dup()
        assert hook.ref_count == 2

        # Dispose once
        hook.dispose()
        assert hook.ref_count == 1
        assert not target.disposed  # Should not be disposed yet

        # Dispose again
        hook2.dispose()
        assert hook.ref_count == 0
        assert target.disposed  # Now it should be disposed

    def test_dispose_without_dispose_method(self):
        """Test disposing target without dispose method doesn't raise."""

        class NoDisposeTarget(RpcTarget):
            async def call(self, method: str, args: list):
                return "ok"

            async def get_property(self, property: str):
                return "prop"

        target = NoDisposeTarget()
        hook = TargetStubHook(target)

        # Should not raise even though target has no dispose method
        hook.dispose()
        assert hook.ref_count == 0

    def test_dup_increments_refcount(self):
        """Test that dup increments reference count."""
        target = SimpleTarget()
        hook = TargetStubHook(target)

        assert hook.ref_count == 1
        dup = hook.dup()
        assert hook.ref_count == 2
        assert dup is hook


class TestPromiseStubHook:
    """Test PromiseStubHook behavior."""

    @pytest.mark.asyncio
    async def test_promise_hook_call(self):
        """Test calling through a promise hook."""
        future: asyncio.Future = asyncio.Future()
        hook = PromiseStubHook(future)
        args = RpcPayload.owned([])

        # Call returns a new promise hook
        result_hook = hook.call(["method"], args)
        assert isinstance(result_hook, PromiseStubHook)

        # The call is chained - resolve the original future
        inner_payload = RpcPayload.owned(lambda: "result")
        inner_hook = PayloadStubHook(inner_payload)
        future.set_result(inner_hook)

    def test_promise_hook_get(self):
        """Test getting property through promise hook."""
        future: asyncio.Future = asyncio.Future()
        hook = PromiseStubHook(future)

        # Get returns a new promise hook
        result_hook = hook.get(["property"])
        assert isinstance(result_hook, PromiseStubHook)

    @pytest.mark.asyncio
    async def test_promise_hook_pull_resolved(self):
        """Test pulling from a resolved promise hook."""
        future: asyncio.Future = asyncio.Future()
        hook = PromiseStubHook(future)

        # Resolve the future
        payload = RpcPayload.owned("resolved")
        resolved_hook = PayloadStubHook(payload)
        future.set_result(resolved_hook)

        # Pull should return the payload
        result = await hook.pull()
        assert result.value == "resolved"

    def test_promise_hook_dispose_not_done(self):
        """Test disposing a promise hook that's not resolved cancels it."""
        future: asyncio.Future = asyncio.Future()
        hook = PromiseStubHook(future)

        hook.dispose()
        assert future.cancelled()

    def test_promise_hook_dispose_done(self):
        """Test disposing a resolved promise hook disposes the result."""
        future: asyncio.Future = asyncio.Future()
        hook = PromiseStubHook(future)

        # Resolve it
        payload = RpcPayload.owned("test")
        resolved_hook = PayloadStubHook(payload)
        future.set_result(resolved_hook)

        # Dispose should dispose the resolved hook
        hook.dispose()
        # Can't easily verify payload disposal, but shouldn't raise

    def test_promise_hook_dispose_cancelled(self):
        """Test disposing an already cancelled future doesn't raise."""
        future: asyncio.Future = asyncio.Future()
        future.cancel()
        hook = PromiseStubHook(future)

        # Should not raise
        hook.dispose()

    def test_promise_hook_dup(self):
        """Test duplicating promise hook shares the same future."""
        future: asyncio.Future = asyncio.Future()
        hook = PromiseStubHook(future)

        dup = hook.dup()
        assert dup.future is future
