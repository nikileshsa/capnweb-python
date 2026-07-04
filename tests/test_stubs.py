"""Tests for RpcStub and RpcPromise user-facing classes."""

import asyncio
import operator

import pytest

from capnweb.error import RpcError
from capnweb.hooks import ErrorStubHook, PayloadStubHook, PromiseStubHook
from capnweb.payload import RpcPayload
from capnweb.stubs import RpcPromise, RpcStub


class TestRpcStubBasics:
    """Test basic RpcStub functionality."""

    def test_stub_initialization(self):
        """Test that stub initializes with a hook."""
        payload = RpcPayload.owned({"value": 42})
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        assert stub._hook is hook

    def test_stub_repr(self):
        """Test stub string representation."""
        payload = RpcPayload.owned({"value": 42})
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        repr_str = repr(stub)
        assert "RpcStub" in repr_str
        assert "PayloadStubHook" in repr_str

    def test_stub_dispose(self):
        """Test disposing a stub."""
        payload = RpcPayload.owned({"value": 42})
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        # Should not raise
        stub.dispose()

    @pytest.mark.asyncio
    async def test_stub_async_context_manager(self):
        """Test using stub as async context manager."""
        payload = RpcPayload.owned({"value": 42})
        hook = PayloadStubHook(payload)

        async with RpcStub(hook) as stub:
            assert stub is not None
            # Stub should work inside context
            promise = stub.value
            result = await promise
            assert result == 42


class TestRpcStubPropertyAccess:
    """Test property access on RpcStub."""

    @pytest.mark.asyncio
    async def test_stub_get_property(self):
        """Test accessing a property returns a promise."""
        payload = RpcPayload.owned({"user": {"id": 123, "name": "Alice"}})
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        # Access property - returns a promise
        promise = stub.user
        assert isinstance(promise, RpcPromise)

        # Await the promise
        result = await promise
        assert result == {"id": 123, "name": "Alice"}

    @pytest.mark.asyncio
    async def test_stub_chained_property_access(self):
        """Test chaining property access."""
        payload = RpcPayload.owned({"user": {"profile": {"bio": "Test bio"}}})
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        # Chain property access
        promise = stub.user.profile.bio
        result = await promise

        assert result == "Test bio"

    def test_stub_private_attribute_raises(self):
        """Test accessing private attributes raises AttributeError."""
        payload = RpcPayload.owned({"value": 42})
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        with pytest.raises(AttributeError, match="has no attribute '_private'"):
            _ = stub._private


class TestRpcStubCalling:
    """Test calling RpcStub."""

    @pytest.mark.asyncio
    async def test_stub_call_with_args(self):
        """Test calling stub with arguments."""

        add = operator.add

        payload = RpcPayload.owned(add)
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        # Call the stub
        promise = stub(5, 3)
        assert isinstance(promise, RpcPromise)

        result = await promise
        assert result == 8

    @pytest.mark.asyncio
    async def test_stub_call_no_args(self):
        """Test calling stub with no arguments."""

        def get_value():
            return 42

        payload = RpcPayload.owned(get_value)
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        promise = stub()
        result = await promise
        assert result == 42

    def test_stub_call_with_kwargs_raises(self):
        """Test that calling with kwargs raises NotImplementedError."""
        payload = RpcPayload.owned(lambda: None)
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        with pytest.raises(
            NotImplementedError, match="Keyword arguments not yet supported"
        ):
            stub(a=1, b=2)


class TestRpcPromiseBasics:
    """Test basic RpcPromise functionality."""

    def test_promise_initialization(self):
        """Test that promise initializes with a hook."""
        payload = RpcPayload.owned({"value": 42})
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        assert promise._hook is hook

    def test_promise_repr(self):
        """Test promise string representation."""
        payload = RpcPayload.owned({"value": 42})
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        repr_str = repr(promise)
        assert "RpcPromise" in repr_str
        assert "PayloadStubHook" in repr_str

    def test_promise_dispose(self):
        """Test disposing a promise."""
        future: asyncio.Future = asyncio.Future()
        hook = PromiseStubHook(future)
        promise = RpcPromise(hook)

        # Should not raise. B2/TS parity (core.ts:1984-1994): dispose
        # defers disposal to the eventual resolution; it does NOT cancel
        # in-flight work (chained hooks share the future).
        promise.dispose()
        assert not future.cancelled()

    @pytest.mark.asyncio
    async def test_promise_async_context_manager(self):
        """Test using promise as async context manager."""
        payload = RpcPayload.owned(42)
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        async with promise as value:
            assert value == 42


class TestRpcPromisePropertyAccess:
    """Test property access on RpcPromise."""

    @pytest.mark.asyncio
    async def test_promise_get_property(self):
        """Test accessing property on a promise."""
        payload = RpcPayload.owned({"user": {"id": 123}})
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        # Access property - returns new promise
        id_promise = promise.user.id
        assert isinstance(id_promise, RpcPromise)

        result = await id_promise
        assert result == 123

    @pytest.mark.asyncio
    async def test_promise_chained_properties(self):
        """Test chaining multiple property accesses."""
        payload = RpcPayload.owned({"a": {"b": {"c": {"d": "value"}}}})
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        # Deep chain
        result_promise = promise.a.b.c.d
        result = await result_promise

        assert result == "value"

    def test_promise_private_attribute_raises(self):
        """Test accessing private attributes raises AttributeError."""
        payload = RpcPayload.owned({"value": 42})
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        with pytest.raises(AttributeError, match="has no attribute '_private'"):
            _ = promise._private


class TestRpcPromiseCalling:
    """Test calling RpcPromise."""

    @pytest.mark.asyncio
    async def test_promise_call_with_args(self):
        """Test calling a promise with arguments."""

        multiply = operator.mul

        payload = RpcPayload.owned(multiply)
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        result_promise = promise(6, 7)
        assert isinstance(result_promise, RpcPromise)

        result = await result_promise
        assert result == 42

    @pytest.mark.asyncio
    async def test_promise_call_no_args(self):
        """Test calling promise with no arguments."""

        def get_constant():
            return "constant"

        payload = RpcPayload.owned(get_constant)
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        result_promise = promise()
        result = await result_promise
        assert result == "constant"

    def test_promise_call_with_kwargs_raises(self):
        """Test that calling with kwargs raises NotImplementedError."""
        payload = RpcPayload.owned(lambda: None)
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        with pytest.raises(
            NotImplementedError, match="Keyword arguments not yet supported"
        ):
            promise(x=1, y=2)


class TestRpcPromiseAwaiting:
    """Test awaiting RpcPromise."""

    @pytest.mark.asyncio
    async def test_await_simple_value(self):
        """Test awaiting a promise for a simple value."""
        payload = RpcPayload.owned(42)
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        result = await promise
        assert result == 42

    @pytest.mark.asyncio
    async def test_await_complex_value(self):
        """Test awaiting a promise for a complex object."""
        data = {"user": {"id": 123, "name": "Bob"}, "count": 5}
        payload = RpcPayload.owned(data)
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        result = await promise
        assert result == data

    @pytest.mark.asyncio
    async def test_await_error_raises(self):
        """Test awaiting a promise that contains an error."""
        error = RpcError.not_found("Resource not found")
        hook = ErrorStubHook(error)
        promise = RpcPromise(hook)

        with pytest.raises(RpcError, match="Resource not found"):
            await promise

    @pytest.mark.asyncio
    async def test_await_async_future(self):
        """Test awaiting a promise backed by a future."""
        future: asyncio.Future = asyncio.Future()
        hook = PromiseStubHook(future)
        promise = RpcPromise(hook)

        # Resolve the future in the background
        async def resolve_later():
            await asyncio.sleep(0.01)
            payload = RpcPayload.owned("resolved")
            future.set_result(PayloadStubHook(payload))

        asyncio.create_task(resolve_later())

        # Await the promise
        result = await promise
        assert result == "resolved"


class TestComplexChaining:
    """Test complex chaining scenarios."""

    @pytest.mark.asyncio
    async def test_stub_property_call_chain(self):
        """Test chaining property access and method calls on stub."""

        class User:
            def __init__(self):
                self.profile = {"name": "Alice"}

            async def get_name(self):
                return self.profile["name"]

        user = User()
        payload = RpcPayload.owned({"user": user})
        hook = PayloadStubHook(payload)
        stub = RpcStub(hook)

        # Chain: property access then method call
        promise = stub.user.get_name()
        result = await promise

        assert result == "Alice"

    @pytest.mark.asyncio
    async def test_promise_property_call_chain(self):
        """Test chaining property access and calls on promise."""
        data = {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        }
        payload = RpcPayload.owned(data)
        hook = PayloadStubHook(payload)
        promise = RpcPromise(hook)

        # Access nested property
        users_promise = promise.users
        users = await users_promise

        assert len(users) == 2
        assert users[0]["name"] == "Alice"
