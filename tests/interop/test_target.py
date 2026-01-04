"""Shared test target implementation matching TypeScript TestTarget.

This provides a consistent interface for interop testing between
TypeScript and Python implementations.
"""

from __future__ import annotations

from capnweb.types import RpcTarget
from capnweb.stubs import RpcStub
from capnweb.payload import RpcPayload


class Counter(RpcTarget):
    """A simple counter that can be incremented."""
    
    __slots__ = ('_value',)
    
    def __init__(self, initial: int = 0) -> None:
        self._value = initial
    
    async def call(self, method: str, args: list) -> any:
        if method == "increment":
            amount = args[0] if args else 1
            self._value += amount
            return self._value
        raise ValueError(f"Unknown method: {method}")
    
    def get_property(self, name: str) -> any:
        if name == "value":
            return self._value
        raise AttributeError(f"Unknown property: {name}")


class TestTarget(RpcTarget):
    """Test target matching TypeScript TestTarget for interop testing.
    
    Methods:
    - square(i) -> i * i
    - callSquare(self, i) -> {result: self.square(i)}
    - callFunction(func, i) -> {result: func(i)}
    - throwError() -> throws RangeError
    - makeCounter(i) -> Counter(i)
    - incrementCounter(c, i=1) -> c.increment(i)
    - generateFibonacci(length) -> [0, 1, 1, 2, ...]
    - returnNull() -> null
    - returnUndefined() -> undefined
    - returnNumber(i) -> i
    - echo(value) -> value
    - add(a, b) -> a + b
    - greet(name) -> "Hello, {name}!"
    """
    
    async def call(self, method: str, args: list) -> any:
        if method == "square":
            return args[0] * args[0]
        
        elif method == "callSquare":
            # args[0] is a stub for self, args[1] is i
            stub = args[0]
            i = args[1]
            hook = stub._hook if isinstance(stub, RpcStub) else stub
            result_hook = hook.call(["square"], RpcPayload.owned([i]))
            result = await result_hook.pull()
            return {"result": result.value}
        
        elif method == "callFunction":
            # args[0] is a function stub, args[1] is i
            func = args[0]
            i = args[1]
            hook = func._hook if isinstance(func, RpcStub) else func
            result_hook = hook.call([], RpcPayload.owned([i]))
            result = await result_hook.pull()
            return {"result": result.value}
        
        elif method == "throwError":
            raise ValueError("test error")
        
        elif method == "makeCounter":
            return Counter(args[0])
        
        elif method == "incrementCounter":
            counter = args[0]
            amount = args[1] if len(args) > 1 else 1
            hook = counter._hook if isinstance(counter, RpcStub) else counter
            result_hook = hook.call(["increment"], RpcPayload.owned([amount]))
            result = await result_hook.pull()
            return result.value
        
        elif method == "generateFibonacci":
            length = args[0]
            if length <= 0:
                return []
            if length == 1:
                return [0]
            result = [0, 1]
            while len(result) < length:
                result.append(result[-1] + result[-2])
            return result
        
        elif method == "returnNull":
            return None
        
        elif method == "returnUndefined":
            return None  # Python doesn't have undefined
        
        elif method == "returnNumber":
            return args[0]
        
        elif method == "echo":
            return args[0] if args else None
        
        elif method == "add":
            return args[0] + args[1]
        
        elif method == "greet":
            return f"Hello, {args[0]}!"
        
        elif method == "getList":
            return [1, 2, 3, 4, 5]
        
        elif method == "registerCallback":
            # Store callback for later use
            self._callback = args[0]
            return "registered"
        
        elif method == "triggerCallback":
            if not hasattr(self, '_callback') or self._callback is None:
                return "no callback"
            hook = (
                self._callback._hook
                if isinstance(self._callback, RpcStub)
                else self._callback
            )
            result_hook = hook.call(["notify"], RpcPayload.owned(["ping"]))
            result = await result_hook.pull()
            return result.value
        
        raise ValueError(f"Unknown method: {method}")
    
    def get_property(self, name: str) -> any:
        raise AttributeError(f"Unknown property: {name}")
