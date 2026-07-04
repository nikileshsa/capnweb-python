"""Core type definitions for Cap'n Web protocol.

Also home of the D5 Python type mappings (capnweb-parity-plan.md):

* ``Undefined``   -- singleton distinct from ``None``; wire ``["undefined"]``.
* ``InvalidDate`` -- singleton for JS ``new Date(NaN)``; wire ``["date", null]``.
* ``Headers``     -- case-insensitive multi-map matching Fetch iteration
                     semantics; wire ``["headers", [[k, v], ...]]``.
* ``Request``     -- frozen dataclass; wire ``["request", url, init]``.
* ``Response``    -- frozen dataclass; wire ``["response", body, init]``.

* ``Blob``       -- immutable (type, bytes) pair with a ``stream()`` method;
                     wire ``["blob", type, ["readable", id]]`` (always piped).

Encode/decode of these forms lives in serializer.py / parser.py
(serialize.ts:230-355 / 723-807). Stream bodies ride the pipe machinery in
capnweb/streams.py (parity stream B1).
"""

from __future__ import annotations

import asyncio
from abc import ABC
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Protocol


class _UndefinedType:
    """JavaScript ``undefined``. Falsy singleton, distinct from ``None``."""

    _instance: _UndefinedType | None = None

    def __new__(cls) -> _UndefinedType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "undefined"

    def __reduce__(self) -> tuple[Any, ...]:
        return (_UndefinedType, ())


class _InvalidDateType:
    """JS ``new Date(NaN)`` sentinel. Wire form ``["date", null]`` (PR #152)."""

    _instance: _InvalidDateType | None = None

    def __new__(cls) -> _InvalidDateType:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "InvalidDate"

    def __reduce__(self) -> tuple[Any, ...]:
        return (_InvalidDateType, ())


#: C-SENTINELS singletons (re-exported as ``capnweb.Undefined`` / ``capnweb.InvalidDate``).
Undefined = _UndefinedType()
InvalidDate = _InvalidDateType()


# Chunk size used when streaming a Blob's bytes through a pipe. The upstream
# encoder always streams blobs (serialize.ts:358-369) precisely so that large
# blobs don't produce excessively large individual messages.
_BLOB_STREAM_CHUNK = 64 * 1024


class Blob:
    """Binary large object (D5 mapping of the JS ``Blob``; matrix 02 row 11).

    Wire form: ``["blob", type, ["readable", importId]]`` — the bytes always
    travel through a pipe (protocol.md:175-177); there is no inline fast path
    even for small blobs. On decode, the RPC system collects the whole pipe
    before delivering the value, so application code always sees a complete
    ``Blob``.
    """

    __slots__ = ("type", "data")

    def __init__(self, type: str, data: bytes) -> None:  # noqa: A002 - JS name
        if not isinstance(type, str):
            raise TypeError(f"Blob type must be a string, got {type!r}")
        if isinstance(data, (bytearray, memoryview)):
            data = bytes(data)
        if not isinstance(data, bytes):
            raise TypeError(f"Blob data must be bytes, got {data.__class__.__name__}")
        object.__setattr__(self, "type", type)
        object.__setattr__(self, "data", data)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("Blob is immutable")

    @property
    def size(self) -> int:
        return len(self.data)

    def stream(self) -> Any:
        """The blob's bytes as an ``RpcReadableStream`` (JS ``blob.stream()``)."""
        from capnweb.streams import RpcReadableStream

        data = self.data

        async def _chunks() -> Any:
            for offset in range(0, len(data), _BLOB_STREAM_CHUNK):
                yield data[offset : offset + _BLOB_STREAM_CHUNK]

        return RpcReadableStream(_chunks())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Blob):
            return NotImplemented
        return self.type == other.type and self.data == other.data

    def __hash__(self) -> int:
        return hash((self.type, self.data))

    def __repr__(self) -> str:
        return f"Blob(type={self.type!r}, size={len(self.data)})"


class Headers:
    """Case-insensitive multi-map of HTTP headers (Fetch semantics).

    Iteration yields ``(name, value)`` pairs with lowercase names, sorted by
    name, with multiple values for the same name combined with ``", "`` --
    exactly what the TS ``Headers`` iterator produces, so the wire form
    ``["headers", [[k, v], ...]]`` is byte-compatible (serialize.ts:230-233).
    """

    __slots__ = ("_entries",)

    def __init__(
        self,
        init: Iterable[tuple[str, str] | list[str]] | dict[str, str] | Headers | None = None,
    ) -> None:
        self._entries: list[tuple[str, str]] = []
        if init is None:
            return
        if isinstance(init, Headers):
            self._entries.extend(init._entries)
        elif isinstance(init, dict):
            for name, value in init.items():
                self.append(name, value)
        else:
            for pair in init:
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    raise TypeError(
                        f"Headers init entries must be [name, value] pairs, got {pair!r}"
                    )
                self.append(pair[0], pair[1])

    @staticmethod
    def _validate(name: Any, value: Any) -> tuple[str, str]:
        if not isinstance(name, str) or not name:
            raise TypeError(f"Invalid header name: {name!r}")
        if not isinstance(value, str):
            raise TypeError(f"Invalid header value for {name!r}: {value!r}")
        return name.lower(), value

    def append(self, name: str, value: str) -> None:
        """Append a value, preserving existing values for the same name."""
        self._entries.append(self._validate(name, value))

    def set(self, name: str, value: str) -> None:
        """Replace all values for ``name`` with a single value."""
        lname, value = self._validate(name, value)
        self._entries = [(n, v) for n, v in self._entries if n != lname]
        self._entries.append((lname, value))

    def get(self, name: str) -> str | None:
        """Combined (", "-joined) value for ``name``, or None."""
        lname = name.lower()
        values = [v for n, v in self._entries if n == lname]
        return ", ".join(values) if values else None

    def has(self, name: str) -> bool:
        lname = name.lower()
        return any(n == lname for n, _ in self._entries)

    def delete(self, name: str) -> None:
        lname = name.lower()
        self._entries = [(n, v) for n, v in self._entries if n != lname]

    def __iter__(self) -> Iterator[tuple[str, str]]:
        combined: dict[str, list[str]] = {}
        for n, v in self._entries:
            combined.setdefault(n, []).append(v)
        for n in sorted(combined):
            yield n, ", ".join(combined[n])

    def items(self) -> Iterator[tuple[str, str]]:
        return iter(self)

    def __len__(self) -> int:
        return len({n for n, _ in self._entries})

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Headers):
            return NotImplemented
        return list(self) == list(other)

    def __repr__(self) -> str:
        return f"Headers({list(self)!r})"


@dataclass(frozen=True)
class Request:
    """HTTP Request value type (wire form ``["request", url, init]``).

    Only non-default init fields are emitted on encode, mirroring
    serialize.ts:235-325. ``body`` may be None, str, bytes, or an
    ``RpcReadableStream`` (streamed bodies; ``duplex: "half"`` is emitted
    for stream bodies per serialize.ts:257-262). Platform-specific init
    fields (duplex, mode, credentials, referrer, referrerPolicy, keepalive,
    cf, encodeResponseBody, ...) are preserved opaquely in ``extensions``.
    """

    url: str
    method: str = "GET"
    headers: Headers = field(default_factory=Headers)
    body: Any = None  # None | str | bytes | RpcReadableStream
    redirect: str = "follow"
    integrity: str = ""
    cache: str = "default"
    extensions: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Response:
    """HTTP Response value type (wire form ``["response", body, init]``).

    ``body`` may be None, str, bytes, or an ``RpcReadableStream`` (streamed
    bodies). ``webSocket`` responses are rejected on both encode and decode
    (serialize.ts:349-352, 794-798). CF-specific init fields are kept in
    ``extensions``.
    """

    body: Any = None  # None | str | bytes | RpcReadableStream
    status: int = 200
    status_text: str = ""
    headers: Headers = field(default_factory=Headers)
    extensions: dict[str, Any] = field(default_factory=dict)


class RpcTarget(ABC):
    """Base class for RPC capability implementations.

    Capabilities are objects that can receive method calls and property access
    over the RPC protocol.

    Usage (ergonomic style - recommended):
        class MyApi(RpcTarget):
            def hello(self, name: str) -> str:
                return f"Hello, {name}!"

            async def fetch_data(self, id: int) -> dict:
                return {"id": id, "data": "..."}

    Usage (explicit style - for custom dispatch):
        class MyApi(RpcTarget):
            async def call(self, method: str, args: list[Any]) -> Any:
                match method:
                    case "hello":
                        return f"Hello, {args[0]}!"
                    case _:
                        raise RpcError.not_found(f"Unknown method: {method}")

    Both styles are fully supported. If you override `call()`, it takes precedence.
    Otherwise, public methods (not starting with '_') are automatically exposed.
    """

    # Methods that should never be exposed as RPC endpoints
    _rpc_reserved_methods = frozenset({
        'call', 'get_property', 'dispose',
        # Python special methods
        '__init__', '__new__', '__del__', '__repr__', '__str__',
        '__hash__', '__eq__', '__ne__', '__lt__', '__le__', '__gt__', '__ge__',
        '__getattr__', '__setattr__', '__delattr__', '__getattribute__',
        '__class__', '__dict__', '__doc__', '__module__', '__weakref__',
    })

    async def call(self, method: str, args: list[Any]) -> Any:
        """Call a method on this capability.

        Default implementation dispatches to public methods on the class.
        Override this method for custom dispatch logic.

        Args:
            method: The method name to call
            args: List of arguments for the method

        Returns:
            The result of the method call

        Raises:
            RpcError: If the method call fails
        """
        from capnweb.error import RpcError

        # Check if method exists and is callable
        if method.startswith('_'):
            raise RpcError.not_found(f"Method not found: {method}")

        if method in self._rpc_reserved_methods:
            raise RpcError.not_found(f"Method not found: {method}")

        func = getattr(self, method, None)
        if func is None or not callable(func):
            raise RpcError.not_found(f"Method not found: {method}")

        # Call the method with args
        try:
            result = func(*args)
            # Handle both sync and async methods
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except TypeError as e:
            # Convert argument errors to RPC errors
            raise RpcError.bad_request(str(e)) from e

    async def get_property(self, prop: str) -> Any:
        """Get a property from this capability.

        Default implementation returns public attributes.
        Override this method for custom property access.

        Args:
            prop: The property name to access

        Returns:
            The property value

        Raises:
            RpcError: If the property access fails
        """
        from capnweb.error import RpcError

        if prop.startswith('_'):
            raise RpcError.not_found(f"Property not found: {prop}")

        if not hasattr(self, prop):
            raise RpcError.not_found(f"Property not found: {prop}")

        value = getattr(self, prop)

        # Don't expose methods as properties
        if callable(value):
            raise RpcError.not_found(f"Property not found: {prop}")

        return value


class Transport(Protocol):
    """Protocol for RPC transports."""

    async def send(self, data: bytes) -> None:
        """Send data over the transport.

        Args:
            data: The data to send

        Raises:
            Exception: If sending fails
        """
        ...

    async def receive(self) -> bytes:
        """Receive data from the transport.

        Returns:
            The received data

        Raises:
            Exception: If receiving fails
        """
        ...

    async def close(self) -> None:
        """Close the transport connection.

        Raises:
            Exception: If closing fails
        """
        ...
