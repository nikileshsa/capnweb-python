# Architecture Guide

This guide explains the internal architecture of Cap'n Web Python, focusing on the hook-based design that makes the library modular and maintainable.

## Table of Contents

- [High-Level Overview](#high-level-overview)
- [Hook-Based Architecture](#hook-based-architecture)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Design Decisions](#design-decisions)

## High-Level Overview

Cap'n Web Python is built around a **decentralized, hook-based architecture**. Instead of a monolithic evaluator that handles all RPC operations, the system is composed of small, focused components called **hooks** that each know how to handle specific scenarios.

```
┌─────────────────────────────────────────────────────────┐
│                        Client/Server                    │
│                   (extends RpcSession)                  │
├─────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │   Parser     │  │  Serializer  │  │   Session    │   │
│  │ (wire → obj) │  │ (obj → wire) │  │  (imports/   │   │
│  │              │  │              │  │   exports)   │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                   StubHook Hierarchy                    │
├─────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ ErrorStub    │  │ PayloadStub  │  │ TargetStub   │   │
│  │ Hook         │  │ Hook         │  │ Hook         │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ ImportHook   │  │ PromiseStub  │  │ PipelineHook │   │
│  │              │  │ Hook         │  │              │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                User-Facing Wrappers                     │
├─────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐                     │
│  │   RpcStub    │  │  RpcPromise  │                     │
│  │ (__getattr__,│  │  (__await__, │                     │
│  │  __call__)   │  │  __getattr__)│                     │
│  └──────────────┘  └──────────────┘                     │
└─────────────────────────────────────────────────────────┘
```

## Hook-Based Architecture

### What is a Hook?

A **hook** is an object that knows how to handle RPC operations for a specific type of capability. All hooks implement the `StubHook` protocol:

```python
class StubHook(ABC):
    @abstractmethod
    def call(self, path: list[str | int], args: RpcPayload) -> StubHook:
        """Call a method through this hook (synchronous).
        
        Synchronous to ensure messages are queued before batch
        transports send their requests.
        """

    @abstractmethod
    def map(self, path: list[str | int], captures: list[StubHook], 
            instructions: list[Any]) -> StubHook:
        """Apply a map operation over an array."""

    @abstractmethod
    def get(self, path: list[str | int]) -> StubHook:
        """Get a property through this hook."""

    @abstractmethod
    async def pull(self) -> RpcPayload:
        """Resolve to final value."""

    @abstractmethod
    def dispose(self) -> None:
        """Release resources."""

    @abstractmethod
    def dup(self) -> StubHook:
        """Duplicate this hook (increment refcount)."""
```

### Hook Types

Each hook type handles a specific scenario:

#### 1. **ErrorStubHook** - Propagates Errors

When an error occurs, it's wrapped in an `ErrorStubHook`. All operations on an error hook return the same error, allowing errors to propagate through call chains.

```python
hook = ErrorStubHook(RpcError.not_found("User not found"))
result = await hook.call(["method"], args)  # Returns same error hook
value = await hook.pull()  # Raises the error
```

**Use case:** Error propagation without try/catch at every level

#### 2. **PayloadStubHook** - Wraps Local Data

Wraps locally-resolved values (primitives, dicts, lists, or callables).

```python
payload = RpcPayload.owned({"user": {"id": 123}})
hook = PayloadStubHook(payload)

# Navigate properties
user_hook = hook.get(["user"])
id_hook = user_hook.get(["id"])
result = await id_hook.pull()  # 123
```

**Use case:** Local data that doesn't need network calls

#### 3. **TargetStubHook** - Wraps Server-Side Objects

Wraps an `RpcTarget` (server-side object) with reference counting.

```python
class Calculator(RpcTarget):
    async def call(self, method: str, args: list):
        match method:
            case "add":
                return args[0] + args[1]

target = Calculator()
hook = TargetStubHook(target)

# Call methods on the target
result_hook = await hook.call(["add"], RpcPayload.owned([5, 3]))
result = await result_hook.pull()  # 8
```

**Use case:** Server-side capabilities

#### 4. **ImportHook** - References Remote Capabilities

Represents a capability imported from a remote peer. Sends messages to the remote side when called.

```python
# Created by the session when receiving ["export", id] from remote
hook = session.import_capability(import_id)

# Calling it sends a message to the remote peer
result_hook = hook.call(["method"], args)  # Synchronous!
```

**Use case:** Remote capability references

#### 5. **PipelineHook** - Pipelined Property Access

Represents a pipelined property access on an unresolved import. Allows chaining calls without waiting for resolution.

```python
# Created when accessing properties on an ImportHook
import_hook = session.import_capability(import_id)
pipeline_hook = import_hook.get(["user", "profile"])

# Can chain further operations
result = pipeline_hook.call(["getName"], args)
```

**Use case:** Promise pipelining, reducing round-trips

#### 6. **PromiseStubHook** - Wraps Async Operations

Wraps a `Future` that will eventually resolve to another hook.

```python
future: asyncio.Future[StubHook] = asyncio.create_task(async_operation())
hook = PromiseStubHook(future)

# pull() awaits the future
final_payload = await hook.pull()
```

**Use case:** Pipelining, async operations

## Core Components

### RpcSession

The `RpcSession` base class manages capability tables:

```python
class BidirectionalSession:
    _imports: dict[int, ImportEntry]   # Remote capabilities we reference
    _exports: dict[int, ExportEntry]   # Local capabilities we expose
    _reverse_exports: dict[int, int]   # O(1) capability lookup
    _pull_count: int                   # Pending operations counter

    def get_import(self, import_id: int) -> ImportHook:
        """Get or create an ImportHook for a remote capability."""

    def push(self, hook: StubHook) -> int:
        """Export a local capability, return export ID."""

    async def drain(self) -> None:
        """Wait for all pending operations to complete."""
```

Both `Client` and `Server` extend `RpcSession`.

### Parser (Deserializer)

Converts wire format to Python objects, creating hooks as needed:

```python
class Parser:
    def parse(self, wire_value: Any) -> RpcPayload:
        """Parse wire format into Python objects."""

# ["export", 1] → RpcStub(RpcImportHook(...))
# ["promise", 2] → RpcPromise(PromiseStubHook(...))
# ["error", "not_found", "msg"] → RpcStub(ErrorStubHook(...))
# {"user": {"id": 123}} → regular dict
```

### Serializer (Valuator)

Converts Python objects to wire format, exporting capabilities:

```python
class Serializer:
    def serialize(self, value: Any) -> Any:
        """Serialize Python objects to wire format."""

# RpcStub → ["export", id]
# RpcPromise → ["promise", id]
# RpcError → ["error", type, message, ...]
# {"user": {"id": 123}} → same dict
```

### RpcPayload - Ownership Tracking

`RpcPayload` tracks data provenance to prevent mutation bugs:

```python
class PayloadSource(Enum):
    PARAMS = auto()  # From app - must deep copy
    RETURN = auto()  # From app - we own it
    OWNED = auto()   # Already copied - safe to use

payload = RpcPayload.from_app_params({"data": [1, 2, 3]})
payload.ensure_deep_copied()  # Makes a copy to prevent mutations
```

**Why this matters:** Prevents the RPC system from accidentally mutating application data.

### RpcStub & RpcPromise - User-Facing Wrappers

Thin wrappers around hooks that provide Pythonic syntax:

```python
class RpcStub:
    def __init__(self, hook: StubHook):
        self._hook = hook

    def __getattr__(self, name: str):
        """Property access → hook.get([name])"""
        return RpcStub(self._hook.get([name]))

    def __call__(self, *args):
        """Method call → hook.call([...], args)"""
        # Returns RpcPromise
```

This allows natural syntax:

```python
result = await stub.user.profile.getName()
# Internally: get(["user"]) → get(["profile"]) → call(["getName"], [])
```

## Data Flow

### Client → Server Call

```
1. Client: stub.method(args)
   ↓
2. Client: Serializer converts args to wire format
   ↓
3. Client: Send WirePush message
   ↓
4. Server: Parser converts wire → Python objects (creates hooks)
   ↓
5. Server: Hook.call() executes the method
   ↓
6. Server: Serializer converts result to wire format
   ↓
7. Server: Send WireResolve message
   ↓
8. Client: Parser converts wire → Python objects
   ↓
9. Client: Return result to application
```

### Promise Pipelining

```
1. Client: Create PipelineBatch
   ↓
2. Client: batch.call("method1")  # Returns promise, no network call yet
3. Client: batch.call("method2")  # Returns promise, no network call yet
   ↓
4. Client: await batch.execute()  # Single network round-trip!
   ↓
5. Server: Receives all calls, executes them
6. Server: Stores results in batch-local import table
   ↓
7. Client: Sends WirePull messages
   ↓
8. Server: Returns all results
   ↓
9. Client: Promises resolve
```

## Design Decisions

### Why Hooks Instead of a Monolithic Evaluator?

**Old approach (monolithic evaluator):**
- Single `evaluate()` function handles everything
- Hundreds of lines, high cyclomatic complexity
- Hard to test, hard to extend
- Tight coupling

**New approach (hook-based):**
- Each hook type is ~50-100 lines
- Easy to test in isolation
- Easy to add new hook types
- Loose coupling

### Why RpcPayload for Ownership Tracking?

JavaScript doesn't have this problem because objects are always passed by reference. In Python, we need to explicitly track:
- When to deep-copy data (PARAMS)
- When we own data (RETURN, OWNED)

This prevents subtle bugs where the RPC system mutates application data.

### Why Separate Parser and Serializer?

**Parser (wire → Python):**
- Creates hooks for remote capabilities
- Uses the `Importer` protocol (RpcSession)

**Serializer (Python → wire):**
- Exports local capabilities
- Uses the `Exporter` protocol (RpcSession)

They're symmetric but opposite operations, so separating them makes the code clearer.

### Why StubHook vs Just Using RpcStub Directly?

**RpcStub** is the user-facing wrapper with magic methods (`__getattr__`, `__call__`).

**StubHook** is the internal implementation that knows how to actually execute operations.

This separation allows:
- Multiple `RpcStub` instances wrapping the same hook (reference counting)
- Different hook types with different behaviors
- Internal hooks that aren't exposed to users

## Next Steps

- **[API Reference](api-reference.md)** - Detailed class documentation
- **[Advanced Topics](advanced.md)** - Resume tokens, transports, bidirectional RPC
- **[Contributing](../CONTRIBUTING.md)** - How to contribute to the project
