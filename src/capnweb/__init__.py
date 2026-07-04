"""Cap'n Web Protocol - Python Implementation

This module provides a Python implementation of the Cap'n Web protocol,
a capability-based RPC system with promise pipelining.
"""

from capnweb.config import (
    ClientConfig,
    RpcSessionConfig,
    WebSocketServerConfig,
    BatchRpcConfig,
)
from capnweb.error import ErrorCode, RpcError
from capnweb.inprocess import InProcessPipeTransport, new_pipe_rpc_session_pair
from capnweb.parser import deserialize
from capnweb.serializer import serialize
from capnweb.types import (
    Blob,
    Headers,
    InvalidDate,
    Request,
    Response,
    RpcTarget,
    Undefined,
)
from capnweb.streams import (
    FlowController,
    RpcReadableStream,
    RpcWritableStream,
)
from capnweb.stubs import RpcStub, RpcPromise, create_stub, get_remote_main
from capnweb.rpc_session import (
    BidirectionalSession,
    RpcSessionOptions,
    RpcTransport,
)
from capnweb.ws_session import (
    WebSocketRpcClient,
    WebSocketRpcServer,
    handle_websocket_rpc,
    new_websocket_rpc_session,
    wait_closed,
)
from capnweb.ws_transport import WebSocketTransport
from capnweb.unified_client import UnifiedClient, UnifiedClientConfig
from capnweb.map_builder import MapBuilder, build_map
from capnweb.batch import (
    new_http_batch_rpc_session,
    new_http_batch_rpc_response,
    aiohttp_batch_rpc_handler,
    aiohttp_rpc_handler,
    fastapi_batch_rpc_handler,
    BatchClientTransport,
    BatchEndError,
    BatchServerTransport,
)

__version__ = "0.1.0"

# TS-parity alias: `RpcSession` is the custom-transport escape hatch
# (index.ts:70-81). Python's implementation class is BidirectionalSession.
RpcSession = BidirectionalSession

__all__ = [
    # Core types
    "RpcTarget",
    "RpcStub",
    "RpcPromise",
    "create_stub",
    "get_remote_main",
    # Errors
    "RpcError",
    "ErrorCode",
    # Configuration (Pydantic models)
    "ClientConfig",
    "RpcSessionConfig",
    "WebSocketServerConfig",
    "BatchRpcConfig",
    # Sentinels + value types (C-SENTINELS / D5)
    "Undefined",
    "InvalidDate",
    "Blob",
    "Headers",
    "Request",
    "Response",
    # Streams (C-STREAM / D5)
    "RpcReadableStream",
    "RpcWritableStream",
    "FlowController",
    # Standalone serialization helpers (no session; stubs/pipes raise)
    "serialize",
    "deserialize",
    # Session (custom-transport escape hatch)
    "RpcSession",  # TS-parity alias for BidirectionalSession
    "BidirectionalSession",
    "RpcSessionOptions",  # Backwards compat alias for RpcSessionConfig
    "RpcTransport",
    # In-process sessions (MessagePort analog)
    "new_pipe_rpc_session_pair",
    "InProcessPipeTransport",
    # WebSocket sessions
    "new_websocket_rpc_session",
    "WebSocketRpcClient",
    "WebSocketRpcServer",
    "WebSocketTransport",
    "handle_websocket_rpc",
    "wait_closed",
    # Unified Client
    "UnifiedClient",
    "UnifiedClientConfig",  # Backwards compat alias for ClientConfig
    # Map operations
    "MapBuilder",
    "build_map",
    # HTTP Batch RPC
    "new_http_batch_rpc_session",
    "new_http_batch_rpc_response",
    "aiohttp_batch_rpc_handler",
    "aiohttp_rpc_handler",  # unified POST+WS endpoint (sets ACAO: *)
    "fastapi_batch_rpc_handler",
    "BatchClientTransport",
    "BatchEndError",
    "BatchServerTransport",
]
