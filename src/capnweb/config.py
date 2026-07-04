"""Pydantic configuration models for Cap'n Web.

This module contains Pydantic models for user-facing configuration classes.
These are NOT used in hot paths (wire parsing, message handling) to maintain
performance - only for startup/initialization configuration.

Wire format classes remain as @dataclass(frozen=True, slots=True) for performance.
"""

from __future__ import annotations

from typing import Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RpcSessionConfig(BaseModel):
    """Configuration options for RPC sessions.

    ``on_send_error`` is the entire TS 0.9.0 option surface
    (rpc.ts:433-446). The remaining fields are documented Python-only
    extensions (asyncio has no equivalent of TS's rely-on-GC model, so
    bounded waits are exposed as knobs).

    Attributes:
        on_send_error: Optional callback to transform errors before sending.
            Useful for redacting sensitive information from stack traces.
        pull_timeout: Python-only. Bound (seconds) on waiting for a promise
            resolution from the peer; a lost/mis-routed resolve surfaces as
            an error instead of hanging forever (TS hangs by design and
            relies on onRpcBroken). Default 120s. ``None`` keeps the default.
        drain_timeout: Python-only. Bound (seconds) on the HTTP batch
            server's wait for the batch to complete. On expiry the server
            raises (HTTP 500) instead of returning a truncated 200.
            Default 30s; ``None`` disables the bound.

    Resource-exhaustion bounds (security hardening, audit findings F1-F6).
    These are LOCAL policy — the wire format is unchanged; a peer that
    exceeds a bound is aborted. Every default is set well above legitimate
    interop traffic, so a well-behaved peer never trips one. Tune per
    deployment (e.g. a trusted intra-cluster link may raise them):

        max_exports: Max LIVE entries in the export table (peer pushes).
            Post-release deletes shrink the table, so a grant+release peer
            stays far under the cap; only a hoarding peer trips it. Closes
            F1. Default 100_000.
        max_imports: Max LIVE entries in the import table (capabilities the
            peer references). Closes F2. Default 100_000.
        max_message_bytes: Max size of a single inbound frame, checked
            BEFORE parsing. Closes F3. Default 16 MiB.
        max_array_len: Max element count of a single decoded wire array.
            Closes F5. Default 1_000_000.
        max_blob_bytes: Max total bytes accumulated while collecting a
            streamed blob. Closes F4. Default 64 MiB.
        redact_internal_errors: When True (default), the free-text message
            of an UNEXPECTED (non-``RpcError``) application exception is
            replaced with a generic ``"internal error"`` before it crosses
            the wire — the exception type/name is preserved, but internal
            detail (filesystem paths, secrets embedded in the message) does
            NOT leak to an untrusted peer. Deliberate ``RpcError`` protocol
            signals raised by app code keep their message. ``on_send_error``,
            if set, still runs and takes precedence. Closes F6.
    """

    model_config = ConfigDict(
        frozen=False,  # Allow mutation for callback assignment
        arbitrary_types_allowed=True,  # Allow Callable types
    )

    on_send_error: Callable[[Exception], Exception | None] | None = None
    pull_timeout: float | None = Field(
        default=None,
        gt=0,
        description="Python-only: bounded pull wait in seconds (default 120)",
    )
    drain_timeout: float | None = Field(
        default=30.0,
        gt=0,
        description="Python-only: HTTP batch server drain bound in seconds",
    )

    # --- Resource-exhaustion bounds (F1-F6) ---------------------------------
    max_exports: int = Field(
        default=100_000,
        gt=0,
        description="F1: max live export-table entries before aborting a peer",
    )
    max_imports: int = Field(
        default=100_000,
        gt=0,
        description="F2: max live import-table entries before aborting a peer",
    )
    max_message_bytes: int = Field(
        default=16 * 1024 * 1024,
        gt=0,
        description="F3: max size (bytes) of a single inbound frame",
    )
    max_array_len: int = Field(
        default=1_000_000,
        gt=0,
        description="F5: max element count of a single decoded wire array",
    )
    max_blob_bytes: int = Field(
        default=64 * 1024 * 1024,
        gt=0,
        description="F4: max total bytes accumulated for a streamed blob",
    )
    redact_internal_errors: bool = Field(
        default=True,
        description="F6: redact unexpected-exception text before the wire",
    )


class ClientConfig(BaseModel):
    """Configuration for the unified RPC client.
    
    This replaces UnifiedClientConfig with Pydantic validation.
    
    Attributes:
        url: The RPC endpoint URL (ws://, wss://, http://, https://)
        timeout: Request timeout in seconds (must be positive)
        local_main: Optional capability to expose to the server
        session_options: Optional session configuration
    """
    
    model_config = ConfigDict(
        frozen=False,
        arbitrary_types_allowed=True,
    )
    
    url: str = Field(..., description="RPC endpoint URL")
    timeout: float = Field(
        default=30.0,
        gt=0,
        description="Request timeout in seconds"
    )
    local_main: Any | None = Field(
        default=None,
        description="Optional capability to expose to server"
    )
    options: RpcSessionConfig | None = Field(
        default=None,
        description="Optional session configuration"
    )
    transport: Literal["auto", "websocket", "http-batch", "webtransport"] = Field(
        default="auto",
        description=(
            "Explicit transport selection. 'auto' picks WebSocket for "
            "ws:///wss:// URLs and HTTP batch for http:///https:// URLs. "
            "WebTransport is NEVER inferred from the URL — request it "
            "explicitly."
        ),
    )
    heartbeat: float | None = Field(
        default=None,
        gt=0,
        description=(
            "Python-only: aiohttp WebSocket heartbeat interval in seconds "
            "(ping/pong keepalive)"
        ),
    )
    
    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format."""
        if not v:
            raise ValueError("URL cannot be empty")
        
        valid_schemes = ("ws://", "wss://", "http://", "https://")
        if not any(v.startswith(scheme) for scheme in valid_schemes):
            raise ValueError(
                f"URL must start with one of: {', '.join(valid_schemes)}"
            )
        return v


class WebSocketServerConfig(BaseModel):
    """Configuration for WebSocket RPC server.
    
    Attributes:
        host: Host to bind to
        port: Port to bind to
        path: WebSocket endpoint path
        local_main_factory: Factory function to create per-connection capability
        session_options: Optional session configuration
    """
    
    model_config = ConfigDict(
        frozen=False,
        arbitrary_types_allowed=True,
    )
    
    host: str = Field(default="0.0.0.0", description="Host to bind to")
    port: int = Field(default=8080, gt=0, le=65535, description="Port to bind to")
    path: str = Field(default="/rpc", description="WebSocket endpoint path")
    local_main_factory: Callable[[], Any] | None = Field(
        default=None,
        description="Factory to create per-connection capability"
    )
    options: RpcSessionConfig | None = None


class BatchRpcConfig(BaseModel):
    """Configuration for HTTP batch RPC.
    
    Attributes:
        url: The batch RPC endpoint URL
        timeout: Request timeout in seconds
        local_main: Optional capability to expose to server
        session_options: Optional session configuration
    """
    
    model_config = ConfigDict(
        frozen=False,
        arbitrary_types_allowed=True,
    )
    
    url: str = Field(..., description="Batch RPC endpoint URL")
    timeout: float = Field(default=30.0, gt=0, description="Request timeout")
    local_main: Any | None = None
    options: RpcSessionConfig | None = None
    
    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format for batch RPC (HTTP only)."""
        if not v:
            raise ValueError("URL cannot be empty")
        
        valid_schemes = ("http://", "https://")
        if not any(v.startswith(scheme) for scheme in valid_schemes):
            raise ValueError(
                f"Batch RPC URL must start with one of: {', '.join(valid_schemes)}"
            )
        return v


# Backwards compatibility aliases
UnifiedClientConfig = ClientConfig
RpcSessionOptions = RpcSessionConfig
