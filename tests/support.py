"""Shared test helpers.

``rpc_call`` replaces the deleted positional ``client.call(cap_id, method,
args)`` API (removed in parity Phase C — TS has no such API; the stub IS the
API). Tests that exercised the positional surface now go through the main
stub exactly like application code.
"""

from __future__ import annotations

from typing import Any


async def rpc_call(client: Any, method: str, args: list[Any] | None = None) -> Any:
    """Call ``method`` on the peer's main capability via the stub API.

    ``client`` is anything with ``get_main_stub()`` (WebSocketRpcClient,
    UnifiedClient in websocket mode, or a raw session wrapper).
    """
    stub = client.get_main_stub()
    return await getattr(stub, method)(*(args or []))
