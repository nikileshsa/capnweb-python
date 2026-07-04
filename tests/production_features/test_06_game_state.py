"""Example 6: Game State with Player Callbacks

Demonstrates:
- Player join/leave with state callbacks
- State broadcast to multiple players
- Concurrent player actions
- Stats tracking
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from typing import Any

from capnweb import RpcTarget, RpcError
from capnweb.ws_session import WebSocketRpcClient, WebSocketRpcServer
from ..support import rpc_call


@dataclass
class GameServer(RpcTarget):
    """Game server with player state management."""
    
    players: dict[str, dict] = field(default_factory=dict)
    game_state: dict = field(default_factory=lambda: {"tick": 0, "objects": []})
    broadcast_count: int = 0
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "join":
                return self._join(args[0], args[1])
            case "leave":
                return self._leave(args[0])
            case "action":
                return await self._action(args[0], args[1])
            case "get_state":
                return self._get_state()
            case "get_stats":
                return self._get_stats()
            case _:
                raise RpcError.not_found(f"Method {method} not found")
    
    def _join(self, player_id: str, callback: Any) -> dict:
        self.players[player_id] = {"callback": callback, "position": [0, 0]}
        return {"player_id": player_id, "state": self.game_state}
    
    def _leave(self, player_id: str) -> dict:
        if player_id in self.players:
            del self.players[player_id]
            return {"left": True}
        return {"left": False}
    
    async def _action(self, player_id: str, action: dict) -> dict:
        if player_id not in self.players:
            raise RpcError.not_found(f"Player {player_id} not found")
        
        # Update player position
        if action.get("type") == "move":
            self.players[player_id]["position"] = action.get("position", [0, 0])
        
        # Broadcast to other players
        self.broadcast_count += 1
        for pid, pdata in self.players.items():
            if pid != player_id:
                try:
                    await pdata["callback"].on_state_update({
                        "player": player_id,
                        "action": action,
                    })
                except Exception:
                    pass
        
        return {"processed": True}
    
    def _get_state(self) -> dict:
        return {
            "game_state": self.game_state,
            "players": {k: v["position"] for k, v in self.players.items()},
        }
    
    def _get_stats(self) -> dict:
        return {
            "player_count": len(self.players),
            "broadcast_count": self.broadcast_count,
        }


@dataclass
class PlayerCallback(RpcTarget):
    """Client-side player callback."""
    
    name: str = "player"
    updates: list = field(default_factory=list)
    
    async def call(self, method: str, args: list[Any]) -> Any:
        match method:
            case "on_state_update":
                self.updates.append(args[0])
                return {"received": True}
            case _:
                raise RpcError.not_found(f"Method {method} not found")


_port = 9600


def get_port():
    global _port
    _port += 1
    return _port


@pytest.mark.asyncio
class TestGameState:
    
    async def test_join_and_get_state(self):
        """ASSERTION: Players can join and get game state."""
        port = get_port()
        game = GameServer()
        server = WebSocketRpcServer(game, port=port)
        await server.start()
        
        try:
            callback = PlayerCallback(name="player1")
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=callback) as client:
                result = await rpc_call(client, "join", ["player1", callback])
                assert result["player_id"] == "player1"
                
                state = await rpc_call(client, "get_state", [])
                assert "player1" in state["players"]
        finally:
            await server.stop()
    
    async def test_state_broadcast_to_other_players(self):
        """ASSERTION: Actions broadcast to other players."""
        port = get_port()
        game = GameServer()
        server = WebSocketRpcServer(game, port=port)
        await server.start()
        
        try:
            cb1 = PlayerCallback(name="player1")
            cb2 = PlayerCallback(name="player2")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=cb1) as client1:
                async with WebSocketRpcClient(url, local_main=cb2) as client2:
                    await rpc_call(client1, "join", ["player1", cb1])
                    await rpc_call(client2, "join", ["player2", cb2])
                    
                    await rpc_call(client1, "action", ["player1", {"type": "move", "position": [10, 20]}])
                    await asyncio.sleep(0.1)
                    
                    # Player2 should receive the update
                    assert len(cb2.updates) == 1
                    assert cb2.updates[0]["player"] == "player1"
        finally:
            await server.stop()
    
    async def test_player_leave(self):
        """ASSERTION: Players can leave the game."""
        port = get_port()
        game = GameServer()
        server = WebSocketRpcServer(game, port=port)
        await server.start()
        
        try:
            callback = PlayerCallback(name="player1")
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=callback) as client:
                await rpc_call(client, "join", ["player1", callback])
                result = await rpc_call(client, "leave", ["player1"])
                assert result["left"] == True
                
                state = await rpc_call(client, "get_state", [])
                assert "player1" not in state["players"]
        finally:
            await server.stop()
    
    async def test_concurrent_player_actions(self):
        """ASSERTION: Concurrent actions are handled correctly."""
        port = get_port()
        game = GameServer()
        server = WebSocketRpcServer(game, port=port)
        await server.start()
        
        try:
            callback = PlayerCallback(name="player1")
            url = f"ws://localhost:{port}/rpc"
            async with WebSocketRpcClient(url, local_main=callback) as client:
                await rpc_call(client, "join", ["player1", callback])
                
                tasks = [
                    asyncio.create_task(rpc_call(client, "action", ["player1", {"type": "move", "position": [i, i]}]))
                    for i in range(5)
                ]
                results = await asyncio.gather(*tasks)
                assert len(results) == 5
                assert all(r["processed"] for r in results)
        finally:
            await server.stop()
    
    async def test_stats_tracking(self):
        """ASSERTION: Stats are tracked correctly."""
        port = get_port()
        game = GameServer()
        server = WebSocketRpcServer(game, port=port)
        await server.start()
        
        try:
            cb1 = PlayerCallback(name="player1")
            cb2 = PlayerCallback(name="player2")
            url = f"ws://localhost:{port}/rpc"
            
            async with WebSocketRpcClient(url, local_main=cb1) as client1:
                async with WebSocketRpcClient(url, local_main=cb2) as client2:
                    await rpc_call(client1, "join", ["player1", cb1])
                    await rpc_call(client2, "join", ["player2", cb2])
                    
                    await rpc_call(client1, "action", ["player1", {}])
                    await rpc_call(client2, "action", ["player2", {}])
                    
                    stats = await rpc_call(client1, "get_stats", [])
                    assert stats["player_count"] == 2
                    assert stats["broadcast_count"] == 2
        finally:
            await server.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
