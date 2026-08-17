import asyncio
import hashlib
import json
import os
import random
import secrets

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as aioredis

app = FastAPI()

# Redis persistent database connection
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

HOUSE_EDGE = 0.03
PASS_FEE = 5.0

game_state = {
    "phase": "lobby",
    "players": [],
    "pot": 0.0,
    "current_holder": None,
    "multiplier": 1.0,
    "crash_point": 0.0,
    "hashed_seed": "",
    "server_seed": "",
}


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        self.active_connections[user_id] = websocket

    def disconnect(self, user_id: str):
        if user_id in self.active_connections:
            del self.active_connections[user_id]

    async def broadcast(self, message: dict):
        for connection in self.active_connections.values():
            try:
                await connection.send_json(message)
            except Exception:
                pass


manager = ConnectionManager()


def generate_crash():
    seed = secrets.token_hex(32)
    h = hashlib.sha256(seed.encode()).hexdigest()
    n = int(h[:8], 16)
    crash = max(1.00, (2**32 / (n + 1)) * (1.0 - HOUSE_EDGE))
    return seed, h, round(crash, 2)


async def tick_bomb():
    try:
        while game_state["phase"] == "running":
            await asyncio.sleep(1.5)
            game_state["multiplier"] = round(game_state["multiplier"] + 0.25, 2)

            if game_state["multiplier"] >= game_state["crash_point"]:
                game_state["phase"] = "ended"
                await detonate()
                break

            await manager.broadcast({"type": "tick", "state": game_state})
    except asyncio.CancelledError:
        pass


async def detonate():
    loser_id = game_state["current_holder"]
    survivors = [p for p in game_state["players"] if p["id"] != loser_id]

    payout = 0.0
    if survivors:
        payout = round(game_state["pot"] / len(survivors), 2)
        for s in survivors:
            await redis_client.hincrbyfloat("ds:balances", s["id"], payout)

    await manager.broadcast(
        {
            "type": "sploded",
            "loser": loser_id,
            "payout": payout,
            "state": game_state,
        }
    )


@app.websocket("/ws/{user_id}/{user_name}")
async def websocket_endpoint(websocket: WebSocket, user_id: str, user_name: str):
    await manager.connect(websocket, user_id)
    await websocket.send_json({"type": "welcome", "state": game_state})

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "join" and game_state["phase"] == "lobby":
                bal = float(await redis_client.hget("ds:balances", user_id) or 500.0)
                if bal >= 100.0 and not any(
                    p["id"] == user_id for p in game_state["players"]
                ):
                    await redis_client.hincrbyfloat("ds:balances", user_id, -100.0)
                    game_state["pot"] += 100.0
                    game_state["players"].append({"id": user_id, "name": user_name})
                    await manager.broadcast({"type": "update", "state": game_state})

            elif (
                action == "force_start"
                and game_state["phase"] == "lobby"
                and len(game_state["players"]) >= 2
            ):
                game_state["phase"] = "running"
                (
                    game_state["server_seed"],
                    game_state["hashed_seed"],
                    game_state["crash_point"],
                ) = generate_crash()
                game_state["multiplier"] = 1.0
                game_state["current_holder"] = random.choice(game_state["players"])["id"]

                await manager.broadcast({"type": "start", "state": game_state})
                asyncio.create_task(tick_bomb())

            elif action == "pass" and game_state["phase"] == "running":
                if game_state["current_holder"] == user_id:
                    bal = float(await redis_client.hget("ds:balances", user_id) or 0.0)
                    if bal >= PASS_FEE:
                        await redis_client.hincrbyfloat(
                            "ds:balances", user_id, -PASS_FEE
                        )
                        game_state["pot"] += PASS_FEE

                        idx = next(
                            (
                                i
                                for i, p in enumerate(game_state["players"])
                                if p["id"] == user_id
                            ),
                            0,
                        )
                        next_idx = (idx + 1) % len(game_state["players"])
                        game_state["current_holder"] = game_state["players"][next_idx]["id"]

                        await manager.broadcast({"type": "update", "state": game_state})

    except WebSocketDisconnect:
        manager.disconnect(user_id)
