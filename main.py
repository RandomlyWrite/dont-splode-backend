import asyncio
import hashlib
import hmac
import json
import math
import os
import random
import secrets
from urllib import request as url_request

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
import redis.asyncio as aioredis

app = FastAPI()

# Redis is the authoritative balance ledger; the browser never supplies a chip balance.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

HOUSE_EDGE = 0.03
PASS_FEE = 5.0
JOIN_COST = 100.0
DEFAULT_BALANCE = 500.0
JOIN_COOLDOWN_SECONDS = 1.5
PASS_COOLDOWN_SECONDS = 0.65
BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "dontsplodebot").lstrip("@")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
PUBLIC_BACKEND_URL = "https://dont-splode-backend.onrender.com"

game_state = {
    "phase": "lobby",
    "players": [],
    "pot": 0.0,
    "current_holder": None,
    "multiplier": 1.0,
    "crash_point": 0.0,
    "hashed_seed": "",
    "server_seed": "",
    "latest_round": None,
}
reset_task = None


async def get_balance(user_id: str) -> float:
    """Return a validated, persistent server-side balance for one player."""
    raw_balance = await redis_client.hget("ds:balances", user_id)
    if raw_balance is None:
        await redis_client.hset("ds:balances", user_id, DEFAULT_BALANCE)
        return DEFAULT_BALANCE

    try:
        balance = float(raw_balance)
    except (TypeError, ValueError):
        balance = DEFAULT_BALANCE

    # Negative values could only have come from the prior uninitialized-balance
    # path, so repair them rather than leaving a new player permanently locked out.
    if not math.isfinite(balance) or balance < 0:
        balance = DEFAULT_BALANCE
        await redis_client.hset("ds:balances", user_id, balance)

    return round(balance, 2)


async def change_balance(user_id: str, amount: float) -> float:
    """Apply a server-authorized balance change and return the rounded result."""
    updated = await redis_client.hincrbyfloat("ds:balances", user_id, amount)
    return round(float(updated), 2)


async def claim_action_slot(user_id: str, action: str, cooldown_seconds: float) -> bool:
    """Atomically reserve a server-enforced action slot for the given cooldown."""
    key = f"ds:cooldowns:{action}:{user_id}"
    acquired = await redis_client.set(
        key,
        "1",
        nx=True,
        px=max(1, int(cooldown_seconds * 1000)),
    )
    return bool(acquired)


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        self.active_connections[user_id] = websocket

    def disconnect(self, user_id: str):
        if user_id in self.active_connections:
            del self.active_connections[user_id]

    async def send_state(self, user_id: str, event_type: str, **extra):
        connection = self.active_connections.get(user_id)
        if connection is None:
            return
        try:
            await connection.send_json(
                {
                    "type": event_type,
                    "state": game_state,
                    "balance": await get_balance(user_id),
                    **extra,
                }
            )
        except Exception:
            pass

    async def broadcast_state(self, event_type: str, **extra):
        for user_id in list(self.active_connections):
            await self.send_state(user_id, event_type, **extra)


manager = ConnectionManager()


def generate_crash():
    seed = secrets.token_hex(32)
    hashed = hashlib.sha256(seed.encode()).hexdigest()
    number = int(hashed[:8], 16)
    crash = max(1.00, (2**32 / (number + 1)) * (1.0 - HOUSE_EDGE))
    return seed, hashed, round(crash, 2)


def current_lobby_card() -> dict:
    """Build the public, visual announcement Telegram inserts into a selected chat."""
    players = game_state["players"]
    player_lines = "\n".join(
        f"{index}. {player['name']}" for index, player in enumerate(players, start=1)
    ) or "No victims have signed the waiver yet."
    phase = game_state["phase"]
    heading = "LOBBY OPEN" if phase == "lobby" else "ROUND IN PROGRESS"
    text = (
        "💣 <b>DON'T SPLODE</b> 💣\n"
        "━━━━━━━━━━━━\n\n"
        f"<b>{heading}</b>\n\n"
        "Buy-in: <b>100 ◉</b>\n"
        "Pass fee: <b>5 ◉</b> (bled into the pot)\n\n"
        f"<b>Players ({len(players)}/12)</b>\n"
        f"{player_lines}\n\n"
        f"<b>Pot: {game_state['pot']:.0f} ◉</b>\n\n"
        "<i>Click Join. Everything lives in this one block.</i>"
    )
    return {
        "type": "article",
        "id": "dont-splode-lobby",
        "title": "DON'T SPLODE — Lobby Card",
        "description": "Send a live lobby card with a Join button.",
        "input_message_content": {
            "message_text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        "reply_markup": {
            "inline_keyboard": [
                [
                    {
                        "text": "JOIN THE LOBBY — 100 ◉",
                        "url": f"https://t.me/{BOT_USERNAME}?startapp=join",
                    }
                ]
            ]
        },
    }


async def register_telegram_webhook() -> None:
    """Register only the inline-query callback without printing credentials."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_WEBHOOK_SECRET:
        return

    endpoint = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
    payload = json.dumps(
        {
            "url": f"{PUBLIC_BACKEND_URL}/telegram/webhook",
            "secret_token": TELEGRAM_WEBHOOK_SECRET,
            "allowed_updates": ["inline_query"],
            "drop_pending_updates": False,
        }
    ).encode("utf-8")

    def post_webhook_registration() -> None:
        request = url_request.Request(
            endpoint,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with url_request.urlopen(request, timeout=12) as response:
            result = json.loads(response.read().decode("utf-8"))
        if not result.get("ok"):
            raise RuntimeError("Telegram rejected webhook registration")

    try:
        await asyncio.to_thread(post_webhook_registration)
    except Exception:
        # Keep gameplay alive if Telegram is temporarily unavailable.
        print("Telegram webhook registration deferred")


@app.on_event("startup")
async def configure_telegram_bot() -> None:
    await register_telegram_webhook()


@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
):
    """Answer only Telegram inline queries; all game actions remain WebSocket-owned."""
    if not TELEGRAM_WEBHOOK_SECRET or not hmac.compare_digest(
        x_telegram_bot_api_secret_token or "", TELEGRAM_WEBHOOK_SECRET
    ):
        raise HTTPException(status_code=403, detail="Invalid Telegram webhook secret")

    update = await request.json()
    inline_query = update.get("inline_query")
    if not inline_query:
        return {"ok": True}

    return {
        "method": "answerInlineQuery",
        "inline_query_id": inline_query["id"],
        "results": [current_lobby_card()],
        "cache_time": 0,
        "is_personal": False,
    }


def reset_round_state():
    """Clear only transient game state while preserving Redis-backed balances."""
    game_state.update(
        {
            "phase": "lobby",
            "players": [],
            "pot": 0.0,
            "current_holder": None,
            "multiplier": 1.0,
            "crash_point": 0.0,
            "hashed_seed": "",
            "server_seed": "",
        }
    )


async def reset_lobby_after_cooldown():
    """Keep an incident visible for five seconds, then open the next lobby."""
    global reset_task
    try:
        await asyncio.sleep(5)
        if game_state["phase"] == "ended":
            reset_round_state()
            await manager.broadcast_state("reset")
    finally:
        reset_task = None


def schedule_lobby_reset():
    global reset_task
    if reset_task is None or reset_task.done():
        reset_task = asyncio.create_task(reset_lobby_after_cooldown())


async def tick_bomb():
    try:
        while game_state["phase"] == "running":
            await asyncio.sleep(1.5)
            game_state["multiplier"] = round(game_state["multiplier"] + 0.25, 2)

            if game_state["multiplier"] >= game_state["crash_point"]:
                game_state["phase"] = "ended"
                await detonate()
                break

            await manager.broadcast_state("tick")
    except asyncio.CancelledError:
        pass


async def detonate():
    loser_id = game_state["current_holder"]
    survivors = [player for player in game_state["players"] if player["id"] != loser_id]

    payout = 0.0
    if survivors:
        payout = round(game_state["pot"] / len(survivors), 2)
        for survivor in survivors:
            await change_balance(survivor["id"], payout)

    # Retain a compact, identity-free ticket after the lobby reopens. Names,
    # user IDs, seeds, and balances are intentionally excluded.
    game_state["latest_round"] = {
        "multiplier": round(game_state["multiplier"], 2),
        "payout": payout,
        "survivor_count": len(survivors),
    }

    await manager.broadcast_state("sploded", loser=loser_id, payout=payout)
    schedule_lobby_reset()


@app.websocket("/ws/{user_id}/{user_name}")
async def websocket_endpoint(websocket: WebSocket, user_id: str, user_name: str):
    await manager.connect(websocket, user_id)
    await manager.send_state(user_id, "welcome")

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "join" and game_state["phase"] == "lobby":
                balance = await get_balance(user_id)
                if balance >= JOIN_COST and not any(
                    player["id"] == user_id for player in game_state["players"]
                ):
                    if not await claim_action_slot(
                        user_id, "join", JOIN_COOLDOWN_SECONDS
                    ):
                        continue
                    await change_balance(user_id, -JOIN_COST)
                    game_state["pot"] += JOIN_COST
                    game_state["players"].append({"id": user_id, "name": user_name})
                    await manager.broadcast_state("update")

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

                await manager.broadcast_state("start")
                asyncio.create_task(tick_bomb())

            elif action == "pass" and game_state["phase"] == "running":
                if game_state["current_holder"] == user_id:
                    balance = await get_balance(user_id)
                    if balance >= PASS_FEE:
                        if not await claim_action_slot(
                            user_id, "pass", PASS_COOLDOWN_SECONDS
                        ):
                            continue
                        await change_balance(user_id, -PASS_FEE)
                        game_state["pot"] += PASS_FEE

                        current_index = next(
                            (
                                index
                                for index, player in enumerate(game_state["players"])
                                if player["id"] == user_id
                            ),
                            0,
                        )
                        next_index = (current_index + 1) % len(game_state["players"])
                        game_state["current_holder"] = game_state["players"][next_index]["id"]

                        await manager.broadcast_state("update")

    except WebSocketDisconnect:
        manager.disconnect(user_id)
