import asyncio
import hashlib
import hmac
import json
import math
import os
import random
import re
import secrets
import time
from html import escape
from io import BytesIO
from pathlib import Path
from urllib import request as url_request
from urllib.parse import parse_qsl

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from PIL import Image, ImageDraw, ImageFilter, ImageFont
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
ROUND_TICK_SECONDS = 1.5
MINIMUM_CRASH_MULTIPLIER = 2.25
ELIMINATION_INTERMISSION_SECONDS = 3
FINAL_LOBBY_RESET_SECONDS = 5
MAX_PLAYERS = 12
MINIMUM_COUNTDOWN_PLAYERS = 3
LOBBY_AUTO_IGNITE_SECONDS = 45
DAILY_CHIP_GRANT = 250.0
DAILY_CLAIM_COOLDOWN_SECONDS = 24 * 60 * 60
PIT_BOSS_DEFAULT_GRANT = 100.0
PIT_BOSS_MIN_GRANT = 1.0
PIT_BOSS_MAX_GRANT = 10000.0
PIT_BOSS_GRANT_COOLDOWN_SECONDS = 1.0
PIT_BOSS_IDS = {
    value.strip()
    for value in os.getenv("PIT_BOSS_USER_IDS", "").split(",")
    if value.strip()
}
BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "dontsplodebot").lstrip("@")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
PUBLIC_BACKEND_URL = "https://dont-splode-backend.onrender.com"
ACTIVE_LOBBY_CARDS_KEY = "ds:active_lobby_cards"
ELIMINATION_MEDIA_CARDS_KEY = "ds:elimination_media_cards"
POSTER_WIDTH = 1080
POSTER_HEIGHT = 1350
POSTER_TTL_SECONDS = 20 * 60
ASSET_DIR = Path(__file__).resolve().parent / "assets"
POSTER_FONT_PATH = ASSET_DIR / "DejaVuSans-Bold.ttf"
POSTER_MASCOT_PATH = ASSET_DIR / "hands-of-calamity.png"
poster_cache: dict[str, tuple[bytes, float]] = {}

game_state = {
    "phase": "lobby",
    "players": [],
    "eliminated_players": [],
    "pot": 0.0,
    "current_holder": None,
    "multiplier": 1.0,
    "crash_point": 0.0,
    "hashed_seed": "",
    "server_seed": "",
    "round_number": 0,
    "latest_round": None,
    "ready_players": [],
    "lobby_auto_start_at": 0.0,
}
reset_task = None
intermission_task = None
lobby_ignition_task = None


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


async def daily_claim_status(user_id: str) -> dict:
    """Return server-owned availability and seconds until the next virtual chip claim."""
    ttl = await redis_client.ttl(f"ds:daily_claims:{user_id}")
    if ttl is None or ttl < 0:
        return {"available": True, "seconds_until": 0, "amount": DAILY_CHIP_GRANT}
    return {"available": False, "seconds_until": int(ttl), "amount": DAILY_CHIP_GRANT}


def verified_telegram_user(init_data: str) -> dict | None:
    """Return verified Telegram user data without ever trusting the URL path identity."""
    if not TELEGRAM_BOT_TOKEN or not init_data:
        return None
    try:
        fields = dict(parse_qsl(init_data, keep_blank_values=True))
        supplied_hash = fields.pop("hash")
        auth_date = int(fields.get("auth_date", "0"))
        if abs(time.time() - auth_date) > 24 * 60 * 60:
            return None
        data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(fields.items()))
        secret_key = hmac.new(b"WebAppData", TELEGRAM_BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calculated_hash, supplied_hash):
            return None
        user = json.loads(fields["user"])
        return user if isinstance(user, dict) and user.get("id") is not None else None
    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
        return None


def verified_telegram_user_id(init_data: str) -> str | None:
    """Return only the verified Telegram ID for privilege checks."""
    user = verified_telegram_user(init_data)
    return str(user["id"]) if user else None


def public_player_label(player: dict | None) -> str:
    """Prefer a verified Telegram handle; otherwise use the existing public lobby name."""
    player = player or {}
    handle = str(player.get("public_handle") or "").strip()
    if handle:
        return f"@{handle.lstrip('@')[:32]}"
    fallback = str(player.get("name") or "UNKNOWN VICTIM").strip()
    return fallback[:36] or "UNKNOWN VICTIM"


def _poster_font(size: int):
    try:
        return ImageFont.truetype(str(POSTER_FONT_PATH), size=size)
    except OSError:
        return ImageFont.load_default()


def _fit_poster_text(draw: ImageDraw.ImageDraw, text: str, max_width: int, start_size: int):
    for size in range(start_size, 28, -2):
        font = _poster_font(size)
        if draw.textbbox((0, 0), text, font=font)[2] <= max_width:
            return font
    return _poster_font(28)


def _center_poster_text(draw: ImageDraw.ImageDraw, text: str, y: int, font, fill: str, *, stroke: int = 0, stroke_fill: str = "#0e0d0b"):
    bounds = draw.textbbox((0, 0), text, font=font, stroke_width=stroke)
    x = (POSTER_WIDTH - (bounds[2] - bounds[0])) // 2
    draw.text((x, y), text, font=font, fill=fill, stroke_width=stroke, stroke_fill=stroke_fill)


def render_elimination_poster(loser: dict | None, survivor_count: int) -> bytes:
    """Render a compact public image card; no balances, IDs, seeds, or private metadata enter it."""
    public_label = public_player_label(loser)
    multiplier = round(float(game_state["multiplier"]), 2)
    image = Image.new("RGB", (POSTER_WIDTH, POSTER_HEIGHT), "#11100e")
    draw = ImageDraw.Draw(image)

    # Brass cabinet body, coal-enamel inset, and intentional soot texture.
    draw.rectangle((0, 0, POSTER_WIDTH, POSTER_HEIGHT), fill="#100f0d")
    draw.rectangle((26, 26, POSTER_WIDTH - 26, POSTER_HEIGHT - 26), fill="#56421f", outline="#c99c4d", width=10)
    draw.rectangle((62, 62, POSTER_WIDTH - 62, POSTER_HEIGHT - 62), fill="#171511", outline="#0b0a08", width=18)
    for x, y in ((48, 48), (POSTER_WIDTH - 48, 48), (48, POSTER_HEIGHT - 48), (POSTER_WIDTH - 48, POSTER_HEIGHT - 48)):
        draw.ellipse((x - 10, y - 10, x + 10, y + 10), fill="#231a0d", outline="#e2bb67", width=3)

    randomizer = random.Random(f"{public_label}:{multiplier}:{survivor_count}")
    soot = Image.new("RGBA", image.size, (0, 0, 0, 0))
    soot_draw = ImageDraw.Draw(soot)
    for _ in range(90):
        x = randomizer.randrange(70, POSTER_WIDTH - 70)
        y = randomizer.randrange(80, POSTER_HEIGHT - 80)
        radius = randomizer.randrange(4, 18)
        soot_draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=(7, 6, 4, randomizer.randrange(8, 28)))
    image = Image.alpha_composite(image.convert("RGBA"), soot.filter(ImageFilter.GaussianBlur(3)))
    draw = ImageDraw.Draw(image)

    small = _poster_font(26)
    headline = _fit_poster_text(draw, f"OPE, {public_label.upper()}", 890, 86)
    subhead = _poster_font(84)
    _center_poster_text(draw, "CABINET CASUALTY REPORT", 95, small, "#e1b861")
    _center_poster_text(draw, f"OPE, {public_label.upper()}", 144, headline, "#f04a40", stroke=4, stroke_fill="#220b09")
    _center_poster_text(draw, "SPLODED!", 246, subhead, "#f04a40", stroke=5, stroke_fill="#220b09")
    draw.line((126, 354, POSTER_WIDTH - 126, 354), fill="#c59a4d", width=3)

    if POSTER_MASCOT_PATH.exists():
        with Image.open(POSTER_MASCOT_PATH).convert("RGBA") as mascot_source:
            mascot = mascot_source.copy()
        mascot.thumbnail((770, 650), Image.Resampling.LANCZOS)
        mascot_x = (POSTER_WIDTH - mascot.width) // 2
        mascot_y = 355
        darkened = mascot.copy()
        black_layer = Image.new("RGBA", darkened.size, (0, 0, 0, 105))
        darkened = Image.alpha_composite(darkened, black_layer)
        image.alpha_composite(darkened, (mascot_x, mascot_y))
        eye_y = mascot_y + int(mascot.height * 0.37)
        eye_gap = int(mascot.width * 0.16)
        eye_x = POSTER_WIDTH // 2
        for offset in (-eye_gap, eye_gap):
            cx = eye_x + offset
            draw.line((cx - 35, eye_y - 35, cx + 35, eye_y + 35), fill="#fff7dd", width=17)
            draw.line((cx + 35, eye_y - 35, cx - 35, eye_y + 35), fill="#fff7dd", width=17)
    else:
        draw.ellipse((310, 410, 770, 870), fill="#2a2822", outline="#b68e47", width=10)
        for offset in (-95, 95):
            cx = POSTER_WIDTH // 2 + offset
            draw.line((cx - 38, 570 - 38, cx + 38, 570 + 38), fill="#fff7dd", width=18)
            draw.line((cx + 38, 570 - 38, cx - 38, 570 + 38), fill="#fff7dd", width=18)

    # Shattered-glass cracks are decorative; their placement never obscures the public result.
    for _ in range(17):
        edge = randomizer.choice(("left", "right", "top", "bottom"))
        if edge == "left":
            start = (75, randomizer.randrange(90, POSTER_HEIGHT - 90))
        elif edge == "right":
            start = (POSTER_WIDTH - 75, randomizer.randrange(90, POSTER_HEIGHT - 90))
        elif edge == "top":
            start = (randomizer.randrange(90, POSTER_WIDTH - 90), 75)
        else:
            start = (randomizer.randrange(90, POSTER_WIDTH - 90), POSTER_HEIGHT - 75)
        end = (start[0] + randomizer.randrange(-180, 181), start[1] + randomizer.randrange(-180, 181))
        draw.line((start, end), fill=(213, 213, 202, 115), width=3)

    draw.rounded_rectangle((112, 1005, POSTER_WIDTH - 112, 1195), radius=18, fill="#0d0c0a", outline="#b58a45", width=4)
    stats_label = _poster_font(25)
    stats_value = _poster_font(49)
    draw.text((158, 1045), "CRASH POINT", font=stats_label, fill="#d2b574")
    draw.text((158, 1085), f"{multiplier:.2f}×", font=stats_value, fill="#f1d07b")
    draw.text((600, 1045), "SOULS WITH A PULSE", font=stats_label, fill="#d2b574")
    draw.text((600, 1085), str(max(0, survivor_count)), font=stats_value, fill="#9ce58c")
    _center_poster_text(draw, "THE FUSE ACCEPTED YOUR DONATION.", 1240, _poster_font(23), "#c9b581")

    buffer = BytesIO()
    image.convert("RGB").save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def cache_elimination_poster(loser: dict | None, survivor_count: int) -> str:
    """Store a short-lived card image until Telegram fetches the remote media URL."""
    now = time.time()
    expired = [key for key, (_, expires_at) in poster_cache.items() if expires_at <= now]
    for key in expired:
        poster_cache.pop(key, None)
    poster_key = secrets.token_urlsafe(12)
    poster_cache[poster_key] = (render_elimination_poster(loser, survivor_count), now + POSTER_TTL_SECONDS)
    return poster_key


def render_lobby_poster() -> bytes:
    """Render the first photo card so Telegram can later replace its media in place."""
    image = Image.new("RGBA", (POSTER_WIDTH, POSTER_HEIGHT), "#11100e")
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, POSTER_WIDTH, POSTER_HEIGHT), fill="#100f0d")
    draw.rectangle((26, 26, POSTER_WIDTH - 26, POSTER_HEIGHT - 26), fill="#56421f", outline="#c99c4d", width=10)
    draw.rectangle((62, 62, POSTER_WIDTH - 62, POSTER_HEIGHT - 62), fill="#171511", outline="#0b0a08", width=18)
    for x, y in ((48, 48), (POSTER_WIDTH - 48, 48), (48, POSTER_HEIGHT - 48), (POSTER_WIDTH - 48, POSTER_HEIGHT - 48)):
        draw.ellipse((x - 10, y - 10, x + 10, y + 10), fill="#231a0d", outline="#e2bb67", width=3)
    _center_poster_text(draw, "GROUP GAME // VIRTUAL CHIPS", 95, _poster_font(26), "#e1b861")
    _center_poster_text(draw, "DON'T", 142, _poster_font(108), "#f1d07b", stroke=4, stroke_fill="#25180d")
    _center_poster_text(draw, "SPLODE", 263, _poster_font(108), "#f1d07b", stroke=4, stroke_fill="#25180d")
    if POSTER_MASCOT_PATH.exists():
        with Image.open(POSTER_MASCOT_PATH).convert("RGBA") as mascot_source:
            mascot = mascot_source.copy()
        mascot.thumbnail((780, 720), Image.Resampling.LANCZOS)
        image.alpha_composite(mascot, ((POSTER_WIDTH - mascot.width) // 2, 356))
    else:
        draw.ellipse((310, 440, 770, 900), fill="#2a2822", outline="#b68e47", width=10)
    draw.rounded_rectangle((118, 1080, POSTER_WIDTH - 118, 1195), radius=18, fill="#0d0c0a", outline="#b58a45", width=4)
    _center_poster_text(draw, "SIGN THE WAIVER. HOLD LIGHT IT UP.", 1120, _poster_font(32), "#f04a40")
    _center_poster_text(draw, "THE FUSE DOES NOT RESPECT HESITATION.", 1242, _poster_font(23), "#c9b581")
    buffer = BytesIO()
    image.convert("RGB").save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.pit_boss_connections: set[str] = set()

    async def connect(self, websocket: WebSocket, user_id: str, is_pit_boss: bool = False):
        await websocket.accept()
        previous_connection = self.active_connections.get(user_id)
        self.active_connections[user_id] = websocket
        if is_pit_boss:
            self.pit_boss_connections.add(user_id)
        else:
            self.pit_boss_connections.discard(user_id)
        if previous_connection is not None and previous_connection is not websocket:
            try:
                await previous_connection.close(code=4001, reason="A newer game session replaced this one.")
            except Exception:
                pass

    def disconnect(self, user_id: str, websocket: WebSocket | None = None):
        if websocket is None or self.active_connections.get(user_id) is websocket:
            del self.active_connections[user_id]
            self.pit_boss_connections.discard(user_id)

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
                    "daily_claim": await daily_claim_status(user_id),
                    "pit_boss": user_id in self.pit_boss_connections,
                    "pit_boss_grant": (
                        {
                            "default": PIT_BOSS_DEFAULT_GRANT,
                            "min": PIT_BOSS_MIN_GRANT,
                            "max": PIT_BOSS_MAX_GRANT,
                        }
                        if user_id in self.pit_boss_connections
                        else None
                    ),
                    **extra,
                }
            )
        except Exception:
            pass

    async def broadcast_state(self, event_type: str, **extra):
        for user_id in list(self.active_connections):
            await self.send_state(user_id, event_type, **extra)


manager = ConnectionManager()


async def reject_action(user_id: str, reason: str) -> None:
    """Return a current authoritative state plus a human-readable action rejection."""
    await manager.send_state(user_id, "action_rejected", reason=reason)


def generate_crash():
    seed = secrets.token_hex(32)
    hashed = hashlib.sha256(seed.encode()).hexdigest()
    number = int(hashed[:8], 16)
    # A newly lit fuse must allow time for at least several deliberate passes.
    # This floor applies equally to every player and does not trust any client timer.
    crash = max(
        MINIMUM_CRASH_MULTIPLIER,
        (2**32 / (number + 1)) * (1.0 - HOUSE_EDGE),
    )
    return seed, hashed, round(crash, 2)


def current_lobby_card() -> dict:
    """Build the public, visual announcement Telegram inserts into a selected chat."""
    players = game_state["players"]
    ready_players = {str(user_id) for user_id in game_state.get("ready_players", [])}
    player_lines = "\n".join(
        f"{index}. {escape(str(player['name']))} {'<b>— LIT</b>' if str(player['id']) in ready_players else '— waiting'}"
        for index, player in enumerate(players, start=1)
    ) or "No victims have signed the waiver yet."
    phase = game_state["phase"]
    eliminated_count = len(game_state["eliminated_players"])
    heading = (
        "LOBBY OPEN"
        if phase == "lobby"
        else "ASH SETTLING"
        if phase == "intermission"
        else "ROUND IN PROGRESS"
    )
    ready_count = sum(str(player["id"]) in ready_players for player in players)
    countdown_at = float(game_state.get("lobby_auto_start_at") or 0)
    seconds_until_auto = max(0, math.ceil(countdown_at - time.time())) if countdown_at else 0
    footer = (
        (
            f"<i>Hold LIGHT IT UP together ({ready_count}/{len(players)} ready). "
            f"Auto-ignition in {seconds_until_auto}s.</i>"
            if countdown_at
            else f"<i>Hold LIGHT IT UP together ({ready_count}/{len(players)} ready). Full lobby ignites immediately.</i>"
        )
        if phase == "lobby"
        else "<i>One soul just met the fuse. The next round lights shortly.</i>"
        if phase == "intermission"
        else "<i>The fuse is lit. Keep your hands where we can see them.</i>"
    )
    button = (
        {
            "text": "JOIN THE LOBBY — 100 ◉",
            "url": f"https://t.me/{BOT_USERNAME}?startapp=join",
        }
        if phase == "lobby"
        else {
            "text": "🚫 LOBBY SEALED — ASH SETTLING"
            if phase == "intermission"
            else "🚫 LOBBY SEALED — FUSE LIT",
            "callback_data": "lobby_closed",
        }
    )
    text = (
        "💣 <b>DON'T SPLODE</b> 💣\n"
        "━━━━━━━━━━━━\n\n"
        f"<b>{heading}</b>\n\n"
        "Buy-in: <b>100 ◉</b>\n"
        "Pass fee: <b>5 ◉</b> (bled into the pot)\n\n"
        f"<b>Active players ({len(players)}/{MAX_PLAYERS})</b>\n"
        f"{player_lines}\n\n"
        f"<b>Eliminated: {eliminated_count}</b>\n"
        f"<b>Pot: {game_state['pot']:.0f} ◉</b>\n\n"
        f"{footer}"
    )
    return {
        "type": "photo",
        "id": "dont-splode-lobby",
        "title": "DON'T SPLODE — Lobby Card",
        "description": "Send a live lobby card with a Join button.",
        "photo_url": f"{PUBLIC_BACKEND_URL}/telegram/posters/lobby.png",
        "thumbnail_url": f"{PUBLIC_BACKEND_URL}/telegram/posters/lobby.png",
        "caption": text,
        "parse_mode": "HTML",
        "reply_markup": {
            "inline_keyboard": [
                [button]
            ]
        },
    }


def current_round_result_card(payout: float, survivor_count: int) -> tuple[str, dict]:
    """Build a compact public final-match result without private player data."""
    multiplier = round(float(game_state["multiplier"]), 2)
    survivor_label = "SOUL" if survivor_count == 1 else "SOULS"
    text = (
        "💥 <b>DON'T SPLODE — ROUND RESULT</b> 💥\n"
        "━━━━━━━━━━━━\n\n"
        "<b>THE FUSE WON.</b>\n\n"
        f"Crash: <b>{multiplier:.2f}×</b>\n"
        f"Last standing: <b>{survivor_count} {survivor_label}</b>\n"
        f"Final pot: <b>{payout:.2f} ◉</b>\n\n"
        "<i>The cabinet swept up the ash. The next lobby needs fresh volunteers.</i>"
    )
    markup = {
        "inline_keyboard": [
            [
                {
                    "text": "OPEN THE CABINET",
                    "url": f"https://t.me/{BOT_USERNAME}?startapp=join",
                }
            ]
        ]
    }
    return text, markup


def current_elimination_card(loser: dict | None, survivor_count: int) -> tuple[str, dict]:
    """Build the caption and sealed-lobby control for a public elimination poster."""
    victim = escape(public_player_label(loser))
    multiplier = round(float(game_state["multiplier"]), 2)
    soul_label = "SOUL" if survivor_count == 1 else "SOULS"
    text = (
        f"💥 <b>OPE, {victim} SPLODED.</b>\n\n"
        f"Crash: <b>{multiplier:.2f}×</b>\n"
        f"Still breathing: <b>{survivor_count} {soul_label}</b>\n\n"
        "<i>The cabinet has accepted a fresh contribution to the ash pile.</i>"
    )
    markup = {
        "inline_keyboard": [[{"text": "🚫 LOBBY SEALED — ASH SETTLING", "callback_data": "lobby_closed"}]]
    }
    return text, markup


async def telegram_api_call(method: str, payload: dict) -> tuple[bool, dict]:
    """Call Telegram without exposing credentials in logs or browser-visible state."""
    if not TELEGRAM_BOT_TOKEN:
        return False, {}

    endpoint = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    body = json.dumps(payload).encode("utf-8")

    def post() -> dict:
        request = url_request.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with url_request.urlopen(request, timeout=12) as response:
            return json.loads(response.read().decode("utf-8"))

    try:
        result = await asyncio.to_thread(post)
        return bool(result.get("ok")), result
    except Exception:
        return False, {}


async def edit_inline_card(inline_message_id: str, text: str, markup: dict) -> bool:
    """Edit one tracked inline card and report whether Telegram accepted the edit."""
    ok, result = await telegram_api_call(
        "editMessageText",
        {
            "inline_message_id": inline_message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "reply_markup": markup,
        },
    )
    if ok:
        return True

    description = str(result.get("description", ""))
    # The selected inline result already contains the current card. Telegram
    # correctly rejects this first no-op edit, but the identifier is still
    # valid and must remain tracked for the next state-changing update.
    if "message is not modified" in description.lower():
        return True

    print(
        "Telegram inline card edit failed",
        json.dumps(
            {
                "description": description or "transport failure",
                "inline_message_id_length": len(inline_message_id),
            }
        ),
    )
    return False


async def edit_inline_caption(inline_message_id: str, text: str, markup: dict) -> bool:
    """Update an inline photo card caption while preserving its current image."""
    ok, result = await telegram_api_call(
        "editMessageCaption",
        {
            "inline_message_id": inline_message_id,
            "caption": text,
            "parse_mode": "HTML",
            "reply_markup": markup,
        },
    )
    if ok or "message is not modified" in str(result.get("description", "")).lower():
        return True
    print("Telegram inline image caption edit failed", json.dumps({"inline_message_id_length": len(inline_message_id)}))
    return False


async def edit_inline_media(inline_message_id: str, poster_key: str, text: str, markup: dict) -> bool:
    """Replace an existing inline photo card with the public-safe knockout poster."""
    ok, _ = await telegram_api_call(
        "editMessageMedia",
        {
            "inline_message_id": inline_message_id,
            "media": {
                "type": "photo",
                "media": f"{PUBLIC_BACKEND_URL}/telegram/posters/{poster_key}.png",
                "caption": text,
                "parse_mode": "HTML",
            },
            "reply_markup": markup,
        },
    )
    if not ok:
        print("Telegram inline card media edit failed", json.dumps({"inline_message_id_length": len(inline_message_id)}))
    return ok


async def refresh_lobby_cards() -> None:
    """Update every tracked lobby card from public authoritative game state."""
    card_ids = await redis_client.smembers(ACTIVE_LOBBY_CARDS_KEY)
    if not card_ids:
        return
    card = current_lobby_card()
    text = card["caption"]
    markup = card["reply_markup"]
    outcomes = await asyncio.gather(
        *(edit_inline_caption(card_id, text, markup) for card_id in card_ids)
    )
    stale_ids = [card_id for card_id, ok in zip(card_ids, outcomes) if not ok]
    if stale_ids:
        print(f"Removing {len(stale_ids)} stale Telegram inline card(s)")
        await redis_client.srem(ACTIVE_LOBBY_CARDS_KEY, *stale_ids)


async def publish_elimination_poster(loser: dict | None, survivor_count: int) -> None:
    """Transform every tracked group lobby card into the current public knockout poster."""
    card_ids = await redis_client.smembers(ACTIVE_LOBBY_CARDS_KEY)
    if not card_ids:
        return
    poster_key = cache_elimination_poster(loser, survivor_count)
    text, markup = current_elimination_card(loser, survivor_count)
    outcomes = await asyncio.gather(
        *(edit_inline_media(card_id, poster_key, text, markup) for card_id in card_ids)
    )
    stale_ids = [card_id for card_id, ok in zip(card_ids, outcomes) if not ok]
    if stale_ids:
        await redis_client.srem(ACTIVE_LOBBY_CARDS_KEY, *stale_ids)


async def publish_round_results(payout: float, survivor_count: int) -> None:
    """Transform live lobby cards into compact public detonation result cards."""
    card_ids = await redis_client.smembers(ACTIVE_LOBBY_CARDS_KEY)
    if not card_ids:
        return
    text, markup = current_round_result_card(payout, survivor_count)
    await asyncio.gather(
        *(edit_inline_caption(card_id, text, markup) for card_id in card_ids)
    )
    await redis_client.delete(ACTIVE_LOBBY_CARDS_KEY)


async def register_telegram_webhook() -> None:
    """Register only the inline-query callback without printing credentials."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_WEBHOOK_SECRET:
        return

    endpoint = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
    payload = json.dumps(
        {
            "url": f"{PUBLIC_BACKEND_URL}/telegram/webhook",
            "secret_token": TELEGRAM_WEBHOOK_SECRET,
            "allowed_updates": [
                "inline_query",
                "chosen_inline_result",
                "callback_query",
            ],
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
        print("Telegram webhook registered")
    except Exception:
        # Keep gameplay alive if Telegram is temporarily unavailable.
        print("Telegram webhook registration deferred")


@app.on_event("startup")
async def configure_telegram_bot() -> None:
    await register_telegram_webhook()


@app.get("/telegram/posters/{poster_key}.png")
async def telegram_elimination_poster(poster_key: str):
    """Serve a short-lived result image for Telegram's editMessageMedia fetch."""
    if poster_key == "lobby":
        return Response(
            content=render_lobby_poster(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=300"},
        )
    cached = poster_cache.get(poster_key)
    if not cached or cached[1] <= time.time():
        poster_cache.pop(poster_key, None)
        raise HTTPException(status_code=404, detail="Elimination poster has expired")
    return Response(content=cached[0], media_type="image/png", headers={"Cache-Control": "public, max-age=300"})


@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str | None = Header(default=None),
):
    """Answer lobby inline queries and remember selected cards for public updates."""
    if not TELEGRAM_WEBHOOK_SECRET or not hmac.compare_digest(
        x_telegram_bot_api_secret_token or "", TELEGRAM_WEBHOOK_SECRET
    ):
        raise HTTPException(status_code=403, detail="Invalid Telegram webhook secret")

    update = await request.json()
    inline_query = update.get("inline_query")
    if inline_query:
        return {
            "method": "answerInlineQuery",
            "inline_query_id": inline_query["id"],
            "results": [current_lobby_card()],
            "cache_time": 0,
            "is_personal": False,
        }

    chosen_result = update.get("chosen_inline_result") or {}
    inline_message_id = chosen_result.get("inline_message_id")
    if (
        chosen_result.get("result_id") == "dont-splode-lobby"
        and isinstance(inline_message_id, str)
        and inline_message_id
    ):
        await redis_client.sadd(ACTIVE_LOBBY_CARDS_KEY, inline_message_id)
        await refresh_lobby_cards()

    callback_query = update.get("callback_query") or {}
    if callback_query.get("data") == "lobby_closed":
        return {
            "method": "answerCallbackQuery",
            "callback_query_id": callback_query["id"],
            "text": "The fuse is lit. Late entries are incinerated.",
            "show_alert": False,
        }
    return {"ok": True}


def reset_round_state():
    """Clear only transient game state while preserving Redis-backed balances."""
    game_state.update(
        {
            "phase": "lobby",
            "players": [],
            "eliminated_players": [],
            "pot": 0.0,
            "current_holder": None,
            "multiplier": 1.0,
            "crash_point": 0.0,
            "hashed_seed": "",
            "server_seed": "",
            "round_number": 0,
            "ready_players": [],
            "lobby_auto_start_at": 0.0,
        }
    )


async def reset_lobby_after_cooldown():
    """Keep an incident visible for five seconds, then open the next lobby."""
    global reset_task
    try:
        await asyncio.sleep(FINAL_LOBBY_RESET_SECONDS)
        if game_state["phase"] == "ended":
            reset_round_state()
            await manager.broadcast_state("reset")
    finally:
        reset_task = None


def schedule_lobby_reset():
    global reset_task
    if reset_task is None or reset_task.done():
        reset_task = asyncio.create_task(reset_lobby_after_cooldown())


def arm_next_round() -> None:
    """Create a fresh server seed and bomb holder for the surviving players only."""
    game_state["phase"] = "running"
    (
        game_state["server_seed"],
        game_state["hashed_seed"],
        game_state["crash_point"],
    ) = generate_crash()
    game_state["multiplier"] = 1.0
    game_state["current_holder"] = random.choice(game_state["players"])["id"]
    game_state["round_number"] += 1
    game_state["ready_players"] = []
    game_state["lobby_auto_start_at"] = 0.0


def lobby_player_ids() -> set[str]:
    return {str(player["id"]) for player in game_state["players"]}


def all_lobby_players_are_ready() -> bool:
    player_ids = lobby_player_ids()
    ready_ids = {str(user_id) for user_id in game_state.get("ready_players", [])}
    return len(player_ids) >= 2 and player_ids.issubset(ready_ids)


async def ignite_lobby(reason: str) -> bool:
    """Start one lobby only after a server-owned ignition condition is satisfied."""
    if game_state["phase"] != "lobby" or len(game_state["players"]) < 2:
        return False
    arm_next_round()
    await refresh_lobby_cards()
    await manager.broadcast_state("start", ignition_reason=reason)
    asyncio.create_task(tick_bomb())
    return True


async def maybe_ignite_lobby() -> bool:
    """Apply all readiness, full-lobby, and countdown ignition rules atomically."""
    if game_state["phase"] != "lobby":
        return False
    player_count = len(game_state["players"])
    if player_count >= MAX_PLAYERS:
        return await ignite_lobby("full_lobby")
    if all_lobby_players_are_ready():
        return await ignite_lobby("all_ready")
    auto_start_at = float(game_state.get("lobby_auto_start_at") or 0)
    if player_count >= MINIMUM_COUNTDOWN_PLAYERS and auto_start_at and time.time() >= auto_start_at:
        return await ignite_lobby("lobby_countdown")
    return False


async def watch_lobby_ignition() -> None:
    """Wake at the lobby timeout while still allowing unanimous ready to light instantly."""
    global lobby_ignition_task
    try:
        while game_state["phase"] == "lobby":
            if await maybe_ignite_lobby():
                return
            auto_start_at = float(game_state.get("lobby_auto_start_at") or 0)
            if not auto_start_at:
                return
            await asyncio.sleep(min(1.0, max(0.1, auto_start_at - time.time())))
    finally:
        lobby_ignition_task = None


def schedule_lobby_ignition() -> None:
    global lobby_ignition_task
    if lobby_ignition_task is None or lobby_ignition_task.done():
        lobby_ignition_task = asyncio.create_task(watch_lobby_ignition())


async def resume_after_elimination():
    """Give the group a short visible pause before relighting the next fuse."""
    global intermission_task
    try:
        await asyncio.sleep(ELIMINATION_INTERMISSION_SECONDS)
        if game_state["phase"] != "intermission" or len(game_state["players"]) < 2:
            return
        arm_next_round()
        await refresh_lobby_cards()
        await manager.broadcast_state("next_round")
        asyncio.create_task(tick_bomb())
    finally:
        intermission_task = None


def schedule_next_round() -> None:
    global intermission_task
    if intermission_task is None or intermission_task.done():
        intermission_task = asyncio.create_task(resume_after_elimination())


async def tick_bomb():
    try:
        while game_state["phase"] == "running":
            await asyncio.sleep(ROUND_TICK_SECONDS)
            game_state["multiplier"] = round(game_state["multiplier"] + 0.25, 2)

            if game_state["multiplier"] >= game_state["crash_point"]:
                await detonate()
                break

            await manager.broadcast_state("tick")
    except asyncio.CancelledError:
        pass


async def detonate():
    loser_id = game_state["current_holder"]
    loser = next(
        (player for player in game_state["players"] if player["id"] == loser_id),
        None,
    )
    survivors = [player for player in game_state["players"] if player["id"] != loser_id]
    game_state["players"] = survivors
    if loser:
        game_state["eliminated_players"].append(loser)
    game_state["current_holder"] = None

    is_final = len(survivors) <= 1
    payout = 0.0
    if is_final and survivors:
        payout = round(game_state["pot"], 2)
        await change_balance(survivors[0]["id"], payout)

    if is_final:
        # Retain only public-safe match statistics after the lobby reopens.
        game_state["phase"] = "ended"
        game_state["latest_round"] = {
            "multiplier": round(game_state["multiplier"], 2),
            "payout": payout,
            "survivor_count": len(survivors),
            "eliminations": len(game_state["eliminated_players"]),
            "rounds": game_state["round_number"],
        }
        await publish_elimination_poster(loser, len(survivors))
        await publish_round_results(payout, len(survivors))
        await manager.broadcast_state(
            "sploded",
            loser=loser_id,
            loser_name=loser["name"] if loser else "UNKNOWN VICTIM",
            payout=payout,
            final=True,
        )
        schedule_lobby_reset()
        return

    game_state["phase"] = "intermission"
    await publish_elimination_poster(loser, len(survivors))
    await manager.broadcast_state(
        "eliminated",
        loser=loser_id,
        loser_name=loser["name"] if loser else "UNKNOWN VICTIM",
        payout=0.0,
        final=False,
        remaining_players=len(survivors),
    )
    schedule_next_round()


@app.websocket("/ws/{user_id}/{user_name}")
async def websocket_endpoint(
    websocket: WebSocket,
    user_id: str,
    user_name: str,
    tg_init_data: str = "",
):
    verified_user = verified_telegram_user(tg_init_data)
    verified_user_id = str(verified_user["id"]) if verified_user else None
    raw_handle = str((verified_user or {}).get("username") or "")
    public_handle = raw_handle if re.fullmatch(r"[A-Za-z0-9_]{5,32}", raw_handle) else ""
    is_pit_boss = bool(
        verified_user_id
        and verified_user_id == user_id
        and verified_user_id in PIT_BOSS_IDS
    )
    await manager.connect(websocket, user_id, is_pit_boss)
    await manager.send_state(user_id, "welcome")

    try:
        while True:
            data = await websocket.receive_json()
            if manager.active_connections.get(user_id) is not websocket:
                await websocket.close(code=4001, reason="A newer game session replaced this one.")
                break
            action = data.get("action")

            if action == "join":
                if game_state["phase"] != "lobby":
                    await reject_action(user_id, "The lobby is sealed. This fuse already has victims.")
                    continue

                if any(player["id"] == user_id for player in game_state["players"]):
                    await reject_action(user_id, "You already signed the waiver. Try not to enjoy it.")
                    continue

                balance = await get_balance(user_id)
                if balance < JOIN_COST:
                    await reject_action(user_id, "You need 100 ◉ to sign this waiver.")
                    continue

                if not await claim_action_slot(user_id, "join", JOIN_COOLDOWN_SECONDS):
                    await reject_action(user_id, "The clerk is stamping your waiver. One moment.")
                    continue

                await change_balance(user_id, -JOIN_COST)
                game_state["pot"] += JOIN_COST
                game_state["players"].append({"id": user_id, "name": user_name, "public_handle": public_handle})
                if (
                    len(game_state["players"]) >= MINIMUM_COUNTDOWN_PLAYERS
                    and not game_state.get("lobby_auto_start_at")
                ):
                    game_state["lobby_auto_start_at"] = time.time() + LOBBY_AUTO_IGNITE_SECONDS
                    schedule_lobby_ignition()
                if not await maybe_ignite_lobby():
                    await refresh_lobby_cards()
                    await manager.broadcast_state("update")

            elif action == "light_it_up":
                if game_state["phase"] != "lobby":
                    await reject_action(user_id, "The fuse is already lit. Keep your hands off the matchbook.")
                    continue
                if user_id not in lobby_player_ids():
                    await reject_action(user_id, "Sign the waiver before trying to light anything.")
                    continue
                ready_players = {str(player_id) for player_id in game_state.get("ready_players", [])}
                if user_id not in ready_players:
                    ready_players.add(user_id)
                    game_state["ready_players"] = sorted(ready_players)
                if not await maybe_ignite_lobby():
                    await refresh_lobby_cards()
                    await manager.broadcast_state("ready_changed")

            elif action == "cool_it_down":
                ready_players = {str(player_id) for player_id in game_state.get("ready_players", [])}
                if user_id in ready_players and game_state["phase"] == "lobby":
                    ready_players.discard(user_id)
                    game_state["ready_players"] = sorted(ready_players)
                    await refresh_lobby_cards()
                    await manager.broadcast_state("ready_changed")

            elif action == "claim_daily":
                claim_key = f"ds:daily_claims:{user_id}"
                claimed = await redis_client.set(
                    claim_key,
                    "1",
                    nx=True,
                    ex=DAILY_CLAIM_COOLDOWN_SECONDS,
                )
                if not claimed:
                    status = await daily_claim_status(user_id)
                    await reject_action(
                        user_id,
                        f"The chip cache is empty. Return in {status['seconds_until']} seconds.",
                    )
                    continue

                await change_balance(user_id, DAILY_CHIP_GRANT)
                await manager.send_state(
                    user_id,
                    "daily_claimed",
                    claim_amount=DAILY_CHIP_GRANT,
                )

            elif action == "pit_boss_grant":
                if user_id not in manager.pit_boss_connections:
                    await reject_action(user_id, "Only a verified Pit Boss may open the chip drawer.")
                    continue

                target_id = str(data.get("target_id", ""))
                if not any(player["id"] == target_id for player in game_state["players"]):
                    await reject_action(user_id, "Select a currently listed victim before issuing chips.")
                    continue

                try:
                    grant_amount = float(data.get("amount"))
                except (TypeError, ValueError):
                    await reject_action(user_id, "Enter a whole chip amount before opening the drawer.")
                    continue

                if (
                    not math.isfinite(grant_amount)
                    or grant_amount != math.floor(grant_amount)
                    or not PIT_BOSS_MIN_GRANT <= grant_amount <= PIT_BOSS_MAX_GRANT
                ):
                    await reject_action(
                        user_id,
                        f"Pit Boss grants must be whole amounts from {PIT_BOSS_MIN_GRANT:.0f} to {PIT_BOSS_MAX_GRANT:.0f} ◉.",
                    )
                    continue

                if not await claim_action_slot(
                    user_id, "pit_boss", PIT_BOSS_GRANT_COOLDOWN_SECONDS
                ):
                    await reject_action(user_id, "The chip drawer is already open. One moment.")
                    continue

                await change_balance(target_id, grant_amount)
                await redis_client.lpush(
                    "ds:pit_boss_grants",
                    json.dumps(
                        {
                            "admin_id": user_id,
                            "recipient_id": target_id,
                            "amount": grant_amount,
                            "created_at": int(time.time()),
                        }
                    ),
                )
                await redis_client.ltrim("ds:pit_boss_grants", 0, 99)
                await manager.send_state(
                    target_id,
                    "pit_boss_granted",
                    grant_amount=grant_amount,
                )
                if target_id != user_id:
                    await manager.send_state(
                        user_id,
                        "pit_boss_grant_sent",
                        grant_amount=grant_amount,
                    )

            elif action == "force_start":
                await reject_action(
                    user_id,
                    "The cabinet lights only when every victim holds LIGHT IT UP, the lobby fills, or three victims wait 45 seconds.",
                )

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
        manager.disconnect(user_id, websocket)
