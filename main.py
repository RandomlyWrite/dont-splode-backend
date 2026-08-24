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
from datetime import datetime, timezone
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

# Redis is the authoritative virtual-chip ledger; the browser never supplies a chip balance.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

BALANCES_KEY = "ds:balances"
PLAYER_PROFILES_KEY = "ds:player_profiles"
PLAYER_PROFILE_REFS_KEY = "ds:player_profile_refs"
PLAYER_LEDGER_PREFIX = "ds:player_ledger:"
ADMIN_LEDGER_KEY = "ds:admin_ledger"
REGISTERED_GROUPS_KEY = "ds:registered_groups"
ACTIVE_GROUP_CARDS_KEY = "ds:active_group_cards"
GROUP_COMPETITIVE_PREFIX = "ds:group_competitive:"
GROUP_SEASON_CURRENT_PREFIX = "ds:group_season_current:"
GROUP_SEASON_ARCHIVE_PREFIX = "ds:group_season_archives:"
PLAYER_LEDGER_LIMIT = 1000
ADMIN_LEDGER_LIMIT = 10000
LEADERBOARD_LIMIT = 10
GROUP_SEASON_ARCHIVE_LIMIT = 12

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
MASTER_RESET_COOLDOWN_SECONDS = 5.0
MASTER_RESET_PHRASE = "RESET ALL CHIPS"
DELETE_PLAYER_PHRASE = "ERASE PLAYER RECORD"
DELETE_PLAYER_COOLDOWN_SECONDS = 5.0
SPECTATOR_REACTIONS = {"👀", "🔥", "😱", "💥", "🪦"}
SPECTATOR_REACTION_COOLDOWN_SECONDS = 1.8
GHOST_REACTIONS = {"💀", "👻", "🍿"}
TAUNT_LINES = [
    "🔥 the pot doesn't want you to make it",
    "👀 everyone's watching your hands shake",
    "⏳ tick tock, whoever's holding it",
    "🎯 statistically, someone's due",
    "🍿 this is the good part",
]
TAUNT_COOLDOWN_SECONDS = 3.0
# Populate with real sticker file_ids once a custom pack exists (via @Stickers +
# BotFather); until then this stays empty and reactions are emoji-only in chat.
# Format: {"💥": "CAACAgEAAxkBAAI...", ...} -- keys must be a subset of
# SPECTATOR_REACTIONS or GHOST_REACTIONS.
try:
    SPECTATOR_STICKER_FILE_IDS: dict[str, str] = json.loads(os.getenv("SPECTATOR_STICKER_FILE_IDS", "{}"))
except (TypeError, ValueError, json.JSONDecodeError):
    SPECTATOR_STICKER_FILE_IDS = {}
SPECTATOR_STICKER_POST_COOLDOWN_SECONDS = 10.0
NUDGE_COOLDOWN_SECONDS = 60.0
RECORD_BIGGEST_POT_PREFIX = "ds:record_biggest_pot:"
REIGNING_CHAMPION_PREFIX = "ds:reigning_champion:"
DEFAULT_GROUP_REF_KEY = "default"
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
    "predictions": {},
}
reset_task = None
intermission_task = None
lobby_ignition_task = None
round_group_rosters: dict[str, set[str]] = {}

# Every task that reads-then-mutates game_state must hold this before touching it.
# Without it, two coroutines interleaving across an `await` (a Redis round-trip, a
# Telegram call) can each act on a game_state snapshot that's gone stale by the
# time they resume -- e.g. a bomb detonating mid-`pass` while the passer still
# thinks they're holding it. asyncio.Lock is non-reentrant, so nothing that runs
# *inside* the lock (arm_next_round, detonate, ignite_lobby, etc.) may acquire it
# again -- only the outer entry points below do that.
game_lock = asyncio.Lock()


async def get_balance(user_id: str) -> float:
    """Return a validated, persistent server-side balance for one player."""
    raw_balance = await redis_client.hget(BALANCES_KEY, user_id)
    if raw_balance is None:
        await redis_client.hset(BALANCES_KEY, user_id, DEFAULT_BALANCE)
        return DEFAULT_BALANCE

    try:
        balance = float(raw_balance)
    except (TypeError, ValueError):
        balance = DEFAULT_BALANCE

    # Negative values could only have come from the prior uninitialized-balance
    # path, so repair them rather than leaving a new player permanently locked out.
    if not math.isfinite(balance) or balance < 0:
        balance = DEFAULT_BALANCE
        await redis_client.hset(BALANCES_KEY, user_id, balance)

    return round(balance, 2)


def clean_profile_name(value: object, fallback: str = "UNKNOWN PLAYER") -> str:
    """Keep only compact, printable public identity metadata in persisted profiles."""
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return (text[:64] or fallback)


def clean_public_handle(value: object) -> str:
    handle = str(value or "").strip().lstrip("@")
    return handle if re.fullmatch(r"[A-Za-z0-9_]{5,32}", handle) else ""


def clean_group_ref(value: object) -> str:
    """Accept only the opaque registered-group reference; Telegram chat IDs never enter Mini App state."""
    candidate = str(value or "").strip()
    return candidate if re.fullmatch(r"[A-Za-z0-9_-]{8,24}", candidate) else ""


def profile_summary(profile: dict) -> dict:
    """Return the Pit Boss-safe view of a profile without the raw Telegram user ID."""
    return {
        "ref": str(profile.get("ref", "")),
        "name": clean_profile_name(profile.get("name")),
        "public_handle": clean_public_handle(profile.get("public_handle")),
        "balance": round(float(profile.get("balance", 0) or 0), 2),
        "first_seen": int(profile.get("first_seen", 0) or 0),
        "last_seen": int(profile.get("last_seen", 0) or 0),
        "matches_entered": int(profile.get("matches_entered", 0) or 0),
        "matches_survived": int(profile.get("matches_survived", 0) or 0),
        "total_pot_won": round(float(profile.get("total_pot_won", 0) or 0), 2),
        "eliminations": int(profile.get("eliminations", 0) or 0),
        "passes": int(profile.get("passes", 0) or 0),
        "chips_granted": round(float(profile.get("chips_granted", 0) or 0), 2),
        "chips_removed": round(float(profile.get("chips_removed", 0) or 0), 2),
    }


def public_leaderboard_row(profile: dict, rank: int, view: str) -> dict:
    """Return one public-safe leaderboard row without profile references or Telegram IDs."""
    row = {
        "rank": int(rank),
        "name": clean_profile_name(profile.get("name")),
        "public_handle": clean_public_handle(profile.get("public_handle")),
        "survivals": max(0, int(profile.get("matches_survived", 0) or 0)),
        "pot_won": max(0.0, round(float(profile.get("total_pot_won", 0) or 0), 2)),
    }
    if view == "chips":
        row["balance"] = max(0.0, round(float(profile.get("balance", 0) or 0), 2))
    return row


async def group_is_registered(group_ref: str) -> bool:
    safe_ref = clean_group_ref(group_ref)
    if not safe_ref:
        return False
    for raw_group in (await redis_client.hgetall(REGISTERED_GROUPS_KEY)).values():
        try:
            if clean_group_ref((json.loads(raw_group) or {}).get("ref")) == safe_ref:
                return True
        except (TypeError, json.JSONDecodeError):
            continue
    return False


def utc_week_id(timestamp: float | None = None) -> str:
    """Return the stable ISO week identifier used for group season windows."""
    stamp = datetime.fromtimestamp(float(timestamp or time.time()), timezone.utc)
    iso = stamp.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def ranked_season_rows(records: list[dict]) -> list[dict]:
    """Rank only public-safe competitive values for a group season or archive."""
    eligible = [record for record in records if int(record.get("matches_entered", 0) or 0) > 0]
    eligible.sort(
        key=lambda record: (
            -int(record.get("matches_survived", 0) or 0),
            -float(record.get("total_pot_won", 0) or 0),
            clean_profile_name(record.get("name")).lower(),
        )
    )
    rows: list[dict] = []
    for rank, record in enumerate(eligible, start=1):
        rows.append(
            {
                "rank": rank,
                "name": clean_profile_name(record.get("name")),
                "public_handle": clean_public_handle(record.get("public_handle")),
                "survivals": max(0, int(record.get("matches_survived", 0) or 0)),
                "pot_won": max(0.0, round(float(record.get("total_pot_won", 0) or 0), 2)),
                "matches": max(0, int(record.get("matches_entered", 0) or 0)),
            }
        )
    return rows


async def group_season_records(group_ref: str, week_id: str) -> list[dict]:
    """Load one week of safe per-group competitive rows without returning player IDs."""
    safe_ref = clean_group_ref(group_ref)
    if not safe_ref or not re.fullmatch(r"\d{4}-W\d{2}", str(week_id)):
        return []
    records: list[dict] = []
    for raw_entry in (await redis_client.hgetall(f"{GROUP_SEASON_CURRENT_PREFIX}{safe_ref}:{week_id}")).values():
        try:
            record = json.loads(raw_entry)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


async def archive_group_season(group_ref: str, week_id: str) -> None:
    """Freeze a compact public-safe season result once, retaining only the bounded honor docket."""
    safe_ref = clean_group_ref(group_ref)
    rows = ranked_season_rows(await group_season_records(safe_ref, week_id))
    if not safe_ref or not rows:
        return
    snapshot = {
        "week": week_id,
        "settled_at": int(time.time()),
        "winner": rows[0] if rows[0]["survivals"] > 0 else None,
        "entries": rows[:LEADERBOARD_LIMIT],
    }
    await redis_client.lpush(f"{GROUP_SEASON_ARCHIVE_PREFIX}{safe_ref}", json.dumps(snapshot, separators=(",", ":")))
    await redis_client.ltrim(f"{GROUP_SEASON_ARCHIVE_PREFIX}{safe_ref}", 0, GROUP_SEASON_ARCHIVE_LIMIT - 1)


async def ensure_current_group_season(group_ref: str) -> str:
    """Roll a registered group forward on first activity or inspection after a UTC week boundary."""
    safe_ref = clean_group_ref(group_ref)
    if not safe_ref or not await group_is_registered(safe_ref):
        return ""
    current_week = utc_week_id()
    state_key = f"{GROUP_SEASON_CURRENT_PREFIX}{safe_ref}"
    prior_week = str(await redis_client.get(state_key) or "")
    if prior_week and prior_week != current_week:
        await archive_group_season(safe_ref, prior_week)
    if prior_week != current_week:
        await redis_client.set(state_key, current_week)
    return current_week


async def group_season_archive_payload(group_ref: str) -> dict:
    """Return the current weekly docket plus bounded historical winners without internal identifiers."""
    safe_ref = clean_group_ref(group_ref)
    if not await group_is_registered(safe_ref):
        return {"available": False, "current": None, "archives": []}
    current_week = await ensure_current_group_season(safe_ref)
    current_rows = ranked_season_rows(await group_season_records(safe_ref, current_week))
    archives: list[dict] = []
    for raw_snapshot in await redis_client.lrange(f"{GROUP_SEASON_ARCHIVE_PREFIX}{safe_ref}", 0, GROUP_SEASON_ARCHIVE_LIMIT - 1):
        try:
            snapshot = json.loads(raw_snapshot)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(snapshot, dict):
            continue
        winner = snapshot.get("winner") if isinstance(snapshot.get("winner"), dict) else None
        entries = snapshot.get("entries") if isinstance(snapshot.get("entries"), list) else []
        archives.append(
            {
                "week": str(snapshot.get("week", ""))[:10],
                "winner": winner,
                "entries": [entry for entry in entries[:LEADERBOARD_LIMIT] if isinstance(entry, dict)],
            }
        )
    return {
        "available": True,
        "current": {
            "week": current_week,
            "winner": current_rows[0] if current_rows and current_rows[0]["survivals"] > 0 else None,
            "entries": current_rows[:LEADERBOARD_LIMIT],
        },
        "archives": archives,
    }


async def record_group_match_results(group_ref: str, participant_ids: set[str], winner_id: str, payout: float) -> None:
    """Append only safe, per-group competitive totals for players who entered via that group’s signed lobby card."""
    safe_ref = clean_group_ref(group_ref)
    if not participant_ids or not await group_is_registered(safe_ref):
        return
    key = f"{GROUP_COMPETITIVE_PREFIX}{safe_ref}"
    current_week = await ensure_current_group_season(safe_ref)
    season_key = f"{GROUP_SEASON_CURRENT_PREFIX}{safe_ref}:{current_week}"
    for participant_id in sorted({str(value) for value in participant_ids}):
        profile = await load_player_profile(participant_id)
        if profile is None:
            profile = await ensure_player_profile(participant_id)
        if profile is None:
            continue
        for target_key in (key, season_key):
            try:
                existing = json.loads(await redis_client.hget(target_key, participant_id) or "{}")
            except (TypeError, json.JSONDecodeError):
                existing = {}
            existing.update(
                {
                    "name": clean_profile_name(profile.get("name")),
                    "public_handle": clean_public_handle(profile.get("public_handle")),
                    "matches_entered": int(existing.get("matches_entered", 0) or 0) + 1,
                    "matches_survived": int(existing.get("matches_survived", 0) or 0) + (1 if participant_id == str(winner_id) else 0),
                    "total_pot_won": round(float(existing.get("total_pot_won", 0) or 0) + (float(payout) if participant_id == str(winner_id) else 0.0), 2),
                    "balance": await get_balance(participant_id),
                }
            )
            await redis_client.hset(target_key, participant_id, json.dumps(existing, separators=(",", ":")))


async def public_leaderboard_payload(user_id: str, view: str = "competitive", group_ref: str = "") -> dict:
    """Build a deterministic global or registered-group ranking without exposing private identifiers."""
    safe_view = "chips" if str(view).lower() == "chips" else "competitive"
    safe_group_ref = clean_group_ref(group_ref)
    group_available = await group_is_registered(safe_group_ref)
    scope = "group" if group_available else "global"
    ranked: list[tuple[str, dict]] = []
    if group_available:
        for candidate_id, raw_entry in (await redis_client.hgetall(f"{GROUP_COMPETITIVE_PREFIX}{safe_group_ref}")).items():
            try:
                profile = json.loads(raw_entry)
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(profile, dict) or int(profile.get("matches_entered", 0) or 0) <= 0:
                continue
            profile["balance"] = await get_balance(str(candidate_id))
            ranked.append((str(candidate_id), profile))
    else:
        existing_profiles = await redis_client.hgetall(PLAYER_PROFILES_KEY)
        for existing_user_id in (await redis_client.hgetall(BALANCES_KEY)).keys():
            if str(existing_user_id) not in existing_profiles:
                await ensure_player_profile(str(existing_user_id), fallback_name="LEGACY PLAYER")
        for candidate_id, raw_profile in (await redis_client.hgetall(PLAYER_PROFILES_KEY)).items():
            try:
                profile = json.loads(raw_profile)
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(profile, dict):
                continue
            profile["balance"] = await get_balance(str(candidate_id))
            survivals = max(0, int(profile.get("matches_survived", 0) or 0))
            if safe_view == "competitive" and survivals <= 0:
                continue
            ranked.append((str(candidate_id), profile))

    if safe_view == "chips":
        ranked.sort(
            key=lambda item: (
                -max(0.0, round(float(item[1].get("balance", 0) or 0), 2)),
                -max(0, int(item[1].get("matches_entered", 0) or 0)),
                clean_profile_name(item[1].get("name")).lower(),
                str(item[1].get("ref", "")),
            )
        )
    else:
        ranked.sort(
            key=lambda item: (
                -max(0, int(item[1].get("matches_survived", 0) or 0)),
                -max(0.0, round(float(item[1].get("total_pot_won", 0) or 0), 2)),
                clean_profile_name(item[1].get("name")).lower(),
                str(item[1].get("ref", "")),
            )
        )

    entries: list[dict] = []
    viewer = None
    for rank, (candidate_id, profile) in enumerate(ranked, start=1):
        row = public_leaderboard_row(profile, rank, safe_view)
        if rank <= LEADERBOARD_LIMIT:
            entries.append(row)
        if candidate_id == str(user_id):
            viewer = row
    return {
        "view": safe_view,
        "scope": scope,
        "group_available": group_available,
        "entries": entries,
        "viewer": viewer if viewer and viewer["rank"] > LEADERBOARD_LIMIT else None,
        "viewer_rank": viewer["rank"] if viewer else None,
        "eligible_count": len(ranked),
        "updated_at": int(time.time()),
    }


async def load_player_profile(user_id: str) -> dict | None:
    raw = await redis_client.hget(PLAYER_PROFILES_KEY, user_id)
    if not raw:
        return None
    try:
        profile = json.loads(raw)
        return profile if isinstance(profile, dict) else None
    except (TypeError, json.JSONDecodeError):
        return None


async def save_player_profile(user_id: str, profile: dict) -> None:
    await redis_client.hset(PLAYER_PROFILES_KEY, user_id, json.dumps(profile, separators=(",", ":")))
    await redis_client.hset(PLAYER_PROFILE_REFS_KEY, str(profile["ref"]), user_id)


async def ensure_player_profile(
    user_id: str,
    verified_user: dict | None = None,
    fallback_name: str = "",
) -> dict:
    """Create or refresh a durable profile using only verified Telegram identity fields."""
    now = int(time.time())
    profile = await load_player_profile(user_id)
    if profile is None:
        profile = {
            "ref": secrets.token_urlsafe(9),
            "name": clean_profile_name((verified_user or {}).get("first_name") or fallback_name),
            "public_handle": clean_public_handle((verified_user or {}).get("username")),
            "first_seen": now,
            "last_seen": now,
            "matches_entered": 0,
            "matches_survived": 0,
            "total_pot_won": 0.0,
            "eliminations": 0,
            "passes": 0,
            "chips_granted": 0.0,
            "chips_removed": 0.0,
        }
    else:
        if verified_user:
            profile["name"] = clean_profile_name(verified_user.get("first_name") or fallback_name, profile.get("name", "UNKNOWN PLAYER"))
            profile["public_handle"] = clean_public_handle(verified_user.get("username"))
        elif fallback_name:
            profile["name"] = clean_profile_name(fallback_name, profile.get("name", "UNKNOWN PLAYER"))
        profile["last_seen"] = now
    profile["balance"] = await get_balance(user_id)
    await save_player_profile(user_id, profile)
    return profile


async def update_player_profile(user_id: str, **increments: float) -> dict | None:
    """Update server-managed profile totals after an already-authorized game event."""
    profile = await load_player_profile(user_id)
    if profile is None:
        profile = await ensure_player_profile(user_id)
    if profile is None:
        return None
    profile["last_seen"] = int(time.time())
    profile["balance"] = await get_balance(user_id)
    for key, value in increments.items():
        if key not in {"matches_entered", "matches_survived", "total_pot_won", "eliminations", "passes", "chips_granted", "chips_removed"}:
            continue
        profile[key] = round(float(profile.get(key, 0) or 0) + float(value), 2)
    await save_player_profile(user_id, profile)
    return profile


async def apply_balance_event(
    user_id: str,
    amount: float,
    reason: str,
    *,
    actor_id: str | None = None,
    round_ref: int | None = None,
    metadata: dict | None = None,
) -> float:
    """Atomically persist a signed virtual-chip event, resulting balance, and bounded audit history."""
    amount = round(float(amount), 2)
    if not math.isfinite(amount) or amount == 0:
        return await get_balance(user_id)

    safe_reason = re.sub(r"[^a-z0-9_:-]", "", str(reason).lower())[:48] or "ledger_adjustment"
    for _ in range(5):
        async with redis_client.pipeline(transaction=True) as pipe:
            try:
                await pipe.watch(BALANCES_KEY)
                raw_balance = await pipe.hget(BALANCES_KEY, user_id)
                current = DEFAULT_BALANCE if raw_balance is None else float(raw_balance)
                if not math.isfinite(current) or current < 0:
                    current = DEFAULT_BALANCE
                updated = round(current + amount, 2)
                if updated < 0:
                    raise ValueError("Insufficient virtual chips for this adjustment.")
                event = {
                    "event_id": secrets.token_urlsafe(12),
                    "created_at": int(time.time()),
                    "target_id": user_id,
                    "actor_id": actor_id or "system",
                    "amount": amount,
                    "balance_after": updated,
                    "reason": safe_reason,
                    "round": int(round_ref or 0),
                    "metadata": metadata or {},
                }
                encoded_event = json.dumps(event, separators=(",", ":"))
                pipe.multi()
                pipe.hset(BALANCES_KEY, user_id, updated)
                pipe.lpush(f"{PLAYER_LEDGER_PREFIX}{user_id}", encoded_event)
                pipe.ltrim(f"{PLAYER_LEDGER_PREFIX}{user_id}", 0, PLAYER_LEDGER_LIMIT - 1)
                pipe.lpush(ADMIN_LEDGER_KEY, encoded_event)
                pipe.ltrim(ADMIN_LEDGER_KEY, 0, ADMIN_LEDGER_LIMIT - 1)
                await pipe.execute()
                profile = await ensure_player_profile(user_id)
                if profile:
                    profile["balance"] = updated
                    if amount > 0 and safe_reason.startswith("pit_boss") and safe_reason != "pit_boss_master_reset":
                        profile["chips_granted"] = round(float(profile.get("chips_granted", 0)) + amount, 2)
                    if amount < 0 and safe_reason.startswith("pit_boss") and safe_reason != "pit_boss_master_reset":
                        profile["chips_removed"] = round(float(profile.get("chips_removed", 0)) + abs(amount), 2)
                    await save_player_profile(user_id, profile)
                return updated
            except ValueError:
                raise
            except Exception as error:
                if "WatchError" not in type(error).__name__:
                    raise
    raise RuntimeError("The chip ledger was busy. Try the adjustment again.")


async def change_balance(
    user_id: str,
    amount: float,
    reason: str = "ledger_adjustment",
    *,
    actor_id: str | None = None,
    round_ref: int | None = None,
    metadata: dict | None = None,
) -> float:
    """Compatibility wrapper for every authoritative balance change in game flow."""
    return await apply_balance_event(
        user_id,
        amount,
        reason,
        actor_id=actor_id,
        round_ref=round_ref,
        metadata=metadata,
    )


async def delete_player_completely(user_id: str, profile_ref: str, actor_id: str, reason: str) -> dict:
    """Irreversibly purge a player's balance, profile, personal ledger, and live leaderboard/season entries.

    Deliberately does NOT rewrite already-archived historical season snapshots:
    archive_group_season() freezes each week's results as a plain name/handle
    string with no live user_id link (see ranked_season_rows), so there is
    nothing to unlink there -- and rewriting a closed week's recorded winner
    would mean editing a historical fact, which is a separate, more invasive
    decision this function does not make silently.
    """
    removed = {"admin_ledger_entries_purged": 0, "group_competitive_removed": 0, "group_season_removed": 0}

    # Strip this user's own entries out of the shared admin audit ledger.
    # Their per-player ledger list is deleted outright below instead of filtered.
    admin_entries = await redis_client.lrange(ADMIN_LEDGER_KEY, 0, -1)
    kept: list[str] = []
    for raw in admin_entries:
        try:
            entry = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            kept.append(raw)
            continue
        if str(entry.get("target_id")) == str(user_id):
            removed["admin_ledger_entries_purged"] += 1
            continue
        kept.append(raw)
    if removed["admin_ledger_entries_purged"]:
        await redis_client.delete(ADMIN_LEDGER_KEY)
        if kept:
            await redis_client.rpush(ADMIN_LEDGER_KEY, *kept)
            await redis_client.ltrim(ADMIN_LEDGER_KEY, 0, ADMIN_LEDGER_LIMIT - 1)

    await redis_client.hdel(BALANCES_KEY, user_id)
    await redis_client.hdel(PLAYER_PROFILES_KEY, user_id)
    if profile_ref:
        await redis_client.hdel(PLAYER_PROFILE_REFS_KEY, profile_ref)
    await redis_client.delete(f"{PLAYER_LEDGER_PREFIX}{user_id}")

    # Strip from every registered group's live competitive board and the
    # current (still-mutable) season week only -- see docstring re: archives.
    groups = await redis_client.hgetall(REGISTERED_GROUPS_KEY)
    for raw_group in groups.values():
        try:
            group = json.loads(raw_group)
        except (TypeError, json.JSONDecodeError):
            continue
        group_ref = clean_group_ref(group.get("ref"))
        if not group_ref:
            continue
        if await redis_client.hdel(f"{GROUP_COMPETITIVE_PREFIX}{group_ref}", user_id):
            removed["group_competitive_removed"] += 1
        current_week = str(await redis_client.get(f"{GROUP_SEASON_CURRENT_PREFIX}{group_ref}") or "")
        if current_week and await redis_client.hdel(f"{GROUP_SEASON_CURRENT_PREFIX}{group_ref}:{current_week}", user_id):
            removed["group_season_removed"] += 1

    # Log the deletion itself using the profile ref, not the raw Telegram user
    # ID -- which no longer exists anywhere in Redis after the lines above.
    audit_entry = {
        "event_id": secrets.token_urlsafe(12),
        "created_at": int(time.time()),
        "target_id": f"deleted:{profile_ref or 'unknown'}",
        "actor_id": actor_id,
        "amount": 0.0,
        "balance_after": 0.0,
        "reason": "player_data_deleted",
        "round": 0,
        "metadata": {"note": str(reason)[:96]},
    }
    await redis_client.lpush(ADMIN_LEDGER_KEY, json.dumps(audit_entry, separators=(",", ":")))
    await redis_client.ltrim(ADMIN_LEDGER_KEY, 0, ADMIN_LEDGER_LIMIT - 1)
    return removed


async def master_reset_virtual_chips(actor_id: str, reason: str) -> int:
    """Restore every known virtual-chip balance to the default through individual append-only ledger events."""
    safe_reason = clean_profile_name(reason, "")[:96]
    if len(safe_reason.strip()) < 3:
        raise ValueError("A master reset requires a short audit reason.")
    existing_profiles = await redis_client.hgetall(PLAYER_PROFILES_KEY)
    balance_ids = {str(user_id) for user_id in (await redis_client.hgetall(BALANCES_KEY)).keys()}
    target_ids = {str(user_id) for user_id in existing_profiles.keys()} | balance_ids
    changed = 0
    for target_id in sorted(target_ids):
        current = await get_balance(target_id)
        adjustment = round(DEFAULT_BALANCE - current, 2)
        if adjustment == 0:
            continue
        await apply_balance_event(
            target_id,
            adjustment,
            "pit_boss_master_reset",
            actor_id=actor_id,
            metadata={"note": safe_reason, "reset_to": DEFAULT_BALANCE},
        )
        changed += 1
    return changed


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


async def pit_boss_dashboard_payload(profile_ref: str = "", search: str = "", sort: str = "balance_desc") -> dict:
    """Return bounded, reference-based administration data without exposing raw Telegram IDs to the client."""
    existing_profiles = await redis_client.hgetall(PLAYER_PROFILES_KEY)
    for existing_user_id in (await redis_client.hgetall(BALANCES_KEY)).keys():
        if str(existing_user_id) not in existing_profiles:
            await ensure_player_profile(str(existing_user_id), fallback_name="LEGACY PLAYER")
    raw_profiles = await redis_client.hgetall(PLAYER_PROFILES_KEY)
    needle = str(search or "").strip().lower().lstrip("@")[:48]
    sort_mode = str(sort or "balance_desc").strip().lower()
    if sort_mode not in {"balance_desc", "balance_asc", "recent", "matches_desc", "name_asc"}:
        sort_mode = "balance_desc"
    profiles: list[dict] = []
    selected_id = ""
    for user_id, raw_profile in raw_profiles.items():
        try:
            profile = json.loads(raw_profile)
        except (TypeError, json.JSONDecodeError):
            continue
        summary = profile_summary(profile)
        haystack = f"{summary['ref']} {summary['name']} {summary['public_handle']}".lower()
        if needle and needle not in haystack:
            continue
        profiles.append(summary)
        if summary["ref"] == profile_ref:
            selected_id = str(user_id)
    if sort_mode == "balance_asc":
        profiles.sort(key=lambda item: (item["balance"], item["name"].lower()))
    elif sort_mode == "recent":
        profiles.sort(key=lambda item: (item["last_seen"], item["name"].lower()), reverse=True)
    elif sort_mode == "matches_desc":
        profiles.sort(key=lambda item: (item["matches_entered"], item["balance"], item["name"].lower()), reverse=True)
    elif sort_mode == "name_asc":
        profiles.sort(key=lambda item: (item["name"].lower(), -item["balance"]))
    else:
        profiles.sort(key=lambda item: (item["balance"], item["name"].lower()), reverse=True)
    profiles = profiles[:100]

    ledger: list[dict] = []
    if selected_id:
        entries = await redis_client.lrange(f"{PLAYER_LEDGER_PREFIX}{selected_id}", 0, 49)
        for entry in entries:
            try:
                event = json.loads(entry)
            except (TypeError, json.JSONDecodeError):
                continue
            ledger.append(
                {
                    "event_id": str(event.get("event_id", "")),
                    "created_at": int(event.get("created_at", 0) or 0),
                    "amount": round(float(event.get("amount", 0) or 0), 2),
                    "balance_after": round(float(event.get("balance_after", 0) or 0), 2),
                    "reason": str(event.get("reason", "ledger_adjustment"))[:48],
                    "round": int(event.get("round", 0) or 0),
                    "note": clean_profile_name((event.get("metadata") or {}).get("note"), "")[:96],
                }
            )

    groups: list[dict] = []
    for raw_group in (await redis_client.hgetall(REGISTERED_GROUPS_KEY)).values():
        try:
            group = json.loads(raw_group)
        except (TypeError, json.JSONDecodeError):
            continue
        groups.append(
            {
                "ref": str(group.get("ref", "")),
                "title": clean_profile_name(group.get("title"), "UNKNOWN GROUP"),
                "type": str(group.get("type", "group"))[:20],
                "first_seen": int(group.get("first_seen", 0) or 0),
                "last_played": int(group.get("last_played", 0) or 0),
                "games_started": int(group.get("games_started", 0) or 0),
                "games_completed": int(group.get("games_completed", 0) or 0),
            }
        )
    groups.sort(key=lambda item: (item["last_played"], item["title"].lower()), reverse=True)
    return {"profiles": profiles, "ledger": ledger, "groups": groups[:100], "selected_ref": profile_ref, "sort": sort_mode}


async def register_telegram_group(chat: dict) -> dict | None:
    """Persist only safe group metadata after an explicit in-group registration command."""
    group_type = str(chat.get("type", ""))
    chat_id = str(chat.get("id", ""))
    if group_type not in {"group", "supergroup"} or not chat_id:
        return None
    now = int(time.time())
    raw = await redis_client.hget(REGISTERED_GROUPS_KEY, chat_id)
    try:
        group = json.loads(raw) if raw else {}
    except (TypeError, json.JSONDecodeError):
        group = {}
    group.update(
        {
            "ref": str(group.get("ref") or secrets.token_urlsafe(8)),
            "title": clean_profile_name(chat.get("title"), "UNTITLED GROUP"),
            "type": group_type,
            "first_seen": int(group.get("first_seen") or now),
            "last_played": int(group.get("last_played") or 0),
            "games_started": int(group.get("games_started") or 0),
            "games_completed": int(group.get("games_completed") or 0),
        }
    )
    await redis_client.hset(REGISTERED_GROUPS_KEY, chat_id, json.dumps(group, separators=(",", ":")))
    return {"chat_id": chat_id, **group}


async def touch_registered_group(group_ref: str, field: str) -> None:
    """Update public-safe group activity counters only for cards posted through explicit registration."""
    if field not in {"games_started", "games_completed"}:
        return
    groups = await redis_client.hgetall(REGISTERED_GROUPS_KEY)
    for chat_id, raw in groups.items():
        try:
            group = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if str(group.get("ref")) != str(group_ref):
            continue
        group[field] = int(group.get(field, 0) or 0) + 1
        group["last_played"] = int(time.time())
        await redis_client.hset(REGISTERED_GROUPS_KEY, chat_id, json.dumps(group, separators=(",", ":")))
        return


async def chat_id_for_group_ref(group_ref: str) -> str | None:
    """Resolve a registered group's chat_id from its opaque public ref, for server-initiated posts."""
    safe_ref = clean_group_ref(group_ref)
    if not safe_ref:
        return None
    groups = await redis_client.hgetall(REGISTERED_GROUPS_KEY)
    for chat_id, raw in groups.items():
        try:
            group = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if str(group.get("ref")) == safe_ref:
            return chat_id
    return None


async def set_reigning_champion(group_ref: str, survivor: dict) -> None:
    """Persist the most recent survivor so the next lobby card can show a champion tag."""
    payload = json.dumps(
        {"name": clean_profile_name(survivor.get("name")), "public_handle": clean_public_handle(survivor.get("public_handle"))},
        separators=(",", ":"),
    )
    await redis_client.set(f"{REIGNING_CHAMPION_PREFIX}{group_ref or DEFAULT_GROUP_REF_KEY}", payload)


async def reigning_champion_label(group_ref: str) -> str | None:
    """Return the display label for the current reigning champion of a group, if any."""
    raw = await redis_client.get(f"{REIGNING_CHAMPION_PREFIX}{group_ref or DEFAULT_GROUP_REF_KEY}")
    if not raw:
        return None
    try:
        champion = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None
    handle = clean_public_handle(champion.get("public_handle"))
    return f"@{handle}" if handle else escape(clean_profile_name(champion.get("name")))


async def check_and_record_biggest_pot(group_ref: str, payout: float) -> bool:
    """Update the group's biggest-pot record; return True only if this payout set a new record."""
    key = f"{RECORD_BIGGEST_POT_PREFIX}{group_ref or DEFAULT_GROUP_REF_KEY}"
    current = await redis_client.get(key)
    try:
        current_value = float(current) if current is not None else 0.0
    except (TypeError, ValueError):
        current_value = 0.0
    if payout <= current_value:
        return False
    await redis_client.set(key, str(payout))
    return True


async def registered_group_for_chat(chat_id: str) -> dict | None:
    """Resolve a Telegram group only inside the trusted webhook boundary."""
    safe_chat_id = str(chat_id or "")
    if not safe_chat_id:
        return None
    raw = await redis_client.hget(REGISTERED_GROUPS_KEY, safe_chat_id)
    try:
        group = json.loads(raw) if raw else None
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(group, dict) or not clean_group_ref(group.get("ref")):
        return None
    return {"chat_id": safe_chat_id, **group}


def leaderboard_label(row: dict) -> str:
    """Format the single public identity a group may see on a leaderboard line."""
    handle = clean_public_handle(row.get("public_handle"))
    return f"@{handle}" if handle else escape(clean_profile_name(row.get("name")))


async def group_leaderboard_message(group: dict) -> str:
    """Render compact group standings for Telegram without a member roster or internal identifiers."""
    board = await public_leaderboard_payload("", "competitive", str(group.get("ref", "")))
    entries = list(board.get("entries") or [])[:LEADERBOARD_LIMIT]
    title = escape(clean_profile_name(group.get("title"), "THIS CABINET"))
    if not entries:
        standings = "<i>No final survivor is on file yet. Light the fuse and make history.</i>"
    else:
        standings = "\n".join(
            f"<b>{row['rank']:02d}.</b> {leaderboard_label(row)} — {int(row.get('survivals', 0) or 0)} survived • {float(row.get('pot_won', 0) or 0):.0f} ◉ won"
            for row in entries
        )
    return (
        "💣 <b>DON'T SPLODE — GROUP LEADERBOARD</b> 💣\n"
        "━━━━━━━━━━━━\n\n"
        f"<b>{title}</b> • ALL TIME\n\n"
        f"{standings}\n\n"
        "<i>Final survivals first. Cumulative virtual pots break the tie.</i>"
    )


async def pit_boss_health_message() -> str:
    """Return an administrator-only operational summary with no player or chat identifiers."""
    try:
        registered_groups = len(await redis_client.hgetall(REGISTERED_GROUPS_KEY))
        active_cards = len(await redis_client.hgetall(ACTIVE_GROUP_CARDS_KEY))
        ledger_status = "NOMINAL"
    except Exception:
        registered_groups = 0
        active_cards = 0
        ledger_status = "DEGRADED"
    phase = str(game_state.get("phase", "lobby")).upper()
    active_players = len(game_state.get("players") or [])
    eliminated = len(game_state.get("eliminated_players") or [])
    spectators = max(0, len(manager.active_connections) - active_players)
    return (
        "🛠 <b>DON'T SPLODE — CABINET HEALTH</b>\n"
        "━━━━━━━━━━━━\n\n"
        f"Ledger: <b>{ledger_status}</b>\n"
        f"Phase: <b>{phase}</b>\n"
        f"Active players: <b>{active_players}</b>\n"
        f"Spectator sessions: <b>{spectators}</b>\n"
        f"Ash on file: <b>{eliminated}</b>\n"
        f"Group cabinets: <b>{registered_groups}</b>\n"
        f"Tracked live cards: <b>{active_cards}</b>\n\n"
        "<i>Operational summary only. No player or chat identifiers are exposed.</i>"
    )


def verified_telegram_context(init_data: str) -> tuple[dict, str] | None:
    """Return verified Telegram user data and its signed Mini App start parameter."""
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
        if not isinstance(user, dict) or user.get("id") is None:
            return None
        return user, str(fields.get("start_param", ""))
    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
        return None


def verified_telegram_user(init_data: str) -> dict | None:
    """Return verified Telegram user data without ever trusting the URL path identity."""
    context = verified_telegram_context(init_data)
    return context[0] if context else None


def verified_telegram_start_param(init_data: str) -> str:
    """Return a verified launch parameter, never an untrusted browser query value."""
    context = verified_telegram_context(init_data)
    return context[1] if context else ""


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


def render_final_survivor_poster(survivor: dict | None, payout: float) -> bytes:
    """Render the final public card with only the winning player’s public label and match outcome."""
    public_label = public_player_label(survivor)
    multiplier = round(float(game_state["multiplier"]), 2)
    image = Image.new("RGBA", (POSTER_WIDTH, POSTER_HEIGHT), "#11100e")
    draw = ImageDraw.Draw(image)

    draw.rectangle((0, 0, POSTER_WIDTH, POSTER_HEIGHT), fill="#100f0d")
    draw.rectangle((26, 26, POSTER_WIDTH - 26, POSTER_HEIGHT - 26), fill="#71511f", outline="#f1cb70", width=10)
    draw.rectangle((62, 62, POSTER_WIDTH - 62, POSTER_HEIGHT - 62), fill="#171511", outline="#0b0a08", width=18)
    for x, y in ((48, 48), (POSTER_WIDTH - 48, 48), (48, POSTER_HEIGHT - 48), (POSTER_WIDTH - 48, POSTER_HEIGHT - 48)):
        draw.ellipse((x - 10, y - 10, x + 10, y + 10), fill="#3d2a0c", outline="#ffe0a0", width=3)

    _center_poster_text(draw, "THE CABINET FOUND A SURVIVOR", 88, _poster_font(26), "#e1b861")
    _center_poster_text(draw, "STILL", 132, _poster_font(102), "#f1d07b", stroke=4, stroke_fill="#281807")
    _center_poster_text(draw, "BREATHING.", 244, _poster_font(102), "#f1d07b", stroke=4, stroke_fill="#281807")
    survivor_font = _fit_poster_text(draw, public_label.upper(), 820, 74)
    _center_poster_text(draw, public_label.upper(), 356, survivor_font, "#f04a40", stroke=3, stroke_fill="#260907")
    _center_poster_text(draw, "LAST SOUL STANDING", 440, _poster_font(28), "#d9b765")

    if POSTER_MASCOT_PATH.exists():
        with Image.open(POSTER_MASCOT_PATH).convert("RGBA") as mascot_source:
            mascot = mascot_source.copy()
        mascot.thumbnail((720, 560), Image.Resampling.LANCZOS)
        image.alpha_composite(mascot, ((POSTER_WIDTH - mascot.width) // 2, 482))
    else:
        draw.ellipse((340, 500, 740, 900), fill="#2a2822", outline="#d5aa55", width=10)

    draw.rounded_rectangle((112, 1005, POSTER_WIDTH - 112, 1195), radius=18, fill="#0d0c0a", outline="#d4ae5a", width=4)
    stats_label = _poster_font(25)
    stats_value = _poster_font(49)
    draw.text((158, 1045), "FINAL CRASH", font=stats_label, fill="#d2b574")
    draw.text((158, 1085), f"{multiplier:.2f}×", font=stats_value, fill="#f1d07b")
    draw.text((600, 1045), "SURVIVOR POT", font=stats_label, fill="#d2b574")
    draw.text((600, 1085), f"{payout:.0f} ◉", font=stats_value, fill="#9ce58c")
    _center_poster_text(draw, "NOT DEAD YET. DISGUSTING.", 1240, _poster_font(23), "#c9b581")

    buffer = BytesIO()
    image.convert("RGB").save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def cache_final_survivor_poster(survivor: dict | None, payout: float) -> str:
    """Store the winner photo briefly while Telegram fetches the transformed final card."""
    now = time.time()
    expired = [key for key, (_, expires_at) in poster_cache.items() if expires_at <= now]
    for key in expired:
        poster_cache.pop(key, None)
    poster_key = secrets.token_urlsafe(12)
    poster_cache[poster_key] = (render_final_survivor_poster(survivor, payout), now + POSTER_TTL_SECONDS)
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
        self.leaderboard_views: dict[str, str] = {}
        self.leaderboard_scopes: dict[str, str] = {}
        self.group_contexts: dict[str, str] = {}
        self.spectator_contexts: dict[str, bool] = {}

    async def connect(self, websocket: WebSocket, user_id: str, is_pit_boss: bool = False, group_ref: str = "", spectator: bool = False):
        await websocket.accept()
        previous_connection = self.active_connections.get(user_id)
        self.active_connections[user_id] = websocket
        self.leaderboard_views.setdefault(user_id, "competitive")
        self.group_contexts[user_id] = clean_group_ref(group_ref)
        self.leaderboard_scopes[user_id] = self.group_contexts[user_id]
        self.spectator_contexts[user_id] = bool(spectator)
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
            self.leaderboard_views.pop(user_id, None)
            self.leaderboard_scopes.pop(user_id, None)
            self.group_contexts.pop(user_id, None)
            self.spectator_contexts.pop(user_id, None)

    async def send_state(self, user_id: str, event_type: str, **extra):
        connection = self.active_connections.get(user_id)
        if connection is None:
            return
        try:
            group_ref = self.group_contexts.get(user_id, "")
            group_seasons = (
                await group_season_archive_payload(group_ref)
                if group_ref and event_type in {"welcome", "sploded", "season_archive"}
                else None
            )
            await connection.send_json(
                {
                    "type": event_type,
                    "state": public_game_state(),
                    "balance": await get_balance(user_id),
                    "daily_claim": await daily_claim_status(user_id),
                    "leaderboard": await public_leaderboard_payload(
                        user_id,
                        self.leaderboard_views.get(user_id, "competitive"),
                        self.leaderboard_scopes.get(user_id, ""),
                    ),
                    "group_context_available": bool(self.group_contexts.get(user_id)),
                    "spectator_mode": bool(self.spectator_contexts.get(user_id)),
                    "group_seasons": group_seasons,
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

    async def broadcast_leaderboard(self) -> None:
        for user_id in list(self.active_connections):
            await self.send_state(user_id, "leaderboard_refresh")

    async def broadcast_spectator_reaction(self, reaction: str, is_ghost: bool = False) -> None:
        """Deliver one anonymous, non-gameplay spectator reaction without sending profile data."""
        payload = {"type": "spectator_reaction", "reaction": reaction, "ghost": is_ghost}
        for connection in list(self.active_connections.values()):
            try:
                await connection.send_json(payload)
            except Exception:
                pass

    async def broadcast_taunt(self, taunt_text: str) -> None:
        """Deliver one anonymous taunt line aimed at whoever's holding the bomb."""
        payload = {"type": "taunt", "text": taunt_text}
        for connection in list(self.active_connections.values()):
            try:
                await connection.send_json(payload)
            except Exception:
                pass


manager = ConnectionManager()


def public_game_state() -> dict:
    """Redact the live server seed and crash point while a round is running.

    game_state is broadcast wholesale on every event, which previously leaked
    the round's crash multiplier and unhashed seed the instant a round armed
    -- before the fuse even started ticking. Both values are restored once the
    round has actually ended (intermission/ended/lobby), so clients can still
    verify hashed_seed == sha256(server_seed) and reproduce crash_point via
    generate_crash() after the fact, which is what "provably fair" requires.
    """
    safe = dict(game_state)
    if safe.get("phase") == "running":
        safe["server_seed"] = ""
        safe["crash_point"] = None
    return safe


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


async def current_lobby_card(group_ref: str = "") -> dict:
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
    safe_group_ref = clean_group_ref(group_ref)
    start_param = f"join_{safe_group_ref}" if safe_group_ref else "join"
    watch_param = f"watch_{safe_group_ref}" if safe_group_ref else "watch"
    button = (
        {
            "text": "JOIN THE LOBBY — 100 ◉",
            "url": f"https://t.me/{BOT_USERNAME}?startapp={start_param}",
        }
        if phase == "lobby"
        else {
            "text": "👁 WATCH THE ASH SETTLE"
            if phase == "intermission"
            else "👁 WATCH THE FUSE LIVE",
            "url": f"https://t.me/{BOT_USERNAME}?startapp={watch_param}",
        }
    )
    keyboard_rows = [[button]]
    # Nudge/leaderboard-drop only make sense for a registered group card, since
    # we need a known chat_id to post a *new* message into -- the anonymous
    # inline-shared card has no chat_id the bot can address directly.
    if safe_group_ref:
        second_row = []
        if phase == "lobby":
            second_row.append({"text": "📣 NUDGE THE GROUP", "callback_data": f"nudge:{safe_group_ref}"})
        second_row.append({"text": "🏆 LEADERBOARD", "callback_data": f"leaderboard_drop:{safe_group_ref}"})
        keyboard_rows.append(second_row)
    champion_label = await reigning_champion_label(safe_group_ref) if safe_group_ref else None
    champion_line = f"🏆 <b>Reigning champion:</b> {champion_label}\n\n" if champion_label else ""
    text = (
        "💣 <b>DON'T SPLODE</b> 💣\n"
        "━━━━━━━━━━━━\n\n"
        f"{champion_line}"
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
        "reply_markup": {"inline_keyboard": keyboard_rows},
    }


def public_remaining_players_caption(players: list[dict]) -> str:
    """Format the remaining public player labels for a Telegram caption without exposing IDs."""
    labels = [escape(public_player_label(player)) for player in players[:MAX_PLAYERS]]
    return " • ".join(labels) if labels else "Nobody. The cabinet got hungry."


def current_round_result_card(payout: float, survivors: list[dict], group_ref: str = "") -> tuple[str, dict]:
    """Build a compact public final-match result without private player data."""
    multiplier = round(float(game_state["multiplier"]), 2)
    survivor_count = len(survivors)
    survivor_label = "SOUL" if survivor_count == 1 else "SOULS"
    remaining_players = public_remaining_players_caption(survivors)
    predictions = game_state.get("predictions") or {}
    prediction_line = ""
    if predictions and survivors:
        winner_id = str(survivors[0]["id"])
        correct = sum(1 for guess in predictions.values() if str(guess) == winner_id)
        if correct:
            caller_label = "caller" if correct == 1 else "callers"
            prediction_line = f"🔮 <b>{correct}</b> {caller_label} saw it coming.\n\n"
    text = (
        "💥 <b>DON'T SPLODE — ROUND RESULT</b> 💥\n"
        "━━━━━━━━━━━━\n\n"
        "<b>THE FUSE WON.</b>\n\n"
        f"Crash: <b>{multiplier:.2f}×</b>\n"
        f"Last standing: <b>{survivor_count} {survivor_label}</b> — <b>{remaining_players}</b>\n"
        f"Final pot: <b>{payout:.2f} ◉</b>\n\n"
        f"{prediction_line}"
        "<i>The cabinet swept up the ash. The next lobby needs fresh volunteers.</i>"
    )
    safe_group_ref = clean_group_ref(group_ref)
    start_param = f"watch_{safe_group_ref}" if safe_group_ref else "watch"
    keyboard_rows = [
        [
            {
                "text": "VIEW THE CABINET RECORD",
                "url": f"https://t.me/{BOT_USERNAME}?startapp={start_param}",
            }
        ]
    ]
    # A rematch button needs a known chat_id to post the fresh lobby card into,
    # which only exists for a registered group -- not the anonymous inline card.
    if safe_group_ref:
        keyboard_rows.append([{"text": "🔁 REMATCH", "callback_data": f"rematch:{safe_group_ref}"}])
    markup = {"inline_keyboard": keyboard_rows}
    return text, markup


def current_elimination_card(loser: dict | None, survivors: list[dict], group_ref: str = "") -> tuple[str, dict]:
    """Build the caption and sealed-lobby control for a public elimination poster."""
    victim = escape(public_player_label(loser))
    multiplier = round(float(game_state["multiplier"]), 2)
    survivor_count = len(survivors)
    soul_label = "SOUL" if survivor_count == 1 else "SOULS"
    remaining_players = public_remaining_players_caption(survivors)
    text = (
        f"💥 <b>OPE, {victim} SPLODED.</b>\n\n"
        f"Crash: <b>{multiplier:.2f}×</b>\n"
        f"Still breathing: <b>{survivor_count} {soul_label}</b>\n"
        f"<b>Remaining with a pulse:</b> {remaining_players}\n\n"
        "<i>The cabinet has accepted a fresh contribution to the ash pile.</i>"
    )
    safe_group_ref = clean_group_ref(group_ref)
    start_param = f"watch_{safe_group_ref}" if safe_group_ref else "watch"
    markup = {"inline_keyboard": [[{"text": "👁 WATCH THE NEXT FUSE", "url": f"https://t.me/{BOT_USERNAME}?startapp={start_param}"}]]}
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


async def telegram_group_admin(chat_id: str, user_id: str) -> bool:
    """Verify a caller administers the target group before recording group metadata."""
    ok, result = await telegram_api_call("getChatMember", {"chat_id": chat_id, "user_id": user_id})
    status = str(((result.get("result") or {}).get("status") or "")) if ok else ""
    return status in {"creator", "administrator"}


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


async def edit_group_caption(chat_id: str, message_id: int, text: str, markup: dict) -> bool:
    """Update a registered group’s photo-card caption without exposing the group ID to Mini App clients."""
    ok, result = await telegram_api_call(
        "editMessageCaption",
        {
            "chat_id": chat_id,
            "message_id": message_id,
            "caption": text,
            "parse_mode": "HTML",
            "reply_markup": markup,
        },
    )
    return ok or "message is not modified" in str(result.get("description", "")).lower()


async def edit_group_media(chat_id: str, message_id: int, poster_key: str, text: str, markup: dict) -> bool:
    """Transform a registered group’s native photo card at the authoritative elimination or final result."""
    ok, _ = await telegram_api_call(
        "editMessageMedia",
        {
            "chat_id": chat_id,
            "message_id": message_id,
            "media": {
                "type": "photo",
                "media": f"{PUBLIC_BACKEND_URL}/telegram/posters/{poster_key}.png",
                "caption": text,
                "parse_mode": "HTML",
            },
            "reply_markup": markup,
        },
    )
    return ok


async def send_registered_group_lobby_card(group: dict) -> bool:
    """Send and track a photo-first lobby card after an explicit in-group registration command."""
    chat_id = str(group.get("chat_id", ""))
    if not chat_id:
        return False
    card = await current_lobby_card(clean_group_ref(group.get("ref")))
    ok, result = await telegram_api_call(
        "sendPhoto",
        {
            "chat_id": chat_id,
            "photo": card["photo_url"],
            "caption": card["caption"],
            "parse_mode": "HTML",
            "reply_markup": card["reply_markup"],
        },
    )
    message_id = (result.get("result") or {}).get("message_id") if ok else None
    if not message_id:
        return False
    record = {"chat_id": chat_id, "message_id": int(message_id), "group_ref": group["ref"]}
    await redis_client.hset(ACTIVE_GROUP_CARDS_KEY, f"{chat_id}:{message_id}", json.dumps(record, separators=(",", ":")))
    return True


async def active_group_cards() -> list[tuple[str, dict]]:
    """Load valid native group cards; malformed records are discarded defensively."""
    if not hasattr(redis_client, "hgetall"):
        return []
    records: list[tuple[str, dict]] = []
    for card_key, raw in (await redis_client.hgetall(ACTIVE_GROUP_CARDS_KEY)).items():
        try:
            record = json.loads(raw)
            if str(record.get("chat_id")) and int(record.get("message_id")):
                records.append((card_key, record))
        except (TypeError, ValueError, json.JSONDecodeError):
            await redis_client.hdel(ACTIVE_GROUP_CARDS_KEY, card_key)
    return records


async def refresh_lobby_cards() -> None:
    """Update every tracked lobby card from public authoritative game state."""
    card_ids = await redis_client.smembers(ACTIVE_LOBBY_CARDS_KEY)
    card = await current_lobby_card()
    text = card["caption"]
    markup = card["reply_markup"]
    outcomes = await asyncio.gather(*(edit_inline_caption(card_id, text, markup) for card_id in card_ids)) if card_ids else []
    stale_ids = [card_id for card_id, ok in zip(card_ids, outcomes) if not ok]
    if stale_ids:
        print(f"Removing {len(stale_ids)} stale Telegram inline card(s)")
        await redis_client.srem(ACTIVE_LOBBY_CARDS_KEY, *stale_ids)
    group_cards = await active_group_cards()
    group_card_content = await asyncio.gather(
        *(current_lobby_card(record.get("group_ref", "")) for _, record in group_cards)
    ) if group_cards else []
    group_outcomes = await asyncio.gather(
        *(
            edit_group_caption(record["chat_id"], record["message_id"], content["caption"], content["reply_markup"])
            for (_, record), content in zip(group_cards, group_card_content)
        )
    ) if group_cards else []
    stale_group_keys = [card_key for (card_key, _), ok in zip(group_cards, group_outcomes) if not ok]
    if stale_group_keys:
        await redis_client.hdel(ACTIVE_GROUP_CARDS_KEY, *stale_group_keys)


async def publish_elimination_poster(loser: dict | None, survivors: list[dict]) -> None:
    """Transform every tracked group lobby card into the current public knockout poster."""
    card_ids = await redis_client.smembers(ACTIVE_LOBBY_CARDS_KEY)
    poster_key = cache_elimination_poster(loser, len(survivors))
    text, markup = current_elimination_card(loser, survivors)
    outcomes = await asyncio.gather(*(edit_inline_media(card_id, poster_key, text, markup) for card_id in card_ids)) if card_ids else []
    stale_ids = [card_id for card_id, ok in zip(card_ids, outcomes) if not ok]
    if stale_ids:
        await redis_client.srem(ACTIVE_LOBBY_CARDS_KEY, *stale_ids)
    group_cards = await active_group_cards()
    group_outcomes = await asyncio.gather(
        *(edit_group_media(record["chat_id"], record["message_id"], poster_key, text, markup) for _, record in group_cards)
    ) if group_cards else []
    stale_group_keys = [card_key for (card_key, _), ok in zip(group_cards, group_outcomes) if not ok]
    if stale_group_keys:
        await redis_client.hdel(ACTIVE_GROUP_CARDS_KEY, *stale_group_keys)


async def publish_round_results(payout: float, survivors: list[dict]) -> None:
    """Transform the live group card into a winner-specific final photo result."""
    card_ids = await redis_client.smembers(ACTIVE_LOBBY_CARDS_KEY)
    text, markup = current_round_result_card(payout, survivors)
    winner = survivors[0] if survivors else None
    poster_key = cache_final_survivor_poster(winner, payout)
    outcomes = await asyncio.gather(*(edit_inline_media(card_id, poster_key, text, markup) for card_id in card_ids)) if card_ids else []
    stale_ids = [card_id for card_id, ok in zip(card_ids, outcomes) if not ok]
    if stale_ids:
        await redis_client.srem(ACTIVE_LOBBY_CARDS_KEY, *stale_ids)
    group_cards = await active_group_cards()
    group_outcomes = await asyncio.gather(
        *(edit_group_media(record["chat_id"], record["message_id"], poster_key, text, markup) for _, record in group_cards)
    ) if group_cards else []
    stale_group_keys = [card_key for (card_key, _), ok in zip(group_cards, group_outcomes) if not ok]
    if stale_group_keys:
        await redis_client.hdel(ACTIVE_GROUP_CARDS_KEY, *stale_group_keys)
    for _, record in group_cards:
        await touch_registered_group(record["group_ref"], "games_completed")
    await redis_client.delete(ACTIVE_LOBBY_CARDS_KEY)
    await redis_client.delete(ACTIVE_GROUP_CARDS_KEY)


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
                "message",
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
            "results": [await current_lobby_card()],
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

    callback_data = str(callback_query.get("data", ""))
    callback_message = callback_query.get("message") or {}
    callback_chat_id = str((callback_message.get("chat") or {}).get("id", ""))

    async def answer_callback(text: str, show_alert: bool = False) -> None:
        await telegram_api_call(
            "answerCallbackQuery",
            {"callback_query_id": callback_query["id"], "text": text, "show_alert": show_alert},
        )

    async def resolve_callback_group() -> dict | None:
        """Only trust a callback's embedded group_ref if it matches the chat it actually came from."""
        target_ref = clean_group_ref(callback_data.split(":", 1)[1]) if ":" in callback_data else ""
        group = await registered_group_for_chat(callback_chat_id) if callback_chat_id else None
        if not group or not target_ref or clean_group_ref(group.get("ref")) != target_ref:
            return None
        return group

    if callback_data.startswith("rematch:"):
        group = await resolve_callback_group()
        if not group:
            await answer_callback("This cabinet isn't registered anymore.", show_alert=True)
            return {"ok": True}
        if game_state["phase"] != "lobby" or game_state["players"]:
            await answer_callback("A round's already brewing. Wait for it to clear.", show_alert=True)
            return {"ok": True}
        sent = await send_registered_group_lobby_card(group)
        await answer_callback("Fresh lobby posted." if sent else "Telegram refused the new card. Try again.")
        return {"ok": True}

    if callback_data.startswith("leaderboard_drop:"):
        group = await resolve_callback_group()
        if not group:
            await answer_callback("This cabinet isn't registered anymore.", show_alert=True)
            return {"ok": True}
        await telegram_api_call(
            "sendMessage",
            {"chat_id": callback_chat_id, "text": await group_leaderboard_message(group), "parse_mode": "HTML"},
        )
        await answer_callback("Standings posted.")
        return {"ok": True}

    if callback_data.startswith("nudge:"):
        group = await resolve_callback_group()
        if not group:
            await answer_callback("This cabinet isn't registered anymore.", show_alert=True)
            return {"ok": True}
        if game_state["phase"] != "lobby":
            await answer_callback("The lobby's already sealed.", show_alert=True)
            return {"ok": True}
        if not await claim_action_slot(f"group:{clean_group_ref(group.get('ref'))}", "nudge", NUDGE_COOLDOWN_SECONDS):
            await answer_callback("Already nudged recently. Give it a minute.", show_alert=True)
            return {"ok": True}
        needed = max(0, MINIMUM_COUNTDOWN_PLAYERS - len(game_state["players"]))
        nudge_text = f"📣 Need {needed} more to light this!" if needed else "📣 Enough to light — someone hold LIGHT IT UP!"
        await telegram_api_call("sendMessage", {"chat_id": callback_chat_id, "text": nudge_text})
        await answer_callback("Nudge sent.")
        return {"ok": True}

    message = update.get("message") or {}
    chat = message.get("chat") or {}
    command = str(message.get("text") or "").strip().split(maxsplit=1)[0].lower()
    chat_id = str(chat.get("id", ""))
    actor_id = str((message.get("from") or {}).get("id", ""))
    if command.startswith("/dont_splode_health"):
        if actor_id not in PIT_BOSS_IDS:
            return {
                "method": "sendMessage",
                "chat_id": chat_id,
                "text": "That diagnostic hatch is reserved for the Pit Boss.",
            }
        return {"method": "sendMessage", "chat_id": chat_id, "text": await pit_boss_health_message(), "parse_mode": "HTML"}
    if command.startswith("/leaderboard"):
        group = await registered_group_for_chat(chat_id)
        if not group:
            return {
                "method": "sendMessage",
                "chat_id": chat_id,
                "text": "This cabinet is not registered yet. A group administrator can use /register_dont_splode first.",
            }
        return {"method": "sendMessage", "chat_id": chat_id, "text": await group_leaderboard_message(group), "parse_mode": "HTML"}
    if command.startswith("/register_dont_splode"):
        authorized = actor_id in PIT_BOSS_IDS or await telegram_group_admin(chat_id, actor_id)
        if not authorized:
            return {
                "method": "sendMessage",
                "chat_id": chat_id,
                "text": "Only a group administrator or the Pit Boss may register this cabinet.",
            }
        group = await register_telegram_group(chat)
        if not group:
            return {
                "method": "sendMessage",
                "chat_id": chat_id,
                "text": "This command belongs in a normal Telegram group, not a private chat.",
            }
        sent = await send_registered_group_lobby_card(group)
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": "Cabinet registered. A live lobby card has been posted." if sent else "Cabinet registered, but Telegram refused the lobby card. Try again in a moment.",
        }
    return {"ok": True}


def reset_round_state():
    """Clear only transient game state while preserving Redis-backed balances."""
    round_group_rosters.clear()
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
            "predictions": {},
        }
    )


async def reset_lobby_after_cooldown():
    """Keep an incident visible for five seconds, then open the next lobby."""
    global reset_task
    try:
        await asyncio.sleep(FINAL_LOBBY_RESET_SECONDS)
        async with game_lock:
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
    for _, record in await active_group_cards():
        await touch_registered_group(record["group_ref"], "games_started")
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
            async with game_lock:
                if game_state["phase"] != "lobby":
                    return
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
        async with game_lock:
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
            async with game_lock:
                if game_state["phase"] != "running":
                    break
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
        await change_balance(
            survivors[0]["id"],
            payout,
            "final_survivor_payout",
            round_ref=game_state["round_number"],
        )
        await update_player_profile(survivors[0]["id"], matches_survived=1, total_pot_won=payout)
    if loser:
        await update_player_profile(loser["id"], eliminations=1)

    if is_final:
        # Retain only public-safe match statistics after the lobby reopens.
        for group_ref, participant_ids in list(round_group_rosters.items()):
            await record_group_match_results(group_ref, participant_ids, survivors[0]["id"] if survivors else "", payout)
            if survivors:
                await set_reigning_champion(group_ref, survivors[0])
                if await check_and_record_biggest_pot(group_ref, payout):
                    record_chat_id = await chat_id_for_group_ref(group_ref)
                    if record_chat_id:
                        await telegram_api_call(
                            "sendMessage",
                            {
                                "chat_id": record_chat_id,
                                "text": (
                                    "🏆 <b>NEW CABINET RECORD</b> 🏆\n"
                                    f"Biggest pot ever won: <b>{payout:.2f} ◉</b>"
                                ),
                                "parse_mode": "HTML",
                            },
                        )
        game_state["phase"] = "ended"
        game_state["latest_round"] = {
            "multiplier": round(game_state["multiplier"], 2),
            "payout": payout,
            "survivor_count": len(survivors),
            "eliminations": len(game_state["eliminated_players"]),
            "rounds": game_state["round_number"],
        }
        await publish_round_results(payout, survivors)
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
    if len(game_state["eliminated_players"]) == 1:
        # First elimination of this round -- fire a one-off "First Blood" post
        # into every involved group's chat, separate from the card that gets
        # edited in place for every subsequent elimination.
        for group_ref in list(round_group_rosters):
            fb_chat_id = await chat_id_for_group_ref(group_ref)
            if fb_chat_id and loser:
                await telegram_api_call(
                    "sendMessage",
                    {
                        "chat_id": fb_chat_id,
                        "text": f"🩸 <b>FIRST BLOOD.</b> {escape(public_player_label(loser))} didn't even make it to the second pass.",
                        "parse_mode": "HTML",
                    },
                )
    await publish_elimination_poster(loser, survivors)
    await manager.broadcast_state(
        "eliminated",
        loser=loser_id,
        loser_name=loser["name"] if loser else "UNKNOWN VICTIM",
        payout=0.0,
        final=False,
        remaining_players=len(survivors),
    )
    schedule_next_round()


async def handle_player_disconnect(user_id: str) -> None:
    """Remove a dropped connection from the active game instead of letting it brick the round.

    Previously ConnectionManager.disconnect() only cleaned up socket bookkeeping.
    A player who rage-quit mid-round while holding the bomb stayed current_holder
    forever, freezing every other player's ability to pass with no refund path.
    """
    if game_state["phase"] == "lobby":
        if any(player["id"] == user_id for player in game_state["players"]):
            game_state["players"] = [p for p in game_state["players"] if p["id"] != user_id]
            ready_players = {str(pid) for pid in game_state.get("ready_players", [])}
            ready_players.discard(user_id)
            game_state["ready_players"] = sorted(ready_players)
            await refresh_lobby_cards()
            await manager.broadcast_state("update")
        return

    if game_state["phase"] not in {"running", "intermission"}:
        return

    if not any(player["id"] == user_id for player in game_state["players"]):
        return

    was_holder = game_state["current_holder"] == user_id
    survivors = [p for p in game_state["players"] if p["id"] != user_id]
    game_state["players"] = survivors
    game_state["eliminated_players"].append({"id": user_id, "name": "DISCONNECTED", "public_handle": ""})

    if not survivors:
        game_state["current_holder"] = None
        game_state["phase"] = "ended"
        game_state["latest_round"] = {
            "multiplier": round(game_state["multiplier"], 2),
            "payout": 0.0,
            "survivor_count": 0,
            "eliminations": len(game_state["eliminated_players"]),
            "rounds": game_state["round_number"],
        }
        await manager.broadcast_state(
            "sploded", loser=user_id, loser_name="DISCONNECTED", payout=0.0, final=True
        )
        schedule_lobby_reset()
        return

    if len(survivors) == 1:
        payout = round(game_state["pot"], 2)
        await change_balance(
            survivors[0]["id"], payout, "final_survivor_payout", round_ref=game_state["round_number"]
        )
        await update_player_profile(survivors[0]["id"], matches_survived=1, total_pot_won=payout)
        for group_ref, participant_ids in list(round_group_rosters.items()):
            await record_group_match_results(group_ref, participant_ids, survivors[0]["id"], payout)
            await set_reigning_champion(group_ref, survivors[0])
            if await check_and_record_biggest_pot(group_ref, payout):
                record_chat_id = await chat_id_for_group_ref(group_ref)
                if record_chat_id:
                    await telegram_api_call(
                        "sendMessage",
                        {
                            "chat_id": record_chat_id,
                            "text": f"🏆 <b>NEW CABINET RECORD</b> 🏆\nBiggest pot ever won: <b>{payout:.2f} ◉</b>",
                            "parse_mode": "HTML",
                        },
                    )
        game_state["phase"] = "ended"
        game_state["current_holder"] = None
        game_state["latest_round"] = {
            "multiplier": round(game_state["multiplier"], 2),
            "payout": payout,
            "survivor_count": 1,
            "eliminations": len(game_state["eliminated_players"]),
            "rounds": game_state["round_number"],
        }
        await publish_round_results(payout, survivors)
        await manager.broadcast_state(
            "sploded", loser=user_id, loser_name="DISCONNECTED", payout=payout, final=True
        )
        schedule_lobby_reset()
        return

    if was_holder:
        game_state["current_holder"] = survivors[0]["id"]
    await manager.broadcast_state(
        "player_disconnected", loser=user_id, loser_name="DISCONNECTED", remaining_players=len(survivors)
    )


@app.websocket("/ws/{user_id}/{user_name}")
async def websocket_endpoint(
    websocket: WebSocket,
    user_id: str,
    user_name: str,
    tg_init_data: str = "",
):
    verified_user = verified_telegram_user(tg_init_data)
    verified_start_param = verified_telegram_start_param(tg_init_data)
    signed_join_ref = clean_group_ref(verified_start_param.removeprefix("join_") if verified_start_param.startswith("join_") else "")
    signed_watch_ref = clean_group_ref(verified_start_param.removeprefix("watch_") if verified_start_param.startswith("watch_") else "")
    spectator_mode = bool(signed_watch_ref)
    signed_group_ref = signed_watch_ref or signed_join_ref
    verified_user_id = str(verified_user["id"]) if verified_user else None
    raw_handle = str((verified_user or {}).get("username") or "")
    public_handle = raw_handle if re.fullmatch(r"[A-Za-z0-9_]{5,32}", raw_handle) else ""
    is_pit_boss = bool(
        verified_user_id
        and verified_user_id == user_id
        and verified_user_id in PIT_BOSS_IDS
    )
    await manager.connect(websocket, user_id, is_pit_boss, signed_group_ref, spectator_mode)
    await ensure_player_profile(user_id, verified_user, user_name)
    await manager.send_state(user_id, "welcome")

    try:
        while True:
            data = await websocket.receive_json()
            if manager.active_connections.get(user_id) is not websocket:
                await websocket.close(code=4001, reason="A newer game session replaced this one.")
                break
            action = data.get("action")

            async with game_lock:
                if action == "join":
                    if manager.spectator_contexts.get(user_id):
                        await reject_action(user_id, "This cabinet launch is watch-only. Enter through a live lobby card to sign the waiver.")
                        continue
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

                    await change_balance(
                        user_id,
                        -JOIN_COST,
                        "join_buy_in",
                        round_ref=game_state["round_number"],
                    )
                    await update_player_profile(user_id, matches_entered=1)
                    game_state["pot"] += JOIN_COST
                    game_state["players"].append({"id": user_id, "name": user_name, "public_handle": public_handle})
                    if signed_group_ref:
                        round_group_rosters.setdefault(signed_group_ref, set()).add(user_id)
                    if (
                        len(game_state["players"]) >= MINIMUM_COUNTDOWN_PLAYERS
                        and not game_state.get("lobby_auto_start_at")
                    ):
                        game_state["lobby_auto_start_at"] = time.time() + LOBBY_AUTO_IGNITE_SECONDS
                        schedule_lobby_ignition()
                    if not await maybe_ignite_lobby():
                        await refresh_lobby_cards()
                        await manager.broadcast_state("update")

                elif action == "leave_lobby":
                    if game_state["phase"] != "lobby":
                        await reject_action(user_id, "The waiver is binding once the fuse is lit. Ride this one out.")
                        continue
                    if not any(player["id"] == user_id for player in game_state["players"]):
                        await reject_action(user_id, "You never signed the waiver, so there's nothing to tear up.")
                        continue

                    game_state["players"] = [p for p in game_state["players"] if p["id"] != user_id]
                    ready_players = {str(pid) for pid in game_state.get("ready_players", [])}
                    ready_players.discard(user_id)
                    game_state["ready_players"] = sorted(ready_players)
                    game_state["pot"] = max(0.0, game_state["pot"] - JOIN_COST)
                    if signed_group_ref and signed_group_ref in round_group_rosters:
                        round_group_rosters[signed_group_ref].discard(user_id)
                    await change_balance(
                        user_id,
                        JOIN_COST,
                        "leave_lobby_refund",
                        round_ref=game_state["round_number"],
                    )
                    # Below the countdown threshold, cancel the pending auto-ignition --
                    # same rule join() uses in reverse.
                    if len(game_state["players"]) < MINIMUM_COUNTDOWN_PLAYERS:
                        game_state["lobby_auto_start_at"] = 0.0
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

                    await change_balance(user_id, DAILY_CHIP_GRANT, "daily_chip_cache")
                    await manager.send_state(
                        user_id,
                        "daily_claimed",
                        claim_amount=DAILY_CHIP_GRANT,
                    )
                    await manager.broadcast_leaderboard()

                elif action == "leaderboard":
                    requested_view = "chips" if str(data.get("view", "")).lower() == "chips" else "competitive"
                    requested_scope = str(data.get("scope", "")).lower()
                    manager.leaderboard_views[user_id] = requested_view
                    manager.leaderboard_scopes[user_id] = manager.group_contexts.get(user_id, "") if requested_scope == "group" else ""
                    await manager.send_state(user_id, "leaderboard")

                elif action == "season_archive":
                    group_ref = manager.group_contexts.get(user_id, "")
                    if not group_ref:
                        await reject_action(user_id, "Open the cabinet from a registered group card to inspect that group’s season file.")
                        continue
                    await manager.send_state(
                        user_id,
                        "season_archive",
                        group_seasons=await group_season_archive_payload(group_ref),
                    )

                elif action == "spectator_reaction":
                    is_ghost = any(str(p["id"]) == user_id for p in game_state["eliminated_players"])
                    is_spectator = bool(manager.spectator_contexts.get(user_id))
                    if not is_ghost and not is_spectator:
                        await reject_action(user_id, "Only watch-only cabinet visitors may trigger the reaction rail.")
                        continue
                    if game_state["phase"] not in {"running", "intermission"}:
                        await reject_action(user_id, "The reaction rail wakes only while the fuse is live or the ash is settling.")
                        continue
                    reaction = str(data.get("reaction", ""))
                    allowed_reactions = GHOST_REACTIONS if is_ghost else SPECTATOR_REACTIONS
                    if reaction not in allowed_reactions:
                        await reject_action(user_id, "That reaction is not mounted on this cabinet.")
                        continue
                    if not await claim_action_slot(user_id, "spectator_reaction", SPECTATOR_REACTION_COOLDOWN_SECONDS):
                        await reject_action(user_id, "The reaction rail needs a moment before another outburst.")
                        continue
                    await manager.broadcast_spectator_reaction(reaction, is_ghost=is_ghost)
                    sticker_file_id = SPECTATOR_STICKER_FILE_IDS.get(reaction)
                    if sticker_file_id and signed_group_ref:
                        if await claim_action_slot(f"group:{signed_group_ref}", "sticker_post", SPECTATOR_STICKER_POST_COOLDOWN_SECONDS):
                            sticker_chat_id = await chat_id_for_group_ref(signed_group_ref)
                            if sticker_chat_id:
                                await telegram_api_call("sendSticker", {"chat_id": sticker_chat_id, "sticker": sticker_file_id})

                elif action == "taunt":
                    is_ghost = any(str(p["id"]) == user_id for p in game_state["eliminated_players"])
                    is_spectator = bool(manager.spectator_contexts.get(user_id))
                    if not is_ghost and not is_spectator:
                        await reject_action(user_id, "Only watch-only cabinet visitors may taunt the current holder.")
                        continue
                    if game_state["phase"] != "running":
                        await reject_action(user_id, "There's nobody sweating to taunt right now.")
                        continue
                    try:
                        taunt_index = int(data.get("taunt_id"))
                    except (TypeError, ValueError):
                        await reject_action(user_id, "Pick an actual taunt off the list.")
                        continue
                    if not 0 <= taunt_index < len(TAUNT_LINES):
                        await reject_action(user_id, "That taunt doesn't exist. Yet.")
                        continue
                    if not await claim_action_slot(user_id, "taunt", TAUNT_COOLDOWN_SECONDS):
                        await reject_action(user_id, "Let the last taunt land before firing another.")
                        continue
                    await manager.broadcast_taunt(TAUNT_LINES[taunt_index])

                elif action == "predict_survivor":
                    if game_state["phase"] != "lobby":
                        await reject_action(user_id, "Predictions lock the moment the fuse lights. Too slow.")
                        continue
                    predicted_id = str(data.get("predicted_player_id", ""))
                    if not any(str(p["id"]) == predicted_id for p in game_state["players"]):
                        await reject_action(user_id, "Pick someone who's actually signed the waiver.")
                        continue
                    game_state.setdefault("predictions", {})[user_id] = predicted_id
                    await manager.send_state(user_id, "prediction_locked", predicted_player_id=predicted_id)

                elif action == "pit_boss_master_reset":
                    if user_id not in manager.pit_boss_connections:
                        await reject_action(user_id, "Only a verified Pit Boss may reset the virtual chip cabinet.")
                        continue
                    if game_state["phase"] != "lobby" or game_state["players"]:
                        await reject_action(user_id, "Master reset is locked until the lobby is empty and no fuse is active.")
                        continue
                    confirmation = str(data.get("confirmation", "")).strip().upper()
                    note = clean_profile_name(data.get("reason", ""), "")[:96]
                    if confirmation != MASTER_RESET_PHRASE:
                        await reject_action(user_id, f"Type {MASTER_RESET_PHRASE} exactly before resetting every virtual stack.")
                        continue
                    if len(note.strip()) < 3:
                        await reject_action(user_id, "Write a short reset reason for the cabinet audit file.")
                        continue
                    if not await claim_action_slot(user_id, "pit_boss_master_reset", MASTER_RESET_COOLDOWN_SECONDS):
                        await reject_action(user_id, "The master reset lever is cooling down. One moment.")
                        continue
                    changed_count = await master_reset_virtual_chips(user_id, note)
                    dashboard = await pit_boss_dashboard_payload()
                    await manager.send_state(
                        user_id,
                        "pit_boss_master_reset",
                        reset_count=changed_count,
                        pit_boss_dashboard=dashboard,
                    )
                    await manager.broadcast_leaderboard()

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

                    await change_balance(
                        target_id,
                        grant_amount,
                        "pit_boss_grant",
                        actor_id=user_id,
                    )
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
                    await manager.broadcast_leaderboard()

                elif action == "pit_boss_dashboard":
                    if user_id not in manager.pit_boss_connections:
                        await reject_action(user_id, "Only a verified Pit Boss may inspect the cabinet ledger.")
                        continue
                    profile_ref = str(data.get("profile_ref", ""))[:32]
                    search = clean_profile_name(data.get("search", ""), "")[:48]
                    sort = str(data.get("sort", "balance_desc"))[:24]
                    dashboard = await pit_boss_dashboard_payload(profile_ref, search, sort)
                    await manager.send_state(user_id, "pit_boss_dashboard", pit_boss_dashboard=dashboard)

                elif action == "pit_boss_adjust":
                    if user_id not in manager.pit_boss_connections:
                        await reject_action(user_id, "Only a verified Pit Boss may alter the cabinet ledger.")
                        continue
                    target_ref = str(data.get("target_ref", ""))[:32]
                    target_id = await redis_client.hget(PLAYER_PROFILE_REFS_KEY, target_ref)
                    if not target_id:
                        await reject_action(user_id, "Choose a known player profile before touching the ledger.")
                        continue
                    direction = str(data.get("direction", "")).lower()
                    try:
                        amount = float(data.get("amount"))
                    except (TypeError, ValueError):
                        amount = 0.0
                    note = clean_profile_name(data.get("reason", ""), "")[:96]
                    if direction not in {"add", "remove"}:
                        await reject_action(user_id, "Choose whether the cabinet adds or removes chips.")
                        continue
                    if (
                        not math.isfinite(amount)
                        or amount != math.floor(amount)
                        or not PIT_BOSS_MIN_GRANT <= amount <= PIT_BOSS_MAX_GRANT
                    ):
                        await reject_action(
                            user_id,
                            f"Ledger adjustments must be whole amounts from {PIT_BOSS_MIN_GRANT:.0f} to {PIT_BOSS_MAX_GRANT:.0f} ◉.",
                        )
                        continue
                    if len(note.strip()) < 3:
                        await reject_action(user_id, "Write a short reason before changing a persistent chip balance.")
                        continue
                    if not await claim_action_slot(user_id, "pit_boss_adjust", PIT_BOSS_GRANT_COOLDOWN_SECONDS):
                        await reject_action(user_id, "The ledger clerk is still stamping the last adjustment. One moment.")
                        continue
                    signed_amount = amount if direction == "add" else -amount
                    try:
                        updated_balance = await change_balance(
                            str(target_id),
                            signed_amount,
                            f"pit_boss_{'credit' if direction == 'add' else 'debit'}",
                            actor_id=user_id,
                            metadata={"note": note},
                        )
                    except ValueError:
                        await reject_action(user_id, "The cabinet will not take a player below zero virtual chips.")
                        continue
                    await manager.send_state(
                        str(target_id),
                        "pit_boss_adjusted",
                        adjustment_amount=signed_amount,
                        adjustment_reason=note,
                    )
                    dashboard = await pit_boss_dashboard_payload(target_ref)
                    await manager.send_state(
                        user_id,
                        "pit_boss_adjusted",
                        adjustment_amount=signed_amount,
                        adjustment_reason=note,
                        adjustment_balance=updated_balance,
                        pit_boss_dashboard=dashboard,
                    )
                    await manager.broadcast_leaderboard()

                elif action == "pit_boss_delete_player":
                    if user_id not in manager.pit_boss_connections:
                        await reject_action(user_id, "Only a verified Pit Boss may erase a player's records.")
                        continue
                    target_ref = str(data.get("target_ref", ""))[:32]
                    target_id = await redis_client.hget(PLAYER_PROFILE_REFS_KEY, target_ref)
                    if not target_id:
                        await reject_action(user_id, "Choose a known player profile before erasing it.")
                        continue
                    confirmation = str(data.get("confirmation", "")).strip().upper()
                    note = clean_profile_name(data.get("reason", ""), "")[:96]
                    if confirmation != DELETE_PLAYER_PHRASE:
                        await reject_action(user_id, f"Type {DELETE_PLAYER_PHRASE} exactly before erasing a player's records.")
                        continue
                    if len(note.strip()) < 3:
                        await reject_action(user_id, "Write a short reason before erasing a player's records.")
                        continue
                    if not await claim_action_slot(user_id, "pit_boss_delete_player", DELETE_PLAYER_COOLDOWN_SECONDS):
                        await reject_action(user_id, "The records room is still cooling down. One moment.")
                        continue
                    removed_summary = await delete_player_completely(str(target_id), target_ref, user_id, note)
                    dashboard = await pit_boss_dashboard_payload()
                    await manager.send_state(
                        user_id,
                        "pit_boss_player_deleted",
                        deleted_ref=target_ref,
                        removed_summary=removed_summary,
                        pit_boss_dashboard=dashboard,
                    )
                    await manager.broadcast_leaderboard()

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
                            await change_balance(
                                user_id,
                                -PASS_FEE,
                                "pass_fee",
                                round_ref=game_state["round_number"],
                            )
                            await update_player_profile(user_id, passes=1)
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
        if manager.active_connections.get(user_id) is None:
            async with game_lock:
                await handle_player_disconnect(user_id)
