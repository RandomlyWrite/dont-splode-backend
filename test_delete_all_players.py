"""Integration test for delete_all_players_completely, run against real Redis.

Run with: python3 test_delete_all_players.py
"""

import asyncio
import json
import main


async def run() -> None:
    r = main.redis_client
    await r.flushdb()

    player_a = "111"
    player_b = "222"
    player_c = "333"

    for uid in (player_a, player_b, player_c):
        await main.ensure_player_profile(uid, fallback_name=f"Player {uid}")
        await main.change_balance(uid, 200.0, "daily_chip_cache")

    group = await main.register_telegram_group({"id": "-100777", "title": "Bulk Test Group", "type": "supergroup"})
    group_ref = group["ref"]
    await main.record_group_match_results(group_ref, {player_a, player_b, player_c}, player_a, 300.0)

    # Sanity: confirm footprint exists for all three before wiping.
    for uid in (player_a, player_b, player_c):
        assert await r.hget(main.BALANCES_KEY, uid) is not None
        assert await r.hget(main.PLAYER_PROFILES_KEY, uid) is not None
    assert len(await r.hgetall(main.PLAYER_PROFILE_REFS_KEY)) == 3
    assert len(await r.hgetall(f"{main.GROUP_COMPETITIVE_PREFIX}{group_ref}")) == 3
    admin_before = await r.lrange(main.ADMIN_LEDGER_KEY, 0, -1)
    assert len(admin_before) >= 3

    # --- The actual bulk deletion ---
    removed = await main.delete_all_players_completely("pitboss1", "full wipe test")

    assert removed["players_removed"] == 3, f"FAIL: expected 3 removed, got {removed}"
    assert removed["groups_cleared"] == 1, f"FAIL: expected 1 group cleared, got {removed}"

    for uid in (player_a, player_b, player_c):
        assert await r.hget(main.BALANCES_KEY, uid) is None, f"FAIL: balance survived for {uid}"
        assert await r.hget(main.PLAYER_PROFILES_KEY, uid) is None, f"FAIL: profile survived for {uid}"
        assert await r.exists(f"{main.PLAYER_LEDGER_PREFIX}{uid}") == 0, f"FAIL: personal ledger survived for {uid}"

    assert await r.exists(main.PLAYER_PROFILE_REFS_KEY) == 0, "FAIL: profile refs hash survived"
    assert await r.exists(f"{main.GROUP_COMPETITIVE_PREFIX}{group_ref}") == 0, "FAIL: group competitive board survived"

    admin_after = await r.lrange(main.ADMIN_LEDGER_KEY, 0, -1)
    assert len(admin_after) == 1, f"FAIL: expected exactly 1 audit entry (the deletion record itself), got {len(admin_after)}"
    deletion_record = json.loads(admin_after[0])
    assert deletion_record["reason"] == "all_player_data_deleted"
    assert deletion_record["metadata"]["players_removed"] == 3
    for uid in (player_a, player_b, player_c):
        assert uid not in deletion_record["target_id"], "FAIL: raw user_id leaked into bulk deletion's own audit entry"

    # --- Registered group itself should be untouched (only player data wiped) ---
    assert await main.registered_group_for_chat("-100777") is not None, "FAIL: group registration was wiped (should only wipe player data)"

    print("ALL DELETE-ALL-PLAYERS INTEGRATION CHECKS PASSED")
    print(f"  removed: {removed}")
    print(f"  admin ledger size before/after: {len(admin_before)} -> {len(admin_after)}")


if __name__ == "__main__":
    asyncio.run(run())
