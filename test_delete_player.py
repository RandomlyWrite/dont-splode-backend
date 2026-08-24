"""Integration test for delete_player_completely, run against a real local Redis
(not a mock) since this is an irreversible destructive operation and deserves
more confidence than a mocked smoke test.

Run with: python3 test_delete_player.py
"""

import asyncio
import json
import main


async def run() -> None:
    r = main.redis_client
    await r.flushdb()

    target_id = "999888777"
    other_id = "111222333"

    # --- Set up a realistic footprint for the target player ---
    profile = await main.ensure_player_profile(target_id, fallback_name="Test Victim")
    target_ref = profile["ref"]
    await main.change_balance(target_id, 500.0, "daily_chip_cache")
    await main.change_balance(target_id, -100.0, "join_buy_in", round_ref=1)
    await main.change_balance(other_id, 50.0, "pit_boss_credit", actor_id="pitboss1", metadata={"note": "test"})

    # Register a group and record match results so group_competitive + season get populated.
    group = await main.register_telegram_group({"id": "-100555", "title": "Test Group", "type": "supergroup"})
    group_ref = group["ref"]
    await main.record_group_match_results(group_ref, {target_id, other_id}, target_id, 150.0)

    # Sanity: confirm the footprint actually exists before deleting.
    assert await r.hget(main.BALANCES_KEY, target_id) is not None
    assert await r.hget(main.PLAYER_PROFILES_KEY, target_id) is not None
    assert await r.hget(main.PLAYER_PROFILE_REFS_KEY, target_ref) == target_id
    assert await r.llen(f"{main.PLAYER_LEDGER_PREFIX}{target_id}") > 0
    assert await r.hget(f"{main.GROUP_COMPETITIVE_PREFIX}{group_ref}", target_id) is not None
    admin_entries_before = await r.lrange(main.ADMIN_LEDGER_KEY, 0, -1)
    target_entries_before = [e for e in admin_entries_before if json.loads(e).get("target_id") == target_id]
    assert len(target_entries_before) >= 2, "setup didn't create admin ledger entries for target"
    other_entries_before = [e for e in admin_entries_before if json.loads(e).get("target_id") == other_id]
    assert len(other_entries_before) >= 1, "setup didn't create admin ledger entry for other player"

    # --- The actual deletion ---
    removed = await main.delete_player_completely(target_id, target_ref, "pitboss1", "test cleanup")

    # --- Verify total erasure of the target ---
    assert await r.hget(main.BALANCES_KEY, target_id) is None, "FAIL: balance survived deletion"
    assert await r.hget(main.PLAYER_PROFILES_KEY, target_id) is None, "FAIL: profile survived deletion"
    assert await r.hget(main.PLAYER_PROFILE_REFS_KEY, target_ref) is None, "FAIL: ref mapping survived deletion"
    assert await r.exists(f"{main.PLAYER_LEDGER_PREFIX}{target_id}") == 0, "FAIL: personal ledger list survived deletion"
    assert await r.hget(f"{main.GROUP_COMPETITIVE_PREFIX}{group_ref}", target_id) is None, "FAIL: group leaderboard entry survived"

    admin_entries_after = await r.lrange(main.ADMIN_LEDGER_KEY, 0, -1)
    target_entries_after = [e for e in admin_entries_after if json.loads(e).get("target_id") == target_id]
    assert len(target_entries_after) == 0, f"FAIL: target's admin ledger entries survived: {target_entries_after}"

    # --- Verify the OTHER player's data is completely untouched ---
    assert await r.hget(main.BALANCES_KEY, other_id) is not None, "FAIL: unrelated player's balance was wiped"
    other_entries_after = [e for e in admin_entries_after if json.loads(e).get("target_id") == other_id]
    assert len(other_entries_after) == len(other_entries_before), "FAIL: unrelated player's ledger entries were touched"
    assert await r.hget(f"{main.GROUP_COMPETITIVE_PREFIX}{group_ref}", other_id) is not None, "FAIL: unrelated player's group entry was wiped"

    # --- Verify the deletion left its own audit trail, without the raw ID ---
    deletion_records = [e for e in admin_entries_after if json.loads(e).get("reason") == "player_data_deleted"]
    assert len(deletion_records) == 1, "FAIL: no audit record of the deletion itself"
    deletion_payload = json.loads(deletion_records[0])
    assert target_id not in deletion_payload["target_id"], "FAIL: raw user_id leaked into the deletion's own audit entry"
    assert target_ref in deletion_payload["target_id"], "FAIL: deletion audit entry doesn't reference the profile ref"

    print("ALL DELETE-PLAYER INTEGRATION CHECKS PASSED")
    print(f"  removed summary: {removed}")
    print(f"  admin ledger size before/after: {len(admin_entries_before)} -> {len(admin_entries_after)}")


if __name__ == "__main__":
    asyncio.run(run())
