"""Focused offline checks for the Last Soul Standing state machine.

Run with: python3 regression_check.py
"""

import asyncio
import json

import main


async def run_checks() -> None:
    for _ in range(250):
        _, _, crash = main.generate_crash()
        assert crash >= main.MINIMUM_CRASH_MULTIPLIER, crash

    broadcasts: list[tuple[str, dict]] = []
    balance_changes: list[tuple[str, float]] = []
    profile_updates: list[tuple[str, dict]] = []
    scheduled: list[str] = []

    async def no_op(*_args, **_kwargs):
        return None

    async def capture_broadcast(event_type: str, **extra):
        broadcasts.append((event_type, extra))

    async def capture_balance(user_id: str, amount: float, *_args, **_kwargs):
        balance_changes.append((user_id, amount))
        return amount

    async def capture_profile_update(user_id: str, **increments):
        profile_updates.append((user_id, increments))
        return {"id": user_id, **increments}

    main.refresh_lobby_cards = no_op
    original_change_balance = main.change_balance
    original_update_profile = main.update_player_profile
    original_publish_round_results = main.publish_round_results
    main.publish_round_results = no_op
    original_publish_elimination_poster = main.publish_elimination_poster
    main.publish_elimination_poster = no_op
    main.manager.broadcast_state = capture_broadcast
    main.change_balance = capture_balance
    main.update_player_profile = capture_profile_update
    main.schedule_next_round = lambda: scheduled.append("next")
    main.schedule_lobby_reset = lambda: scheduled.append("reset")
    print("Simulating: One → Two → Three eliminated; Four takes the final pot.")
    main.game_state.update(
        {
            "phase": "running",
            "players": [
                {"id": "one", "name": "One"},
                {"id": "two", "name": "Two"},
                {"id": "three", "name": "Three"},
                {"id": "four", "name": "Four"},
            ],
            "eliminated_players": [],
            "pot": 415.0,
            "current_holder": "one",
            "multiplier": 2.5,
            "round_number": 1,
            "latest_round": None,
        }
    )

    await main.detonate()
    assert main.game_state["phase"] == "intermission"
    assert [player["id"] for player in main.game_state["players"]] == ["two", "three", "four"]
    assert [player["id"] for player in main.game_state["eliminated_players"]] == ["one"]
    assert broadcasts[-1][0] == "eliminated"
    assert broadcasts[-1][1]["remaining_players"] == 3
    assert scheduled == ["next"]
    assert balance_changes == []
    print("Fuse 01: One eliminated; 3 active; pot carries 415.0 ◉; intermission armed.")

    main.game_state["phase"] = "running"
    main.game_state["current_holder"] = "two"
    main.game_state["multiplier"] = 3.25
    main.game_state["round_number"] = 2
    await main.detonate()
    assert main.game_state["phase"] == "intermission"
    assert [player["id"] for player in main.game_state["players"]] == ["three", "four"]
    assert [player["id"] for player in main.game_state["eliminated_players"]] == ["one", "two"]
    assert broadcasts[-1][0] == "eliminated"
    assert broadcasts[-1][1]["remaining_players"] == 2
    assert scheduled == ["next", "next"]
    assert balance_changes == []
    print("Fuse 02: Two eliminated; 2 active; pot carries 415.0 ◉; intermission armed.")

    main.game_state["phase"] = "running"
    main.game_state["current_holder"] = "three"
    main.game_state["multiplier"] = 4.0
    main.game_state["round_number"] = 3
    await main.detonate()
    assert main.game_state["phase"] == "ended"
    assert [player["id"] for player in main.game_state["players"]] == ["four"]
    assert [player["id"] for player in main.game_state["eliminated_players"]] == ["one", "two", "three"]
    assert balance_changes == [("four", 415.0)]
    assert ("one", {"eliminations": 1}) in profile_updates
    assert ("four", {"matches_survived": 1, "total_pot_won": 415.0}) in profile_updates
    assert broadcasts[-1][0] == "sploded"
    assert broadcasts[-1][1]["final"] is True
    assert scheduled == ["next", "next", "reset"]
    assert main.game_state["latest_round"]["eliminations"] == 3
    assert main.game_state["latest_round"]["rounds"] == 3
    print("Fuse 03: Three eliminated; Four receives 415.0 ◉; final reset armed.")

    main.reset_round_state()
    assert main.game_state["phase"] == "lobby"
    assert main.game_state["players"] == []
    assert main.game_state["eliminated_players"] == []
    assert main.game_state["pot"] == 0.0
    assert main.game_state["round_number"] == 0
    print("Reset: fresh lobby; no active players; no ash record; pot 0.0 ◉.")

    class FakeSocket:
        def __init__(self):
            self.accepted = False
            self.closed: tuple[int, str] | None = None
            self.sent: list[dict] = []

        async def accept(self):
            self.accepted = True

        async def close(self, code: int = 1000, reason: str = ""):
            self.closed = (code, reason)

        async def send_json(self, payload: dict):
            self.sent.append(payload)

    session_manager = main.ConnectionManager()
    first_session = FakeSocket()
    second_session = FakeSocket()
    await session_manager.connect(first_session, "duplicate-user")
    await session_manager.connect(second_session, "duplicate-user")
    assert first_session.closed is not None
    assert first_session.closed[0] == 4001
    assert session_manager.active_connections["duplicate-user"] is second_session
    session_manager.disconnect("duplicate-user", first_session)
    assert session_manager.active_connections["duplicate-user"] is second_session
    session_manager.disconnect("duplicate-user", second_session)
    assert "duplicate-user" not in session_manager.active_connections
    print("Session guard: stale duplicate closes; newer session stays authoritative.")

    verified_session = FakeSocket()
    unverified_replacement = FakeSocket()
    await session_manager.connect(verified_session, "pit-boss", is_pit_boss=True)
    assert "pit-boss" in session_manager.pit_boss_connections
    await session_manager.connect(unverified_replacement, "pit-boss", is_pit_boss=False)
    assert verified_session.closed is not None
    assert "pit-boss" not in session_manager.pit_boss_connections
    assert session_manager.active_connections["pit-boss"] is unverified_replacement
    print("Role guard: unverified replacement cannot inherit a prior Pit Boss session.")

    class SlotRedis:
        def __init__(self):
            self.keys: set[str] = set()

        async def set(self, key, value, nx=False, px=None):
            if nx and key in self.keys:
                return False
            self.keys.add(key)
            return True

    original_redis = main.redis_client
    main.redis_client = SlotRedis()
    try:
        join_slots = await asyncio.gather(
            *(main.claim_action_slot("racer", "join", 1.5) for _ in range(8))
        )
        pass_slots = await asyncio.gather(
            *(main.claim_action_slot("holder", "pass", 0.65) for _ in range(8))
        )
        assert sum(join_slots) == 1
        assert sum(pass_slots) == 1
        print("Race guard: concurrent join and pass action slots each admit exactly one request.")
    finally:
        main.redis_client = original_redis

    original_telegram_call = main.telegram_api_call

    async def no_change_telegram_call(*_args, **_kwargs):
        return False, {"description": "Bad Request: message is not modified"}

    async def failed_telegram_call(*_args, **_kwargs):
        return False, {"description": "Bad Request: inline message not found"}

    main.telegram_api_call = no_change_telegram_call
    try:
        assert await main.edit_inline_card("inline-card-id", "same card", {"inline_keyboard": []})
        main.telegram_api_call = failed_telegram_call
        assert not await main.edit_inline_card("inline-card-id", "next card", {"inline_keyboard": []})
        print("Card guard: harmless initial no-op edits retain the tracked inline message ID.")
    finally:
        main.telegram_api_call = original_telegram_call

    poster_loser = {"id": "private-user-id", "name": "Private Name", "public_handle": "cabinetvictim"}
    poster_survivors = [
        {"id": "safe-1", "name": "First Public", "public_handle": "fusefriend"},
        {"id": "safe-2", "name": "Second Public"},
        {"id": "safe-3", "name": "Third Public", "public_handle": "lastbreather"},
    ]
    main.game_state["multiplier"] = 2.75
    poster = main.render_elimination_poster(poster_loser, len(poster_survivors))
    assert poster.startswith(b"\x89PNG\r\n\x1a\n")
    caption, _ = main.current_elimination_card(poster_loser, poster_survivors)
    assert "@cabinetvictim" in caption
    assert "2.75×" in caption
    assert "3 SOULS" in caption
    assert "@fusefriend" in caption
    assert "Second Public" in caption
    assert "@lastbreather" in caption
    assert "private-user-id" not in caption
    assert "balance" not in caption.lower()
    final_caption, _ = main.current_round_result_card(415.0, poster_survivors[:1])
    assert "@fusefriend" in final_caption
    lobby_card = await main.current_lobby_card()
    assert lobby_card["type"] == "photo"
    assert lobby_card["photo_url"].endswith("/telegram/posters/lobby.png")

    media_calls: list[tuple[str, dict]] = []

    async def capture_media_call(method: str, payload: dict):
        media_calls.append((method, payload))
        return True, {"ok": True}

    main.telegram_api_call = capture_media_call
    try:
        assert await main.edit_inline_media("inline-card-id", "poster-key", caption, {"inline_keyboard": []})
        assert media_calls[-1][0] == "editMessageMedia"
        assert media_calls[-1][1]["media"]["type"] == "photo"
        assert media_calls[-1][1]["media"]["media"].endswith("/telegram/posters/poster-key.png")
        assert "private-user-id" not in media_calls[-1][1]["media"]["caption"]
        print("Poster guard: inline cards begin as photos and transform with public-safe knockout media.")
    finally:
        main.telegram_api_call = original_telegram_call

    class PosterRedis:
        async def smembers(self, key):
            return {"three-player-inline-card"} if key == main.ACTIVE_LOBBY_CARDS_KEY else set()

        async def srem(self, *_args):
            return 0

    three_player_calls: list[tuple[str, dict]] = []

    async def capture_three_player_media(method: str, payload: dict):
        three_player_calls.append((method, payload))
        return True, {"ok": True}

    original_poster_redis = main.redis_client
    main.redis_client = PosterRedis()
    main.telegram_api_call = capture_three_player_media
    main.publish_elimination_poster = original_publish_elimination_poster
    try:
        main.game_state.update(
            {
                "phase": "running",
                "players": [
                    {"id": "blast", "name": "Blast Test", "public_handle": "blasttest"},
                    {"id": "survivor-a", "name": "Alpha", "public_handle": "alphaalive"},
                    {"id": "survivor-b", "name": "Bravo"},
                ],
                "eliminated_players": [],
                "current_holder": "blast",
                "multiplier": 3.5,
                "pot": 300.0,
                "round_number": 1,
            }
        )
        await main.detonate()
        assert main.game_state["phase"] == "intermission"
        assert [player["id"] for player in main.game_state["players"]] == ["survivor-a", "survivor-b"]
        media_method, media_payload = three_player_calls[-1]
        assert media_method == "editMessageMedia"
        assert media_payload["inline_message_id"] == "three-player-inline-card"
        media_caption = media_payload["media"]["caption"]
        assert "@blasttest" in media_caption
        assert "@alphaalive" in media_caption
        assert "Bravo" in media_caption
        assert "3.50×" in media_caption
        assert "survivor-a" not in media_caption
        print("Three-player poster test: first elimination replaced the inline photo and named the two remaining public players.")
    finally:
        main.redis_client = original_poster_redis
        main.telegram_api_call = original_telegram_call

    winner = {"id": "winner-private-id", "name": "Winning Name", "public_handle": "lastbreather"}
    main.game_state["multiplier"] = 4.25
    winner_poster = main.render_final_survivor_poster(winner, 415.0)
    assert winner_poster.startswith(b"\x89PNG\r\n\x1a\n")
    winner_caption, _ = main.current_round_result_card(415.0, [winner])
    assert "@lastbreather" in winner_caption
    assert "415.00 ◉" in winner_caption
    assert "winner-private-id" not in winner_caption
    assert "balance" not in winner_caption.lower()

    class FinalPosterRedis:
        def __init__(self):
            self.deleted: list[str] = []

        async def smembers(self, key):
            return {"final-inline-card"} if key == main.ACTIVE_LOBBY_CARDS_KEY else set()

        async def srem(self, *_args):
            return 0

        async def delete(self, key):
            self.deleted.append(key)
            return 1

    final_calls: list[tuple[str, dict]] = []

    async def capture_final_media(method: str, payload: dict):
        final_calls.append((method, payload))
        return True, {"ok": True}

    final_poster_redis = FinalPosterRedis()
    main.redis_client = final_poster_redis
    main.telegram_api_call = capture_final_media
    main.publish_round_results = original_publish_round_results
    try:
        await main.publish_round_results(415.0, [winner])
        final_method, final_payload = final_calls[-1]
        assert final_method == "editMessageMedia"
        assert final_payload["inline_message_id"] == "final-inline-card"
        final_caption = final_payload["media"]["caption"]
        assert "@lastbreather" in final_caption
        assert "winner-private-id" not in final_caption
        assert final_payload["media"]["media"].startswith(f"{main.PUBLIC_BACKEND_URL}/telegram/posters/")
        assert main.ACTIVE_LOBBY_CARDS_KEY in final_poster_redis.deleted
        print("Final survivor poster test: winner image replaced the group card and named only the public handle.")
    finally:
        main.redis_client = original_poster_redis
        main.telegram_api_call = original_telegram_call

    ignition_ticks: list[str] = []
    original_tick_bomb = main.tick_bomb
    original_ignition_redis = main.redis_client

    class EmptyGroupRedis:
        async def hgetall(self, _key):
            return {}

    async def fake_tick_bomb():
        ignition_ticks.append("tick")

    main.tick_bomb = fake_tick_bomb
    main.redis_client = EmptyGroupRedis()
    try:
        def lobby_players(count: int) -> list[dict]:
            return [{"id": f"ready-{index}", "name": f"Ready {index}"} for index in range(count)]

        main.game_state.update(
            {
                "phase": "lobby",
                "players": lobby_players(2),
                "eliminated_players": [],
                "ready_players": ["ready-0"],
                "lobby_auto_start_at": 0.0,
                "pot": 200.0,
                "round_number": 0,
            }
        )
        assert not await main.maybe_ignite_lobby()
        main.game_state["ready_players"] = ["ready-0", "ready-1"]
        assert await main.maybe_ignite_lobby()
        assert main.game_state["phase"] == "running"
        assert broadcasts[-1][0] == "start"
        assert broadcasts[-1][1]["ignition_reason"] == "all_ready"
        await asyncio.sleep(0)
        assert ignition_ticks == ["tick"]
        print("Ignition guard: two-player lobby lights only when every signed player holds LIGHT IT UP.")

        main.game_state.update(
            {
                "phase": "lobby",
                "players": lobby_players(main.MAX_PLAYERS),
                "ready_players": [],
                "lobby_auto_start_at": 0.0,
                "round_number": 0,
            }
        )
        assert await main.maybe_ignite_lobby()
        assert broadcasts[-1][1]["ignition_reason"] == "full_lobby"
        print("Ignition guard: a full twelve-player lobby lights without requiring readiness holds.")

        main.game_state.update(
            {
                "phase": "lobby",
                "players": lobby_players(3),
                "ready_players": [],
                "lobby_auto_start_at": main.time.time() - 1,
                "round_number": 0,
            }
        )
        assert await main.maybe_ignite_lobby()
        assert broadcasts[-1][1]["ignition_reason"] == "lobby_countdown"
        print("Ignition guard: a three-player lobby lights after its 45-second server countdown expires.")

        main.reset_round_state()
        assert main.game_state["ready_players"] == []
        assert main.game_state["lobby_auto_start_at"] == 0.0
        print("Reset guard: lobby readiness and auto-ignite countdown do not leak into the next match.")

        # leave_lobby, predict_survivor, and taunt are implemented as inline
        # elif branches inside the websocket dispatch loop rather than standalone
        # functions (unlike detonate/handle_player_disconnect), so their actual
        # logic can't be exercised here without either refactoring live dispatch
        # code or building full WebSocket-level test scaffolding. Neither is done
        # blind in this pass. What's cheap and worth checking is the data those
        # branches depend on -- a typo emptying one of these sets would silently
        # break every reaction/taunt in production with no test catching it.
        assert main.SPECTATOR_REACTIONS and all(isinstance(r, str) for r in main.SPECTATOR_REACTIONS)
        assert main.GHOST_REACTIONS and all(isinstance(r, str) for r in main.GHOST_REACTIONS)
        assert main.TAUNT_LINES and all(isinstance(line, str) and line.strip() for line in main.TAUNT_LINES)
        assert main.DELETE_PLAYER_PHRASE != main.DELETE_ALL_PLAYERS_PHRASE, "single vs bulk delete confirm phrases must differ to prevent mistyping one for the other"
        assert main.DELETE_PLAYER_PHRASE != main.MASTER_RESET_PHRASE
        print("Constant guard: reaction/taunt pools are non-empty and destructive confirm phrases are distinct.")
    finally:
        main.tick_bomb = original_tick_bomb
        main.redis_client = original_ignition_redis

    class LedgerPipeline:
        def __init__(self, redis):
            self.redis = redis
            self.commands = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def watch(self, *_args):
            return True

        async def hget(self, key, field):
            return await self.redis.hget(key, field)

        def multi(self):
            return None

        def hset(self, key, field, value):
            self.commands.append(("hset", key, field, value))

        def lpush(self, key, value):
            self.commands.append(("lpush", key, value))

        def ltrim(self, key, start, stop):
            self.commands.append(("ltrim", key, start, stop))

        async def execute(self):
            for command, *args in self.commands:
                await getattr(self.redis, command)(*args)
            return True

    class LedgerRedis:
        def __init__(self):
            self.hashes: dict[str, dict[str, str]] = {}
            self.lists: dict[str, list[str]] = {}
            self.values: dict[str, str] = {}

        def pipeline(self, transaction=True):
            return LedgerPipeline(self)

        async def hget(self, key, field):
            return self.hashes.get(key, {}).get(str(field))

        async def hset(self, key, field, value):
            self.hashes.setdefault(key, {})[str(field)] = str(value)
            return 1

        async def hgetall(self, key):
            return dict(self.hashes.get(key, {}))

        async def hdel(self, key, *fields):
            bucket = self.hashes.get(key, {})
            removed = 0
            for field in fields:
                if str(field) in bucket:
                    del bucket[str(field)]
                    removed += 1
            return removed

        async def delete(self, *keys):
            removed = 0
            for key in keys:
                if key in self.hashes:
                    del self.hashes[key]
                    removed += 1
                if key in self.lists:
                    del self.lists[key]
                    removed += 1
                if key in self.values:
                    del self.values[key]
                    removed += 1
            return removed

        async def rpush(self, key, *values):
            self.lists.setdefault(key, []).extend(str(value) for value in values)
            return len(self.lists[key])

        async def get(self, key):
            return self.values.get(key)

        async def set(self, key, value, **_kwargs):
            self.values[key] = str(value)
            return True

        async def lpush(self, key, value):
            self.lists.setdefault(key, []).insert(0, str(value))
            return len(self.lists[key])

        async def ltrim(self, key, start, stop):
            self.lists[key] = self.lists.get(key, [])[start : stop + 1]
            return True

        async def lrange(self, key, start, stop):
            values = self.lists.get(key, [])
            return values[start:] if stop == -1 else values[start : stop + 1]

    ledger_redis = LedgerRedis()
    main.redis_client = ledger_redis
    main.change_balance = original_change_balance
    main.update_player_profile = original_update_profile
    try:
        player = await main.ensure_player_profile(
            "verified-ledger-user",
            {"first_name": "Ledger Player", "username": "ledgerplayer"},
        )
        assert player["balance"] == main.DEFAULT_BALANCE
        assert player["public_handle"] == "ledgerplayer"
        assert await main.apply_balance_event("verified-ledger-user", -100, "join_buy_in", round_ref=1) == 400.0
        assert await main.apply_balance_event("verified-ledger-user", 25, "pit_boss_credit", actor_id="pit-boss", metadata={"note": "QA refill"}) == 425.0
        try:
            await main.apply_balance_event("verified-ledger-user", -426, "pit_boss_debit", actor_id="pit-boss", metadata={"note": "Overdraw"})
            raise AssertionError("Negative balances must be rejected")
        except ValueError:
            pass
        profile = await main.load_player_profile("verified-ledger-user")
        assert profile and profile["balance"] == 425.0 and profile["chips_granted"] == 25.0
        events = await ledger_redis.lrange(f"{main.PLAYER_LEDGER_PREFIX}verified-ledger-user", 0, -1)
        assert len(events) == 2
        dashboard = await main.pit_boss_dashboard_payload(profile["ref"])
        assert dashboard["profiles"][0]["public_handle"] == "ledgerplayer"
        assert "verified-ledger-user" not in str(dashboard)
        second_player = await main.ensure_player_profile(
            "verified-ledger-search-user",
            {"first_name": "Balance Baron", "username": "chipbaron"},
        )
        assert await main.apply_balance_event("verified-ledger-search-user", 175, "pit_boss_credit", actor_id="pit-boss") == 675.0
        handle_search = await main.pit_boss_dashboard_payload(search="@chipbaron", sort="balance_desc")
        assert len(handle_search["profiles"]) == 1 and handle_search["profiles"][0]["name"] == "Balance Baron"
        high_balance = await main.pit_boss_dashboard_payload(sort="balance_desc")
        low_balance = await main.pit_boss_dashboard_payload(sort="balance_asc")
        assert high_balance["profiles"][0]["ref"] == second_player["ref"]
        assert low_balance["profiles"][0]["ref"] == profile["ref"]
        leader_alpha = await main.ensure_player_profile("leader-alpha", {"first_name": "Alpha", "username": "alpha_soul"})
        leader_beta = await main.ensure_player_profile("leader-beta", {"first_name": "Beta", "username": "beta_soul"})
        leader_gamma = await main.ensure_player_profile("leader-gamma", {"first_name": "Gamma", "username": "gamma_soul"})
        await main.update_player_profile("leader-alpha", matches_survived=3, total_pot_won=275)
        await main.update_player_profile("leader-beta", matches_survived=3, total_pot_won=490)
        await main.update_player_profile("leader-gamma", matches_survived=2, total_pot_won=900)
        competitive = await main.public_leaderboard_payload("leader-alpha", "competitive")
        assert competitive["view"] == "competitive"
        assert competitive["entries"][0]["name"] == "Beta"
        assert competitive["entries"][1]["name"] == "Alpha"
        assert competitive["entries"][2]["name"] == "Gamma"
        assert competitive["entries"][0]["survivals"] == 3 and competitive["entries"][0]["pot_won"] == 490.0
        assert all("ref" not in row and "target_id" not in row and "chips_granted" not in row for row in competitive["entries"])
        assert "leader-alpha" not in str(competitive) and "leader-beta" not in str(competitive)
        await main.apply_balance_event("leader-alpha", 900, "pit_boss_credit", actor_id="pit-boss")
        chip_stack = await main.public_leaderboard_payload("leader-alpha", "chips")
        assert chip_stack["view"] == "chips" and chip_stack["entries"][0]["name"] == "Alpha"
        assert chip_stack["entries"][0]["balance"] == 1400.0
        assert competitive["entries"][1]["rank"] == 2
        ledger_redis.hashes.setdefault(main.BALANCES_KEY, {})["legacy-balance-user"] = "333"
        legacy_stack = await main.public_leaderboard_payload("legacy-balance-user", "chips")
        assert legacy_stack["eligible_count"] >= chip_stack["eligible_count"] + 1
        assert "legacy-balance-user" not in str(legacy_stack)
        for index in range(12):
            candidate_id = f"rank-fixture-{index}"
            await main.ensure_player_profile(candidate_id, {"first_name": f"Rank {index}", "username": f"rank_soul_{index}"})
            await main.update_player_profile(candidate_id, matches_survived=20 - index, total_pot_won=100 + index)
        outsider = await main.ensure_player_profile("rank-outsider", {"first_name": "Outside Ten", "username": "outside_ten"})
        await main.update_player_profile("rank-outsider", matches_survived=1, total_pot_won=1)
        outside_board = await main.public_leaderboard_payload("rank-outsider", "competitive")
        assert len(outside_board["entries"]) == main.LEADERBOARD_LIMIT
        assert outside_board["viewer"] and outside_board["viewer"]["name"] == "Outside Ten"
        assert outside_board["viewer_rank"] == outside_board["viewer"]["rank"] > main.LEADERBOARD_LIMIT
        assert outsider["ref"] not in str(outside_board)
        group = await main.register_telegram_group({"id": -100123, "type": "supergroup", "title": "Cabinet QA"})
        assert group and group["title"] == "Cabinet QA"
        await main.touch_registered_group(group["ref"], "games_started")
        refreshed_dashboard = await main.pit_boss_dashboard_payload()
        assert refreshed_dashboard["groups"][0]["games_started"] == 1
        assert "-100123" not in str(refreshed_dashboard)
        await main.record_group_match_results(group["ref"], {"leader-alpha", "leader-beta", "leader-gamma"}, "leader-beta", 650)
        group_board = await main.public_leaderboard_payload("leader-alpha", "competitive", group["ref"])
        assert group_board["scope"] == "group" and group_board["group_available"] is True
        assert group_board["entries"][0]["name"] == "Beta" and group_board["entries"][0]["survivals"] == 1
        assert group_board["entries"][0]["pot_won"] == 650.0
        assert len(group_board["entries"]) == 3 and "leader-alpha" not in str(group_board)
        assert (await main.public_leaderboard_payload("leader-alpha", "competitive", "bad-group-ref"))["scope"] == "global"
        current_season = await main.group_season_archive_payload(group["ref"])
        assert current_season["available"] is True and current_season["current"]["entries"][0]["name"] == "Beta"
        old_week = "2020-W01"
        old_key = f"{main.GROUP_SEASON_CURRENT_PREFIX}{group['ref']}:{old_week}"
        await ledger_redis.hset(old_key, "leader-beta", json.dumps({"name": "Beta", "public_handle": "beta_soul", "matches_entered": 2, "matches_survived": 2, "total_pot_won": 400}))
        await ledger_redis.set(f"{main.GROUP_SEASON_CURRENT_PREFIX}{group['ref']}", old_week)
        rolled_season = await main.group_season_archive_payload(group["ref"])
        assert rolled_season["archives"] and rolled_season["archives"][0]["winner"]["name"] == "Beta"
        leaderboard_message = await main.group_leaderboard_message(group)
        assert "GROUP LEADERBOARD" in leaderboard_message and "@beta_soul" in leaderboard_message and "-100123" not in leaderboard_message
        health_message = await main.pit_boss_health_message()
        assert "CABINET HEALTH" in health_message and "-100123" not in health_message and "leader-alpha" not in health_message
        class WebhookRequest:
            def __init__(self, update):
                self.update = update

            async def json(self):
                return self.update

        original_webhook_secret = main.TELEGRAM_WEBHOOK_SECRET
        original_pit_boss_ids = set(main.PIT_BOSS_IDS)
        main.TELEGRAM_WEBHOOK_SECRET = "regression-webhook-secret"
        main.PIT_BOSS_IDS.add("pit-boss")
        try:
            command_base = {"chat": {"id": -100123, "type": "supergroup"}, "from": {"id": "ordinary-user"}}
            in_chat_board = await main.telegram_webhook(WebhookRequest({"message": {**command_base, "text": "/leaderboard"}}), "regression-webhook-secret")
            assert in_chat_board["method"] == "sendMessage" and "@beta_soul" in in_chat_board["text"] and "-100123" not in in_chat_board["text"]
            denied_health = await main.telegram_webhook(WebhookRequest({"message": {**command_base, "text": "/dont_splode_health"}}), "regression-webhook-secret")
            assert "reserved for the Pit Boss" in denied_health["text"]
            allowed_health = await main.telegram_webhook(WebhookRequest({"message": {"chat": {"id": 9, "type": "private"}, "from": {"id": "pit-boss"}, "text": "/dont_splode_health"}}), "regression-webhook-secret")
            assert "CABINET HEALTH" in allowed_health["text"] and "-100123" not in allowed_health["text"]
        finally:
            main.TELEGRAM_WEBHOOK_SECRET = original_webhook_secret
            main.PIT_BOSS_IDS.clear()
            main.PIT_BOSS_IDS.update(original_pit_boss_ids)
        spectator_socket = FakeSocket()
        reaction_peer = FakeSocket()
        spectator_manager = main.ConnectionManager()
        await spectator_manager.connect(spectator_socket, "spectator-user", group_ref=group["ref"], spectator=True)
        await spectator_manager.connect(reaction_peer, "reaction-peer")
        assert spectator_manager.spectator_contexts["spectator-user"] is True and spectator_manager.group_contexts["spectator-user"] == group["ref"]
        await spectator_manager.broadcast_spectator_reaction("🔥")
        assert spectator_socket.sent[-1] == {"type": "spectator_reaction", "reaction": "🔥", "ghost": False}
        assert reaction_peer.sent[-1] == {"type": "spectator_reaction", "reaction": "🔥", "ghost": False}
        assert "spectator-user" not in str(spectator_socket.sent[-1]) and "reaction-peer" not in str(reaction_peer.sent[-1])
        assert "🔥" in main.SPECTATOR_REACTIONS and "not-a-reaction" not in main.SPECTATOR_REACTIONS
        spectator_manager.disconnect("spectator-user", spectator_socket)
        assert "spectator-user" not in spectator_manager.spectator_contexts
        native_card_calls: list[tuple[str, dict]] = []

        async def capture_native_card(method: str, payload: dict):
            native_card_calls.append((method, payload))
            return True, {"result": {"message_id": 77}}

        main.telegram_api_call = capture_native_card
        assert await main.send_registered_group_lobby_card({"chat_id": "-100123", "ref": group["ref"]})
        stored_cards = await main.active_group_cards()
        assert stored_cards[0][1]["group_ref"] == group["ref"]
        assert native_card_calls[0][0] == "sendPhoto"
        group_launch_url = native_card_calls[0][1]["reply_markup"]["inline_keyboard"][0][0]["url"]
        assert f"startapp=join_{group['ref']}" in group_launch_url and "-100123" not in group_launch_url
        main.game_state["phase"] = "running"
        watch_card = await main.current_lobby_card(group["ref"])
        watch_url = watch_card["reply_markup"]["inline_keyboard"][0][0]["url"]
        assert f"startapp=watch_{group['ref']}" in watch_url and "-100123" not in watch_url
        _, elimination_watch_markup = main.current_elimination_card({"name": "QA Loser"}, [{"name": "QA Winner"}], group["ref"])
        assert f"startapp=watch_{group['ref']}" in elimination_watch_markup["inline_keyboard"][0][0]["url"]
        main.game_state["phase"] = "lobby"
        assert await main.edit_group_media("-100123", 77, "elimination-qa", "Public result", {"inline_keyboard": []})
        assert native_card_calls[-1][0] == "editMessageMedia"
        assert native_card_calls[-1][1]["media"]["media"].endswith("/elimination-qa.png")
        assert "-100123" not in str(refreshed_dashboard)
        alpha_before_reset = await main.load_player_profile("leader-alpha")
        assert alpha_before_reset and alpha_before_reset["matches_survived"] == 3
        reset_count = await main.master_reset_virtual_chips("pit-boss", "QA cabinet reset")
        assert reset_count > 0
        assert await main.get_balance("leader-alpha") == main.DEFAULT_BALANCE
        alpha_after_reset = await main.load_player_profile("leader-alpha")
        assert alpha_after_reset and alpha_after_reset["matches_survived"] == 3 and alpha_after_reset["total_pot_won"] == 275.0
        reset_events = await ledger_redis.lrange(f"{main.PLAYER_LEDGER_PREFIX}leader-alpha", 0, -1)
        assert any("pit_boss_master_reset" in event and "QA cabinet reset" in event for event in reset_events)

        # --- Single player deletion: leader-gamma has a live profile, balance,
        # personal ledger, and a group-competitive entry from record_group_match_results
        # above. Deleting them must not disturb leader-alpha or leader-beta.
        gamma_ref = leader_gamma["ref"]
        alpha_ledger_before = len(await ledger_redis.lrange(f"{main.PLAYER_LEDGER_PREFIX}leader-alpha", 0, -1))
        removed_one = await main.delete_player_completely("leader-gamma", gamma_ref, "pit-boss", "regression: single delete")
        assert removed_one["group_competitive_removed"] == 1
        assert await main.get_balance("leader-gamma") == main.DEFAULT_BALANCE, "get_balance must silently recreate a deleted user, not error"
        assert await main.load_player_profile("leader-gamma") is None
        assert await ledger_redis.hget(main.PLAYER_PROFILE_REFS_KEY, gamma_ref) is None
        assert await ledger_redis.lrange(f"{main.PLAYER_LEDGER_PREFIX}leader-gamma", 0, -1) == []
        assert await ledger_redis.hget(f"{main.GROUP_COMPETITIVE_PREFIX}{group['ref']}", "leader-gamma") is None
        assert await ledger_redis.hget(f"{main.GROUP_COMPETITIVE_PREFIX}{group['ref']}", "leader-beta") is not None, "unrelated player's group-leaderboard entry must survive"
        assert await main.load_player_profile("leader-alpha") is not None, "unrelated player's profile must survive"
        assert len(await ledger_redis.lrange(f"{main.PLAYER_LEDGER_PREFIX}leader-alpha", 0, -1)) == alpha_ledger_before, "unrelated player's personal ledger must be untouched"
        admin_ledger_after_single = await ledger_redis.lrange(main.ADMIN_LEDGER_KEY, 0, -1)
        assert not any('"target_id": "leader-gamma"' in event for event in admin_ledger_after_single), "deleted user's admin ledger entries must be purged"
        assert any(json.loads(event).get("reason") == "player_data_deleted" and gamma_ref in json.loads(event)["target_id"] for event in admin_ledger_after_single)
        assert not any("leader-gamma" in json.loads(event)["target_id"] for event in admin_ledger_after_single if json.loads(event).get("reason") == "player_data_deleted"), "deletion's own audit record must key by ref, not raw user_id"
        print("Delete guard: single-player erasure wipes balance/profile/ledger/group-board and leaves everyone else untouched.")

        # --- Bulk deletion: wipe every remaining player this whole ledger section
        # ever created (profiles-only, balance-only, and everything in between),
        # confirm the registered group's own metadata survives the wipe.
        surviving_profile_ids = set((await ledger_redis.hgetall(main.PLAYER_PROFILES_KEY)).keys())
        surviving_balance_ids = set((await ledger_redis.hgetall(main.BALANCES_KEY)).keys())
        expected_removed = surviving_profile_ids | surviving_balance_ids
        removed_all = await main.delete_all_players_completely("pit-boss", "regression: full wipe")
        assert removed_all["players_removed"] == len(expected_removed)
        assert removed_all["groups_cleared"] == 1
        assert await ledger_redis.hgetall(main.PLAYER_PROFILES_KEY) == {}
        assert await ledger_redis.hgetall(main.BALANCES_KEY) == {}
        assert await ledger_redis.hgetall(main.PLAYER_PROFILE_REFS_KEY) == {}
        assert await ledger_redis.hgetall(f"{main.GROUP_COMPETITIVE_PREFIX}{group['ref']}") == {}
        admin_ledger_after_all = await ledger_redis.lrange(main.ADMIN_LEDGER_KEY, 0, -1)
        assert len(admin_ledger_after_all) == 1, "only the bulk deletion's own audit record should remain"
        assert json.loads(admin_ledger_after_all[0])["reason"] == "all_player_data_deleted"
        assert await main.registered_group_for_chat("-100123") is not None, "registered group metadata itself must survive a player-data wipe"
        print(f"Delete guard: bulk erasure wiped {removed_all['players_removed']} player record(s) and left the registered group intact.")

        print("Ledger guard: profiles, signed events, non-negative debits, and public-safe group registry all hold.")
    finally:
        main.redis_client = original_redis
        main.telegram_api_call = original_telegram_call


if __name__ == "__main__":
    asyncio.run(run_checks())
    print("Last Soul Standing regression checks passed.")
