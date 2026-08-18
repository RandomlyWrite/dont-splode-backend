"""Focused offline checks for the Last Soul Standing state machine.

Run with: python3 regression_check.py
"""

import asyncio

import main


async def run_checks() -> None:
    for _ in range(250):
        _, _, crash = main.generate_crash()
        assert crash >= main.MINIMUM_CRASH_MULTIPLIER, crash

    broadcasts: list[tuple[str, dict]] = []
    balance_changes: list[tuple[str, float]] = []
    scheduled: list[str] = []

    async def no_op(*_args, **_kwargs):
        return None

    async def capture_broadcast(event_type: str, **extra):
        broadcasts.append((event_type, extra))

    async def capture_balance(user_id: str, amount: float):
        balance_changes.append((user_id, amount))
        return amount

    main.refresh_lobby_cards = no_op
    main.publish_round_results = no_op
    main.publish_elimination_poster = no_op
    main.manager.broadcast_state = capture_broadcast
    main.change_balance = capture_balance
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

        async def accept(self):
            self.accepted = True

        async def close(self, code: int = 1000, reason: str = ""):
            self.closed = (code, reason)

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
    main.game_state["multiplier"] = 2.75
    poster = main.render_elimination_poster(poster_loser, 3)
    assert poster.startswith(b"\x89PNG\r\n\x1a\n")
    caption, _ = main.current_elimination_card(poster_loser, 3)
    assert "@cabinetvictim" in caption
    assert "2.75×" in caption
    assert "3 SOULS" in caption
    assert "private-user-id" not in caption
    assert "balance" not in caption.lower()
    lobby_card = main.current_lobby_card()
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

    ignition_ticks: list[str] = []
    original_tick_bomb = main.tick_bomb

    async def fake_tick_bomb():
        ignition_ticks.append("tick")

    main.tick_bomb = fake_tick_bomb
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
    finally:
        main.tick_bomb = original_tick_bomb


if __name__ == "__main__":
    asyncio.run(run_checks())
    print("Last Soul Standing regression checks passed.")
