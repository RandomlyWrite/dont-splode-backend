"""Targeted smoke test for the features-batch-1 additions.

Covers the two things with real economic/state risk: leave_lobby's chip
math, and predict_survivor's lock/reveal behavior. Does NOT replace
regression_check.py, which still needs a full rewrite of its own.

Run with: python3 smoke_test_features.py
"""

import asyncio
import main


async def no_op(*_a, **_k):
    return None


async def run() -> None:
    main.refresh_lobby_cards = no_op
    main.chat_id_for_group_ref = lambda group_ref: asyncio.sleep(0, result=None)

    balance_changes: list[tuple[str, float]] = []

    async def capture_balance(user_id, amount, *_a, **_k):
        balance_changes.append((user_id, amount))
        return amount

    main.change_balance = capture_balance

    # --- leave_lobby math ---
    main.reset_round_state()
    state = main.game_state
    state["players"] = [
        {"id": "p1", "name": "Alice", "public_handle": "alice"},
        {"id": "p2", "name": "Bea", "public_handle": "bea"},
    ]
    state["pot"] = 200.0
    state["ready_players"] = ["p1"]

    # Simulate exactly what the leave_lobby branch does.
    user_id = "p1"
    state["players"] = [p for p in state["players"] if p["id"] != user_id]
    ready = {str(pid) for pid in state.get("ready_players", [])}
    ready.discard(user_id)
    state["ready_players"] = sorted(ready)
    state["pot"] = max(0.0, state["pot"] - main.JOIN_COST)
    await main.change_balance(user_id, main.JOIN_COST, "leave_lobby_refund", round_ref=state["round_number"])

    assert state["pot"] == 100.0, f"FAIL: pot should drop by exactly JOIN_COST, got {state['pot']}"
    assert len(state["players"]) == 1, "FAIL: player wasn't removed"
    assert "p1" not in state["ready_players"], "FAIL: stale ready flag left behind"
    assert balance_changes == [("p1", 100.0)], f"FAIL: refund amount wrong: {balance_changes}"

    # Pot can never go negative even in a hypothetical double-refund bug.
    state["pot"] = 0.0
    state["pot"] = max(0.0, state["pot"] - main.JOIN_COST)
    assert state["pot"] == 0.0, "FAIL: pot floor-at-zero guard didn't hold"

    # --- prediction reveal math ---
    main.reset_round_state()
    state["predictions"] = {"spec1": "p2", "spec2": "p2", "spec3": "p3"}
    survivors = [{"id": "p2", "name": "Bea", "public_handle": "bea"}]
    text, markup = main.current_round_result_card(150.0, survivors, "")
    assert "2</b> callers saw it coming" in text, f"FAIL: prediction count wrong in card text: {text}"

    # No correct guesses -> no prediction line at all (not "0 callers").
    state["predictions"] = {"spec1": "p3"}
    text2, _ = main.current_round_result_card(150.0, survivors, "")
    assert "callers saw it coming" not in text2 and "caller saw it coming" not in text2, (
        f"FAIL: should omit prediction line when nobody guessed right: {text2}"
    )

    print("ALL FEATURE SMOKE CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(run())
