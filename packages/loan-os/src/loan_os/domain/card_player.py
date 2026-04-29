from __future__ import annotations

from typing import Any, Mapping

from loan_os.domain.card_router import default_card_router
from loan_os.domain.scenario import scenario_to_state, state_to_scenario


def play_card(card_id: str, scenario: Mapping[str, Any] | None, voice_session_id: str) -> dict[str, Any]:
  router = default_card_router()
  text = router.get_card_text(card_id)
  state = scenario_to_state(scenario)
  state.on_assistant_turn(text, enforce_guards=False)
  updated_scenario = state_to_scenario(state, scenario)
  updated_scenario["last_card_id"] = card_id
  updated_scenario["last_card_text"] = text
  return {
    "ok": True,
    "request_id": "card-player-response",
    "card_id": card_id,
    "card_text": text,
    "voice_session_id": voice_session_id,
    "playback_started": True,
    "timing_ms": 90 + min(len(text) // 30, 140),
    "scenario": updated_scenario,
  }
