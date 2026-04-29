from __future__ import annotations

from typing import Any, Mapping

from loan_os.domain.call_state import CallStage
from loan_os.domain.card_router import DISCOVERY_OPEN_CARD_ID, default_card_router
from loan_os.domain.scenario import scenario_to_state, state_to_scenario


POST_PITCH_STAGES = {
  CallStage.PITCH_DELIVERED,
  CallStage.TRANSFER_CONSENT_PENDING,
  CallStage.TRANSFER_CONSENT_RECEIVED,
  CallStage.TOOL_INVOKED,
  CallStage.APPOINTMENT_FALLBACK,
  CallStage.WRAP_UP,
}


def decide_next_action(
  transcript: str,
  scenario: Mapping[str, Any] | None,
  history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
  state = scenario_to_state(scenario)
  history = history or []
  router = default_card_router()
  match = router.match_deterministic_card(
    transcript,
    post_pitch=state.stage in POST_PITCH_STAGES,
    pitch_required=state.stage == CallStage.PITCH_REQUIRED,
  )
  if state.stage == CallStage.TRANSFER_CONSENT_RECEIVED and state.consent_explicit_yes_received:
    return {
      "ok": True,
      "request_id": "conversation-controller-response",
      "action": "CALL_TOOL",
      "message_text": None,
      "card_id": None,
      "tool_name": "book_or_transfer",
      "tool_payload": {
        "consent": True,
        "product": (scenario or {}).get("product"),
        "state": (scenario or {}).get("state"),
      },
      "reason": "explicit_transfer_consent_received",
      "scenario": state_to_scenario(state, scenario),
    }
  if match is not None:
    return {
      "ok": True,
      "request_id": "conversation-controller-response",
      "action": "PLAY_CARD" if match.card_name != "wrap_dnc" else "END_CALL",
      "message_text": None,
      "card_id": match.card_name,
      "tool_name": None,
      "tool_payload": None,
      "reason": match.reason,
      "scenario": state_to_scenario(state, scenario),
    }
  if state.stage in {CallStage.OPEN_DISCOVERY, CallStage.LEVERAGE_DISCOVERY}:
    return {
      "ok": True,
      "request_id": "conversation-controller-response",
      "action": "ASK_DISCOVERY",
      "message_text": "Tell me about the property.",
      "card_id": DISCOVERY_OPEN_CARD_ID,
      "tool_name": None,
      "tool_payload": None,
      "reason": "discovery_fields_incomplete",
      "scenario": state_to_scenario(state, scenario),
    }
  if state.stage == CallStage.PITCH_DELIVERED:
    last_card = next(
      (
        entry.get("card_id")
        for entry in reversed(history)
        if entry.get("kind") == "card_playback" and entry.get("card_id")
      ),
      None,
    )
    if last_card != "close_transfer_consent_ask":
      return {
        "ok": True,
        "request_id": "conversation-controller-response",
        "action": "PLAY_CARD",
        "message_text": None,
        "card_id": "close_transfer_consent_ask",
        "tool_name": None,
        "tool_payload": None,
        "reason": "pitch_delivered_follow_up",
        "scenario": state_to_scenario(state, scenario),
      }
  return {
    "ok": True,
    "request_id": "conversation-controller-response",
    "action": "ASK_DISCOVERY",
    "message_text": "Tell me about the property.",
    "card_id": DISCOVERY_OPEN_CARD_ID,
    "tool_name": None,
    "tool_payload": None,
    "reason": "default_discovery_repair",
    "scenario": state_to_scenario(state, scenario),
  }
