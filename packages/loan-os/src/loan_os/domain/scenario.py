from __future__ import annotations

import re
from typing import Any, Mapping

from loan_os.domain.call_state import CallStage, CallState


STATE_ABBREVIATIONS = {
  "Alabama": "AL",
  "Alaska": "AK",
  "Arizona": "AZ",
  "Arkansas": "AR",
  "California": "CA",
  "Colorado": "CO",
  "Connecticut": "CT",
  "Delaware": "DE",
  "District Of Columbia": "DC",
  "Florida": "FL",
  "Georgia": "GA",
  "Hawaii": "HI",
  "Idaho": "ID",
  "Illinois": "IL",
  "Indiana": "IN",
  "Iowa": "IA",
  "Kansas": "KS",
  "Kentucky": "KY",
  "Louisiana": "LA",
  "Maine": "ME",
  "Maryland": "MD",
  "Massachusetts": "MA",
  "Michigan": "MI",
  "Minnesota": "MN",
  "Mississippi": "MS",
  "Missouri": "MO",
  "Montana": "MT",
  "Nebraska": "NE",
  "Nevada": "NV",
  "New Hampshire": "NH",
  "New Jersey": "NJ",
  "New Mexico": "NM",
  "New York": "NY",
  "North Carolina": "NC",
  "North Dakota": "ND",
  "Ohio": "OH",
  "Oklahoma": "OK",
  "Oregon": "OR",
  "Pennsylvania": "PA",
  "Rhode Island": "RI",
  "South Carolina": "SC",
  "South Dakota": "SD",
  "Tennessee": "TN",
  "Texas": "TX",
  "Utah": "UT",
  "Vermont": "VT",
  "Virginia": "VA",
  "Washington": "WA",
  "West Virginia": "WV",
  "Wisconsin": "WI",
  "Wyoming": "WY",
}
GOAL_NORMALIZATION = {
  "cash_out_refi": "cash_out",
  "rate_term_refi": "rate_term",
}
YES_PATTERN = re.compile(r"\b(yes|yeah|yep|sure|ok|okay|please do|go ahead)\b", re.IGNORECASE)
NO_PATTERN = re.compile(r"\b(no|nope|not now|don't|do not)\b", re.IGNORECASE)


def scenario_to_state(scenario: Mapping[str, Any] | None) -> CallState:
  if not scenario:
    return CallState()
  snapshot = scenario.get("state_machine")
  if isinstance(snapshot, Mapping):
    history = []
    for entry in snapshot.get("history", []):
      if isinstance(entry, Mapping):
        history.append(
          (
            float(entry.get("timestamp", 0.0)),
            CallStage(entry.get("stage", CallStage.OPENER.value)),
            str(entry.get("reason", "")),
          )
        )
    return CallState(
      stage=CallStage(snapshot.get("stage", CallStage.OPENER.value)),
      fields_collected=dict(snapshot.get("fields_collected", {})),
      leverage_signals=list(snapshot.get("leverage_signals", [])),
      pitch_attempt_count=int(snapshot.get("pitch_attempt_count", 0)),
      consent_explicit_yes_received=bool(snapshot.get("consent_explicit_yes_received", False)),
      regen_count_per_turn=int(snapshot.get("regen_count_per_turn", 0)),
      history=history,
    )
  return CallState()


def detect_consent_response(transcript: str) -> bool | None:
  text = transcript.strip()
  if not text:
    return None
  if YES_PATTERN.search(text):
    return True
  if NO_PATTERN.search(text):
    return False
  return None


def normalize_amount(value: str | None) -> int | None:
  if not value:
    return None
  lowered = value.strip().lower().replace("$", "").replace(",", "")
  multiplier = 1
  if lowered.endswith("k"):
    lowered = lowered[:-1]
    multiplier = 1_000
  elif lowered.endswith("m"):
    lowered = lowered[:-1]
    multiplier = 1_000_000
  try:
    return int(float(lowered) * multiplier)
  except ValueError:
    return None


def normalize_credit(value: str | None) -> int | str | None:
  if not value:
    return None
  digits = value.rstrip("+")
  if digits.isdigit():
    return int(digits)
  return value


def normalize_goal(value: str | None) -> str | None:
  if not value:
    return None
  return GOAL_NORMALIZATION.get(value, value)


def typed_field_updates(raw_updates: Mapping[str, str], state: CallState) -> dict[str, Any]:
  result: dict[str, Any] = {}
  if "property_type" in raw_updates:
    result["property_type"] = raw_updates["property_type"]
  if "state_or_location" in raw_updates:
    result["state"] = STATE_ABBREVIATIONS.get(raw_updates["state_or_location"], raw_updates["state_or_location"])
  if "rough_credit" in raw_updates:
    result["credit"] = normalize_credit(raw_updates["rough_credit"])
  if "value_or_price" in raw_updates:
    result["value"] = normalize_amount(raw_updates["value_or_price"])
  if "loan_balance" in raw_updates:
    result["balance"] = normalize_amount(raw_updates["loan_balance"])
  if "goal" in raw_updates:
    result["goal"] = normalize_goal(raw_updates["goal"])
  if "dscr" in state.leverage_signals:
    result["product"] = "dscr"
  return result


def supplemental_caller_updates(transcript: str) -> dict[str, str]:
  updates: dict[str, str] = {}
  credit_match = re.search(r"\b(\d{3})\s+credit\b", transcript, re.IGNORECASE)
  if credit_match:
    updates["rough_credit"] = credit_match.group(1)
  return updates


def state_to_scenario(state: CallState, current: Mapping[str, Any] | None = None) -> dict[str, Any]:
  current = current or {}
  fields = state.fields_collected
  scenario = dict(current)
  scenario.update(
    {
      "scenario_id": str(current.get("scenario_id", "scenario-fake-001")),
      "product": "dscr" if "dscr" in state.leverage_signals else current.get("product"),
      "property_type": fields.get("property_type"),
      "state": STATE_ABBREVIATIONS.get(
        str(fields.get("state_or_location")) if fields.get("state_or_location") else "",
        current.get("state"),
      ),
      "credit": normalize_credit(fields.get("rough_credit")),
      "value": normalize_amount(fields.get("value_or_price")),
      "balance": normalize_amount(fields.get("loan_balance")),
      "goal": normalize_goal(fields.get("goal")),
      "stage": state.stage.value,
      "leverage_signals": list(state.leverage_signals),
      "consent_to_transfer": state.consent_explicit_yes_received,
      "state_machine": state.snapshot(),
    }
  )
  return scenario


def collect_scenario_fields(
  transcript: str,
  scenario: Mapping[str, Any] | None,
) -> dict[str, Any]:
  state = scenario_to_state(scenario)
  if transcript.strip():
    state.on_user_speech_started()
  raw_updates = state.on_user_turn(transcript, enforce_guards=False)
  supplemental_updates = supplemental_caller_updates(transcript)
  if supplemental_updates:
    state.apply_field_updates(supplemental_updates)
    raw_updates = {**raw_updates, **supplemental_updates}
    if state.stage in {CallStage.OPEN_DISCOVERY, CallStage.LEVERAGE_DISCOVERY} and all(
      state.fields_collected.get(key)
      for key in ("property_type", "state_or_location", "rough_credit", "value_or_price", "goal")
    ):
      state.transition(
        CallStage.MIN_FIELDS_GATHERED,
        "minimum discovery fields gathered from collector supplement",
        enforce_guards=False,
      )
      state.transition(
        CallStage.PITCH_REQUIRED,
        "pitch now required after collector supplement",
        enforce_guards=False,
      )
  consent_response = None
  if state.stage == CallStage.TRANSFER_CONSENT_PENDING:
    consent_response = detect_consent_response(transcript)
    state.on_transfer_consent_response(consent_response, enforce_guards=False)
  updated_scenario = state_to_scenario(state, scenario)
  return {
    "ok": True,
    "scenario": updated_scenario,
    "extracted_fields": typed_field_updates(raw_updates, state),
    "consent_response": consent_response,
    "signals": list(state.leverage_signals),
    "request_id": "scenario-collector-response",
  }
