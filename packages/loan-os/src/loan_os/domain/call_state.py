from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


FIELD_KEYS = (
  "property_type",
  "state_or_location",
  "rough_credit",
  "value_or_price",
  "loan_balance",
  "goal",
)
MINIMUM_FIELD_KEYS = (
  "property_type",
  "state_or_location",
  "rough_credit",
  "value_or_price",
  "goal",
)
BOOKING_SIGNAL_PREFIX = "booking:"
BOOKING_SIGNAL_KEYS = (
  f"{BOOKING_SIGNAL_PREFIX}time",
  f"{BOOKING_SIGNAL_PREFIX}timezone",
  f"{BOOKING_SIGNAL_PREFIX}email",
  f"{BOOKING_SIGNAL_PREFIX}text_confirm",
)
MAX_REGENS_PER_TURN = 2
PITCH_MARKERS = (
  "50 and 250 properties",
  "this is our specialty",
  "900 out of 1000",
  "documents",
  "friction",
)
TRANSFER_CONSENT_PATTERN_GROUPS = (
  ("want me to see if someone", "available now"),
  ("want me to see if someone's available",),
  ("can i get a loan officer", "look at this"),
  ("want me to get a loan officer", "look at this"),
  ("want me to see if i can get someone", "available now"),
)
STATE_NAMES = (
  "alabama",
  "alaska",
  "arizona",
  "arkansas",
  "california",
  "colorado",
  "connecticut",
  "delaware",
  "florida",
  "georgia",
  "hawaii",
  "idaho",
  "illinois",
  "indiana",
  "iowa",
  "kansas",
  "kentucky",
  "louisiana",
  "maine",
  "maryland",
  "massachusetts",
  "michigan",
  "minnesota",
  "mississippi",
  "missouri",
  "montana",
  "nebraska",
  "nevada",
  "new hampshire",
  "new jersey",
  "new mexico",
  "new york",
  "north carolina",
  "north dakota",
  "ohio",
  "oklahoma",
  "oregon",
  "pennsylvania",
  "rhode island",
  "south carolina",
  "south dakota",
  "tennessee",
  "texas",
  "utah",
  "vermont",
  "virginia",
  "washington",
  "west virginia",
  "wisconsin",
  "wyoming",
  "district of columbia",
)


class StateGuardViolation(RuntimeError):
  """Raised when the call state machine is asked to violate a hard guard."""


class CallStage(str, Enum):
  OPENER = "opener"
  OPEN_DISCOVERY = "open_discovery"
  LEVERAGE_DISCOVERY = "leverage_discovery"
  MIN_FIELDS_GATHERED = "min_fields_gathered"
  PITCH_REQUIRED = "pitch_required"
  PITCH_DELIVERED = "pitch_delivered"
  TRANSFER_CONSENT_PENDING = "transfer_consent_pending"
  TRANSFER_CONSENT_RECEIVED = "transfer_consent_received"
  TOOL_INVOKED = "tool_invoked"
  APPOINTMENT_FALLBACK = "appointment_fallback"
  WRAP_UP = "wrap_up"


def default_fields() -> dict[str, str | None]:
  return {key: None for key in FIELD_KEYS}


def normalize_whitespace(text: str) -> str:
  return re.sub(r"\s+", " ", text).strip()


def has_pitch_markers(text: str) -> bool:
  normalized = normalize_whitespace(text).lower()
  return all(marker in normalized for marker in PITCH_MARKERS)


def has_transfer_consent_question(text: str) -> bool:
  normalized = normalize_whitespace(text).lower()
  return any(
    all(fragment in normalized for fragment in pattern_group)
    for pattern_group in TRANSFER_CONSENT_PATTERN_GROUPS
  )


def _clean_amount(value: str) -> str:
  return value.replace(" ", "").rstrip(".,")


def extract_fields(text: str) -> dict[str, str]:
  lowered = normalize_whitespace(text).lower()
  updates: dict[str, str] = {}

  property_patterns = (
    (r"\bduplex\b", "duplex"),
    (r"\btriplex\b", "triplex"),
    (r"\bfourplex\b", "fourplex"),
    (r"\b(?:2|two)[-\s]?unit\b", "2-unit"),
    (r"\b(?:3|three)[-\s]?unit\b", "3-unit"),
    (r"\b(?:4|four)[-\s]?unit\b", "4-unit"),
    (r"\bsingle[-\s]?family\b|\bsfr\b", "single_family"),
    (r"\bcondo\b|\bcondominium\b", "condo"),
    (r"\btownhome\b|\btownhouse\b", "townhome"),
    (r"\bmixed[-\s]?use\b", "mixed_use"),
    (r"\bshort[-\s]?term rental\b|\bstr\b", "short_term_rental"),
  )
  for pattern, value in property_patterns:
    if re.search(pattern, lowered):
      updates["property_type"] = value
      break

  for state_name in sorted(STATE_NAMES, key=len, reverse=True):
    if re.search(rf"\b{re.escape(state_name)}\b", lowered):
      updates["state_or_location"] = state_name.title()
      break

  credit_patterns = (
    r"\b(?:fico|credit(?: score)?|score)\s*(?:is|'s|was|around|about|of)?\s*(\d{3}\+?)\b",
    r"\b(\d{3}\+)\b",
  )
  for pattern in credit_patterns:
    match = re.search(pattern, lowered)
    if match:
      updates["rough_credit"] = match.group(1)
      break
  if "rough_credit" not in updates:
    if "strong credit" in lowered:
      updates["rough_credit"] = "strong credit"
    elif "excellent credit" in lowered:
      updates["rough_credit"] = "excellent credit"
    elif "good credit" in lowered:
      updates["rough_credit"] = "good credit"

  value_match = re.search(
    r"\b(?:value|worth|purchase price|price)\s*(?:is|'s|was|around|about|of)?\s*(\$?[\d,]+(?:\.\d+)?\s*[km]?)\b",
    lowered,
  )
  if value_match:
    updates["value_or_price"] = _clean_amount(value_match.group(1))

  balance_match = re.search(
    r"\b(?:loan balance|balance|owe|owes|payoff)\s*(?:is|'s|was|around|about|of)?\s*(\$?[\d,]+(?:\.\d+)?\s*[km]?)\b",
    lowered,
  )
  if balance_match:
    updates["loan_balance"] = _clean_amount(balance_match.group(1))

  goal_patterns = (
    (r"\bcash[\s-]?out(?:\s+refi)?\b", "cash_out_refi"),
    (r"\brate[\s-]?term(?:\s+refi)?\b", "rate_term_refi"),
    (r"\brefi\b|\brefinance\b", "refinance"),
    (r"\bpurchase\b|\bbuy\b", "purchase"),
    (r"\bfix(?:\s+and\s+|[-\s]?)flip\b", "fix_and_flip"),
  )
  for pattern, value in goal_patterns:
    if re.search(pattern, lowered):
      updates["goal"] = value
      break

  return updates


def extract_leverage_signals(text: str) -> list[str]:
  lowered = normalize_whitespace(text).lower()
  signals: list[str] = []
  signal_patterns = (
    (r"\bdscr\b", "dscr"),
    (r"\bcash[\s-]?out\b", "cash_out"),
    (r"\brefi\b|\brefinance\b", "refi"),
    (r"\bfix(?:\s+and\s+|[-\s]?)flip\b", "fix_and_flip"),
    (r"\brent\b|\brental\b", "rental"),
    (r"\bequity\b", "equity"),
    (r"\bloan balance\b|\bbalance\b", "loan_balance"),
  )
  for pattern, value in signal_patterns:
    if re.search(pattern, lowered):
      signals.append(value)
  return signals


@dataclass
class CallState:
  stage: CallStage = CallStage.OPENER
  fields_collected: dict[str, str | None] = field(default_factory=default_fields)
  leverage_signals: list[str] = field(default_factory=list)
  pitch_attempt_count: int = 0
  consent_explicit_yes_received: bool = False
  regen_count_per_turn: int = 0
  history: list[tuple[float, CallStage, str]] = field(default_factory=list)

  def __post_init__(self) -> None:
    merged_fields = default_fields()
    merged_fields.update(self.fields_collected)
    self.fields_collected = merged_fields
    if self.stage == CallStage.TRANSFER_CONSENT_RECEIVED:
      self.consent_explicit_yes_received = True
    if not self.history:
      self._record("initialized")

  def _record(self, reason: str) -> None:
    self.history.append((time.time(), self.stage, reason))

  def transition(
    self,
    new_stage: CallStage,
    reason: str,
    *,
    enforce_guards: bool = True,
  ) -> None:
    if new_stage == self.stage:
      return
    if enforce_guards:
      self._assert_transition_allowed(new_stage)
    self.stage = new_stage
    self._record(reason)

  def _assert_transition_allowed(self, new_stage: CallStage) -> None:
    if new_stage == CallStage.PITCH_DELIVERED and self.stage not in {
      CallStage.MIN_FIELDS_GATHERED,
      CallStage.PITCH_REQUIRED,
      CallStage.TRANSFER_CONSENT_PENDING,
    }:
      raise StateGuardViolation(
        "Cannot advance to PITCH_DELIVERED before minimum discovery is complete."
      )
    if new_stage == CallStage.TRANSFER_CONSENT_PENDING and self.stage != CallStage.PITCH_DELIVERED:
      raise StateGuardViolation(
        "Cannot ask for transfer consent before the pitch has been delivered."
      )
    if new_stage == CallStage.TRANSFER_CONSENT_RECEIVED and self.stage != CallStage.TRANSFER_CONSENT_PENDING:
      raise StateGuardViolation(
        "Cannot mark transfer consent received before asking for consent."
      )

  def on_user_speech_started(self) -> None:
    self.reset_regen_count()
    if self.stage == CallStage.OPENER:
      self.transition(CallStage.OPEN_DISCOVERY, "first user speech started")

  def on_assistant_turn(self, text: str, *, enforce_guards: bool = True) -> dict[str, str]:
    normalized = normalize_whitespace(text)
    if not normalized:
      return {}

    updates = extract_fields(normalized)
    self.apply_field_updates(updates)
    self._extend_leverage_signals(extract_leverage_signals(normalized))

    if self.stage == CallStage.OPEN_DISCOVERY:
      self.transition(
        CallStage.LEVERAGE_DISCOVERY,
        "first assistant discovery turn completed",
        enforce_guards=enforce_guards,
      )

    if self._has_minimum_fields() and self.stage in {
      CallStage.OPEN_DISCOVERY,
      CallStage.LEVERAGE_DISCOVERY,
    }:
      self.transition(
        CallStage.MIN_FIELDS_GATHERED,
        "minimum discovery fields gathered",
        enforce_guards=enforce_guards,
      )
      self.transition(
        CallStage.PITCH_REQUIRED,
        "pitch now required",
        enforce_guards=enforce_guards,
      )

    if self.stage in {CallStage.MIN_FIELDS_GATHERED, CallStage.PITCH_REQUIRED}:
      self.pitch_attempt_count += 1

    if has_pitch_markers(normalized):
      self.transition(
        CallStage.PITCH_DELIVERED,
        "assistant delivered required pitch",
        enforce_guards=enforce_guards,
      )

    if has_transfer_consent_question(normalized):
      self.consent_explicit_yes_received = False
      self.transition(
        CallStage.TRANSFER_CONSENT_PENDING,
        "assistant requested transfer consent",
        enforce_guards=enforce_guards,
      )

    if self.stage == CallStage.APPOINTMENT_FALLBACK and self._has_booking_fields():
      self.transition(
        CallStage.WRAP_UP,
        "appointment fallback fields complete",
        enforce_guards=enforce_guards,
      )

    self.reset_regen_count()
    return updates

  def on_user_turn(self, text: str, *, enforce_guards: bool = True) -> dict[str, str]:
    normalized = normalize_whitespace(text)
    if not normalized:
      return {}

    updates = extract_fields(normalized)
    self.apply_field_updates(updates)
    self._extend_leverage_signals(extract_leverage_signals(normalized))

    if self._has_minimum_fields() and self.stage in {
      CallStage.OPEN_DISCOVERY,
      CallStage.LEVERAGE_DISCOVERY,
    }:
      self.transition(
        CallStage.MIN_FIELDS_GATHERED,
        "minimum discovery fields gathered from caller transcript",
        enforce_guards=enforce_guards,
      )
      self.transition(
        CallStage.PITCH_REQUIRED,
        "pitch now required after caller-provided discovery",
        enforce_guards=enforce_guards,
      )

    return updates

  def apply_field_updates(self, updates: dict[str, str]) -> None:
    for key, value in updates.items():
      if key in self.fields_collected and value:
        self.fields_collected[key] = value

  def _has_minimum_fields(self) -> bool:
    return all(self.fields_collected.get(key) for key in MINIMUM_FIELD_KEYS)

  def _extend_leverage_signals(self, signals: list[str]) -> None:
    for signal in signals:
      if signal not in self.leverage_signals:
        self.leverage_signals.append(signal)

  def on_transfer_consent_response(
    self,
    consent_granted: bool | None,
    *,
    enforce_guards: bool = True,
  ) -> None:
    if self.stage != CallStage.TRANSFER_CONSENT_PENDING or consent_granted is None:
      return
    if consent_granted:
      self.consent_explicit_yes_received = True
      self.transition(
        CallStage.TRANSFER_CONSENT_RECEIVED,
        "caller explicitly granted transfer consent",
        enforce_guards=enforce_guards,
      )
      return
    self.consent_explicit_yes_received = False
    self.transition(
      CallStage.PITCH_DELIVERED,
      "caller declined transfer consent",
      enforce_guards=enforce_guards,
    )

  def assert_tool_call_allowed(
    self,
    tool_name: str,
    *,
    require_explicit_consent: bool = False,
  ) -> None:
    if tool_name != "book_or_transfer":
      return
    allowed_stages = (
      {CallStage.TRANSFER_CONSENT_RECEIVED, CallStage.TOOL_INVOKED}
      if require_explicit_consent
      else {CallStage.TRANSFER_CONSENT_PENDING, CallStage.TOOL_INVOKED}
    )
    if self.stage not in allowed_stages:
      raise StateGuardViolation(f"book_or_transfer is not allowed during {self.stage.value}")

  def on_tool_call(
    self,
    tool_name: str,
    *,
    enforce_guards: bool = True,
    require_explicit_consent: bool = False,
  ) -> None:
    if tool_name != "book_or_transfer":
      return
    if enforce_guards:
      self.assert_tool_call_allowed(
        tool_name,
        require_explicit_consent=require_explicit_consent,
      )
    self.transition(
      CallStage.TOOL_INVOKED,
      "book_or_transfer invoked",
      enforce_guards=False,
    )

  def on_tool_result(
    self,
    tool_name: str,
    result: dict[str, Any],
    *,
    args: dict[str, Any] | None = None,
    enforce_guards: bool = True,
  ) -> None:
    if tool_name != "book_or_transfer":
      return

    self._record_booking_details(args or {})
    outcome = self._classify_book_or_transfer_result(result)
    if outcome == "no_lo_available":
      self.transition(
        CallStage.APPOINTMENT_FALLBACK,
        "book_or_transfer reported no loan officer available",
        enforce_guards=enforce_guards,
      )
      if self._has_booking_fields():
        self.transition(
          CallStage.WRAP_UP,
          "appointment fallback fields complete",
          enforce_guards=enforce_guards,
        )
      return
    if outcome in {"transferred", "booked_immediately"}:
      self.transition(
        CallStage.WRAP_UP,
        f"book_or_transfer resolved with {outcome}",
        enforce_guards=enforce_guards,
      )

  def _classify_book_or_transfer_result(self, result: dict[str, Any]) -> str | None:
    if result.get("transferred") is True or result.get("live_transfer") is True:
      return "transferred"
    if result.get("booked_immediately") is True or result.get("booked") is True:
      return "booked_immediately"
    if result.get("no_lo_available") is True:
      return "no_lo_available"
    if result.get("live_transfer_available") is False:
      return "no_lo_available"
    if result.get("fallback") == "appointment":
      return "no_lo_available"
    return None

  def _record_booking_details(self, args: dict[str, Any]) -> None:
    if str(args.get("preferred_time") or "").strip():
      self._extend_leverage_signals([BOOKING_SIGNAL_KEYS[0]])
    if str(args.get("timezone") or "").strip():
      self._extend_leverage_signals([BOOKING_SIGNAL_KEYS[1]])
    if str(args.get("email") or "").strip():
      self._extend_leverage_signals([BOOKING_SIGNAL_KEYS[2]])
    if args.get("phone_confirmed") is True:
      self._extend_leverage_signals([BOOKING_SIGNAL_KEYS[3]])

  def _has_booking_fields(self) -> bool:
    return all(signal in self.leverage_signals for signal in BOOKING_SIGNAL_KEYS)

  def note_regeneration(self, reason: str) -> None:
    self.regen_count_per_turn += 1
    self._record(reason)
    if self.regen_count_per_turn > MAX_REGENS_PER_TURN:
      raise StateGuardViolation(f"Regeneration loop limit exceeded for {self.stage.value}")

  def reset_regen_count(self) -> None:
    self.regen_count_per_turn = 0

  def snapshot(self) -> dict[str, Any]:
    return {
      "stage": self.stage.value,
      "fields_collected": dict(self.fields_collected),
      "leverage_signals": list(self.leverage_signals),
      "pitch_attempt_count": self.pitch_attempt_count,
      "consent_explicit_yes_received": self.consent_explicit_yes_received,
      "regen_count_per_turn": self.regen_count_per_turn,
      "history": [
        {
          "timestamp": timestamp,
          "stage": stage.value,
          "reason": reason,
        }
        for timestamp, stage, reason in self.history
      ],
    }
