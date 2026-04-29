from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loan_os.paths import CARDS_PATH


DEFAULT_CARDS_PATH = CARDS_PATH

REQUIRED_CARD_NAMES = (
  "pitch_full",
  "obj_rate_early",
  "obj_rate_post_pitch",
  "obj_down_payment_post_pitch",
  "obj_credit_post_context",
  "obj_already_has_lender_friction",
  "close_transfer_consent_ask",
  "wrap_dnc",
)

RATE_RE = re.compile(r"\b(rate|rates|interest)\b", re.IGNORECASE)
DOWN_PAYMENT_RE = re.compile(r"\b(down|down payment|put down)\b", re.IGNORECASE)
CREDIT_RE = re.compile(r"\b(credit|credit score|fico)\b", re.IGNORECASE)
LENDER_FRICTION_RE = re.compile(
  r"\b(already have|got a lender|working with).{0,80}\b(lender|bank|broker|mortgage)\b|"
  r"\b(lender|bank|broker|mortgage).{0,80}\b(already have|got|working with)\b",
  re.IGNORECASE,
)
DNC_RE = re.compile(r"\b(stop calling|remove me|do not call|don't call|take me off)\b", re.IGNORECASE)

DISCOVERY_OPEN_CARD_ID = "discovery_open"
DISCOVERY_OPEN_TEXT = "Tell me about the property."


class CardRouterError(ValueError):
  """Raised when locked card configuration is invalid."""


@dataclass(frozen=True)
class CardMatch:
  card_name: str
  reason: str
  deterministic: bool = True


class CardRouter:
  def __init__(self, path: Path = DEFAULT_CARDS_PATH) -> None:
    self.path = path
    self._cards = self._load(path)
    self._validate_required_cards()

  @staticmethod
  def _load(path: Path) -> dict[str, str]:
    try:
      payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
      raise CardRouterError(f"card config not found: {path}") from exc
    except json.JSONDecodeError as exc:
      raise CardRouterError("cards.yaml is intentionally JSON-shaped YAML") from exc
    cards = payload.get("cards")
    if not isinstance(cards, dict):
      raise CardRouterError("cards.yaml must contain an object at key 'cards'")
    normalized: dict[str, str] = {}
    for name, text in cards.items():
      if not isinstance(name, str) or not isinstance(text, str) or not text.strip():
        raise CardRouterError("each card must have a non-empty string name and text")
      normalized[name] = text
    return normalized

  def _validate_required_cards(self) -> None:
    missing = [name for name in REQUIRED_CARD_NAMES if name not in self._cards]
    if missing:
      raise CardRouterError(f"missing required script cards: {', '.join(missing)}")

  def list_card_names(self) -> tuple[str, ...]:
    return tuple(self._cards)

  def get_card_text(self, name: str) -> str:
    if name == DISCOVERY_OPEN_CARD_ID:
      return DISCOVERY_OPEN_TEXT
    try:
      return self._cards[name]
    except KeyError as exc:
      allowed = ", ".join((*self.list_card_names(), DISCOVERY_OPEN_CARD_ID))
      raise CardRouterError(f"unknown script card {name!r}; use one of: {allowed}") from exc

  def match_deterministic_card(
    self,
    transcript: str,
    *,
    post_pitch: bool = False,
    pitch_required: bool = False,
  ) -> CardMatch | None:
    text = transcript.strip()
    if not text:
      return None
    if DNC_RE.search(text):
      return CardMatch("wrap_dnc", "dnc_request")
    if RATE_RE.search(text):
      return CardMatch(
        "obj_rate_post_pitch" if post_pitch else "obj_rate_early",
        "rate_objection_post_pitch" if post_pitch else "rate_objection_early",
      )
    if DOWN_PAYMENT_RE.search(text) and post_pitch:
      return CardMatch("obj_down_payment_post_pitch", "down_payment_post_pitch")
    if CREDIT_RE.search(text) and post_pitch:
      return CardMatch("obj_credit_post_context", "credit_post_context")
    if LENDER_FRICTION_RE.search(text):
      return CardMatch("obj_already_has_lender_friction", "already_has_lender")
    if pitch_required:
      return CardMatch("pitch_full", "pitch_required_state")
    return None


def default_card_router() -> CardRouter:
  return CardRouter()
