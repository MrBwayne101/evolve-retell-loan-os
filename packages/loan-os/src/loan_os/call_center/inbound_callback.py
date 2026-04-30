from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from loan_os.call_center.ledger import (
  EventEnvelope,
  EvidenceRef,
  payload_excerpt,
  redact_email,
  redact_phone,
  stable_id,
  utc_now_iso,
)
from loan_os.call_center.speed_to_lead import normalize_phone_e164


SHADOW_QUEUE_NAME = "inbound-callback-shadow-queue"
SHADOW_MODE = "shadow_only_no_external_writes"

ACTIVE_STATUSES = {
  "active",
  "application_started",
  "app_started",
  "submitted",
  "submitted_to_processing",
  "in_process",
  "processing",
  "funded",
  "closed",
  "declined",
}
WRONG_PRODUCT_HINTS = {
  "owner occupied",
  "owner-occupied",
  "primary residence",
  "conventional",
  "fha",
  "va loan",
}


@dataclass(frozen=True)
class InboundCallbackShadowResult:
  callback_event: dict[str, Any]
  context_match: dict[str, Any]
  opener: str
  next_best_question: str
  transfer_book_recommendation: dict[str, Any]
  review_gate: dict[str, Any]
  audit_event: EventEnvelope
  shadow_queue_row: dict[str, Any]

  def to_record(self) -> dict[str, Any]:
    return {
      "callback_event": self.callback_event,
      "context_match": self.context_match,
      "opener": self.opener,
      "next_best_question": self.next_best_question,
      "transfer_book_recommendation": self.transfer_book_recommendation,
      "review_gate": self.review_gate,
      "audit_event": self.audit_event.to_record(),
      "shadow_queue_row": self.shadow_queue_row,
    }


def prepare_inbound_callback_shadow(
  callback_event: Mapping[str, Any],
  contact_context: Mapping[str, Any] | None = None,
  *,
  now_iso: str | None = None,
) -> InboundCallbackShadowResult:
  normalized_event = normalize_callback_event(callback_event, now_iso=now_iso)
  normalized_context = normalize_contact_context(contact_context or {})
  context_match = build_context_match(normalized_event, normalized_context)
  safety = evaluate_callback_safety(normalized_context)
  opener = build_callback_opener(context_match, normalized_context)
  next_best_question = build_next_best_question(context_match, normalized_context, safety)
  recommendation = build_transfer_book_recommendation(context_match, normalized_context, safety)
  review_gate = build_review_gate(context_match, safety, recommendation)
  shadow_queue_row = build_shadow_queue_row(
    normalized_event,
    normalized_context,
    context_match,
    opener,
    next_best_question,
    recommendation,
    review_gate,
  )
  audit_event = build_audit_event(normalized_event, normalized_context, context_match, recommendation, review_gate, shadow_queue_row)

  return InboundCallbackShadowResult(
    callback_event=normalized_event,
    context_match=context_match,
    opener=opener,
    next_best_question=next_best_question,
    transfer_book_recommendation=recommendation,
    review_gate=review_gate,
    audit_event=audit_event,
    shadow_queue_row=shadow_queue_row,
  )


def normalize_callback_event(callback_event: Mapping[str, Any], *, now_iso: str | None = None) -> dict[str, Any]:
  caller_phone = normalize_phone_e164(
    _coalesce_text(
      callback_event.get("caller_phone"),
      callback_event.get("from_number"),
      callback_event.get("from"),
      callback_event.get("phone"),
    )
  )
  inbound_number = normalize_phone_e164(
    _coalesce_text(
      callback_event.get("inbound_number"),
      callback_event.get("to_number"),
      callback_event.get("to"),
    )
  )
  received_at = _coalesce_text(callback_event.get("received_at"), callback_event.get("timestamp"), now_iso, utc_now_iso())
  callback_id = _coalesce_text(callback_event.get("callback_id"), callback_event.get("call_id")) or stable_id(
    "inbound_callback",
    caller_phone,
    inbound_number,
    received_at,
  )
  return {
    "callback_id": callback_id,
    "retell_call_id": _coalesce_text(callback_event.get("retell_call_id"), callback_event.get("call_id")),
    "caller_phone": caller_phone,
    "caller_phone_redacted": redact_phone(caller_phone),
    "inbound_number": inbound_number,
    "inbound_number_redacted": redact_phone(inbound_number),
    "received_at": received_at,
    "source": _coalesce_text(callback_event.get("source"), "retell_inbound_callback"),
    "raw_direction": _coalesce_text(callback_event.get("direction"), "inbound"),
  }


def normalize_contact_context(contact_context: Mapping[str, Any]) -> dict[str, Any]:
  scenario = _mapping(contact_context.get("scenario"))
  recent_outbound = _mapping(contact_context.get("recent_outbound"))
  last_call = _mapping(contact_context.get("last_call"))
  contact = _mapping(contact_context.get("contact")) or contact_context
  tags = _normalize_tags(contact.get("tags") or contact_context.get("tags"))
  status = _coalesce_text(
    contact_context.get("loan_status"),
    contact_context.get("pipeline_status"),
    contact.get("loan_status"),
    contact.get("pipeline_status"),
    contact.get("status"),
  ).lower()
  phone = normalize_phone_e164(_coalesce_text(contact.get("phone"), contact_context.get("phone")))
  email = _coalesce_text(contact.get("email"), contact_context.get("email")).lower()
  loan_goal = _normalize_goal(_coalesce_text(scenario.get("loan_goal"), contact_context.get("loan_goal"), contact.get("loan_goal")))
  return {
    "ghl_contact_id": _coalesce_text(contact.get("ghl_contact_id"), contact.get("id"), contact_context.get("ghl_contact_id")),
    "first_name": _coalesce_text(contact.get("first_name"), contact.get("firstName"), contact_context.get("first_name"), "there"),
    "last_name": _coalesce_text(contact.get("last_name"), contact.get("lastName"), contact_context.get("last_name")),
    "phone": phone,
    "phone_redacted": redact_phone(phone),
    "email": email,
    "email_redacted": redact_email(email),
    "tags": tags,
    "dnc": _coalesce_bool(contact_context.get("dnc"), contact.get("dnc"), _has_any_tag(tags, {"dnc", "do not call", "stop calling"})),
    "loan_status": status,
    "active_status": status in ACTIVE_STATUSES,
    "loan_goal": loan_goal,
    "property_state": _coalesce_text(scenario.get("property_state"), contact_context.get("property_state"), contact.get("property_state")),
    "property_type": _coalesce_text(scenario.get("property_type"), contact_context.get("property_type"), contact.get("property_type")),
    "purchase_price": _coalesce_text(scenario.get("purchase_price"), contact_context.get("purchase_price")),
    "estimated_value": _coalesce_text(scenario.get("estimated_value"), contact_context.get("estimated_value")),
    "current_balance": _coalesce_text(scenario.get("current_balance"), contact_context.get("current_balance")),
    "credit_score": _coalesce_text(scenario.get("credit_score"), contact_context.get("credit_score")),
    "down_payment_available": _coalesce_text(scenario.get("down_payment_available"), contact_context.get("down_payment_available")),
    "opening_context_line": _coalesce_text(
      contact_context.get("opening_context_line"),
      recent_outbound.get("opening_context_line"),
      _build_context_line(loan_goal, scenario, contact_context),
    ),
    "last_outbound_call_id": _coalesce_text(
      recent_outbound.get("call_id"),
      recent_outbound.get("retell_call_id"),
      contact_context.get("last_outbound_call_id"),
    ),
    "last_outbound_outcome": _coalesce_text(recent_outbound.get("outcome"), contact_context.get("last_outbound_outcome")),
    "last_agent_name": _coalesce_text(recent_outbound.get("agent_name"), contact_context.get("last_agent_name"), "Alex"),
    "last_summary": _coalesce_text(last_call.get("summary"), recent_outbound.get("summary"), contact_context.get("last_summary")),
    "appointment_fallback_requested": _coalesce_bool(
      contact_context.get("appointment_fallback_requested"),
      recent_outbound.get("appointment_fallback_requested"),
    ),
    "explicit_human_request": _coalesce_bool(contact_context.get("explicit_human_request"), contact.get("explicit_human_request")),
    "wrong_product": _detect_wrong_product(contact_context, scenario, tags),
  }


def build_context_match(callback_event: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, Any]:
  callback_phone = _coalesce_text(callback_event.get("caller_phone"))
  context_phone = _coalesce_text(context.get("phone"))
  contact_id = _coalesce_text(context.get("ghl_contact_id"))
  last_outbound_call_id = _coalesce_text(context.get("last_outbound_call_id"))
  matched = bool(contact_id or last_outbound_call_id or (callback_phone and context_phone and callback_phone == context_phone))
  evidence_refs = []
  if contact_id:
    evidence_refs.append(asdict(EvidenceRef("ghl_contact", contact_id, note="callback_contact_match")))
  if last_outbound_call_id:
    evidence_refs.append(asdict(EvidenceRef("retell_call", last_outbound_call_id, note="recent_outbound_context")))
  if callback_phone and context_phone and callback_phone == context_phone:
    evidence_refs.append(asdict(EvidenceRef("phone_match", redact_phone(callback_phone), note="caller_id_matches_context")))

  if not matched:
    return {
      "status": "missing_context",
      "matched": False,
      "confidence": 0.2,
      "reason": "No recent outbound, contact id, or matching phone context supplied.",
      "evidence_refs": [],
    }
  confidence = 0.9 if contact_id and last_outbound_call_id else 0.72
  return {
    "status": "context_found",
    "matched": True,
    "confidence": confidence,
    "reason": "Callback matched to known contact or recent outbound context.",
    "evidence_refs": evidence_refs,
  }


def evaluate_callback_safety(context: Mapping[str, Any]) -> dict[str, Any]:
  blockers: list[str] = []
  warnings: list[str] = []
  if bool(context.get("dnc")):
    blockers.append("do_not_call_flagged")
  if bool(context.get("wrong_product")):
    blockers.append("wrong_product")
  status = _coalesce_text(context.get("loan_status"))
  if status in {"funded", "closed"}:
    blockers.append("already_funded_or_closed")
  elif status in {"submitted", "submitted_to_processing", "in_process", "processing", "active", "application_started", "app_started"}:
    blockers.append("already_active_or_in_process")
  elif status == "declined":
    blockers.append("declined_suppress")
  if not _coalesce_text(context.get("ghl_contact_id")):
    warnings.append("missing_ghl_contact_id")
  if not _coalesce_text(context.get("phone")):
    warnings.append("missing_context_phone")
  return {
    "eligible": not blockers,
    "blockers": blockers,
    "warnings": warnings,
    "write_mode": SHADOW_MODE,
    "external_writes_enabled": False,
  }


def build_callback_opener(context_match: Mapping[str, Any], context: Mapping[str, Any]) -> str:
  first_name = _coalesce_text(context.get("first_name"), "there")
  if not bool(context_match.get("matched")):
    return "Thanks for calling Evolve Funding, this is Alex. Were you calling back about a DSCR loan?"
  context_line = _coalesce_text(context.get("opening_context_line"))
  if context_line:
    return f"Hi {first_name}, this is Alex with Evolve Funding. Looks like you may be calling back about {context_line}. Is that still what you are looking at?"
  return f"Hi {first_name}, this is Alex with Evolve Funding. Were you calling back about DSCR loan options?"


def build_next_best_question(context_match: Mapping[str, Any], context: Mapping[str, Any], safety: Mapping[str, Any]) -> str:
  if safety.get("blockers"):
    return "Confirm the caller's need briefly, avoid sales pressure, and route to internal review instead of transfer."
  if not bool(context_match.get("matched")):
    return "Were you calling back about a DSCR loan, or was it something else?"
  if bool(context.get("explicit_human_request")):
    return "Let me get a loan officer on the line for you."
  goal = _coalesce_text(context.get("loan_goal"))
  if goal == "purchase":
    if not _coalesce_text(context.get("purchase_price")):
      return "Is this still for a purchase, and what is the rough purchase price?"
    if not _coalesce_text(context.get("down_payment_available")):
      return "Do you have the 20% down for that purchase?"
    return "Let me get someone on the line who can look at the purchase details with you."
  if goal in {"cash_out", "refinance"}:
    if not _coalesce_text(context.get("estimated_value")):
      return "What do you think the property is worth today?"
    if not _coalesce_text(context.get("current_balance")):
      return "What is the current loan balance on it?"
    return "Let me get someone on the line who can look at the cash-out details with you."
  return "Is this for a purchase, cash-out, or another DSCR scenario?"


def build_transfer_book_recommendation(
  context_match: Mapping[str, Any],
  context: Mapping[str, Any],
  safety: Mapping[str, Any],
) -> dict[str, Any]:
  if safety.get("blockers"):
    return {
      "action": "internal_review_only",
      "reason": "Safety blocker prevents transfer or booking.",
      "tool_name": "",
      "fallback_action": "stage_internal_summary",
      "handoff_summary": _build_handoff_summary(context),
    }
  if bool(context.get("appointment_fallback_requested")):
    return {
      "action": "book_appointment",
      "reason": "Caller is returning after a missed or failed transfer path.",
      "tool_name": "book_or_transfer",
      "fallback_action": "offer_real_ghl_slots_only",
      "handoff_summary": _build_handoff_summary(context),
    }
  goal = _coalesce_text(context.get("loan_goal"))
  if bool(context.get("explicit_human_request")):
    tool = _transfer_tool_for_goal(goal)
    return {
      "action": "transfer_now",
      "reason": "Caller explicitly requested a human.",
      "tool_name": tool,
      "fallback_action": "book_appointment_if_no_answer",
      "handoff_summary": _build_handoff_summary(context),
    }
  if not bool(context_match.get("matched")):
    return {
      "action": "light_discovery_then_route",
      "reason": "Campaign/contact context is missing; do not assume details.",
      "tool_name": "",
      "fallback_action": "ask_dscl_context_then_transfer_or_book",
      "handoff_summary": "Inbound callback with missing context.",
    }
  return {
    "action": "quick_confirm_then_transfer",
    "reason": "Callback context is available; avoid repeating known details and route quickly.",
    "tool_name": _transfer_tool_for_goal(goal),
    "fallback_action": "book_appointment_if_no_answer",
    "handoff_summary": _build_handoff_summary(context),
  }


def build_review_gate(
  context_match: Mapping[str, Any],
  safety: Mapping[str, Any],
  recommendation: Mapping[str, Any],
) -> dict[str, Any]:
  blockers = list(safety.get("blockers") or [])
  warnings = list(safety.get("warnings") or [])
  if blockers:
    status = "blocked_requires_human_review"
  elif not bool(context_match.get("matched")):
    status = "review_missing_context"
  else:
    status = "ready_for_shadow_review"
  return {
    "status": status,
    "blockers": blockers,
    "warnings": warnings,
    "approval_required_before_live": True,
    "external_writes_enabled": False,
    "live_call_launched": False,
    "ghl_write_attempted": False,
    "los_write_attempted": False,
    "borrower_message_attempted": False,
    "recommended_action": _coalesce_text(recommendation.get("action")),
  }


def build_shadow_queue_row(
  callback_event: Mapping[str, Any],
  context: Mapping[str, Any],
  context_match: Mapping[str, Any],
  opener: str,
  next_best_question: str,
  recommendation: Mapping[str, Any],
  review_gate: Mapping[str, Any],
) -> dict[str, Any]:
  row_id = stable_id(
    SHADOW_QUEUE_NAME,
    _coalesce_text(callback_event.get("callback_id")),
    _coalesce_text(context.get("ghl_contact_id")),
    _coalesce_text(review_gate.get("status")),
  )
  return {
    "queue_row_id": row_id,
    "queue_name": SHADOW_QUEUE_NAME,
    "created_at": utc_now_iso(),
    "status": _coalesce_text(review_gate.get("status")),
    "context_match_status": _coalesce_text(context_match.get("status")),
    "context_confidence": float(context_match.get("confidence") or 0),
    "write_mode": SHADOW_MODE,
    "callback_id": _coalesce_text(callback_event.get("callback_id")),
    "retell_call_id": _coalesce_text(callback_event.get("retell_call_id")),
    "caller_phone_redacted": _coalesce_text(callback_event.get("caller_phone_redacted")),
    "ghl_contact_id": _coalesce_text(context.get("ghl_contact_id")),
    "contact_name": " ".join(
      part for part in [_coalesce_text(context.get("first_name")), _coalesce_text(context.get("last_name"))] if part
    ).strip(),
    "loan_goal": _coalesce_text(context.get("loan_goal")),
    "opener": opener,
    "next_best_question": next_best_question,
    "recommended_action": _coalesce_text(recommendation.get("action")),
    "tool_name": _coalesce_text(recommendation.get("tool_name")),
    "fallback_action": _coalesce_text(recommendation.get("fallback_action")),
    "handoff_summary": _coalesce_text(recommendation.get("handoff_summary")),
    "blockers": list(review_gate.get("blockers") or []),
    "warnings": list(review_gate.get("warnings") or []),
    "evidence_refs": list(context_match.get("evidence_refs") or []),
  }


def build_audit_event(
  callback_event: Mapping[str, Any],
  context: Mapping[str, Any],
  context_match: Mapping[str, Any],
  recommendation: Mapping[str, Any],
  review_gate: Mapping[str, Any],
  shadow_queue_row: Mapping[str, Any],
) -> EventEnvelope:
  occurred_at = utc_now_iso()
  payload = {
    "callback_id": _coalesce_text(callback_event.get("callback_id")),
    "queue_row_id": _coalesce_text(shadow_queue_row.get("queue_row_id")),
    "queue_name": SHADOW_QUEUE_NAME,
    "context_match_status": _coalesce_text(context_match.get("status")),
    "review_gate_status": _coalesce_text(review_gate.get("status")),
    "recommended_action": _coalesce_text(recommendation.get("action")),
    "tool_name": _coalesce_text(recommendation.get("tool_name")),
    "fallback_action": _coalesce_text(recommendation.get("fallback_action")),
    "blockers": list(review_gate.get("blockers") or []),
    "warnings": list(review_gate.get("warnings") or []),
    "caller_phone_redacted": _coalesce_text(callback_event.get("caller_phone_redacted")),
    "contact_phone_redacted": _coalesce_text(context.get("phone_redacted")),
    "contact_email_redacted": _coalesce_text(context.get("email_redacted")),
    "external_writes_enabled": False,
  }
  return EventEnvelope(
    event_id=stable_id(
      "loan_os",
      "inbound_callback_shadow_prepared",
      _coalesce_text(callback_event.get("callback_id")),
      occurred_at,
    ),
    event_type="inbound_callback_shadow_prepared",
    occurred_at=occurred_at,
    ingested_at=occurred_at,
    source_system="loan_os",
    source_id=_coalesce_text(callback_event.get("callback_id")),
    contact_id=_coalesce_text(context.get("ghl_contact_id")),
    campaign_id="inbound_callback",
    actor_type="system",
    actor_id="inbound_callback_shadow",
    payload=payload,
    pii_classification="internal",
    confidence=float(context_match.get("confidence") or 0.5),
    evidence_refs=[
      asdict(EvidenceRef("shadow_queue", _coalesce_text(shadow_queue_row.get("queue_row_id")), note=_coalesce_text(review_gate.get("status")))),
      asdict(EvidenceRef("callback_event", _coalesce_text(callback_event.get("callback_id")), excerpt=payload_excerpt(str(payload)))),
    ],
  )


def _build_context_line(goal: str, scenario: Mapping[str, Any], context: Mapping[str, Any]) -> str:
  explicit = _coalesce_text(context.get("context_line"), context.get("opening_context_line"))
  if explicit:
    return explicit
  goal_text = goal.replace("_", " ").strip()
  property_bits = " ".join(
    bit
    for bit in [
      _coalesce_text(scenario.get("property_type"), context.get("property_type")).replace("_", " "),
      _coalesce_text(scenario.get("property_state"), context.get("property_state")),
    ]
    if bit
  ).strip()
  if goal_text and property_bits:
    return f"the DSCR {goal_text} in {property_bits}"
  if goal_text:
    return f"the DSCR {goal_text}"
  return "DSCR loan options"


def _build_handoff_summary(context: Mapping[str, Any]) -> str:
  name = " ".join(
    part for part in [_coalesce_text(context.get("first_name")), _coalesce_text(context.get("last_name"))] if part
  ).strip() or "the caller"
  goal = _coalesce_text(context.get("loan_goal")).replace("_", " ")
  state = _coalesce_text(context.get("property_state"))
  if goal and state:
    return f"I've got {name} on the line. They're calling back about a DSCR {goal} in {state}."
  if goal:
    return f"I've got {name} on the line. They're calling back about a DSCR {goal}."
  return f"I've got {name} on the line. They're calling back about a DSCR loan."


def _transfer_tool_for_goal(goal: str) -> str:
  normalized = _normalize_goal(goal)
  if normalized == "purchase":
    return "transfer_purchase_to_jr_reactivation_queue"
  if normalized in {"cash_out", "refinance"}:
    return "transfer_cashout_to_jr_reactivation_queue"
  return "transfer_general_to_jr_reactivation_queue"


def _detect_wrong_product(context: Mapping[str, Any], scenario: Mapping[str, Any], tags: list[str]) -> bool:
  text = " ".join(
    str(value or "")
    for value in [
      context.get("loan_goal"),
      context.get("loan_purpose"),
      context.get("product"),
      context.get("last_summary"),
      scenario.get("loan_goal"),
      scenario.get("product"),
      " ".join(tags),
    ]
  ).lower()
  return any(hint in text for hint in WRONG_PRODUCT_HINTS)


def _normalize_goal(value: str) -> str:
  text = _coalesce_text(value).lower().replace("-", "_").replace(" ", "_")
  if text in {"cashout", "cash_out_refi", "cash_out_refinance", "refi", "refinance"}:
    return "cash_out" if text != "refinance" else "refinance"
  if text in {"purchase", "buy", "acquisition"}:
    return "purchase"
  return text


def _mapping(value: Any) -> dict[str, Any]:
  return dict(value) if isinstance(value, Mapping) else {}


def _coalesce_text(*values: Any) -> str:
  for value in values:
    if value is None:
      continue
    text = str(value).strip()
    if text:
      return text
  return ""


def _coalesce_bool(*values: Any) -> bool:
  for value in values:
    if isinstance(value, bool):
      return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
      return True
    if text in {"0", "false", "no", "n", "off"}:
      return False
  return False


def _normalize_tags(value: Any) -> list[str]:
  if isinstance(value, list):
    return [str(item).strip() for item in value if str(item).strip()]
  if isinstance(value, str):
    return [part.strip() for part in value.split(",") if part.strip()]
  return []


def _has_any_tag(tags: list[str], candidates: set[str]) -> bool:
  normalized = {tag.lower() for tag in tags}
  return any(candidate in normalized for candidate in candidates)
