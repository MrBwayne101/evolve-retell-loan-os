from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from loan_os.call_center.ledger import payload_excerpt, stable_id, utc_now_iso


SHADOW_MODE = "shadow_only_no_external_writes"
MAX_SLOTS_TO_OFFER = 3
RECOVERABLE_TRANSFER_FAILURES = {"no_answer", "busy", "voicemail", "timeout", "missed", "not_available"}


@dataclass(frozen=True)
class AppointmentRecoveryResult:
  recovery_id: str
  contact_id: str
  appointment_status: str
  no_show_count: int
  prior_appointment_purpose: str
  assigned_lo: str
  transfer_failure_reason: str
  available_slots: list[dict[str, str]]
  opener: str
  slot_offer_policy: dict[str, Any]
  recommended_action: str
  urgency: str
  review_gate: dict[str, Any]
  note_draft: str
  external_write_allowed: bool
  generated_at: str

  def to_dict(self) -> dict[str, Any]:
    return asdict(self)


def build_appointment_recovery_shadow(
  payload: Mapping[str, Any],
  *,
  generated_at: str | None = None,
) -> AppointmentRecoveryResult:
  generated_at = generated_at or utc_now_iso()
  contact_id = _text(payload.get("contact_id") or payload.get("ghl_contact_id") or payload.get("lead_id"))
  first_name = _text(payload.get("first_name") or payload.get("name")).split(" ")[0] or "there"
  appointment_status = _normalize_status(payload.get("appointment_status") or payload.get("status"))
  no_show_count = _to_int(payload.get("no_show_count") or payload.get("missed_appointment_count"))
  prior_purpose = _purpose(payload)
  assigned_lo = _text(payload.get("assigned_lo") or payload.get("owner") or payload.get("loan_officer")) or "the team"
  transfer_failure_reason = _normalize_reason(payload.get("transfer_failure_reason") or payload.get("transfer_status"))
  available_slots = _normalize_slots(payload.get("available_slots"))
  suppression = _suppression_reason(payload)

  slot_policy = _slot_offer_policy(available_slots)
  action, urgency, review_gate = _decision(
    appointment_status=appointment_status,
    no_show_count=no_show_count,
    transfer_failure_reason=transfer_failure_reason,
    available_slots=available_slots,
    suppression=suppression,
  )
  opener = _opener(
    first_name=first_name,
    prior_purpose=prior_purpose,
    assigned_lo=assigned_lo,
    appointment_status=appointment_status,
    no_show_count=no_show_count,
    transfer_failure_reason=transfer_failure_reason,
    available_slots=available_slots,
    suppression=suppression,
  )
  note_draft = _note_draft(
    contact_id=contact_id,
    appointment_status=appointment_status,
    no_show_count=no_show_count,
    prior_purpose=prior_purpose,
    assigned_lo=assigned_lo,
    transfer_failure_reason=transfer_failure_reason,
    available_slots=available_slots,
    recommended_action=action,
    urgency=urgency,
    suppression=suppression,
    source_summary=_text(payload.get("source_summary") or payload.get("summary") or payload.get("notes")),
  )

  return AppointmentRecoveryResult(
    recovery_id=stable_id(
      "appointment_recovery",
      contact_id,
      appointment_status,
      str(no_show_count),
      transfer_failure_reason,
    ),
    contact_id=contact_id,
    appointment_status=appointment_status,
    no_show_count=no_show_count,
    prior_appointment_purpose=prior_purpose,
    assigned_lo=assigned_lo,
    transfer_failure_reason=transfer_failure_reason,
    available_slots=available_slots,
    opener=opener,
    slot_offer_policy=slot_policy,
    recommended_action=action,
    urgency=urgency,
    review_gate=review_gate,
    note_draft=note_draft,
    external_write_allowed=False,
    generated_at=generated_at,
  )


def _decision(
  *,
  appointment_status: str,
  no_show_count: int,
  transfer_failure_reason: str,
  available_slots: list[dict[str, str]],
  suppression: str,
) -> tuple[str, str, dict[str, Any]]:
  if suppression:
    return (
      f"suppress_{suppression}",
      "blocked",
      _review_gate("blocked_suppression", True, [suppression, "no borrower outreach allowed"]),
    )
  if transfer_failure_reason in RECOVERABLE_TRANSFER_FAILURES:
    if available_slots:
      return (
        "offer_exact_slots_after_transfer_failed",
        "high",
        _review_gate("calendar_safe_shadow_review", True, ["transfer failed; exact slots available"]),
      )
    return (
      "stage_internal_review_no_slots_after_transfer_failed",
      "medium",
      _review_gate("needs_calendar_inventory", True, ["transfer failed but no exact slots available"]),
    )
  if appointment_status in {"no_show", "missed", "cancelled_late"}:
    if no_show_count >= 2:
      return (
        "stage_repeat_no_show_review",
        "medium",
        _review_gate("repeat_no_show_manager_review", True, ["repeat no-show; avoid automated pressure"]),
      )
    if available_slots:
      return (
        "offer_exact_slots_for_first_no_show",
        "high",
        _review_gate("calendar_safe_shadow_review", True, ["first no-show; exact slots available"]),
      )
    return (
      "stage_internal_review_no_slots",
      "medium",
      _review_gate("needs_calendar_inventory", True, ["first no-show but no exact slots available"]),
    )
  return (
    "stage_internal_review_unclear_recovery_context",
    "low",
    _review_gate("unclear_context_review", True, ["appointment or transfer context is insufficient"]),
  )


def _opener(
  *,
  first_name: str,
  prior_purpose: str,
  assigned_lo: str,
  appointment_status: str,
  no_show_count: int,
  transfer_failure_reason: str,
  available_slots: list[dict[str, str]],
  suppression: str,
) -> str:
  if suppression:
    return "No borrower-facing opener. Suppressed for review."
  if transfer_failure_reason in RECOVERABLE_TRANSFER_FAILURES:
    if available_slots:
      return (
        f"Hi {first_name}, this is Evolve Funding. Looks like we could not get someone live for "
        f"{prior_purpose}. I can get you back on the calendar with {assigned_lo}. "
        f"I have {_slot_phrase(available_slots)}. Which one works best?"
      )
    return (
      f"Hi {first_name}, this is Evolve Funding. Looks like we could not get someone live for "
      f"{prior_purpose}. I am checking the calendar now and will get a real time over to you."
    )
  if appointment_status in {"no_show", "missed", "cancelled_late"}:
    if no_show_count >= 2:
      return (
        f"Hi {first_name}, this is Evolve Funding. I was touching base on the DSCR appointment "
        "we missed. If it still matters, I can help find a better time."
      )
    if available_slots:
      return (
        f"Hi {first_name}, this is Evolve Funding. Looks like we missed each other for "
        f"{prior_purpose}. No worries. I have {_slot_phrase(available_slots)}. "
        "Which one is easiest?"
      )
    return (
      f"Hi {first_name}, this is Evolve Funding. Looks like we missed each other for "
      f"{prior_purpose}. I am checking the calendar now and will get a real time over to you."
    )
  return (
    f"Hi {first_name}, this is Evolve Funding. I was checking whether you still needed help with "
    f"{prior_purpose}, but I want to confirm the right next step before we put anything on the calendar."
  )


def _slot_offer_policy(slots: list[dict[str, str]]) -> dict[str, Any]:
  offered = slots[:MAX_SLOTS_TO_OFFER]
  return {
    "policy": "offer_exact_real_slots_only" if offered else "do_not_offer_fake_slots",
    "max_slots_to_offer": MAX_SLOTS_TO_OFFER,
    "slots_offered": offered,
    "if_none_available": "stage_internal_review_and_request_calendar_inventory",
    "fallback_wording": "I am checking the real calendar now so I do not give you a fake time.",
  }


def _review_gate(gate: str, human_review_required: bool, reasons: list[str]) -> dict[str, Any]:
  return {
    "gate": gate,
    "human_review_required": human_review_required,
    "external_write_allowed": False,
    "reasons": reasons,
    "allowed_next_step": "internal_review_only",
  }


def _note_draft(
  *,
  contact_id: str,
  appointment_status: str,
  no_show_count: int,
  prior_purpose: str,
  assigned_lo: str,
  transfer_failure_reason: str,
  available_slots: list[dict[str, str]],
  recommended_action: str,
  urgency: str,
  suppression: str,
  source_summary: str,
) -> str:
  slots = "; ".join(_format_slot(slot) for slot in available_slots[:MAX_SLOTS_TO_OFFER]) or "none available"
  lines = [
    "[APPOINTMENT RECOVERY DRAFT - REVIEW BEFORE POSTING]",
    f"Contact: {contact_id or 'missing'}",
    f"Assigned LO: {assigned_lo}",
    f"Prior purpose: {prior_purpose}",
    f"Appointment status: {appointment_status}",
    f"No-show count: {no_show_count}",
    f"Transfer failure reason: {transfer_failure_reason or 'none'}",
    f"Recommended action: {recommended_action}",
    f"Urgency: {urgency}",
    f"Exact slots available: {slots}",
    f"Suppression: {suppression or 'none'}",
    f"Source summary: {payload_excerpt(source_summary, limit=300) or 'none'}",
    "External write status: blocked; internal review required.",
  ]
  return "\n".join(lines)


def _normalize_slots(value: Any) -> list[dict[str, str]]:
  if not isinstance(value, list):
    return []
  slots: list[dict[str, str]] = []
  for item in value:
    if not isinstance(item, Mapping):
      continue
    start = _text(item.get("start") or item.get("start_time") or item.get("time"))
    label = _text(item.get("label") or item.get("display") or start)
    timezone = _text(item.get("timezone") or item.get("tz") or "America/Los_Angeles")
    lo = _text(item.get("lo") or item.get("owner") or "")
    if start or label:
      slots.append({"start": start, "label": label, "timezone": timezone, "lo": lo})
  return slots


def _slot_phrase(slots: list[dict[str, str]]) -> str:
  labels = [_format_slot(slot) for slot in slots[:MAX_SLOTS_TO_OFFER]]
  if len(labels) == 1:
    return labels[0]
  if len(labels) == 2:
    return f"{labels[0]} or {labels[1]}"
  return f"{', '.join(labels[:-1])}, or {labels[-1]}"


def _format_slot(slot: Mapping[str, str]) -> str:
  label = _text(slot.get("label") or slot.get("start"))
  timezone = _text(slot.get("timezone"))
  return f"{label} {timezone}".strip()


def _suppression_reason(payload: Mapping[str, Any]) -> str:
  if _truthy(payload.get("dnc") or payload.get("dnd") or payload.get("do_not_call")):
    return "dnc"
  if _truthy(payload.get("wrong_product")):
    return "wrong_product"
  if _truthy(payload.get("owner_occupied")):
    return "wrong_product"
  if _truthy(payload.get("already_funded") or payload.get("funded") or payload.get("declined")):
    return "loan_status_suppression"
  return ""


def _purpose(payload: Mapping[str, Any]) -> str:
  explicit = _text(payload.get("prior_appointment_purpose") or payload.get("purpose"))
  if explicit:
    return explicit
  product = _text(payload.get("product") or "DSCR loan")
  goal = _text(payload.get("loan_goal") or payload.get("goal"))
  state = _text(payload.get("property_state") or payload.get("state"))
  parts = [product]
  if goal:
    parts.append(goal.replace("_", " "))
  if state:
    parts.append(f"in {state}")
  return " ".join(parts)


def _normalize_status(value: Any) -> str:
  text = _text(value).lower().replace("-", "_").replace(" ", "_")
  aliases = {
    "noshow": "no_show",
    "no_showed": "no_show",
    "missed_appointment": "missed",
    "transfer_failed": "transfer_failed",
  }
  return aliases.get(text, text)


def _normalize_reason(value: Any) -> str:
  text = _text(value).lower().replace("-", "_").replace(" ", "_")
  aliases = {
    "noanswer": "no_answer",
    "not_answered": "no_answer",
    "agent_unavailable": "not_available",
    "lo_unavailable": "not_available",
    "failed": "not_available",
  }
  return aliases.get(text, text)


def _truthy(value: Any) -> bool:
  if isinstance(value, bool):
    return value
  return _text(value).lower() in {"1", "true", "yes", "y", "on", "dnc", "dnd"}


def _to_int(value: Any) -> int:
  try:
    return int(value)
  except (TypeError, ValueError):
    return 0


def _text(value: Any) -> str:
  return str(value or "").strip()
