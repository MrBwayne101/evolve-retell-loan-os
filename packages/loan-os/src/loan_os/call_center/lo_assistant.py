from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

from loan_os.call_center.ledger import amount_bucket, payload_excerpt, redact_email, redact_phone


SHADOW_MODE = "shadow_only_no_external_writes"
UNASSIGNED_OWNER = "unassigned_review"
SUPPRESS_STATUSES = {
  "already_funded",
  "funded",
  "in_process",
  "submitted_to_processing",
  "declined",
  "do_not_call",
  "dnc",
  "wrong_person",
  "bad_number",
}


@dataclass(frozen=True)
class OwnerResolution:
  assigned_lo: str
  owner_source: str
  owner_confidence: str
  owner_confidence_score: float
  evidence_excerpt: str

  def to_record(self) -> dict[str, Any]:
    return asdict(self)


@dataclass(frozen=True)
class LOAssistantShadowRow:
  contact_id: str
  first_name: str
  assigned_lo: str
  owner_confidence: str
  owner_source: str
  owner_evidence_excerpt: str
  status: str
  opener: str
  next_question: str
  recommended_action: str
  review_gate: dict[str, Any]
  note_task_draft: dict[str, Any]
  scenario_summary: str
  prior_objection: str
  score: float
  revenue_band: str
  write_mode: str = SHADOW_MODE

  def to_record(self) -> dict[str, Any]:
    return asdict(self)


def build_lo_assistant_shadow_queue(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
  """Build a deterministic review-only LO Assistant queue.

  This path is intentionally operational shadow mode. It ranks and drafts work,
  but never writes to GHL, LOS, Retell, calendars, or borrower channels.
  """

  built = [build_lo_assistant_row(row).to_record() for row in rows]
  return sorted(
    built,
    key=lambda row: (
      _action_rank(str(row.get("recommended_action") or "")),
      float(row.get("score") or 0),
      str(row.get("contact_id") or ""),
    ),
    reverse=True,
  )


def build_lo_assistant_row(row: Mapping[str, Any]) -> LOAssistantShadowRow:
  contact_id = _text(row.get("contact_id") or row.get("ghl_contact_id"))
  first_name = _text(row.get("first_name"), "there")
  scenario = _mapping(row.get("scenario"))
  status = _status(row)
  owner = resolve_owner(row)
  score = _float(row.get("score") or row.get("readiness_score") or row.get("priority_score"))
  prior_objection = _text(row.get("prior_objection") or row.get("last_objection"))
  open_next_step = _text(row.get("open_next_step") or row.get("recommended_next_step") or row.get("next_step"))
  scenario_summary = summarize_scenario(row, scenario)
  revenue_band = amount_bucket(
    row.get("loan_amount")
    or row.get("estimated_largest_amount")
    or scenario.get("loan_amount")
    or scenario.get("purchase_price")
    or scenario.get("estimated_value")
  )

  recommended_action = recommend_action(
    status=status,
    owner=owner,
    open_next_step=open_next_step,
    score=score,
    row=row,
  )
  opener = build_opener(
    first_name=first_name,
    owner=owner,
    scenario_summary=scenario_summary,
    open_next_step=open_next_step,
    row=row,
  )
  next_question = build_next_question(
    scenario=scenario,
    prior_objection=prior_objection,
    open_next_step=open_next_step,
    recommended_action=recommended_action,
  )
  review_gate = build_review_gate(
    status=status,
    owner=owner,
    open_next_step=open_next_step,
    recommended_action=recommended_action,
    row=row,
  )
  note_task_draft = build_note_task_draft(
    contact_id=contact_id,
    first_name=first_name,
    owner=owner,
    status=status,
    scenario_summary=scenario_summary,
    prior_objection=prior_objection,
    open_next_step=open_next_step,
    recommended_action=recommended_action,
    review_gate=review_gate,
    row=row,
  )

  return LOAssistantShadowRow(
    contact_id=contact_id,
    first_name=first_name,
    assigned_lo=owner.assigned_lo,
    owner_confidence=owner.owner_confidence,
    owner_source=owner.owner_source,
    owner_evidence_excerpt=owner.evidence_excerpt,
    status=status,
    opener=opener,
    next_question=next_question,
    recommended_action=recommended_action,
    review_gate=review_gate,
    note_task_draft=note_task_draft,
    scenario_summary=scenario_summary,
    prior_objection=prior_objection,
    score=score,
    revenue_band=revenue_band,
  )


def resolve_owner(row: Mapping[str, Any]) -> OwnerResolution:
  transcript_evidence = _mapping(row.get("transcript_owner_evidence"))
  transcript_owner = _text(
    transcript_evidence.get("owner")
    or transcript_evidence.get("assigned_lo")
    or row.get("transcript_owner")
    or row.get("owner_from_transcript")
  )
  if transcript_owner:
    confidence = _float(transcript_evidence.get("confidence") or row.get("transcript_owner_confidence") or 0.9)
    return OwnerResolution(
      assigned_lo=transcript_owner,
      owner_source="transcript_evidence",
      owner_confidence=_confidence_label(confidence),
      owner_confidence_score=confidence,
      evidence_excerpt=payload_excerpt(
        _text(transcript_evidence.get("excerpt") or row.get("transcript_owner_excerpt") or "Transcript indicates LO ownership.")
      ),
    )

  crm_owner = _text(row.get("crm_owner") or row.get("assigned_lo") or row.get("owner"))
  if crm_owner:
    return OwnerResolution(
      assigned_lo=crm_owner,
      owner_source="crm_owner",
      owner_confidence="medium",
      owner_confidence_score=0.62,
      evidence_excerpt="CRM owner only; no transcript ownership evidence found.",
    )

  return OwnerResolution(
    assigned_lo=UNASSIGNED_OWNER,
    owner_source="unassigned",
    owner_confidence="low",
    owner_confidence_score=0.2,
    evidence_excerpt="No transcript or CRM owner evidence found.",
  )


def recommend_action(
  *,
  status: str,
  owner: OwnerResolution,
  open_next_step: str,
  score: float,
  row: Mapping[str, Any],
) -> str:
  if status in SUPPRESS_STATUSES:
    return "suppress_from_lo_assistant_pending_human_review"
  if owner.assigned_lo == UNASSIGNED_OWNER or owner.owner_confidence == "low":
    return "unassigned_owner_human_review"
  if not open_next_step:
    return "human_review_missing_next_step"
  if _truthy(row.get("appointment_pending")) or "appointment" in open_next_step.lower():
    return "book_or_confirm_assigned_lo_appointment_shadow"
  if _truthy(row.get("transfer_ready")) or score >= 80:
    return "same_day_transfer_or_callback_with_assigned_lo_shadow"
  return "stage_assigned_lo_followup_task_shadow"


def build_opener(
  *,
  first_name: str,
  owner: OwnerResolution,
  scenario_summary: str,
  open_next_step: str,
  row: Mapping[str, Any],
) -> str:
  if owner.assigned_lo == UNASSIGNED_OWNER:
    return (
      f"Hi {first_name}, this is Alex with Evolve Funding. I was touching base on your DSCR loan "
      "request. Are you still looking at options, or are you all set?"
    )

  context = scenario_summary or _text(row.get("last_meaningful_context"), "your DSCR loan options")
  next_step = f" about {open_next_step}" if open_next_step else ""
  return (
    f"Hi {first_name}, this is Alex with Evolve Funding. I'm helping {owner.assigned_lo} follow up. "
    f"I was just touching base on {context}{next_step}. Are you still looking at this, or did you get it handled?"
  )


def build_next_question(
  *,
  scenario: Mapping[str, Any],
  prior_objection: str,
  open_next_step: str,
  recommended_action: str,
) -> str:
  if recommended_action.startswith("suppress"):
    return "Do not call. Human review should confirm suppression reason before any CRM status change."
  if recommended_action == "unassigned_owner_human_review":
    return "Human review: identify the LO who actually spoke with the borrower before outreach."
  if recommended_action == "human_review_missing_next_step":
    return "Human review: choose the next step from transcript evidence before borrower outreach."
  if prior_objection:
    return f"Last time it sounded like {prior_objection}. Is that still the main thing holding this up?"
  goal = _text(scenario.get("goal") or scenario.get("loan_goal") or scenario.get("purpose"))
  if "purchase" in goal.lower():
    return "Did you already find a property, or are you still trying to get pre-approved before offers?"
  if "cash" in goal.lower() or "refi" in goal.lower():
    return "Are you still looking to pull cash out of that rental, or did you already handle the refinance?"
  if open_next_step:
    return f"Is {open_next_step} still the right next step, or did something change?"
  return "Are you still looking at DSCR loan options, or are you all set?"


def build_review_gate(
  *,
  status: str,
  owner: OwnerResolution,
  open_next_step: str,
  recommended_action: str,
  row: Mapping[str, Any],
) -> dict[str, Any]:
  reasons: list[str] = []
  if status in SUPPRESS_STATUSES:
    reasons.append(f"suppression_status:{status}")
  if owner.assigned_lo == UNASSIGNED_OWNER:
    reasons.append("missing_owner")
  if owner.owner_source != "transcript_evidence":
    reasons.append("owner_not_transcript_backed")
  if not open_next_step:
    reasons.append("missing_next_step")
  if _truthy(row.get("dnc")) or _truthy(row.get("phone_dnc")):
    reasons.append("dnc_flag")

  return {
    "external_writes_allowed": False,
    "borrower_contact_allowed": not reasons and not recommended_action.startswith("suppress"),
    "human_review_required": bool(reasons),
    "review_reasons": reasons or ["shadow_mode_approval_required_before_launch"],
    "write_mode": SHADOW_MODE,
  }


def build_note_task_draft(
  *,
  contact_id: str,
  first_name: str,
  owner: OwnerResolution,
  status: str,
  scenario_summary: str,
  prior_objection: str,
  open_next_step: str,
  recommended_action: str,
  review_gate: Mapping[str, Any],
  row: Mapping[str, Any],
) -> dict[str, Any]:
  phone = redact_phone(_text(row.get("phone") or row.get("contact_phone")))
  email = redact_email(_text(row.get("email") or row.get("contact_email")))
  lines = [
    "LO ASSISTANT SHADOW DRAFT - DO NOT AUTO-WRITE",
    f"Contact: {contact_id or 'unknown'} / {first_name}",
    f"Assigned LO: {owner.assigned_lo} ({owner.owner_source}, {owner.owner_confidence})",
    f"Status: {status}",
    f"Scenario: {scenario_summary or 'unknown'}",
    f"Prior objection: {prior_objection or 'none captured'}",
    f"Open next step: {open_next_step or 'missing - human review required'}",
    f"Recommended action: {recommended_action}",
    f"Owner evidence: {owner.evidence_excerpt}",
    f"External write status: blocked ({SHADOW_MODE})",
  ]
  if phone:
    lines.append(f"Phone: {phone}")
  if email:
    lines.append(f"Email: {email}")
  if review_gate.get("review_reasons"):
    lines.append("Review reasons: " + ", ".join(str(reason) for reason in review_gate["review_reasons"]))

  return {
    "type": "ghl_note_and_task_draft",
    "contact_id": contact_id,
    "assigned_lo": owner.assigned_lo,
    "task_title": _task_title(owner, recommended_action),
    "note_body": "\n".join(lines),
    "external_write_allowed": False,
    "created_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
  }


def summarize_scenario(row: Mapping[str, Any], scenario: Mapping[str, Any]) -> str:
  pieces: list[str] = []
  goal = _text(scenario.get("goal") or scenario.get("loan_goal") or scenario.get("purpose") or row.get("loan_goal"))
  state = _text(scenario.get("property_state") or row.get("property_state"))
  property_type = _text(scenario.get("property_type") or row.get("property_type"))
  amount = _text(scenario.get("loan_amount") or scenario.get("purchase_price") or scenario.get("estimated_value") or row.get("loan_amount"))
  if goal:
    pieces.append(f"DSCR {goal}")
  else:
    pieces.append("DSCR loan")
  if property_type:
    pieces.append(property_type.replace("_", " "))
  if state:
    pieces.append(f"in {state}")
  if amount:
    pieces.append(f"around {amount}")
  return " ".join(pieces)


def _status(row: Mapping[str, Any]) -> str:
  explicit = _text(row.get("status") or row.get("lead_status") or row.get("loan_status")).lower().replace(" ", "_")
  if explicit:
    return explicit
  if _truthy(row.get("dnc")) or _truthy(row.get("phone_dnc")):
    return "do_not_call"
  if _truthy(row.get("already_funded")):
    return "already_funded"
  if _truthy(row.get("in_process")):
    return "in_process"
  return "active_or_stalled"


def _task_title(owner: OwnerResolution, recommended_action: str) -> str:
  if recommended_action.startswith("suppress"):
    return "Review before suppression - LO Assistant shadow"
  if owner.assigned_lo == UNASSIGNED_OWNER:
    return "Resolve LO ownership - LO Assistant shadow"
  if "transfer" in recommended_action:
    return f"{owner.assigned_lo}: same-day callback/transfer candidate"
  if "appointment" in recommended_action:
    return f"{owner.assigned_lo}: book or confirm appointment"
  return f"{owner.assigned_lo}: follow up on stalled DSCR lead"


def _action_rank(action: str) -> int:
  if "transfer" in action:
    return 5
  if "appointment" in action:
    return 4
  if "followup" in action:
    return 3
  if "human_review" in action:
    return 2
  if "suppress" in action:
    return 1
  return 0


def _mapping(value: Any) -> Mapping[str, Any]:
  return value if isinstance(value, Mapping) else {}


def _text(value: Any, fallback: str = "") -> str:
  text = str(value or "").strip()
  return text if text else fallback


def _float(value: Any) -> float:
  try:
    return float(str(value or "0").strip())
  except ValueError:
    return 0.0


def _confidence_label(score: float) -> str:
  if score >= 0.8:
    return "high"
  if score >= 0.5:
    return "medium"
  return "low"


def _truthy(value: Any) -> bool:
  if isinstance(value, bool):
    return value
  return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}
