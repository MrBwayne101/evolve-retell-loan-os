from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from loan_os.call_center.ledger import amount_bucket, confidence_to_float, redact_phone


OUTCOME_TAXONOMY = [
  "pending_call",
  "booked",
  "hot_callback",
  "interested_not_ready",
  "nurture",
  "voicemail",
  "no_answer_or_short",
  "not_interested",
  "do_not_call",
  "wrong_product",
  "unqualified",
  "bad_number",
  "tool_failure",
]

APPROVAL_STATES = ["draft", "review_required", "approved_internal_only", "rejected", "escalated"]


@dataclass(frozen=True)
class PostCallAssessment:
  contact_id: str
  owner: str
  outcome: str
  route: str
  next_action: str
  approval_state: str
  confidence_label: str
  confidence_score: float
  evidence_summary: str
  review_reason: str
  phone_redacted: str
  revenue_band: str


def as_int(value: Any) -> int:
  try:
    return int(float(str(value or "0").strip()))
  except ValueError:
    return 0


def classify_outcome(row: Mapping[str, Any]) -> str:
  explicit = str(row.get("outcome") or "").strip().lower()
  if explicit:
    return explicit
  if str(row.get("appointment_booked") or "").strip().lower() == "true":
    return "booked"
  if str(row.get("transfer_requested") or "").strip().lower() == "true":
    return "hot_callback"
  seconds = as_int(row.get("connected_seconds"))
  words = as_int(row.get("prospect_words_estimate"))
  if seconds <= 0 and words <= 0:
    return "pending_call"
  if seconds < 20 or words < 8:
    return "no_answer_or_short"
  if words >= 80 or seconds >= 180:
    return "interested_not_ready"
  return "nurture"


def route_for_outcome(outcome: str) -> str:
  return {
    "pending_call": "await_observer_capture",
    "booked": "prepare_lo_handoff",
    "hot_callback": "same_day_lo_callback",
    "interested_not_ready": "lo_review_then_callback_or_nurture",
    "nurture": "review_gated_nurture",
    "voicemail": "retry_later",
    "no_answer_or_short": "retry_later",
    "not_interested": "suppress_after_review",
    "do_not_call": "dnc_review_and_suppress",
    "wrong_product": "alternate_product_review",
    "unqualified": "no_transfer_unqualified_review",
    "bad_number": "phone_verification_review",
    "tool_failure": "manual_recovery_required",
  }.get(outcome, "human_review")


def next_action_for_outcome(outcome: str, row: Mapping[str, Any]) -> str:
  explicit = str(row.get("lo_next_action") or row.get("default_next_action") or "").strip()
  if explicit:
    return explicit
  return {
    "pending_call": "Capture transcript, recording, and disposition before any writeback discussion.",
    "booked": "Prepare LO handoff summary and calendar context in draft only.",
    "hot_callback": "Queue same-day LO callback with transcript evidence and no borrower-facing pricing.",
    "interested_not_ready": "Assign transcript review and choose callback or nurture after approval.",
    "nurture": "Hold for nurture copy review; do not write GHL status.",
    "voicemail": "Retry under campaign rules only after review.",
    "no_answer_or_short": "Retry later or pause based on ownership capacity.",
    "not_interested": "Document evidence and suppress only through audited workflow later.",
    "do_not_call": "Escalate for DNC review; no autonomous write.",
    "wrong_product": "Route to alternate-product review, not DSCR transfer.",
    "unqualified": "Keep in review queue; no live transfer.",
    "bad_number": "Verify phone quality before suppression.",
    "tool_failure": "Create internal recovery task and inspect tool trace.",
  }.get(outcome, "Review manually.")


def confidence_label(row: Mapping[str, Any], outcome: str) -> tuple[str, float]:
  explicit = str(row.get("confidence") or row.get("enrichment_confidence") or "").strip().lower()
  if explicit in {"high", "medium", "low"}:
    score = confidence_to_float(explicit)
  else:
    seconds = as_int(row.get("connected_seconds"))
    words = as_int(row.get("prospect_words_estimate"))
    score = 0.35
    if outcome == "pending_call":
      score = 0.42
    elif seconds >= 120 or words >= 80:
      score = 0.8
    elif seconds >= 30 or words >= 20:
      score = 0.6
  if score >= 0.8:
    return "high", score
  if score >= 0.55:
    return "medium", score
  return "low", score


def review_reason(row: Mapping[str, Any], outcome: str) -> str:
  if outcome in {"do_not_call", "bad_number", "tool_failure"}:
    return "safety_or_data_quality"
  if outcome == "pending_call":
    return "awaiting_transcript_and_recording"
  if str(row.get("writeback_status") or "").strip():
    return str(row.get("writeback_status"))
  return "shadow_mode_review_required"


def build_assessment(row: Mapping[str, Any], owner: str) -> PostCallAssessment:
  outcome = classify_outcome(row)
  label, score = confidence_label(row, outcome)
  evidence = []
  if row.get("call_id"):
    evidence.append(f"call_id={row.get('call_id')}")
  if row.get("recording_url"):
    evidence.append("recording=present")
  if row.get("review_questions"):
    evidence.append("review_packet")
  if row.get("reactivation_brief"):
    evidence.append("reactivation_brief")
  return PostCallAssessment(
    contact_id=str(row.get("contact_id") or ""),
    owner=owner,
    outcome=outcome,
    route=route_for_outcome(outcome),
    next_action=next_action_for_outcome(outcome, row),
    approval_state="review_required",
    confidence_label=label,
    confidence_score=score,
    evidence_summary=", ".join(evidence) or "fixture_only",
    review_reason=review_reason(row, outcome),
    phone_redacted=redact_phone(str(row.get("phone") or "")),
    revenue_band=amount_bucket(row.get("estimated_largest_amount") or row.get("largest_amount")),
  )
