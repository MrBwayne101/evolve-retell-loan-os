from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping

from loan_os.call_center.ledger import payload_excerpt, stable_id, utc_now_iso


FollowUpType = Literal["app_completion", "missing_docs", "condition_follow_up"]
EscalationTarget = Literal["none", "loan_officer", "processor", "compliance_review", "human_review"]

SHADOW_MODE = "shadow_only_no_external_writes"
SECURE_LINK_RULE = "secure_link_only"

SENSITIVE_BY_VOICE_PATTERNS = (
  re.compile(r"\b(ssn|social security|social)\b", re.IGNORECASE),
  re.compile(r"\b(bank login|bank password|password|username|credentials?)\b", re.IGNORECASE),
  re.compile(r"\b(account number|routing number)\b", re.IGNORECASE),
  re.compile(r"\b(send|give|tell|read).{0,30}\b(ssn|social|password|account number|routing number)\b", re.IGNORECASE),
)

PRICING_OR_UNDERWRITING_PATTERNS = (
  re.compile(r"\b(rate|pricing|points?|apr|payment|term sheet|quote)\b", re.IGNORECASE),
  re.compile(r"\b(approved|denied|declined|underwriting|condition means|clear to close|ctc)\b", re.IGNORECASE),
  re.compile(r"\b(can i qualify|will i qualify|am i approved)\b", re.IGNORECASE),
)

CONDITION_STATUS_HINTS = (
  "condition",
  "uw",
  "underwriting",
  "stp",
  "suspense",
  "restructure",
)


@dataclass(frozen=True)
class DocumentConditionInput:
  contact_id: str
  first_name: str
  loan_id: str
  app_status: str
  loan_status: str
  missing_items: list[str]
  processor: str
  assigned_lo: str
  secure_link_status: str
  last_condition_email_timestamp: str
  borrower_question: str
  source: str

  def to_record(self) -> dict[str, Any]:
    return asdict(self)


@dataclass(frozen=True)
class ChecklistItem:
  label: str
  category: str
  secure_link_required: bool
  borrower_can_discuss_by_voice: bool

  def to_record(self) -> dict[str, Any]:
    return asdict(self)


@dataclass(frozen=True)
class ReviewGate:
  status: str
  reasons: list[str]
  external_write_allowed: bool
  live_call_allowed: bool
  requires_human_approval: bool
  write_mode: str = SHADOW_MODE

  def to_record(self) -> dict[str, Any]:
    return asdict(self)


@dataclass(frozen=True)
class DocumentConditionShadowResult:
  shadow_id: str
  follow_up_type: FollowUpType
  normalized_input: DocumentConditionInput
  safe_opener: str
  item_checklist: list[ChecklistItem]
  escalation_target: EscalationTarget
  escalation_rule: str
  processor_alert_draft: str
  borrower_safe_note_draft: str
  review_gate: ReviewGate
  evidence_refs: list[str]
  generated_at: str

  def to_record(self) -> dict[str, Any]:
    return {
      "shadow_id": self.shadow_id,
      "follow_up_type": self.follow_up_type,
      "normalized_input": self.normalized_input.to_record(),
      "safe_opener": self.safe_opener,
      "item_checklist": [item.to_record() for item in self.item_checklist],
      "escalation_target": self.escalation_target,
      "escalation_rule": self.escalation_rule,
      "processor_alert_draft": self.processor_alert_draft,
      "borrower_safe_note_draft": self.borrower_safe_note_draft,
      "review_gate": self.review_gate.to_record(),
      "evidence_refs": list(self.evidence_refs),
      "generated_at": self.generated_at,
    }


def normalize_document_condition_input(payload: Mapping[str, Any]) -> DocumentConditionInput:
  missing_items = _list_text(
    payload.get("missing_items")
    or payload.get("conditions")
    or payload.get("required_documents")
    or payload.get("requested_items")
  )
  return DocumentConditionInput(
    contact_id=_text(payload.get("contact_id") or payload.get("ghl_contact_id")),
    first_name=_text(payload.get("first_name") or payload.get("borrower_first_name")),
    loan_id=_text(payload.get("loan_id") or payload.get("app_id") or payload.get("los_file_id")),
    app_status=_text(payload.get("app_status") or payload.get("application_status")),
    loan_status=_text(payload.get("loan_status") or payload.get("status")),
    missing_items=missing_items,
    processor=_text(payload.get("processor") or payload.get("processor_name")),
    assigned_lo=_text(payload.get("assigned_lo") or payload.get("loan_officer") or payload.get("owner")),
    secure_link_status=_text(payload.get("secure_link_status") or payload.get("upload_link_status")),
    last_condition_email_timestamp=_text(
      payload.get("last_condition_email_timestamp")
      or payload.get("condition_email_timestamp")
      or payload.get("last_email_at")
    ),
    borrower_question=_text(payload.get("borrower_question") or payload.get("latest_borrower_message")),
    source=_text(payload.get("source") or "shadow_fixture"),
  )


def build_document_conditions_shadow(
  payload: Mapping[str, Any],
  *,
  generated_at: str | None = None,
) -> DocumentConditionShadowResult:
  normalized = normalize_document_condition_input(payload)
  generated = generated_at or utc_now_iso()
  follow_up_type = infer_follow_up_type(normalized)
  checklist = build_item_checklist(normalized.missing_items)
  safety_flags = detect_safety_flags(normalized)
  escalation_target, escalation_rule = choose_escalation(normalized, safety_flags)
  safe_opener = build_safe_opener(normalized, follow_up_type, safety_flags)
  processor_alert = build_processor_alert_draft(normalized, follow_up_type, escalation_target, safety_flags, checklist)
  borrower_note = build_borrower_safe_note_draft(normalized, follow_up_type, escalation_target, checklist)
  review_gate = build_review_gate(normalized, safety_flags, escalation_target)
  shadow_id = stable_id(
    "document_conditions",
    normalized.contact_id,
    normalized.loan_id,
    follow_up_type,
    generated,
  )
  evidence_refs = [
    f"source:{normalized.source or 'unknown'}",
    f"contact:{normalized.contact_id or 'missing'}",
    f"loan:{normalized.loan_id or 'missing'}",
    f"condition_email:{'present' if normalized.last_condition_email_timestamp else 'missing'}",
    f"secure_link:{normalized.secure_link_status or 'unknown'}",
  ]
  return DocumentConditionShadowResult(
    shadow_id=shadow_id,
    follow_up_type=follow_up_type,
    normalized_input=normalized,
    safe_opener=safe_opener,
    item_checklist=checklist,
    escalation_target=escalation_target,
    escalation_rule=escalation_rule,
    processor_alert_draft=processor_alert,
    borrower_safe_note_draft=borrower_note,
    review_gate=review_gate,
    evidence_refs=evidence_refs,
    generated_at=generated,
  )


def infer_follow_up_type(data: DocumentConditionInput) -> FollowUpType:
  status_text = " ".join([data.app_status, data.loan_status]).lower()
  if data.last_condition_email_timestamp or any(hint in status_text for hint in CONDITION_STATUS_HINTS):
    return "condition_follow_up"
  if data.missing_items:
    return "missing_docs"
  return "app_completion"


def build_item_checklist(items: list[str]) -> list[ChecklistItem]:
  if not items:
    return []
  return [
    ChecklistItem(
      label=payload_excerpt(item, limit=120),
      category=classify_item(item),
      secure_link_required=True,
      borrower_can_discuss_by_voice=False,
    )
    for item in items
  ]


def classify_item(item: str) -> str:
  text = item.lower()
  if any(term in text for term in ("bank", "statement", "asset", "liquidity", "reserve")):
    return "asset_or_bank_document"
  if any(term in text for term in ("lease", "rent", "dscr", "income")):
    return "property_income_document"
  if any(term in text for term in ("insurance", "binder", "hazard")):
    return "insurance_document"
  if any(term in text for term in ("entity", "llc", "operating agreement", "ein")):
    return "entity_document"
  if any(term in text for term in ("id", "license", "passport")):
    return "identity_document"
  if any(term in text for term in ("ssn", "social security", "account number", "routing number")):
    return "restricted_sensitive_data"
  return "loan_condition_document"


def detect_safety_flags(data: DocumentConditionInput) -> list[str]:
  flags: list[str] = []
  question = data.borrower_question
  if any(pattern.search(question) for pattern in SENSITIVE_BY_VOICE_PATTERNS):
    flags.append("sensitive_info_by_voice_request")
  if any(pattern.search(question) for pattern in PRICING_OR_UNDERWRITING_PATTERNS):
    flags.append("pricing_or_underwriting_question")
  if data.secure_link_status.lower() in {"", "missing", "not_sent", "expired", "broken", "unknown"}:
    flags.append("secure_link_not_confirmed")
  if not data.processor and infer_follow_up_type(data) == "condition_follow_up":
    flags.append("missing_processor_for_condition_follow_up")
  if not data.contact_id:
    flags.append("missing_contact_id")
  return flags


def choose_escalation(
  data: DocumentConditionInput,
  safety_flags: list[str],
) -> tuple[EscalationTarget, str]:
  if "sensitive_info_by_voice_request" in safety_flags:
    return (
      "compliance_review",
      "Do not collect or repeat sensitive information by voice; send/confirm secure link only and route for human review.",
    )
  if "pricing_or_underwriting_question" in safety_flags:
    target: EscalationTarget = "processor" if infer_follow_up_type(data) == "condition_follow_up" else "loan_officer"
    return (
      target,
      "Do not answer pricing, approval, denial, or underwriting-meaning questions; alert the assigned human owner.",
    )
  if "secure_link_not_confirmed" in safety_flags or "missing_processor_for_condition_follow_up" in safety_flags:
    return (
      "processor",
      "Processor review required before borrower outreach because secure-link delivery or condition owner is not confirmed.",
    )
  return (
    "none",
    "Safe to draft secure-link follow-up for human approval; no external write or call is launched from shadow mode.",
  )


def build_safe_opener(
  data: DocumentConditionInput,
  follow_up_type: FollowUpType,
  safety_flags: list[str],
) -> str:
  name = data.first_name or "there"
  if "sensitive_info_by_voice_request" in safety_flags:
    return (
      f"Hi {name}, this is Evolve Funding. I cannot take sensitive information over the phone, "
      "but I can make sure you have the secure link and get a human to help with next steps."
    )
  if follow_up_type == "condition_follow_up":
    processor = data.processor or "the processing team"
    email_part = _relative_email_phrase(data.last_condition_email_timestamp)
    return (
      f"Hi {name}, this is Evolve Funding. {processor} asked me to touch base on the condition "
      f"email{email_part}. Did you receive it and the secure upload link?"
    )
  if follow_up_type == "missing_docs":
    return (
      f"Hi {name}, this is Evolve Funding. I am calling about the secure upload link for the "
      "remaining DSCR loan items. Did you receive it?"
    )
  return (
    f"Hi {name}, this is Evolve Funding. I am calling to make sure you received the secure "
    "application link and see if anything is blocking you from finishing it."
  )


def build_processor_alert_draft(
  data: DocumentConditionInput,
  follow_up_type: FollowUpType,
  escalation_target: EscalationTarget,
  safety_flags: list[str],
  checklist: list[ChecklistItem],
) -> str:
  owner = data.processor or data.assigned_lo or "Human review"
  return "\n".join(
    [
      "[DOCUMENT/CONDITION FOLLOW-UP DRAFT - REVIEW BEFORE SENDING]",
      f"Owner: {owner}",
      f"Contact: {data.contact_id or 'missing'}",
      f"Loan/App: {data.loan_id or 'missing'}",
      f"Follow-up type: {follow_up_type}",
      f"Application status: {data.app_status or 'unknown'}",
      f"Loan status: {data.loan_status or 'unknown'}",
      f"Secure link status: {data.secure_link_status or 'unknown'}",
      f"Last condition email: {data.last_condition_email_timestamp or 'missing'}",
      f"Checklist: {_checklist_summary(checklist)}",
      f"Escalation target: {escalation_target}",
      f"Safety flags: {', '.join(safety_flags) if safety_flags else 'none'}",
      "External write status: blocked; human approval required.",
    ]
  )


def build_borrower_safe_note_draft(
  data: DocumentConditionInput,
  follow_up_type: FollowUpType,
  escalation_target: EscalationTarget,
  checklist: list[ChecklistItem],
) -> str:
  lines = [
    "[BORROWER-SAFE NOTE DRAFT - REVIEW BEFORE POSTING/SENDING]",
    f"Purpose: {follow_up_type}",
    "Message: Confirm borrower received the secure Evolve Funding link and ask whether they need help using it.",
  ]
  if checklist:
    lines.append(f"Items to reference generally: {_checklist_summary(checklist)}")
  else:
    lines.append("Items to reference generally: application completion only; do not request sensitive details by voice.")
  if escalation_target in {"loan_officer", "processor"}:
    lines.append(f"Human escalation: route borrower question to {escalation_target.replace('_', ' ')}.")
  lines.extend(
    [
      "Forbidden: do not ask for SSN, bank credentials, account numbers, document images, or document contents by voice.",
      "Allowed: confirm receipt of secure link, resend secure link after human approval, and answer basic process questions.",
    ]
  )
  return "\n".join(lines)


def build_review_gate(
  data: DocumentConditionInput,
  safety_flags: list[str],
  escalation_target: EscalationTarget,
) -> ReviewGate:
  reasons = ["shadow_mode_blocks_external_writes"]
  reasons.extend(safety_flags)
  if data.missing_items:
    reasons.append("secure_link_required_for_all_missing_items")
  if escalation_target != "none":
    reasons.append(f"escalation_required:{escalation_target}")
  return ReviewGate(
    status="review_required",
    reasons=sorted(set(reasons)),
    external_write_allowed=False,
    live_call_allowed=False,
    requires_human_approval=True,
  )


def _relative_email_phrase(timestamp: str) -> str:
  raw = timestamp.strip()
  if not raw:
    return ""
  parsed = _parse_timestamp(raw)
  if not parsed:
    return f" from {payload_excerpt(raw, limit=40)}"
  return f" sent {parsed.date().isoformat()}"


def _parse_timestamp(value: str) -> datetime | None:
  text = value.strip()
  if not text:
    return None
  try:
    if text.endswith("Z"):
      return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    return datetime.fromisoformat(text)
  except ValueError:
    return None


def _checklist_summary(checklist: list[ChecklistItem]) -> str:
  if not checklist:
    return "none"
  labels = [item.label for item in checklist[:6]]
  extra = len(checklist) - len(labels)
  suffix = f" (+{extra} more)" if extra > 0 else ""
  return "; ".join(labels) + suffix


def _text(value: Any) -> str:
  return str(value or "").strip()


def _list_text(value: Any) -> list[str]:
  if value is None:
    return []
  if isinstance(value, str):
    return [line.strip(" -\t") for line in value.splitlines() if line.strip(" -\t")]
  if isinstance(value, (list, tuple, set)):
    return [_text(item) for item in value if _text(item)]
  return [_text(value)] if _text(value) else []
