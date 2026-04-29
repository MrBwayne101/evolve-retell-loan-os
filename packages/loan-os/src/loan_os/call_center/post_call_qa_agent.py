from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from loan_os.call_center.continuation import _confidence_hint, _explicit_outcome, extract_shadow_call_features
from loan_os.call_center.ledger import payload_excerpt, stable_id
from loan_os.call_center.post_call import build_assessment
from loan_os.call_center.three_agent_harness import utc_now_iso


@dataclass(frozen=True)
class PostCallQaRecord:
  qa_id: str
  call_id: str
  contact_id: str
  owner: str
  outcome: str
  route: str
  next_action: str
  approval_state: str
  confidence_label: str
  confidence_score: float
  transfer_status: str
  appointment_result: str
  prospect_words: int
  questions_answered: int
  objection_type: str
  user_sentiment: str
  transcript_excerpt: str
  call_summary_excerpt: str
  recording_available: bool
  transcript_available: bool
  evidence_refs: list[str]
  ghl_note_draft: str
  external_write_allowed: bool
  human_review_required: bool
  generated_at: str

  def to_dict(self) -> dict[str, Any]:
    return asdict(self)


def _load_json(path: Path) -> Mapping[str, Any]:
  payload = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
  return payload if isinstance(payload, Mapping) else {}


def _owner_for(features: Mapping[str, Any], fallback_owner: str = "Unassigned LO Review") -> str:
  owner = str(features.get("owner_hint") or "").strip()
  return owner or fallback_owner


def _assessment_input(features: Mapping[str, Any]) -> dict[str, Any]:
  return {
    "contact_id": str(features.get("contact_id") or ""),
    "call_id": str(features.get("call_id") or ""),
    "connected_seconds": int(features.get("duration_seconds") or 0),
    "prospect_words_estimate": int(features.get("prospect_words") or 0),
    "recording_url": "present" if features.get("recording_available") else "",
    "appointment_booked": str(features.get("appointment_result") == "booked").lower(),
    "transfer_requested": str(features.get("transfer_attempted")).lower(),
    "outcome": _explicit_outcome(features),
    "confidence": _confidence_hint(features),
    "writeback_status": "shadow_only_no_external_writes",
    "estimated_largest_amount": int(features.get("estimated_amount") or 0),
  }


def build_ghl_note_draft(features: Mapping[str, Any], assessment: Mapping[str, Any]) -> str:
  contact_id = str(features.get("contact_id") or "unknown")
  call_id = str(features.get("call_id") or "unknown")
  route = str(assessment.get("route") or "")
  next_action = str(assessment.get("next_action") or "")
  summary = str(features.get("call_summary_excerpt") or "").strip() or "No Retell summary available."
  transcript = str(features.get("transcript_excerpt") or "").strip() or "No transcript excerpt available."
  return "\n".join(
    [
      "[AI CALL QA DRAFT - REVIEW BEFORE POSTING]",
      f"Contact: {contact_id}",
      f"Call: {call_id}",
      f"Outcome: {assessment.get('outcome')}",
      f"Route: {route}",
      f"Next action: {next_action}",
      f"Transfer: {features.get('transfer_result')}",
      f"Appointment: {features.get('appointment_result')}",
      f"Prospect words: {features.get('prospect_words')}",
      f"Questions answered: {features.get('questions_answered')}",
      f"Sentiment: {features.get('user_sentiment') or 'unknown'}",
      f"Summary: {payload_excerpt(summary, limit=360)}",
      f"Evidence: {payload_excerpt(transcript, limit=360)}",
      "External write status: blocked; internal review required.",
    ]
  )


def build_qa_record_from_call_payload(
  payload: Mapping[str, Any],
  *,
  source_name: str = "",
  generated_at: str | None = None,
) -> PostCallQaRecord | None:
  features = extract_shadow_call_features(payload, source_name=source_name)
  if not features.get("call_id"):
    return None
  owner = _owner_for(features)
  assessment = asdict(build_assessment(_assessment_input(features), owner))
  evidence_refs = [
    f"retell_call:{source_name or features.get('call_id')}",
    f"recording:{'present' if features.get('recording_available') else 'missing'}",
    f"transcript:{'present' if features.get('transcript_available') else 'missing'}",
  ]
  note = build_ghl_note_draft(features, assessment)
  return PostCallQaRecord(
    qa_id=stable_id("post_call_qa", features.get("call_id"), generated_at or ""),
    call_id=str(features.get("call_id") or ""),
    contact_id=str(features.get("contact_id") or ""),
    owner=owner,
    outcome=str(assessment.get("outcome") or ""),
    route=str(assessment.get("route") or ""),
    next_action=str(assessment.get("next_action") or ""),
    approval_state="review_required",
    confidence_label=str(assessment.get("confidence_label") or ""),
    confidence_score=float(assessment.get("confidence_score") or 0),
    transfer_status=str(features.get("transfer_result") or ""),
    appointment_result=str(features.get("appointment_result") or ""),
    prospect_words=int(features.get("prospect_words") or 0),
    questions_answered=int(features.get("questions_answered") or 0),
    objection_type=str(features.get("objection_type") or ""),
    user_sentiment=str(features.get("user_sentiment") or ""),
    transcript_excerpt=str(features.get("transcript_excerpt") or ""),
    call_summary_excerpt=str(features.get("call_summary_excerpt") or ""),
    recording_available=bool(features.get("recording_available")),
    transcript_available=bool(features.get("transcript_available")),
    evidence_refs=evidence_refs,
    ghl_note_draft=note,
    external_write_allowed=False,
    human_review_required=True,
    generated_at=generated_at or utc_now_iso(),
  )


def build_qa_records_from_paths(paths: Iterable[Path], *, generated_at: str | None = None) -> list[PostCallQaRecord]:
  records: list[PostCallQaRecord] = []
  for path in sorted(paths):
    record = build_qa_record_from_call_payload(_load_json(path), source_name=path.name, generated_at=generated_at)
    if record is not None:
      records.append(record)
  records.sort(key=lambda row: (row.outcome in {"hot_callback", "booked"}, row.prospect_words), reverse=True)
  return records


def render_post_call_qa_markdown(records: Iterable[PostCallQaRecord]) -> str:
  rows = list(records)
  lines = [
    "# Post-Call QA Agent Run",
    "",
    "Executable shadow-mode QA over Retell call records. No GHL notes are posted automatically.",
    "",
    f"- Records: {len(rows)}",
    f"- Human review required: {sum(1 for row in rows if row.human_review_required)}",
    f"- External writes allowed: {sum(1 for row in rows if row.external_write_allowed)}",
    "",
    "## Top Records",
    "",
  ]
  for row in rows[:30]:
    lines.extend(
      [
        f"### {row.call_id}",
        "",
        f"- Contact: {row.contact_id or 'missing'}",
        f"- Owner: {row.owner}",
        f"- Outcome: {row.outcome}",
        f"- Route: {row.route}",
        f"- Transfer: {row.transfer_status}",
        f"- Appointment: {row.appointment_result}",
        f"- Confidence: {row.confidence_label} ({row.confidence_score:.2f})",
        f"- Next action: {row.next_action}",
        f"- Evidence: {row.transcript_excerpt or row.call_summary_excerpt or 'missing'}",
        "",
      ]
    )
  return "\n".join(lines).rstrip() + "\n"


def write_post_call_qa_artifacts(records: list[PostCallQaRecord], *, out_dir: Path, date_label: str) -> dict[str, str]:
  out_dir.mkdir(parents=True, exist_ok=True)
  payload = [record.to_dict() for record in records]
  json_path = out_dir / f"post-call-qa-agent-run-{date_label}.json"
  jsonl_path = out_dir / f"post-call-qa-agent-run-{date_label}.jsonl"
  md_path = out_dir / f"POST_CALL_QA_AGENT_RUN_{date_label}.md"
  notes_path = out_dir / f"ghl-note-drafts-post-call-qa-{date_label}.jsonl"
  json_path.write_text(json.dumps({"count": len(payload), "records": payload}, indent=2, ensure_ascii=False), encoding="utf-8")
  jsonl_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in payload) + "\n", encoding="utf-8")
  notes_path.write_text(
    "\n".join(
      json.dumps(
        {
          "qa_id": row["qa_id"],
          "contact_id": row["contact_id"],
          "call_id": row["call_id"],
          "approval_state": row["approval_state"],
          "external_write_allowed": row["external_write_allowed"],
          "note_body": row["ghl_note_draft"],
        },
        ensure_ascii=False,
      )
      for row in payload
    )
    + "\n",
    encoding="utf-8",
  )
  md_path.write_text(render_post_call_qa_markdown(records), encoding="utf-8")
  return {"json": str(json_path), "jsonl": str(jsonl_path), "markdown": str(md_path), "ghl_note_drafts": str(notes_path)}
