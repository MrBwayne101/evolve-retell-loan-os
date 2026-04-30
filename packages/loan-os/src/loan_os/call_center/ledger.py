from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


PHONE_RE = re.compile(r"(?:\+?1[\s.-]*)?(?:\(?\d{3}\)?[\s.-]*)\d{3}[\s.-]*\d{4}")
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
SPOKEN_PHONE_RE = re.compile(
  r"\b(?:(?:zero|oh|o|one|two|three|four|five|six|seven|eight|nine|"
  r"0|1|2|3|4|5|6|7|8|9)[\s,.;:-]+){6,}"
  r"(?:zero|oh|o|one|two|three|four|five|six|seven|eight|nine|0|1|2|3|4|5|6|7|8|9)\b",
  re.IGNORECASE,
)


NORMALIZATION_RULES: list[dict[str, str]] = [
  {
    "source": "retell.call_started",
    "event_type": "call_started",
    "identity": "contact_id or call metadata -> contact_id",
    "notes": "Append the call shell immediately; no borrower transcript content required.",
  },
  {
    "source": "retell.call_ended",
    "event_type": "call_ended",
    "identity": "call_id + contact_id",
    "notes": "Capture connected seconds, disposition, and redacted transcript excerpts only.",
  },
  {
    "source": "retell.call_analyzed",
    "event_type": "call_analyzed",
    "identity": "call_id + contact_id",
    "notes": "Emit transcript, recording, transfer, and callback-derived child events when present.",
  },
  {
    "source": "ghl.note",
    "event_type": "call_analyzed",
    "identity": "contact_id + note_id",
    "notes": "Treat notes as derived summaries; redact phone/email before storage.",
  },
  {
    "source": "ghl.appointment",
    "event_type": "appointment_booked",
    "identity": "contact_id + appointment_id",
    "notes": "Store slot timestamps and LO assignment; do not mutate calendars from this path.",
  },
  {
    "source": "ghl.transfer",
    "event_type": "transfer_started/transfer_bridged/transfer_failed",
    "identity": "call_id + transfer_attempt_id",
    "notes": "Model transfer attempts as append-only lifecycle events.",
  },
  {
    "source": "loan_os.scenario_enrichment",
    "event_type": "lead_enriched",
    "identity": "contact_id + scenario_id",
    "notes": "Persist only structured scenario facts and amount buckets in shadow mode.",
  },
  {
    "source": "email.submission_status",
    "event_type": "submission_sent/submission_received",
    "identity": "loan_id + message_id",
    "notes": "Preserve lender/process state changes without borrower-facing writes.",
  },
]


def utc_now_iso() -> str:
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_timestamp(value: Any) -> str:
  raw = str(value or "").strip()
  if not raw:
    return utc_now_iso()
  if raw.endswith("Z") or "T" in raw:
    return raw
  if raw.isdigit():
    numeric = int(raw)
    if numeric > 10_000_000_000:
      numeric = numeric / 1000
    return datetime.fromtimestamp(numeric, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
  return raw


def stable_id(*parts: str) -> str:
  digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()
  return digest[:16]


def normalize_digits(value: str | None) -> str:
  return re.sub(r"\D+", "", value or "")


def redact_phone(value: str | None) -> str:
  digits = normalize_digits(value)
  if len(digits) >= 4:
    return f"***-***-{digits[-4:]}"
  if digits:
    return "***-***-****"
  return ""


def redact_email(value: str | None) -> str:
  if not value:
    return ""
  local, _, domain = value.partition("@")
  if not domain:
    return "[redacted-email]"
  prefix = local[:1] if local else ""
  return f"{prefix}***@{domain}"


def redact_text(value: str | None) -> str:
  text = value or ""
  text = PHONE_RE.sub(lambda match: redact_phone(match.group(0)), text)
  text = SPOKEN_PHONE_RE.sub("[redacted-spoken-phone]", text)
  text = EMAIL_RE.sub(lambda match: redact_email(match.group(0)), text)
  return text


def amount_bucket(value: Any) -> str:
  try:
    amount = float(str(value or "0").replace("$", "").replace(",", ""))
  except ValueError:
    amount = 0
  if amount <= 0:
    return "unknown"
  if amount >= 2_000_000:
    return "2m_plus"
  if amount >= 1_000_000:
    return "1m_to_2m"
  if amount >= 500_000:
    return "500k_to_1m"
  return "sub_500k"


def confidence_to_float(value: str | None) -> float:
  return {
    "high": 0.9,
    "medium": 0.6,
    "low": 0.35,
  }.get(str(value or "").strip().lower(), 0.5)


def payload_excerpt(text: str | None, limit: int = 180) -> str:
  safe = redact_text(text).strip()
  if len(safe) <= limit:
    return safe
  return safe[: limit - 3].rstrip() + "..."


def _transfer_status_rank(value: str | None) -> int:
  status = str(value or "").strip().lower()
  if not status or status == "not_attempted":
    return 0
  if status.startswith("bridged"):
    return 3
  if status.startswith("failed"):
    return 2
  return 1


def merge_transfer_status(current: str | None, candidate: str | None) -> str:
  current_text = str(current or "").strip()
  candidate_text = str(candidate or "").strip()
  if _transfer_status_rank(candidate_text) > _transfer_status_rank(current_text):
    return candidate_text
  return current_text


def _appointment_status_rank(value: str | None) -> int:
  status = str(value or "").strip().lower()
  if not status or status in {"not_booked", "not_attempted"}:
    return 0
  if status == "booked":
    return 3
  if status in {"booking_error", "slots_offered", "fallback_discussed"}:
    return 2
  return 1


def merge_appointment_status(current: str | None, candidate: str | None) -> str:
  current_text = str(current or "").strip()
  candidate_text = str(candidate or "").strip()
  if _appointment_status_rank(candidate_text) > _appointment_status_rank(current_text):
    return candidate_text
  return current_text


def parse_embedded_json(value: Any) -> dict[str, Any]:
  text = str(value or "").strip()
  if not text:
    return {}
  try:
    data = json.loads(text)
  except json.JSONDecodeError:
    return {}
  return data if isinstance(data, dict) else {}


def infer_appointment_status(result: Mapping[str, Any], transcript_hint: str | None = None) -> str:
  if result.get("booked") is True:
    return "booked"
  if result.get("needs_slot_selection") is True or result.get("available_slots"):
    return "slots_offered"
  if result.get("ok") is False or result.get("error"):
    return "booking_error"
  text = json.dumps(result).lower()
  if "appointment" in text or "slot" in text:
    return "fallback_discussed"
  if transcript_hint and "appointment" in transcript_hint.lower():
    return "fallback_discussed"
  if result:
    return "attempted"
  return "not_attempted"


def _call_reconstruction_gap_reasons(state: Mapping[str, Any]) -> list[str]:
  reasons: list[str] = []
  if not str(state.get("contact_id") or "").strip():
    reasons.append("missing_contact_id")
  if not bool(state.get("transcript_available")):
    reasons.append("missing_transcript")
  if not bool(state.get("recording_available")):
    reasons.append("missing_recording")
  if not str(state.get("owner_hint") or "").strip():
    reasons.append("missing_owner_hint")
  if int(state.get("transfer_event_count") or 0) > 0 and str(state.get("transfer_status") or "") in {"", "started"}:
    reasons.append("transfer_outcome_incomplete")
  if int(state.get("appointment_event_count") or 0) > 0 and str(state.get("appointment_status") or "") in {"not_booked", "attempted"}:
    reasons.append("appointment_outcome_incomplete")
  return reasons


def _contact_reconstruction_gap_reasons(state: Mapping[str, Any]) -> list[str]:
  reasons: list[str] = []
  if int(state.get("call_count") or 0) > 0 and not bool(state.get("transcript_covered")):
    reasons.append("missing_transcript")
  if int(state.get("call_count") or 0) > 0 and not bool(state.get("recording_covered")):
    reasons.append("missing_recording")
  if not str(state.get("owner_hint") or "").strip():
    reasons.append("missing_owner_hint")
  if int(state.get("call_count") or 0) <= 0:
    reasons.append("no_shadow_call_link")
  return reasons


def _reconstruction_confidence_label(reasons: list[str], transcript_present: bool, recording_present: bool, owner_present: bool) -> str:
  if not reasons and transcript_present and recording_present and owner_present:
    return "high"
  if transcript_present and (recording_present or owner_present):
    return "medium"
  return "low"


@dataclass(frozen=True)
class EvidenceRef:
  source_type: str
  source_id: str
  note: str = ""
  excerpt: str = ""


@dataclass(frozen=True)
class EventEnvelope:
  event_id: str
  event_type: str
  occurred_at: str
  ingested_at: str
  source_system: str
  source_id: str
  contact_id: str = ""
  loan_id: str = ""
  campaign_id: str = ""
  agent_id: str = ""
  agent_version: str = ""
  batch_id: str = ""
  experiment_id: str = ""
  actor_type: str = "system"
  actor_id: str = ""
  payload: dict[str, Any] = field(default_factory=dict)
  pii_classification: str = "restricted"
  confidence: float = 0.5
  evidence_refs: list[dict[str, str]] = field(default_factory=list)

  def to_record(self) -> dict[str, Any]:
    return asdict(self)


class EventLedger:
  def __init__(self, output_path: Path) -> None:
    self.output_path = output_path
    self._events: list[EventEnvelope] = []

  @property
  def events(self) -> list[EventEnvelope]:
    return list(self._events)

  def append(self, event: EventEnvelope) -> EventEnvelope:
    self._events.append(event)
    return event

  def extend(self, events: Iterable[EventEnvelope]) -> None:
    for event in events:
      self.append(event)

  def write(self) -> Path:
    self.output_path.parent.mkdir(parents=True, exist_ok=True)
    with self.output_path.open("w", encoding="utf-8") as handle:
      for event in self._events:
        handle.write(json.dumps(event.to_record(), ensure_ascii=True) + "\n")
    return self.output_path


def _evidence_dicts(*refs: EvidenceRef) -> list[dict[str, str]]:
  return [asdict(ref) for ref in refs]


def normalize_retell_payload(payload: Mapping[str, Any]) -> list[EventEnvelope]:
  event_name = str(payload.get("event") or payload.get("event_type") or "")
  call = payload.get("call") if isinstance(payload.get("call"), Mapping) else payload
  metadata = call.get("metadata") if isinstance(call.get("metadata"), Mapping) else {}
  call_id = str(call.get("call_id") or call.get("callId") or payload.get("call_id") or "")
  contact_id = str(
    call.get("contact_id")
    or call.get("contactId")
    or call.get("ghl_contact_id")
    or metadata.get("contact_id")
    or metadata.get("ghl_contact_id")
    or metadata.get("ghlContactId")
    or payload.get("contact_id")
    or ""
  )
  occurred_at = normalize_timestamp(
    call.get("timestamp")
    or call.get("ended_at")
    or call.get("start_timestamp")
    or call.get("end_timestamp")
    or payload.get("timestamp")
    or utc_now_iso()
  )
  ingested_at = utc_now_iso()
  transcript = str(call.get("transcript") or payload.get("transcript") or "")
  recording_url = str(call.get("recording_url") or payload.get("recording_url") or "")
  disposition = str(call.get("disposition") or payload.get("call_status") or "")
  duration_ms = int(float(str(call.get("duration_ms") or payload.get("duration_ms") or "0")))
  connected_seconds = int(
    float(
      str(
        call.get("connected_seconds")
        or payload.get("connected_seconds")
        or (duration_ms / 1000 if duration_ms else "0")
      )
    )
  )
  campaign_id = str(
    call.get("campaign_id")
    or payload.get("campaign_id")
    or metadata.get("safe_batch_tag")
    or metadata.get("campaign_id")
    or metadata.get("project")
    or ""
  )
  purpose = str(metadata.get("purpose") or "")
  call_status = str(call.get("call_status") or payload.get("call_status") or "")
  call_summary = ""
  if isinstance(call.get("call_analysis"), Mapping):
    call_summary = str(call["call_analysis"].get("call_summary") or "")

  events: list[EventEnvelope] = []

  type_map = {
    "call_started": "call_started",
    "call_ended": "call_ended",
    "call_analyzed": "call_analyzed",
    "call_transcript_ready": "call_transcript_updated",
  }
  primary_type = type_map.get(event_name, "call_analyzed")
  primary_payload = {
    "call_id": call_id,
    "disposition": disposition,
    "connected_seconds": connected_seconds,
    "duration_ms": duration_ms,
    "call_status": call_status,
    "purpose": purpose,
    "owner_hint": str(metadata.get("suggested_owner") or ""),
    "recording_available": bool(recording_url),
    "transcript_excerpt": payload_excerpt(transcript),
    "call_summary_excerpt": payload_excerpt(call_summary),
    "transfer_status": str(call.get("transfer_status") or payload.get("transfer_status") or ""),
  }
  events.append(
    EventEnvelope(
      event_id=stable_id("retell", primary_type, call_id or event_name, occurred_at),
      event_type=primary_type,
      occurred_at=occurred_at,
      ingested_at=ingested_at,
      source_system="retell",
      source_id=call_id or event_name,
      contact_id=contact_id,
      campaign_id=campaign_id,
      agent_id=str(call.get("agent_id") or payload.get("agent_id") or ""),
      agent_version=str(call.get("agent_version") or payload.get("agent_version") or ""),
      batch_id=str(metadata.get("safe_batch_tag") or payload.get("batch_id") or ""),
      actor_type="system",
      payload=primary_payload,
      confidence=0.92,
      evidence_refs=_evidence_dicts(
        EvidenceRef("retell_event", call_id or event_name, note=event_name),
        EvidenceRef("retell_metadata", call_id or event_name, note=payload_excerpt(purpose or call_status)),
      ),
    )
  )

  if transcript:
    events.append(
      EventEnvelope(
        event_id=stable_id("retell", "call_transcript_updated", call_id, transcript[:48]),
        event_type="call_transcript_updated",
        occurred_at=occurred_at,
        ingested_at=ingested_at,
        source_system="retell",
        source_id=call_id or "transcript",
        contact_id=contact_id,
        campaign_id=campaign_id,
        agent_id=str(call.get("agent_id") or payload.get("agent_id") or ""),
        agent_version=str(call.get("agent_version") or payload.get("agent_version") or ""),
        actor_type="system",
        payload={"call_id": call_id, "transcript_excerpt": payload_excerpt(transcript)},
        confidence=0.9,
        evidence_refs=_evidence_dicts(
          EvidenceRef("retell_transcript", call_id or "transcript", excerpt=payload_excerpt(transcript))
        ),
      )
    )

  if recording_url:
    events.append(
      EventEnvelope(
        event_id=stable_id("retell", "recording_available", call_id, recording_url),
        event_type="recording_available",
        occurred_at=occurred_at,
        ingested_at=ingested_at,
        source_system="retell",
        source_id=call_id or "recording",
        contact_id=contact_id,
        campaign_id=campaign_id,
        agent_id=str(call.get("agent_id") or payload.get("agent_id") or ""),
        agent_version=str(call.get("agent_version") or payload.get("agent_version") or ""),
        actor_type="system",
        payload={"call_id": call_id, "recording_url_state": "present"},
        confidence=0.95,
        evidence_refs=_evidence_dicts(EvidenceRef("retell_recording", call_id or "recording", note="recording_url_present")),
      )
    )

  transfer_status = str(call.get("transfer_status") or payload.get("transfer_status") or "").lower()
  if transfer_status in {"started", "bridged", "failed"}:
    transfer_type = {
      "started": "transfer_started",
      "bridged": "transfer_bridged",
      "failed": "transfer_failed",
    }[transfer_status]
    events.append(
      EventEnvelope(
        event_id=stable_id("retell", transfer_type, call_id, transfer_status),
        event_type=transfer_type,
        occurred_at=occurred_at,
        ingested_at=ingested_at,
        source_system="retell",
        source_id=call_id or "transfer",
        contact_id=contact_id,
        campaign_id=campaign_id,
        agent_id=str(call.get("agent_id") or payload.get("agent_id") or ""),
        agent_version=str(call.get("agent_version") or payload.get("agent_version") or ""),
        actor_type="system",
        payload={"call_id": call_id, "transfer_status": transfer_status},
        confidence=0.85,
        evidence_refs=_evidence_dicts(EvidenceRef("retell_transfer", call_id or "transfer", note=transfer_status)),
        )
      )

  transcript_nodes = call.get("transcript_with_tool_calls") if isinstance(call.get("transcript_with_tool_calls"), list) else []
  tool_invocations = [
    node
    for node in transcript_nodes
    if isinstance(node, dict) and str(node.get("role") or "") == "tool_call_invocation"
  ]
  tool_results = {
    str(node.get("tool_call_id") or ""): parse_embedded_json(node.get("content"))
    for node in transcript_nodes
    if isinstance(node, dict)
    and str(node.get("role") or "") == "tool_call_result"
    and str(node.get("tool_call_id") or "")
  }
  for invocation in tool_invocations:
    tool_call_id = str(invocation.get("tool_call_id") or "")
    tool_name = str(invocation.get("name") or "")
    tool_type = str(invocation.get("type") or "")
    result = tool_results.get(tool_call_id, {})
    if tool_type == "transfer_call" or tool_name.startswith("transfer_"):
      events.append(
        EventEnvelope(
          event_id=stable_id("retell", "transfer_started", call_id or tool_call_id, tool_name),
          event_type="transfer_started",
          occurred_at=occurred_at,
          ingested_at=ingested_at,
          source_system="retell",
          source_id=call_id or tool_call_id or "transfer",
          contact_id=contact_id,
          campaign_id=campaign_id,
          agent_id=str(call.get("agent_id") or payload.get("agent_id") or ""),
          agent_version=str(call.get("agent_version") or payload.get("agent_version") or ""),
          actor_type="system",
          payload={"call_id": call_id, "tool_name": tool_name},
          confidence=0.78,
          evidence_refs=_evidence_dicts(EvidenceRef("retell_tool_call", tool_call_id or tool_name, note=tool_name)),
        )
      )
      result_text = json.dumps(result)
      if "did not pick up" in result_text or "did not go through" in result_text:
        events.append(
          EventEnvelope(
            event_id=stable_id("retell", "transfer_failed", call_id or tool_call_id, tool_name),
            event_type="transfer_failed",
            occurred_at=occurred_at,
            ingested_at=ingested_at,
            source_system="retell",
            source_id=call_id or tool_call_id or "transfer",
            contact_id=contact_id,
            campaign_id=campaign_id,
            agent_id=str(call.get("agent_id") or payload.get("agent_id") or ""),
            agent_version=str(call.get("agent_version") or payload.get("agent_version") or ""),
            actor_type="system",
            payload={"call_id": call_id, "tool_name": tool_name, "result_excerpt": payload_excerpt(result_text)},
            confidence=0.84,
            evidence_refs=_evidence_dicts(EvidenceRef("retell_tool_result", tool_call_id or tool_name, note="transfer_failed")),
          )
        )
      elif result.get("live_transfer_available") is True:
        events.append(
          EventEnvelope(
            event_id=stable_id("retell", "transfer_bridged", call_id or tool_call_id, tool_name),
            event_type="transfer_bridged",
            occurred_at=occurred_at,
            ingested_at=ingested_at,
            source_system="retell",
            source_id=call_id or tool_call_id or "transfer",
            contact_id=contact_id,
            campaign_id=campaign_id,
            agent_id=str(call.get("agent_id") or payload.get("agent_id") or ""),
            agent_version=str(call.get("agent_version") or payload.get("agent_version") or ""),
            actor_type="system",
            payload={"call_id": call_id, "tool_name": tool_name},
            confidence=0.81,
            evidence_refs=_evidence_dicts(EvidenceRef("retell_tool_result", tool_call_id or tool_name, note="transfer_bridged")),
          )
        )
    if tool_name.startswith("book_") or "appointment" in tool_name or tool_name == "book_or_transfer":
      appointment_status = infer_appointment_status(result, transcript_hint=transcript)
      if appointment_status != "not_attempted":
        appointment_type = f"appointment_{appointment_status}"
        events.append(
          EventEnvelope(
            event_id=stable_id("retell", appointment_type, call_id or tool_call_id, tool_name),
            event_type=appointment_type,
            occurred_at=occurred_at,
            ingested_at=ingested_at,
            source_system="retell",
            source_id=call_id or tool_call_id or "appointment",
            contact_id=contact_id,
            campaign_id=campaign_id,
            agent_id=str(call.get("agent_id") or payload.get("agent_id") or ""),
            agent_version=str(call.get("agent_version") or payload.get("agent_version") or ""),
            actor_type="system",
            payload={
              "call_id": call_id,
              "tool_name": tool_name,
              "appointment_status": appointment_status,
              "slot_state": "booked" if appointment_status == "booked" else "",
              "result_excerpt": payload_excerpt(json.dumps(result)),
            },
            confidence=0.82 if appointment_status == "booked" else 0.76,
            evidence_refs=_evidence_dicts(
              EvidenceRef("retell_tool_result", tool_call_id or tool_name, note=appointment_type)
            ),
          )
        )

  if "appointment" in transcript.lower():
    has_appointment_event = any(event.event_type.startswith("appointment_") for event in events)
    if not has_appointment_event:
      events.append(
        EventEnvelope(
          event_id=stable_id("retell", "appointment_fallback_discussed", call_id or event_name, transcript[:64]),
          event_type="appointment_fallback_discussed",
          occurred_at=occurred_at,
          ingested_at=ingested_at,
          source_system="retell",
          source_id=call_id or "appointment",
          contact_id=contact_id,
          campaign_id=campaign_id,
          agent_id=str(call.get("agent_id") or payload.get("agent_id") or ""),
          agent_version=str(call.get("agent_version") or payload.get("agent_version") or ""),
          actor_type="system",
          payload={"call_id": call_id, "appointment_status": "fallback_discussed"},
          confidence=0.68,
          evidence_refs=_evidence_dicts(EvidenceRef("retell_transcript", call_id or "appointment", note="appointment_discussed")),
        )
      )

  return events


def normalize_ghl_note(note: Mapping[str, Any]) -> EventEnvelope:
  note_id = str(note.get("id") or note.get("note_id") or "")
  contact_id = str(note.get("contact_id") or note.get("contactId") or "")
  occurred_at = str(note.get("created_at") or note.get("createdAt") or utc_now_iso())
  body = str(note.get("body") or note.get("note") or "")
  return EventEnvelope(
    event_id=stable_id("ghl", "call_analyzed", note_id or contact_id, occurred_at),
    event_type="call_analyzed",
    occurred_at=occurred_at,
    ingested_at=utc_now_iso(),
    source_system="ghl",
    source_id=note_id or contact_id,
    contact_id=contact_id,
    actor_type="loan_officer",
    actor_id=str(note.get("user_id") or note.get("userId") or ""),
    payload={"note_excerpt": payload_excerpt(body), "note_type": str(note.get("type") or "general")},
    confidence=0.78,
    evidence_refs=_evidence_dicts(EvidenceRef("ghl_note", note_id or contact_id, excerpt=payload_excerpt(body))),
  )


def normalize_ghl_appointment(appointment: Mapping[str, Any]) -> EventEnvelope:
  appointment_id = str(appointment.get("id") or appointment.get("appointment_id") or "")
  contact_id = str(appointment.get("contact_id") or appointment.get("contactId") or "")
  occurred_at = str(appointment.get("start_time") or appointment.get("startTime") or utc_now_iso())
  return EventEnvelope(
    event_id=stable_id("ghl", "appointment_booked", appointment_id or contact_id, occurred_at),
    event_type="appointment_booked",
    occurred_at=occurred_at,
    ingested_at=utc_now_iso(),
    source_system="ghl",
    source_id=appointment_id or contact_id,
    contact_id=contact_id,
    actor_type="system",
    actor_id=str(appointment.get("assigned_user_id") or appointment.get("assignedUserId") or ""),
    payload={
      "calendar_id": str(appointment.get("calendar_id") or appointment.get("calendarId") or ""),
      "slot_state": "booked",
    },
    confidence=0.88,
    evidence_refs=_evidence_dicts(EvidenceRef("ghl_appointment", appointment_id or contact_id, note="calendar_booking")),
  )


def normalize_lead_enrichment(row: Mapping[str, Any]) -> EventEnvelope:
  scenario_id = str(row.get("scenario_id") or "")
  contact_id = str(row.get("contact_id") or "")
  occurred_at = utc_now_iso()
  return EventEnvelope(
    event_id=stable_id("loan_os", "lead_enriched", scenario_id or contact_id, str(row.get("goal") or "")),
    event_type="lead_enriched",
    occurred_at=occurred_at,
    ingested_at=utc_now_iso(),
    source_system="loan_os",
    source_id=scenario_id or contact_id,
    contact_id=contact_id,
    campaign_id=str(row.get("campaign_id") or row.get("campaign") or row.get("safe_batch_tag") or ""),
    actor_type="system",
    payload={
      "goal": str(row.get("goal") or ""),
      "state": str(row.get("state") or ""),
      "property_type": str(row.get("property_type") or ""),
      "automation_stage": str(row.get("automation_stage") or ""),
      "suggested_owner": str(row.get("suggested_owner") or row.get("owner") or ""),
      "amount_bucket": amount_bucket(row.get("largest_amount")),
      "recommended_tool": str(row.get("recommended_tool") or ""),
      "source_attribution": str(row.get("enrichment_source") or row.get("source_artifact") or "scenario_ledger"),
    },
    confidence=confidence_to_float(str(row.get("confidence") or "")),
    evidence_refs=_evidence_dicts(EvidenceRef("scenario_row", scenario_id or contact_id, note="scenario_enrichment")),
  )


def normalize_email_submission(row: Mapping[str, Any]) -> EventEnvelope:
  status = str(row.get("status") or row.get("submission_status") or "sent").lower()
  event_type = "submission_received" if status in {"received", "accepted"} else "submission_sent"
  loan_id = str(row.get("loan_id") or row.get("loanId") or "")
  source_id = str(row.get("message_id") or row.get("messageId") or loan_id)
  occurred_at = str(row.get("occurred_at") or row.get("timestamp") or utc_now_iso())
  return EventEnvelope(
    event_id=stable_id("email", event_type, source_id, status),
    event_type=event_type,
    occurred_at=occurred_at,
    ingested_at=utc_now_iso(),
    source_system="email",
    source_id=source_id,
    loan_id=loan_id,
    actor_type="system",
    payload={
      "lender": str(row.get("lender") or ""),
      "status": status,
      "subject_excerpt": payload_excerpt(str(row.get("subject") or "")),
    },
    pii_classification="internal",
    confidence=0.82,
    evidence_refs=_evidence_dicts(EvidenceRef("email_submission", source_id, note=status)),
  )


def render_normalization_markdown() -> str:
  lines = [
    "# Event Normalization Rules - 2026-04-28",
    "",
    "These rules define the append-only observer contract for the Call Center OS shadow ledger.",
    "",
    "## Principles",
    "",
    "- Append-only: never mutate prior events; derived state is rebuilt from replay.",
    "- PII-safe: redact phones, emails, and transcript free text in generated artifacts.",
    "- Evidence-first: every derived decision cites a source artifact or excerpt.",
    "- Shadow-only: no external mutation is authorized from these normalized outputs.",
    "",
    "## Rules",
    "",
  ]
  for rule in NORMALIZATION_RULES:
    lines.extend(
      [
        f"### {rule['source']}",
        "",
        f"- Event type: `{rule['event_type']}`",
        f"- Identity: {rule['identity']}",
        f"- Notes: {rule['notes']}",
        "",
      ]
    )
  return "\n".join(lines).rstrip() + "\n"


def write_json(path: Path, payload: dict[str, Any]) -> Path:
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
  return path


def derive_call_states(events: Iterable[EventEnvelope]) -> dict[str, dict[str, Any]]:
  states: dict[str, dict[str, Any]] = {}
  ordered = sorted(
    events,
    key=lambda event: (
      event.occurred_at,
      event.ingested_at,
      event.event_id,
    ),
  )
  for event in ordered:
    call_id = str(event.payload.get("call_id") or (event.source_id if event.source_system == "retell" else ""))
    if not call_id:
      continue
    state = states.setdefault(
      call_id,
      {
        "call_id": call_id,
        "contact_id": "",
        "resolved_contact_id": "",
        "latest_event_at": "",
        "latest_event_type": "",
        "last_event_type": "",
        "campaign_id": "",
        "source_systems": [],
        "event_count": 0,
        "transcript_available": False,
        "transcript_excerpt": "",
        "recording_available": False,
        "recording_url_state": "missing",
        "transfer_status": "",
        "transfer_event_count": 0,
        "appointment_booked": False,
        "appointment_status": "not_booked",
        "appointment_event_count": 0,
        "submission_status": "",
        "connected_seconds": 0,
        "call_status": "",
        "disposition": "",
        "purpose": "",
        "owner_hint": "",
        "call_summary_excerpt": "",
        "event_types": [],
        "reconstruction_gap_reasons": [],
        "reconstruction_confidence_label": "low",
        "evidence_refs": [],
        "confidence": 0.0,
      },
    )
    state["contact_id"] = str(event.contact_id or state["contact_id"])
    state["resolved_contact_id"] = str(event.contact_id or state["resolved_contact_id"])
    state["latest_event_at"] = event.occurred_at
    state["latest_event_type"] = event.event_type
    state["last_event_type"] = event.event_type
    state["campaign_id"] = str(event.campaign_id or state["campaign_id"])
    state["event_count"] = int(state["event_count"]) + 1
    if event.event_type and event.event_type not in state["event_types"]:
      state["event_types"].append(event.event_type)
    if event.source_system and event.source_system not in state["source_systems"]:
      state["source_systems"].append(event.source_system)
    state["confidence"] = max(float(state["confidence"]), float(event.confidence))
    if event.evidence_refs:
      state["evidence_refs"] = (state["evidence_refs"] + event.evidence_refs)[-12:]

    if event.event_type == "call_transcript_updated":
      state["transcript_available"] = True
      state["transcript_excerpt"] = str(event.payload.get("transcript_excerpt") or state["transcript_excerpt"])
    elif event.event_type == "recording_available":
      state["recording_available"] = True
      state["recording_url_state"] = "present"
    elif event.event_type.startswith("transfer_"):
      state["transfer_status"] = merge_transfer_status(state["transfer_status"], event.event_type.removeprefix("transfer_"))
      state["transfer_event_count"] = int(state["transfer_event_count"]) + 1
    elif event.event_type.startswith("appointment_"):
      appointment_status = event.event_type.removeprefix("appointment_")
      state["appointment_event_count"] = int(state["appointment_event_count"]) + 1
      if appointment_status == "booked":
        state["appointment_booked"] = True
      state["appointment_status"] = merge_appointment_status(state["appointment_status"], appointment_status)
    elif event.event_type in {"submission_received", "submission_sent"}:
      state["submission_status"] = event.event_type

    payload = event.payload if isinstance(event.payload, dict) else {}
    state["connected_seconds"] = max(int(state["connected_seconds"]), int(payload.get("connected_seconds") or 0))
    state["call_status"] = str(payload.get("call_status") or state["call_status"])
    state["disposition"] = str(payload.get("disposition") or state["disposition"])
    state["purpose"] = str(payload.get("purpose") or state["purpose"])
    state["owner_hint"] = str(payload.get("owner_hint") or state["owner_hint"])
    state["transcript_excerpt"] = str(payload.get("transcript_excerpt") or state["transcript_excerpt"])
    state["call_summary_excerpt"] = str(payload.get("call_summary_excerpt") or state["call_summary_excerpt"])
    state["owner_hint"] = str(payload.get("owner_hint") or state["owner_hint"])
    if payload.get("recording_available"):
      state["recording_available"] = True
      state["recording_url_state"] = "present"
    if payload.get("transfer_status"):
      state["transfer_status"] = merge_transfer_status(state["transfer_status"], str(payload["transfer_status"]))
    if payload.get("appointment_status"):
      appointment_status = str(payload.get("appointment_status") or "")
      if appointment_status == "booked":
        state["appointment_booked"] = True
      state["appointment_status"] = merge_appointment_status(state["appointment_status"], appointment_status)
  for state in states.values():
    if not state["resolved_contact_id"]:
      state["resolved_contact_id"] = f"shadow_contact__{state['call_id']}"
    gap_reasons = _call_reconstruction_gap_reasons(state)
    state["reconstruction_gap_reasons"] = gap_reasons
    state["reconstruction_confidence_label"] = _reconstruction_confidence_label(
      gap_reasons,
      transcript_present=bool(state.get("transcript_available")),
      recording_present=bool(state.get("recording_available")),
      owner_present=bool(str(state.get("owner_hint") or "").strip()),
    )
  return states


def derive_contact_states(events: Iterable[EventEnvelope]) -> dict[str, dict[str, Any]]:
  call_states = derive_call_states(events)
  states: dict[str, dict[str, Any]] = {}
  ordered = sorted(
    events,
    key=lambda event: (
      event.occurred_at,
      event.ingested_at,
      event.event_id,
    ),
  )
  for event in ordered:
    contact_id = str(event.contact_id or "")
    if not contact_id:
      continue
    state = states.setdefault(
      contact_id,
      {
        "contact_id": contact_id,
        "resolved_contact_id": contact_id,
        "latest_event_at": "",
        "latest_event_type": "",
        "last_event_type": "",
        "campaign_ids": [],
        "source_systems": [],
        "event_count": 0,
        "call_count": 0,
        "call_ids": [],
        "automation_stage": "",
        "recommended_tool": "",
        "amount_bucket": "",
        "owner_hint": "",
        "transcript_covered": False,
        "transcript_excerpt": "",
        "recording_covered": False,
        "recording_url_state": "missing",
        "appointment_booked": False,
        "appointment_status": "not_booked",
        "transfer_status": "",
        "submission_status": "",
        "call_summary_excerpt": "",
        "event_types": [],
        "reconstruction_gap_reasons": [],
        "reconstruction_confidence_label": "low",
        "evidence_refs": [],
        "confidence": 0.0,
      },
    )
    state["latest_event_at"] = event.occurred_at
    state["latest_event_type"] = event.event_type
    state["last_event_type"] = event.event_type
    state["event_count"] = int(state["event_count"]) + 1
    if event.event_type and event.event_type not in state["event_types"]:
      state["event_types"].append(event.event_type)
    if event.campaign_id and event.campaign_id not in state["campaign_ids"]:
      state["campaign_ids"].append(event.campaign_id)
    if event.source_system and event.source_system not in state["source_systems"]:
      state["source_systems"].append(event.source_system)
    state["confidence"] = max(float(state["confidence"]), float(event.confidence))
    if event.evidence_refs:
      state["evidence_refs"] = (state["evidence_refs"] + event.evidence_refs)[-10:]

    payload = event.payload if isinstance(event.payload, dict) else {}
    if event.event_type == "lead_enriched":
      state["automation_stage"] = str(payload.get("automation_stage") or state["automation_stage"])
      state["recommended_tool"] = str(payload.get("recommended_tool") or state["recommended_tool"])
      state["amount_bucket"] = str(payload.get("amount_bucket") or state["amount_bucket"])
      state["owner_hint"] = str(payload.get("suggested_owner") or state["owner_hint"])
    elif event.event_type.startswith("appointment_"):
      appointment_status = event.event_type.removeprefix("appointment_")
      if appointment_status == "booked":
        state["appointment_booked"] = True
      state["appointment_status"] = merge_appointment_status(state["appointment_status"], appointment_status)
    elif event.event_type in {"submission_received", "submission_sent"}:
      state["submission_status"] = event.event_type
    elif event.event_type.startswith("transfer_"):
      state["transfer_status"] = merge_transfer_status(state["transfer_status"], event.event_type.removeprefix("transfer_"))
    elif event.event_type == "call_transcript_updated":
      state["transcript_covered"] = True
      state["transcript_excerpt"] = str(payload.get("transcript_excerpt") or state["transcript_excerpt"])
    elif event.event_type == "recording_available":
      state["recording_covered"] = True
      state["recording_url_state"] = "present"
    if payload.get("call_summary_excerpt"):
      state["call_summary_excerpt"] = str(payload["call_summary_excerpt"])

  for state in states.values():
    related_calls = [
      item
      for item in call_states.values()
      if str(item.get("contact_id") or "") == state["contact_id"]
    ]
    if related_calls:
      latest_call = sorted(
        related_calls,
        key=lambda item: (str(item.get("latest_event_at") or ""), str(item.get("call_id") or "")),
      )[-1]
      state["last_call_id"] = str(latest_call.get("call_id") or "")
      state["call_count"] = len(related_calls)
      state["call_ids"] = [str(item.get("call_id") or "") for item in related_calls[-10:]]
      state["transcript_covered"] = bool(state["transcript_covered"] or latest_call.get("transcript_available"))
      state["transcript_excerpt"] = str(latest_call.get("transcript_excerpt") or state["transcript_excerpt"])
      state["recording_covered"] = bool(state["recording_covered"] or latest_call.get("recording_available"))
      state["recording_url_state"] = str(latest_call.get("recording_url_state") or state["recording_url_state"])
      state["appointment_booked"] = bool(state["appointment_booked"] or latest_call.get("appointment_booked"))
      if latest_call.get("appointment_status") and str(latest_call.get("appointment_status")) != "not_booked":
        state["appointment_status"] = merge_appointment_status(state["appointment_status"], str(latest_call["appointment_status"]))
      if latest_call.get("transfer_status"):
        state["transfer_status"] = merge_transfer_status(state["transfer_status"], str(latest_call["transfer_status"]))
      state["call_summary_excerpt"] = str(latest_call.get("call_summary_excerpt") or state["call_summary_excerpt"])
      state["owner_hint"] = str(latest_call.get("owner_hint") or state["owner_hint"])
  for call_state in call_states.values():
    resolved_contact_id = str(call_state.get("resolved_contact_id") or "")
    if not resolved_contact_id or resolved_contact_id in states:
      continue
    states[resolved_contact_id] = {
      "contact_id": resolved_contact_id,
      "resolved_contact_id": resolved_contact_id,
      "latest_event_at": str(call_state.get("latest_event_at") or ""),
      "latest_event_type": str(call_state.get("latest_event_type") or ""),
      "last_event_type": str(call_state.get("last_event_type") or ""),
      "campaign_ids": [str(call_state.get("campaign_id") or "")] if call_state.get("campaign_id") else [],
      "source_systems": list(call_state.get("source_systems") or []),
      "event_count": int(call_state.get("event_count") or 0),
      "call_count": 1,
      "call_ids": [str(call_state.get("call_id") or "")],
      "automation_stage": "",
      "recommended_tool": "",
      "amount_bucket": "",
      "owner_hint": str(call_state.get("owner_hint") or ""),
      "transcript_covered": bool(call_state.get("transcript_available")),
      "transcript_excerpt": str(call_state.get("transcript_excerpt") or ""),
      "recording_covered": bool(call_state.get("recording_available")),
      "recording_url_state": str(call_state.get("recording_url_state") or "missing"),
      "appointment_booked": bool(call_state.get("appointment_booked")),
      "appointment_status": str(call_state.get("appointment_status") or "not_booked"),
      "transfer_status": str(call_state.get("transfer_status") or ""),
      "submission_status": str(call_state.get("submission_status") or ""),
      "call_summary_excerpt": str(call_state.get("call_summary_excerpt") or ""),
      "event_types": list(call_state.get("event_types") or []),
      "reconstruction_gap_reasons": list(call_state.get("reconstruction_gap_reasons") or []),
      "reconstruction_confidence_label": str(call_state.get("reconstruction_confidence_label") or "low"),
      "evidence_refs": list(call_state.get("evidence_refs") or []),
      "confidence": float(call_state.get("confidence") or 0.0),
      "last_call_id": str(call_state.get("call_id") or ""),
    }
  for state in states.values():
    gap_reasons = _contact_reconstruction_gap_reasons(state)
    state["reconstruction_gap_reasons"] = gap_reasons
    state["reconstruction_confidence_label"] = _reconstruction_confidence_label(
      gap_reasons,
      transcript_present=bool(state.get("transcript_covered")),
      recording_present=bool(state.get("recording_covered")),
      owner_present=bool(str(state.get("owner_hint") or "").strip()),
    )
  return states
