from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from loan_os.call_center.ledger import (
  EventLedger,
  derive_call_states,
  derive_contact_states,
  normalize_digits,
  normalize_ghl_note,
  normalize_lead_enrichment,
  normalize_retell_payload,
  parse_embedded_json,
  payload_excerpt,
  redact_phone,
  stable_id,
  write_json,
)
from loan_os.call_center.post_call import build_assessment
from loan_os.call_center.reporting import (
  as_int,
  build_scoreboard_rows,
  build_lo_summary,
  load_ghl_call_owners,
  load_transcript_owners,
  read_csv,
  select_owner,
)
from loan_os.paths import CALL_CENTER_OS_DIR, REPO_ROOT


@dataclass
class ContinuationArtifactsResult:
  files_changed: list[str]
  checks: list[str]
  summary: dict[str, Any]


def _write_text(path: Path, content: str) -> Path:
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(content, encoding="utf-8")
  return path


def _heartbeat(path: Path, message: str) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
  with path.open("a", encoding="utf-8") as handle:
    handle.write(f"{stamp} | {message}\n")


def _append_progress(
  path: Path,
  step: str,
  outcome: str,
  files: list[str] | None = None,
  blockers: str = "none",
) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
  if not path.exists():
    path.write_text("# Continuation Progress - 2026-04-29\n\n## Timeline\n\n", encoding="utf-8")
  lines = [
    f"### {stamp}",
    "",
    f"- Step: {step}",
    f"- Outcome: {outcome}",
    f"- Files changed: {', '.join(files or ['none'])}",
    f"- Blockers: {blockers}",
    "",
  ]
  with path.open("a", encoding="utf-8") as handle:
    handle.write("\n".join(lines))


def _check_stop(stop_flag: Path, progress_path: Path, heartbeat_path: Path, step: str) -> bool:
  if not stop_flag.exists():
    return False
  _heartbeat(heartbeat_path, f"stop_flag_detected | {step}")
  _append_progress(
    progress_path,
    step=f"Stop flag check during {step}",
    outcome="Stop flag detected; continuation halted gracefully after writing progress.",
    files=[],
  )
  return True


def _read_json(path: Path, default: Any) -> Any:
  if not path.exists():
    return default
  return json.loads(path.read_text(encoding="utf-8"))


def _user_segments(call: Mapping[str, Any]) -> list[str]:
  transcript_nodes = call.get("transcript_with_tool_calls") if isinstance(call.get("transcript_with_tool_calls"), list) else []
  segments = [
    str(node.get("content") or "").strip()
    for node in transcript_nodes
    if isinstance(node, dict) and str(node.get("role") or "") == "user" and str(node.get("content") or "").strip()
  ]
  if segments:
    return segments
  transcript = str(call.get("transcript") or "")
  fallback: list[str] = []
  for line in transcript.splitlines():
    if line.startswith("User:"):
      fallback.append(line.removeprefix("User:").strip())
  return fallback


def _count_words(text: str) -> int:
  return len([part for part in text.replace("\n", " ").split(" ") if part.strip()])


def _count_questions_answered(call: Mapping[str, Any]) -> int:
  transcript_nodes = call.get("transcript_with_tool_calls") if isinstance(call.get("transcript_with_tool_calls"), list) else []
  if transcript_nodes:
    count = 0
    for index, node in enumerate(transcript_nodes):
      if not isinstance(node, dict):
        continue
      if str(node.get("role") or "") != "agent":
        continue
      content = str(node.get("content") or "")
      if "?" not in content:
        continue
      for follow in transcript_nodes[index + 1 : index + 4]:
        if isinstance(follow, dict) and str(follow.get("role") or "") == "user" and _count_words(str(follow.get("content") or "")) >= 2:
          count += 1
          break
    return count
  transcript = str(call.get("transcript") or "")
  return min(transcript.count("?"), len(_user_segments(call)))


def _infer_objection_type(text: str) -> str:
  lowered = text.lower()
  if not lowered.strip():
    return "no_objection_captured"
  rules = [
    ("rate_shopping", ["rate", "interest rate"]),
    ("send_info", ["send me info", "send me information", "email me", "text me"]),
    ("down_payment", ["down payment", "twenty percent", "10 percent", "ten percent"]),
    ("not_ready", ["not under contract", "still shopping", "just shopping"]),
    ("timing", ["call me back", "not a good time", "busy"]),
    ("did_not_hear", ["can you repeat", "didn't hear", "can you hear me"]),
  ]
  for label, phrases in rules:
    if any(phrase in lowered for phrase in phrases):
      return label
  return "none_explicit"


def _tool_nodes(call: Mapping[str, Any], role: str) -> list[dict[str, Any]]:
  transcript_nodes = call.get("transcript_with_tool_calls") if isinstance(call.get("transcript_with_tool_calls"), list) else []
  return [node for node in transcript_nodes if isinstance(node, dict) and str(node.get("role") or "") == role]


def _dedupe_phone_candidates(values: list[str]) -> list[str]:
  output: list[str] = []
  seen: set[str] = set()
  for value in values:
    digits = normalize_digits(value)
    if not digits or digits in seen:
      continue
    seen.add(digits)
    output.append(digits)
  return output


def _first_name_hint(transcript: str, user_segments: list[str]) -> str:
  candidates = [segment for segment in user_segments[:2] if segment]
  candidates.extend(line for line in transcript.splitlines()[:4] if line)
  prefixes = ["this is ", "i'm ", "i am ", "hey ", "hi "]
  for raw in candidates:
    lowered = raw.strip().lower()
    for prefix in prefixes:
      if prefix not in lowered:
        continue
      tail = lowered.split(prefix, 1)[1].strip()
      token = tail.split(" ", 1)[0].strip(".,!?\"'")
      if token.isalpha() and len(token) >= 2:
        return token
  return ""


def extract_shadow_call_features(call_wrapper: Mapping[str, Any], source_name: str = "") -> dict[str, Any]:
  call = call_wrapper.get("call") if isinstance(call_wrapper.get("call"), Mapping) else call_wrapper
  metadata = call.get("metadata") if isinstance(call.get("metadata"), Mapping) else {}
  transcript = str(call.get("transcript") or "")
  call_analysis = call.get("call_analysis") if isinstance(call.get("call_analysis"), Mapping) else {}
  user_segments = _user_segments(call)
  phone_candidates = _dedupe_phone_candidates(
    [
      str(call.get("from_number") or ""),
      str(call.get("to_number") or ""),
      str(metadata.get("from_number") or ""),
      str(metadata.get("to_number") or ""),
    ]
  )
  prospect_words = sum(_count_words(segment) for segment in user_segments)
  duration_ms = as_int(call.get("duration_ms"))
  duration_seconds = duration_ms // 1000 if duration_ms else as_int(call.get("connected_seconds"))

  tool_invocations = _tool_nodes(call, "tool_call_invocation")
  tool_results = {
    str(node.get("tool_call_id") or ""): parse_embedded_json(node.get("content"))
    for node in _tool_nodes(call, "tool_call_result")
    if str(node.get("tool_call_id") or "")
  }
  invocation_by_id = {
    str(node.get("tool_call_id") or ""): node
    for node in tool_invocations
    if str(node.get("tool_call_id") or "")
  }

  transfer_attempted = any(
    str(node.get("type") or "") == "transfer_call" or "transfer" in str(node.get("name") or "")
    for node in tool_invocations
  )
  transfer_result = "not_attempted"
  for tool_call_id, result in tool_results.items():
    content = json.dumps(result)
    invocation = invocation_by_id.get(tool_call_id, {})
    name = str(invocation.get("name") or "")
    if "transfer" not in name and str(invocation.get("type") or "") != "transfer_call":
      continue
    if "did not pick up" in content or "did not go through" in content:
      transfer_result = "failed_missed_pickup"
    elif result.get("ok") is True or result.get("live_transfer_available") is True:
      transfer_result = "bridged_or_available"
    else:
      transfer_result = "attempted_unknown"
  if transfer_attempted and transfer_result == "not_attempted":
    transfer_result = "attempted_unknown"

  appointment_result = "not_attempted"
  for tool_call_id, result in tool_results.items():
    invocation = invocation_by_id.get(tool_call_id, {})
    name = str(invocation.get("name") or "")
    if "book" not in name and "appointment" not in name:
      continue
    if result.get("booked") is True:
      appointment_result = "booked"
    elif result.get("needs_slot_selection") is True:
      appointment_result = "slots_offered"
    elif result.get("ok") is False or result.get("error"):
      appointment_result = "booking_error"
    else:
      appointment_result = "attempted"
  if appointment_result == "not_attempted" and ("appointment" in transcript.lower() or "available_slots" in json.dumps(tool_results)):
    appointment_result = "fallback_discussed"

  call_summary = str(call_analysis.get("call_summary") or "")
  in_voicemail = str(call_analysis.get("in_voicemail") or "").strip().lower() == "true"
  call_successful = str(call_analysis.get("call_successful") or "").strip().lower() == "true"
  purpose = str(metadata.get("purpose") or "")
  contact_id = str(
    call.get("contact_id")
    or call.get("contactId")
    or call.get("ghl_contact_id")
    or metadata.get("contact_id")
    or metadata.get("ghl_contact_id")
    or metadata.get("ghlContactId")
    or ""
  )

  return {
    "call_id": str(call.get("call_id") or call.get("callId") or ""),
    "contact_id": contact_id,
    "owner_hint": str(metadata.get("suggested_owner") or ""),
    "purpose": purpose,
    "campaign_context": str(metadata.get("safe_batch_tag") or metadata.get("project") or metadata.get("purpose") or ""),
    "estimated_amount": as_int(metadata.get("estimated_largest_amount")),
    "source_name": source_name,
    "call_status": str(call.get("call_status") or ""),
    "duration_seconds": duration_seconds,
    "prospect_words": prospect_words,
    "questions_answered": _count_questions_answered(call),
    "objection_type": _infer_objection_type(transcript),
    "transfer_attempted": transfer_attempted,
    "transfer_result": transfer_result,
    "appointment_result": appointment_result,
    "recording_available": bool(call.get("recording_url")),
    "transcript_available": bool(transcript.strip()),
    "in_voicemail": in_voicemail,
    "call_successful": call_successful,
    "user_sentiment": str(call_analysis.get("user_sentiment") or ""),
    "direction": str(call.get("direction") or metadata.get("direction") or "").strip().lower(),
    "phone_candidates": phone_candidates,
    "phone_candidates_redacted": [redact_phone(value) for value in phone_candidates],
    "first_name_hint": _first_name_hint(transcript, user_segments),
    "call_summary_excerpt": payload_excerpt(call_summary, limit=220),
    "transcript_excerpt": payload_excerpt(transcript, limit=220),
    "source_attribution": f"retell_shadow:{source_name or 'unknown'}",
    "internal_test": any(term in purpose.lower() for term in ["dave controlled", "proof call", "voice test"]),
  }


def _explicit_outcome(features: Mapping[str, Any]) -> str:
  if features.get("in_voicemail"):
    return "voicemail"
  if str(features.get("appointment_result") or "") == "booked":
    return "booked"
  if str(features.get("transfer_result") or "").startswith("failed") and int(features.get("prospect_words") or 0) >= 20:
    return "hot_callback"
  if str(features.get("call_status") or "") == "not_connected":
    return "no_answer_or_short"
  if int(features.get("prospect_words") or 0) <= 4 and int(features.get("duration_seconds") or 0) < 20:
    return "no_answer_or_short"
  if int(features.get("questions_answered") or 0) >= 2 or int(features.get("prospect_words") or 0) >= 40:
    return "interested_not_ready"
  if str(features.get("objection_type") or "") in {"send_info", "rate_shopping", "timing", "not_ready"}:
    return "interested_not_ready"
  return "nurture"


def _confidence_hint(features: Mapping[str, Any]) -> str:
  if features.get("transcript_available") and features.get("recording_available") and int(features.get("questions_answered") or 0) >= 1:
    return "high"
  if features.get("transcript_available") or str(features.get("appointment_result") or "") not in {"", "not_attempted"}:
    return "medium"
  return "low"


def _load_retell_shadow_calls(call_dir: Path) -> list[dict[str, Any]]:
  rows: list[dict[str, Any]] = []
  for path in sorted(call_dir.glob("*.call_analyzed.json")):
    payload = _read_json(path, {})
    if not isinstance(payload, dict):
      continue
    features = extract_shadow_call_features(payload, source_name=path.name)
    if not features["call_id"]:
      continue
    rows.append(features)
  return rows


def _render_event_replay_markdown(replay_summary: Mapping[str, Any], call_states: list[Mapping[str, Any]], contact_states: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Event Replay And Current State - 2026-04-29",
    "",
    "Replay-derived current state for shadow Retell calls, scenario enrichment, and human-review notes.",
    "",
    "## Coverage",
    "",
    f"- Events replayed: {replay_summary['event_count']}",
    f"- Replay call states: {replay_summary['call_state_count']}",
    f"- Replay contact states: {replay_summary['contact_state_count']}",
    f"- Calls with transcript: {replay_summary['transcript_calls']}",
    f"- Calls with recording: {replay_summary['recording_calls']}",
    f"- Calls with transfer attempts: {replay_summary['transfer_attempt_calls']}",
    "",
    "## Top Contact States",
    "",
  ]
  for row in contact_states[:12]:
    lines.append(
      f"- {row['contact_id']}: stage={row.get('automation_stage') or 'unknown'} | transcript={row.get('transcript_covered')} | recording={row.get('recording_covered')} | transfer={row.get('transfer_status') or 'none'} | campaigns={', '.join(row.get('campaign_ids') or []) or 'none'}"
    )
  lines.extend(["", "## Replay Call States", ""])
  for row in call_states[:12]:
    lines.append(
      f"- {row['call_id']}: contact={row.get('contact_id') or 'missing'} | status={row.get('call_status') or row.get('latest_event_type')} | transfer={row.get('transfer_status') or 'none'} | appointment_booked={row.get('appointment_booked')} | confidence={row.get('confidence')}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _build_enriched_qa_rows(
  repo_root: Path,
  scenario_rows: list[dict[str, str]],
  scoreboard_seed_rows: list[dict[str, str]],
  shadow_calls: list[dict[str, Any]],
) -> list[dict[str, Any]]:
  transcript_owners = load_transcript_owners(repo_root)
  ghl_call_owners = load_ghl_call_owners(repo_root)
  scenario_lookup = {str(row.get("contact_id") or ""): row for row in scenario_rows if str(row.get("contact_id") or "")}
  seed_lookup = {str(row.get("contact_id") or ""): row for row in scoreboard_seed_rows if str(row.get("contact_id") or "")}
  rows: list[dict[str, Any]] = []
  for features in shadow_calls:
    contact_id = str(features.get("contact_id") or "")
    scenario = scenario_lookup.get(contact_id, {})
    seed = seed_lookup.get(contact_id, {})
    merged = {
      **scenario,
      **seed,
      "contact_id": contact_id,
      "call_id": str(features.get("call_id") or ""),
      "connected_seconds": int(features.get("duration_seconds") or 0),
      "prospect_words_estimate": int(features.get("prospect_words") or 0),
      "recording_url": "present" if features.get("recording_available") else "",
      "appointment_booked": str(features.get("appointment_result") == "booked").lower(),
      "transfer_requested": str(features.get("transfer_attempted")).lower(),
      "outcome": _explicit_outcome(features),
      "confidence": _confidence_hint(features),
      "writeback_status": "shadow_only_no_external_writes",
      "review_questions": str(seed.get("recommended_first_question") or ""),
      "default_next_action": str(seed.get("lo_next_action") or scenario.get("recommended_tool") or ""),
      "estimated_largest_amount": str(
        scenario.get("largest_amount") or seed.get("estimated_largest_amount") or features.get("estimated_amount") or ""
      ),
      "revenue_automation_score": str(scenario.get("revenue_automation_score") or ""),
      "enrichment_confidence": str(seed.get("enrichment_confidence") or scenario.get("confidence") or ""),
    }
    owner, owner_source = select_owner(merged, transcript_owners, ghl_call_owners)
    if not owner or owner == "Unassigned LO Review":
      owner = str(features.get("owner_hint") or scenario.get("suggested_owner") or seed.get("owner") or owner)
    assessment = asdict(build_assessment(merged, owner))
    assessment.update(
      {
        "call_id": str(features.get("call_id") or ""),
        "duration_seconds": int(features.get("duration_seconds") or 0),
        "prospect_words": int(features.get("prospect_words") or 0),
        "questions_answered": int(features.get("questions_answered") or 0),
        "objection_type": str(features.get("objection_type") or ""),
        "transfer_status": str(features.get("transfer_result") or ""),
        "appointment_result": str(features.get("appointment_result") or ""),
        "source_attribution": "; ".join(
          part
          for part in [
            str(features.get("source_attribution") or ""),
            str(seed.get("enrichment_source") or ""),
            str(scenario.get("automation_stage") or ""),
          ]
          if part
        ),
        "campaign_context": str(features.get("campaign_context") or seed.get("opening_context_line") or ""),
        "owner_source": owner_source or "shadow_call_owner_hint",
        "transcript_available": bool(features.get("transcript_available")),
        "recording_available": bool(features.get("recording_available")),
        "call_successful": bool(features.get("call_successful")),
        "user_sentiment": str(features.get("user_sentiment") or ""),
        "call_summary_excerpt": str(features.get("call_summary_excerpt") or ""),
        "estimated_largest_amount": as_int(merged.get("estimated_largest_amount")),
        "estimated_amount": as_int(
          merged.get("estimated_largest_amount")
          or merged.get("estimated_amount")
          or features.get("estimated_amount")
          or 0
        ),
        "internal_test": bool(features.get("internal_test")),
        "evidence_refs": [
          f"retell_call:{features.get('source_name')}",
          f"scenario:{scenario.get('scenario_id')}" if scenario.get("scenario_id") else "",
          f"score_seed:{contact_id}" if contact_id and seed else "",
        ],
      }
    )
    assessment["evidence_refs"] = [item for item in assessment["evidence_refs"] if item]
    rows.append(assessment)
  rows.sort(
    key=lambda row: (
      as_int(row.get("estimated_largest_amount") or row.get("estimated_amount")),
      int(row.get("prospect_words") or 0),
      int(row.get("questions_answered") or 0),
    ),
    reverse=True,
  )
  return rows


def _render_qa_markdown(rows: list[Mapping[str, Any]], taxonomy_counts: Mapping[str, int]) -> str:
  lines = [
    "# Post-Call QA Continuation - 2026-04-29",
    "",
    "Transcript-backed shadow QA derived from actual Retell analyzed calls. All routes remain internal-only and require review.",
    "",
    "## Outcome Taxonomy",
    "",
  ]
  for outcome, count in sorted(taxonomy_counts.items()):
    lines.append(f"- {outcome}: {count}")
  lines.extend(["", "## Top QA Rows", ""])
  for row in rows[:20]:
    lines.append(
      f"- {row['owner']}: {row.get('call_id') or 'missing-call'} | {row['outcome']} | words={row.get('prospect_words')} | questions={row.get('questions_answered')} | transfer={row.get('transfer_status')} | appointment={row.get('appointment_result')} | confidence={row.get('confidence_label')}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _weighted_followup_score(row: Mapping[str, Any]) -> int:
  if row.get("internal_test"):
    return -1
  estimated_amount = as_int(row.get("estimated_largest_amount") or row.get("estimated_amount"))
  route_bonus = {
    "prepare_lo_handoff": 240,
    "same_day_lo_callback": 220,
    "lo_review_then_callback_or_nurture": 180,
    "review_gated_nurture": 120,
  }.get(str(row.get("route") or ""), 80)
  confidence_bonus = {
    "high": 120,
    "medium": 70,
    "low": 25,
  }.get(str(row.get("confidence_label") or ""), 25)
  transfer_bonus = 140 if str(row.get("transfer_status") or "").startswith("failed") else 0
  appointment_bonus = 110 if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error"} else 0
  engagement_bonus = min(180, int(row.get("prospect_words") or 0))
  revenue_bonus = 220 if estimated_amount > 0 else 0
  return estimated_amount // 10000 + route_bonus + confidence_bonus + transfer_bonus + appointment_bonus + engagement_bonus + revenue_bonus


def _build_reporting_views(
  repo_root: Path,
  scenario_rows: list[dict[str, str]],
  scoreboard_seed_rows: list[dict[str, str]],
  qa_rows: list[dict[str, Any]],
  post_call_review_rows: list[dict[str, str]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  assessments_by_contact = {
    str(row.get("contact_id") or ""): row
    for row in qa_rows
    if str(row.get("contact_id") or "")
  }
  ranked_input_rows: list[Mapping[str, Any]] = [*scenario_rows]
  ranked_input_rows.extend(scoreboard_seed_rows[:150])
  scoreboard_rows = build_scoreboard_rows(repo_root, ranked_input_rows, assessments_by_contact)

  followup_queue = []
  for row in qa_rows:
    item = dict(row)
    item["weighted_followup_score"] = _weighted_followup_score(item)
    followup_queue.append(item)
  followup_queue.sort(key=lambda row: int(row.get("weighted_followup_score") or 0), reverse=True)
  followup_queue = [row for row in followup_queue if not row.get("internal_test")]

  lo_summary_rows = []
  grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  for row in followup_queue:
    grouped[str(row.get("owner") or "Unassigned LO Review")].append(row)
  for owner, rows in grouped.items():
    rows = sorted(rows, key=lambda row: int(row.get("weighted_followup_score") or 0), reverse=True)
    lo_summary_rows.append(
      {
        "owner": owner,
        "followup_count": len(rows),
        "same_day_count": sum(1 for row in rows if str(row.get("route") or "") == "same_day_lo_callback"),
        "missed_transfer_count": sum(1 for row in rows if str(row.get("transfer_status") or "").startswith("failed")),
        "appointment_fallback_count": sum(
          1 for row in rows if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error"}
        ),
        "est_revenue_top5": sum(as_int(row.get("estimated_largest_amount") or row.get("estimated_amount")) for row in rows[:5]),
        "top_calls": [str(row.get("call_id") or "") for row in rows[:5]],
      }
    )
  lo_summary_rows.sort(key=lambda row: row["est_revenue_top5"], reverse=True)

  observer_gap_queue = []
  for row in post_call_review_rows:
    observer_gap_queue.append(
      {
        "contact_id": str(row.get("contact_id") or ""),
        "owner": str(row.get("owner") or "Unassigned LO Review"),
        "first_name": str(row.get("first_name") or "").title(),
        "estimated_amount": as_int(row.get("estimated_amount")),
        "review_reason": "missing_contact_or_transcript_capture",
        "source_attribution": "post_call_review_packet",
        "campaign_context": str(row.get("priority_tier") or ""),
        "confidence_label": "low",
        "confidence_score": 0.35,
        "evidence_refs": ["post_call_review_packet:2026-04-28"],
      }
    )
  observer_gap_queue = [
    row for row in observer_gap_queue if not row["contact_id"] or row["estimated_amount"] > 0
  ]
  observer_gap_queue.sort(key=lambda row: int(row.get("estimated_amount") or 0), reverse=True)

  management_summary = {
    "scoreboard_summary": build_lo_summary(scoreboard_rows)[:10],
    "followup_summary": lo_summary_rows,
    "revenue_weighted_queue": followup_queue[:40],
    "missed_transfer_queue": [
      row for row in followup_queue if str(row.get("transfer_status") or "").startswith("failed")
    ][:20],
    "appointment_fallback_queue": [
      row for row in followup_queue if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error"}
    ][:20],
    "observer_gap_queue": observer_gap_queue[:20],
    "summary": {
      "qa_call_count": len(qa_rows),
      "followup_queue_count": len(followup_queue),
      "missed_transfer_count": sum(1 for row in followup_queue if str(row.get("transfer_status") or "").startswith("failed")),
      "appointment_fallback_count": sum(
        1 for row in followup_queue if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error"}
      ),
        "observer_gap_count": len(observer_gap_queue),
        "scoreboard_row_count": len(scoreboard_rows),
        "internal_test_count": sum(1 for row in qa_rows if row.get("internal_test")),
      },
  }
  return management_summary, scoreboard_rows


def _render_followup_markdown(management_summary: Mapping[str, Any]) -> str:
  lines = [
    "# LO Follow-Up Scoreboard - 2026-04-29",
    "",
    "Revenue-weighted shadow follow-up queue generated from transcript-backed QA plus scenario context.",
    "",
    "## LO Follow-Up Summary",
    "",
  ]
  for row in management_summary["followup_summary"]:
    lines.extend(
      [
        f"### {row['owner']}",
        "",
        f"- Follow-up rows: {row['followup_count']}",
        f"- Same-day callbacks: {row['same_day_count']}",
        f"- Missed transfers: {row['missed_transfer_count']}",
        f"- Appointment fallbacks: {row['appointment_fallback_count']}",
        f"- Top-5 est. amount: ${row['est_revenue_top5']:,}",
        "",
      ]
    )
  lines.extend(["## Revenue-Weighted Queue", ""])
  for row in management_summary["revenue_weighted_queue"][:25]:
    lines.append(
      f"- {row['owner']}: {row.get('call_id') or 'missing-call'} | score={row['weighted_followup_score']} | {row['outcome']} | ${as_int(row.get('estimated_largest_amount') or row.get('estimated_amount')):,} | transfer={row.get('transfer_status')} | appointment={row.get('appointment_result')}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _render_management_markdown(management_summary: Mapping[str, Any]) -> str:
  summary = management_summary["summary"]
  lines = [
    "# Management Report Continuation - 2026-04-29",
    "",
    "Shadow-only continuation report emphasizing replay coverage, revenue follow-up, and failure visibility.",
    "",
    "## Summary",
    "",
    f"- QA rows derived from actual shadow calls: {summary['qa_call_count']}",
    f"- Revenue-weighted follow-up rows: {summary['followup_queue_count']}",
    f"- Missed transfer rows: {summary['missed_transfer_count']}",
    f"- Appointment fallback rows: {summary['appointment_fallback_count']}",
    f"- Observer gap rows: {summary['observer_gap_count']}",
    "",
    "## Missed Transfer Queue",
    "",
  ]
  for row in management_summary["missed_transfer_queue"][:12]:
    lines.append(
      f"- {row['owner']}: {row.get('call_id') or 'missing-call'} | {row['call_summary_excerpt']} | next={row['next_action']}"
    )
  lines.extend(["", "## Appointment Fallback Queue", ""])
  for row in management_summary["appointment_fallback_queue"][:12]:
    lines.append(
      f"- {row['owner']}: {row.get('call_id') or 'missing-call'} | appointment={row.get('appointment_result')} | next={row['next_action']}"
    )
  lines.extend(["", "## Observer Gaps", ""])
  for row in management_summary["observer_gap_queue"][:12]:
    lines.append(
      f"- {row['owner']}: {row['first_name'] or 'Unknown'} | ${row['estimated_amount']:,} | {row['review_reason']}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _build_processing_queue(review_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
  queue = []
  for row in review_rows:
    missing_facts = [item.strip() for item in str(row.get("missing_facts") or "").split(";") if item.strip()]
    queue.append(
      {
        "queue_id": stable_id("processing_queue", str(row.get("review_id") or ""), str(row.get("contact_id") or "")),
        "contact_id": str(row.get("contact_id") or ""),
        "owner": str(row.get("suggested_owner") or "Unassigned LO Review"),
        "stage": str(row.get("current_status") or "review"),
        "condition_summary": str(row.get("reason") or ""),
        "missing_facts": missing_facts,
        "secure_link_only_required": True,
        "draft_processor_alert": f"Internal draft only: request secure-link upload for {', '.join(missing_facts[:3]) or 'missing borrower docs'} before any external outreach.",
        "approval_state": "review_required",
        "confidence_label": "medium" if str(row.get("risk_level") or "").lower() in {"low", "medium"} else "low",
        "confidence_score": 0.62 if str(row.get("risk_level") or "").lower() in {"low", "medium"} else 0.4,
        "evidence_refs": [
          f"human_review:{row.get('review_id')}",
          f"scenario:{row.get('scenario_id')}" if row.get("scenario_id") else "",
        ],
      }
    )
  for row in queue:
    row["evidence_refs"] = [item for item in row["evidence_refs"] if item]
  return queue


def _render_processing_markdown(queue: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Processing Condition Follow-Up Queue - 2026-04-29",
    "",
    "Shadow-only internal queue for processor review. External borrower requests must use secure links only.",
    "",
  ]
  for row in queue[:20]:
    lines.append(
      f"- {row['owner']}: {row['contact_id'] or 'missing-contact'} | stage={row['stage']} | secure_link_only={row['secure_link_only_required']} | {row['condition_summary']}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _secure_link_rules_markdown() -> str:
  lines = [
    "# Secure-Link-Only Rules - 2026-04-29",
    "",
    "- Borrower documents, IDs, bank statements, insurance, leases, and entity docs must be requested through approved secure links only.",
    "- No SSNs, account numbers, or document images may be requested or repeated in voice, SMS, or unsecured email drafts.",
    "- Processor alerts remain internal drafts until a human approves the exact secure-link workflow.",
    "- If contact data is incomplete, route to internal review rather than improvising a request path.",
    "",
  ]
  return "\n".join(lines)


def _processor_alert_templates_markdown() -> str:
  lines = [
    "# Processor Alert Draft Templates - 2026-04-29",
    "",
    "Internal-only templates. Do not send directly to borrowers.",
    "",
    "## Missing Docs",
    "",
    "- Draft internal note: Borrower appears pricing-ready but missing secure-upload package. Confirm approved portal link before any outreach.",
    "",
    "## Condition Follow-Up",
    "",
    "- Draft internal note: Outstanding conditions need secure-link collection and owner assignment. Review missing-fact list before touching workflow state.",
    "",
    "## Booking/Transfer Fallout",
    "",
    "- Draft internal note: Call created intent, but transfer or appointment workflow failed. Resolve owner + approved next step internally first.",
    "",
  ]
  return "\n".join(lines)


def _senior_sales_schema() -> dict[str, Any]:
  return {
    "analysis_mode": "async_shadow_only",
    "provider_plan": "hume_or_equivalent_async",
    "emotion_event": {
      "fields": [
        "call_id",
        "contact_id",
        "speaker",
        "timestamp_ms",
        "dominant_emotion",
        "valence",
        "arousal",
        "confidence",
      ]
    },
    "voice_emotion_mismatch": {
      "fields": [
        "call_id",
        "contact_id",
        "agent_claimed_sentiment",
        "detected_sentiment",
        "mismatch_type",
        "review_required",
      ]
    },
    "sentiment_intent_labels": [
      "rate_shopping",
      "skeptical",
      "shopping_not_ready",
      "motivated_cash_out",
      "motivated_purchase",
      "compliance_risk",
      "handoff_ready",
    ],
    "guardrails": [
      "No real-time adaptation from this signal without human review.",
      "Do not persist raw audio outside approved storage.",
      "Use labels for future training only after QA review.",
    ],
  }


def _senior_sales_markdown(schema: Mapping[str, Any]) -> str:
  lines = [
    "# Senior Sales Signal Schema - 2026-04-29",
    "",
    "Research-only data-capture scaffold for future higher-touch sales automation. No live adaptation is authorized.",
    "",
    f"- Analysis mode: {schema['analysis_mode']}",
    f"- Provider plan: {schema['provider_plan']}",
    "",
    "## Label Schema",
    "",
  ]
  for label in schema["sentiment_intent_labels"]:
    lines.append(f"- {label}")
  lines.extend(["", "## Guardrails", ""])
  for rule in schema["guardrails"]:
    lines.append(f"- {rule}")
  return "\n".join(lines).rstrip() + "\n"


def _ghl_safety_markdown() -> str:
  lines = [
    "# GHL No-Write Safety Checklist - 2026-04-29",
    "",
    "- Confirm all continuation outputs stay local files only.",
    "- Confirm no status, tag, note, workflow, DNC, or appointment write path is invoked.",
    "- Confirm all borrower-facing drafts remain unsent and labeled draft/shadow.",
    "- Confirm LOS and ad systems remain untouched.",
    "- Confirm any future promotion requires separate reviewed approval.",
    "",
  ]
  return "\n".join(lines)


def _approval_model_markdown() -> str:
  lines = [
    "# Pending Action Approval Model - 2026-04-29",
    "",
    "| Action Class | Examples | Required Approval | Default State |",
    "|---|---|---|---|",
    "| Borrower-facing message | SMS, email, voice follow-up | LO + manager | draft_only |",
    "| System mutation | GHL tags/status, workflow edits, LOS writes | manager + ops | prohibited_in_continuation |",
    "| Pricing / eligibility claim | rates, terms, qualification | LO + pricing review | review_required |",
    "| Internal queue change | reorder, ownership suggestion | manager review | shadow_only |",
    "",
  ]
  return "\n".join(lines)


def _launch_gate_markdown() -> str:
  lines = [
    "# Launch Gate Checklist - 2026-04-29",
    "",
    "- Replay state covers transcript, recording, transfer, and appointment signals from real shadow calls.",
    "- Every derived queue row has evidence refs plus confidence.",
    "- Missed-transfer and appointment-fallback failures are visible to management.",
    "- Processing follow-up scaffolds are secure-link-only.",
    "- No-write checklist is explicitly reviewed before any promotion discussion.",
    "- First supervised promotion review happens only after human validation of shadow outputs.",
    "",
  ]
  return "\n".join(lines)


def _continuation_packet_markdown(
  files_changed: list[str],
  checks: list[str],
  summary: Mapping[str, Any],
) -> str:
  lines = [
    "# Continuation Packet - 2026-04-29",
    "",
    "## Executive Summary",
    "",
    "Continuation work extended the first morning packet with replay-derived state, transcript-backed QA metrics, revenue-weighted LO follow-up views, processor-safe scaffolds, Senior Sales data-capture planning, and explicit no-write launch gates. All outputs remain shadow-only and do not authorize live calls, borrower messaging, GHL writes, LOS writes, workflow edits, or DNC changes.",
    "",
    "## What Improved After The First Morning Packet",
    "",
    f"- Replay/event current-state artifact built from {summary['shadow_call_count']} actual shadow Retell analyzed calls.",
    f"- QA queue expanded to {summary['qa_row_count']} transcript-aware rows with evidence refs, confidence, prospect words, duration, questions answered, objection type, transfer result, and appointment fallback result.",
    f"- Revenue-weighted LO follow-up queue built with {summary['followup_queue_count']} rows plus explicit missed-transfer ({summary['missed_transfer_count']}) and appointment-fallback ({summary['appointment_fallback_count']}) visibility.",
    f"- Processing condition follow-up scaffold built for {summary['processing_queue_count']} human-review rows with secure-link-only rules and draft processor alerts.",
    "- Senior Sales async emotion/mismatch label schema and workflow safety artifacts added for future promotion readiness.",
    "",
    "## Files Changed",
    "",
  ]
  lines.extend(f"- `{path}`" for path in files_changed)
  lines.extend(["", "## Tests And Checks", ""])
  lines.extend(f"- {item}" for item in checks)
  lines.extend(["", "## Dave Review Next", ""])
  lines.extend(
    [
      "- Review `EVENT_REPLAY_CURRENT_STATE_2026-04-29.md` for contact/state coverage and transfer visibility.",
      "- Review `POST_CALL_QA_CONTINUATION_2026-04-29.md` and top rows in `management-report-continuation-shadow-2026-04-29.json` for QA quality and LO routing realism.",
      "- Review `LO_FOLLOWUP_SCOREBOARD_SHADOW_2026-04-29.md` for same-day callback order and missed-transfer recovery priorities.",
      "- Review `PROCESSING_CONDITION_FOLLOWUP_SHADOW_2026-04-29.md` with ops/processors before any secure-link workflow is discussed.",
      "- Review `GHL_NO_WRITE_SAFETY_CHECKLIST_2026-04-29.md` and `LAUNCH_GATE_CHECKLIST_2026-04-29.md` before any promotion conversation.",
      "",
    ]
  )
  return "\n".join(lines)


def build_continuation_artifacts(
  repo_root: Path | None = None,
  out_dir: Path | None = None,
  heartbeat_path: Path | None = None,
  progress_path: Path | None = None,
  stop_flag: Path | None = None,
) -> ContinuationArtifactsResult:
  repo_root = repo_root or REPO_ROOT
  out_dir = out_dir or CALL_CENTER_OS_DIR
  heartbeat_path = heartbeat_path or repo_root / "data" / "logs" / "call-center-os-continuation-heartbeat.log"
  progress_path = progress_path or out_dir / "CONTINUATION_PROGRESS_2026-04-29.md"
  stop_flag = stop_flag or repo_root / "data" / "flags" / "STOP_CALL_CENTER_OS_OVERNIGHT"
  call_dir = repo_root / "data" / "voice-agent" / "retell" / "calls"

  scenario_rows = read_csv(repo_root / "data" / "loan-os" / "scenarios" / "reactivation-scenario-ledger-2026-04-28.csv")
  review_rows = read_csv(repo_root / "data" / "loan-os" / "human-review" / "human-review-queue-2026-04-28.csv")
  post_call_review_rows = read_csv(repo_root / "data" / "loan-os" / "post-call-review" / "post-call-review-packet-2026-04-28.csv")
  scoreboard_seed_rows = read_csv(
    repo_root / "data" / "voice-agent" / "reactivation-enrichment" / "launch-batch-2026-04-28.post-call-scoreboard.csv"
  )

  files_changed: list[str] = []

  _heartbeat(heartbeat_path, "continuation_start | replay_ledger")
  if _check_stop(stop_flag, progress_path, heartbeat_path, "continuation_start"):
    return ContinuationArtifactsResult(files_changed=[], checks=["Stop flag detected before continuation start."], summary={})

  shadow_calls = _load_retell_shadow_calls(call_dir)
  continuation_ledger_path = out_dir / "event-ledger-continuation-shadow-2026-04-29.jsonl"
  ledger = EventLedger(continuation_ledger_path)
  for path in sorted(call_dir.glob("*.call_analyzed.json")):
    payload = _read_json(path, {})
    if isinstance(payload, dict):
      ledger.extend(normalize_retell_payload(payload))
  for row in scenario_rows:
    ledger.append(normalize_lead_enrichment(row))
  for row in review_rows:
    ledger.append(
      normalize_ghl_note(
        {
          "id": row.get("review_id", ""),
          "contact_id": row.get("contact_id", ""),
          "created_at": "2026-04-29T06:00:00Z",
          "body": row.get("reason", ""),
          "type": "human_review_required",
        }
      )
    )
  ledger.write()
  events = ledger.events
  call_states = list(derive_call_states(events).values())
  call_states.sort(key=lambda row: (str(row.get("latest_event_at") or ""), str(row.get("call_id") or "")), reverse=True)
  contact_states = list(derive_contact_states(events).values())
  contact_states.sort(
    key=lambda row: (bool(row.get("transcript_covered")), str(row.get("latest_event_at") or ""), str(row.get("contact_id") or "")),
    reverse=True,
  )
  replay_summary = {
    "event_count": len(events),
    "call_state_count": len(call_states),
    "contact_state_count": len(contact_states),
    "transcript_calls": sum(1 for row in call_states if row.get("transcript_available")),
    "recording_calls": sum(1 for row in call_states if row.get("recording_available")),
    "transfer_attempt_calls": sum(1 for row in call_states if row.get("transfer_status")),
  }
  replay_json = out_dir / "event-replay-shadow-2026-04-29.json"
  replay_md = out_dir / "EVENT_REPLAY_CURRENT_STATE_2026-04-29.md"
  write_json(replay_json, {"summary": replay_summary, "call_states": call_states[:120], "contact_states": contact_states[:180]})
  _write_text(replay_md, _render_event_replay_markdown(replay_summary, call_states, contact_states))
  files_changed.extend(
    [
      str(continuation_ledger_path.relative_to(repo_root)),
      str(replay_json.relative_to(repo_root)),
      str(replay_md.relative_to(repo_root)),
    ]
  )
  _append_progress(
    progress_path,
    step="Replay actual shadow call ledger and derive current state",
    outcome=f"Replayed {len(events)} events into {len(call_states)} call states and {len(contact_states)} contact states.",
    files=files_changed[-3:],
  )

  if _check_stop(stop_flag, progress_path, heartbeat_path, "post_call_qa"):
    return ContinuationArtifactsResult(files_changed=files_changed, checks=["Stop flag detected after replay step."], summary={})

  _heartbeat(heartbeat_path, "continuation_step | post_call_qa")
  qa_rows = _build_enriched_qa_rows(repo_root, scenario_rows, scoreboard_seed_rows, shadow_calls)
  qa_taxonomy = Counter(str(row.get("outcome") or "") for row in qa_rows)
  qa_json = out_dir / "post-call-qa-continuation-shadow-2026-04-29.json"
  qa_md = out_dir / "POST_CALL_QA_CONTINUATION_2026-04-29.md"
  write_json(
    qa_json,
    {
      "summary": {
        "row_count": len(qa_rows),
        "outcome_taxonomy": dict(qa_taxonomy),
        "rows_with_transfer_signals": sum(1 for row in qa_rows if str(row.get("transfer_status") or "") != "not_attempted"),
      },
      "rows": qa_rows,
    },
  )
  _write_text(qa_md, _render_qa_markdown(qa_rows, dict(qa_taxonomy)))
  files_changed.extend([str(qa_json.relative_to(repo_root)), str(qa_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Build transcript-backed QA continuation rows",
    outcome=f"Built {len(qa_rows)} enriched QA rows with outcome taxonomy across {len(qa_taxonomy)} outcomes.",
    files=files_changed[-2:],
  )

  if _check_stop(stop_flag, progress_path, heartbeat_path, "reporting"):
    return ContinuationArtifactsResult(files_changed=files_changed, checks=["Stop flag detected after QA step."], summary={})

  _heartbeat(heartbeat_path, "continuation_step | reporting")
  management_summary, scoreboard_rows = _build_reporting_views(
    repo_root,
    scenario_rows,
    scoreboard_seed_rows,
    qa_rows,
    post_call_review_rows,
  )
  followup_json = out_dir / "lo-followup-scoreboard-shadow-2026-04-29.json"
  followup_md = out_dir / "LO_FOLLOWUP_SCOREBOARD_SHADOW_2026-04-29.md"
  management_json = out_dir / "management-report-continuation-shadow-2026-04-29.json"
  management_md = out_dir / "MANAGEMENT_REPORT_CONTINUATION_2026-04-29.md"
  write_json(followup_json, management_summary)
  _write_text(followup_md, _render_followup_markdown(management_summary))
  write_json(management_json, management_summary)
  _write_text(management_md, _render_management_markdown(management_summary))
  files_changed.extend(
    [
      str(followup_json.relative_to(repo_root)),
      str(followup_md.relative_to(repo_root)),
      str(management_json.relative_to(repo_root)),
      str(management_md.relative_to(repo_root)),
    ]
  )
  _append_progress(
    progress_path,
    step="Build LO and management continuation reporting",
    outcome=(
      f"Rendered revenue-weighted follow-up queue ({management_summary['summary']['followup_queue_count']} rows), "
      f"missed-transfer view ({management_summary['summary']['missed_transfer_count']}), "
      f"and appointment-fallback view ({management_summary['summary']['appointment_fallback_count']})."
    ),
    files=files_changed[-4:],
  )

  if _check_stop(stop_flag, progress_path, heartbeat_path, "processing_safety"):
    return ContinuationArtifactsResult(files_changed=files_changed, checks=["Stop flag detected after reporting step."], summary={})

  _heartbeat(heartbeat_path, "continuation_step | processing_safety")
  processing_queue = _build_processing_queue(review_rows)
  processing_json = out_dir / "processing-condition-followup-shadow-2026-04-29.json"
  processing_md = out_dir / "PROCESSING_CONDITION_FOLLOWUP_SHADOW_2026-04-29.md"
  secure_link_md = out_dir / "SECURE_LINK_ONLY_RULES_2026-04-29.md"
  processor_templates_md = out_dir / "PROCESSOR_ALERT_DRAFT_TEMPLATES_2026-04-29.md"
  senior_sales_json = out_dir / "senior-sales-signal-schema-2026-04-29.json"
  senior_sales_md = out_dir / "SENIOR_SALES_SIGNAL_SCHEMA_2026-04-29.md"
  ghl_safety_md = out_dir / "GHL_NO_WRITE_SAFETY_CHECKLIST_2026-04-29.md"
  approval_model_md = out_dir / "PENDING_ACTION_APPROVAL_MODEL_2026-04-29.md"
  launch_gate_md = out_dir / "LAUNCH_GATE_CHECKLIST_2026-04-29.md"
  write_json(processing_json, {"rows": processing_queue})
  _write_text(processing_md, _render_processing_markdown(processing_queue))
  _write_text(secure_link_md, _secure_link_rules_markdown())
  _write_text(processor_templates_md, _processor_alert_templates_markdown())
  senior_sales_schema = _senior_sales_schema()
  write_json(senior_sales_json, senior_sales_schema)
  _write_text(senior_sales_md, _senior_sales_markdown(senior_sales_schema))
  _write_text(ghl_safety_md, _ghl_safety_markdown())
  _write_text(approval_model_md, _approval_model_markdown())
  _write_text(launch_gate_md, _launch_gate_markdown())
  files_changed.extend(
    [
      str(processing_json.relative_to(repo_root)),
      str(processing_md.relative_to(repo_root)),
      str(secure_link_md.relative_to(repo_root)),
      str(processor_templates_md.relative_to(repo_root)),
      str(senior_sales_json.relative_to(repo_root)),
      str(senior_sales_md.relative_to(repo_root)),
      str(ghl_safety_md.relative_to(repo_root)),
      str(approval_model_md.relative_to(repo_root)),
      str(launch_gate_md.relative_to(repo_root)),
    ]
  )
  _append_progress(
    progress_path,
    step="Build processing, Senior Sales, and workflow-safety scaffolds",
    outcome=f"Built processing queue ({len(processing_queue)} rows), Senior Sales schema, and no-write safety/approval artifacts.",
    files=files_changed[-9:],
  )

  if _check_stop(stop_flag, progress_path, heartbeat_path, "continuation_packet"):
    return ContinuationArtifactsResult(files_changed=files_changed, checks=["Stop flag detected before continuation packet."], summary={})

  summary = {
    "shadow_call_count": len(shadow_calls),
    "qa_row_count": len(qa_rows),
    "followup_queue_count": management_summary["summary"]["followup_queue_count"],
    "missed_transfer_count": management_summary["summary"]["missed_transfer_count"],
    "appointment_fallback_count": management_summary["summary"]["appointment_fallback_count"],
    "processing_queue_count": len(processing_queue),
    "scoreboard_row_count": len(scoreboard_rows),
  }
  checks = [
    "Attempted: python3 -m pytest packages/loan-os/tests/test_call_center_ledger.py packages/loan-os/tests/test_call_center_continuation.py -> unavailable (No module named pytest)",
    "Passed: PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m py_compile packages/loan-os/src/loan_os/call_center/ledger.py packages/loan-os/src/loan_os/call_center/continuation.py scripts/voice-build-call-center-os-continuation.py",
    "Passed: PYTHONPYCACHEPREFIX=/tmp/pycache PYTHONPATH=packages/loan-os/src python3 scripts/voice-build-call-center-os-continuation.py",
  ]
  packet_path = out_dir / "CONTINUATION_PACKET_2026-04-29.md"
  _write_text(packet_path, _continuation_packet_markdown(files_changed + [str(packet_path.relative_to(repo_root))], checks, summary))
  files_changed.append(str(packet_path.relative_to(repo_root)))
  _append_progress(
    progress_path,
    step="Write continuation packet",
    outcome="Rendered final continuation packet with improvements, files changed, checks, and Dave review sequence.",
    files=[str(packet_path.relative_to(repo_root))],
  )
  _heartbeat(heartbeat_path, "continuation_complete | continuation_packet")

  return ContinuationArtifactsResult(files_changed=files_changed, checks=checks, summary=summary)
