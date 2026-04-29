from __future__ import annotations

import csv
import hashlib
import html
import json
from io import StringIO
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from loan_os.call_center.agents import AGENT_SCAFFOLDS
from loan_os.call_center.continuation import extract_shadow_call_features
from loan_os.call_center.ledger import (
  EventLedger,
  derive_call_states,
  derive_contact_states,
  merge_appointment_status,
  merge_transfer_status,
  normalize_digits,
  normalize_ghl_note,
  normalize_lead_enrichment,
  normalize_retell_payload,
  redact_phone,
  stable_id,
  write_json,
)
from loan_os.call_center.post_call import build_assessment
from loan_os.call_center.reporting import (
  as_int,
  build_lo_summary,
  build_scoreboard_rows,
  load_ghl_call_owners,
  load_transcript_owners,
  normalize_owner_name,
  read_csv,
  select_owner,
)
from loan_os.call_center.rsi import (
  build_governed_rsi_recommendations,
  render_governed_recommendations_markdown,
)
from loan_os.paths import CALL_CENTER_OS_DIR, REPO_ROOT


AUTONOMOUS_DATE = "2026-04-29"
AUTONOMOUS_BUILD_VERSION = "2026-04-29.6"
RAW_CALL_SOURCE_PATTERNS = {
  "call_analyzed": "*.call_analyzed.json",
  "call_ended": "*.call_ended.json",
  "summary": "*.summary.json",
  "manual_pull": "*.manual_pull.json",
}
AUTONOMOUS_REQUIRED_ARTIFACTS = (
  "AUTONOMOUS_PACKET_2026-04-29.md",
  "CONSOLIDATED_MORNING_MANAGEMENT_PACKET_2026-04-29.md",
  "CALL_CENTER_OS_COMMAND_CENTER_2026-04-29.md",
  "call-center-os-command-center-2026-04-29.json",
  "call-center-os-command-center-2026-04-29.html",
  "CAPTURE_GAP_ROOT_CAUSE_2026-04-29.md",
  "capture-gap-root-cause-shadow-2026-04-29.json",
  "REVENUE_ATTRIBUTION_GAP_REPORT_2026-04-29.md",
  "revenue-attribution-gap-shadow-2026-04-29.json",
  "revenue-attribution-gap-shadow-2026-04-29.csv",
  "PRICING_APP_SUBMISSION_READINESS_2026-04-29.md",
  "pricing-app-submission-readiness-shadow-2026-04-29.json",
  "pricing-app-submission-readiness-shadow-2026-04-29.csv",
)
AUTONOMOUS_EXTENSION_BASE_ARTIFACTS = (
  "event-replay-shadow-2026-04-29.json",
  "reconstruction-audit-shadow-2026-04-29.json",
  "contact-resolution-review-shadow-2026-04-29.json",
  "observer-capture-review-shadow-2026-04-29.json",
  "post-call-qa-continuation-shadow-2026-04-29.json",
  "capture-gap-root-cause-shadow-2026-04-29.json",
  "lo-followup-scoreboard-shadow-2026-04-29.json",
  "management-report-continuation-shadow-2026-04-29.json",
  "actual-call-cohort-report-shadow-2026-04-29.json",
  "owner-attribution-review-shadow-2026-04-29.json",
  "post-call-action-approval-shadow-2026-04-29.json",
  "rsi-recommendation-queue-continuation-shadow-2026-04-29.json",
  "last30-lead-review-queue-shadow-2026-04-29.json",
  "agent-scaffolds-shadow-2026-04-29.json",
)
ATTRIBUTION_TRACKING_FIELDS = (
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_term",
  "utm_content",
  "ad_group",
  "adgroup",
  "keyword",
  "campaign_id",
  "landing_page",
  "landingPage",
  "gclid",
  "fbclid",
)


@dataclass
class AutonomousArtifactsResult:
  files_changed: list[str]
  checks: list[str]
  summary: dict[str, Any]


def _write_text(path: Path, content: str) -> Path:
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(content, encoding="utf-8")
  return path


def _write_text_if_changed(path: Path, content: str) -> bool:
  if path.exists() and path.read_text(encoding="utf-8") == content:
    return False
  _write_text(path, content)
  return True


def _write_csv(path: Path, rows: list[Mapping[str, Any]], fieldnames: list[str]) -> Path:
  path.parent.mkdir(parents=True, exist_ok=True)
  with path.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
      writer.writerow({field: row.get(field, "") for field in fieldnames})
  return path


def _csv_content(rows: list[Mapping[str, Any]], fieldnames: list[str]) -> str:
  handle = StringIO()
  writer = csv.DictWriter(handle, fieldnames=fieldnames)
  writer.writeheader()
  for row in rows:
    writer.writerow({field: row.get(field, "") for field in fieldnames})
  return handle.getvalue()


def _write_csv_if_changed(path: Path, rows: list[Mapping[str, Any]], fieldnames: list[str]) -> bool:
  content = _csv_content(rows, fieldnames)
  if path.exists() and path.read_text(encoding="utf-8") == content:
    return False
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(content, encoding="utf-8")
  return True


def _relative_path(path: Path, repo_root: Path) -> str:
  try:
    return str(path.relative_to(repo_root))
  except ValueError:
    return str(path)


def _read_json(path: Path, default: Any) -> Any:
  if not path.exists():
    return default
  return json.loads(path.read_text(encoding="utf-8"))


def _json_content(payload: Mapping[str, Any] | list[Any]) -> str:
  return json.dumps(payload, indent=2)


def _write_json_if_changed(path: Path, payload: Mapping[str, Any] | list[Any]) -> bool:
  content = _json_content(payload)
  if path.exists() and path.read_text(encoding="utf-8") == content:
    return False
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(content, encoding="utf-8")
  return True


def _track_file_change(
  files_changed: list[str],
  repo_root: Path,
  path: Path,
  changed: bool,
) -> bool:
  if changed:
    files_changed.append(str(path.relative_to(repo_root)))
  return changed


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
    path.write_text(f"# Autonomous Progress - {AUTONOMOUS_DATE}\n\n## Timeline\n\n", encoding="utf-8")
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


def _collect_autonomous_input_files(repo_root: Path, call_dir: Path) -> list[Path]:
  files = [
    repo_root / "data" / "loan-os" / "scenarios" / "reactivation-scenario-ledger-2026-04-28.csv",
    repo_root / "data" / "loan-os" / "scenarios" / "reactivation-scenario-ledger-2026-04-28.jsonl",
    repo_root / "data" / "loan-os" / "human-review" / "human-review-queue-2026-04-28.csv",
    repo_root / "data" / "loan-os" / "post-call-review" / "post-call-review-packet-2026-04-28.csv",
    repo_root / "data" / "voice-agent" / "reactivation-enrichment" / "launch-batch-2026-04-28.post-call-scoreboard.csv",
    repo_root / "data" / "voice-agent" / "reactivation-enrichment" / "launch-batch-2026-04-28-last30.scoreboard.json",
    repo_root / "data" / "ghl-users.json",
    repo_root / "data" / "ghl-calls" / "all-calls.json",
  ]
  for pattern in RAW_CALL_SOURCE_PATTERNS.values():
    files.extend(sorted(call_dir.glob(pattern)))
  files.extend(sorted((repo_root / "data" / "voice-agent" / "call-analysis").glob("*.json")))
  return [path for path in files if path.exists()]


def _fingerprint_inputs(repo_root: Path, input_files: list[Path]) -> dict[str, Any]:
  digest = hashlib.sha1()
  normalized_files: list[dict[str, Any]] = []
  for path in sorted(input_files, key=lambda item: _relative_path(item, repo_root)):
    stat = path.stat()
    relative = _relative_path(path, repo_root)
    digest.update(relative.encode("utf-8"))
    digest.update(str(stat.st_size).encode("utf-8"))
    digest.update(str(stat.st_mtime_ns).encode("utf-8"))
    normalized_files.append(
      {
        "path": relative,
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
      }
    )
  return {
    "fingerprint": digest.hexdigest(),
    "file_count": len(normalized_files),
    "files": normalized_files,
  }


def _autonomous_manifest_path(out_dir: Path) -> Path:
  return out_dir / "autonomous-build-manifest-2026-04-29.json"


def _autonomous_required_artifacts(out_dir: Path) -> list[Path]:
  return [out_dir / name for name in AUTONOMOUS_REQUIRED_ARTIFACTS]


def _autonomous_extension_base_artifacts(out_dir: Path) -> list[Path]:
  return [out_dir / name for name in AUTONOMOUS_EXTENSION_BASE_ARTIFACTS]


def _pricing_app_submission_artifact_paths(out_dir: Path) -> tuple[Path, Path, Path]:
  return (
    out_dir / "pricing-app-submission-readiness-shadow-2026-04-29.json",
    out_dir / "pricing-app-submission-readiness-shadow-2026-04-29.csv",
    out_dir / "PRICING_APP_SUBMISSION_READINESS_2026-04-29.md",
  )


def _should_skip_autonomous_build(
  repo_root: Path,
  out_dir: Path,
  fingerprint_payload: Mapping[str, Any],
) -> tuple[bool, str, dict[str, Any]]:
  manifest_path = _autonomous_manifest_path(out_dir)
  manifest = _read_json(manifest_path, {})
  if not isinstance(manifest, Mapping):
    manifest = {}
  required_paths = _autonomous_required_artifacts(out_dir)
  missing_artifacts = [_relative_path(path, repo_root) for path in required_paths if not path.exists()]
  previous_fingerprint = str(manifest.get("input_fingerprint") or "")
  previous_build_version = str(manifest.get("build_version") or "")
  current_fingerprint = str(fingerprint_payload.get("fingerprint") or "")
  previous_summary = manifest.get("summary")
  if (
    previous_build_version == AUTONOMOUS_BUILD_VERSION
    and previous_fingerprint
    and previous_fingerprint == current_fingerprint
    and not missing_artifacts
    and isinstance(previous_summary, Mapping)
  ):
    return True, "unchanged_inputs_and_required_artifacts_present", dict(manifest)

  reasons: list[str] = []
  if previous_build_version != AUTONOMOUS_BUILD_VERSION:
    reasons.append(f"build_version {previous_build_version or 'none'} -> {AUTONOMOUS_BUILD_VERSION}")
  if previous_fingerprint != current_fingerprint:
    reasons.append("source_inputs_changed")
  if missing_artifacts:
    reasons.append(f"missing_artifacts={len(missing_artifacts)}")
  return False, "; ".join(reasons) if reasons else "initial_build", dict(manifest)


def _can_extend_from_existing_shadow_bundle(
  out_dir: Path,
  fingerprint_payload: Mapping[str, Any],
  existing_manifest: Mapping[str, Any],
) -> bool:
  previous_fingerprint = str(existing_manifest.get("input_fingerprint") or "")
  current_fingerprint = str(fingerprint_payload.get("fingerprint") or "")
  previous_summary = existing_manifest.get("summary")
  if not previous_fingerprint or previous_fingerprint != current_fingerprint:
    return False
  if not isinstance(previous_summary, Mapping):
    return False
  return all(path.exists() for path in _autonomous_extension_base_artifacts(out_dir))


def _write_autonomous_manifest(
  path: Path,
  fingerprint_payload: Mapping[str, Any],
  summary: Mapping[str, Any],
  files_changed: list[str],
  artifact_decisions: Mapping[str, Any] | None = None,
) -> Path:
  payload = {
    "build_version": AUTONOMOUS_BUILD_VERSION,
    "generated_at": datetime.now().isoformat(timespec="seconds"),
    "input_fingerprint": str(fingerprint_payload.get("fingerprint") or ""),
    "input_file_count": int(fingerprint_payload.get("file_count") or 0),
    "required_artifacts": list(AUTONOMOUS_REQUIRED_ARTIFACTS),
    "summary": dict(summary),
    "files_changed": list(files_changed),
    "artifact_decisions": dict(artifact_decisions or {}),
  }
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
  return path


def _normalize_first_name(value: str | None) -> str:
  raw = str(value or "").strip().lower()
  if not raw:
    return ""
  return raw.split(" ", 1)[0].strip(".,!?\"'")


def _call_from_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
  call = payload.get("call")
  return call if isinstance(call, Mapping) else payload


def _build_contact_resolution_index(*row_groups: list[dict[str, str]]) -> dict[str, Any]:
  contacts: dict[str, dict[str, Any]] = {}
  by_phone: dict[str, list[dict[str, Any]]] = defaultdict(list)
  for rows in row_groups:
    for row in rows:
      contact_id = str(row.get("contact_id") or "").strip()
      if not contact_id:
        continue
      first_name = _normalize_first_name(
        str(row.get("first_name") or row.get("borrower_name") or row.get("name") or "")
      )
      estimated_amount = as_int(row.get("largest_amount") or row.get("estimated_largest_amount") or row.get("estimated_amount"))
      candidate = contacts.setdefault(
        contact_id,
        {
          "contact_id": contact_id,
          "first_name": first_name,
          "estimated_amount": estimated_amount,
          "phones": set(),
          "sources": set(),
        },
      )
      if first_name and not candidate["first_name"]:
        candidate["first_name"] = first_name
      if estimated_amount > int(candidate.get("estimated_amount") or 0):
        candidate["estimated_amount"] = estimated_amount
      phone_digits = normalize_digits(str(row.get("phone") or row.get("phone_number") or ""))
      if phone_digits:
        candidate["phones"].add(phone_digits)
      candidate["sources"].add(str(row.get("scenario_id") or row.get("score_evidence") or row.get("source_artifact") or "seed"))
  for candidate in contacts.values():
    for phone_digits in sorted(candidate["phones"]):
      by_phone[phone_digits].append(candidate)
  return {
    "contacts": contacts,
    "by_phone": by_phone,
  }


def _resolve_contact_identity(
  row: Mapping[str, Any],
  features: Mapping[str, Any],
  contact_index: Mapping[str, Any],
) -> dict[str, Any]:
  source_contact_id = str(row.get("contact_id") or features.get("contact_id") or "").strip()
  if source_contact_id:
    return {
      "source_contact_id": source_contact_id,
      "resolved_contact_id": source_contact_id,
      "resolution_status": "source_contact_id",
      "resolution_confidence": "high",
      "matched_phone_redacted": "",
      "candidate_contact_ids": [source_contact_id],
      "candidate_count": 1,
      "evidence_refs": [],
    }

  phone_candidates = [str(item) for item in features.get("phone_candidates") or [] if str(item).strip()]
  direction = str(features.get("direction") or "").strip().lower()
  prioritized_numbers = phone_candidates[:]
  if direction == "inbound" and phone_candidates:
    prioritized_numbers = phone_candidates[:]
  elif direction == "outbound" and len(phone_candidates) >= 2:
    prioritized_numbers = [phone_candidates[1], phone_candidates[0], *phone_candidates[2:]]

  first_name_hint = _normalize_first_name(str(features.get("first_name_hint") or ""))
  by_phone = contact_index.get("by_phone") if isinstance(contact_index.get("by_phone"), Mapping) else {}
  evidence_refs: list[str] = []
  for phone_digits in prioritized_numbers:
    matches = by_phone.get(phone_digits, []) if isinstance(by_phone, Mapping) else []
    unique_ids = sorted({str(item.get("contact_id") or "") for item in matches if str(item.get("contact_id") or "")})
    if not unique_ids:
      continue
    evidence_refs.append(f"phone_match:{redact_phone(phone_digits)} -> {len(unique_ids)} candidate(s)")
    if len(unique_ids) == 1:
      candidate = next(item for item in matches if str(item.get("contact_id") or "") == unique_ids[0])
      candidate_first_name = _normalize_first_name(str(candidate.get("first_name") or ""))
      matched_name = bool(first_name_hint and candidate_first_name and candidate_first_name == first_name_hint)
      return {
        "source_contact_id": "",
        "resolved_contact_id": unique_ids[0],
        "resolution_status": "resolved_by_phone_unique_name" if matched_name else "resolved_by_phone_unique",
        "resolution_confidence": "high" if matched_name else "medium",
        "matched_phone_redacted": redact_phone(phone_digits),
        "candidate_contact_ids": unique_ids,
        "candidate_count": 1,
        "evidence_refs": evidence_refs,
      }
    if first_name_hint:
      named_matches = [
        item
        for item in matches
        if _normalize_first_name(str(item.get("first_name") or "")) == first_name_hint and str(item.get("contact_id") or "")
      ]
      unique_named_ids = sorted({str(item.get("contact_id") or "") for item in named_matches})
      if len(unique_named_ids) == 1:
        return {
          "source_contact_id": "",
          "resolved_contact_id": unique_named_ids[0],
          "resolution_status": "resolved_by_phone_name",
          "resolution_confidence": "medium",
          "matched_phone_redacted": redact_phone(phone_digits),
          "candidate_contact_ids": unique_named_ids,
          "candidate_count": len(unique_ids),
          "evidence_refs": evidence_refs,
        }
      return {
        "source_contact_id": "",
        "resolved_contact_id": "",
        "resolution_status": "ambiguous_phone_match",
        "resolution_confidence": "low",
        "matched_phone_redacted": redact_phone(phone_digits),
        "candidate_contact_ids": unique_ids,
        "candidate_count": len(unique_ids),
        "evidence_refs": evidence_refs,
      }

  return {
    "source_contact_id": "",
    "resolved_contact_id": "",
    "resolution_status": "unresolved_no_contact_match",
    "resolution_confidence": "low",
    "matched_phone_redacted": redact_phone(prioritized_numbers[0]) if prioritized_numbers else "",
    "candidate_contact_ids": [],
    "candidate_count": 0,
    "evidence_refs": evidence_refs,
  }


def _owner_confidence(owner_source: str) -> str:
  if owner_source.startswith("transcript_evidence"):
    return "high"
  if owner_source.startswith("ghl_call_assignment") or owner_source.startswith("ghl_assignment"):
    return "medium"
  if owner_source == "owner_hint":
    return "medium"
  if owner_source == "unassigned_review":
    return "low"
  return "medium"


def _actual_owner(owner: str, owner_hint: str) -> str:
  return owner if owner and owner != "Unassigned LO Review" else (owner_hint or "Unassigned LO Review")


def _canonical_owner(owner: str) -> str:
  return normalize_owner_name(owner) or "Unassigned LO Review"


def _dedupe_text(values: list[str]) -> list[str]:
  output: list[str] = []
  seen: set[str] = set()
  for value in values:
    text = str(value or "").strip()
    if not text or text in seen:
      continue
    seen.add(text)
    output.append(text)
  return output


def _merge_gap_reasons(*groups: list[str]) -> list[str]:
  output: list[str] = []
  seen: set[str] = set()
  for group in groups:
    for value in group:
      text = str(value or "").strip()
      if not text or text in seen:
        continue
      seen.add(text)
      output.append(text)
  return output


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
  if not path.exists():
    return []
  rows: list[dict[str, Any]] = []
  for raw_line in path.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line:
      continue
    try:
      payload = json.loads(line)
    except json.JSONDecodeError:
      continue
    if isinstance(payload, dict):
      rows.append(payload)
  return rows


def _parse_transcript_sources(value: Any) -> list[dict[str, Any]]:
  if isinstance(value, list):
    return [item for item in value if isinstance(item, dict)]
  text = str(value or "").strip()
  if not text:
    return []
  try:
    payload = json.loads(text)
  except json.JSONDecodeError:
    return []
  if isinstance(payload, list):
    return [item for item in payload if isinstance(item, dict)]
  return []


def _safe_int(value: Any) -> int:
  try:
    return int(float(str(value or "0").replace("$", "").replace(",", "")))
  except ValueError:
    return 0


def _last30_missing_facts(lead: Mapping[str, Any], scenario: Mapping[str, Any]) -> str:
  scenario_facts = scenario.get("facts") if isinstance(scenario.get("facts"), Mapping) else {}
  missing: list[str] = []
  goal = str(scenario_facts.get("goal") or scenario.get("goal") or "").strip()
  if not goal:
    missing.append("goal")
  property_type = str(scenario_facts.get("property_type") or scenario.get("property_type") or "").strip()
  if not property_type:
    missing.append("property")
  credit_score = _safe_int(scenario_facts.get("credit_score") or scenario.get("credit_score"))
  if credit_score <= 0:
    missing.append("credit")
  amount = _safe_int(
    scenario_facts.get("largest_amount")
    or scenario.get("largest_amount")
    or lead.get("estimated_largest_amount")
  )
  if amount <= 0:
    missing.append("amount")
  return ", ".join(missing)


def _opener_risk(lead: Mapping[str, Any]) -> str:
  opener = str(lead.get("miner_style_opener") or "").strip()
  context = str(lead.get("opening_context_line") or "").strip()
  if opener and len(opener.split()) >= 45:
    return "long opener"
  if not context:
    return "weak context"
  return "ok"


def _priority_tier_rank(value: str) -> int:
  return {
    "A": 4,
    "B": 3,
    "C": 2,
    "D": 1,
  }.get(str(value or "").strip().upper(), 0)


def _render_last30_review_markdown(summary: Mapping[str, Any], rows: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Last-30-Day Lead Review Queue - 2026-04-29",
    "",
    "Shadow-only. No calls, GHL writes, borrower messages, or status changes were triggered.",
    "",
    "## Summary",
    "",
    f"- total_last30: {summary.get('total_last30', 0)}",
    f"- tier_counts: {summary.get('tier_counts', {})}",
    f"- source_counts: {summary.get('source_counts', {})}",
    f"- call_early_count: {summary.get('call_early_count', 0)}",
    f"- enrich_before_call_count: {summary.get('enrich_before_call_count', 0)}",
    f"- transcript_backed_count: {summary.get('transcript_backed_count', 0)}",
    f"- top_50_estimated_amount: {summary.get('top_50_estimated_amount', 0)}",
    "",
    "## Top 25 Review Rows",
    "",
    "| Tier | LO Score | Ready | Profit | Amount | Name | Source | Lane | Context | Missing |",
    "|---|---:|---:|---:|---:|---|---|---|---|---|",
  ]
  for row in rows[:25]:
    lines.append(
      "| "
      + " | ".join(
        [
          str(row.get("priority_tier") or ""),
          str(row.get("lo_priority_score") or ""),
          str(row.get("readiness_score") or ""),
          str(row.get("profitability_score") or ""),
          str(row.get("estimated_largest_amount") or ""),
          str(row.get("first_name") or ""),
          str(row.get("source_category") or ""),
          str(row.get("review_lane") or ""),
          str(row.get("opening_context_line") or ""),
          str(row.get("missing_facts") or ""),
        ]
      )
      + " |"
    )
  return "\n".join(lines).rstrip() + "\n"


def _summarize_last30_rows(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
  return {
    "total_last30": len(rows),
    "tier_counts": dict(Counter(str(row.get("priority_tier") or "") for row in rows)),
    "source_counts": dict(Counter(str(row.get("source_category") or "") for row in rows)),
    "call_early_count": sum(1 for row in rows if str(row.get("review_lane") or "") == "call_early"),
    "enrich_before_call_count": sum(1 for row in rows if str(row.get("review_lane") or "") == "enrich_before_call"),
    "transcript_backed_count": sum(1 for row in rows if str(row.get("source_category") or "") != "form_only"),
    "top_50_estimated_amount": sum(_safe_int(row.get("estimated_largest_amount")) for row in rows[:50]),
  }


def _build_last30_review_queue(
  repo_root: Path,
  out_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
  queue_json = out_dir / "last30-lead-review-queue-shadow-2026-04-29.json"
  queue_md = out_dir / "LAST30_LEAD_REVIEW_QUEUE_SHADOW_2026-04-29.md"
  if queue_json.exists():
    payload = _read_json(queue_json, {})
    summary = payload.get("summary") if isinstance(payload, Mapping) else {}
    rows = payload.get("rows") if isinstance(payload, Mapping) else []
    rows = rows if isinstance(rows, list) else []
    summary = summary if isinstance(summary, Mapping) else {}
    normalized_rows = [dict(row) for row in rows if isinstance(row, Mapping)]
    normalized_summary = _summarize_last30_rows(normalized_rows) if normalized_rows else dict(summary)
    refresh_files: list[str] = []
    if normalized_rows and (dict(summary) != normalized_summary or not queue_md.exists()):
      write_json(queue_json, {"summary": normalized_summary, "rows": normalized_rows})
      _write_text(queue_md, _render_last30_review_markdown(normalized_summary, normalized_rows))
      refresh_files = [str(queue_json.relative_to(repo_root)), str(queue_md.relative_to(repo_root))]
    return normalized_summary, normalized_rows, refresh_files

  scoreboard_path = repo_root / "data" / "voice-agent" / "reactivation-enrichment" / "launch-batch-2026-04-28-last30.scoreboard.json"
  if not scoreboard_path.exists():
    return {}, [], []

  payload = _read_json(scoreboard_path, {})
  leads = payload.get("leads") if isinstance(payload, Mapping) else []
  if not isinstance(leads, list):
    return {}, [], []

  scenario_lookup = {
    str(row.get("contact_id") or ""): row
    for row in _read_jsonl(repo_root / "data" / "loan-os" / "scenarios" / "reactivation-scenario-ledger-2026-04-28.jsonl")
    if str(row.get("contact_id") or "")
  }
  rows: list[dict[str, Any]] = []
  for lead in leads:
    if not isinstance(lead, Mapping):
      continue
    contact_id = str(lead.get("contact_id") or "").strip()
    if not contact_id:
      continue
    transcript_sources = _parse_transcript_sources(lead.get("transcript_sources"))
    transcript_count = len(transcript_sources)
    transcript_total_seconds = sum(_safe_int(item.get("duration_seconds")) for item in transcript_sources)
    source_category = str(lead.get("enrichment_source") or "").strip() or ("ghl_transcript" if transcript_count else "form_only")
    scenario = scenario_lookup.get(contact_id, {})
    rows.append(
      {
        "priority_tier": str(lead.get("priority_tier") or "D"),
        "lo_priority_score": str(_safe_int(lead.get("lo_priority_score"))),
        "readiness_score": str(_safe_int(lead.get("readiness_score"))),
        "profitability_score": str(_safe_int(lead.get("profitability_score"))),
        "estimated_largest_amount": str(_safe_int(lead.get("estimated_largest_amount"))),
        "first_name": str(lead.get("first_name") or ""),
        "phone": str(lead.get("phone") or ""),
        "source_category": source_category,
        "transcript_count": str(transcript_count),
        "transcript_total_seconds": str(transcript_total_seconds),
        "review_lane": "call_early" if str(lead.get("priority_tier") or "") in {"A", "B"} else "nurture_or_later",
        "opening_context_line": str(lead.get("opening_context_line") or ""),
        "recommended_first_question": str(lead.get("recommended_first_question") or ""),
        "missing_facts": _last30_missing_facts(lead, scenario),
        "opener_risk": _opener_risk(lead),
        "contact_id": contact_id,
      }
    )
  rows.sort(
    key=lambda row: (
      _priority_tier_rank(str(row.get("priority_tier") or "")),
      _safe_int(row.get("lo_priority_score")),
      _safe_int(row.get("estimated_largest_amount")),
    ),
    reverse=True,
  )
  summary = _summarize_last30_rows(rows)
  write_json(queue_json, {"summary": summary, "rows": rows})
  _write_text(queue_md, _render_last30_review_markdown(summary, rows))
  return summary, rows, [str(queue_json.relative_to(repo_root)), str(queue_md.relative_to(repo_root))]


def _first_nonempty(*values: Any, default: str = "") -> str:
  for value in values:
    text = str(value or "").strip()
    if text:
      return text
  return default


def _tracking_key_and_type(*pairs: tuple[str, Any]) -> tuple[str, str]:
  for tracking_type, value in pairs:
    text = str(value or "").strip()
    if text and text.lower() != "unknown":
      return text, tracking_type
  fallback_type = pairs[-1][0] if pairs else "unknown"
  return "unknown", fallback_type


def _tracking_fields_present(row: Mapping[str, Any]) -> list[str]:
  present: list[str] = []
  for field in ATTRIBUTION_TRACKING_FIELDS:
    if str(row.get(field) or "").strip():
      present.append(field)
  return present


def _sorted_issue_counts(rows: list[Mapping[str, Any]]) -> dict[str, int]:
  counts: Counter[str] = Counter()
  for row in rows:
    for issue in row.get("gap_reasons") or []:
      counts[str(issue)] += 1
  return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _build_revenue_attribution_gap_report(
  qa_rows: list[Mapping[str, Any]],
  followup_queue: list[Mapping[str, Any]],
  last30_seed_rows: list[Mapping[str, Any]],
  last30_rows: list[Mapping[str, Any]],
  call_payload_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
  followup_by_call = {str(row.get("call_id") or ""): row for row in followup_queue if str(row.get("call_id") or "")}
  last30_by_contact = {str(row.get("contact_id") or ""): row for row in last30_seed_rows if str(row.get("contact_id") or "")}
  last30_review_by_contact = {str(row.get("contact_id") or ""): row for row in last30_rows if str(row.get("contact_id") or "")}
  actual_by_contact: dict[str, list[Mapping[str, Any]]] = defaultdict(list)

  rows: list[dict[str, Any]] = []
  for qa_row in qa_rows:
    call_id = str(qa_row.get("call_id") or "")
    payload = call_payload_by_id.get(call_id, {})
    call = _call_from_payload(payload)
    metadata = call.get("metadata") if isinstance(call.get("metadata"), Mapping) else {}
    contact_id = str(qa_row.get("actual_contact_id") or qa_row.get("contact_id") or metadata.get("contact_id") or metadata.get("ghl_contact_id") or "")
    tracking_key, tracking_type = _tracking_key_and_type(
      ("safe_batch_tag", metadata.get("safe_batch_tag")),
      ("campaign_context", qa_row.get("campaign_context")),
      ("project", metadata.get("project")),
      ("purpose", metadata.get("purpose")),
      ("fallback", "unknown"),
    )
    present_tracking_fields = _tracking_fields_present(metadata)
    linked_last30 = bool(contact_id and contact_id in last30_by_contact)
    gap_reasons = _dedupe_text(
      [
        "missing_contact_id" if not contact_id else "",
        "missing_safe_batch_tag" if not str(metadata.get("safe_batch_tag") or "").strip() else "",
        "unknown_campaign_context" if str(qa_row.get("campaign_context") or "").strip().lower() in {"", "unknown"} else "",
        "missing_project" if not str(metadata.get("project") or "").strip() else "",
        "missing_purpose" if not str(metadata.get("purpose") or "").strip() else "",
        "missing_ad_tracking_fields" if not present_tracking_fields else "",
        "no_last30_source_link" if contact_id and not linked_last30 else "",
      ]
    )
    row = {
      "row_type": "actual_shadow_call",
      "row_id": call_id,
      "call_id": call_id,
      "contact_id": contact_id,
      "first_name": str(qa_row.get("first_name") or ""),
      "owner": str(qa_row.get("owner") or "Unassigned LO Review"),
      "estimated_amount": as_int(qa_row.get("estimated_amount")),
      "tracking_key": tracking_key,
      "tracking_type": tracking_type,
      "safe_batch_tag": str(metadata.get("safe_batch_tag") or ""),
      "project": str(metadata.get("project") or ""),
      "purpose": str(metadata.get("purpose") or ""),
      "enrichment_source": "",
      "outcome": str(qa_row.get("outcome") or ""),
      "follow_up_urgency": str(followup_by_call.get(call_id, {}).get("follow_up_urgency") or ""),
      "priority_tier": "",
      "review_lane": "",
      "linked_shadow_call_count": 1,
      "linked_last30_lead_count": 1 if linked_last30 else 0,
      "ad_tracking_field_count": len(present_tracking_fields),
      "ad_tracking_fields_present": ",".join(present_tracking_fields),
      "gap_reasons": gap_reasons,
    }
    rows.append(row)
    if contact_id:
      actual_by_contact[contact_id].append(row)

  for lead in last30_seed_rows:
    contact_id = str(lead.get("contact_id") or "")
    review_row = last30_review_by_contact.get(contact_id, {})
    tracking_key, tracking_type = _tracking_key_and_type(
      ("safe_batch_tag", lead.get("safe_batch_tag")),
      ("enrichment_source", lead.get("enrichment_source")),
      ("fallback", "unknown"),
    )
    present_tracking_fields = _tracking_fields_present(lead)
    linked_actual = bool(contact_id and contact_id in actual_by_contact)
    gap_reasons = _dedupe_text(
      [
        "missing_contact_id" if not contact_id else "",
        "missing_safe_batch_tag" if not str(lead.get("safe_batch_tag") or "").strip() else "",
        "missing_ad_tracking_fields" if not present_tracking_fields else "",
        "form_only_enrichment" if str(lead.get("enrichment_source") or "").strip() == "form_only" else "",
        "no_actual_shadow_call" if contact_id and not linked_actual else "",
      ]
    )
    rows.append(
      {
        "row_type": "last30_lead",
        "row_id": contact_id,
        "call_id": "",
        "contact_id": contact_id,
        "first_name": str(lead.get("first_name") or review_row.get("first_name") or ""),
        "owner": str(review_row.get("owner") or "Unassigned LO Review"),
        "estimated_amount": as_int(lead.get("estimated_largest_amount")),
        "tracking_key": tracking_key,
        "tracking_type": tracking_type,
        "safe_batch_tag": str(lead.get("safe_batch_tag") or ""),
        "project": "",
        "purpose": "",
        "enrichment_source": str(lead.get("enrichment_source") or ""),
        "outcome": "",
        "follow_up_urgency": "",
        "priority_tier": str(lead.get("priority_tier") or review_row.get("priority_tier") or ""),
        "review_lane": str(review_row.get("review_lane") or ""),
        "linked_shadow_call_count": len(actual_by_contact.get(contact_id, [])),
        "linked_last30_lead_count": 1,
        "ad_tracking_field_count": len(present_tracking_fields),
        "ad_tracking_fields_present": ",".join(present_tracking_fields),
        "gap_reasons": gap_reasons,
      }
    )

  cohort_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  for row in rows:
    cohort_groups[str(row.get("tracking_key") or "unknown")].append(row)

  tracking_cohorts: list[dict[str, Any]] = []
  for tracking_key, tracking_rows in cohort_groups.items():
    tracking_type = Counter(str(row.get("tracking_type") or "") for row in tracking_rows).most_common(1)[0][0]
    tracking_cohorts.append(
      {
        "tracking_key": tracking_key,
        "tracking_type": tracking_type,
        "row_count": len(tracking_rows),
        "actual_call_count": sum(1 for row in tracking_rows if row.get("row_type") == "actual_shadow_call"),
        "last30_lead_count": sum(1 for row in tracking_rows if row.get("row_type") == "last30_lead"),
        "linked_actual_call_count": sum(
          1 for row in tracking_rows if row.get("row_type") == "actual_shadow_call" and int(row.get("linked_last30_lead_count") or 0) > 0
        ),
        "linked_last30_lead_count": sum(
          1 for row in tracking_rows if row.get("row_type") == "last30_lead" and int(row.get("linked_shadow_call_count") or 0) > 0
        ),
        "same_day_call_count": sum(
          1 for row in tracking_rows if row.get("row_type") == "actual_shadow_call" and str(row.get("follow_up_urgency") or "") == "same_day"
        ),
        "call_early_lead_count": sum(
          1 for row in tracking_rows if row.get("row_type") == "last30_lead" and str(row.get("review_lane") or "") == "call_early"
        ),
        "estimated_amount_sum": sum(as_int(row.get("estimated_amount")) for row in tracking_rows),
        "gap_row_count": sum(1 for row in tracking_rows if row.get("gap_reasons")),
      }
    )
  tracking_cohorts.sort(
    key=lambda item: (int(item.get("estimated_amount_sum") or 0), int(item.get("row_count") or 0)),
    reverse=True,
  )

  priority_gap_queue = [dict(row) for row in rows if row.get("gap_reasons")]
  major_gap_reasons = {
    "missing_contact_id",
    "missing_safe_batch_tag",
    "unknown_campaign_context",
    "missing_project",
    "missing_purpose",
    "no_last30_source_link",
    "no_actual_shadow_call",
    "form_only_enrichment",
  }
  priority_gap_queue.sort(
    key=lambda row: (
      1 if any(issue in major_gap_reasons for issue in row.get("gap_reasons") or []) else 0,
      as_int(row.get("estimated_amount")),
      1 if row.get("row_type") == "actual_shadow_call" else 0,
      len(list(row.get("gap_reasons") or [])),
    ),
    reverse=True,
  )
  summary = {
    "tracking_key_count": len(tracking_cohorts),
    "actual_call_row_count": sum(1 for row in rows if row.get("row_type") == "actual_shadow_call"),
    "actual_call_linked_last30_count": sum(
      1 for row in rows if row.get("row_type") == "actual_shadow_call" and int(row.get("linked_last30_lead_count") or 0) > 0
    ),
    "actual_call_missing_tracking_count": sum(
      1
      for row in rows
      if row.get("row_type") == "actual_shadow_call"
      and any(issue in set(row.get("gap_reasons") or []) for issue in {"missing_safe_batch_tag", "unknown_campaign_context", "missing_project", "missing_purpose"})
    ),
    "actual_call_missing_ad_tracking_count": sum(
      1 for row in rows if row.get("row_type") == "actual_shadow_call" and "missing_ad_tracking_fields" in set(row.get("gap_reasons") or [])
    ),
    "last30_lead_row_count": sum(1 for row in rows if row.get("row_type") == "last30_lead"),
    "last30_linked_call_count": sum(
      1 for row in rows if row.get("row_type") == "last30_lead" and int(row.get("linked_shadow_call_count") or 0) > 0
    ),
    "last30_missing_tracking_count": sum(
      1 for row in rows if row.get("row_type") == "last30_lead" and "missing_safe_batch_tag" in set(row.get("gap_reasons") or [])
    ),
    "last30_missing_ad_tracking_count": sum(
      1 for row in rows if row.get("row_type") == "last30_lead" and "missing_ad_tracking_fields" in set(row.get("gap_reasons") or [])
    ),
    "gap_row_count": len(priority_gap_queue),
    "high_value_gap_count": sum(1 for row in priority_gap_queue if as_int(row.get("estimated_amount")) >= 500000),
    "unattributed_estimated_amount_sum": sum(
      as_int(row.get("estimated_amount"))
      for row in priority_gap_queue
      if any(issue in set(row.get("gap_reasons") or []) for issue in {"missing_safe_batch_tag", "unknown_campaign_context", "no_last30_source_link", "no_actual_shadow_call"})
    ),
    "top_tracking_key_by_amount": tracking_cohorts[0]["tracking_key"] if tracking_cohorts else "unknown",
    "gap_reason_counts": _sorted_issue_counts(priority_gap_queue),
    "action_list": [
      "Persist safe_batch_tag, project, and purpose together on every outbound shadow call so campaign and operating-lane attribution stay linkable.",
      "Backfill ad-level tracking fields into the last-30 lead seed before promotion; current local shadow artifacts do not expose reliable UTM or keyword coverage.",
      "Use contact-linked actual-call rows first for management revenue attribution, then quarantine high-value unlinked rows for manual source reconciliation.",
    ],
  }
  return {
    "summary": summary,
    "tracking_cohorts": tracking_cohorts,
    "priority_gap_queue": priority_gap_queue[:50],
    "rows": rows,
  }


def _render_revenue_attribution_gap_markdown(report: Mapping[str, Any]) -> str:
  summary = report.get("summary", {}) if isinstance(report.get("summary"), Mapping) else {}
  tracking_cohorts = report.get("tracking_cohorts", []) if isinstance(report.get("tracking_cohorts"), list) else []
  priority_gap_queue = report.get("priority_gap_queue", []) if isinstance(report.get("priority_gap_queue"), list) else []
  lines = [
    "# Revenue Attribution Gap Report - 2026-04-29",
    "",
    "Shadow-only source/campaign attribution scaffold combining actual shadow calls and the last-30 lead queue without exposing raw phone or email PII.",
    "",
    "## Summary",
    "",
    f"- Tracking keys: {summary.get('tracking_key_count', 0)}",
    f"- Actual shadow calls: {summary.get('actual_call_row_count', 0)}",
    f"- Actual calls linked to last-30 leads: {summary.get('actual_call_linked_last30_count', 0)}",
    f"- Actual calls missing campaign/project/purpose coverage: {summary.get('actual_call_missing_tracking_count', 0)}",
    f"- Actual calls missing ad-level tracking fields: {summary.get('actual_call_missing_ad_tracking_count', 0)}",
    f"- Last-30 leads: {summary.get('last30_lead_row_count', 0)}",
    f"- Last-30 leads linked to actual calls: {summary.get('last30_linked_call_count', 0)}",
    f"- Last-30 leads missing safe batch tags: {summary.get('last30_missing_tracking_count', 0)}",
    f"- Last-30 leads missing ad-level tracking fields: {summary.get('last30_missing_ad_tracking_count', 0)}",
    f"- Priority gap rows: {summary.get('gap_row_count', 0)}",
    f"- High-value gap rows: {summary.get('high_value_gap_count', 0)}",
    f"- Unattributed estimated amount: ${int(summary.get('unattributed_estimated_amount_sum', 0)):,}",
    f"- Top tracking key by amount: {summary.get('top_tracking_key_by_amount', 'unknown')}",
    f"- Gap reasons: {summary.get('gap_reason_counts', {})}",
    "",
    "## Tracking Cohorts",
    "",
  ]
  for row in tracking_cohorts[:12]:
    lines.append(
      f"- {row['tracking_key']}: rows={row['row_count']} | actual_calls={row['actual_call_count']} | "
      f"last30={row['last30_lead_count']} | linked_calls={row['linked_actual_call_count']} | "
      f"call_early={row['call_early_lead_count']} | same_day={row['same_day_call_count']} | "
      f"gap_rows={row['gap_row_count']} | est_amount=${int(row['estimated_amount_sum']):,}"
    )
  lines.extend(["", "## Priority Gap Queue", ""])
  for row in priority_gap_queue[:25]:
    lines.append(
      f"- {row['row_type']}: {row['row_id']} | tracking={row['tracking_key']} ({row['tracking_type']}) | "
      f"owner={row['owner']} | amount=${int(row['estimated_amount']):,} | gaps={', '.join(row['gap_reasons'])}"
    )
  lines.extend(["", "## Action List", ""])
  for item in summary.get("action_list", []):
    lines.append(f"1. {item}")
  return "\n".join(lines).rstrip() + "\n"

RECONSTRUCTION_FIELD_WEIGHTS = {
  "contact": 20,
  "owner": 15,
  "transcript": 20,
  "recording": 15,
  "transfer": 10,
  "appointment": 10,
  "evidence": 5,
  "confidence": 5,
}


def _owner_present(owner: str | None) -> bool:
  normalized = _canonical_owner(str(owner or ""))
  return normalized != "Unassigned LO Review"


def _contact_present(
  row: Mapping[str, Any],
  contact_state_present: bool | None = None,
) -> bool:
  if contact_state_present is not None:
    return contact_state_present
  for key in ("contact_id", "resolved_contact_id", "contact_state_id"):
    value = str(row.get(key) or "").strip()
    if value and not value.startswith("shadow_contact__"):
      return True
  return False


def _required_field_presence(
  row: Mapping[str, Any],
  contact_state_present: bool | None = None,
) -> dict[str, bool]:
  observer_capture_status = _observer_capture_status(row)
  return {
    "contact": _contact_present(row, contact_state_present),
    "owner": _owner_present(str(row.get("owner") or row.get("owner_hint") or "")),
    "transcript": bool(row.get("transcript_available") or row.get("transcript_covered")) or observer_capture_status == "not_expected_no_conversation",
    "recording": bool(row.get("recording_available") or row.get("recording_covered")) or observer_capture_status == "not_expected_no_conversation",
    "transfer": bool(str(row.get("transfer_status") or "").strip()),
    "appointment": bool(str(row.get("appointment_status") or row.get("appointment_result") or "").strip()),
    "evidence": bool(list(row.get("evidence_refs") or [])),
    "confidence": bool(
      str(
        row.get("reconstruction_confidence_label")
        or row.get("confidence_label")
        or row.get("derived_confidence_label")
        or ""
      ).strip()
    ),
  }


def _reconstruction_metrics(
  row: Mapping[str, Any],
  contact_state_present: bool | None = None,
) -> dict[str, Any]:
  field_presence = _required_field_presence(row, contact_state_present=contact_state_present)
  covered_count = sum(1 for present in field_presence.values() if present)
  total_count = len(field_presence)
  readiness_score = sum(
    weight for field, weight in RECONSTRUCTION_FIELD_WEIGHTS.items() if field_presence.get(field, False)
  )
  coverage_rate = round(covered_count / max(total_count, 1), 3)
  if readiness_score >= 85:
    readiness_band = "high"
  elif readiness_score >= 60:
    readiness_band = "medium"
  else:
    readiness_band = "low"
  return {
    "required_field_presence": field_presence,
    "required_field_covered_count": covered_count,
    "required_field_total_count": total_count,
    "required_field_coverage_rate": coverage_rate,
    "reconstruction_readiness_score": readiness_score,
    "reconstruction_readiness_band": readiness_band,
  }


def _normalized_reconstruction_gap_reasons(
  gap_reasons: list[str],
  owner: str,
  contact_present: bool = False,
  observer_capture_status: str = "complete",
) -> list[str]:
  output = _dedupe_text(gap_reasons)
  if _owner_present(owner):
    output = [reason for reason in output if reason not in {"missing_owner_hint", "owner_unresolved"}]
  if contact_present:
    output = [reason for reason in output if reason != "missing_contact_id"]
  if observer_capture_status == "not_expected_no_conversation":
    output = [reason for reason in output if reason not in {"missing_transcript", "missing_recording"}]
  return output


def _observer_capture_status(row: Mapping[str, Any]) -> str:
  call_status = str(row.get("call_status") or "").strip().lower()
  if call_status == "not_connected":
    return "not_expected_no_conversation"
  transcript_available = bool(row.get("transcript_available") or row.get("transcript_covered"))
  recording_available = bool(row.get("recording_available") or row.get("recording_covered"))
  if transcript_available and recording_available:
    return "complete"
  if transcript_available:
    return "missing_recording"
  if recording_available:
    return "missing_transcript"
  return "missing_transcript_and_recording"


def _evidence_strings(evidence_refs: list[Mapping[str, Any]] | list[str], extra: list[str] | None = None) -> list[str]:
  output: list[str] = []
  for item in evidence_refs:
    if isinstance(item, str):
      output.append(item)
      continue
    source_type = str(item.get("source_type") or "")
    source_id = str(item.get("source_id") or "")
    note = str(item.get("note") or "")
    excerpt = str(item.get("excerpt") or "")
    parts = [part for part in [f"{source_type}:{source_id}" if source_type or source_id else "", note, excerpt] if part]
    if parts:
      output.append(" | ".join(parts))
  output.extend(extra or [])
  return _dedupe_text(output)


def _explicit_outcome(features: Mapping[str, Any]) -> str:
  text = " ".join(
    str(features.get(key) or "")
    for key in [
      "transcript_excerpt",
      "call_summary_excerpt",
      "user_sentiment",
      "objection_type",
    ]
  ).lower()
  dnc_phrases = [
    "do not call",
    "don't call",
    "dont call",
    "stop calling",
    "remove me",
    "take me off",
    "take me out",
    "unsubscribe",
    "wrong number",
  ]
  not_interested_phrases = [
    "not interested",
    "no longer interested",
    "already handled",
    "all set",
    "no thanks",
    "i'm good",
    "im good",
  ]
  if any(phrase in text for phrase in dnc_phrases):
    return "do_not_call"
  if any(phrase in text for phrase in not_interested_phrases):
    return "not_interested"
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


def _explicit_transfer_status(value: str | None) -> str:
  return str(value or "").strip() or "not_attempted"


def _load_shadow_call_payloads(call_dir: Path) -> list[tuple[Path, dict[str, Any], dict[str, Any]]]:
  rows: list[tuple[Path, dict[str, Any], dict[str, Any]]] = []
  for path in sorted(call_dir.glob("*.call_analyzed.json")):
    payload = _read_json(path, {})
    if not isinstance(payload, dict):
      continue
    features = extract_shadow_call_features(payload, source_name=path.name)
    if not features.get("call_id"):
      continue
    rows.append((path, payload, features))
  return rows


def _load_shadow_call_source_index(call_dir: Path) -> dict[str, dict[str, Any]]:
  output: dict[str, dict[str, Any]] = {}
  for source_kind, pattern in RAW_CALL_SOURCE_PATTERNS.items():
    for path in sorted(call_dir.glob(pattern)):
      payload = _read_json(path, {})
      if not isinstance(payload, dict):
        continue
      call = _call_from_payload(payload)
      call_id = str(call.get("call_id") or call.get("callId") or payload.get("call_id") or "").strip()
      if not call_id:
        continue
      state = output.setdefault(
        call_id,
        {
          "raw_source_kinds": [],
          "raw_source_paths": [],
          "raw_call_statuses": [],
          "transcript_source_kinds": [],
          "recording_source_kinds": [],
        },
      )
      if source_kind not in state["raw_source_kinds"]:
        state["raw_source_kinds"].append(source_kind)
      if path.name not in state["raw_source_paths"]:
        state["raw_source_paths"].append(path.name)
      call_status = str(call.get("call_status") or payload.get("call_status") or "").strip()
      if call_status and call_status not in state["raw_call_statuses"]:
        state["raw_call_statuses"].append(call_status)
      if call.get("transcript") and source_kind not in state["transcript_source_kinds"]:
        state["transcript_source_kinds"].append(source_kind)
      if call.get("recording_url") and source_kind not in state["recording_source_kinds"]:
        state["recording_source_kinds"].append(source_kind)
  for state in output.values():
    state["raw_source_kinds"].sort()
    state["raw_source_paths"].sort()
    state["raw_call_statuses"].sort()
    state["transcript_source_kinds"].sort()
    state["recording_source_kinds"].sort()
    state["raw_source_count"] = len(state["raw_source_kinds"])
    state["transcript_source_count"] = len(state["transcript_source_kinds"])
    state["recording_source_count"] = len(state["recording_source_kinds"])
  return output


def _state_owner_row(
  call_state: Mapping[str, Any],
  scenario_row: Mapping[str, Any],
  seed_row: Mapping[str, Any],
) -> dict[str, Any]:
  return {
    "contact_id": str(call_state.get("contact_id") or ""),
    "suggested_owner": str(scenario_row.get("suggested_owner") or seed_row.get("suggested_owner") or ""),
    "owner": str(seed_row.get("owner") or scenario_row.get("owner") or ""),
    "assigned_lo": str(scenario_row.get("assigned_lo") or ""),
    "loan_officer": str(scenario_row.get("loan_officer") or ""),
  }


def _enrich_call_states(
  repo_root: Path,
  call_states: list[dict[str, Any]],
  features_by_call: Mapping[str, Mapping[str, Any]],
  source_index_by_call: Mapping[str, Mapping[str, Any]],
  scenario_lookup: Mapping[str, Mapping[str, Any]],
  seed_lookup: Mapping[str, Mapping[str, Any]],
  contact_index: Mapping[str, Any],
) -> list[dict[str, Any]]:
  transcript_owners = load_transcript_owners(repo_root)
  ghl_call_owners = load_ghl_call_owners(repo_root)
  enriched: list[dict[str, Any]] = []
  for row in call_states:
    call_id = str(row.get("call_id") or "")
    features = features_by_call.get(call_id, {})
    source_index = source_index_by_call.get(call_id, {})
    contact_resolution = _resolve_contact_identity(row, features, contact_index)
    source_contact_id = str(contact_resolution.get("source_contact_id") or "")
    contact_id = str(contact_resolution.get("resolved_contact_id") or source_contact_id or "")
    scenario = scenario_lookup.get(contact_id, {})
    seed = seed_lookup.get(contact_id, {})
    owner_state_row = _state_owner_row(row, scenario, seed)
    owner_state_row["contact_id"] = contact_id
    owner, owner_source = select_owner(owner_state_row, transcript_owners, ghl_call_owners)
    owner = _canonical_owner(_actual_owner(owner, str(row.get("owner_hint") or features.get("owner_hint") or scenario.get("suggested_owner") or "")))
    if owner_source == "unassigned_review" and owner != "Unassigned LO Review":
      owner_source = "owner_hint"
    transcript_excerpt = str(row.get("transcript_excerpt") or features.get("transcript_excerpt") or "")
    call_summary_excerpt = str(row.get("call_summary_excerpt") or features.get("call_summary_excerpt") or "")
    transfer_status = _explicit_transfer_status(
      merge_transfer_status(str(row.get("transfer_status") or ""), str(features.get("transfer_result") or ""))
    )
    appointment_status = merge_appointment_status(str(row.get("appointment_status") or "not_booked"), str(features.get("appointment_result") or "not_booked"))
    observer_capture_status = _observer_capture_status(
      {
        "call_status": str(row.get("call_status") or features.get("call_status") or ""),
        "transcript_available": bool(row.get("transcript_available") or features.get("transcript_available")),
        "recording_available": bool(row.get("recording_available") or features.get("recording_available")),
      }
    )
    reconstruction_gap_reasons = _normalized_reconstruction_gap_reasons(
      _merge_gap_reasons(
        list(row.get("reconstruction_gap_reasons") or []),
        ["owner_unresolved"] if owner == "Unassigned LO Review" else [],
      ),
      owner,
      contact_present=bool(contact_id),
      observer_capture_status=observer_capture_status,
    )
    reconstruction_confidence_label = str(row.get("reconstruction_confidence_label") or "low")
    if owner == "Unassigned LO Review" and reconstruction_confidence_label == "high":
      reconstruction_confidence_label = "medium"
    resolved_contact_state_id = contact_id or str(row.get("resolved_contact_id") or f"shadow_contact__{call_id}")
    evidence_refs = _evidence_strings(
      list(row.get("evidence_refs") or []),
      extra=[
        f"retell_call:{features.get('source_name')}" if features.get("source_name") else "",
        f"scenario:{scenario.get('scenario_id')}" if scenario.get("scenario_id") else "",
        f"score_seed:{contact_id}" if contact_id and seed else "",
      ],
    )
    enriched_row = {
      **row,
      "source_contact_id": source_contact_id,
      "contact_id": contact_id,
      "resolved_contact_id": resolved_contact_state_id,
      "contact_state_id": resolved_contact_state_id,
      "last_event_type": str(row.get("last_event_type") or row.get("latest_event_type") or ""),
      "actual_shadow_call": True,
      "owner": owner,
      "owner_source": owner_source,
      "owner_confidence": _owner_confidence(owner_source),
      "transcript_backed_owner": owner_source.startswith("transcript_evidence"),
      "transcript_excerpt": transcript_excerpt,
      "call_summary_excerpt": call_summary_excerpt,
      "transfer_status": transfer_status,
      "appointment_status": appointment_status,
      "observer_capture_status": observer_capture_status,
      "conversation_evidence_expectation": "not_expected_no_conversation" if observer_capture_status == "not_expected_no_conversation" else "expected",
      "raw_source_kinds": list(source_index.get("raw_source_kinds") or []),
      "raw_source_paths": list(source_index.get("raw_source_paths") or []),
      "raw_source_count": int(source_index.get("raw_source_count") or 0),
      "raw_call_statuses": list(source_index.get("raw_call_statuses") or []),
      "transcript_source_kinds": list(source_index.get("transcript_source_kinds") or []),
      "recording_source_kinds": list(source_index.get("recording_source_kinds") or []),
      "transcript_source_count": int(source_index.get("transcript_source_count") or 0),
      "recording_source_count": int(source_index.get("recording_source_count") or 0),
      "contact_resolution_status": str(contact_resolution.get("resolution_status") or ""),
      "contact_resolution_confidence": str(contact_resolution.get("resolution_confidence") or "low"),
      "matched_phone_redacted": str(contact_resolution.get("matched_phone_redacted") or ""),
      "candidate_contact_ids": list(contact_resolution.get("candidate_contact_ids") or []),
      "contact_resolution_review_required": str(contact_resolution.get("resolution_confidence") or "") != "high",
      "duration_seconds": int(features.get("duration_seconds") or row.get("connected_seconds") or 0),
      "prospect_words": int(features.get("prospect_words") or 0),
      "questions_answered": int(features.get("questions_answered") or 0),
      "objection_type": str(features.get("objection_type") or ""),
      "user_sentiment": str(features.get("user_sentiment") or ""),
      "recording_url_state": "present" if row.get("recording_available") else "missing",
      "derived_outcome": _explicit_outcome(features) if features else "",
      "derived_confidence_label": _confidence_hint(features) if features else "low",
      "estimated_amount": as_int(scenario.get("largest_amount") or seed.get("estimated_largest_amount") or features.get("estimated_amount")),
      "campaign_context": str(features.get("campaign_context") or scenario.get("campaign") or ""),
      "internal_test": bool(features.get("internal_test")),
      "reconstruction_gap_reasons": reconstruction_gap_reasons,
      "reconstruction_confidence_label": reconstruction_confidence_label,
      "reconstruction_status": "complete" if not reconstruction_gap_reasons else "partial",
      "evidence_refs": _dedupe_text([*evidence_refs, *list(contact_resolution.get("evidence_refs") or [])]),
    }
    enriched.append({**enriched_row, **_reconstruction_metrics(enriched_row)})
  enriched.sort(key=lambda item: (str(item.get("latest_event_at") or ""), str(item.get("call_id") or "")), reverse=True)
  return enriched


def _enrich_contact_states(
  repo_root: Path,
  contact_states: list[dict[str, Any]],
  call_states: list[Mapping[str, Any]],
  scenario_lookup: Mapping[str, Mapping[str, Any]],
  seed_lookup: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
  transcript_owners = load_transcript_owners(repo_root)
  ghl_call_owners = load_ghl_call_owners(repo_root)
  calls_by_contact: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  for row in call_states:
    calls_by_contact[str(row.get("contact_state_id") or row.get("resolved_contact_id") or row.get("contact_id") or "")].append(row)

  enriched: list[dict[str, Any]] = []
  for row in contact_states:
    resolved_contact_id = str(row.get("resolved_contact_id") or row.get("contact_id") or "")
    related_calls = sorted(
      calls_by_contact.get(resolved_contact_id, []),
      key=lambda item: (str(item.get("latest_event_at") or ""), str(item.get("call_id") or "")),
      reverse=True,
    )
    raw_contact_id = str(row.get("contact_id") or "")
    lookup_contact_id = raw_contact_id if not raw_contact_id.startswith("shadow_contact__") else ""
    scenario = scenario_lookup.get(lookup_contact_id, {})
    seed = seed_lookup.get(lookup_contact_id, {})
    owner, owner_source = select_owner(_state_owner_row(row, scenario, seed), transcript_owners, ghl_call_owners)
    if related_calls:
      latest_call = related_calls[0]
      owner = _canonical_owner(_actual_owner(owner, str(latest_call.get("owner") or row.get("owner_hint") or scenario.get("suggested_owner") or "")))
      if owner_source == "unassigned_review" and str(latest_call.get("owner_source") or ""):
        owner_source = str(latest_call.get("owner_source") or owner_source)
    else:
      owner = _canonical_owner(_actual_owner(owner, str(row.get("owner_hint") or scenario.get("suggested_owner") or "")))
      if owner_source == "unassigned_review" and owner != "Unassigned LO Review":
        owner_source = "owner_hint"
    conversation_expected = (
      any(_observer_capture_status(call) != "not_expected_no_conversation" for call in related_calls)
      if related_calls
      else _observer_capture_status(row) != "not_expected_no_conversation"
    )
    observer_capture_status = _observer_capture_status(
      {
        "call_status": "ended" if conversation_expected else "not_connected",
        "transcript_covered": any(bool(call.get("transcript_available")) for call in related_calls) or bool(row.get("transcript_covered")),
        "recording_covered": any(bool(call.get("recording_available")) for call in related_calls) or bool(row.get("recording_covered")),
      }
    )
    reconstruction_gap_reasons = _normalized_reconstruction_gap_reasons(
      _merge_gap_reasons(
        list(row.get("reconstruction_gap_reasons") or []),
        ["owner_unresolved"] if owner == "Unassigned LO Review" else [],
      ),
      owner,
      contact_present=bool(resolved_contact_id),
      observer_capture_status=observer_capture_status,
    )
    reconstruction_confidence_label = str(row.get("reconstruction_confidence_label") or "low")
    if owner == "Unassigned LO Review" and reconstruction_confidence_label == "high":
      reconstruction_confidence_label = "medium"
    evidence_refs = _evidence_strings(
      list(row.get("evidence_refs") or []),
      extra=[f"call_state:{call.get('call_id')}" for call in related_calls[:5]],
    )
    enriched_row = {
      **row,
      "contact_id": resolved_contact_id,
      "resolved_contact_id": resolved_contact_id,
      "last_event_type": str(row.get("last_event_type") or row.get("latest_event_type") or ""),
      "owner": owner,
      "owner_source": owner_source,
      "owner_confidence": _owner_confidence(owner_source),
      "transcript_backed_owner": owner_source.startswith("transcript_evidence"),
      "call_count": len(related_calls) if related_calls else int(row.get("call_count") or 0),
      "call_ids": [str(call.get("call_id") or "") for call in related_calls[:10]] or list(row.get("call_ids") or []),
      "last_call_id": str(related_calls[0].get("call_id") or row.get("last_call_id") or "") if related_calls else str(row.get("last_call_id") or ""),
      "transcript_excerpt": str(related_calls[0].get("transcript_excerpt") or row.get("transcript_excerpt") or "") if related_calls else str(row.get("transcript_excerpt") or ""),
      "call_summary_excerpt": str(related_calls[0].get("call_summary_excerpt") or row.get("call_summary_excerpt") or "") if related_calls else str(row.get("call_summary_excerpt") or ""),
      "recording_url_state": str(related_calls[0].get("recording_url_state") or row.get("recording_url_state") or "missing") if related_calls else str(row.get("recording_url_state") or "missing"),
      "transfer_status": _explicit_transfer_status(
        merge_transfer_status(
          str(row.get("transfer_status") or ""),
          str(related_calls[0].get("transfer_status") or "") if related_calls else "",
        )
      ),
      "appointment_status": merge_appointment_status(
        str(row.get("appointment_status") or "not_booked"),
        str(related_calls[0].get("appointment_status") or "not_booked") if related_calls else "not_booked",
      ),
      "actual_shadow_call_count": len(related_calls),
      "call_status": str(related_calls[0].get("call_status") or row.get("call_status") or "") if related_calls else str(row.get("call_status") or ""),
      "observer_capture_status": observer_capture_status,
      "conversation_evidence_expectation": "not_expected_no_conversation" if observer_capture_status == "not_expected_no_conversation" else "expected",
      "reconstruction_gap_reasons": reconstruction_gap_reasons,
      "reconstruction_confidence_label": reconstruction_confidence_label,
      "reconstruction_status": "complete" if not reconstruction_gap_reasons else "partial",
      "evidence_refs": evidence_refs,
    }
    enriched.append({**enriched_row, **_reconstruction_metrics(enriched_row)})
  enriched.sort(
    key=lambda item: (
      int(item.get("actual_shadow_call_count") or 0),
      bool(item.get("transcript_covered")),
      str(item.get("latest_event_at") or ""),
      str(item.get("contact_id") or ""),
    ),
    reverse=True,
  )
  return enriched


def _build_actual_shadow_contact_states(call_states: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
  grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  for row in call_states:
    grouped[str(row.get("contact_state_id") or row.get("resolved_contact_id") or row.get("contact_id") or "")].append(row)

  actual_states: list[dict[str, Any]] = []
  for contact_state_id, rows in grouped.items():
    if not contact_state_id:
      continue
    rows = sorted(rows, key=lambda item: (str(item.get("latest_event_at") or ""), str(item.get("call_id") or "")), reverse=True)
    latest = rows[0]
    conversation_expected = any(_observer_capture_status(row) != "not_expected_no_conversation" for row in rows)
    observer_capture_status = _observer_capture_status(
      {
        "call_status": "ended" if conversation_expected else "not_connected",
        "transcript_covered": any(bool(row.get("transcript_available")) for row in rows),
        "recording_covered": any(bool(row.get("recording_available")) for row in rows),
      }
    )
    actual_state = {
      "contact_id": contact_state_id,
      "resolved_contact_id": contact_state_id,
      "source_contact_ids": _dedupe_text([str(row.get("source_contact_id") or "") for row in rows]),
      "owner": str(latest.get("owner") or "Unassigned LO Review"),
      "owner_source": str(latest.get("owner_source") or ""),
      "owner_confidence": str(latest.get("owner_confidence") or "low"),
      "transcript_backed_owner": bool(latest.get("transcript_backed_owner")),
      "actual_shadow_call_count": len(rows),
      "call_ids": [str(row.get("call_id") or "") for row in rows[:10]],
      "latest_event_at": str(latest.get("latest_event_at") or ""),
      "call_status": "ended" if conversation_expected else "not_connected",
      "transcript_covered": any(bool(row.get("transcript_available")) for row in rows),
      "recording_covered": any(bool(row.get("recording_available")) for row in rows),
      "transfer_status": _explicit_transfer_status(str(latest.get("transfer_status") or "")),
      "appointment_status": str(latest.get("appointment_status") or ""),
      "estimated_amount_max": max(as_int(row.get("estimated_amount")) for row in rows),
      "reconstruction_gap_reasons": _normalized_reconstruction_gap_reasons(
        _dedupe_text([reason for row in rows for reason in list(row.get("reconstruction_gap_reasons") or [])]),
        str(latest.get("owner") or ""),
        contact_present=bool(contact_state_id),
        observer_capture_status=observer_capture_status,
      ),
      "observer_capture_status": observer_capture_status,
      "conversation_evidence_expectation": "not_expected_no_conversation" if observer_capture_status == "not_expected_no_conversation" else "expected",
      "contact_resolution_statuses": _dedupe_text([str(row.get("contact_resolution_status") or "") for row in rows]),
      "contact_resolution_review_required": any(bool(row.get("contact_resolution_review_required")) for row in rows),
      "evidence_refs": _dedupe_text([ref for row in rows for ref in list(row.get("evidence_refs") or [])])[:10],
      "reconstruction_confidence_label": str(latest.get("reconstruction_confidence_label") or "low"),
    }
    actual_states.append({**actual_state, **_reconstruction_metrics(actual_state)})
  actual_states.sort(
    key=lambda item: (
      int(item.get("actual_shadow_call_count") or 0),
      int(item.get("estimated_amount_max") or 0),
      str(item.get("latest_event_at") or ""),
    ),
    reverse=True,
  )
  return actual_states


def _build_reconstruction_review_queue(call_states: list[Mapping[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  queue: list[dict[str, Any]] = []
  for row in call_states:
    gap_reasons = [str(item) for item in row.get("reconstruction_gap_reasons") or [] if str(item).strip()]
    if not gap_reasons:
      continue
    estimated_amount = as_int(row.get("estimated_amount"))
    review_priority_score = (
      min(500, estimated_amount // 10000)
      + (120 if "missing_transcript" in gap_reasons else 0)
      + (80 if "missing_recording" in gap_reasons else 0)
      + (100 if "owner_unresolved" in gap_reasons else 0)
      + (90 if "transfer_outcome_incomplete" in gap_reasons else 0)
      + (70 if "appointment_outcome_incomplete" in gap_reasons else 0)
    )
    queue.append(
      {
        "call_id": str(row.get("call_id") or ""),
        "contact_state_id": str(row.get("contact_state_id") or row.get("resolved_contact_id") or ""),
        "owner": str(row.get("owner") or "Unassigned LO Review"),
        "owner_source": str(row.get("owner_source") or ""),
        "estimated_amount": estimated_amount,
        "reconstruction_confidence_label": str(row.get("reconstruction_confidence_label") or "low"),
        "review_priority_score": review_priority_score,
        "gap_reasons": gap_reasons,
        "transfer_status": str(row.get("transfer_status") or ""),
        "appointment_status": str(row.get("appointment_status") or ""),
        "transcript_available": bool(row.get("transcript_available")),
        "recording_available": bool(row.get("recording_available")),
        "required_field_coverage_rate": float(row.get("required_field_coverage_rate") or 0.0),
        "reconstruction_readiness_score": int(row.get("reconstruction_readiness_score") or 0),
        "reconstruction_readiness_band": str(row.get("reconstruction_readiness_band") or "low"),
        "evidence_refs": list(row.get("evidence_refs") or []),
      }
    )
  queue.sort(key=lambda item: (int(item.get("review_priority_score") or 0), int(item.get("estimated_amount") or 0)), reverse=True)
  summary = {
    "row_count": len(queue),
    "missing_transcript_count": sum(1 for row in queue if "missing_transcript" in row["gap_reasons"]),
    "missing_recording_count": sum(1 for row in queue if "missing_recording" in row["gap_reasons"]),
    "owner_unresolved_count": sum(1 for row in queue if "owner_unresolved" in row["gap_reasons"]),
    "transfer_incomplete_count": sum(1 for row in queue if "transfer_outcome_incomplete" in row["gap_reasons"]),
    "appointment_incomplete_count": sum(1 for row in queue if "appointment_outcome_incomplete" in row["gap_reasons"]),
    "avg_required_field_coverage_rate": round(
      sum(float(row.get("required_field_coverage_rate") or 0.0) for row in queue) / max(len(queue), 1),
      3,
    ),
    "avg_reconstruction_readiness_score": round(
      sum(int(row.get("reconstruction_readiness_score") or 0) for row in queue) / max(len(queue), 1),
      1,
    ),
  }
  return summary, queue


def _event_call_id(event: Any) -> str:
  payload = event.payload if isinstance(getattr(event, "payload", None), Mapping) else {}
  call_id = str(payload.get("call_id") or "")
  if call_id:
    return call_id
  source_system = str(getattr(event, "source_system", "") or "")
  source_id = str(getattr(event, "source_id", "") or "")
  if source_system == "retell" and source_id.startswith("call"):
    return source_id
  return ""


def _event_timeline_note(event: Any) -> str:
  payload = event.payload if isinstance(getattr(event, "payload", None), Mapping) else {}
  for key in (
    "call_summary_excerpt",
    "transcript_excerpt",
    "result_excerpt",
    "note_excerpt",
    "transfer_status",
    "appointment_status",
    "tool_name",
    "purpose",
  ):
    value = str(payload.get(key) or "").strip()
    if value:
      return value
  evidence_refs = getattr(event, "evidence_refs", None)
  if isinstance(evidence_refs, list):
    for ref in evidence_refs:
      if not isinstance(ref, Mapping):
        continue
      value = str(ref.get("note") or ref.get("excerpt") or "").strip()
      if value:
        return value
  return ""


def _reconstruction_review_lane(gap_reasons: list[str], row: Mapping[str, Any], contact_state_present: bool) -> str:
  if not contact_state_present or str(row.get("contact_resolution_status") or "").startswith("unresolved"):
    return "contact_resolution_review"
  if "owner_unresolved" in gap_reasons or str(row.get("owner") or "") == "Unassigned LO Review":
    return "owner_resolution_review"
  if _observer_capture_status(row) in {"missing_transcript", "missing_recording", "missing_transcript_and_recording"} or "missing_event_timeline" in gap_reasons:
    return "observer_capture_review"
  if "transfer_outcome_incomplete" in gap_reasons or "appointment_outcome_incomplete" in gap_reasons:
    return "handoff_outcome_review"
  return "ready"


def _audit_status(gap_reasons: list[str], row: Mapping[str, Any], contact_state_present: bool, timeline_count: int) -> str:
  observer_capture_status = _observer_capture_status(row)
  if not contact_state_present or timeline_count <= 0:
    return "blocked"
  if not gap_reasons and observer_capture_status in {"complete", "not_expected_no_conversation"}:
    return "ready"
  if observer_capture_status == "missing_transcript_and_recording":
    return "blocked"
  return "review_required"


def _build_reconstruction_audit(
  events: list[Any],
  call_states: list[Mapping[str, Any]],
  actual_shadow_contact_states: list[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  contact_state_lookup = {
    str(row.get("contact_id") or row.get("resolved_contact_id") or ""): row for row in actual_shadow_contact_states
  }
  events_by_call: dict[str, list[Any]] = defaultdict(list)
  for event in events:
    call_id = _event_call_id(event)
    if call_id:
      events_by_call[call_id].append(event)

  rows: list[dict[str, Any]] = []
  for row in call_states:
    call_id = str(row.get("call_id") or "")
    contact_state_id = str(row.get("contact_state_id") or row.get("resolved_contact_id") or row.get("contact_id") or "")
    contact_state = contact_state_lookup.get(contact_state_id, {})
    timeline_events = sorted(
      events_by_call.get(call_id, []),
      key=lambda item: (str(getattr(item, "occurred_at", "") or ""), str(getattr(item, "event_id", "") or "")),
    )
    base_gap_reasons = [str(item) for item in row.get("reconstruction_gap_reasons") or [] if str(item).strip()]
    if str(row.get("owner") or "") and str(row.get("owner") or "") != "Unassigned LO Review":
      base_gap_reasons = [reason for reason in base_gap_reasons if reason != "missing_owner_hint"]
    gap_reasons = _merge_gap_reasons(
      base_gap_reasons,
      ["missing_contact_state"] if not contact_state else [],
      ["missing_event_timeline"] if not timeline_events else [],
      ["missing_evidence_refs"] if not list(row.get("evidence_refs") or []) else [],
    )
    contact_state_present = bool(contact_state)
    field_presence = _required_field_presence(row, contact_state_present=contact_state_present)
    readiness_metrics = _reconstruction_metrics(row, contact_state_present=contact_state_present)
    audit_status = _audit_status(gap_reasons, row, contact_state_present, len(timeline_events))
    estimated_amount = as_int(row.get("estimated_amount"))
    review_priority_score = (
      min(500, estimated_amount // 10000)
      + (180 if audit_status == "blocked" else 90 if audit_status == "review_required" else 0)
      + (120 if "missing_transcript" in gap_reasons else 0)
      + (100 if "missing_recording" in gap_reasons else 0)
      + (120 if "owner_unresolved" in gap_reasons else 0)
      + (110 if "missing_contact_state" in gap_reasons else 0)
      + (90 if str(row.get("transfer_status") or "").startswith("failed") else 0)
    )
    rows.append(
      {
        "call_id": call_id,
        "contact_id": str(row.get("contact_id") or ""),
        "contact_state_id": contact_state_id,
        "owner": str(row.get("owner") or "Unassigned LO Review"),
        "owner_source": str(row.get("owner_source") or ""),
        "owner_confidence": str(row.get("owner_confidence") or "low"),
        "estimated_amount": estimated_amount,
        "campaign_context": str(row.get("campaign_context") or ""),
        "transfer_status": _explicit_transfer_status(str(row.get("transfer_status") or "")),
        "appointment_status": str(row.get("appointment_status") or "not_booked"),
        "reconstruction_confidence_label": str(row.get("reconstruction_confidence_label") or "low"),
        "contact_resolution_status": str(row.get("contact_resolution_status") or ""),
        "contact_resolution_confidence": str(row.get("contact_resolution_confidence") or "low"),
        "audit_status": audit_status,
        "review_lane": _reconstruction_review_lane(gap_reasons, row, contact_state_present),
        "review_priority_score": review_priority_score,
        "field_presence": {
          "call_state": True,
          "contact_state": contact_state_present,
          **field_presence,
        },
        "required_field_covered_count": readiness_metrics["required_field_covered_count"],
        "required_field_total_count": readiness_metrics["required_field_total_count"],
        "required_field_coverage_rate": readiness_metrics["required_field_coverage_rate"],
        "reconstruction_readiness_score": readiness_metrics["reconstruction_readiness_score"],
        "reconstruction_readiness_band": readiness_metrics["reconstruction_readiness_band"],
        "timeline_event_count": len(timeline_events),
        "timeline_event_types": [str(getattr(event, "event_type", "") or "") for event in timeline_events],
        "timeline": [
          {
            "occurred_at": str(getattr(event, "occurred_at", "") or ""),
            "event_type": str(getattr(event, "event_type", "") or ""),
            "source_system": str(getattr(event, "source_system", "") or ""),
            "confidence": float(getattr(event, "confidence", 0.0) or 0.0),
            "note": _event_timeline_note(event),
          }
          for event in timeline_events
        ],
        "gap_reasons": gap_reasons,
        "contact_state_snapshot": {
          "contact_id": str(contact_state.get("contact_id") or ""),
          "owner": str(contact_state.get("owner") or ""),
          "owner_source": str(contact_state.get("owner_source") or ""),
          "actual_shadow_call_count": int(contact_state.get("actual_shadow_call_count") or 0),
          "transcript_covered": bool(contact_state.get("transcript_covered")),
          "recording_covered": bool(contact_state.get("recording_covered")),
          "transfer_status": _explicit_transfer_status(str(contact_state.get("transfer_status") or "")),
          "appointment_status": str(contact_state.get("appointment_status") or "not_booked"),
        },
        "evidence_refs": list(row.get("evidence_refs") or []),
      }
    )
  rows.sort(key=lambda item: (int(item.get("review_priority_score") or 0), int(item.get("estimated_amount") or 0)), reverse=True)
  gap_counter = Counter(reason for row in rows for reason in row["gap_reasons"])
  review_lane_counter = Counter(str(row.get("review_lane") or "") for row in rows)
  summary = {
    "row_count": len(rows),
    "ready_count": sum(1 for row in rows if row.get("audit_status") == "ready"),
    "review_required_count": sum(1 for row in rows if row.get("audit_status") == "review_required"),
    "blocked_count": sum(1 for row in rows if row.get("audit_status") == "blocked"),
    "timeline_covered_count": sum(1 for row in rows if int(row.get("timeline_event_count") or 0) > 0),
    "contact_state_present_count": sum(1 for row in rows if row["field_presence"]["contact_state"]),
    "full_schema_coverage_count": sum(1 for row in rows if all(bool(value) for value in row["field_presence"].values())),
    "avg_required_field_coverage_rate": round(
      sum(float(row.get("required_field_coverage_rate") or 0.0) for row in rows) / max(len(rows), 1),
      3,
    ),
    "avg_reconstruction_readiness_score": round(
      sum(int(row.get("reconstruction_readiness_score") or 0) for row in rows) / max(len(rows), 1),
      1,
    ),
    "review_lane_counts": dict(review_lane_counter),
    "gap_reason_counts": dict(gap_counter),
  }
  return summary, rows


def _build_contact_resolution_review(call_states: list[Mapping[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  queue: list[dict[str, Any]] = []
  for row in call_states:
    resolution_status = str(row.get("contact_resolution_status") or "")
    resolution_confidence = str(row.get("contact_resolution_confidence") or "low")
    source_contact_id = str(row.get("source_contact_id") or "")
    resolved_contact_id = str(row.get("resolved_contact_id") or row.get("contact_state_id") or "")
    if source_contact_id and resolution_confidence == "high":
      continue
    estimated_amount = as_int(row.get("estimated_amount"))
    review_priority_score = (
      min(500, estimated_amount // 10000)
      + (220 if not source_contact_id else 0)
      + (110 if resolution_confidence == "medium" else 180 if resolution_confidence == "low" else 0)
      + (140 if resolved_contact_id.startswith("shadow_contact__") else 0)
      + (120 if str(row.get("transfer_status") or "").startswith("failed") else 0)
    )
    queue.append(
      {
        "call_id": str(row.get("call_id") or ""),
        "source_contact_id": source_contact_id,
        "resolved_contact_id": resolved_contact_id,
        "resolution_status": resolution_status,
        "resolution_confidence": resolution_confidence,
        "matched_phone_redacted": str(row.get("matched_phone_redacted") or ""),
        "candidate_contact_ids": list(row.get("candidate_contact_ids") or []),
        "owner": str(row.get("owner") or "Unassigned LO Review"),
        "owner_source": str(row.get("owner_source") or ""),
        "estimated_amount": estimated_amount,
        "transfer_status": str(row.get("transfer_status") or ""),
        "review_priority_score": review_priority_score,
        "required_field_coverage_rate": float(row.get("required_field_coverage_rate") or 0.0),
        "reconstruction_readiness_score": int(row.get("reconstruction_readiness_score") or 0),
        "reconstruction_readiness_band": str(row.get("reconstruction_readiness_band") or "low"),
        "evidence_refs": list(row.get("evidence_refs") or []),
      }
    )
  queue.sort(key=lambda item: (int(item.get("review_priority_score") or 0), int(item.get("estimated_amount") or 0)), reverse=True)
  summary = {
    "row_count": len(queue),
    "resolved_by_phone_count": sum(1 for row in queue if str(row.get("resolution_status") or "").startswith("resolved_by_phone")),
    "synthetic_fallback_count": sum(1 for row in queue if str(row.get("resolved_contact_id") or "").startswith("shadow_contact__")),
    "medium_confidence_count": sum(1 for row in queue if str(row.get("resolution_confidence") or "") == "medium"),
    "low_confidence_count": sum(1 for row in queue if str(row.get("resolution_confidence") or "") == "low"),
  }
  return summary, queue


def _render_event_replay_markdown(
  replay_summary: Mapping[str, Any],
  call_states: list[Mapping[str, Any]],
  actual_shadow_contact_states: list[Mapping[str, Any]],
) -> str:
  lines = [
    "# Event Replay And Current State - 2026-04-29",
    "",
    "Replay-derived current state for actual shadow Retell calls plus scenario and review evidence.",
    "",
    "## Coverage",
    "",
    f"- Events replayed: {replay_summary['event_count']}",
    f"- Shadow call states: {replay_summary['call_state_count']}",
    f"- Actual shadow contact states: {replay_summary['actual_shadow_contact_state_count']}",
    f"- All-source contact states: {replay_summary['contact_state_count']}",
    f"- Scenario-only contact states: {replay_summary['scenario_only_contact_state_count']}",
    f"- Transcript coverage: {replay_summary['transcript_calls']} / {replay_summary['call_state_count']} ({replay_summary['transcript_coverage_rate']:.1%})",
    f"- Recording coverage: {replay_summary['recording_calls']} / {replay_summary['call_state_count']} ({replay_summary['recording_coverage_rate']:.1%})",
    f"- Owner-covered shadow calls: {replay_summary['owner_covered_calls']} / {replay_summary['call_state_count']}",
    f"- Calls resolved from missing contact IDs: {replay_summary['phone_resolved_call_count']}",
    f"- Synthetic contact fallbacks still present: {replay_summary['synthetic_contact_fallback_count']}",
    f"- Actual observer gaps: {replay_summary['observer_gap_count']}",
    f"- No-conversation-expected calls: {replay_summary['expected_no_conversation_count']}",
    f"- Complete reconstructions: {replay_summary['complete_reconstruction_calls']} / {replay_summary['call_state_count']}",
    f"- Reconstruction review queue: {replay_summary['reconstruction_review_count']}",
    "",
    "## Top Shadow Call States",
    "",
  ]
  for row in call_states[:15]:
    lines.append(
      f"- {row['call_id']}: contact_state={row['contact_state_id']} | owner={row['owner']} ({row['owner_source']}) | readiness={row['reconstruction_readiness_score']} | transfer={row['transfer_status'] or 'none'} | appointment={row['appointment_status']} | transcript={row['transcript_available']} | recording={row['recording_available']} | reconstruction={row['reconstruction_confidence_label']} | gaps={', '.join(row['reconstruction_gap_reasons']) or 'none'}"
    )
  lines.extend(["", "## Top Actual Shadow Contact States", ""])
  for row in actual_shadow_contact_states[:15]:
    lines.append(
      f"- {row['contact_id']}: owner={row['owner']} ({row['owner_source']}) | calls={row['actual_shadow_call_count']} | readiness={row['reconstruction_readiness_score']} | transcript={row['transcript_covered']} | recording={row['recording_covered']} | transfer={row['transfer_status'] or 'none'} | appointment={row['appointment_status']} | gaps={', '.join(row['reconstruction_gap_reasons']) or 'none'}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _render_reconstruction_audit_markdown(summary: Mapping[str, Any], rows: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Reconstruction Audit - 2026-04-29",
    "",
    "Deterministic audit of whether each actual shadow Retell call can be reconstructed into a usable call state and contact state with required evidence fields.",
    "",
    "## Summary",
    "",
    f"- Shadow call rows audited: {summary['row_count']}",
    f"- Ready: {summary['ready_count']}",
    f"- Review required: {summary['review_required_count']}",
    f"- Blocked: {summary['blocked_count']}",
    f"- Event timelines present: {summary['timeline_covered_count']}",
    f"- Contact states present: {summary['contact_state_present_count']}",
    f"- Full required-schema coverage: {summary['full_schema_coverage_count']}",
    f"- Avg required-field coverage: {summary['avg_required_field_coverage_rate']:.1%}",
    f"- Avg readiness score: {summary['avg_reconstruction_readiness_score']}",
    "",
    "## Review Lanes",
    "",
  ]
  for lane, count in sorted(summary.get("review_lane_counts", {}).items(), key=lambda item: item[1], reverse=True):
    lines.append(f"- {lane}: {count}")
  lines.extend(["", "## Top Audit Queue", ""])
  for row in rows[:20]:
    lines.append(
      f"- {row['call_id']}: status={row['audit_status']} | lane={row['review_lane']} | score={row['review_priority_score']} | readiness={row['reconstruction_readiness_score']} | timeline={row['timeline_event_count']} | owner={row['owner']} | transfer={row['transfer_status']} | appointment={row['appointment_status']} | gaps={', '.join(row['gap_reasons'])}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _build_qa_rows(
  qa_call_states: list[Mapping[str, Any]],
  scenario_lookup: Mapping[str, Mapping[str, Any]],
  seed_lookup: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
  rows: list[dict[str, Any]] = []
  for call_state in qa_call_states:
    contact_id = str(call_state.get("contact_id") or "")
    resolved_contact_id = str(call_state.get("resolved_contact_id") or "")
    scenario = scenario_lookup.get(contact_id, {})
    seed = seed_lookup.get(contact_id, {})
    merged = {
      **scenario,
      **seed,
      "contact_id": contact_id or resolved_contact_id,
      "call_id": str(call_state.get("call_id") or ""),
      "connected_seconds": int(call_state.get("duration_seconds") or call_state.get("connected_seconds") or 0),
      "prospect_words_estimate": int(call_state.get("prospect_words") or 0),
      "recording_url": "present" if call_state.get("recording_available") else "",
      "appointment_booked": str(call_state.get("appointment_status") == "booked").lower(),
      "transfer_requested": str(bool(call_state.get("transfer_status"))).lower(),
      "outcome": str(call_state.get("derived_outcome") or ""),
      "confidence": str(call_state.get("derived_confidence_label") or "low"),
      "writeback_status": "shadow_only_no_external_writes",
      "review_questions": str(seed.get("recommended_first_question") or ""),
      "default_next_action": str(seed.get("lo_next_action") or scenario.get("recommended_tool") or ""),
      "estimated_largest_amount": str(call_state.get("estimated_amount") or scenario.get("largest_amount") or seed.get("estimated_largest_amount") or ""),
      "revenue_automation_score": str(scenario.get("revenue_automation_score") or seed.get("post_call_priority_score") or ""),
      "enrichment_confidence": str(seed.get("enrichment_confidence") or scenario.get("confidence") or call_state.get("derived_confidence_label") or ""),
    }
    assessment = asdict(build_assessment(merged, str(call_state.get("owner") or "Unassigned LO Review")))
    assessment.update(
      {
        "actual_shadow_call": True,
        "call_id": str(call_state.get("call_id") or ""),
        "actual_contact_id": contact_id,
        "source_contact_id": str(call_state.get("source_contact_id") or ""),
        "contact_state_id": resolved_contact_id,
        "duration_seconds": int(call_state.get("duration_seconds") or 0),
        "prospect_words": int(call_state.get("prospect_words") or 0),
        "questions_answered": int(call_state.get("questions_answered") or 0),
        "objection_type": str(call_state.get("objection_type") or ""),
        "transfer_status": _explicit_transfer_status(str(call_state.get("transfer_status") or "")),
        "transfer_result": _explicit_transfer_status(str(call_state.get("transfer_status") or "")),
        "appointment_result": str(call_state.get("appointment_status") or ""),
        "appointment_status": str(call_state.get("appointment_status") or ""),
        "source_attribution": str(call_state.get("campaign_context") or ""),
        "campaign_context": str(call_state.get("campaign_context") or ""),
        "owner_source": str(call_state.get("owner_source") or ""),
        "owner_confidence": str(call_state.get("owner_confidence") or ""),
        "transcript_backed_owner": bool(call_state.get("transcript_backed_owner")),
        "transcript_available": bool(call_state.get("transcript_available")),
        "transcript_excerpt": str(call_state.get("transcript_excerpt") or ""),
        "recording_available": bool(call_state.get("recording_available")),
        "recording_url_state": str(call_state.get("recording_url_state") or "missing"),
        "observer_capture_status": str(call_state.get("observer_capture_status") or ""),
        "conversation_evidence_expectation": str(call_state.get("conversation_evidence_expectation") or "expected"),
        "raw_source_kinds": list(call_state.get("raw_source_kinds") or []),
        "raw_source_count": int(call_state.get("raw_source_count") or 0),
        "call_successful": bool(call_state.get("call_status") == "ended" or call_state.get("disposition")),
        "call_status": str(call_state.get("call_status") or ""),
        "user_sentiment": str(call_state.get("user_sentiment") or ""),
        "call_summary_excerpt": str(call_state.get("call_summary_excerpt") or ""),
        "contact_resolution_status": str(call_state.get("contact_resolution_status") or ""),
        "contact_resolution_confidence": str(call_state.get("contact_resolution_confidence") or "low"),
        "matched_phone_redacted": str(call_state.get("matched_phone_redacted") or ""),
        "candidate_contact_ids": list(call_state.get("candidate_contact_ids") or []),
        "contact_resolution_review_required": bool(call_state.get("contact_resolution_review_required")),
        "estimated_largest_amount": int(call_state.get("estimated_amount") or 0),
        "estimated_amount": int(call_state.get("estimated_amount") or 0),
        "internal_test": bool(call_state.get("internal_test")),
        "event_count": int(call_state.get("event_count") or 0),
        "reconstruction_gap_reasons": list(call_state.get("reconstruction_gap_reasons") or []),
        "reconstruction_confidence_label": str(call_state.get("reconstruction_confidence_label") or "low"),
        "required_field_presence": dict(call_state.get("required_field_presence") or {}),
        "required_field_covered_count": int(call_state.get("required_field_covered_count") or 0),
        "required_field_total_count": int(call_state.get("required_field_total_count") or 0),
        "required_field_coverage_rate": float(call_state.get("required_field_coverage_rate") or 0.0),
        "reconstruction_readiness_score": int(call_state.get("reconstruction_readiness_score") or 0),
        "reconstruction_readiness_band": str(call_state.get("reconstruction_readiness_band") or "low"),
        "evidence_refs": list(call_state.get("evidence_refs") or []),
      }
    )
    rows.append(assessment)
  rows.sort(
    key=lambda row: (
      int(row.get("estimated_amount") or 0),
      int(row.get("prospect_words") or 0),
      int(row.get("questions_answered") or 0),
    ),
    reverse=True,
  )
  return rows


def _follow_up_urgency(row: Mapping[str, Any]) -> str:
  if str(row.get("route") or "") == "same_day_lo_callback" or str(row.get("transfer_status") or "").startswith("failed"):
    return "same_day"
  if str(row.get("appointment_result") or "") in {"booking_error", "slots_offered"}:
    return "next_business_day"
  if str(row.get("route") or "") in {"prepare_lo_handoff", "lo_review_then_callback_or_nurture"}:
    return "24h"
  return "review_queue"


def _score_qa_row(row: Mapping[str, Any]) -> dict[str, Any]:
  estimated_amount = as_int(row.get("estimated_amount") or row.get("estimated_largest_amount"))
  profitability_score = min(350, estimated_amount // 4000) + (90 if estimated_amount >= 500000 else 40 if estimated_amount else 0)
  evidence_coverage_score = int(row.get("reconstruction_readiness_score") or 0)
  no_conversation_expected = str(row.get("observer_capture_status") or "") == "not_expected_no_conversation"
  readiness_score = 0
  if str(row.get("outcome") or "") == "booked":
    readiness_score += 160
  if str(row.get("route") or "") == "same_day_lo_callback":
    readiness_score += 145
  if bool(row.get("recording_available")) or no_conversation_expected:
    readiness_score += 30
  if bool(row.get("transcript_available")) or no_conversation_expected:
    readiness_score += 40
  readiness_score += min(120, int(row.get("questions_answered") or 0) * 25)
  readiness_score += evidence_coverage_score // 4
  if bool(row.get("contact_resolution_review_required")):
    readiness_score = max(0, readiness_score - 35)
  engagement_score = min(180, int(row.get("prospect_words") or 0)) + min(60, int(row.get("questions_answered") or 0) * 10)
  urgency_label = _follow_up_urgency(row)
  urgency_score = {
    "same_day": 180,
    "next_business_day": 120,
    "24h": 80,
    "review_queue": 40,
  }[urgency_label]
  transcript_owner_bonus = 70 if row.get("transcript_backed_owner") else 0
  owner_conf_bonus = {"high": 40, "medium": 20, "low": 0}.get(str(row.get("owner_confidence") or ""), 0)
  revenue_priority_score = profitability_score + readiness_score + engagement_score + urgency_score + transcript_owner_bonus + owner_conf_bonus
  return {
    **row,
    "profitability_score": profitability_score,
    "readiness_score": readiness_score,
    "contact_engagement_score": engagement_score,
    "evidence_coverage_score": evidence_coverage_score,
    "follow_up_urgency": urgency_label,
    "revenue_priority_score": revenue_priority_score,
    "revenue_priority_reason": _dedupe_text(
      [
        "transcript-backed-owner" if row.get("transcript_backed_owner") else "",
        "failed-transfer" if str(row.get("transfer_status") or "").startswith("failed") else "",
        "appointment-fallback" if str(row.get("appointment_result") or "") in {"booking_error", "slots_offered"} else "",
        "high-amount" if estimated_amount >= 500000 else "mid-amount" if estimated_amount >= 200000 else "",
      ]
    ),
  }


def _build_actual_shadow_owner_summary(followup_queue: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
  grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  for row in followup_queue:
    grouped[_canonical_owner(str(row.get("owner") or "Unassigned LO Review"))].append(row)

  summary: list[dict[str, Any]] = []
  for owner, rows in grouped.items():
    rows = sorted(rows, key=lambda item: int(item.get("revenue_priority_score") or 0), reverse=True)
    summary.append(
      {
        "owner": owner,
        "followup_count": len(rows),
        "same_day_count": sum(1 for row in rows if str(row.get("follow_up_urgency") or "") == "same_day"),
        "transcript_backed_owner_count": sum(1 for row in rows if row.get("transcript_backed_owner")),
        "high_readiness_count": sum(1 for row in rows if int(row.get("readiness_score") or 0) >= 150),
        "missed_transfer_count": sum(1 for row in rows if str(row.get("transfer_status") or "").startswith("failed")),
        "appointment_fallback_count": sum(
          1 for row in rows if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error", "fallback_discussed"}
        ),
        "unassigned_count": sum(1 for row in rows if _canonical_owner(str(row.get("owner") or "")) == "Unassigned LO Review"),
        "est_revenue_top5": sum(as_int(row.get("estimated_amount")) for row in rows[:5]),
        "top_calls": [str(row.get("call_id") or "") for row in rows[:5]],
      }
    )
  summary.sort(key=lambda item: item["est_revenue_top5"], reverse=True)
  return summary


def _build_owner_attribution_review(followup_queue: list[Mapping[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  queue: list[dict[str, Any]] = []
  for row in followup_queue:
    owner = _canonical_owner(str(row.get("owner") or "Unassigned LO Review"))
    estimated_amount = as_int(row.get("estimated_amount"))
    transcript_expected = str(row.get("observer_capture_status") or "") != "not_expected_no_conversation"
    reasons = _dedupe_text(
      [
        "unassigned_owner" if owner == "Unassigned LO Review" else "",
        "owner_not_transcript_backed" if transcript_expected and not row.get("transcript_backed_owner") else "",
        "missing_transcript" if transcript_expected and not row.get("transcript_available") else "",
        "failed_transfer" if str(row.get("transfer_status") or "").startswith("failed") else "",
        "medium_owner_confidence" if str(row.get("owner_confidence") or "") == "medium" else "",
      ]
    )
    if not reasons:
      continue
    if estimated_amount < 250000 and not str(row.get("transfer_status") or "").startswith("failed"):
      continue
    review_priority_score = (
      min(500, estimated_amount // 10000)
      + (200 if owner == "Unassigned LO Review" else 0)
      + (110 if transcript_expected and not row.get("transcript_available") else 0)
      + (120 if transcript_expected and not row.get("transcript_backed_owner") else 0)
      + (140 if str(row.get("transfer_status") or "").startswith("failed") else 0)
    )
    queue.append(
      {
        "call_id": str(row.get("call_id") or ""),
        "contact_id": str(row.get("actual_contact_id") or row.get("contact_state_id") or ""),
        "owner": owner,
        "owner_source": str(row.get("owner_source") or ""),
        "owner_confidence": str(row.get("owner_confidence") or ""),
        "transcript_available": bool(row.get("transcript_available")),
        "transcript_backed_owner": bool(row.get("transcript_backed_owner")),
        "transfer_status": str(row.get("transfer_status") or ""),
        "appointment_result": str(row.get("appointment_result") or ""),
        "outcome": str(row.get("outcome") or ""),
        "estimated_amount": estimated_amount,
        "review_priority_score": review_priority_score,
        "review_reasons": reasons,
        "transcript_excerpt": str(row.get("transcript_excerpt") or ""),
        "evidence_refs": list(row.get("evidence_refs") or []),
      }
    )
  queue.sort(key=lambda item: (int(item.get("review_priority_score") or 0), int(item.get("estimated_amount") or 0)), reverse=True)
  summary = {
    "row_count": len(queue),
    "high_value_unassigned_count": sum(
      1 for row in queue if row["owner"] == "Unassigned LO Review" and int(row.get("estimated_amount") or 0) >= 250000
    ),
    "missing_transcript_count": sum(1 for row in queue if not row.get("transcript_available")),
    "non_transcript_backed_owner_count": sum(1 for row in queue if not row.get("transcript_backed_owner")),
    "failed_transfer_count": sum(1 for row in queue if str(row.get("transfer_status") or "").startswith("failed")),
  }
  return summary, queue


def _build_observer_capture_review(call_states: list[Mapping[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  queue: list[dict[str, Any]] = []
  status_counts: Counter[str] = Counter()
  raw_source_kind_counts: Counter[str] = Counter()
  for row in call_states:
    observer_capture_status = str(row.get("observer_capture_status") or _observer_capture_status(row))
    status_counts[observer_capture_status] += 1
    for source_kind in row.get("raw_source_kinds") or []:
      raw_source_kind_counts[str(source_kind)] += 1
    if observer_capture_status in {"complete", "not_expected_no_conversation"}:
      continue
    estimated_amount = as_int(row.get("estimated_amount"))
    review_priority_score = (
      min(500, estimated_amount // 10000)
      + (160 if observer_capture_status == "missing_transcript_and_recording" else 100)
      + (70 if str(row.get("call_status") or "") == "ended" else 0)
    )
    queue.append(
      {
        "call_id": str(row.get("call_id") or ""),
        "contact_id": str(row.get("contact_id") or ""),
        "owner": str(row.get("owner") or "Unassigned LO Review"),
        "call_status": str(row.get("call_status") or ""),
        "observer_capture_status": observer_capture_status,
        "estimated_amount": estimated_amount,
        "review_priority_score": review_priority_score,
        "raw_source_kinds": list(row.get("raw_source_kinds") or []),
        "raw_source_count": int(row.get("raw_source_count") or 0),
        "transcript_source_kinds": list(row.get("transcript_source_kinds") or []),
        "recording_source_kinds": list(row.get("recording_source_kinds") or []),
        "event_count": int(row.get("event_count") or 0),
        "gap_reasons": [
          reason
          for reason in list(row.get("reconstruction_gap_reasons") or [])
          if reason in {"missing_transcript", "missing_recording"}
        ],
        "evidence_refs": list(row.get("evidence_refs") or []),
      }
    )
  queue.sort(key=lambda item: (int(item.get("review_priority_score") or 0), int(item.get("estimated_amount") or 0)), reverse=True)
  summary = {
    "row_count": len(queue),
    "complete_count": status_counts.get("complete", 0),
    "expected_no_conversation_count": status_counts.get("not_expected_no_conversation", 0),
    "missing_transcript_count": status_counts.get("missing_transcript", 0) + status_counts.get("missing_transcript_and_recording", 0),
    "missing_recording_count": status_counts.get("missing_recording", 0) + status_counts.get("missing_transcript_and_recording", 0),
    "raw_source_kind_counts": dict(raw_source_kind_counts),
  }
  return summary, queue


def _render_observer_capture_review_markdown(summary: Mapping[str, Any], queue: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Observer Capture Review - 2026-04-29",
    "",
    "Source-aware shadow review separating true observer capture gaps from calls where no conversation evidence is expected.",
    "",
    "## Summary",
    "",
    f"- Actual observer gap rows: {summary['row_count']}",
    f"- Complete capture rows: {summary['complete_count']}",
    f"- No-conversation-expected rows: {summary['expected_no_conversation_count']}",
    f"- Missing transcript rows: {summary['missing_transcript_count']}",
    f"- Missing recording rows: {summary['missing_recording_count']}",
    f"- Raw source kind counts: {summary['raw_source_kind_counts']}",
    "",
    "## Top Observer Gaps",
    "",
  ]
  for row in queue[:20]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | status={row['observer_capture_status']} | call_status={row['call_status']} | score={row['review_priority_score']} | sources={','.join(row['raw_source_kinds']) or 'none'} | est_amount=${row['estimated_amount']:,}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _build_reporting_views(
  repo_root: Path,
  scenario_rows: list[dict[str, str]],
  scoreboard_seed_rows: list[dict[str, str]],
  qa_rows: list[dict[str, Any]],
  contact_resolution_summary: Mapping[str, Any],
  contact_resolution_queue: list[Mapping[str, Any]],
  observer_capture_summary: Mapping[str, Any],
) -> tuple[
  dict[str, Any],
  list[dict[str, Any]],
  list[dict[str, Any]],
  dict[str, Any],
  list[dict[str, Any]],
  dict[str, Any],
  list[dict[str, Any]],
]:
  scored_rows = [_score_qa_row(row) for row in qa_rows]
  followup_queue = [row for row in scored_rows if not row.get("internal_test")]
  followup_queue.sort(key=lambda row: int(row.get("revenue_priority_score") or 0), reverse=True)
  assessments_by_contact = {
    str(row.get("actual_contact_id") or row.get("contact_state_id") or ""): row
    for row in scored_rows
    if str(row.get("actual_contact_id") or row.get("contact_state_id") or "")
  }
  ranked_input_rows: list[Mapping[str, Any]] = [*scenario_rows, *scoreboard_seed_rows[:150]]
  scoreboard_rows = build_scoreboard_rows(repo_root, ranked_input_rows, assessments_by_contact)
  for row in scoreboard_rows:
    assessment = assessments_by_contact.get(str(row.get("contact_id") or ""), {})
    row["transcript_backed_owner"] = bool(assessment.get("transcript_backed_owner"))
    row["profitability_score"] = int(assessment.get("profitability_score") or 0)
    row["readiness_score"] = int(assessment.get("readiness_score") or 0)
    row["contact_engagement_score"] = int(assessment.get("contact_engagement_score") or 0)
    row["evidence_coverage_score"] = int(assessment.get("evidence_coverage_score") or 0)
    row["follow_up_urgency"] = str(assessment.get("follow_up_urgency") or "")
    row["revenue_priority_score"] = int(assessment.get("revenue_priority_score") or row.get("hot_score") or 0)
    row["actual_shadow_call"] = bool(assessment.get("actual_shadow_call"))
    row["owner_source"] = str(assessment.get("owner_source") or row.get("owner_source") or "")
  scoreboard_rows.sort(key=lambda item: int(item.get("revenue_priority_score") or item.get("hot_score") or 0), reverse=True)

  lo_followup_summary = _build_actual_shadow_owner_summary(followup_queue)
  owner_review_summary, owner_review_queue = _build_owner_attribution_review(followup_queue)
  action_approval_summary, action_approval_queue = _build_post_call_action_approval_queue(followup_queue)

  observer_gap_queue: list[dict[str, Any]] = []
  for row in followup_queue:
    observer_capture_status = str(row.get("observer_capture_status") or "")
    if observer_capture_status in {"", "complete", "not_expected_no_conversation"}:
      continue
    observer_gap_queue.append(
      {
        "contact_id": str(row.get("actual_contact_id") or row.get("contact_state_id") or ""),
        "call_id": str(row.get("call_id") or ""),
        "owner": str(row.get("owner") or "Unassigned LO Review"),
        "estimated_amount": as_int(row.get("estimated_amount")),
        "review_reason": observer_capture_status,
        "raw_source_kinds": list(row.get("raw_source_kinds") or []),
        "evidence_refs": list(row.get("evidence_refs") or []),
      }
    )
  observer_gap_queue.sort(key=lambda item: int(item.get("estimated_amount") or 0), reverse=True)

  reconstruction_review_summary, reconstruction_review_queue = _build_reconstruction_review_queue(
    [row for row in scored_rows if row.get("actual_shadow_call")]
  )

  management_summary = {
    "scoreboard_summary": lo_followup_summary,
    "actual_shadow_owner_summary": lo_followup_summary,
    "all_lead_scoreboard_summary": build_lo_summary(scoreboard_rows)[:10],
    "followup_summary": lo_followup_summary,
    "revenue_weighted_queue": followup_queue[:50],
    "missed_transfer_queue": [row for row in followup_queue if str(row.get("transfer_status") or "").startswith("failed")][:20],
    "appointment_fallback_queue": [
      row for row in followup_queue if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error", "fallback_discussed"}
    ][:20],
    "observer_gap_queue": observer_gap_queue[:20],
    "reconstruction_review_summary": reconstruction_review_summary,
    "reconstruction_review_queue": reconstruction_review_queue[:25],
    "contact_resolution_review_summary": dict(contact_resolution_summary),
    "contact_resolution_review_queue": [dict(row) for row in contact_resolution_queue[:25]],
    "owner_attribution_review_summary": owner_review_summary,
    "owner_attribution_review_queue": owner_review_queue[:25],
    "post_call_action_approval_summary": action_approval_summary,
    "post_call_action_approval_queue": action_approval_queue[:25],
    "summary": {
      "qa_call_count": len(qa_rows),
      "followup_queue_count": len(followup_queue),
      "missed_transfer_count": sum(1 for row in followup_queue if str(row.get("transfer_status") or "").startswith("failed")),
      "appointment_fallback_count": sum(
        1 for row in followup_queue if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error", "fallback_discussed"}
      ),
      "observer_gap_count": len(observer_gap_queue),
      "observer_expected_absence_count": int(observer_capture_summary.get("expected_no_conversation_count") or 0),
      "reconstruction_review_count": reconstruction_review_summary["row_count"],
      "contact_resolution_review_count": int(contact_resolution_summary.get("row_count") or 0),
      "owner_review_count": len(owner_review_queue),
      "post_call_action_approval_count": len(action_approval_queue),
      "high_value_unassigned_count": owner_review_summary["high_value_unassigned_count"],
      "scoreboard_row_count": len(scoreboard_rows),
      "internal_test_count": sum(1 for row in qa_rows if row.get("internal_test")),
      "transcript_backed_owner_count": sum(1 for row in followup_queue if row.get("transcript_backed_owner")),
      "high_urgency_count": sum(1 for row in followup_queue if str(row.get("follow_up_urgency") or "") == "same_day"),
      "avg_reconstruction_readiness_score": round(
        sum(int(row.get("reconstruction_readiness_score") or 0) for row in qa_rows) / max(len(qa_rows), 1),
        1,
      ),
    },
  }
  return (
    management_summary,
    scoreboard_rows,
    followup_queue,
    owner_review_summary,
    owner_review_queue,
    action_approval_summary,
    action_approval_queue,
  )


def _average_int(rows: list[Mapping[str, Any]], key: str) -> float:
  if not rows:
    return 0.0
  return round(sum(as_int(row.get(key)) for row in rows) / len(rows), 1)


def _build_actual_call_cohort_report(
  qa_rows: list[Mapping[str, Any]],
  followup_queue: list[Mapping[str, Any]],
) -> dict[str, Any]:
  followup_by_call = {str(row.get("call_id") or ""): row for row in followup_queue if str(row.get("call_id") or "")}
  campaign_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  owner_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  outcome_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  objection_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)

  for row in qa_rows:
    campaign_groups[str(row.get("campaign_context") or "unknown")].append(row)
    owner_groups[_canonical_owner(str(row.get("owner") or "Unassigned LO Review"))].append(row)
    outcome_groups[str(row.get("outcome") or "unknown")].append(row)
    objection_groups[str(row.get("objection_type") or "none_explicit")].append(row)

  def cohort_row(label: str, rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    followup_rows = [followup_by_call[str(row.get("call_id") or "")] for row in rows if str(row.get("call_id") or "") in followup_by_call]
    return {
      "label": label,
      "call_count": len(rows),
      "followup_count": len(followup_rows),
      "transcript_count": sum(1 for row in rows if row.get("transcript_available")),
      "recording_count": sum(1 for row in rows if row.get("recording_available")),
      "transcript_backed_owner_count": sum(1 for row in rows if row.get("transcript_backed_owner")),
      "missed_transfer_count": sum(1 for row in rows if str(row.get("transfer_status") or "").startswith("failed")),
      "appointment_fallback_count": sum(
        1 for row in rows if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error", "fallback_discussed"}
      ),
      "same_day_count": sum(1 for row in followup_rows if str(row.get("follow_up_urgency") or "") == "same_day"),
      "estimated_amount_sum": sum(as_int(row.get("estimated_amount")) for row in rows),
      "avg_prospect_words": _average_int(rows, "prospect_words"),
      "avg_questions_answered": _average_int(rows, "questions_answered"),
      "avg_reconstruction_readiness_score": _average_int(rows, "reconstruction_readiness_score"),
      "high_readiness_count": sum(1 for row in rows if int(row.get("reconstruction_readiness_score") or 0) >= 85),
      "internal_test_count": sum(1 for row in rows if row.get("internal_test")),
    }

  campaign_cohorts = [cohort_row(label, rows) for label, rows in campaign_groups.items()]
  owner_cohorts = [cohort_row(label, rows) for label, rows in owner_groups.items()]
  outcome_cohorts = [cohort_row(label, rows) for label, rows in outcome_groups.items()]
  objection_cohorts = [cohort_row(label, rows) for label, rows in objection_groups.items()]

  for group in (campaign_cohorts, owner_cohorts, outcome_cohorts, objection_cohorts):
    group.sort(key=lambda item: (int(item.get("estimated_amount_sum") or 0), int(item.get("call_count") or 0)), reverse=True)

  profitability_readiness_gap_queue: list[dict[str, Any]] = []
  for row in followup_queue:
    gap_score = int(row.get("profitability_score") or 0) - int(row.get("readiness_score") or 0)
    if gap_score <= 0:
      continue
    profitability_readiness_gap_queue.append(
      {
        "call_id": str(row.get("call_id") or ""),
        "owner": str(row.get("owner") or "Unassigned LO Review"),
        "campaign_context": str(row.get("campaign_context") or ""),
        "outcome": str(row.get("outcome") or ""),
        "estimated_amount": as_int(row.get("estimated_amount")),
        "profitability_score": int(row.get("profitability_score") or 0),
        "readiness_score": int(row.get("readiness_score") or 0),
        "contact_engagement_score": int(row.get("contact_engagement_score") or 0),
        "evidence_coverage_score": int(row.get("evidence_coverage_score") or 0),
        "follow_up_urgency": str(row.get("follow_up_urgency") or ""),
        "gap_score": gap_score,
        "reasons": list(row.get("revenue_priority_reason") or []),
      }
    )
  profitability_readiness_gap_queue.sort(
    key=lambda item: (int(item.get("gap_score") or 0), int(item.get("estimated_amount") or 0)), reverse=True
  )

  same_day_revenue_queue = [
    {
      "call_id": str(row.get("call_id") or ""),
      "owner": str(row.get("owner") or "Unassigned LO Review"),
      "estimated_amount": as_int(row.get("estimated_amount")),
      "revenue_priority_score": int(row.get("revenue_priority_score") or 0),
      "transfer_status": _explicit_transfer_status(str(row.get("transfer_status") or "")),
      "appointment_result": str(row.get("appointment_result") or "not_booked"),
      "route": str(row.get("route") or ""),
    }
    for row in followup_queue
    if str(row.get("follow_up_urgency") or "") == "same_day"
  ]

  summary = {
    "campaign_count": len(campaign_cohorts),
    "owner_count": len(owner_cohorts),
    "outcome_count": len(outcome_cohorts),
    "objection_count": len(objection_cohorts),
    "largest_campaign_by_amount": campaign_cohorts[0]["label"] if campaign_cohorts else "",
    "largest_owner_by_amount": owner_cohorts[0]["label"] if owner_cohorts else "",
    "top_outcome_by_count": max(outcome_cohorts, key=lambda item: int(item.get("call_count") or 0))["label"] if outcome_cohorts else "",
    "profitability_readiness_gap_count": len(profitability_readiness_gap_queue),
    "same_day_queue_count": len(same_day_revenue_queue),
    "observer_gap_count": sum(
      1 for row in qa_rows if str(row.get("observer_capture_status") or "") not in {"", "complete", "not_expected_no_conversation"}
    ),
    "observer_expected_absence_count": sum(
      1 for row in qa_rows if str(row.get("observer_capture_status") or "") == "not_expected_no_conversation"
    ),
    "avg_reconstruction_readiness_score": round(
      sum(int(row.get("reconstruction_readiness_score") or 0) for row in qa_rows) / max(len(qa_rows), 1),
      1,
    ),
  }
  return {
    "summary": summary,
    "campaign_cohorts": campaign_cohorts,
    "owner_cohorts": owner_cohorts,
    "outcome_cohorts": outcome_cohorts,
    "objection_cohorts": objection_cohorts,
    "profitability_readiness_gap_queue": profitability_readiness_gap_queue[:25],
    "same_day_revenue_queue": same_day_revenue_queue[:25],
  }


def _render_actual_call_cohort_markdown(report: Mapping[str, Any]) -> str:
  summary = report.get("summary", {}) if isinstance(report.get("summary"), Mapping) else {}
  campaign_cohorts = report.get("campaign_cohorts", []) if isinstance(report.get("campaign_cohorts"), list) else []
  owner_cohorts = report.get("owner_cohorts", []) if isinstance(report.get("owner_cohorts"), list) else []
  outcome_cohorts = report.get("outcome_cohorts", []) if isinstance(report.get("outcome_cohorts"), list) else []
  gap_queue = report.get("profitability_readiness_gap_queue", []) if isinstance(report.get("profitability_readiness_gap_queue"), list) else []
  lines = [
    "# Actual Call Cohort Report - 2026-04-29",
    "",
    "Actual shadow Retell call cohort reporting for campaign, owner, outcome, and profitability-versus-readiness patterns.",
    "",
    "## Summary",
    "",
    f"- Campaign cohorts: {summary.get('campaign_count', 0)}",
    f"- Owner cohorts: {summary.get('owner_count', 0)}",
    f"- Outcome cohorts: {summary.get('outcome_count', 0)}",
    f"- Objection cohorts: {summary.get('objection_count', 0)}",
    f"- Largest campaign by amount: {summary.get('largest_campaign_by_amount', 'n/a')}",
    f"- Largest owner by amount: {summary.get('largest_owner_by_amount', 'n/a')}",
    f"- Top outcome by count: {summary.get('top_outcome_by_count', 'n/a')}",
    f"- Profitability/readiness gap rows: {summary.get('profitability_readiness_gap_count', 0)}",
    f"- Actual observer gap rows: {summary.get('observer_gap_count', 0)}",
    f"- No-conversation-expected rows: {summary.get('observer_expected_absence_count', 0)}",
    f"- Avg reconstruction readiness score: {summary.get('avg_reconstruction_readiness_score', 0)}",
    "",
    "## Campaign Cohorts",
    "",
  ]
  for row in campaign_cohorts[:10]:
    lines.append(
      f"- {row['label']}: calls={row['call_count']} | transcript={row['transcript_count']} | recording={row['recording_count']} | ready85+={row['high_readiness_count']} | same_day={row['same_day_count']} | est_amount=${row['estimated_amount_sum']:,}"
    )
  lines.extend(["", "## Owner Cohorts", ""])
  for row in owner_cohorts[:10]:
    lines.append(
      f"- {row['label']}: calls={row['call_count']} | transcript_backed={row['transcript_backed_owner_count']} | ready85+={row['high_readiness_count']} | missed_transfer={row['missed_transfer_count']} | est_amount=${row['estimated_amount_sum']:,}"
    )
  lines.extend(["", "## Outcome Cohorts", ""])
  for row in outcome_cohorts[:10]:
    lines.append(
      f"- {row['label']}: calls={row['call_count']} | avg_words={row['avg_prospect_words']} | avg_questions={row['avg_questions_answered']} | est_amount=${row['estimated_amount_sum']:,}"
    )
  lines.extend(["", "## Profitability Vs Readiness Gap Queue", ""])
  for row in gap_queue[:15]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | gap={row['gap_score']} | profitability={row['profitability_score']} | readiness={row['readiness_score']} | evidence={row['evidence_coverage_score']} | urgency={row['follow_up_urgency']} | est_amount=${row['estimated_amount']:,}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _render_qa_markdown(summary: Mapping[str, Any], rows: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Post-Call QA Continuation - 2026-04-29",
    "",
    "Transcript-backed shadow QA derived from actual Retell analyzed calls. All routes remain internal-only and review-gated.",
    "",
    "## Summary",
    "",
    f"- Actual shadow calls: {summary['row_count']}",
    f"- Transcript-covered calls: {summary['transcript_row_count']}",
    f"- Recording-covered calls: {summary['recording_row_count']}",
    f"- Transcript-backed owner rows: {summary['transcript_backed_owner_count']}",
    f"- Actual observer gap rows: {summary['observer_gap_row_count']}",
    f"- No-conversation-expected rows: {summary['observer_expected_absence_count']}",
    f"- Avg reconstruction readiness score: {summary['avg_reconstruction_readiness_score']}",
    "",
    "## Top QA Rows",
    "",
  ]
  for row in rows[:20]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | outcome={row['outcome']} | words={row['prospect_words']} | questions={row['questions_answered']} | readiness={row['reconstruction_readiness_score']} | transfer={row['transfer_status'] or 'none'} | appointment={row['appointment_result']} | owner_source={row['owner_source']}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _render_followup_markdown(management_summary: Mapping[str, Any]) -> str:
  lines = [
    "# LO Follow-Up Scoreboard - 2026-04-29",
    "",
    "Revenue-prioritized shadow follow-up queue built from actual Retell shadow calls, transcript-backed ownership, and call evidence.",
    "",
    "## Owner Summary",
    "",
  ]
  for row in management_summary["followup_summary"]:
    lines.extend(
      [
        f"### {row['owner']}",
        "",
        f"- Follow-up rows: {row['followup_count']}",
        f"- Same-day urgency: {row['same_day_count']}",
        f"- Transcript-backed owner rows: {row['transcript_backed_owner_count']}",
        f"- High-readiness rows: {row['high_readiness_count']}",
        f"- Est. top-5 opportunity amount: ${row['est_revenue_top5']:,}",
        "",
      ]
    )
  lines.extend(["## Revenue Priority Queue", ""])
  for row in management_summary["revenue_weighted_queue"][:25]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | score={row['revenue_priority_score']} | profitability={row['profitability_score']} | readiness={row['readiness_score']} | evidence={row['evidence_coverage_score']} | engagement={row['contact_engagement_score']} | urgency={row['follow_up_urgency']} | owner_source={row['owner_source']}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _render_management_markdown(management_summary: Mapping[str, Any]) -> str:
  summary = management_summary["summary"]
  reconstruction_review_summary = management_summary["reconstruction_review_summary"]
  contact_resolution_review_summary = management_summary["contact_resolution_review_summary"]
  owner_review_summary = management_summary["owner_attribution_review_summary"]
  action_approval_summary = management_summary["post_call_action_approval_summary"]
  cohort_report = management_summary.get("actual_call_cohort_report") if isinstance(management_summary.get("actual_call_cohort_report"), Mapping) else {}
  cohort_summary = cohort_report.get("summary") if isinstance(cohort_report.get("summary"), Mapping) else {}
  lines = [
    "# Management Report Continuation - 2026-04-29",
    "",
    "Shadow-only management report emphasizing actual call evidence, owner coverage, urgency, and recovery lanes.",
    "",
    "## Summary",
    "",
    f"- QA rows derived from actual shadow calls: {summary['qa_call_count']}",
    f"- Revenue-priority follow-up rows: {summary['followup_queue_count']}",
    f"- Transcript-backed owner rows: {summary['transcript_backed_owner_count']}",
    f"- High-urgency rows: {summary['high_urgency_count']}",
    f"- Missed transfer rows: {summary['missed_transfer_count']}",
    f"- Appointment fallback rows: {summary['appointment_fallback_count']}",
    f"- Observer gap rows: {summary['observer_gap_count']}",
    f"- No-conversation-expected rows: {summary['observer_expected_absence_count']}",
    f"- Reconstruction review rows: {summary['reconstruction_review_count']}",
    f"- Contact resolution review rows: {summary['contact_resolution_review_count']}",
    f"- Owner attribution review rows: {summary['owner_review_count']}",
    f"- Post-call action approval rows: {summary['post_call_action_approval_count']}",
    f"- High-value unassigned owner rows: {summary['high_value_unassigned_count']}",
    f"- Avg reconstruction readiness score: {summary['avg_reconstruction_readiness_score']}",
    f"- Campaign cohorts: {cohort_summary.get('campaign_count', 0)}",
    f"- Profitability/readiness gap rows: {cohort_summary.get('profitability_readiness_gap_count', 0)}",
    "",
    "## Reconstruction Review",
    "",
    f"- Review rows: {reconstruction_review_summary['row_count']}",
    f"- Missing transcript rows: {reconstruction_review_summary['missing_transcript_count']}",
    f"- Missing recording rows: {reconstruction_review_summary['missing_recording_count']}",
    f"- Owner unresolved rows: {reconstruction_review_summary['owner_unresolved_count']}",
    f"- Avg required-field coverage: {reconstruction_review_summary['avg_required_field_coverage_rate']:.1%}",
    f"- Avg readiness score: {reconstruction_review_summary['avg_reconstruction_readiness_score']}",
    "",
  ]
  for row in management_summary["reconstruction_review_queue"][:10]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | ${row['estimated_amount']:,} | readiness={row['reconstruction_readiness_score']} | gaps={', '.join(row['gap_reasons'])}"
    )
  lines.extend([
    "",
    "## Contact Resolution Review",
    "",
    f"- Review rows: {contact_resolution_review_summary['row_count']}",
    f"- Resolved by phone: {contact_resolution_review_summary['resolved_by_phone_count']}",
    f"- Synthetic contact fallbacks: {contact_resolution_review_summary['synthetic_fallback_count']}",
    "",
  ])
  for row in management_summary["contact_resolution_review_queue"][:10]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | {row['resolution_status']} | {row['resolution_confidence']} | {row['matched_phone_redacted'] or 'no-phone-match'}"
    )
  lines.extend([
    "",
    "## Owner Attribution Review",
    "",
    f"- Review rows: {owner_review_summary['row_count']}",
    f"- Missing transcript rows: {owner_review_summary['missing_transcript_count']}",
    f"- Non-transcript-backed owner rows: {owner_review_summary['non_transcript_backed_owner_count']}",
    f"- Failed transfer rows: {owner_review_summary['failed_transfer_count']}",
    "",
  ])
  for row in management_summary["owner_attribution_review_queue"][:10]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | ${row['estimated_amount']:,} | reasons={', '.join(row['review_reasons'])}"
    )
  lines.extend([
    "",
    "## Post-Call Action Approval",
    "",
    f"- Review rows: {action_approval_summary['row_count']}",
    f"- Same-day rows: {action_approval_summary['same_day_count']}",
    f"- Handoff recovery rows: {action_approval_summary['handoff_recovery_count']}",
    f"- Observer repair rows: {action_approval_summary['observer_repair_count']}",
    "",
  ])
  for row in management_summary["post_call_action_approval_queue"][:10]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | lane={row['approval_lane']} | score={row['approval_priority_score']} | action={row['recommended_shadow_action']}"
    )
  lines.extend(["", "## Actual Call Cohorts", ""])
  for row in cohort_report.get("campaign_cohorts", [])[:8]:
    lines.append(
      f"- Campaign {row['label']}: calls={row['call_count']} | transcript={row['transcript_count']} | recording={row['recording_count']} | est_amount=${row['estimated_amount_sum']:,}"
    )
  lines.extend(["", "## Profitability Vs Readiness Gaps", ""])
  for row in cohort_report.get("profitability_readiness_gap_queue", [])[:10]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | gap={row['gap_score']} | profitability={row['profitability_score']} | readiness={row['readiness_score']}"
    )
  lines.extend(["", "## Missed Transfer Queue", ""])
  for row in management_summary["missed_transfer_queue"][:12]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | ${row['estimated_amount']:,} | next={row['next_action']}"
    )
  lines.extend(["", "## Appointment Fallback Queue", ""])
  for row in management_summary["appointment_fallback_queue"][:12]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | appointment={row['appointment_result']} | next={row['next_action']}"
    )
  lines.extend(["", "## Observer Gaps", ""])
  for row in management_summary["observer_gap_queue"][:12]:
    lines.append(
      f"- {row['owner']}: {row['call_id'] or row['contact_id']} | ${row['estimated_amount']:,} | {row['review_reason']}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _render_owner_attribution_review_markdown(summary: Mapping[str, Any], queue: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Owner Attribution Review - 2026-04-29",
    "",
    "Shadow-only review queue for actual Retell calls that still need stronger owner evidence or explicit manager confirmation.",
    "",
    "## Summary",
    "",
    f"- Review rows: {summary['row_count']}",
    f"- High-value unassigned rows: {summary['high_value_unassigned_count']}",
    f"- Missing transcript rows: {summary['missing_transcript_count']}",
    f"- Non-transcript-backed owner rows: {summary['non_transcript_backed_owner_count']}",
    f"- Failed transfer rows: {summary['failed_transfer_count']}",
    "",
    "## Priority Queue",
    "",
  ]
  for row in queue[:20]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | ${row['estimated_amount']:,} | score={row['review_priority_score']} | reasons={', '.join(row['review_reasons'])}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _render_contact_resolution_review_markdown(summary: Mapping[str, Any], queue: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Contact Resolution Review - 2026-04-29",
    "",
    "Shadow-only review queue for Retell calls that still need stronger contact identity resolution before promotion.",
    "",
    "## Summary",
    "",
    f"- Review rows: {summary['row_count']}",
    f"- Resolved by phone: {summary['resolved_by_phone_count']}",
    f"- Synthetic contact fallbacks: {summary['synthetic_fallback_count']}",
    f"- Medium-confidence resolutions: {summary['medium_confidence_count']}",
    f"- Low-confidence resolutions: {summary['low_confidence_count']}",
    "",
    "## Priority Queue",
    "",
  ]
  for row in queue[:20]:
    lines.append(
      f"- {row['call_id']}: resolved={row['resolved_contact_id']} | status={row['resolution_status']} | confidence={row['resolution_confidence']} | phone={row['matched_phone_redacted'] or 'none'} | score={row['review_priority_score']}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _critical_reconstruction_gaps(row: Mapping[str, Any]) -> list[str]:
  gap_reasons = [str(item) for item in row.get("reconstruction_gap_reasons") or [] if str(item).strip()]
  contact_resolution_status = str(row.get("contact_resolution_status") or "")
  if "missing_contact_id" in gap_reasons and contact_resolution_status.startswith("resolved_by_phone"):
    gap_reasons = [reason for reason in gap_reasons if reason != "missing_contact_id"]
  if str(row.get("observer_capture_status") or _observer_capture_status(row)) == "not_expected_no_conversation":
    gap_reasons = [reason for reason in gap_reasons if reason not in {"missing_transcript", "missing_recording"}]
  critical = {
    "missing_transcript",
    "missing_recording",
    "missing_contact_state",
    "missing_event_timeline",
    "missing_evidence_refs",
    "transfer_outcome_incomplete",
    "appointment_outcome_incomplete",
  }
  return [reason for reason in gap_reasons if reason in critical]


def _action_approval_lane(row: Mapping[str, Any]) -> str:
  owner = _canonical_owner(str(row.get("owner") or "Unassigned LO Review"))
  transfer_status = str(row.get("transfer_status") or "")
  appointment_result = str(row.get("appointment_result") or "")
  outcome = str(row.get("outcome") or "")
  route = str(row.get("route") or "")
  transcript_expected = str(row.get("observer_capture_status") or "") != "not_expected_no_conversation"
  if bool(row.get("contact_resolution_review_required")):
    return "contact_resolution_repair"
  if _critical_reconstruction_gaps(row):
    return "observer_repair"
  if owner == "Unassigned LO Review" or (transcript_expected and not bool(row.get("transcript_backed_owner"))):
    return "owner_resolution"
  if transfer_status.startswith("failed") or appointment_result in {"slots_offered", "booking_error", "fallback_discussed"}:
    return "handoff_recovery"
  if route == "prepare_lo_handoff" or outcome == "booked":
    return "lo_handoff_preparation"
  if str(row.get("follow_up_urgency") or "") == "same_day":
    return "lo_callback_priority"
  if outcome in {"do_not_call", "not_interested", "bad_number", "wrong_product", "unqualified"}:
    return "suppression_or_disposition_review"
  return "lo_review_nurture"


def _approval_gate_for_lane(lane: str) -> str:
  return {
    "contact_resolution_repair": "Ops + sales manager review before any downstream routing or writeback discussion.",
    "observer_repair": "Ops review before changing observer, replay, or post-call routing assumptions.",
    "owner_resolution": "Sales manager transcript spot-check before LO queue ownership is trusted.",
    "handoff_recovery": "LO manager review before any callback order or appointment recovery workflow change.",
    "lo_handoff_preparation": "LO + manager review before any borrower-facing handoff or calendar action.",
    "lo_callback_priority": "LO manager review before queue becomes a real callback order.",
    "suppression_or_disposition_review": "Manager + compliance review before any suppression or disposition writeback.",
    "lo_review_nurture": "LO manager review before nurture sequencing or borrower messaging is considered.",
  }.get(lane, "Human review required before any workflow or routing promotion.")


def _recommended_action_for_lane(lane: str, row: Mapping[str, Any]) -> str:
  next_action = str(row.get("next_action") or "").strip()
  if lane == "contact_resolution_repair":
    return "Confirm contact identity from phone-backed evidence, then rebuild the call/contact state before any follow-up routing."
  if lane == "observer_repair":
    return "Repair missing replay evidence and rerun the ledger before any action is approved."
  if lane == "owner_resolution":
    return "Manager-confirm owner from transcript and queue evidence before LO follow-up is trusted."
  if lane == "handoff_recovery":
    return "Create a manager-reviewed recovery plan for the failed transfer or appointment fallout using transcript-backed intent."
  if lane == "lo_handoff_preparation":
    return next_action or "Prepare a draft LO handoff packet with transcript, recording, and scenario evidence only."
  if lane == "lo_callback_priority":
    return next_action or "Queue a same-day LO callback in shadow mode with transcript-backed ownership and no pricing claims."
  if lane == "suppression_or_disposition_review":
    return next_action or "Review disposition evidence and hold any suppression/writeback decision until manager approval."
  return next_action or "Hold in LO review until the transcript-backed next step is approved."


def _expected_impact_for_lane(lane: str) -> str:
  return {
    "contact_resolution_repair": "Reduces wrong-contact routing risk before any automation or human follow-up is promoted.",
    "observer_repair": "Improves replay completeness so downstream QA and routing decisions stay evidence-backed.",
    "owner_resolution": "Improves ownership accuracy on high-value calls and reduces wrong-LO follow-up leakage.",
    "handoff_recovery": "Recovers warm borrower intent that would otherwise leak after failed transfers or booking fallout.",
    "lo_handoff_preparation": "Shortens LO response time on booked or ready-to-handoff calls without enabling live writes.",
    "lo_callback_priority": "Improves same-day response on high-intent calls while keeping the queue shadow-only.",
    "suppression_or_disposition_review": "Reduces compliance and bad-disposition risk from low-confidence post-call outcomes.",
    "lo_review_nurture": "Keeps lower-readiness leads organized for review without forcing premature outreach.",
  }.get(lane, "Improves action routing quality while keeping the system shadow-only.")


def _measurement_plan_for_lane(lane: str) -> list[str]:
  return {
    "contact_resolution_repair": [
      "Measure resolved contact coverage on the next 25 analyzed shadow calls.",
      "Track synthetic-contact fallback count before and after review.",
    ],
    "observer_repair": [
      "Measure reconstruction-gap queue size on the next daily build.",
      "Track transcript, recording, and evidence-ref coverage after replay repair.",
    ],
    "owner_resolution": [
      "Measure transcript-backed owner coverage on the next 25 high-value queue rows.",
      "Spot-check five owner resolutions against transcript evidence.",
    ],
    "handoff_recovery": [
      "Track failed-transfer and appointment-fallback recovery rows on the next 20 qualifying calls.",
      "Compare same-day recovery readiness before and after manager review.",
    ],
    "lo_handoff_preparation": [
      "Track how many booked or ready-handoff rows remain blocked by missing evidence.",
      "Measure transcript-backed handoff packet coverage on the next daily build.",
    ],
    "lo_callback_priority": [
      "Track same-day urgency coverage on the next 20 high-intent calls.",
      "Compare callback ordering against manager review for the top 10 rows.",
    ],
    "suppression_or_disposition_review": [
      "Track low-confidence suppression/disposition rows and manager overrides on the next daily build.",
      "Spot-check five disposition recommendations against transcript evidence.",
    ],
    "lo_review_nurture": [
      "Track nurture-versus-callback recommendation mix on the next 20 reviewed rows.",
      "Measure how many lower-readiness rows still lack transcript-backed next-step evidence.",
    ],
  }.get(lane, ["Track queue size and manager review outcomes on the next daily build."])


def _rollback_plan_for_lane(lane: str) -> str:
  return {
    "contact_resolution_repair": "Return unresolved calls to synthetic-contact shadow rows and require manual identity confirmation.",
    "observer_repair": "Revert to the previous observer ruleset and mark new replay fields advisory-only.",
    "owner_resolution": "Fall back to scenario-suggested owners only and require manual assignment.",
    "handoff_recovery": "Remove recovery prioritization and send the calls back to the generic review queue.",
    "lo_handoff_preparation": "Disable handoff prioritization and return booked rows to the broader LO review queue.",
    "lo_callback_priority": "Disable same-day scoring bonus and return rows to the standard LO review queue.",
    "suppression_or_disposition_review": "Treat all suppression/disposition recommendations as manual-only until evidence quality improves.",
    "lo_review_nurture": "Remove nurture recommendation scoring and return rows to generic review.",
  }.get(lane, "Return rows to the generic human-review queue.")


def _result_attribution_for_lane(lane: str) -> str:
  return {
    "contact_resolution_repair": "Compare contact-resolution queue counts and synthetic-contact fallbacks by build date.",
    "observer_repair": "Use reconstruction-gap counts and replay coverage rates by build date.",
    "owner_resolution": "Compare owner_source and transcript_backed_owner rates by build date.",
    "handoff_recovery": "Use transfer_result, appointment_result, and recovery-lane counts by build date.",
    "lo_handoff_preparation": "Compare booked/ready-handoff queue coverage and blocker counts by build date.",
    "lo_callback_priority": "Compare same-day urgency ordering and manager-approved queue placement by build date.",
    "suppression_or_disposition_review": "Compare disposition recommendation counts and manager overrides by build date.",
    "lo_review_nurture": "Compare nurture-versus-callback recommendation mix by build date.",
  }.get(lane, "Compare queue counts and review outcomes by build date.")


def _linked_recommendation_ids_for_lane(lane: str) -> list[str]:
  mapping = {
    "contact_resolution_repair": ["rsi-2026-04-29-001"],
    "observer_repair": ["rsi-2026-04-29-001"],
    "owner_resolution": ["rsi-2026-04-29-002"],
    "handoff_recovery": ["rsi-2026-04-29-003"],
    "lo_handoff_preparation": ["rsi-2026-04-29-002", "rsi-2026-04-29-003"],
    "lo_callback_priority": ["rsi-2026-04-29-003"],
  }
  return mapping.get(lane, [])


def _build_post_call_action_approval_queue(followup_queue: list[Mapping[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  queue: list[dict[str, Any]] = []
  lane_counter: Counter[str] = Counter()
  for row in followup_queue:
    lane = _action_approval_lane(row)
    estimated_amount = as_int(row.get("estimated_amount"))
    blockers = _dedupe_text(
      [
        *[str(item) for item in row.get("reconstruction_gap_reasons") or [] if str(item).strip()],
        "contact_resolution_review_required" if bool(row.get("contact_resolution_review_required")) else "",
        "owner_confirmation_required" if lane == "owner_resolution" else "",
        "failed_transfer" if str(row.get("transfer_status") or "").startswith("failed") else "",
        "appointment_fallback" if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error", "fallback_discussed"} else "",
      ]
    )
    approval_priority_score = (
      int(row.get("revenue_priority_score") or 0)
      + (180 if lane in {"handoff_recovery", "lo_callback_priority"} else 0)
      + (140 if lane in {"contact_resolution_repair", "observer_repair", "owner_resolution"} else 0)
      + (90 if estimated_amount >= 500000 else 40 if estimated_amount >= 250000 else 0)
    )
    queue.append(
      {
        "approval_id": stable_id("post_call_action_approval", str(row.get("call_id") or ""), lane),
        "call_id": str(row.get("call_id") or ""),
        "contact_id": str(row.get("actual_contact_id") or row.get("contact_state_id") or ""),
        "owner": _canonical_owner(str(row.get("owner") or "Unassigned LO Review")),
        "owner_source": str(row.get("owner_source") or ""),
        "approval_lane": lane,
        "approval_state": "review_required",
        "follow_up_urgency": str(row.get("follow_up_urgency") or ""),
        "route": str(row.get("route") or ""),
        "outcome": str(row.get("outcome") or ""),
        "estimated_amount": estimated_amount,
        "revenue_priority_score": int(row.get("revenue_priority_score") or 0),
        "approval_priority_score": approval_priority_score,
        "recommended_shadow_action": _recommended_action_for_lane(lane, row),
        "approval_gate": _approval_gate_for_lane(lane),
        "expected_impact": _expected_impact_for_lane(lane),
        "blocking_factors": blockers,
        "measurement_plan": _measurement_plan_for_lane(lane),
        "rollback_plan": _rollback_plan_for_lane(lane),
        "result_attribution": _result_attribution_for_lane(lane),
        "linked_recommendation_ids": _linked_recommendation_ids_for_lane(lane),
        "transcript_backed_owner": bool(row.get("transcript_backed_owner")),
        "transcript_available": bool(row.get("transcript_available")),
        "recording_available": bool(row.get("recording_available")),
        "transfer_status": str(row.get("transfer_status") or ""),
        "appointment_result": str(row.get("appointment_result") or ""),
        "contact_resolution_review_required": bool(row.get("contact_resolution_review_required")),
        "reconstruction_gap_reasons": list(row.get("reconstruction_gap_reasons") or []),
        "required_field_coverage_rate": float(row.get("required_field_coverage_rate") or 0.0),
        "reconstruction_readiness_score": int(row.get("reconstruction_readiness_score") or 0),
        "reconstruction_readiness_band": str(row.get("reconstruction_readiness_band") or "low"),
        "transcript_excerpt": str(row.get("transcript_excerpt") or ""),
        "call_summary_excerpt": str(row.get("call_summary_excerpt") or ""),
        "evidence_refs": list(row.get("evidence_refs") or []),
      }
    )
    lane_counter[lane] += 1
  queue.sort(
    key=lambda item: (
      int(item.get("approval_priority_score") or 0),
      int(item.get("estimated_amount") or 0),
    ),
    reverse=True,
  )
  summary = {
    "row_count": len(queue),
    "same_day_count": sum(1 for row in queue if str(row.get("follow_up_urgency") or "") == "same_day"),
    "high_value_count": sum(1 for row in queue if int(row.get("estimated_amount") or 0) >= 500000),
    "lane_counts": dict(lane_counter),
    "contact_resolution_repair_count": lane_counter.get("contact_resolution_repair", 0),
    "observer_repair_count": lane_counter.get("observer_repair", 0),
    "owner_resolution_count": lane_counter.get("owner_resolution", 0),
    "handoff_recovery_count": lane_counter.get("handoff_recovery", 0),
    "lo_followup_count": lane_counter.get("lo_handoff_preparation", 0) + lane_counter.get("lo_callback_priority", 0),
    "suppression_review_count": lane_counter.get("suppression_or_disposition_review", 0),
  }
  return summary, queue


def _render_post_call_action_approval_markdown(summary: Mapping[str, Any], queue: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Post-Call Action Approval Queue - 2026-04-29",
    "",
    "Shadow-only manager review queue turning actual-call evidence into approval-gated follow-up, recovery, and repair actions.",
    "",
    "## Summary",
    "",
    f"- Review rows: {summary['row_count']}",
    f"- Same-day rows: {summary['same_day_count']}",
    f"- High-value rows: {summary['high_value_count']}",
    f"- Contact resolution repairs: {summary['contact_resolution_repair_count']}",
    f"- Observer repairs: {summary['observer_repair_count']}",
    f"- Owner resolutions: {summary['owner_resolution_count']}",
    f"- Handoff recoveries: {summary['handoff_recovery_count']}",
    f"- LO follow-up actions: {summary['lo_followup_count']}",
    "",
    "## Priority Queue",
    "",
  ]
  for row in queue[:20]:
    lines.append(
      f"- {row['owner']}: {row['call_id']} | lane={row['approval_lane']} | score={row['approval_priority_score']} | readiness={row['reconstruction_readiness_score']} | urgency={row['follow_up_urgency']} | est_amount=${row['estimated_amount']:,} | action={row['recommended_shadow_action']}"
    )
  return "\n".join(lines).rstrip() + "\n"


def _approval_gate_for_readiness_lane(lane: str) -> str:
  return {
    "submission_packet_shadow": "Processor + LO review before any submission packet, LOS write, or borrower doc request.",
    "pricing_dry_run_shadow": "Human deal-desk fact check before any pricing claim, quote, or lender-facing action.",
    "application_start_shadow": "LO confirms borrower intent and authorization before any live application start or outreach.",
  }.get(lane, "Human review required before any external action.")


def _shadow_action_for_readiness_lane(lane: str) -> str:
  return {
    "submission_packet_shadow": "Prepare the internal submission packet checklist and secure-link-only draft request list.",
    "pricing_dry_run_shadow": "Stage an internal pricing dry run and verify missing scenario facts before any quote discussion.",
    "application_start_shadow": "Prepare an LO-reviewed app-start checklist and confirm borrower authorization prerequisites.",
  }.get(lane, "Review scenario gaps and stage the next internal-only action.")


def _build_pricing_app_submission_readiness_report(
  repo_root: Path,
  scenario_rows: list[dict[str, str]],
  followup_queue: list[Mapping[str, Any]],
  qa_rows: list[Mapping[str, Any]],
  last30_rows: list[Mapping[str, Any]],
  processing_queue: list[Mapping[str, Any]],
) -> dict[str, Any]:
  readiness_path = repo_root / "data" / "loan-os" / "submission-readiness" / "reactivation-readiness-2026-04-28.json"
  readiness_payload = _read_json(readiness_path, {})
  readiness_rows = readiness_payload.get("rows") if isinstance(readiness_payload, Mapping) else []
  readiness_rows = readiness_rows if isinstance(readiness_rows, list) else []
  eligibility_rows = read_csv(repo_root / "data" / "loan-os" / "eligibility" / "reactivation-eligibility-2026-04-28.csv")
  scenario_by_contact = {str(row.get("contact_id") or ""): row for row in scenario_rows if str(row.get("contact_id") or "")}
  eligibility_by_contact = {str(row.get("contact_id") or ""): row for row in eligibility_rows if str(row.get("contact_id") or "")}
  followup_by_contact = {str(row.get("contact_id") or ""): row for row in followup_queue if str(row.get("contact_id") or "")}
  last30_by_contact = {str(row.get("contact_id") or ""): row for row in last30_rows if str(row.get("contact_id") or "")}
  processing_by_contact = {str(row.get("contact_id") or ""): row for row in processing_queue if str(row.get("contact_id") or "")}
  qa_candidates: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  for row in qa_rows:
    contact_id = str(row.get("contact_id") or "")
    if contact_id:
      qa_candidates[contact_id].append(row)
  qa_by_contact: dict[str, Mapping[str, Any]] = {}
  for contact_id, items in qa_candidates.items():
    items.sort(
      key=lambda row: (
        1 if row.get("transcript_available") else 0,
        int(row.get("prospect_words") or 0),
        int(row.get("reconstruction_readiness_score") or 0),
      ),
      reverse=True,
    )
    qa_by_contact[contact_id] = items[0]

  rows: list[dict[str, Any]] = []
  lane_counts: Counter[str] = Counter()
  common_missing_items: Counter[str] = Counter()
  for raw_row in readiness_rows:
    if not isinstance(raw_row, Mapping):
      continue
    contact_id = str(raw_row.get("contact_id") or "").strip()
    if not contact_id:
      continue
    scenario = scenario_by_contact.get(contact_id, {})
    eligibility = eligibility_by_contact.get(contact_id, {})
    qa_row = qa_by_contact.get(contact_id, {})
    followup_row = followup_by_contact.get(contact_id, {})
    last30_row = last30_by_contact.get(contact_id, {})
    processing_row = processing_by_contact.get(contact_id, {})
    docs = raw_row.get("docs") if isinstance(raw_row.get("docs"), list) else []
    missing_items = [
      str(item.get("item") or "").strip()
      for item in docs
      if isinstance(item, Mapping) and str(item.get("item") or "").strip()
    ]

    readiness_score = as_int(raw_row.get("readiness_score"))
    revenue_score = as_int(eligibility.get("revenue_automation_score") or scenario.get("revenue_automation_score"))
    estimated_amount = as_int(
      raw_row.get("amount_signal") or eligibility.get("largest_amount") or scenario.get("largest_amount")
    )
    can_price = str(eligibility.get("can_price") or "").lower() == "true"
    can_transfer = str(eligibility.get("can_transfer") or "").lower() == "true"
    transcript_backed = bool(qa_row.get("transcript_available")) and int(qa_row.get("prospect_words") or 0) >= 20
    pricing_ready = can_price and readiness_score >= 80
    app_start_ready = can_transfer and readiness_score >= 70
    submission_packet_ready = pricing_ready and transcript_backed
    if not (pricing_ready or app_start_ready or submission_packet_ready):
      continue

    for item in missing_items:
      common_missing_items[item] += 1

    lane = (
      "submission_packet_shadow"
      if submission_packet_ready
      else "pricing_dry_run_shadow"
      if pricing_ready
      else "application_start_shadow"
    )
    lane_counts[lane] += 1
    follow_up_urgency = _first_nonempty(
      followup_row.get("follow_up_urgency"),
      "call_early" if str(last30_row.get("review_lane") or "") == "call_early" else "",
      default="review",
    )
    queue_priority_score = (
      revenue_score
      + (readiness_score * 6)
      + (180 if lane == "submission_packet_shadow" else 120 if lane == "pricing_dry_run_shadow" else 80)
      + (80 if transcript_backed else 0)
      + (50 if str(followup_row.get("follow_up_urgency") or "") == "same_day" else 25 if str(last30_row.get("review_lane") or "") == "call_early" else 0)
      + min(180, estimated_amount // 25000)
    )
    evidence_refs = _dedupe_text(
      [
        f"submission_readiness:{raw_row.get('scenario_id')}" if raw_row.get("scenario_id") else "",
        f"eligibility:{eligibility.get('scenario_id')}" if eligibility.get("scenario_id") else "",
        f"scenario:{scenario.get('scenario_id')}" if scenario.get("scenario_id") else "",
        f"followup:{followup_row.get('call_id')}" if followup_row.get("call_id") else "",
        f"qa:{qa_row.get('call_id')}" if qa_row.get("call_id") else "",
        f"last30:{contact_id}" if last30_row else "",
        f"processing:{processing_row.get('queue_id')}" if processing_row.get("queue_id") else "",
      ]
    )
    rows.append(
      {
        "queue_id": stable_id("pricing_app_submission_readiness", str(raw_row.get("scenario_id") or ""), contact_id, lane),
        "scenario_id": str(raw_row.get("scenario_id") or ""),
        "contact_id": contact_id,
        "first_name": str(raw_row.get("first_name") or scenario.get("first_name") or "").title(),
        "phone_redacted": redact_phone(str(scenario.get("phone") or "")),
        "owner": _canonical_owner(
          _first_nonempty(raw_row.get("suggested_owner"), scenario.get("suggested_owner"), eligibility.get("suggested_owner"), default="Unassigned LO Review")
        ),
        "goal": _first_nonempty(raw_row.get("goal"), eligibility.get("goal"), scenario.get("goal")),
        "state": _first_nonempty(raw_row.get("state"), eligibility.get("state"), scenario.get("state")),
        "property_type": _first_nonempty(raw_row.get("property_type"), eligibility.get("property_type"), scenario.get("property_type")),
        "credit_score": as_int(raw_row.get("credit_score") or eligibility.get("credit_score") or scenario.get("credit_score")),
        "estimated_amount": estimated_amount,
        "readiness_score": readiness_score,
        "revenue_automation_score": revenue_score,
        "queue_priority_score": queue_priority_score,
        "pricing_ready": pricing_ready,
        "app_start_ready": app_start_ready,
        "submission_packet_ready": submission_packet_ready,
        "recommended_shadow_lane": lane,
        "follow_up_urgency": follow_up_urgency,
        "actual_call_outcome": str(qa_row.get("outcome") or ""),
        "actual_call_transcript_backed": transcript_backed,
        "last30_review_lane": str(last30_row.get("review_lane") or ""),
        "processing_condition_present": bool(processing_row),
        "missing_high_priority_count": as_int(raw_row.get("missing_high_priority_count")),
        "missing_items": missing_items,
        "required_missing_fields": missing_items[:4],
        "approval_gate": _approval_gate_for_readiness_lane(lane),
        "shadow_action": _shadow_action_for_readiness_lane(lane),
        "evidence_refs": evidence_refs,
      }
    )

  lane_rank = {
    "submission_packet_shadow": 3,
    "pricing_dry_run_shadow": 2,
    "application_start_shadow": 1,
  }
  rows.sort(
    key=lambda row: (
      lane_rank.get(str(row.get("recommended_shadow_lane") or ""), 0),
      int(row.get("queue_priority_score") or 0),
      int(row.get("estimated_amount") or 0),
    ),
    reverse=True,
  )
  summary = {
    "row_count": len(rows),
    "pricing_ready_count": sum(1 for row in rows if row.get("pricing_ready")),
    "app_start_ready_count": sum(1 for row in rows if row.get("app_start_ready")),
    "submission_packet_ready_count": sum(1 for row in rows if row.get("submission_packet_ready")),
    "transcript_backed_count": sum(1 for row in rows if row.get("actual_call_transcript_backed")),
    "followup_backed_count": sum(1 for row in rows if str(row.get("follow_up_urgency") or "") in {"same_day", "call_early"}),
    "processing_condition_count": sum(1 for row in rows if row.get("processing_condition_present")),
    "lane_counts": dict(lane_counts),
    "top_10_estimated_amount_sum": sum(int(row.get("estimated_amount") or 0) for row in rows[:10]),
  }
  return {
    "summary": summary,
    "rows": rows,
    "common_missing_items": [
      {"item": item, "count": count}
      for item, count in common_missing_items.most_common(15)
    ],
  }


def _render_pricing_app_submission_readiness_markdown(report: Mapping[str, Any]) -> str:
  summary = report.get("summary") if isinstance(report.get("summary"), Mapping) else {}
  rows = report.get("rows") if isinstance(report.get("rows"), list) else []
  common_missing_items = report.get("common_missing_items") if isinstance(report.get("common_missing_items"), list) else []
  lines = [
    "# Pricing / App / Submission Readiness - 2026-04-29",
    "",
    "Shadow-only readiness ladder for leads and calls that are close enough for pricing review, app-start prep, or submission-packet work. This does not quote, submit, write LOS data, message borrowers, or stage external actions.",
    "",
    "## Summary",
    "",
    f"- Shadow readiness rows: {int(summary.get('row_count') or 0)}",
    f"- Pricing dry-run ready: {int(summary.get('pricing_ready_count') or 0)}",
    f"- Application-start ready: {int(summary.get('app_start_ready_count') or 0)}",
    f"- Submission-packet ready: {int(summary.get('submission_packet_ready_count') or 0)}",
    f"- Transcript-backed rows: {int(summary.get('transcript_backed_count') or 0)}",
    f"- Follow-up-backed rows: {int(summary.get('followup_backed_count') or 0)}",
    f"- Processing-condition overlap: {int(summary.get('processing_condition_count') or 0)}",
    f"- Top-10 estimated amount: ${int(summary.get('top_10_estimated_amount_sum') or 0):,}",
    "",
    "## Approval Gates",
    "",
    "- `submission_packet_shadow`: Processor + LO review before any submission packet, LOS write, or borrower document request.",
    "- `pricing_dry_run_shadow`: Human deal-desk fact check before any pricing claim, quote, or lender-facing action.",
    "- `application_start_shadow`: LO confirms borrower intent and authorization before any live application start or outreach.",
    "",
    "## Top Queue",
    "",
  ]
  for row in rows[:25]:
    lines.append(
      f"- {row['owner']}: {row['first_name']} {row['phone_redacted']} | lane={row['recommended_shadow_lane']} | score={row['queue_priority_score']} | est_amount=${int(row['estimated_amount']):,} | urgency={row['follow_up_urgency']} | missing={', '.join(row['required_missing_fields']) or 'none'}"
    )
  lines.extend(["", "## Common Missing Items", ""])
  for item in common_missing_items:
    if not isinstance(item, Mapping):
      continue
    lines.append(f"- {item.get('item')}: {item.get('count')}")
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
        "draft_processor_alert": f"Internal draft only: request secure-link upload for {', '.join(missing_facts[:3]) or 'missing borrower docs'}.",
        "approval_state": "review_required",
        "evidence_refs": _dedupe_text(
          [
            f"human_review:{row.get('review_id')}" if row.get("review_id") else "",
            f"scenario:{row.get('scenario_id')}" if row.get("scenario_id") else "",
          ]
        ),
      }
    )
  return queue


def _build_agent_scaffolds(
  management_summary: Mapping[str, Any],
  processing_queue: list[Mapping[str, Any]],
  followup_queue: list[Mapping[str, Any]],
  scenario_rows: list[dict[str, str]],
  qa_rows: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
  pricing_ready_count = sum(1 for row in scenario_rows if str(row.get("automation_stage") or "") == "pricing_ready_review")
  scaffold_metrics = {
    "Speed-to-Lead": {
      "seed_row_count": sum(1 for row in scenario_rows if str(row.get("automation_stage") or "") == "scenario_collection"),
      "primary_artifact": "scenario ledger + owner-capacity planning",
    },
    "Inbound Callback": {
      "seed_row_count": sum(1 for row in qa_rows if "inbound_callback" in str(row.get("campaign_context") or row.get("source_attribution") or "")),
      "primary_artifact": "actual shadow inbound/callback call states",
    },
    "LO Assistant": {
      "seed_row_count": len(followup_queue),
      "primary_artifact": "revenue-priority LO follow-up queue",
    },
    "No-Show Recovery": {
      "seed_row_count": len(management_summary["appointment_fallback_queue"]),
      "primary_artifact": "appointment fallback queue",
    },
    "Document/App Completion": {
      "seed_row_count": pricing_ready_count,
      "primary_artifact": "pricing-ready review rows",
    },
    "Processing Condition Follow-Up": {
      "seed_row_count": len(processing_queue),
      "primary_artifact": "processing queue",
    },
    "Senior Sales Data-Capture": {
      "seed_row_count": pricing_ready_count,
      "primary_artifact": "pricing-ready transcripts and objections",
    },
    "Revenue Prioritization": {
      "seed_row_count": len(followup_queue),
      "primary_artifact": "revenue-priority queue",
    },
    "Manager Strategy": {
      "seed_row_count": len(management_summary["observer_gap_queue"]),
      "primary_artifact": "management summary + RSI queue",
    },
    "Post-Call QA": {
      "seed_row_count": len(qa_rows),
      "primary_artifact": "actual shadow-call QA rows",
    },
  }
  output: list[dict[str, Any]] = []
  for item in AGENT_SCAFFOLDS:
    metrics = scaffold_metrics.get(str(item.get("agent_name") or ""), {})
    output.append(
      {
        **item,
        "seed_row_count": int(metrics.get("seed_row_count") or 0),
        "primary_artifact": str(metrics.get("primary_artifact") or "shadow_only"),
        "safety_mode": "shadow_only_no_external_writes",
        "approval_state": "review_required",
      }
    )
  output.sort(key=lambda row: (0 if int(row.get("seed_row_count") or 0) > 0 else 1, str(row.get("agent_name") or "")))
  return output


def _render_agent_scaffolds_markdown(scaffolds: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Agent Scaffolds - 2026-04-29",
    "",
    "All agents below remain shadow-only and non-live. No borrower-facing action or external write path is enabled here.",
    "",
  ]
  for item in scaffolds:
    lines.extend(
      [
        f"## {item['agent_name']}",
        "",
        f"- Queue: `{item['queue_name']}`",
        f"- Launch stage: `{item['launch_stage']}`",
        f"- Seed rows: {item['seed_row_count']}",
        f"- Primary artifact: {item['primary_artifact']}",
        f"- Purpose: {item['purpose']}",
        f"- Approval gate: {item['approval_gate']}",
        f"- Safety mode: {item['safety_mode']}",
        "",
      ]
    )
  return "\n".join(lines).rstrip() + "\n"


def _render_command_center_markdown(
  command_center: Mapping[str, Any],
  recommendations: list[Mapping[str, Any]],
  scaffolds: list[Mapping[str, Any]],
) -> str:
  lines = [
    "# Call Center OS Command Center - 2026-04-29",
    "",
    "Autonomous shadow command-center digest for the overnight lender build.",
    "",
    "## Snapshot",
    "",
    f"- Shadow calls replayed: {command_center['shadow_call_count']}",
    f"- Transcript coverage: {command_center['transcript_coverage_rate']:.1%}",
    f"- Recording coverage: {command_center['recording_coverage_rate']:.1%}",
    f"- Follow-up queue: {command_center['followup_queue_count']}",
    f"- High-urgency queue: {command_center['high_urgency_count']}",
    f"- Observer gap queue: {command_center['observer_gap_count']}",
    f"- No-conversation-expected calls: {command_center['observer_expected_absence_count']}",
    f"- Reconstruction review queue: {command_center['reconstruction_review_count']}",
    f"- Reconstruction audit blocked rows: {command_center['reconstruction_audit_blocked_count']}",
    f"- Contact resolution review queue: {command_center['contact_resolution_review_count']}",
    f"- Owner attribution review queue: {command_center['owner_review_count']}",
    f"- Post-call action approval queue: {command_center['action_approval_count']}",
    f"- Campaign cohorts: {command_center['campaign_cohort_count']}",
    f"- Profitability/readiness gap queue: {command_center['profitability_readiness_gap_count']}",
    f"- High-value unassigned owner rows: {command_center['high_value_unassigned_count']}",
    f"- Revenue attribution tracking keys: {command_center['attribution_tracking_key_count']}",
    f"- Revenue attribution priority gaps: {command_center['attribution_gap_count']}",
    f"- Actual calls linked to last-30 leads: {command_center['actual_call_linked_last30_count']}",
    f"- Last-30 leads linked to actual calls: {command_center['last30_linked_call_count']}",
    f"- Unattributed estimated amount: ${command_center['unattributed_estimated_amount_sum']:,}",
    f"- Pricing dry-run ready rows: {command_center['pricing_ready_count']}",
    f"- Application-start ready rows: {command_center['app_start_ready_count']}",
    f"- Submission-packet ready rows: {command_center['submission_packet_ready_count']}",
    f"- Last-30-day review queue: {command_center['last30_review_count']}",
    f"- Last-30-day call-early rows: {command_center['last30_call_early_count']}",
    f"- Last-30-day transcript-backed rows: {command_center['last30_transcript_backed_count']}",
    f"- Last-30-day top-50 estimated amount: ${command_center['last30_top_50_estimated_amount']:,}",
    "",
    "## Readiness Ladder",
    "",
    f"- Pricing dry-run shadow queue: {command_center['pricing_ready_count']}",
    f"- Application-start shadow queue: {command_center['app_start_ready_count']}",
    f"- Submission-packet shadow queue: {command_center['submission_packet_ready_count']}",
    "",
    "## Top RSI Recommendations",
    "",
  ]
  for item in recommendations[:5]:
    lines.append(f"- {item['recommendation_id']}: {item['recommendation']}")
  lines.extend(["", "## Agent Scaffolds With Seed Rows", ""])
  for item in scaffolds[:8]:
    lines.append(f"- {item['agent_name']}: seed_rows={item['seed_row_count']} | {item['primary_artifact']}")
  lines.extend(["", "## Dave Morning Review Order", ""])
  for index, step in enumerate(command_center["dave_review_order"], start=1):
    lines.append(f"{index}. {step}")
  return "\n".join(lines).rstrip() + "\n"


def _format_pct(value: Any) -> str:
  try:
    return f"{float(value) * 100:.1f}%"
  except (TypeError, ValueError):
    return "0.0%"


def _format_money(value: Any) -> str:
  amount = as_int(value)
  if amount <= 0:
    return "$0"
  return f"${amount:,}"


def _html_table(rows: list[Mapping[str, Any]], columns: list[tuple[str, str]]) -> str:
  header = "".join(f"<th>{html.escape(label)}</th>" for label, _ in columns)
  body_rows: list[str] = []
  for row in rows:
    cells = "".join(f"<td>{html.escape(str(row.get(key) or ''))}</td>" for _, key in columns)
    body_rows.append(f"<tr>{cells}</tr>")
  return (
    "<table>"
    f"<thead><tr>{header}</tr></thead>"
    f"<tbody>{''.join(body_rows) if body_rows else '<tr><td colspan=\"99\">No rows</td></tr>'}</tbody>"
    "</table>"
  )


def _build_capture_gap_root_cause(
  call_states: list[Mapping[str, Any]],
  qa_rows: list[Mapping[str, Any]],
  call_payload_by_id: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
  qa_by_call_id = {str(row.get("call_id") or ""): row for row in qa_rows if str(row.get("call_id") or "")}
  expected_rows: list[dict[str, Any]] = []
  true_gap_rows: list[dict[str, Any]] = []
  for row in call_states:
    call_id = str(row.get("call_id") or "")
    payload = call_payload_by_id.get(call_id, {})
    call = _call_from_payload(payload)
    metadata = call.get("metadata") if isinstance(call.get("metadata"), Mapping) else {}
    outcome = str(qa_by_call_id.get(call_id, {}).get("outcome") or row.get("derived_outcome") or "")
    normalized = {
      "call_id": call_id,
      "observer_capture_status": str(row.get("observer_capture_status") or ""),
      "outcome": outcome or "unknown",
      "call_status": str(row.get("call_status") or ""),
      "duration_seconds": int(row.get("duration_seconds") or 0),
      "prospect_words": int(row.get("prospect_words") or 0),
      "recording_available": bool(row.get("recording_available")),
      "transcript_available": bool(row.get("transcript_available")),
      "estimated_amount": int(row.get("estimated_amount") or 0),
      "owner": str(row.get("owner") or "Unassigned LO Review"),
      "campaign_context": str(
        row.get("campaign_context")
        or metadata.get("safe_batch_tag")
        or metadata.get("project")
        or "unknown"
      ),
      "agent_id": str(call.get("agent_id") or "unknown"),
      "agent_version": str(call.get("agent_version") or "unknown"),
      "raw_source_kinds": list(row.get("raw_source_kinds") or []),
      "manual_pull_present": "manual_pull" in list(row.get("raw_source_kinds") or []),
      "gap_reasons": [
        reason
        for reason in list(row.get("reconstruction_gap_reasons") or [])
        if reason in {"missing_transcript", "missing_recording"}
      ],
    }
    if normalized["observer_capture_status"] == "not_expected_no_conversation":
      expected_rows.append(normalized)
    elif normalized["observer_capture_status"] != "complete":
      true_gap_rows.append(normalized)

  expected_outcome_counts = Counter(str(row.get("outcome") or "unknown") for row in expected_rows)
  gap_status_counts = Counter(str(row.get("observer_capture_status") or "unknown") for row in true_gap_rows)
  gap_version_counts = Counter(str(row.get("agent_version") or "unknown") for row in true_gap_rows)
  gap_campaign_counts = Counter(str(row.get("campaign_context") or "unknown") for row in true_gap_rows)
  gap_rows = sorted(
    true_gap_rows,
    key=lambda item: (
      int(item.get("duration_seconds") or 0),
      int(item.get("estimated_amount") or 0),
      str(item.get("call_id") or ""),
    ),
    reverse=True,
  )
  action_list = [
    (
      f"Treat the {len(expected_rows)} `not_connected` no-conversation rows as expected shadow absences, "
      "not observer failures or launch blockers."
    ),
    (
      f"Prioritize transcript recovery only for the {len(gap_rows)} true observer gaps, starting with "
      f"{gap_rows[0]['call_id']} ({gap_rows[0]['duration_seconds']}s, version {gap_rows[0]['agent_version']})."
      if gap_rows
      else "No true observer gaps remain."
    ),
    (
      "Investigate transcript generation upstream for controlled-test calls that already have "
      "recordings plus `manual_pull` artifacts but still lack transcripts."
      if any(row.get("manual_pull_present") and row.get("recording_available") for row in gap_rows)
      else "No manual-pull transcript mismatch detected."
    ),
    (
      "Quarantine or separately audit legacy/no-metadata calls that lack `manual_pull` coverage "
      "before using them in observer-quality metrics."
      if any(not row.get("manual_pull_present") for row in gap_rows)
      else "All true gaps already have manual-pull coverage."
    ),
  ]
  summary = {
    "expected_no_conversation_count": len(expected_rows),
    "expected_no_answer_count": expected_outcome_counts.get("no_answer_or_short", 0),
    "expected_voicemail_count": expected_outcome_counts.get("voicemail", 0),
    "expected_other_count": len(expected_rows)
    - expected_outcome_counts.get("no_answer_or_short", 0)
    - expected_outcome_counts.get("voicemail", 0),
    "true_gap_count": len(gap_rows),
    "gap_with_recording_count": sum(1 for row in gap_rows if row.get("recording_available")),
    "gap_with_manual_pull_count": sum(1 for row in gap_rows if row.get("manual_pull_present")),
    "gap_status_counts": dict(gap_status_counts),
    "gap_agent_version_counts": dict(gap_version_counts),
    "gap_campaign_counts": dict(gap_campaign_counts),
    "expected_outcome_counts": dict(expected_outcome_counts),
    "action_list": action_list,
  }
  return summary, gap_rows, expected_rows


def _render_capture_gap_root_cause_markdown(
  summary: Mapping[str, Any],
  gap_rows: list[Mapping[str, Any]],
) -> str:
  lines = [
    "# Capture Gap Root Cause - 2026-04-29",
    "",
    "Read-only local diagnosis separating expected no-conversation calls from true observer capture gaps.",
    "",
    "## Separation",
    "",
    f"- Expected no-conversation rows: {summary['expected_no_conversation_count']}",
    f"- No-answer rows: {summary['expected_no_answer_count']}",
    f"- Voicemail rows: {summary['expected_voicemail_count']}",
    f"- Other expected-no-conversation rows: {summary['expected_other_count']}",
    f"- True observer capture gaps: {summary['true_gap_count']}",
    f"- True gaps with recording already present: {summary['gap_with_recording_count']}",
    f"- True gaps with manual-pull artifacts: {summary['gap_with_manual_pull_count']}",
    "",
    "## Affected Versions And Campaigns",
    "",
    f"- Agent versions: {summary['gap_agent_version_counts']}",
    f"- Campaigns: {summary['gap_campaign_counts']}",
    f"- Gap statuses: {summary['gap_status_counts']}",
    "",
    "## True Gap Details",
    "",
  ]
  for row in gap_rows[:10]:
    lines.append(
      "- "
      f"{row['call_id']} | version={row['agent_version']} | campaign={row['campaign_context']} | "
      f"status={row['observer_capture_status']} | duration={row['duration_seconds']}s | "
      f"recording={'yes' if row['recording_available'] else 'no'} | manual_pull={'yes' if row['manual_pull_present'] else 'no'} | "
      f"outcome={row['outcome']}"
    )
  lines.extend(["", "## Action List", ""])
  for item in summary["action_list"]:
    lines.append(f"1. {item}")
  return "\n".join(lines).rstrip() + "\n"


def _render_command_center_html(
  command_center: Mapping[str, Any],
  recommendations: list[Mapping[str, Any]],
  scaffolds: list[Mapping[str, Any]],
  actual_call_cohort_report: Mapping[str, Any],
  revenue_attribution_report: Mapping[str, Any],
  followup_queue: list[Mapping[str, Any]],
  owner_review_queue: list[Mapping[str, Any]],
  action_approval_queue: list[Mapping[str, Any]],
  last30_rows: list[Mapping[str, Any]],
  capture_gap_summary: Mapping[str, Any],
  capture_gap_rows: list[Mapping[str, Any]],
) -> str:
  kpis = [
    ("Shadow Calls", str(command_center["shadow_call_count"])),
    ("Transcript Coverage", _format_pct(command_center["transcript_coverage_rate"])),
    ("Recording Coverage", _format_pct(command_center["recording_coverage_rate"])),
    ("LO Follow-up Queue", str(command_center["followup_queue_count"])),
    ("Pricing Ready", str(command_center["pricing_ready_count"])),
    ("True Capture Gaps", str(capture_gap_summary["true_gap_count"])),
    ("Expected No-Conversation", str(capture_gap_summary["expected_no_conversation_count"])),
    ("Owner Review", str(command_center["owner_review_count"])),
    ("Action Approval", str(command_center["action_approval_count"])),
    ("Attribution Gaps", str(command_center["attribution_gap_count"])),
    ("Last-30 Call-Early", str(command_center["last30_call_early_count"])),
  ]
  kpi_html = "".join(
    "<article class='kpi'>"
    f"<div class='label'>{html.escape(label)}</div>"
    f"<div class='value'>{html.escape(value)}</div>"
    "</article>"
    for label, value in kpis
  )
  campaign_rows = []
  for row in list(actual_call_cohort_report.get("campaign_cohorts") or [])[:8]:
    campaign_rows.append(
      {
        "campaign": str(row.get("label") or "unknown"),
        "calls": int(row.get("call_count") or 0),
        "transcripts": int(row.get("transcript_count") or 0),
        "recordings": int(row.get("recording_count") or 0),
        "same_day": int(row.get("same_day_count") or 0),
        "est_amount": _format_money(row.get("estimated_amount_sum")),
      }
    )
  followup_rows = []
  for row in followup_queue[:12]:
    followup_rows.append(
      {
        "owner": str(row.get("owner") or ""),
        "lead": f"{row.get('first_name') or 'Lead'} {row.get('phone_redacted') or ''}".strip(),
        "amount": _format_money(row.get("estimated_amount")),
        "urgency": str(row.get("follow_up_urgency") or ""),
        "route": str(row.get("route") or ""),
        "coverage": str(row.get("evidence_coverage_score") or ""),
      }
    )
  owner_rows = []
  for row in owner_review_queue[:10]:
    owner_rows.append(
      {
        "owner": str(row.get("owner") or ""),
        "call_id": str(row.get("call_id") or ""),
        "amount": _format_money(row.get("estimated_amount")),
        "reason": ", ".join(list(row.get("review_reasons") or [])[:2]),
        "transcript": "yes" if row.get("transcript_available") else "no",
      }
    )
  action_rows = []
  for row in action_approval_queue[:10]:
    action_rows.append(
      {
        "call_id": str(row.get("call_id") or ""),
        "owner": str(row.get("owner") or ""),
        "lane": str(row.get("approval_lane") or ""),
        "amount": _format_money(row.get("estimated_amount")),
        "urgency": str(row.get("follow_up_urgency") or ""),
        "approval": str(row.get("approval_state") or ""),
      }
    )
  attribution_rows = []
  for row in list(revenue_attribution_report.get("priority_gap_queue") or [])[:12]:
    attribution_rows.append(
      {
        "type": str(row.get("row_type") or ""),
        "row_id": str(row.get("row_id") or ""),
        "tracking": str(row.get("tracking_key") or ""),
        "amount": _format_money(row.get("estimated_amount")),
        "gaps": ", ".join(list(row.get("gap_reasons") or [])[:3]),
      }
    )
  backlog_rows = []
  for row in last30_rows[:12]:
    backlog_rows.append(
      {
        "lead": str(row.get("first_name") or "Lead"),
        "tier": str(row.get("priority_tier") or ""),
        "amount": _format_money(row.get("estimated_largest_amount")),
        "lane": str(row.get("review_lane") or ""),
        "source": str(row.get("source_category") or ""),
        "transcripts": str(row.get("transcript_count") or "0"),
      }
    )
  recommendation_items = "".join(
    "<li>"
    f"<strong>{html.escape(str(item.get('recommendation_id') or ''))}</strong>: "
    f"{html.escape(str(item.get('recommendation') or ''))}"
    "</li>"
    for item in recommendations[:5]
  )
  review_items = "".join(
    f"<li>{html.escape(str(step))}</li>"
    for step in command_center["dave_review_order"]
  )
  scaffold_items = "".join(
    "<li>"
    f"<strong>{html.escape(str(item.get('agent_name') or ''))}</strong> "
    f"({html.escape(str(item.get('launch_stage') or ''))})"
    f" - {int(item.get('seed_row_count') or 0)} seed rows"
    "</li>"
    for item in scaffolds[:8]
  )
  return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Call Center OS Command Center - 2026-04-29</title>
  <style>
    :root {{
      --bg: #f3efe6;
      --panel: rgba(255, 251, 245, 0.92);
      --ink: #1f1d1a;
      --muted: #6d655c;
      --accent: #8f3d21;
      --accent-soft: #f3d6c7;
      --line: rgba(31, 29, 26, 0.12);
      --ok: #2f6b3b;
      --warn: #8b5a12;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(143, 61, 33, 0.14), transparent 28%),
        radial-gradient(circle at top right, rgba(47, 107, 59, 0.12), transparent 24%),
        linear-gradient(180deg, #faf7f1 0%, var(--bg) 100%);
    }}
    .wrap {{ max-width: 1360px; margin: 0 auto; padding: 32px 20px 48px; }}
    .hero {{
      background: linear-gradient(135deg, rgba(255,255,255,0.82), rgba(243,214,199,0.9));
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 18px 48px rgba(31, 29, 26, 0.08);
    }}
    .eyebrow {{
      letter-spacing: 0.14em;
      text-transform: uppercase;
      font-size: 12px;
      color: var(--accent);
      margin-bottom: 10px;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: clamp(30px, 5vw, 52px);
      line-height: 1;
    }}
    .sub {{
      max-width: 880px;
      color: var(--muted);
      font-size: 18px;
      line-height: 1.5;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 14px;
      margin: 22px 0 0;
    }}
    .kpi, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 18px;
      box-shadow: 0 14px 32px rgba(31, 29, 26, 0.05);
      backdrop-filter: blur(6px);
    }}
    .kpi .label {{
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .kpi .value {{
      margin-top: 8px;
      font-size: 30px;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 18px;
      margin-top: 18px;
    }}
    .stack {{
      display: grid;
      gap: 18px;
    }}
    h2, h3 {{
      margin: 0 0 12px;
      font-size: 24px;
    }}
    p, li {{
      color: var(--muted);
      line-height: 1.5;
    }}
    ul, ol {{
      margin: 0;
      padding-left: 18px;
    }}
    .chip {{
      display: inline-block;
      margin: 6px 8px 0 0;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 13px;
    }}
    .warn {{ color: var(--warn); }}
    .ok {{ color: var(--ok); }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}
    .section-title {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 10px;
    }}
    .section-title span {{
      color: var(--muted);
      font-size: 14px;
    }}
    @media (max-width: 980px) {{
      .grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="eyebrow">Evolve Funding · Shadow Only · 2026-04-29</div>
      <h1>Call Center OS Command Center</h1>
      <div class="sub">
        Read-only management and LO review dashboard built from shadow call-center OS artifacts.
        No borrower contact details, GHL writes, LOS writes, or live actions are exposed here.
      </div>
      <div class="kpis">{kpi_html}</div>
    </section>

    <div class="grid">
      <div class="stack">
        <section class="panel">
          <div class="section-title">
            <h2>Management Snapshot</h2>
            <span>{html.escape(str(command_center.get('generated_at') or ''))}</span>
          </div>
          <div class="chip">Campaign cohorts: {int(command_center['campaign_cohort_count'])}</div>
          <div class="chip">Profitability/readiness gaps: {int(command_center['profitability_readiness_gap_count'])}</div>
          <div class="chip">Attribution gaps: {int(command_center['attribution_gap_count'])}</div>
          <div class="chip">Linked actual calls: {int(command_center['actual_call_linked_last30_count'])}</div>
          <div class="chip">Blocked reconstruction rows: {int(command_center['reconstruction_audit_blocked_count'])}</div>
          <div class="chip">App-start ready: {int(command_center['app_start_ready_count'])}</div>
          <div class="chip">Submission-packet ready: {int(command_center['submission_packet_ready_count'])}</div>
          <div class="chip">Top-50 last-30 amount: {_format_money(command_center['last30_top_50_estimated_amount'])}</div>
          <div class="chip">Unattributed amount: {_format_money(command_center['unattributed_estimated_amount_sum'])}</div>
          <div style="margin-top:16px">{_html_table(campaign_rows, [('Campaign', 'campaign'), ('Calls', 'calls'), ('Transcripts', 'transcripts'), ('Recordings', 'recordings'), ('Same Day', 'same_day'), ('Est Amount', 'est_amount')])}</div>
        </section>

        <section class="panel">
          <div class="section-title">
            <h2>LO Follow-up Review</h2>
            <span>PII-safe top shadow queue</span>
          </div>
          {_html_table(followup_rows, [('Owner', 'owner'), ('Lead', 'lead'), ('Amount', 'amount'), ('Urgency', 'urgency'), ('Route', 'route'), ('Coverage', 'coverage')])}
        </section>

        <section class="panel">
          <div class="section-title">
            <h2>Backlog Ladder</h2>
            <span>{int(command_center['last30_review_count'])} rows</span>
          </div>
          {_html_table(backlog_rows, [('Lead', 'lead'), ('Tier', 'tier'), ('Amount', 'amount'), ('Lane', 'lane'), ('Source', 'source'), ('Transcripts', 'transcripts')])}
        </section>

        <section class="panel">
          <div class="section-title">
            <h2>Revenue Attribution Gaps</h2>
            <span>{int(command_center['attribution_gap_count'])} rows</span>
          </div>
          {_html_table(attribution_rows, [('Type', 'type'), ('Row ID', 'row_id'), ('Tracking', 'tracking'), ('Amount', 'amount'), ('Gaps', 'gaps')])}
        </section>
      </div>

      <div class="stack">
        <section class="panel">
          <div class="section-title">
            <h2>Observer Root Cause</h2>
            <span>Local read-only diagnosis</span>
          </div>
          <p><strong>{int(capture_gap_summary['expected_no_conversation_count'])}</strong> rows are expected no-conversation outcomes. <strong>{int(capture_gap_summary['true_gap_count'])}</strong> rows remain as true observer gaps.</p>
          <p class="warn">True gap versions: {html.escape(str(capture_gap_summary['gap_agent_version_counts']))}</p>
          <p class="warn">True gap campaigns: {html.escape(str(capture_gap_summary['gap_campaign_counts']))}</p>
          {_html_table(capture_gap_rows[:10], [('Call ID', 'call_id'), ('Version', 'agent_version'), ('Campaign', 'campaign_context'), ('Status', 'observer_capture_status'), ('Duration', 'duration_seconds')])}
        </section>

        <section class="panel">
          <div class="section-title">
            <h2>Owner Review</h2>
            <span>{int(command_center['owner_review_count'])} rows</span>
          </div>
          {_html_table(owner_rows, [('Owner', 'owner'), ('Call ID', 'call_id'), ('Amount', 'amount'), ('Reason', 'reason'), ('Transcript', 'transcript')])}
        </section>

        <section class="panel">
          <div class="section-title">
            <h2>Action Approval Queue</h2>
            <span>{int(command_center['action_approval_count'])} rows</span>
          </div>
          {_html_table(action_rows, [('Call ID', 'call_id'), ('Owner', 'owner'), ('Lane', 'lane'), ('Amount', 'amount'), ('Urgency', 'urgency'), ('Approval', 'approval')])}
        </section>

        <section class="panel">
          <h3>Top RSI Recommendations</h3>
          <ul>{recommendation_items}</ul>
        </section>

        <section class="panel">
          <h3>Dave Morning Review Order</h3>
          <ol>{review_items}</ol>
        </section>

        <section class="panel">
          <h3>Shadow Agent Lanes</h3>
          <ul>{scaffold_items}</ul>
        </section>
      </div>
    </div>
  </div>
</body>
</html>
"""


def _render_consolidated_morning_management_packet(
  command_center: Mapping[str, Any],
  followup_queue: list[Mapping[str, Any]],
  actual_call_cohort_report: Mapping[str, Any],
  attribution_report: Mapping[str, Any],
  readiness_report: Mapping[str, Any],
  processing_queue: list[Mapping[str, Any]],
  owner_review_queue: list[Mapping[str, Any]],
  action_approval_queue: list[Mapping[str, Any]],
  last30_rows: list[Mapping[str, Any]],
  recommendations: list[Mapping[str, Any]],
) -> str:
  owner_summary = _build_actual_shadow_owner_summary(followup_queue)
  readiness_rows = [
    dict(row)
    for row in list(readiness_report.get("rows") or [])
    if isinstance(row, Mapping)
  ]
  readiness_summary = readiness_report.get("summary") if isinstance(readiness_report, Mapping) else {}
  readiness_summary = readiness_summary if isinstance(readiness_summary, Mapping) else {}
  attribution_summary = attribution_report.get("summary") if isinstance(attribution_report, Mapping) else {}
  attribution_summary = attribution_summary if isinstance(attribution_summary, Mapping) else {}
  cohort_summary = actual_call_cohort_report.get("summary") if isinstance(actual_call_cohort_report, Mapping) else {}
  cohort_summary = cohort_summary if isinstance(cohort_summary, Mapping) else {}
  action_summary = {
    "same_day_count": sum(1 for row in action_approval_queue if str(row.get("follow_up_urgency") or "") == "same_day"),
    "review_queue_count": sum(1 for row in action_approval_queue if str(row.get("follow_up_urgency") or "") == "review_queue"),
    "repair_count": sum(
      1
      for row in action_approval_queue
      if str(row.get("approval_lane") or "") in {"observer_repair", "contact_resolution_repair", "owner_resolution"}
    ),
  }
  top_followup_rows = followup_queue[:5]
  top_readiness_rows = readiness_rows[:5]
  top_processing_rows = [dict(row) for row in processing_queue[:5] if isinstance(row, Mapping)]
  top_last30_rows = [dict(row) for row in last30_rows[:5] if isinstance(row, Mapping)]
  top_action_rows = [dict(row) for row in action_approval_queue[:5] if isinstance(row, Mapping)]
  top_gap_rows = [
    dict(row)
    for row in list(attribution_report.get("priority_gap_queue") or [])[:5]
    if isinstance(row, Mapping)
  ]
  lines = [
    "# Consolidated Morning Management Packet - 2026-04-29",
    "",
    "Shadow-only operating plan for Dave. This packet compares the existing queues, prioritizes the safest money-forward work, and does not authorize live calls, borrower outreach, GHL writes, LOS writes, or workflow changes.",
    "",
    "## Executive Snapshot",
    "",
    f"- Actual shadow calls in replay: {int(command_center.get('shadow_call_count') or 0)}",
    f"- True observer capture gaps: {int(command_center.get('observer_gap_count') or 0)}",
    f"- Reconstruction review rows: {int(command_center.get('reconstruction_review_count') or 0)}",
    f"- LO follow-up queue: {int(command_center.get('followup_queue_count') or 0)}",
    f"- Owner attribution review queue: {int(command_center.get('owner_review_count') or 0)}",
    f"- Post-call action approval queue: {int(command_center.get('action_approval_count') or 0)}",
    f"- Pricing/app/submission readiness rows: {int(readiness_summary.get('row_count') or 0)}",
    f"- Processor condition follow-up rows: {len(processing_queue)}",
    f"- Last-30 call-early backlog: {int(command_center.get('last30_call_early_count') or 0)}",
    f"- Revenue attribution gaps: {int(attribution_summary.get('gap_row_count') or 0)} rows covering ${int(attribution_summary.get('unattributed_estimated_amount_sum') or 0):,}",
    "",
    "## Queue Comparison",
    "",
    "| Queue | Count | Money Signal | What It Means Today |",
    "|---|---:|---:|---|",
    f"| Actual-call LO follow-up | {int(command_center.get('followup_queue_count') or 0)} | ${sum(as_int(row.get('estimated_amount')) for row in top_followup_rows):,} top-5 | Warmest replay-backed opportunities already tied to actual calls. |",
    f"| Owner attribution review | {len(owner_review_queue)} | ${sum(as_int(row.get('estimated_amount')) for row in owner_review_queue[:10]):,} top-10 | Ownership evidence is still too weak for automated LO assistance. |",
    f"| Post-call action approval | {len(action_approval_queue)} | {action_summary['same_day_count']} same-day | Most rows still need repair or manager confirmation before any routing trust. |",
    f"| Pricing/app/submission readiness | {int(readiness_summary.get('row_count') or 0)} | ${int(readiness_summary.get('top_10_estimated_amount_sum') or 0):,} top-10 | Best internal deal-desk and submission-prep lane if facts can be completed safely. |",
    f"| Processor conditions | {len(processing_queue)} | {int(readiness_summary.get('processing_condition_count') or 0)} overlaps | Internal condition queue is ready for secure-link-only processor review. |",
    f"| Last-30 backlog ladder | {int(command_center.get('last30_review_count') or 0)} | ${int(command_center.get('last30_top_50_estimated_amount') or 0):,} top-50 | Larger prospecting backlog, but less immediate than replay-backed call opportunities. |",
    f"| Revenue attribution gaps | {int(attribution_summary.get('gap_row_count') or 0)} | ${int(attribution_summary.get('unattributed_estimated_amount_sum') or 0):,} | Reporting gap, not first-call-center action gap. Fix after queue operations are understood. |",
    "",
    "## Operating Plan",
    "",
    "1. Repair trust in the observer before trusting any automation.",
    f"   Start with {int(command_center.get('observer_gap_count') or 0)} true capture gaps, {int(command_center.get('contact_resolution_review_count') or 0)} contact-resolution repairs, and {int(command_center.get('reconstruction_review_count') or 0)} replay-review rows.",
    f"   The most likely affected agent versions remain {command_center.get('gap_agent_versions') or 'unknown'}, so use `CAPTURE_GAP_ROOT_CAUSE_2026-04-29.md` before touching follow-up logic.",
    "2. Work the actual-call money queue before the last-30 backlog.",
    f"   The actual-call cohort still has {int(cohort_summary.get('same_day_queue_count') or 0)} same-day rows and {int(cohort_summary.get('profitability_readiness_gap_count') or 0)} profitability-versus-readiness gaps.",
    "3. Keep owner proof ahead of LO automation.",
    f"   There are {len(owner_review_queue)} owner-review rows and {action_summary['repair_count']} repair-oriented action approvals, so LO Assistant should stay shadow-only.",
    "4. Push internal deal-desk and processor review in parallel.",
    f"   Readiness already exposes {int(readiness_summary.get('pricing_ready_count') or 0)} pricing-ready rows and {int(readiness_summary.get('submission_packet_ready_count') or 0)} submission-packet rows, while processing has {len(processing_queue)} secure-link-only follow-up candidates.",
    "5. Treat attribution as a management reporting backlog, not a morning execution blocker.",
    f"   There are still {int(attribution_summary.get('actual_call_linked_last30_count') or 0)} actual calls linked back to the last-30 queue, so source truth is not ready for revenue-grade optimization yet.",
    "",
    "## Top Actual-Call Follow-Up Rows",
    "",
  ]
  for row in top_followup_rows:
    lines.append(
      f"- {row.get('owner')}: {row.get('phone_redacted')} | outcome={row.get('outcome')} | route={row.get('route')} | est_amount=${as_int(row.get('estimated_amount')):,} | evidence={row.get('evidence_summary')}"
    )
  lines.extend(["", "## Top Readiness Rows", ""])
  for row in top_readiness_rows:
    lines.append(
      f"- {row.get('owner')}: {row.get('first_name')} {row.get('phone_redacted')} | lane={row.get('recommended_shadow_lane')} | est_amount=${as_int(row.get('estimated_amount')):,} | missing={', '.join(list(row.get('missing_items') or [])[:4])}"
    )
  lines.extend(["", "## Top Processor Condition Rows", ""])
  for row in top_processing_rows:
    missing_items = ", ".join(list(row.get("missing_facts") or [])[:4])
    lines.append(
      f"- {row.get('owner')}: {row.get('contact_id')} | {row.get('condition_summary')} | missing={missing_items or 'none'}"
    )
  lines.extend(["", "## Top Action-Approval Repairs", ""])
  for row in top_action_rows:
    lines.append(
      f"- {row.get('owner')}: {row.get('call_id')} | lane={row.get('approval_lane')} | urgency={row.get('follow_up_urgency')} | est_amount=${as_int(row.get('estimated_amount')):,}"
    )
  lines.extend(["", "## Top Last-30 Call-Early Rows", ""])
  for row in top_last30_rows:
    lines.append(
      f"- {str(row.get('first_name') or '').title()}: {redact_phone(str(row.get('phone') or ''))} | tier={row.get('priority_tier')} | est_amount=${as_int(row.get('estimated_largest_amount')):,} | source={row.get('source_category')}"
    )
  lines.extend(["", "## Top Attribution Gap Rows", ""])
  for row in top_gap_rows:
    lines.append(
      f"- {row.get('row_type')}: {row.get('tracking_key')} | owner={row.get('owner')} | est_amount=${as_int(row.get('estimated_amount')):,} | gaps={', '.join(list(row.get('gap_reasons') or [])[:3])}"
    )
  lines.extend(["", "## LO Focus", ""])
  for row in owner_summary[:4]:
    lines.append(
      f"- {row.get('owner')}: {row.get('followup_count')} rows | {row.get('transcript_backed_owner_count')} transcript-backed | est top-5=${as_int(row.get('est_revenue_top5')):,}"
    )
  lines.extend(["", "## Dave Morning Review Order", ""])
  lines.extend(
    [
      "1. `CAPTURE_GAP_ROOT_CAUSE_2026-04-29.md` and `RECONSTRUCTION_AUDIT_2026-04-29.md`",
      "2. `OWNER_ATTRIBUTION_REVIEW_SHADOW_2026-04-29.md` and `POST_CALL_ACTION_APPROVAL_SHADOW_2026-04-29.md`",
      "3. `LO_FOLLOWUP_SCOREBOARD_SHADOW_2026-04-29.md` and `ACTUAL_CALL_COHORT_REPORT_2026-04-29.md`",
      "4. `PRICING_APP_SUBMISSION_READINESS_2026-04-29.md` and `PROCESSING_CONDITION_FOLLOWUP_SHADOW_2026-04-29.md`",
      "5. `LAST30_LEAD_REVIEW_QUEUE_SHADOW_2026-04-29.md` and `REVENUE_ATTRIBUTION_GAP_REPORT_2026-04-29.md`",
      "6. `CALL_CENTER_OS_COMMAND_CENTER_2026-04-29.md` and `call-center-os-command-center-2026-04-29.html`",
      "",
      "## Top RSI Recommendations",
      "",
    ]
  )
  for item in recommendations[:3]:
    lines.append(f"- {item['recommendation_id']}: {item['recommendation']}")
  return "\n".join(lines).rstrip() + "\n"


def _render_autonomous_packet(
  files_changed: list[str],
  checks: list[str],
  summary: Mapping[str, Any],
  recommendations: list[Mapping[str, Any]],
) -> str:
  lines = [
    "# Autonomous Packet - 2026-04-29",
    "",
    "## Executive Summary",
    "",
    "This autonomous pass hardened replay/state reconstruction, pushed actual shadow-call evidence through QA and management reporting, added governed RSI mechanics, expanded safe agent scaffolds, and refreshed the command-center review flow. All artifacts remain shadow-only and do not launch calls or write to external systems.",
    "",
    "## What Changed",
    "",
    f"- Replay now covers {summary['shadow_call_count']} actual shadow calls with transcript, recording, transfer, appointment, owner, evidence, and confidence fields on every call state.",
    f"- Contact reconstruction now isolates {summary['contact_resolution_review_count']} calls for identity review and preserves phone-backed resolution evidence before synthetic fallback.",
    f"- Source-aware observer review now separates {summary['observer_expected_absence_count']} expected no-conversation calls from {summary['observer_gap_count']} true capture gaps.",
    f"- Capture-gap root cause now proves {summary['expected_no_answer_count']} of those expected absences are no-answer rows and isolates true gaps to agent versions {summary['gap_agent_versions']} for targeted follow-up.",
    f"- Reconstruction review now isolates {summary['reconstruction_review_count']} shadow calls whose replay still needs owner, transfer, appointment, or actual capture-gap review.",
    f"- Reconstruction audit now gives deterministic readiness verdicts for {summary['shadow_call_count']} shadow calls and isolates {summary['reconstruction_audit_blocked_count']} blocked rows with missing schema or timeline coverage.",
    f"- QA now exposes {summary['qa_row_count']} actual shadow-call rows with transcript excerpts, owner source, transfer and appointment fields, and evidence references.",
    f"- Follow-up reporting now ranks {summary['followup_queue_count']} rows by profitability, readiness, evidence coverage, contact engagement, follow-up urgency, and transcript-backed owner evidence.",
    f"- Actual-call cohort reporting now summarizes {summary['campaign_cohort_count']} campaigns and surfaces {summary['profitability_readiness_gap_count']} profitability-versus-readiness gap rows from real shadow calls.",
    f"- Revenue attribution reporting now ties {summary['actual_call_linked_last30_count']} actual shadow calls to the last-30 lead queue, exposes {summary['attribution_tracking_key_count']} tracking keys, and isolates {summary['attribution_gap_count']} attribution gap rows covering ${summary['unattributed_estimated_amount_sum']:,} in estimated amount.",
    f"- Pricing/app/submission readiness now isolates {summary['readiness_shadow_count']} shadow rows, with {summary['pricing_ready_count']} pricing-ready, {summary['app_start_ready_count']} app-start-ready, and {summary['submission_packet_ready_count']} submission-packet-ready lanes under human approval gates.",
    f"- Owner attribution review now isolates {summary['owner_review_count']} shadow rows that still need transcript-backed or manager-confirmed owner resolution.",
    f"- Post-call action approval now isolates {summary['action_approval_count']} manager-review rows with approval gates, measurement plans, and rollback paths before any live routing discussion.",
    f"- RSI now emits {summary['rsi_recommendation_count']} governed recommendations with approval gate, measurement plan, rollback, and attribution fields.",
    f"- Safe shadow scaffolds refreshed for {summary['agent_scaffold_count']} agent lanes, including Speed-to-Lead, Inbound Callback, LO Assistant, No-Show Recovery, Document/App Completion, Processing Conditions, and Senior Sales data-capture.",
    f"- The last-30-day backlog ladder now exposes {summary['last30_review_count']} shadow review rows, {summary['last30_call_early_count']} call-early priorities, and ${summary['last30_top_50_estimated_amount']:,} estimated amount across the top 50 ranked rows.",
    "- A local HTML command center now mirrors the shadow command-center packet with management, LO, observer-gap, and backlog sections while keeping contact presentation redacted.",
    "- A consolidated morning management packet now compares the queues and turns the shadow bundle into a simple operating plan for Dave's day.",
    "",
    "## Files Changed",
    "",
  ]
  lines.extend(f"- `{path}`" for path in files_changed)
  lines.extend(["", "## Checks", ""])
  lines.extend(f"- {item}" for item in checks)
  lines.extend(["", "## Dave Morning Review Order", ""])
  lines.extend(
    [
      "1. Review `EVENT_REPLAY_CURRENT_STATE_2026-04-29.md` for owner coverage, transcript coverage, phone-backed contact resolution, and remaining synthetic contact fallbacks.",
      "2. Review `OBSERVER_CAPTURE_REVIEW_2026-04-29.md` and `CAPTURE_GAP_ROOT_CAUSE_2026-04-29.md` for true capture gaps versus expected no-conversation calls.",
      "3. Review `RECONSTRUCTION_AUDIT_2026-04-29.md` for blocked or review-required shadow calls and exact replay/timeline gap reasons.",
      "4. Review `CONTACT_RESOLUTION_REVIEW_SHADOW_2026-04-29.md` for calls that still need stronger identity resolution before promotion.",
      "5. Review the reconstruction review queue inside `management-report-continuation-shadow-2026-04-29.json` for calls that still have replay evidence gaps.",
      "6. Review `POST_CALL_QA_CONTINUATION_2026-04-29.md` for top shadow-call QA rows and confirm the evidence looks trustworthy.",
      "7. Review `ACTUAL_CALL_COHORT_REPORT_2026-04-29.md` and `LO_FOLLOWUP_SCOREBOARD_SHADOW_2026-04-29.md` for campaign patterns, revenue ordering, and urgency.",
      "8. Review `REVENUE_ATTRIBUTION_GAP_REPORT_2026-04-29.md` plus `revenue-attribution-gap-shadow-2026-04-29.csv` for safe-batch coverage, missing campaign/project/purpose fields, and unattributed revenue gaps.",
      "9. Review `PRICING_APP_SUBMISSION_READINESS_2026-04-29.md` plus `pricing-app-submission-readiness-shadow-2026-04-29.csv` for pricing, app-start, and submission-packet shadow lanes and their approval gates.",
      "10. Review `LAST30_LEAD_REVIEW_QUEUE_SHADOW_2026-04-29.md` for the shadow backlog ladder, call-early rows, and transcript-backed reactivation coverage.",
      "11. Review `OWNER_ATTRIBUTION_REVIEW_SHADOW_2026-04-29.md` for high-value owner resolution gaps and failed-transfer ownership review.",
      "12. Review `POST_CALL_ACTION_APPROVAL_SHADOW_2026-04-29.md` for the manager-review action queue before any workflow or staging change is discussed.",
      "13. Review `CONSOLIDATED_MORNING_MANAGEMENT_PACKET_2026-04-29.md` for the queue comparison and day plan.",
      "14. Review `MANAGEMENT_REPORT_CONTINUATION_2026-04-29.md`, `RSI_RECOMMENDATION_QUEUE_CONTINUATION_2026-04-29.md`, `AGENT_SCAFFOLDS_SHADOW_2026-04-29.md`, `CALL_CENTER_OS_COMMAND_CENTER_2026-04-29.md`, and `call-center-os-command-center-2026-04-29.html` before any workflow or staging change.",
      "",
      "## Top RSI Recommendations",
      "",
    ]
  )
  for item in recommendations[:5]:
    lines.append(f"- {item['recommendation_id']}: {item['recommendation']}")
  return "\n".join(lines).rstrip() + "\n"


def _load_existing_extension_bundle(out_dir: Path) -> dict[str, Any]:
  return {
    "replay": _read_json(out_dir / "event-replay-shadow-2026-04-29.json", {}),
    "reconstruction_audit": _read_json(out_dir / "reconstruction-audit-shadow-2026-04-29.json", {}),
    "capture_gap": _read_json(out_dir / "capture-gap-root-cause-shadow-2026-04-29.json", {}),
    "qa": _read_json(out_dir / "post-call-qa-continuation-shadow-2026-04-29.json", {}),
    "followup": _read_json(out_dir / "lo-followup-scoreboard-shadow-2026-04-29.json", {}),
    "actual_call_cohort": _read_json(out_dir / "actual-call-cohort-report-shadow-2026-04-29.json", {}),
    "owner_review": _read_json(out_dir / "owner-attribution-review-shadow-2026-04-29.json", {}),
    "action_approval": _read_json(out_dir / "post-call-action-approval-shadow-2026-04-29.json", {}),
    "rsi": _read_json(out_dir / "rsi-recommendation-queue-continuation-shadow-2026-04-29.json", {}),
    "last30": _read_json(out_dir / "last30-lead-review-queue-shadow-2026-04-29.json", {}),
    "scaffolds": _read_json(out_dir / "agent-scaffolds-shadow-2026-04-29.json", {}),
  }


def build_autonomous_artifacts(
  repo_root: Path | None = None,
  out_dir: Path | None = None,
  progress_path: Path | None = None,
) -> AutonomousArtifactsResult:
  repo_root = repo_root or REPO_ROOT
  out_dir = out_dir or CALL_CENTER_OS_DIR
  progress_path = progress_path or out_dir / "AUTONOMOUS_PROGRESS_2026-04-29.md"
  call_dir = repo_root / "data" / "voice-agent" / "retell" / "calls"
  input_files = _collect_autonomous_input_files(repo_root, call_dir)
  fingerprint_payload = _fingerprint_inputs(repo_root, input_files)
  should_skip, decision_reason, existing_manifest = _should_skip_autonomous_build(repo_root, out_dir, fingerprint_payload)
  if should_skip:
    summary = existing_manifest.get("summary") if isinstance(existing_manifest.get("summary"), Mapping) else {}
    checks = [
      "Skipped rebuild: unchanged source inputs and required artifacts already present.",
      f"Idempotency guard fingerprint={fingerprint_payload['fingerprint'][:12]}",
    ]
    _append_progress(
      progress_path,
      step="Evaluate autonomous input fingerprint",
      outcome=(
        "Skip unchanged source inputs because the build version, input fingerprint, and required artifacts "
        f"already match ({fingerprint_payload['file_count']} input files; fingerprint={fingerprint_payload['fingerprint'][:12]})."
      ),
      files=["none"],
    )
    return AutonomousArtifactsResult(files_changed=[], checks=checks, summary=dict(summary))

  _append_progress(
    progress_path,
    step="Evaluate autonomous input fingerprint",
    outcome=(
      "Extend autonomous work because "
      f"{decision_reason or 'initial_build'} "
      f"({fingerprint_payload['file_count']} input files; fingerprint={fingerprint_payload['fingerprint'][:12]})."
    ),
    files=["none"],
  )

  scenario_rows = read_csv(repo_root / "data" / "loan-os" / "scenarios" / "reactivation-scenario-ledger-2026-04-28.csv")
  review_rows = read_csv(repo_root / "data" / "loan-os" / "human-review" / "human-review-queue-2026-04-28.csv")
  post_call_review_rows = read_csv(repo_root / "data" / "loan-os" / "post-call-review" / "post-call-review-packet-2026-04-28.csv")
  scoreboard_seed_rows = read_csv(
    repo_root / "data" / "voice-agent" / "reactivation-enrichment" / "launch-batch-2026-04-28.post-call-scoreboard.csv"
  )
  last30_seed_payload = _read_json(
    repo_root / "data" / "voice-agent" / "reactivation-enrichment" / "launch-batch-2026-04-28-last30.scoreboard.json",
    {},
  )
  last30_seed_rows = last30_seed_payload.get("leads") if isinstance(last30_seed_payload, Mapping) else []
  last30_seed_rows = last30_seed_rows if isinstance(last30_seed_rows, list) else []
  shadow_call_payloads = _load_shadow_call_payloads(call_dir)
  call_payload_by_id = {
    str(features.get("call_id") or ""): payload
    for _, payload, features in shadow_call_payloads
    if str(features.get("call_id") or "")
  }
  if _can_extend_from_existing_shadow_bundle(out_dir, fingerprint_payload, existing_manifest):
    existing_bundle = _load_existing_extension_bundle(out_dir)
    qa_rows = existing_bundle["qa"].get("rows") if isinstance(existing_bundle["qa"], Mapping) else []
    followup_queue = existing_bundle["followup"].get("revenue_weighted_queue") if isinstance(existing_bundle["followup"], Mapping) else []
    actual_call_cohort_report = existing_bundle["actual_call_cohort"] if isinstance(existing_bundle["actual_call_cohort"], Mapping) else {}
    owner_review_queue = existing_bundle["owner_review"].get("rows") if isinstance(existing_bundle["owner_review"], Mapping) else []
    action_approval_queue = existing_bundle["action_approval"].get("rows") if isinstance(existing_bundle["action_approval"], Mapping) else []
    recommendations = existing_bundle["rsi"].get("recommendations") if isinstance(existing_bundle["rsi"], Mapping) else []
    last30_rows = existing_bundle["last30"].get("rows") if isinstance(existing_bundle["last30"], Mapping) else []
    scaffolds = existing_bundle["scaffolds"].get("agents") if isinstance(existing_bundle["scaffolds"], Mapping) else []
    replay_summary = existing_bundle["replay"].get("summary") if isinstance(existing_bundle["replay"], Mapping) else {}
    reconstruction_audit_summary = existing_bundle["reconstruction_audit"].get("summary") if isinstance(existing_bundle["reconstruction_audit"], Mapping) else {}
    capture_gap_summary = existing_bundle["capture_gap"].get("summary") if isinstance(existing_bundle["capture_gap"], Mapping) else {}
    capture_gap_rows = existing_bundle["capture_gap"].get("true_gap_rows") if isinstance(existing_bundle["capture_gap"], Mapping) else []
    qa_rows = qa_rows if isinstance(qa_rows, list) else []
    followup_queue = followup_queue if isinstance(followup_queue, list) else []
    owner_review_queue = owner_review_queue if isinstance(owner_review_queue, list) else []
    action_approval_queue = action_approval_queue if isinstance(action_approval_queue, list) else []
    recommendations = recommendations if isinstance(recommendations, list) else []
    last30_rows = last30_rows if isinstance(last30_rows, list) else []
    scaffolds = scaffolds if isinstance(scaffolds, list) else []
    capture_gap_rows = capture_gap_rows if isinstance(capture_gap_rows, list) else []

    files_changed: list[str] = []
    artifact_decisions: dict[str, str] = {"base_shadow_bundle": "reused_unchanged"}
    force_refresh_extension = str(existing_manifest.get("build_version") or "") != AUTONOMOUS_BUILD_VERSION
    _append_progress(
      progress_path,
      step="Skip unchanged replay and QA rebuild",
      outcome=(
        "Reused the existing replay, observer, QA, follow-up, and RSI shadow bundle because the raw input "
        f"fingerprint is unchanged ({fingerprint_payload['fingerprint'][:12]})."
      ),
      files=["none"],
    )

    processing_queue = _build_processing_queue(review_rows)
    attribution_json = out_dir / "revenue-attribution-gap-shadow-2026-04-29.json"
    attribution_csv = out_dir / "revenue-attribution-gap-shadow-2026-04-29.csv"
    attribution_md = out_dir / "REVENUE_ATTRIBUTION_GAP_REPORT_2026-04-29.md"
    existing_summary = existing_manifest.get("summary") if isinstance(existing_manifest.get("summary"), Mapping) else {}
    if not force_refresh_extension and attribution_json.exists() and attribution_csv.exists() and attribution_md.exists():
      attribution_report = _read_json(attribution_json, {})
      artifact_decisions["revenue_attribution_gap_report"] = "reused_unchanged"
      _append_progress(
        progress_path,
        step="Evaluate source/campaign attribution gap scaffold",
        outcome="Reused the existing attribution scaffold because the unchanged input fingerprint already has the full artifact set.",
        files=["none"],
      )
    else:
      attribution_report = _build_revenue_attribution_gap_report(
        qa_rows,
        followup_queue,
        [row for row in last30_seed_rows if isinstance(row, Mapping)],
        last30_rows,
        call_payload_by_id,
      )
      csv_rows = [
        {
          **row,
          "gap_reasons": "; ".join(list(row.get("gap_reasons") or [])),
        }
        for row in attribution_report.get("priority_gap_queue", [])
        if isinstance(row, Mapping)
      ]
      csv_fields = [
        "row_type",
        "row_id",
        "call_id",
        "contact_id",
        "first_name",
        "owner",
        "estimated_amount",
        "tracking_key",
        "tracking_type",
        "safe_batch_tag",
        "project",
        "purpose",
        "enrichment_source",
        "outcome",
        "follow_up_urgency",
        "priority_tier",
        "review_lane",
        "linked_shadow_call_count",
        "linked_last30_lead_count",
        "ad_tracking_field_count",
        "ad_tracking_fields_present",
        "gap_reasons",
      ]
      wrote_attribution = False
      wrote_attribution |= _track_file_change(files_changed, repo_root, attribution_json, _write_json_if_changed(attribution_json, attribution_report))
      wrote_attribution |= _track_file_change(files_changed, repo_root, attribution_csv, _write_csv_if_changed(attribution_csv, csv_rows, csv_fields))
      wrote_attribution |= _track_file_change(
        files_changed,
        repo_root,
        attribution_md,
        _write_text_if_changed(attribution_md, _render_revenue_attribution_gap_markdown(attribution_report)),
      )
      artifact_decisions["revenue_attribution_gap_report"] = "updated" if wrote_attribution else "revalidated_no_content_change"
      _append_progress(
        progress_path,
        step="Build source/campaign attribution gap scaffold",
        outcome=(
          f"Built {int(attribution_report['summary']['tracking_key_count'])} tracking cohorts and "
          f"{int(attribution_report['summary']['gap_row_count'])} priority attribution gaps without replaying unchanged shadow calls."
          if wrote_attribution
          else (
            f"Revalidated {int(attribution_report['summary']['tracking_key_count'])} tracking cohorts and "
            f"{int(attribution_report['summary']['gap_row_count'])} priority attribution gaps; content matched the existing scaffold."
          )
        ),
        files=files_changed[-3:] if wrote_attribution else ["none"],
      )

    summary = dict(existing_summary)
    summary.update(
      {
        "attribution_tracking_key_count": int(attribution_report["summary"]["tracking_key_count"]),
        "attribution_gap_count": int(attribution_report["summary"]["gap_row_count"]),
        "actual_call_linked_last30_count": int(attribution_report["summary"]["actual_call_linked_last30_count"]),
        "last30_linked_call_count": int(attribution_report["summary"]["last30_linked_call_count"]),
        "unattributed_estimated_amount_sum": int(attribution_report["summary"]["unattributed_estimated_amount_sum"]),
      }
    )

    readiness_json, readiness_csv, readiness_md = _pricing_app_submission_artifact_paths(out_dir)
    if not force_refresh_extension and readiness_json.exists() and readiness_csv.exists() and readiness_md.exists():
      readiness_report = _read_json(readiness_json, {})
      artifact_decisions["pricing_app_submission_readiness"] = "reused_unchanged"
      _append_progress(
        progress_path,
        step="Evaluate pricing/app/submission readiness scaffold",
        outcome="Reused the existing readiness scaffold because the unchanged input fingerprint already has the full artifact set.",
        files=["none"],
      )
    else:
      readiness_report = _build_pricing_app_submission_readiness_report(
        repo_root,
        scenario_rows,
        followup_queue,
        qa_rows,
        last30_rows,
        processing_queue,
      )
      readiness_rows = [
        {
          **row,
          "pricing_ready": "true" if row.get("pricing_ready") else "false",
          "app_start_ready": "true" if row.get("app_start_ready") else "false",
          "submission_packet_ready": "true" if row.get("submission_packet_ready") else "false",
          "actual_call_transcript_backed": "true" if row.get("actual_call_transcript_backed") else "false",
          "processing_condition_present": "true" if row.get("processing_condition_present") else "false",
          "missing_items": "; ".join(list(row.get("missing_items") or [])),
          "required_missing_fields": "; ".join(list(row.get("required_missing_fields") or [])),
          "evidence_refs": "; ".join(list(row.get("evidence_refs") or [])),
        }
        for row in readiness_report.get("rows", [])
        if isinstance(row, Mapping)
      ]
      readiness_fields = [
        "queue_id",
        "scenario_id",
        "contact_id",
        "first_name",
        "phone_redacted",
        "owner",
        "goal",
        "state",
        "property_type",
        "credit_score",
        "estimated_amount",
        "readiness_score",
        "revenue_automation_score",
        "queue_priority_score",
        "pricing_ready",
        "app_start_ready",
        "submission_packet_ready",
        "recommended_shadow_lane",
        "follow_up_urgency",
        "actual_call_outcome",
        "actual_call_transcript_backed",
        "last30_review_lane",
        "processing_condition_present",
        "missing_high_priority_count",
        "missing_items",
        "required_missing_fields",
        "approval_gate",
        "shadow_action",
        "evidence_refs",
      ]
      wrote_readiness = False
      wrote_readiness |= _track_file_change(files_changed, repo_root, readiness_csv, _write_csv_if_changed(readiness_csv, readiness_rows, readiness_fields))
      wrote_readiness |= _track_file_change(files_changed, repo_root, readiness_json, _write_json_if_changed(readiness_json, readiness_report))
      wrote_readiness |= _track_file_change(
        files_changed,
        repo_root,
        readiness_md,
        _write_text_if_changed(readiness_md, _render_pricing_app_submission_readiness_markdown(readiness_report)),
      )
      artifact_decisions["pricing_app_submission_readiness"] = "updated" if wrote_readiness else "revalidated_no_content_change"
      _append_progress(
        progress_path,
        step="Build pricing/app/submission readiness shadow queue",
        outcome=(
          f"Built {int(readiness_report['summary']['row_count'])} close-enough readiness rows with "
          f"{int(readiness_report['summary']['pricing_ready_count'])} pricing-ready, "
          f"{int(readiness_report['summary']['app_start_ready_count'])} app-start-ready, and "
          f"{int(readiness_report['summary']['submission_packet_ready_count'])} submission-packet-ready lanes."
          if wrote_readiness
          else f"Revalidated {int(readiness_report['summary']['row_count'])} readiness rows; content matched the existing readiness scaffold."
        ),
        files=files_changed[-3:] if wrote_readiness else ["none"],
      )
    readiness_summary = readiness_report.get("summary") if isinstance(readiness_report, Mapping) else {}
    readiness_summary = readiness_summary if isinstance(readiness_summary, Mapping) else {}
    summary.update(
      {
        "readiness_shadow_count": int(readiness_summary.get("row_count") or 0),
        "pricing_ready_count": int(readiness_summary.get("pricing_ready_count") or 0),
        "app_start_ready_count": int(readiness_summary.get("app_start_ready_count") or 0),
        "submission_packet_ready_count": int(readiness_summary.get("submission_packet_ready_count") or 0),
      }
    )

    command_center = {
      "generated_at": datetime.now().isoformat(timespec="seconds"),
      "shadow_call_count": int(summary.get("shadow_call_count") or replay_summary.get("call_state_count") or 0),
      "transcript_coverage_rate": float(replay_summary.get("transcript_coverage_rate") or 0.0),
      "recording_coverage_rate": float(replay_summary.get("recording_coverage_rate") or 0.0),
      "followup_queue_count": int(summary.get("followup_queue_count") or len(followup_queue)),
      "high_urgency_count": int(summary.get("high_urgency_count") or 0),
      "observer_gap_count": int(summary.get("observer_gap_count") or 0),
      "observer_expected_absence_count": int(summary.get("observer_expected_absence_count") or 0),
      "reconstruction_review_count": int(summary.get("reconstruction_review_count") or replay_summary.get("reconstruction_review_count") or 0),
      "reconstruction_audit_blocked_count": int(summary.get("reconstruction_audit_blocked_count") or reconstruction_audit_summary.get("blocked_count") or 0),
      "contact_resolution_review_count": int(summary.get("contact_resolution_review_count") or 0),
      "owner_review_count": int(summary.get("owner_review_count") or len(owner_review_queue)),
      "action_approval_count": int(summary.get("action_approval_count") or len(action_approval_queue)),
      "campaign_cohort_count": int(summary.get("campaign_cohort_count") or actual_call_cohort_report.get("summary", {}).get("campaign_count") or 0),
      "profitability_readiness_gap_count": int(summary.get("profitability_readiness_gap_count") or actual_call_cohort_report.get("summary", {}).get("profitability_readiness_gap_count") or 0),
      "high_value_unassigned_count": int(summary.get("high_value_unassigned_count") or 0),
      "attribution_tracking_key_count": int(attribution_report["summary"]["tracking_key_count"]),
      "attribution_gap_count": int(attribution_report["summary"]["gap_row_count"]),
      "actual_call_linked_last30_count": int(attribution_report["summary"]["actual_call_linked_last30_count"]),
      "last30_linked_call_count": int(attribution_report["summary"]["last30_linked_call_count"]),
      "unattributed_estimated_amount_sum": int(attribution_report["summary"]["unattributed_estimated_amount_sum"]),
      "pricing_ready_count": int(summary.get("pricing_ready_count") or 0),
      "app_start_ready_count": int(summary.get("app_start_ready_count") or 0),
      "submission_packet_ready_count": int(summary.get("submission_packet_ready_count") or 0),
      "last30_review_count": int(summary.get("last30_review_count") or len(last30_rows)),
      "last30_call_early_count": int(summary.get("last30_call_early_count") or 0),
      "last30_transcript_backed_count": int(summary.get("last30_transcript_backed_count") or 0),
      "last30_top_50_estimated_amount": int(summary.get("last30_top_50_estimated_amount") or 0),
      "expected_no_answer_count": int(summary.get("expected_no_answer_count") or capture_gap_summary.get("expected_no_answer_count") or 0),
      "expected_voicemail_count": int(summary.get("expected_voicemail_count") or capture_gap_summary.get("expected_voicemail_count") or 0),
      "true_capture_gap_count": int(summary.get("observer_gap_count") or capture_gap_summary.get("true_gap_count") or 0),
      "gap_agent_versions": str(summary.get("gap_agent_versions") or "none"),
      "dave_review_order": [
        "Replay coverage, phone-backed contact resolution, and synthetic contact fallbacks",
        "Source-aware observer capture review with true gaps versus expected no-conversation calls",
        "Transcript/recording root-cause review grouped by agent version and campaign",
        "Deterministic reconstruction audit with ready/review/blocked lanes",
        "Replay-backed contact resolution review queue",
        "Replay reconstruction gap queue",
        "Top QA rows and evidence quality",
        "Actual-call cohort patterns and revenue-priority follow-up ordering",
        "Revenue attribution gaps across actual calls and the last-30 lead queue",
        "Pricing, app-start, and submission-packet shadow readiness lanes",
        "Last-30-day backlog ladder with call-early and transcript-backed priority lanes",
        "High-value owner attribution review queue",
        "Post-call action approval queue with rollback and measurement hooks",
        "Consolidated queue comparison and operating plan for the day",
        "RSI approval gates before any workflow change",
      ],
    }

    management_packet_path = out_dir / "CONSOLIDATED_MORNING_MANAGEMENT_PACKET_2026-04-29.md"
    management_packet_changed = _track_file_change(
      files_changed,
      repo_root,
      management_packet_path,
      _write_text_if_changed(
        management_packet_path,
        _render_consolidated_morning_management_packet(
          command_center,
          followup_queue,
          actual_call_cohort_report,
          attribution_report,
          readiness_report,
          processing_queue,
          owner_review_queue,
          action_approval_queue,
          last30_rows,
          recommendations,
        ),
      ),
    )
    artifact_decisions["consolidated_morning_management_packet"] = "updated" if management_packet_changed else "revalidated_no_content_change"
    _append_progress(
      progress_path,
      step="Build consolidated morning management packet",
      outcome=(
        "Built a queue-comparison operating plan for Dave that prioritizes observer repair, owner proof, actual-call follow-up, readiness, processing, and backlog lanes."
        if management_packet_changed
        else "Revalidated the consolidated morning management packet; content matched the existing operating plan."
      ),
      files=[str(management_packet_path.relative_to(repo_root))] if management_packet_changed else ["none"],
    )

    command_center_json = out_dir / "call-center-os-command-center-2026-04-29.json"
    command_center_md = out_dir / "CALL_CENTER_OS_COMMAND_CENTER_2026-04-29.md"
    command_center_html = out_dir / "call-center-os-command-center-2026-04-29.html"
    command_center_payload = {
      "summary": command_center,
      "recommendations": recommendations,
      "agents": scaffolds[:12],
      "capture_gap_root_cause": capture_gap_summary,
      "revenue_attribution_gap_report": attribution_report["summary"],
      "pricing_app_submission_readiness_report": readiness_summary,
      "management_packet": {"path": str(management_packet_path.relative_to(repo_root))},
    }
    command_center_changed = False
    command_center_changed |= _track_file_change(files_changed, repo_root, command_center_json, _write_json_if_changed(command_center_json, command_center_payload))
    command_center_changed |= _track_file_change(
      files_changed,
      repo_root,
      command_center_md,
      _write_text_if_changed(command_center_md, _render_command_center_markdown(command_center, recommendations, scaffolds)),
    )
    command_center_changed |= _track_file_change(
      files_changed,
      repo_root,
      command_center_html,
      _write_text_if_changed(
        command_center_html,
        _render_command_center_html(
          command_center,
          recommendations,
          scaffolds,
          actual_call_cohort_report,
          attribution_report,
          followup_queue,
          owner_review_queue,
          action_approval_queue,
          last30_rows,
          capture_gap_summary,
          capture_gap_rows,
        ),
      ),
    )
    artifact_decisions["command_center"] = "updated" if command_center_changed else "revalidated_no_content_change"

    checks = [
      "Incremental extension reused the unchanged replay, QA, and reporting shadow bundle.",
      "Builder executed in extension-only mode to add only the missing downstream shadow artifacts.",
      "Phase-level diff guard reused unchanged artifacts and only rewrote files when rendered content changed.",
      "Idempotency guard active via autonomous-build-manifest-2026-04-29.json.",
    ]
    packet_path = out_dir / "AUTONOMOUS_PACKET_2026-04-29.md"
    packet_changed = _track_file_change(
      files_changed,
      repo_root,
      packet_path,
      _write_text_if_changed(packet_path, _render_autonomous_packet(files_changed, checks, summary, recommendations)),
    )
    artifact_decisions["autonomous_packet"] = "updated" if packet_changed else "revalidated_no_content_change"
    manifest_path = _autonomous_manifest_path(out_dir)
    manifest_changed = _track_file_change(
      files_changed,
      repo_root,
      manifest_path,
      _write_json_if_changed(
        manifest_path,
        {
          "build_version": AUTONOMOUS_BUILD_VERSION,
          "generated_at": datetime.now().isoformat(timespec="seconds"),
          "input_fingerprint": str(fingerprint_payload.get("fingerprint") or ""),
          "input_file_count": int(fingerprint_payload.get("file_count") or 0),
          "required_artifacts": list(AUTONOMOUS_REQUIRED_ARTIFACTS),
          "summary": dict(summary),
          "files_changed": [*files_changed, str(manifest_path.relative_to(repo_root))],
          "artifact_decisions": dict(artifact_decisions),
        },
      ),
    )
    artifact_decisions["manifest"] = "updated" if manifest_changed else "revalidated_no_content_change"
    _append_progress(
      progress_path,
      step="Refresh command-center and autonomous packet",
      outcome=(
        "Updated the command center, packet, and manifest to reflect the downstream extension while preserving the unchanged base bundle."
        if command_center_changed or packet_changed or manifest_changed
        else "Revalidated the command center, packet, and manifest; rendered output matched the existing extension bundle."
      ),
      files=files_changed[-5:] if (command_center_changed or packet_changed or manifest_changed) else ["none"],
    )
    return AutonomousArtifactsResult(files_changed=files_changed, checks=checks, summary=summary)

  source_index_by_call = _load_shadow_call_source_index(call_dir)
  features_by_call = {str(features.get("call_id") or ""): features for _, _, features in shadow_call_payloads}
  scenario_lookup = {str(row.get("contact_id") or ""): row for row in scenario_rows if str(row.get("contact_id") or "")}
  seed_lookup = {str(row.get("contact_id") or ""): row for row in scoreboard_seed_rows if str(row.get("contact_id") or "")}
  contact_index = _build_contact_resolution_index(scenario_rows, scoreboard_seed_rows, post_call_review_rows)

  files_changed: list[str] = []

  ledger_path = out_dir / "event-ledger-continuation-shadow-2026-04-29.jsonl"
  ledger = EventLedger(ledger_path)
  for _, payload, _ in shadow_call_payloads:
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
  call_states = _enrich_call_states(
    repo_root,
    list(derive_call_states(events).values()),
    features_by_call,
    source_index_by_call,
    scenario_lookup,
    seed_lookup,
    contact_index,
  )
  contact_states = _enrich_contact_states(
    repo_root,
    list(derive_contact_states(events).values()),
    call_states,
    scenario_lookup,
    seed_lookup,
  )
  actual_shadow_contact_states = _build_actual_shadow_contact_states(call_states)
  contact_resolution_summary, contact_resolution_queue = _build_contact_resolution_review(call_states)
  observer_capture_summary, observer_capture_queue = _build_observer_capture_review(call_states)
  reconstruction_audit_summary, reconstruction_audit_rows = _build_reconstruction_audit(events, call_states, actual_shadow_contact_states)
  replay_summary = {
    "event_count": len(events),
    "call_state_count": len(call_states),
    "contact_state_count": len(contact_states),
    "actual_shadow_contact_state_count": len(actual_shadow_contact_states),
    "scenario_only_contact_state_count": max(len(contact_states) - len(actual_shadow_contact_states), 0),
    "transcript_calls": sum(1 for row in call_states if row.get("transcript_available")),
    "recording_calls": sum(1 for row in call_states if row.get("recording_available")),
    "transfer_attempt_calls": sum(1 for row in call_states if row.get("transfer_status")),
    "owner_covered_calls": sum(1 for row in call_states if str(row.get("owner") or "") != "Unassigned LO Review"),
    "phone_resolved_call_count": sum(
      1 for row in call_states if str(row.get("contact_resolution_status") or "").startswith("resolved_by_phone")
    ),
    "synthetic_contact_fallback_count": sum(
      1 for row in call_states if str(row.get("contact_state_id") or "").startswith("shadow_contact__")
    ),
    "complete_reconstruction_calls": sum(1 for row in call_states if not row.get("reconstruction_gap_reasons")),
    "observer_gap_count": int(observer_capture_summary.get("row_count") or 0),
    "expected_no_conversation_count": int(observer_capture_summary.get("expected_no_conversation_count") or 0),
  }
  replay_summary["transcript_coverage_rate"] = replay_summary["transcript_calls"] / max(replay_summary["call_state_count"], 1)
  replay_summary["recording_coverage_rate"] = replay_summary["recording_calls"] / max(replay_summary["call_state_count"], 1)
  replay_summary["reconstruction_review_count"] = replay_summary["call_state_count"] - replay_summary["complete_reconstruction_calls"]
  replay_json = out_dir / "event-replay-shadow-2026-04-29.json"
  replay_md = out_dir / "EVENT_REPLAY_CURRENT_STATE_2026-04-29.md"
  write_json(
    replay_json,
    {
      "summary": replay_summary,
      "call_states": call_states,
      "contact_states": contact_states,
      "actual_shadow_contact_states": actual_shadow_contact_states,
    },
  )
  _write_text(replay_md, _render_event_replay_markdown(replay_summary, call_states, actual_shadow_contact_states))
  files_changed.extend([str(ledger_path.relative_to(repo_root)), str(replay_json.relative_to(repo_root)), str(replay_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Harden replay state from actual shadow Retell calls",
    outcome=f"Replayed {len(events)} events into {len(call_states)} call states, {len(actual_shadow_contact_states)} actual shadow contact states, and {len(contact_states)} all-source contact states with owner-aware required-field coverage, transcript, recording, transfer, appointment, evidence, and confidence fields.",
    files=files_changed[-3:],
  )

  reconstruction_audit_json = out_dir / "reconstruction-audit-shadow-2026-04-29.json"
  reconstruction_audit_md = out_dir / "RECONSTRUCTION_AUDIT_2026-04-29.md"
  write_json(reconstruction_audit_json, {"summary": reconstruction_audit_summary, "rows": reconstruction_audit_rows})
  _write_text(reconstruction_audit_md, _render_reconstruction_audit_markdown(reconstruction_audit_summary, reconstruction_audit_rows))
  files_changed.extend([str(reconstruction_audit_json.relative_to(repo_root)), str(reconstruction_audit_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Add deterministic replay reconstruction audit",
    outcome=f"Audited {len(reconstruction_audit_rows)} shadow calls into ready/review/blocked lanes with explicit schema coverage and event timelines.",
    files=files_changed[-2:],
  )

  contact_resolution_json = out_dir / "contact-resolution-review-shadow-2026-04-29.json"
  contact_resolution_md = out_dir / "CONTACT_RESOLUTION_REVIEW_SHADOW_2026-04-29.md"
  write_json(contact_resolution_json, {"summary": contact_resolution_summary, "rows": contact_resolution_queue})
  _write_text(contact_resolution_md, _render_contact_resolution_review_markdown(contact_resolution_summary, contact_resolution_queue))
  files_changed.extend([str(contact_resolution_json.relative_to(repo_root)), str(contact_resolution_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Add replay-backed contact resolution review queue",
    outcome=f"Built {len(contact_resolution_queue)} contact-resolution review rows with phone-backed evidence and synthetic-fallback visibility.",
    files=files_changed[-2:],
  )

  observer_capture_json = out_dir / "observer-capture-review-shadow-2026-04-29.json"
  observer_capture_md = out_dir / "OBSERVER_CAPTURE_REVIEW_2026-04-29.md"
  write_json(observer_capture_json, {"summary": observer_capture_summary, "rows": observer_capture_queue})
  _write_text(observer_capture_md, _render_observer_capture_review_markdown(observer_capture_summary, observer_capture_queue))
  files_changed.extend([str(observer_capture_json.relative_to(repo_root)), str(observer_capture_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Add source-aware observer capture review",
    outcome=(
      f"Classified {int(observer_capture_summary.get('expected_no_conversation_count') or 0)} no-conversation shadow calls separately "
      f"from {int(observer_capture_summary.get('row_count') or 0)} true observer capture gaps."
    ),
    files=files_changed[-2:],
  )

  qa_rows = _build_qa_rows(call_states, scenario_lookup, seed_lookup)
  qa_summary = {
    "row_count": len(qa_rows),
    "outcome_taxonomy": dict(Counter(str(row.get("outcome") or "") for row in qa_rows)),
    "rows_with_transfer_signals": sum(1 for row in qa_rows if str(row.get("transfer_status") or "") not in {"", "not_attempted"}),
    "transcript_row_count": sum(1 for row in qa_rows if row.get("transcript_available")),
    "recording_row_count": sum(1 for row in qa_rows if row.get("recording_available")),
    "transcript_backed_owner_count": sum(1 for row in qa_rows if row.get("transcript_backed_owner")),
    "observer_gap_row_count": sum(1 for row in qa_rows if str(row.get("observer_capture_status") or "") not in {"", "complete", "not_expected_no_conversation"}),
    "observer_expected_absence_count": sum(1 for row in qa_rows if str(row.get("observer_capture_status") or "") == "not_expected_no_conversation"),
    "reconstruction_gap_row_count": sum(1 for row in qa_rows if row.get("reconstruction_gap_reasons")),
    "avg_reconstruction_readiness_score": round(
      sum(int(row.get("reconstruction_readiness_score") or 0) for row in qa_rows) / max(len(qa_rows), 1),
      1,
    ),
  }
  qa_json = out_dir / "post-call-qa-continuation-shadow-2026-04-29.json"
  qa_md = out_dir / "POST_CALL_QA_CONTINUATION_2026-04-29.md"
  write_json(qa_json, {"summary": qa_summary, "rows": qa_rows})
  _write_text(qa_md, _render_qa_markdown(qa_summary, qa_rows))
  files_changed.extend([str(qa_json.relative_to(repo_root)), str(qa_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Improve post-call QA from actual shadow-call evidence",
    outcome=f"Built {len(qa_rows)} QA rows with transcript excerpts, owner evidence, transfer/appointment fields, replay-backed references, and reconstruction readiness scoring.",
    files=files_changed[-2:],
  )

  capture_gap_summary, capture_gap_rows, expected_no_conversation_rows = _build_capture_gap_root_cause(
    call_states,
    qa_rows,
    call_payload_by_id,
  )
  capture_gap_json = out_dir / "capture-gap-root-cause-shadow-2026-04-29.json"
  capture_gap_md = out_dir / "CAPTURE_GAP_ROOT_CAUSE_2026-04-29.md"
  write_json(
    capture_gap_json,
    {
      "summary": capture_gap_summary,
      "true_gap_rows": capture_gap_rows,
      "expected_no_conversation_rows": expected_no_conversation_rows,
    },
  )
  _write_text(capture_gap_md, _render_capture_gap_root_cause_markdown(capture_gap_summary, capture_gap_rows))
  files_changed.extend([str(capture_gap_json.relative_to(repo_root)), str(capture_gap_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Root-cause transcript and recording gaps",
    outcome=(
      f"Separated {capture_gap_summary['expected_no_conversation_count']} expected no-conversation calls from "
      f"{capture_gap_summary['true_gap_count']} true observer gaps and grouped the true gaps by "
      f"agent version {capture_gap_summary['gap_agent_version_counts']} and campaign {capture_gap_summary['gap_campaign_counts']}."
    ),
    files=files_changed[-2:],
  )

  (
    management_summary,
    scoreboard_rows,
    followup_queue,
    owner_review_summary,
    owner_review_queue,
    action_approval_summary,
    action_approval_queue,
  ) = _build_reporting_views(
    repo_root,
    scenario_rows,
    scoreboard_seed_rows,
    qa_rows,
    contact_resolution_summary,
    contact_resolution_queue,
    observer_capture_summary,
  )
  actual_call_cohort_report = _build_actual_call_cohort_report(qa_rows, followup_queue)
  management_summary["actual_call_cohort_report"] = actual_call_cohort_report
  management_summary["summary"]["campaign_count"] = actual_call_cohort_report["summary"]["campaign_count"]
  management_summary["summary"]["profitability_readiness_gap_count"] = actual_call_cohort_report["summary"]["profitability_readiness_gap_count"]
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
    step="Improve LO scoreboard and management reporting",
    outcome=f"Ranked {len(followup_queue)} follow-up rows by profitability, readiness, evidence coverage, engagement, urgency, and transcript-backed owner evidence.",
    files=files_changed[-4:],
  )

  actual_call_cohort_json = out_dir / "actual-call-cohort-report-shadow-2026-04-29.json"
  actual_call_cohort_md = out_dir / "ACTUAL_CALL_COHORT_REPORT_2026-04-29.md"
  write_json(actual_call_cohort_json, actual_call_cohort_report)
  _write_text(actual_call_cohort_md, _render_actual_call_cohort_markdown(actual_call_cohort_report))
  files_changed.extend([str(actual_call_cohort_json.relative_to(repo_root)), str(actual_call_cohort_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Add actual-call cohort reporting",
    outcome=f"Built {actual_call_cohort_report['summary']['campaign_count']} campaign cohorts, {actual_call_cohort_report['summary']['owner_count']} owner cohorts, and {actual_call_cohort_report['summary']['profitability_readiness_gap_count']} profitability/readiness gap rows from actual shadow calls.",
    files=files_changed[-2:],
  )

  owner_review_json = out_dir / "owner-attribution-review-shadow-2026-04-29.json"
  owner_review_md = out_dir / "OWNER_ATTRIBUTION_REVIEW_SHADOW_2026-04-29.md"
  write_json(owner_review_json, {"summary": owner_review_summary, "rows": owner_review_queue})
  _write_text(owner_review_md, _render_owner_attribution_review_markdown(owner_review_summary, owner_review_queue))
  files_changed.extend([str(owner_review_json.relative_to(repo_root)), str(owner_review_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Add shadow owner attribution review queue",
    outcome=f"Built {len(owner_review_queue)} owner-review rows for transcript-backed ownership resolution, failed transfers, and high-value unassigned calls.",
    files=files_changed[-2:],
  )

  action_approval_json = out_dir / "post-call-action-approval-shadow-2026-04-29.json"
  action_approval_md = out_dir / "POST_CALL_ACTION_APPROVAL_SHADOW_2026-04-29.md"
  write_json(action_approval_json, {"summary": action_approval_summary, "rows": action_approval_queue})
  _write_text(action_approval_md, _render_post_call_action_approval_markdown(action_approval_summary, action_approval_queue))
  files_changed.extend([str(action_approval_json.relative_to(repo_root)), str(action_approval_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Add shadow post-call action approval queue",
    outcome=f"Built {len(action_approval_queue)} manager-review action rows linking actual-call evidence to approval gates, measurement plans, and rollback paths.",
    files=files_changed[-2:],
  )

  recommendations = build_governed_rsi_recommendations(management_summary["summary"], qa_rows, followup_queue, replay_summary)
  rsi_json = out_dir / "rsi-recommendation-queue-continuation-shadow-2026-04-29.json"
  rsi_md = out_dir / "RSI_RECOMMENDATION_QUEUE_CONTINUATION_2026-04-29.md"
  write_json(rsi_json, {"summary": {"recommendation_count": len(recommendations)}, "recommendations": recommendations})
  _write_text(rsi_md, render_governed_recommendations_markdown(recommendations))
  files_changed.extend([str(rsi_json.relative_to(repo_root)), str(rsi_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Add governed RSI recommendation queue mechanics",
    outcome=f"Built {len(recommendations)} governed RSI recommendations with approval, measurement, rollback, and attribution fields.",
    files=files_changed[-2:],
  )

  last30_summary, last30_rows, last30_files = _build_last30_review_queue(repo_root, out_dir)
  if last30_files:
    files_changed.extend(last30_files)
  if last30_rows:
    _append_progress(
      progress_path,
      step="Integrate last-30-day shadow backlog ladder",
      outcome=(
        f"Built or refreshed {len(last30_rows)} last-30-day review rows with "
        f"{last30_summary.get('call_early_count', 0)} call-early priorities and "
        f"{last30_summary.get('transcript_backed_count', 0)} transcript-backed rows."
      ),
      files=last30_files or [str((out_dir / "last30-lead-review-queue-shadow-2026-04-29.json").relative_to(repo_root))],
    )

  processing_queue = _build_processing_queue(review_rows)
  scaffolds = _build_agent_scaffolds(management_summary, processing_queue, followup_queue, scenario_rows, qa_rows)
  scaffolds_json = out_dir / "agent-scaffolds-shadow-2026-04-29.json"
  scaffolds_md = out_dir / "AGENT_SCAFFOLDS_SHADOW_2026-04-29.md"
  write_json(scaffolds_json, {"summary": {"agent_count": len(scaffolds)}, "agents": scaffolds})
  _write_text(scaffolds_md, _render_agent_scaffolds_markdown(scaffolds))
  files_changed.extend([str(scaffolds_json.relative_to(repo_root)), str(scaffolds_md.relative_to(repo_root))])
  _append_progress(
    progress_path,
    step="Refresh safe shadow agent scaffolds",
    outcome=f"Built {len(scaffolds)} safe scaffold rows spanning Speed-to-Lead, Inbound Callback, LO Assistant, No-Show Recovery, Document/App Completion, Processing Conditions, and Senior Sales data-capture.",
    files=files_changed[-2:],
  )

  readiness_report = _build_pricing_app_submission_readiness_report(
    repo_root,
    scenario_rows,
    followup_queue,
    qa_rows,
    last30_rows,
    processing_queue,
  )
  readiness_json, readiness_csv, readiness_md = _pricing_app_submission_artifact_paths(out_dir)
  readiness_csv_rows = [
    {
      **row,
      "pricing_ready": "true" if row.get("pricing_ready") else "false",
      "app_start_ready": "true" if row.get("app_start_ready") else "false",
      "submission_packet_ready": "true" if row.get("submission_packet_ready") else "false",
      "actual_call_transcript_backed": "true" if row.get("actual_call_transcript_backed") else "false",
      "processing_condition_present": "true" if row.get("processing_condition_present") else "false",
      "missing_items": "; ".join(list(row.get("missing_items") or [])),
      "required_missing_fields": "; ".join(list(row.get("required_missing_fields") or [])),
      "evidence_refs": "; ".join(list(row.get("evidence_refs") or [])),
    }
    for row in readiness_report.get("rows", [])
    if isinstance(row, Mapping)
  ]
  _write_csv(
    readiness_csv,
    readiness_csv_rows,
    [
      "queue_id",
      "scenario_id",
      "contact_id",
      "first_name",
      "phone_redacted",
      "owner",
      "goal",
      "state",
      "property_type",
      "credit_score",
      "estimated_amount",
      "readiness_score",
      "revenue_automation_score",
      "queue_priority_score",
      "pricing_ready",
      "app_start_ready",
      "submission_packet_ready",
      "recommended_shadow_lane",
      "follow_up_urgency",
      "actual_call_outcome",
      "actual_call_transcript_backed",
      "last30_review_lane",
      "processing_condition_present",
      "missing_high_priority_count",
      "missing_items",
      "required_missing_fields",
      "approval_gate",
      "shadow_action",
      "evidence_refs",
    ],
  )
  write_json(readiness_json, readiness_report)
  _write_text(readiness_md, _render_pricing_app_submission_readiness_markdown(readiness_report))
  files_changed.extend(
    [
      str(readiness_json.relative_to(repo_root)),
      str(readiness_csv.relative_to(repo_root)),
      str(readiness_md.relative_to(repo_root)),
    ]
  )
  _append_progress(
    progress_path,
    step="Build pricing/app/submission readiness shadow queue",
    outcome=(
      f"Built {int(readiness_report['summary']['row_count'])} close-enough readiness rows with "
      f"{int(readiness_report['summary']['pricing_ready_count'])} pricing-ready, "
      f"{int(readiness_report['summary']['app_start_ready_count'])} app-start-ready, and "
      f"{int(readiness_report['summary']['submission_packet_ready_count'])} submission-packet-ready lanes."
    ),
    files=files_changed[-3:],
  )

  attribution_report = _build_revenue_attribution_gap_report(
    qa_rows,
    followup_queue,
    [row for row in last30_seed_rows if isinstance(row, Mapping)],
    last30_rows,
    call_payload_by_id,
  )
  attribution_json = out_dir / "revenue-attribution-gap-shadow-2026-04-29.json"
  attribution_csv = out_dir / "revenue-attribution-gap-shadow-2026-04-29.csv"
  attribution_md = out_dir / "REVENUE_ATTRIBUTION_GAP_REPORT_2026-04-29.md"
  write_json(attribution_json, attribution_report)
  attribution_csv_rows = [
    {
      **row,
      "gap_reasons": "; ".join(list(row.get("gap_reasons") or [])),
    }
    for row in attribution_report.get("priority_gap_queue", [])
    if isinstance(row, Mapping)
  ]
  _write_csv(
    attribution_csv,
    attribution_csv_rows,
    [
      "row_type",
      "row_id",
      "call_id",
      "contact_id",
      "first_name",
      "owner",
      "estimated_amount",
      "tracking_key",
      "tracking_type",
      "safe_batch_tag",
      "project",
      "purpose",
      "enrichment_source",
      "outcome",
      "follow_up_urgency",
      "priority_tier",
      "review_lane",
      "linked_shadow_call_count",
      "linked_last30_lead_count",
      "ad_tracking_field_count",
      "ad_tracking_fields_present",
      "gap_reasons",
    ],
  )
  _write_text(attribution_md, _render_revenue_attribution_gap_markdown(attribution_report))
  files_changed.extend(
    [
      str(attribution_json.relative_to(repo_root)),
      str(attribution_csv.relative_to(repo_root)),
      str(attribution_md.relative_to(repo_root)),
    ]
  )
  _append_progress(
    progress_path,
    step="Build source/campaign attribution gap scaffold",
    outcome=(
      f"Built {int(attribution_report['summary']['tracking_key_count'])} tracking cohorts, "
      f"{int(attribution_report['summary']['gap_row_count'])} priority attribution gaps, and "
      f"${int(attribution_report['summary']['unattributed_estimated_amount_sum']):,} in unattributed estimated amount."
    ),
    files=files_changed[-3:],
  )

  command_center = {
    "generated_at": datetime.now().isoformat(timespec="seconds"),
    "shadow_call_count": len(call_states),
    "transcript_coverage_rate": replay_summary["transcript_coverage_rate"],
    "recording_coverage_rate": replay_summary["recording_coverage_rate"],
    "followup_queue_count": len(followup_queue),
    "high_urgency_count": management_summary["summary"]["high_urgency_count"],
    "observer_gap_count": management_summary["summary"]["observer_gap_count"],
    "observer_expected_absence_count": management_summary["summary"]["observer_expected_absence_count"],
    "reconstruction_review_count": management_summary["summary"]["reconstruction_review_count"],
    "reconstruction_audit_blocked_count": reconstruction_audit_summary["blocked_count"],
    "contact_resolution_review_count": management_summary["summary"]["contact_resolution_review_count"],
    "owner_review_count": len(owner_review_queue),
    "action_approval_count": len(action_approval_queue),
    "campaign_cohort_count": actual_call_cohort_report["summary"]["campaign_count"],
    "profitability_readiness_gap_count": actual_call_cohort_report["summary"]["profitability_readiness_gap_count"],
    "high_value_unassigned_count": owner_review_summary["high_value_unassigned_count"],
    "attribution_tracking_key_count": int(attribution_report["summary"]["tracking_key_count"]),
    "attribution_gap_count": int(attribution_report["summary"]["gap_row_count"]),
    "actual_call_linked_last30_count": int(attribution_report["summary"]["actual_call_linked_last30_count"]),
    "last30_linked_call_count": int(attribution_report["summary"]["last30_linked_call_count"]),
    "unattributed_estimated_amount_sum": int(attribution_report["summary"]["unattributed_estimated_amount_sum"]),
    "pricing_ready_count": int(readiness_report["summary"]["pricing_ready_count"]),
    "app_start_ready_count": int(readiness_report["summary"]["app_start_ready_count"]),
    "submission_packet_ready_count": int(readiness_report["summary"]["submission_packet_ready_count"]),
    "last30_review_count": int(last30_summary.get("total_last30") or 0),
    "last30_call_early_count": int(last30_summary.get("call_early_count") or 0),
    "last30_transcript_backed_count": int(last30_summary.get("transcript_backed_count") or 0),
    "last30_top_50_estimated_amount": int(last30_summary.get("top_50_estimated_amount") or 0),
    "expected_no_answer_count": int(capture_gap_summary.get("expected_no_answer_count") or 0),
    "expected_voicemail_count": int(capture_gap_summary.get("expected_voicemail_count") or 0),
    "true_capture_gap_count": int(capture_gap_summary.get("true_gap_count") or 0),
    "gap_agent_versions": ", ".join(sorted(capture_gap_summary.get("gap_agent_version_counts", {}).keys())) or "none",
    "dave_review_order": [
      "Replay coverage, phone-backed contact resolution, and synthetic contact fallbacks",
      "Source-aware observer capture review with true gaps versus expected no-conversation calls",
      "Transcript/recording root-cause review grouped by agent version and campaign",
      "Deterministic reconstruction audit with ready/review/blocked lanes",
      "Replay-backed contact resolution review queue",
      "Replay reconstruction gap queue",
      "Top QA rows and evidence quality",
      "Actual-call cohort patterns and revenue-priority follow-up ordering",
      "Revenue attribution gaps across actual calls and the last-30 lead queue",
      "Pricing, app-start, and submission-packet shadow readiness lanes",
      "Last-30-day backlog ladder with call-early and transcript-backed priority lanes",
      "High-value owner attribution review queue",
      "Post-call action approval queue with rollback and measurement hooks",
      "Consolidated queue comparison and operating plan for the day",
      "RSI approval gates before any workflow change",
    ],
  }
  management_packet_path = out_dir / "CONSOLIDATED_MORNING_MANAGEMENT_PACKET_2026-04-29.md"
  _write_text(
    management_packet_path,
    _render_consolidated_morning_management_packet(
      command_center,
      followup_queue,
      actual_call_cohort_report,
      attribution_report,
      readiness_report,
      processing_queue,
      owner_review_queue,
      action_approval_queue,
      last30_rows,
      recommendations,
    ),
  )
  files_changed.append(str(management_packet_path.relative_to(repo_root)))
  _append_progress(
    progress_path,
    step="Build consolidated morning management packet",
    outcome="Built a queue-comparison operating plan for Dave that prioritizes observer repair, owner proof, actual-call follow-up, readiness, processing, and backlog lanes.",
    files=[str(management_packet_path.relative_to(repo_root))],
  )
  command_center_json = out_dir / "call-center-os-command-center-2026-04-29.json"
  command_center_md = out_dir / "CALL_CENTER_OS_COMMAND_CENTER_2026-04-29.md"
  command_center_html = out_dir / "call-center-os-command-center-2026-04-29.html"
  write_json(
    command_center_json,
    {
      "summary": command_center,
      "recommendations": recommendations,
      "agents": scaffolds[:12],
      "capture_gap_root_cause": capture_gap_summary,
      "revenue_attribution_gap_report": attribution_report["summary"],
      "pricing_app_submission_readiness_report": readiness_report["summary"],
      "management_packet": {"path": str(management_packet_path.relative_to(repo_root))},
    },
  )
  _write_text(command_center_md, _render_command_center_markdown(command_center, recommendations, scaffolds))
  _write_text(
    command_center_html,
    _render_command_center_html(
      command_center,
      recommendations,
      scaffolds,
      actual_call_cohort_report,
      attribution_report,
      followup_queue,
      owner_review_queue,
      action_approval_queue,
      last30_rows,
      capture_gap_summary,
      capture_gap_rows,
    ),
  )
  files_changed.extend(
    [
      str(command_center_json.relative_to(repo_root)),
      str(command_center_md.relative_to(repo_root)),
      str(command_center_html.relative_to(repo_root)),
    ]
  )
  _append_progress(
    progress_path,
    step="Improve command-center and morning review artifacts",
    outcome=(
      "Published an updated command-center digest, local HTML dashboard, and explicit Dave morning review "
      "order for the hardened shadow bundle."
    ),
    files=files_changed[-3:],
  )

  checks = [
    "Builder executed successfully via scripts/voice-build-call-center-os-autonomous.py.",
    "Idempotency guard active via autonomous-build-manifest-2026-04-29.json.",
  ]
  summary = {
    "shadow_call_count": len(call_states),
    "contact_resolution_review_count": management_summary["summary"]["contact_resolution_review_count"],
    "observer_gap_count": management_summary["summary"]["observer_gap_count"],
    "observer_expected_absence_count": management_summary["summary"]["observer_expected_absence_count"],
    "expected_no_answer_count": int(capture_gap_summary.get("expected_no_answer_count") or 0),
    "gap_agent_versions": ", ".join(sorted(capture_gap_summary.get("gap_agent_version_counts", {}).keys())) or "none",
    "reconstruction_review_count": management_summary["summary"]["reconstruction_review_count"],
    "reconstruction_audit_blocked_count": reconstruction_audit_summary["blocked_count"],
    "qa_row_count": len(qa_rows),
    "followup_queue_count": len(followup_queue),
    "campaign_cohort_count": actual_call_cohort_report["summary"]["campaign_count"],
    "profitability_readiness_gap_count": actual_call_cohort_report["summary"]["profitability_readiness_gap_count"],
    "attribution_tracking_key_count": int(attribution_report["summary"]["tracking_key_count"]),
    "attribution_gap_count": int(attribution_report["summary"]["gap_row_count"]),
    "actual_call_linked_last30_count": int(attribution_report["summary"]["actual_call_linked_last30_count"]),
    "last30_linked_call_count": int(attribution_report["summary"]["last30_linked_call_count"]),
    "unattributed_estimated_amount_sum": int(attribution_report["summary"]["unattributed_estimated_amount_sum"]),
    "readiness_shadow_count": int(readiness_report["summary"]["row_count"]),
    "pricing_ready_count": int(readiness_report["summary"]["pricing_ready_count"]),
    "app_start_ready_count": int(readiness_report["summary"]["app_start_ready_count"]),
    "submission_packet_ready_count": int(readiness_report["summary"]["submission_packet_ready_count"]),
    "owner_review_count": len(owner_review_queue),
    "action_approval_count": len(action_approval_queue),
    "rsi_recommendation_count": len(recommendations),
    "agent_scaffold_count": len(scaffolds),
    "last30_review_count": int(last30_summary.get("total_last30") or 0),
    "last30_call_early_count": int(last30_summary.get("call_early_count") or 0),
    "last30_transcript_backed_count": int(last30_summary.get("transcript_backed_count") or 0),
    "last30_top_50_estimated_amount": int(last30_summary.get("top_50_estimated_amount") or 0),
  }
  manifest_path = _autonomous_manifest_path(out_dir)
  _write_autonomous_manifest(manifest_path, fingerprint_payload, summary, files_changed)
  files_changed.append(str(manifest_path.relative_to(repo_root)))
  packet_path = out_dir / "AUTONOMOUS_PACKET_2026-04-29.md"
  _write_text(packet_path, _render_autonomous_packet(files_changed, checks, summary, recommendations))
  files_changed.append(str(packet_path.relative_to(repo_root)))
  _write_autonomous_manifest(manifest_path, fingerprint_payload, summary, files_changed)
  _append_progress(
    progress_path,
    step="Write autonomous packet",
    outcome="Published the final autonomous packet with review order, changed artifacts, and governed next steps.",
    files=[str(packet_path.relative_to(repo_root))],
  )

  return AutonomousArtifactsResult(files_changed=files_changed, checks=checks, summary=summary)
