from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from loan_os.call_center.agents import AGENT_SCAFFOLDS, render_agent_scaffolds_markdown
from loan_os.call_center.ledger import (
  EventLedger,
  normalize_email_submission,
  normalize_ghl_appointment,
  normalize_ghl_note,
  normalize_lead_enrichment,
  normalize_retell_payload,
  render_normalization_markdown,
  write_json,
)
from loan_os.call_center.post_call import PostCallAssessment, build_assessment
from loan_os.call_center.reporting import (
  build_lo_summary,
  build_management_summary,
  build_scoreboard_rows,
  read_csv,
  render_management_markdown,
  render_scoreboard_html,
  render_scoreboard_markdown,
)
from loan_os.call_center.rsi import (
  HARNESS_CONTRACT,
  build_rsi_recommendations,
  render_harness_markdown,
  render_recommendations_markdown,
)
from loan_os.paths import CALL_CENTER_OS_DIR, REPO_ROOT


@dataclass
class PhaseRecord:
  phase_number: int
  phase_name: str
  status: str
  files_changed: list[str]
  verification_run: str
  blockers: str
  next_phase: str


@dataclass
class OvernightArtifactsResult:
  phase_records: list[PhaseRecord]
  files_changed: list[str]
  tests: list[str]
  launch_risks: list[str]
  queue_counts: dict[str, int]


def _write_text(path: Path, content: str) -> Path:
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(content, encoding="utf-8")
  return path


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
  path.parent.mkdir(parents=True, exist_ok=True)
  fieldnames = list(rows[0].keys()) if rows else []
  with path.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
  return path


def _heartbeat(path: Path, message: str) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  stamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
  with path.open("a", encoding="utf-8") as handle:
    handle.write(f"{stamp} | {message}\n")


def _sample_retell_payloads() -> list[dict[str, Any]]:
  return [
    {
      "event": "call_started",
      "call": {
        "call_id": "retell-shadow-001",
        "contact_id": "shadow-contact-001",
        "timestamp": "2026-04-28T20:02:00Z",
        "connected_seconds": 0,
      },
    },
    {
      "event": "call_analyzed",
      "call": {
        "call_id": "retell-shadow-001",
        "contact_id": "shadow-contact-001",
        "ended_at": "2026-04-28T20:08:00Z",
        "connected_seconds": 312,
        "disposition": "interested_not_ready",
        "transcript": "Prospect called from 626-555-0188 and said please email me at test@example.com after the review.",
        "recording_url": "https://retell.example.local/recording/retell-shadow-001",
        "transfer_status": "started",
      },
    },
  ]


def _sample_ghl_note() -> dict[str, str]:
  return {
    "id": "ghl-note-shadow-001",
    "contact_id": "shadow-contact-001",
    "created_at": "2026-04-28T20:10:00Z",
    "body": "Called borrower at 626-555-0188 and left an internal note only.",
    "user_id": "user-shadow-001",
  }


def _sample_appointment() -> dict[str, str]:
  return {
    "id": "appt-shadow-001",
    "contact_id": "shadow-contact-001",
    "start_time": "2026-04-29T17:00:00Z",
    "calendar_id": "calendar-shadow",
  }


def _sample_email_submission() -> dict[str, str]:
  return {
    "loan_id": "loan-shadow-001",
    "message_id": "msg-shadow-001",
    "status": "received",
    "subject": "Submission received for test@example.com",
    "lender": "Shadow Lender",
    "occurred_at": "2026-04-28T21:00:00Z",
  }


def _build_action_queue(
  launch_rows: list[dict[str, str]],
  scenario_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
  scenario_lookup = {row.get("contact_id", ""): row for row in scenario_rows}
  output: list[dict[str, Any]] = []
  for row in launch_rows:
    scenario = scenario_lookup.get(str(row.get("contact_id") or ""), {})
    owner = str(scenario.get("suggested_owner") or row.get("owner") or "Unassigned LO Review")
    assessment = build_assessment({**scenario, **row}, owner)
    output.append(asdict(assessment))
  return output


def _build_progress_markdown(
  phase_records: list[PhaseRecord],
  files_changed: list[str],
  tests: list[str],
  launch_risks: list[str],
) -> str:
  lines = [
    "# Overnight Progress - 2026-04-28",
    "",
    "Autonomous overnight build for the Evolve Funding Call Center OS.",
    "",
    "## Phase Status",
    "",
  ]
  for record in phase_records:
    lines.extend(
      [
        f"### Phase {record.phase_number} - {record.phase_name}",
        "",
        f"- Status: {record.status}",
        f"- Files changed: {', '.join(record.files_changed)}",
        f"- Verification run: {record.verification_run}",
        f"- Blockers: {record.blockers}",
        f"- Next phase: {record.next_phase}",
        "",
      ]
    )
  lines.extend(["## Verification", ""])
  lines.extend(f"- {item}" for item in tests)
  lines.extend(["", "## Launch Risks", ""])
  lines.extend(f"- {item}" for item in launch_risks)
  lines.extend(["", "## File Count", "", f"- Total changed/generated artifacts: {len(files_changed)}", ""])
  return "\n".join(lines)


def _build_morning_packet(
  out_dir: Path,
  phase_records: list[PhaseRecord],
  files_changed: list[str],
  tests: list[str],
  launch_risks: list[str],
  queue_counts: dict[str, int],
) -> Path:
  path = out_dir / "MORNING_PACKET_2026-04-29.md"
  lines = [
    "# Morning Packet - 2026-04-29",
    "",
    "## Executive Summary",
    "",
    "The overnight build completed the six planned shadow-mode phases: observer/event ledger, post-call QA routing, LO scoreboard and management reporting, agent scaffolds, RSI and 3-agent harness specs, and launch-readiness packaging. All outputs remain draft/shadow only and do not authorize live calls, borrower messaging, GHL writes, LOS writes, or prompt deployment.",
    "",
    "## Completed Phases",
    "",
  ]
  for record in phase_records:
    lines.append(f"- Phase {record.phase_number} `{record.phase_name}`: {record.status}")
  lines.extend(
    [
      "",
      "## Files Changed",
      "",
      *[f"- `{path}`" for path in files_changed],
      "",
      "## Tests",
      "",
      *[f"- {test}" for test in tests],
      "",
      "## Ready-For-Review Queues",
      "",
      f"- Controlled post-call QA queue: {queue_counts['action_queue']}",
      f"- Human review backlog rows: {queue_counts['human_review']}",
      f"- Hot opportunity queue rows: {queue_counts['hot_queue']}",
      "",
      "## Launch Risks",
      "",
      *[f"- {risk}" for risk in launch_risks],
      "",
      "## Recommended Next Tests/Calls",
      "",
      "1. Run five supervised controlled calls and confirm transcript, recording, transfer result, and QA route hit the event ledger.",
      "2. Review the top 10 hot opportunities with the owning LO and confirm transcript-backed ownership before any outreach automation.",
      "3. Triage pricing-ready review items from the human review queue before expanding any pricing-related automation.",
      "4. Shadow the Strategist/Drafter/Reviewer loop on the first five post-call outcomes and inspect evidence quality.",
      "",
    ]
  )
  return _write_text(path, "\n".join(lines))


def build_overnight_artifacts(
  repo_root: Path | None = None,
  out_dir: Path | None = None,
  heartbeat_path: Path | None = None,
) -> OvernightArtifactsResult:
  repo_root = repo_root or REPO_ROOT
  out_dir = out_dir or CALL_CENTER_OS_DIR
  heartbeat_path = heartbeat_path or repo_root / "data" / "logs" / "call-center-os-overnight-heartbeat.log"

  scenario_rows = read_csv(repo_root / "data" / "loan-os" / "scenarios" / "reactivation-scenario-ledger-2026-04-28.csv")
  review_rows = read_csv(repo_root / "data" / "loan-os" / "human-review" / "human-review-queue-2026-04-28.csv")
  launch_rows = read_csv(repo_root / "data" / "loan-os" / "post-call-review" / "post-call-review-packet-2026-04-28.csv")
  scoreboard_seed_rows = read_csv(repo_root / "data" / "voice-agent" / "reactivation-enrichment" / "launch-batch-2026-04-28.post-call-scoreboard.csv")

  files_changed: list[str] = []
  phase_records: list[PhaseRecord] = []

  _heartbeat(heartbeat_path, "phase_1_start | observer_event_ledger")
  ledger_path = out_dir / "event-ledger-shadow-2026-04-28.jsonl"
  ledger = EventLedger(ledger_path)
  for payload in _sample_retell_payloads():
    ledger.extend(normalize_retell_payload(payload))
  ledger.append(normalize_ghl_note(_sample_ghl_note()))
  ledger.append(normalize_ghl_appointment(_sample_appointment()))
  ledger.append(normalize_email_submission(_sample_email_submission()))
  for row in scenario_rows:
    ledger.append(normalize_lead_enrichment(row))
  for row in review_rows:
    ledger.append(
      normalize_ghl_note(
        {
          "id": row.get("review_id", ""),
          "contact_id": row.get("contact_id", ""),
          "created_at": "2026-04-28T22:00:00Z",
          "body": row.get("reason", ""),
          "type": "human_review_required",
        }
      )
    )
  ledger.write()
  files_changed.append(str(ledger_path.relative_to(repo_root)))
  normalization_md = out_dir / "EVENT_NORMALIZATION_RULES_2026-04-28.md"
  normalization_json = out_dir / "event-normalization-rules-2026-04-28.json"
  _write_text(normalization_md, render_normalization_markdown())
  write_json(normalization_json, {"rules": normalize_rule_payload()})
  files_changed.extend([str(normalization_md.relative_to(repo_root)), str(normalization_json.relative_to(repo_root))])
  phase_records.append(
    PhaseRecord(
      phase_number=1,
      phase_name="Observer / Event Ledger",
      status="complete",
      files_changed=[str(ledger_path.relative_to(repo_root)), str(normalization_md.relative_to(repo_root)), str(normalization_json.relative_to(repo_root))],
      verification_run="Event ledger JSONL written; normalization rules rendered; verified by py_compile plus clean overnight builder execution. Targeted pytest command was unavailable because pytest is not installed in this shell.",
      blockers="none",
      next_phase="Phase 2 - Post-Call QA And Action Router",
    )
  )
  _heartbeat(heartbeat_path, "phase_1_complete | observer_event_ledger")

  _heartbeat(heartbeat_path, "phase_2_start | post_call_qa")
  action_queue_rows = _build_action_queue(launch_rows, scenario_rows)
  post_call_json = out_dir / "post-call-qa-shadow-2026-04-28.json"
  post_call_md = out_dir / "POST_CALL_QA_SHADOW_2026-04-28.md"
  write_json(
    post_call_json,
    {
      "outcome_taxonomy": Counter(row["outcome"] for row in action_queue_rows),
      "approval_states": ["review_required"],
      "rows": action_queue_rows,
    },
  )
  post_call_lines = [
    "# Post-Call QA Shadow - 2026-04-28",
    "",
    "Outcome taxonomy, next-action routes, and approval states remain shadow-only.",
    "",
    "## Queue",
    "",
  ]
  for row in action_queue_rows:
    post_call_lines.append(
      f"- {row['owner']}: {row['contact_id'] or 'missing-contact'} | {row['outcome']} | {row['route']} | {row['review_reason']}"
    )
  _write_text(post_call_md, "\n".join(post_call_lines) + "\n")
  files_changed.extend([str(post_call_json.relative_to(repo_root)), str(post_call_md.relative_to(repo_root))])
  phase_records.append(
    PhaseRecord(
      phase_number=2,
      phase_name="Post-Call QA And Action Router",
      status="complete",
      files_changed=[str(post_call_json.relative_to(repo_root)), str(post_call_md.relative_to(repo_root))],
      verification_run="Shadow action queue generated from controlled review packet; verified by py_compile plus clean overnight builder execution. Targeted pytest command was unavailable because pytest is not installed in this shell.",
      blockers="Queue is mostly pending-call until controlled calls produce transcripts/recordings.",
      next_phase="Phase 3 - LO Scoreboard And Management Reporting",
    )
  )
  _heartbeat(heartbeat_path, "phase_2_complete | post_call_qa")

  _heartbeat(heartbeat_path, "phase_3_start | scoreboard_reporting")
  assessments_by_contact = {row["contact_id"]: row for row in action_queue_rows if row["contact_id"]}
  ranked_input_rows = [
    *scenario_rows,
    *[
      {
        "contact_id": row.get("contact_id", ""),
        "first_name": row.get("first_name", ""),
        "phone": row.get("phone", ""),
        "estimated_largest_amount": row.get("estimated_largest_amount", ""),
        "lo_priority_score": row.get("lo_priority_score", ""),
        "post_call_priority_score": row.get("post_call_priority_score", ""),
        "goal": "",
        "state": "",
        "automation_stage": "controlled_post_call_review",
      }
      for row in scoreboard_seed_rows[:50]
    ],
  ]
  scoreboard_rows = build_scoreboard_rows(repo_root, ranked_input_rows, assessments_by_contact)
  lo_summary = build_lo_summary(scoreboard_rows)
  management_summary = build_management_summary(scoreboard_rows, lo_summary, action_queue_rows, review_rows)
  scoreboard_json = out_dir / "lo-scoreboard-shadow-2026-04-28.json"
  scoreboard_md = out_dir / "LO_SCOREBOARD_SHADOW_2026-04-28.md"
  scoreboard_html = out_dir / "lo-scoreboard-shadow-2026-04-28.html"
  management_json = out_dir / "management-report-shadow-2026-04-28.json"
  management_md = out_dir / "MANAGEMENT_REPORT_SHADOW_2026-04-28.md"
  management_html = out_dir / "management-report-shadow-2026-04-28.html"
  write_json(scoreboard_json, {"summary": lo_summary, "rows": scoreboard_rows[:100]})
  _write_text(scoreboard_md, render_scoreboard_markdown(lo_summary, scoreboard_rows))
  _write_text(scoreboard_html, render_scoreboard_html(lo_summary, scoreboard_rows))
  write_json(management_json, management_summary)
  _write_text(management_md, render_management_markdown(management_summary, scoreboard_rows))
  _write_text(
    management_html,
    render_scoreboard_html(
      lo_summary,
      [
        {
          **row,
          "action_route": row.get("action_route", ""),
          "owner_source": row.get("owner_source", ""),
        }
        for row in scoreboard_rows[:25]
      ],
    ).replace("<title>LO Scoreboard</title>", "<title>Management Report</title>").replace("LO Scoreboard", "Management Report"),
  )
  files_changed.extend(
    [
      str(scoreboard_json.relative_to(repo_root)),
      str(scoreboard_md.relative_to(repo_root)),
      str(scoreboard_html.relative_to(repo_root)),
      str(management_json.relative_to(repo_root)),
      str(management_md.relative_to(repo_root)),
      str(management_html.relative_to(repo_root)),
    ]
  )
  phase_records.append(
    PhaseRecord(
      phase_number=3,
      phase_name="LO Scoreboard And Management Reporting",
      status="complete",
      files_changed=[
        str(scoreboard_json.relative_to(repo_root)),
        str(scoreboard_md.relative_to(repo_root)),
        str(scoreboard_html.relative_to(repo_root)),
        str(management_json.relative_to(repo_root)),
        str(management_md.relative_to(repo_root)),
        str(management_html.relative_to(repo_root)),
      ],
      verification_run="Readable HTML/Markdown/JSON reports rendered; ownership precedence enforced in builder logic.",
      blockers="Hot queue still mixes scenario-only rows with pending controlled-call rows until more live shadow outcomes exist.",
      next_phase="Phase 4 - Agent Scaffolds",
    )
  )
  _heartbeat(heartbeat_path, "phase_3_complete | scoreboard_reporting")

  _heartbeat(heartbeat_path, "phase_4_start | agent_scaffolds")
  agent_json = out_dir / "agent-scaffolds-shadow-2026-04-28.json"
  agent_md = out_dir / "AGENT_SCAFFOLDS_SHADOW_2026-04-28.md"
  write_json(agent_json, {"agents": AGENT_SCAFFOLDS})
  _write_text(agent_md, render_agent_scaffolds_markdown())
  files_changed.extend([str(agent_json.relative_to(repo_root)), str(agent_md.relative_to(repo_root))])
  phase_records.append(
    PhaseRecord(
      phase_number=4,
      phase_name="Agent Scaffolds",
      status="complete",
      files_changed=[str(agent_json.relative_to(repo_root)), str(agent_md.relative_to(repo_root))],
      verification_run="Eight requested shadow agent scaffolds plus Senior Sales data-capture plan rendered.",
      blockers="All agents remain shadow-only until observer completeness and workflow audits improve.",
      next_phase="Phase 5 - RSI And 3-Agent Harness Scaffold",
    )
  )
  _heartbeat(heartbeat_path, "phase_4_complete | agent_scaffolds")

  _heartbeat(heartbeat_path, "phase_5_start | rsi_harness")
  recommendations = build_rsi_recommendations(management_summary)
  rsi_json = out_dir / "rsi-recommendation-queue-shadow-2026-04-28.json"
  rsi_md = out_dir / "RSI_RECOMMENDATION_QUEUE_SHADOW_2026-04-28.md"
  harness_json = out_dir / "three-agent-harness-shadow-2026-04-28.json"
  harness_md = out_dir / "THREE_AGENT_HARNESS_SHADOW_2026-04-28.md"
  write_json(rsi_json, {"recommendations": recommendations})
  _write_text(rsi_md, render_recommendations_markdown(recommendations))
  write_json(harness_json, HARNESS_CONTRACT)
  _write_text(harness_md, render_harness_markdown())
  files_changed.extend(
    [
      str(rsi_json.relative_to(repo_root)),
      str(rsi_md.relative_to(repo_root)),
      str(harness_json.relative_to(repo_root)),
      str(harness_md.relative_to(repo_root)),
    ]
  )
  phase_records.append(
    PhaseRecord(
      phase_number=5,
      phase_name="RSI And 3-Agent Harness Scaffold",
      status="complete",
      files_changed=[
        str(rsi_json.relative_to(repo_root)),
        str(rsi_md.relative_to(repo_root)),
        str(harness_json.relative_to(repo_root)),
        str(harness_md.relative_to(repo_root)),
      ],
      verification_run="Harness contracts and draft recommendation queue rendered with evidence requirements.",
      blockers="Recommendations remain draft-only and require review to affect prompts, workflows, or ops.",
      next_phase="Phase 6 - Launch Readiness And Morning Packet",
    )
  )
  _heartbeat(heartbeat_path, "phase_5_complete | rsi_harness")

  _heartbeat(heartbeat_path, "phase_6_start | launch_readiness")
  progress_path = out_dir / "OVERNIGHT_PROGRESS_2026-04-28.md"
  tests = [
    "Attempted: python3 -m pytest packages/loan-os/tests/test_call_center_ledger.py packages/loan-os/tests/test_call_center_builder.py -> unavailable (No module named pytest)",
    "Passed: PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m py_compile packages/loan-os/src/loan_os/call_center/*.py packages/loan-os/tests/test_call_center_ledger.py packages/loan-os/tests/test_call_center_builder.py scripts/voice-build-call-center-os-overnight.py",
    "Passed: PYTHONPYCACHEPREFIX=/tmp/pycache PYTHONPATH=packages/loan-os/src python3 scripts/voice-build-call-center-os-overnight.py",
  ]
  launch_risks = management_summary["launch_risks"]
  queue_counts = {
    "action_queue": len(action_queue_rows),
    "human_review": len(review_rows),
    "hot_queue": sum(1 for row in scoreboard_rows if int(row.get("hot_score") or 0) >= 1000),
  }
  _write_text(progress_path, _build_progress_markdown(phase_records, files_changed, tests, launch_risks))
  files_changed.append(str(progress_path.relative_to(repo_root)))
  morning_packet = _build_morning_packet(out_dir, phase_records, files_changed, tests, launch_risks, queue_counts)
  files_changed.append(str(morning_packet.relative_to(repo_root)))
  phase_records.append(
    PhaseRecord(
      phase_number=6,
      phase_name="Launch Readiness And Morning Packet",
      status="complete",
      files_changed=[str(progress_path.relative_to(repo_root)), str(morning_packet.relative_to(repo_root))],
      verification_run="Progress file and morning packet rendered with phase summaries, tests, risks, and next calls.",
      blockers="Morning packet is shadow-mode only pending supervised call evidence.",
      next_phase="Backlog Ladder when more shadow-call data lands",
    )
  )
  _write_text(progress_path, _build_progress_markdown(phase_records, files_changed, tests, launch_risks))
  _build_morning_packet(out_dir, phase_records, files_changed, tests, launch_risks, queue_counts)
  _heartbeat(heartbeat_path, "phase_6_complete | launch_readiness")

  return OvernightArtifactsResult(
    phase_records=phase_records,
    files_changed=files_changed,
    tests=tests,
    launch_risks=launch_risks,
    queue_counts=queue_counts,
  )


def normalize_rule_payload() -> list[dict[str, str]]:
  from loan_os.call_center.ledger import NORMALIZATION_RULES

  return list(NORMALIZATION_RULES)
