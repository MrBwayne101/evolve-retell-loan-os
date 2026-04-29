from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


HARNESS_CONTRACT = {
  "roles": [
    {
      "role": "Strategist",
      "job": "Read event ledger, scoreboards, and QA findings; propose a recommendation with evidence.",
      "required_output": ["problem_statement", "evidence_refs", "expected_lift", "risk"],
    },
    {
      "role": "Drafter",
      "job": "Convert the strategy recommendation into a concrete draft artifact or queue update.",
      "required_output": ["artifact_type", "draft_payload", "dependencies", "approval_state"],
    },
    {
      "role": "Reviewer",
      "job": "Approve, revise, reject, or escalate using compliance and evidence gates.",
      "required_output": ["decision", "reason", "missing_evidence", "revision_count"],
    },
  ],
  "revision_cap": 2,
  "autonomy_mode": "shadow_only",
  "required_evidence": [
    "event_ledger_refs",
    "scoreboard_metrics",
    "human_review_or_transcript_excerpt",
  ],
}


def build_rsi_recommendations(summary: Mapping[str, Any]) -> list[dict[str, Any]]:
  recommendations: list[dict[str, Any]] = []
  if int(summary.get("human_review_backlog") or 0) > 20:
    recommendations.append(
      {
        "recommendation_id": "rsi-2026-04-28-001",
        "priority": "high",
        "theme": "review_backlog",
        "proposal": "Create a reviewer-first queue split between pricing-ready and transcript-missing leads.",
        "expected_lift": "Reduce same-day opportunity leakage on hot leads.",
        "risk": "medium",
        "approval_state": "draft",
        "evidence": [
          f"human_review_backlog={summary.get('human_review_backlog')}",
          f"pricing_ready_count={summary.get('pricing_ready_count')}",
        ],
      }
    )
  if int(summary.get("transcript_owned_count") or 0) < int(summary.get("lead_count") or 0) * 0.5:
    recommendations.append(
      {
        "recommendation_id": "rsi-2026-04-28-002",
        "priority": "high",
        "theme": "ownership_evidence",
        "proposal": "Increase transcript-backed ownership coverage before promoting LO assistant automation.",
        "expected_lift": "Improve follow-up routing accuracy and reduce wrong-owner outreach.",
        "risk": "low",
        "approval_state": "draft",
        "evidence": [
          f"transcript_owned_count={summary.get('transcript_owned_count')}",
          f"lead_count={summary.get('lead_count')}",
        ],
      }
    )
  recommendations.append(
    {
      "recommendation_id": "rsi-2026-04-28-003",
      "priority": "medium",
      "theme": "observer_completeness",
      "proposal": "Require transcript, recording, transfer result, and QA route before any post-call automation leaves shadow mode.",
      "expected_lift": "Prevents partial evidence from driving borrower-facing mistakes.",
      "risk": "low",
      "approval_state": "draft",
      "evidence": [
        "post_call_queue_requires_observer_capture",
        "shadow_only_governance",
      ],
    }
  )
  return recommendations


def build_governed_rsi_recommendations(
  summary: Mapping[str, Any],
  qa_rows: list[Mapping[str, Any]],
  followup_queue: list[Mapping[str, Any]],
  replay_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
  outcome_counts = Counter(str(row.get("outcome") or "unknown") for row in qa_rows)
  transcript_covered = int(replay_summary.get("transcript_calls") or 0)
  shadow_call_count = int(replay_summary.get("call_state_count") or len(qa_rows) or 1)
  transcript_coverage = round(transcript_covered / max(shadow_call_count, 1), 3)
  missed_transfers = sum(1 for row in qa_rows if str(row.get("transfer_status") or "").startswith("failed"))
  appointment_fallbacks = sum(
    1 for row in qa_rows if str(row.get("appointment_result") or "") in {"slots_offered", "booking_error", "fallback_discussed"}
  )
  transcript_backed_owners = sum(1 for row in followup_queue if row.get("transcript_backed_owner"))
  high_value_unassigned = sum(
    1
    for row in followup_queue
    if str(row.get("owner") or "") == "Unassigned LO Review" and int(row.get("estimated_amount") or 0) >= 250000
  )

  recommendations: list[dict[str, Any]] = [
    {
      "recommendation_id": "rsi-2026-04-29-001",
      "priority": "high" if transcript_coverage < 0.8 else "medium",
      "theme": "observer_completeness",
      "recommendation": "Close transcript/contact gaps before any post-call action is considered for promotion.",
      "expected_impact": "Higher reconstruction reliability and fewer wrong-owner or no-evidence follow-ups.",
      "approval_gate": "Manager + ops review required before changing observer or routing rules.",
      "post_change_measurement_plan": [
        "Track transcript coverage on shadow calls for the next 25 analyzed calls.",
        "Track resolved contact coverage and owner coverage on the same batch.",
        "Compare observer-gap queue size before and after the change.",
      ],
      "rollback_plan": "Revert to the last shadow-only observer ruleset and mark new fields as advisory-only if coverage regresses.",
      "result_attribution": "Use event-ledger replay counts plus observer-gap queue deltas by build date.",
      "baseline_metrics": {
        "shadow_call_count": shadow_call_count,
        "transcript_coverage": transcript_coverage,
        "observer_gap_count": int(summary.get("observer_gap_count") or 0),
      },
      "evidence_refs": [
        f"event_replay:transcript_calls={transcript_covered}",
        f"event_replay:call_state_count={shadow_call_count}",
        f"management_summary:observer_gap_count={summary.get('observer_gap_count')}",
      ],
      "approval_state": "draft",
    },
    {
      "recommendation_id": "rsi-2026-04-29-002",
      "priority": "high" if high_value_unassigned > 0 else "medium",
      "theme": "owner_attribution",
      "recommendation": "Prioritize transcript-backed ownership resolution on high-value calls before enabling LO assistant automation.",
      "expected_impact": "Improves same-day follow-up accuracy and reduces wrong-owner queue assignments.",
      "approval_gate": "Sales manager review plus transcript spot-check on top 10 impacted leads.",
      "post_change_measurement_plan": [
        "Measure transcript-backed owner coverage on the top 25 revenue queue rows.",
        "Measure unassigned high-value row count on the next daily build.",
        "Spot-check five owner resolutions against transcript evidence.",
      ],
      "rollback_plan": "Fall back to scenario-suggested owners only and require manual assignment if owner evidence quality drops.",
      "result_attribution": "Compare follow-up queue owner_source and transcript_backed_owner rates by build date.",
      "baseline_metrics": {
        "transcript_backed_owner_count": transcript_backed_owners,
        "followup_queue_count": len(followup_queue),
        "high_value_unassigned": high_value_unassigned,
      },
      "evidence_refs": [
        f"followup_queue:transcript_backed_owner_count={transcript_backed_owners}",
        f"followup_queue:count={len(followup_queue)}",
        f"followup_queue:high_value_unassigned={high_value_unassigned}",
      ],
      "approval_state": "draft",
    },
    {
      "recommendation_id": "rsi-2026-04-29-003",
      "priority": "high" if missed_transfers or appointment_fallbacks else "medium",
      "theme": "handoff_recovery",
      "recommendation": "Create a manager-reviewed recovery lane for failed transfers and appointment fallout before broadening callback automation.",
      "expected_impact": "Captures warm intent that would otherwise leak after transfer or booking failures.",
      "approval_gate": "LO manager review before any callback order or appointment recovery workflow change.",
      "post_change_measurement_plan": [
        "Track missed-transfer rows and appointment-fallback rows for the next 20 qualifying calls.",
        "Measure same-day recovery completion rate in shadow mode.",
        "Compare callback queue ordering changes against manual LO review.",
      ],
      "rollback_plan": "Disable the recovery lane scoring bonus and return those rows to the generic human-review queue.",
      "result_attribution": "Use transfer_result, appointment_result, and follow-up queue outcome counts by build date.",
      "baseline_metrics": {
        "missed_transfer_count": missed_transfers,
        "appointment_fallback_count": appointment_fallbacks,
        "hot_callback_count": outcome_counts.get("hot_callback", 0),
      },
      "evidence_refs": [
        f"qa_rows:missed_transfers={missed_transfers}",
        f"qa_rows:appointment_fallbacks={appointment_fallbacks}",
        f"qa_rows:hot_callback={outcome_counts.get('hot_callback', 0)}",
      ],
      "approval_state": "draft",
    },
  ]
  return recommendations


def render_harness_markdown() -> str:
  lines = [
    "# Three-Agent Harness Shadow Spec - 2026-04-28",
    "",
    "The harness remains async and shadow-only. Nothing here is wired into live calls or external writes.",
    "",
    "## Roles",
    "",
  ]
  for role in HARNESS_CONTRACT["roles"]:
    lines.extend(
      [
        f"### {role['role']}",
        "",
        f"- Job: {role['job']}",
        f"- Required output: {', '.join(role['required_output'])}",
        "",
      ]
    )
  lines.extend(
    [
      "## Rules",
      "",
      f"- Revision cap: {HARNESS_CONTRACT['revision_cap']}",
      f"- Autonomy mode: {HARNESS_CONTRACT['autonomy_mode']}",
      f"- Required evidence: {', '.join(HARNESS_CONTRACT['required_evidence'])}",
      "",
    ]
  )
  return "\n".join(lines).rstrip() + "\n"


def render_recommendations_markdown(recommendations: list[Mapping[str, Any]]) -> str:
  lines = [
    "# RSI Recommendation Queue - 2026-04-28",
    "",
    "Draft recommendations only. None auto-deploy prompts, workflows, or borrower-facing changes.",
    "",
  ]
  for item in recommendations:
    lines.extend(
      [
        f"## {item['recommendation_id']}",
        "",
        f"- Priority: {item['priority']}",
        f"- Theme: {item['theme']}",
        f"- Proposal: {item['proposal']}",
        f"- Expected lift: {item['expected_lift']}",
        f"- Risk: {item['risk']}",
        f"- Approval state: {item['approval_state']}",
        f"- Evidence: {', '.join(item['evidence'])}",
        "",
      ]
    )
  return "\n".join(lines).rstrip() + "\n"


def render_governed_recommendations_markdown(recommendations: list[Mapping[str, Any]]) -> str:
  lines = [
    "# RSI Recommendation Queue - 2026-04-29",
    "",
    "Governed shadow recommendations only. None of these items auto-deploy prompts, workflows, or borrower-facing changes.",
    "",
  ]
  for item in recommendations:
    baseline_metrics = item.get("baseline_metrics") if isinstance(item.get("baseline_metrics"), Mapping) else {}
    lines.extend(
      [
        f"## {item['recommendation_id']}",
        "",
        f"- Priority: {item['priority']}",
        f"- Theme: {item['theme']}",
        f"- Recommendation: {item['recommendation']}",
        f"- Expected impact: {item['expected_impact']}",
        f"- Approval gate: {item['approval_gate']}",
        f"- Approval state: {item['approval_state']}",
        f"- Evidence: {', '.join(item['evidence_refs'])}",
        f"- Measurement plan: {'; '.join(item['post_change_measurement_plan'])}",
        f"- Rollback: {item['rollback_plan']}",
        f"- Result attribution: {item['result_attribution']}",
        f"- Baseline metrics: {', '.join(f'{key}={value}' for key, value in baseline_metrics.items())}",
        "",
      ]
    )
  return "\n".join(lines).rstrip() + "\n"
