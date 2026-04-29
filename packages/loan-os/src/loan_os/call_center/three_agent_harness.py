from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


REQUIRED_EVIDENCE = ("event_ledger_refs", "scoreboard_metrics", "human_review_or_transcript_excerpt")
REVISION_CAP = 2


def utc_now_iso() -> str:
  return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_id(*parts: object, length: int = 16) -> str:
  digest = hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()
  return digest[:length]


def _as_list(value: Any) -> list[Any]:
  if isinstance(value, list):
    return value
  if value in (None, ""):
    return []
  return [value]


def _text(value: Any) -> str:
  if value is None:
    return ""
  if isinstance(value, str):
    return value.strip()
  return str(value).strip()


def _evidence_from_recommendation(item: Mapping[str, Any]) -> dict[str, list[str]]:
  evidence_refs = [_text(value) for value in _as_list(item.get("evidence_refs") or item.get("evidence")) if _text(value)]
  metrics = []
  baseline = item.get("baseline_metrics")
  if isinstance(baseline, Mapping):
    metrics = [f"{key}={value}" for key, value in baseline.items()]
  excerpts = []
  for key in ("transcript_excerpt", "call_excerpt", "human_review_excerpt", "evidence_excerpt"):
    if _text(item.get(key)):
      excerpts.append(_text(item.get(key)))

  return {
    "event_ledger_refs": evidence_refs,
    "scoreboard_metrics": metrics,
    "human_review_or_transcript_excerpt": excerpts,
  }


def _missing_evidence(evidence: Mapping[str, list[str]]) -> list[str]:
  return [key for key in REQUIRED_EVIDENCE if not evidence.get(key)]


@dataclass(frozen=True)
class HarnessResult:
  run_id: str
  generated_at: str
  recommendation_id: str
  theme: str
  priority: str
  strategist: dict[str, Any]
  drafter: dict[str, Any]
  reviewer: dict[str, Any]
  final_state: str
  revision_count: int

  def to_dict(self) -> dict[str, Any]:
    return {
      "run_id": self.run_id,
      "generated_at": self.generated_at,
      "recommendation_id": self.recommendation_id,
      "theme": self.theme,
      "priority": self.priority,
      "strategist": self.strategist,
      "drafter": self.drafter,
      "reviewer": self.reviewer,
      "final_state": self.final_state,
      "revision_count": self.revision_count,
    }


def strategist_stage(item: Mapping[str, Any]) -> dict[str, Any]:
  evidence = _evidence_from_recommendation(item)
  proposal = _text(item.get("recommendation") or item.get("proposal"))
  problem = _text(item.get("problem_statement")) or proposal
  return {
    "role": "Strategist",
    "problem_statement": problem,
    "proposal": proposal,
    "theme": _text(item.get("theme")) or "unknown",
    "priority": _text(item.get("priority")) or "medium",
    "expected_lift": _text(item.get("expected_impact") or item.get("expected_lift")),
    "risk": _text(item.get("risk")) or "review_required",
    "evidence": evidence,
    "missing_evidence": _missing_evidence(evidence),
  }


def drafter_stage(item: Mapping[str, Any], strategy: Mapping[str, Any]) -> dict[str, Any]:
  recommendation_id = _text(item.get("recommendation_id")) or stable_id(strategy.get("proposal"))
  theme = _text(strategy.get("theme"))
  draft_payload = {
    "recommendation_id": recommendation_id,
    "theme": theme,
    "change_type": "shadow_recommendation",
    "proposed_change": _text(strategy.get("proposal")),
    "approval_gate": _text(item.get("approval_gate")) or "manager_review",
    "measurement_plan": _as_list(item.get("post_change_measurement_plan")),
    "rollback_plan": _text(item.get("rollback_plan")) or "revert_to_previous_shadow_artifact",
    "result_attribution": _text(item.get("result_attribution")) or "compare_before_after_event_ledger_metrics",
  }
  return {
    "role": "Drafter",
    "artifact_type": "governed_rsi_change_request",
    "draft_payload": draft_payload,
    "dependencies": [
      "event_ledger",
      "post_call_qa",
      "management_scoreboard",
      "human_approval",
    ],
    "approval_state": "review_required",
  }


def reviewer_stage(strategy: Mapping[str, Any], draft: Mapping[str, Any], *, revision_count: int = 0) -> dict[str, Any]:
  missing = list(strategy.get("missing_evidence") or [])
  if missing:
    decision = "revise" if revision_count < REVISION_CAP else "escalate"
    reason = "Missing required evidence before any recommendation can be promoted."
  elif not _text(draft.get("draft_payload", {}).get("proposed_change")):
    decision = "reject"
    reason = "No concrete proposed change was drafted."
  else:
    decision = "approve_internal_shadow"
    reason = "Required evidence is present and the artifact remains shadow-only behind human approval."
  return {
    "role": "Reviewer",
    "decision": decision,
    "reason": reason,
    "missing_evidence": missing,
    "revision_count": revision_count,
    "revision_cap": REVISION_CAP,
  }


def run_harness(recommendations: Iterable[Mapping[str, Any]], *, generated_at: str | None = None) -> list[HarnessResult]:
  stamp = generated_at or utc_now_iso()
  results: list[HarnessResult] = []
  for item in recommendations:
    recommendation_id = _text(item.get("recommendation_id")) or stable_id(json.dumps(item, sort_keys=True))
    strategy = strategist_stage(item)
    draft = drafter_stage(item, strategy)
    review = reviewer_stage(strategy, draft)
    revision_count = 0
    if review["decision"] == "revise":
      revision_count = 1
      draft = dict(draft)
      draft["revision_request"] = {
        "reason": review["reason"],
        "missing_evidence": review["missing_evidence"],
        "required_before_approval": "Attach real event-ledger, scoreboard, and transcript or human-review evidence.",
      }
      review = {
        **review,
        "decision": "review_required",
        "reason": "Recommendation stayed in review because the harness cannot synthesize missing evidence.",
        "revision_count": revision_count,
      }

    if review["decision"] == "approve_internal_shadow":
      final_state = "approved_internal_shadow"
    elif review["decision"] == "reject":
      final_state = "rejected"
    elif review["decision"] == "escalate":
      final_state = "escalated"
    else:
      final_state = "review_required"

    results.append(
      HarnessResult(
        run_id=stable_id("three_agent_harness", recommendation_id, stamp),
        generated_at=stamp,
        recommendation_id=recommendation_id,
        theme=_text(item.get("theme")) or _text(strategy.get("theme")) or "unknown",
        priority=_text(item.get("priority")) or "medium",
        strategist=dict(strategy),
        drafter=dict(draft),
        reviewer=dict(review),
        final_state=final_state,
        revision_count=revision_count,
      )
    )
  return results


def render_harness_results_markdown(results: Iterable[HarnessResult]) -> str:
  rows = list(results)
  lines = [
    "# Three-Agent RSI Harness Run",
    "",
    "Executable Strategist -> Drafter -> Reviewer shadow run. No recommendation is deployed automatically.",
    "",
    f"- Results: {len(rows)}",
    f"- Approved internal shadow: {sum(1 for row in rows if row.final_state == 'approved_internal_shadow')}",
    f"- Review required: {sum(1 for row in rows if row.final_state == 'review_required')}",
    f"- Escalated/rejected: {sum(1 for row in rows if row.final_state in {'escalated', 'rejected'})}",
    "",
  ]
  for row in rows:
    reviewer = row.reviewer
    draft_payload = row.drafter.get("draft_payload", {})
    lines.extend(
      [
        f"## {row.recommendation_id}",
        "",
        f"- Theme: {row.theme}",
        f"- Priority: {row.priority}",
        f"- Final state: {row.final_state}",
        f"- Reviewer decision: {reviewer.get('decision')}",
        f"- Reviewer reason: {reviewer.get('reason')}",
        f"- Missing evidence: {', '.join(reviewer.get('missing_evidence') or []) or 'none'}",
        f"- Proposed change: {draft_payload.get('proposed_change')}",
        f"- Approval gate: {draft_payload.get('approval_gate')}",
        f"- Rollback: {draft_payload.get('rollback_plan')}",
        "",
      ]
    )
  return "\n".join(lines).rstrip() + "\n"


def write_harness_artifacts(results: list[HarnessResult], *, out_dir: Path, date_label: str) -> dict[str, str]:
  out_dir.mkdir(parents=True, exist_ok=True)
  json_path = out_dir / f"three-agent-harness-run-{date_label}.json"
  jsonl_path = out_dir / f"three-agent-harness-run-{date_label}.jsonl"
  md_path = out_dir / f"THREE_AGENT_HARNESS_RUN_{date_label}.md"
  payload = [row.to_dict() for row in results]
  json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
  jsonl_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in payload) + "\n", encoding="utf-8")
  md_path.write_text(render_harness_results_markdown(results), encoding="utf-8")
  return {"json": str(json_path), "jsonl": str(jsonl_path), "markdown": str(md_path)}
