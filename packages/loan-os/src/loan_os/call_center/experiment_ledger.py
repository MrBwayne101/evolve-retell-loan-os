from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


def utc_now_iso() -> str:
  return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_id(*parts: object, length: int = 16) -> str:
  digest = hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()
  return digest[:length]


def _text(value: Any, default: str = "") -> str:
  if value is None:
    return default
  if isinstance(value, str):
    return value.strip() or default
  return str(value).strip() or default


def _as_list(value: Any) -> list[Any]:
  if isinstance(value, list):
    return value
  if value in (None, ""):
    return []
  return [value]


def _version_value(config: Mapping[str, Any], *keys: str, default: str) -> str:
  for key in keys:
    value = _text(config.get(key))
    if value:
      return value
  return default


@dataclass(frozen=True)
class ExperimentRecord:
  experiment_id: str
  generated_at: str
  recommendation_id: str
  theme: str
  priority: str
  agent_role: str
  agent_version: str
  voice_version: str
  opener_version: str
  objection_script_version: str
  batch_id: str
  source_campaign: str
  hypothesis: str
  approval_state: str
  deployment_state: str
  risk: str
  sample_size: int
  baseline_metrics: dict[str, Any]
  outcome_metrics: dict[str, Any]
  evidence_refs: list[str]
  measurement_plan: list[str]
  rollback_plan: str
  next_gate: str

  def to_dict(self) -> dict[str, Any]:
    return {
      "experiment_id": self.experiment_id,
      "generated_at": self.generated_at,
      "recommendation_id": self.recommendation_id,
      "theme": self.theme,
      "priority": self.priority,
      "agent_role": self.agent_role,
      "agent_version": self.agent_version,
      "voice_version": self.voice_version,
      "opener_version": self.opener_version,
      "objection_script_version": self.objection_script_version,
      "batch_id": self.batch_id,
      "source_campaign": self.source_campaign,
      "hypothesis": self.hypothesis,
      "approval_state": self.approval_state,
      "deployment_state": self.deployment_state,
      "risk": self.risk,
      "sample_size": self.sample_size,
      "baseline_metrics": self.baseline_metrics,
      "outcome_metrics": self.outcome_metrics,
      "evidence_refs": self.evidence_refs,
      "measurement_plan": self.measurement_plan,
      "rollback_plan": self.rollback_plan,
      "next_gate": self.next_gate,
    }


def summarize_outcomes(qa_rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
  rows = list(qa_rows)
  counts = Counter(_text(row.get("outcome"), "unknown") for row in rows)
  transfer_count = sum(1 for row in rows if _text(row.get("transfer_status")).startswith(("connected", "success")))
  missed_transfer_count = sum(1 for row in rows if _text(row.get("transfer_status")).startswith(("failed", "missed")))
  appointment_count = sum(1 for row in rows if _text(row.get("appointment_result")) in {"booked", "appointment_booked"})
  conversation_count = sum(
    1
    for row in rows
    if _text(row.get("outcome")) not in {"no_answer_or_short", "voicemail", "screener"}
  )
  return {
    "row_count": len(rows),
    "outcome_counts": dict(counts),
    "conversation_count": conversation_count,
    "transfer_count": transfer_count,
    "missed_transfer_count": missed_transfer_count,
    "appointment_count": appointment_count,
  }


def _harness_payload(item: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]:
  strategist = item.get("strategist") if isinstance(item.get("strategist"), Mapping) else {}
  drafter = item.get("drafter") if isinstance(item.get("drafter"), Mapping) else {}
  reviewer = item.get("reviewer") if isinstance(item.get("reviewer"), Mapping) else {}
  draft_payload = drafter.get("draft_payload") if isinstance(drafter.get("draft_payload"), Mapping) else {}
  return strategist, draft_payload, reviewer


def build_experiment_records(
  harness_results: Iterable[Mapping[str, Any]],
  qa_rows: Iterable[Mapping[str, Any]],
  *,
  generated_at: str | None = None,
  config: Mapping[str, Any] | None = None,
) -> list[ExperimentRecord]:
  stamp = generated_at or utc_now_iso()
  config = config or {}
  outcome_metrics = summarize_outcomes(qa_rows)
  records: list[ExperimentRecord] = []

  for item in harness_results:
    strategist, draft_payload, reviewer = _harness_payload(item)
    recommendation_id = _text(item.get("recommendation_id") or draft_payload.get("recommendation_id"), "unknown")
    theme = _text(item.get("theme") or strategist.get("theme"), "unknown")
    final_state = _text(item.get("final_state"), "review_required")
    reviewer_decision = _text(reviewer.get("decision"), final_state)
    evidence = strategist.get("evidence") if isinstance(strategist.get("evidence"), Mapping) else {}
    evidence_refs = [str(value) for key in ("event_ledger_refs", "scoreboard_metrics", "human_review_or_transcript_excerpt") for value in _as_list(evidence.get(key))]
    baseline_metrics = {}
    if isinstance(strategist.get("baseline_metrics"), Mapping):
      baseline_metrics = dict(strategist["baseline_metrics"])
    elif evidence.get("scoreboard_metrics"):
      baseline_metrics = {"scoreboard_metrics": list(evidence.get("scoreboard_metrics") or [])}

    approval_state = "ready_for_manager_review" if final_state == "approved_internal_shadow" else "evidence_required"
    deployment_state = "not_deployed_shadow_only"
    records.append(
      ExperimentRecord(
        experiment_id=stable_id("rsi_experiment", recommendation_id, theme, stamp),
        generated_at=stamp,
        recommendation_id=recommendation_id,
        theme=theme,
        priority=_text(item.get("priority"), "medium"),
        agent_role=_version_value(config, "agent_role", default="call_center_os"),
        agent_version=_version_value(config, "agent_version", default="shadow-2026-04-29"),
        voice_version=_version_value(config, "voice_version", default="retell-current-shadow"),
        opener_version=_version_value(config, "opener_version", default="manual-review-required"),
        objection_script_version=_version_value(config, "objection_script_version", default="manual-review-required"),
        batch_id=_version_value(config, "batch_id", default="shadow-batch-2026-04-29"),
        source_campaign=_version_value(config, "source_campaign", "campaign", default="mixed_or_unknown"),
        hypothesis=_text(draft_payload.get("proposed_change") or strategist.get("proposal"), "No hypothesis drafted."),
        approval_state=approval_state,
        deployment_state=deployment_state,
        risk=_text(strategist.get("risk"), "review_required"),
        sample_size=int(outcome_metrics["row_count"]),
        baseline_metrics=baseline_metrics,
        outcome_metrics=dict(outcome_metrics),
        evidence_refs=evidence_refs,
        measurement_plan=[str(value) for value in _as_list(draft_payload.get("measurement_plan"))],
        rollback_plan=_text(draft_payload.get("rollback_plan"), "Keep current production config; no rollout occurred."),
        next_gate="manager_review" if final_state == "approved_internal_shadow" and reviewer_decision == "approve_internal_shadow" else "attach_missing_evidence",
      )
    )
  return records


def render_experiment_ledger_markdown(records: Iterable[ExperimentRecord]) -> str:
  rows = list(records)
  ready = sum(1 for row in rows if row.approval_state == "ready_for_manager_review")
  blocked = len(rows) - ready
  lines = [
    "# RSI Experiment Ledger - 2026-04-29",
    "",
    "Shadow-only ledger for governed script, voice, opener, and scoring experiments. Nothing here deploys automatically.",
    "",
    f"- Experiments: {len(rows)}",
    f"- Ready for manager review: {ready}",
    f"- Evidence required: {blocked}",
    "",
  ]
  for row in rows:
    outcome_counts = row.outcome_metrics.get("outcome_counts") if isinstance(row.outcome_metrics.get("outcome_counts"), Mapping) else {}
    lines.extend(
      [
        f"## {row.experiment_id}",
        "",
        f"- Recommendation: {row.recommendation_id}",
        f"- Theme: {row.theme}",
        f"- Priority: {row.priority}",
        f"- Agent/version: {row.agent_role} / {row.agent_version}",
        f"- Voice/opener/script: {row.voice_version} / {row.opener_version} / {row.objection_script_version}",
        f"- Batch/source: {row.batch_id} / {row.source_campaign}",
        f"- Approval state: {row.approval_state}",
        f"- Deployment state: {row.deployment_state}",
        f"- Next gate: {row.next_gate}",
        f"- Sample size: {row.sample_size}",
        f"- Outcome counts: {json.dumps(dict(outcome_counts), sort_keys=True)}",
        f"- Hypothesis: {row.hypothesis}",
        f"- Evidence refs: {', '.join(row.evidence_refs) or 'none'}",
        f"- Measurement plan: {'; '.join(row.measurement_plan) or 'not defined'}",
        f"- Rollback: {row.rollback_plan}",
        "",
      ]
    )
  return "\n".join(lines).rstrip() + "\n"


def write_experiment_ledger_artifacts(records: list[ExperimentRecord], *, out_dir: Path, date_label: str) -> dict[str, str]:
  out_dir.mkdir(parents=True, exist_ok=True)
  json_path = out_dir / f"rsi-experiment-ledger-{date_label}.json"
  jsonl_path = out_dir / f"rsi-experiment-ledger-{date_label}.jsonl"
  md_path = out_dir / f"RSI_EXPERIMENT_LEDGER_{date_label}.md"
  payload = [row.to_dict() for row in records]
  json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
  jsonl_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in payload) + "\n", encoding="utf-8")
  md_path.write_text(render_experiment_ledger_markdown(records), encoding="utf-8")
  return {"json": str(json_path), "jsonl": str(jsonl_path), "markdown": str(md_path)}
