from __future__ import annotations

from typing import Any


AGENT_SCAFFOLDS: list[dict[str, Any]] = [
  {
    "agent_name": "Speed-to-Lead",
    "queue_name": "speed-to-lead-shadow-queue",
    "launch_stage": "shadow_only",
    "purpose": "Prepare shadow routing and review packets for fresh inbound leads without launching live calls.",
    "inputs": ["recent lead intake", "campaign/source context", "consent flags", "owner capacity"],
    "outputs": ["draft first-call route", "draft priority rank", "approval-gated callback brief"],
    "approval_gate": "Ops + manager review before any live routing or borrower touch",
    "first_test_plan": "Replay same-day inbound lead snapshots and score route quality against later human outcomes.",
  },
  {
    "agent_name": "LO Assistant",
    "queue_name": "lo-assistant-shadow-queue",
    "launch_stage": "shadow_only",
    "purpose": "Re-engage recent active leads on behalf of the owning LO.",
    "inputs": ["transcript-backed owner", "last call summary", "missing next step", "suppression flags"],
    "outputs": ["draft callback plan", "draft appointment suggestion", "draft note summary"],
    "approval_gate": "LO + manager review before any borrower-facing action",
    "first_test_plan": "Replay 10 stale active leads and compare suggested next step vs transcript evidence.",
  },
  {
    "agent_name": "No-Show Recovery",
    "queue_name": "no-show-recovery-shadow-queue",
    "launch_stage": "shadow_only",
    "purpose": "Recover missed appointments while intent is warm.",
    "inputs": ["appointment event", "assigned LO", "approved slot inventory"],
    "outputs": ["draft rebook plan", "no-show reason hypothesis", "manager escalation if repeat miss"],
    "approval_gate": "Calendar-safe review only; no live outreach",
    "first_test_plan": "Shadow 15 historical no-shows and score rebook recommendations for specificity.",
  },
  {
    "agent_name": "Document/App Completion",
    "queue_name": "doc-app-completion-shadow-queue",
    "launch_stage": "shadow_only",
    "purpose": "Identify next missing submission or document step without collecting sensitive PII by voice.",
    "inputs": ["submission status", "missing fields", "processor notes"],
    "outputs": ["draft checklist", "processor follow-up cue", "borrower-safe script suggestion"],
    "approval_gate": "Processor review before any communication",
    "first_test_plan": "Review 20 incomplete submissions and validate missing-step ordering against processor notes.",
  },
  {
    "agent_name": "Processing Condition Follow-Up",
    "queue_name": "condition-follow-up-shadow-queue",
    "launch_stage": "shadow_only",
    "purpose": "Rank outstanding conditions and propose internal follow-up.",
    "inputs": ["email submission events", "condition backlog", "owner attribution"],
    "outputs": ["condition priority queue", "draft internal ask", "risk escalation"],
    "approval_gate": "Processing manager approval only",
    "first_test_plan": "Use recent condition threads to validate severity ranking and stale-condition detection.",
  },
  {
    "agent_name": "Inbound Callback",
    "queue_name": "inbound-callback-shadow-queue",
    "launch_stage": "shadow_only",
    "purpose": "Catch callbacks and route with campaign context already known.",
    "inputs": ["caller phone", "recent outbound call", "contact context", "transfer readiness"],
    "outputs": ["draft routing decision", "callback summary", "transfer-readiness note"],
    "approval_gate": "Ops review before autonomous routing promotion",
    "first_test_plan": "Replay inbound callback fixtures and compare route accuracy against historical outcomes.",
  },
  {
    "agent_name": "Revenue Prioritization",
    "queue_name": "revenue-prioritization-shadow-queue",
    "launch_stage": "shadow_only",
    "purpose": "Rank follow-up work by expected funded revenue and risk.",
    "inputs": ["scenario ledger", "post-call QA", "owner capacity", "review backlog"],
    "outputs": ["hot opportunity queue", "LO same-day queue", "manager recommendation"],
    "approval_gate": "Manager review before queue becomes production dial order",
    "first_test_plan": "Compare top-25 queue against funded-submission hindsight when available.",
  },
  {
    "agent_name": "Manager Strategy",
    "queue_name": "manager-strategy-shadow-queue",
    "launch_stage": "shadow_only",
    "purpose": "Surface money leaks, reviewer bottlenecks, and safe experiments.",
    "inputs": ["ledger", "scoreboard", "review queue", "RSI recommendations"],
    "outputs": ["daily digest", "experiment proposals", "launch risks"],
    "approval_gate": "Management sign-off for any workflow change",
    "first_test_plan": "Generate daily digests for three historical snapshots and compare to actual bottlenecks.",
  },
  {
    "agent_name": "Post-Call QA",
    "queue_name": "post-call-qa-shadow-queue",
    "launch_stage": "shadow_only",
    "purpose": "Turn each call into a structured outcome, evidence packet, and next action.",
    "inputs": ["transcript", "recording metadata", "call metrics", "scenario context"],
    "outputs": ["outcome", "route", "confidence", "approval state", "draft internal summary"],
    "approval_gate": "Human review required for every risky action",
    "first_test_plan": "Run 25 calls through shadow classification and measure agreement with manual review.",
  },
  {
    "agent_name": "Senior Sales Data-Capture",
    "queue_name": "senior-sales-data-capture-shadow-queue",
    "launch_stage": "research_only",
    "purpose": "Define what deeper sales conversations must capture before autonomy is considered.",
    "inputs": ["pricing-ready scenarios", "objection clusters", "human review notes"],
    "outputs": ["data-capture spec", "guardrail gaps", "future prompt requirements"],
    "approval_gate": "Research artifact only",
    "first_test_plan": "Review 15 pricing-ready transcripts and enumerate missing deterministic facts.",
  },
]


def render_agent_scaffolds_markdown() -> str:
  lines = [
    "# Agent Scaffolds - 2026-04-28",
    "",
    "All agents below remain shadow/draft only unless explicitly noted otherwise in a later approval flow.",
    "",
  ]
  for item in AGENT_SCAFFOLDS:
    lines.extend(
      [
        f"## {item['agent_name']}",
        "",
        f"- Queue: `{item['queue_name']}`",
        f"- Launch stage: `{item['launch_stage']}`",
        f"- Purpose: {item['purpose']}",
        f"- Inputs: {', '.join(item['inputs'])}",
        f"- Outputs: {', '.join(item['outputs'])}",
        f"- Approval gate: {item['approval_gate']}",
        f"- First test plan: {item['first_test_plan']}",
        "",
      ]
    )
  return "\n".join(lines).rstrip() + "\n"
