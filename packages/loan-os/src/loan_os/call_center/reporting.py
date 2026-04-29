from __future__ import annotations

import csv
import html
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping

from loan_os.call_center.ledger import amount_bucket, normalize_digits, redact_phone


KNOWN_OWNER_NAMES = {
  "dave": "Dave",
  "dominic": "Dominic Garcia",
  "dominique": "Dominic Garcia",
  "quinn": "Quinn Van Vranken",
  "sydney": "Sydney Wilson",
}

SPECIAL_OWNER_NAMES = {
  "unassigned lo review": "Unassigned LO Review",
  "unassigned review": "Unassigned LO Review",
}


def read_csv(path: Path) -> list[dict[str, str]]:
  if not path.exists():
    return []
  with path.open(encoding="utf-8", newline="") as handle:
    return list(csv.DictReader(handle))


def read_json(path: Path, default: Any) -> Any:
  if not path.exists():
    return default
  return json.loads(path.read_text(encoding="utf-8"))


def as_int(value: Any) -> int:
  try:
    return int(float(str(value or "0").replace(",", "").replace("$", "")))
  except ValueError:
    return 0


def normalize_owner_name(value: str | None) -> str:
  raw = str(value or "").strip()
  if not raw:
    return ""
  special = SPECIAL_OWNER_NAMES.get(raw.lower())
  if special:
    return special
  if raw in KNOWN_OWNER_NAMES.values():
    return raw
  first = raw.split()[0].lower()
  if first in KNOWN_OWNER_NAMES:
    return KNOWN_OWNER_NAMES[first]
  parts = []
  for part in raw.split():
    if part.lower() == "lo":
      parts.append("LO")
    else:
      parts.append(part.title())
  return " ".join(parts)


def load_user_names(repo_root: Path) -> dict[str, str]:
  output: dict[str, str] = {}
  for user in read_json(repo_root / "data" / "ghl-users.json", []):
    if not isinstance(user, dict):
      continue
    user_id = str(user.get("id") or "")
    first = str(user.get("firstName") or "").strip()
    last = str(user.get("lastName") or "").strip()
    name = normalize_owner_name(" ".join(part for part in [first, last] if part).strip())
    if user_id and name:
      output[user_id] = name
  return output


def load_transcript_owners(repo_root: Path) -> dict[str, dict[str, Any]]:
  candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
  for path in (repo_root / "data" / "voice-agent" / "call-analysis").glob("*.json"):
    data = read_json(path, {})
    call = data.get("call") if isinstance(data, dict) else {}
    analysis = data.get("analysis") if isinstance(data, dict) and isinstance(data.get("analysis"), dict) else {}
    if not isinstance(call, dict):
      continue
    contact_id = str(call.get("contactId") or "")
    owner = normalize_owner_name(str(call.get("caller") or analysis.get("lo_name_identified") or ""))
    if not contact_id or not owner:
      continue
    candidates[contact_id].append(
      {
        "owner": owner,
        "source": "transcript_evidence",
        "evidence_id": str(call.get("messageId") or path.stem),
        "duration": as_int(call.get("durationSeconds")),
        "date": str(call.get("dateAdded") or ""),
      }
    )
  output: dict[str, dict[str, Any]] = {}
  for contact_id, items in candidates.items():
    items.sort(key=lambda item: (item["duration"], item["date"]), reverse=True)
    output[contact_id] = items[0]
  return output


def load_ghl_call_owners(repo_root: Path) -> dict[str, dict[str, Any]]:
  user_names = load_user_names(repo_root)
  candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
  for row in read_json(repo_root / "data" / "ghl-calls" / "all-calls.json", []):
    if not isinstance(row, dict):
      continue
    contact_id = str(row.get("contactId") or "")
    owner = normalize_owner_name(user_names.get(str(row.get("userId") or ""), ""))
    if not contact_id or not owner:
      continue
    candidates[contact_id].append(
      {
        "owner": owner,
        "source": "ghl_call_assignment",
        "evidence_id": str(row.get("messageId") or ""),
        "duration": as_int(row.get("durationSeconds")),
        "date": str(row.get("dateAdded") or ""),
      }
    )
  output: dict[str, dict[str, Any]] = {}
  for contact_id, items in candidates.items():
    items.sort(key=lambda item: (item["duration"], item["date"]), reverse=True)
    output[contact_id] = items[0]
  return output


def select_owner(
  row: Mapping[str, Any],
  transcript_owners: Mapping[str, Mapping[str, Any]],
  ghl_call_owners: Mapping[str, Mapping[str, Any]],
) -> tuple[str, str]:
  contact_id = str(row.get("contact_id") or "")
  if contact_id and contact_id in transcript_owners:
    item = transcript_owners[contact_id]
    return str(item["owner"]), f"{item['source']}:{item['evidence_id']}"
  for field in ("assigned_lo", "suggested_owner", "owner", "loan_officer"):
    value = normalize_owner_name(str(row.get(field) or ""))
    if value:
      return value, f"ghl_assignment:{field}"
  if contact_id and contact_id in ghl_call_owners:
    item = ghl_call_owners[contact_id]
    return str(item["owner"]), f"{item['source']}:{item['evidence_id']}"
  return "Unassigned LO Review", "unassigned_review"


def hot_score(row: Mapping[str, Any], action_route: str) -> int:
  revenue = as_int(row.get("revenue_automation_score") or row.get("post_call_priority_score") or row.get("lo_priority_score"))
  amount = as_int(row.get("largest_amount") or row.get("estimated_largest_amount"))
  route_bonus = {
    "prepare_lo_handoff": 180,
    "same_day_lo_callback": 160,
    "lo_review_then_callback_or_nurture": 120,
    "await_observer_capture": 90,
  }.get(action_route, 60)
  return revenue + route_bonus + min(250, amount // 10000)


def build_scoreboard_rows(
  repo_root: Path,
  ranked_rows: list[Mapping[str, Any]],
  assessments_by_contact: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
  transcript_owners = load_transcript_owners(repo_root)
  ghl_call_owners = load_ghl_call_owners(repo_root)
  output: list[dict[str, Any]] = []
  for row in ranked_rows:
    owner, owner_source = select_owner(row, transcript_owners, ghl_call_owners)
    assessment = assessments_by_contact.get(str(row.get("contact_id") or ""), {})
    output.append(
      {
        "contact_id": str(row.get("contact_id") or ""),
        "first_name": str(row.get("first_name") or "").title(),
        "phone_redacted": redact_phone(str(row.get("phone") or "")),
        "owner": owner,
        "owner_source": owner_source,
        "hot_score": hot_score(row, str(assessment.get("route") or "")),
        "confidence": str(row.get("confidence") or assessment.get("confidence_label") or "medium"),
        "automation_stage": str(row.get("automation_stage") or ""),
        "action_route": str(assessment.get("route") or ""),
        "next_action": str(assessment.get("next_action") or row.get("recommended_tool") or ""),
        "revenue_band": amount_bucket(row.get("largest_amount") or row.get("estimated_largest_amount")),
        "estimated_amount": as_int(row.get("largest_amount") or row.get("estimated_largest_amount")),
        "state": str(row.get("state") or ""),
        "goal": str(row.get("goal") or ""),
      }
    )
  output.sort(key=lambda row: row["hot_score"], reverse=True)
  return output


def build_lo_summary(scoreboard_rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
  grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
  for row in scoreboard_rows:
    grouped[str(row.get("owner") or "Unassigned LO Review")].append(row)
  summary: list[dict[str, Any]] = []
  for owner, rows in grouped.items():
    rows = sorted(rows, key=lambda row: int(row.get("hot_score") or 0), reverse=True)
    summary.append(
      {
        "owner": owner,
        "lead_count": len(rows),
        "hot_count": sum(1 for row in rows if int(row.get("hot_score") or 0) >= 1000),
        "review_count": sum(1 for row in rows if "review" in str(row.get("action_route") or "")),
        "transcript_backed_count": sum(1 for row in rows if str(row.get("owner_source") or "").startswith("transcript_evidence")),
        "estimated_revenue_sum": sum(int(row.get("estimated_amount") or 0) for row in rows[:10]),
        "top_contacts": [f"{row.get('first_name')} ({row.get('phone_redacted')})" for row in rows[:5]],
      }
    )
  summary.sort(key=lambda row: row["estimated_revenue_sum"], reverse=True)
  return summary


def render_scoreboard_markdown(summary_rows: list[Mapping[str, Any]], lead_rows: list[Mapping[str, Any]]) -> str:
  lines = [
    "# LO Scoreboard - 2026-04-28",
    "",
    "Lead ownership uses transcript evidence first, then GHL assignment fields, then call-assignment fallback.",
    "",
    "## LO Summary",
    "",
  ]
  for row in summary_rows:
    lines.extend(
      [
        f"### {row['owner']}",
        "",
        f"- Lead count: {row['lead_count']}",
        f"- Hot leads: {row['hot_count']}",
        f"- Review queue: {row['review_count']}",
        f"- Transcript-backed ownership rows: {row['transcript_backed_count']}",
        f"- Est. top-10 opportunity amount: ${row['estimated_revenue_sum']:,}",
        f"- Top contacts: {', '.join(row['top_contacts']) if row['top_contacts'] else 'None'}",
        "",
      ]
    )
  lines.extend(["## Top Opportunity Queue", ""])
  for row in lead_rows[:25]:
    lines.append(
      f"- {row['owner']}: {row['first_name']} {row['phone_redacted']} | hot_score={row['hot_score']} | {row['action_route']} | {row['owner_source']}"
    )
  return "\n".join(lines).rstrip() + "\n"


def render_scoreboard_html(summary_rows: list[Mapping[str, Any]], lead_rows: list[Mapping[str, Any]]) -> str:
  owner_tiles = []
  for row in summary_rows:
    owner_tiles.append(
      "<article>"
      f"<h3>{html.escape(str(row['owner']))}</h3>"
      f"<p><b>{row['lead_count']}</b> leads · <b>{row['hot_count']}</b> hot · <b>{row['review_count']}</b> review</p>"
      f"<p>Transcript-backed: {row['transcript_backed_count']}</p>"
      f"<p>Top-10 est. amount: ${row['estimated_revenue_sum']:,}</p>"
      "</article>"
    )
  queue_rows = []
  for row in lead_rows[:25]:
    queue_rows.append(
      "<tr>"
      f"<td>{html.escape(str(row['owner']))}</td>"
      f"<td>{html.escape(str(row['first_name']))}</td>"
      f"<td>{html.escape(str(row['phone_redacted']))}</td>"
      f"<td>{row['hot_score']}</td>"
      f"<td>{html.escape(str(row['action_route']))}</td>"
      f"<td>{html.escape(str(row['owner_source']))}</td>"
      "</tr>"
    )
  return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>LO Scoreboard</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f4f0e8;color:#172033;margin:0}}
header{{padding:28px 32px;background:linear-gradient(135deg,#efe1c7,#fff9f1);border-bottom:1px solid #d3c3a6}}
main{{padding:24px 32px}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}}
article,section{{background:#fff;border:1px solid #d8c8aa;border-radius:10px;padding:16px}}table{{width:100%;border-collapse:collapse}}
th,td{{padding:10px;border-bottom:1px solid #eee;text-align:left}}th{{background:#fbf6ed}}
</style></head><body>
<header><h1>LO Scoreboard</h1><p>Transcript-evidence ownership first. Shadow-only recommendations.</p></header>
<main>
<section><div class="grid">{''.join(owner_tiles)}</div></section>
<section><h2>Top Opportunity Queue</h2><table><thead><tr><th>Owner</th><th>Lead</th><th>Phone</th><th>Hot Score</th><th>Route</th><th>Owner Source</th></tr></thead><tbody>{''.join(queue_rows)}</tbody></table></section>
</main></body></html>"""


def build_management_summary(
  scoreboard_rows: list[Mapping[str, Any]],
  lo_summary: list[Mapping[str, Any]],
  assessments: list[Mapping[str, Any]],
  review_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
  route_counts = Counter(str(row.get("route") or row.get("action_route") or "") for row in assessments)
  confidence_counts = Counter(str(row.get("confidence_label") or row.get("confidence") or "") for row in assessments)
  return {
    "lead_count": len(scoreboard_rows),
    "owner_count": len(lo_summary),
    "human_review_backlog": len(review_rows),
    "top_owner": lo_summary[0]["owner"] if lo_summary else "",
    "transcript_owned_count": sum(1 for row in scoreboard_rows if str(row.get("owner_source") or "").startswith("transcript_evidence")),
    "hot_queue_count": sum(1 for row in scoreboard_rows if int(row.get("hot_score") or 0) >= 1000),
    "pricing_ready_count": sum(1 for row in scoreboard_rows if str(row.get("automation_stage") or "") == "pricing_ready_review"),
    "route_counts": dict(route_counts),
    "confidence_counts": dict(confidence_counts),
    "launch_risks": [
      "Controlled queue still requires transcript and recording capture before any automation promotion.",
      "Human review backlog remains shadow-only; no GHL/LOS mutation path is authorized.",
      "Ownership fallback is strong but not universal; unassigned leads still need manual review.",
    ],
  }


def render_management_markdown(summary: Mapping[str, Any], scoreboard_rows: list[Mapping[str, Any]]) -> str:
  lines = [
    "# Management Report - 2026-04-28",
    "",
    "Shadow-mode business view generated from scenario, post-call, and review artifacts.",
    "",
    "## Summary",
    "",
    f"- Leads in scope: {summary['lead_count']}",
    f"- Owners represented: {summary['owner_count']}",
    f"- Human review backlog: {summary['human_review_backlog']}",
    f"- Transcript-backed ownership rows: {summary['transcript_owned_count']}",
    f"- Hot queue count: {summary['hot_queue_count']}",
    f"- Pricing-ready review rows: {summary['pricing_ready_count']}",
    "",
    "## Launch Risks",
    "",
  ]
  lines.extend(f"- {risk}" for risk in summary["launch_risks"])
  lines.extend(["", "## Top 15 Opportunities", ""])
  for row in scoreboard_rows[:15]:
    lines.append(
      f"- {row['first_name']} ({row['owner']}) | hot_score={row['hot_score']} | stage={row['automation_stage']} | route={row['action_route']}"
    )
  return "\n".join(lines).rstrip() + "\n"
