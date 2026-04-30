from __future__ import annotations

import argparse
import csv
import hmac
import html
import json
import logging
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib import error, request

from aiohttp import web

from loan_os.ghl_calendar import book_selected_slot as book_selected_calendar_slot
from loan_os.ghl_calendar import format_slot_options, get_availability, select_spread_slots


LOGGER = logging.getLogger("loan_os.retell.proof_server")


def _default_repo_root() -> Path:
  return Path(os.getenv("EVOLVE_REPO_ROOT", "/Users/brucewayne/evolve-integration")).expanduser()


def _default_data_root() -> Path:
  return Path(os.getenv("EVOLVE_DATA_ROOT", str(_default_repo_root() / "data"))).expanduser()


REPO_ROOT = _default_repo_root()
DATA_ROOT = _default_data_root()
EVENT_DIR = DATA_ROOT / "voice-agent" / "retell"
EVENT_LOG = EVENT_DIR / "events.jsonl"
CONFIG_PATH = EVENT_DIR / "proof-config.json"
WEB_BUNDLE_PATH = REPO_ROOT / "data" / "voice-agent" / "retell-web" / "dist" / "client.bundle.js"
CALL_CAPTURE_DIR = EVENT_DIR / "calls"
GHL_NOTE_DIR = EVENT_DIR / "ghl-notes"
GHL_NOTE_RETRY_DIR = EVENT_DIR / "ghl-notes-retry"
REACTIVATION_DIR = DATA_ROOT / "voice-agent" / "reactivation-enrichment"
SCOREBOARD_DIR = DATA_ROOT / "loan-os" / "scoreboards"
RECENT_LO_SCOREBOARD_SOURCE = SCOREBOARD_DIR / "recent-lo-source.csv"
SCOREBOARD_ACCESS_TOKEN_FILE = SCOREBOARD_DIR / "access-token"


def _now_ms() -> int:
  return int(time.time() * 1000)


def _append_event(kind: str, payload: dict[str, Any]) -> None:
  EVENT_DIR.mkdir(parents=True, exist_ok=True)
  record = {
    "kind": kind,
    "timestamp_ms": _now_ms(),
    "payload": payload,
  }
  with EVENT_LOG.open("a", encoding="utf-8") as f:
    f.write(json.dumps(record, ensure_ascii=False) + "\n")
  LOGGER.info("event", extra={"event_kind": kind})


def _setup_logging() -> None:
  level = os.getenv("LOG_LEVEL", "INFO").upper()
  logging.basicConfig(
    level=getattr(logging, level, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
  )


def _kill_switch_active() -> bool:
  if os.getenv("RETELL_KILL_SWITCH", "").strip().lower() in {"1", "true", "yes", "on"}:
    return True
  kill_file = os.getenv("RETELL_KILL_FILE")
  if kill_file and Path(kill_file).expanduser().exists():
    return True
  return (EVENT_DIR / "KILL_SWITCH").exists()


def _side_effects_disabled() -> bool:
  return os.getenv("RETELL_DISABLE_SIDE_EFFECTS", "").strip().lower() in {"1", "true", "yes", "on"}


def _admin_authorized(request: web.Request) -> bool:
  token = os.getenv("RETELL_ADMIN_TOKEN", "").strip()
  if not token:
    return False
  supplied = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
  return hmac.compare_digest(supplied, token)


def _scoreboard_authorized(request: web.Request) -> bool:
  token = os.getenv("SCOREBOARD_ACCESS_TOKEN", "").strip()
  if not token and SCOREBOARD_ACCESS_TOKEN_FILE.exists():
    token = SCOREBOARD_ACCESS_TOKEN_FILE.read_text(encoding="utf-8", errors="ignore").strip()
  if not token:
    token = os.getenv("RETELL_ADMIN_TOKEN", "").strip()
  if not token:
    return False
  supplied = (
    request.query.get("token", "")
    or request.cookies.get("scoreboard_token", "")
    or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
  ).strip()
  return hmac.compare_digest(supplied, token)


@web.middleware
async def operational_guard(request: web.Request, handler):  # noqa: ANN001
  if request.path.startswith("/admin/"):
    if not _admin_authorized(request):
      return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
  if _kill_switch_active() and not request.path.startswith(("/health", "/ready", "/admin/")):
    return web.json_response({"ok": False, "error": "kill_switch_active"}, status=503)
  return await handler(request)


def _extract_args(payload: dict[str, Any]) -> dict[str, Any]:
  for key in ("args", "arguments", "parameters"):
    value = payload.get(key)
    if isinstance(value, dict):
      return value
  tool_call = payload.get("tool_call")
  if isinstance(tool_call, dict):
    for key in ("args", "arguments", "parameters"):
      value = tool_call.get(key)
      if isinstance(value, dict):
        return value
  return {}


def _digits(value: str | None) -> str:
  return re.sub(r"\D+", "", value or "")


def _as_int(value: Any) -> int:
  try:
    return int(float(str(value or "0").strip()))
  except ValueError:
    return 0


def _parse_dt(value: str | None) -> datetime | None:
  raw = str(value or "").strip()
  if not raw:
    return None
  try:
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
  except ValueError:
    return None


def _money(value: Any) -> str:
  amount = _as_int(value)
  if amount >= 1_000_000:
    return f"${amount / 1_000_000:.1f}M"
  if amount >= 1_000:
    return f"${amount / 1_000:.0f}K"
  return f"${amount}" if amount else ""


def _detect_transaction_type(*values: Any) -> str:
  text = " ".join(str(value or "").lower() for value in values)
  if "cash-out" in text or "cash out" in text:
    return "Cash-out refi"
  if "refi" in text or "refinance" in text:
    return "Refinance"
  if "purchase" in text or "buy" in text or "acquir" in text:
    return "Purchase"
  if "bridge" in text:
    return "Bridge / payoff"
  if "fix" in text and "flip" in text:
    return "Fix-and-flip"
  return "DSCR inquiry"


def _concise_overview(row: dict[str, Any], opening: str) -> str:
  source = str(row.get("lead_overview") or row.get("reactivation_brief") or opening or "Recent DSCR lead with limited context available.").strip()
  source = source.replace("\n", " ").replace("  ", " ")
  if len(source) > 280:
    source = source[:277].rsplit(" ", 1)[0] + "..."
  return source


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
  if not path.exists():
    return []
  try:
    with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
      return list(csv.DictReader(handle))
  except OSError:
    return []


def _freshness_points(age_days: int) -> int:
  if age_days <= 1:
    return 100
  if age_days <= 3:
    return 90
  if age_days <= 7:
    return 75
  if age_days <= 14:
    return 55
  if age_days <= 21:
    return 35
  return 0


def _build_recent_lo_scoreboard() -> dict[str, Any]:
  enriched_path = RECENT_LO_SCOREBOARD_SOURCE if RECENT_LO_SCOREBOARD_SOURCE.exists() else REACTIVATION_DIR / "launch-batch-2026-04-28-last30.transcript-enriched.csv"
  enriched = _read_csv_rows(enriched_path)
  generated_at = datetime.now(UTC)
  rows: list[dict[str, Any]] = []
  for row in enriched:
    form_dt = _parse_dt(row.get("original_form_fill_at"))
    age_days = _as_int(row.get("age_days")) if row.get("age_days") not in {None, ""} else -1
    if age_days < 0:
      if not form_dt:
        continue
      age_days = max(0, (generated_at.date() - form_dt.date()).days)
    if age_days > 21:
      continue
    estimated = _as_int(row.get("estimated_amount")) or _as_int(row.get("estimated_largest_amount"))
    prior_connected_seconds = _as_int(row.get("prior_connected_seconds"))
    prior_call_count = _as_int(row.get("prior_call_count"))
    readiness = _as_int(row.get("readiness_score")) or (85 if prior_connected_seconds >= 90 else 60 if prior_call_count else 42)
    profitability = _as_int(row.get("profitability_score")) or (min(100, round(estimated / 12_000)) if estimated else 20)
    transcript_points = 25 if prior_connected_seconds >= 300 else 15 if prior_connected_seconds >= 90 else 5 if prior_call_count else 0
    score = _as_int(row.get("score")) or round((_freshness_points(age_days) * 0.48) + (readiness * 0.23) + (profitability * 0.17) + transcript_points)
    first_name = str(row.get("first_name") or "there").strip()
    opening = str(row.get("opening_context_line") or "you had reached out about a DSCR loan").strip()
    transaction_type = str(row.get("transaction_type") or "").strip() or _detect_transaction_type(
      opening,
      row.get("reactivation_brief"),
      row.get("known_facts"),
      row.get("recommended_first_question"),
    )
    rows.append(
      {
        "rank": 0,
        "owner": row.get("owner") or "Unassigned LO Review",
        "score": score,
        "age_days": age_days,
        "estimated_amount": estimated,
        "estimated_amount_label": _money(estimated),
        "readiness_score": readiness,
        "profitability_score": profitability,
        "first_name": first_name,
        "phone": row.get("phone", ""),
        "email": row.get("email", ""),
        "contact_id": row.get("contact_id", ""),
        "source_category": row.get("enrichment_source", ""),
        "prior_call_count": prior_call_count,
        "prior_connected_seconds": prior_connected_seconds,
        "days_since_last_call": row.get("days_since_last_call", ""),
        "transaction_type": transaction_type,
        "lead_overview": _concise_overview(row, opening),
        "opening_context_line": opening,
      }
    )
  rows.sort(key=lambda item: (item["score"], item["estimated_amount"], -item["age_days"]), reverse=True)
  for index, row in enumerate(rows, start=1):
    row["rank"] = index
  payload = {
    "generated_at": generated_at.isoformat(),
    "source_file": str(enriched_path),
    "summary": {
      "row_count": len(rows),
      "same_week_count": sum(1 for row in rows if row["age_days"] <= 7),
      "top_25_estimated_amount": sum(_as_int(row["estimated_amount"]) for row in rows[:25]),
      "auto_falloff_days": 21,
    },
    "rows": rows,
  }
  SCOREBOARD_DIR.mkdir(parents=True, exist_ok=True)
  (SCOREBOARD_DIR / "recent-lo-scoreboard.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
  return payload


def _render_recent_lo_scoreboard(payload: dict[str, Any], token: str | None = None) -> str:
  rows = payload["rows"]
  owners: dict[str, int] = {}
  for row in rows:
    owner = str(row.get("owner") or "Unassigned LO Review")
    owners[owner] = owners.get(owner, 0) + 1
  owner_tabs = "".join(
    f"<button type='button' data-owner='{html.escape(owner)}'>{html.escape(owner)} <span>{count}</span></button>"
    for owner, count in sorted(owners.items())
  )
  cards: list[str] = []
  for row in rows:
    cards.append(
      f"""<article class="lead" data-owner="{html.escape(str(row.get('owner') or 'Unassigned LO Review'))}" data-score="{row['score']}" data-amount="{row['estimated_amount']}" data-age="{row['age_days']}" data-lastcall="{html.escape(str(row.get('days_since_last_call') or '-1'))}">
  <div class="rank">#{row['rank']}</div>
  <div class="mainline"><strong>{html.escape(str(row['first_name']).title())}</strong><span>{html.escape(str(row['estimated_amount_label'] or 'Amount unknown'))}</span></div>
  <div class="meta">{row['age_days']} days old · <span class="last-call" data-days="{html.escape(str(row.get('days_since_last_call') or '-1'))}">Last call: {html.escape(str(row.get('days_since_last_call') or 'unknown'))} days ago</span> · {html.escape(str(row['source_category']))}</div>
  <div class="score">Score {row['score']}</div>
  <p>{html.escape(str(row['lead_overview']))}</p>
  <div class="chips"><span>{html.escape(str(row['transaction_type']))}</span><span>{html.escape(str(row['prior_call_count']))} prior calls</span><span>{html.escape(str(row['prior_connected_seconds']))} connected seconds</span></div>
  <div class="links"><a href="tel:{html.escape(str(row['phone']))}">Call</a><a href="https://app.getmoremortgages.com/v2/location/HSCyuJDGKA5J5gfjfHzi/contacts/detail/{html.escape(str(row['contact_id']))}">GHL</a></div>
</article>"""
    )
  refresh = "/scoreboards/recent-lo"
  if token:
    refresh += f"?token={html.escape(token)}"
  generated = html.escape(str(payload.get("generated_at") or ""))
  return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fresh LO Scoreboard</title>
<style>
:root{{--ink:#111827;--muted:#667085;--line:#d0d5dd;--bg:#f6f7f9;--card:#fff;--accent:#0f766e;--gold:#a16207}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}
.shell{{max-width:1240px;margin:0 auto;padding:26px}}.hero{{display:flex;align-items:flex-end;justify-content:space-between;gap:20px;margin-bottom:16px}}
h1{{font-size:32px;line-height:1.05;margin:0 0 8px}}p{{line-height:1.42}}.muted,.meta{{color:var(--muted)}}.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:18px 0}}
.stat,.lead{{background:var(--card);border:1px solid var(--line);border-radius:8px}}.stat{{padding:15px}}.stat strong{{display:block;font-size:28px}}.stat span{{color:var(--muted)}}
.toolbar{{display:flex;gap:8px;flex-wrap:wrap;margin:18px 0}}button,select{{border:1px solid var(--line);background:#fff;border-radius:999px;padding:9px 12px;font-weight:700}}button.active{{background:var(--accent);color:#fff;border-color:var(--accent)}}button span{{opacity:.75}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:12px}}.lead{{padding:14px;position:relative;min-height:240px}}.rank{{position:absolute;right:14px;top:14px;color:var(--accent);font-weight:800}}
.mainline{{display:flex;gap:10px;align-items:baseline;padding-right:52px}}.mainline strong{{font-size:20px}}.mainline span{{color:var(--gold);font-weight:800}}.lead p{{margin:13px 0;color:#344054}}
.score{{display:inline-flex;margin-top:10px;background:#dcfce7;color:#166534;border:1px solid #86efac;border-radius:999px;padding:7px 11px;font-size:18px;font-weight:900}}.last-call{{border-radius:999px;padding:3px 7px;font-weight:800}}.last-green{{background:#dcfce7;color:#166534}}.last-yellow{{background:#fef9c3;color:#854d0e}}.last-orange{{background:#ffedd5;color:#9a3412}}.last-red{{background:#fee2e2;color:#991b1b}}.last-gray{{background:#f2f4f7;color:#475467}}
.chips{{display:flex;gap:6px;flex-wrap:wrap}}.chips span{{font-size:12px;background:#eef4f3;color:#134e48;border-radius:999px;padding:6px 8px}}.links{{display:flex;gap:10px;margin-top:14px}}.links a,.refresh{{color:var(--accent);font-weight:800;text-decoration:none}}
@media(max-width:760px){{.hero{{display:block}}.stats{{grid-template-columns:1fr 1fr}}.shell{{padding:16px}}}}
</style></head><body><div class="shell">
<section class="hero"><div><h1>Fresh LO Scoreboard</h1><p class="muted">Recent leads only. Rows auto-fall off after 21 days. Refreshing this page rebuilds the board from hosted data.</p></div><a class="refresh" href="{refresh}">Refresh</a></section>
<section class="stats">
  <div class="stat"><strong>{payload['summary']['row_count']}</strong><span>Fresh leads</span></div>
  <div class="stat"><strong>{payload['summary']['same_week_count']}</strong><span>0-7 days old</span></div>
  <div class="stat"><strong>{_money(payload['summary']['top_25_estimated_amount'])}</strong><span>Top 25 est. amount</span></div>
  <div class="stat"><strong>21</strong><span>Day falloff</span></div>
</section>
<p class="muted">Generated {generated}</p>
<section class="toolbar"><button class="active" type="button" data-owner="all">All <span>{len(rows)}</span></button>{owner_tabs}<select id="sort"><option value="score">Sort: Best score</option><option value="amount">Sort: Loan amount</option><option value="fresh">Sort: Freshest lead</option><option value="lastcall">Sort: Oldest last call</option></select></section>
<section class="grid">{''.join(cards)}</section>
</div><script>
const buttons=[...document.querySelectorAll('button[data-owner]')];
const cards=[...document.querySelectorAll('.lead')];
const grid=document.querySelector('.grid');
let activeOwner='all';
document.querySelectorAll('.last-call').forEach(el=>{{
  const days=Number(el.dataset.days);
  if(days>=0 && days<=1) el.classList.add('last-green');
  else if(days===2) el.classList.add('last-yellow');
  else if(days===3) el.classList.add('last-orange');
  else if(days>=4) el.classList.add('last-red');
  else el.classList.add('last-gray');
}});
function sortCards(){{
  const mode=document.querySelector('#sort').value;
  cards.sort((a,b)=>{{
    if(mode==='amount') return Number(b.dataset.amount||0)-Number(a.dataset.amount||0);
    if(mode==='fresh') return Number(a.dataset.age||999)-Number(b.dataset.age||999);
    if(mode==='lastcall') return Number(b.dataset.lastcall||-1)-Number(a.dataset.lastcall||-1);
    return Number(b.dataset.score||0)-Number(a.dataset.score||0);
  }}).forEach(card=>grid.appendChild(card));
}}
function render(){{
  cards.forEach(card=>card.style.display=(activeOwner==='all'||card.dataset.owner===activeOwner)?'block':'none');
  sortCards();
}}
buttons.forEach(button=>button.addEventListener('click',()=>{{
  buttons.forEach(item=>item.classList.remove('active'));
  button.classList.add('active');
  activeOwner=button.dataset.owner;
  render();
}}));
document.querySelector('#sort').addEventListener('input', render);
render();
</script></body></html>"""


async def recent_lo_scoreboard(request: web.Request) -> web.Response:
  if not _scoreboard_authorized(request):
    return web.Response(
      text="Unauthorized. Use the private scoreboard link or ask Dave for access.",
      status=401,
      content_type="text/plain",
    )
  payload = _build_recent_lo_scoreboard()
  token = request.query.get("token", "")
  return web.Response(text=_render_recent_lo_scoreboard(payload, token), content_type="text/html")


async def recent_lo_scoreboard_json(request: web.Request) -> web.Response:
  if not _scoreboard_authorized(request):
    return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
  return web.json_response(_build_recent_lo_scoreboard())


async def admin_recent_lo_scoreboard_import(request: web.Request) -> web.Response:
  payload = await request.json()
  rows = payload.get("rows")
  if not isinstance(rows, list) or not rows:
    return web.json_response({"ok": False, "error": "rows_required"}, status=400)
  fieldnames = list(rows[0].keys()) if isinstance(rows[0], dict) else []
  if not fieldnames:
    return web.json_response({"ok": False, "error": "dict_rows_required"}, status=400)
  SCOREBOARD_DIR.mkdir(parents=True, exist_ok=True)
  with RECENT_LO_SCOREBOARD_SOURCE.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
      if isinstance(row, dict):
        writer.writerow({field: row.get(field, "") for field in fieldnames})
  payload = _build_recent_lo_scoreboard()
  _append_event(
    "recent_lo_scoreboard_imported",
    {"row_count": len(rows), "source_path": str(RECENT_LO_SCOREBOARD_SOURCE)},
  )
  return web.json_response({"ok": True, "imported_rows": len(rows), "published_rows": payload["summary"]["row_count"]})


async def admin_set_scoreboard_access_token(request: web.Request) -> web.Response:
  payload = await request.json()
  token = str(payload.get("token") or "").strip()
  if len(token) < 24:
    return web.json_response({"ok": False, "error": "token_too_short"}, status=400)
  SCOREBOARD_DIR.mkdir(parents=True, exist_ok=True)
  SCOREBOARD_ACCESS_TOKEN_FILE.write_text(token, encoding="utf-8")
  _append_event("scoreboard_access_token_updated", {"token_length": len(token)})
  return web.json_response({"ok": True, "token_length": len(token)})


def _lookup_reactivation_lead(phone: str | None) -> dict[str, str]:
  target = _digits(phone)
  if not target:
    return {}
  if len(target) == 11 and target.startswith("1"):
    target10 = target[1:]
  else:
    target10 = target[-10:]

  candidate_paths = sorted(
    REACTIVATION_DIR.glob("launch-batch-2026-04-28*.csv"),
    key=lambda path: path.stat().st_mtime,
    reverse=True,
  )
  for path in candidate_paths:
    try:
      with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
          row_phone = _digits(row.get("phone"))
          row10 = row_phone[1:] if len(row_phone) == 11 and row_phone.startswith("1") else row_phone[-10:]
          if row10 and row10 == target10:
            return {key: str(value or "") for key, value in row.items()}
    except OSError:
      continue
  return {}


async def health(_: web.Request) -> web.Response:
  return web.json_response({"ok": True, "service": "loan-os-retell-proof"})


async def ready(_: web.Request) -> web.Response:
  checks: dict[str, Any] = {}
  status = 200
  try:
    EVENT_DIR.mkdir(parents=True, exist_ok=True)
    probe = EVENT_DIR / ".ready_probe"
    probe.write_text(str(_now_ms()), encoding="utf-8")
    checks["event_storage"] = {"ok": probe.exists(), "path": str(EVENT_DIR)}
  except Exception as exc:
    checks["event_storage"] = {"ok": False, "error": str(exc)}
    status = 503

  checks["reactivation_context"] = {
    "ok": REACTIVATION_DIR.exists(),
    "path": str(REACTIVATION_DIR),
    "csv_count": len(list(REACTIVATION_DIR.glob("launch-batch-2026-04-28*.csv")))
    if REACTIVATION_DIR.exists()
    else 0,
  }
  if not checks["reactivation_context"]["ok"]:
    status = 503

  checks["ghl_configured"] = {"ok": _ghl_configured()}
  checks["kill_switch"] = {"active": _kill_switch_active()}
  checks["side_effects"] = {"disabled": _side_effects_disabled()}
  return web.json_response({"ok": status == 200, "checks": checks}, status=status)


async def admin_status(_: web.Request) -> web.Response:
  return web.json_response(
    {
      "ok": True,
      "kill_switch_active": _kill_switch_active(),
      "side_effects_disabled": _side_effects_disabled(),
      "event_dir": str(EVENT_DIR),
      "event_log_exists": EVENT_LOG.exists(),
      "captured_calls": len(list(CALL_CAPTURE_DIR.glob("call_*.json")))
      if CALL_CAPTURE_DIR.exists()
      else 0,
      "ghl_note_markers": len(list(GHL_NOTE_DIR.glob("*.json"))) if GHL_NOTE_DIR.exists() else 0,
      "ghl_note_retries": len(list(GHL_NOTE_RETRY_DIR.glob("*.json")))
      if GHL_NOTE_RETRY_DIR.exists()
      else 0,
    }
  )


async def admin_kill(_: web.Request) -> web.Response:
  EVENT_DIR.mkdir(parents=True, exist_ok=True)
  (EVENT_DIR / "KILL_SWITCH").write_text(str(_now_ms()), encoding="utf-8")
  _append_event("admin_kill_switch_enabled", {"source": "admin_endpoint"})
  return web.json_response({"ok": True, "kill_switch_active": True})


async def admin_resume(_: web.Request) -> web.Response:
  kill_file = EVENT_DIR / "KILL_SWITCH"
  if kill_file.exists():
    kill_file.unlink()
  _append_event("admin_kill_switch_disabled", {"source": "admin_endpoint"})
  return web.json_response({"ok": True, "kill_switch_active": _kill_switch_active()})


async def inbound_callback_webhook(request: web.Request) -> web.Response:
  payload = await request.json()
  inbound = payload.get("call_inbound") if isinstance(payload.get("call_inbound"), dict) else {}
  from_number = str(inbound.get("from_number") or payload.get("from_number") or "")
  lead = _lookup_reactivation_lead(from_number)
  dynamic_variables = {
    "first_name": lead.get("first_name", ""),
    "ghl_contact_id": lead.get("contact_id", ""),
    "opening_context_line": lead.get("opening_context_line", "your DSCR loan options"),
    "pain_point_opener": lead.get("pain_point_opener", ""),
    "reactivation_brief": lead.get("reactivation_brief", ""),
    "recommended_first_question": lead.get("recommended_first_question", ""),
    "callback_context_found": "true" if lead else "false",
  }
  response = {
    "call_inbound": {
      "dynamic_variables": dynamic_variables,
      "metadata": {
        "project": "evolve_voice_agent",
        "purpose": "jr_reactivation_inbound_callback",
        "from_number": from_number,
        "ghl_contact_id": lead.get("contact_id", ""),
        "context_found": bool(lead),
      },
    }
  }
  _append_event("retell_inbound_callback_webhook", {"payload": payload, "response": response})
  return web.json_response(response)


def _load_env() -> None:
  env_path = REPO_ROOT / ".env"
  if not env_path.exists():
    return
  for raw in env_path.read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if not line or line.startswith("#") or "=" not in line:
      continue
    key, value = line.split("=", 1)
    os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _retell_create_web_call(agent_id: str) -> dict[str, Any]:
  key = os.environ.get("RETELL_API_KEY")
  if not key:
    raise RuntimeError("RETELL_API_KEY is not set")
  body = {
    "agent_id": agent_id,
    "metadata": {
      "project": "evolve_voice_agent",
      "purpose": "Dave controlled Retell web proof",
    },
    "retell_llm_dynamic_variables": {
      "test_name": "managed_voice_bakeoff_retell_web_proof",
    },
  }
  req = request.Request(
    "https://api.retellai.com/v2/create-web-call",
    data=json.dumps(body).encode("utf-8"),
    method="POST",
    headers={
      "Authorization": f"Bearer {key}",
      "Content-Type": "application/json",
    },
  )
  try:
    with request.urlopen(req, timeout=30) as resp:
      return json.loads(resp.read().decode("utf-8"))
  except error.HTTPError as exc:
    detail = exc.read().decode("utf-8", errors="replace")
    raise RuntimeError(f"Retell create-web-call failed: {exc.code} {detail}") from exc


async def web_call_page(_: web.Request) -> web.Response:
  html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Evolve Retell Proof</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #111827; }
    button { font-size: 18px; padding: 14px 18px; margin-right: 8px; border: 0; border-radius: 8px; background: #111827; color: white; }
    button:disabled { opacity: 0.5; }
    button.secondary { background: #6b7280; }
    #status { margin: 16px 0; font-weight: 700; }
    #log { margin-top: 20px; white-space: pre-wrap; background: #f3f4f6; padding: 16px; border-radius: 8px; min-height: 220px; max-height: 55vh; overflow: auto; }
  </style>
</head>
<body>
  <h1>Evolve Retell Proof</h1>
  <p>Use this for Dave-controlled latency and quality testing. It does not dial prospects.</p>
  <button id="start">Start Web Call</button>
  <button id="stop" class="secondary">Stop</button>
  <div id="status">Idle</div>
  <div id="log"></div>
  <script src="/retell/client.bundle.js"></script>
</body>
</html>
"""
  return web.Response(text=html, content_type="text/html")


async def web_client_bundle(_: web.Request) -> web.Response:
  if not WEB_BUNDLE_PATH.exists():
    return web.Response(text="Retell web bundle missing.", status=500)
  return web.Response(
    body=WEB_BUNDLE_PATH.read_bytes(),
    content_type="application/javascript",
  )


async def create_web_call(_: web.Request) -> web.Response:
  _load_env()
  try:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    agent_id = config["agent_id"]
    payload = _retell_create_web_call(agent_id)
  except Exception as exc:
    return web.json_response({"ok": False, "error": str(exc)}, status=500)
  _append_event("retell_create_web_call", payload)
  return web.json_response(payload)


async def retell_webhook(request: web.Request) -> web.Response:
  _load_env()
  try:
    payload = await request.json()
  except Exception:
    payload = {"raw": await request.text()}
  _append_event("retell_webhook", payload)
  _capture_call_payload(payload)
  _sync_call_note_to_ghl(payload)
  return web.json_response({"ok": True})


def _capture_call_payload(payload: dict[str, Any]) -> None:
  call = payload.get("call")
  if not isinstance(call, dict):
    return
  call_id = call.get("call_id")
  if not isinstance(call_id, str) or not call_id:
    return
  event = str(payload.get("event") or "")
  if event not in {"call_ended", "call_analyzed"}:
    return
  CALL_CAPTURE_DIR.mkdir(parents=True, exist_ok=True)
  raw_path = CALL_CAPTURE_DIR / f"{call_id}.{event}.json"
  raw_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
  summary_path = CALL_CAPTURE_DIR / f"{call_id}.summary.json"
  summary = {
    "call_id": call_id,
    "event": event,
    "agent_id": call.get("agent_id"),
    "agent_version": call.get("agent_version"),
    "call_status": call.get("call_status"),
    "direction": call.get("direction"),
    "from_number": call.get("from_number"),
    "to_number": call.get("to_number"),
    "start_timestamp": call.get("start_timestamp"),
    "end_timestamp": call.get("end_timestamp"),
    "duration_ms": call.get("duration_ms"),
    "disconnection_reason": call.get("disconnection_reason"),
    "latency": call.get("latency") or {},
    "tool_calls": call.get("tool_calls") or [],
    "call_analysis": call.get("call_analysis") or {},
    "recording_url": call.get("recording_url"),
    "recording_multi_channel_url": call.get("recording_multi_channel_url"),
    "public_log_url": call.get("public_log_url"),
    "transcript": call.get("transcript") or "",
  }
  summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")


def _ghl_configured() -> bool:
  return bool(
    (os.getenv("GHL_PRIVATE_INTEGRATION_TOKEN") or os.getenv("GHL_API_KEY") or "").strip()
    and (os.getenv("GHL_SUB_ACCOUNT_ID") or os.getenv("GHL_LOCATION_ID") or "").strip()
  )


def _ghl_headers(*, json_body: bool = False) -> dict[str, str]:
  headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {(os.getenv('GHL_PRIVATE_INTEGRATION_TOKEN') or os.getenv('GHL_API_KEY') or '').strip()}",
    "Version": (os.getenv("GHL_API_VERSION") or "2021-07-28").strip(),
    "User-Agent": "EvolveFundingLoanOS/1.0",
  }
  if json_body:
    headers["Content-Type"] = "application/json"
  return headers


def _ghl_post(path: str, body: dict[str, Any]) -> dict[str, Any]:
  req = request.Request(
    f"{(os.getenv('GHL_API_BASE') or 'https://services.leadconnectorhq.com').rstrip('/')}{path}",
    data=json.dumps(body).encode("utf-8"),
    method="POST",
    headers=_ghl_headers(json_body=True),
  )
  try:
    with request.urlopen(req, timeout=20) as resp:
      raw = resp.read().decode("utf-8")
      return json.loads(raw) if raw else {"ok": True}
  except error.HTTPError as exc:
    detail = exc.read().decode("utf-8", errors="replace")[:1200]
    raise RuntimeError(f"GHL POST {path} failed: HTTP {exc.code}: {detail}") from exc


def _ghl_get(path: str, params: dict[str, str]) -> dict[str, Any]:
  from urllib.parse import urlencode

  qs = urlencode({key: value for key, value in params.items() if value})
  req = request.Request(
    f"{(os.getenv('GHL_API_BASE') or 'https://services.leadconnectorhq.com').rstrip('/')}{path}?{qs}",
    method="GET",
    headers=_ghl_headers(),
  )
  try:
    with request.urlopen(req, timeout=20) as resp:
      raw = resp.read().decode("utf-8")
      return json.loads(raw) if raw else {"ok": True}
  except error.HTTPError as exc:
    detail = exc.read().decode("utf-8", errors="replace")[:1200]
    raise RuntimeError(f"GHL GET {path} failed: HTTP {exc.code}: {detail}") from exc


def _extract_contact_id(payload: Any) -> str | None:
  if isinstance(payload, dict):
    for key in ("contactId", "contact_id", "id", "_id"):
      value = payload.get(key)
      if isinstance(value, str) and value.strip():
        return value.strip()
    for key in ("contact", "data"):
      found = _extract_contact_id(payload.get(key))
      if found:
        return found
  if isinstance(payload, list):
    for item in payload:
      found = _extract_contact_id(item)
      if found:
        return found
  return None


def _resolve_ghl_contact_id(call: dict[str, Any]) -> str | None:
  for container_key in ("retell_llm_dynamic_variables", "metadata"):
    values = call.get(container_key)
    if isinstance(values, dict):
      for key in ("ghl_contact_id", "contact_id", "ghlContactId"):
        value = values.get(key)
        if isinstance(value, str) and value.strip():
          return value.strip()
  direction = str(call.get("direction") or "")
  phone = str(call.get("to_number") if direction == "outbound" else call.get("from_number") or "").strip()
  if not phone:
    return None
  try:
    duplicate = _ghl_get("/contacts/search/duplicate", {"phone": phone})
    return _extract_contact_id(duplicate)
  except Exception as exc:
    _append_event("retell_ghl_contact_lookup_failed", {"call_id": call.get("call_id"), "phone": phone, "error": str(exc)})
    return None


def _short_transcript_excerpt(transcript: str) -> str:
  lines = [line.strip() for line in transcript.splitlines() if line.strip()]
  return "\n".join(lines[-8:])[:1200]


def _build_ghl_call_note(call: dict[str, Any]) -> str:
  analysis = call.get("call_analysis") if isinstance(call.get("call_analysis"), dict) else {}
  tool_calls = call.get("tool_calls") if isinstance(call.get("tool_calls"), list) else []
  transfer_status = "none"
  if any(tool.get("type") == "transfer_call" and tool.get("success") for tool in tool_calls if isinstance(tool, dict)):
    transfer_status = "attempted / Retell tool success"
  if str(call.get("disconnection_reason") or "") == "call_transfer":
    transfer_status = "transferred out of Retell"
  booking_status = "none"
  if any(tool.get("name") == "book_selected_slot" and tool.get("success") for tool in tool_calls if isinstance(tool, dict)):
    booking_status = "booked / Retell tool success"
  elif any(tool.get("name") == "book_selected_slot" for tool in tool_calls if isinstance(tool, dict)):
    booking_status = "attempted / needs review"
  transcript = str(call.get("transcript") or "")
  summary = str(analysis.get("call_summary") or "Retell Alex Jr Reactivation call completed.").strip()
  return (
    "Alex Jr Reactivation AI Call\n"
    f"Call ID: {call.get('call_id')}\n"
    f"Outcome: {call.get('disconnection_reason') or call.get('call_status')}\n"
    f"Transfer: {transfer_status}\n"
    f"Booking: {booking_status}\n"
    f"Duration: {round((int(call.get('duration_ms') or 0) / 1000), 1)} seconds\n"
    f"Summary: {summary}\n"
    f"Recording: {call.get('recording_url') or 'not available'}\n"
    f"Public log: {call.get('public_log_url') or 'not available'}\n\n"
    "Transcript tail:\n"
    f"{_short_transcript_excerpt(transcript)}"
  )[:5000]


def _sync_call_note_to_ghl(payload: dict[str, Any]) -> None:
  if payload.get("event") != "call_analyzed":
    return
  call = payload.get("call")
  if not isinstance(call, dict):
    return
  call_id = call.get("call_id")
  if not isinstance(call_id, str) or not call_id:
    return
  GHL_NOTE_DIR.mkdir(parents=True, exist_ok=True)
  marker = GHL_NOTE_DIR / f"{call_id}.json"
  if marker.exists():
    return
  if _side_effects_disabled():
    marker.write_text(
      json.dumps({"ok": False, "skipped": "side_effects_disabled", "call_id": call_id}, indent=2),
      encoding="utf-8",
    )
    return
  if not _ghl_configured():
    marker.write_text(json.dumps({"ok": False, "skipped": "ghl_not_configured", "call_id": call_id}, indent=2), encoding="utf-8")
    return
  contact_id = _resolve_ghl_contact_id(call)
  if not contact_id:
    marker.write_text(json.dumps({"ok": False, "skipped": "contact_not_found", "call_id": call_id}, indent=2), encoding="utf-8")
    return
  note = _build_ghl_call_note(call)
  try:
    result = _ghl_post(
      f"/contacts/{contact_id}/notes",
      {"body": note},
    )
    marker.write_text(
      json.dumps({"ok": True, "call_id": call_id, "contact_id": contact_id, "result": result}, indent=2, ensure_ascii=False),
      encoding="utf-8",
    )
    _append_event("retell_ghl_note_created", {"call_id": call_id, "contact_id": contact_id})
  except Exception as exc:
    GHL_NOTE_RETRY_DIR.mkdir(parents=True, exist_ok=True)
    retry_path = GHL_NOTE_RETRY_DIR / f"{call_id}.json"
    retry_path.write_text(
      json.dumps(
        {"call_id": call_id, "contact_id": contact_id, "note": note, "error": str(exc)},
        indent=2,
        ensure_ascii=False,
      ),
      encoding="utf-8",
    )
    marker.write_text(
      json.dumps({"ok": False, "call_id": call_id, "contact_id": contact_id, "error": str(exc)}, indent=2, ensure_ascii=False),
      encoding="utf-8",
    )
    _append_event("retell_ghl_note_failed", {"call_id": call_id, "contact_id": contact_id, "error": str(exc)})


async def book_or_transfer(request: web.Request) -> web.Response:
  _load_env()
  payload = await request.json()
  args = _extract_args(payload)
  _append_event("retell_tool_book_or_transfer", {"payload": payload, "args": args})

  consent = bool(args.get("caller_confirmed_transfer_or_booking") or args.get("consent"))
  if not consent:
    return web.json_response(
      {
        "ok": False,
        "message": "Consent was not explicit. Ask the caller if they want a loan officer to take a closer look before booking or transfer.",
      }
    )

  availability = await get_availability(
    timezone_name=str(args.get("timezone") or "America/Los_Angeles"),
    limit=24,
  )
  slots = select_spread_slots(availability.get("slots", []), limit=3)
  if not availability.get("ok") or not slots:
    return web.json_response(
      {
        "ok": True,
        "live_transfer_available": False,
        "booked": False,
        "needs_slot_selection": False,
        "availability": availability,
        "message": "Looks like I couldn't get someone live, and I am not seeing a clean calendar opening. Tell the caller a loan officer will follow up, then log the note.",
      }
    )

  slot_message = format_slot_options(slots)

  result = {
    "ok": True,
    "live_transfer_available": False,
    "booked": False,
    "needs_slot_selection": True,
    "calendar_id": availability.get("calendar_id"),
    "availability_mode": availability.get("mode"),
    "available_slots": slots,
    "message": f"The loan officer didn't pick up, but I can grab a time. I have {slot_message}. Which works best?",
  }
  _append_event("retell_tool_availability_returned", result)
  return web.json_response(result)


async def book_selected_slot(request: web.Request) -> web.Response:
  _load_env()
  payload = await request.json()
  args = _extract_args(payload)
  _append_event("retell_tool_book_selected_slot", {"payload": payload, "args": args})
  result = await book_selected_calendar_slot(args)
  _append_event("retell_tool_book_selected_slot_result", result)
  return web.json_response(result)


async def log_call_note(request: web.Request) -> web.Response:
  payload = await request.json()
  args = _extract_args(payload)
  _append_event("retell_tool_log_call_note", {"payload": payload, "args": args})
  return web.json_response({"ok": True, "message": "Call note logged for controlled proof."})


def build_app() -> web.Application:
  _load_env()
  _setup_logging()
  app = web.Application(middlewares=[operational_guard])
  app.router.add_get("/health", health)
  app.router.add_get("/ready", ready)
  app.router.add_get("/scoreboards/recent-lo", recent_lo_scoreboard)
  app.router.add_get("/scoreboards/recent-lo.json", recent_lo_scoreboard_json)
  app.router.add_get("/admin/status", admin_status)
  app.router.add_post("/admin/scoreboards/recent-lo/import", admin_recent_lo_scoreboard_import)
  app.router.add_post("/admin/scoreboards/access-token", admin_set_scoreboard_access_token)
  app.router.add_post("/admin/kill", admin_kill)
  app.router.add_post("/admin/resume", admin_resume)
  app.router.add_get("/retell/web-call", web_call_page)
  app.router.add_get("/retell/client.bundle.js", web_client_bundle)
  app.router.add_post("/retell/create-web-call", create_web_call)
  app.router.add_post("/retell/webhook", retell_webhook)
  app.router.add_post("/retell/inbound-callback-webhook", inbound_callback_webhook)
  app.router.add_post("/retell/tools/book_or_transfer", book_or_transfer)
  app.router.add_post("/retell/tools/book_selected_slot", book_selected_slot)
  app.router.add_post("/retell/tools/log_call_note", log_call_note)
  return app


def main() -> None:
  parser = argparse.ArgumentParser(description="Run the Loan OS Retell proof webhook server.")
  parser.add_argument("--host", default=os.getenv("RETELL_PROOF_HOST", "127.0.0.1"))
  parser.add_argument("--port", type=int, default=int(os.getenv("RETELL_PROOF_PORT", "8080")))
  args = parser.parse_args()

  EVENT_DIR.mkdir(parents=True, exist_ok=True)
  web.run_app(build_app(), host=args.host, port=args.port)


if __name__ == "__main__":
  main()
