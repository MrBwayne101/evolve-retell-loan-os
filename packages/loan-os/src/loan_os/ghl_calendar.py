from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx


DEFAULT_API_BASE = "https://services.leadconnectorhq.com"
DEFAULT_API_VERSION = "2021-04-15"
DEFAULT_TIMEZONE = "America/Los_Angeles"
DEFAULT_SLOT_MINUTES = 30


@dataclass(frozen=True)
class CalendarConfig:
  api_base: str
  api_version: str
  token: str
  location_id: str
  calendar_id: str
  timezone: str


def env_flag(name: str, default: bool) -> bool:
  raw = os.getenv(name)
  if raw is None:
    return default
  return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def live_availability_enabled() -> bool:
  return env_flag("RETELL_PROOF_USE_LIVE_GHL_AVAILABILITY", True)


def live_booking_enabled() -> bool:
  return env_flag("RETELL_PROOF_ALLOW_LIVE_GHL_BOOKING", False)


def _config() -> CalendarConfig:
  calendar_id = (
    os.getenv("GHL_LO_CALENDAR_ID")
    or os.getenv("GHL_DSCR_CALENDAR_ID")
    or "mAMgSTlMmmJAkQS7MYNy"
  ).strip()
  return CalendarConfig(
    api_base=os.getenv("GHL_API_BASE", DEFAULT_API_BASE).rstrip("/"),
    api_version=os.getenv("GHL_API_VERSION", DEFAULT_API_VERSION).strip() or DEFAULT_API_VERSION,
    token=os.getenv("GHL_PRIVATE_INTEGRATION_TOKEN", "").strip(),
    location_id=(
      os.getenv("GHL_SUB_ACCOUNT_ID") or os.getenv("GHL_LOCATION_ID") or ""
    ).strip(),
    calendar_id=calendar_id,
    timezone=os.getenv("GHL_BOOKING_TIMEZONE", DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE,
  )


def _headers(config: CalendarConfig, *, json_body: bool = False) -> dict[str, str]:
  headers = {
    "Accept": "application/json",
    "Authorization": f"Bearer {config.token}",
    "Version": config.api_version,
    "User-Agent": "EvolveFundingLoanOS/1.0",
  }
  if json_body:
    headers["Content-Type"] = "application/json"
  return headers


def _timezone(name: str | None) -> ZoneInfo:
  try:
    return ZoneInfo(name or DEFAULT_TIMEZONE)
  except ZoneInfoNotFoundError:
    return ZoneInfo(DEFAULT_TIMEZONE)


def _parse_dt(value: Any) -> datetime | None:
  if isinstance(value, (int, float)):
    if value > 10_000_000_000:
      return datetime.fromtimestamp(value / 1000, UTC)
    return datetime.fromtimestamp(value, UTC)
  if not isinstance(value, str) or not value.strip():
    return None
  raw = value.strip()
  if raw.isdigit():
    return _parse_dt(int(raw))
  if raw.endswith("Z"):
    raw = f"{raw[:-1]}+00:00"
  try:
    return datetime.fromisoformat(raw)
  except ValueError:
    return None


def _display_slot(start: datetime, timezone_name: str) -> str:
  local = start.astimezone(_timezone(timezone_name))
  now = datetime.now(local.tzinfo)
  if local.date() == now.date():
    day = "today"
  elif local.date() == (now + timedelta(days=1)).date():
    day = "tomorrow"
  else:
    day = local.strftime("%A")
  time_part = local.strftime("%-I:%M %p").replace(":00", "").lower()
  return f"{day} at {time_part}"


def _slot_from_start(start: datetime, *, timezone_name: str, calendar_id: str) -> dict[str, Any]:
  if start.tzinfo is None:
    start = start.replace(tzinfo=_timezone(timezone_name))
  end = start + timedelta(minutes=DEFAULT_SLOT_MINUTES)
  return {
    "start_iso": start.isoformat(),
    "end_iso": end.isoformat(),
    "calendar_id": calendar_id,
    "display": _display_slot(start, timezone_name),
  }


def _collect_slots(payload: Any, *, timezone_name: str, calendar_id: str) -> list[dict[str, Any]]:
  slots: list[dict[str, Any]] = []
  if isinstance(payload, list):
    for item in payload:
      slots.extend(_collect_slots(item, timezone_name=timezone_name, calendar_id=calendar_id))
    return slots
  if not isinstance(payload, dict):
    start = _parse_dt(payload)
    return [] if start is None else [_slot_from_start(start, timezone_name=timezone_name, calendar_id=calendar_id)]

  start = _parse_dt(
    payload.get("start_iso")
    or payload.get("start")
    or payload.get("startTime")
    or payload.get("startDate")
  )
  if start:
    end = _parse_dt(payload.get("end_iso") or payload.get("end") or payload.get("endTime"))
    if end is None:
      end = start + timedelta(minutes=DEFAULT_SLOT_MINUTES)
    return [{
      "start_iso": start.isoformat(),
      "end_iso": end.isoformat(),
      "calendar_id": str(payload.get("calendar_id") or payload.get("calendarId") or calendar_id),
      "display": _display_slot(start, timezone_name),
    }]

  for key in ("slots", "availableSlots", "freeSlots", "data"):
    if key in payload:
      slots.extend(_collect_slots(payload[key], timezone_name=timezone_name, calendar_id=calendar_id))
  for value in payload.values():
    if isinstance(value, (list, dict)):
      slots.extend(_collect_slots(value, timezone_name=timezone_name, calendar_id=calendar_id))
  return slots


def _mock_slots(timezone_name: str, calendar_id: str) -> list[dict[str, Any]]:
  tz = _timezone(timezone_name)
  now = datetime.now(tz)
  candidate_days = [now.date(), (now + timedelta(days=1)).date()]
  wanted = [(candidate_days[0], 15), (candidate_days[1], 10), (candidate_days[1], 13)]
  slots: list[dict[str, Any]] = []
  for day, hour in wanted:
    start = datetime(day.year, day.month, day.day, hour, tzinfo=tz)
    if start <= now + timedelta(minutes=45):
      continue
    slots.append(_slot_from_start(start, timezone_name=timezone_name, calendar_id=calendar_id))
  return slots[:3]


def _slot_start(slot: dict[str, Any]) -> datetime | None:
  return _parse_dt(slot.get("start_iso") or slot.get("startTime") or slot.get("start"))


def _dedupe_slot_sequence(slots: list[dict[str, Any]]) -> list[dict[str, Any]]:
  unique: dict[str, dict[str, Any]] = {}
  for slot in sorted(slots, key=lambda item: str(item.get("start_iso") or "")):
    key = str(slot.get("start_iso") or slot.get("display") or len(unique))
    unique.setdefault(key, slot)
  return list(unique.values())


def select_spread_slots(slots: list[dict[str, Any]], *, limit: int = 3) -> list[dict[str, Any]]:
  """Choose borrower-facing calendar options that are not stacked back-to-back.

  GHL often returns adjacent openings first, like 7:30, 8:00, 9:00. For sales calls,
  that is a poor offer set. This chooses the earliest reasonable slot, then spreads
  the remaining options across the next business window when inventory allows.
  """
  if limit <= 0:
    return []
  clean = [slot for slot in slots if isinstance(slot, dict)]
  if len(clean) <= limit:
    return clean[:limit]

  ordered = _dedupe_slot_sequence(clean)
  ordered.sort(key=lambda slot: (_slot_start(slot) or datetime.max.replace(tzinfo=UTC), str(slot.get("display") or "")))
  if len(ordered) <= limit:
    return ordered[:limit]

  first_start = _slot_start(ordered[0])
  if first_start is None:
    midpoint = len(ordered) // 2
    return [ordered[0], ordered[midpoint], ordered[-1]][:limit]

  # "Next 12 business hours" usually spans today plus the next morning. Cap the
  # candidate set to roughly two calendar days so we do not offer far-out slots.
  candidate_window_end = first_start + timedelta(hours=36)
  candidates = [slot for slot in ordered if (_slot_start(slot) or first_start) <= candidate_window_end]
  if len(candidates) < limit:
    candidates = ordered

  targets = [first_start, first_start + timedelta(hours=4), first_start + timedelta(hours=24)]
  selected: list[dict[str, Any]] = []

  def far_enough(candidate: dict[str, Any], *, minimum_hours: float) -> bool:
    candidate_start = _slot_start(candidate)
    if candidate_start is None:
      return True
    for existing in selected:
      existing_start = _slot_start(existing)
      if existing_start is not None and abs((candidate_start - existing_start).total_seconds()) < minimum_hours * 3600:
        return False
    return True

  for target in targets:
    remaining = [slot for slot in candidates if slot not in selected]
    if not remaining:
      break
    spaced = [slot for slot in remaining if far_enough(slot, minimum_hours=2.5)]
    pool = spaced or remaining
    choice = min(
      pool,
      key=lambda slot: abs(((_slot_start(slot) or target) - target).total_seconds()),
    )
    selected.append(choice)
    if len(selected) >= limit:
      break

  for slot in candidates:
    if len(selected) >= limit:
      break
    if slot not in selected:
      selected.append(slot)

  selected.sort(key=lambda slot: (_slot_start(slot) or datetime.max.replace(tzinfo=UTC), str(slot.get("display") or "")))
  return selected[:limit]


def format_slot_options(slots: list[dict[str, Any]]) -> str:
  displays = [str(slot.get("display") or "").strip() for slot in slots if str(slot.get("display") or "").strip()]
  if not displays:
    return ""
  if len(displays) == 1:
    return displays[0]
  if len(displays) == 2:
    return f"{displays[0]} or {displays[1]}"
  return f"{displays[0]}, {displays[1]}, or {displays[2]}"


async def get_availability(*, timezone_name: str | None = None, limit: int = 3) -> dict[str, Any]:
  config = _config()
  tz_name = timezone_name or config.timezone
  if not live_availability_enabled() or not config.token:
    slots = _mock_slots(tz_name, config.calendar_id)
    return {"ok": True, "mode": "mock", "calendar_id": config.calendar_id, "slots": slots[:limit]}

  tz = _timezone(tz_name)
  start_dt = datetime.now(tz)
  end_dt = start_dt + timedelta(days=5)
  try:
    async with httpx.AsyncClient(timeout=20.0) as client:
      response = await client.get(
        f"{config.api_base}/calendars/{config.calendar_id}/free-slots",
        params={
          "startDate": int(start_dt.timestamp() * 1000),
          "endDate": int(end_dt.timestamp() * 1000),
          "timezone": tz_name,
        },
        headers=_headers(config),
      )
      response.raise_for_status()
      slots = _collect_slots(
        response.json(),
        timezone_name=tz_name,
        calendar_id=config.calendar_id,
      )
  except Exception as exc:
    return {
      "ok": False,
      "mode": "live",
      "calendar_id": config.calendar_id,
      "slots": [],
      "error": str(exc),
    }

  unique: dict[str, dict[str, Any]] = {}
  for slot in sorted(slots, key=lambda item: item["start_iso"]):
    unique.setdefault(slot["start_iso"], slot)
  return {
    "ok": True,
    "mode": "live",
    "calendar_id": config.calendar_id,
    "slots": list(unique.values())[:limit],
  }


def _extract_contact_id(payload: Any) -> str | None:
  if isinstance(payload, dict):
    for key in ("contactId", "id", "_id"):
      value = payload.get(key)
      if isinstance(value, str) and value.strip():
        return value
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


def _extract_appointment_id(payload: Any) -> str | None:
  if isinstance(payload, dict):
    for key in ("appointmentId", "id", "_id"):
      value = payload.get(key)
      if isinstance(value, str) and value.strip():
        return value
    for key in ("appointment", "event", "data"):
      found = _extract_appointment_id(payload.get(key))
      if found:
        return found
  if isinstance(payload, list):
    for item in payload:
      found = _extract_appointment_id(item)
      if found:
        return found
  return None


async def book_selected_slot(arguments: dict[str, Any]) -> dict[str, Any]:
  contact_phone = str(arguments.get("contact_phone") or "").strip()
  contact_email = str(arguments.get("contact_email") or "").strip() or None
  provided_contact_id = str(
    arguments.get("ghl_contact_id") or arguments.get("contact_id") or ""
  ).strip()
  summary = str(arguments.get("summary") or "Evolve Funding follow-up").strip()
  slot = arguments.get("slot") if isinstance(arguments.get("slot"), dict) else {}
  start_iso = str(arguments.get("slot_start_iso") or slot.get("start_iso") or "").strip()
  end_iso = str(arguments.get("slot_end_iso") or slot.get("end_iso") or "").strip()
  config = _config()

  if not start_iso:
    return {"ok": False, "booked": False, "error": "Missing selected appointment time."}

  start = _parse_dt(start_iso)
  if start is None:
    return {"ok": False, "booked": False, "error": "Selected appointment time was invalid."}
  end = _parse_dt(end_iso) or (start + timedelta(minutes=DEFAULT_SLOT_MINUTES))
  if end <= start:
    end = start + timedelta(minutes=DEFAULT_SLOT_MINUTES)
  slot_payload = {
    "start_iso": start.isoformat(),
    "end_iso": end.isoformat(),
    "display": _display_slot(start, config.timezone),
  }

  if not live_booking_enabled():
    return {
      "ok": True,
      "booked": True,
      "dry_run": True,
      "appointment_id": f"MOCK-{uuid.uuid4().hex[:10].upper()}",
      "slot": slot_payload,
      "message": f"Booked in test mode for {slot_payload['display']}.",
    }

  if not config.token:
    return {"ok": False, "booked": False, "error": "GHL token is not configured."}
  if not config.location_id:
    return {"ok": False, "booked": False, "error": "GHL location id is not configured."}

  try:
    async with httpx.AsyncClient(timeout=20.0) as client:
      contact_id = None
      if provided_contact_id:
        contact_id = provided_contact_id
      elif contact_phone or contact_email:
        duplicate = await client.get(
          f"{config.api_base}/contacts/search/duplicate",
          params={k: v for k, v in {"phone": contact_phone, "email": contact_email}.items() if v},
          headers=_headers(config),
        )
        if duplicate.status_code < 400:
          contact_id = _extract_contact_id(duplicate.json())
      if not contact_id:
        if not contact_phone and not contact_email:
          return {
            "ok": False,
            "booked": False,
            "slot": slot_payload,
            "error": "Missing ghl_contact_id, phone, or email for calendar booking.",
          }
        upsert = await client.post(
          f"{config.api_base}/contacts/upsert",
          json={
            "locationId": config.location_id,
            "phone": contact_phone,
            "email": contact_email,
          },
          headers=_headers(config, json_body=True),
        )
        upsert.raise_for_status()
        contact_id = _extract_contact_id(upsert.json())
      if not contact_id:
        raise ValueError("GHL contact lookup did not return a contactId")

      booking = await client.post(
        f"{config.api_base}/calendars/events/appointments",
        json={
          "calendarId": config.calendar_id,
          "locationId": config.location_id,
          "contactId": contact_id,
          "startTime": slot_payload["start_iso"],
          "endTime": slot_payload["end_iso"],
          "title": summary[:120] or "Evolve Funding follow-up",
          "notes": summary[:500],
        },
        headers=_headers(config, json_body=True),
      )
      booking.raise_for_status()
      appointment_id = _extract_appointment_id(booking.json())
      if not appointment_id:
        raise ValueError("GHL appointment response did not include an appointment id")
  except httpx.HTTPStatusError as exc:
    detail = exc.response.text[:1000] if exc.response is not None else ""
    return {
      "ok": False,
      "booked": False,
      "slot": slot_payload,
      "contact_id": provided_contact_id or contact_phone or contact_email,
      "error": f"{exc}; response={detail}",
    }
  except Exception as exc:
    return {
      "ok": False,
      "booked": False,
      "slot": slot_payload,
      "contact_id": provided_contact_id or contact_phone or contact_email,
      "error": str(exc),
    }

  return {
    "ok": True,
    "booked": True,
    "dry_run": False,
    "appointment_id": appointment_id,
    "contact_id": contact_id,
    "slot": slot_payload,
    "message": f"Booked for {slot_payload['display']}.",
  }
