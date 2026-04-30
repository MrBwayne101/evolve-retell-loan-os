from __future__ import annotations

import asyncio
from typing import Any

import httpx

from loan_os import ghl_calendar


class FakeResponse:
  def __init__(self, status_code: int, payload: Any) -> None:
    self.status_code = status_code
    self._payload = payload
    self.request = httpx.Request("GET", "https://example.test")

  def json(self) -> Any:
    return self._payload

  def raise_for_status(self) -> None:
    if self.status_code >= 400:
      raise httpx.HTTPStatusError(
        f"HTTP {self.status_code}",
        request=self.request,
        response=httpx.Response(self.status_code, request=self.request),
      )


class FakeAsyncClient:
  def __init__(self, *, responses: list[FakeResponse], calls: list[dict[str, Any]], timeout: float) -> None:
    self._responses = responses
    self.calls = calls
    self.timeout = timeout

  async def __aenter__(self) -> FakeAsyncClient:
    return self

  async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
    return None

  async def get(self, url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> FakeResponse:
    self.calls.append({"method": "GET", "url": url, "params": params or {}, "headers": headers or {}})
    return self._responses.pop(0)

  async def post(self, url: str, *, json: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> FakeResponse:
    self.calls.append({"method": "POST", "url": url, "json": json or {}, "headers": headers or {}})
    return self._responses.pop(0)


def test_availability_uses_mock_without_token(monkeypatch) -> None:
  monkeypatch.delenv("GHL_PRIVATE_INTEGRATION_TOKEN", raising=False)
  monkeypatch.setenv("RETELL_PROOF_USE_LIVE_GHL_AVAILABILITY", "false")

  result = asyncio.run(ghl_calendar.get_availability(limit=3))

  assert result["ok"] is True
  assert result["mode"] == "mock"
  assert 1 <= len(result["slots"]) <= 3
  assert result["slots"][0]["display"]


def test_live_availability_calls_ghl(monkeypatch) -> None:
  calls: list[dict[str, Any]] = []
  responses = [
    FakeResponse(200, {"slots": [{"startTime": "2026-04-28T15:00:00-07:00"}]}),
  ]
  monkeypatch.setenv("GHL_PRIVATE_INTEGRATION_TOKEN", "token-123")
  monkeypatch.setenv("GHL_LO_CALENDAR_ID", "calendar-123")
  monkeypatch.setenv("RETELL_PROOF_USE_LIVE_GHL_AVAILABILITY", "true")
  monkeypatch.setattr(
    ghl_calendar.httpx,
    "AsyncClient",
    lambda timeout: FakeAsyncClient(responses=responses, calls=calls, timeout=timeout),
  )

  result = asyncio.run(ghl_calendar.get_availability(timezone_name="America/Los_Angeles"))

  assert result["ok"] is True
  assert result["mode"] == "live"
  assert result["slots"][0]["start_iso"] == "2026-04-28T15:00:00-07:00"
  assert calls[0]["url"] == "https://services.leadconnectorhq.com/calendars/calendar-123/free-slots"


def test_select_spread_slots_skips_adjacent_openings() -> None:
  slots = [
    {"start_iso": "2026-04-28T07:30:00-07:00", "display": "tomorrow at 7:30 am"},
    {"start_iso": "2026-04-28T08:00:00-07:00", "display": "tomorrow at 8 am"},
    {"start_iso": "2026-04-28T09:00:00-07:00", "display": "tomorrow at 9 am"},
    {"start_iso": "2026-04-28T11:30:00-07:00", "display": "tomorrow at 11:30 am"},
    {"start_iso": "2026-04-28T14:00:00-07:00", "display": "tomorrow at 2 pm"},
    {"start_iso": "2026-04-29T09:30:00-07:00", "display": "Wednesday at 9:30 am"},
  ]

  result = ghl_calendar.select_spread_slots(slots, limit=3)

  assert [slot["display"] for slot in result] == [
    "tomorrow at 7:30 am",
    "tomorrow at 11:30 am",
    "Wednesday at 9:30 am",
  ]


def test_format_slot_options_uses_three_clean_options() -> None:
  slots = [
    {"display": "today at 11 am"},
    {"display": "today at 2 pm"},
    {"display": "tomorrow at 9 am"},
  ]

  assert ghl_calendar.format_slot_options(slots) == "today at 11 am, today at 2 pm, or tomorrow at 9 am"


def test_book_selected_slot_defaults_to_dry_run(monkeypatch) -> None:
  monkeypatch.setenv("RETELL_PROOF_ALLOW_LIVE_GHL_BOOKING", "false")

  result = asyncio.run(ghl_calendar.book_selected_slot({
    "slot_start_iso": "2026-04-28T15:00:00-07:00",
    "summary": "Ohio duplex purchase",
    "contact_phone": "+17147911882",
  }))

  assert result["ok"] is True
  assert result["booked"] is True
  assert result["dry_run"] is True
  assert result["appointment_id"].startswith("MOCK-")
