from __future__ import annotations

import time
from typing import Any, Awaitable, Callable

from aiohttp import web

from loan_os.domain.card_player import play_card
from loan_os.domain.controller import decide_next_action
from loan_os.domain.scenario import scenario_to_state, state_to_scenario
from loan_os.schemas import assert_contract, validate_payload, validation_error_response


HandlerFunc = Callable[[dict[str, Any]], dict[str, Any]]
AUDIT_ENTRIES_KEY: web.AppKey[list[dict[str, Any]]] = web.AppKey("audit_entries")


def run_pricing(scenario: dict[str, Any]) -> dict[str, Any]:
  credit = scenario.get("credit")
  credit_adj = 0.0 if not isinstance(credit, int) else max(0, (760 - credit) * 0.005)
  rate = round(6.125 + credit_adj, 3)
  return {
    "product": scenario.get("product", "dscr"),
    "rate": rate,
    "ltv_max": 75,
    "points": 1.25,
  }


def run_dscr_calc(arguments: dict[str, Any]) -> dict[str, Any]:
  rent = float(arguments.get("rent", 2500))
  pitia = float(arguments.get("pitia", 2000))
  ratio = round(rent / pitia, 3) if pitia else 0.0
  return {
    "rent": rent,
    "pitia": pitia,
    "dscr": ratio,
    "passes_minimum": ratio >= 1.0,
  }


def run_app_fill(scenario: dict[str, Any]) -> dict[str, Any]:
  return {
    "application_id": "app-fake-001",
    "fields_prefilled": {
      "product": scenario.get("product"),
      "property_type": scenario.get("property_type"),
      "state": scenario.get("state"),
      "goal": scenario.get("goal"),
    },
  }


def run_ghl_crm(arguments: dict[str, Any]) -> dict[str, Any]:
  return {
    "crm_contact_id": "ghl-contact-fake-001",
    "crm_action": arguments.get("action", "upsert"),
    "status": "recorded",
  }


def run_book_or_transfer(scenario: dict[str, Any]) -> dict[str, Any]:
  return {
    "booking_id": "booking-fake-001",
    "booked": True,
    "booked_immediately": True,
    "loan_officer_id": "lo-fake-007",
    "live_transfer_available": True,
    "scenario_id": scenario.get("scenario_id"),
  }


def run_compliance_gate(payload: dict[str, Any]) -> dict[str, Any]:
  transcript = str(payload.get("transcript", "")).lower()
  local_hour = int(payload.get("local_hour", 14))
  dnc_match = any(phrase in transcript for phrase in ("stop calling", "do not call", "don't call"))
  allowed_hour = 8 <= local_hour <= 20
  ai_disclosure_present = bool(payload.get("ai_disclosure_present", True))
  blocked = dnc_match or not allowed_hour or not ai_disclosure_present
  reasons = []
  if dnc_match:
    reasons.append("dnc_request_detected")
  if not allowed_hour:
    reasons.append("outside_calling_hours")
  if not ai_disclosure_present:
    reasons.append("missing_ai_disclosure")
  return {
    "ok": True,
    "request_id": "compliance-gate-response",
    "blocked": blocked,
    "allow_call": not blocked,
    "reasons": reasons,
    "timing_ms": 25,
  }


class FakeLoanOSServer:
  def __init__(self) -> None:
    self.audit_entries: list[dict[str, Any]] = []
    self._app = self.build_app()
    self._runner: web.AppRunner | None = None
    self._site: web.TCPSite | None = None
    self.base_url: str | None = None

  def build_app(self) -> web.Application:
    app = web.Application()
    app[AUDIT_ENTRIES_KEY] = self.audit_entries
    app.router.add_get("/health", self.handle_health)
    app.router.add_post("/conversation/controller", self._make_handler(
      input_schema="conversation_controller_input",
      output_schema="conversation_controller_output",
      logic=self._conversation_logic,
    ))
    app.router.add_post("/scenario/collector", self._make_handler(
      input_schema="scenario_collector_input",
      output_schema="scenario_collector_output",
      logic=self._scenario_logic,
    ))
    app.router.add_post("/cards/play", self._make_handler(
      input_schema="card_player_input",
      output_schema="card_player_output",
      logic=self._card_logic,
    ))
    app.router.add_post("/loan-tools", self._make_handler(
      input_schema="loan_tools_input",
      output_schema="loan_tools_output",
      logic=self._loan_tool_logic,
    ))
    app.router.add_post("/compliance/gate", self._make_handler(
      input_schema="compliance_gate_input",
      output_schema="compliance_gate_output",
      logic=self._compliance_logic,
    ))
    app.router.add_post("/audit/log", self._make_handler(
      input_schema="audit_log_input",
      output_schema="audit_log_output",
      logic=self._audit_logic,
    ))
    app.router.add_get("/audit/log", self.handle_audit_dump)
    return app

  async def start(self, host: str = "127.0.0.1", port: int = 0) -> str:
    self._runner = web.AppRunner(self._app)
    await self._runner.setup()
    self._site = web.TCPSite(self._runner, host=host, port=port)
    await self._site.start()
    sockets = self._site._server.sockets  # type: ignore[union-attr]
    resolved_port = sockets[0].getsockname()[1]
    self.base_url = f"http://{host}:{resolved_port}"
    return self.base_url

  async def stop(self) -> None:
    if self._runner is not None:
      await self._runner.cleanup()
      self._runner = None
      self._site = None

  async def handle_health(self, _: web.Request) -> web.Response:
    return web.json_response({"ok": True, "service": "fake-loan-os"})

  async def handle_audit_dump(self, _: web.Request) -> web.Response:
    return web.json_response({"ok": True, "entries": list(self.audit_entries)})

  def _make_handler(
    self,
    *,
    input_schema: str,
    output_schema: str,
    logic: HandlerFunc,
  ) -> Callable[[web.Request], Awaitable[web.Response]]:
    async def handler(request: web.Request) -> web.Response:
      try:
        payload = await request.json()
        validate_payload(input_schema, payload)
      except Exception as exc:
        return web.json_response(
          validation_error_response(
            adapter=input_schema,
            message=str(exc),
            request_id=f"{input_schema}-error",
          ),
          status=400,
        )
      started = time.perf_counter()
      response_payload = logic(payload)
      if response_payload.get("ok") is not False and "timing_ms" not in response_payload:
        response_payload["timing_ms"] = round((time.perf_counter() - started) * 1000, 3)
      valid, message = assert_contract(output_schema, response_payload)
      if not valid:
        return web.json_response(
          validation_error_response(
            adapter=output_schema,
            message=message or "response validation failed",
            request_id=f"{output_schema}-error",
          ),
          status=500,
        )
      return web.json_response(response_payload)

    return handler

  def _scenario_logic(self, payload: dict[str, Any]) -> dict[str, Any]:
    from loan_os.domain.scenario import collect_scenario_fields

    return collect_scenario_fields(payload["transcript"], payload.get("scenario"))

  def _conversation_logic(self, payload: dict[str, Any]) -> dict[str, Any]:
    return decide_next_action(
      payload.get("transcript", ""),
      payload.get("scenario"),
      payload.get("history", []),
    )

  def _card_logic(self, payload: dict[str, Any]) -> dict[str, Any]:
    return play_card(payload["card_id"], payload.get("scenario"), payload["voice_session_id"])

  def _loan_tool_logic(self, payload: dict[str, Any]) -> dict[str, Any]:
    operation = payload["operation"]
    scenario = dict(payload.get("scenario") or {})
    arguments = dict(payload.get("arguments") or {})
    if operation == "pricing":
      result = run_pricing(scenario)
    elif operation == "dscr_calc":
      result = run_dscr_calc(arguments)
    elif operation == "app_fill":
      result = run_app_fill(scenario)
    elif operation == "ghl_crm":
      result = run_ghl_crm(arguments)
    elif operation == "calendar":
      result = {"calendar_slot_id": "slot-fake-001", "status": "held"}
    elif operation == "book_or_transfer":
      state = scenario_to_state(scenario)
      state.on_tool_call("book_or_transfer", require_explicit_consent=True, enforce_guards=False)
      result = run_book_or_transfer(scenario)
      state.on_tool_result("book_or_transfer", result, args=arguments, enforce_guards=False)
      scenario = state_to_scenario(state, scenario)
      self.audit_entries.append(
        {
          "kind": "tool_call",
          "tool_name": "book_or_transfer",
          "consent": arguments.get("consent"),
          "result": result,
        }
      )
    else:
      result = {"status": "noop"}
    response = {
      "ok": True,
      "request_id": "loan-tools-response",
      "operation": operation,
      "result": result,
      "scenario": scenario,
      "timing_ms": 55,
    }
    return response

  def _compliance_logic(self, payload: dict[str, Any]) -> dict[str, Any]:
    return run_compliance_gate(payload)

  def _audit_logic(self, payload: dict[str, Any]) -> dict[str, Any]:
    entry = {
      "kind": payload["kind"],
      "timestamp_ms": int(payload["timestamp_ms"]),
      "details": payload["details"],
    }
    self.audit_entries.append(entry)
    return {
      "ok": True,
      "request_id": "audit-log-response",
      "logged": True,
      "entry_count": len(self.audit_entries),
      "timing_ms": 10,
    }
