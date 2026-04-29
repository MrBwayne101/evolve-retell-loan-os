from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Any

import httpx

from loan_os.fakes.fake_loan_os_server import FakeLoanOSServer
from loan_os.fakes.fake_voice_kernel import FakeVoiceKernel
from loan_os.paths import BAKEOFF_OUTPUT_PATH
from loan_os.schemas import schema_slo, validate_payload


TEST_SCRIPT = [
  "I'm looking into a DSCR loan",
  "duplex in Ohio, 720 credit, value 400k, owe 200k, cash out",
  "what's your rate?",
  "yeah, sure",
]


async def _log_audit(
  client: httpx.AsyncClient,
  base_url: str,
  *,
  kind: str,
  details: dict[str, Any],
) -> dict[str, Any]:
  payload = {
    "request_id": f"audit-{kind}",
    "kind": kind,
    "timestamp_ms": int(time.time() * 1000),
    "details": details,
  }
  validate_payload("audit_log_input", payload)
  response = await client.post(f"{base_url}/audit/log", json=payload)
  body = response.json()
  validate_payload("audit_log_output", body)
  return body


async def _post_contract(
  client: httpx.AsyncClient,
  base_url: str,
  path: str,
  input_schema: str,
  output_schema: str,
  payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
  validate_payload(input_schema, payload)
  started = time.perf_counter()
  response = await client.post(f"{base_url}{path}", json=payload)
  latency_ms = round((time.perf_counter() - started) * 1000, 3)
  response.raise_for_status()
  body = response.json()
  validate_payload(output_schema, body)
  slo = schema_slo(output_schema)
  return body, {
    "path": path,
    "input_schema": input_schema,
    "output_schema": output_schema,
    "latency_ms": latency_ms,
    "schema_valid": True,
    "slo": slo,
    "slo_passed": latency_ms <= slo["p50_ms"] and latency_ms <= slo["p95_ms"],
  }


async def run_bakeoff(
  *,
  voice: str = "fake",
  loan_os: str = "fake",
  output_path: Path | None = None,
) -> dict[str, Any]:
  if voice != "fake" or loan_os != "fake":
    raise ValueError("Track 0 only supports fake adapters.")

  output_path = output_path or BAKEOFF_OUTPUT_PATH
  server = FakeLoanOSServer()
  kernel = FakeVoiceKernel(scripted_transcript=list(TEST_SCRIPT))
  base_url = await server.start()
  measurements: list[dict[str, Any]] = []
  history: list[dict[str, Any]] = []
  expectations: list[dict[str, Any]] = []
  try:
    async with httpx.AsyncClient(timeout=5.0) as client:
      await _log_audit(client, base_url, kind="run_started", details={"voice": voice, "loan_os": loan_os})

      compliance_payload = {
        "request_id": "compliance-1",
        "transcript": "",
        "caller_phone": "+15555550123",
        "local_hour": 14,
        "ai_disclosure_present": True,
      }
      compliance_response, measurement = await _post_contract(
        client,
        base_url,
        "/compliance/gate",
        "compliance_gate_input",
        "compliance_gate_output",
        compliance_payload,
      )
      measurements.append(measurement)
      await _log_audit(client, base_url, kind="compliance_check", details=compliance_response)

      voice_open = kernel.open_call()
      measurements.append(
        {
          "path": "voice_transport.open_call",
          "output_schema": "voice_transport_output",
          "latency_ms": voice_open["timing_ms"],
          "schema_valid": True,
          "slo": schema_slo("voice_transport_output"),
          "slo_passed": True,
        }
      )

      scenario: dict[str, Any] = {
        "scenario_id": "scenario-fake-001",
        "stage": "opener",
        "state_machine": {
          "stage": "opener",
          "fields_collected": {},
          "leverage_signals": [],
          "pitch_attempt_count": 0,
          "consent_explicit_yes_received": False,
          "regen_count_per_turn": 0,
          "history": [],
        },
      }

      turn1_event = kernel.next_caller_transcript()
      await _log_audit(client, base_url, kind="caller_turn", details=turn1_event or {})
      collect1, m1 = await _post_contract(
        client,
        base_url,
        "/scenario/collector",
        "scenario_collector_input",
        "scenario_collector_output",
        {
          "request_id": "scenario-1",
          "transcript": TEST_SCRIPT[0],
          "scenario": scenario,
        },
      )
      measurements.append(m1)
      scenario = collect1["scenario"]
      controller1, m2 = await _post_contract(
        client,
        base_url,
        "/conversation/controller",
        "conversation_controller_input",
        "conversation_controller_output",
        {
          "request_id": "controller-1",
          "transcript": TEST_SCRIPT[0],
          "scenario": scenario,
          "history": history,
        },
      )
      measurements.append(m2)
      expectations.append(
        {
          "turn": 1,
          "checks": {
            "product": scenario.get("product") == "dscr",
            "action": controller1["action"] == "ASK_DISCOVERY",
            "card": controller1["card_id"] == "discovery_open",
          },
        }
      )
      card1, m3 = await _post_contract(
        client,
        base_url,
        "/cards/play",
        "card_player_input",
        "card_player_output",
        {
          "request_id": "card-1",
          "card_id": controller1["card_id"],
          "voice_session_id": kernel.voice_session_id,
          "scenario": scenario,
        },
      )
      measurements.append(m3)
      scenario = card1["scenario"]
      history.append({"kind": "card_playback", "card_id": card1["card_id"]})
      kernel.play_card(card_id=card1["card_id"], text=card1["card_text"])
      await _log_audit(client, base_url, kind="card_played", details={"card_id": card1["card_id"]})

      turn2_event = kernel.next_caller_transcript()
      await _log_audit(client, base_url, kind="caller_turn", details=turn2_event or {})
      collect2, m4 = await _post_contract(
        client,
        base_url,
        "/scenario/collector",
        "scenario_collector_input",
        "scenario_collector_output",
        {
          "request_id": "scenario-2",
          "transcript": TEST_SCRIPT[1],
          "scenario": scenario,
        },
      )
      measurements.append(m4)
      scenario = collect2["scenario"]
      controller2, m5 = await _post_contract(
        client,
        base_url,
        "/conversation/controller",
        "conversation_controller_input",
        "conversation_controller_output",
        {
          "request_id": "controller-2",
          "transcript": TEST_SCRIPT[1],
          "scenario": scenario,
          "history": history,
        },
      )
      measurements.append(m5)
      expectations.append(
        {
          "turn": 2,
          "checks": {
            "property_type": scenario.get("property_type") == "duplex",
            "state": scenario.get("state") == "OH",
            "credit": scenario.get("credit") == 720,
            "value": scenario.get("value") == 400000,
            "balance": scenario.get("balance") == 200000,
            "goal": scenario.get("goal") == "cash_out",
            "action": controller2["card_id"] == "pitch_full",
          },
        }
      )
      card2, m6 = await _post_contract(
        client,
        base_url,
        "/cards/play",
        "card_player_input",
        "card_player_output",
        {
          "request_id": "card-2",
          "card_id": controller2["card_id"],
          "voice_session_id": kernel.voice_session_id,
          "scenario": scenario,
        },
      )
      measurements.append(m6)
      scenario = card2["scenario"]
      history.append({"kind": "card_playback", "card_id": card2["card_id"]})
      kernel.play_card(card_id=card2["card_id"], text=card2["card_text"])
      await _log_audit(client, base_url, kind="card_played", details={"card_id": card2["card_id"]})

      turn3_event = kernel.next_caller_transcript()
      await _log_audit(client, base_url, kind="caller_turn", details=turn3_event or {})
      collect3, m7 = await _post_contract(
        client,
        base_url,
        "/scenario/collector",
        "scenario_collector_input",
        "scenario_collector_output",
        {
          "request_id": "scenario-3",
          "transcript": TEST_SCRIPT[2],
          "scenario": scenario,
        },
      )
      measurements.append(m7)
      scenario = collect3["scenario"]
      controller3, m8 = await _post_contract(
        client,
        base_url,
        "/conversation/controller",
        "conversation_controller_input",
        "conversation_controller_output",
        {
          "request_id": "controller-3",
          "transcript": TEST_SCRIPT[2],
          "scenario": scenario,
          "history": history,
        },
      )
      measurements.append(m8)
      expectations.append(
        {
          "turn": 3,
          "checks": {
            "action": controller3["card_id"] == "obj_rate_post_pitch",
            "stage": scenario.get("stage") == "pitch_delivered",
          },
        }
      )
      card3, m9 = await _post_contract(
        client,
        base_url,
        "/cards/play",
        "card_player_input",
        "card_player_output",
        {
          "request_id": "card-3",
          "card_id": controller3["card_id"],
          "voice_session_id": kernel.voice_session_id,
          "scenario": scenario,
        },
      )
      measurements.append(m9)
      scenario = card3["scenario"]
      history.append({"kind": "card_playback", "card_id": card3["card_id"]})
      kernel.play_card(card_id=card3["card_id"], text=card3["card_text"])
      await _log_audit(client, base_url, kind="card_played", details={"card_id": card3["card_id"]})

      controller4, m10 = await _post_contract(
        client,
        base_url,
        "/conversation/controller",
        "conversation_controller_input",
        "conversation_controller_output",
        {
          "request_id": "controller-4",
          "transcript": "",
          "scenario": scenario,
          "history": history,
        },
      )
      measurements.append(m10)
      expectations.append(
        {
          "turn": 4,
          "checks": {"action": controller4["card_id"] == "close_transfer_consent_ask"},
        }
      )
      card4, m11 = await _post_contract(
        client,
        base_url,
        "/cards/play",
        "card_player_input",
        "card_player_output",
        {
          "request_id": "card-4",
          "card_id": controller4["card_id"],
          "voice_session_id": kernel.voice_session_id,
          "scenario": scenario,
        },
      )
      measurements.append(m11)
      scenario = card4["scenario"]
      history.append({"kind": "card_playback", "card_id": card4["card_id"]})
      kernel.play_card(card_id=card4["card_id"], text=card4["card_text"])
      await _log_audit(client, base_url, kind="card_played", details={"card_id": card4["card_id"]})

      turn5_event = kernel.next_caller_transcript()
      await _log_audit(client, base_url, kind="caller_turn", details=turn5_event or {})
      collect5, m12 = await _post_contract(
        client,
        base_url,
        "/scenario/collector",
        "scenario_collector_input",
        "scenario_collector_output",
        {
          "request_id": "scenario-5",
          "transcript": TEST_SCRIPT[3],
          "scenario": scenario,
        },
      )
      measurements.append(m12)
      scenario = collect5["scenario"]
      controller5, m13 = await _post_contract(
        client,
        base_url,
        "/conversation/controller",
        "conversation_controller_input",
        "conversation_controller_output",
        {
          "request_id": "controller-5",
          "transcript": TEST_SCRIPT[3],
          "scenario": scenario,
          "history": history,
        },
      )
      measurements.append(m13)
      expectations.append(
        {
          "turn": 5,
          "checks": {
            "action": controller5["action"] == "CALL_TOOL",
            "consent": controller5["tool_payload"]["consent"] is True,
          },
        }
      )
      tools5, m14 = await _post_contract(
        client,
        base_url,
        "/loan-tools",
        "loan_tools_input",
        "loan_tools_output",
        {
          "request_id": "tools-5",
          "operation": "book_or_transfer",
          "scenario": scenario,
          "arguments": controller5["tool_payload"],
        },
      )
      measurements.append(m14)
      scenario = tools5["scenario"]
      await _log_audit(
        client,
        base_url,
        kind="tool_result",
        details={"tool_name": "book_or_transfer", "result": tools5["result"]},
      )

      audit_dump = (await client.get(f"{base_url}/audit/log")).json()
      all_expectations_passed = all(all(checks.values()) for checks in (item["checks"] for item in expectations))
      all_schema_passed = all(item["schema_valid"] for item in measurements)
      all_slo_passed = all(item["slo_passed"] for item in measurements)
      audit_entries = audit_dump["entries"]
      audit_complete = any(
        entry.get("kind") == "tool_result" for entry in audit_entries
      ) and any(
        entry.get("kind") == "tool_call" and entry.get("consent") is True for entry in audit_entries
      )
      result = {
        "run_id": "track-0-fake-run",
        "voice": voice,
        "loan_os": loan_os,
        "passed": all_expectations_passed and all_schema_passed and all_slo_passed and audit_complete,
        "measurements": measurements,
        "expectations": expectations,
        "contract_compliance": {"all_valid": all_schema_passed},
        "timing_slos": {"all_met": all_slo_passed},
        "audit": {
          "complete": audit_complete,
          "entry_count": len(audit_entries),
          "entries": audit_entries,
        },
        "final_scenario": scenario,
        "voice_event_log": kernel.event_log,
      }
      output_path.parent.mkdir(parents=True, exist_ok=True)
      output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
      return result
  finally:
    await server.stop()


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(description="Run the Track 0 fake bakeoff.")
  parser.add_argument("--voice", default="fake")
  parser.add_argument("--loan_os", default="fake")
  parser.add_argument("--output", default=str(BAKEOFF_OUTPUT_PATH))
  return parser.parse_args()


def main() -> int:
  args = parse_args()
  result = asyncio.run(
    run_bakeoff(
      voice=args.voice,
      loan_os=args.loan_os,
      output_path=Path(args.output),
    )
  )
  print(json.dumps(result, indent=2))
  return 0 if result["passed"] else 1


if __name__ == "__main__":
  raise SystemExit(main())
