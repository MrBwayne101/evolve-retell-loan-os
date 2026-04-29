from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import ValidationError
from jsonschema.validators import validator_for

from loan_os.paths import CONTRACTS_DIR


def schema_path(name: str) -> Path:
  return CONTRACTS_DIR / f"{name}.schema.json"


@lru_cache(maxsize=None)
def load_schema(name: str) -> dict[str, Any]:
  return json.loads(schema_path(name).read_text(encoding="utf-8"))


@lru_cache(maxsize=None)
def schema_validator(name: str) -> Any:
  schema = load_schema(name)
  validator_cls = validator_for(schema)
  validator_cls.check_schema(schema)
  return validator_cls(schema)


def validate_payload(name: str, payload: Any) -> None:
  schema_validator(name).validate(payload)


def validate_schema_file(path: Path) -> None:
  schema = json.loads(path.read_text(encoding="utf-8"))
  validator_for(schema).check_schema(schema)


def all_schema_paths() -> list[Path]:
  return sorted(CONTRACTS_DIR.glob("*.schema.json"))


def validate_all_schemas() -> None:
  for path in all_schema_paths():
    validate_schema_file(path)


def schema_slo(name: str) -> dict[str, int]:
  schema = load_schema(name)
  return {
    "p50_ms": int(schema.get("x-sla-p50-ms", 0)),
    "p95_ms": int(schema.get("x-sla-p95-ms", 0)),
  }


def validation_error_response(
  *,
  adapter: str,
  message: str,
  request_id: str = "validation-error",
  details: dict[str, Any] | None = None,
) -> dict[str, Any]:
  payload = {
    "ok": False,
    "request_id": request_id,
    "error": {
      "adapter": adapter,
      "code": "validation_error",
      "message": message,
      "retryable": False,
      "details": details or {},
    },
  }
  validate_payload("error_response", payload)
  return payload


def assert_contract(name: str, payload: Any) -> tuple[bool, str | None]:
  try:
    validate_payload(name, payload)
  except ValidationError as exc:
    return False, exc.message
  return True, None
