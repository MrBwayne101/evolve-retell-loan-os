from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from loan_os.call_center.ledger import (
  EventEnvelope,
  EvidenceRef,
  normalize_timestamp,
  payload_excerpt,
  redact_email,
  redact_phone,
  stable_id,
  utc_now_iso,
)


ATTRIBUTION_FIELDS = [
  "gclid",
  "gbraid",
  "wbraid",
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_adgroup",
  "utm_term",
  "utm_content",
  "matchtype",
  "device",
  "network",
  "campaign_id",
  "adgroup_id",
  "creative",
  "keyword",
  "search_term",
  "msclkid",
  "fbclid",
  "source",
  "campaign",
  "adgroup",
]

SCENARIO_FIELDS = [
  "loan_goal",
  "property_state",
  "property_type",
  "estimated_value",
  "purchase_price",
  "current_balance",
  "cash_out_requested",
  "credit_band",
  "rental_type",
  "properties_owned",
  "entity_vesting",
  "loan_amount_estimate",
]

SHADOW_QUEUE_NAME = "speed-to-lead-shadow-queue"
SHADOW_MODE = "shadow_only_no_external_writes"
SPEED_TO_LEAD_TIMEZONE = "America/Los_Angeles"
WINDOW_RULE = (
  "Pacific policy: M-F 08:00-19:00; M-F 17:00-19:00 appointment-only; "
  "Saturday 08:00-16:00; Sunday closed"
)

DEFAULT_TIMEZONE_BY_STATE = {
  "AL": "America/Chicago",
  "AK": "America/Anchorage",
  "AZ": "America/Phoenix",
  "AR": "America/Chicago",
  "CA": "America/Los_Angeles",
  "CO": "America/Denver",
  "CT": "America/New_York",
  "DC": "America/New_York",
  "DE": "America/New_York",
  "FL": "America/New_York",
  "GA": "America/New_York",
  "HI": "Pacific/Honolulu",
  "IA": "America/Chicago",
  "ID": "America/Denver",
  "IL": "America/Chicago",
  "IN": "America/Indiana/Indianapolis",
  "KS": "America/Chicago",
  "KY": "America/New_York",
  "LA": "America/Chicago",
  "MA": "America/New_York",
  "MD": "America/New_York",
  "ME": "America/New_York",
  "MI": "America/Detroit",
  "MN": "America/Chicago",
  "MO": "America/Chicago",
  "MS": "America/Chicago",
  "MT": "America/Denver",
  "NC": "America/New_York",
  "ND": "America/Chicago",
  "NE": "America/Chicago",
  "NH": "America/New_York",
  "NJ": "America/New_York",
  "NM": "America/Denver",
  "NV": "America/Los_Angeles",
  "NY": "America/New_York",
  "OH": "America/New_York",
  "OK": "America/Chicago",
  "OR": "America/Los_Angeles",
  "PA": "America/New_York",
  "RI": "America/New_York",
  "SC": "America/New_York",
  "SD": "America/Chicago",
  "TN": "America/Chicago",
  "TX": "America/Chicago",
  "UT": "America/Denver",
  "VA": "America/New_York",
  "VT": "America/New_York",
  "WA": "America/Los_Angeles",
  "WI": "America/Chicago",
  "WV": "America/New_York",
  "WY": "America/Denver",
}


@dataclass(frozen=True)
class ComplianceDecision:
  eligible: bool
  status: str
  reasons: list[str]
  warnings: list[str]
  timezone: str
  local_time_iso: str
  allowed_at: str | None
  window_rule: str
  dnc_flag: bool
  tcpa_accepted: bool
  privacy_accepted: bool | None
  ai_voice_disclosure_present: bool
  operating_mode: str
  transfer_allowed: bool
  appointment_only: bool
  transfer_hold_seconds: int
  write_mode: str = SHADOW_MODE

  def to_record(self) -> dict[str, Any]:
    return asdict(self)


@dataclass(frozen=True)
class SpeedToLeadShadowResult:
  normalized_event: dict[str, Any]
  compliance: ComplianceDecision
  call_context: dict[str, Any]
  retell_request: dict[str, Any]
  audit_event: EventEnvelope
  shadow_queue_row: dict[str, Any]

  def to_record(self) -> dict[str, Any]:
    return {
      "normalized_event": self.normalized_event,
      "compliance": self.compliance.to_record(),
      "call_context": self.call_context,
      "retell_request": self.retell_request,
      "audit_event": self.audit_event.to_record(),
      "shadow_queue_row": self.shadow_queue_row,
    }


def normalize_phone_e164(phone: str | None) -> str:
  raw = str(phone or "").strip()
  if not raw:
    return ""
  if raw.startswith("+"):
    digits = "".join(ch for ch in raw if ch.isdigit())
    return f"+{digits}" if digits else ""
  digits = "".join(ch for ch in raw if ch.isdigit())
  if len(digits) == 10:
    return f"+1{digits}"
  if len(digits) == 11 and digits.startswith("1"):
    return f"+{digits}"
  return raw


def redact_retell_request(payload: Mapping[str, Any]) -> dict[str, Any]:
  dynamic_variables = payload.get("retell_llm_dynamic_variables")
  metadata = payload.get("metadata")
  safe_dynamic = dict(dynamic_variables) if isinstance(dynamic_variables, Mapping) else {}
  safe_metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
  if safe_dynamic.get("contact_phone"):
    safe_dynamic["contact_phone"] = redact_phone(str(safe_dynamic["contact_phone"]))
  if safe_dynamic.get("contact_email"):
    safe_dynamic["contact_email"] = redact_email(str(safe_dynamic["contact_email"]))
  if safe_metadata.get("contact_email"):
    safe_metadata["contact_email"] = redact_email(str(safe_metadata["contact_email"]))
  return {
    "from_number": redact_phone(str(payload.get("from_number") or "")),
    "to_number": redact_phone(str(payload.get("to_number") or "")),
    "override_agent_id": str(payload.get("override_agent_id") or ""),
    "retell_llm_dynamic_variables": safe_dynamic,
    "metadata": safe_metadata,
  }


def normalize_new_lead_payload(
  payload: Mapping[str, Any],
  *,
  source_system: str | None = None,
) -> dict[str, Any]:
  contact = _mapping(payload.get("lead")) or _mapping(payload.get("contact"))
  scenario = _mapping(payload.get("scenario"))
  consent = _mapping(payload.get("consent"))
  page = _mapping(payload.get("page"))
  attribution = _mapping(payload.get("attribution"))
  custom_fields = _normalize_custom_fields(
    contact.get("customFields")
    or contact.get("custom_fields")
    or payload.get("customFields")
    or payload.get("custom_fields")
  )
  tags = _normalize_tags(contact.get("tags") or payload.get("tags"))
  detected_source = source_system or _detect_source_system(payload)

  event_name = _coalesce_text(
    payload.get("event"),
    payload.get("type"),
    payload.get("trigger"),
    "new_lead",
  )
  occurred_at = normalize_timestamp(
    _coalesce_text(
      payload.get("timestamp"),
      payload.get("created_at"),
      payload.get("createdAt"),
      payload.get("dateAdded"),
      payload.get("event_created_at"),
      consent.get("consent_timestamp"),
    )
  )
  page_url = _coalesce_text(page.get("url"), payload.get("page_url"), custom_fields.get("landing_page_url"))
  landing_session_id = _coalesce_text(
    payload.get("landing_session_id"),
    page.get("landing_session_id"),
    custom_fields.get("landing_session_id"),
  ) or stable_id(
    "landing_session",
    _coalesce_text(contact.get("id"), payload.get("contactId"), occurred_at, page_url),
  )
  first_name = _coalesce_text(
    contact.get("first_name"),
    contact.get("firstName"),
    payload.get("first_name"),
    payload.get("firstName"),
    custom_fields.get("first_name"),
  )
  last_name = _coalesce_text(
    contact.get("last_name"),
    contact.get("lastName"),
    payload.get("last_name"),
    payload.get("lastName"),
    custom_fields.get("last_name"),
  )
  email = _coalesce_text(contact.get("email"), payload.get("email"), custom_fields.get("email")).lower()
  phone_e164 = normalize_phone_e164(
    _coalesce_text(
      contact.get("phone"),
      contact.get("phoneE164"),
      payload.get("phone"),
      payload.get("phoneE164"),
      custom_fields.get("phone"),
    )
  )
  property_state = _coalesce_text(
    scenario.get("property_state"),
    payload.get("property_state"),
    contact.get("state"),
    custom_fields.get("property_state"),
    custom_fields.get("state"),
  ).upper()
  timezone_name = _resolve_timezone(
    _coalesce_text(
      contact.get("timezone"),
      payload.get("timezone"),
      scenario.get("timezone"),
      custom_fields.get("timezone"),
    ),
    property_state,
  )
  ghl_contact_id = _coalesce_text(
    payload.get("ghl_contact_id"),
    contact.get("id"),
    payload.get("contactId"),
    custom_fields.get("ghl_contact_id"),
  )

  normalized_attribution = _normalize_attribution(attribution, payload, custom_fields)
  scenario_fields = {
    field: _coalesce_text(scenario.get(field), payload.get(field), custom_fields.get(field))
    for field in SCENARIO_FIELDS
  }
  source_name = _coalesce_text(
    normalized_attribution.get("source"),
    normalized_attribution.get("utm_source"),
    payload.get("source"),
    contact.get("source"),
    detected_source,
  )
  campaign_name = _coalesce_text(
    normalized_attribution.get("campaign"),
    normalized_attribution.get("utm_campaign"),
  )
  tcpa_accepted = _coalesce_bool(
    consent.get("tcpa_accepted"),
    payload.get("tcpa_accepted"),
    payload.get("tcpaAccepted"),
    custom_fields.get("tcpa_accepted"),
  )
  privacy_accepted = _coalesce_optional_bool(
    consent.get("privacy_accepted"),
    payload.get("privacy_accepted"),
    payload.get("privacyAccepted"),
    custom_fields.get("privacy_accepted"),
  )
  ai_voice_accepted = _coalesce_optional_bool(
    consent.get("ai_voice_accepted"),
    consent.get("ai_voice_consent"),
    payload.get("ai_voice_accepted"),
    custom_fields.get("ai_voice_accepted"),
  )
  ai_voice_consent_text = _coalesce_text(
    consent.get("ai_voice_consent_text"),
    payload.get("ai_voice_consent_text"),
    custom_fields.get("ai_voice_consent_text"),
  )
  dnc_flag = _detect_dnc_flag(payload, contact, consent, custom_fields, tags)

  lead_event_id = stable_id(
    "speed_to_lead",
    detected_source,
    landing_session_id,
    phone_e164 or email or occurred_at,
  )
  loan_amount = _coalesce_text(
    scenario_fields.get("loan_amount_estimate"),
    payload.get("loan_amount_estimate"),
    custom_fields.get("loan_amount_estimate"),
    scenario_fields.get("purchase_price"),
    scenario_fields.get("cash_out_requested"),
    scenario_fields.get("estimated_value"),
  )

  return {
    "lead_event_id": lead_event_id,
    "event_name": event_name,
    "event_source": detected_source,
    "received_at": utc_now_iso(),
    "occurred_at": occurred_at,
    "routing": {
      "queue_name": SHADOW_QUEUE_NAME,
      "mode": SHADOW_MODE,
      "channel": "voice_outbound_shadow",
      "live_call_enabled": False,
      "ghl_write_enabled": False,
      "los_write_enabled": False,
      "borrower_messaging_enabled": False,
    },
    "lead": {
      "first_name": first_name or "there",
      "last_name": last_name,
      "full_name": " ".join(part for part in [first_name, last_name] if part).strip(),
      "email": email,
      "email_redacted": redact_email(email),
      "phone_e164": phone_e164,
      "phone_redacted": redact_phone(phone_e164),
      "timezone": timezone_name,
      "state": property_state,
      "ghl_contact_id": ghl_contact_id,
      "tags": tags,
    },
    "scenario": scenario_fields,
    "consent": {
      "tcpa_accepted": tcpa_accepted,
      "privacy_accepted": privacy_accepted,
      "ai_voice_accepted": ai_voice_accepted,
      "consent_timestamp": normalize_timestamp(
        _coalesce_text(
          consent.get("consent_timestamp"),
          payload.get("consent_timestamp"),
          occurred_at,
        )
      ),
      "ai_voice_consent_text": ai_voice_consent_text,
      "dnc": dnc_flag,
    },
    "page": {
      "url": page_url,
      "variant": _coalesce_text(page.get("variant"), payload.get("variant"), custom_fields.get("landing_page_variant")),
      "ab_test_id": _coalesce_text(page.get("ab_test_id"), payload.get("ab_test_id"), custom_fields.get("ab_test_id")),
      "landing_session_id": landing_session_id,
    },
    "attribution": normalized_attribution,
    "source": {
      "source_name": source_name,
      "campaign_name": campaign_name,
      "search_term": _coalesce_text(
        normalized_attribution.get("search_term"),
        normalized_attribution.get("utm_term"),
        normalized_attribution.get("keyword"),
      ),
      "loan_amount_estimate": loan_amount,
    },
    "custom_fields": custom_fields,
    "raw_flags": {
      "has_custom_fields": bool(custom_fields),
      "has_nested_consent": bool(consent),
      "has_nested_scenario": bool(scenario),
    },
  }


def evaluate_compliance_gate(
  normalized_event: Mapping[str, Any],
  *,
  now: datetime | None = None,
) -> ComplianceDecision:
  lead = _mapping(normalized_event.get("lead"))
  consent = _mapping(normalized_event.get("consent"))
  timezone_name = SPEED_TO_LEAD_TIMEZONE
  current_time = _ensure_utc(now)
  local_time = current_time.astimezone(ZoneInfo(timezone_name))
  operating_policy = _speed_to_lead_operating_policy(local_time)
  reasons: list[str] = []
  warnings: list[str] = []

  tcpa_accepted = bool(consent.get("tcpa_accepted"))
  privacy_accepted = _coalesce_optional_bool(consent.get("privacy_accepted"))
  ai_voice_disclosure_present = bool(_coalesce_text(consent.get("ai_voice_consent_text"))) or bool(
    consent.get("ai_voice_accepted")
  )
  dnc_flag = bool(consent.get("dnc"))

  if not str(lead.get("phone_e164") or "").strip():
    reasons.append("missing_phone")
  if not tcpa_accepted:
    reasons.append("missing_tcpa_consent")
  if privacy_accepted is False:
    reasons.append("privacy_consent_rejected")
  elif privacy_accepted is None:
    warnings.append("privacy_consent_unverified")
  if not ai_voice_disclosure_present:
    warnings.append("ai_voice_disclosure_unverified")
  if dnc_flag:
    reasons.append("do_not_call_flagged")

  allowed_at: str | None = None
  if not operating_policy["within_window"]:
    reasons.append("outside_dial_window")
    allowed_at = _next_allowed_dial_time(local_time).astimezone(UTC).replace(microsecond=0).isoformat().replace(
      "+00:00",
      "Z",
    )

  status = "eligible" if not reasons else "blocked"
  return ComplianceDecision(
    eligible=not reasons,
    status=status,
    reasons=reasons,
    warnings=warnings,
    timezone=timezone_name,
    local_time_iso=local_time.replace(microsecond=0).isoformat(),
    allowed_at=allowed_at,
    window_rule=WINDOW_RULE,
    dnc_flag=dnc_flag,
    tcpa_accepted=tcpa_accepted,
    privacy_accepted=privacy_accepted,
    ai_voice_disclosure_present=ai_voice_disclosure_present,
    operating_mode=str(operating_policy["operating_mode"]),
    transfer_allowed=bool(operating_policy["transfer_allowed"]) and not reasons,
    appointment_only=bool(operating_policy["appointment_only"]) and not reasons,
    transfer_hold_seconds=int(operating_policy["transfer_hold_seconds"]),
  )


def build_call_context(
  normalized_event: Mapping[str, Any],
  compliance: ComplianceDecision,
  *,
  now: datetime | None = None,
) -> dict[str, Any]:
  lead = _mapping(normalized_event.get("lead"))
  scenario = _mapping(normalized_event.get("scenario"))
  source = _mapping(normalized_event.get("source"))
  attribution = _mapping(normalized_event.get("attribution"))
  event_time = _parse_datetime(str(normalized_event.get("occurred_at") or utc_now_iso()))
  current_time = _ensure_utc(now)
  lead_age_seconds = max(0, int((current_time - event_time).total_seconds()))

  loan_goal = _coalesce_text(scenario.get("loan_goal"), "loan")
  property_state = _coalesce_text(scenario.get("property_state"), lead.get("state"))
  property_type = _coalesce_text(scenario.get("property_type"))
  opening_context_line = _build_opening_context_line(loan_goal, property_state, property_type)
  request_type_label = _build_request_type_label(loan_goal)
  recommended_first_question = _build_first_question(loan_goal)

  source_bits = [
    bit
    for bit in [
      _coalesce_text(source.get("source_name")),
      _coalesce_text(source.get("campaign_name")),
      _coalesce_text(source.get("search_term")),
    ]
    if bit
  ]
  lead_summary = " | ".join(
    bit
    for bit in [
      f"{lead.get('full_name') or lead.get('first_name') or 'Lead'} fresh lead",
      f"age_seconds={lead_age_seconds}",
      f"source={' / '.join(source_bits) if source_bits else 'unknown'}",
    ]
    if bit
  )

  return {
    "opening_context_line": opening_context_line,
    "request_type_label": request_type_label,
    "recommended_first_question": recommended_first_question,
    "lead_summary": lead_summary,
    "source_summary": _build_source_summary(attribution),
    "loan_amount_context": _coalesce_text(source.get("loan_amount_estimate")),
    "priority_hint": "speed_to_lead_seconds" if lead_age_seconds <= 300 else "stale_new_lead_review",
    "lead_age_seconds": lead_age_seconds,
    "compliance_status": compliance.status,
    "compliance_summary": ", ".join(compliance.reasons) if compliance.reasons else "eligible_now",
    "speed_to_lead_timezone": compliance.timezone,
    "speed_to_lead_operating_mode": compliance.operating_mode,
    "transfer_allowed": compliance.transfer_allowed,
    "appointment_only": compliance.appointment_only,
    "transfer_hold_seconds": compliance.transfer_hold_seconds,
    "next_allowed_call_at": compliance.allowed_at or "",
    "business_hours_label": WINDOW_RULE,
  }


def build_retell_dynamic_variables(
  normalized_event: Mapping[str, Any],
  compliance: ComplianceDecision,
  call_context: Mapping[str, Any],
) -> dict[str, Any]:
  lead = _mapping(normalized_event.get("lead"))
  scenario = _mapping(normalized_event.get("scenario"))
  source = _mapping(normalized_event.get("source"))
  return {
    "first_name": _coalesce_text(lead.get("first_name"), "there"),
    "lead_event_id": str(normalized_event.get("lead_event_id") or ""),
    "landing_session_id": _coalesce_text(_mapping(normalized_event.get("page")).get("landing_session_id")),
    "opening_context_line": _coalesce_text(call_context.get("opening_context_line")),
    "request_type_label": _coalesce_text(call_context.get("request_type_label"), _build_request_type_label(_coalesce_text(scenario.get("loan_goal")))),
    "recommended_first_question": _coalesce_text(call_context.get("recommended_first_question")),
    "lead_summary": _coalesce_text(call_context.get("lead_summary")),
    "borrower_timezone": _coalesce_text(lead.get("timezone")),
    "loan_goal": _coalesce_text(scenario.get("loan_goal")),
    "property_state": _coalesce_text(scenario.get("property_state")),
    "property_type": _coalesce_text(scenario.get("property_type")),
    "credit_band": _coalesce_text(scenario.get("credit_band")),
    "loan_amount_estimate": _coalesce_text(source.get("loan_amount_estimate")),
    "source_name": _coalesce_text(source.get("source_name")),
    "campaign_name": _coalesce_text(source.get("campaign_name")),
    "search_term": _coalesce_text(source.get("search_term")),
    "compliance_status": compliance.status,
    "consent_status": "tcpa_on_file" if compliance.tcpa_accepted else "consent_missing",
    "speed_to_lead_timezone": compliance.timezone,
    "speed_to_lead_operating_mode": compliance.operating_mode,
    "transfer_allowed": "true" if compliance.transfer_allowed else "false",
    "appointment_only": "true" if compliance.appointment_only else "false",
    "transfer_hold_seconds": str(compliance.transfer_hold_seconds),
    "next_allowed_call_at": compliance.allowed_at or "",
    "business_hours_label": WINDOW_RULE,
    "shadow_mode_notice": SHADOW_MODE,
    "contact_phone": _coalesce_text(lead.get("phone_e164")),
    "contact_email": _coalesce_text(lead.get("email")),
    "ghl_contact_id": _coalesce_text(lead.get("ghl_contact_id")),
  }


def build_dry_run_retell_request(
  normalized_event: Mapping[str, Any],
  compliance: ComplianceDecision,
  call_context: Mapping[str, Any],
  retell_config: Mapping[str, Any],
  *,
  to_number_override: str | None = None,
) -> dict[str, Any]:
  lead = _mapping(normalized_event.get("lead"))
  source = _mapping(normalized_event.get("source"))
  page = _mapping(normalized_event.get("page"))
  dynamic_variables = build_retell_dynamic_variables(normalized_event, compliance, call_context)
  target_number = normalize_phone_e164(to_number_override or _coalesce_text(lead.get("phone_e164")))
  queue_disposition = "ready_shadow_only" if compliance.eligible else "hold_or_review"
  return {
    "from_number": normalize_phone_e164(str(retell_config.get("phone_number") or "")),
    "to_number": target_number,
    "override_agent_id": str(retell_config.get("agent_id") or ""),
    "retell_llm_dynamic_variables": dynamic_variables,
    "metadata": {
      "project": "evolve_voice_agent",
      "purpose": "speed_to_lead_shadow_dry_run",
      "lead_event_id": str(normalized_event.get("lead_event_id") or ""),
      "event_source": str(normalized_event.get("event_source") or ""),
      "landing_session_id": _coalesce_text(page.get("landing_session_id")),
      "source_name": _coalesce_text(source.get("source_name")),
      "campaign_name": _coalesce_text(source.get("campaign_name")),
      "search_term": _coalesce_text(source.get("search_term")),
      "queue_name": SHADOW_QUEUE_NAME,
      "queue_disposition": queue_disposition,
      "speed_to_lead_timezone": compliance.timezone,
      "speed_to_lead_operating_mode": compliance.operating_mode,
      "transfer_allowed": "true" if compliance.transfer_allowed else "false",
      "appointment_only": "true" if compliance.appointment_only else "false",
      "transfer_hold_seconds": str(compliance.transfer_hold_seconds),
      "next_allowed_call_at": compliance.allowed_at or "",
      "safe_batch_tag": f"speed-to-lead-shadow-{utc_now_iso()[:10]}",
      "shadow_mode": "true",
      "launch_live_calls": "false",
      "ghl_write_enabled": "false",
      "los_write_enabled": "false",
      "borrower_messaging_enabled": "false",
      "ghl_contact_id": _coalesce_text(lead.get("ghl_contact_id")),
      "contact_email": _coalesce_text(lead.get("email")),
    },
  }


def build_shadow_queue_row(
  normalized_event: Mapping[str, Any],
  compliance: ComplianceDecision,
  call_context: Mapping[str, Any],
  retell_request: Mapping[str, Any],
) -> dict[str, Any]:
  lead = _mapping(normalized_event.get("lead"))
  source = _mapping(normalized_event.get("source"))
  next_action = "prepare_retell_request_shadow_only"
  if "outside_dial_window" in compliance.reasons:
    next_action = "hold_until_window_opens"
  elif compliance.reasons:
    next_action = "blocked_pending_ops_review"

  queue_row_id = stable_id(
    SHADOW_QUEUE_NAME,
    str(normalized_event.get("lead_event_id") or ""),
    compliance.status,
  )
  return {
    "queue_row_id": queue_row_id,
    "queue_name": SHADOW_QUEUE_NAME,
    "created_at": utc_now_iso(),
    "lead_event_id": str(normalized_event.get("lead_event_id") or ""),
    "status": compliance.status,
    "next_action": next_action,
    "write_mode": SHADOW_MODE,
    "lead_name": _coalesce_text(lead.get("full_name"), lead.get("first_name"), "Unknown Lead"),
    "phone_redacted": _coalesce_text(lead.get("phone_redacted")),
    "email_redacted": _coalesce_text(lead.get("email_redacted")),
    "timezone": compliance.timezone,
    "allowed_at": compliance.allowed_at or "",
    "operating_mode": compliance.operating_mode,
    "transfer_allowed": compliance.transfer_allowed,
    "appointment_only": compliance.appointment_only,
    "transfer_hold_seconds": compliance.transfer_hold_seconds,
    "reasons": list(compliance.reasons),
    "warnings": list(compliance.warnings),
    "source_name": _coalesce_text(source.get("source_name")),
    "campaign_name": _coalesce_text(source.get("campaign_name")),
    "search_term": _coalesce_text(source.get("search_term")),
    "opening_context_line": _coalesce_text(call_context.get("opening_context_line")),
    "recommended_first_question": _coalesce_text(call_context.get("recommended_first_question")),
    "retell_to_number_redacted": redact_phone(str(retell_request.get("to_number") or "")),
    "retell_agent_id": str(retell_request.get("override_agent_id") or ""),
    "evidence_refs": [
      asdict(EvidenceRef("lead_event", str(normalized_event.get("lead_event_id") or ""), note=str(normalized_event.get("event_source") or ""))),
      asdict(EvidenceRef("landing_session", _coalesce_text(_mapping(normalized_event.get("page")).get("landing_session_id")), note="landing_session_id")),
    ],
  }


def build_audit_event(
  normalized_event: Mapping[str, Any],
  compliance: ComplianceDecision,
  retell_request: Mapping[str, Any],
  shadow_queue_row: Mapping[str, Any],
) -> EventEnvelope:
  occurred_at = utc_now_iso()
  lead = _mapping(normalized_event.get("lead"))
  payload = {
    "lead_event_id": str(normalized_event.get("lead_event_id") or ""),
    "queue_row_id": str(shadow_queue_row.get("queue_row_id") or ""),
    "queue_name": SHADOW_QUEUE_NAME,
    "compliance_status": compliance.status,
    "compliance_reasons": list(compliance.reasons),
    "operating_mode": compliance.operating_mode,
    "transfer_allowed": compliance.transfer_allowed,
    "appointment_only": compliance.appointment_only,
    "transfer_hold_seconds": compliance.transfer_hold_seconds,
    "next_action": str(shadow_queue_row.get("next_action") or ""),
    "to_number_redacted": redact_phone(str(retell_request.get("to_number") or "")),
    "from_number_redacted": redact_phone(str(retell_request.get("from_number") or "")),
    "agent_id": str(retell_request.get("override_agent_id") or ""),
    "lead_phone_redacted": _coalesce_text(lead.get("phone_redacted")),
    "lead_email_redacted": _coalesce_text(lead.get("email_redacted")),
  }
  return EventEnvelope(
    event_id=stable_id("loan_os", "speed_to_lead_shadow_prepared", str(normalized_event.get("lead_event_id") or ""), occurred_at),
    event_type="speed_to_lead_shadow_prepared",
    occurred_at=occurred_at,
    ingested_at=occurred_at,
    source_system="loan_os",
    source_id=str(normalized_event.get("lead_event_id") or ""),
    contact_id=str(lead.get("ghl_contact_id") or ""),
    campaign_id=_coalesce_text(_mapping(normalized_event.get("source")).get("campaign_name")),
    actor_type="system",
    actor_id="speed_to_lead_shadow",
    payload=payload,
    pii_classification="internal",
    confidence=0.98,
    evidence_refs=[
      asdict(EvidenceRef("shadow_queue", str(shadow_queue_row.get("queue_row_id") or ""), note=str(shadow_queue_row.get("status") or ""))),
      asdict(EvidenceRef("retell_payload", str(normalized_event.get("lead_event_id") or ""), excerpt=payload_excerpt(str(payload)))),
    ],
  )


def build_activation_checklist(script_verified: bool = False) -> list[dict[str, Any]]:
  return [
    {"item": "Custom website lead event normalization works in shadow mode", "status": "pass"},
    {"item": "Future GHL webhook normalization path exists without requiring native form submissions", "status": "pass"},
    {"item": "TCPA, DNC, and dial-window gate runs before any live-call path", "status": "pass"},
    {"item": "Retell outbound request is generated as dry-run payload only", "status": "pass"},
    {"item": "Shadow queue row and audit event are written locally only", "status": "pass" if script_verified else "pending"},
    {"item": "GHL writes remain disabled for contacts, notes, tags, statuses, appointments, workflows, and messages", "status": "pass"},
    {"item": "LOS writes remain disabled", "status": "pass"},
    {"item": "Borrower SMS and email remain disabled", "status": "pass"},
    {"item": "Explicit live-call approval switch and operator runbook are reviewed by Dave", "status": "fail"},
    {"item": "Workflow-risk review is completed before any future GHL writeback", "status": "fail"},
    {"item": "Retell live-dial wrapper is reviewed with rollback and kill-switch steps", "status": "fail"},
  ]


def prepare_speed_to_lead_shadow(
  payload: Mapping[str, Any],
  retell_config: Mapping[str, Any],
  *,
  source_system: str | None = None,
  now: datetime | None = None,
  to_number_override: str | None = None,
) -> SpeedToLeadShadowResult:
  normalized_event = normalize_new_lead_payload(payload, source_system=source_system)
  compliance = evaluate_compliance_gate(normalized_event, now=now)
  call_context = build_call_context(normalized_event, compliance, now=now)
  retell_request = build_dry_run_retell_request(
    normalized_event,
    compliance,
    call_context,
    retell_config,
    to_number_override=to_number_override,
  )
  shadow_queue_row = build_shadow_queue_row(normalized_event, compliance, call_context, retell_request)
  audit_event = build_audit_event(normalized_event, compliance, retell_request, shadow_queue_row)
  return SpeedToLeadShadowResult(
    normalized_event=normalized_event,
    compliance=compliance,
    call_context=call_context,
    retell_request=retell_request,
    audit_event=audit_event,
    shadow_queue_row=shadow_queue_row,
  )


def _build_first_question(loan_goal: str) -> str:
  goal = loan_goal.strip().lower()
  if goal == "purchase":
    return "Is this a good time for a quick call about the purchase scenario you asked about?"
  if goal in {"cash_out", "cash-out", "refinance"}:
    return "Is this a good time for a quick call about what you are trying to do with that property?"
  return "Is this a good time for a quick call about the loan request you just submitted?"


def _build_opening_context_line(loan_goal: str, property_state: str, property_type: str) -> str:
  goal_text = loan_goal.replace("_", " ").strip() or "loan"
  property_bits = " ".join(bit for bit in [property_type.replace("_", " ").strip(), property_state] if bit).strip()
  if property_bits:
    return f"you requested a DSCR {goal_text} quote for a {property_bits}".strip()
  return f"you requested a DSCR {goal_text} quote".strip()


def _build_request_type_label(loan_goal: str) -> str:
  goal = loan_goal.strip().lower().replace("_", "-")
  if goal == "purchase":
    return "DSCR purchase request"
  if goal in {"cash-out", "cashout", "refinance", "refi"}:
    return "DSCR cash-out request"
  return "DSCR loan request"


def _build_source_summary(attribution: Mapping[str, Any]) -> str:
  bits = [
    _coalesce_text(attribution.get("source"), attribution.get("utm_source")),
    _coalesce_text(attribution.get("campaign"), attribution.get("utm_campaign")),
    _coalesce_text(attribution.get("adgroup"), attribution.get("utm_adgroup")),
    _coalesce_text(attribution.get("keyword"), attribution.get("search_term"), attribution.get("utm_term")),
  ]
  return " | ".join(bit for bit in bits if bit)


def _coalesce_optional_bool(*values: Any) -> bool | None:
  for value in values:
    normalized = _normalize_bool(value)
    if normalized is not None:
      return normalized
  return None


def _coalesce_bool(*values: Any) -> bool:
  return bool(_coalesce_optional_bool(*values))


def _coalesce_text(*values: Any) -> str:
  for value in values:
    if value is None:
      continue
    text = str(value).strip()
    if text:
      return text
  return ""


def _detect_dnc_flag(
  payload: Mapping[str, Any],
  contact: Mapping[str, Any],
  consent: Mapping[str, Any],
  custom_fields: Mapping[str, Any],
  tags: list[str],
) -> bool:
  explicit = _coalesce_optional_bool(
    consent.get("dnc"),
    consent.get("do_not_call"),
    payload.get("dnc"),
    payload.get("do_not_call"),
    contact.get("dnc"),
    custom_fields.get("dnc"),
    custom_fields.get("do_not_call"),
  )
  if explicit is True:
    return True
  dnc_tags = {"dnc", "dnd", "do not call", "do-not-call", "do not disturb", "stop calling"}
  return any(tag.lower() in dnc_tags for tag in tags)


def _detect_source_system(payload: Mapping[str, Any]) -> str:
  if isinstance(payload.get("contact"), Mapping):
    return "ghl_webhook"
  if payload.get("landing_session_id") or isinstance(payload.get("lead"), Mapping):
    return "custom_website_api"
  return "unknown_lead_source"


def _ensure_utc(value: datetime | None) -> datetime:
  if value is None:
    return datetime.now(tz=UTC)
  if value.tzinfo is None:
    return value.replace(tzinfo=UTC)
  return value.astimezone(UTC)


def _speed_to_lead_operating_policy(local_time: datetime) -> dict[str, Any]:
  weekday = local_time.weekday()
  minutes = local_time.hour * 60 + local_time.minute
  if weekday <= 4:
    if 8 * 60 <= minutes < 17 * 60:
      return {
        "within_window": True,
        "operating_mode": "weekday_instant_transfer",
        "transfer_allowed": True,
        "appointment_only": False,
        "transfer_hold_seconds": 15,
      }
    if 17 * 60 <= minutes < 19 * 60:
      return {
        "within_window": True,
        "operating_mode": "weekday_after_5_appointment_only",
        "transfer_allowed": False,
        "appointment_only": True,
        "transfer_hold_seconds": 15,
      }
  if weekday == 5 and 8 * 60 <= minutes < 16 * 60:
    return {
      "within_window": True,
      "operating_mode": "saturday_instant_transfer",
      "transfer_allowed": True,
      "appointment_only": False,
      "transfer_hold_seconds": 10,
    }
  return {
    "within_window": False,
    "operating_mode": "queued_until_next_speed_to_lead_window",
    "transfer_allowed": False,
    "appointment_only": False,
    "transfer_hold_seconds": 15,
  }


def _is_within_dial_window(local_time: datetime) -> bool:
  return bool(_speed_to_lead_operating_policy(local_time)["within_window"])


def _mapping(value: Any) -> dict[str, Any]:
  return dict(value) if isinstance(value, Mapping) else {}


def _next_allowed_dial_time(local_time: datetime) -> datetime:
  local_tz = local_time.tzinfo or UTC
  if _is_within_dial_window(local_time):
    return local_time
  candidate = local_time.replace(hour=8, minute=0, second=0, microsecond=0)
  if local_time >= candidate:
    candidate = candidate + timedelta(days=1)
  while candidate.weekday() == 6:
    candidate = candidate + timedelta(days=1)
  return candidate


def _normalize_attribution(
  attribution: Mapping[str, Any],
  payload: Mapping[str, Any],
  custom_fields: Mapping[str, Any],
) -> dict[str, str]:
  normalized: dict[str, str] = {}
  for field in ATTRIBUTION_FIELDS:
    normalized[field] = _coalesce_text(
      attribution.get(field),
      payload.get(field),
      custom_fields.get(field),
    )
  if not normalized["source"]:
    normalized["source"] = _coalesce_text(normalized.get("utm_source"), payload.get("source"))
  if not normalized["campaign"]:
    normalized["campaign"] = _coalesce_text(normalized.get("utm_campaign"), payload.get("campaign"))
  if not normalized["adgroup"]:
    normalized["adgroup"] = _coalesce_text(normalized.get("utm_adgroup"), payload.get("adgroup"))
  if not normalized["search_term"]:
    normalized["search_term"] = _coalesce_text(
      payload.get("search_term"),
      normalized.get("utm_term"),
      normalized.get("keyword"),
    )
  return normalized


def _normalize_bool(value: Any) -> bool | None:
  if isinstance(value, bool):
    return value
  if isinstance(value, (int, float)):
    return bool(value)
  text = str(value or "").strip().lower()
  if not text:
    return None
  if text in {"1", "true", "yes", "y", "on"}:
    return True
  if text in {"0", "false", "no", "n", "off"}:
    return False
  return None


def _normalize_custom_fields(value: Any) -> dict[str, Any]:
  if isinstance(value, Mapping):
    return {str(key).strip().lower(): item for key, item in value.items()}
  if isinstance(value, list):
    normalized: dict[str, Any] = {}
    for item in value:
      if not isinstance(item, Mapping):
        continue
      key = _coalesce_text(item.get("key"), item.get("fieldKey"), item.get("name"), item.get("id")).lower()
      if key:
        normalized[key] = item.get("value")
    return normalized
  return {}


def _normalize_tags(value: Any) -> list[str]:
  if isinstance(value, list):
    return [str(item).strip() for item in value if str(item).strip()]
  return []


def _parse_datetime(value: str) -> datetime:
  text = str(value or "").strip()
  if not text:
    return datetime.now(tz=UTC)
  if text.endswith("Z"):
    text = text[:-1] + "+00:00"
  parsed = datetime.fromisoformat(text)
  if parsed.tzinfo is None:
    return parsed.replace(tzinfo=UTC)
  return parsed.astimezone(UTC)


def _resolve_timezone(explicit_timezone: str, state: str) -> str:
  if explicit_timezone:
    return explicit_timezone
  return DEFAULT_TIMEZONE_BY_STATE.get(state.upper(), "America/Chicago")
