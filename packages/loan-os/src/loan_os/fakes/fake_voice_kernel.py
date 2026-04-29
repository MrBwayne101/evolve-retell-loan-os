from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter
from typing import Any

from loan_os.schemas import validate_payload


@dataclass
class FakeVoiceKernel:
  scripted_transcript: list[str]
  voice_session_id: str = "voice-fake-001"
  event_log: list[dict[str, Any]] = field(default_factory=list)
  _sequence: int = 0
  _clock_ms: int = 0

  def _next_event(self, event_type: str, **payload: Any) -> dict[str, Any]:
    self._sequence += 1
    self._clock_ms += 120
    event = {
      "sequence": self._sequence,
      "timestamp_ms": self._clock_ms,
      "event_type": event_type,
      **payload,
    }
    self.event_log.append(event)
    return event

  def open_call(self, *, caller_id: str = "caller-fake-001") -> dict[str, Any]:
    started = perf_counter()
    event = self._next_event("call_opened", caller_id=caller_id, voice_session_id=self.voice_session_id)
    response = {
      "ok": True,
      "request_id": "voice-transport-open",
      "operation": "open_call",
      "voice_session_id": self.voice_session_id,
      "event": event,
      "timing_ms": round((perf_counter() - started) * 1000, 3),
    }
    validate_payload("voice_transport_output", response)
    return response

  def play_card(self, *, card_id: str, text: str) -> dict[str, Any]:
    started = perf_counter()
    event = self._next_event(
      "play_card",
      voice_session_id=self.voice_session_id,
      card_id=card_id,
      text=text,
    )
    response = {
      "ok": True,
      "request_id": f"voice-transport-play-{card_id}",
      "operation": "play_card",
      "voice_session_id": self.voice_session_id,
      "event": event,
      "timing_ms": round((perf_counter() - started) * 1000, 3),
    }
    validate_payload("voice_transport_output", response)
    return response

  def speak_text(self, *, text: str) -> dict[str, Any]:
    started = perf_counter()
    event = self._next_event(
      "speak_text",
      voice_session_id=self.voice_session_id,
      text=text,
    )
    response = {
      "ok": True,
      "request_id": "voice-transport-speak",
      "operation": "speak_text",
      "voice_session_id": self.voice_session_id,
      "event": event,
      "timing_ms": round((perf_counter() - started) * 1000, 3),
    }
    validate_payload("voice_transport_output", response)
    return response

  def next_caller_transcript(self) -> dict[str, Any] | None:
    if not self.scripted_transcript:
      return None
    text = self.scripted_transcript.pop(0)
    return self._next_event(
      "caller_transcript",
      voice_session_id=self.voice_session_id,
      transcript=text,
    )
