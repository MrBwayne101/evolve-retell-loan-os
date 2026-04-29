"""Shadow-mode call center operating system builders."""

from loan_os.call_center.builder import OvernightArtifactsResult, build_overnight_artifacts
from loan_os.call_center.continuation import ContinuationArtifactsResult, build_continuation_artifacts
from loan_os.call_center.speed_to_lead import SpeedToLeadShadowResult, prepare_speed_to_lead_shadow

__all__ = [
  "ContinuationArtifactsResult",
  "OvernightArtifactsResult",
  "SpeedToLeadShadowResult",
  "build_continuation_artifacts",
  "build_overnight_artifacts",
  "prepare_speed_to_lead_shadow",
]
