"""The credential accredits only the JOB (plan V2 5), never an executable dimension.

Source is ``NATURAL_ALPHA``. An interval that no longer corresponds to the same
raw text makes the credential invalid: it is refused, never clipped into range.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.core.natural_alpha.models import GroundedJob

NATURAL_ALPHA = "NATURAL_ALPHA"


@dataclass(frozen=True, slots=True)
class CapabilityCredential:
    """What the model witnessed: which job, over which span of which turn."""

    capability: str
    action: str
    aspect: str
    evidence_interval: tuple[int, int]
    source: str = NATURAL_ALPHA

    def __post_init__(self) -> None:
        for name in ("capability", "action", "aspect"):
            value = getattr(self, name)
            if type(value) is not str or not value.strip():
                raise ValueError(f"INVALID_CREDENTIAL_{name.upper()}")
        if self.source != NATURAL_ALPHA:
            raise ValueError("INVALID_CREDENTIAL_SOURCE")
        interval = self.evidence_interval
        if type(interval) is not tuple or len(interval) != 2:
            raise ValueError("INVALID_EVIDENCE_INTERVAL")
        start, end = interval
        if type(start) is not int or type(end) is not int or start < 0 or end <= start:
            raise ValueError("INVALID_EVIDENCE_INTERVAL")

    def covers(self, raw: str) -> bool:
        """True only while this interval still corresponds to this raw text."""
        return type(raw) is str and self.evidence_interval[1] <= len(raw)


def from_grounded_job(
    raw: str, job: GroundedJob, *, capability: str,
) -> CapabilityCredential:
    """Build a credential from a job already grounded against ``raw``."""
    if type(raw) is not str or not raw:
        raise ValueError("INVALID_RAW_TEXT")
    start, end = job.start, job.end
    if (
        type(start) is not int
        or type(end) is not int
        or start < 0
        or end <= start
        or end > len(raw)
    ):
        raise ValueError("UNGROUNDED_CREDENTIAL_INTERVAL")
    return CapabilityCredential(capability, job.action, job.aspect, (start, end))
