"""The credential accredits only the JOB (plan V2 5), never an executable dimension.

Source is ``NATURAL_ALPHA``. The credential is bound to the very raw text its job
was grounded against, through a stable sha256 fingerprint: a text that is not
that same raw makes the credential invalid, and it is refused — never clipped,
never repaired.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from app.core.natural_alpha.models import GroundedJob

NATURAL_ALPHA = "NATURAL_ALPHA"
_HEX = frozenset("0123456789abcdef")


def raw_fingerprint(raw: str) -> str:
    """Stable sha256 of the raw text a credential is bound to."""
    if type(raw) is not str or not raw:
        raise ValueError("INVALID_RAW_TEXT")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class CapabilityCredential:
    """What the model witnessed: which job, over which span, of which exact turn."""

    capability: str
    action: str
    aspect: str
    evidence_interval: tuple[int, int]
    raw_fingerprint: str
    source: str = NATURAL_ALPHA

    def __post_init__(self) -> None:
        for name in ("capability", "action", "aspect"):
            value = getattr(self, name)
            if type(value) is not str or not value.strip():
                raise ValueError(f"INVALID_CREDENTIAL_{name.upper()}")
        if self.source != NATURAL_ALPHA:
            raise ValueError("INVALID_CREDENTIAL_SOURCE")
        fingerprint = self.raw_fingerprint
        if (type(fingerprint) is not str or len(fingerprint) != 64
                or not set(fingerprint) <= _HEX):
            raise ValueError("INVALID_CREDENTIAL_FINGERPRINT")
        interval = self.evidence_interval
        if type(interval) is not tuple or len(interval) != 2:
            raise ValueError("INVALID_EVIDENCE_INTERVAL")
        start, end = interval
        if type(start) is not int or type(end) is not int or start < 0 or end <= start:
            raise ValueError("INVALID_EVIDENCE_INTERVAL")

    def corresponds_to(self, raw: str) -> bool:
        """True only for the very raw text this credential was grounded against.

        A different text of the same length, a longer text that merely contains
        the interval, and a stale credential from another turn all answer False.
        The interval is re-checked as well, so a hand-built credential cannot
        claim a span longer than the raw it names.
        """
        if type(raw) is not str or not raw:
            return False
        if hashlib.sha256(raw.encode("utf-8")).hexdigest() != self.raw_fingerprint:
            return False
        return self.evidence_interval[1] <= len(raw)


def from_grounded_job(
    raw: str, job: GroundedJob, *, capability: str,
) -> CapabilityCredential:
    """Build a credential from a job already grounded against ``raw``."""
    fingerprint = raw_fingerprint(raw)
    start, end = job.start, job.end
    if (
        type(start) is not int
        or type(end) is not int
        or start < 0
        or end <= start
        or end > len(raw)
    ):
        raise ValueError("UNGROUNDED_CREDENTIAL_INTERVAL")
    return CapabilityCredential(
        capability, job.action, job.aspect, (start, end), fingerprint,
    )
