"""Small strict proposal; host-owned grounding and observation receipts."""
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .catalog import ACTION_ASPECTS


class ProposedJob(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    action: str
    aspect: str
    evidence_quote: str = Field(min_length=1)
    occurrence: int = Field(ge=1)

    @model_validator(mode="after")
    def catalog_pair(self):
        if self.aspect not in ACTION_ASPECTS.get(self.action, ()):
            raise ValueError("INVALID_CATALOG_PAIR")
        return self


class Proposal(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    jobs: list[ProposedJob] = Field(max_length=3)


@dataclass(frozen=True)
class GroundedJob:
    action: str
    aspect: str
    start: int
    end: int

    def metadata(self) -> dict:
        return {"action": self.action, "aspect": self.aspect,
                "evidence_interval": [self.start, self.end]}


@dataclass(frozen=True)
class Observation:
    mode: str
    status: str
    jobs: tuple[GroundedJob, ...] = ()
    provider_calls: int = 0
    latency_ms: float = 0.0
    fallback_reason: str = ""

    @property
    def terminal(self) -> bool:
        return self.status == "MULTI_JOB_BLOCKED"

    def metadata(self) -> dict:
        return {
            "natural_alpha_mode": self.mode,
            "natural_alpha_eligible": True,
            "natural_alpha_status": self.status,
            "natural_alpha_job_count": len(self.jobs),
            "natural_alpha_jobs": [job.metadata() for job in self.jobs],
            "natural_alpha_provider_calls": self.provider_calls,
            "natural_alpha_latency_ms": self.latency_ms,
            "natural_alpha_fallback_reason": self.fallback_reason,
        }
