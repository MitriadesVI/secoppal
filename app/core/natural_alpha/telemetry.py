"""Explicit, text-minimal trace fields and additive SQLite column definitions."""
import json
import math

from .catalog import ACTION_ASPECTS

TRACE_COLUMNS = {
    "natural_alpha_mode": "TEXT",
    "natural_alpha_eligible": "INTEGER",
    "natural_alpha_status": "TEXT",
    "natural_alpha_job_count": "INTEGER",
    "natural_alpha_jobs": "TEXT",
    "natural_alpha_provider_calls": "INTEGER",
    "natural_alpha_latency_ms": "REAL",
    "natural_alpha_fallback_reason": "TEXT",
    "natural_alpha_legacy_route_reason": "TEXT",
}
STATUSES = {"SKIPPED", "MATCHED", "MULTI_JOB_BLOCKED", "INVALID", "PROVIDER_ERROR"}
FALLBACKS = {
    "", "EMPTY_PROPOSAL", "INVALID_PROPOSAL", "PROVIDER_FAILURE", "PROVIDER_TIMEOUT",
    "MISSING_API_KEY", "MISSING_OUTPUT_TEXT", "PROVIDER_CONTRACT_INCOMPATIBLE",
}


def trace_metadata(result: dict) -> dict:
    if (result.get("natural_alpha_mode") not in ("shadow", "alpha")
            or result.get("natural_alpha_eligible") is not True
            or result.get("natural_alpha_status") not in STATUSES):
        return {}
    jobs = []
    for job in (result.get("natural_alpha_jobs") or [])[:3]:
        if type(job) is not dict:
            continue
        action, aspect, interval = job.get("action"), job.get("aspect"), job.get("evidence_interval")
        if type(action) is not str or aspect not in ACTION_ASPECTS.get(action, ()):
            continue
        if (type(interval) is not list or len(interval) != 2
                or any(type(value) is not int for value in interval)
                or not 0 <= interval[0] < interval[1]):
            continue
        jobs.append({"action": action, "aspect": aspect, "evidence_interval": list(interval)})
    latency = result.get("natural_alpha_latency_ms")
    if type(latency) not in (float, int) or not math.isfinite(latency) or latency < 0:
        latency = 0.0
    reason = result.get("natural_alpha_fallback_reason", "")
    legacy = result.get("natural_alpha_legacy_route_reason", "")
    return {
        "natural_alpha_mode": result["natural_alpha_mode"],
        "natural_alpha_eligible": True,
        "natural_alpha_status": result["natural_alpha_status"],
        "natural_alpha_job_count": len(jobs),
        "natural_alpha_jobs": jobs,
        "natural_alpha_provider_calls": int(result.get("natural_alpha_provider_calls") == 1),
        "natural_alpha_latency_ms": latency,
        "natural_alpha_fallback_reason": reason if reason in FALLBACKS else "PROVIDER_FAILURE",
        "natural_alpha_legacy_route_reason": legacy if type(legacy) is str else "",
    }


def sqlite_values(trace: dict) -> dict:
    metadata = trace_metadata(trace)
    return {
        name: (json.dumps(metadata[name], ensure_ascii=False)
               if name == "natural_alpha_jobs" and name in metadata else metadata.get(name))
        for name in TRACE_COLUMNS
    }
