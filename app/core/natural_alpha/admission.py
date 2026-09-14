"""Eligibility and observation only. Never materializes execution parameters."""
import time
import uuid

from .grounding import validate_and_ground
from .models import Observation
from .prompt import provider_request
from .provider import DeepSeekResponsesProvider, Provider


def mode(settings) -> str:
    value = settings.secoppal_natural_alpha_mode
    return value if value in ("off", "shadow", "alpha") else "off"


def eligible(settings, channel: str, identity: dict | None) -> bool:
    if settings.secoppal_auth_enabled is not True or channel != "streamlit":
        return False
    if type(identity) is not dict or identity.get("is_anonymous") is not False:
        return False
    email = identity.get("email")
    allowlist = settings.secoppal_natural_alpha_email
    return (type(email) is str and type(allowlist) is str and bool(allowlist.strip())
            and email.strip().lower() == allowlist.strip().lower())


def observe(settings, raw: str, channel: str, identity: dict | None,
            provider: Provider | None = None) -> Observation | None:
    selected_mode = mode(settings)
    if selected_mode == "off" or not eligible(settings, channel, identity):
        return None
    started = time.perf_counter()
    provider = provider if provider is not None else DeepSeekResponsesProvider(
        api_key=settings.deepseek_api_key,
        timeout_seconds=settings.secoppal_natural_alpha_timeout_seconds,
    )
    try:
        response = provider.observe(uuid.uuid4().hex, provider_request(raw))
    except Exception:
        # Injected provider failures must also preserve legacy; never log its text.
        return Observation(selected_mode, "PROVIDER_ERROR", provider_calls=1,
                           latency_ms=_elapsed(started), fallback_reason="PROVIDER_FAILURE")
    latency = _elapsed(started)
    if response.error:
        return Observation(selected_mode, "PROVIDER_ERROR", provider_calls=response.provider_calls,
                           latency_ms=latency, fallback_reason=response.error)
    try:
        jobs = validate_and_ground(raw, response.payload)
    except (ValueError, TypeError):
        return Observation(selected_mode, "INVALID", provider_calls=response.provider_calls,
                           latency_ms=latency, fallback_reason="INVALID_PROPOSAL")
    status = "MATCHED" if jobs else "SKIPPED"
    if selected_mode == "alpha" and len(jobs) >= 2:
        status = "MULTI_JOB_BLOCKED"
    return Observation(selected_mode, status, jobs, response.provider_calls, latency,
                       "EMPTY_PROPOSAL" if not jobs else "")


def _elapsed(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 3)
