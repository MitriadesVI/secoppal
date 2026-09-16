"""Where the flag, the credential and the provider meet the pure contract.

Kept apart from both sides: the contract never imports a network client, and
the provider never reads configuration. Flag OFF alone yields ``None`` and
preserves the V1 contract. Flag ON always yields a veto: missing configuration,
credential, or provider becomes an explicit ``UNAVAILABLE`` verdict.
"""
from __future__ import annotations

from app.config import get_settings, supplier_polarity_sidecar_enabled
from app.core.capability_binding.supplier_semantics_provider import classify
from app.core.capability_binding.supplier_semantics import (
    MentionRef,
    PolarityVeto,
    QuantityRef,
    VetoOutcome,
    VetoVerdict,
    adjudicate,
)


def configured_polarity_veto() -> PolarityVeto | None:
    """The wired veto, or ``None`` only when the sidecar is explicitly off."""
    if not supplier_polarity_sidecar_enabled():
        return None
    try:
        settings = get_settings()
        api_key = settings.deepseek_api_key
        model = settings.deepseek_model
        timeout = settings.secoppal_a2_supplier_polarity_timeout_seconds
    except Exception:  # pragma: no cover - configuración rota
        return _unavailable("CONFIGURATION_UNAVAILABLE")
    if not api_key:
        return _unavailable("MISSING_CREDENTIAL")

    def veto(
        user_query: str,
        refs: tuple[MentionRef, ...],
        quantities: tuple[QuantityRef, ...] = (),
    ) -> VetoVerdict:
        if not refs:
            return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "NO_REFS")
        payload = classify(
            user_query, refs, quantities,
            api_key=api_key, model=model, timeout_seconds=timeout,
        )
        if payload is None:
            return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "PROVIDER_UNAVAILABLE")
        return adjudicate(refs, payload, quantities)

    return veto


def _unavailable(detail: str) -> PolarityVeto:
    def veto(
        user_query: str,
        refs: tuple[MentionRef, ...],
        quantities: tuple[QuantityRef, ...] = (),
    ) -> VetoVerdict:
        return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", detail)

    return veto
