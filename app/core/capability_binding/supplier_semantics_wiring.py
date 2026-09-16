"""Where the flag, the credential and the provider meet the pure contract.

Kept apart from both sides: the contract never imports a network client, and
the provider never reads configuration. Flag OFF alone yields ``None`` and
preserves the V1 contract. Flag ON always yields a veto: missing configuration,
credential, or provider becomes an explicit ``UNAVAILABLE`` verdict.
"""
from __future__ import annotations

from app.config import (
    SupplierPolaritySidecarState,
    supplier_polarity_sidecar_config,
)
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
    configuration = supplier_polarity_sidecar_config()
    if configuration.state is SupplierPolaritySidecarState.DISABLED:
        return None
    if configuration.state is SupplierPolaritySidecarState.ENABLED_BUT_UNAVAILABLE:
        return _unavailable(configuration.detail)

    api_key = configuration.api_key
    model = configuration.model
    timeout = configuration.timeout_seconds
    if not api_key or not model or timeout is None:  # pragma: no cover - config invariant
        return _unavailable("CONFIGURATION_INCOMPLETE")

    def veto(
        user_query: str,
        refs: tuple[MentionRef, ...],
        quantities: tuple[QuantityRef, ...] = (),
    ) -> VetoVerdict:
        if not refs:
            return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "NO_REFS")
        try:
            payload = classify(
                user_query, refs, quantities,
                api_key=api_key, model=model, timeout_seconds=timeout,
            )
        except Exception:  # pragma: no cover - provider normally normalizes this
            payload = None
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
