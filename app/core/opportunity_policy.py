"""LLM-OPP-001: Opportunity intent enforcement for bidder/proponent queries.

Deterministic post-LLM policy that detects when a user wants to bid/participate
and forces process-oriented parameters regardless of what the LLM returned.
"""
from __future__ import annotations

import unicodedata


def _normalize(text: str) -> str:
    """Strip accents and lowercase."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower()


_BIDDER_PHRASES = [
    "quiero presentarme",
    "para presentarme",
    "para poder presentarme",
    "puedo ofertar",
    "quiero ofertar",
    "quiero participar",
    "donde pueda ofertar",
    "donde pueda presentarme",
]

_CLOSED_STATES = {"cerrado", "cerrados", "terminado", "terminados", "liquidado", "liquidados"}


def enforce_bidder_opportunity_policy(user_query: str, params: dict) -> dict:
    """Force opportunity_search + oferta_abierta when the user wants to bid.

    Called AFTER LLM merge (or on regex-only path if the heuristics missed it).
    Does NOT modify the input dict — always returns a (possibly new) dict.
    """
    normalized = _normalize(user_query)
    if not any(phrase in normalized for phrase in _BIDDER_PHRASES):
        return params

    cleaned = dict(params)
    cleaned["dataset"] = "procesos"
    cleaned["intent_type"] = "opportunity_search"
    cleaned["estado_family"] = "oferta_abierta"

    # Remove closed/terminated states that may have come from LLM or context
    if cleaned.get("estado") and _normalize(str(cleaned["estado"])) in _CLOSED_STATES:
        cleaned.pop("estado", None)

    if isinstance(cleaned.get("estado_contrato"), list):
        cleaned["estado_contrato"] = [
            s for s in cleaned["estado_contrato"]
            if _normalize(str(s)) not in _CLOSED_STATES
        ]
        if not cleaned["estado_contrato"]:
            cleaned.pop("estado_contrato", None)
            cleaned.pop("estado_field", None)

    return cleaned
