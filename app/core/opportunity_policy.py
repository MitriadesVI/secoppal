"""LLM-OPP-001: Opportunity intent enforcement for bidder/proponent queries.

Deterministic post-LLM policy that detects when a user wants to bid/participate
and forces process-oriented parameters regardless of what the LLM returned.
"""
from __future__ import annotations

from app.core.contract_invariants import apply_opportunity_invariant


def enforce_bidder_opportunity_policy(user_query: str, params: dict) -> dict:
    """Force opportunity_search + oferta_abierta when the user wants to bid.

    Called AFTER LLM merge (or on regex-only path if the heuristics missed it).
    Does NOT modify the input dict — always returns a (possibly new) dict.
    """
    cleaned, _ = apply_opportunity_invariant(user_query, params)
    return cleaned
