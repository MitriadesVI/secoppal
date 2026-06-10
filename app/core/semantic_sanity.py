"""semantic_sanity.py — passive pre-execution semantic risk flags.

Pure, side-effect-free judgments about whether parsed params are likely to
return 0/garbage BEFORE spending a SECOP round-trip.

PASSIVE / fail-open by contract: these functions NEVER mutate params and NEVER
block execution. They only emit ``risk_flag`` strings (the same channel as
P10–P13 in query_plan) for downstream consumption — telemetry today, targeted
self-correction tomorrow. A false positive therefore costs nothing: the query
still runs exactly as it would have.

Anchored to existing business vocabulary (``COMMON_TOKENS``) and the
SoQLBuilder demotion rule, not to newly invented heuristics.
"""

from __future__ import annotations

try:
    from app.core.intent_vocabulary import COMMON_TOKENS
except Exception:  # pragma: no cover - defensive, mirrors soql_builder import guard
    COMMON_TOKENS: frozenset[str] = frozenset()

# SoQLBuilder._should_demote_objeto degrades an objeto AND→OR only at len>=4.
# So 2..3 string tokens stay ANDed. An AND of >=2 *rare* tokens (not in
# COMMON_TOKENS) rarely co-occur literally in a single SECOP objeto string,
# so the conjunction is the class that silently returns 0 (root cause #3,
# e.g. "construir parques"). Keep this threshold in sync with the builder.
_AND_DEMOTION_THRESHOLD = 4
_MIN_RARE_TERMS = 2


def compute_sanity_flags(params: dict) -> list[str]:
    """Return passive semantic risk flags for ``params``. Never mutates input."""
    flags: list[str] = []
    if _is_fragile_objeto_and(params.get("objeto") or []):
        flags.append("fragile_objeto_and")
    return flags


def _is_fragile_objeto_and(objeto) -> bool:
    """True when objeto is an AND of >=2 rare tokens the builder won't demote."""
    if not isinstance(objeto, list):
        return False
    if not (2 <= len(objeto) < _AND_DEMOTION_THRESHOLD):
        return False
    if not all(isinstance(term, str) for term in objeto):
        return False
    rare = [term for term in objeto if term.lower() not in COMMON_TOKENS]
    return len(rare) >= _MIN_RARE_TERMS
