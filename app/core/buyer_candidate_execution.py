"""Política temporal de capacidad para conjuntos de BuyerUnits.

La estrategia de ejecución real para BuyerGroups pertenece a la Etapa 3A.  La
autoridad de este módulo es más estrecha: mientras el OR actual sólo está
acreditado hasta 15 nombres, un conjunto mayor se bloquea antes de consultar
SECOP y se describe con metadata estable.  Nunca se transforma en un subset.
"""
from __future__ import annotations

from dataclasses import dataclass


BUYER_CANDIDATE_SAFE_LIMIT = 15
BUYER_CANDIDATE_LIMIT_REASON = "candidate_set_exceeds_safe_limit"


@dataclass(frozen=True, slots=True)
class BuyerCandidateExecutionDecision:
    """Resultado puro de evaluar la capacidad del OR de compradores."""

    blocked: bool
    candidate_count: int
    safe_limit: int
    family: str
    reason: str | None = None

    def to_metadata(self) -> dict:
        return {
            "blocked": self.blocked,
            "reason": self.reason,
            "candidate_count": self.candidate_count,
            "safe_limit": self.safe_limit,
            "family": self.family,
        }


def evaluate_buyer_candidate_execution(
    candidates: list[str] | tuple[str, ...],
    *,
    family: str,
    safe_limit: int = BUYER_CANDIDATE_SAFE_LIMIT,
) -> BuyerCandidateExecutionDecision:
    """Bloquea conjuntos que el OR actual no puede ejecutar fielmente."""
    candidate_count = len(candidates)
    blocked = candidate_count > safe_limit
    return BuyerCandidateExecutionDecision(
        blocked=blocked,
        candidate_count=candidate_count,
        safe_limit=safe_limit,
        family=family,
        reason=BUYER_CANDIDATE_LIMIT_REASON if blocked else None,
    )


def buyer_candidate_limit_message(
    decision: BuyerCandidateExecutionDecision,
    *,
    label: str,
) -> str:
    """Mensaje transparente; no presenta el bloqueo como ambigüedad del usuario."""
    if decision.family == "ICBF" and decision.candidate_count == 33:
        scope = "El ICBF nacional comprende la sede y 32 regionales."
    else:
        scope = (
            f"{label} comprende {decision.candidate_count} unidades compradoras."
        )
    return (
        f"{scope} Por ahora no puedo consultar todo ese conjunto de forma "
        "completa sin arriesgar resultados parciales. Puedes pedirme una "
        "regional específica o una macro-región."
    )
