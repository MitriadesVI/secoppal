"""Modelo bidimensional de resultado de un turno — Etapa 4.

`needs_clarification` mezclaba ambigüedad real con "0 resultados tras refinamiento"
(las falsas clarificaciones del dogfood). Aquí se separa en DOS ejes:

- ``Outcome``        — qué pasó a alto nivel (success/partial/empty/…).
- ``OutcomeReason``  — por qué (results_found/zero_after_refinement/count_timeout/…).

Crucial: un fetch parcial (filas obtenidas pero el count agotó tiempo) NO es ni
``SUCCESS`` pleno ni ``EMPTY``; es ``PARTIAL`` con ``COUNT_TIMEOUT``. Y un fetch que
falló por timeout NO es ``EMPTY`` (ausencia confirmada): es ``ERROR``/``FETCH_TIMEOUT``,
porque no sabemos si había resultados.

Función pura, sin red — testeable en aislamiento.
"""
from __future__ import annotations


class Outcome:
    SUCCESS = "success"           # filas + total exacto
    PARTIAL = "partial"           # filas, pero total inexacto (count agotó tiempo)
    EMPTY = "empty"               # 0 filas confirmadas por la fuente
    CLARIFICATION = "clarification"  # el sistema necesita más información del usuario
    ERROR = "error"               # no se pudo responder con datos


class OutcomeReason:
    RESULTS_FOUND = "results_found"
    ZERO_RESULTS = "zero_results"
    ZERO_AFTER_REFINEMENT = "zero_results_after_refinement"
    COUNT_TIMEOUT = "count_timeout"
    FETCH_TIMEOUT = "fetch_timeout"
    SOURCE_UNAVAILABLE = "source_unavailable"
    QUERY_ERROR = "query_error"
    AMBIGUOUS_REFERENCE = "ambiguous_reference"
    CLARIFICATION_REQUESTED = "clarification_requested"


def classify_outcome(
    *,
    clarification_requested: bool = False,
    ambiguous_reference: bool = False,
    source_unavailable: bool = False,
    query_failed: bool = False,
    query_timed_out: bool = False,
    count_approximate: bool = False,
    rows_returned: int = 0,
    previous_had_results: bool = False,
) -> tuple[str, str]:
    """Clasifica un turno en (outcome, outcome_reason). Pura y determinista.

    Orden de decisión (el primero que aplica gana):
    1. clarificación genuina pedida por el sistema (P15/query_plan).
    2. referencia ambigua (lookup directo con >1 entidad).
    3. fuente no disponible (todas las consultas fallaron).
    4. el fetch falló (timeout u otro error) → no hay certeza de ausencia.
    5. hay filas → success, o partial si el conteo quedó aproximado.
    6. 0 filas → vacío confirmado; tras refinamiento si el turno previo sí tenía.
    """
    if clarification_requested:
        return Outcome.CLARIFICATION, OutcomeReason.CLARIFICATION_REQUESTED
    if ambiguous_reference:
        return Outcome.CLARIFICATION, OutcomeReason.AMBIGUOUS_REFERENCE
    if source_unavailable:
        return Outcome.ERROR, OutcomeReason.SOURCE_UNAVAILABLE
    if query_failed:
        reason = OutcomeReason.FETCH_TIMEOUT if query_timed_out else OutcomeReason.QUERY_ERROR
        return Outcome.ERROR, reason
    if rows_returned > 0:
        if count_approximate:
            return Outcome.PARTIAL, OutcomeReason.COUNT_TIMEOUT
        return Outcome.SUCCESS, OutcomeReason.RESULTS_FOUND
    # rows_returned == 0
    if previous_had_results:
        return Outcome.EMPTY, OutcomeReason.ZERO_AFTER_REFINEMENT
    return Outcome.EMPTY, OutcomeReason.ZERO_RESULTS


def is_false_clarification(outcome: str, outcome_reason: str) -> bool:
    """True si esto se reportaría como 'clarificación' pero en realidad es un vacío.

    Para el evaluador: un EMPTY/ZERO_AFTER_REFINEMENT que el sistema viejo habría
    mostrado como menú '¿cuál camino?' no debe contarse como clarificación genuina.
    """
    return outcome == Outcome.EMPTY and outcome_reason == OutcomeReason.ZERO_AFTER_REFINEMENT
