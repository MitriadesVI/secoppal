"""Etapa 4 — modelo bidimensional outcome + outcome_reason."""
from __future__ import annotations

import pytest

from app.core.outcome import Outcome, OutcomeReason, classify_outcome, is_false_clarification


# Matriz explícita: (kwargs) → (outcome, reason). Cubre todas las combinaciones
# que el asesor pidió blindar contra el "timeout-como-cero" reapareciendo.
_MATRIX = [
    # fetch ok + filas + count exacto
    (dict(rows_returned=25, count_approximate=False), (Outcome.SUCCESS, OutcomeReason.RESULTS_FOUND)),
    # fetch ok + filas + count timeout
    (dict(rows_returned=25, count_approximate=True), (Outcome.PARTIAL, OutcomeReason.COUNT_TIMEOUT)),
    # fetch timeout (sin filas) — NUNCA empty
    (dict(query_failed=True, query_timed_out=True), (Outcome.ERROR, OutcomeReason.FETCH_TIMEOUT)),
    # fetch error no-timeout
    (dict(query_failed=True, query_timed_out=False), (Outcome.ERROR, OutcomeReason.QUERY_ERROR)),
    # fetch ok + 0 filas, búsqueda fresca
    (dict(rows_returned=0, previous_had_results=False), (Outcome.EMPTY, OutcomeReason.ZERO_RESULTS)),
    # fetch ok + 0 filas, tras refinamiento exitoso
    (dict(rows_returned=0, previous_had_results=True), (Outcome.EMPTY, OutcomeReason.ZERO_AFTER_REFINEMENT)),
    # clarificación genuina gana sobre 0 filas
    (dict(clarification_requested=True, rows_returned=0), (Outcome.CLARIFICATION, OutcomeReason.CLARIFICATION_REQUESTED)),
    # fuente caída gana sobre 0 filas
    (dict(source_unavailable=True, rows_returned=0), (Outcome.ERROR, OutcomeReason.SOURCE_UNAVAILABLE)),
    # referencia ambigua
    (dict(ambiguous_reference=True), (Outcome.CLARIFICATION, OutcomeReason.AMBIGUOUS_REFERENCE)),
    # borde: fetch timeout aunque previous tuviera resultados → ERROR, no EMPTY
    (dict(query_failed=True, query_timed_out=True, previous_had_results=True),
     (Outcome.ERROR, OutcomeReason.FETCH_TIMEOUT)),
]


@pytest.mark.parametrize("kwargs,expected", _MATRIX)
def test_outcome_matrix(kwargs, expected):
    assert classify_outcome(**kwargs) == expected


def test_results_with_exact_count_is_success():
    assert classify_outcome(rows_returned=25, count_approximate=False) == (
        Outcome.SUCCESS, OutcomeReason.RESULTS_FOUND,
    )


def test_rows_with_count_timeout_is_partial_not_success():
    # El caso real: fetch trajo 25 filas pero el count agotó 60s.
    assert classify_outcome(rows_returned=25, count_approximate=True) == (
        Outcome.PARTIAL, OutcomeReason.COUNT_TIMEOUT,
    )


def test_fetch_timeout_is_error_not_empty():
    # No sabemos si había resultados — NUNCA EMPTY.
    assert classify_outcome(query_failed=True, query_timed_out=True, rows_returned=0) == (
        Outcome.ERROR, OutcomeReason.FETCH_TIMEOUT,
    )


def test_fetch_error_non_timeout_is_query_error():
    assert classify_outcome(query_failed=True, query_timed_out=False) == (
        Outcome.ERROR, OutcomeReason.QUERY_ERROR,
    )


def test_zero_results_fresh_search():
    assert classify_outcome(rows_returned=0, previous_had_results=False) == (
        Outcome.EMPTY, OutcomeReason.ZERO_RESULTS,
    )


def test_zero_after_refinement_distinguished():
    # El corazón de la etapa: el turno previo SÍ tenía resultados.
    assert classify_outcome(rows_returned=0, previous_had_results=True) == (
        Outcome.EMPTY, OutcomeReason.ZERO_AFTER_REFINEMENT,
    )


def test_genuine_clarification_wins():
    assert classify_outcome(clarification_requested=True, rows_returned=0) == (
        Outcome.CLARIFICATION, OutcomeReason.CLARIFICATION_REQUESTED,
    )


def test_ambiguous_reference():
    assert classify_outcome(ambiguous_reference=True) == (
        Outcome.CLARIFICATION, OutcomeReason.AMBIGUOUS_REFERENCE,
    )


def test_source_unavailable_beats_zero():
    assert classify_outcome(source_unavailable=True, rows_returned=0) == (
        Outcome.ERROR, OutcomeReason.SOURCE_UNAVAILABLE,
    )


def test_false_clarification_detector():
    out, reason = classify_outcome(rows_returned=0, previous_had_results=True)
    assert is_false_clarification(out, reason) is True
    out2, reason2 = classify_outcome(clarification_requested=True)
    assert is_false_clarification(out2, reason2) is False
