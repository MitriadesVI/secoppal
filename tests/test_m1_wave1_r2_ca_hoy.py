"""M1 wave 1 R2 (ZEUS P2-1): in Contract Activity «hoy» is the currentness of a reported state.

R1 made the legacy reader compare its period with the temporal authority's, and the
authority resolves the word «hoy» as the day «hoy»: «¿Cuántos contratos … figuran hoy
suspendidos?» started to ask for a period that nobody asked. Only that word, only where
it plainly is the currentness of a state, is set aside; every bound on the signature
date («antes de hoy», «hasta hoy», «desde 2024 hasta hoy») and the signature day itself
(«firmados hoy») stay with the authority exactly as in R1.

Also pins two R1 mutants that survived: the injected clock (M17) and the money mask (M15).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.currentness import currentness_hoy, governs_reported_state
from app.core.analytics.contract_activity.models import AdmissionStatus, Operation
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter

TODAY = date(2026, 10, 1)
# A clock that is NOT the real one: a reader that falls back to the machine's date would
# resolve «hoy» as another day and disagree with the parse.
INJECTED = date(2031, 5, 15)
B = "la Alcaldía de Galapa"


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _admit(router: QueryRouter, raw: str, *, today: date = TODAY, tamper: dict | None = None):
    parsed = router.parse(raw, today=today)
    params = {**parsed.params, **(tamper or {})}
    return admit_contract_activity(
        raw, params, today=today, semantic_evidence=parsed.semantic_evidence.to_state(),
    )


# (query, (date_from, date_to), reported states).  The base executed every one of them.
PARITY = [
    (f"¿Cuántos contratos de {B} figuran hoy suspendidos?", (None, None), ("Suspendido",)),
    (f"¿Cuántos contratos de {B} están hoy suspendidos?", (None, None), ("Suspendido",)),
    (f"¿Cuántos contratos de {B} hoy figuran suspendidos?", (None, None), ("Suspendido",)),
    (f"¿Cuántos contratos de {B} figuran hoy en ejecución?", (None, None),
     ("En ejecución", "Modificado", "Prorrogado")),
    (f"¿Cuántos contratos firmados en 2025 por {B} figuran hoy suspendidos?",
     ("2025-01-01", "2025-12-31"), ("Suspendido",)),
    (f"¿Cuántos contratos firmados en marzo de 2025 por {B} figuran hoy suspendidos?",
     ("2025-03-01", "2025-03-31"), ("Suspendido",)),
    (f"¿Cuántos contratos de {B} figuran actualmente suspendidos?", (None, None), ("Suspendido",)),
]


@pytest.mark.parametrize("raw,period,states", PARITY)
def test_hoy_as_currentness_is_executed_as_the_base_did(router, raw, period, states):
    admission = _admit(router, raw)
    assert admission.status is AdmissionStatus.READY, (raw, admission.reason)
    request = admission.request
    assert (request.date_from, request.date_to) == period, raw
    assert request.reported_states == states, raw


def test_the_breakdown_by_state_a_hoy_is_executed_as_the_base_did(router):
    raw = f"Desglosa por estado reportado los contratos de {B} a hoy"
    admission = _admit(router, raw)
    assert admission.status is AdmissionStatus.READY, admission.reason
    assert admission.request.operation is Operation.BY_REPORTED_STATE
    assert (admission.request.date_from, admission.request.date_to) == (None, None)


# (query, the reason R1 gave and R2 keeps).  None of these is currentness.
STILL_ASKED = [
    (f"¿Cuántos contratos de {B} figuran antes de hoy suspendidos?", "SIGNATURE_PERIOD_NOT_REPRESENTED"),
    (f"¿Cuántos contratos de {B} figuran hasta hoy suspendidos?", "UNSUPPORTED_OR_PARTIAL_SIGNATURE_PERIOD"),
    (f"¿Cuántos contratos de {B} figuran desde 2024 hasta hoy suspendidos?",
     "UNSUPPORTED_OR_PARTIAL_SIGNATURE_PERIOD"),
    (f"Desglosa por estado reportado los contratos de {B} antes de hoy", "SIGNATURE_PERIOD_NOT_REPRESENTED"),
    (f"Desglosa por estado reportado los contratos de {B} hasta hoy",
     "UNSUPPORTED_OR_PARTIAL_SIGNATURE_PERIOD"),
    (f"Desglosa por estado reportado los contratos de {B} desde 2024 hasta hoy",
     "UNSUPPORTED_OR_PARTIAL_SIGNATURE_PERIOD"),
    (f"Desglosa por estado reportado los contratos de {B} de 2024 a hoy", "SIGNATURE_PERIOD_NOT_REPRESENTED"),
    (f"¿Cuántos contratos firmó {B} hoy?", "SIGNATURE_PERIOD_NOT_REPRESENTED"),
    # a verb of state without any state named, or a breakdown by something else, is not a
    # reported-state context
    (f"¿Cuántos contratos de {B} están hoy?", "SIGNATURE_PERIOD_NOT_REPRESENTED"),
    (f"Desglosa por modalidad los contratos de {B} a hoy", "SIGNATURE_PERIOD_NOT_REPRESENTED"),
    # the signature day is «hoy» even if a verb of state appears elsewhere in the turn
    (f"¿Cuántos contratos firmados hoy por {B} figuran suspendidos?", "SIGNATURE_PERIOD_NOT_REPRESENTED"),
    (f"¿Cuántos contratos firmados ayer por {B} figuran hoy suspendidos?", "SIGNATURE_PERIOD_NOT_REPRESENTED"),
    (f"¿Cuántos contratos de {B} figuran este año suspendidos?", "SIGNATURE_PERIOD_NOT_REPRESENTED"),
    (f"¿Cuántos contratos de {B} figuran hoy suspendidos en los últimos 3 meses?",
     "SIGNATURE_PERIOD_NOT_REPRESENTED"),
]


@pytest.mark.parametrize("raw,reason", STILL_ASKED)
def test_every_other_use_of_hoy_is_still_asked_exactly_as_in_r1(router, raw, reason):
    admission = _admit(router, raw)
    assert admission.status is AdmissionStatus.CLARIFY, raw
    assert admission.reason == reason, raw


@pytest.mark.parametrize("tamper", [
    {"fecha_desde": "2025-01-01", "fecha_hasta": "2025-12-31"},
    {"fecha_desde": "2026-09-01"},
    {"fecha_hasta": "2026-09-30"},
])
def test_the_one_day_period_of_hoy_is_dropped_but_no_other_period_the_parse_carries(router, tamper):
    raw = f"¿Cuántos contratos de {B} figuran hoy suspendidos?"
    admission = _admit(router, raw, tamper=tamper)
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.reason == "SIGNATURE_PERIOD_AUTHORITY_CONFLICT"


def test_the_parse_that_carries_exactly_the_day_of_hoy_is_accepted(router):
    raw = f"¿Cuántos contratos de {B} figuran hoy suspendidos?"
    parsed = router.parse(raw, today=TODAY)
    assert (parsed.params.get("fecha_desde"), parsed.params.get("fecha_hasta")) == ("2026-10-01", "2026-10-01")
    assert _admit(router, raw).status is AdmissionStatus.READY


# ── mutant M17: the reader uses the INJECTED clock, never the machine's ───────────────

@pytest.mark.parametrize("raw", [
    f"¿Cuántos contratos de {B} figuran hoy suspendidos?",
    f"¿Cuántos contratos firmados en 2025 por {B} figuran hoy suspendidos?",
    f"Desglosa por estado reportado los contratos de {B} a hoy",
])
def test_hoy_is_read_with_the_injected_clock(router, raw):
    parsed = router.parse(raw, today=INJECTED)
    assert INJECTED.isoformat() in (parsed.params.get("fecha_desde"), parsed.params.get("fecha_hasta"))
    assert _admit(router, raw, today=INJECTED).status is AdmissionStatus.READY, raw


def test_a_bound_on_a_relative_window_follows_the_injected_clock_too(router):
    # «hoy» next to a clock-dependent period: the authority must read BOTH with INJECTED.
    raw = f"¿Cuántos contratos de {B} figuran hoy suspendidos en los últimos 3 meses?"
    admission = _admit(router, raw, today=INJECTED)
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.reason == "SIGNATURE_PERIOD_NOT_REPRESENTED"


@pytest.mark.parametrize("today", [TODAY, date(2020, 1, 1), INJECTED])
def test_currentness_hoy_uses_the_clock_it_is_given(today):
    found = currentness_hoy("contratos de galapa figuran hoy suspendidos", today, grouped_by_state=False)
    assert found is not None
    assert found[1] == (today.isoformat(), today.isoformat())


# ── mutant M15: a money amount that looks like a year is not a period ─────────────────

def test_an_amount_that_looks_like_a_year_is_not_read_as_the_period(router):
    """«2025 cop»: the authority guards «2025 millones» but not «cop»; the mask does."""
    raw = f"¿Cuántos contratos firmó {B} por al menos 2025 cop en 2024?"
    admission = _admit(router, raw)
    assert admission.status is AdmissionStatus.READY, admission.reason
    assert (admission.request.date_from, admission.request.date_to) == ("2024-01-01", "2024-12-31")
    assert admission.request.money.amount_cop == 2025


def test_an_amount_in_millions_that_looks_like_a_year_is_not_read_as_the_period(router):
    raw = f"¿Cuántos contratos de más de 2025 millones firmó {B} en 2024?"
    admission = _admit(router, raw)
    assert admission.status is AdmissionStatus.READY, admission.reason
    assert (admission.request.date_from, admission.request.date_to) == ("2024-01-01", "2024-12-31")


# ── the small module, on its own ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("content,grouped,expected", [
    ("contratos de galapa suspendidos", False, True),
    ("contratos de galapa", False, False),
    ("contratos de galapa", True, True),
    ("contratos en ejecucion de galapa", False, True),
])
def test_governs_reported_state(content, grouped, expected):
    assert governs_reported_state(content, grouped_by_state=grouped) is expected


@pytest.mark.parametrize("text,grouped,span_text", [
    ("contratos de galapa figuran hoy suspendidos", False, "hoy"),
    ("contratos de galapa estan hoy suspendidos", False, "hoy"),
    ("contratos de galapa hoy figuran suspendidos", False, "hoy"),
    ("contratos de galapa se encuentran hoy suspendidos", False, "hoy"),
    ("contratos firmados en 2025 de galapa figuran hoy suspendidos", False, "hoy"),
    ("desglosa por estado los contratos de galapa a hoy", True, "hoy"),
])
def test_currentness_hoy_finds_the_word_in_a_currentness_position(text, grouped, span_text):
    found = currentness_hoy(text, TODAY, grouped_by_state=grouped)
    assert found is not None, text
    assert text[slice(*found[0])] == span_text
    assert found[1] == ("2026-10-01", "2026-10-01")


@pytest.mark.parametrize("text,grouped", [
    ("contratos de galapa figuran antes de hoy suspendidos", False),
    ("contratos de galapa figuran hasta hoy suspendidos", False),
    ("contratos de galapa figuran desde 2024 hasta hoy", False),
    ("contratos firmados hoy de galapa figuran suspendidos", False),
    ("contratos de galapa hoy", False),
    ("contratos de galapa figuran suspendidos", False),
    ("contratos de galapa a hoy", False),                      # «a hoy» only in a breakdown by state
    ("desglosa por estado los contratos de galapa de 2024 a hoy", True),  # another period exists
    ("contratos de galapa figuran este ano suspendidos", False),
    ("contratos de galapa figuran el 31 de febrero de 2025 hoy", False),
])
def test_currentness_hoy_sets_nothing_aside_anywhere_else(text, grouped):
    assert currentness_hoy(text, TODAY, grouped_by_state=grouped) is None, text
