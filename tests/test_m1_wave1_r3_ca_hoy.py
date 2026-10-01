"""M1 wave 1 R3 (ZEUS2 P2-1 and P2-2): «hoy» next to an event is the day; after a state it is currentness.

R2 set «hoy» aside as currentness whenever it touched a verb of state, so «firmados hoy
están en ejecución» (the contracts SIGNED today that are in execution) executed with NO
period: R1 asked and the base executed nothing wrong. And the reverse: «están suspendidos
hoy» (the word AFTER the state) asked although it is plain currentness.

The rule is by position: the last of {event word, verb of state} before «hoy» governs it.
An event word (firmar, celebrar, suscribir, publicar, adjudicar… any inflection) makes it the
signature day; «de hoy» is the day of the contracts; a verb of state makes it currentness
however many words apart.

The tests pin the contract on adjacent AND non-adjacent forms, not the forms themselves.
"""
from __future__ import annotations

import itertools
from datetime import date
from pathlib import Path

import pytest

from app.core.analytics.contract_activity import currentness as cur
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.currentness import currentness_hoy
from app.core.analytics.contract_activity.models import AdmissionStatus
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter

TODAY = date(2026, 10, 1)
B = "la Alcaldía de Galapa"


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _admit(router: QueryRouter, raw: str):
    parsed = router.parse(raw, today=TODAY)
    return admit_contract_activity(
        raw, dict(parsed.params), today=TODAY, semantic_evidence=parsed.semantic_evidence.to_state(),
    )


EVENTS = [
    "firmados", "celebrados", "suscritos", "publicados", "adjudicados", "perfeccionados",
    "registrados", "otorgados", "aprobados", "que firmó", "que firmaron", "que se firmaron",
    "firmados el día de", "suscritos el día de", "celebrados por la Alcaldía de Galapa",
]
STATE_PHRASES = [("están", "en ejecución"), ("figuran", "suspendidos"), ("se encuentran", "suspendidos"),
                 ("siguen", "suspendidos")]


@pytest.mark.parametrize("event", EVENTS)
@pytest.mark.parametrize("verb,state", STATE_PHRASES)
def test_hoy_glued_to_an_event_is_the_signature_day_and_never_executes_without_a_period(
    router, event, verb, state,
):
    raw = f"¿Cuántos contratos de {B} {event} hoy {verb} {state}?"
    admission = _admit(router, raw)
    assert admission.status is AdmissionStatus.CLARIFY, raw
    assert admission.request is None, raw


@pytest.mark.parametrize("raw", [
    f"¿Cuántos contratos de hoy de {B} están suspendidos?",
    f"¿Cuántos contratos de {B} de hoy están suspendidos?",
    f"¿Cuántos contratos de {B} del día de hoy están suspendidos?",
    f"¿Cuántos contratos de {B} figuran suspendidos firmados hoy?",
    f"¿Cuántos contratos firmó {B} hoy y siguen en ejecución?",
])
def test_the_day_of_the_contracts_is_a_period_not_a_currentness(router, raw):
    admission = _admit(router, raw)
    assert admission.status is AdmissionStatus.CLARIFY, raw
    assert admission.request is None


@pytest.mark.parametrize("verb", ["están", "figuran"])
@pytest.mark.parametrize("state,states", [
    ("suspendidos", ("Suspendido",)),
    ("en ejecución", ("En ejecución", "Modificado", "Prorrogado")),
])
def test_hoy_after_the_state_is_currentness_as_the_base_executed(router, verb, state, states):
    raw = f"¿Cuántos contratos de {B} {verb} {state} hoy?"
    admission = _admit(router, raw)
    assert admission.status is AdmissionStatus.READY, (raw, admission.reason)
    assert (admission.request.date_from, admission.request.date_to) == (None, None)
    assert admission.request.reported_states == states


@pytest.mark.parametrize("raw", [
    f"¿Cuántos contratos de {B} figuran hoy suspendidos?",
    f"¿Cuántos contratos de {B} están hoy suspendidos?",
    f"¿Cuántos contratos de {B} hoy están suspendidos?",
    f"¿Cuántos contratos de 2025 de {B} están hoy suspendidos?",
])
def test_the_forms_that_r2_already_executed_still_execute(router, raw):
    assert _admit(router, raw).status is AdmissionStatus.READY, raw


# ── the position rule on its own: event x state order, any distance ───────────────────────────

POSITIVE = [
    "contratos de galapa estan en ejecucion hoy",
    "contratos de galapa estan en ejecucion en galapa y obras hoy",
    "contratos de galapa figuran hoy suspendidos",
    "contratos de galapa hoy figuran suspendidos",
    "contratos firmados ayer de galapa estan hoy en ejecucion",
    "contratos firmados en 2025 de galapa figuran suspendidos hoy",
    "contratos de galapa que estan suspendidos hoy",
    "contratos al dia de hoy estan suspendidos",
    "contratos de galapa figuran al dia de hoy suspendidos",
]
NEGATIVE = [
    "contratos de galapa firmados hoy estan en ejecucion",
    "contratos de galapa celebrados hoy figuran suspendidos",
    "contratos de galapa que firmo hoy estan suspendidos",
    "contratos de galapa firmados el dia de hoy estan en ejecucion",
    "contratos de galapa de hoy estan suspendidos",
    "contratos de hoy de galapa estan suspendidos",
    "contratos de galapa estan suspendidos y se firmaron hoy",
    "contratos de galapa estan en ejecucion firmados hoy",
    "contratos de galapa publicados por galapa hoy estan suspendidos",
    "contratos de galapa hoy",
    "contratos de galapa antes de hoy estan suspendidos",
]


@pytest.mark.parametrize("text", POSITIVE)
def test_position_rule_currentness(text):
    found = currentness_hoy(text, TODAY, grouped_by_state=False)
    assert found is not None, text
    assert text[slice(*found[0])] == "hoy"


@pytest.mark.parametrize("text", NEGATIVE)
def test_position_rule_period(text):
    assert currentness_hoy(text, TODAY, grouped_by_state=False) is None, text


def test_the_last_of_event_and_state_governs_in_both_orders():
    for event, verb in itertools.product(("firmados", "celebrados", "publicados"), ("estan", "figuran")):
        assert currentness_hoy(f"contratos {event} {verb} hoy suspendidos", TODAY, grouped_by_state=False), \
            f"{event} {verb} hoy: the state verb is the last before hoy"
        assert currentness_hoy(f"contratos {verb} suspendidos {event} hoy", TODAY, grouped_by_state=False) is None, \
            f"{verb} {event} hoy: the event is the last before hoy"


def test_a_clause_boundary_resets_the_position():
    """An event in a previous sentence does not date a «hoy» of the next one."""
    same_sentence = "cuantos contratos firmo galapa, hoy estan suspendidos"
    next_sentence = "cuantos contratos firmo galapa. hoy estan suspendidos"
    assert currentness_hoy(same_sentence, TODAY, grouped_by_state=False) is None
    assert currentness_hoy(next_sentence, TODAY, grouped_by_state=False) is not None


def test_the_event_stems_cover_the_verbs_that_date_a_contract():
    for word in ("firmo", "firmaron", "firmados", "firma", "celebrado", "suscrito", "suscribio",
                 "publicados", "adjudicadas", "perfeccionados", "registrados", "otorgo",
                 "aprobados", "expedidos", "legalizados"):
        assert cur._EVENT.search(word), word
    for word in ("suspendidos", "ejecucion", "terminados", "estan", "figuran", "galapa"):
        assert not cur._EVENT.search(word), word
