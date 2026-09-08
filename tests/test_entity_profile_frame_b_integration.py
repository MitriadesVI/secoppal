"""Los ocho escenarios `INTEGRATION_B_REQUIRED`, como pruebas REALES.

`tests/test_entity_profile_frame_integration_contract.py` los declara y dice
—con razón— que su puerta es V1.2-B. Mantener sus nombres en una tupla no
acredita nada: aquí cada uno es una conversación completa con publicación en
disco, un `SecopalWorkflow` nuevo por turno y las llamadas realmente enviadas.

La correspondencia con aquella lista está en `_COVERS`, y una prueba comprueba
que no queda ninguno sin cubrir.
"""
from __future__ import annotations

import pytest

from app.core.entity_profile.frame_values import SourceKind, SourceOrigin
from app.core.workflow.continuation import ContinuationOwner, ResultKind
from entity_profile_frame_b_harness import (  # noqa: F401 — fixture por nombre
    BUYER,
    Conversation,
    frame_v12,
)
from test_entity_profile_frame_integration_contract import INTEGRATION_B_REQUIRED


_COVERS = {
    "R5a displayed inherited period after a real reload":
        "test_r5a_inherited_period_is_in_every_query_after_a_real_reload",
    "R5b explicit completeness after a real reload":
        "test_r5b_explicit_completeness_survives_the_reload",
    "R5c pending selection across turns":
        "test_r5c_a_pending_selection_crosses_turns_and_keeps_its_focus",
    "R5d pending period does not leak into confirmed execution":
        "test_r5d_a_pending_period_never_reaches_a_confirmed_execution",
    "R6 explicit origin survives persistence":
        "test_r6_explicit_origin_survives_persistence",
    "R7a profile does not seed tabular pagination":
        "test_r7a_a_profile_does_not_seed_tabular_pagination",
    "R7b CLEAR survives another reload":
        "test_r7b_clear_survives_another_reload",
    "R7c a tabular cursor is inert after a profile":
        "test_r7c_a_tabular_cursor_is_inert_after_a_profile",
}

_CONTRACT_DATE = "fecha_de_firma"
_PROCESS_DATE = "fecha_de_publicacion_del"
_AUGUST = "2026-08-01"


def test_every_declared_b_scenario_has_a_real_test():
    assert set(INTEGRATION_B_REQUIRED) == set(_COVERS)
    missing = [name for name in _COVERS.values() if name not in globals()]
    assert missing == []


# ── R5a ────────────────────────────────────────────────────────────────────

def test_r5a_inherited_period_is_in_every_query_after_a_real_reload(frame_v12):
    chat = Conversation("b-r5a")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    followup = chat.say("proveedores")

    assert followup.route == "entity_profile_suppliers"
    assert not followup.clarifies
    assert followup.planned_jobs == 2
    assert len(followup.calls) == 2
    # El periodo heredado está en TODAS las consultas ejecutadas, no sólo en el
    # alcance visible: era exactamente la disociación del hallazgo original.
    for soql in followup.sql:
        assert _CONTRACT_DATE in soql
        assert _AUGUST in soql
    assert "Agosto de 2026, según fecha de firma." in followup.response

    # Y el foco sobrevive a la recarga, porque viaja en el Frame publicado.
    state = chat.profile_state()
    assert state.confirmed_frame is not None
    assert state.confirmed_frame.focus == "suppliers"
    period = state.confirmed_frame.predicates_for(SourceKind.CONTRACTS).temporal.period
    assert period.lower == _AUGUST


# ── R5b ────────────────────────────────────────────────────────────────────

def test_r5b_explicit_completeness_survives_the_reload(frame_v12):
    chat = Conversation("b-r5b")
    chat.say(f"analiza procesos y contratos de {BUYER} en agosto")
    pending = chat.say("proveedores")

    assert pending.clarifies
    assert pending.calls == ()  # cero llamadas: preguntar no consulta
    assert chat.profile_state().has_pending

    accepted = chat.say("solo contratos")
    assert not accepted.clarifies
    assert accepted.route == "entity_profile_suppliers"
    for soql in accepted.sql:
        assert _AUGUST in soql and _CONTRACT_DATE in soql

    state = chat.profile_state()
    assert state.confirmed_frame.focus == "suppliers"
    assert state.confirmed_frame.source_selection.requested == frozenset(
        {SourceKind.CONTRACTS},
    )
    assert not state.has_pending


# ── R5c ────────────────────────────────────────────────────────────────────

def test_r5c_a_pending_selection_crosses_turns_and_keeps_its_focus(frame_v12):
    chat = Conversation("b-r5c")
    chat.say(f"analiza procesos de {BUYER} en agosto")

    pending = chat.say("proveedores")
    assert pending.clarifies and pending.calls == ()
    # La alternativa publicada DICE que contratos usa fecha de firma y que
    # conserva agosto; no lo deja como una herencia silenciosa.
    assert "fecha de firma" in pending.response
    assert "agosto de 2026" in pending.response.lower()

    stored = chat.profile_state().pending
    assert stored is not None and len(stored.alternatives) == 1

    selected = chat.say("solo contratos")
    assert not selected.clarifies
    assert selected.route == "entity_profile_suppliers"
    assert selected.planned_jobs == 2 and len(selected.calls) == 2
    assert selected.datasets == (SourceKind.CONTRACTS.dataset_id,)
    for soql in selected.sql:
        assert _AUGUST in soql and _CONTRACT_DATE in soql


def test_r5c_an_ordinal_selects_the_published_alternative(frame_v12):
    chat = Conversation("b-r5c-ordinal")
    chat.say(f"analiza procesos de {BUYER} en agosto")
    chat.say("proveedores")
    chosen = chat.say("1")
    assert not chosen.clarifies
    assert chosen.route == "entity_profile_suppliers"
    assert chosen.datasets == (SourceKind.CONTRACTS.dataset_id,)


def test_a_bare_yes_never_chooses_the_alternative_for_the_person(frame_v12):
    chat = Conversation("b-r5c-yes")
    chat.say(f"analiza procesos de {BUYER} en agosto")
    chat.say("proveedores")
    answer = chat.say("sí")
    assert answer.clarifies
    assert answer.calls == ()
    assert chat.profile_state().has_pending


# ── R5d ────────────────────────────────────────────────────────────────────

def test_r5d_a_pending_period_never_reaches_a_confirmed_execution(frame_v12):
    chat = Conversation("b-r5d")
    chat.say(f"analiza procesos de {BUYER} en julio")

    second = chat.say("proveedores en agosto")
    assert second.clarifies and second.calls == ()

    third = chat.say("recientes")
    assert third.clarifies and third.calls == ()

    # La confirmada sigue siendo JULIO mientras hubo pendiente.
    confirmed = chat.profile_state().confirmed_frame
    assert confirmed.predicates_for(SourceKind.PROCESSES).temporal.period.lower == (
        "2026-07-01"
    )

    chat.say("cancela ese cambio")
    executed = chat.say("recientes")
    assert not executed.clarifies
    assert executed.datasets == (SourceKind.PROCESSES.dataset_id,)
    for soql in executed.sql:
        assert "2026-07-01" in soql and _PROCESS_DATE in soql
        assert _AUGUST not in soql


# ── R6 ─────────────────────────────────────────────────────────────────────

def test_r6_explicit_origin_survives_persistence(frame_v12):
    chat = Conversation("b-r6")
    chat.say(f"analiza procesos y contratos de {BUYER} en agosto")
    assert chat.profile_state().confirmed_frame.source_selection.origin is (
        SourceOrigin.EXPLICIT
    )

    for focus in ("proveedores", "montos"):
        turn = chat.say(focus)
        assert turn.clarifies, focus
        assert turn.calls == (), focus
        # Aclarar NO destruye la confirmada ni degrada su origen.
        state = chat.profile_state()
        assert state.confirmed_frame.source_selection.origin is SourceOrigin.EXPLICIT
        assert state.confirmed_frame.focus == "overview"
        chat.say("cancela ese cambio")


def test_r6_control_default_origin_narrows_and_declares_it(frame_v12):
    chat = Conversation("b-r6-default")
    chat.say(f"analiza {BUYER} en agosto")
    turn = chat.say("proveedores")
    assert not turn.clarifies
    # Se ejecuta el subconjunto capaz y se DECLARA la limitación...
    assert turn.datasets == (SourceKind.CONTRACTS.dataset_id,)
    assert "sólo se pueden responder sobre" in turn.response
    # ...pero el universo confirmado NO se recorta: volver a la vista general
    # no puede heredar un recorte de capacidad como si lo hubiera elegido la
    # persona, y su origen sigue siendo DEFAULT.
    confirmed = chat.profile_state().confirmed_frame
    assert confirmed.source_selection.requested == frozenset(
        {SourceKind.PROCESSES, SourceKind.CONTRACTS},
    )
    assert confirmed.source_selection.origin is SourceOrigin.DEFAULT


# ── R7a ────────────────────────────────────────────────────────────────────

def test_r7a_a_profile_does_not_seed_tabular_pagination(frame_v12):
    chat = Conversation("b-r7a")
    profile = chat.say(f"analiza contratos de {BUYER} en agosto")
    assert chat.contract().owner is ContinuationOwner.ENTITY_PROFILE
    assert chat.contract().result_kind is ResultKind.ENTITY_PROFILE
    assert not chat.contract().can_paginate

    more = chat.say("mas")
    assert more.clarifies
    assert more.calls == ()
    assert more.route == "admission_profile_is_not_paginable"
    assert "OFFSET" not in " ".join(profile.sql).upper()
    assert "profundizar" in more.response


# ── R7b ────────────────────────────────────────────────────────────────────

def test_r7b_clear_survives_another_reload(frame_v12):
    chat = Conversation("b-r7b")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    cleared = chat.say("sin filtro de fecha")

    assert not cleared.clarifies
    for soql in cleared.sql:
        assert _AUGUST not in soql
    assert "Todo el histórico disponible" in cleared.response

    later = chat.say("recientes")
    for soql in later.sql:
        assert _AUGUST not in soql
    period = chat.profile_state().confirmed_frame.predicates_for(
        SourceKind.CONTRACTS,
    ).temporal.period
    assert period.is_unbounded


def test_r7b_clearing_a_period_does_not_invent_a_negative_topic(frame_v12):
    chat = Conversation("b-r7b-topic")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    cleared = chat.say("sin filtro de fecha")
    # «sin filtro de fecha» lleva un «sin» que el router lee como negación.
    # La autoridad de limpieza consume sus propios términos; si no, el turno
    # AÑADIRÍA un tema negativo que nadie pidió, y llegaría al SQL.
    assert "Tema:" not in cleared.response
    for soql in cleared.sql:
        assert "filtro" not in soql.lower()


# ── R7c ────────────────────────────────────────────────────────────────────

def test_r7c_a_tabular_cursor_is_inert_after_a_profile(frame_v12):
    chat = Conversation("b-r7c")
    tabular = chat.say("contratos de pavimentacion en baranoa")
    assert tabular.result.get("analytical_intent") != "entity_profile"

    chat.say(f"analiza contratos de {BUYER} en agosto")
    more = chat.say("mas")

    assert more.clarifies
    assert more.calls == ()
    assert more.route == "admission_profile_is_not_paginable"


@pytest.mark.parametrize("scenario", sorted(_COVERS))
def test_declared_scenarios_are_named_by_a_real_test(scenario):
    assert _COVERS[scenario] in globals()
