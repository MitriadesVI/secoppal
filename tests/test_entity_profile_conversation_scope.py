"""La conversación profundiza en el MISMO análisis, y el texto dice lo ejecutado."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.effective_scope import (
    build_effective_scope,
    humanize_period,
)
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.source_request import RequestedSources
from app.core.entity_profile.turn import (
    PROFILE_REQUEST_PARAM,
    classify_entity_profile_turn,
)
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_queries import FakeClient


TODAY = date(2026, 9, 5)


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _turn(router, text, carried=None):
    params = router.parse(text, today=TODAY).params
    if carried:
        params = {**carried, **params}
    return classify_entity_profile_turn(text, params)


def _profile(turn):
    return build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=FakeClient(), soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )


# Lo que `followup_engine` arrastra de un turno al siguiente además de los
# marcadores del perfil: el alcance ya resuelto. Se enumera aquí para que el
# test mida la composición semántica y no la mecánica del merge, que tiene su
# propio corpus.
_INHERITED_SCOPE_KEYS = (
    "entidad", "entidad_resolved", "entidad_resolution",
    "fecha_desde", "fecha_hasta",
)


def _carry(turn) -> dict:
    """Lo que la conversación persiste: sólo tipos JSON."""
    profile_markers = {
        key: value for key, value in turn.params.items()
        if key.startswith("_entity_profile")
    }
    assert json.loads(json.dumps(profile_markers)) == profile_markers
    return {
        **{
            key: turn.params[key]
            for key in _INHERITED_SCOPE_KEYS
            if turn.params.get(key) is not None
        },
        **profile_markers,
    }


# ── 5A: contexto serializable ───────────────────────────────────────────────

def test_the_profile_context_is_json_safe_and_holds_no_live_objects(router):
    turn = _turn(router, "analizame que contratos firmo alcaldia de baranoa en agosto")
    carried = _carry(turn)
    assert carried["_entity_profile_sources"] == "CONTRACTS"
    assert carried[PROFILE_REQUEST_PARAM]["event"] == "CONTRACT_SIGNATURE"


def test_a_reloaded_request_reproduces_the_same_plan(router):
    text = "analizame que contratos firmo alcaldia de baranoa en agosto"
    turn = _turn(router, text)
    revived = build_entity_profile(
        scope=turn.scope, identity=turn.identity,
        params=json.loads(json.dumps(turn.params)),
        focus=turn.focus, secop_client=FakeClient(), soql_builder=SoQLBuilder(),
    )
    assert revived.plan.effective_sources is RequestedSources.CONTRACTS
    assert revived.plan.executed_call_ids == turn.plan.executed_call_ids


# ── 5B / 5C: herencia y preservación por dimensión ──────────────────────────

def test_an_explicit_source_narrows_the_conversation_and_is_kept(router):
    first = _turn(router, "analiza alcaldia de baranoa")
    assert first.request.sources is RequestedSources.BOTH

    second = _turn(router, "solo contratos", carried=_carry(first))
    assert second.request.sources is RequestedSources.CONTRACTS

    third = _turn(router, "y los proveedores", carried=_carry(second))
    assert third.focus == "suppliers"
    assert third.request.sources is RequestedSources.CONTRACTS


def test_changing_the_period_does_not_reset_the_focus_or_the_source(router):
    first = _turn(router, "analiza contratos de alcaldia de baranoa")
    second = _turn(router, "y en agosto", carried=_carry(first))
    assert second.request.sources is RequestedSources.CONTRACTS
    assert second.request.time_from == "2026-08-01"


def test_an_explicit_source_switch_replaces_the_inherited_one(router):
    first = _turn(router, "analiza contratos de alcaldia de baranoa")
    second = _turn(router, "ahora procesos", carried=_carry(first))
    assert second.request.sources is RequestedSources.PROCESSES


def test_a_supplier_question_over_a_process_conversation_asks_instead_of_widening(router):
    first = _turn(router, "analiza procesos de alcaldia de baranoa")
    assert first.request.sources is RequestedSources.PROCESSES
    second = _turn(router, "y los proveedores", carried=_carry(first))
    assert second.clarification is not None
    assert second.plan.jobs == ()


def test_a_focus_narrowing_keeps_the_requested_source_intact(router):
    first = _turn(router, "analiza procesos y contratos de alcaldia de baranoa")
    second = _turn(router, "y los proveedores", carried=_carry(first))
    assert second.request.sources is RequestedSources.BOTH
    assert second.plan.effective_sources is RequestedSources.CONTRACTS


# ── 5D: seguridad de la aclaración ──────────────────────────────────────────

def test_a_pending_clarification_leaves_the_previous_request_untouched(router):
    first = _turn(router, "analiza contratos de alcaldia de baranoa en agosto")
    carried = _carry(first)
    blocked = _turn(router, "analiza procesos de alcaldia de baranoa con proveedores",
                    carried=carried)
    assert blocked.clarification is not None
    assert carried[PROFILE_REQUEST_PARAM]["sources"] == "CONTRACTS"
    assert carried[PROFILE_REQUEST_PARAM]["time_from"] == "2026-08-01"


# ── 5E / 5F / 5G: alcance renderizado == alcance ejecutado ──────────────────

@pytest.mark.parametrize("time_from,time_to,expected", [
    ("2026-01-01", "2026-12-31", "2026"),
    ("2026-08-01", "2026-08-31", "del 1 al 31 de agosto de 2026"),
    ("2026-02-01", "2026-02-28", "del 1 al 28 de febrero de 2026"),
    ("2028-02-01", "2028-02-29", "del 1 al 29 de febrero de 2028"),
    ("2026-08-05", "2026-08-20", "del 2026-08-05 al 2026-08-20"),
    (None, None, "todo el histórico disponible"),
])
def test_a_month_is_never_reduced_to_its_year(time_from, time_to, expected):
    assert humanize_period(time_from, time_to) == expected


def test_a_contracts_profile_names_contracts_and_their_signature_date(router):
    rendered = render_entity_profile(
        _profile(_turn(router, "analizame que contratos firmo alcaldia de baranoa en agosto")),
    )
    assert rendered.startswith("ALCALDIA MUNICIPAL DE BARANOA — SECOP II Contratos")
    assert "Firmados del 1 al 31 de agosto de 2026." in rendered
    assert "procesos" not in rendered.lower()


def test_a_processes_profile_names_processes_and_their_publication_date(router):
    rendered = render_entity_profile(
        _profile(_turn(
            router, "analiza procesos de vias publicados por alcaldia de baranoa en agosto",
        )),
    )
    assert rendered.startswith("ALCALDIA MUNICIPAL DE BARANOA — SECOP II Procesos")
    assert "Publicados del 1 al 31 de agosto de 2026." in rendered
    assert "Tema: vias." in rendered


def test_a_both_profile_declares_one_period_and_two_date_bases(router):
    rendered = render_entity_profile(
        _profile(_turn(router, "analiza alcaldia de baranoa en 2026")),
    )
    assert "— actividad contractual" in rendered
    assert "Procesos: 2026, según fecha de publicación." in rendered
    assert "Contratos: 2026, según fecha de firma." in rendered


def test_a_focus_narrowing_is_stated_and_not_hidden(router):
    first = _turn(router, "analiza procesos y contratos de alcaldia de baranoa")
    second = _turn(router, "y los proveedores", carried=_carry(first))
    rendered = render_entity_profile(_profile(second))
    assert "sólo se puede responder sobre contratos" in rendered


def test_the_rendered_scope_never_offers_a_source_the_plan_did_not_cover(router):
    rendered = render_entity_profile(
        _profile(_turn(router, "analizame que contratos firmo alcaldia de baranoa en agosto")),
    )
    assert "modalidades" not in rendered
    assert "Puedes pedirme sus proveedores, valores o contratos recientes." in rendered


def test_the_snapshot_reports_exactly_what_the_plan_executed(router):
    turn = _turn(router, "analizame que contratos firmo alcaldia de baranoa en agosto")
    profile = _profile(turn)
    scope = build_effective_scope(profile)
    assert scope.requested_sources is RequestedSources.CONTRACTS
    assert scope.effective_sources is profile.plan.effective_sources
    assert scope.buyer == profile.identity.display_name
    assert scope.period == humanize_period(
        turn.request.time_from, turn.request.time_to,
    )
