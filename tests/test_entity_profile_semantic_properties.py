"""P1–P12: las doce propiedades del plan semántico, sobre entradas generadas.

Un corpus prueba los casos que alguien imaginó. Estas propiedades prueban lo que
debe valer SIEMPRE, y por eso se generan las consultas combinando fragmentos
reales: marco de análisis, fuente, evento, estado, tema, periodo y foco.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.effective_scope import build_effective_scope
from app.core.entity_profile.execution_plan import (
    FOCUS_CAPABILITY,
    build_execution_plan,
)
from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.semantic_request import build_semantic_request
from app.core.entity_profile.source_plan import project_sources
from app.core.entity_profile.source_request import RequestedSources, SourceProvenance
from app.core.entity_profile.temporal_event import TemporalEvent
from app.core.entity_profile.turn import classify_entity_profile_turn, profile_query_params
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_queries import FakeClient


TODAY = date(2026, 9, 5)
BUYER = "alcaldia de baranoa"

_ROUTER = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))
_SETTINGS = settings(
    max_examples=60,
    derandomize=True,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

FRAMES = st.sampled_from(["analiza", "analizame", "perfil de", "radiografia de"])
SOURCES = st.sampled_from(["", "procesos", "contratos", "procesos y contratos"])
EVENTS = st.sampled_from(["", "publicados", "firmados", "celebrados"])
UNSUPPORTED = st.sampled_from(["terminados", "liquidados", "iniciados", "cedidos"])
STATES = st.sampled_from(["", "actualmente abiertos", "actualmente en ejecucion"])
TOPICS = st.sampled_from(["", "de mantenimiento", "de vias", "de firma digital"])
PERIODS = st.sampled_from(["", "en 2026", "en agosto", "en julio"])
FOCUS_WORDS = st.sampled_from(["", "proveedores", "modalidades", "valores", "recientes"])


def _query(frame, source, topic, event, state, period, focus="") -> str:
    parts = [frame, focus, source, topic, event, f"de {BUYER}", state, period]
    return " ".join(part for part in parts if part)


def _turn(text: str):
    return classify_entity_profile_turn(text, _ROUTER.parse(text, today=TODAY).params)


def _request(text: str, focus: str = "overview"):
    params = profile_query_params(_ROUTER.parse(text, today=TODAY).params)
    scope = resolve_profile_scope(params).scope
    if scope is None:
        return None, params
    return build_semantic_request(
        user_query=text, params=params, scope=scope, focus=focus,
    ), params


# ── P1: la fuente solicitada nunca se ensancha en silencio ──────────────────

@_SETTINGS
@given(frame=FRAMES, topic=TOPICS, period=PERIODS, focus=FOCUS_WORDS)
def test_p1_a_named_source_is_never_silently_widened(frame, topic, period, focus):
    for source, expected in (
        ("procesos", RequestedSources.PROCESSES),
        ("contratos", RequestedSources.CONTRACTS),
    ):
        text = _query(frame, source, topic, "", "", period, focus)
        request, _ = _request(text)
        if request is None:
            continue
        assert request.sources is expected
        assert request.sources_provenance is SourceProvenance.EXPLICIT


# ── P2: el foco nunca reescribe la fuente solicitada ────────────────────────

@_SETTINGS
@given(frame=FRAMES, source=SOURCES, topic=TOPICS, period=PERIODS,
       focus=st.sampled_from(sorted(FOCUS_CAPABILITY)))
def test_p2_focus_never_overwrites_the_requested_source(frame, source, topic, period, focus):
    text = _query(frame, source, topic, "", "", period)
    request, params = _request(text, focus)
    if request is None:
        return
    plan = build_execution_plan(request, params)
    assert plan.request.sources is request.sources
    if plan.effective_sources is not None:
        requested = {
            RequestedSources.PROCESSES: {PROCESS_DATASET},
            RequestedSources.CONTRACTS: {CONTRACT_DATASET},
            RequestedSources.BOTH: {PROCESS_DATASET, CONTRACT_DATASET},
        }[request.sources]
        assert plan.executed_datasets <= requested


# ── P3 / P4: evento y estado son independientes ─────────────────────────────

@_SETTINGS
@given(frame=FRAMES, source=SOURCES, topic=TOPICS,
       event=st.sampled_from(["publicados", "firmados"]), period=PERIODS)
def test_p3_an_event_never_implies_a_current_state(frame, source, topic, event, period):
    text = _query(frame, source, topic, event, "", period)
    request, params = _request(text)
    if request is None or request.event is TemporalEvent.NONE:
        return
    if request.keeps_current_state:
        return
    for plan in project_sources(request, params):
        for key in (
            "estado", "estado_family", "estado_del_procedimiento",
            "estado_de_apertura_del_proceso", "estado_contrato",
        ):
            assert key not in plan.params


@_SETTINGS
@given(frame=FRAMES, source=SOURCES, topic=TOPICS, state=STATES)
def test_p4_a_current_state_never_invents_a_historical_event(frame, source, topic, state):
    text = _query(frame, source, topic, "", state, "")
    request, _ = _request(text)
    if request is None:
        return
    assert request.event is TemporalEvent.NONE
    assert request.unsupported_event is None


# ── P5: un evento sin fecha productiva no consulta la fuente ────────────────

@_SETTINGS
@given(frame=FRAMES, source=SOURCES, event=UNSUPPORTED,
       period=st.sampled_from(["en 2026", "en agosto"]))
def test_p5_an_unsupported_event_issues_zero_calls(frame, source, event, period):
    text = _query(frame, source, "", event, "", period)
    turn = _turn(text)
    if not turn.detected or turn.request is None:
        return
    assert turn.clarification is not None
    assert turn.plan.jobs == ()


# ── P6: la proyección sólo lleva predicados compatibles ─────────────────────

@_SETTINGS
@given(frame=FRAMES, source=SOURCES, topic=TOPICS, event=EVENTS,
       state=STATES, period=PERIODS)
def test_p6_a_source_plan_holds_only_compatible_predicates(
    frame, source, topic, event, state, period,
):
    text = _query(frame, source, topic, event, state, period)
    request, params = _request(text)
    if request is None:
        return
    process_plan, contract_plan = project_sources(request, params)
    for key in ("estado_contrato", "estado_contrato_adicional"):
        assert key not in process_plan.params
    for key in ("estado_del_procedimiento", "estado_de_apertura_del_proceso"):
        assert key not in contract_plan.params
    for plan in (process_plan, contract_plan):
        assert "dataset_scope" not in plan.params
        assert "dataset_explicit" not in plan.params


# ── P7 / P8: intención fuera del tema, tema real dentro ─────────────────────

_ACTION_WORDS = frozenset({
    "analiza", "analizame", "firmo", "firmaron", "firmados", "celebro",
    "celebrados", "suscribio", "publico", "publicados", "terminados",
    "liquidados", "iniciados", "cedidos", "todavia", "actualmente",
})


@_SETTINGS
@given(frame=FRAMES, source=SOURCES, event=EVENTS, state=STATES, period=PERIODS)
def test_p7_an_action_or_state_word_is_never_the_topic(
    frame, source, event, state, period,
):
    text = _query(frame, source, "", event, state, period)
    request, _ = _request(text)
    if request is None:
        return
    assert not (set(request.topic) & _ACTION_WORDS)


@_SETTINGS
@given(frame=FRAMES, source=SOURCES, event=EVENTS, state=STATES, period=PERIODS,
       topic=st.sampled_from([
           ("de mantenimiento", "mantenimiento"),
           ("de vias", "vias"),
           ("de interventoria", "interventoria"),
       ]))
def test_p8_a_real_topic_always_survives(frame, source, event, state, period, topic):
    phrase, expected = topic
    text = _query(frame, source, phrase, event, state, period)
    request, _ = _request(text)
    if request is None:
        return
    assert expected in request.topic


# ── P9 / P10: nada no acreditado se presenta como cero ──────────────────────

@_SETTINGS
@given(frame=FRAMES, source=st.sampled_from(["procesos", "contratos"]),
       period=PERIODS)
def test_p9_a_source_that_was_not_requested_never_renders_a_zero(frame, source, period):
    text = _query(frame, source, "", "", "", period)
    turn = _turn(text)
    if not turn.detected or turn.clarification or turn.plan is None:
        return
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=FakeClient(), soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )
    rendered = render_entity_profile(profile)
    absent = "procesos" if source == "contratos" else "contratos"
    assert f"0 {absent}" not in rendered
    assert profile.partial is False


@_SETTINGS
@given(count=st.integers(min_value=1, max_value=10**9),
       total=st.sampled_from([None, "", "n/d", "NaN"]))
def test_p10_an_unusable_metric_never_becomes_zero(count, total):
    from app.core.entity_profile.aspects.values import parse_contract_values

    parsed = parse_contract_values([{
        "contract_count": str(count), "total_contract_value": total,
    }])
    assert parsed is not None
    assert parsed.contract_count == count
    assert parsed.total_contract_value is None


# ── P11: el alcance renderizado es el ejecutado ─────────────────────────────

@_SETTINGS
@given(frame=FRAMES, source=SOURCES, topic=TOPICS, period=PERIODS)
def test_p11_the_rendered_scope_equals_the_executed_scope(frame, source, topic, period):
    text = _query(frame, source, topic, "", "", period)
    turn = _turn(text)
    if not turn.detected or turn.clarification or turn.plan is None:
        return
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=FakeClient(), soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )
    snapshot = build_effective_scope(profile)
    assert snapshot.effective_sources is turn.plan.effective_sources
    rendered = render_entity_profile(profile)
    assert rendered.startswith(snapshot.title)
    if snapshot.effective_sources is RequestedSources.CONTRACTS:
        assert "SECOP II Procesos" not in rendered
    if snapshot.effective_sources is RequestedSources.PROCESSES:
        assert "SECOP II Contratos" not in rendered


# ── P12: un cambio ortogonal sólo mueve su dimensión ────────────────────────

@_SETTINGS
@given(source=st.sampled_from(["procesos", "contratos"]),
       topic=st.sampled_from(["", "de mantenimiento"]),
       focus=st.sampled_from(["", "proveedores", "recientes"]))
def test_p12_an_orthogonal_followup_moves_only_its_dimension(source, topic, focus):
    base = _query("analiza", source, topic, "", "", "")
    first = _turn(base)
    if not first.detected or first.request is None:
        return
    # Lo que `followup_engine` arrastra en un refinamiento: alcance, tema y
    # marcadores del perfil. La herencia de TEMA es suya (TOPIC_FIELDS en
    # `followup_mutations`, ADR-061) y no del perfil; duplicarla aquí crearía
    # una segunda autoridad sobre la misma dimensión.
    carried = {
        key: value for key, value in first.params.items()
        if key.startswith("_entity_profile")
        or key in (
            "entidad", "entidad_resolved", "entidad_resolution",
            "objeto", "objeto_or", "objeto_phrases",
        )
    }
    follow_up = "y en agosto"
    second = classify_entity_profile_turn(
        follow_up,
        {**carried, **_ROUTER.parse(follow_up, today=TODAY).params},
    )
    if not second.detected or second.request is None:
        return
    # Cambia el periodo; fuente y tema se conservan.
    assert second.request.time_from == "2026-08-01"
    assert second.request.sources is first.request.sources
    assert second.request.topic == first.request.topic
