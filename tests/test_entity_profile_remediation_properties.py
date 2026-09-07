"""P13–P23: las propiedades que la revisión independiente echó en falta.

Las 12 de V1 se conservan y no bastan: varias salían temprano por la misma
decisión que debían refutar, o generaban temas que nunca contenían una palabra
de otra fuente. Éstas atacan exactamente los siete P1.

Cada propiedad se comprueba además contra el SQL o el disco reales, no contra
la forma que produce la misma función bajo prueba.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import re

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.aspects.activity import parse_process_activity
from app.core.entity_profile.aspects.common import as_optional_int
from app.core.entity_profile.aspects.temporal import merge_periods
from app.core.entity_profile.conversation_state import STATE_PARAM, EntityProfileConversationState
from app.core.entity_profile.effective_scope import build_effective_scope, scope_filter_lines
from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET, SectionStatus
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.source_request import RequestedSources, SourceProvenance
from app.core.entity_profile.temporal_event import TemporalEvent
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_queries import FakeClient
from test_entity_profile_workflow import _workflow


TODAY = date(2026, 9, 7)
BUYER = "alcaldia de baranoa"
ROUTER = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))

_SETTINGS = settings(
    max_examples=40, derandomize=True, deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)

SOURCE_WORDS = st.sampled_from(["procesos", "contratos", "licitaciones"])
TOPIC_HEADS = st.sampled_from(["gestion", "administracion", "publicacion", "control"])
TOPIC_TAILS = st.sampled_from(["procesos", "contratos", "convocatorias"])
SUPPORTED = st.sampled_from(["publicados", "firmados"])
UNSUPPORTED = st.sampled_from(["terminados", "liquidados", "iniciados", "cerrados"])
CURRENT = st.sampled_from(["actualmente activos", "hoy vigentes", "todavia abiertos"])
PERIODS = st.sampled_from(["en agosto", "en 2026", "en julio"])


def _turn(query: str, injected: dict | None = None):
    params = ROUTER.parse(query, today=TODAY).params
    params.update(injected or {})
    return classify_entity_profile_turn(query, params)


def _execute(turn):
    client = FakeClient()
    if turn.plan is None or turn.clarification:
        return client, None
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=client, soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )
    return client, profile


_WHERE_END = re.compile(r"\s+(?:GROUP BY|ORDER BY|LIMIT)\s", re.IGNORECASE)


def _where_only(soql: str) -> str:
    """Sólo el predicado.

    `GROUP BY modalidad_de_contratacion` no es un filtro de modalidad, y una
    columna del SELECT no es un filtro de nada. Cortar en la primera cláusula
    posterior es lo que impide que estas propiedades pasen por la razón
    equivocada.
    """
    where = soql.split(" WHERE ", 1)[1]
    end = _WHERE_END.search(where)
    return where[: end.start()] if end else where


def _wheres(client: FakeClient) -> list[tuple[str, str]]:
    return [(dataset, _where_only(soql)) for _, dataset, soql in client.calls]


# ── P13: una fuente EXPLÍCITA nunca se pierde en silencio ──────────────────

@_SETTINGS
@given(focus=st.sampled_from(["proveedores", "montos", "modalidades"]),
       period=st.sampled_from(["", "en agosto"]))
def test_p13_an_explicit_source_is_answered_or_asked_about(focus, period):
    turn = _turn(f"analiza {focus} de procesos y contratos de {BUYER} {period}".strip())
    assert turn.request.sources is RequestedSources.BOTH
    assert turn.request.sources_provenance is SourceProvenance.EXPLICIT
    client, _ = _execute(turn)
    if turn.clarification:
        assert client.calls == []
        return
    # Si se ejecuta, ninguna fuente explícita puede faltar.
    assert {dataset for dataset, _ in _wheres(client)} == {
        PROCESS_DATASET, CONTRACT_DATASET,
    }


# ── P14: dos hechos nombrados no colapsan ──────────────────────────────────

@_SETTINGS
@given(order=st.booleans(), period=PERIODS)
def test_p14_two_temporal_claims_never_collapse_silently(order, period):
    first, second = ("procesos publicados", "contratos firmados")
    if order:
        first, second = second, first
    turn = _turn(f"analiza {first} y {second} {period} de {BUYER}")
    claims = turn.request.claimed_events
    assert claims == {
        TemporalEvent.PROCESS_PUBLICATION, TemporalEvent.CONTRACT_SIGNATURE,
    }, "un escalar volvería a perder uno de los dos"
    client, _ = _execute(turn)
    if turn.clarification:
        assert client.calls == []
        return
    for dataset, where in _wheres(client):
        expected = (
            "fecha_de_publicacion_del >=" if dataset == PROCESS_DATASET
            else "fecha_de_firma >="
        )
        assert expected in where


# ── P15: el marcador actual sólo gobierna su cláusula ──────────────────────

@_SETTINGS
@given(event=UNSUPPORTED, current=CURRENT, period=PERIODS)
def test_p15_a_current_marker_cannot_erase_another_clauses_event(event, current, period):
    turn = _turn(f"analiza contratos {event} {period} y {current} de {BUYER}")
    assert turn.request.unsupported_events, (
        "el marcador de la segunda cláusula borró el hecho de la primera"
    )
    client, _ = _execute(turn)
    assert turn.clarification and client.calls == []


@_SETTINGS
@given(current=st.sampled_from(["actualmente", "hoy", "todavia"]), period=PERIODS)
def test_p15b_a_marker_in_its_own_clause_makes_it_a_state_not_an_event(current, period):
    turn = _turn(f"analiza procesos {current} publicados {period} de {BUYER}")
    assert turn.request.event is TemporalEvent.NONE
    client, _ = _execute(turn)
    if turn.clarification:
        return
    for dataset, where in _wheres(client):
        if dataset == PROCESS_DATASET:
            assert "estado_del_procedimiento" in where


# ── P16: una palabra de fuente dentro del tema no crea fuente ──────────────

@_SETTINGS
@given(head=st.sampled_from(["contratos", "procesos"]),
       modifier=TOPIC_HEADS, tail=TOPIC_TAILS)
def test_p16_a_source_word_inside_the_topic_creates_no_source(head, modifier, tail):
    turn = _turn(f"analiza {head} de {modifier} de {tail} industriales de {BUYER}")
    expected = (
        RequestedSources.CONTRACTS if head == "contratos"
        else RequestedSources.PROCESSES
    )
    assert turn.request.sources is expected
    client, _ = _execute(turn)
    if turn.clarification:
        return
    dataset = PROCESS_DATASET if head == "procesos" else CONTRACT_DATASET
    assert {ds for ds, _ in _wheres(client)} == {dataset}


# ── P17: filtro en el SQL implica filtro declarado ─────────────────────────

_MATERIAL_MARKERS = (
    ("estado_contrato", "Estado actual"),
    ("estado_del_procedimiento", "Estado actual"),
    ("modalidad_de_contratacion", "Modalidad"),
    ("tipo_de_contrato", "Tipo de contrato"),
    ("NOT (coalesce", "excepto"),
)


@pytest.mark.parametrize("phrase,injected", [
    ("contratos actualmente terminados", None),
    ("contratos de mantenimiento excepto interventoria", None),
    ("contratos por contratacion directa", None),
    ("contratos de obra", None),
    ("procesos actualmente publicados", None),
    ("contratos", {"objeto_or": ["mantenimiento", "vias"]}),
    ("contratos", {"objeto": ["mantenimiento", "vias"]}),
    ("contratos entre 100 y 500 millones", None),
])
def test_p17_every_material_filter_in_sql_is_declared_in_the_scope(phrase, injected):
    turn = _turn(f"analiza {phrase} de {BUYER}", injected)
    client, profile = _execute(turn)
    assert profile is not None
    declared = " ".join(scope_filter_lines(build_effective_scope(profile)))
    sql = "\n".join(where for _, where in _wheres(client))
    exercised = False
    for marker, visible in _MATERIAL_MARKERS:
        if marker in sql:
            exercised = True
            assert visible.casefold() in declared.casefold(), (marker, declared)
    if "valor_del_contrato >=" in sql or "precio_base >=" in sql:
        exercised = True
        assert "100" in declared
    if "LIKE" in sql:
        exercised = True
        assert "Tema:" in declared
    assert exercised, ("el caso no ejercitó ningún filtro material", sql)


# ── P18: AND y OR no pueden declararse igual ───────────────────────────────

@_SETTINGS
@given(terms=st.lists(
    st.sampled_from(["mantenimiento", "vias", "alumbrado", "acueducto"]),
    min_size=2, max_size=3, unique=True,
))
def test_p18_conjunction_and_disjunction_never_share_a_declaration(terms):
    query = f"analiza contratos de {BUYER}"
    conjunction = _turn(query, {"objeto": list(terms)})
    disjunction = _turn(query, {"objeto_or": list(terms)})
    and_client, and_profile = _execute(conjunction)
    or_client, or_profile = _execute(disjunction)
    assert and_client.calls[0][2] != or_client.calls[0][2], "mismo SQL: caso inválido"
    and_scope = build_effective_scope(and_profile)
    or_scope = build_effective_scope(or_profile)
    assert and_scope != or_scope
    assert scope_filter_lines(and_scope) != scope_filter_lines(or_scope)


# ── P19: las llamadas observadas son exactamente las planeadas ─────────────

@pytest.mark.parametrize("phrase", [
    "procesos y contratos publicados en agosto",
    "proveedores de procesos y contratos",
    "montos de procesos y contratos",
    "contratos firmados en agosto",
    "procesos publicados en agosto",
    "evolucion de contratos",
    "recientes de procesos",
    "contratos terminados en agosto",
])
def test_p19_observed_calls_exactly_equal_planned_calls(phrase):
    query = f"analiza {phrase} de {BUYER}"
    turn = _turn(query)
    planned = turn.plan.jobs if turn.plan and not turn.clarification else ()
    workflow, client = _workflow()
    workflow.run_query(query, today=TODAY)
    assert len(client.calls) == len(planned)
    assert sorted(c[1] for c in client.calls) == sorted(
        job.dataset_id for job in planned
    )


# ── P20: una aclaración pendiente no muta lo confirmado ────────────────────

@_SETTINGS
@given(focus=st.sampled_from(["proveedores", "montos"]),
       period=st.sampled_from(["", "en agosto"]))
def test_p20_a_pending_clarification_cannot_mutate_the_confirmed_frame(focus, period):
    chat = f"p20-{focus}-{len(period)}"
    workflow, _ = _workflow()
    first = workflow.run_query(
        f"analiza procesos de {BUYER} {period}".strip(), chat_id=chat, today=TODAY,
    )
    confirmed = EntityProfileConversationState.from_dict(
        first["resolved_params"].get(STATE_PARAM),
    ).confirmed
    assert confirmed is not None

    workflow, client = _workflow()
    second = workflow.run_query(focus, chat_id=chat, today=TODAY)
    assert second["needs_clarification"] and not client.calls
    held = EntityProfileConversationState.from_dict(
        second["resolved_params"].get(STATE_PARAM),
    )
    assert held.has_pending
    assert held.confirmed == confirmed


# ── P21: un follow-up ortogonal conserva las demás dimensiones ─────────────

@_SETTINGS
@given(follow_up=st.sampled_from(["en 2025", "en julio", "en 2024"]))
def test_p21_a_temporal_followup_preserves_focus_event_and_topic(follow_up):
    chat = f"p21-{follow_up.replace(' ', '')}"
    for query in (
        f"analiza contratos de mantenimiento firmados de {BUYER} en agosto",
        "proveedores",
    ):
        workflow, _ = _workflow()
        base = workflow.run_query(query, chat_id=chat, today=TODAY)
    before = EntityProfileConversationState.from_dict(
        base["resolved_params"].get(STATE_PARAM),
    ).confirmed

    workflow, _ = _workflow()
    after_result = workflow.run_query(follow_up, chat_id=chat, today=TODAY)
    after = EntityProfileConversationState.from_dict(
        after_result["resolved_params"].get(STATE_PARAM),
    ).confirmed
    assert after is not None and before is not None
    assert after.focus == before.focus
    assert after.sources is before.sources
    assert after.claimed_events == before.claimed_events
    assert after.time_from != before.time_from


# ── P22: un conteo inválido nunca se vuelve un hecho entero ────────────────

@_SETTINGS
@given(raw=st.one_of(
    st.integers(min_value=1, max_value=99).filter(lambda n: n % 10).map(
        lambda n: str(n / 10),
    ),
    st.integers(min_value=-999, max_value=-1).map(str),
    st.sampled_from(["nan", "inf", "-inf", "unknown", "", "n/a", "  ", "1,5"]),
))
def test_p22_an_invalid_count_never_becomes_an_integer_fact(raw):
    assert as_optional_int(raw) is None
    assert parse_process_activity([{"process_count": raw}]).process_count is None
    assert merge_periods([{"period": "2026", "row_count": raw}], None)[0].process_count is None
    assert merge_periods(None, [{"period": "2026", "row_count": raw}])[0].contract_count is None


@_SETTINGS
@given(count=st.integers(min_value=0, max_value=10**9))
def test_p22b_an_exact_count_survives_however_the_source_serialized_it(count):
    for raw in (str(count), f"{count}.0", count):
        assert as_optional_int(raw) == count


# ── P23: un cero desmentido por evidencia del mismo scope es inconsistente ─

@pytest.mark.parametrize("dataset,zero_call,witness_call", [
    (PROCESS_DATASET, "process_activity", "recent_processes"),
    (CONTRACT_DATASET, "contract_values", "recent_contracts"),
])
def test_p23_a_zero_denied_by_same_scope_evidence_is_inconsistent(
    dataset, zero_call, witness_call,
):
    class Client(FakeClient):
        def aggregate(self, ds, soql):
            self._record("aggregate", ds, soql)
            if "GROUP BY modalidad_de_contratacion" in soql:
                return ([{"modalidad_de_contratacion": "Directa", "process_count": "0"}]
                        if dataset == PROCESS_DATASET
                        else super().aggregate(ds, soql))
            if "AS contract_count" in soql and "GROUP BY" not in soql:
                return ([{"contract_count": "0", "total_contract_value": None}]
                        if dataset == CONTRACT_DATASET
                        else super().aggregate(ds, soql))
            return super().aggregate(ds, soql)

    turn = _turn(f"analiza {BUYER} en 2026")
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus="overview", secop_client=Client(), soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )
    rendered = render_entity_profile(profile)
    fact = "process_count" if dataset == PROCESS_DATASET else "contract_count"
    denied = "0 procesos" if dataset == PROCESS_DATASET else "0 contratos"
    assert profile.activity.fact(fact) is None
    assert denied not in rendered
    assert profile.activity.status is not SectionStatus.SUPPORTED
    assert any("contradictorios" in warning for warning in profile.warnings)
