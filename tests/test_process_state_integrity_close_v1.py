"""PROCESS_STATE_INTEGRITY_CLOSE_V1.

PSRI (PROCESS_STATE_REQUEST_INTEGRITY_V1 + SINGLE_REQUEST_V1) decides process
states by cardinality: two or more explicit readings clarify. That turned the
real turn "que procesos en convocatoria o publicados tiene la alcaldia de
manizales?" (data/feedback.jsonl, 2026-05-13) into a clarification, although
main has served it as the open funnel since May.

"<open funnel> o <literal of that funnel>" — exactly two mentions, a bare
"o"/"u" between them — is one request for the funnel. The oracle is the
effective query: the turn reaches the source exactly as the funnel alone does.
That keeps the behavior the user already has; it is not a set union, because
O1B gives the funnel a current-opportunity policy a lone "publicados" lacks.
"""
from datetime import date
from pathlib import Path

import pytest

from app.config import Settings, get_settings
from app.core.conversation_store import ConversationStore
from app.core.estado_families import read_process_state_mentions
from app.core.orchestrator import SecopalWorkflow
from itertools import count

from app.core.estado_families import ESTADO_FAMILIES, _MENTION_TOKENS, _process_reading
from app.core.estado_projection import legacy_estado_scalar
from app.core.process_state_request import (
    CLARIFICATION_KEY, FUNNEL_FAMILIES, OPPORTUNITY_HEAD_SURFACES, ProcessStateDecision,
    apply_process_state_decision, decide_process_states,
)
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter

TODAY = date(2026, 9, 23)
MANIZALES = 'que procesos en convocatoria o publicados tiene la alcaldia de manizales?'


@pytest.fixture(scope='module')
def router():
    from app.core.entity_resolver import EntityResolver
    return QueryRouter(entity_resolver=EntityResolver(Path('app/data/aliases_db.json')))


def plan(router, text):
    params = router.parse(text, today=TODAY).params
    return params, compute_query_plan(params, text, {'today': TODAY})


def decide(text):
    return decide_process_states(read_process_state_mentions(text), set(), text)


# ── The funnel absorbs its own literal under a bare "o"/"u" ──────────────

@pytest.mark.parametrize('text', [
    'procesos en convocatoria o publicados de obra',
    'procesos publicados o en convocatoria de obra',
    'procesos abiertos o publicados de obra',
    'procesos abiertos o borrador',
    'procesos borrador u abiertas',
])
def test_funnel_or_its_literal_is_the_funnel(text):
    decision = decide(text)
    assert decision.clarification is None, decision
    assert decision.literal is None, decision
    assert decision.family == 'oferta_abierta' and decision.funnel, decision


@pytest.mark.parametrize('text', [
    'procesos publicados o adjudicados de obra',        # another family
    'procesos abiertos o aprobados de obra',            # "Aprobado" reads as en_evaluacion
    'procesos abiertos o cancelados',                   # another family
    'procesos adjudicados o seleccionados',             # its own literal, but not the funnel
    'procesos abiertos y publicados de obra',           # conjunction, not "o"
    'procesos abiertos, publicados de obra',            # comma
    'procesos abiertos publicados de obra',             # juxtaposition (PSRI narrowed)
    'procesos abiertos o publicados o cancelados',      # three mentions
    'procesos abiertos o (abiertos y aprobados) de obra',  # ZEUS PSRI R3.1
])
def test_everything_else_keeps_psri_cardinality(text):
    assert decide(text).clarification, text


def test_without_the_text_cardinality_alone_decides():
    text = 'procesos en convocatoria o publicados de obra'
    assert decide_process_states(read_process_state_mentions(text), set()).clarification


STATE_KEYS = ('estado', 'estado_field', 'estado_family', 'estado_del_procedimiento',
              'estado_de_apertura_del_proceso', 'intent_type')

PAIRS = [
    (MANIZALES, 'que procesos en convocatoria tiene la alcaldia de manizales?'),
    ('procesos en convocatoria o publicados de obra', 'procesos en convocatoria de obra'),
    ('procesos publicados o en convocatoria de obra', 'procesos en convocatoria de obra'),
    ('procesos abiertos o publicados de obra', 'procesos abiertos de obra'),
    # ZEUS NO_GO on 4402f1e1: the literal first left its own filter behind.
    ('procesos publicados o abiertos de obra', 'procesos abiertos de obra'),
]


@pytest.mark.parametrize('absorbed, funnel_alone', PAIRS)
def test_absorbed_turn_plans_the_funnel(router, absorbed, funnel_alone):
    params, result = plan(router, absorbed)
    alone, _ = plan(router, funnel_alone)
    assert CLARIFICATION_KEY not in params, params
    assert params.get('estado_family') == 'oferta_abierta', params
    assert params.get('intent_type') == 'opportunity_search', params
    assert {k: params.get(k) for k in STATE_KEYS} == {k: alone.get(k) for k in STATE_KEYS}
    assert result.should_execute, result.reason


# ── Oracle: the effective query ───────────────────────────────────────────

@pytest.fixture
def flow(tmp_path, monkeypatch):
    get_settings.cache_clear()
    settings = Settings(deepseek_api_key=None,
                        secop_feedback_path=tmp_path / 'feedback.jsonl',
                        secop_feedback_db_path=tmp_path / 'feedback.sqlite',
                        secop_feedback_sqlite_enabled=False)
    workflow = SecopalWorkflow(settings)
    workflow.conv_store = ConversationStore(Path(tmp_path) / 'conversations')
    monkeypatch.setattr(workflow.narrator, 'narrate_with_grounding', lambda **kwargs: None)
    calls = []

    def record(dataset, soql_text, *args, **kwargs):
        calls.append((dataset, str(soql_text)))
        return []

    client = workflow.secop_client
    monkeypatch.setattr(client, 'query', record)
    monkeypatch.setattr(client, 'aggregate', record)
    monkeypatch.setattr(client, 'count', lambda *a, **k: (record(*a, **k), 0)[1])
    monkeypatch.setattr(client, 'with_timeout', lambda *a, **k: client)
    monkeypatch.setattr(client, 'derived_for_timeout', lambda *a, **k: client)
    workflow.source_calls = calls
    return workflow


def source_calls(flow, text, chat_id):
    flow.source_calls.clear()
    result = flow.run_query(text, chat_id=chat_id, today=TODAY)
    assert not result.get('needs_clarification'), result
    return list(flow.source_calls)


@pytest.mark.parametrize('absorbed, funnel_alone', PAIRS[1:])
def test_absorbed_turn_reaches_the_source_as_the_funnel_alone(flow, absorbed, funnel_alone):
    calls = source_calls(flow, absorbed, 'psc-absorbed')
    assert calls, calls
    assert calls == source_calls(flow, funnel_alone, 'psc-funnel')


def test_manizales_reaches_the_source_with_the_funnel(flow):
    calls = source_calls(flow, MANIZALES, 'psc-manizales')
    assert calls, calls
    query = calls[0][1]
    assert "'MUNICIPIO DE MANIZALES'" in query, query
    assert "estado_del_procedimiento IN ('Publicado', 'Borrador', 'En aprobación', 'Aprobado', 'Abierto')" in query, query


def test_another_family_still_clarifies_with_no_source_call(flow):
    result = flow.run_query('procesos publicados o adjudicados de obra', chat_id='psc-other', today=TODAY)
    assert result.get('needs_clarification'), result
    assert flow.source_calls == []


# ── R1 (ZEUS NO_GO on 4402f1e1): the decision is applied whole ───────────
# The producer answers with the first state it reads, a multiword form before
# any single word; the family projection respects any filter it finds. So a
# funnel decision over "publicados o abiertos" still sent 'Publicado' alone.
# The pairs are enumerated from the lexicon, not sampled: every pair the
# exception admits, in both orders (266 on 4402f1e1, 128 of them wrong).

def _admitted_surfaces():
    funnel, literals = [], []
    for token in sorted(_MENTION_TOKENS):
        literal, _, family = _process_reading(token)
        if family in FUNNEL_FAMILIES and token not in OPPORTUNITY_HEAD_SURFACES:
            (funnel if literal is None else literals).append(token)
    return funnel, literals


FUNNEL_SURFACES, FUNNEL_LITERALS = _admitted_surfaces()


def test_the_enumeration_is_not_vacuous():
    assert {'abiertos', 'en convocatoria', 'vigentes'} <= set(FUNNEL_SURFACES)
    assert {'publicados', 'borrador', 'en borrador'} <= set(FUNNEL_LITERALS)


def both_orders(funnel, literal, suffix):
    return (f'procesos {funnel} o {literal} {suffix}', f'procesos {literal} o {funnel} {suffix}')


@pytest.mark.parametrize('funnel', FUNNEL_SURFACES)
def test_every_admitted_pair_plans_the_funnel_alone(router, funnel):
    alone, _ = plan(router, f'procesos {funnel} de obra')
    expected = {k: alone.get(k) for k in STATE_KEYS}
    for literal in FUNNEL_LITERALS:
        for text in both_orders(funnel, literal, 'de obra'):
            params, result = plan(router, text)
            assert {k: params.get(k) for k in STATE_KEYS} == expected, text
            assert result.should_execute, (text, result.reason)


_CHATS = count()


@pytest.mark.parametrize('suffix', ['de obra', 'de la alcaldia de manizales en 2026'])
def test_every_admitted_pair_reaches_the_source_as_the_funnel_alone(flow, suffix):
    wrong = []
    for funnel in FUNNEL_SURFACES:
        alone = source_calls(flow, f'procesos {funnel} {suffix}', f'psc-r1-{next(_CHATS)}')
        assert alone, funnel
        for literal in FUNNEL_LITERALS:
            for text in both_orders(funnel, literal, suffix):
                if source_calls(flow, text, f'psc-r1-{next(_CHATS)}') != alone:
                    wrong.append(text)
    assert not wrong, wrong


def test_the_other_conditions_survive_the_absorption(flow):
    query = source_calls(flow, 'procesos publicados o abiertos de la alcaldia de manizales en 2026',
                         'psc-r1-conditions')[0][1]
    assert "entidad = 'MUNICIPIO DE MANIZALES'" in query, query
    assert "fecha_de_publicacion_del >= '2026-01-01'" in query, query
    assert "fecha_de_publicacion_del <= '2026-12-31'" in query, query


@pytest.mark.parametrize('literal', FUNNEL_LITERALS)
def test_a_lone_literal_of_the_funnel_stays_exact(flow, literal):
    value = _process_reading(literal)[0]
    query = source_calls(flow, f'procesos {literal} de obra', f'psc-r1-{next(_CHATS)}')[0][1]
    assert f"estado_del_procedimiento = '{value}'" in query, query


def test_a_family_decision_replaces_what_the_producer_left():
    params = {'dataset': 'procesos', 'estado_del_procedimiento': ['Publicado'], 'estado': 'Publicado',
              'estado_field': 'estado_del_procedimiento', 'estado_family': 'oferta_abierta',
              'entidad': 'MUNICIPIO DE MANIZALES', 'fecha_desde': '2026-01-01'}
    apply_process_state_decision(params, ProcessStateDecision(family='oferta_abierta', funnel=True))
    members = ESTADO_FAMILIES['oferta_abierta']['procesos']['estado_del_procedimiento']
    assert params['estado_del_procedimiento'] == list(members)
    assert (params['estado'], params['estado_field']) == (members[0], 'estado_del_procedimiento')
    assert params['entidad'] == 'MUNICIPIO DE MANIZALES' and params['fecha_desde'] == '2026-01-01'


def test_a_decision_that_decides_nothing_leaves_the_params_alone():
    params = {'dataset': 'procesos', 'estado': 'Publicado', 'estado_del_procedimiento': ['Publicado']}
    apply_process_state_decision(params, ProcessStateDecision())
    assert params == {'dataset': 'procesos', 'estado': 'Publicado', 'estado_del_procedimiento': ['Publicado']}


def test_the_legacy_scalar_keeps_the_producers_precedence():
    # contrato_activo projects two process fields; the producer always took
    # the opening state first. The decision and the producer share this rule.
    filters = {field: list(values)
               for field, values in ESTADO_FAMILIES['contrato_activo']['procesos'].items() if values}
    assert set(filters) == {'estado_de_apertura_del_proceso', 'estado_del_procedimiento'}
    assert legacy_estado_scalar(filters) == (
        filters['estado_de_apertura_del_proceso'][0], 'estado_de_apertura_del_proceso')

