"""PROCESS_STATE_REQUEST_INTEGRITY_V1.

INV-PROCESS-STATE-NO-SILENT-LOSS: an explicit process state is represented as
an exact compatible filter or the turn clarifies. It never disappears, never
becomes topic and never selects the contract dataset by itself.

Source and state are independent dimensions: the head that names the source
("procesos", "oportunidades", "contratos") owns the dataset; a bare state word
only narrows it.
"""
from datetime import date
from pathlib import Path

import pytest

from app.config import Settings, get_settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder

TODAY = date(2026, 9, 23)
STATE_KEYS = ('estado_de_apertura_del_proceso', 'estado_del_procedimiento')


@pytest.fixture(scope='module')
def router():
    from app.core.entity_resolver import EntityResolver
    return QueryRouter(entity_resolver=EntityResolver(Path('app/data/aliases_db.json')))


def parse(router, text):
    return router.parse(text, today=TODAY).params


def plan(router, text):
    params = parse(router, text)
    return params, compute_query_plan(params, text, {'today': TODAY})


def soql(params):
    return SoQLBuilder().build(SoQLBuilder.dataset_id_for(str(params['dataset'])), params)


def state_values(params):
    values = set()
    for key in STATE_KEYS:
        values.update(params.get(key) or ())
    if params.get('estado') and not any(params.get(key) for key in STATE_KEYS):
        values.add(params['estado'])
    return values


def topic(params):
    return ' '.join(str(term) for term in params.get('objeto') or ())


# ── Single explicit state: exact, on processes, never topic ────────────────

@pytest.mark.parametrize('text, literal, field, word', [
    ('procesos cerrados de mantenimiento en atlantico', 'Cerrado',
     'estado_de_apertura_del_proceso', 'cerrad'),
    ('procesos cancelados de construccion en antioquia', 'Cancelado',
     'estado_del_procedimiento', 'cancelad'),
    ('procesos seleccionados de alimentacion en bolivar', 'Seleccionado',
     'estado_del_procedimiento', 'seleccionad'),
    ('procesos aprobados de obra en sucre', 'Aprobado',
     'estado_del_procedimiento', 'aprobad'),
])
def test_explicit_process_state_is_exact_and_not_topic(router, text, literal, field, word):
    params = parse(router, text)
    assert params['dataset'] == 'procesos'
    assert state_values(params) == {literal}, params
    assert word not in topic(params), params
    assert not params.get('contract_universe')
    assert '_process_state_clarification' not in params
    # "bolivar"/"sucre" are also municipalities: the builder refuses that
    # pre-existing ambiguity, so the SoQL is checked on the same state request.
    where = soql(parse(router, text.rsplit(' en ', 1)[0]))
    assert f"'{literal}'" in where
    assert field in where


@pytest.mark.parametrize('text, literal, wider', [
    ('procesos cancelados de construccion', 'Cancelado', 'Suspendido'),
    ('procesos suspendidos de construccion', 'Suspendido', 'Cancelado'),
    ('procesos seleccionados de obra', 'Seleccionado', 'Evaluación'),
    ('procesos aprobados de obra', 'Aprobado', 'Evaluación'),
    ('procesos en evaluacion de obra', 'Evaluación', 'Seleccionado'),
])
def test_exact_literal_does_not_widen_to_family(router, text, literal, wider):
    params = parse(router, text)
    assert state_values(params) == {literal}, params
    assert f"'{wider}'" not in soql(params)


@pytest.mark.parametrize('surface', [
    'cancelados', 'canceladas', 'cancelado', 'cancelada',
    'aprobados', 'aprobadas', 'seleccionados', 'seleccionadas',
])
def test_every_inflection_names_the_same_literal(router, surface):
    params = parse(router, f'procesos {surface} de obra')
    assert len(state_values(params)) == 1, params
    assert surface[:6] not in topic(params), params


def test_conceptual_family_still_projects_family(router):
    # "no procedieron"-style concepts keep the family; only literals narrow.
    params = parse(router, 'procesos desiertos de obra')
    assert params.get('estado_family') == 'no_procede'
    assert state_values(params) == {'Cancelado', 'Suspendido'}


# ── Multi-state V1: clarify, never partial, never topic, never contracts ───

@pytest.mark.parametrize('text', [
    'procesos aprobados y cancelados de construccion',
    'oportunidades aprobadas y canceladas de construccion',
    'procesos seleccionados y cancelados de obra',
    'procesos cancelados y aprobados de construccion',
])
def test_two_explicit_states_clarify_on_processes(router, text):
    params, result = plan(router, text)
    assert params['dataset'] == 'procesos', params
    assert not params.get('contract_universe'), params
    assert result.should_execute is False
    assert result.reason == 'process_state_unrepresentable'
    for word in ('aprobad', 'cancelad', 'seleccionad'):
        assert word not in topic(params), params
    # No partial state survives into the persisted frame; the question names both.
    assert not state_values(params) and not params.get('estado_family'), params
    assert 'Cancelado' in result.clarification_question


# ── Opportunity + incompatible explicit state: clarify on processes ────────

@pytest.mark.parametrize('text, literal', [
    ('oportunidades canceladas de construccion en antioquia', 'Cancelado'),
    ('oportunidades cerradas de mantenimiento en atlantico', 'Cerrado'),
])
def test_opportunity_with_incompatible_state_clarifies(router, text, literal):
    params, result = plan(router, text)
    assert params['dataset'] == 'procesos', params
    assert params.get('intent_type') == 'opportunity_search', params
    assert not params.get('contract_universe'), params
    assert not params.get('estado_contrato'), params
    assert result.should_execute is False
    assert result.reason == 'process_state_unrepresentable'
    assert 'cancelad' not in topic(params) and 'cerrad' not in topic(params)
    assert literal in result.clarification_question
    assert not state_values(params), params


def test_opportunity_with_compatible_literal_executes_exact(router):
    params, result = plan(router, 'oportunidades publicadas de obra en antioquia')
    assert params['dataset'] == 'procesos'
    assert state_values(params) == {'Publicado'}
    assert result.reason != 'process_state_unrepresentable'


def test_family_concept_plus_foreign_literal_clarifies(router):
    params, result = plan(router, 'procesos abiertos y cancelados de obra')
    assert result.should_execute is False
    assert result.reason == 'process_state_unrepresentable'


# ── Controls: contracts, modality ownership, dual scope ────────────────────

def test_contract_closed_keeps_contract_semantics(router):
    params = parse(router, 'contratos cerrados de mantenimiento')
    assert params['dataset'] == 'contratos'
    assert params['estado_contrato'] == ['Cerrado']
    assert params['contract_universe'] == 'celebrated'
    assert '_process_state_clarification' not in params


def test_contract_cancelled_records_keep_source_records(router):
    params = parse(router, 'contratos cancelados de construccion')
    assert params['dataset'] == 'contratos'
    assert params['contract_universe'] == 'source_records'
    assert params['estado_contrato'] == ['Cancelado']


def test_modality_owns_sobre_cerrado(router):
    params = parse(router, 'enajenacion de bienes con sobre cerrado')
    assert params['modalidad_include_procedure_ids'] == ['enajenacion_sobre_cerrado']
    assert not state_values(params), params
    assert '_process_state_clarification' not in params


def test_dual_scope_is_preserved(router):
    params = parse(router, 'procesos o contratos cancelados de construccion')
    assert params['dataset_scope'] == ['procesos', 'contratos']
    assert params['dataset'] == 'procesos'
    assert not params.get('contract_universe')


# ── End to end: zero source calls when the conflict is detectable ─────────

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
        calls.append((dataset, soql_text))
        return []

    client = workflow.secop_client
    monkeypatch.setattr(client, 'query', record)
    monkeypatch.setattr(client, 'aggregate', record)
    monkeypatch.setattr(client, 'count', lambda *a, **k: (record(*a, **k), 0)[1])
    monkeypatch.setattr(client, 'with_timeout', lambda *a, **k: client)
    monkeypatch.setattr(client, 'derived_for_timeout', lambda *a, **k: client)
    workflow.source_calls = calls
    return workflow


@pytest.mark.parametrize('text', [
    'procesos aprobados y cancelados de construccion',
    'oportunidades canceladas de construccion en antioquia',
    'oportunidades aprobadas y canceladas de construccion',
    'oportunidades cerradas de mantenimiento en atlantico',
])
def test_conflict_reaches_no_source(flow, text):
    result = flow.run_query(text, chat_id='psri', today=TODAY)
    assert result.get('needs_clarification'), result
    assert flow.source_calls == []
    assert result['resolved_params'].get('dataset') == 'procesos'


def test_cancelled_processes_query_processes_only(flow):
    flow.run_query('procesos cancelados de construccion en antioquia',
                   chat_id='psri', today=TODAY)
    assert flow.source_calls
    for dataset, soql_text in flow.source_calls:
        assert dataset == SoQLBuilder.PROCESOS_DATASET
        assert "'Cancelado'" in soql_text and "'Suspendido'" not in soql_text


def test_blocker_does_not_poison_the_next_turn(flow):
    flow.run_query('procesos aprobados y cancelados de construccion', chat_id='psri', today=TODAY)
    result = flow.run_query('procesos cancelados de construccion en antioquia',
                            chat_id='psri', today=TODAY)
    # The empty synthetic source answers "no results"; what matters is that
    # the previous turn's blocker did not stop this one from reaching it.
    assert 'estado de proceso' not in result['response']
    assert flow.source_calls
    for dataset, soql_text in flow.source_calls:
        assert dataset == SoQLBuilder.PROCESOS_DATASET
        assert "'Cancelado'" in soql_text and "'Aprobado'" not in soql_text
        assert 'Antioquia' in soql_text


# ── Follow-ups: the blocker is turn evidence, the answer stays exact ──────

def test_followup_multi_state_blocks_before_source(flow):
    flow.run_query('procesos de construccion en antioquia', chat_id='psri', today=TODAY)
    flow.source_calls.clear()
    result = flow.run_query('ahora los seleccionados y cancelados', chat_id='psri', today=TODAY)
    assert result.get('needs_clarification')
    assert 'Seleccionado' in result['response'] and 'Cancelado' in result['response']
    assert flow.source_calls == []


def test_answer_to_clarification_is_exact(flow):
    flow.run_query('procesos aprobados y cancelados de construccion', chat_id='psri', today=TODAY)
    result = flow.run_query('los cancelados', chat_id='psri', today=TODAY)
    params = result['resolved_params']
    assert params['dataset'] == 'procesos'
    assert params['estado_del_procedimiento'] == ['Cancelado'], params
    for _dataset, soql_text in flow.source_calls:
        assert "'Suspendido'" not in soql_text


def test_clarified_frame_does_not_block_next_followup(flow):
    flow.run_query('procesos aprobados y cancelados de construccion', chat_id='psri', today=TODAY)
    result = flow.run_query('en antioquia', chat_id='psri', today=TODAY)
    assert 'estado de proceso' not in result['response']
    assert flow.source_calls
    for _dataset, soql_text in flow.source_calls:
        assert "'Aprobado'" not in soql_text  # no partial state was persisted


# ── V1.1: one occurrence authority for producer and guard ─────────────────
#
# ZEUS broke V1 with "procesos cancelados y adjudicados": the guard enumerated
# only literals, "adjudicados" is family vocabulary the producer reads, and the
# verdict depended on word order. The mentions now come from the producers'
# own lexicon and reading (estado_families.read_process_state_mentions).

from app.core.estado_families import (  # noqa: E402
    FAMILY_TOKENS,
    _LEGACY_PROCESS_SYNONYMS,
    _MENTION_TOKENS,
    detect_estado_family,
    read_process_state_mentions,
)
from app.core.process_state_request import FUNNEL_FAMILIES  # noqa: E402


@pytest.mark.parametrize('text', [
    'procesos cancelados y adjudicados de obra',
    'procesos adjudicados y cancelados de obra',
    'procesos cancelados y desiertos de obra',
    'procesos adjudicados y en evaluacion de obra',
    'procesos o contratos aprobados y seleccionados de obra',
])
def test_family_mention_is_a_state_request(router, text):
    params, result = plan(router, text)
    assert result.should_execute is False, params
    assert result.reason == 'process_state_unrepresentable'
    assert not state_values(params) and not params.get('estado_family'), params
    for word in ('adjudicad', 'desiert', 'evaluacion', 'cancelad'):
        assert word not in topic(params), params


def test_literal_inside_a_seeded_funnel_is_applied_exactly(router):
    # "convocatorias" is the opportunity head (like "oportunidades"), not a
    # second state: Publicado is the only explicit reading (SINGLE_REQUEST_V1).
    params, result = plan(router, 'convocatorias publicadas de obra')
    assert state_values(params) == {'Publicado'}, params
    assert result.reason != 'process_state_unrepresentable'
    assert params.get('intent_type') == 'opportunity_search'
    assert "'Borrador'" not in soql(params)


def test_opportunity_with_non_funnel_family_clarifies(router):
    params, result = plan(router, 'oportunidades adjudicadas de obra')
    assert result.reason == 'process_state_unrepresentable'
    assert params['dataset'] == 'procesos'


def _representatives():
    """One single-word surface per distinct producer reading."""
    by_reading = {}
    for token in sorted(_MENTION_TOKENS):
        if ' ' in token:
            continue
        mentions = read_process_state_mentions(token)
        if len(mentions) == 1:
            by_reading.setdefault(mentions[0].reading, (token, mentions[0]))
    return list(by_reading.values())


REPRESENTATIVES = _representatives()


def test_representatives_cover_literals_and_families():
    readings = {mention.reading for _token, mention in REPRESENTATIVES}
    assert {'Cancelado', 'Aprobado', 'Seleccionado', 'Cerrado'} <= readings
    assert {'familia:en_evaluacion', 'familia:no_procede', 'familia:oferta_abierta'} <= readings


def _human_plural(token):
    """The masculine plural a person writes after "procesos": aprobado -> aprobados."""
    if token.endswith('os') or token.endswith('es'):
        return token
    if token.endswith('as'):
        return token[:-2] + 'os'
    if token.endswith(('o', 'e')):
        return token + 's'
    if token.endswith('a'):
        return token[:-1] + 'os'
    return token + 'es'


def _human_representatives():
    """One concordant surface per distinct reading ("abiertos", not "abierta")."""
    by_reading = {}
    for token, mention in REPRESENTATIVES:
        # Adjectives agree with "procesos"; a noun state is written "en ...".
        adjective = token.endswith(('o', 'a', 'os', 'as'))
        candidates = (_human_plural(token),) if adjective else (f'en {token}',)
        chosen = token
        for candidate in candidates:
            read = read_process_state_mentions(candidate)
            if len(read) == 1 and read[0].reading == mention.reading:
                chosen = candidate
        by_reading[mention.reading] = (chosen, mention)
    return list(by_reading.values())


HUMAN_REPRESENTATIVES = _human_representatives()
HUMAN_PAIRS = [
    (a, b) for i, a in enumerate(HUMAN_REPRESENTATIVES) for b in HUMAN_REPRESENTATIVES[i + 1:]
]


def executable_signature(router, text):
    """Everything word order could change downstream of the parse (R3).

    Taken on the PLANNED params: P4 and every later writer count. Explanations
    are not compared; the executable meaning is.
    """
    from app.core.estado_projection import STATE_FILTER_FIELDS
    params, result = plan(router, text)
    planned = result.params
    question = result.clarification_question or ''
    kind = ('multi' if 'más de un estado' in question
            else 'family_conflict' if 'también pide' in question
            else 'other' if question else 'none')
    where = soql(planned) if result.should_execute else None
    return (
        result.should_execute, result.reason, kind,
        planned.get('dataset'), tuple(planned.get('dataset_scope') or ()),
        planned.get('intent_type'), planned.get('estado_family'),
        planned.get('estado'), planned.get('estado_field'),
        tuple((key, tuple(planned.get(key) or ())) for key in STATE_FILTER_FIELDS),
        where,
    )


def test_human_representatives_are_concordant():
    surfaces = {token for token, _mention in HUMAN_REPRESENTATIVES}
    assert {'abiertos', 'aprobados', 'cancelados'} <= surfaces, surfaces
    assert len(HUMAN_REPRESENTATIVES) == len(REPRESENTATIVES)


# PROCESS_HEAD_CONTRACT_LIFECYCLE_ROUTING_V1: once "en ejecución" has moved the
# turn to contratos, "desiertos" is topic in one order only (identical on main
# f70dd9d5). The contract path is outside the process-state authority.
_CONTRACT_ROUTED_ORDER_DEPENDENT = {('desiertos', 'en ejecucion')}


def _order_params():
    for first, second in HUMAN_PAIRS:
        marks = ()
        if (first[0], second[0]) in _CONTRACT_ROUTED_ORDER_DEPENDENT:
            marks = pytest.mark.xfail(strict=True, reason='PROCESS_HEAD_CONTRACT_LIFECYCLE_ROUTING_V1')
        yield pytest.param(first, second, marks=marks, id=f'{first[0]}-{second[0]}')


@pytest.mark.parametrize('first, second', list(_order_params()))
def test_verdict_does_not_depend_on_word_order(router, first, second):
    (a, _mention_a), (b, _mention_b) = first, second
    forward = executable_signature(router, f'procesos {a} y {b} de obra')
    backward = executable_signature(router, f'procesos {b} y {a} de obra')
    assert forward == backward, (a, b)


# PROCESS_HEAD_CONTRACT_LIFECYCLE_ROUTING_V1 (pre-existing on main f70dd9d5):
# "procesos en ejecución" is routed to contratos before the process-state
# authority runs, so a second process state there becomes topic. Order-
# symmetric, and a dataset decision, not a state one: frozen, not fixed here.
_CONTRACT_LIFECYCLE_ROUTED = 'en ejecucion'


def _never_partial_params():
    # SINGLE_REQUEST_V1: the open funnel is no exception. Two explicit
    # readings clarify even when one would contain the other.
    for first, second in HUMAN_PAIRS:
        marks = ()
        # An open-funnel word keeps the head on procesos ("abiertos y en
        # ejecución" clarifies there); only the other pairs move to contratos.
        funnel = {m.family for m in (first[1], second[1]) if not m.literal} & FUNNEL_FAMILIES
        if _CONTRACT_LIFECYCLE_ROUTED in (first[0], second[0]) and not funnel:
            marks = pytest.mark.xfail(strict=True, reason='PROCESS_HEAD_CONTRACT_LIFECYCLE_ROUTING_V1')
        yield pytest.param(first[0], second[0], marks=marks, id=f'{first[0]}-{second[0]}')


@pytest.mark.parametrize('a, b', list(_never_partial_params()))
def test_two_explicit_readings_never_execute(router, a, b):
    signature = executable_signature(router, f'procesos {a} y {b} de obra')
    assert signature[1] == 'process_state_unrepresentable', signature[:10]


# ── R3: the shared decision is the producer, not an auditor ──────────────
#
# R3 used to narrow "abiertos y aprobados" to Aprobado ∧ Abierto. ZEUS (R3.1)
# showed that is composition: "abiertos o (abiertos y aprobados)" collapsed to
# the same intersection. SINGLE_REQUEST_V1 clarifies both, in both orders.

@pytest.mark.parametrize('text', [
    'procesos abiertos y aprobados de obra',
    'procesos aprobados y abiertos de obra',
    'procesos abiertos aprobados de obra',
    'procesos abiertos publicados de obra',
])
def test_open_funnel_plus_explicit_literal_clarifies(router, text):
    params, result = plan(router, text)
    assert result.should_execute is False, params
    assert result.reason == 'process_state_unrepresentable'
    assert 'más de un estado' in result.clarification_question
    assert not state_values(params) and not params.get('estado_family'), params
    assert params.get('intent_type') is None, params


def test_open_approved_orders_emit_the_same_verdict(router):
    forward = executable_signature(router, 'procesos abiertos y aprobados de obra')
    backward = executable_signature(router, 'procesos aprobados y abiertos de obra')
    assert forward == backward
    assert forward[1] == 'process_state_unrepresentable'


def test_lemma_reading_widens_recognition_not_priority():
    # R2 made "aprobados" family vocabulary (lemma -> en_evaluacion). That is
    # recognition: the producer now sees it first in "aprobados y abiertos".
    produced = detect_estado_family('procesos aprobados y abiertos de obra', 'procesos')
    assert produced['family'] == 'en_evaluacion'
    assert produced['consumed_span'] == 'aprobados'
    (mention,) = read_process_state_mentions('aprobados')
    assert (mention.literal, mention.family) == ('Aprobado', 'en_evaluacion')


def test_funnel_literal_alone_keeps_its_opportunity_intent(router):
    # HCA-INTENT-012: "publicados" is a literal of the open funnel, so it is
    # an opportunity search on its own (CORPUS-F006/OC001).
    _params, result = plan(router, 'procesos publicados de dotacion escolar')
    assert result.params.get('intent_type') == 'opportunity_search'
    assert result.params.get('estado_family') == 'oferta_abierta'
    assert result.params.get('estado_del_procedimiento') == ['Publicado']


def test_literal_alone_invents_no_opportunity(router):
    _params, result = plan(router, 'procesos aprobados de software')
    assert result.params.get('intent_type') is None
    assert result.params.get('estado_family') == 'en_evaluacion'
    assert result.params.get('estado_del_procedimiento') == ['Aprobado']


def test_independent_opportunity_intent_survives_the_state_decision(router):
    # "oportunidades" sets the intent; the state decision may not remove it,
    # neither when it executes nor when it clarifies.
    _params, executed = plan(router, 'oportunidades aprobadas de software')
    assert executed.params.get('intent_type') == 'opportunity_search'
    assert executed.params.get('estado_del_procedimiento') == ['Aprobado']
    _params, clarified = plan(router, 'oportunidades canceladas de software')
    assert clarified.reason == 'process_state_unrepresentable'
    assert clarified.params.get('intent_type') == 'opportunity_search'


@pytest.mark.parametrize('first, second', [
    ('abiertos', 'cancelados'),
    ('abiertos', 'adjudicados'),
])
def test_clarified_frame_carries_no_positional_intent(router, first, second):
    # The clarified frame persists as context: the funnel's intent is not an
    # independent request, so neither order may keep it.
    for text in (f'procesos {first} y {second} de obra', f'procesos {second} y {first} de obra'):
        _params, result = plan(router, text)
        assert result.reason == 'process_state_unrepresentable', text
        assert result.params.get('intent_type') is None, text


from hypothesis import given, settings, strategies as st  # noqa: E402


def _singular(plural):
    """procesos cancelados -> proceso cancelado, when it keeps the reading."""
    return plural[:-2] if plural.endswith('es') and not plural.endswith('os') else plural[:-1]


@settings(max_examples=60, derandomize=True, deadline=None)
@given(
    pair=st.sampled_from([p for p in HUMAN_PAIRS
                          if (p[0][0], p[1][0]) not in _CONTRACT_ROUTED_ORDER_DEPENDENT]),
    joiner=st.sampled_from([' y ', ', ', ' y, ']),
    singular=st.booleans(),
)
def test_permutation_property(router, pair, joiner, singular):
    """Order, basic punctuation and number of the same reading are one request."""
    (a, mention_a), (b, mention_b) = pair
    head = 'procesos'
    if singular:
        one_a, one_b = _singular(a), _singular(b)
        readings = [tuple(m.reading for m in read_process_state_mentions(w)) for w in (one_a, one_b)]
        if readings == [(mention_a.reading,), (mention_b.reading,)]:
            head, a, b = 'proceso', one_a, one_b
    forward = executable_signature(router, f'{head} {a}{joiner}{b} de obra')
    backward = executable_signature(router, f'{head} {b}{joiner}{a} de obra')
    assert forward == backward, (head, a, b, joiner)


@pytest.mark.parametrize('token', sorted(FAMILY_TOKENS | set(_LEGACY_PROCESS_SYNONYMS)))
def test_every_surface_a_producer_reads_is_enumerated(token):
    text = f'procesos {token} de obra'
    surfaces = {mention.surface for mention in read_process_state_mentions(text)}
    produced = detect_estado_family(text, 'procesos')
    if produced is not None:
        assert produced['consumed_span'] in surfaces, (token, produced)
    if token in _LEGACY_PROCESS_SYNONYMS:
        assert token in surfaces or any(token in surface for surface in surfaces), token


@pytest.mark.parametrize('text', [
    'procesos cancelados y adjudicados de obra',
    'procesos o contratos aprobados y cancelados de obra',
    'procesos o contratos aprobados y seleccionados de obra',
])
def test_v1_1_conflicts_reach_no_source(flow, text):
    result = flow.run_query(text, chat_id='psri', today=TODAY)
    assert result.get('needs_clarification'), result
    assert flow.source_calls == []


def test_dual_child_does_not_run_after_a_clarification():
    from app.core.workflow.dual_dataset import build_dataset_groups

    class Deps:
        def __getattr__(self, name):
            raise AssertionError(f'secondary child touched deps.{name}')

    result = {
        'needs_clarification': True,
        'query_plan': {'should_execute': True},
        'resolved_params': {'dataset': 'contratos', 'dataset_scope': ['procesos', 'contratos']},
    }
    assert build_dataset_groups(Deps(), result, {'dataset_id': 'jbjy-vk9h'}) is None


# ── PROCESS_STATE_SINGLE_REQUEST_V1: cardinality, not grammar ────────────
#
# One explicit reading executes; two or more distinct explicit readings
# clarify, whatever joins them. A funnel seeded by another authority is not a
# reading: it only checks compatibility. R3.1's "o" guard is not ported; its
# cases are pinned here and hold by cardinality alone.

MULTI_BLOCKED = [
    # ZEUS NO_GO_R3 (e53e6316): used to execute Aprobado ∧ Abierto.
    'procesos abiertos o (abiertos y aprobados) de obra',
    'procesos abiertos o (abiertos y aprobados) de obra en antioquia',
    'procesos (abiertos y aprobados) o abiertos de obra',
    # R3.1 OR regressions.
    'procesos abiertos o aprobados de obra',
    'procesos aprobados o abiertos de obra',
    'procesos cancelados o suspendidos de obra',
    'procesos suspendidos o cancelados de obra',
    'procesos publicados o aprobados de obra',
    'procesos aprobados o publicados de obra',
    'procesos abiertos ó aprobados de obra',
    'procesos cancelados y publicados o aprobados de obra',
    'procesos abiertos o aprobados y publicados de obra',
    # AND, juxtaposition, comma: the same cardinality.
    'procesos abiertos y aprobados de obra',
    'procesos abiertos aprobados de obra',
    'procesos abiertos, aprobados de obra',
    'procesos cancelados y suspendidos de obra',
    'procesos abiertos publicados de obra',
    'procesos en convocatoria aprobados de obra',
    'oportunidades abiertas aprobadas de obra',
]


@pytest.mark.parametrize('text', MULTI_BLOCKED)
def test_two_explicit_readings_clarify_whatever_joins_them(router, text):
    params, result = plan(router, text)
    assert result.should_execute is False, params
    assert result.reason == 'process_state_unrepresentable', result.reason
    assert 'más de un estado' in (result.clarification_question or '')
    assert not state_values(params) and not params.get('estado_family'), params
    for word in ('abiert', 'aprobad', 'cancelad', 'suspendid', 'publicad'):
        assert word not in topic(params), params


@settings(max_examples=60, derandomize=True, deadline=None)
@given(
    pair=st.sampled_from([p for p in HUMAN_PAIRS
                          if (p[0][0], p[1][0]) not in _CONTRACT_ROUTED_ORDER_DEPENDENT
                          and _CONTRACT_LIFECYCLE_ROUTED not in (p[0][0], p[1][0])]),
    shape=st.sampled_from(['{a} y {b}', '{a} o {b}', '{a}, {b}', '{a} ó {b}',
                           '{a} o ({a} y {b})', '({a} y {b}) o {a}', '{b} o {a} y {b}']),
)
def test_cardinality_does_not_depend_on_connector_or_grouping(router, pair, shape):
    (a, mention_a), (b, mention_b) = pair
    signature = executable_signature(router, f"procesos {shape.format(a=a, b=b)} de obra")
    if shape in _BARE_DISJUNCTIONS and _funnel_and_its_literal(mention_a, mention_b):
        # PROCESS_STATE_INTEGRITY_CLOSE_V1, declared contract change: "<open
        # funnel> o <literal of that funnel>" is one request for the funnel.
        # Only a bare disjunction of exactly these two; grouping, conjunction
        # and comma keep the cardinality verdict below.
        assert signature[0] is True and signature[6] == 'oferta_abierta', (shape, a, b, signature[:7])
        return
    assert signature[0] is False and signature[2] == 'multi', (shape, a, b, signature[:3])


_BARE_DISJUNCTIONS = frozenset({'{a} o {b}', '{a} ó {b}'})


def _funnel_and_its_literal(first, second):
    funnels = [m for m in (first, second) if not m.literal and m.family in FUNNEL_FAMILIES]
    literals = [m for m in (first, second) if m.literal]
    return bool(funnels and literals and literals[0].family == funnels[0].family)


@pytest.mark.parametrize('text, literal, field', [
    ('procesos cancelados de obra', 'Cancelado', 'estado_del_procedimiento'),
    ('procesos aprobados de obra', 'Aprobado', 'estado_del_procedimiento'),
    ('procesos publicados de obra', 'Publicado', 'estado_del_procedimiento'),
    ('procesos seleccionados de obra', 'Seleccionado', 'estado_del_procedimiento'),
    ('procesos suspendidos de obra', 'Suspendido', 'estado_del_procedimiento'),
])
def test_one_explicit_literal_executes_exactly(router, text, literal, field):
    params, result = plan(router, text)
    assert result.should_execute, result.reason
    assert result.params.get(field) == [literal], result.params
    assert f"'{literal}'" in soql(result.params)


@pytest.mark.parametrize('text', [
    'procesos abiertos de obra',
    'procesos en convocatoria de obra',
    'procesos abiertos o convocatorias de obra',
    'procesos abiertos o abiertos de obra',
])
def test_one_funnel_reading_projects_the_funnel(router, text):
    params, result = plan(router, text)
    assert result.should_execute, result.reason
    assert params.get('estado_family') == 'oferta_abierta', params
    assert params.get('intent_type') == 'opportunity_search', params
    assert 'convocatoria' not in topic(params)


@pytest.mark.parametrize('text, literal', [
    ('oportunidades aprobadas de software', 'Aprobado'),
    ('oportunidades publicadas de software', 'Publicado'),
    ('convocatorias publicadas de obra', 'Publicado'),
    ('convocatorias aprobadas de obra', 'Aprobado'),
])
def test_seeded_funnel_does_not_count_and_admits_its_literal(router, text, literal):
    # Source probe p6dx-8zbt (2026-09-24): Aprobado ∧ apertura=Abierto = 7.458
    # of 7.568 Aprobado; the seeded funnel frames the one literal named.
    params, result = plan(router, text)
    assert result.should_execute, result.reason
    assert result.params.get('estado_del_procedimiento') == [literal], result.params
    assert result.params.get('intent_type') == 'opportunity_search'
    assert "estado_de_apertura_del_proceso = 'Abierto'" in soql(result.params)


@pytest.mark.parametrize('text, conflict', [
    ('oportunidades canceladas de software', 'también pide'),
    ('convocatorias canceladas de obra', 'también pide'),
    ('oportunidades adjudicadas de obra', 'más de un estado'),
])
def test_seeded_funnel_incompatible_with_the_reading_clarifies(router, text, conflict):
    params, result = plan(router, text)
    assert result.should_execute is False, params
    assert result.reason == 'process_state_unrepresentable'
    assert conflict in result.clarification_question
    assert params['dataset'] == 'procesos' and not params.get('contract_universe')


@pytest.mark.parametrize('text', [
    'procesos que no esten en convocatoria de obra',
    'procesos de obra que no esten en convocatoria',
])
def test_negated_en_convocatoria_never_seeds_a_positive_funnel(router, text):
    # Identical on main f70dd9d5: the negated phrase leaves no positive state
    # and no opportunity intent; SINGLE_REQUEST_V1 does not read it as a head.
    params, result = plan(router, text)
    assert result.reason != 'process_state_unrepresentable'
    assert params.get('intent_type') != 'opportunity_search', params
    assert params.get('estado_family') != 'oferta_abierta', params
    assert not state_values(params), params


def test_decision_ignores_seed_for_cardinality():
    from app.core.process_state_request import decide_process_states
    (aprobado,) = read_process_state_mentions('aprobados')
    decision = decide_process_states((aprobado,), {'oferta_abierta'})
    assert decision.clarification is None
    assert (decision.literal, decision.family, decision.funnel) == ('Aprobado', 'oferta_abierta', True)
    (abierto,) = read_process_state_mentions('abiertos')
    assert decide_process_states((abierto, aprobado), set()).clarification
    assert decide_process_states((abierto, abierto, aprobado), set()).clarification


CLARIFY_END_TO_END = [
    'procesos abiertos o (abiertos y aprobados) de obra',
    'procesos abiertos y aprobados de obra',
    'procesos abiertos o aprobados de obra',
    'procesos abiertos publicados de obra',
    'procesos cancelados y suspendidos de obra',
    'convocatorias canceladas de obra',
    'oportunidades adjudicadas de obra',
]


@pytest.mark.parametrize('text', CLARIFY_END_TO_END)
def test_every_clarification_reaches_no_source(flow, text):
    result = flow.run_query(text, chat_id='psri-single', today=TODAY)
    assert result.get('needs_clarification'), result
    assert (result.get('query_plan') or {}).get('reason') == 'process_state_unrepresentable'
    assert flow.source_calls == []


def test_dual_multi_state_reaches_neither_child(flow):
    result = flow.run_query('procesos o contratos abiertos o aprobados de obra',
                            chat_id='psri-single-dual', today=TODAY)
    assert result.get('needs_clarification'), result
    assert (result.get('query_plan') or {}).get('reason') == 'process_state_unrepresentable'
    assert flow.source_calls == []


def test_followup_multi_state_reaches_no_source(flow):
    flow.run_query('procesos de obra', chat_id='psri-single-fu', today=TODAY)
    flow.source_calls.clear()
    result = flow.run_query('abiertos o aprobados', chat_id='psri-single-fu', today=TODAY)
    assert result.get('needs_clarification'), result
    assert (result.get('query_plan') or {}).get('reason') == 'process_state_unrepresentable'
    assert flow.source_calls == []


def test_followup_state_replaces_the_previous_one(flow):
    # Observed follow-up policy (R3, kept): a new state replaces the old one;
    # it never composes with it. Inherited-intent loss on "oportunidades →
    # y las aprobadas" is SHARED_CONTINUATION_STATE_V1 debt, not asserted here.
    flow.run_query('procesos abiertos de obra', chat_id='psri-single-rep', today=TODAY)
    flow.source_calls.clear()
    flow.run_query('y los aprobados?', chat_id='psri-single-rep', today=TODAY)
    assert flow.source_calls
    for _dataset, soql_text in flow.source_calls:
        where = soql_text.split(' WHERE ', 1)[1].split(' ORDER BY ', 1)[0]
        assert "estado_del_procedimiento = 'Aprobado'" in where
        assert "'Publicado'" not in where and 'estado_de_apertura_del_proceso' not in where
