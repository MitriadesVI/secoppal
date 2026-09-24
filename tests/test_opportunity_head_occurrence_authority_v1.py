"""OPPORTUNITY_HEAD_OCCURRENCE_AUTHORITY_V1.

INV-SOURCE-OCCURRENCE-OWNERSHIP: a surface that can name a source, a state or
topic content only exercises authority when its concrete occurrence is
accredited for that role. "convocatoria(s)" is a source word
(source_occurrence), so OPP-002 may seed an opportunity search only through an
occurrence that source_request_occurrences accredits as a PROCESS request —
never because the word appears somewhere in the text.

ZEUS (PROCESS_STATE_SINGLE_REQUEST_V1, NO_GO): "contratos de publicación de
convocatorias de obra" was accredited as a contract request and still ran two
queries against procesos, because OPP-002 matched the substring.
"""
from datetime import date
from pathlib import Path

import pytest

from app.config import Settings, get_settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter, opportunity_head_occurrences
from app.core.soql_builder import SoQLBuilder
from app.core.source_occurrence import source_request_occurrences

TODAY = date(2026, 9, 23)


@pytest.fixture(scope='module')
def router():
    from app.core.entity_resolver import EntityResolver
    return QueryRouter(entity_resolver=EntityResolver(Path('app/data/aliases_db.json')))


def parse(router, text):
    return router.parse(text, today=TODAY).params


def topic(params):
    return ' '.join(str(term) for term in params.get('objeto') or ())


# ── Accredited head: the opportunity search is kept ───────────────────────

@pytest.mark.parametrize('text', [
    'convocatorias de obra',
    'convocatoria de aseo',
    'hay convocatorias de aseo en bogota',
    'convocatorias publicadas de obra',
    'convocatorias abiertas de obra',
])
def test_accredited_convocatorias_head_seeds_the_opportunity(router, text):
    params = parse(router, text)
    assert params['dataset'] == 'procesos'
    assert params.get('intent_type') == 'opportunity_search', params
    assert params.get('estado_family') == 'oferta_abierta', params


@pytest.mark.xfail(strict=True, reason=(
    'PROCESS_STATE_SINGLE_REQUEST_V1 (rebuilt on this fix): the named literal '
    'narrows the seeded funnel. On main the funnel still wins.'))
def test_convocatorias_publicadas_is_exactly_published(router):
    params = parse(router, 'convocatorias publicadas de obra')
    assert params.get('estado_del_procedimiento') == ['Publicado'], params


# ── Topic occurrence: no opportunity, the accredited head decides ─────────

@pytest.mark.parametrize('text', [
    'contratos de publicacion de convocatorias de obra',
    'contratos de publicación de convocatorias de obra',
    'contratos para difusion de convocatorias',
    'contratos de apoyo a convocatorias culturales',
    'contratos de publicacion de convocatorias',  # Entity Profile precedent
])
def test_topic_convocatorias_never_moves_a_contract_request(router, text):
    occurrences = source_request_occurrences(text.replace('ó', 'o'))
    assert [(o.surface, o.is_process) for o in occurrences] == [('contratos', False)]
    params = parse(router, text)
    assert params['dataset'] == 'contratos', params
    assert params.get('intent_type') != 'opportunity_search', params
    assert params.get('estado_family') != 'oferta_abierta', params
    assert not params.get('dataset_scope'), params
    assert 'convocatorias' in topic(params), params


# ── Occurrence, not surface ───────────────────────────────────────────────

def heads(text):
    return [(o.span, o.surface) for o in opportunity_head_occurrences(source_request_occurrences(text))]


def test_same_word_twice_only_the_accredited_occurrence_is_a_head():
    text = 'convocatorias de obra y servicios de publicacion de convocatorias'
    assert text.count('convocatorias') == 2
    assert heads(text) == [((0, 13), 'convocatorias')]


@pytest.mark.parametrize('text, expected', [
    ('convocatorias de obra', [((0, 13), 'convocatorias')]),
    ('hay convocatorias de aseo en bogota', [((4, 17), 'convocatorias')]),
    ('procesos en convocatoria', [((12, 24), 'convocatoria')]),
    ('obra en convocatoria', [((8, 20), 'convocatoria')]),
    # A process head owns the request; its topic word is no second head.
    ('procesos de gestion de convocatorias', []),
    ('licitaciones de publicacion de convocatorias', []),
    ('contratos de publicacion de convocatorias', []),
    ('procesos que no esten en convocatoria', []),
])
def test_opportunity_heads_are_accredited_occurrences(text, expected):
    # This is the ownership PSRI's rebuild consumes. While the state lexicon
    # still reads bare "convocatorias" as oferta_abierta (decision B), the
    # router-level seed is redundant for heads, so the property is pinned here.
    assert heads(text) == expected


# ── "en convocatoria" keeps its current meaning ───────────────────────────

@pytest.mark.parametrize('text', [
    'procesos en convocatoria de obra',
    'procesos en convocatoria de obras en bolivar',
    'obra en convocatoria',
])
def test_en_convocatoria_keeps_the_open_funnel(router, text):
    params = parse(router, text)
    assert params['dataset'] == 'procesos'
    assert params.get('estado_family') == 'oferta_abierta', params
    assert params.get('intent_type') == 'opportunity_search', params
    assert 'convocatoria' not in topic(params)


@pytest.mark.parametrize('text', [
    'procesos que no esten en convocatoria',
    'procesos de obra que no esten en convocatoria',
])
def test_negated_en_convocatoria_never_seeds_a_positive_funnel(router, text):
    params = parse(router, text)
    assert params.get('intent_type') != 'opportunity_search', params
    assert params.get('estado_family') != 'oferta_abierta', params


# ── Other opportunity keywords are unchanged (audited, not touched) ───────

@pytest.mark.parametrize('text', [
    'oportunidades de obra',
    'licitaciones abiertas de obra',
    'procesos abiertos de obra',
    'licitacion publica abierta de obra',
])
def test_other_opportunity_keywords_are_unchanged(router, text):
    params = parse(router, text)
    assert params['dataset'] == 'procesos'
    assert params.get('intent_type') == 'opportunity_search', params


# ── Residuals owned by other authorities, frozen here ─────────────────────

@pytest.mark.xfail(strict=True, reason=(
    'STATE lexicon (decision B, PSRI rebuild): NATURAL_TO_FAMILY reads bare '
    '"convocatorias" as oferta_abierta and STATE-PRIORITY adds the opportunity '
    'intent. OPP-002 no longer seeds it.'))
def test_topic_convocatorias_under_a_process_head_is_not_a_state(router):
    params = parse(router, 'procesos de gestion de convocatorias')
    assert params.get('intent_type') != 'opportunity_search', params
    assert params.get('estado_family') != 'oferta_abierta', params


@pytest.mark.xfail(strict=True, reason=(
    'SOURCE_OCCURRENCE does not know "oportunidades" as a source word; '
    'extending it is outside this microfix.'))
def test_topic_oportunidades_never_moves_a_contract_request(router):
    params = parse(router, 'contratos de publicidad de oportunidades laborales')
    assert params['dataset'] == 'contratos', params


# ── End to end: a contract request never reaches procesos ─────────────────

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
    'contratos de publicacion de convocatorias de obra',
    'contratos para difusion de convocatorias',
])
def test_contract_request_with_topic_convocatorias_queries_only_contratos(flow, text):
    flow.run_query(text, chat_id='ohoa', today=TODAY)
    assert flow.source_calls
    assert {dataset for dataset, _ in flow.source_calls} == {SoQLBuilder.CONTRATOS_DATASET}


def test_accredited_head_queries_procesos(flow):
    flow.run_query('convocatorias de obra', chat_id='ohoa-head', today=TODAY)
    assert flow.source_calls
    assert {dataset for dataset, _ in flow.source_calls} == {SoQLBuilder.PROCESOS_DATASET}
