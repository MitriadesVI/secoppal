"""Independent F1-F5 oracles for Contract Universe V1.1.

Expectations are written from the POLICY/POLICY_DELTA text and from what a user
would be entitled to observe; none of them is derived from a runtime helper.
"""
import pytest

from app.core.contract_universe_intent import apply_request, request
from tests.buyer_actor_independent_support import SEEDS, alias, say
from tests.contract_universe_independent_support import DESCRIPTION, ids, setup

CARTAGENA, MIN = SEEDS[0][2], SEEDS[0][3]
POSTERIOR = {f'A-{i}-{s}' for i in range(7) for s in ('dated', 'absent', 'invalid')}
CELEBRATED = POSTERIOR | {'A-7-dated'}


def universe(result):
    return result['resolved_params'].get('contract_universe', 'celebrated')


def states_seen(result):
    return {r['estado_contrato'] for r in result.get('results', [])}


# ══════════════════════════ F1 ══════════════════════════════════════════════
@pytest.mark.parametrize('frame', [False, True])
def test_f1_a_object_words_do_not_become_state(tmp_path, monkeypatch, frame):
    """A: a state word inside the contractual object is object language."""
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, frame)
    try:
        result = say(flow, 'contratos de elaboración de borradores de documentos '
                           'de la Alcaldía de Cartagena')
        params = result['resolved_params']
        assert universe(result) == 'celebrated'
        assert 'Borrador' not in str(params.get('estado_contrato'))
        assert 'Borrador' not in str(params.get('estado'))
        assert 'borrador' in str(params.get('objeto')).lower()
        assert not states_seen(result) & {'Borrador', 'Cancelado', 'enviado Proveedor',
                                          'En aprobación'}
        for call in flow.audit_trace['sql']:
            assert "'BORRADOR'" not in call['soql'].upper()
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_f1_b_actor_name_word_is_identity_not_state(tmp_path, monkeypatch, frame):
    """B: a state word inside an Actor name resolves the Actor, not a state."""
    name = 'FUNDACION BORRADORES'
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, frame, aliases=[alias(name)])
    conn.execute('UPDATE contracts SET proveedor_adjudicado=?, documento_proveedor=?',
                 (name, '71000001'))
    try:
        result = say(flow, 'qué ha contratado FUNDACION BORRADORES')
        params = result['resolved_params']
        assert params['actor_scope']['base_number'] == '71000001'
        assert universe(result) == 'celebrated'
        assert 'Borrador' not in str(params.get('estado_contrato'))
        for call in flow.audit_trace['sql']:
            assert "'BORRADOR'" not in call['soql'].upper()
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_f1_c_explicit_records_still_work(tmp_path, monkeypatch, frame):
    """C: the explicit record construction keeps its authority and executes."""
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, frame)
    try:
        result = say(flow, 'registros en borrador de la Alcaldía de Cartagena')
        assert not result.get('needs_clarification'), result['response']
        assert universe(result) == 'source_records'
        assert result['resolved_params'].get('estado_contrato') == ['Borrador']
        assert states_seen(result) == {'Borrador'}
        assert result['total_count'] == 10
        assert flow.audit_trace['sql']
    finally:
        conn.close()


@pytest.mark.parametrize('surface,number', [
    ('CONSTRUCTORA CANCELADOS', '71000002'),
    ('FUNDACION REGISTROS', '71000003'),
    ('COOPERATIVA BORRADORES', '71000004'),
    ('UNION APROBACION', '71000005'),
])
def test_f1_counterproof_actor_names_never_change_universe(tmp_path, monkeypatch,
                                                           surface, number):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False,
                       aliases=[alias(surface, number)])
    conn.execute('UPDATE contracts SET proveedor_adjudicado=?, documento_proveedor=?',
                 (surface, number))
    try:
        result = say(flow, f'qué ha contratado {surface}')
        assert universe(result) == 'celebrated'
        assert result['resolved_params']['actor_scope']['base_number'] == number
        for call in flow.audit_trace['sql']:
            upper = call['soql'].upper()
            for excluded in ("'CANCELADO'", "'BORRADOR'", "'ENVIADO PROVEEDOR'",
                             "'EN APROBACION'"):
                assert excluded not in upper
    finally:
        conn.close()


@pytest.mark.parametrize('phrase', [
    'contratos de custodia de registros clínicos de la Alcaldía de Cartagena',
    'contratos de elaboración de borradores de documentos de la Alcaldía de Cartagena',
    'contratos de gestión de cancelados bancarios de la Alcaldía de Cartagena',
    'contratos de aprobacion de estudios previos de la Alcaldía de Cartagena',
])
def test_f1_counterproof_object_words_never_change_universe(tmp_path, monkeypatch, phrase):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        result = say(flow, phrase)
        assert universe(result) == 'celebrated'
        assert not states_seen(result) & {'Borrador', 'Cancelado', 'enviado Proveedor',
                                          'En aprobación'}
    finally:
        conn.close()


# ══════════════════════════ F2 ══════════════════════════════════════════════
@pytest.mark.parametrize('text', [
    'registros cancelados y borradores de la Alcaldía de Cartagena',
    'registros borradores y cancelados de la Alcaldía de Cartagena',
])
def test_f2_two_states_clarify_with_zero_sql(tmp_path, monkeypatch, text):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        result = say(flow, text)
        assert result.get('needs_clarification'), result['response']
        assert not flow.audit_trace['sql']
        assert not result.get('results')
        # Both states must be recognised as the reason, not one of them silently.
        intent = request(text)
        assert intent is not None and intent.conflict and intent.state is None
        assert 'cancelad' in text[slice(*intent.span)].lower()
        assert 'borrador' in text[slice(*intent.span)].lower()
    finally:
        conn.close()


@pytest.mark.parametrize('text,state,total', [
    ('registros cancelados de la Alcaldía de Cartagena', 'Cancelado', 10),
    ('registros en borrador de la Alcaldía de Cartagena', 'Borrador', 10),
])
def test_f2_single_state_still_executes(tmp_path, monkeypatch, text, state, total):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        result = say(flow, text)
        assert not result.get('needs_clarification')
        assert states_seen(result) == {state}
        assert result['total_count'] == total
    finally:
        conn.close()


@pytest.mark.parametrize('text', [
    'contratos celebrados y cancelados de la Alcaldía de Cartagena',
    'registros cancelados y celebrados de la Alcaldía de Cartagena',
])
def test_f2_celebrated_plus_excluded_clarifies(tmp_path, monkeypatch, text):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        result = say(flow, text)
        assert result.get('needs_clarification'), result['response']
        assert not flow.audit_trace['sql']
    finally:
        conn.close()


# ══════════════════════════ F3 ══════════════════════════════════════════════
@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('query', [
    'analiza registros cancelados de alcaldia de cartagena',
    'contratos de alcaldia de cartagena por mas de 500 millones',
])
def test_f3_source_content_is_immutable(tmp_path, monkeypatch, frame, query):
    """The literal source string survives Search and both profile renders."""
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, frame)
    try:
        result = say(flow, query)
        response = result['response']
        assert DESCRIPTION in response, response
        assert 'y registros de consultoría' not in response
        assert 'borradores de documentos y contratos de consultoría' in response
        for row in result.get('results', ()):
            assert row['objeto_del_contrato'] == DESCRIPTION
            assert row['descripcion_del_proceso'] == DESCRIPTION
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_f3_dual_response_preserves_source_content(tmp_path, monkeypatch, frame):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, frame)
    try:
        result = say(flow, 'analiza procesos y contratos de alcaldia de cartagena '
                           'por mas de 500 millones')
        assert DESCRIPTION in result['response'] or not result.get('results')
        assert 'y registros de consultoría' not in result['response']
    finally:
        conn.close()


def test_f3_no_global_substitution_survives_in_runtime():
    """No equivalent of record_terms may remain reachable."""
    import app.core.contract_universe as cu
    assert not hasattr(cu, 'record_terms')
    import subprocess
    out = subprocess.run(['grep', '-rn', 'record_terms', 'app/'],
                         capture_output=True, text=True).stdout
    assert out == '', out


# ══════════════════════════ F4 ══════════════════════════════════════════════
@pytest.mark.parametrize('params', [
    dict(dataset='contratos', estado=['Cancelado'], estado_field='estado_del_procedimiento'),
    dict(dataset='contratos', estado_contrato=['Cancelado'], raw_query='sin cancelados'),
    dict(dataset='contratos', estado=['Cancelado'], estado_family='cancelled',
         raw_query='contratos suspendidos'),
    dict(dataset='contratos', estado_family='no_procede'),
    dict(dataset='contratos', estado_family='en_evaluacion'),
    dict(dataset='contratos', estado_contrato=['Borrador']),
    dict(dataset='contratos', contract_universe='source_records'),
    dict(dataset='contratos', contract_universe='source_records',
         contract_universe_version='contract-universe-1'),
])
def test_f4_materialized_params_are_not_consent(params):
    from app.core.contract_universe import ContractUniverse
    try:
        result = apply_request(dict(params), request(params.get('raw_query', '')))
    except ValueError as exc:                       # stale version is rejected outright
        assert 'revisión' in str(exc)
        return
    assert result.get('contract_universe') != ContractUniverse.SOURCE_RECORDS.value, result
    if any(params.get(k) for k in ('estado', 'estado_contrato', 'estado_family')) \
            or params.get('contract_universe') == 'source_records':
        assert result.get('contract_universe_error'), result


def test_f4_versioned_authority_still_admits_records():
    result = apply_request(dict(dataset='contratos', contract_universe='source_records',
                                contract_universe_version='contract-universe-1.1'), None)
    assert result['contract_universe'] == 'source_records'
    assert not result.get('contract_universe_error')


def test_f4_direct_lookup_still_reaches_the_record():
    result = apply_request(dict(dataset='contratos', contract_universe='source_records',
                                intent_type='direct_lookup'), None)
    assert result['contract_universe'] == 'source_records'


def test_f4_producers_do_not_launder_consent_through_reload(tmp_path, monkeypatch):
    """parser -> plan -> persistence -> reload keeps the ordinary universe."""
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        first = say(flow, 'contratos de la Alcaldía de Cartagena sin cancelados')
        assert universe(first) == 'celebrated' or first.get('needs_clarification')
        again = say(flow, 'contratos de alcaldia de cartagena por mas de 500 millones')
        assert universe(again) == 'celebrated'
        assert again['resolved_params']['contract_universe_version'] == 'contract-universe-1.1'
    finally:
        conn.close()


# ══════════════════════════ F5 ══════════════════════════════════════════════
@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('text', [
    'analiza procesos y todos los registros del dataset de alcaldia de cartagena por mas de 500 millones',
    'analiza todos los registros del dataset y procesos de alcaldia de cartagena por mas de 500 millones',
])
def test_f5_a_dual_records_clarifies_without_any_child(tmp_path, monkeypatch, frame, text):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, frame)
    try:
        result = say(flow, text)
        assert result.get('needs_clarification'), result['response']
        assert not flow.audit_trace['sql'], flow.audit_trace['sql']
        assert not result.get('results')
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('text', [
    'analiza procesos y contratos de alcaldia de cartagena por mas de 500 millones',
    'analiza contratos y procesos de alcaldia de cartagena por mas de 500 millones',
])
def test_f5_b_ordinary_dual_executes_both_children(tmp_path, monkeypatch, frame, text):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, frame)
    try:
        result = say(flow, text)
        assert not result.get('needs_clarification'), result['response']
        datasets = {c['dataset'] for c in flow.audit_trace['sql']}
        assert datasets == {'jbjy-vk9h', 'p6dx-8zbt'}, datasets
        for call in flow.audit_trace['sql']:
            if call['dataset'] == 'p6dx-8zbt':
                assert 'estado_contrato' not in call['soql']
                assert 'contract_universe' not in call['soql']
            else:
                assert 'APROBADO' in call['soql'].upper()
        for call in flow.audit_trace['sql']:
            assert CARTAGENA in call['soql'], call['soql']
            assert str(MIN) in call['soql'], call['soql']
    finally:
        conn.close()


@pytest.mark.parametrize('text', [
    'procesos y registros cancelados de alcaldia de cartagena',
    'registros cancelados y procesos de alcaldia de cartagena',
])
def test_f5_c_dual_with_excluded_state_never_executes_only_one(tmp_path, monkeypatch, text):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        result = say(flow, text)
        if result.get('needs_clarification'):
            assert not flow.audit_trace['sql']
        else:
            datasets = {c['dataset'] for c in flow.audit_trace['sql']}
            assert datasets == {'jbjy-vk9h', 'p6dx-8zbt'}, datasets
    finally:
        conn.close()
