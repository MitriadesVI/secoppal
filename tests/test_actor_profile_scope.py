"""Applied scope is presentation only; SQL oracle frozen at f6fa849."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, replace
import json
from pathlib import Path
import socket

import pytest
from hypothesis import given, settings, strategies as st

from app.core.actor_profile.assembler import build_actor_profile
from app.core.actor_profile.aspects import ASPECTS
from app.core.actor_profile.render import render_actor_profile
from app.core.actor_profile.turn import is_actor_profile_turn
from app.core.outcome import Outcome
from app.core.soql_builder import SoQLBuilder
from test_actor_profile import FakeSecopClient
from test_actor_profile_render import make_profile

FROZEN = json.loads(Path('tests/fixtures/actor_profile_scope_before.json').read_text())
CASES = {case['name']: case for case in FROZEN['cases']}
DATASET = SoQLBuilder.CONTRATOS_DATASET


class RecordingClient:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def derived_for_timeout(self, timeout):
        return self

    def _read(self, transport, dataset, soql):
        self.calls.append({'transport': transport, 'dataset': dataset, 'soql': soql})
        if self.fail:
            raise RuntimeError('synthetic source unavailable')
        return []

    def aggregate(self, dataset, soql):
        return self._read('aggregate', dataset, soql)

    def query(self, dataset, soql):
        return self._read('query', dataset, soql)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError('scope tests must not use network')
    monkeypatch.setattr(socket.socket, 'connect', blocked)


def scope_for(name):
    from app.core.actor_profile.scope_context import scope_from_resolved_params
    return scope_from_resolved_params(CASES[name]['params'])


@pytest.mark.parametrize('name', CASES)
def test_before_after_sql_and_source_calls_identical_for_every_aspect(name):
    params = deepcopy(CASES[name]['params'])
    original = deepcopy(params)
    expected = CASES[name]['queries']
    assert set(expected) == {a.name for a in ASPECTS}
    assert {a.name: a.build(SoQLBuilder(), DATASET, params) for a in ASPECTS} == {
        name: query['soql'] for name, query in expected.items()
    }
    client = RecordingClient()
    profile = build_actor_profile(params, DATASET, client, SoQLBuilder())
    assert sorted(client.calls, key=lambda q: q['soql']) == sorted(
        expected.values(), key=lambda q: q['soql'],
    )
    assert len(client.calls) == 8
    assert profile.identity == original['actor_scope']
    assert profile.contract_count == 0
    assert params == original
    assert profile.applied_scope.is_empty() == (name == 'global')


def test_global_profile_keeps_frozen_output_byte_for_byte():
    assert render_actor_profile(make_profile()) == FROZEN['global_render']
    assert 'Filtros aplicados' not in FROZEN['global_render']
    assert scope_for('global').is_empty()


@pytest.mark.parametrize('name, expected', [
    ('buyer', ['Entidad: DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA']),
    ('year', ['Periodo: 2026']),
    ('buyer_year', ['Entidad:', 'Periodo: 2026']),
    ('value', ['$0', '$2.000.000.000']),
    ('state', ['En ejecución', 'Modificado', 'Prorrogado']),
    ('modality', ['Modalidad:', 'Contratación directa']),
    ('contract_type', ['Prestación de servicios', 'Convenio solidario']),
    ('topic', ['software', 'soporte', 'licencia']),
    ('topic_exclusion', ['Excluir del objeto:', 'vigilancia']),
    ('topic_or_phrases', ['software', 'soporte', 'aire acondicionado']),
    ('region', ['Atlántico', 'Bolívar']),
    ('date_from', ['Desde 2025-02-03']),
    ('date_to', ['Hasta 2026-09-06']),
])
def test_visible_dimensions(name, expected):
    text = render_actor_profile(replace(make_profile(), applied_scope=scope_for(name)))
    assert '*Filtros aplicados*' in text
    for label in expected:
        assert label in text


def test_buyer_authority_and_sql_precedence_are_preserved():
    scope = scope_for('approximate_buyer')
    assert scope.buyer_scope_kind == 'approximate'
    assert scope.buyer_label == 'alcaldia'
    text = render_actor_profile(replace(make_profile(), applied_scope=scope))
    assert 'Entidad filtrada por coincidencia: alcaldia' in text
    assert 'Entidad: alcaldia' not in text
    assert scope_for('group').buyer_scope_kind == 'group'
    assert scope_for('group').buyer_label == 'Administración central de Bogotá'
    assert scope_for('buyer').buyer_scope_kind == 'exact'


def test_only_effective_values_are_exposed():
    assert 'Bolívar' not in str(scope_for('geography'))
    assert 'Ignorado' not in str(scope_for('state_precedence'))
    assert 'frase no catalogada' not in str(scope_for('topic_or_phrases'))
    assert 'actor_scope' not in asdict(scope_for('buyer'))
    assert 'params' not in asdict(scope_for('buyer'))


@pytest.mark.parametrize('failed', [False, True])
def test_scoped_empty_and_error_stay_distinct(failed):
    profile = build_actor_profile(
        CASES['buyer_year']['params'], DATASET, RecordingClient(failed), SoQLBuilder(),
    )
    text = render_actor_profile(profile)
    assert 'Periodo: 2026' in text
    assert 'Entidad:' in text
    if failed:
        assert profile.contract_count is None
        assert all(a.outcome == Outcome.ERROR for a in profile.aspects)
        assert 'No pude obtener el total de contratos' in text
        assert '0 contratos' not in text
    else:
        assert all(a.outcome == Outcome.EMPTY for a in profile.aspects)
        assert '- 0 contratos observados dentro de estos filtros' in text
        assert text.index('Filtros aplicados') < text.index('- 0 contratos')


@pytest.mark.parametrize('key', ['objeto', 'objeto_or', 'objeto_phrases'])
def test_positive_topics_remain_listing_not_profile(key):
    params = CASES['global']['params'] | {key: ['software']}
    assert not is_actor_profile_turn(params)


def test_pure_builder_never_reads_files_or_resolves(monkeypatch):
    from app.core.actor_profile.scope import build_actor_profile_scope
    def blocked(*args, **kwargs):
        raise AssertionError('pure display builder attempted I/O')
    monkeypatch.setattr(Path, 'read_text', blocked)
    monkeypatch.setattr('builtins.open', blocked)
    scope = build_actor_profile_scope(CASES['buyer_year']['params'])
    assert scope.buyer_label == CASES['buyer']['params']['entidad_resolved']
    assert scope.date_from == '2026-01-01'


@settings(max_examples=35, derandomize=True, deadline=None)
@given(st.integers(min_value=0, max_value=10**13), st.integers(min_value=0, max_value=10**13), st.booleans())
def test_property_metadata_never_changes_sql_metrics_or_outcomes(lower, upper, failed):
    from app.core.actor_profile.scope import build_actor_profile_scope
    from app.core.actor_profile.scope_models import ActorProfileAppliedScope
    params = CASES['buyer_year']['params'] | {'valor_min': min(lower, upper), 'valor_max': max(lower, upper)}
    before = deepcopy(params)
    queries = {a.name: a.build(SoQLBuilder(), DATASET, params) for a in ASPECTS}
    scope = build_actor_profile_scope(params)
    assert params == before
    decorated = params | {'actor_profile_applied_scope': asdict(scope)}
    assert {a.name: a.build(SoQLBuilder(), DATASET, decorated) for a in ASPECTS} == queries
    client = FakeSecopClient(fail={'core'} if failed else set())
    profile = build_actor_profile(params, DATASET, client, SoQLBuilder())
    metrics_before = asdict(profile)
    text = render_actor_profile(profile)
    assert asdict(profile) == metrics_before
    assert ('No pude obtener el total' in text) == failed
    bare = replace(profile, applied_scope=ActorProfileAppliedScope())
    assert asdict(bare) | {'applied_scope': metrics_before['applied_scope']} == metrics_before
    assert build_actor_profile_scope(CASES['global']['params']).is_empty()


@settings(max_examples=25, derandomize=True, deadline=None)
@given(st.lists(st.sampled_from(['buyer', 'year', 'value', 'state', 'topic_exclusion', 'region']), unique=True), st.booleans())
def test_property_empty_and_error_never_collapse(filters, failed):
    from app.core.actor_profile.scope import build_actor_profile_scope
    params = deepcopy(CASES['global']['params'])
    for name in filters:
        params.update(CASES[name]['params'])
    scope = build_actor_profile_scope(params)
    profile = build_actor_profile(params, DATASET, RecordingClient(failed), SoQLBuilder())
    text = render_actor_profile(profile)
    assert profile.contract_count is None if failed else profile.contract_count == 0
    assert ('0 contratos' not in text) if failed else ('- 0 contratos' in text)
    for key in set(params) - set(CASES['global']['params']):
        params.pop(key)
    assert build_actor_profile_scope(params).is_empty()
    assert scope.is_empty() == (not filters)
