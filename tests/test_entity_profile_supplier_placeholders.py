"""Execute the generated aggregation in SQLite: WHERE precedes GROUP BY/LIMIT.

SQLite is an offline relational oracle, not a Socrata dialect emulator. Live
SODA3 receipts separately verify acceptance and semantics of the predicate.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest
from hypothesis import given, settings, strategies as st

from app.core.actor_identity import normalize_identity
from app.core.entity_profile.aspects.suppliers import build_supplier_query, parse_suppliers
from app.core.soql_builder import SoQLBuilder


FIXTURE = Path('tests/fixtures/entity_profile_supplier_placeholders.json')
SENTINELS = (
    'NO DEFINIDO', 'NO DEFINIDA', 'NODEFINIDO', 'NO APLICA', 'N/A', 'NA',
    'SIN DATO', 'SIN INFORMACION', 'SIN INFORMACIÓN', 'NULL', 'NONE', '-',
)
BUILDER = SimpleNamespace(
    SPECS=SoQLBuilder.SPECS,
    _build_where=lambda spec, params: "nombre_entidad = 'BUYER'",
)


def aggregate(rows, ranking, *, legacy=False):
    sql = build_supplier_query(BUILDER, {}, ranking)
    if legacy:
        # Frozen pre-fix eligibility, leaving the actual aggregation untouched.
        start, tail = sql.split(' WHERE ', 1)
        _, suffix = tail.split(' GROUP BY ', 1)
        sql = start + " WHERE nombre_entidad = 'BUYER' AND documento_proveedor IS NOT NULL AND documento_proveedor != '' GROUP BY " + suffix
    return execute_sql(rows, sql)


def execute_sql(rows, sql):
    with sqlite3.connect(':memory:') as db:
        db.row_factory = sqlite3.Row
        db.create_function('upper', 1, lambda v: v.upper() if v is not None else None)
        db.execute('CREATE TABLE contracts (documento_proveedor TEXT, tipodocproveedor TEXT, proveedor_adjudicado TEXT, valor_del_contrato INTEGER, nombre_entidad TEXT)')
        db.executemany('INSERT INTO contracts VALUES (?,?,?,?,?)', [(*row, 'BUYER') for row in rows])
        sql = sql.replace(' WHERE ', ' FROM contracts WHERE ', 1)
        return [dict(row) for row in db.execute(sql)]


def candidate_rows(case):
    valid = [(str(900123450 + i), 'NIT', f'VALID {i}', (i + 1) * 100) for i in range(5)]
    placeholders = [('No Definido', 'No Definido', f'NAME {i % case["distinct_names"]}', 1000) for i in range(case['placeholder_rows'])]
    return valid, placeholders


@pytest.mark.parametrize('case', json.loads(FIXTURE.read_text())['cases'], ids=lambda c: c['city'])
@pytest.mark.parametrize('ranking', ['count', 'value'])
def test_placeholder_group_cannot_consume_top_five(case, ranking):
    valid, placeholders = candidate_rows(case)
    old = aggregate(valid + placeholders, ranking, legacy=True)
    artificial = next(row for row in old if row['documento_proveedor'] == 'No Definido')
    assert artificial['contract_count'] == 11
    assert artificial['supplier_name'] == max(row[2] for row in placeholders)
    assert len(parse_suppliers(old)) == 4
    actual = parse_suppliers(aggregate(valid + placeholders, ranking))
    expected = parse_suppliers(aggregate(valid, ranking))
    assert len(actual) == 5
    assert actual == expected


@pytest.mark.parametrize('ranking', ['count', 'value'])
def test_predicate_precedes_grouping_and_keeps_typed_key_and_limit(ranking):
    sql = build_supplier_query(BUILDER, {}, ranking)
    where = sql.split(' WHERE ')[1].split(' GROUP BY ')[0]
    assert 'NOT IN' in where
    assert "'NO DEFINIDO'" in where
    assert 'GROUP BY documento_proveedor, tipodocproveedor' in sql
    assert sql.endswith('LIMIT 5')


@pytest.mark.parametrize('value', [None, '', 'unknown', '12', *SENTINELS])
def test_parser_never_manufactures_identity(value):
    assert parse_suppliers([{'documento_proveedor': value, 'supplier_name': 'UT SOMETHING'}]) == ()


@settings(max_examples=50, derandomize=True, deadline=None)
@given(
    sentinels=st.lists(st.tuples(st.sampled_from(SENTINELS), st.booleans(), st.integers(0, 10**9)), max_size=30),
    values=st.lists(st.integers(0, 10**9), min_size=5, max_size=5),
    repetitions=st.lists(st.integers(1, 5), min_size=5, max_size=5),
)
def test_sentinel_insertion_preserves_valid_ranking_and_metrics(sentinels, values, repetitions):
    valid = [
        (str(10000000 + i), 'Cédula de Ciudadanía', f'ACTOR {i}', value)
        for i, (value, count) in enumerate(zip(values, repetitions))
        for _ in range(count)
    ]
    extra = []
    for text, lower, value in sentinels:
        raw = '  ' + (text.lower() if lower else text) + ' '
        assert normalize_identity(raw).key is None
        extra.append((raw, 'No Definido', 'UNATTRIBUTABLE', value))
    for ranking in ('count', 'value'):
        expected = parse_suppliers(aggregate(valid, ranking))
        actual = parse_suppliers(aggregate(valid + extra, ranking))
        assert actual == expected
        assert len(actual) == 5
        for metric in actual:
            index = int(metric.document) - 10000000
            assert metric.contract_count == repetitions[index]
            assert metric.total_contract_value == values[index] * repetitions[index]


def test_typed_supplier_key_keeps_same_digits_separate():
    rows = [('900123456', kind, kind, 100) for kind in ('NIT', 'Cédula de Ciudadanía')]
    metrics = parse_suppliers(aggregate(rows, 'count'))
    assert {m.actor_identity for m in metrics} == {'NIT:900123456', 'CC:900123456'}


def test_unknown_non_sentinel_is_still_rejected_by_parser():
    rows = [('xyz', 'Otro', 'NOT AN ACTOR', 100)]
    assert len(aggregate(rows, 'count')) == 1  # no new SQL identity heuristic
    assert parse_suppliers(aggregate(rows, 'count')) == ()


def test_supplier_focus_executes_two_actual_queries_and_renders_five_per_ranking():
    from datetime import date
    from app.core.entity_profile.assembler import build_entity_profile
    from app.core.entity_profile.render import render_entity_profile
    from app.core.entity_profile.turn import classify_entity_profile_turn
    from app.core.entity_resolver import EntityResolver
    from app.core.query_router import QueryRouter

    query = 'analiza la alcaldia de baranoa en 2026'
    router = QueryRouter(EntityResolver(Path('app/data/aliases_db.json')))
    turn = classify_entity_profile_turn(query, router.parse(query, today=date(2026, 9, 6)).params)
    valid, placeholders = candidate_rows({'distinct_names': 9, 'placeholder_rows': 11})

    class Client:
        def __init__(self):
            self.calls = []

        def derived_for_timeout(self, timeout):
            assert timeout <= 8
            return self

        def aggregate(self, dataset, sql):
            assert dataset == 'jbjy-vk9h'
            self.calls.append(sql)
            return execute_sql(valid + placeholders, sql)

    client = Client()
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus='suppliers', secop_client=client, soql_builder=BUILDER,
    )
    assert profile.source_calls.calls_total == len(client.calls) == 2
    assert not profile.partial
    assert len(profile.suppliers.fact('top_by_contract_count')) == 5
    assert len(profile.suppliers.fact('top_by_contract_value')) == 5
    assert render_entity_profile(profile).count('•') == 10
