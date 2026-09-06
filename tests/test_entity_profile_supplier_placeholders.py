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
