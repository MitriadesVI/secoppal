"""Counterexamples and generated invariants for the adjudicated R3 oracles."""
from __future__ import annotations

import itertools
import sqlite3

import pytest
from hypothesis import given, settings, strategies as st

from app.core.query_fingerprint import query_fingerprint
from app.core.soql_builder import SoQLBuilder
from tests.sql_oracle_properties import (
    CELEBRATED,
    assert_identity,
    assert_object_exclusion,
    assert_unsatisfiable,
    conjunctions,
    exact_identity,
)

CONTRACTS = "jbjy-vk9h"


@given(st.permutations([CELEBRATED, "1=0", "nombre_entidad = 'A AND B OR C'"]))
@settings(max_examples=12, derandomize=True)
def test_unsatisfiable_is_independent_of_conjunct_order(clauses):
    assert_unsatisfiable(" AND ".join(clauses))
    with pytest.raises(AssertionError):
        assert_unsatisfiable(" AND ".join(c for c in clauses if c != "1=0"))


@pytest.mark.parametrize("sql", [
    CELEBRATED + " OR 1=0", CELEBRATED + " AND (1=0 OR 1=1)",
    CELEBRATED + " AND nombre_entidad = '1=0'",
])
def test_false_inside_or_or_string_does_not_prove_unsatisfiable(sql):
    with pytest.raises(AssertionError):
        assert_unsatisfiable(sql)


@given(st.text(alphabet="AB '()", min_size=1, max_size=30))
@settings(max_examples=40, derandomize=True)
def test_identity_literals_are_atomic_and_fuzzy_mutation_is_rejected(name):
    expected = exact_identity("nombre_entidad", (name,))
    sql = CELEBRATED + " AND " + expected
    assert_identity(sql, CONTRACTS, expected)
    assert conjunctions(sql) == [CELEBRATED, expected]
    mutant = sql.replace("nombre_entidad =", "unaccent(UPPER(nombre_entidad)) LIKE")
    with pytest.raises(AssertionError):
        assert_identity(mutant, CONTRACTS, expected)


def test_contract_universe_changes_fingerprint_and_version_is_semantic():
    params = {"dataset": "contratos", "objeto": ["alimentacion"],
              "contract_universe": "celebrated",
              "contract_universe_version": "contract-universe-1.1"}
    assert query_fingerprint(params) != query_fingerprint(
        {**params, "contract_universe": "source_records"}
    )
    assert query_fingerprint(params) != query_fingerprint(
        {**params, "contract_universe_version": "contract-universe-future"}
    )


@given(st.permutations([
    ("dataset", "contratos"), ("objeto", ["alimentacion"]),
    ("contract_universe", "celebrated"),
    ("contract_universe_version", "contract-universe-1.1"),
]))
@settings(max_examples=24, derandomize=True)
def test_fingerprint_same_universe_version_different_key_order(items):
    assert query_fingerprint(dict(items)) == query_fingerprint(dict(reversed(items)))


def test_generated_empty_sql_and_object_exclusion_execute_on_null_decoys():
    builder = SoQLBuilder()
    db = sqlite3.connect(":memory:")
    db.create_function("unaccent", 1, lambda x: x)
    db.create_function("date_extract_y", 1, lambda x: 2025 if x else None)
    db.execute("CREATE TABLE rows (estado_contrato, fecha_de_firma, "
               "objeto_del_contrato, descripcion_del_proceso)")
    values = list(itertools.product([None, "other", "zznegtest"], repeat=2))
    db.executemany("INSERT INTO rows VALUES ('Cerrado', NULL, ?, ?)", values)
    empty = builder.build_count(CONTRACTS, {
        "modalidad_include_procedure_ids": ["solicitud_informacion_proveedores"],
    }).split(" WHERE ", 1)[1]
    assert_unsatisfiable(empty)
    assert db.execute("SELECT count(*) FROM rows WHERE " + empty).fetchone()[0] == 0
    no_false = " AND ".join(c for c in conjunctions(empty) if c != "1=0")
    assert db.execute("SELECT count(*) FROM rows WHERE " + no_false).fetchone()[0] == 9
    where = builder.build_count(CONTRACTS, {"objeto_exclude": ["zznegtest"]}).split(" WHERE ", 1)[1]
    fields = ("objeto_del_contrato", "descripcion_del_proceso")
    assert_object_exclusion(where, CONTRACTS, fields)
    assert db.execute("SELECT count(*) FROM rows WHERE " + where).fetchone()[0] == 4
    for mutant in [where.replace("NOT (", "("), where.replace(
        "coalesce(unaccent(UPPER(objeto_del_contrato)) LIKE "
        "unaccent(UPPER('%zznegtest%')), false)",
        "unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%zznegtest%'))",
    )]:
        with pytest.raises(AssertionError):
            assert_object_exclusion(mutant, CONTRACTS, fields)
        assert db.execute("SELECT count(*) FROM rows WHERE " + mutant).fetchone()[0] != 4
    db.close()
