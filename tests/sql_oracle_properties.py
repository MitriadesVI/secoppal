"""Independent, fail-closed WHERE oracles for Contract Universe R3 tests.

The frozen policy is deliberately independent of the product predicate factory.
Only top-level conjunctions may be separated; an OR escape is rejected.
"""
from __future__ import annotations

import re

CELEBRATED = (
    "(upper(unaccent(trim(estado_contrato))) = 'APROBADO' AND "
    "date_extract_y(fecha_de_firma) IS NOT NULL OR "
    "upper(unaccent(trim(estado_contrato))) IN ('EN EJECUCION', 'CERRADO', "
    "'MODIFICADO', 'TERMINADO', 'CEDIDO', 'SUSPENDIDO', 'PRORROGADO'))"
)
_TOKEN = re.compile(r"'(?:[^']|'')*'|[()]|\bAND\b|\bOR\b", re.IGNORECASE)


def conjunctions(sql: str) -> list[str]:
    """Split the tested SELECT/WHERE subset, respecting parentheses and strings."""
    where = sql.split(" WHERE ", 1)[-1].rsplit(" ORDER BY ", 1)[0]
    depth, start = 0, 0
    clauses = []
    for match in _TOKEN.finditer(where):
        token = match.group().upper()
        if token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
            assert depth >= 0, where
        elif token == "OR" and depth == 0:
            raise AssertionError(f"OR escapes conjunction: {where}")
        elif token == "AND" and depth == 0:
            clauses.append(where[start:match.start()].strip())
            start = match.end()
    assert depth == 0, where
    clauses.append(where[start:].strip())
    assert all(clauses), where
    return clauses


def scoped_clauses(sql: str, dataset: str) -> list[str]:
    clauses = conjunctions(sql)
    if dataset == "jbjy-vk9h":
        assert clauses.count(CELEBRATED) == 1, sql
        clauses.remove(CELEBRATED)
    else:
        assert "estado_contrato" not in sql, sql
        assert "fecha_de_firma" not in sql, sql
    return clauses


def assert_identity(sql: str, dataset: str, expected: str) -> None:
    assert scoped_clauses(sql, dataset) == [expected], sql


def exact_identity(field: str, physical_names: tuple[str, ...]) -> str:
    terms = [f"{field} = '" + name.replace("'", "''") + "'" for name in physical_names]
    return terms[0] if len(terms) == 1 else "(" + " OR ".join(terms) + ")"


def assert_unsatisfiable(sql: str) -> None:
    # A false top-level conjunct proves UNSAT for every valuation, including NULL.
    assert "1=0" in scoped_clauses(sql, "jbjy-vk9h"), sql


def assert_object_exclusion(sql: str, dataset: str, fields: tuple[str, str]) -> None:
    terms = [
        f"coalesce(unaccent(UPPER({field})) LIKE "
        "unaccent(UPPER('%zznegtest%')), false)" for field in fields
    ]
    assert scoped_clauses(sql, dataset) == ["NOT (" + " OR ".join(terms) + ")"], sql
