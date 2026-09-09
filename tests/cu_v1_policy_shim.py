"""Audit-only shim: restore the V1 Aprobado rule on top of the V1.1 runtime.

Purpose: isolate the cause of the ten frozen reds. If reverting ONLY the
Aprobado adjudication turns the frozen batch fully green, then no other
behavioural change of V1.1 is responsible for those failures.
"""
import pytest


@pytest.fixture(autouse=True)
def restore_v1_aprobado(monkeypatch):
    import app.core.contract_universe as cu

    def eligible_v1(row, universe=cu.ContractUniverse.CELEBRATED):
        universe = cu.ContractUniverse(universe)
        return (universe is cu.ContractUniverse.SOURCE_RECORDS
                or cu.fold_state(row.get('estado_contrato'))
                in {cu.fold_state(s) for s in cu.CELEBRATED_STATES})

    def predicate_v1(universe=cu.ContractUniverse.CELEBRATED):
        if cu.ContractUniverse(universe) is cu.ContractUniverse.SOURCE_RECORDS:
            return ''
        values = ', '.join("'" + cu.fold_state(s) + "'" for s in cu.CELEBRATED_STATES)
        return f"upper(unaccent(trim(estado_contrato))) IN ({values})"

    monkeypatch.setattr(cu, 'eligible', eligible_v1)
    monkeypatch.setattr(cu, 'predicate', predicate_v1)
