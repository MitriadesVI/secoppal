"""M1A.2A: no affiliation from department/NIT; exact Turbaco physical projection."""
from copy import deepcopy
from pathlib import Path

import pytest

from scripts.audits.gate_divipola_buyer_matrix import classify
from scripts.audits.municipal_directory_authority import directory_affiliation
from scripts.audits.promote_municipal_m1a import read

ROOT = Path(__file__).resolve().parents[1]
M0 = ROOT / 'artifacts/audits/municipal_m0_v1'


@pytest.fixture
def evidence():
    owner = 'ALCALDIA MUNICIPAL DE OCAÑA'
    record = next(r for r in read(ROOT / 'app/data/aliases_db.json') if r['official_name'] == owner)
    rows = [r for r in read(M0 / 'analysis.json.gz')['strata'] if r['name'] == owner]
    directory = [r for r in read(M0 / 'sources/directory.json.gz')['rows'] if r.get('nombre_entidad') == owner]
    return deepcopy((record, rows, directory, {'verdict': 'ACCEPT_SCOPED_AUTHORITY'}))


def test_directory_authority_does_not_need_sigep_fiscal_identity(evidence):
    assert directory_affiliation(*evidence)['codes'] == ['54498']


@pytest.mark.parametrize('mutation', ['department_only', 'wrong_department', 'wrong_account', 'wrong_name',
                                     'missing_dataset', 'two_accounts', 'conflicting_codes', 'nonexecutive',
                                     'unvalidated_cohort', 'invalid_divipola'])
def test_directory_authority_fails_closed_on_missing_or_conflicting_evidence(evidence, mutation):
    record, rows, directory, validation = evidence
    if mutation == 'department_only':
        directory[0]['c_digo_ubicaci_n'] = 'CO-NSA'
    elif mutation == 'wrong_department':
        directory[0]['departamento'] = 'Santander'
    elif mutation == 'wrong_account':
        directory[0]['codigo_entidad'] = 'other-account'
    elif mutation == 'wrong_name':
        directory[0]['nombre_entidad'] += ' 2'
    elif mutation == 'missing_dataset':
        rows.pop()
    elif mutation == 'two_accounts':
        rows[-1]['account'] = 'other-account'
    elif mutation == 'conflicting_codes':
        other = deepcopy(directory[0])
        other['c_digo_ubicaci_n'] = 'CO-NSA-54001'
        directory.append(other)
    elif mutation == 'nonexecutive':
        owner = 'CONCEJO MUNICIPAL DE OCAÑA'
        record['official_name'] = directory[0]['nombre_entidad'] = owner
        for row in rows:
            row['name'] = owner
    elif mutation == 'unvalidated_cohort':
        validation['verdict'] = 'REJECT_AUTHORITY'
    else:
        directory[0]['c_digo_ubicaci_n'] = 'CO-NSA-54999'
    assert directory_affiliation(record, rows, directory, validation)['codes'] == []


def test_rule_is_general_for_a_different_existing_physical_executive(evidence):
    record, rows, directory, validation = evidence
    owner = 'ALCALDIA MUNICIPAL DE CHIA'
    record.update(official_name=owner, departamento='Cundinamarca')
    for row in rows:
        row.update(name=owner, account='synthetic-fixture-account')
    directory[0].update(nombre_entidad=owner, codigo_entidad='synthetic-fixture-account',
                        departamento='Cundinamarca', c_digo_ubicaci_n='CO-CUN-25175')
    assert directory_affiliation(record, rows, directory, validation)['codes'] == ['25175']



def test_unknown_affiliation_still_reviews_in_the_unchanged_matrix():
    assert classify('known buyer', [], '13836', False, True, None, False) == 'REVIEW'
