"""Falsify fiscal identity shortcuts and verify offline discovery accounting."""
from collections import defaultdict
from copy import deepcopy
import socket

import pytest

from scripts.audits.m2a.affiliation import investigate
from scripts.audits.m2a.identity import reconcile
from scripts.audits.m2a.io import ACC, BOGOTA_AUTHORITY, DS, ROOT, read
from scripts.audits.m2a.matching import entity_match, guarded_town
from scripts.audits.audit_municipal_m2a import build, deny_network

TOWNS = {t['divipola']: t for t in read(ROOT / 'app/data/municipal_territories_v1.json')['territories']}


def observation(name='CONCEJO MUNICIPAL DE BARANOA', nit='123', account='account-1'):
    p = dict(profile_id=account + name, account=account, physical_source_name=name,
             fiscal_identity=[nit], stratum_indices=[0], volumes=dict(
                 process_physical_rows=1, unique_processes=1, contract_physical_rows=0,
                 unique_contracts=0, unique_contracts_provenance='DERIVED_FROM_GLOBAL_IDENTITY'))
    rows = [dict(dataset=DS[0], account=account, name=name, nit_evidence=nit,
                 municipality=None, status='UNRESOLVED_AFFILIATION',
                 affiliation_basis='UNRESOLVED', physical_rows=1, unique_ids=1, sigep_codes=[], covered=False)]
    ix = {k: defaultdict(list) for k in ('sigep', 'bridge', 'directory', 'prior', 'recognized')}
    return p, rows, ix


def sigep(name='CONCEJO MUNICIPAL DE BARANOA', code='08078', nature='Concejo Municipal'):
    return {'NOMBRE': name, 'NIT': '123', 'CÓDIGO SIGEP': 'test-entity', 'SUBORDEN': 'Municipal',
            'divipola': code, 'MUNICIPIO': TOWNS[code]['municipality'], 'NATURALEZA JURÍDICA': nature}


def own_directory(p, code='08078'):
    return dict(codigo_entidad=p['account'], nombre_entidad=p['physical_source_name'],
                nit_entidad=p['fiscal_identity'][0], c_digo_ubicaci_n=f'CO-ATL-{code}')


def test_bridge_alone_probable_and_own_directory_corroborates_territory():
    p, rows, ix = observation()
    ix['bridge']['123'] = [(9, '08078')]
    a = investigate(p, rows, ix, TOWNS)
    assert a['affiliation'] == 'PROBABLE'
    assert a['via_a'] and not a['entity_sigep_codes']
    ix['directory'][p['account']] = [(0, own_directory(p))]
    assert investigate(p, rows, ix, TOWNS)['affiliation'] == 'VERIFIED'


def test_bridge_with_own_entity_is_verified_without_borrowed_nature():
    p, rows, ix = observation()
    ix['bridge']['123'] = [(9, '08078')]
    ix['sigep']['123'] = [(0, sigep()), (1, sigep('ALCALDIA DE BARANOA', nature='Alcaldía'))]
    a = investigate(p, rows, ix, TOWNS)
    assert a['affiliation'] == 'VERIFIED'
    assert a['institutional_family'] == 'CONCEJO'
    assert a['evidence_type'] == 'NIT_BRIDGE_AND_INDEPENDENT_SIGEP_ENTITY'


def test_conflicting_nit_territories_close_bridge():
    p, rows, ix = observation()
    ix['bridge']['123'] = [(1, '08078'), (2, '08001')]
    a = investigate(p, rows, ix, TOWNS)
    assert not a['via_a']
    assert a['affiliation'] != 'VERIFIED'


@pytest.mark.parametrize('name,family', [('CONCEJO MUNICIPAL DE BARANOA', 'CONCEJO'),
                                       ('PERSONERIA DE BARANOA', 'PERSONERIA')])
def test_alcaldia_fiscal_row_cannot_classify_another_institution(name, family):
    p, rows, ix = observation(name)
    ix['sigep']['123'] = [(0, sigep('ALCALDIA DE BARANOA', nature='Alcaldía'))]
    ix['directory'][p['account']] = [(0, own_directory(p))]
    a = investigate(p, rows, ix, TOWNS)
    assert a['affiliation'] == 'VERIFIED'  # Territorial chain, never borrowed institutional identity.
    assert not a['entity_sigep_codes']
    assert a['institutional_family'] == family
    assert a['family_status'] == 'PROVISIONAL'


def test_via_b_own_entity_and_directory_and_conflict():
    p, rows, ix = observation()
    ix['sigep']['123'] = [(0, sigep())]
    ix['directory'][p['account']] = [(0, own_directory(p))]
    a = investigate(p, rows, ix, TOWNS)
    assert a['affiliation'] == 'VERIFIED' and a['directory_crosscheck']
    ix['directory'][p['account']] = [(0, own_directory(p, '08001'))]
    a = investigate(p, rows, ix, TOWNS)
    assert a['affiliation'] == 'AMBIGUOUS' and a['territory_divipola'] is None


def test_directory_other_name_does_not_corroborate_profile():
    p, rows, ix = observation()
    ix['sigep']['123'] = [(0, sigep())]
    reg = own_directory(p)
    reg['nombre_entidad'] = 'ALCALDIA DE BARANOA'
    ix['directory'][p['account']] = [(0, reg)]
    assert investigate(p, rows, ix, TOWNS)['affiliation'] == 'PROBABLE'


def test_national_order_does_not_accredit_nonmunicipal():
    p, rows, ix = observation('UNKNOWN')
    rows[0]['source_order'] = 'Nacional'
    rows[0]['status'] = 'ACCREDITED_OTHER_AFFILIATION'
    assert investigate(p, rows, ix, TOWNS)['affiliation'] == 'UNRESOLVED'


def test_bogota_census_does_not_enable_generic_routes():
    p, rows, ix = observation('ALCALDIA DE BOGOTA')
    ix['bridge']['123'] = [(9, '11001')]
    ix['sigep']['123'] = [(0, sigep('ALCALDIA DE BOGOTA', '11001', 'Alcaldía'))]
    ix['directory'][p['account']] = [(0, own_directory(p, '11001'))]
    a = investigate(p, rows, ix, TOWNS)
    assert not a['via_a'] and not a['via_b'] and a['affiliation'] == 'UNRESOLVED'
    rows[0].update(affiliation_basis=BOGOTA_AUTHORITY, status=ACC, municipality='11001')
    a = investigate(p, rows, ix, TOWNS)
    assert a['affiliation'] == 'VERIFIED' and a['evidence_type'] == BOGOTA_AUTHORITY


def test_complete_compound_guard_rejects_wrong_prefix():
    assert guarded_town('CONCEJO MUNICIPAL DE SAN ANDRES DE SOTAVENTO', '23670')
    assert not guarded_town('CONCEJO MUNICIPAL DE SAN ANDRES DE SOTAVENTO', '68669')
    assert entity_match('CONCEJO DE SAN ANDRES DE SOTAVENTO', sigep('CONCEJO DE SAN ANDRES', '68669'), TOWNS) is None


def test_qualified_strict_role_match_preserves_full_municipality():
    s = sigep('PERSONERÍA DE SAN CARLOS', '05649', 'Personeria Municipal')
    assert entity_match('ANTIOQUIA - PERSONERIA MUNICIPIO DE SAN CARLOS', s, TOWNS)
    assert not entity_match('ANTIOQUIA - PERSONERIA MUNICIPIO DE SAN CARLOS EXTRA', s, TOWNS)


def test_multi_scope_sigep_pool_is_not_applicable_fiscal_via():
    p, rows, ix = observation()
    other = dict(sigep(), SUBORDEN='Departamental', divipola='23001', NOMBRE='ASAMBLEA DE CORDOBA')
    ix['sigep']['123'] = [(0, sigep()), (1, other)]
    a = investigate(p, rows, ix, TOWNS)
    assert a['via_b_hypothesis_pool'] and not a['via_b']


def test_multi_account_entity_without_adjudication_is_not_merged():
    p, rows, ix = observation()
    q = dict(deepcopy(p), profile_id='another', account='account-2')
    a = dict(profile_id=p['profile_id'], affiliation='VERIFIED', territory_divipola='08078',
             entity_sigep_codes=['one'], institutional_family='CONCEJO', family_status='VERIFIED')
    b = dict(a, profile_id=q['profile_id'])
    logical, _ = reconcile([p, q], [a, b], {'strata': rows}, ix, [], {'profiles': []}, {'candidates': []})
    assert logical['units'] == []
    assert len(logical['decisions']) == 2
    assert not logical['auto_merge_applied']


def test_network_tripwire():
    with pytest.raises(RuntimeError, match='M2A_OFFLINE_NETWORK_PROHIBITED'):
        deny_network()


@pytest.fixture(scope='module')
def built(tmp_path_factory):
    out = tmp_path_factory.mktemp('m2a')
    original = socket.socket.connect
    socket.socket.connect = deny_network
    try:
        build(out)
    finally:
        socket.socket.connect = original
    return out


def test_frozen_reconciliation_and_m1b_delta(built):
    cov = read(built / 'coverage.json')
    assert cov['grains']['PHYSICAL_STRATUM'] == 15494
    assert cov['grains']['PHYSICAL_PROFILE'] == 11298
    assert cov['baseline_residual_reconciliation']['inside_recognized']['volumes']['unique_processes'] == 13272
    delta = read(built / 'family_mapping_m1b.json')['delta']
    residual = [d for d in delta if d['scope'] == 'RESIDUAL_COHORT']
    assert len(residual) == 39
    assert [d['changes']['family']['after'] for d in residual if d['changes']] == ['EDUCATION']


def test_frozen_hypotheses_and_canary(built):
    vias = read(built / 'affiliation_vias.json')
    assert vias['via_a']['hypothesis_pool']['physical_profiles'] == 232
    assert vias['via_a']['hypothesis_pool']['volumes']['unique_processes'] == 4823
    assert vias['via_b']['hypothesis_pool']['physical_profiles'] == 1814
    assert vias['canary']['council_personeria_strata'] == 538
    assert vias['canary']['own_role_row_present_strata'] == 444
    assert vias['canary']['classified_executive'] == 0
    assert vias['nit_territory_cardinality']['property_holds']


def test_profile_partition_and_unique_contract_provenance(built):
    ps = read(built / 'physical_profiles.json')['profiles']
    assert len({p['profile_id'] for p in ps}) == len(ps)
    assert all(p['volumes']['unique_contracts_provenance'] == 'DERIVED_FROM_GLOBAL_IDENTITY' for p in ps)
    assert read(built / 'gates.json')['TOTALS_RECONCILE'] == 'PASS'
    assert read(built / 'byte_identity.json')['changed'] == []


def test_every_compound_prefix_is_guarded():
    from scripts.audits.gate_divipola_buyer_matrix import discovery
    universe = discovery()
    for collision in universe['prefix_collisions']:
        name = 'CONCEJO MUNICIPAL DE ' + collision['full']
        for code in collision['full_codes']:
            assert guarded_town(name, code)
        for code in collision['prefix_codes']:
            assert not guarded_town(name, code)


def test_verified_territory_does_not_create_logical_identity(built):
    profiles = read(built / 'physical_profiles.json')['profiles']
    pending = [p for p in profiles if p['affiliation'] == 'VERIFIED' and not p['logical_unit_id']]
    assert pending
    assert all(not p['recognized'] for p in pending)
    cov = read(built / 'coverage.json')
    assert cov['universe']['VERIFIED_AFFILIATION_IDENTITY_UNRESOLVED'] == len(pending)


def test_gates_reject_single_signal_verification():
    from scripts.audits.m2a.gates import invalid_single_signal
    a = dict(affiliation='VERIFIED', evidence_type='NIT_ALONE', territory_divipola='08078',
             entity_sigep_codes=[], entity_municipal_codes=[],
             evidence_chain=[dict(evidence_type='NIT_BRIDGE_UNIQUE_TERRITORY')])
    assert invalid_single_signal(a)


def test_competing_directory_fiscal_evidence_is_not_filtered_away():
    p, rows, ix = observation()
    ix['sigep']['123'] = [(0, sigep())]
    ix['directory'][p['account']] = [(0, own_directory(p)), (1, dict(own_directory(p), nit_entidad='456'))]
    a = investigate(p, rows, ix, TOWNS)
    assert a['affiliation'] == 'AMBIGUOUS'
    assert any(c.get('type') == 'OWN_DIRECTORY_FISCAL_CONTRADICTION' for c in a['conflicts'] if isinstance(c, dict))
