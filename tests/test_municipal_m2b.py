"""Falsify institutional shortcuts and verify the frozen M2B conservation ledger."""
from copy import deepcopy
import socket

import pytest

from scripts.audits.audit_municipal_m2b import build, offline
from scripts.audits.m2b.identity_vias import evaluate, strict_match
from scripts.audits.m2b.io import ROOT, read, partition, validate_output

TOWNS = {t['divipola']: t for t in read(ROOT / 'app/data/municipal_territories_v1.json')['territories']}


def observation(name='ESE HOSPITAL ALFA', account='one', sid='own', territory='08078', nit='123'):
    p = dict(profile_id=account, account=account, physical_source_name=name, affiliation='VERIFIED',
             territory_divipola=territory, institutional_family='ESE_HEALTH', family_status='PROVISIONAL',
             logical_unit_id=None, recognized=False, fiscal_identity=[nit], stratum_indices=[0],
             volumes=dict(process_physical_rows=5, unique_processes=5, contract_physical_rows=2, unique_contracts=2))
    a = dict(profile_id=account, candidate_territories=[territory], observed_bogota_context=territory == '11001',
             conflicts=[], entity_sigep_codes=[])
    s = dict(NOMBRE='HOSPITAL ALFA', NIT=nit, divipola=territory, MUNICIPIO=TOWNS[territory]['municipality'],
             SUBORDEN='Municipal', **{'CÓDIGO SIGEP': sid, 'NATURALEZA JURÍDICA': 'Empresas Sociales del Estado'})
    r = dict(codigo_entidad=account, nombre_entidad=name, nit_entidad=nit, c_digo_ubicaci_n=f'CO-ATL-{territory}')
    return p, a, s, r


def context():
    p, a, s, r = observation()
    return dict(profiles=[p], pending=[p], by_id={p['profile_id']: p}, aff={p['profile_id']: a}, sigep=[s],
                directory=[r], aliases=[], bindings=[], towns=TOWNS)


def append_case(ctx, name='ESE HOSPITAL BETA', account='two', sid='other', nit='123'):
    p, a, s, r = observation(name, account, sid, nit=nit)
    s['NOMBRE'] = name.removeprefix('ESE ')
    ctx['profiles'].append(p)
    ctx['pending'].append(p)
    ctx['by_id'][account] = p
    ctx['aff'][account] = a
    ctx['sigep'].append(s)
    ctx['directory'].append(r)
    return p


@pytest.mark.parametrize('name', ['ESE HOSPITAL ALFA', 'E.S.E. HOSPITAL ALFA',
    'EMPRESA SOCIAL DEL ESTADO HOSPITAL ALFA', 'HOSPITAL ALFA EMPRESA SOCIAL DEL ESTADO', 'HOSPITAL ALFA E.S.E.'])
def test_own_legal_nature_licenses_boundary_label(name):
    _, _, s, _ = observation()
    assert strict_match(name, s, TOWNS) == 'OWN_SIGEP_ESE_LEGAL_LABEL_EXACT_CORE'


@pytest.mark.parametrize('name', ['ESE HOSPITAL BETA', 'ESE HOSPITAL ALFA REGIONAL', 'HMI',
                                  'ESE ALFA', 'ESE HOSPITAL ALFAA', 'ESE ESE HOSPITAL ALFA'])
def test_no_substring_fuzzy_or_generic_abbreviation(name):
    _, _, s, _ = observation()
    assert strict_match(name, s, TOWNS) is None


def test_legal_nature_cannot_be_borrowed():
    ctx = context()
    ctx['sigep'][0]['NATURALEZA JURÍDICA'] = 'Alcaldía'
    assert not evaluate(ctx)['accepted']


def test_town_alone_and_compound_prefix_are_rejected():
    _, _, s, _ = observation()
    s['NOMBRE'] = 'BARANOA'
    assert strict_match('ESE BARANOA', s, TOWNS) is None
    s.update(NOMBRE='HOSPITAL SAN ANDRES DE SOTAVENTO', divipola='68669', MUNICIPIO='SAN ANDRÉS')
    assert strict_match('ESE HOSPITAL SAN ANDRES DE SOTAVENTO', s, TOWNS) is None


def test_one_complete_independent_chain_is_candidate():
    result = evaluate(context())
    assert len(result['accepted']) == 1
    assert result['accepted'][0]['family_status'] == 'VERIFIED'


def test_directory_alone_is_not_institutional_identity():
    ctx = context()
    ctx['sigep'] = []
    result = evaluate(ctx)
    assert not result['accepted'] and result['decisions'][0]['own_directory_clean']


def test_same_territory_same_family_same_nit_distinct_hospitals():
    ctx = context()
    append_case(ctx)
    result = evaluate(ctx)
    assert len(result['accepted']) == 2
    assert len({d['candidate_logical_unit_id'] for d in result['accepted']}) == 2
    assert not any(d['merge_applied'] for d in result['decisions'])


def test_competing_observed_account_blocks_even_unverified():
    ctx = context()
    q = append_case(ctx, 'ESE HOSPITAL ALFA', sid='own')
    q['affiliation'] = 'UNRESOLVED'
    result = evaluate(ctx)
    assert not result['accepted']
    assert 'COMPETING_OBSERVED_ACCOUNTS' in result['decisions'][0]['reasons']


def test_nontransacting_directory_account_blocks():
    ctx = context()
    r = dict(ctx['directory'][0], codigo_entidad='other', nombre_entidad='HOSPITAL ALFA E.S.E.')
    ctx['directory'].append(r)
    d = evaluate(ctx)['decisions'][0]
    assert d['status'] == 'REVIEW' and d['competing_directory_accounts']


def test_unexplained_fiscal_peer_is_veto_not_merge():
    ctx = context()
    ctx['directory'].append(dict(ctx['directory'][0], codigo_entidad='other', nombre_entidad='HA'))
    result = evaluate(ctx)
    assert not result['accepted']
    assert 'UNEXPLAINED_DIRECTORY_FISCAL_PEER_ACCOUNT' in result['decisions'][0]['reasons']


def test_same_account_other_institution_blocks():
    ctx = context()
    ctx['directory'].append(dict(ctx['directory'][0], nombre_entidad='HOSPITAL BETA'))
    assert 'ACCOUNT_DIRECTORY_NAME_CONFLICT' in evaluate(ctx)['decisions'][0]['reasons']


@pytest.mark.parametrize('change', ['fiscal', 'directory', 'nature', 'scope', 'territory', 'canonical', 'bogota'])
def test_contradictions_remain_review(change):
    ctx = context()
    if change == 'fiscal':
        ctx['sigep'][0]['NIT'] = '456'
    elif change == 'directory':
        ctx['directory'][0]['nit_entidad'] = '456'
    elif change == 'nature':
        ctx['sigep'][0]['NATURALEZA JURÍDICA'] = 'Concejo Municipal'
    elif change == 'scope':
        ctx['sigep'][0]['SUBORDEN'] = 'Departamental'
    elif change == 'territory':
        ctx['directory'][0]['c_digo_ubicaci_n'] = 'CO-ATL-08001'
    elif change == 'canonical':
        ctx['aliases'] = [dict(official_name='ESE HOSPITAL ALFA')]
    else:
        ctx['aff']['one']['observed_bogota_context'] = True
    assert not evaluate(ctx)['accepted']


def test_one_sigep_code_with_two_scopes_blocks():
    ctx = context()
    ctx['sigep'].append(dict(ctx['sigep'][0], divipola='08001'))
    assert 'OFFICIAL_ENTITY_MULTIPLE_SCOPES' in evaluate(ctx)['decisions'][0]['reasons']


def test_no_fiscal_value_can_create_match():
    ctx = context()
    ctx['sigep'][0]['NOMBRE'] = 'ALCALDIA DE BARANOA'
    for nit in ('123', '456'):
        ctx['sigep'][0]['NIT'] = nit
        assert not evaluate(ctx)['accepted']


def test_fail_before_writes_on_wrong_universe():
    with pytest.raises(ValueError, match='STOP'):
        partition([])
    for path in (ROOT / 'app', ROOT / 'artifacts/audits/municipal_m2a', ROOT):
        with pytest.raises(ValueError, match='output'):
            validate_output(path)


def test_actual_socket_tripwire_and_restoration():
    original = socket.create_connection
    with offline(), pytest.raises(RuntimeError, match='M2B_OFFLINE_NETWORK_PROHIBITED'):
        socket.create_connection(('example.invalid', 443))
    assert socket.create_connection is original


@pytest.fixture(scope='module')
def built(tmp_path_factory):
    out = tmp_path_factory.mktemp('m2b')
    build(out)
    return out


def test_frozen_partition_and_volume_conservation(built):
    pending = read(built / 'pending_identity_profiles.json')['summary']
    assert (pending['profiles'], pending['unique_processes'], pending['contract_rows']) == (931, 347163, 117277)
    tax = read(built / 'identity_failure_taxonomy.json')
    assert sum(c['primary']['profiles'] for c in tax['classes'].values()) == 931
    assert sum(f['unique_processes'] for f in tax['family_priority']) == 347163
    assert read(built / 'verified_logical_unrecognized.json')['summary']['profiles'] == 370


def test_frozen_candidates_and_remaining_have_no_lost_debt(built):
    gate = read(built / 'gates.json')
    assert gate['audit_integrity'] == 'PASS'
    assert gate['profiles_resolved'] > 0
    assert gate['profiles_resolved'] + gate['remaining_identity_pending'] == 931
    assert read(built / 'byte_identity.json')['changed'] == []
    via = read(built / 'identity_vias.json')
    assert via['vias']['VIA_I']['profiles'] == via['vias']['VIA_II']['profiles'] == 0
    assert via['vias']['VIA_III']['profiles'] == 931


def test_scenarios_and_territory_delta_reconcile(built):
    impact = read(built / 'impact.json')
    safe = impact['scenarios']['ALL_RULE_BASED']
    assert impact['remaining']['unique_processes'] + safe['processes_resolved'] == 347163
    assert impact['remaining']['contract_rows'] + safe['contract_rows_resolved'] == 117277
    assert impact['scenarios']['TOP_SAFE_50']['profiles_resolved'] == min(50, safe['profiles_resolved'])
    delta = read(built / 'territory_delta.json')['territories']
    assert len(delta) == 1103
    assert sum(len(t['newly_adjudicated_candidate_buyer_units']) for t in delta) == safe['logical_units_created']
    assert not next(t for t in delta if t['divipola'] == '11001')['newly_adjudicated_candidate_buyer_units']


def test_build_does_not_mutate_frozen_payload():
    from scripts.audits.m2b.io import load
    ctx = load()
    before = deepcopy(ctx)
    evaluate(ctx)
    assert ctx == before
