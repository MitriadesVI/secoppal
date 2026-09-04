"""Adversarial checks for every M2B.1 identity relaxation and frozen accounting."""
from copy import deepcopy
import socket

import pytest

from scripts.audits.audit_municipal_m2b1 import build, offline
from scripts.audits.m2a.io import ROOT
from scripts.audits.m2b1.io import load, read, validate_output
from scripts.audits.m2b1.rules import (RULES, esp_legal_designator, evaluate,
    official_location_qualifier, officially_printed_acronym, trailing_care_level)

TOWNS = {t['divipola']: t for t in read(ROOT / 'app/data/municipal_territories_v1.json')['territories']}


def official(name='HOSPITAL ALFA - BARANOA', nit='123', code='one', territory='08078',
             nature='Empresas Sociales del Estado'):
    return dict(NOMBRE=name, NIT=nit, divipola=territory, SUBORDEN='Municipal',
                MUNICIPIO=TOWNS[territory]['municipality'],
                **{'CÓDIGO SIGEP': code, 'NATURALEZA JURÍDICA': nature})


def profile(name='ESE HOSPITAL ALFA', account='one', nit='123', territory='08078'):
    return dict(profile_id=account, account=account, physical_source_name=name, affiliation='VERIFIED',
                territory_divipola=territory, institutional_family='ESE_HEALTH', family_status='PROVISIONAL',
                logical_unit_id=None, recognized=False, fiscal_identity=[nit], stratum_indices=[0],
                volumes=dict(process_physical_rows=5, unique_processes=5,
                             contract_physical_rows=2, unique_contracts=2))


def context(name='ESE HOSPITAL ALFA', official_name='HOSPITAL ALFA - BARANOA'):
    p = profile(name)
    s = official(official_name)
    row = dict(codigo_entidad='one', nombre_entidad=name, nit_entidad='123', c_digo_ubicaci_n='CO-ATL-08078')
    old = dict(concordant_sigep_codes=[], existing_canonical_claims=[], matched_sigep=[])
    return dict(pending=[p], by_id={'one': p}, primary={'one': 'F'}, m2b_decisions={'one': old},
                m2b_accepted=[], accepted_ids=set(), target_all={'F': [p], 'C': [], 'I': [], 'H': []},
                target_new={'F': [p], 'C': [], 'I': [], 'H': []}, target_profiles=[p], sigep=[s],
                directory=[row], all_profiles=[p], all_by_id={'one': p}, aliases=[], towns=TOWNS)


@pytest.mark.parametrize('source,official_name', [
    ('ESE HOSPITAL ALFA', 'HOSPITAL ALFA - BARANOA'),
    ('ESE HOSPITAL ALFA DE BARANOA', 'HOSPITAL ALFA - BARANOA'),
    ('HOSPITAL ALFA E.S.E.', 'HOSPITAL ALFA DE BARANOA')])
def test_location_qualifier_requires_exact_full_town(source, official_name):
    assert official_location_qualifier(source, official(official_name), '08078', TOWNS)


@pytest.mark.parametrize('source,official_name', [
    ('ESE HOSPITAL DE BARANOA', 'HOSPITAL BARANOA'),
    ('ESE HOSPITAL ALFA REGIONAL', 'HOSPITAL ALFA - BARANOA'),
    ('ESE HOSPITAL ALFA', 'HOSPITAL ALFA - BARRANQUILLA')])
def test_location_rule_rejects_town_only_extra_qualifier_and_wrong_town(source, official_name):
    assert not official_location_qualifier(source, official(official_name), '08078', TOWNS)


def test_compound_town_is_indivisible():
    code = '23670'
    row = official('HOSPITAL ALFA - SAN ANDRES DE SOTAVENTO', territory=code)
    assert official_location_qualifier('ESE HOSPITAL ALFA', row, code, TOWNS)
    row['divipola'] = '68669'
    assert not official_location_qualifier('ESE HOSPITAL ALFA', row, '68669', TOWNS)


@pytest.mark.parametrize('suffix', ['NIVEL I', 'NIVEL II', 'NIVEL III', 'NIVEL 1', 'NIVEL 2', 'NIVEL 3'])
def test_only_declared_terminal_care_levels(suffix):
    assert trailing_care_level('ESE HOSPITAL ALFA', official('HOSPITAL ALFA ' + suffix), '08078', TOWNS)


def test_internal_or_free_text_care_level_is_rejected():
    assert not trailing_care_level('ESE HOSPITAL ALFA', official('HOSPITAL NIVEL II ALFA'), '08078', TOWNS)
    assert not trailing_care_level('ESE HOSPITAL ALFA', official('HOSPITAL ALFA BAJA COMPLEJIDAD'), '08078', TOWNS)


def test_esp_requires_own_public_services_nature():
    row = official('EMPRESA DE AGUA', nature='Empresas de Servicios Públicos Domiciliarios - Oficiales')
    assert esp_legal_designator('EMPRESA DE AGUA E.S.P.', row, '08078', TOWNS)
    row['NATURALEZA JURÍDICA'] = 'Establecimiento Público'
    assert not esp_legal_designator('EMPRESA DE AGUA E.S.P.', row, '08078', TOWNS)


def test_other_legal_forms_are_not_equated():
    row = official('EMPRESA DE AGUA S.A.', nature='Empresas de Servicios Públicos Domiciliarios - Oficiales')
    assert not esp_legal_designator('EMPRESA DE AGUA S.A.S. E.S.P.', row, '08078', TOWNS)


def test_acronym_must_be_printed_literally():
    row = official('INSTITUTO DE DEPORTE -INDER-')
    assert officially_printed_acronym('INDER', row, '08078', TOWNS)
    assert not officially_printed_acronym('ID', row, '08078', TOWNS)
    assert not officially_printed_acronym('HMI', official('HOSPITAL MATERNO INFANTIL'), '08078', TOWNS)
    assert not officially_printed_acronym('BARANOA', official('HOSPITAL ALFA -BARANOA-'), '08078', TOWNS)
    assert not officially_printed_acronym('ATLANTICO', official('HOSPITAL ALFA (ATLANTICO)'), '08078', TOWNS)


def test_complete_synthetic_chain_is_candidate():
    result = evaluate(context())
    assert len(result['accepted']) == 1
    assert result['accepted'][0]['identity_authority'] == RULES[1]


@pytest.mark.parametrize('change,reason', [
    ('fiscal', 'FISCAL_CONTRADICTION'), ('directory', 'OWN_DIRECTORY_CONTRADICTION'),
    ('scope', 'OFFICIAL_ENTITY_SCOPE_CONFLICT'), ('family', 'FAMILY_CONFLICT_OR_UNVERIFIED'),
    ('bogota', 'BOGOTA_RESERVED_REGISTRY')])
def test_contradictions_veto_candidate(change, reason):
    ctx = context()
    if change == 'fiscal':
        ctx['sigep'][0]['NIT'] = '456'
    elif change == 'directory':
        ctx['directory'][0]['nit_entidad'] = '456'
    elif change == 'scope':
        ctx['sigep'][0]['SUBORDEN'] = 'Departamental'
    elif change == 'family':
        ctx['sigep'][0]['NATURALEZA JURÍDICA'] = 'Alcaldía'
    else:
        ctx['target_profiles'][0]['territory_divipola'] = '11001'
        ctx['target_profiles'][0]['affiliation'] = 'VERIFIED'
        ctx['sigep'][0].update(divipola='11001', MUNICIPIO=TOWNS['11001']['municipality'])
        ctx['directory'][0]['c_digo_ubicaci_n'] = 'CO-DC-11001'
        ctx['towns'] = TOWNS
    decision = evaluate(ctx)['decisions'][0]
    assert decision['status'] == 'KEEP_REVIEW'
    assert reason in decision['contradictions']


def test_competing_account_blocks_without_merging():
    ctx = context()
    ctx['directory'].append(dict(ctx['directory'][0], codigo_entidad='two'))
    result = evaluate(ctx)
    assert not result['accepted']
    assert 'COMPETING_DIRECTORY_ACCOUNT' in result['decisions'][0]['contradictions']
    assert not result['decisions'][0]['merge_applied']


def test_same_nit_distinct_hospitals_remain_distinct():
    ctx = context()
    q = profile('ESE HOSPITAL BETA', 'two')
    ctx['profiles'] = ctx['all_profiles'] = [ctx['target_profiles'][0], q]
    ctx['all_by_id']['two'] = q
    ctx['target_profiles'].append(q)
    ctx['target_new']['F'].append(q)
    ctx['by_id']['two'] = q
    ctx['primary']['two'] = 'F'
    ctx['m2b_decisions']['two'] = dict(concordant_sigep_codes=[], existing_canonical_claims=[], matched_sigep=[])
    ctx['sigep'].append(official('HOSPITAL BETA - BARANOA', code='two'))
    ctx['directory'].append(dict(ctx['directory'][0], codigo_entidad='two', nombre_entidad='ESE HOSPITAL BETA'))
    result = evaluate(ctx)
    assert len(result['accepted']) == 2
    assert len({d['candidate_logical_unit_id'] for d in result['accepted']}) == 2


def test_existing_canonical_is_link_not_new_unit():
    ctx = context('EMPRESA SOCIAL DEL ESTADO HOSPITAL ALFA', 'HOSPITAL ALFA')
    p = ctx['target_profiles'][0]
    ctx['primary'][p['profile_id']] = 'C'
    ctx['m2b_decisions'][p['profile_id']] = dict(concordant_sigep_codes=['one'],
        existing_canonical_claims=[p['physical_source_name']],
        matched_sigep=[dict(sigep_index=0, sigep_code='one', territory='08078')])
    ctx['aliases'] = [dict(official_name=p['physical_source_name'])]
    result = evaluate(ctx)
    assert len(result['accepted']) == 1
    assert not result['accepted'][0]['creates_new_logical_unit']


def test_no_nit_value_can_make_names_match():
    ctx = context(official_name='HOSPITAL BETA - BARANOA')
    for value in ('123', '456'):
        ctx['sigep'][0]['NIT'] = value
        assert not evaluate(ctx)['accepted']


def test_offline_tripwire_and_output_boundary():
    original = socket.create_connection
    with offline(), pytest.raises(RuntimeError, match='M2B1_OFFLINE_NETWORK_PROHIBITED'):
        socket.create_connection(('example.invalid', 443))
    assert socket.create_connection is original
    for path in (ROOT, ROOT / 'app', ROOT / 'artifacts/audits/municipal_m2b'):
        with pytest.raises(ValueError, match='output'):
            validate_output(path)


@pytest.fixture(scope='module')
def built(tmp_path_factory):
    out = tmp_path_factory.mktemp('m2b1')
    build(out)
    return out


def test_frozen_input_and_delta_reconciliation(built):
    gates = read(built / 'gates.json')
    assert gates['audit_integrity'] == 'PASS'
    assert gates['INPUT']['F'] == {'profiles': 444, 'unique_processes': 168407, 'contract_rows': 28436}
    assert gates['PROFILES_RESOLVED'] == 50
    assert gates['NEW_LOGICAL_UNITS'] == 43
    assert gates['EXISTING_CANONICAL_PROFILES_LINKED'] == 7
    assert gates['M2B_ACCEPTED_CANDIDATES'] == 55


def test_rules_and_conflicts_fail_closed(built):
    rules = read(built / 'new_identity_rules.json')['rules']
    assert rules[RULES[1]]['profiles_accepted'] == 28
    assert rules[RULES[2]]['profiles_accepted'] == 1
    assert rules[RULES[3]]['profiles_accepted'] == 16
    assert rules[RULES[4]]['profiles_accepted'] == 0
    conflict = read(built / 'official_conflicts.json')
    assert conflict['resolved'] == 0 and conflict['keep_review'] == 59
    assert read(built / 'multi_account_audit.json')['productive_merges'] == 0


def test_impact_and_top_volume(built):
    impact = read(built / 'impact.json')
    assert impact['cumulative_m2b']['total_profiles_resolved'] == 105
    assert impact['cumulative_m2b']['remaining_identity_pending'] == 826
    assert impact['remaining']['unique_processes'] + impact['cumulative_m2b']['process_gain'] == 347163
    top = read(built / 'top_volume.json')['scenarios']
    assert [top[k]['profiles'] for k in ('TOP_25', 'TOP_50', 'TOP_100')] == [25, 50, 100]
    assert top['TOP_25']['unique_processes'] < top['TOP_50']['unique_processes'] < top['TOP_100']['unique_processes']


def test_hmi_stays_unresolved_and_no_directory_only_grant(built):
    ctx = load()
    hmi = next(p['profile_id'] for p in ctx['pending'] if p['physical_source_name'] == 'HMI')
    acronym = read(built / 'acronym_registry_audit.json')
    assert any(r['profile_id'] == hmi and r['status'] == 'UNRESOLVED_ACRONYM' for r in acronym['rows'])
    assert read(built / 'directory_exact_audit.json')['directory_only_identity_grants'] == 0


def test_build_does_not_mutate_loaded_inputs():
    ctx = load()
    before = deepcopy(ctx)
    evaluate(ctx)
    assert ctx == before
