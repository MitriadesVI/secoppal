"""M1B must explain the fixed cohort without turning geography/NIT into identity."""
from copy import deepcopy
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from scripts.audits.audit_municipal_m1b import (
    ACCREDITED, DATASETS, DEBTS, FAMILIES, METRICS, candidate, coverage_scenario,
    derive_universe, family, sum_volumes, volumes,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/audits/municipal_m1b'


def read(path):
    raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == '.gz' else raw)


@pytest.fixture(scope='module')
def evidence():
    return (read(ROOT / 'artifacts/audits/municipal_m1a_final/m0_post_analysis.json.gz'),
            read(OUT / 'buyer_profiles.json')['profiles'], read(OUT / 'residual_territories.json'))


def test_universe_is_an_exact_set_difference_and_every_stratum_is_accounted_for(evidence):
    post, profiles, report = evidence
    active, recognized, residual = derive_universe(post)
    assert residual == set(report['residual_divipola']) == active - recognized
    primary = [p for p in profiles if p['scope'] == 'RESIDUAL_COHORT']
    expected = {i for i, r in enumerate(post['strata']) if r['status'] == ACCREDITED and r['municipality'] in residual}
    observed = [s['index'] for p in primary for s in p['strata']]
    assert set(observed) == expected and len(observed) == len(set(observed))
    assert sum_volumes([p['uncovered_cohort_volumes'] for p in primary]) == volumes([post['strata'][i] for i in expected])


def test_changed_universe_stops_instead_of_truncating_to_36(evidence):
    post = deepcopy(evidence[0])
    row = next(r for r in post['strata'] if r['municipality'] in evidence[2]['residual_divipola'])
    row['covered'] = True
    with pytest.raises(ValueError, match='STOP'):
        derive_universe(post)


def test_profiles_preserve_accounts_physical_names_and_fiscal_evidence(evidence):
    post, profiles, _ = evidence
    keys = [(p['account'], p['physical_source_name']) for p in profiles]
    assert len(keys) == len(set(keys))
    for p in profiles:
        rows = [post['strata'][s['index']] for s in p['strata']]
        assert {(r['account'], r['name']) for r in rows} == {(p['account'], p['physical_source_name'])}
        assert p['volumes'] == volumes(rows)
        assert p['nit_evidence'] == sorted({r['nit_evidence'] for r in rows})
    # Same physical name, two municipalities and two accounts cannot be one owner.
    homonyms = [p for p in profiles if p['physical_source_name'] == 'MUNICIPIO DE SAN FRANCISCO'
                and p['scope'] == 'RESIDUAL_COHORT']
    assert {p['divipola'] for p in homonyms} == {'25658', '86755'}
    assert len({p['account'] for p in homonyms}) == 2
    assert all(not p['current_owner_asserted'] for p in homonyms)


def test_distinct_profiles_sharing_nit_are_not_collapsed(evidence):
    profiles = evidence[1]
    by_id = {p['profile_id']: p for p in profiles}
    shared = [(p, by_id[peer]) for p in profiles for peer in p['shared_nit_other_profiles']]
    assert shared
    for p, q in shared:
        assert p['account'] != q['account'] and p['profile_id'] != q['profile_id']
    angelopolis = [p for p in profiles if p['scope'] == 'RESIDUAL_COHORT' and p['divipola'] == '05036']
    assert len(angelopolis) == 2
    assert len({p['account'] for p in angelopolis}) == 1
    assert {p['physical_source_name'] for p in angelopolis} == {
        'ALCALDIA MUNICIPIO DE ANGELOPOLIS.', 'ALCALDIA MUNICIPIO DE ANGELOPOLIS..'}
    assert all(p['debt'] == 'SOURCE_NAME_VARIANT' for p in angelopolis)


@pytest.mark.parametrize('nature,expected', [
    ('Empresas Sociales del Estado', 'ESE_HEALTH'),
    ('Concejo Municipal', 'CONCEJO'), ('Personeria Municipal', 'PERSONERIA'),
    ('Órganos autónomos', 'UNKNOWN_REVIEW'),
])
def test_official_legal_nature_wins_over_misleading_name(nature, expected):
    assert family('ALCALDIA HOSPITAL EMPRESA', [{'NATURALEZA JURÍDICA': nature}])[0] == expected


def test_regional_autonomous_entity_is_reviewed_not_counted_as_an_executive(evidence):
    p = next(p for p in evidence[1] if p['physical_source_name'] == 'CORPORACION AUTONOMA REGIONAL DE SANTANDER+')
    assert p['scope'] == 'CONTEXT_ONLY'
    assert all(s['status'] == 'ACCREDITED_OTHER_AFFILIATION' for s in p['strata'])
    assert p['institutional_family'] == 'UNKNOWN_REVIEW' and p['debt'] == 'REVIEW'
    assert not p['current_owner_asserted']


def test_directory_authority_never_credits_nonexecutive_residual_volume(evidence):
    gates = read(OUT / 'gates.json')
    directory = gates['directory_authority']
    assert directory['residual_accredited_profile_affiliations_added'] == 0
    newly = directory['newly_adjudicable_existing_units']
    assert [(r['canonical'], r['codes']) for r in newly] == [
        ('ALCALDIA MUNICIPIO DE SAN GIL - SANTANDER', ['68679'])]
    territories = {t['divipola']: t for t in evidence[2]['territories']}
    for code in ['68081', '68679', '76892']:
        assert territories[code]['EXECUTIVE_ALREADY_RECOGNIZED'] == 'YES'
        assert not territories[code]['accredited_executive_profile_ids']
        assert territories[code]['other_municipal_profile_ids']


def test_cut_scenarios_are_additive_unique_counts_with_fixed_denominators(evidence):
    ranking = read(OUT / 'impact_ranking.json')
    rows = ranking['rows']
    assert rows == sorted(rows, key=lambda r: (-r['unique_processes'], -r['unique_contracts'], r['divipola']))
    for label, count in [('TOP_5', 5), ('TOP_10', 10), ('TOP_20', 20), ('ALL_36', 36)]:
        expected = coverage_scenario(rows[:count], ranking['baseline'])
        for key, value in expected.items():
            assert ranking['cuts'][label][key] == value
    residual = ranking['cuts']['ALL_36']['incremental_volume']
    other = ranking['uncovered_in_already_recognized_territories']
    for metric in METRICS:
        assert ranking['baseline']['denominator'][metric] == ranking['baseline']['covered'][metric] + residual[metric] + other[metric]
    assert rows[0]['process_physical_rows'] >= rows[0]['unique_processes']
    assert all(r['contract_value'] is None for r in rows)


def test_ambiguous_or_incomplete_candidate_never_becomes_auto_safe(evidence):
    primary = [p for p in evidence[1] if p['scope'] == 'RESIDUAL_COHORT']
    for p in primary:
        c = candidate(p)
        assert p['institutional_family'] in FAMILIES and p['debt'] in DEBTS
        if p['recent_source_collisions']:
            assert c['disposition'] == 'KEEP_SEPARATE'
        assert not c['safe_to_apply']
        assert 'PROSPECTIVE_PIPELINE_NOT_VALIDATED_NO_CANDIDATE_CATALOGUE' in c['blocking_requirements']
    summary = read(OUT / 'classification.json')
    assert sum(summary['residual_cohort']['by_family'].values()) == len(primary)
    assert sum(summary['debt_counts'].values()) == len(primary)
    candidates = read(OUT / 'promotion_candidates.json')
    assert sum(candidates['residual_dispositions'].values()) == len(primary)


def test_every_protected_baseline_file_retains_its_digest():
    gates = read(OUT / 'gates.json')
    assert gates['WRONG_OWNER'] == 0 and not gates['protected_changed']
    assert all(r['owner_check_passed'] for r in gates['existing_executive_pipeline'])
    assert all(not r['pipeline_safe'] for r in gates['existing_executive_pipeline'])
    for path, expected in gates['protected_sha256'].items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == expected, path
