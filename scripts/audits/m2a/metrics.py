"""Separate grains, disjoint denominators, hypotheses and territory products."""
from collections import Counter, defaultdict

from scripts.audits.m2a.io import ACC, BOGOTA, BOGOTA_AUTHORITY, CONTRACT_BASIS, DS, sum_volumes, volumes
from scripts.audits.m2a.matching import FAMILIES, provisional_family


def via_metrics(profiles, affiliations, analysis, ix):
    pmap = {p['profile_id']: p for p in profiles}
    result = {}
    for via in ('a', 'b'):
        pool = [a for a in affiliations if a[f'via_{via}_hypothesis_pool']]
        rows = [analysis['strata'][i] for a in pool for i in pmap[a['profile_id']]['stratum_indices']
                if analysis['strata'][i]['status'] == 'UNRESOLVED_AFFILIATION']
        applied = [a for a in affiliations if a[f'via_{via}']]
        result[f'via_{via}'] = dict(
            hypothesis_pool=dict(physical_profiles=len(pool), physical_strata=len(rows),
                                 territories=len({c for a in pool for c in a['bridge_territories' if via == 'a' else 'fiscal_territories']}),
                                 volumes=volumes(rows), profile_ids=[a['profile_id'] for a in pool]),
            applied_profiles=len(applied), status_counts=dict(Counter(a['affiliation'] for a in applied)),
            applied_territories=len({a['territory_divipola'] for a in applied if a['territory_divipola']}),
            verified_profile_ids=[a['profile_id'] for a in applied if a['affiliation'] == 'VERIFIED'],
            probable_profile_ids=[a['profile_id'] for a in applied if a['affiliation'] == 'PROBABLE'],
            entity_match_profiles=sum(bool(a['entity_sigep_codes']) for a in pool),
            directory_crosscheck_profiles=sum(a['directory_crosscheck'] for a in pool),
            directory_crosscheck_rate=sum(a['directory_crosscheck'] for a in pool) / len(pool) if pool else 0,
            verified_after_directory_crosscheck=sum(a['affiliation'] == 'VERIFIED' and a['directory_crosscheck'] for a in applied),
            contradictions=[dict(profile_id=a['profile_id'], conflicts=a['conflicts'], scopes=a['all_sigep_scopes'])
                            for a in pool if a['conflicts'] or len(a['all_sigep_scopes']) > 1])
    pool_b = [a for a in affiliations if a['via_b_hypothesis_pool']]
    alcalde = [a for a in pool_b if 'Alcaldía' in a['sigep_by_nit_natures']]
    canary = [a for a in alcalde if provisional_family(pmap[a['profile_id']]['physical_source_name']) in ('CONCEJO', 'PERSONERIA')]
    own_nature = [a for a in canary if any(n in a['sigep_by_nit_natures'] for n in ('Concejo Municipal', 'Personeria Municipal'))]
    strata_count = lambda aa: sum(len(pmap[a['profile_id']]['stratum_indices']) for a in aa)
    # This presence count intentionally does not claim that the row has matched the observed entity.
    result['canary'] = dict(pool_with_alcaldia_strata=strata_count(alcalde), council_personeria_strata=strata_count(canary),
                            council_personeria_profiles=len(canary), own_role_row_present_strata=strata_count(own_nature),
                            own_entity_matched_profiles=sum(bool(a['entity_sigep_codes']) for a in canary),
                            classified_executive=sum(a['institutional_family'] == 'MUNICIPAL_EXECUTIVE' for a in canary))
    sets = {nit: sorted({c for _, c in pairs}) for nit, pairs in ix['bridge'].items() if pairs}
    result['nit_territory_cardinality'] = dict(nits=len(sets), histogram=dict(Counter(map(len, sets.values()))),
                                            conflicting_nits={n: c for n, c in sets.items() if len(c) != 1},
                                            territories_by_nit=sets,
                                            property_holds=all(len(c) == 1 for c in sets.values()))
    result['acceptance_scope'] = ('Offline evidence chains only. A bridge alone is PROBABLE. SIGEP nature requires '
                                  'an independent entity match. VERIFIED B requires own exact directory crosscheck; '
                                  'this verifies territorial affiliation, never institutional identity or family. No runtime or promotion authority.')
    return result


def coverage(profiles, units, affiliations, analysis, reconciliation):
    verified = [p for p in profiles if p['affiliation'] == 'VERIFIED']
    recognized = [p for p in verified if p['recognized']]
    unrecognized = [p for p in verified if p['logical_unit_id'] and not p['recognized']]
    identity_pending = [p for p in verified if not p['logical_unit_id']]
    unresolved = [p for p in profiles if p['affiliation'] in ('UNRESOLVED', 'PROBABLE', 'AMBIGUOUS')]
    nonmunicipal = [p for p in profiles if p['affiliation'] == 'NON_MUNICIPAL']
    cohorts = {'all_observed': profiles, 'affiliated_verified': verified, 'recognized': recognized,
               'unrecognized_verified_logical': unrecognized, 'verified_affiliation_identity_unresolved': identity_pending,
               'unresolved_affiliation_pool': unresolved, 'non_municipal_verified': nonmunicipal}
    result = dict(cohorts={k: dict(physical_profiles=len(v), volumes=sum_volumes(v)) for k, v in cohorts.items()},
                  baseline_residual_reconciliation=reconciliation,
                  contract_measurement=dict(physical_rows='MEASURED', unique_contracts=CONTRACT_BASIS,
                      window_only=True, reason='Frozen global count(*) == count(distinct id_contrato); '
                      'per-profile contract DISTINCT was not captured. Never extrapolate outside this window.'),
                  affiliation_counts=dict(Counter(a['affiliation'] for a in affiliations)),
                  grains=dict(PHYSICAL_STRATUM=len(analysis['strata']), PHYSICAL_PROFILE=len(profiles), LOGICAL_BUYER_UNIT=len(units)),
                  universe=dict(TOTAL_PHYSICAL_PROFILES=len(profiles), TOTAL_LOGICAL_BUYER_UNITS_VERIFIED=len(units),
                      TOTAL_AFFILIATED_TO_MUNICIPAL_TERRITORY=len(verified),
                      TOTAL_RECOGNIZED_BY_SECOPPAL=sum(u['recognized'] for u in units),
                      TOTAL_UNRECOGNIZED=sum(not u['recognized'] for u in units),
                      TOTAL_UNRESOLVED=len(unresolved) + len(identity_pending),
                      NON_MUNICIPAL_PROFILES=len(nonmunicipal), VERIFIED_AFFILIATION_IDENTITY_UNRESOLVED=len(identity_pending)),
                  by_family={f: dict(physical_profiles=sum(p['institutional_family'] == f for p in profiles),
                                     verified_logical_units=sum(u['institutional_family'] == f for u in units),
                                     verified_affiliation_profiles=sum(p['institutional_family'] == f for p in verified),
                                     unresolved_profiles=sum(p['institutional_family'] == f for p in unresolved + identity_pending))
                             for f in FAMILIES})
    denominator = sum_volumes(verified)
    numerator = sum_volumes(recognized)
    unresolved_volume = sum_volumes(unresolved)
    result['municipal_volume_bounds'] = {m: dict(lower=denominator[m], upper=denominator[m] + unresolved_volume[m])
                                         for m in ('unique_processes', 'unique_contracts')}
    result['municipal_volume_bounds']['unique_contracts_provenance'] = CONTRACT_BASIS
    result['PROCESS_COVERAGE'] = numerator['unique_processes'] / denominator['unique_processes']
    result['CONTRACT_COVERAGE'] = numerator['unique_contracts'] / denominator['unique_contracts']
    result['denominator_contract'] = ('Recognized logical + unrecognized logical = verified logical units. '
        'Recognized profiles + unrecognized profiles + identity-unresolved profiles = verified territorial cohort. '
        'Verified territorial + affiliation-unresolved + nonmunicipal = all physical profiles. Never add unlike grains.')
    # The legacy 13,272 is an independent frozen ledger, not silently redefined by the new routes.
    recognized_towns = {r['municipality'] for r in analysis['strata'] if r['covered']}
    residue = defaultdict(list)
    for p in profiles:
        rows = [analysis['strata'][i] for i in p['stratum_indices'] if analysis['strata'][i]['status'] == ACC
                and not analysis['strata'][i]['covered'] and analysis['strata'][i]['municipality'] in recognized_towns]
        if rows:
            residue[p['institutional_family']].append(dict(profile_id=p['profile_id'], account=p['account'],
                name=p['physical_source_name'], territory=rows[0]['municipality'], volumes=volumes(rows)))
    result['inside_recognized_residue_by_family'] = {f: dict(profiles=pp, volumes=sum_volumes(pp)) for f, pp in sorted(residue.items())}
    result['unresolved_by_family'] = {f: sum_volumes([p for p in unresolved + identity_pending if p['institutional_family'] == f]) for f in FAMILIES}
    return result


def territories(profiles, affiliations, units, towns, analysis):
    result = []
    aff = {a['profile_id']: a for a in affiliations}
    buckets = {'MUNICIPAL_EXECUTIVE': 'executive', 'CONCEJO': 'concejo', 'PERSONERIA': 'personeria',
               'CONTRALORIA': 'contraloria', 'ESE_HEALTH': 'health', 'INSTITUTE': 'institutes', 'PUBLIC_COMPANY': 'public_companies'}
    for code, town in sorted(towns.items()):
        certain = [p for p in profiles if p['territory_divipola'] == code and p['affiliation'] == 'VERIFIED']
        pending = [p for p in profiles if code in aff[p['profile_id']]['candidate_territories']
                   and (p['affiliation'] != 'VERIFIED' or not p['logical_unit_id'])]
        local_units = [u for u in units if u['territory_divipola'] == code]
        groups = {k: [] for k in (*buckets.values(), 'other')}
        for u in local_units:
            groups[buckets.get(u['institutional_family'], 'other')].append(u['logical_unit_id'])
        row = dict(divipola=code, municipality=town['municipality'], department=town['department'], buyer_units=groups,
                   recognized_count=sum(u['recognized'] for u in local_units),
                   unrecognized_count=sum(not u['recognized'] for u in local_units), unresolved_count=len(pending),
                   unresolved_profile_ids=[p['profile_id'] for p in pending],
                   unresolved_membership='CONTEXT_ONLY_NONADDITIVE_ACROSS_TERRITORIES',
                   verified_profile_ids=[p['profile_id'] for p in certain], volumes=sum_volumes(certain),
                   uncovered_volumes=sum_volumes([p for p in certain if not p['recognized']]),
                   institutional_families=sorted({u['institutional_family'] for u in local_units}))
        if code == BOGOTA:
            old = [r for r in analysis['strata'] if r['municipality'] == BOGOTA]
            row.update(affiliation_authority=BOGOTA_AUTHORITY,
                       frozen_m0_observed_strata=len(old), frozen_m0_observed_profiles=len({(r['account'], r['name']) for r in old}),
                       frozen_m0_routes=dict(Counter(r['affiliation_basis'] for r in old)),
                       restriction='Only declared executive registry affirmed by M2A; other M0 affiliations remain historical context.')
        result.append(row)
    return result


def rankings(units, territory_profiles):
    unseen = [u for u in units if not u['recognized']]
    rank_units = lambda metric: sorted(unseen, key=lambda u: (-u['volumes'][metric], u['territory_divipola'], u['logical_unit_id']))
    return dict(tie_break='metric descending, DIVIPOLA ascending, stable logical ID ascending',
                TOP_UNRECOGNIZED_BUYER_UNITS_BY_PROCESSES=rank_units('unique_processes'),
                TOP_UNRECOGNIZED_BUYER_UNITS_BY_CONTRACTS=rank_units('unique_contracts'),
                TOP_TERRITORIES_BY_INSTITUTIONAL_COMPLEXITY=sorted(territory_profiles, key=lambda t: (
                    -len(t['institutional_families']), -(t['recognized_count'] + t['unrecognized_count']), t['divipola'])),
                TOP_TERRITORIES_BY_UNCOVERED_ACTIVITY=sorted(territory_profiles, key=lambda t: (
                    -t['uncovered_volumes']['unique_processes'], t['divipola'])))
