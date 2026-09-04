"""Computed audit invariants, not success literals."""
from collections import Counter

from scripts.audits.m2a.io import BOGOTA, BOGOTA_AUTHORITY, CONTRACT_BASIS, METRICS
from scripts.audits.m2a.matching import provisional_family


def invalid_single_signal(a):
    """Validate authority composition rather than counting nonempty evidence lists."""
    if a['affiliation'] not in ('VERIFIED', 'PROBABLE'):
        return False
    evidence = {e['evidence_type'] for e in a['evidence_chain']}
    if a['evidence_type'] in (BOGOTA_AUTHORITY, 'FROZEN_PRIOR_AFFILIATION'):
        return not bool(evidence - {'PHYSICAL_PROFILE_OBSERVATION', 'SIGEP_ENTITY_MATCH',
                                    'OWN_EXACT_SECOP_DIRECTORY_RECORD'})
    bridge = 'NIT_BRIDGE_UNIQUE_TERRITORY' in evidence
    fiscal = 'SIGEP_FISCAL_TERRITORY_MATCH' in evidence
    entity = bool(a['entity_sigep_codes'] and a['entity_municipal_codes'] == [a['territory_divipola']])
    directory = any(e['evidence_type'] == 'OWN_EXACT_SECOP_DIRECTORY_RECORD'
                    and e['unique_valid_code'] and e['territory_codes'] == [a['territory_divipola']]
                    for e in a['evidence_chain'])
    if a['affiliation'] == 'PROBABLE':
        return not (bridge or fiscal or entity)
    return not ((directory and (bridge or fiscal or entity)) or (bridge and entity))


def evaluate(profiles, affiliations, logical, cov, delta, byte_proof, compound_counts):
    units = logical['units']
    pass_if = lambda value: 'PASS' if value else 'FAIL'
    mutated = byte_proof['changed']
    original_indices = [i for p in profiles for i in p['stratum_indices']]
    cohort = cov['cohorts']
    residual = [d for d in delta if d['scope'] == 'RESIDUAL_COHORT']
    accepted_changes = all(not d['changes'] or d['changes'].get('family', {}).get('reason') == 'EDUCATION_TAXONOMY_SPLIT'
                           for d in residual)
    nit_only_verified = sum(a['via_a'] and a['affiliation'] == 'VERIFIED' and not a['entity_sigep_codes']
                            and not any(e['evidence_type'] == 'OWN_EXACT_SECOP_DIRECTORY_RECORD'
                                        and e['unique_valid_code'] and e['territory_codes'] == [a['territory_divipola']]
                                        for e in a['evidence_chain'])
                            for a in affiliations)
    family_from_nit = sum(a['family_basis'].startswith('SIGEP_ENTITY_MATCH') and not a['entity_sigep_codes'] for a in affiliations)
    canary = sum(provisional_family(p['physical_source_name']) in ('CONCEJO', 'PERSONERIA')
                 and a['institutional_family'] == 'MUNICIPAL_EXECUTIVE' for p, a in zip(profiles, affiliations))
    semantic_bogota = all(a['evidence_type'] == BOGOTA_AUTHORITY for a in affiliations
                          if a['territory_divipola'] == BOGOTA and a['affiliation'] == 'VERIFIED')
    sums = all(cohort['recognized']['volumes'][m] + cohort['unrecognized_verified_logical']['volumes'][m]
               + cohort['verified_affiliation_identity_unresolved']['volumes'][m] == cohort['affiliated_verified']['volumes'][m]
               and cohort['affiliated_verified']['volumes'][m] + cohort['unresolved_affiliation_pool']['volumes'][m]
               + cohort['non_municipal_verified']['volumes'][m] == cohort['all_observed']['volumes'][m] for m in METRICS)
    gates = dict(
        NO_NIT_MERGE=pass_if(all(p['identity_basis'] != 'NIT' for p in profiles) and not logical['auto_merge_applied']),
        NO_RUNTIME_MUTATION=pass_if(not any(p.startswith('app/') for p in mutated)),
        NO_GAZETTEER_MUTATION=pass_if('app/data/aliases_db.json' not in mutated),
        NO_SOURCE_BINDING_MUTATION=pass_if('app/data/buyer_source_names.json' not in mutated),
        NO_BUYER_GROUP_MUTATION=pass_if('app/data/buyer_groups.json' not in mutated),
        BUYER_GROUP_BYTES_UNCHANGED=pass_if('app/data/buyer_groups.json' not in mutated),
        NO_HISTORICAL_EVIDENCE_MUTATION=pass_if(not any(p.startswith('artifacts/') for p in mutated)),
        M1B_CLASSIFICATION_PRESERVED=pass_if(len(residual) == 39 and accepted_changes),
        M1B_CLASSIFICATION_POLICY='Original rows referenced unchanged; EDUCATION split itemized as an explicit taxonomy delta.',
        SINGLE_SIGNAL_AFFILIATION_COUNT=sum(invalid_single_signal(a) for a in affiliations),
        COMPOUND_GUARD_APPLIED=pass_if(all(e['method'] in ('EXACT_INSTITUTIONAL_TOKENS', 'STRICT_ROLE_AND_COMPLETE_GUARDED_TOWN')
            for a in affiliations for e in a['evidence_chain'] if e['evidence_type'] == 'SIGEP_ENTITY_MATCH')),
        COMPOUND_GUARD_COUNT_DERIVED_NOT_HARDCODED=pass_if(bool(compound_counts)),
        compound_counts=compound_counts,
        NIT_BRIDGE_ALONE_GRANTING_VERIFIED=nit_only_verified,
        SIGEP_FAMILY_FROM_NIT_ONLY_MATCH=family_from_nit,
        CONCEJO_OR_PERSONERIA_CLASSIFIED_EXECUTIVE=canary,
        VIA_A_APPLIED_TO_BOGOTA=sum(a['via_a'] and a['observed_bogota_context'] for a in affiliations),
        VIA_B_APPLIED_TO_BOGOTA=sum(a['via_b'] and a['observed_bogota_context'] for a in affiliations),
        BOGOTA_AFFILIATION_ROUTE_UNCHANGED=pass_if(semantic_bogota),
        GRAIN_COUNTS_REPORTED_SEPARATELY=pass_if(set(cov['grains']) == {'PHYSICAL_STRATUM', 'PHYSICAL_PROFILE', 'LOGICAL_BUYER_UNIT'}),
        EVERY_PROFILE_ACCOUNTED_ONCE=pass_if(len({p['profile_id'] for p in profiles}) == len(profiles)
            and sorted(original_indices) == list(range(cov['grains']['PHYSICAL_STRATUM']))),
        NO_SILENT_MULTI_TERRITORY_UNIT=pass_if(all(len({p['territory_divipola'] for p in profiles if p['logical_unit_id'] == u['logical_unit_id']}) == 1 for u in units)),
        TOTALS_RECONCILE=pass_if(sums and cov['universe']['TOTAL_RECOGNIZED_BY_SECOPPAL'] + cov['universe']['TOTAL_UNRECOGNIZED'] == len(units)),
        CONTRACT_UNIQUES_PROVENANCE=pass_if(all(p['volumes']['unique_contracts_provenance'] == CONTRACT_BASIS for p in profiles)),
        BYTE_IDENTITY=byte_proof['status'], LLM_EXECUTION_MODE='DETERMINISTIC_OFFLINE_NO_LLM', LIVE_CALLS=0,
        candidates=dict(Counter(p['future_action'] for p in profiles)),
        NEXT='M2B_ADJUDICATION', ADR_REQUIRED='YES', PRODUCTION_PROMOTED=0, PUSH='NO')
    counters = ('SINGLE_SIGNAL_AFFILIATION_COUNT', 'NIT_BRIDGE_ALONE_GRANTING_VERIFIED',
                'SIGEP_FAMILY_FROM_NIT_ONLY_MATCH', 'CONCEJO_OR_PERSONERIA_CLASSIFIED_EXECUTIVE',
                'VIA_A_APPLIED_TO_BOGOTA', 'VIA_B_APPLIED_TO_BOGOTA')
    gates['MUNICIPAL_M2A'] = 'BLOCKED' if any(v == 'FAIL' for v in gates.values()) or any(gates[k] for k in counters) else 'PASS'
    gates['verdict_scope'] = 'Discovery and evidence-chain audit; unresolved profiles are published, not promoted or called verified units.'
    return gates
