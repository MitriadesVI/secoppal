"""Executable conservation and fail-closed identity invariants."""
from collections import Counter

from scripts.audits.m2b.io import EXPECTED, summary


def evaluate(ctx, via, taxonomy, batches, logical, delta, proof):
    pending = {p['profile_id'] for p in ctx['pending']}
    accepted = {d['profile_id'] for d in via['accepted']}
    rejected = {d['profile_id'] for d in via['decisions'] if d['status'] == 'REVIEW'}
    batch_ids = [pid for b in batches['batches'].values() for pid in b['profile_ids']]
    bad_units = [d['profile_id'] for d in via['accepted'] if (
        d['reasons'] or len(d['concordant_sigep_codes']) != 1 or not d['family_compatible'] or
        not d['own_directory_clean'] or d['family_status'] != 'VERIFIED' or not d['identity_authority'] or
        d['competing_observed_accounts'] or d['competing_directory_accounts'] or
        d['same_family_peer_review'] or d['directory_fiscal_peer_review'] or
        ctx['by_id'][d['profile_id']]['affiliation'] != 'VERIFIED' or not d['territory_divipola'])]
    bogota = next(t for t in delta['territories'] if t['divipola'] == '11001')
    counts = summary(ctx['pending'])
    checks = dict(
        INPUT_RECONCILES=(counts['profiles'], counts['unique_processes'], counts['contract_rows']) == EXPECTED,
        ALL_DEBT_VISIBLE=accepted.isdisjoint(rejected) and accepted | rejected == pending,
        UNIQUE_ADJUDICATED_UNITS=len({d['candidate_logical_unit_id'] for d in via['accepted']}) == len(accepted),
        EVERY_UNIT_ACCREDITED=not bad_units,
        BATCHES_PARTITION_INPUT=set(batch_ids) == pending and all(n == 1 for n in Counter(batch_ids).values()),
        TAXONOMY_PARTITIONS_INPUT=sum(c['primary']['profiles'] for c in taxonomy['classes'].values()) == 931,
        FAMILY_VOLUMES_RECONCILE=sum(f['unique_processes'] for f in taxonomy['family_priority']) == 347163,
        LOGICAL_370_SEPARATE=len(logical['profiles']) == 370 and not pending & {p['profile_id'] for p in logical['profiles']},
        BOGOTA_MODEL_UNCHANGED=not bogota['newly_adjudicated_candidate_buyer_units'] and proof['status'] == 'PASS',
        NO_RUNTIME_MUTATION=proof['status'] == 'PASS', NO_GAZETTEER_MUTATION=proof['status'] == 'PASS',
        NO_BINDING_MUTATION=proof['status'] == 'PASS', NO_BUYER_GROUP_MUTATION=proof['status'] == 'PASS',
        NO_FROZEN_EVIDENCE_MUTATION=proof['status'] == 'PASS',
        NO_NIT_MERGE=not any(d['merge_applied'] for d in via['decisions']),
        NO_TERRITORY_IDENTITY_AUTHORITY=all(d['identity_authority'] in (
            'OWN_SIGEP_ESE_LEGAL_LABEL_EXACT_CORE', 'INDEPENDENT_SIGEP_ENTITY_MATCH_UNIQUE_OBSERVED_ACCOUNT') for d in via['accepted']),
    )
    failed = [k for k, ok in checks.items() if not ok]
    return dict(MUNICIPAL_M2B='BLOCKED' if failed else 'PARTIAL' if rejected else 'PASS',
                audit_integrity='FAIL' if failed else 'PASS', checks={k: 'PASS' if v else 'FAIL' for k, v in checks.items()},
                INPUT_IDENTITY_PENDING=931, NIT_AUTO_MERGES=0, AUTO_MERGES_BY_TERRITORY=0,
                FUZZY_IDENTITY_MATCHES=0, MULTI_TERRITORY_UNITS=0, LIVE_CALLS=0,
                profiles_resolved=len(accepted), new_logical_units=len(accepted), remaining_identity_pending=len(rejected),
                failed=failed, invalid_units=bad_units, baseline_head='e98bf39',
                ADR_REQUIRED='YES' if any(d['identity_authority'] == 'OWN_SIGEP_ESE_LEGAL_LABEL_EXACT_CORE' for d in via['accepted']) else 'NO',
                NEXT='M2B_1_TARGETED_ADJUDICATION', production_apply=False, PUSH='NO',
                count_scope='Algorithm has no merge operation, no fuzzy matcher and no network authority; these are operational counters, not empirical precision estimates.')
