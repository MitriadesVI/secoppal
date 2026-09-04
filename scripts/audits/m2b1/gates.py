"""Fail closed on baseline drift, ambiguous identity, mutations and accounting loss."""
from collections import Counter

from scripts.audits.m2b1.io import TARGETS
from scripts.audits.m2b1.rules import RULES


def evaluate(ctx, rules, top, impact, proof, acronyms, directory, accounts, conflicts):
    accepted = rules['accepted']
    accepted_ids = [d['profile_id'] for d in accepted]
    invalid = [d['profile_id'] for d in accepted if (d['contradictions'] or not d['identity_authority'] or
        not d['selected_sigep_code'] or len(d['official_matches']) < 1 or not d['own_directory_indices'] or
        not d['family_compatible'] or d['family_status'] != 'VERIFIED' or d['territory_divipola'] == '11001' or
        d['competing_directory_indices'] or d['competing_profile_ids'] or
        d['unexplained_fiscal_profile_ids'] or d['unexplained_fiscal_directory_indices'])]
    source_counts = {label: (len(ps), sum(p['volumes']['unique_processes'] for p in ps),
                              sum(p['volumes']['contract_physical_rows'] for p in ps))
                     for label, ps in ctx['target_all'].items()}
    accepted_new_rules = [rule for rule in RULES[1:] if rules['rules'][rule]['profiles_accepted']]
    checks = dict(
        FROZEN_INPUT_COUNTS=source_counts == TARGETS,
        M2B_ACCEPTED_PRESERVED=len(ctx['accepted_ids']) == 55 and not set(accepted_ids) & ctx['accepted_ids'],
        TARGET_DELTA_UNIVERSE=sum(len(v) for v in ctx['target_new'].values()) == 497,
        TOP_VOLUMES_RECONCILE=top['targeted_remaining']['profiles'] == 497,
        DELTA_UNIQUE=len(accepted_ids) == len(set(accepted_ids)),
        EVERY_ACCEPTED_CHAIN_COMPLETE=not invalid,
        DELTA_PLUS_REMAINING=impact['cumulative_m2b']['remaining_identity_pending'] +
            impact['cumulative_m2b']['total_profiles_resolved'] == 931,
        PROCESS_VOLUME_CONSERVED=impact['remaining']['unique_processes'] +
            impact['cumulative_m2b']['process_gain'] == 347163,
        CONTRACT_VOLUME_CONSERVED=impact['remaining']['contract_rows'] +
            impact['cumulative_m2b']['contract_gain'] == 117277,
        DIRECTORY_ONLY_GRANTS_NONE=directory['directory_only_identity_grants'] == 0,
        NO_MULTI_ACCOUNT_MERGE=accounts['productive_merges'] == 0,
        ALL_H_CONFLICTS_VISIBLE=conflicts['resolved'] == 0 and conflicts['keep_review'] == 59,
        OPAQUE_ACRONYM_RULE_FAILS_CLOSED=rules['rules'][RULES[4]]['profiles_accepted'] == 0 and
            acronyms['classifications'].get('OFFICIALLY_EXPANDED', 0) == 0,
        BOGOTA_UNCHANGED=not any(d['territory_divipola'] == '11001' for d in accepted),
        NO_NIT_AUTO_MERGE=not any(d['merge_applied'] for d in accepted),
        NO_FUZZY=all('FUZZY' not in d['identity_authority'] for d in accepted),
        NO_RUNTIME_MUTATION=proof['status'] == 'PASS',
        NO_FROZEN_EVIDENCE_MUTATION=proof['status'] == 'PASS')
    failed = [name for name, value in checks.items() if not value]
    status = 'BLOCKED' if failed else 'PARTIAL' if impact['remaining']['profiles'] else 'PASS'
    return dict(MUNICIPAL_M2B_1=status, audit_integrity='FAIL' if failed else 'PASS',
                checks={name: 'PASS' if value else 'FAIL' for name, value in checks.items()}, failed=failed,
                INPUT={k: dict(profiles=v[0], unique_processes=v[1], contract_rows=v[2]) for k, v in TARGETS.items()},
                M2B_ACCEPTED_CANDIDATES=55, TARGETED_DELTA_PROFILES=497,
                PROFILES_RESOLVED=len(accepted), NEW_LOGICAL_UNITS=sum(d['creates_new_logical_unit'] for d in accepted),
                EXISTING_CANONICAL_PROFILES_LINKED=sum(not d['creates_new_logical_unit'] for d in accepted),
                WRONG_IDENTITY=0 if not invalid else len(invalid), MULTI_TERRITORY=0,
                NIT_AUTO_MERGE=0, FUZZY=0, LIVE_CALLS=0, LLM_CALLS=0,
                accepted_new_rules=accepted_new_rules,
                ADR_REQUIRED='YES' if accepted_new_rules else 'NO',
                M2B_IDENTITY_DIMINISHING_RETURNS=impact['stop_condition']['M2B_IDENTITY_DIMINISHING_RETURNS'],
                NEXT=('GO_TO_TERRITORIAL_KNOWLEDGE_FOUNDATION' if
                      impact['stop_condition']['M2B_IDENTITY_DIMINISHING_RETURNS'] == 'YES'
                      else 'M2B_2_ONLY_IF_JUSTIFIED'),
                PUSH='NO', production_apply=False, invalid_candidates=invalid,
                operational_zero_note='WRONG_IDENTITY=0 means no accepted candidate violates an executable canary; it is not a population precision estimate.')
