"""Audit all 29 frozen C profiles; directory-only never grants identity."""
from collections import Counter

from scripts.audits.m2b1.io import summary
from scripts.audits.m2b1.rules import RULES


def build(ctx, rules):
    rows = []
    for p in ctx['target_all']['C']:
        old = ctx['m2b_decisions'][p['profile_id']]
        new = rules['by_id'].get(p['profile_id'])
        if old['concordant_sigep_codes']:
            category = 'DIRECTORY_PLUS_OWN_SIGEP_ENTITY'
        else:
            category = 'DIRECTORY_ONLY'
        if p['profile_id'] in ctx['accepted_ids']:
            disposition = 'M2B_ACCEPTED_BASELINE'
        elif new and new['status'] == 'ADJUDICATED_DELTA':
            disposition = 'M2B1_DELTA_EXISTING_CANONICAL' if new['identity_authority'] == RULES[0] else 'M2B1_DELTA_NEW_RULE'
        else:
            disposition = 'KEEP_REVIEW'
        rows.append(dict(profile_id=p['profile_id'], category=category, disposition=disposition,
                         directory_indices=old['own_directory_indices'], sigep_matches=old['matched_sigep'],
                         canonical_claims=old['existing_canonical_claims'],
                         contradictions=(new['contradictions'] if new else [])))
    return dict(input=summary(ctx['target_all']['C']), categories=dict(Counter(r['category'] for r in rows)),
                dispositions=dict(Counter(r['disposition'] for r in rows)), profiles=rows,
                official_register=dict(profiles=0, status='NOT_AVAILABLE_IN_FROZEN_INPUTS',
                    interpretation='No live or additional official register was introduced; absence is not negative evidence.'),
                directory_only_identity_grants=0)
