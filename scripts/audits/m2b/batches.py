"""Disjoint work batches and separately audited already-logical profiles."""
from collections import Counter

from scripts.audits.m2b.health import is_health
from scripts.audits.m2b.io import summary

NAMES = ('BATCH_A_HIGH_CONFIDENCE', 'BATCH_B_SOURCE_VARIANTS', 'BATCH_C_HEALTH_IDENTITY',
         'BATCH_D_MULTI_ACCOUNT_REVIEW', 'BATCH_E_MANUAL')
ACTIONS = ('READY_FOR_BUYER_PROMOTION', 'READY_FOR_REGISTRY_ONLY', 'SOURCE_BINDING_ONLY',
           'KEEP_SEPARATE', 'MANUAL_REVIEW')


def build(ctx, via):
    groups = {name: [] for name in NAMES}
    for p in ctx['pending']:
        d = via['by_id'][p['profile_id']]
        multi = any('ACCOUNT' in r and r != 'SAME_ACCOUNT_VARIANTS_REQUIRE_INDEPENDENT_ADJUDICATION' for r in d['reasons'])
        if multi:
            batch = NAMES[3]
        elif d['same_account_profiles']:
            batch = NAMES[1]
        elif is_health(p):
            batch = NAMES[2]
        elif d['status'] == 'ADJUDICATED_CANDIDATE':
            batch = NAMES[0]
        else:
            batch = NAMES[4]
        groups[batch].append(p)
    rows = {}
    for batch, ps in groups.items():
        ds = [via['by_id'][p['profile_id']] for p in ps]
        safe = [p for p in ps if via['by_id'][p['profile_id']]['status'] == 'ADJUDICATED_CANDIDATE']
        rows[batch] = dict(summary(ps), candidate_logical_units=sorted({d['candidate_logical_unit_id'] for d in ds
                            if d['candidate_logical_unit_id']}),
                          identity_authority=dict(Counter(d['identity_authority'] or 'UNRESOLVED' for d in ds)),
                          contradictions=dict(Counter(r for d in ds for r in d['reasons'])),
                          expected_gain=summary(safe), remaining_profiles=len(ps) - len(safe))
    return dict(batches=rows, partition='Disjoint: multi-account, source variants, health, safe nonhealth, manual.',
                expected_gain_scope='Only adjudicated candidates, never all profiles in a review batch.')


def already_logical(ctx):
    """Preserve M1B dispositions; registry-ready is not promotion-ready."""
    units = {u['logical_unit_id']: u for u in ctx['units']}
    rows = []
    for p in ctx['unrecognized']:
        a = ctx['aff'][p['profile_id']]
        unit = units[p['logical_unit_id']]
        if unit['recognized']:
            action, reason = 'SOURCE_BINDING_ONLY', 'Logical unit already recognized; this profile needs separate source binding review.'
        elif p['future_action'] == 'KEEP_SEPARATE':
            action, reason = 'KEEP_SEPARATE', 'Frozen M1B separation disposition retained.'
        elif a['conflicts'] or p['family_status'] != 'VERIFIED' or p['future_action'] == 'MANUAL_REVIEW':
            action, reason = 'MANUAL_REVIEW', 'Frozen conflicts, provisional family or prior manual disposition retained.'
        else:
            action, reason = 'READY_FOR_REGISTRY_ONLY', 'Own logical identity and family accredited; aliases/promotion gates not adjudicated in M2B.'
        rows.append(dict(profile_id=p['profile_id'], logical_unit_id=p['logical_unit_id'],
                         disposition=action, reason=reason, frozen_future_action=p['future_action'],
                         identity_authority=p['identity_basis'], conflicts=a['conflicts'],
                         m1b_reference=p['m1b_reference'], identity_adjudication_required=False))
    return dict(summary=summary(ctx['unrecognized']), profiles=rows,
                dispositions={action: summary([ctx['by_id'][r['profile_id']] for r in rows if r['disposition'] == action])
                              for action in ACTIONS},
                promotion_policy='No READY_FOR_BUYER_PROMOTION without a separately reviewed promotion payload and gates.')
