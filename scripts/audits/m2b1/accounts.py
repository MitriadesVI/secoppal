"""Classify the 20 multi-account candidates without manufacturing account history."""
from collections import Counter

from scripts.audits.m2b1.io import summary


def build(ctx, rules):
    observed_accounts = {}
    for p in ctx['all_profiles']:
        observed_accounts.setdefault(p['account'], []).append(p)
    rows = []
    for p in ctx['target_all']['I']:
        old = ctx['m2b_decisions'][p['profile_id']]
        accounts = sorted({str(item.get('account', '')) for item in old['competing_directory_accounts']})
        activity = []
        for account in accounts:
            ps = observed_accounts.get(account, [])
            activity.append(dict(account=account, observed_in_window=bool(ps),
                                 profile_ids=sorted(q['profile_id'] for q in ps),
                                 unique_processes=sum(q['volumes']['unique_processes'] for q in ps),
                                 contract_rows=sum(q['volumes']['contract_physical_rows'] for q in ps)))
        if accounts and all(not a['observed_in_window'] for a in activity):
            category = 'REGISTRY_ONLY_ACCOUNT'
        else:
            category = 'UNRESOLVED'
        rows.append(dict(profile_id=p['profile_id'], category=category, competing_accounts=activity,
                         same_entity_historical_account=False, distinct_buyer_units_proven=False,
                         delta_status=rules['by_id'][p['profile_id']]['status'],
                         contradictions=rules['by_id'][p['profile_id']]['contradictions'],
                         limitation='The frozen aggregate has no dated account succession evidence.'))
    return dict(input=summary(ctx['target_all']['I']), classifications={
        name: sum(r['category'] == name for r in rows) for name in
        ('SAME_ENTITY_HISTORICAL_ACCOUNT', 'DISTINCT_BUYER_UNITS', 'REGISTRY_ONLY_ACCOUNT', 'UNRESOLVED')},
        profiles=rows, productive_merges=0,
        caveat='No account is called historical from absence of activity alone; REGISTRY_ONLY_ACCOUNT is contextual.')
