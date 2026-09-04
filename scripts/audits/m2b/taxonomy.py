"""Observable multi-label causes and an explicitly ordered primary partition."""
from collections import Counter
import re

from scripts.audits.m2b.io import summary
from scripts.audits.municipal_directory_authority import fiscal

CLASSES = {
    'A': 'SAME_ACCOUNT_MULTIPLE_SOURCE_NAMES',
    'B': 'SIGEP_ENTITY_MULTIPLE_ACCOUNTS',
    'C': 'EXACT_DIRECTORY_ENTITY_WITHOUT_LOGICAL_UNIT',
    'D': 'CROSS_DATASET_SOURCE_VARIANT',
    'E': 'PUNCTUATION_ONLY_VARIANT',
    'F': 'ABBREVIATED_INSTITUTIONAL_NAME',
    'G': 'SHARED_NIT_DISTINCT_BUYER_UNIT',
    'H': 'OWN_SIGEP_ENTITY_BUT_IDENTITY_CONFLICT',
    'I': 'MULTIPLE_ACCOUNTS_SAME_INSTITUTION_CANDIDATE',
    'J': 'NO_INDEPENDENT_IDENTITY_EVIDENCE',
    'K': 'OTHER_REVIEW',
}
# Explain root debt before the ubiquitous exact directory observation.
PRIORITY = ('H', 'B', 'I', 'A', 'D', 'E', 'G', 'F', 'J', 'C', 'K')


def classify(ctx, via):
    rows = []
    for p in ctx['pending']:
        d = via['by_id'][p['profile_id']]
        labels = []
        peers = [ctx['by_id'][q] for q in ctx['decisions'][p['profile_id']]['fiscal_peers']]
        distinct = [q['profile_id'] for q in peers if q['logical_unit_id'] and
                    q['family_status'] == 'VERIFIED' and p['institutional_family'] != 'UNKNOWN' and
                    q['institutional_family'] != p['institutional_family']]
        same = [ctx['by_id'][q] for q in d['same_account_profiles']]
        pdatasets = {ctx['strata'][i]['dataset'] for i in p['stratum_indices']}
        if same:
            labels.append('A')
        if d['competing_observed_accounts']:
            labels.append('B')
        if d['own_directory_clean']:
            labels.append('C')
        if any(pdatasets.isdisjoint({ctx['strata'][i]['dataset'] for i in q['stratum_indices']}) for q in same):
            labels.append('D')
        if any(p['physical_source_name'].rstrip('.,;:*!') == q['physical_source_name'].rstrip('.,;:*!') for q in same):
            labels.append('E')
        if re.search(r'\b(?:e\s*\.\s*s\s*\.\s*[ep]|ese|esp|s\s*\.\s*a|ltda)\b',
                     p['physical_source_name'], re.I):
            labels.append('F')
        if distinct and d['concordant_sigep_codes'] and d['family_status'] == 'VERIFIED':
            labels.append('G')  # Independently accredited unlike-role fiscal peer; no merge inferred.
        if d['concordant_sigep_codes'] and any(r in d['reasons'] for r in (
                'OFFICIAL_FISCAL_CONTRADICTION', 'OFFICIAL_SCOPE_CONFLICT', 'FROZEN_M2A_CONTRADICTION',
                'FAMILY_CONFLICT', 'NO_INDEPENDENT_COMPATIBLE_LEGAL_NATURE', 'MULTIPLE_OWN_SIGEP_ENTITIES')):
            labels.append('H')
        if d['competing_directory_accounts']:
            labels.append('I')
        if not d['concordant_sigep_codes']:
            labels.append('J')
        if not labels:
            labels.append('K')
        primary = next(k for k in PRIORITY if k in labels)
        rows.append(dict(profile_id=p['profile_id'], primary=primary, labels=sorted(labels),
                         accredited_distinct_role_peer_ids=distinct, identity_status=d['status'],
                         unresolved_reasons=d['reasons']))
    classes = {k: dict(name=name, primary=summary([ctx['by_id'][r['profile_id']] for r in rows if r['primary'] == k]),
                       all_signals=summary([ctx['by_id'][r['profile_id']] for r in rows if k in r['labels']]))
               for k, name in CLASSES.items()}
    families = []
    for f in sorted({p['institutional_family'] for p in ctx['pending']}):
        ps = [p for p in ctx['pending'] if p['institutional_family'] == f]
        safe = [p for p in ps if via['by_id'][p['profile_id']]['status'] == 'ADJUDICATED_CANDIDATE']
        families.append(dict(family=f, **summary(ps), safe_profiles=len(safe),
                             safe_processes=summary(safe)['unique_processes'],
                             cause_counts=dict(Counter(r['primary'] for r in rows if ctx['by_id'][r['profile_id']] in ps))))
    families.sort(key=lambda f: (-f['unique_processes'], -f['contract_rows'], -f['profiles'], -f['safe_profiles'], f['family']))
    return dict(primary_precedence=list(PRIORITY), classes=classes, profiles=rows, family_priority=families,
                interpretation='Primary classes partition 931; all_signals overlap. Abbreviation signals are not identity authorities.')


def shared_nit(ctx, via):
    rows = []
    directory_cases = []
    for p in ctx['pending']:
        peers = ctx['decisions'][p['profile_id']]['fiscal_peers']
        dir_peers = sorted({i for nit in p['fiscal_identity'] for i in via['ix']['directory_fiscal'][fiscal(nit)]
                            if str(ctx['directory'][i]['codigo_entidad']) != p['account']})
        if dir_peers:
            directory_cases.append(dict(profile_id=p['profile_id'], disposition='KEEP_SEPARATE',
                peer_indices=dir_peers, unaccounted_same_family_peer_indices=via['by_id'][p['profile_id']]['directory_fiscal_peer_review'],
                reference='artifacts/audits/municipal_m0_v1/sources/directory.json.gz#/rows',
                reason='Registry account peers are retained separately from physical-profile fiscal peers; no merge authority.'))
        if not peers:
            continue
        rows.append(dict(profile_id=p['profile_id'], fiscal_identity=p['fiscal_identity'],
                         disposition='KEEP_SEPARATE',
                         peers=[dict(profile_id=q, account=ctx['by_id'][q]['account'],
                                     name=ctx['by_id'][q]['physical_source_name'],
                                     family=ctx['by_id'][q]['institutional_family'],
                                     logical_unit_id=ctx['by_id'][q]['logical_unit_id'],
                                     territory=ctx['by_id'][q]['territory_divipola']) for q in peers],
                         own_identity_status=via['by_id'][p['profile_id']]['status'],
                         reason='Fiscal co-occurrence does not authorize SAME_BUYER_UNIT or membership.'))
    return dict(NIT_AUTO_MERGES=0, cases=rows, summary=summary([ctx['by_id'][r['profile_id']] for r in rows]),
                directory_cases=directory_cases, directory_summary=summary([ctx['by_id'][r['profile_id']] for r in directory_cases]),
                peer_families=dict(Counter(q['family'] for r in rows for q in r['peers'])),
                counting='Per pending profile; symmetric peer edges are not additive units.')
