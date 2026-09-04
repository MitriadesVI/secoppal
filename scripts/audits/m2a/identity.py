"""Logical identities require an authority; fiscal peers remain separate."""
from collections import defaultdict

from scripts.audits.municipal_directory_authority import fiscal
from scripts.audits.m2a.io import stable_id, sum_volumes, ACC
from scripts.audits.m2a.matching import provisional_family


def reconcile(profiles, affiliations, analysis, ix, records, m1b, candidates, sidecar=None):
    aff = {a['profile_id']: a for a in affiliations}
    bindings = {b['canonical_name']: b for b in (sidecar or {}).get('bindings', [])}
    sigep_accounts = defaultdict(set)
    by_account, by_fiscal, by_surface = defaultdict(list), defaultdict(list), defaultdict(list)
    for p in profiles:
        a = aff[p['profile_id']]
        for code in a['entity_sigep_codes']:
            sigep_accounts[code].add(p['account'])
        by_account[p['account']].append(p['profile_id'])
        by_surface[p['physical_source_name']].append(p['profile_id'])
        for nit in {fiscal(n) for n in p['fiscal_identity']}:
            by_fiscal[nit].append(p['profile_id'])
    old = {(p['account'], p['physical_source_name']): (i, p) for i, p in enumerate(m1b['profiles'])}
    dispositions = {c['profile_id']: c['disposition'] for c in candidates['candidates']}
    groups, decisions, delta = defaultdict(list), [], []
    for p in profiles:
        a = aff[p['profile_id']]
        key = p['account'], p['physical_source_name']
        recognized = ix['recognized'][key]
        priors = ix['prior'][key]
        owners = sorted({u['canonical'] for u in recognized} | {v['canonical'] for v in priors})
        logical_key, identity_basis = None, 'UNRESOLVED_INSTITUTIONAL_IDENTITY'
        code = a['territory_divipola']
        if a['affiliation'] == 'VERIFIED':
            if len(owners) == 1:
                logical_key, identity_basis = ('canonical', owners[0]), 'FROZEN_RECOGNIZED_OR_ADJUDICATED_UNIT'
            elif len(a['entity_sigep_codes']) == 1 and len(sigep_accounts[a['entity_sigep_codes'][0]]) == 1:
                logical_key = ('sigep', a['entity_sigep_codes'][0], p['account'])
                identity_basis = 'INDEPENDENT_SIGEP_ENTITY_MATCH_UNIQUE_OBSERVED_ACCOUNT'
            else:
                # Prior M0 executive accreditation is explicit identity evidence, unlike a fiscal bridge.
                ss = {c for i in p['stratum_indices'] for c in analysis['strata'][i]['sigep_codes']
                      if analysis['strata'][i]['status'] == ACC}
                if len(ss) == 1 and len(by_account[p['account']]) == 1:
                    logical_key, identity_basis = ('m0_entity', next(iter(ss)), p['account']), 'FROZEN_M0_INSTITUTIONAL_ACCREDITATION'
        logical_id = stable_id('logical', *logical_key) if logical_key else None
        if logical_id:
            groups[logical_id].append(p)
        peers = sorted({q for n in p['fiscal_identity'] for q in by_fiscal[fiscal(n)]} - {p['profile_id']})
        identity = 'SAME_BUYER_UNIT' if logical_id else 'UNRESOLVED'
        if len(by_account[p['account']]) > 1 and not owners:
            identity = 'SOURCE_VARIANT' if len(a['entity_sigep_codes']) == 1 else 'PROBABLE_SAME_NEEDS_REVIEW'
        if peers and a['institutional_family'] in ('CONCEJO', 'PERSONERIA', 'CONTRALORIA'):
            identity = 'KEEP_SEPARATE'
        action = ('DO_NOT_ADD' if a['affiliation'] == 'NON_MUNICIPAL' else
                  'SOURCE_BINDING_ONLY' if owners and logical_id else
                  'READY_FOR_REGISTRY' if logical_id and a['family_status'] == 'VERIFIED' else 'MANUAL_REVIEW')
        if owners and logical_id and all(
                p['physical_source_name'] in bindings.get(owner, {}).get('sources', {}).get(analysis['strata'][i]['dataset'], [])
                for owner in owners for i in p['stratum_indices']):
            action = 'DO_NOT_ADD'
        m1b_reference = None
        if key in old:
            i, previous = old[key]
            m1b_reference = f'artifacts/audits/municipal_m1b/buyer_profiles.json#/profiles/{i}'
            action = dispositions[previous['profile_id']]
            changes = {}
            if previous['institutional_family'] != a['institutional_family']:
                changes['family'] = dict(before=previous['institutional_family'], after=a['institutional_family'],
                                        reason='EDUCATION_TAXONOMY_SPLIT' if a['institutional_family'] == 'EDUCATION'
                                        else 'INDEPENDENT_ENTITY_MATCH_OR_PROVISIONAL_NAME_RULE')
            delta.append(dict(m1b_reference=m1b_reference, profile_id=p['profile_id'], scope=previous['scope'],
                              m1b_profile_id=previous['profile_id'], changes=changes,
                              m1b_debt_preserved=previous['debt'], disposition_preserved=action,
                              affiliation_before=previous['divipola'], affiliation_after=code,
                              affiliation_status_after=a['affiliation']))
        p.update(affiliation=a['affiliation'], territory_divipola=code, logical_unit_id=logical_id,
                 institutional_family=a['institutional_family'], family_status=a['family_status'],
                 recognized=bool(logical_id and owners), identity_status=identity, identity_basis=identity_basis,
                 future_action=action, m1b_reference=m1b_reference)
        decisions.append(dict(profile_id=p['profile_id'], logical_unit_id=logical_id, identity_status=identity,
                              identity_basis=identity_basis, fiscal_peers=peers,
                              same_account_profiles=sorted(set(by_account[p['account']]) - {p['profile_id']}),
                              same_source_other_profiles=sorted(set(by_surface[p['physical_source_name']]) - {p['profile_id']}),
                              existing_canonical_claims=owners, activity_without_current_buyer_unit=not bool(owners), future_action=action,
                              auto_merge_applied=False))
    units = []
    for uid, members in sorted(groups.items()):
        codes = {p['territory_divipola'] for p in members}
        if len(codes) != 1:
            raise ValueError(f'Logical unit has multiple territories: {uid}: {codes}')
        families = {p['institutional_family'] for p in members}
        units.append(dict(logical_unit_id=uid, territory_divipola=next(iter(codes)),
                          profile_ids=sorted(p['profile_id'] for p in members),
                          institutional_family=next(iter(families)) if len(families) == 1 else 'UNKNOWN',
                          recognized=any(p['recognized'] for p in members), volumes=sum_volumes(members),
                          identity_authorities=sorted({p['identity_basis'] for p in members})))
    physical_names = {p['physical_source_name'] for p in profiles}
    absent = [dict(canonical=r['official_name'], institutional_family=provisional_family(r['official_name']),
                   classification='CANONICAL_WITHOUT_BYTE_EXACT_PHYSICAL_APPEARANCE',
                   synthetic_status='NOT_PROVEN_SYNTHETIC', future_action='MANUAL_REVIEW')
              for r in records if r['official_name'] not in physical_names]
    for item in absent:
        item['observed_binding_sources'] = sorted({n for ns in bindings.get(item['canonical'], {}).get('sources', {}).values()
                                                    for n in ns if n in physical_names})
        if item['observed_binding_sources']:
            item['synthetic_status'] = 'DECLARED_DISPLAY_WITH_OBSERVED_PHYSICAL_SOURCE_BINDING'
    multi_accounts = {code: sorted(accounts) for code, accounts in sigep_accounts.items() if len(accounts) > 1}
    return dict(units=units, decisions=decisions, canonical_without_physical_source=absent,
                possible_duplicate_or_multi_account_sigep_entities=multi_accounts,
                auto_merge_applied=False), delta
