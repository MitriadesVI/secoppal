"""Territorial evidence chains, competing witnesses and frozen prior authorities."""
from collections import defaultdict

from scripts.audits.municipal_directory_authority import fiscal, location_code
from scripts.audits.m2a.io import ACC, BOGOTA, BOGOTA_AUTHORITY
from scripts.audits.m2a.matching import entity_match, classify_family


def indexes(analysis, sigep, directory, m1b, adjudications):
    ix = {k: defaultdict(list) for k in ('sigep', 'directory', 'bridge', 'prior', 'recognized')}
    for i, s in enumerate(sigep):
        s = dict(s, divipola=s['DIVIPOLA DEPARTAMENTO'].zfill(2) + s['DIVIPOLA MUNICIPIO'].zfill(3))
        ix['sigep'][fiscal(s['NIT'])].append((i, s))
    for i, r in enumerate(directory):
        ix['directory'][str(r['codigo_entidad'])].append((i, r))
    for i, r in enumerate(analysis['strata']):
        if r['status'] == ACC:
            ix['bridge'][fiscal(r['nit_evidence'])].append((i, r['municipality']))
    for i, p in enumerate(m1b['profiles']):
        a = p['current_affiliation']
        if a:
            ix['prior'][p['account'], p['physical_source_name']].append(dict(
                code=a['codes'][0], authority=a['authority'], canonical=p['current_gazetteer_owner'],
                reference=f'artifacts/audits/municipal_m1b/buyer_profiles.json#/profiles/{i}/current_affiliation'))
    for i, a in enumerate(adjudications['cases']):
        if a['decision'] != 'VERIFIED_MUNICIPAL_AFFILIATION':
            continue
        for names in a['physical_source_names'].values():
            for name in names:
                value = dict(code=a['expected_divipola'], authority=a['affiliation']['authority'],
                             canonical=a['canonical'], reference=f'artifacts/audits/municipal_m1a_2a/adjudication.json#/cases/{i}')
                if value not in ix['prior'][a['account'], name]:
                    ix['prior'][a['account'], name].append(value)
    for u in analysis['recognized_units']:
        for names in u['source_names'].values():
            for name in names:
                if u not in ix['recognized'][u['account'], name]:
                    ix['recognized'][u['account'], name].append(u)
    return ix


def investigate(p, rows, ix, towns):
    key = p['account'], p['physical_source_name']
    nits = {fiscal(n) for n in p['fiscal_identity']}
    sigs = {i: s for nit in nits for i, s in ix['sigep'][nit]}
    municipal_sigs = {i: s for i, s in sigs.items()
                      if s['SUBORDEN'] in ('Municipal', 'Distrital')}
    fiscal_codes = sorted({s['divipola'] for s in municipal_sigs.values()})
    all_scopes = sorted({(s['SUBORDEN'], s['divipola']) for s in sigs.values()})
    bridge = sorted({code for nit in nits for _, code in ix['bridge'][nit]})
    bridge_eligible = len(nits) == 1 and len(bridge) == 1
    named_regs = [(i, r) for i, r in ix['directory'][p['account']]
                  if r.get('nombre_entidad') == p['physical_source_name']]
    regs = [(i, r) for i, r in named_regs if fiscal(r.get('nit_entidad', '')) in nits]
    directory_fiscal_conflict = len(named_regs) != len(regs)
    directory_codes = sorted({location_code(r) for _, r in regs if location_code(r) in towns})
    directory_clean = bool(regs) and not directory_fiscal_conflict and len({location_code(r) for _, r in regs}) == 1
    observed_bogota = (any(r['municipality'] == BOGOTA for r in rows)
                       or BOGOTA in directory_codes or BOGOTA in fiscal_codes or BOGOTA in bridge)
    matched = [(i, s, method) for i, s in sigs.items()
               if (method := entity_match(p['physical_source_name'], s, towns))]
    own_codes = sorted({s['divipola'] for _, s, _ in matched
                        if s['SUBORDEN'] in ('Municipal', 'Distrital') and s['divipola'] in towns})
    own_scopes = sorted({(s['SUBORDEN'], s['divipola']) for _, s, _ in matched})
    family, basis, family_status = classify_family(p['physical_source_name'], [s for _, s, _ in matched])
    prior = ix['prior'][key]
    accredited = sorted({r['municipality'] for r in rows if r['status'] == ACC})
    prior_codes = sorted({a['code'] for a in prior})
    unresolved_baseline = any(r['status'] == 'UNRESOLVED_AFFILIATION' for r in rows)
    via_a = bridge_eligible and not observed_bogota and not accredited and not prior and unresolved_baseline
    via_b_pool = len(fiscal_codes) == 1 and unresolved_baseline
    via_b = via_b_pool and fiscal_codes[0] in towns and len(all_scopes) == 1 and not observed_bogota and not accredited and not prior
    chain = []
    def link(kind, refs, **data):
        chain.append(dict(evidence_type=kind, evidence_sources=refs, **data))
    link('PHYSICAL_PROFILE_OBSERVATION',
         [f'artifacts/audits/municipal_m1a_final/m0_post_analysis.json.gz#/strata/{i}'
          for i in p['stratum_indices']], account=p['account'], physical_source_name=p['physical_source_name'],
         fiscal_identity=sorted(nits), grants='OBSERVATION_ONLY')
    if accredited:
        link('M0_ACCREDITED_AFFILIATION',
             [f'artifacts/audits/municipal_m1a_final/m0_post_analysis.json.gz#/strata/{i}' for i in p['stratum_indices']],
             territory_codes=accredited)
    for a in prior:
        link(a['authority'], [a['reference']], territory_codes=[a['code']])
    if via_a:
        link('NIT_BRIDGE_UNIQUE_TERRITORY',
             [f'artifacts/audits/municipal_m1a_final/m0_post_analysis.json.gz#/strata/{i}'
              for nit in sorted(nits) for i, _ in ix['bridge'][nit]],
             fiscal_identity=sorted(nits), territory_codes=bridge, grants='TERRITORY_PROBABLE_ONLY',
             links=['observed_profile_fiscal_identity', 'accredited_physical_profile_same_fiscal_identity',
                    'unique_accredited_territory'])
    if via_b:
        link('SIGEP_FISCAL_TERRITORY_MATCH',
             [f'artifacts/audits/municipal_m0_v1/sources/sigep.json.gz#/{i}' for i in sorted(municipal_sigs)],
             territory_codes=fiscal_codes, all_scopes=all_scopes, grants='TERRITORY_PROBABLE_ONLY',
             links=['observed_profile_fiscal_identity', 'same_fiscal_SIGEP_municipal_suborder', 'unique_valid_DIVIPOLA'])
    for i, s, method in matched:
        link('SIGEP_ENTITY_MATCH', [f'artifacts/audits/municipal_m0_v1/sources/sigep.json.gz#/{i}'],
             sigep_code=s['CÓDIGO SIGEP'], method=method, territory=s['divipola'],
             suborder=s['SUBORDEN'], nature=s['NATURALEZA JURÍDICA'])
    if regs:
        link('OWN_EXACT_SECOP_DIRECTORY_RECORD',
             [f'artifacts/audits/municipal_m0_v1/sources/directory.json.gz#/rows/{i}' for i, _ in regs],
             match_dimensions=['account', 'byte_exact_physical_name', 'fiscal_evidence'],
             territory_codes=directory_codes, unique_valid_code=directory_clean,
             grants='PROFILE_CORROBORATION_NOT_GROUP_MEMBERSHIP')
    status, code, authority = 'UNRESOLVED', None, 'INSUFFICIENT_INDEPENDENT_EVIDENCE'
    conflicts = []
    baseline_codes = sorted(set(accredited + prior_codes))
    if observed_bogota:
        # Even another pre-existing M0 municipal route must not be reinterpreted as the declared registry.
        declared = [r for r in rows if r['affiliation_basis'] == BOGOTA_AUTHORITY and r['status'] == ACC]
        if declared and len(declared) == len(rows):
            status, code, authority = 'VERIFIED', BOGOTA, BOGOTA_AUTHORITY
        else:
            authority = 'BOGOTA_REQUIRES_OWN_DECLARED_REGISTRY_AUTHORITY'
    elif len(baseline_codes) == 1:
        status, code, authority = 'VERIFIED', baseline_codes[0], 'FROZEN_PRIOR_AFFILIATION'
    elif len(baseline_codes) > 1:
        status, authority = 'AMBIGUOUS', 'CONFLICTING_FROZEN_AFFILIATIONS'
        conflicts.append(baseline_codes)
    else:
        candidates = set((bridge if via_a else []) + (fiscal_codes if via_b else []) + own_codes)
        if len(candidates) > 1 or len(nits) > 1 or len(own_scopes) > 1 or directory_fiscal_conflict:
            status, authority = 'AMBIGUOUS', 'CONFLICTING_FISCAL_OR_TERRITORIAL_WITNESSES'
            conflicts.append(sorted(candidates))
        elif candidates:
            code = next(iter(candidates))
            if directory_codes and directory_codes != [code]:
                status, authority = 'AMBIGUOUS', 'DIRECTORY_CONTRADICTION'
                conflicts.append(directory_codes)
            elif directory_clean and directory_codes == [code] and (via_a or via_b or own_codes == [code]):
                status = 'VERIFIED'
                authority = ('SIGEP_ENTITY_AND_OWN_DIRECTORY_EXACT_DIVIPOLA' if own_codes == [code]
                             else 'FISCAL_TERRITORY_AND_OWN_DIRECTORY_EXACT_DIVIPOLA')
            elif via_a and own_codes == [code]:
                status, authority = 'VERIFIED', 'NIT_BRIDGE_AND_INDEPENDENT_SIGEP_ENTITY'
            else:
                status, authority = 'PROBABLE', 'COMPOSITE_TERRITORY_WITHOUT_COMPLETE_PROFILE_CORROBORATION'
        elif matched and all(s['SUBORDEN'] not in ('Municipal', 'Distrital') for _, s, _ in matched):
            status, authority = 'NON_MUNICIPAL', 'SIGEP_ENTITY_MATCH_NON_MUNICIPAL_SUBORDER'
    if status == 'AMBIGUOUS':
        code = None
    # Frozen manual decisions override source debts, without erasing their contradictory witnesses.
    if baseline_codes and fiscal_codes and baseline_codes != fiscal_codes:
        conflicts.append(dict(type='OPEN_SIGEP_DEBT_PRIOR_AUTHORITY_PRESERVED', codes=fiscal_codes))
    if len(own_scopes) > 1:
        conflicts.append(dict(type='INDEPENDENT_ENTITY_MATCH_MULTIPLE_SCOPES', scopes=own_scopes))
    if directory_fiscal_conflict:
        conflicts.append(dict(type='OWN_DIRECTORY_FISCAL_CONTRADICTION', directory_indices=[i for i, _ in named_regs]))
    return dict(profile_id=p['profile_id'], affiliation=status, territory_divipola=code,
                municipality=towns.get(code, {}).get('municipality'), department=towns.get(code, {}).get('department'),
                evidence_type=authority, evidence_chain=chain,
                evidence_sources=sorted({ref for e in chain for ref in e['evidence_sources']}),
                candidate_territories=sorted(set(bridge + fiscal_codes + own_codes + directory_codes + baseline_codes)),
                conflicts=conflicts, observed_bogota_context=observed_bogota,
                via_a=via_a, via_b=via_b, via_b_hypothesis_pool=via_b_pool,
                via_b_exclusion_reasons=([reason for condition, reason in [
                    (observed_bogota, 'BOGOTA_RESERVED_AUTHORITY'),
                    (len(all_scopes) != 1, 'CONFLICTING_SIGEP_FISCAL_SCOPES'),
                    (any(c not in towns for c in fiscal_codes), 'CODE_OUTSIDE_MUNICIPAL_DIVIPOLA'),
                    (bool(accredited or prior), 'FROZEN_PRIOR_AUTHORITY_TAKES_PRECEDENCE')] if condition]
                    if via_b_pool and not via_b else []),
                via_a_hypothesis_pool=unresolved_baseline and bridge_eligible,
                bridge_territories=bridge, fiscal_territories=fiscal_codes, all_sigep_scopes=all_scopes,
                directory_crosscheck=bool(len(fiscal_codes) == 1 and directory_clean and directory_codes == fiscal_codes),
                entity_sigep_codes=sorted({s['CÓDIGO SIGEP'] for _, s, _ in matched}),
                entity_municipal_codes=own_codes, sigep_by_nit_indices=sorted(sigs),
                sigep_by_nit_natures=sorted({s['NATURALEZA JURÍDICA'] for s in sigs.values()}),
                institutional_family=family, family_basis=basis, family_status=family_status)
