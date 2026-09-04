"""Conservative offline hypotheses with positive and negative witnesses.

NIT never selects an identity candidate. Exact official name indexes span the
entire frozen SIGEP extract. Fiscal peers only expose possible contradictions.
"""
from collections import Counter, defaultdict

from app.core.entity_types import name_tokens
from app.utils.money import normalize_text
from scripts.audits.m2a.io import stable_id
from scripts.audits.m2a.matching import classify_family, entity_match, guarded_town, provisional_family
from scripts.audits.municipal_directory_authority import fiscal, location_code
from scripts.audits.m2b.io import index, summary

ESE_LABELS = (('empresa', 'social', 'del', 'estado'), ('empresas', 'sociales', 'del', 'estado'),
              ('e', 's', 'e'), ('ese',))
ESE_NATURE = 'empresas sociales del estado'


def ese_core(name):
    """Remove at most one whole boundary legal label; preserve every core token."""
    tokens = tuple(name_tokens(name))
    for label in ESE_LABELS:
        if tokens[:len(label)] == label:
            return tokens[len(label):], 'PREFIX:' + ' '.join(label)
        if tokens[-len(label):] == label:
            return tokens[:-len(label)], 'SUFFIX:' + ' '.join(label)
    return tokens, None


def strict_match(name, sigep, towns):
    town = towns.get(sigep['divipola'])
    if town and ese_core(name)[0] == tuple(name_tokens(town['municipality'])):
        return None
    exact = entity_match(name, sigep, towns)
    if exact:
        return exact
    left, decoration = ese_core(name)
    right, official_decoration = ese_core(sigep['NOMBRE'])
    # The official nature licenses this particular legal label, never a fiscal row.
    if normalize_text(sigep['NATURALEZA JURÍDICA']) != ESE_NATURE:
        return None
    town = towns.get(sigep['divipola'])
    if not town or not left or left != right or not (decoration or official_decoration):
        return None
    if left == tuple(name_tokens(town['municipality'])):
        return None  # A municipality alone is not an institutional name.
    tt = tuple(name_tokens(town['municipality']))
    if any(left[i:i + len(tt)] == tt for i in range(len(left))):
        if not guarded_town(name, sigep['divipola']):
            return None
    return 'OWN_SIGEP_ESE_LEGAL_LABEL_EXACT_CORE'


def build_indexes(ctx):
    ix = dict(accounts=index(ctx['profiles'], lambda p: p['account']),
              directories=defaultdict(list), sigep_core=defaultdict(list), sigep_tokens=defaultdict(list),
              sigep_territory=defaultdict(list), claims=defaultdict(set), fiscal=defaultdict(list),
              directory_fiscal=defaultdict(list))
    for i, s in enumerate(ctx['sigep']):
        ix['sigep_core'][ese_core(s['NOMBRE'])[0]].append(i)
        ix['sigep_tokens'][tuple(name_tokens(s['NOMBRE']))].append(i)
        ix['sigep_territory'][s['divipola']].append(i)
    for i, r in enumerate(ctx['directory']):
        ix['directories'][str(r['codigo_entidad'])].append(i)
        if fiscal(r.get('nit_entidad', '')):
            ix['directory_fiscal'][fiscal(r.get('nit_entidad', ''))].append(i)
    for a in ctx['aliases']:
        ix['claims'][a['official_name']].add(a['official_name'])
    for b in ctx['bindings']:
        for names in b['sources'].values():
            for name in names:
                ix['claims'][name].add(b['canonical_name'])
    for p in ctx['profiles']:
        for nit in {fiscal(n) for n in p['fiscal_identity']}:
            if nit:
                ix['fiscal'][nit].append(p['profile_id'])
    return ix


def discover(ctx, ix):
    """Probe all 11,298 profiles, including unverified competing accounts."""
    matches = {}
    sigep_accounts = defaultdict(set)
    for p in ctx['profiles']:
        a = ctx['aff'][p['profile_id']]
        # Same role template already authorized by ADR-075; no new fuzzy logic.
        indices = set(ix['sigep_tokens'][tuple(name_tokens(p['physical_source_name']))])
        indices.update(ix['sigep_core'][ese_core(p['physical_source_name'])[0]])
        if p['institutional_family'] in ('CONCEJO', 'PERSONERIA', 'CONTRALORIA', 'MUNICIPAL_EXECUTIVE'):
            for code in set(a['candidate_territories']) | {p['territory_divipola']}:
                indices.update(ix['sigep_territory'][code])
        found = []
        for i in sorted(indices):
            s = ctx['sigep'][i]
            method = strict_match(p['physical_source_name'], s, ctx['towns'])
            if method:
                # Keep matches to other territories for contradiction inspection.
                found.append(dict(sigep_index=i, sigep_code=s['CÓDIGO SIGEP'],
                                  territory=s['divipola'], method=method))
                possible = set(a['candidate_territories']) | {p['territory_divipola']}
                if s['divipola'] in possible:
                    sigep_accounts[s['CÓDIGO SIGEP']].add(p['account'])
        matches[p['profile_id']] = found
    return matches, sigep_accounts


def assess(p, ctx, ix, matches, sigep_accounts):
    pid, territory, name = p['profile_id'], p['territory_divipola'], p['physical_source_name']
    a = ctx['aff'][pid]
    own = matches[pid]
    concordant = [m for m in own if m['territory'] == territory]
    candidates = sorted({m['sigep_code'] for m in concordant})
    dirs = [i for i in ix['directories'][p['account']] if ctx['directory'][i]['nombre_entidad'] == name]
    dir_clean = bool(dirs) and {location_code(ctx['directory'][i]) for i in dirs} == {territory}
    reasons = []
    if p['affiliation'] != 'VERIFIED':
        reasons.append('TERRITORIAL_AFFILIATION_NOT_VERIFIED')
    if territory == '11001' or a['observed_bogota_context']:
        reasons.append('BOGOTA_RESERVED_REGISTRY')
    if not concordant:
        reasons.append('NO_INDEPENDENT_OFFICIAL_NAME_MATCH')
    elif len(candidates) != 1:
        reasons.append('MULTIPLE_OWN_SIGEP_ENTITIES')
    if a['conflicts']:
        reasons.append('FROZEN_M2A_CONTRADICTION')
    if not dir_clean:
        reasons.append('NO_CLEAN_OWN_EXACT_DIRECTORY')
    if any(fiscal(ctx['directory'][i].get('nit_entidad', '')) not in
           {fiscal(n) for n in p['fiscal_identity']} for i in dirs):
        reasons.append('OWN_DIRECTORY_FISCAL_CONTRADICTION')
    competing = sorted({acc for code in candidates for acc in sigep_accounts[code]} - {p['account']})
    if competing:
        reasons.append('COMPETING_OBSERVED_ACCOUNTS')
    directory_competitors = []
    family, basis, family_status = classify_family(name, [ctx['sigep'][m['sigep_index']] for m in concordant])
    if concordant and (family_status != 'VERIFIED' or family == 'UNKNOWN'):
        reasons.append('NO_INDEPENDENT_COMPATIBLE_LEGAL_NATURE')
    # PROVISIONAL UNKNOWN can be refined from a positively matched official row.
    compatible = p['institutional_family'] in (family, 'UNKNOWN') or (
        p['institutional_family'] == 'PUBLIC_SERVICES' and family == 'PUBLIC_COMPANY')
    if concordant and not compatible:
        reasons.append('FAMILY_CONFLICT')
    for m in concordant:
        s = ctx['sigep'][m['sigep_index']]
        if s['SUBORDEN'] not in ('Municipal', 'Distrital'):
            reasons.append('OFFICIAL_SCOPE_CONFLICT')
        if fiscal(s['NIT']) not in {fiscal(n) for n in p['fiscal_identity']}:
            reasons.append('OFFICIAL_FISCAL_CONTRADICTION')
    same_code_rows = [s for s in ctx['sigep'] if s['CÓDIGO SIGEP'] in candidates]
    if concordant and ({s['divipola'] for s in same_code_rows} != {territory} or
                       any(s['SUBORDEN'] not in ('Municipal', 'Distrital') for s in same_code_rows)):
        reasons.append('OFFICIAL_ENTITY_MULTIPLE_SCOPES')
    account_directory_conflicts = [i for i in ix['directories'][p['account']]
        if concordant and not any(strict_match(ctx['directory'][i].get('nombre_entidad', ''),
                                              ctx['sigep'][m['sigep_index']], ctx['towns']) for m in concordant)]
    if account_directory_conflicts:
        reasons.append('ACCOUNT_DIRECTORY_NAME_CONFLICT')
    # Same institution in the directory on another account is unexplained even
    # when that account has no transactions in the observation window.
    for i in ix.get('directory_identity_candidates', {}).get(pid, []):
        r = ctx['directory'][i]
        if str(r['codigo_entidad']) != p['account']:
            directory_competitors.append(dict(index=i, account=str(r['codigo_entidad']), name=r.get('nombre_entidad', '')))
    if directory_competitors:
        reasons.append('COMPETING_DIRECTORY_ACCOUNTS')
    peers = sorted({q for n in p['fiscal_identity'] for q in ix['fiscal'][fiscal(n)]} - {pid})
    peer_review = []
    for qid in peers:
        q = ctx['by_id'][qid]
        if q['account'] == p['account']:
            continue
        qa = ctx['aff'][qid]
        if territory not in set(qa['candidate_territories']) | {q['territory_divipola']}:
            continue
        qcodes = {m['sigep_code'] for m in matches[qid] if m['territory'] == territory}
        distinct_official = bool(qcodes) and not (qcodes & set(candidates))
        if not distinct_official and q['institutional_family'] in (family, 'UNKNOWN'):
            peer_review.append(qid)
    if concordant and peer_review:
        reasons.append('UNEXPLAINED_SAME_FAMILY_FISCAL_PEER_ACCOUNT')
    directory_peer_review = []
    directory_peer_explanations = []
    for i in sorted({i for n in p['fiscal_identity'] for i in ix['directory_fiscal'][fiscal(n)]}):
        r = ctx['directory'][i]
        if str(r['codigo_entidad']) == p['account']:
            continue
        if location_code(r) not in (territory, None):
            continue
        rname = r.get('nombre_entidad', '')
        rfamily = 'ESE_HEALTH' if ese_core(rname)[1] else provisional_family(rname)
        if rfamily in (family, 'UNKNOWN'):
            # A different accredited institution can explain this fiscal peer;
            # a different name alone cannot explain another same-family account.
            possible = ix['sigep_core'][ese_core(rname)[0]]
            other_codes = {ctx['sigep'][j]['CÓDIGO SIGEP'] for j in possible
                           if ctx['sigep'][j]['divipola'] == territory
                           and strict_match(rname, ctx['sigep'][j], ctx['towns'])}
            if not other_codes or other_codes & set(candidates):
                directory_peer_review.append(i)
            else:
                directory_peer_explanations.append(dict(directory_index=i, distinct_sigep_codes=sorted(other_codes),
                                                        explanation='INDEPENDENT_OFFICIAL_NAME_DIFFERENT_ENTITY'))
    if concordant and directory_peer_review:
        reasons.append('UNEXPLAINED_DIRECTORY_FISCAL_PEER_ACCOUNT')
    claims = sorted(ix['claims'][name])
    if claims:
        reasons.append('EXISTING_CANONICAL_CLAIM_REQUIRES_BINDING_REVIEW')
    same_acc = [q['profile_id'] for q in ix['accounts'][p['account']] if q['profile_id'] != pid]
    if same_acc:
        reasons.append('SAME_ACCOUNT_VARIANTS_REQUIRE_INDEPENDENT_ADJUDICATION')
    methods = sorted({m['method'] for m in concordant})
    authority = ('OWN_SIGEP_ESE_LEGAL_LABEL_EXACT_CORE' if 'OWN_SIGEP_ESE_LEGAL_LABEL_EXACT_CORE' in methods
                 else 'INDEPENDENT_SIGEP_ENTITY_MATCH_UNIQUE_OBSERVED_ACCOUNT')
    return dict(profile_id=pid, account=p['account'], physical_source_name=name, territory_divipola=territory,
                matched_sigep=own, concordant_sigep_codes=candidates, methods=methods,
                own_directory_indices=dirs, own_directory_clean=dir_clean,
                account_directory_conflicts=account_directory_conflicts,
                competing_observed_accounts=competing, competing_directory_accounts=directory_competitors,
                same_account_profiles=same_acc, same_family_peer_review=peer_review,
                directory_fiscal_peer_review=directory_peer_review,
                directory_fiscal_peer_explanations=directory_peer_explanations,
                existing_canonical_claims=claims, frozen_conflicts=a['conflicts'],
                institutional_family=family, family_basis=basis, family_status=family_status,
                family_compatible=compatible, reasons=sorted(set(reasons)),
                status='ADJUDICATED_CANDIDATE' if not reasons else 'REVIEW',
                identity_authority=authority if not reasons else None,
                candidate_logical_unit_id=stable_id('m2b-logical', candidates[0], p['account']) if not reasons else None,
                via_i='NO_OWN_SIGEP_MATCH_IN_M2A' if not a['entity_sigep_codes'] else 'RECHECK_FROZEN_OWN_MATCH',
                via_ii='NOT_APPLICABLE_SINGLE_SURFACE_ACCOUNT' if not same_acc else 'REVIEW',
                via_iii='CORROBORATION_ONLY' if dir_clean else 'INSUFFICIENT',
                via_iv='SAME_BUYER_UNIT' if not reasons else 'REVIEW',
                evidence_sources=[f'artifacts/audits/municipal_m0_v1/sources/sigep.json.gz#/{m["sigep_index"]}' for m in concordant]
                    + [f'artifacts/audits/municipal_m0_v1/sources/directory.json.gz#/rows/{i}' for i in dirs],
                profile_reference=f'artifacts/audits/municipal_m2a/physical_profiles.json#profile_id={pid}',
                territorial_authority_reference=f'artifacts/audits/municipal_m2a/territorial_affiliation.json#profile_id={pid}',
                volumes=p['volumes'],
                merge_applied=False)


def evaluate(ctx):
    ix = build_indexes(ctx)
    matches, accounts = discover(ctx, ix)
    # Index directory cores once; retain every account, irrespective of fiscal ID.
    directory_cores = defaultdict(list)
    for i, r in enumerate(ctx['directory']):
        directory_cores[location_code(r), ese_core(r.get('nombre_entidad', ''))[0]].append(i)
    ix['directory_identity_candidates'] = {}
    for p in ctx['pending']:
        ds = set()
        for m in matches[p['profile_id']]:
            if m['territory'] != p['territory_divipola']:
                continue
            s = ctx['sigep'][m['sigep_index']]
            for i in directory_cores[s['divipola'], ese_core(s['NOMBRE'])[0]]:
                if strict_match(ctx['directory'][i]['nombre_entidad'], s, ctx['towns']):
                    ds.add(i)
        ix['directory_identity_candidates'][p['profile_id']] = sorted(ds)
    decisions = [assess(p, ctx, ix, matches, accounts) for p in ctx['pending']]
    by_id = {d['profile_id']: d for d in decisions}
    accepted = [d for d in decisions if d['status'] == 'ADJUDICATED_CANDIDATE']
    rows = {}
    for label, pred in (
        ('VIA_I', lambda d: bool(ctx['aff'][d['profile_id']]['entity_sigep_codes'])),
        ('VIA_II', lambda d: bool(d['same_account_profiles'])),
        ('VIA_III', lambda d: d['own_directory_clean']),
        ('VIA_IV', lambda d: bool(d['concordant_sigep_codes']))):
        eligible = [d for d in decisions if pred(d)]
        rows[label] = dict(summary([ctx['by_id'][d['profile_id']] for d in eligible]),
                           accepted_profiles=sum(d['status'] == 'ADJUDICATED_CANDIDATE' for d in eligible),
                           reasons=dict(Counter(r for d in eligible for r in d['reasons'])))
    rows['VIA_III']['standalone_identity_grants'] = 0
    rows['VIA_III']['interpretation'] = 'Exact directory corroborates an independently matched institution; it never lends SIGEP legal nature.'
    rows['VIA_I']['why_remaining'] = dict(Counter(d['via_i'] for d in decisions))
    rows['VIA_II']['classification'] = dict(Counter(d['via_ii'] for d in decisions))
    return dict(vias=rows, decisions=decisions, accepted=accepted, by_id=by_id, matches=matches,
                ix=ix, sigep_accounts=accounts)
