"""Falsifiable identity hypotheses. Each trial adds at most one new normalization."""
from collections import Counter, defaultdict
import re

from app.core.entity_types import name_tokens
from app.utils.money import normalize_text
from scripts.audits.m2a.io import stable_id
from scripts.audits.m2a.matching import classify_family, provisional_family
from scripts.audits.m2b.identity_vias import ese_core, strict_match as adr076_match
from scripts.audits.municipal_directory_authority import fiscal, location_code
from scripts.audits.m2b1.io import indexes, summary

RULES = ('M2B1_R0_EXISTING_CANONICAL_ADR076', 'M2B1_R1_OFFICIAL_LOCATION_QUALIFIER',
         'M2B1_R2_TRAILING_CARE_LEVEL', 'M2B1_R3_ESP_LEGAL_DESIGNATOR',
         'M2B1_R4_OFFICIALLY_PRINTED_ACRONYM')
GENERIC_CORE = {'hospital', 'centro', 'puesto', 'salud', 'empresa', 'instituto', 'municipal',
                'local', 'regional', 'de', 'del', 'la', 'el'}
CARE_SUFFIXES = (('de', 'nivel', 'i'), ('de', 'nivel', 'ii'), ('de', 'nivel', 'iii'),
                 ('nivel', 'i'), ('nivel', 'ii'), ('nivel', 'iii'),
                 ('nivel', '1'), ('nivel', '2'), ('nivel', '3'))


def _base(name):
    return ese_core(name)[0]


def _location_core(name, code, towns):
    value = _base(name)
    town = tuple(name_tokens(towns[code]['municipality']))
    if len(value) >= len(town) and value[-len(town):] == town:
        result = value[:-len(town)]
        if result and result[-1] in ('de', 'del', 'municipio'):
            result = result[:-1]
        return result, True
    return value, False


def official_location_qualifier(name, official, code, towns):
    left, left_changed = _location_core(name, code, towns)
    right, right_changed = _location_core(official['NOMBRE'], code, towns)
    distinctive = [token for token in left if token not in GENERIC_CORE]
    if not (left_changed or right_changed) or _base(name) == _base(official['NOMBRE']):
        return False
    return bool(left and left == right and distinctive)


def _care_core(name):
    value = _base(name)
    for suffix in CARE_SUFFIXES:
        if len(value) >= len(suffix) and value[-len(suffix):] == suffix:
            return value[:-len(suffix)], True
    return value, False


def trailing_care_level(name, official, code, towns):
    left, a = _care_core(name)
    right, b = _care_core(official['NOMBRE'])
    return bool((a or b) and left and left == right and _base(name) != _base(official['NOMBRE']))


def _esp_core(name):
    value = tuple(name_tokens(name))
    if value[-3:] == ('e', 's', 'p'):
        return value[:-3], True
    if value[-1:] == ('esp',):
        return value[:-1], True
    return value, False


def esp_legal_designator(name, official, code, towns):
    left, a = _esp_core(name)
    right, b = _esp_core(official['NOMBRE'])
    nature = normalize_text(official['NATURALEZA JURÍDICA'])
    return bool((a or b) and left and left == right and
                nature.startswith('empresas de servicios publicos domiciliarios'))


def printed_acronyms(name):
    values = set()
    for match in re.finditer(r'(?:-|\()\s*([A-ZÁÉÍÓÚÑ]{2,12})\s*(?:-|\))', name):
        token = ''.join(name_tokens(match.group(1)))
        if token not in {'ese', 'esp', 'sa', 'sas', 'ltda'}:
            values.add(token)
    return values


def geographic_tokens(code, towns):
    place = towns[code]
    values = set(name_tokens(place['municipality'])) | set(name_tokens(place['department']))
    values.update((''.join(name_tokens(place['municipality'])),
                   ''.join(name_tokens(place['department']))))
    return {value for value in values if len(value) >= 2}


def officially_printed_acronym(name, official, code, towns):
    value = _base(name)
    return (len(value) == 1 and value[0] not in geographic_tokens(code, towns)
            and value[0] in printed_acronyms(official['NOMBRE']))


MATCHERS = {
    RULES[1]: official_location_qualifier,
    RULES[2]: trailing_care_level,
    RULES[3]: esp_legal_designator,
    RULES[4]: officially_printed_acronym,
}


def _candidate_matches(name, territory, ctx, ix, rule_ids=None):
    result = []
    for i in ix['sigep_territory'][territory]:
        official = ctx['sigep'][i]
        for rule_id in rule_ids or MATCHERS:
            if MATCHERS[rule_id](name, official, territory, ctx['towns']):
                result.append(dict(rule_id=rule_id, sigep_index=i,
                                   sigep_code=official['CÓDIGO SIGEP'], territory=territory))
    return result


def _any_official_identity(name, territory, ctx, ix):
    key = name, territory
    if key in ix['explain_cache']:
        return ix['explain_cache'][key]
    matches = []
    for i in ix['sigep_territory'][territory]:
        s = ctx['sigep'][i]
        method = adr076_match(name, s, ctx['towns'])
        if method:
            matches.append((s['CÓDIGO SIGEP'], method))
        for rule_id in MATCHERS:
            if MATCHERS[rule_id](name, s, territory, ctx['towns']):
                matches.append((s['CÓDIGO SIGEP'], rule_id))
    result = sorted(set(matches))
    ix['explain_cache'][key] = result
    return result


def _same_identity(name, official, rule_id, territory, towns):
    # A cheap over-inclusive ADR-076 screen is correct for veto discovery. The
    # positive candidate path still executes the complete M2B guard.
    left, left_label = ese_core(name)
    right, right_label = ese_core(official['NOMBRE'])
    town = tuple(name_tokens(towns[territory]['municipality']))
    if (tuple(name_tokens(name)) == tuple(name_tokens(official['NOMBRE'])) or
            (normalize_text(official['NATURALEZA JURÍDICA']) == 'empresas sociales del estado'
             and left and left == right and (left_label or right_label) and left != town)):
        return True
    return rule_id in MATCHERS and MATCHERS[rule_id](name, official, territory, towns)


def _identity_base_keys(official, rule_id, territory, towns):
    """Over-inclusive finite keys for competitor discovery; candidates get full checks."""
    base = _base(official['NOMBRE'])
    keys = {base}
    if rule_id == RULES[1]:
        core, _ = _location_core(official['NOMBRE'], territory, towns)
        town = tuple(name_tokens(towns[territory]['municipality']))
        keys.update((core, core + town, core + ('de',) + town, core + ('del',) + town))
    elif rule_id == RULES[2]:
        core, _ = _care_core(official['NOMBRE'])
        keys.update(core + suffix for suffix in CARE_SUFFIXES)
    elif rule_id == RULES[3]:
        core, _ = _esp_core(official['NOMBRE'])
        keys.update((core, core + ('esp',), core + ('e', 's', 'p')))
    elif rule_id == RULES[4]:
        blocked = geographic_tokens(territory, towns)
        keys.update((acronym,) for acronym in printed_acronyms(official['NOMBRE'])
                    if acronym not in blocked)
    return keys


def discover(ctx):
    ix = indexes(ctx)
    matches = defaultdict(list)
    for p in ctx['target_profiles']:
        d = ctx['m2b_decisions'][p['profile_id']]
        # R0 composes existing ADRs: it adds no name normalization.
        if d['concordant_sigep_codes'] and d['existing_canonical_claims'] == [p['physical_source_name']]:
            for m in d['matched_sigep']:
                if m['territory'] == p['territory_divipola']:
                    matches[p['profile_id']].append(dict(rule_id=RULES[0], sigep_index=m['sigep_index'],
                        sigep_code=m['sigep_code'], territory=m['territory']))
        matches[p['profile_id']].extend(_candidate_matches(p['physical_source_name'],
                                                           p['territory_divipola'], ctx, ix))
    return ix, matches


def assess(p, raw_matches, ctx, ix):
    pid, account, territory = p['profile_id'], p['account'], p['territory_divipola']
    # Multiple matching rules for the same row are evidence redundancy; differing entities are ambiguity.
    codes = sorted({m['sigep_code'] for m in raw_matches})
    selected = []
    if len(codes) == 1:
        by_rule = {r: [m for m in raw_matches if m['rule_id'] == r] for r in RULES}
        selected = next((by_rule[r] for r in RULES if by_rule[r]), [])
    reasons = []
    if not raw_matches:
        reasons.append('NO_RULE_MATCH')
    elif len(codes) != 1:
        reasons.append('MULTIPLE_OFFICIAL_ENTITIES')
    match = selected[0] if selected else None
    official = ctx['sigep'][match['sigep_index']] if match else None
    rule_id = match['rule_id'] if match else None
    profile_nits = {fiscal(n) for n in p['fiscal_identity'] if fiscal(n)}
    if p['affiliation'] != 'VERIFIED' or not territory:
        reasons.append('TERRITORY_NOT_FROZEN_VERIFIED')
    if territory == '11001':
        reasons.append('BOGOTA_RESERVED_REGISTRY')
    if official:
        if fiscal(official['NIT']) not in profile_nits or len(profile_nits) != 1:
            reasons.append('FISCAL_CONTRADICTION')
        scope_rows = [ctx['sigep'][i] for i in ix['sigep_code'][match['sigep_code']]]
        if ({s['divipola'] for s in scope_rows} != {territory} or
                any(s['SUBORDEN'] not in ('Municipal', 'Distrital') for s in scope_rows)):
            reasons.append('OFFICIAL_ENTITY_SCOPE_CONFLICT')
        family, family_basis, family_status = classify_family(
            p['physical_source_name'], [ctx['sigep'][m['sigep_index']] for m in selected])
        compatible = (p['institutional_family'] in (family, 'UNKNOWN') or
                      (p['institutional_family'] == 'PUBLIC_SERVICES' and family == 'PUBLIC_COMPANY'))
        if family_status != 'VERIFIED' or not compatible:
            reasons.append('FAMILY_CONFLICT_OR_UNVERIFIED')
    else:
        family, family_basis, family_status, compatible = p['institutional_family'], 'NONE', p['family_status'], False
    own_directory = [i for i in ix['directory_account'][account]
                     if ctx['directory'][i].get('nombre_entidad') == p['physical_source_name']]
    if not own_directory:
        reasons.append('NO_OWN_BYTE_EXACT_DIRECTORY')
    elif ({location_code(ctx['directory'][i]) for i in own_directory} != {territory} or
          any(fiscal(ctx['directory'][i].get('nit_entidad', '')) not in profile_nits for i in own_directory)):
        reasons.append('OWN_DIRECTORY_CONTRADICTION')
    account_other_names = []
    if official:
        for i in ix['directory_account'][account]:
            name = ctx['directory'][i].get('nombre_entidad', '')
            if name != p['physical_source_name'] and not _same_identity(name, official, rule_id, territory, ctx['towns']):
                account_other_names.append(i)
        if account_other_names:
            reasons.append('UNEXPLAINED_SAME_ACCOUNT_DIRECTORY_NAME')
    competing_directory, competing_observed = [], []
    if official:
        possible_directory = {i for key in _identity_base_keys(official, rule_id, territory, ctx['towns'])
                              for i in ix['directory_base'][key]}
        for i in sorted(possible_directory):
            row = ctx['directory'][i]
            other_account = str(row.get('codigo_entidad', ''))
            if other_account == account:
                continue
            row_code = location_code(row)
            if row_code not in (None, territory):
                continue
            if _same_identity(row.get('nombre_entidad', ''), official, rule_id, territory, ctx['towns']):
                competing_directory.append(i)
        possible_profiles = {qid for key in _identity_base_keys(official, rule_id, territory, ctx['towns'])
                             for qid in ix['profile_base'][key]}
        for qid in sorted(possible_profiles):
            q = ctx['all_by_id'][qid]
            if q['account'] == account:
                continue
            qterritory = q.get('territory_divipola')
            if qterritory not in (None, territory):
                continue
            if _same_identity(q['physical_source_name'], official, rule_id, territory, ctx['towns']):
                competing_observed.append(q['profile_id'])
    if competing_directory:
        reasons.append('COMPETING_DIRECTORY_ACCOUNT')
    if competing_observed:
        reasons.append('COMPETING_OBSERVED_ACCOUNT')
    unexplained_fiscal_profiles, unexplained_fiscal_directory = [], []
    explained_fiscal = []
    if official:
        for qid in sorted({qid for nit in profile_nits for qid in ix['profile_fiscal'][nit]}):
            q = ctx['all_by_id'][qid]
            if qid == pid or q['account'] == account:
                continue
            qterritory = q.get('territory_divipola')
            if qterritory not in (None, territory):
                continue
            qfamily = q.get('institutional_family', 'UNKNOWN')
            if qfamily not in (family, 'UNKNOWN'):
                explained_fiscal.append(dict(type='PROFILE', reference=qid, explanation='DISTINCT_INSTITUTIONAL_FAMILY'))
                continue
            other = _any_official_identity(q['physical_source_name'], territory, ctx, ix)
            other_codes = {code for code, _ in other}
            if len(other_codes) == 1 and match['sigep_code'] not in other_codes:
                explained_fiscal.append(dict(type='PROFILE', reference=qid, explanation='DISTINCT_OWN_OFFICIAL_ENTITY'))
            else:
                unexplained_fiscal_profiles.append(qid)
        for i in sorted({i for nit in profile_nits for i in ix['directory_fiscal'][nit]}):
            row = ctx['directory'][i]
            if str(row.get('codigo_entidad', '')) == account:
                continue
            row_code = location_code(row)
            if row_code not in (None, territory):
                continue
            rfamily = provisional_family(row.get('nombre_entidad', ''))
            if rfamily not in (family, 'UNKNOWN'):
                explained_fiscal.append(dict(type='DIRECTORY', reference=i, explanation='DISTINCT_INSTITUTIONAL_FAMILY'))
                continue
            other = _any_official_identity(row.get('nombre_entidad', ''), territory, ctx, ix)
            other_codes = {code for code, _ in other}
            if len(other_codes) == 1 and match['sigep_code'] not in other_codes:
                explained_fiscal.append(dict(type='DIRECTORY', reference=i, explanation='DISTINCT_OWN_OFFICIAL_ENTITY'))
            else:
                unexplained_fiscal_directory.append(i)
    if unexplained_fiscal_profiles or unexplained_fiscal_directory:
        reasons.append('UNEXPLAINED_SAME_FAMILY_FISCAL_PEER')
    claims = []
    if official:
        possible_claims = {name for key in _identity_base_keys(official, rule_id, territory, ctx['towns'])
                           for name in ix['canonical_base'][key]}
        for name in possible_claims:
            if _same_identity(name, official, rule_id, territory, ctx['towns']):
                claims.append(name)
    claims = sorted(set(claims))
    target_existing = p['physical_source_name'] if claims == [p['physical_source_name']] else None
    if len(claims) > 1 or (claims and not target_existing):
        reasons.append('MULTIPLE_OR_DIFFERENT_CANONICAL_CLAIM')
    reasons = sorted(set(reasons))
    accepted = bool(match and not reasons)
    logical_id = (stable_id('logical', 'canonical', target_existing) if accepted and target_existing
                  else stable_id('m2b1-logical', match['sigep_code'], account) if accepted else None)
    return dict(profile_id=pid, source_class=ctx['primary'][pid], account=account,
                physical_source_name=p['physical_source_name'], territory_divipola=territory,
                unique_processes=p['volumes']['unique_processes'], contract_rows=p['volumes']['contract_physical_rows'],
                rules_matched=sorted({m['rule_id'] for m in raw_matches}), selected_rule=rule_id,
                official_matches=raw_matches, selected_sigep_index=match['sigep_index'] if match else None,
                selected_sigep_code=match['sigep_code'] if match else None,
                identity_authority=rule_id if accepted else None, status='ADJUDICATED_DELTA' if accepted else 'KEEP_REVIEW',
                target_existing_canonical=target_existing, candidate_logical_unit_id=logical_id,
                creates_new_logical_unit=bool(accepted and not target_existing), family=family,
                family_basis=family_basis, family_status=family_status, family_compatible=compatible,
                own_directory_indices=own_directory, account_other_name_indices=account_other_names,
                competing_directory_indices=competing_directory, competing_profile_ids=competing_observed,
                unexplained_fiscal_profile_ids=unexplained_fiscal_profiles,
                unexplained_fiscal_directory_indices=unexplained_fiscal_directory,
                explained_fiscal_peers=explained_fiscal, canonical_claims=claims,
                contradictions=reasons, merge_applied=False,
                evidence_sources=([f'artifacts/audits/municipal_m0_v1/sources/sigep.json.gz#/{match["sigep_index"]}'] if match else []) +
                    [f'artifacts/audits/municipal_m0_v1/sources/directory.json.gz#/rows/{i}' for i in own_directory] +
                    [f'artifacts/audits/municipal_m2b/pending_identity_profiles.json#profile_id={pid}'])


def evaluate(ctx):
    ix, discovered = discover(ctx)
    decisions = [assess(p, discovered[p['profile_id']], ctx, ix) for p in ctx['target_profiles']]
    by_id = {d['profile_id']: d for d in decisions}
    rows = {}
    for rule_id in RULES:
        matched = [d for d in decisions if rule_id in d['rules_matched']]
        accepted = [d for d in decisions if d['identity_authority'] == rule_id]
        rejected = [d for d in matched if d['status'] == 'KEEP_REVIEW']
        rows[rule_id] = dict(rule_id=rule_id, scope={
                RULES[0]: 'Exact canonical plus own ADR-076 identity; composition of accepted authorities.',
                RULES[1]: 'Remove only the complete accredited municipality as terminal location qualifier after ADR-076 legal-label handling.',
                RULES[2]: 'Remove only a terminal NIVEL I/II/III/1/2/3 care qualifier after ADR-076 legal-label handling.',
                RULES[3]: 'Remove only terminal ESP when own SIGEP nature is public-services enterprise.',
                RULES[4]: 'Use only an acronym printed literally between delimiters in the own official name.',
            }[rule_id], profiles_matched=len(matched), profiles_accepted=len(accepted), profiles_rejected=len(rejected),
            process_gain=sum(d['unique_processes'] for d in accepted),
            contract_gain=sum(d['contract_rows'] for d in accepted),
            matched_profile_ids=sorted(d['profile_id'] for d in matched),
            accepted_profile_ids=sorted(d['profile_id'] for d in accepted),
            rejected_profile_ids=sorted(d['profile_id'] for d in rejected),
            contradictions=dict(Counter(r for d in rejected for r in d['contradictions'])),
            authority_chain=['PHYSICAL_PROFILE', 'FROZEN_VERIFIED_TERRITORY', 'OWN_BYTE_EXACT_DIRECTORY',
                             'OWN_OFFICIAL_SIGEP_NAME', 'OWN_SIGEP_LEGAL_NATURE',
                             'UNIQUE_ENTITY_AND_EXPLAINED_ACCOUNTS'],
            false_positive_canaries=['NO_TOWN_ONLY_CORE', 'NO_INTERNAL_OR_MULTIPLE_NEW_RELAXATIONS',
                                     'NO_UNPRINTED_ACRONYM_EXPANSION', 'NO_FISCAL_SELECTION',
                                     'DISTINCT_HOSPITALS_SAME_TERRITORY_STAY_SEPARATE'])
    return dict(rules=rows, decisions=decisions, by_id=by_id,
                accepted=[d for d in decisions if d['status'] == 'ADJUDICATED_DELTA'], ix=ix)


def official_conflicts(ctx):
    rows = []
    for p in ctx['target_all']['H']:
        d = ctx['m2b_decisions'][p['profile_id']]
        categories = []
        if any('FISCAL' in reason for reason in d['reasons']):
            categories.append('FISCAL_CONFLICT')
        if any(m['territory'] != p['territory_divipola'] for m in d['matched_sigep']) or d['frozen_conflicts']:
            categories.append('TERRITORY_CONFLICT')
        if len({m['sigep_code'] for m in d['matched_sigep']}) > 1:
            categories.append('MULTIPLE_OFFICIAL_ENTITIES')
        if d['competing_observed_accounts'] or d['competing_directory_accounts']:
            categories.append('ACCOUNT_CONFLICT')
        if any('FAMILY' in reason or 'NATURE' in reason for reason in d['reasons']):
            categories.append('NAME_CONFLICT')
        if not categories:
            categories.append('NAME_CONFLICT')
        rows.append(dict(profile_id=p['profile_id'], conflict_types=categories, status='KEEP_REVIEW',
                         higher_independent_authority='NONE_IN_FROZEN_INPUTS',
                         official_matches=d['matched_sigep'], frozen_conflicts=d['frozen_conflicts'],
                         reasons=d['reasons']))
    return dict(input=summary(ctx['target_all']['H']),
                conflict_counts=dict(Counter(value for row in rows for value in row['conflict_types'])),
                profiles=rows, resolved=0, keep_review=len(rows),
                policy='Contradictions are not ignored or voted away; no higher independent authority was present.')
