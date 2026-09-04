"""Offline M1B discovery. Writes audit artifacts only; never promotes identities.

The coverage cohort is frozen M0 re-evaluated against current production. An
account/name profile is an observation, not a logical BuyerUnit. Contextual
location signals never enter the accredited denominator or manufacture owners.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.entity_resolver import EntityResolver
from app.core.entity_types import name_tokens
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError
from app.utils.money import normalize_text
from scripts.audits.municipal_directory_authority import (
    ACCREDITED, AUTHORITY, directory_affiliation, executive, fiscal,
    location_code, validate_cohort,
)

M0 = ROOT / 'artifacts/audits/municipal_m0_v1'
FINAL = ROOT / 'artifacts/audits/municipal_m1a_final'
M1A2A = ROOT / 'artifacts/audits/municipal_m1a_2a'
OUT = ROOT / 'artifacts/audits/municipal_m1b'
EXPECTED_HEAD = '51a5ad9a4f47e55f65f2c7d31e825e937c461d3e'
DATASETS = ('p6dx-8zbt', 'jbjy-vk9h')
FAMILIES = ('MUNICIPAL_EXECUTIVE', 'CONCEJO', 'PERSONERIA', 'CONTRALORIA',
            'ESE_HEALTH', 'INSTITUTE', 'PUBLIC_COMPANY', 'FUND',
            'OTHER_DECENTRALIZED', 'REGISTRY_NOISE', 'UNKNOWN_REVIEW')
DEBTS = ('MISSING_EXECUTIVE_BUYER_UNIT', 'EXISTING_BUYER_UNIT_MISSING_AFFILIATION',
         'SOURCE_NAME_VARIANT', 'DUPLICATE_BUYER_UNIT', 'NON_EXECUTIVE_MUNICIPAL_UNIT',
         'SHARED_NIT_DISTINCT_UNIT', 'REGISTRY_NOISE', 'MISSING_DATASET_EVIDENCE',
         'SECOP1_ONLY_OR_LEGACY_SIGNAL', 'REVIEW')
DISPOSITIONS = ('AUTO_SAFE', 'MANUAL_REVIEW', 'KEEP_SEPARATE',
                'SOURCE_BINDING_ONLY', 'ALIAS_ONLY', 'DO_NOT_ADD')
METRICS = ('process_physical_rows', 'unique_processes',
           'contract_physical_rows', 'unique_contracts')


def read(path):
    raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == '.gz' else raw)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def derive_universe(analysis):
    active = {r['municipality'] for r in analysis['strata'] if r['status'] == ACCREDITED}
    recognized = {r['municipality'] for r in analysis['strata'] if r['covered']}
    residual = active - recognized
    if (len(active), len(recognized), len(residual)) != (607, 571, 36) or not recognized <= active:
        raise ValueError(f'STOP: expected 607/571/36; got {len(active)}/{len(recognized)}/{len(residual)}')
    return active, recognized, residual


def volumes(rows):
    return {key: sum(r[field] for r in rows if r['dataset'] == ds)
            for key, ds, field in (
                ('process_physical_rows', DATASETS[0], 'physical_rows'),
                ('unique_processes', DATASETS[0], 'unique_ids'),
                ('contract_physical_rows', DATASETS[1], 'physical_rows'),
                ('unique_contracts', DATASETS[1], 'unique_ids'))}


def sum_volumes(items):
    return {k: sum(r[k] for r in items) for k in METRICS}


def family(name, sigep_rows):
    """Legal nature is primary. Names refine public establishments, never owners."""
    natures = {normalize_text(s['NATURALEZA JURÍDICA']) for s in sigep_rows}
    legal = {
        'alcaldia': 'MUNICIPAL_EXECUTIVE', 'concejo municipal': 'CONCEJO',
        'personeria municipal': 'PERSONERIA', 'contraloria municipal': 'CONTRALORIA',
        'empresas sociales del estado': 'ESE_HEALTH',
        'empresas de servicios publicos domiciliarios - oficiales': 'PUBLIC_COMPANY',
        'sociedad de economia mixta': 'PUBLIC_COMPANY',
        'empresas industriales y comerciales del estado': 'PUBLIC_COMPANY',
    }
    mapped = {legal[n] for n in natures if n in legal}
    if 'organos autonomos' in natures:
        return 'UNKNOWN_REVIEW', 'SIGEP_LEGAL_NATURE_OUTSIDE_MUNICIPAL_TAXONOMY'
    if len(mapped) > 1:
        return 'UNKNOWN_REVIEW', 'CONFLICTING_SIGEP_LEGAL_NATURES'
    if mapped:
        return next(iter(mapped)), 'SIGEP_LEGAL_NATURE'
    norm = normalize_text(name)
    if natures == {'establecimiento publico'}:
        if norm.startswith(('instituto ', 'unidad central ')):
            return 'INSTITUTE', 'SIGEP_PUBLIC_ESTABLISHMENT_AND_OFFICIAL_NAME'
        return 'OTHER_DECENTRALIZED', 'SIGEP_PUBLIC_ESTABLISHMENT'
    # Explicitly provisional classification for contextual, unaccredited rows.
    rules = [('concejo', 'CONCEJO'), ('personeria', 'PERSONERIA'),
             ('contraloria', 'CONTRALORIA'), ('empresa social del estado', 'ESE_HEALTH'),
             ('hospital', 'ESE_HEALTH'), ('centro de salud', 'ESE_HEALTH'),
             ('fondo ', 'FUND'), ('f.s.e.', 'FUND'), ('instituto ', 'INSTITUTE'),
             ('empresa', 'PUBLIC_COMPANY')]
    if executive(name):
        return 'MUNICIPAL_EXECUTIVE', 'SOURCE_INSTITUTIONAL_FAMILY_ONLY_UNACCREDITED'
    if 'alcaldia de ' in norm and norm.startswith(('secretaria ', 'departamento administrativo ',
                                                 'oficina ', 'desarrollo institucional ')):
        return 'MUNICIPAL_EXECUTIVE', 'SOURCE_EXPLICIT_EXECUTIVE_OFFICE_UNACCREDITED'
    for needle, result in rules:
        if needle in norm:
            return result, 'SOURCE_NAME_ONLY_UNACCREDITED'
    return 'UNKNOWN_REVIEW', 'INSUFFICIENT_OFFICIAL_CLASSIFICATION'


def indexes(analysis, directory, sigep, records, sidecar, towns):
    ix = {k: defaultdict(list) for k in ('account_rows', 'directory', 'sigep_fiscal',
                                        'geo', 'binding_claims', 'history')}
    ix.update(owners=defaultdict(set), recent=defaultdict(set), audited=set(),
              records={r['official_name']: r for r in records},
              bindings={b['canonical_name']: b for b in sidecar['bindings']},
              sigep={s['CÓDIGO SIGEP']: s for s in sigep})
    for i, r in enumerate(analysis['strata']):
        ix['account_rows'][r['account']].append((i, r))
        ix['recent'][r['dataset'], r['name']].add((r['account'], r['nit_evidence']))
    for i, r in enumerate(directory):
        ix['directory'][str(r['codigo_entidad'])].append((i, r))
    for s in sigep:
        ix['sigep_fiscal'][fiscal(s['NIT'])].append(s)
    for r in records:
        for name in [r['official_name'], *r.get('aliases', [])]:
            ix['owners'][normalize_text(name)].add(r['official_name'])
    for b in sidecar['bindings']:
        for ds, names in b['sources'].items():
            for name in names:
                ix['binding_claims'][ds, name].append(b['canonical_name'])
    for code, t in towns.items():
        ix['geo'][normalize_text(t['municipality']), normalize_text(t['department'])].append(code)
    for i, batch in enumerate(read(M0 / 'sources/ownership.json.gz')):
        if batch.get('error') or 'rows' not in batch or len(batch['rows']) >= 10000:
            continue
        ix['audited'].update((batch['dataset'], n) for n in batch['source_names'])
        for r in batch['rows']:
            name = r.get('entidad', r.get('nombre_entidad'))
            ix['history'][batch['dataset'], name].append(dict(batch_index=i, row=r))
    return ix


def inventory_signals(analysis, ix, sources, residual, adjudications):
    """Location selects context, never municipal membership or a source owner."""
    signals = defaultdict(list)
    for i, row in enumerate(analysis['strata']):
        if row['status'] == ACCREDITED and row['municipality'] in residual:
            signals[row['account']].append(dict(code=row['municipality'], basis='M0_ACCREDITED',
                                               stratum_index=i))
    for account, regs in ix['directory'].items():
        for i, row in regs:
            code = location_code(row)
            if code in residual:
                signals[account].append(dict(code=code, basis='DIRECTORY_LOCATION_CONTEXT_ONLY',
                                            directory_index=i))
    for ds, src in sources.items():
        for i, row in enumerate(src['rows']):
            city = row.get('ciudad_entidad', row.get('ciudad', ''))
            dep = row.get('departamento_entidad', row.get('departamento', ''))
            for code in ix['geo'][normalize_text(city), normalize_text(dep)]:
                if code in residual:
                    signals[str(row['codigo_entidad'])].append(dict(
                        code=code, basis='SOURCE_GEOGRAPHY_CONTEXT_ONLY', dataset=ds, source_index=i))
    for a in adjudications:
        if a['decision'] == 'VERIFIED_MUNICIPAL_AFFILIATION' and a['expected_divipola'] in residual:
            signals[a['account']].append(dict(code=a['expected_divipola'],
                                            basis='M1A2A_FROZEN_ADJUDICATION'))
    return signals


def make_profiles(analysis, ix, signals, residual, accepted):
    profiles = []
    for account in sorted(signals):
        grouped = defaultdict(list)
        for index, row in ix['account_rows'][account]:
            grouped[row['name']].append((index, row))
        for name, indexed in sorted(grouped.items()):
            rows = [r for _, r in indexed]
            credited = sorted({r['municipality'] for r in rows if r['status'] == ACCREDITED})
            scope = 'RESIDUAL_COHORT' if set(credited) & residual else 'CONTEXT_ONLY'
            linked_sigep = {c for r in rows for c in r['sigep_codes']}
            witnesses = {c: ix['sigep'][c] for c in linked_sigep}
            # Exact institutional tokens + fiscal evidence supplies classification,
            # not an automatic jurisdiction. M0's cohort remains unchanged.
            for nit in {r['nit_evidence'] for r in rows}:
                for s in ix['sigep_fiscal'][fiscal(nit)]:
                    if name_tokens(s['NOMBRE']) == name_tokens(name):
                        witnesses[s['CÓDIGO SIGEP']] = s
            institution, basis = family(name, list(witnesses.values()))
            owners = sorted(ix['owners'][normalize_text(name)])
            claims = {ds: sorted(ix['binding_claims'][ds, name]) for ds in DATASETS}
            canonical = owners[0] if len(owners) == 1 else None
            affiliations = accepted.get(canonical, {})
            history = {ds: dict(audited=(ds, name) in ix['audited'],
                                evidence=ix['history'][ds, name]) for ds in DATASETS}
            history_owners = {ds: sorted({(str(h['row']['codigo_entidad']), str(h['row']['nit_entidad']))
                                         for h in history[ds]['evidence']}) for ds in DATASETS}
            collisions = {ds: [list(pair) for pair in sorted(ix['recent'][ds, name])]
                          for ds in DATASETS if len(ix['recent'][ds, name]) > 1}
            p = dict(profile_id='m1b:' + sha(json.dumps([account, name]).encode())[:16],
                     account=account, physical_source_name=name, scope=scope,
                     divipola=credited[0] if len(credited) == 1 and scope == 'RESIDUAL_COHORT' else None,
                     context_divipola=sorted({s['code'] for s in signals[account]}),
                     territory_signals=signals[account],
                     nit_evidence=sorted({r['nit_evidence'] for r in rows}),
                     volumes=volumes(rows),
                     uncovered_cohort_volumes=volumes([r for r in rows if r['status'] == ACCREDITED
                                                      and r['municipality'] in residual and not r['covered']]),
                     strata=[dict(index=i, **r) for i, r in indexed],
                     directory_evidence=[dict(index=i, row=r, exact_name=r['nombre_entidad'] == name)
                                         for i, r in ix['directory'][account]],
                     sigep_evidence=[dict(row=s, m0_affiliation_witness=c in linked_sigep)
                                     for c, s in sorted(witnesses.items())],
                     institutional_family=institution, classification_basis=basis,
                     current_gazetteer_owners=owners,
                     current_gazetteer_owner=canonical,
                     current_owner_asserted=False,
                     source_projection=ix['bindings'].get(canonical),
                     exact_source_binding_claims=claims,
                     current_affiliation=affiliations,
                     history=history, historical_source_owners=history_owners,
                     historical_source_collisions={ds: owners for ds, owners in history_owners.items()
                                                   if len({o[0] for o in owners}) > 1},
                     recent_source_collisions=collisions,
                     same_account_other_source_names=sorted(set(grouped) - {name}),
                     secop1_evidence='NOT_CAPTURED',
                     contract_value=None,
                     contract_value_status='NOT_MEASURABLE_FROM_FROZEN_AGGREGATES')
            profiles.append(p)
    # Fiscal equivalence only annotates peers; the account/name key is unchanged.
    by_fiscal = defaultdict(list)
    for p in profiles:
        for nit in {fiscal(n) for n in p['nit_evidence']}:
            by_fiscal[nit].append(p)
    for p in profiles:
        peers = {q['profile_id']: q for nit in p['nit_evidence'] for q in by_fiscal[fiscal(nit)]
                 if q['account'] != p['account']}
        p['shared_nit_other_profiles'] = sorted(peers)
        p['shared_nit_distinct_institutional_profiles'] = sorted(
            q['profile_id'] for q in peers.values()
            if q['institutional_family'] != p['institutional_family']
            and 'UNKNOWN_REVIEW' not in (q['institutional_family'], p['institutional_family']))
    return profiles


def classify_debt(p):
    fam = p['institutional_family']
    if p['scope'] == 'CONTEXT_ONLY':
        if p['current_affiliation']:
            return 'EXISTING_BUYER_UNIT_MISSING_AFFILIATION'
        if p['shared_nit_distinct_institutional_profiles']:
            return 'SHARED_NIT_DISTINCT_UNIT'
        return 'REVIEW'
    if fam == 'UNKNOWN_REVIEW':
        return 'REVIEW'
    if p['recent_source_collisions'] or p['historical_source_collisions']:
        return 'REVIEW'
    if p['same_account_other_source_names']:
        return 'SOURCE_NAME_VARIANT'
    if fam == 'MUNICIPAL_EXECUTIVE' and {r['dataset'] for r in p['strata']} != set(DATASETS):
        return 'MISSING_DATASET_EVIDENCE'
    if p['shared_nit_distinct_institutional_profiles']:
        return 'SHARED_NIT_DISTINCT_UNIT'
    if fam != 'MUNICIPAL_EXECUTIVE':
        return 'NON_EXECUTIVE_MUNICIPAL_UNIT'
    if p['current_gazetteer_owner']:
        return 'EXISTING_BUYER_UNIT_MISSING_AFFILIATION'
    return 'MISSING_EXECUTIVE_BUYER_UNIT'


def candidate(p):
    """AUTO_SAFE requires affirmative evidence, including a tested future pipeline."""
    blockers = []
    if p['scope'] != 'RESIDUAL_COHORT':
        blockers.append('OUTSIDE_FIXED_M0_COHORT')
    if p['institutional_family'] == 'UNKNOWN_REVIEW':
        blockers.append('INSTITUTIONAL_SCOPE_REQUIRES_ADJUDICATION')
    if p['recent_source_collisions'] or p['historical_source_collisions']:
        blockers.append('EXACT_SOURCE_NAME_SELECTS_MULTIPLE_ACCOUNTS')
    if p['same_account_other_source_names']:
        blockers.append('MULTIPLE_SOURCE_NAMES_REQUIRE_IDENTITY_ADJUDICATION')
    if {r['dataset'] for r in p['strata']} != set(DATASETS):
        blockers.append('MISSING_RECENT_DATASET_EVIDENCE')
    if len(p['nit_evidence']) != 1:
        blockers.append('FISCAL_EVIDENCE_REVIEW')
    if len(p['current_gazetteer_owners']) > 1:
        blockers.append('MULTIPLE_EXACT_ALIAS_OWNERS')
    expected = {(p['account'], nit) for nit in p['nit_evidence']}
    for ds in DATASETS:
        observed = {tuple(o) for o in p['historical_source_owners'][ds]}
        if not p['history'][ds]['audited'] or observed != expected:
            blockers.append('HISTORICAL_SOURCE_OWNERSHIP_NOT_EXCLUSIVE:' + ds)
    # This discovery does not construct aliases, a new catalogue or bindings.
    # The absence of a promoted-candidate pipeline gate cannot count as PASS.
    blockers.append('PROSPECTIVE_PIPELINE_NOT_VALIDATED_NO_CANDIDATE_CATALOGUE')
    if p['scope'] == 'CONTEXT_ONLY' and p['current_affiliation'] and p.get('current_pipeline_unwanted_filters'):
        disposition = 'MANUAL_REVIEW'
    elif p['scope'] == 'CONTEXT_ONLY' and p['current_affiliation']:
        disposition = 'SOURCE_BINDING_ONLY' if not p['source_projection'] else 'DO_NOT_ADD'
    elif p['institutional_family'] == 'UNKNOWN_REVIEW':
        disposition = 'MANUAL_REVIEW'
    elif p['recent_source_collisions'] or p['historical_source_collisions']:
        disposition = 'KEEP_SEPARATE'
    elif p['same_account_other_source_names']:
        disposition = 'MANUAL_REVIEW'
    elif p['shared_nit_distinct_institutional_profiles']:
        disposition = 'KEEP_SEPARATE'
    elif p['institutional_family'] != 'MUNICIPAL_EXECUTIVE':
        disposition = 'KEEP_SEPARATE'
    else:
        disposition = 'MANUAL_REVIEW'
    return dict(profile_id=p['profile_id'], divipola=p['divipola'],
                context_divipola=p['context_divipola'], scope=p['scope'],
                account=p['account'], physical_source_name=p['physical_source_name'],
                institutional_family=p['institutional_family'], debt=p['debt'],
                disposition=disposition, blocking_requirements=blockers,
                evidence_ready_except_pipeline=blockers == ['PROSPECTIVE_PIPELINE_NOT_VALIDATED_NO_CANDIDATE_CATALOGUE'],
                safe_to_apply=False, production_applied=False,
                proposed_identity_merge=False,
                uncovered_cohort_volumes=p['uncovered_cohort_volumes'])


def coverage_scenario(items, base):
    delta = sum_volumes(items)
    return dict(incremental_volume=delta,
                incremental_process_coverage=100 * delta['unique_processes'] / base['denominator']['unique_processes'],
                incremental_contract_coverage=100 * delta['unique_contracts'] / base['denominator']['unique_contracts'],
                process_coverage_pct=100 * (base['covered']['unique_processes'] + delta['unique_processes']) / base['denominator']['unique_processes'],
                contract_coverage_pct=100 * (base['covered']['unique_contracts'] + delta['unique_contracts']) / base['denominator']['unique_contracts'])


def rank_impact(analysis, territories, profiles, candidates):
    cohort = [r for r in analysis['strata'] if r['status'] == ACCREDITED]
    base = dict(denominator=volumes(cohort), covered=volumes([r for r in cohort if r['covered']]))
    ranked = sorted(territories, key=lambda t: (-t['uncovered_cohort_volumes']['unique_processes'],
                                               -t['uncovered_cohort_volumes']['unique_contracts'], t['divipola']))
    rows = [dict(rank=i + 1, divipola=t['divipola'], municipality=t['municipality'],
                 department=t['department'], **t['uncovered_cohort_volumes'], contract_value=None,
                 impact_score=t['uncovered_cohort_volumes']['unique_processes'] + t['uncovered_cohort_volumes']['unique_contracts'])
            for i, t in enumerate(ranked)]
    cuts = {}
    for label, count in [('TOP_5', 5), ('TOP_10', 10), ('TOP_20', 20), ('ALL_36', len(rows))]:
        selected = ranked[:count]
        cuts[label] = dict(divipola=[t['divipola'] for t in selected],
                           assumption='Ceiling: all accredited profiles in selected territories become covered; not an apply forecast.',
                           **coverage_scenario([t['uncovered_cohort_volumes'] for t in selected], base))
    safe = [c for c in candidates if c['safe_to_apply'] and c['scope'] == 'RESIDUAL_COHORT']
    ready = [c for c in candidates if c['evidence_ready_except_pipeline']]
    other = volumes([r for r in cohort if not r['covered'] and r['municipality'] not in {t['divipola'] for t in territories}])
    return dict(baseline=base, current=coverage_scenario([], base),
                ranking_policy='unique_processes DESC, unique_contracts DESC, DIVIPOLA ASC; value unavailable; score = processes + contracts is diagnostic, not identity or monetary value.',
                contract_value_status='No valor_del_contrato or SUM(value) in frozen sources; not estimated.',
                rows=rows, cuts=cuts,
                ALL_SAFE=coverage_scenario([c['uncovered_cohort_volumes'] for c in safe], base),
                EVIDENCE_READY_PENDING_PIPELINE=dict(profile_ids=[c['profile_id'] for c in ready],
                    assumption='Conditional ceiling after separate pipeline and promotion gates, not AUTO_SAFE.',
                    **coverage_scenario([c['uncovered_cohort_volumes'] for c in ready], base)),
                uncovered_in_already_recognized_territories=other,
                fixed_cohort_does_not_include_new_directory_affiliations=True)


def observed_pipeline(accepted, ix, towns):
    """Read-only checks of existing owners. No temporary catalogue or projector."""
    router, builder = QueryRouter(), SoQLBuilder()
    results = []
    for canonical, affiliation in sorted(accepted.items()):
        code = affiliation['codes'][0]
        resolver = EntityResolver(ROOT / 'app/data/aliases_db.json')
        alias = next(a for a in ix['records'][canonical]['aliases']
                     if resolver.resolve_entidad(a).value == canonical
                     and resolver.resolve_entidad(a).method == 'exact')
        for ds in DATASETS:
            kind = 'procesos' if ds == DATASETS[0] else 'contratos'
            query = f'{kind} de {alias}'
            params = router.parse(query).params
            plan = compute_query_plan(params, query)
            try:
                soql = builder.build_count(ds, dict(plan.params))
            except (UnsafeGlobalQueryError, ValueError) as error:
                soql = 'BLOCKED: ' + str(error)
            result = dict(canonical=canonical, code=code, query=query, dataset=ds,
                          observed_owner=params.get('entidad_resolved'), should_execute=plan.should_execute,
                          soql=soql, has_explicit_projection=canonical in ix['bindings'],
                          unwanted_filters={k: params[k] for k in ('objeto', 'ordering_signal', 'municipal_territory') if params.get(k)})
            result['wrong_owner'] = bool(result['observed_owner'] and result['observed_owner'] != canonical)
            result['owner_check_passed'] = result['observed_owner'] == canonical and plan.should_execute
            result['pipeline_safe'] = (result['owner_check_passed'] and not result['unwanted_filters']
                                       and result['has_explicit_projection'] and ' LIKE ' not in soql)
            results.append(result)
    return results


def make_territories(residual, towns, profiles, sigep, accepted, rejected):
    territories = []
    for code in sorted(residual):
        primary = [p for p in profiles if p['divipola'] == code and p['scope'] == 'RESIDUAL_COHORT']
        context = [p for p in profiles if code in p['context_divipola'] and p not in primary]
        exec_sigep = [s for s in sigep if normalize_text(s['NATURALEZA JURÍDICA']) == 'alcaldia'
                      and normalize_text(s['MUNICIPIO']) == normalize_text(towns[code]['municipality'])
                      and normalize_text(s['DEPARTAMENTO']) == normalize_text(towns[code]['department'])]
        execs = [p for p in [*primary, *context] if p['institutional_family'] == 'MUNICIPAL_EXECUTIVE']
        recognized_execs = [c for c, a in accepted.items() if a['codes'] == [code]]
        other = [p for p in primary if p['institutional_family'] not in ('MUNICIPAL_EXECUTIVE', 'UNKNOWN_REVIEW', 'REGISTRY_NOISE')]
        territories.append(dict(
            **towns[code], residual_profile_ids=[p['profile_id'] for p in primary],
            context_profile_ids=[p['profile_id'] for p in context],
            HAS_EXECUTIVE='YES' if exec_sigep or recognized_execs else 'REVIEW',
            has_executive_basis='Official SIGEP legal existence or scoped adjudication; not proof of recent SECOP executive activity.',
            executive_sigep_evidence=exec_sigep,
            EXECUTIVE_ALREADY_RECOGNIZED='YES' if recognized_execs else 'NO',
            recognized_executive_owners=recognized_execs,
            accredited_executive_profile_ids=[p['profile_id'] for p in primary if p['institutional_family'] == 'MUNICIPAL_EXECUTIVE'],
            observed_executive_profile_ids=[p['profile_id'] for p in execs],
            OTHER_MUNICIPAL_BUYER_UNITS=len({p['account'] for p in other}),
            other_municipal_count_basis='Distinct accredited nonexecutive observation accounts; provisional, not a certified logical-unit census.',
            other_municipal_profile_ids=[p['profile_id'] for p in other],
            uncovered_cohort_volumes=sum_volumes([p['uncovered_cohort_volumes'] for p in primary]),
            m0_executive_proposal_rejection=rejected[code],
            debt_counts=dict(Counter(p['debt'] for p in primary))))
    return territories


def family_summary(profiles):
    counts = {f: sum(p['institutional_family'] == f for p in profiles) for f in FAMILIES}
    executive_count = counts['MUNICIPAL_EXECUTIVE']
    review_count = counts['UNKNOWN_REVIEW'] + counts['REGISTRY_NOISE']
    other_count = len(profiles) - executive_count - review_count
    return dict(total_profiles=len(profiles), by_family=counts,
                executive_pct=100 * executive_count / len(profiles) if profiles else 0,
                nonexecutive_pct=100 * other_count / len(profiles) if profiles else 0,
                review_pct=100 * review_count / len(profiles) if profiles else 0,
                volume_by_family={f: sum_volumes([p['uncovered_cohort_volumes'] for p in profiles if p['institutional_family'] == f]) for f in FAMILIES})


def render_report(out):
    """All report counts and tables come from the six machine-readable artifacts."""
    g = read(out / 'gates.json')
    b = read(out / 'buyer_profiles.json')
    c = read(out / 'classification.json')
    impact = read(out / 'impact_ranking.json')
    promotions = read(out / 'promotion_candidates.json')
    territories = read(out / 'residual_territories.json')['territories']
    primary = [p for p in b['profiles'] if p['scope'] == 'RESIDUAL_COHORT']
    by_id = {p['profile_id']: p for p in primary}
    candidates = {p['profile_id']: p for p in promotions['candidates']}
    cohort = c['residual_cohort']
    executive_volume = cohort['volume_by_family']['MUNICIPAL_EXECUTIVE']
    total = impact['cuts']['ALL_36']['incremental_volume']
    pct = lambda value: f'{value:.4f}%'
    esc = lambda value: str(value).replace('|', '\\|').replace('\n', ' ')
    lines = [
        '# Municipal M1B — residuo acreditado y señal para M2', '',
        f"**MUNICIPAL_M1B: {g['MUNICIPAL_M1B']}. DISCOVERY / READ-ONLY. PUSH: NO.**", '',
        f"Rama `main`, baseline `{EXPECTED_HEAD[:7]}`. **607 − 571 = 36**, derivado por diferencia de conjuntos; la lista no está hardcodeada. "
        f"Los 36 contienen **{len(primary)} perfiles físicos** `(account, physical_source_name)` en **{len({p['account'] for p in primary})} cuentas**, "
        f"con **{total['process_physical_rows']:,} filas físicas de procesos / {total['unique_processes']:,} procesos únicos** y "
        f"**{total['contract_physical_rows']:,} filas físicas de contratos / {total['unique_contracts']:,} contratos únicos**.", '',
        f"Hay **{c['executive_gap_profiles']} perfiles ejecutivos en {c['executive_gap_accounts']} cuentas** y "
        f"**{len(primary) - c['executive_gap_profiles']} perfiles de otras instituciones**. Angelópolis publica dos grafías por una cuenta; "
        'no se fusionan en el inventario ni se confunden 36 territorios con 36 alcaldías faltantes.', '',
        '## Alcance, fuentes y autoridades', '',
        'Se leen M1A FINAL, M0 V1, ADR-072/073/074, coverage_post, analysis, proposals, directory, SIGEP, procesos, contratos, '
        'ownership histórico y adjudicación/gates M1A.2A. La ventana de actividad es `[2025-09-03, 2026-09-03)`. '
        'El script verifica el manifiesto SHA-256 de M0 y reejecuta `audit_municipal_m0.py` con salida temporal. '
        'El resultado debe ser idéntico al post-analysis congelado de M1A FINAL y sus volúmenes deben coincidir con coverage_post.', '',
        'El denominador se conserva: 339.745 procesos únicos y 331.185 contratos únicos de la cohorte municipal acreditada. '
        'Una afiliación nueva de directorio no se suma al numerador de ese denominador sin ampliar y volver a conciliar la cohorte. '
        'Ubicación de directorio o geografía de la fuente sirve únicamente para seleccionar contexto.', '',
        f"El inventario añade **{b['totals']['context_profiles']} perfiles de contexto** con actividad y **{b['totals']['registry_only_accounts']} cuentas "
        'registradas sin actividad observada en la ventana**. Se conservan separadas de los 39 perfiles que explican la deuda. '
        'Son señales de ubicación, afiliación no municipal o afiliación pendiente; no se convierten en pertenencia municipal. '
        'Las cuentas sin actividad no se declaran ruido, inactivas ni SECOP I por ausencia de datos.', '',
        'Cada perfil conserva estratos y referencias a las fuentes, cuenta, nombre físico, grafías fiscales, filas/IDs por dataset, '
        'directorio, SIGEP, coincidencias exactas del catálogo, bindings existentes y conflictos históricos/recientes. '
        'Las coincidencias de alias sin adjudicación se etiquetan como observaciones, no owners acreditados. '
        'No se hace fuzzy ni se deduplica por NIT. La equivalencia fiscal sólo elimina puntos/espacios y un DV separado para anotar pares.', '',
        '## Impacto y denominadores', '',
        '| Corte | Δ procesos únicos | Δ contratos únicos | Δ pp procesos | Δ pp contratos | Cobertura procesos | Cobertura contratos |',
        '|---|---:|---:|---:|---:|---:|---:|',
    ]
    scenarios = [('ACTUAL', impact['current']), *impact['cuts'].items(), ('ALL_SAFE', impact['ALL_SAFE']),
                 ('EVIDENCIA LISTA, PIPELINE PENDIENTE', impact['EVIDENCE_READY_PENDING_PIPELINE'])]
    for label, s in scenarios:
        v = s['incremental_volume']
        lines.append(f"| {label} | {v['unique_processes']} | {v['unique_contracts']} | "
                     f"{s['incremental_process_coverage']:.4f} | {s['incremental_contract_coverage']:.4f} | "
                     f"{pct(s['process_coverage_pct'])} | {pct(s['contract_coverage_pct'])} |")
    other = impact['uncovered_in_already_recognized_territories']
    lines += ['', 'Top 5/10/20/36 son **techos condicionales**: suponen resolver todos los perfiles acreditados de esos territorios. '
              'No son una promesa de APPLY. Los candidatos sin pipeline no se cuentan como seguros.', '',
              f"El resto de la deuda de cobertura está en los **571 territorios ya reconocidos**: {other['process_physical_rows']:,} filas físicas / "
              f"**{other['unique_processes']:,} procesos únicos**, {other['contract_physical_rows']:,} filas físicas / **{other['unique_contracts']:,} contratos únicos**. "
              'La cobertura nominal se obtiene con al menos una unidad; no acredita todo su ecosistema.', '',
              'Reconciliación exacta: **323.960 + 2.513 + 13.272 = 339.745** procesos; '
              '**318.904 + 1.711 + 10.570 = 331.185** contratos. '
              'La CAS de San Gil aporta 1.602 procesos y 1.495 contratos de `ACCREDITED_OTHER_AFFILIATION`: '
              'queda en contexto. Filtrar sólo por `municipality` la incluiría erróneamente y daría 4.115/3.206.', '',
              'Los agregados capturados no contienen valor contractual ni listas de IDs. Se informa valor `null`, no cero ni estimado. '
              'La suma de DISTINCT por estrato está habilitada por la reconciliación global M0; en contratos se acreditó igualdad '
              'entre filas físicas y IDs distintos. Se mantienen ambas métricas y no se extrapola esa igualdad a otras ventanas.', '',
              '## Ranking completo y ejecutivo frente a ecosistema', '',
              'Orden: procesos únicos descendentes, contratos únicos descendentes y DIVIPOLA como desempate. '
              'El score adicional es la suma de ambos conteos, sin valor monetario. '
              '`HAS_EXECUTIVE` indica existencia institucional por SIGEP/autoridad acotada; no prueba actividad reciente de una alcaldía. '
              '`OTHER` cuenta cuentas no ejecutivas acreditadas como aproximación explícita, no como censo lógico de BuyerUnits.', '',
              '| # | DIVIPOLA | Municipio / departamento | Proc. filas / únicos | Contr. filas / únicos | HAS_EXECUTIVE | Ejecutivo reconocido | OTHER |',
              '|---:|---|---|---:|---:|---|---|---:|']
    by_code = {t['divipola']: t for t in territories}
    for r in impact['rows']:
        t = by_code[r['divipola']]
        lines.append(f"| {r['rank']} | {r['divipola']} | {r['municipality']} / {r['department']} | "
                     f"{r['process_physical_rows']} / {r['unique_processes']} | {r['contract_physical_rows']} / {r['unique_contracts']} | "
                     f"{t['HAS_EXECUTIVE']} | {t['EXECUTIVE_ALREADY_RECOGNIZED']} | {t['OTHER_MUNICIPAL_BUYER_UNITS']} |")
    lines += ['', '## Los 39 perfiles que explican exactamente los 36 territorios', '',
              'La naturaleza jurídica SIGEP es la primera autoridad institucional. Los establecimientos públicos se refinan con su nombre oficial. '
              'Las clasificaciones basadas sólo en nombre quedan identificadas como provisionales en el inventario de contexto. '
              'La deuda principal usa precedencia explícita: conflicto de fuente, variante de fuente, falta de dataset ejecutivo, '
              'NIT compartido entre instituciones distintas, unidad no ejecutiva y demás casos. Los demás impedimentos siguen como flags.', '',
              '| DIVIPOLA | Cuenta | Nombre físico | Familia | Deuda | Proc. filas / únicos | Contr. filas / únicos | Disposición |',
              '|---|---|---|---|---|---:|---:|---|']
    for t in territories:
        for pid in t['residual_profile_ids']:
            p = by_id[pid]
            v = p['uncovered_cohort_volumes']
            lines.append(f"| {t['divipola']} | {p['account']} | {esc(p['physical_source_name'])} | {p['institutional_family']} | {p['debt']} | "
                         f"{v['process_physical_rows']} / {v['unique_processes']} | {v['contract_physical_rows']} / {v['unique_contracts']} | {candidates[pid]['disposition']} |")
    lines += ['', '## Decisiones de identidad y límites de promoción', '',
              '- **Angelópolis:** misma cuenta y dos grafías, `...ANGELOPOLIS..` en procesos y `...ANGELOPOLIS.` en contratos. '
              'El histórico observado es disjunto por dataset y cambia un signo terminal. Es candidato a adjudicar fuente/identidad; '
              'no se crean dos BuyerUnits ni se aplica automáticamente una consolidación de canonicals inexistentes.',
              '- **San Luis, San Francisco y Nariño:** las fuentes físicas tienen otros propietarios por cuenta '
              '(incluida evidencia histórica para Nariño). Un binding exacto sólo por nombre puede seleccionar otra unidad; '
              'quedan separados y sin alias o identidad automáticos.',
              '- **Ejecutivos con un solo dataset reciente:** se informa la evidencia histórica de cada dataset, incluso los conjuntos vacíos. '
              'No se confunde cero actividad reciente con SECOP I ni se oculta un contrato histórico bajo la etiqueta de ausencia total.',
              '- **Instituciones no ejecutivas:** concejos, personerías, contraloría, salud, institutos, empresas y otros establecimientos '
              'son unidades independientes. `KEEP_SEPARATE` permite proponerlas individualmente en M2; no es autorización para un grupo.', '',
              '| Disposición | Perfiles del residuo | Perfiles de contexto |', '|---|---:|---:|']
    for d in DISPOSITIONS:
        lines.append(f"| {d} | {promotions['residual_dispositions'][d]} | {promotions['context_dispositions'][d]} |")
    ready = impact['EVIDENCE_READY_PENDING_PIPELINE']
    lines += ['', f"**AUTO_SAFE = {promotions['residual_dispositions']['AUTO_SAFE']}**. Todos los ejecutivos residuales tienen conflicto de nombre, "
              'variante por dataset o evidencia reciente incompleta; no hay un lote ejecutivo equivalente a M1A. '
              f"Hay **{len(ready['profile_ids'])} perfiles con afiliación/familia/fuentes históricas exclusivas listos para evaluar pipeline**. "
              'El techo condicional de esa selección figura arriba, sin promoverla a AUTO_SAFE. No se construyó ni mutó un catálogo candidato, '
              'no se añadieron aliases ni bindings. `ALIAS_ONLY` y `DUPLICATE_BUYER_UNIT` no se declaran por similitud.', '',
              '## SECOP_DIRECTORY_EXACT_DIVIPOLA', '',
              'Se revalida el contrato M1A.2A sobre las mismas capturas: **646 filas**, **616 códigos municipales**, '
              '**616 acuerdos primarios**, **0 contradicciones**, **606 corroboraciones independientes** por municipio/departamento y '
              '**10 ambiguas**. No se cuentan las ambiguas como acuerdos.', '',
              '**San Gil (68679)** es la nueva afiliación adjudicable: canonical existente, nombre físico byte-idéntico en ambos datasets, '
              'cuenta única `700043011`, directorio exacto y departamento compatible. '
              '**Barrancabermeja (68081) y Yumbo (76892)** conservan su adjudicación congelada M1A.2A; no se vuelven a acreditar desde '
              'códigos de departamento de M0. Total: **1 nuevo caso / 3 ejecutivos existentes** en los 36.', '',
              'Esto añade **0 perfiles y 0 volumen al residuo municipal acreditado fijo**: los perfiles residuales de estos territorios '
              'son otras instituciones. Los nuevos ejecutivos físicamente observados de Caracolí, Apía y San Martín, por ejemplo, '
              'no reciben afiliación automática por esta autoridad: todavía no son unidades existentes con el contrato completo.', '',
              'Los seis controles del pipeline actual aciertan el owner existente. **Eso no equivale a pipeline seguro de promoción**: '
              'los tres carecen de binding exacto ADR-073. El primer alias exacto observado de Barrancabermeja '
              '(`alcaldía distrital del municipio de barrancabermeja`) además deja `alcaldia`/`distrital` como objeto en la consulta. '
              'Se conserva el SoQL como diagnóstico, sin modificar runtime. Por ello Barrancabermeja requiere revisión y los otros dos '
              'se señalan para binding de fuente; ninguno incrementa ALL_SAFE.', '',
              '## Señal para M2 y siguiente paso', '',
              '| Familia | Perfiles | Procesos únicos | Contratos únicos |', '|---|---:|---:|---:|']
    for f in FAMILIES:
        v = cohort['volume_by_family'][f]
        lines.append(f"| {f} | {cohort['by_family'][f]} | {v['unique_processes']} | {v['unique_contracts']} |")
    lines += ['', f"Por número de perfiles el residuo **todavía es mayoritariamente ejecutivo: {cohort['executive_pct']:.2f}%**, "
              f"frente a **{cohort['nonexecutive_pct']:.2f}%** de otras instituciones. Por volumen se invierte: los ejecutivos aportan "
              f"**{100 * executive_volume['unique_processes'] / total['unique_processes']:.2f}%** de los procesos y "
              f"**{100 * executive_volume['unique_contracts'] / total['unique_contracts']:.2f}%** de los contratos del residuo. "
              'No se afirma que el residuo sea principalmente no ejecutivo por conteo de perfiles.', '',
              '**NEXT: GO_DIRECT_TO_M2.** Priorizar instituciones no ejecutivas de Yumbo, Puerres, Barrancabermeja, Chimá, Tuluá y Paipa; '
              'adjudicar por separado la pareja de Angelópolis y los homónimos antes de plantear un M1B APPLY acotado. '
              'La mayor deuda de volumen permanece dentro de territorios nominalmente reconocidos. '
              'El denominador global de BuyerUnits municipales sigue sin adjudicar.', '',
              '## Gates, validación y reproducción', '',
              f"Se protegen **{len(g['protected_sha256'])} archivos** del baseline mediante hash: runtime, datos productivos, tests/scripts previos, "
              'autoridades y evidencia municipal histórica. `WRONG_OWNER=0` se refiere a los owners afirmados en las comprobaciones existentes, '
              'no a una garantía de toda consulta futura. `NO_NIT_MERGE`, `NO_RUNTIME_MUTATION`, `NO_GAZETTEER_MUTATION`, '
              '`NO_SOURCE_BINDING_MUTATION` y `NO_HISTORICAL_EVIDENCE_MUTATION`: **PASS**. Live calls: **0**.', '']
    validation = g.get('validation')
    if validation:
        for label, result in validation['commands'].items():
            lines.append(f"- `{label}`: **{result['status']}**. Evidencia: `artifacts/audits/municipal_m1b/validation/{result['log']}`.")
        lines.append(f"- Replay independiente: **{validation['reproducibility']['status']}**, seis artefactos iguales salvo metadata de validación; informe idéntico antes de adjuntar los logs.")
    else:
        lines.append('Validación pendiente; ejecutar el comando `--validate` de abajo antes del cierre.')
    lines += ['', 'Suite completa no requerida: sólo docs/audit/tests nuevos, sin cambios productivos. '
              '`LLM_EXECUTION_MODE=DETERMINISTIC_OFFLINE_NO_LLM`; proveedor LLM vivo no verificado.', '',
              '```bash', '.venv/bin/python3 scripts/audits/audit_municipal_m1b.py --validate',
              '# Replay en otra carpeta sin sobrescribir evidencia histórica:',
              '.venv/bin/python3 scripts/audits/audit_municipal_m1b.py --output /tmp/municipal_m1b_replay', '```', '',
              'Artefactos: `residual_territories.json`, `buyer_profiles.json`, `impact_ranking.json`, `classification.json`, '
              '`promotion_candidates.json`, `gates.json`, bajo `artifacts/audits/municipal_m1b/`. '
              'Commits exclusivamente docs/audit. **PUSH: NO.**', '']
    path = ROOT / 'docs/gazetteer/MUNICIPAL_M1B.md' if out == OUT else out / 'MUNICIPAL_M1B.md'
    path.write_text('\n'.join(lines))


def validate(out):
    if out != OUT:
        raise ValueError('Use --validate only with the default audit output; focal tests read it.')
    commands = {
        'focal_tests': [sys.executable, '-m', 'pytest', 'tests/test_municipal_m1b.py',
                        'tests/test_municipal_m0_evidence.py', 'tests/test_municipal_m1a2a.py', '-q'],
        'corpus_89': [sys.executable, 'scripts/run_query_corpus.py', '--mode', 'all'],
        'ruff': [str(ROOT / '.venv/bin/ruff'), 'check', 'app/', 'tests/', 'scripts/'],
        'lint_core': ['make', 'lint-core'],
        'diff_check': ['git', 'diff', '--check'],
    }
    results = {}
    (out / 'validation').mkdir(exist_ok=True)
    for name, command in commands.items():
        completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        output = completed.stdout + completed.stderr
        log = name + '.log'
        (out / 'validation' / log).write_text(output)
        results[name] = dict(command=command, exit_code=completed.returncode,
                             status='PASS' if completed.returncode == 0 else 'FAIL',
                             log=log, sha256=sha(output.encode()))
        print(name, results[name]['status'], flush=True)
    with tempfile.TemporaryDirectory(prefix='secoppal-m1b-replay-') as temp:
        replay = Path(temp)
        subprocess.run([sys.executable, str(Path(__file__).resolve()), '--output', str(replay)],
                       cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
        replay_checks = {}
        for filename in ('residual_territories.json', 'buyer_profiles.json', 'impact_ranking.json',
                         'classification.json', 'promotion_candidates.json', 'gates.json'):
            original, repeated = read(out / filename), read(replay / filename)
            if filename == 'gates.json':
                original.pop('validation', None)
                repeated.pop('validation', None)
            replay_checks[filename] = original == repeated
        # The original may contain still-valid logs from an earlier replay.
        # Compare report rendering against the same metadata rather than deleting it.
        save(replay / 'gates.json', read(out / 'gates.json'))
        render_report(replay)
        replay_checks['MUNICIPAL_M1B.md'] = (replay / 'MUNICIPAL_M1B.md').read_bytes() == (ROOT / 'docs/gazetteer/MUNICIPAL_M1B.md').read_bytes()
        reproducibility = dict(status='PASS' if all(replay_checks.values()) else 'FAIL',
                               comparisons=replay_checks)
        save(out / 'validation/reproducibility.json', reproducibility)
        print('reproducibility', reproducibility['status'], flush=True)
    gates = read(out / 'gates.json')
    changed = [p for p, digest in gates['protected_sha256'].items()
               if sha((ROOT / p).read_bytes()) != digest]
    if changed:
        raise ValueError('STOP: protected mutation during validation: ' + ', '.join(changed))
    gates['validation'] = dict(commands=results, reproducibility=reproducibility,
                              protected_files_unchanged_after_validation=True,
                              code_sha256={p: sha((ROOT / p).read_bytes()) for p in
                                  ('scripts/audits/audit_municipal_m1b.py', 'tests/test_municipal_m1b.py')})
    if any(r['status'] != 'PASS' for r in results.values()) or reproducibility['status'] != 'PASS':
        gates['MUNICIPAL_M1B'] = 'BLOCKED'
    save(out / 'gates.json', gates)
    render_report(out)
    if gates['MUNICIPAL_M1B'] != 'PASS':
        raise SystemExit(1)


def run(out):
    branch, head = git('branch', '--show-current'), git('rev-parse', 'HEAD')
    # Replays remain possible after a docs/audit-only commit, with production
    # and every authority pinned byte-for-byte to the user-requested HEAD.
    if branch != 'main' or git('merge-base', EXPECTED_HEAD, head) != EXPECTED_HEAD:
        raise ValueError('STOP: branch/HEAD is not main at or descended from the expected baseline')
    tree = subprocess.check_output(['git', 'ls-tree', '-r', '-z', EXPECTED_HEAD], cwd=ROOT)
    baseline_files = {entry.split(b'\t', 1)[1].decode(): entry.split(b'\t', 1)[0].split()[2].decode()
                      for entry in tree.split(b'\0') if entry}
    protected = [p for p in baseline_files if p.startswith(('app/', 'scripts/', 'tests/',
                 'artifacts/audits/municipal_', 'docs/gazetteer/MUNICIPAL_', 'docs/decisions/'))]
    hashes = {}
    for path in protected:
        current = (ROOT / path).read_bytes()
        blob = hashlib.sha1(b'blob ' + str(len(current)).encode() + b'\0' + current).hexdigest()
        if blob != baseline_files[path]:
            raise ValueError('STOP: protected baseline mutation: ' + path)
        hashes[path] = sha(current)
    m0, coverage = read(M0 / 'analysis.json.gz'), read(FINAL / 'coverage_post.json')
    source_manifest = read(M0 / 'sources/manifest.json')
    for snap in source_manifest['snapshots']:
        if sha(gzip.decompress((M0 / 'sources' / snap['file']).read_bytes())) != snap['uncompressed_sha256']:
            raise ValueError('STOP: M0 source manifest mismatch: ' + snap['file'])
    with tempfile.TemporaryDirectory(prefix='secoppal-m1b-') as temp:
        path = Path(temp) / 'post.json.gz'
        subprocess.run([sys.executable, str(ROOT / 'scripts/audits/audit_municipal_m0.py'),
                        '--output', str(path)], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
        analysis = read(path)
    if analysis != read(FINAL / 'm0_post_analysis.json.gz'):
        raise ValueError('STOP: M0 replay differs from frozen M1A final analysis')
    active, recognized, residual = derive_universe(analysis)
    for ds in DATASETS:
        if analysis['summary']['volumes'][ds][ACCREDITED] != coverage['volumes'][ds]:
            raise ValueError('STOP: coverage authority mismatch')
    directory = read(M0 / 'sources/directory.json.gz')['rows']
    sigep = read(M0 / 'sources/sigep.json.gz')
    sources = {DATASETS[0]: read(M0 / 'sources/processes.json.gz'),
               DATASETS[1]: read(M0 / 'sources/contracts.json.gz')}
    records, sidecar = read(ROOT / 'app/data/aliases_db.json'), read(ROOT / 'app/data/buyer_source_names.json')
    towns = {t['divipola']: t for t in read(ROOT / 'app/data/municipal_territories_v1.json')['territories']}
    adjudications = read(M1A2A / 'adjudication.json')['cases']
    prior_gates = read(M1A2A / 'gates.json')
    if prior_gates['new_wrong_owners'] or not prior_gates['no_nit_merge']:
        raise ValueError('STOP: M1A.2A authority gates are not clean')
    validation = validate_cohort(m0, directory, sigep)
    frozen_validation = read(M1A2A / 'directory_location_validation.json')
    if validation != frozen_validation:
        raise ValueError('STOP: directory authority revalidation changed')
    accepted = {}
    for a in adjudications:
        if a['decision'] == 'VERIFIED_MUNICIPAL_AFFILIATION' and a['expected_divipola'] in residual:
            accepted[a['canonical']] = dict(**a['affiliation'], evidence='M1A2A_FROZEN_ADJUDICATION')
    directory_checks = []
    for record in records:
        if not executive(record['official_name']):
            continue
        result = directory_affiliation(record, analysis['strata'], directory, validation)
        if set(result['codes']) & residual:
            accepted[record['official_name']] = dict(**result, evidence='M0_SNAPSHOT_REVALIDATED_IN_M1B')
            directory_checks.append(dict(canonical=record['official_name'], **result))
    ix = indexes(analysis, directory, sigep, records, sidecar, towns)
    signals = inventory_signals(analysis, ix, sources, residual, adjudications)
    profiles = make_profiles(analysis, ix, signals, residual, accepted)
    pipeline = observed_pipeline(accepted, ix, towns)
    for p in profiles:
        p['debt'] = classify_debt(p)
        p['current_owner_asserted'] = bool(p['current_affiliation'] and
            all(r['owner_check_passed'] for r in pipeline if r['canonical'] == p['current_gazetteer_owner']))
        p['current_pipeline_unwanted_filters'] = [r['unwanted_filters'] for r in pipeline
            if r['canonical'] == p['current_gazetteer_owner'] and r['unwanted_filters']]
        p['current_owner_interpretation'] = ('SCOPED_AFFILIATION_AND_CURRENT_OWNER_CHECK' if p['current_owner_asserted']
            else 'CATALOGUE_EXACT_MATCH_ONLY_NOT_AN_IDENTITY_ADJUDICATION')
    candidates = [candidate(p) for p in profiles]
    rejected = {r['code']: r for r in read(M0 / 'proposals.json')['rejected']}
    territories = make_territories(residual, towns, profiles, sigep, accepted, rejected)
    primary = [p for p in profiles if p['scope'] == 'RESIDUAL_COHORT']
    primary_candidates = [c for c in candidates if c['scope'] == 'RESIDUAL_COHORT']
    impact = rank_impact(analysis, territories, profiles, candidates)
    classification = dict(profile_definition='Exact (account, physical source name), across datasets; never NIT-deduplicated. Not a logical BuyerUnit denominator.',
        TOTAL_RESIDUAL_BUYER_PROFILES=len(primary), residual_cohort=family_summary(primary),
        context_only=family_summary([p for p in profiles if p['scope'] == 'CONTEXT_ONLY']),
        debt_counts={d: sum(p['debt'] == d for p in primary) for d in DEBTS},
        executive_gap_profiles=sum(p['institutional_family'] == 'MUNICIPAL_EXECUTIVE' and not p['current_owner_asserted'] for p in primary),
        executive_gap_accounts=len({p['account'] for p in primary if p['institutional_family'] == 'MUNICIPAL_EXECUTIVE' and not p['current_owner_asserted']}),
        rows=[dict(profile_id=p['profile_id'], scope=p['scope'], family=p['institutional_family'],
                   basis=p['classification_basis'], debt=p['debt']) for p in profiles])
    registry_only = [dict(account=a, territory_signals=signals[a], directory_evidence=[dict(index=i, row=r) for i, r in ix['directory'][a]],
                          debt='MISSING_DATASET_EVIDENCE', disposition='DO_NOT_ADD',
                          reason='Directory signal with zero rows in the frozen recent datasets; not proven noise, inactivity or SECOP I.')
                     for a in sorted(signals) if not ix['account_rows'][a]]
    changed = [p for p, digest in hashes.items() if not (ROOT / p).exists() or sha((ROOT / p).read_bytes()) != digest]
    if changed:
        raise ValueError('STOP: incidental protected mutation: ' + ', '.join(changed))
    if len({(p['account'], p['physical_source_name']) for p in profiles}) != len(profiles):
        raise ValueError('STOP: duplicate observation profiles')
    assert sum_volumes([p['uncovered_cohort_volumes'] for p in primary]) == volumes(
        [r for r in analysis['strata'] if r['municipality'] in residual and r['status'] == ACCREDITED])
    assert all(not c['safe_to_apply'] or not c['blocking_requirements'] for c in candidates)
    wrong = sum(r['wrong_owner'] for r in pipeline)
    gates = dict(MUNICIPAL_M1B='PASS' if not wrong else 'BLOCKED',
                 verdict_scope='Discovery complete; review dispositions are explicit and are not promotions.',
                 branch=branch, baseline_head=EXPECTED_HEAD, replay_head=head,
                 ACCREDITED_ACTIVE=len(active), RECOGNIZED_POST_M1A=len(recognized), RESIDUAL_TERRITORIES=len(residual),
                 WRONG_OWNER=wrong, wrong_owner_scope='Asserted existing executive owners in six current parser/plan checks; no future owner asserted.',
                 NO_NIT_MERGE='PASS', NO_RUNTIME_MUTATION='PASS', NO_GAZETTEER_MUTATION='PASS',
                 NO_SOURCE_BINDING_MUTATION='PASS', NO_HISTORICAL_EVIDENCE_MUTATION='PASS',
                 protected_sha256=hashes, protected_changed=changed,
                 source_manifest_verified=True, m0_replay_equals_m1a_final=True,
                 directory_authority=dict(authority=AUTHORITY,
                     revalidated_cohort={k: v for k, v in validation.items() if k != 'rows'},
                     newly_adjudicable_existing_units=directory_checks,
                     previously_adjudicated_residual_executives=[c for c, a in accepted.items() if a['evidence'] == 'M1A2A_FROZEN_ADJUDICATION'],
                     residual_territories_with_scoped_executive_affiliation=len({a['codes'][0] for a in accepted.values()}),
                     residual_accredited_profile_affiliations_added=0,
                     limits='Existing executive units only; no new units, nonexecutives, department-only codes, approximate names or unvalidated future rows.'),
                 existing_executive_pipeline=pipeline,
                 LLM_EXECUTION_MODE='DETERMINISTIC_OFFLINE_NO_LLM', LIVE_CALLS=0,
                 production_applied=False, push=False, NEXT='GO_DIRECT_TO_M2')
    cuts = impact['cuts']
    family_counts = classification['residual_cohort']['by_family']
    gates['RESIDUAL'] = dict(TERRITORIES=len(residual), BUYER_PROFILES=len(primary),
        EXECUTIVE_GAPS=classification['executive_gap_profiles'],
        EXECUTIVE_GAP_ACCOUNTS=classification['executive_gap_accounts'],
        NON_EXECUTIVE_PROFILES=len(primary) - classification['executive_gap_profiles'],
        REVIEW=classification['debt_counts']['REVIEW'],
        INSTITUTIONAL_REVIEW=family_counts['UNKNOWN_REVIEW'])
    gates['IMPACT'] = dict(CURRENT_PROCESS_COVERAGE=impact['current']['process_coverage_pct'],
        CURRENT_CONTRACT_COVERAGE=impact['current']['contract_coverage_pct'],
        TOP_5_PROCESS_PCT=cuts['TOP_5']['process_coverage_pct'],
        TOP_5_CONTRACT_PCT=cuts['TOP_5']['contract_coverage_pct'],
        TOP_10_PROCESS_PCT=cuts['TOP_10']['process_coverage_pct'],
        TOP_10_CONTRACT_PCT=cuts['TOP_10']['contract_coverage_pct'],
        ALL_SAFE_PROCESS_PCT=impact['ALL_SAFE']['process_coverage_pct'],
        ALL_SAFE_CONTRACT_PCT=impact['ALL_SAFE']['contract_coverage_pct'])
    gates['CANDIDATES'] = {('BINDING_ONLY' if d == 'SOURCE_BINDING_ONLY' else d):
                           sum(c['disposition'] == d for c in primary_candidates) for d in DISPOSITIONS}
    gates['M2_SIGNAL'] = dict(EXECUTIVE=family_counts['MUNICIPAL_EXECUTIVE'],
        CONCEJO=family_counts['CONCEJO'], PERSONERIA=family_counts['PERSONERIA'],
        CONTRALORIA=family_counts['CONTRALORIA'], HEALTH=family_counts['ESE_HEALTH'],
        INSTITUTE=family_counts['INSTITUTE'], PUBLIC_COMPANY=family_counts['PUBLIC_COMPANY'],
        OTHER=family_counts['OTHER_DECENTRALIZED'] + family_counts['FUND'],
        REVIEW=family_counts['UNKNOWN_REVIEW'] + family_counts['REGISTRY_NOISE'])
    gates['COMMITS'] = 'docs/audit only'
    gates['PUSH'] = 'NO'
    save(out / 'residual_territories.json', dict(
        counts=dict(ACCREDITED_ACTIVE=len(active), RECOGNIZED_POST_M1A=len(recognized), RESIDUAL=len(residual)),
        accredited_active_divipola=sorted(active), recognized_post_m1a_divipola=sorted(recognized),
        residual_divipola=sorted(residual), territories=territories))
    save(out / 'buyer_profiles.json', dict(window=source_manifest['recent_window'],
        profile_key=['account', 'physical_source_name'], profiles=profiles, registry_only_signals=registry_only,
        totals=dict(residual_profiles=len(primary), context_profiles=len(profiles) - len(primary),
                    registry_only_accounts=len(registry_only)),
        source_reference_base='M0 sources: indices are zero-based; stratum indices refer to unchanged M1A final post-analysis.',
        additive_unique_counts_basis='M0 global DISTINCT(process IDs) equals sum of stratum DISTINCT; contracts global physical rows equals DISTINCT. No raw ID lists or monetary values captured.'))
    save(out / 'classification.json', classification)
    save(out / 'promotion_candidates.json', dict(
        residual_dispositions={d: sum(c['disposition'] == d for c in primary_candidates) for d in DISPOSITIONS},
        context_dispositions={d: sum(c['disposition'] == d for c in candidates if c['scope'] == 'CONTEXT_ONLY') for d in DISPOSITIONS},
        candidates=candidates, production_applied=False,
        automatic_five_condition_merge_applied=False,
        next='GO_DIRECT_TO_M2',
        policy='KEEP_SEPARATE preserves institutional units and fiscal peers; it does not mean never add independently. Missing recent data is not SECOP I. AUTO_SAFE needs a future candidate pipeline gate; none was constructed in this discovery.'))
    save(out / 'impact_ranking.json', impact)
    prior = read(out / 'gates.json').get('validation') if (out / 'gates.json').exists() else None
    if prior and all(sha((ROOT / p).read_bytes()) == digest for p, digest in prior['code_sha256'].items()):
        gates['validation'] = prior
    save(out / 'gates.json', gates)
    render_report(out)
    print(json.dumps(dict(verdict=gates['MUNICIPAL_M1B'], counts=classification['residual_cohort'],
                          cuts=impact['cuts'], safe=impact['ALL_SAFE'],
                          dispositions=read(out / 'promotion_candidates.json')['residual_dispositions']), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUT)
    parser.add_argument('--validate', action='store_true')
    args = parser.parse_args()
    run(args.output.resolve())
    if args.validate:
        validate(args.output.resolve())
