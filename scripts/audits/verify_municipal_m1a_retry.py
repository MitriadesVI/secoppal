"""Read-only production verification for the M1A retry; no candidate injection."""
from __future__ import annotations

import argparse
import gzip
from pathlib import Path
import subprocess
import sys
import tempfile
from time import perf_counter

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.buyer_source_names import project_buyer_names
from app.core.entity_resolver import EntityResolver
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError
from scripts.audits.benchmark_municipal_m1a import measure, stats
from scripts.audits.promote_municipal_m1a import (
    DATASETS, HEAD_BEFORE, OUT as M1A, encode, load_inputs, read, sha, validate,
)
from scripts.audits.replay_municipal_m1a1 import cross_interaction, geography_gate
from scripts.audits.verify_municipal_m1a import select_canaries

OUT = ROOT / 'artifacts/audits/municipal_m1a_retry'
PRODUCTION = ('app/data/aliases_db.json', 'app/data/buyer_source_names.json')


def save(name, value):
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = encode(value)
    path.write_bytes(gzip.compress(raw, mtime=0) if path.suffix == '.gz' else raw)


def production():
    records, sources = [read(ROOT / p) for p in PRODUCTION]
    return dict(sha256={p: sha((ROOT / p).read_bytes()) for p in PRODUCTION},
                buyer_units=len(records), aliases=sum(len(r['aliases']) for r in records),
                source_bindings=len(sources['bindings']))


def preflight():
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    branch = subprocess.check_output(['git', 'branch', '--show-current'], cwd=ROOT, text=True).strip()
    assert head.startswith('7bcad4b') and branch == 'main'
    assert not subprocess.check_output(['git', 'diff', 'HEAD', '--name-only'], cwd=ROOT)
    files = subprocess.check_output(['git', 'ls-files', '-z', 'app', 'tests',
                                     'artifacts/audits/municipal_m0_v1',
                                     'scripts/audits/promote_municipal_m1a.py'], cwd=ROOT)
    protected = {p: sha((ROOT / p).read_bytes()) for p in files.decode().split('\0')
                 if p and p not in PRODUCTION}
    metrics = read(M1A / 'preview/before_after.json')
    pipeline = read(M1A / 'preview/pipeline_gate.json')
    data = read(M1A / 'preview/safety_gates.json')
    assert metrics['status'] == data['status'] == pipeline['status'] == 'PASS'
    assert metrics['input'] == {'ADD_NEW_ACCREDITED_UNIT': 498, 'ADD_ALIASES_TO_EXISTING_UNIT': 5}
    assert metrics['rejected_at_promotion'] == 0
    assert pipeline['checks'] == 1888 and pipeline['failed_queries'] == 0
    assert pipeline['new_wrong_owner_queries'] == 0
    assert not data['new_scan_cross_owner'] and not any(data['new_debt'].values())
    assert data['existing_units_preserved'] and data['existing_bindings_preserved']
    save('preflight.json', dict(branch=branch, retry_head_before=head, baseline_head=HEAD_BEFORE,
                               tracked_tree_clean=True, production=production(),
                               protected_sha256=protected))
    for name in ('before_after', 'pipeline_gate', 'safety_gates', 'rejected', 'promotion_manifest'):
        save('dry_run/' + name + '.json', read(M1A / ('preview/' + name + '.json')))
    print('PREFLIGHT / DRY RUN: PASS', flush=True)


def write_check(second=False):
    before = read(OUT / ('first_apply.json' if second else 'preflight.json'))['production']
    current = production()
    protected = read(OUT / 'preflight.json')['protected_sha256']
    changed = [p for p, digest in protected.items() if sha((ROOT / p).read_bytes()) != digest]
    delta = {k: current[k] - before[k] for k in ('buyer_units', 'aliases', 'source_bindings')}
    identical = current['sha256'] == before['sha256']
    metrics = read(M1A / 'before_after.json')
    result = dict(production=current, incremental_delta=delta, protected_files_changed=changed,
                  production_bytes_identical=identical, promoter_metrics=metrics,
                  metric_basis='Promoter counts use frozen baseline, not current input.')
    save('second_apply.json' if second else 'first_apply.json', result)
    assert not changed and metrics['production_applied'] and metrics['status'] == 'PASS'
    if second:
        assert identical and not any(delta.values()), 'STOP: second apply changed production'
    else:
        assert delta == {'buyer_units': 498, 'aliases': 1883, 'source_bindings': 503}
        assert current['sha256'] == metrics['production_sha256']
    print(encode(result).decode(), flush=True)


def buyer_check(router, builder, p, alias, ds):
    kind = 'contratos' if ds == DATASETS[1] else 'procesos'
    query = f'{kind} de la {alias}' if alias.startswith('alcaldia') else f'{kind} del {alias}'
    params = router.parse(query).params
    plan = compute_query_plan(params, query)
    field = 'nombre_entidad' if ds == DATASETS[1] else 'entidad'
    expected = p['source_names'][ds]
    try:
        soql = builder.build_count(ds, dict(plan.params))
    except UnsafeGlobalQueryError:
        soql = 'BLOCKED: UnsafeGlobalQueryError'
    # The count query has only buyer scope, so the entire WHERE must equal
    # the accredited physical name. This rejects partial/substring fallbacks.
    predicate = field + " = '" + expected[0].replace("'", "''") + "'"
    passed = (params.get('entidad_resolved') == p['canonical'] and plan.should_execute
              and plan.params.get('entidad_resolved') == p['canonical']
              and soql.partition(' WHERE ')[2] == predicate
              and project_buyer_names(p['canonical'], ds) == tuple(expected)
              and not any(params.get(k) for k in ('objeto', 'ordering_signal', 'municipal_territory')))
    return dict(code=p['code'], query=query, dataset=ds, expected=p['canonical'],
                observed=plan.params.get('entidad_resolved'), passed=passed, soql=soql,
                should_execute=plan.should_execute)


def gates():
    hashes = production()['sha256']
    proposals, analysis, _ = load_inputs()
    records, sources = [read(ROOT / p) for p in PRODUCTION]
    baseline = read(M1A / 'before_aliases_db.json.gz')
    baseline_sources = read(M1A / 'before_buyer_source_names.json.gz')
    data = validate(baseline, baseline_sources, records, sources, proposals, analysis)
    save('data_gates.json', data)
    router, builder = QueryRouter(), SoQLBuilder()
    rows = [buyer_check(router, builder, p, alias,
                        DATASETS[1] if alias.startswith('alcaldia') else DATASETS[0])
            for p in proposals for alias in p['aliases']]
    failures = [r for r in rows if not r['passed']]
    pipeline = dict(status='FAIL' if failures else 'PASS', checks=len(rows),
                    wrong_owner=sum(bool(r['observed'] and r['observed'] != r['expected']) for r in rows),
                    pipeline_failure=len(failures), failures=failures, results=rows,
                    catalog_path=PRODUCTION[0], projector_path=PRODUCTION[1],
                    temporary_candidate_or_projector_patch=False)
    save('pipeline_1888.json.gz', pipeline)
    print('Production alias replay:', len(rows), 'checks;', len(failures), 'failures', flush=True)
    projection = []
    bindings = {b['canonical_name']: b for b in sources['bindings']}
    for p in proposals:
        binding = bindings.get(p['canonical'])
        checks = [buyer_check(router, builder, p, p['aliases'][0], ds) for ds in DATASETS]
        valid = bool(binding and binding['sources'] == p['source_names']
                     and binding['identity_key'] == {'type': 'BUYER_UNIT', 'value': 'municipal_m0:' + p['code']}
                     and all(c['passed'] for c in checks))
        projection.append(dict(code=p['code'], canonical=p['canonical'], passed=valid, checks=checks))
    save('source_projection.json.gz', dict(status='PASS' if all(r['passed'] for r in projection) else 'FAIL',
                                          bindings=len(projection), dataset_checks=2 * len(projection),
                                          partial_bindings= sum(not r['passed'] for r in projection),
                                          results=projection))
    sample = select_canaries(proposals)
    required = {'BARANOA', 'SABANETA', 'PUERTO GAITÁN', 'MAGANGUÉ', 'LOS PATIOS', 'TOCANCIPÁ',
                'AGUAZUL', 'IPIALES', 'SUSACÓN', 'SUSA', 'NUEVO COLÓN', 'NEMOCÓN', 'BELÉN',
                'VALPARAÍSO', 'EL CARMEN'}
    chosen = {p['code']: p for p in sample['proposals']}
    chosen.update({p['code']: p for p in proposals if p['town'] in required
                   or p['town'].startswith('EL CARMEN ') or p['town'].startswith('BELÉN ')})
    matrix = [buyer_check(router, builder, p, alias, ds)
              for p in chosen.values() for alias in p['aliases'] for ds in DATASETS]
    with tempfile.TemporaryDirectory(prefix='m1a-retry-baseline-') as temp:
        path = Path(temp) / 'baseline.json'
        path.write_bytes(encode(baseline))
        routers = [QueryRouter(EntityResolver(path)), router]
        geo = geography_gate(routers, proposals, analysis)
        save('geography_gate.json.gz', geo)
        print('Production geography:', len(geo['checks']), 'checks;', len(geo['failures']), 'failures', flush=True)
        cross = cross_interaction(routers, baseline, proposals)
        cross['production_catalog'] = PRODUCTION[0]
        cross['candidate_owner_field_means'] = 'Actual production owner in this retry.'
        save('interaction_gate.json.gz', cross)
    report = dict(status='PASS' if all(r['passed'] for r in matrix) and not geo['failures'] else 'FAIL',
                  pipeline={k: v for k, v in pipeline.items() if k != 'results'},
                  matrix_municipalities=len(chosen), matrix_buyer_checks=len(matrix),
                  selected_codes=sorted(chosen), required_names=sorted(required),
                  stratification={k: v for k, v in sample.items() if k != 'proposals'},
                  matrix=matrix, geography={k: v for k, v in geo.items() if k != 'checks'},
                  baranoa=[r for r in matrix if r['code'] == '08078'] +
                          [r for r in geo['checks'] if r['code'] == '08078'],
                  llm_execution_mode='DETERMINISTIC_OFFLINE_NO_LLM', llm_provider_live_coverage='NOT_VERIFIED')
    save('query_gates.json', report)
    assert hashes == production()['sha256']
    assert data['status'] == pipeline['status'] == cross['status'] == report['status'] == 'PASS'
    assert all(r['passed'] for r in projection)
    assert len(rows) == 1888 and cross['checks'] == 3071
    print('PRODUCTION GATES: PASS', flush=True)


def coverage():
    replay = read(OUT / 'm0_post_analysis.json.gz')
    before = read(ROOT / 'artifacts/audits/municipal_m0_v1/analysis.json.gz')
    proposals, _, _ = load_inputs()
    summary = replay['summary']
    n = summary['recognized_municipal_territories']
    active = summary['accredited_active_municipal_territories']
    divipola = summary['divipola_municipalities']
    volumes = {ds: summary['volumes'][ds]['ACCREDITED_MUNICIPAL_AFFILIATION'] for ds in DATASETS}
    base = {r['municipality'] for r in before['strata'] if r['covered']}
    new = {r['municipality'] for r in replay['strata'] if r['covered']}
    baranoa = next(p for p in proposals if p['code'] == '08078')
    parity = {}
    for ds in DATASETS:
        reached = [r for r in replay['strata'] if r['dataset'] == ds
                   and r['name'] in project_buyer_names(baranoa['canonical'], ds)]
        parity[ds] = dict(physical_rows=sum(r['physical_rows'] for r in reached),
                         unique_ids=sum(r['unique_ids'] for r in reached),
                         covered_unique_ids=sum(r['unique_ids'] for r in reached if r['covered']),
                         expected=baranoa['volumes'][ds], source_names=project_buyer_names(baranoa['canonical'], ds))
        assert parity[ds]['covered_unique_ids'] == baranoa['volumes'][ds]['unique_ids']
    result = dict(status='REPRODUCED_OFFLINE_PRODUCTION', accredited_active_territories=active,
                  recognized_territories=n, coverage_in_accredited_active_cohort_pct=100 * n / active,
                  divipola_denominator=divipola, coverage_over_divipola_pct=100 * n / divipola,
                  unique_volume_coverage_pct={ds: 100 * v['covered_unique_ids'] / v['unique_ids']
                                              for ds, v in volumes.items()}, volumes=volumes,
                  recognized_buyer_units=summary['accredited_recognized_buyer_units'],
                  added_territories=len(new - base), lost_territories=sorted(base - new),
                  proposal_overlap_with_previously_covered=sorted({p['code'] for p in proposals} & base),
                  disputed_source_claims=summary['disputed_source_claims'], baranoa_parity=parity,
                  global_municipal_buyer_unit_denominator='UNADJUDICATED_M2',
                  source_mode='M0 frozen snapshots, not live Socrata')
    save('coverage_post.json', result)
    print(encode(result).decode(), flush=True)


def benchmark():
    previous = read(ROOT / 'artifacts/audits/municipal_m1a_1/performance.json')
    queries = previous['queries']
    result = dict(unit='milliseconds', method='Same M1A.1 phrase lists/rounds and corrected runtime; warmup; no concurrent tests.',
                  queries=queries, production_sha256=production()['sha256'],
                  m1a1_candidate=previous['candidate'], verdict='REQUIRES_REVIEW')
    with tempfile.TemporaryDirectory(prefix='m1a-retry-benchmark-') as temp:
        baseline = Path(temp) / 'baseline.json'
        baseline.write_bytes(encode(read(M1A / 'before_aliases_db.json.gz')))
        for label, path in [('baseline', baseline), ('production', ROOT / PRODUCTION[0])]:
            result[label] = measure(path, queries['exact'], queries['scan'], queries['fuzzy'])
            router = QueryRouter(EntityResolver(path))
            for q in queries['pipeline']:
                compute_query_plan(router.parse(q).params, q)
            samples = []
            for _ in range(7):
                for q in queries['pipeline']:
                    start = perf_counter()
                    compute_query_plan(router.parse(q).params, q)
                    samples.append(perf_counter() - start)
            result[label]['pipeline'] = stats(samples)
    result['absolute_delta_ms'] = {op: {p: result['production'][op][p] - result['baseline'][op][p]
                                       for p in ('p50_ms', 'p95_ms')}
                                   for op in ('load', 'exact', 'scan', 'fuzzy', 'pipeline')}
    save('performance.json', result)
    print(encode({k: v for k, v in result.items() if k != 'queries'}).decode(), flush=True)


def blocking_canary():
    """Freeze the observed out-of-lot wrong owner before rolling back production."""
    proposals, _, _ = load_inputs()
    promoted = next(p for p in proposals if p['code'] == '68669')
    query = 'contratos del municipio de San Andrés de Sotavento'
    builder = SoQLBuilder()
    states = {}
    with tempfile.TemporaryDirectory(prefix='m1a-retry-blocker-') as temp:
        path = Path(temp) / 'baseline.json'
        path.write_bytes(encode(read(M1A / 'before_aliases_db.json.gz')))
        for label, router in [('baseline', QueryRouter(EntityResolver(path))),
                              ('promoted_production', QueryRouter())]:
            params = router.parse(query).params
            plan = compute_query_plan(params, query)
            try:
                soql = builder.build_count(DATASETS[1], dict(plan.params))
            except UnsafeGlobalQueryError:
                soql = 'BLOCKED: UnsafeGlobalQueryError'
            states[label] = dict(params=params, should_execute=plan.should_execute,
                                 clarification=plan.clarification_question,
                                 soql=soql)
    new_wrong_owner = (states['baseline']['params'].get('entidad_resolved') is None
                       and states['promoted_production']['params'].get('entidad_resolved')
                       == promoted['canonical'])
    result = dict(status='BLOCKED' if new_wrong_owner else 'NOT_REPRODUCED', query=query,
                  requested_territory=dict(code='23670', municipality='SAN ANDRÉS DE SOTAVENTO',
                                           department='CÓRDOBA', in_promoted_batch=False),
                  incorrectly_selected_proposal=promoted, states=states,
                  production_at_detection=production(), new_wrong_owner=new_wrong_owner,
                  cause='Exact accredited short fragment wins after full compound name is unresolved.',
                  scope_gap='The 3071-case gate covers baseline owners and M0 proposals, not all unpromoted compound municipalities.')
    save('blocking_canary.json', result)
    for p in PRODUCTION:
        save('promoted_snapshot/' + Path(p).name + '.gz', read(ROOT / p))
    for name in ('promotion_manifest', 'before_after', 'rejected', 'safety_gates', 'pipeline_gate'):
        save('promoted_snapshot/' + name + '.json.gz', read(M1A / (name + '.json')))
    print(encode(result).decode(), flush=True)
    assert new_wrong_owner, 'Blocker must be reproduced before documenting it.'


def reproduce_blocker():
    """Replay the frozen failure without applying the promotion again."""
    from unittest.mock import patch

    from app.core.buyer_source_names import parse_registry

    actual = production()
    observed = read(OUT / 'blocking_canary.json')
    query = observed['query']
    results = {}
    with tempfile.TemporaryDirectory(prefix='m1a-retry-reproduce-') as temp:
        for label, catalog, sidecar in [
            ('baseline', M1A / 'before_aliases_db.json.gz', M1A / 'before_buyer_source_names.json.gz'),
            ('promoted_snapshot', OUT / 'promoted_snapshot/aliases_db.json.gz',
             OUT / 'promoted_snapshot/buyer_source_names.json.gz'),
        ]:
            path = Path(temp) / (label + '.json')
            path.write_bytes(encode(read(catalog)))
            registry = parse_registry(read(sidecar))
            params = QueryRouter(EntityResolver(path)).parse(query).params
            plan = compute_query_plan(params, query)
            with patch('app.core.soql_builder.project_buyer_names', registry.project):
                try:
                    soql = SoQLBuilder().build_count(DATASETS[1], dict(plan.params))
                except UnsafeGlobalQueryError:
                    soql = 'BLOCKED: UnsafeGlobalQueryError'
            results[label] = dict(owner=params.get('entidad_resolved'), params=params, soql=soql)
    reproduced = (results['baseline']['owner'] is None
                  and results['promoted_snapshot']['owner']
                  == observed['incorrectly_selected_proposal']['canonical'])
    unchanged = actual == production()
    save('blocker_replay.json', dict(blocker_reproduced=reproduced,
                                   production_unchanged=unchanged, results=results))
    assert reproduced and unchanged
    print('BLOCKER REPRODUCED; production unchanged.', flush=True)


def rollback_check():
    pre = read(OUT / 'preflight.json')
    actual = production()
    changed = [p for p, digest in pre['protected_sha256'].items()
               if sha((ROOT / p).read_bytes()) != digest]
    replay = read(OUT / 'm0_rollback_analysis.json.gz')
    original = read(ROOT / 'artifacts/audits/municipal_m0_v1/analysis.json.gz')
    identical = actual == pre['production']
    result = dict(status='PASS' if identical and not changed and replay == original else 'FAIL',
                  production=actual, original_production_bytes_restored=identical,
                  protected_files_changed=changed, m0_analysis_identical=replay == original,
                  final_coverage_summary=replay['summary'], retained_promotions=0)
    save('rollback.json', result)
    assert result['status'] == 'PASS'
    print('ROLLBACK: PASS; original production and coverage restored.', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('preflight', 'first-write', 'second-write', 'gates', 'coverage',
                                         'benchmark', 'blocking-canary', 'reproduce-blocker', 'rollback-check'))
    phase = parser.parse_args().phase
    {'preflight': preflight, 'first-write': write_check, 'second-write': lambda: write_check(True),
     'gates': gates, 'coverage': coverage, 'benchmark': benchmark, 'blocking-canary': blocking_canary,
     'reproduce-blocker': reproduce_blocker, 'rollback-check': rollback_check}[phase]()
