"""Final M1A promotion driver: the CURRENT baseline is the only equality authority.

Phases are separate processes on purpose: the runtime caches the catalogue, so
post-promotion gates must observe production in a fresh interpreter.

    preflight -> dry-run -> apply -> second-apply -> gates -> coverage

The promoter's pure ``transform`` is reused byte-for-byte, but
``promote_municipal_m1a.main`` is NOT called: it reads the frozen M1A baseline
(599 records, Turbaco duplicate still present), which makes ``validate`` raise a
KeyError against the corrected tree and would overwrite historical artefacts.
"""
from __future__ import annotations

import argparse
import gzip
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.buyer_source_names import parse_registry
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from scripts.audits.municipal_m1a_final_gates import (
    M0, PRODUCTION, baranoa_acceptance, compound_identity, divipola_matrix,
    identity_canaries, source_projection,
)
from scripts.audits.promote_municipal_m1a import (
    DATASETS, encode, load_inputs, pipeline_gate, read, sha, transform, validate,
)
from scripts.audits.replay_municipal_m1a1 import cross_interaction, geography_gate
from scripts.audits.verify_municipal_m1a_retry import buyer_check

OUT = ROOT / 'artifacts/audits/municipal_m1a_final'
TRACKED = ('app', 'tests', 'scripts', 'artifacts', 'docs')
PROJECTOR_TARGETS = ('app.core.soql_builder.project_buyer_names',
                     'scripts.audits.verify_municipal_m1a_retry.project_buyer_names')


def save(name, value):
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = encode(value)
    path.write_bytes(gzip.compress(raw, mtime=0) if path.suffix == '.gz' else raw)


def production():
    records, sidecar = [read(ROOT / p) for p in PRODUCTION]
    return dict(sha256={p: sha((ROOT / p).read_bytes()) for p in PRODUCTION},
                buyer_units=len(records), aliases=sum(len(r.get('aliases', [])) for r in records),
                source_bindings=len(sidecar['bindings']))


def protected_hashes():
    files = subprocess.check_output(['git', 'ls-files', '-z', *TRACKED], cwd=ROOT)
    return {p: sha((ROOT / p).read_bytes()) for p in files.decode().split('\0')
            if p and p not in PRODUCTION and (ROOT / p).is_file()}


def alias_replay(router, proposals):
    """The 1.888 M0 aliases through the real parser, plan and exact SoQL."""
    builder = SoQLBuilder()
    rows = [buyer_check(router, builder, p, alias,
                        DATASETS[1] if alias.startswith('alcaldia') else DATASETS[0])
            for p in proposals for alias in p['aliases']]
    failures = [r for r in rows if not r['passed']]
    return dict(status='PASS' if not failures else 'BLOCKED', checks=len(rows),
                failed_queries=len(failures),
                wrong_owner=sum(bool(r['observed'] and r['observed'] != r['expected']) for r in rows),
                failures=failures, results=rows)


def preflight():
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    branch = subprocess.check_output(['git', 'branch', '--show-current'], cwd=ROOT, text=True).strip()
    dirty = subprocess.check_output(['git', 'status', '--porcelain', '-uno'], cwd=ROOT, text=True)
    if not head.startswith('67281fc') or branch != 'main' or dirty:
        raise ValueError(f'STOP: expected clean main at 67281fc; got {branch} {head} {dirty!r}')
    for name, path in zip(('aliases_db.json.gz', 'buyer_source_names.json.gz'), PRODUCTION):
        save('baseline/' + name, read(ROOT / path))
    save('preflight.json', dict(head_before=head, branch=branch, tracked_tree_clean=True,
                                baseline=production(), protected_sha256=protected_hashes(),
                                equality_authority='CURRENT_BASELINE_67281fc',
                                retry_snapshot_used_as_authority=False))
    print('PREFLIGHT: PASS;', production(), flush=True)


def _gate_battery(baseline, baseline_sources, candidate, bindings, proposals, analysis, accepted,
                  post, prefix):
    """Shared battery. ``post`` means production already carries the lot."""
    catalogs = {'baseline': (baseline, baseline_sources), 'candidate': (candidate, bindings)}
    with tempfile.TemporaryDirectory(prefix='m1a-final-') as temp:
        routers = {}
        for label, (records, _) in catalogs.items():
            path = Path(temp) / (label + '.json')
            path.write_bytes(encode(records))
            routers[label] = QueryRouter(EntityResolver(path))
        if post:
            routers['candidate'] = QueryRouter()
        ordered = [routers['baseline'], routers['candidate']]
        cross = cross_interaction(ordered, baseline, proposals)
        save(prefix + 'cross_interaction.json.gz', cross)
        print('cross:', cross['status'], cross['checks'], flush=True)
        geo = geography_gate(ordered, proposals, analysis)
        save(prefix + 'geography_gate.json.gz', geo)
        print('geography:', len(geo['checks']), 'checks;', len(geo['failures']), 'failures', flush=True)
        matrix = divipola_matrix(routers, catalogs)
        reports = matrix.pop('reports')
        for label, sub in reports.items():
            save(f'{prefix}matrix_{label}.json.gz', sub)
        save(prefix + 'divipola_matrix.json', matrix)
        print('matrix:', matrix['status'], matrix['total_queries'], matrix['totals'], flush=True)
        compound = compound_identity(dict(reports=reports))
        save(prefix + 'compound_identity.json', compound)
        registry = parse_registry(bindings)
        patches = [] if post else [patch(t, registry.project) for t in PROJECTOR_TARGETS]
        for context in patches:
            context.start()
        try:
            replay = alias_replay(routers['candidate'], proposals) if post else pipeline_gate(
                baseline, candidate, bindings, proposals)
            projection = source_projection(routers['candidate'], proposals, bindings)
            canaries = identity_canaries(routers['candidate'])
            baranoa = baranoa_acceptance(routers['candidate'], proposals)
        finally:
            for context in patches:
                context.stop()
    save(prefix + 'alias_replay.json.gz', replay)
    save(prefix + 'source_projection.json.gz', projection)
    save(prefix + 'identity_canaries.json', canaries)
    save(prefix + 'baranoa.json', baranoa)
    data = validate(baseline, baseline_sources, candidate, bindings, accepted, analysis)
    save(prefix + 'data_gates.json', data)
    failed = replay.get('failed_queries', 0)
    print('data:', data['status'], '| alias replay:', replay['status'], replay['checks'], flush=True)
    report = dict(
        data_status=data['status'], alias_queries=replay['checks'], alias_failures=failed,
        alias_wrong_owner=replay.get('wrong_owner', replay.get('new_wrong_owner_queries', 0)),
        cross_interaction={k: cross[k] for k in ('status', 'checks', 'new_wrong_owner',
                                                 'lost_correct_owner', 'new_scan_cross_owner')},
        geography_checks=len(geo['checks']), geography_failures=len(geo['failures']),
        divipola_matrix={k: v for k, v in matrix.items() if k != 'per_catalog'},
        compound_identity=compound,
        source_projection={k: v for k, v in projection.items() if k not in ('results', 'failures')},
        identity_canaries=canaries['status'], baranoa=baranoa['status'],
        new_multi_owner=len(data['new_debt']['multi_owner_alias_keys']),
        new_scan_cross_owner=len(data['new_scan_cross_owner']),
        no_nit_merge=data['existing_units_preserved'] and data['existing_bindings_preserved'])
    report['status'] = 'PASS' if (
        data['status'] == replay['status'] == cross['status'] == matrix['status'] == 'PASS'
        and compound['status'] == projection['status'] == canaries['status'] == baranoa['status'] == 'PASS'
        and not geo['failures'] and replay['checks'] == 1888 and not failed
        and matrix['totals']['REVIEW'] == 0 and matrix['totals']['WRONG_BUYER'] == 0
        and matrix['totals']['WRONG_DIVIPOLA'] == 0 and matrix['totals']['FAIL_CLOSED'] == 0
        and report['new_multi_owner'] == 0 and report['new_scan_cross_owner'] == 0
        and report['no_nit_merge']) else 'BLOCKED'
    return report


def dry_run():
    pre = read(OUT / 'preflight.json')
    proposals, analysis, manifest = load_inputs()
    baseline, baseline_sources = [read(ROOT / p) for p in PRODUCTION]
    if production()['sha256'] != pre['baseline']['sha256']:
        raise ValueError('STOP: production changed since preflight')
    candidate, bindings, accepted, rejected = transform(baseline, baseline_sources, proposals,
                                                        analysis, manifest['as_of'])
    idempotent = transform(candidate, bindings, proposals, analysis,
                           manifest['as_of']) == (candidate, bindings, accepted, rejected)
    delta = dict(input=len(proposals), accepted=len(accepted), rejected=len(rejected),
                 kinds={k: sum(p['kind'] == k for p in accepted)
                        for k in ('ADD_NEW_ACCREDITED_UNIT', 'ADD_ALIASES_TO_EXISTING_UNIT')},
                 new_buyer_units=len(candidate) - len(baseline),
                 aliases_added=sum(len(r.get('aliases', [])) for r in candidate)
                 - sum(len(r.get('aliases', [])) for r in baseline),
                 source_bindings_added=len(bindings['bindings']) - len(baseline_sources['bindings']),
                 gazetteer_before=len(baseline), gazetteer_after=len(candidate),
                 candidate_sha256={p: sha(encode(v)) for p, v in zip(PRODUCTION, (candidate, bindings))})
    report = _gate_battery(baseline, baseline_sources, candidate, bindings, proposals, analysis,
                           accepted, post=False, prefix='dry_run/')
    report.update(delta=delta, idempotent=idempotent, production_promoted=0,
                  protected_unchanged=protected_hashes() == pre['protected_sha256'])
    if not (idempotent and report['protected_unchanged'] and not rejected):
        report['status'] = 'BLOCKED'
    save('dry_run.json', report)
    print('DRY RUN:', report['status'], flush=True)
    if report['status'] != 'PASS':
        raise SystemExit(1)


def apply_promotion():
    pre, dry = read(OUT / 'preflight.json'), read(OUT / 'dry_run.json')
    if dry['status'] != 'PASS':
        raise ValueError('STOP: dry run is not PASS')
    proposals, analysis, manifest = load_inputs()
    baseline, baseline_sources = [read(ROOT / p) for p in PRODUCTION]
    candidate, bindings, _, rejected = transform(baseline, baseline_sources, proposals, analysis,
                                                 manifest['as_of'])
    payloads = [encode(candidate), encode(bindings)]
    if {p: sha(raw) for p, raw in zip(PRODUCTION, payloads)} != dry['delta']['candidate_sha256']:
        raise ValueError('STOP: candidate bytes differ from the audited dry run')
    if rejected:
        raise ValueError('STOP: rejected proposals at apply time')
    for path, raw in zip(PRODUCTION, payloads):
        (ROOT / path).write_bytes(raw)
    changed = [p for p, digest in pre['protected_sha256'].items()
               if sha((ROOT / p).read_bytes()) != digest]
    current = production()
    result = dict(production=current, applied=len(proposals), protected_files_changed=changed,
                  incremental_delta={k: current[k] - pre['baseline'][k]
                                     for k in ('buyer_units', 'aliases', 'source_bindings')})
    save('first_apply.json', result)
    if changed:
        raise ValueError('STOP: protected files changed during apply')
    print('APPLY:', result['incremental_delta'], flush=True)


def second_apply():
    first = read(OUT / 'first_apply.json')
    proposals, analysis, manifest = load_inputs()
    records, sidecar = [read(ROOT / p) for p in PRODUCTION]
    candidate, bindings, accepted, rejected = transform(records, sidecar, proposals, analysis,
                                                        manifest['as_of'])
    payloads = [encode(candidate), encode(bindings)]
    identical = all((ROOT / p).read_bytes() == raw for p, raw in zip(PRODUCTION, payloads))
    for path, raw in zip(PRODUCTION, payloads):
        (ROOT / path).write_bytes(raw)
    current = production()
    delta = {k: current[k] - first['production'][k] for k in ('buyer_units', 'aliases', 'source_bindings')}
    result = dict(production=current, production_delta=delta, reapplied=len(accepted),
                  rejected=len(rejected),
                  bytes_identical=identical and current['sha256'] == first['production']['sha256'])
    save('second_apply.json', result)
    if any(delta.values()) or not result['bytes_identical']:
        raise ValueError('STOP: second apply changed production')
    print('SECOND APPLY: PRODUCTION_DELTA = 0; bytes identical', flush=True)


def post_gates():
    proposals, analysis, _ = load_inputs()
    baseline = read(OUT / 'baseline/aliases_db.json.gz')
    baseline_sources = read(OUT / 'baseline/buyer_source_names.json.gz')
    records, sidecar = [read(ROOT / p) for p in PRODUCTION]
    report = _gate_battery(baseline, baseline_sources, records, sidecar, proposals, analysis,
                           proposals, post=True, prefix='post/')
    report.update(production=production(), catalog_under_test=list(PRODUCTION),
                  temporary_candidate_or_projector_patch=False)
    save('post_gates.json', report)
    print('POST GATES:', report['status'], flush=True)
    if report['status'] != 'PASS':
        raise SystemExit(1)


def coverage():
    output = OUT / 'm0_post_analysis.json.gz'
    subprocess.run([sys.executable, 'scripts/audits/audit_municipal_m0.py', '--output', str(output)],
                   cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
    replay, before = read(output), read(M0 / 'analysis.json.gz')
    proposals, _, _ = load_inputs()
    summary = replay['summary']
    n = summary['recognized_municipal_territories']
    active = summary['accredited_active_municipal_territories']
    divipola = summary['divipola_municipalities']
    volumes = {ds: summary['volumes'][ds]['ACCREDITED_MUNICIPAL_AFFILIATION'] for ds in DATASETS}
    base = {r['municipality'] for r in before['strata'] if r['covered']}
    new = {r['municipality'] for r in replay['strata'] if r['covered']}
    baranoa = next(p for p in proposals if p['code'] == '08078')
    reached = {ds: [r for r in replay['strata'] if r['dataset'] == ds
                    and r['name'] in baranoa['source_names'][ds]] for ds in DATASETS}
    result = dict(status='REPRODUCED_OFFLINE_PRODUCTION', accredited_active_territories=active,
                  recognized_territories=n, coverage_in_accredited_active_cohort_pct=100 * n / active,
                  divipola_denominator=divipola, coverage_over_divipola_pct=100 * n / divipola,
                  unique_volume_coverage_pct={ds: 100 * v['covered_unique_ids'] / v['unique_ids']
                                              for ds, v in volumes.items()}, volumes=volumes,
                  recognized_buyer_units=summary['accredited_recognized_buyer_units'],
                  added_territories=len(new - base), lost_territories=sorted(base - new),
                  proposal_overlap_with_previously_covered=sorted({p['code'] for p in proposals} & base),
                  disputed_source_claims=summary['disputed_source_claims'],
                  baranoa_parity={ds: dict(unique_ids=sum(r['unique_ids'] for r in rows),
                                           covered_unique_ids=sum(r['unique_ids'] for r in rows
                                                                  if r['covered']),
                                           expected=baranoa['volumes'][ds]['unique_ids'])
                                  for ds, rows in reached.items()},
                  global_municipal_buyer_unit_denominator='UNADJUDICATED_M2',
                  source_mode='M0 frozen snapshots, not live Socrata')
    save('coverage_post.json', result)
    print(encode(result).decode(), flush=True)
    if result['lost_territories'] or any(v['covered_unique_ids'] != v['expected']
                                         for v in result['baranoa_parity'].values()):
        raise SystemExit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('preflight', 'dry-run', 'apply', 'second-apply',
                                          'gates', 'coverage'))
    {'preflight': preflight, 'dry-run': dry_run, 'apply': apply_promotion,
     'second-apply': second_apply, 'gates': post_gates,
     'coverage': coverage}[parser.parse_args().phase]()
