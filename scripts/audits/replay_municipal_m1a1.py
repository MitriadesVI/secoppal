"""Offline joint M1A.1 replay. Never writes production BuyerUnits or M0 proposals."""
from __future__ import annotations

from collections import Counter
import gzip
import json
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.entity_resolver import EntityResolver
from app.core.entity_types import rewrite_entity
from app.core.municipal_territory import get_municipal_territories
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.utils.money import normalize_text
from scripts.audits.promote_municipal_m1a import (
    OUT as M1A, encode, load_inputs, pipeline_gate, read, sha, transform, validate,
)

OUT = ROOT / 'artifacts/audits/municipal_m1a_1'


def buyer_query(alias):
    return f'contratos de la {alias}' if alias.startswith('alcaldia') else f'procesos del {alias}'


def geography_gate(routers, proposals, analysis):
    registry = get_municipal_territories()
    covered = {r['municipality'] for r in analysis['strata']
               if r['status'] == 'ACCREDITED_MUNICIPAL_AFFILIATION' and r['covered']}
    additional = {p['code'] for p in proposals} - covered
    assert len(additional) == 501
    rows = []
    builder = SoQLBuilder()
    for t in registry.by_code.values():
        for qualified in (False, True):
            query = f'contratos en {t.municipality}' + (
                f', departamento {t.department}' if qualified else '')
            params = routers[0].parse(query).params
            candidate_params = routers[1].parse(query).params
            plan = compute_query_plan(params, query)
            resolution = params.get('municipal_territory', {})
            classification = 'FAIL'
            same = params == candidate_params
            if (same and plan.should_execute and resolution.get('divipola') == t.divipola
                    and not params.get('entidad_resolved') and not params.get('objeto')
                    and not params.get('ordering_signal')):
                classification = 'PASS_EXACT_TERRITORY'
                # Both source datasets use city + department, never buyer scope.
                for ds in ('p6dx-8zbt', 'jbjy-vk9h'):
                    soql = builder.build_count(ds, params)
                    assert 'LIKE' not in soql and t.department_value in soql
            elif (same and not qualified and not plan.should_execute
                  and resolution.get('status') == 'ambiguous'
                  and t.divipola in [c['divipola'] for c in resolution['candidates']]):
                classification = 'AMBIGUOUS_REQUIRES_DEPARTMENT'
            rows.append(dict(code=t.divipola, query=query, additional=t.divipola in additional,
                             qualified=qualified, classification=classification,
                             production_candidate_identical=same, params=params,
                             should_execute=plan.should_execute,
                             clarification_question=plan.clarification_question))
    def summary(rows):
        return dict(Counter(r['classification'] for r in rows))
    return dict(territories=len(registry.by_code), distinct_names=len(registry.by_alias),
                unique_names=sum(len(v) == 1 for v in registry.by_alias.values()),
                ambiguous_names=sum(len(v) > 1 for v in registry.by_alias.values()),
                all_bare=summary([r for r in rows if not r['qualified']]),
                all_qualified=summary([r for r in rows if r['qualified']]),
                additional_territories=len(additional),
                additional_bare=summary([r for r in rows if r['additional'] and not r['qualified']]),
                additional_qualified=summary([r for r in rows if r['additional'] and r['qualified']]),
                failures=[r for r in rows if r['classification'] == 'FAIL'], checks=rows)


def cross_interaction(routers, records, proposals):
    before, after = [r.entity_resolver for r in routers]
    failures, scan_failures, checks = [], [], []
    towns = sorted({normalize_text(p['town']) for p in proposals})
    prefix_suffix_pairs = [(a, b) for a in towns for b in towns
                           if a != b and (b.startswith(a) or b.endswith(a))]
    queries = {(buyer_query(a), p['canonical'], 'proposal')
               for p in proposals for a in p['aliases']}
    # Every existing canonical, plus nearby owned aliases (at most three per
    # owner). Baseline debt is recorded, never converted into a passing owner.
    nearby = []
    for record in records:
        owner = record['official_name']
        queries.add(('contratos de ' + owner, owner, 'existing_canonical'))
        aliases = sorted(a for a in record.get('aliases', [])
                         if any(town in normalize_text(a) for town in towns))[:3]
        for alias in aliases:
            queries.add(('contratos de ' + alias, owner, 'existing_nearby_alias'))
            nearby.append(alias)
    for query, expected, category in sorted(queries):
        params = routers[1].parse(query).params
        observed = params.get('entidad_resolved')
        baseline = None
        if category != 'proposal' or observed != expected:
            baseline = routers[0].parse(query).params.get('entidad_resolved')
        wrong = bool(observed and observed != expected and observed != baseline)
        lost = bool(category != 'proposal' and baseline == expected and observed != expected)
        semantics = bool(category == 'proposal' and (
            params.get('objeto') or params.get('ordering_signal') or params.get('municipal_territory')))
        row = dict(query=query, expected=expected, category=category,
                   baseline_owner=baseline, candidate_owner=observed,
                   new_wrong_owner=wrong, lost_correct_owner=lost, contaminated_buyer=semantics)
        checks.append(row)
        if wrong or lost or semantics:
            failures.append(row)
        if category == 'proposal':
            scan = after.scan_entity(normalize_text(query))
            if scan and scan.official_name != expected:
                old = before.scan_entity(normalize_text(query))
                if not old or old.official_name != scan.official_name:
                    scan_failures.append(dict(query=query, expected=expected,
                                              candidate_scan_owner=scan.official_name))
    return dict(status='PASS' if not failures and not scan_failures else 'FAIL',
                jointly_loaded_proposals=len(proposals), checks=len(checks),
                categories=dict(Counter(c['category'] for c in checks)),
                prefix_suffix_pairs=prefix_suffix_pairs, existing_nearby_aliases=nearby,
                new_wrong_owner=sum(c['new_wrong_owner'] for c in checks),
                lost_correct_owner=sum(c['lost_correct_owner'] for c in checks),
                new_scan_cross_owner=len(scan_failures), scan_failures=scan_failures,
                failures=failures, results=checks)


def main():
    proposals, analysis, source_manifest = load_inputs()
    before = read(M1A / 'before_aliases_db.json.gz')
    sidecar = read(M1A / 'before_buyer_source_names.json.gz')
    paths = [ROOT / 'app/data/aliases_db.json', ROOT / 'app/data/buyer_source_names.json',
             ROOT / 'artifacts/audits/municipal_m0_v1/proposals.json']
    hashes = {str(p.relative_to(ROOT)): sha(p.read_bytes()) for p in paths}
    records, sources, accepted, rejected = transform(before, sidecar, proposals, analysis, source_manifest['as_of'])
    assert not rejected and len(accepted) == 503
    assert transform(records, sources, proposals, analysis, source_manifest['as_of']) == (records, sources, accepted, rejected)
    data = validate(before, sidecar, records, sources, accepted, analysis)
    pipeline = pipeline_gate(before, records, sources, proposals)
    with tempfile.TemporaryDirectory(prefix='m1a1-replay-') as temp:
        catalog_paths = [Path(temp) / 'baseline.json', Path(temp) / 'candidate.json']
        for path, catalog in zip(catalog_paths, (before, records)):
            path.write_bytes(encode(catalog))
        routers = [QueryRouter(EntityResolver(p)) for p in catalog_paths]
        geo = geography_gate(routers, proposals, analysis)
        cross = cross_interaction(routers, before, proposals)
    capture = [dict(**row, after_span=rewrite_entity(row['query'])[0].consumed_span)
               for row in read(OUT / 'capture_before.json')]
    for name, report in [('data_gates', data), ('pipeline_gate', pipeline),
                         ('geography_gate', geo), ('cross_interaction', cross), ('capture_after', capture)]:
        compressed = name in {'geography_gate', 'cross_interaction'}
        path = OUT / (name + ('.json.gz' if compressed else '.json'))
        path.write_bytes(gzip.compress(encode(report), mtime=0) if compressed else encode(report))
    assert hashes == {str(p.relative_to(ROOT)): sha(p.read_bytes()) for p in paths}
    report = dict(proposals=503, new_units=498, alias_extensions=5, aliases=1888,
                  idempotence='PASS', production_promoted=0, unchanged_sha256=hashes,
                  data_status=data['status'], pipeline_status=pipeline['status'],
                  pipeline_failures=pipeline['failed_queries'], wrong_owners=pipeline['new_wrong_owner_queries'],
                  new_multi_owner_keys=len(data['new_debt']['multi_owner_alias_keys']),
                  geography={k: v for k, v in geo.items() if k not in {'checks', 'failures'}},
                  geography_failures=len(geo['failures']),
                  cross_interaction={k: v for k, v in cross.items() if k not in {
                      'results', 'prefix_suffix_pairs', 'existing_nearby_aliases', 'failures', 'scan_failures'}},
                  performance='SEE_PERFORMANCE_JSON', push=False)
    (OUT / 'replay_summary.json').write_bytes(encode(report))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if data['status'] != 'PASS' or pipeline['status'] != 'PASS' or geo['failures'] or cross['status'] != 'PASS':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
