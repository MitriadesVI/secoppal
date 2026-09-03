"""Paired absolute latency and CPU profiles for the jointly loaded M1A candidate."""
from __future__ import annotations

import cProfile
import json
from pathlib import Path
import pstats
import sys
import tempfile
from time import perf_counter

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.deadline import TURN_DEADLINE_SECONDS
from app.core.entity_resolver import EntityResolver
from app.core.municipal_territory import MunicipalTerritoryRegistry
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from scripts.audits.benchmark_municipal_m1a import measure, stats
from scripts.audits.promote_municipal_m1a import OUT as M1A, encode, load_inputs, read, transform

OUT = ROOT / 'artifacts/audits/municipal_m1a_1'


def profile(operation):
    profiler = cProfile.Profile()
    profiler.runcall(operation)
    rows = []
    for (file, line, name), (primitive, calls, self_time, cumulative, _) in pstats.Stats(profiler).stats.items():
        rows.append(dict(file=file.replace(str(ROOT), '.'), line=line, function=name,
                         primitive_calls=primitive, calls=calls, self_ms=1000 * self_time,
                         cumulative_ms=1000 * cumulative))
    return sorted(rows, key=lambda r: r['cumulative_ms'], reverse=True)[:25]


def pipeline(router, query):
    return compute_query_plan(router.parse(query).params, query)


def main():
    proposals, analysis, manifest = load_inputs()
    before = read(M1A / 'before_aliases_db.json.gz')
    sidecar = read(M1A / 'before_buyer_source_names.json.gz')
    after, _, _, _ = transform(before, sidecar, proposals, analysis, manifest['as_of'])
    # Same resolver phrase lists and sample sizes as the blocked M1A measurement.
    queries = read(M1A / 'performance.json')['queries']
    pipeline_queries = [
        'contratos de la alcaldia de susacon', 'procesos del municipio de nemocon',
        'contratos de la alcaldia de nuevo colon', 'contratos en baranoa',
        'contratos en la union', 'contratos en la union, departamento valle del cauca',
        'contratos nuevos de la alcaldia de baranoa', 'contratos de la universidad nacional de colombia',
    ]
    loads = []
    for _ in range(15):
        start = perf_counter()
        registry = MunicipalTerritoryRegistry()
        loads.append(perf_counter() - start)
    geo_samples = []
    for t in registry.by_code.values():
        start = perf_counter()
        assert registry.lookup(t.municipality, t.department) == (t,)
        geo_samples.append(perf_counter() - start)
    result = dict(unit='milliseconds', method='Paired same process, same M1A lists and rounds; warmup; no network/LLM.',
                  municipal_registry_load=stats(loads), municipal_exact_lookup=stats(geo_samples),
                  queries={**queries, 'pipeline': pipeline_queries}, profiles={})
    with tempfile.TemporaryDirectory(prefix='m1a1-bench-') as temp:
        for label, records in [('baseline', before), ('candidate', after)]:
            path = Path(temp) / (label + '.json')
            path.write_bytes(encode(records))
            result[label] = measure(path, queries['exact'], queries['scan'], queries['fuzzy'])
            resolver = EntityResolver(path)
            router = QueryRouter(resolver)
            for query in pipeline_queries:
                pipeline(router, query)
            samples = []
            for _ in range(7):
                for query in pipeline_queries:
                    start = perf_counter()
                    pipeline(router, query)
                    samples.append(perf_counter() - start)
            result[label]['pipeline'] = stats(samples)
            result['profiles'][label] = dict(
                load_and_alias_scan_construction=profile(lambda: EntityResolver(path)),
                scan_index_construction=profile(lambda: resolver._build_scan_index([
                    edge for entries in resolver._scan_index.values() for edge in entries
                ])),
                scan_entity=profile(lambda: [resolver.scan_entity(q) for q in queries['scan']]),
                fuzzy_candidate_selection=profile(lambda: [resolver.resolve_entidad(q) for q in queries['fuzzy']]),
                fuzzy_choice_list=profile(lambda: list(resolver.choice_to_official)),
            )
    result['product_turn_budget_ms'] = TURN_DEADLINE_SECONDS * 1000
    result['pipeline_p95_budget_share_pct'] = 100 * result['candidate']['pipeline']['p95_ms'] / result['product_turn_budget_ms']
    # Report measured facts for review; correctness gates do not consult this
    # percentage or silently convert a relative change into a promotion veto.
    result['verdict'] = 'REQUIRES_REVIEW'
    (OUT / 'performance.json').write_bytes(encode(result))
    print(json.dumps({k: v for k, v in result.items() if k not in {'profiles', 'queries'}}, indent=2))


if __name__ == '__main__':
    main()
