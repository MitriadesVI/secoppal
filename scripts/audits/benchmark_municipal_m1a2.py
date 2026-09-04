"""Paired unchanged-catalog pipeline timings; run pre/post without other gates."""
import argparse
from pathlib import Path
import sys
import tempfile
from time import perf_counter

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.entity_resolver import EntityResolver
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from scripts.audits.benchmark_municipal_m1a import stats
from scripts.audits.gate_divipola_buyer_matrix import OUT, SNAPSHOT, save
from scripts.audits.promote_municipal_m1a import encode, read


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['pre', 'post'])
    phase = parser.parse_args().phase
    queries = [
        'contratos del municipio de San Andrés de Sotavento',
        'municipio de Villa de Leyva', 'alcaldía de San Pedro',
        'municipio de San Pedro, Valle del Cauca',
        'gobernación de Antioquia de 2025',
        'alcaldía de Medellín de obras de infraestructura',
        'contratos en Baranoa', 'contratos de la universidad nacional de colombia',
    ]
    report = dict(queries=queries, rounds=9, warmup=True, unit='milliseconds')
    with tempfile.TemporaryDirectory(prefix='m1a2-perf-') as temp:
        for label, source in [('baseline', ROOT / 'app/data/aliases_db.json'),
                              ('candidate', SNAPSHOT / 'aliases_db.json.gz')]:
            path = Path(temp) / 'catalog.json'
            path.write_bytes(encode(read(source)))
            router = QueryRouter(EntityResolver(path))
            for q in queries:
                compute_query_plan(router.parse(q).params, q)
            samples = []
            for _ in range(9):
                for q in queries:
                    start = perf_counter()
                    compute_query_plan(router.parse(q).params, q)
                    samples.append(perf_counter() - start)
            report[label] = stats(samples)
    if phase == 'post':
        before = read(OUT / 'performance_pre.json')
        report['delta_ms'] = {label: {p: report[label][p] - before[label][p]
                                      for p in ('p50_ms', 'p95_ms')}
                              for label in ('baseline', 'candidate')}
    save(f'performance_{phase}.json', report)
    print(report)


if __name__ == '__main__':
    main()
