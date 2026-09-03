"""Paired resolver benchmark on frozen baseline and deterministic M1A preview."""

from __future__ import annotations

import json
from pathlib import Path
import statistics
import sys
import tempfile
from time import perf_counter

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.entity_resolver import EntityResolver
from app.utils.money import normalize_text
from scripts.audits.promote_municipal_m1a import OUT, encode, load_inputs, read, transform


def stats(samples):
    ordered = sorted(samples)
    return {
        'p50_ms': statistics.median(ordered) * 1000,
        'p95_ms': ordered[int((len(ordered) - 1) * 0.95)] * 1000,
        'samples': len(ordered),
    }


def measure(path, exact, scan, fuzzy):
    loads = []
    for _ in range(7):
        start = perf_counter()
        resolver = EntityResolver(path)
        loads.append(perf_counter() - start)
    result = {'load': stats(loads)}
    for key, phrases, operation, rounds in [
        ('exact', exact, resolver.resolve_entidad, 30),
        ('scan', scan, resolver.scan_entity, 3),
        ('fuzzy', fuzzy, resolver.resolve_entidad, 3),
    ]:
        for phrase in phrases:
            operation(phrase)
        samples = []
        for _ in range(rounds):
            for phrase in phrases:
                start = perf_counter()
                operation(phrase)
                samples.append(perf_counter() - start)
        result[key] = stats(samples)
    return result


def main():
    before = read(OUT / 'before_aliases_db.json.gz')
    sidecar = read(OUT / 'before_buyer_source_names.json.gz')
    proposals, analysis, manifest = load_inputs()
    after, _, _, _ = transform(before, sidecar, proposals, analysis, manifest['as_of'])
    exact = [
        'sdis',
        'sena',
        'icbf',
        'alcaldia de barranquilla',
        'gobernacion de antioquia',
        'universidad nacional de colombia',
    ]
    # Use only queries which really are exact in both catalogs.
    with tempfile.TemporaryDirectory(prefix='m1a-benchmark-') as temp:
        paths = [Path(temp) / 'before.json', Path(temp) / 'after.json']
        for path, value in zip(paths, (before, after)):
            path.write_bytes(encode(value))
        resolvers = [EntityResolver(path) for path in paths]
        exact = [q for q in exact if all(r.resolve_entidad(q).method == 'exact' for r in resolvers)]
        scan = [
            normalize_text(q)
            for q in ['contratos de ' + a for a in exact]
            + [
                'contratos de la alcaldia de baranoa',
                'procesos del municipio de puerto gaitan',
                'contratos en baranoa',
                'contratos en bogota',
                'contratos de la alcaldia de magangue',
                'procesos de la alcaldia de aguazul',
            ]
        ]
        fuzzy = [
            'ministerio de justica y del derecho',
            'universidad nacionl de colombia',
            'instituto nacional de vis',
            'hospital departamental san vicente',
        ]
        result = {
            'unit': 'milliseconds',
            'method': 'Same process, 7 loads; identical query lists before/after; warmup; wall time; no network or LLM.',
            'queries': {'exact': exact, 'scan': scan, 'fuzzy': fuzzy},
            'before': measure(paths[0], exact, scan, fuzzy),
            'after': measure(paths[1], exact, scan, fuzzy),
        }
        result['p50_ratio'] = {
            k: result['after'][k]['p50_ms'] / result['before'][k]['p50_ms']
            for k in ('load', 'exact', 'scan', 'fuzzy')
        }
    (OUT / 'performance.json').write_bytes(encode(result))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
