#!/usr/bin/env python3
"""Reproduce independent baseline/oracle from the exact frozen checkout.

This script was executed by the orchestrator before runtime implementation.
CLI paths replace that run's fixed /tmp paths; algorithm and SQL unchanged.
The archived measurements, including the budget contract, must not be repinned.
"""
from __future__ import annotations

import argparse
import subprocess
import collections
import hashlib
import itertools
import json
import math
from pathlib import Path
import sqlite3
import sys
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--baseline-root", required=True, type=Path)
parser.add_argument("--index-path", required=True, type=Path)
parser.add_argument("--output-dir", required=True, type=Path)
args = parser.parse_args()
ROOT = args.baseline_root
OUT = args.output_dir
assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip() == "345804cf55becaf728d3ec9c20dc9e2f76a3a5b9"
OUT.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(ROOT))
from app.data.actor_alias_resolver import ActorNameResolver

INDEX = args.index_path
SQL = (
    'SELECT normalized_alias,raw_alias,raw_aliases_json,document_type,'
    'base_number,occurrences,first_seen,last_seen '
    'FROM actor_aliases INDEXED BY sqlite_autoindex_actor_aliases_1 '
    "WHERE normalized_alias >= ? AND normalized_alias < ? AND document_type = 'CC' "
    'AND base_number != ? ORDER BY normalized_alias,document_type,base_number'
)

def summary(values):
    v = sorted(values)
    return {'n': len(v), 'p50_ms': v[math.ceil(len(v) * .5) - 1],
            'p95_ms': v[math.ceil(len(v) * .95) - 1], 'max_ms': v[-1]}

def token_bucket(q):
    return str(min(len(q.split()), 4))

def digest(q):
    return hashlib.sha256(q.encode()).hexdigest()

def enumerate_rows(c, args):
    seen = set()
    rows = []
    cur = c.execute(SQL, args)
    try:
        for row in cur:
            k = row['base_number']
            if k not in seen:
                seen.add(k)
                rows.append(row)
                if len(rows) == 10:
                    break
    finally:
        cur.close()
    return rows

def main():
    start = time.perf_counter()
    stat_before = INDEX.stat()
    c = sqlite3.connect(INDEX.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA query_only=ON')
    eligible = {}
    for alias, group in itertools.groupby(c.execute(
        'SELECT normalized_alias,document_type,base_number FROM actor_aliases '
        'INDEXED BY sqlite_autoindex_actor_aliases_1 ORDER BY normalized_alias,document_type,base_number'
    ), lambda r: r[0]):
        group = list(group)
        if len(group) == 1 and group[0][1] == 'CC':
            eligible[alias] = group[0][2]
    print(json.dumps({'stage': 'eligible', 'count': len(eligible)}), flush=True)
    found = {}
    cc_rows = 0
    for row in c.execute("SELECT normalized_alias,base_number FROM actor_aliases WHERE document_type='CC'"):
        cc_rows += 1
        alias, number = row
        for pos, ch in enumerate(alias):
            if ch != ' ':
                continue
            prefix = alias[:pos]
            exact = eligible.get(prefix)
            if exact is not None and exact != number:
                keys = found.setdefault(prefix, set())
                if len(keys) < 10:
                    keys.add(number)
    counts = {k: collections.Counter() for k in ('1', '2', '3', '4')}
    groups = collections.defaultdict(list)
    for q in eligible:
        b = token_bucket(q)
        counts[b]['eligible'] += 1
        n = len(found.get(q, ()))
        bucket = 'absent' if n == 0 else 'one' if n == 1 else 'two_to_nine' if n < 10 else 'ten_plus'
        counts[b][bucket] += 1
        groups[(b, bucket)].append(q)
    audit = {
        'method': 'Independent full traversal; proper token prefixes intersect globally unique CC aliases; distinct challenger keys capped at 10 only for 1/2-9/10+ classification',
        'eligible_exact_unique_cc_aliases': len(eligible),
        'affected_found': len(found), 'unaffected_absent_confirmed': len(eligible) - len(found),
        'inconclusive_checks': 0, 'cc_alias_rows_traversed': cc_rows,
        'by_token_count': {k: dict(v) for k, v in counts.items()},
        'found_fraction': len(found) / len(eligible),
        'elapsed_seconds': time.perf_counter() - start,
    }
    (OUT / 'impact_oracle.json').write_text(json.dumps(audit, indent=2) + '\n')
    print(json.dumps({'stage': 'impact', **audit}), flush=True)
    samples = []
    for (shape, bucket), names in sorted(groups.items()):
        for q in sorted(names, key=digest)[:10]:
            samples.append({'query': q, 'shape': shape, 'challengers_bucket': bucket})
    for q in ('jose david cantillo', 'jose david cantillo arcon', 'jose david cantillo solano'):
        if q in eligible and not any(s['query'] == q for s in samples):
            samples.append({'query': q, 'shape': token_bucket(q), 'challengers_bucket': 'case_cantillo'})
    (OUT / 'benchmark_queries.json').write_text(json.dumps(samples, indent=2, ensure_ascii=False) + '\n')
    resolver = ActorNameResolver(INDEX)
    results = []
    for sample in samples:
        q = sample['query']
        args = (q + ' ', q + '!', eligible[q])
        timers = {'resolver_before': [], 'existence': [], 'enumeration': []}
        for iteration in range(55):
            t = time.perf_counter_ns()
            result = resolver.resolve_exact(q)
            elapsed = (time.perf_counter_ns() - t) / 1e6
            assert result.status == 'RESOLVED'
            if iteration >= 5:
                timers['resolver_before'].append(elapsed)
            t = time.perf_counter_ns()
            witness = c.execute(SQL + ' LIMIT 1', args).fetchone()
            elapsed = (time.perf_counter_ns() - t) / 1e6
            assert bool(witness) == (q in found)
            if iteration >= 5:
                timers['existence'].append(elapsed)
            if witness:
                t = time.perf_counter_ns()
                rows = enumerate_rows(c, args)
                elapsed = (time.perf_counter_ns() - t) / 1e6
                assert len(rows) == len(found[q])
                if iteration >= 5:
                    timers['enumeration'].append(elapsed)
        results.append({
            'query_sha256': digest(q), 'shape': sample['shape'],
            'challengers_bucket': sample['challengers_bucket'],
            **{key: summary(values) for key, values in timers.items() if values},
        })
    plans = {key: [r[3] for r in c.execute('EXPLAIN QUERY PLAN ' + SQL + suffix,
               ('example person name ', 'example person name!', '00000000'))]
             for key, suffix in [('existence', ' LIMIT 1'), ('enumeration', '')]}
    benchmark = {
        'base': '345804cf55becaf728d3ec9c20dc9e2f76a3a5b9',
        'sqlite_version': sqlite3.sqlite_version, 'sql': SQL,
        'plans': plans, 'samples': len(samples), 'warmups': 5, 'repetitions': 50,
        'cache': 'warm, after full local index traversal; no claim of cold-cache bound',
        'results': results,
    }
    (OUT / 'baseline_benchmark.json').write_text(json.dumps(benchmark, indent=2) + '\n')
    with INDEX.open('rb') as f:
        sha = hashlib.file_digest(f, 'sha256').hexdigest()
    st = INDEX.stat()
    manifest = {'index_path': str(INDEX), 'sha256': sha, 'size': st.st_size,
                'mtime_ns': st.st_mtime_ns,
                'unchanged_stat': (st.st_size, st.st_mtime_ns) == (stat_before.st_size, stat_before.st_mtime_ns),
                'encoding': c.execute('PRAGMA encoding').fetchone()[0],
                'metadata': dict(c.execute('SELECT * FROM actor_index_metadata').fetchone()),
                'external_calls': 0, 'readonly': True}
    (OUT / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    for metric in ('resolver_before', 'existence', 'enumeration'):
        available = [r[metric] for r in results if metric in r]
        print(json.dumps({'stage': metric, 'maximum_per_query_p95_ms': max(r['p95_ms'] for r in available),
                          'maximum_ms': max(r['max_ms'] for r in available)}), flush=True)
    print(json.dumps({'stage': 'done', 'samples': len(samples), 'plans': plans,
                      'index_unchanged': manifest['unchanged_stat'], 'seconds': time.perf_counter() - start}), flush=True)

if __name__ == '__main__':
    main()
