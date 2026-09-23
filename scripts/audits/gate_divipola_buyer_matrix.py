"""Offline exhaustive buyer identity gate; DIVIPOLA is never buyer accreditation."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import gzip
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.audits.adr073_historical_projection import parse_historical_projection
from app.core.buyer_groups import group_for_params
from app.core.entity_resolver import EntityResolver
from app.core.municipal_territory import PATH, get_municipal_territories
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError
from scripts.audits.promote_municipal_m1a import encode, read, sha

OUT = ROOT / 'artifacts/audits/municipal_m1a_2'
SNAPSHOT = ROOT / 'artifacts/audits/municipal_m1a_retry/promoted_snapshot'
FORMS = ('municipio de {}', 'alcaldía de {}', 'contratos del municipio de {}',
         'contratos de la alcaldía de {}')


def save(name, data):
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = encode(data)
    path.write_bytes(gzip.compress(raw, mtime=0) if path.suffix == '.gz' else raw)


def discovery():
    registry = get_municipal_territories()
    names = registry.by_alias
    compounds = [name for name in names if ' ' in name]
    articulated = [name for name in names if any(w in {'de', 'del'} for w in name.split()[1:])]
    collisions = [dict(full=a, prefix=b, full_codes=[t.divipola for t in names[a]],
                       prefix_codes=[t.divipola for t in names[b]])
                  for a in names for b in names if a.startswith(b + ' ')]
    homonyms = {name: [t.to_dict() for t in ts] for name, ts in names.items() if len(ts) > 1}
    return dict(source=str(PATH.relative_to(ROOT)), sha256=sha(PATH.read_bytes()),
                definitions=dict(compound='More than one name token',
                                 articulated='Internal de/del token',
                                 prefix_collision='Ordered distinct name pair, whole-token prefix'),
                counts=dict(divipola=len(registry.by_code), compound_names=len(compounds),
                            articulated_names=len(articulated), prefix_collisions=len(collisions),
                            homonym_groups=len(homonyms)),
                compound_names=compounds, articulated_names=articulated,
                prefix_collisions=collisions, homonym_groups=homonyms)


def buyer_oracle(records, sidecar, candidate):
    """Join exact physical sources to M0 adjudication; never consult NIT."""
    analysis = read(ROOT / 'artifacts/audits/municipal_m0_v1/analysis.json.gz')
    evidence = defaultdict(list)
    for row in analysis['strata']:
        evidence[row['dataset'], row['name']].append(row)
    recognized = {r['canonical']: r for r in analysis['recognized_units']}
    proposals = read(ROOT / 'artifacts/audits/municipal_m0_v1/proposals.json')['proposals']
    proposed = {p['canonical']: p for p in proposals} if candidate else {}
    registry = parse_historical_projection(sidecar)
    result = {}
    for record in records:
        owner = record['official_name']
        if owner in proposed:
            p = proposed[owner]
            result[owner] = dict(codes=[p['code']], authority='M0 proposal', sources=p['source_names'])
        elif owner in recognized:
            r = recognized[owner]
            result[owner] = dict(codes=[r['municipality']], authority='M0 recognized_units', sources=r['source_names'])
        else:
            sources = {ds: (registry.project(owner, ds) or (owner,)) for ds in ('p6dx-8zbt', 'jbjy-vk9h')}
            rows = [r for ds, names in sources.items() for name in names for r in evidence[ds, name]]
            codes = {r['municipality'] for r in rows if r['status'] == 'ACCREDITED_MUNICIPAL_AFFILIATION'}
            accredited = bool(rows) and all(r['status'] == 'ACCREDITED_MUNICIPAL_AFFILIATION' for r in rows)
            result[owner] = dict(codes=sorted(codes) if accredited and len(codes) == 1 else [],
                                 authority='M0 exact source strata' if accredited and len(codes) == 1 else 'REVIEW',
                                 sources=sources)
    return result


def query_cases(relevant=False):
    registry = get_municipal_territories()
    for t in registry.by_code.values():
        homonym = len(registry.lookup(t.municipality)) > 1
        if relevant and ' ' not in t.municipality and not homonym:
            continue
        for form in FORMS:
            yield t, form.format(t.municipality), False, homonym
        if homonym:
            for form in FORMS[:2]:
                yield t, form.format(f'{t.municipality}, {t.department}'), True, homonym


def classify(owner, codes, expected, ambiguous, should_execute, clarification, builder_blocked):
    if owner:
        if ambiguous:
            return 'WRONG_BUYER'
        if not codes:
            return 'REVIEW'
        if codes != [expected]:
            return 'WRONG_DIVIPOLA'
        return 'CORRECT_BUYER' if should_execute else 'REVIEW'
    if not should_execute and builder_blocked:
        return 'CLARIFICATION' if clarification else 'FAIL_CLOSED'
    return 'REVIEW'


def evaluate(router, sidecar, oracle, relevant=False):
    results = []
    registry = parse_historical_projection(sidecar)
    with patch('app.core.soql_builder.project_buyer_names', registry.project):
        for t, query, qualified, homonym in query_cases(relevant):
            params = router.parse(query).params
            plan = compute_query_plan(params, query)
            owner = params.get('entidad_resolved')
            group = group_for_params(params)
            members = list(group.members) if group else ([owner] if owner else [])
            codes = sorted({c for member in members for c in oracle.get(member, {}).get('codes', [])})
            if any(not oracle.get(member, {}).get('codes') for member in members):
                codes = []
            blocked = False
            try:
                soql = SoQLBuilder().build_count('jbjy-vk9h', dict(plan.params))
            except (ValueError, UnsafeGlobalQueryError) as exc:
                soql, blocked = f'BLOCKED: {type(exc).__name__}: {exc}', True
            classification = classify(owner or (group.group_id if group else None), codes, t.divipola, homonym and not qualified,
                                      plan.should_execute, plan.clarification_question, blocked)
            results.append(dict(code=t.divipola, query=query, qualified=qualified,
                                classification=classification, owner=owner, owner_codes=codes,
                                buyer_group=group.reference() if group else None, buyer_units=members,
                                should_execute=plan.should_execute, soql=soql,
                                clarification=plan.clarification_question, params=params))
    counts = {k: 0 for k in ('CORRECT_BUYER', 'CLARIFICATION', 'FAIL_CLOSED', 'REVIEW', 'WRONG_BUYER', 'WRONG_DIVIPOLA')}
    counts.update(Counter(r['classification'] for r in results))
    return dict(total_queries=len(results), municipalities=len({r['code'] for r in results}),
                counts=counts, status='BLOCKED' if counts['WRONG_BUYER'] + counts['WRONG_DIVIPOLA'] else
                'PARTIAL' if counts['REVIEW'] else 'PASS', results=results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--discovery', action='store_true')
    parser.add_argument('--pre', action='store_true')
    args = parser.parse_args()
    if args.discovery:
        matrix = discovery()
        save('municipal_identity_matrix.json', matrix)
        print(matrix['counts'], flush=True)
        return
    summary = {}
    with tempfile.TemporaryDirectory(prefix='m1a2-matrix-') as temp:
        for label, catalog, sources in (
            ('baseline', ROOT / 'app/data/aliases_db.json', ROOT / 'app/data/buyer_source_names.json'),
            ('candidate', SNAPSHOT / 'aliases_db.json.gz', SNAPSHOT / 'buyer_source_names.json.gz'),
        ):
            records, sidecar = read(catalog), read(sources)
            path = Path(temp) / f'{label}.json'
            path.write_bytes(encode(records))
            oracle = buyer_oracle(records, sidecar, label == 'candidate')
            report = evaluate(QueryRouter(EntityResolver(path)), sidecar, oracle, relevant=args.pre)
            prefix = 'pre' if args.pre else 'post'
            save(f'{prefix}_{label}_matrix.json.gz', report)
            save(f'{prefix}_{label}_oracle.json', oracle)
            summary[label] = {k: v for k, v in report.items() if k != 'results'}
            print(label, summary[label], flush=True)
    save(('pre' if args.pre else 'post') + '_summary.json', summary)
    if not args.pre and any(r['status'] != 'PASS' for r in summary.values()):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
