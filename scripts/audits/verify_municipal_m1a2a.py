"""Re-run the unchanged M1A.2 matrix with reviewed evidence and fresh candidates."""
from collections import Counter
import gzip
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from scripts.audits.adjudicate_municipal_m1a2a import adjudicate
from scripts.audits.consolidate_turbaco_m1a2a import A, B, OUT, consolidate, check
from scripts.audits.gate_divipola_buyer_matrix import buyer_oracle, evaluate
from scripts.audits.promote_municipal_m1a import encode, load_inputs, pipeline_gate, read, sha, transform, validate
from scripts.audits.replay_municipal_m1a1 import cross_interaction


def save(name, data):
    raw = encode(data)
    (OUT / name).write_bytes(gzip.compress(raw, mtime=0) if name.endswith('.gz') else raw)


def main():
    decisions = adjudicate()
    before = read(OUT / 'evidence/before_aliases_db.json.gz')
    before_sources = read(OUT / 'evidence/before_buyer_source_names.json.gz')
    records, sources = read(ROOT / 'app/data/aliases_db.json'), read(ROOT / 'app/data/buyer_source_names.json')
    assert (records, sources) == consolidate(before, before_sources)
    correction = check(before, before_sources, records, sources)
    proposals, analysis, manifest = load_inputs()
    candidate, bindings, accepted, rejected = transform(records, sources, proposals, analysis, manifest['as_of'])
    assert not rejected and len(accepted) == 503
    assert transform(candidate, bindings, proposals, analysis, manifest['as_of']) == (candidate, bindings, accepted, rejected)
    data = validate(records, sources, candidate, bindings, accepted, analysis)
    save('candidate_data_gates.json', data)
    pipeline = pipeline_gate(records, candidate, bindings, proposals)
    save('candidate_pipeline_gate.json', pipeline)
    print('candidate data/pipeline', data['status'], pipeline['status'], flush=True)
    summary, differences = {}, []
    with tempfile.TemporaryDirectory(prefix='m1a2a-') as temp:
        routers = []
        for label, catalog, sidecar in [('baseline', records, sources), ('candidate', candidate, bindings)]:
            path = Path(temp) / (label + '.json')
            path.write_bytes(encode(catalog))
            router = QueryRouter(EntityResolver(path))
            routers.append(router)
            oracle = buyer_oracle(catalog, sidecar, label == 'candidate')
            # Add only affirmative reviewed evidence. classify/evaluate/guard
            # are the original M1A.2 implementation, unchanged byte-for-byte.
            for case in decisions['cases']:
                if case['decision'] == 'VERIFIED_MUNICIPAL_AFFILIATION':
                    assert case['canonical'] in oracle
                    assert not oracle[case['canonical']]['codes']
                    oracle[case['canonical']] = case['affiliation']
            report = evaluate(router, sidecar, oracle)
            save(f'post_{label}_matrix.json.gz', report)
            save(f'post_{label}_oracle.json', oracle)
            historical = read(ROOT / f'artifacts/audits/municipal_m1a_2/post_{label}_matrix.json.gz')
            assert len(historical['results']) == len(report['results'])
            for old, new in zip(historical['results'], report['results']):
                assert (old['code'], old['query']) == (new['code'], new['query'])
                if (old['owner'], old['classification']) != (new['owner'], new['classification']):
                    differences.append(dict(catalog=label, query=new['query'], code=new['code'],
                                            old_owner=old['owner'], new_owner=new['owner'],
                                            old_classification=old['classification'], new_classification=new['classification']))
            summary[label] = {k: v for k, v in report.items() if k != 'results'}
            print(label, summary[label], flush=True)
        cross = cross_interaction(routers, records, proposals)
        save('candidate_cross_interaction.json.gz', cross)
    assert all(d['old_classification'] == 'REVIEW' and d['new_classification'] == 'CORRECT_BUYER'
               and (d['old_owner'] == d['new_owner'] or (d['old_owner'], d['new_owner']) == (A, B))
               for d in differences)
    protected = read(OUT / 'evidence/protected_hashes.json')
    unchanged = all(sha((ROOT / p).read_bytes()) == digest for p, digest in protected.items())
    totals = Counter()
    for report in summary.values():
        totals.update(report['counts'])
    ready = (decisions['status'] == data['status'] == pipeline['status'] == cross['status'] == 'PASS'
             and all(r['status'] == 'PASS' for r in summary.values()) and unchanged)
    gates = dict(status='PASS' if ready else 'PARTIAL', head_before='bd60e399a0b73c66fad60299b992729ae7b850fa',
                 M1A_RETRY_2_READY='YES' if ready else 'NO', production_promoted=0,
                 baseline_correction=correction, post_matrix=summary,
                 totals=dict(TOTAL=sum(r['total_queries'] for r in summary.values()), **totals),
                 matrix_changes=differences, protected_unchanged=unchanged,
                 new_wrong_owners=pipeline['new_wrong_owner_queries'] + cross['new_wrong_owner'],
                 new_multi_owner=len(data['new_debt']['multi_owner_alias_keys']),
                 new_scan_cross_owner=len(data['new_scan_cross_owner']),
                 no_nit_merge=correction['no_nit_merge'] and data['existing_units_preserved'] and data['existing_bindings_preserved'],
                 candidate_proposals=len(accepted), candidate_pipeline_checks=pipeline['checks'],
                 candidate_cross_interaction={k: cross[k] for k in ('status', 'checks', 'new_wrong_owner', 'lost_correct_owner')},
                 policy_unchanged_baseline_review_introduced=False)
    save('gates.json', gates)
    assert ready and gates['new_wrong_owners'] == gates['new_multi_owner'] == gates['new_scan_cross_owner'] == 0
    print('M1A_RETRY_2_READY:', gates['M1A_RETRY_2_READY'], flush=True)


if __name__ == '__main__':
    main()
