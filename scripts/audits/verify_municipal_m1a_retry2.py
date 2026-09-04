"""Retry #2 readiness against the complete candidate; never apply production."""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.buyer_source_names import parse_registry
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from scripts.audits.gate_divipola_buyer_matrix import SNAPSHOT, save
from scripts.audits.promote_municipal_m1a import (
    DATASETS, encode, load_inputs, pipeline_gate, read, sha, transform, validate,
)
from scripts.audits.replay_municipal_m1a1 import cross_interaction, geography_gate
from scripts.audits.verify_municipal_m1a_retry import buyer_check


def protected_hashes():
    paths = [ROOT / 'app/data/aliases_db.json', ROOT / 'app/data/buyer_source_names.json',
             ROOT / 'app/data/municipal_territories_v1.json',
             ROOT / 'scripts/audits/verify_municipal_m1a_retry.py']
    for directory in ('municipal_m0_v1', 'municipal_m1a', 'municipal_m1a_1', 'municipal_m1a_retry'):
        paths.extend(p for p in (ROOT / 'artifacts/audits' / directory).rglob('*') if p.is_file())
    return {str(p.relative_to(ROOT)): sha(p.read_bytes()) for p in paths}


def main():
    hashes = protected_hashes()
    proposals, analysis, manifest = load_inputs()
    before = read(ROOT / 'app/data/aliases_db.json')
    before_sources = read(ROOT / 'app/data/buyer_source_names.json')
    records, sources, accepted, rejected = transform(before, before_sources, proposals, analysis, manifest['as_of'])
    assert not rejected and len(accepted) == 503
    assert records == read(SNAPSHOT / 'aliases_db.json.gz')
    assert sources == read(SNAPSHOT / 'buyer_source_names.json.gz')
    assert transform(records, sources, proposals, analysis, manifest['as_of']) == (records, sources, accepted, rejected)
    data = validate(before, before_sources, records, sources, accepted, analysis)
    save('data_gates.json', data)
    print('data:', data['status'], flush=True)
    pipeline = pipeline_gate(before, records, sources, proposals)
    save('pipeline_gate.json', pipeline)
    print('pipeline:', pipeline['status'], pipeline['checks'], pipeline['failed_queries'], flush=True)
    with tempfile.TemporaryDirectory(prefix='m1a2-retry-') as temp:
        paths = [Path(temp) / 'before.json', Path(temp) / 'after.json']
        for path, catalog in zip(paths, (before, records)):
            path.write_bytes(encode(catalog))
        routers = [QueryRouter(EntityResolver(path)) for path in paths]
        cross = cross_interaction(routers, before, proposals)
        save('cross_interaction.json.gz', cross)
        print('cross:', cross['status'], cross['checks'], flush=True)
        geo = geography_gate(routers, proposals, analysis)
        save('geography_gate.json.gz', geo)
        print('geo failures:', len(geo['failures']), flush=True)
        registry = parse_registry(sources)
        with patch('app.core.soql_builder.project_buyer_names', registry.project), patch(
                'scripts.audits.verify_municipal_m1a_retry.project_buyer_names', registry.project):
            projection = [buyer_check(routers[1], SoQLBuilder(), p, p['aliases'][0], ds)
                          for p in proposals for ds in DATASETS]
        save('source_projection.json.gz', dict(checks=len(projection),
                                             failures=[r for r in projection if not r['passed']],
                                             results=projection))
    unchanged = protected_hashes() == hashes
    summary = dict(proposals=len(proposals), candidate_equals_retry_snapshot=True, idempotence='PASS',
                   production_promoted=0, protected_unchanged=unchanged, protected_sha256=hashes,
                   data_status=data['status'], alias_checks=pipeline['checks'],
                   alias_failures=pipeline['failed_queries'], new_wrong_owner=pipeline['new_wrong_owner_queries'],
                   cross_interaction={k: cross[k] for k in ('status', 'checks', 'new_wrong_owner', 'lost_correct_owner')},
                   new_multi_owner=len(data['new_debt']['multi_owner_alias_keys']),
                   new_scan_cross_owner=len(data['new_scan_cross_owner']),
                   no_nit_merge=data['existing_units_preserved'] and data['existing_bindings_preserved'],
                   source_projection_checks=len(projection),
                   source_projection_failures=sum(not r['passed'] for r in projection),
                   geography_failures=len(geo['failures']))
    save('candidate_summary.json', summary)
    assert unchanged and data['status'] == pipeline['status'] == cross['status'] == 'PASS'
    assert not geo['failures'] and all(r['passed'] for r in projection)
    assert pipeline['checks'] == 1888 and cross['checks'] == 3071
    print('CANDIDATE GATES: PASS; production unchanged', flush=True)


if __name__ == '__main__':
    main()
