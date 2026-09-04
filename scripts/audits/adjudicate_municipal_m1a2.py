"""Adjudicate Retry #1's other failures using frozen buyer/source evidence."""
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.buyer_source_names import parse_registry
from app.core.entity_resolver import EntityResolver
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError
from app.utils.money import normalize_text
from scripts.audits.gate_divipola_buyer_matrix import SNAPSHOT, save
from scripts.audits.promote_municipal_m1a import DATASETS, encode, read


def main():
    proposals = read(ROOT / 'artifacts/audits/municipal_m0_v1/proposals.json')['proposals']
    cases = []
    with tempfile.TemporaryDirectory(prefix='m1a2-adjudication-') as temp:
        for label, records, sidecar in (
            ('baseline', read(ROOT / 'app/data/aliases_db.json'), read(ROOT / 'app/data/buyer_source_names.json')),
            ('candidate', read(SNAPSHOT / 'aliases_db.json.gz'), read(SNAPSHOT / 'buyer_source_names.json.gz')),
        ):
            path = Path(temp) / f'{label}.json'
            path.write_bytes(encode(records))
            resolver = EntityResolver(path)
            router = QueryRouter(resolver)
            registry = parse_registry(sidecar)
            with patch('app.core.soql_builder.project_buyer_names', registry.project):
                for name in ('Baranoa', 'Nuevo Colón', 'Villa de Leyva'):
                    proposal = next(p for p in proposals if normalize_text(p['town']) == normalize_text(name))
                    query = 'contratos de la alcaldía de ' + name
                    params = router.parse(query).params
                    plan = compute_query_plan(params, query)
                    soql = {}
                    for ds in DATASETS:
                        try:
                            soql[ds] = SoQLBuilder().build_count(ds, params)
                        except (ValueError, UnsafeGlobalQueryError) as exc:
                            soql[ds] = 'BLOCKED: ' + type(exc).__name__
                    assert not params.get('objeto') and not params.get('ordering_signal')
                    assert not params.get('municipal_territory') and not params.get('ciudad')
                    assert (params.get('entidad_resolved') == proposal['canonical']) if label == 'candidate' else (
                        not params.get('entidad_resolved') and not plan.should_execute)
                    cases.append(dict(catalog=label, name=name, expected_code=proposal['code'],
                                      classification='PRE_PROMOTION_ABSENCE_ORACLE', query=query,
                                      params=params, should_execute=plan.should_execute, soql=soql,
                                      source_names=proposal['source_names']))
            if label == 'candidate':
                belmira = next(p for p in proposals if p['town'] == 'BELMIRA')
                record = next(r for r in records if r['official_name'] == belmira['canonical'])
                alias = belmira['aliases'][0]
                exact = resolver.resolve_entidad(alias)
                scan = resolver.scan_entity(normalize_text('personeria ' + record['official_name']))
                assert exact.value == record['official_name'] and exact.method == 'exact'
                assert scan is None
                assert record['official_name'] in record['scan_disabled_aliases']
                cases.append(dict(name='Belmira', classification='STALE_ALIAS_ONLY_SCHEMA_ASSERTION',
                                  canonical=record['official_name'], exact=exact.to_dict(),
                                  foreign_institution_scan=None, scan_disabled=record['scan_disabled_aliases']))
    save('adjudication.json', dict(status='PASS', cases=cases,
                                 tests_policy='Absence assertions use frozen baseline; candidate semantics tested separately. '
                                 'Scan exclusion accepts owned aliases and own canonical; exact lookup stays available.'))
    print('ADJUDICATION: PASS')


if __name__ == '__main__':
    main()
