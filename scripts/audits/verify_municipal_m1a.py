"""Post-promotion deterministic parser/plan/SoQL canaries and M0 replay metrics."""

from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.core.buyer_source_names import project_buyer_names
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.utils.money import normalize_text
from scripts.audits.promote_municipal_m1a import DATASETS, OUT, encode, read


def select_canaries(proposals):
    ordered = sorted(
        proposals, key=lambda p: (sum(p['volumes'][ds]['unique_ids'] for ds in DATASETS), p['code'])
    )
    middle = len(ordered) // 2
    strata = {
        'small_activity': ordered[:10],
        'medium_activity': ordered[middle - 5 : middle + 5],
        'large_activity': ordered[-10:],
    }
    required = {
        'BARANOA',
        'SABANETA',
        'PUERTO GAITÁN',
        'MAGANGUÉ',
        'LOS PATIOS',
        'TOCANCIPÁ',
        'AGUAZUL',
        'IPIALES',
    }
    selected = {p['code']: p for group in strata.values() for p in group}
    selected.update({p['code']: p for p in proposals if p['town'] in required})
    # Qualifiers and similar/homonymous names are drawn from M0, not invented.
    categories = {}
    for prefix in ['SAN ', 'SANTA ', 'EL ', 'LA ']:
        cases = sorted(
            [p for p in proposals if p['town'].startswith(prefix)], key=lambda p: p['code']
        )[:2]
        categories[prefix.strip()] = [p['code'] for p in cases]
        selected.update({p['code']: p for p in cases})
    for label, predicate in [
        ('multiword', lambda p: ' ' in p['town']),
        ('accents', lambda p: any(c in p['town'] for c in 'ÁÉÍÓÚÑ')),
        ('homonyms_qualified_only', lambda p: len(p['aliases']) == 2),
        (
            'department_city_names',
            lambda p: (
                normalize_text(p['town'])
                in {'arauca', 'sucre', 'bolivar', 'cordoba', 'narino', 'santander'}
            ),
        ),
    ]:
        cases = sorted([p for p in proposals if predicate(p)], key=lambda p: p['code'])[:4]
        categories[label] = [p['code'] for p in cases]
        selected.update({p['code']: p for p in cases})
    return {
        'strata_basis': 'Rank by sum of unique process and contract activity in M0; not population.',
        'strata': {k: [p['code'] for p in v] for k, v in strata.items()},
        'categories': categories,
        'proposals': [selected[c] for c in sorted(selected)],
    }


def main():
    import tempfile
    from unittest.mock import patch

    from scripts.audits.adr073_historical_projection import parse_historical_projection
    from app.core.entity_resolver import EntityResolver
    from app.core.soql_builder import UnsafeGlobalQueryError
    from scripts.audits.promote_municipal_m1a import load_inputs, transform

    proposals, analysis, source_manifest = load_inputs()
    before = read(OUT / 'before_aliases_db.json.gz')
    before_sources = read(OUT / 'before_buyer_source_names.json.gz')
    candidate, sources, _, _ = transform(
        before, before_sources, proposals, analysis, source_manifest['as_of']
    )
    required = {
        'BARANOA',
        'SABANETA',
        'PUERTO GAITÁN',
        'MAGANGUÉ',
        'LOS PATIOS',
        'TOCANCIPÁ',
        'AGUAZUL',
        'IPIALES',
    }
    checks = []
    with tempfile.TemporaryDirectory(prefix='m1a-canaries-') as temp:
        path = Path(temp) / 'candidate.json'
        path.write_bytes(encode(candidate))
        routers = {'production': QueryRouter(), 'candidate': QueryRouter(EntityResolver(path))}
        registry = parse_historical_projection(sources)
        builder = SoQLBuilder()
        for p in proposals:
            if p['town'] not in required:
                continue
            for query, ds, expected_kind in [
                (f'contratos de la alcaldia de {p["town"]}', DATASETS[1], 'BUYER_UNIT'),
                (f'procesos del municipio de {p["town"]}', DATASETS[0], 'BUYER_UNIT'),
                (f'contratos en {p["town"]}', DATASETS[1], 'GEOGRAPHY'),
            ]:
                states = {}
                for label, router in routers.items():
                    params = router.parse(query).params
                    plan = compute_query_plan(params, query)
                    projector = registry.project if label == 'candidate' else project_buyer_names
                    with patch('app.core.soql_builder.project_buyer_names', projector):
                        try:
                            soql = builder.build_count(ds, dict(plan.params))
                        except UnsafeGlobalQueryError:
                            soql = 'BLOCKED: UnsafeGlobalQueryError'
                    if expected_kind == 'BUYER_UNIT':
                        passed = (
                            plan.should_execute
                            and plan.params.get('entidad_resolved') == p['canonical']
                        )
                    else:
                        passed = not plan.params.get('entidad_resolved') and normalize_text(
                            plan.params.get('ciudad') or ''
                        ) == normalize_text(p['town'])
                    states[label] = dict(
                        params=plan.params,
                        should_execute=plan.should_execute,
                        soql=soql,
                        passes_required_behavior=passed,
                    )
                checks.append(
                    dict(
                        code=p['code'],
                        query=query,
                        expected_kind=expected_kind,
                        expected_canonical=p['canonical']
                        if expected_kind == 'BUYER_UNIT'
                        else None,
                        **states,
                    )
                )
    sample = select_canaries(proposals)
    result = dict(
        status='BLOCKED',
        production_applied=False,
        checks=checks,
        stratified_sample={k: v for k, v in sample.items() if k != 'proposals'},
        stratified_municipalities=len(sample['proposals']),
        exhaustive_institutional_gate='preview/pipeline_gate.json',
        llm_execution_mode='DETERMINISTIC_OFFLINE_NO_LLM',
        llm_provider_live_coverage='NOT_VERIFIED',
    )
    (OUT / 'query_canaries.json').write_bytes(encode(result))
    replay = read(OUT / 'unchanged_runtime_replay.json.gz')
    summary = replay['summary']
    nominal = summary['divipola_municipalities']
    active = summary['accredited_active_municipal_territories']
    recognized = summary['recognized_municipal_territories']
    volumes = {ds: summary['volumes'][ds]['ACCREDITED_MUNICIPAL_AFFILIATION'] for ds in DATASETS}
    baranoa = next(p for p in proposals if p['code'] == '08078')
    parity = {}
    for ds in DATASETS:
        reached = [
            r
            for r in replay['strata']
            if r['dataset'] == ds and r['name'] in registry.project(baranoa['canonical'], ds)
        ]
        parity[ds] = dict(
            candidate_physical_rows=sum(r['physical_rows'] for r in reached),
            candidate_unique_ids=sum(r['unique_ids'] for r in reached),
            production_covered_unique_ids=sum(r['unique_ids'] for r in reached if r['covered']),
            expected=baranoa['volumes'][ds],
        )
    coverage = dict(
        status='UNCHANGED_NO_PROMOTION',
        offline_replay_identical_to_m0=replay == analysis,
        accredited_active_territories=active,
        recognized_territories=recognized,
        coverage_in_accredited_active_cohort_pct=100 * recognized / active,
        divipola_nominal_denominator=nominal,
        coverage_over_divipola_1103_pct=100 * recognized / nominal,
        process_unique_volume_coverage_pct=100
        * volumes[DATASETS[0]]['covered_unique_ids']
        / volumes[DATASETS[0]]['unique_ids'],
        contract_unique_volume_coverage_pct=100
        * volumes[DATASETS[1]]['covered_unique_ids']
        / volumes[DATASETS[1]]['unique_ids'],
        volumes=volumes,
        recognized_buyer_units=summary['accredited_recognized_buyer_units'],
        disputed_source_claims=summary['disputed_source_claims'],
        baranoa_parity=parity,
        global_municipal_buyer_unit_denominator='UNADJUDICATED',
    )
    (OUT / 'coverage_post.json').write_bytes(encode(coverage))
    print(
        json.dumps(
            dict(
                status='BLOCKED',
                canary_checks=len(checks),
                candidate_buyer_checks_passed=sum(
                    c['candidate']['passes_required_behavior']
                    for c in checks
                    if c['expected_kind'] == 'BUYER_UNIT'
                ),
                candidate_geography_checks_passed=sum(
                    c['candidate']['passes_required_behavior']
                    for c in checks
                    if c['expected_kind'] == 'GEOGRAPHY'
                ),
                coverage=coverage,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == '__main__':
    main()
