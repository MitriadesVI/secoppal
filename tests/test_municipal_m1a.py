"""Predictive M1A gates run against a temporary catalog before production writes."""

from copy import deepcopy
import json
from pathlib import Path

import pytest

from app.core.buyer_source_names import parse_registry
from app.core.entity_resolver import EntityResolver
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.utils.money import normalize_text
from scripts.audits.promote_municipal_m1a import (
    DATASETS,
    OUT,
    encode,
    load_inputs,
    pipeline_gate,
    read,
    transform,
    validate,
)


@pytest.fixture(scope='module')
def prediction(tmp_path_factory):
    proposals, analysis, source_manifest = load_inputs()
    before = read(OUT / 'before_aliases_db.json.gz')
    sources = read(OUT / 'before_buyer_source_names.json.gz')
    result = transform(before, sources, proposals, analysis, source_manifest['as_of'])
    records, sidecar, accepted, rejected = result
    path = tmp_path_factory.mktemp('m1a') / 'aliases.json'
    path.write_bytes(encode(records))
    return dict(
        proposals=proposals,
        analysis=analysis,
        before=before,
        sources=sources,
        result=result,
        resolver=EntityResolver(path),
        registry=parse_registry(sidecar),
        accepted=accepted,
        rejected=rejected,
    )


def test_all_m0_aliases_exact_unique_and_all_sources_byte_identical(prediction):
    p = prediction
    assert not p['rejected']
    assert len(p['result'][0]) - len(p['before']) == 498
    for proposal in p['proposals']:
        for alias in proposal['aliases']:
            result = p['resolver'].resolve_entidad(alias)
            assert (result.value, result.method) == (proposal['canonical'], 'exact')
            assert p['resolver'].owners_by_alias[normalize_text(alias)] == {proposal['canonical']}
        for ds in DATASETS:
            assert p['registry'].project(proposal['canonical'], ds) == tuple(
                proposal['source_names'][ds]
            )


def test_idempotence_no_new_debt_and_no_alias_invention(prediction):
    p = prediction
    records, sidecar, accepted, rejected = p['result']
    assert transform(records, sidecar, p['proposals'], p['analysis'], '2026-09-03') == p['result']
    gates = validate(p['before'], p['sources'], records, sidecar, accepted, p['analysis'])
    assert gates['status'] == 'PASS', gates
    old = {r['official_name']: r for r in p['before']}
    proposals = {r['canonical']: r for r in p['proposals']}
    for r in records:
        before_aliases = old.get(r['official_name'], {}).get('aliases', [])
        expected = proposals.get(r['official_name'], {}).get('aliases', [])
        assert r['aliases'] == (
            list(dict.fromkeys([*before_aliases, *expected])) if expected else before_aliases
        )
    assert all(b in sidecar['bindings'] for b in p['sources']['bindings'])
    assert all(
        b['identity_key']['type'] == 'BUYER_UNIT'
        for b in sidecar['bindings'][len(p['sources']['bindings']) :]
    )


def test_real_query_pipeline_stratified_canaries(prediction, monkeypatch):
    """Independent candidates work; the global blocker is tested separately."""
    monkeypatch.setattr('app.core.soql_builder.project_buyer_names', prediction['registry'].project)
    router = QueryRouter(entity_resolver=prediction['resolver'])
    builder = SoQLBuilder()
    from scripts.audits.verify_municipal_m1a import select_canaries

    for p in select_canaries(prediction['proposals'])['proposals']:
        if p['code'] in {
            '05148',
            '05856',
            '13244',
            '15087',
            '15494',
            '15774',
            '18860',
            '25486',
            '66088',
        }:
            continue  # Explicitly blocked by the exhaustive gate below.
        for alias in p['aliases']:
            for kind, ds, column in [
                ('contratos', 'jbjy-vk9h', 'nombre_entidad'),
                ('procesos', 'p6dx-8zbt', 'entidad'),
            ]:
                query = (
                    f'{kind} de la {alias}'
                    if alias.startswith('alcaldia')
                    else f'{kind} del {alias}'
                )
                params = router.parse(query).params
                assert params.get('entidad_resolved') == p['canonical'], (query, params)
                plan = compute_query_plan(params, query)
                assert plan.should_execute, (query, plan)
                assert plan.params.get('entidad_resolved') == p['canonical']
                soql = builder.build_count(ds, dict(plan.params))
                source = p['source_names'][ds][0].replace("'", "''")
                assert f"{column} = '{source}'" in soql, (query, soql)
                assert f'{column} like' not in soql.lower()


def test_exhaustive_pipeline_gate_blocks_systemic_wrong_owner(prediction):
    p = prediction
    gate = pipeline_gate(p['before'], p['result'][0], p['result'][1], p['proposals'])
    assert gate['status'] == 'BLOCKED'
    assert gate['checks'] == 1888
    assert gate['failed_queries'] == 30
    assert gate['affected_municipalities'] == 9
    assert gate['new_wrong_owner_queries'] == 4
    assert {f['expected'] for f in gate['failures'] if f['new_wrong_owner']} == {
        'ALCALDIA MUNICIPAL SUSACON BOYACA'
    }
    assert {f['after']['entidad_resolved'] for f in gate['failures'] if f['new_wrong_owner']} == {
        'ALCALDIA DE SUSA'
    }


@pytest.mark.parametrize(
    'town',
    [
        'baranoa',
        'sabaneta',
        'puerto gaitan',
        'magangue',
        'los patios',
        'tocancipa',
        'aguazul',
        'ipiales',
    ],
)
def test_geography_debt_is_not_silently_claimed_closed(prediction, town, tmp_path):
    query = f'contratos en {town}'
    path = tmp_path / 'baseline.json'
    path.write_bytes(encode(prediction['before']))
    baseline = QueryRouter(EntityResolver(path)).parse(query).params
    candidate = QueryRouter(entity_resolver=prediction['resolver']).parse(query).params
    assert compute_query_plan(candidate, query).params == compute_query_plan(baseline, query).params
    if town == 'baranoa':
        # Mandatory canary currently fails the requested geography semantics
        # even before M1A; record the blocker rather than add an xfail.
        assert candidate == {
            'dataset': 'contratos',
            'dataset_explicit': True,
            'objeto': ['baranoa'],
        }


def test_baranoa_source_parity_is_380_processes_and_374_contracts(prediction, monkeypatch):
    monkeypatch.setattr('app.core.soql_builder.project_buyer_names', prediction['registry'].project)
    proposal = next(p for p in prediction['proposals'] if p['code'] == '08078')
    for kind, ds, count in [('procesos', 'p6dx-8zbt', 380), ('contratos', 'jbjy-vk9h', 374)]:
        query = f'{kind} de la alcaldia de baranoa'
        params = QueryRouter(entity_resolver=prediction['resolver']).parse(query).params
        assert params['entidad_resolved'] == proposal['canonical']
        reached = [
            r
            for r in prediction['analysis']['strata']
            if r['dataset'] == ds
            and r['name'] in prediction['registry'].project(params['entidad_resolved'], ds)
        ]
        assert {(r['account'], r['nit_evidence']) for r in reached} == {('701706467', '890112371')}
        assert sum(r['unique_ids'] for r in reached) == count
    promoter = Path('scripts/audits/promote_municipal_m1a.py').read_text().lower()
    assert 'baranoa' not in promoter and '08078' not in promoter


def test_canonical_can_be_scan_disabled_without_inventing_alias(tmp_path):
    path = tmp_path / 'aliases.json'
    path.write_text(
        json.dumps(
            [
                dict(
                    official_name='MUNICIPIO DE PRUEBA',
                    aliases=['alcaldia de prueba'],
                    scan_disabled_aliases=['MUNICIPIO DE PRUEBA'],
                )
            ]
        )
    )
    resolver = EntityResolver(path)
    assert resolver.scan_entity('personeria municipio de prueba') is None
    assert resolver.resolve_entidad('alcaldia de prueba').value == 'MUNICIPIO DE PRUEBA'
    assert 'municipio de prueba' in resolver.source_names_to_official
    assert 'municipio de prueba' not in resolver.owners_by_alias


@pytest.mark.parametrize(
    'change,reason',
    [
        ('alias', 'NEW_MULTI_OWNER_ALIAS'),
        ('fiscal', 'FISCAL_EVIDENCE_CONTRADICTION'),
        ('projection', 'EXISTING_SOURCE_PROJECTION_CHANGED'),
        ('family', 'INSTITUTIONAL_FAMILY_NOT_MUNICIPAL_EXECUTIVE'),
        ('dataset', 'MISSING_DATASET_EVIDENCE'),
    ],
)
def test_affected_proposal_is_review_and_independent_proposals_continue(prediction, change, reason):
    records = deepcopy(prediction['before'])
    sidecar = deepcopy(prediction['sources'])
    proposals = deepcopy(prediction['proposals'])
    p = next(p for p in proposals if p['kind'] == 'ADD_ALIASES_TO_EXISTING_UNIT')
    if change == 'alias':
        records.append(dict(official_name='FOREIGN OWNER', aliases=[p['aliases'][0]]))
    elif change == 'fiscal':
        next(r for r in records if r['official_name'] == p['canonical'])['nit'] = '999999999'
    elif change == 'projection':
        sidecar['bindings'].append(
            dict(
                canonical_name=p['canonical'],
                identity_key={'type': 'BUYER_UNIT', 'value': 'test'},
                observed_at='2026-09-03',
                evidence='test',
                sources={ds: ['DIFFERENT PHYSICAL SOURCE'] for ds in DATASETS},
            )
        )
    elif change == 'family':
        p['canonical'] = 'PERSONERIA MUNICIPIO DE PRUEBA'
    elif change == 'dataset':
        p['source_names'].pop(DATASETS[0])
    result = transform(records, sidecar, proposals, prediction['analysis'], '2026-09-03')
    assert len(result[2]) == 502
    assert result[3] == [
        dict(proposal_code=p['code'], canonical=p['canonical'], status='REVIEW', reason=reason)
    ]
