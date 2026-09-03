"""The municipal census never turns an unresolved residue into full coverage."""
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path

import pytest

BASE=Path(__file__).resolve().parents[1]/'artifacts/audits/municipal_m0_v1'


def source(name):
    return json.loads(gzip.decompress((BASE/'sources'/f'{name}.json.gz').read_bytes()))


def analysis():
    return json.loads(gzip.decompress((BASE/'analysis.json.gz').read_bytes()))


def test_every_source_snapshot_has_verified_provenance():
    manifest=json.loads((BASE/'sources/manifest.json').read_text())
    for record in manifest['snapshots']:
        raw=gzip.decompress((BASE/'sources'/record['file']).read_bytes())
        assert hashlib.sha256(raw).hexdigest()==record['uncompressed_sha256']
    assert len(source('sigep'))==6300
    territories=source('divipola')['features']
    assert len({r['attributes']['MPIO_CDPMP'] for r in territories})==len(territories)==1122
    assert sum(r['attributes']['MPIO_TIPO']=='MUNICIPIO' for r in territories)==1103


@pytest.mark.parametrize('key,ds',[('processes','p6dx-8zbt'),('contracts','jbjy-vk9h')])
def test_recent_bulk_census_and_classification_reconcile(key,ds):
    snapshot=source(key)
    assert snapshot['status']=='PASS'
    assert snapshot['counts_before']==snapshot['counts_after']
    expected=int(snapshot['counts_after'][0]['physical_rows'])
    assert sum(int(r['physical_rows']) for r in snapshot['rows'])==expected
    strata=[r for r in analysis()['strata'] if r['dataset']==ds]
    assert sum(r['physical_rows'] for r in strata)==expected
    if key=='processes':
        assert sum(r['unique_ids'] for r in strata)==int(source('process_distinct')['rows'][0]['unique_processes'])
    else:
        assert sum(r['unique_ids'] for r in strata)==expected==int(snapshot['counts_after'][0]['unique_contracts'])
    assert all(not r['covered'] for r in strata if r['status']=='UNRESOLVED_AFFILIATION')


def test_proposals_have_one_observed_owner_in_each_dataset_without_nit_merge():
    report=json.loads((BASE/'proposals.json').read_text())
    assert report['summary']['status']=='PASS' and not report['summary']['failures']
    assert report['summary']['production_applied'] is False
    assert len(report['proposals'])+len(report['rejected'])==1103
    owners=defaultdict(set)
    for batch in source('ownership'):
        assert not batch.get('error') and len(batch['rows'])<10000
        for row in batch['rows']:
            owners[batch['dataset'],row.get('entidad',row.get('nombre_entidad'))].add((str(row['codigo_entidad']),str(row['nit_entidad'])))
    for proposal in report['proposals']:
        assert proposal['source_names']['p6dx-8zbt']==proposal['source_names']['jbjy-vk9h']
        for ds,names in proposal['source_names'].items():
            assert len(names)==1
            assert owners[ds,names[0]]=={(proposal['account'],proposal['nit_evidence'])}
    assert report['summary']['alias_checks']==sum(len(p['aliases']) for p in report['proposals'])


def test_baranoa_is_an_ordinary_accredited_proposal():
    report=json.loads((BASE/'proposals.json').read_text())
    baranoa=next(r for r in report['proposals'] if r['code']=='08078')
    assert baranoa['canonical']=='ALCALDIA MUNICIPAL DE BARANOA'
    assert baranoa['volumes']['p6dx-8zbt']=={'physical_rows':381,'unique_ids':380}
    assert baranoa['volumes']['jbjy-vk9h']=={'physical_rows':374,'unique_ids':374}
    script=BASE.parents[2]/'scripts/audits/audit_municipal_m0_proposals.py'
    assert 'baranoa' not in script.read_text().lower() and '08078' not in script.read_text()


def test_published_coverage_exposes_unknown_denominator_and_disjoint_proposals():
    report=json.loads((BASE/'summary.json').read_text())
    assert report['status']=='PARTIAL' and report['production_import'] is False
    assert report['nominal']['recognized']<=report['nominal']['accredited_active']<=1103
    for metrics in report['activity'].values():
        for metric in metrics.values():
            low,high=metric['municipal_denominator_bounds']
            assert high==low+metric['unresolved_affiliation'] and high>low
            assert metric['certified_recognized']<=low
            assert metric['certified_recognized']+metric['additional_if_proposals_imported']<=low
            assert metric['certified_coverage_bounds'][0]<metric['coverage_in_accredited_cohort']
