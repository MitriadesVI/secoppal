"""Conservative executable proposals; only a temporary gazetteer is changed."""
from collections import Counter, defaultdict
import argparse
import gzip
import json
from pathlib import Path
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from app.core.entity_resolver import EntityResolver
from app.core.buyer_source_names import project_buyer_names
from app.utils.money import normalize_text


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit-dir',type=Path,default=ROOT/'artifacts/audits/municipal_m0_v1')
    args=parser.parse_args()
    base=args.audit_dir
    analysis=json.loads(gzip.decompress((base/'analysis.json.gz').read_bytes()))
    def source(name): return json.loads(gzip.decompress((base/'sources'/f'{name}.json.gz').read_bytes()))
    dane={r['attributes']['MPIO_CDPMP']:r['attributes'] for r in source('divipola')['features'] if r['attributes']['MPIO_TIPO']=='MUNICIPIO'}
    records=json.loads((ROOT/'app/data/aliases_db.json').read_text())
    by_name={r['official_name']:r for r in records}
    owners=defaultdict(set)
    for r in records:
        for alias in [r['official_name'],*r.get('aliases',[])]: owners[normalize_text(alias)].add(r['official_name'])
    town_counts=Counter(normalize_text(r['MPIO_CNMBRE']) for r in dane.values())
    all_owners=defaultdict(set)
    audited=set()
    for report in source('ownership'):
        # Supplementary batches include units with zero activity in one dataset.
        # A completed, untruncated GROUP BY still proves their observed owners;
        # absent names yield an empty set and cannot satisfy expected ownership.
        if report.get('error') or 'rows' not in report or len(report['rows'])>=10000: continue
        ds=report['dataset']
        audited.update((ds,n) for n in report['source_names'])
        for r in report['rows']:
            name=r.get('entidad',r.get('nombre_entidad'))
            all_owners[ds,name].add((str(r['codigo_entidad']),str(r['nit_entidad'])))
    # Check names against the entire recent population, not just the accepted cohort.
    recent_owners=defaultdict(set)
    by_town=defaultdict(list)
    for r in analysis['strata']:
        recent_owners[r['dataset'],r['name']].add((r['account'],r['nit_evidence']))
        if r['status']=='ACCREDITED_MUNICIPAL_AFFILIATION' and EntityResolver._official_matches_institutional_family(r['name'],'municipality'):
            by_town[r['municipality']].append(r)
    dispositions=Counter(); proposals=[]; rejected=[]
    for code,town in dane.items():
        obs=by_town[code]
        def reject(reason):
            dispositions[reason]+=1
            rejected.append(dict(code=code,town=town['MPIO_CNMBRE'],reason=reason,accounts=sorted({r['account'] for r in obs}),source_names=sorted({r['name'] for r in obs})))
        reason=''
        if not obs: reason='NO_SIGEP_ACCREDITED_EXECUTIVE_ACTIVITY'
        elif any(r['covered'] for r in obs): reason='ALREADY_RECOGNIZED_EXECUTIVE'
        elif len({r['account'] for r in obs})!=1: reason='KEEP_SEPARATE_MULTIPLE_ACCOUNTS'
        elif {r['dataset'] for r in obs}!={'p6dx-8zbt','jbjy-vk9h'}: reason='MISSING_DATASET_EVIDENCE'
        elif len({r['name'] for r in obs})!=1: reason='SOURCE_PAIR_REQUIRES_ADR074_ADJUDICATION'
        elif len({r['nit_evidence'] for r in obs})!=1: reason='FISCAL_EVIDENCE_REVIEW'
        if reason:
            reject(reason)
            continue
        name=obs[0]['name'];account=obs[0]['account'];nit=obs[0]['nit_evidence']
        expected={(account,nit)}
        if any((ds,name) not in audited for ds in ('p6dx-8zbt','jbjy-vk9h')):
            reject('ALL_TIME_OWNERSHIP_NOT_AUDITED');continue
        if any(all_owners[ds,name]!=expected or recent_owners[ds,name]!=expected for ds in ('p6dx-8zbt','jbjy-vk9h')):
            reject('SOURCE_HAS_OTHER_ACCOUNT_OR_FISCAL_EVIDENCE');continue
        known=owners[normalize_text(name)]
        if len(known)>1:
            reject('SOURCE_HAS_MULTIPLE_ALIAS_OWNERS');continue
        canonical=next(iter(known)) if known else name
        if any(project_buyer_names(canonical,ds) not in (None,(name,)) for ds in ('p6dx-8zbt','jbjy-vk9h')):
            reject('EXISTING_PROJECTION_REQUIRES_REVIEW');continue
        aliases=[f"alcaldia de {town['MPIO_CNMBRE']}, {town['DPTO_CNMBRE']}",f"municipio de {town['MPIO_CNMBRE']}, {town['DPTO_CNMBRE']}"]
        if town_counts[normalize_text(town['MPIO_CNMBRE'])]==1:
            aliases += [f"alcaldia de {town['MPIO_CNMBRE']}",f"municipio de {town['MPIO_CNMBRE']}"]
        if any(owners[normalize_text(a)]-{canonical} for a in aliases):
            reject('HUMAN_ALIAS_OWNERSHIP_CONFLICT');continue
        kind='ADD_ALIASES_TO_EXISTING_UNIT' if known else 'ADD_NEW_ACCREDITED_UNIT'
        dispositions[kind]+=1
        proposals.append(dict(code=code,town=town['MPIO_CNMBRE'],department=town['DPTO_CNMBRE'],canonical=canonical,aliases=aliases,account=account,nit_evidence=nit,source_names={'p6dx-8zbt':[name],'jbjy-vk9h':[name]},kind=kind,volumes={ds:{'physical_rows':sum(r['physical_rows'] for r in obs if r['dataset']==ds),'unique_ids':sum(r['unique_ids'] for r in obs if r['dataset']==ds)} for ds in ('p6dx-8zbt','jbjy-vk9h')}))
    for proposal in proposals:
        record=by_name.get(proposal['canonical'])
        if record is None:
            record=dict(official_name=proposal['canonical'],departamento=proposal['department'],aliases=[])
            records.append(record);by_name[proposal['canonical']]=record
        record['aliases']=list(dict.fromkeys([*record.get('aliases',[]),*proposal['aliases']]))
    failures=[];checks=0
    with tempfile.TemporaryDirectory(prefix='secoppal-m0-') as temp:
        path=Path(temp)/'aliases.json';path.write_text(json.dumps(records,ensure_ascii=False))
        resolver=EntityResolver(path)
        for p in proposals:
            for a in p['aliases']:
                checks+=1;result=resolver.resolve_entidad(a)
                if result.value!=p['canonical'] or result.method!='exact' or resolver.owners_by_alias.get(normalize_text(a))!={p['canonical']}:
                    failures.append(dict(code=p['code'],alias=a,result=result.to_dict()))
    recognized={r['municipality'] for r in analysis['recognized_units']}
    summary=dict(status='PASS' if not failures else 'PARTIAL',eligible_executive_municipalities=len(proposals),additional_nominal_municipal_territories=len({p['code'] for p in proposals}-recognized),dispositions=dict(dispositions),alias_checks=checks,failures=failures,additional_volume={ds:{metric:sum(p['volumes'][ds][metric] for p in proposals) for metric in ('physical_rows','unique_ids')} for ds in ('p6dx-8zbt','jbjy-vk9h')},production_applied=False,zero_wrong_owners_scope='Exact aliases and complete observed all-time account/name strata; not a guarantee against future source changes.')
    (base/'proposals.json').write_text(json.dumps(dict(summary=summary,proposals=proposals,rejected=rejected),ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(summary,ensure_ascii=False,indent=2))


if __name__=='__main__': main()
