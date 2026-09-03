"""Read-only SIGEP affiliation reconciliation; no group or identity merge."""
import json
import gzip
import argparse
import re
import sys
from collections import defaultdict, Counter
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from app.core.entity_types import name_tokens
from app.core.entity_resolver import EntityResolver
from app.core.buyer_source_names import project_buyer_names
from app.core.buyer_groups import get_groups
from app.utils.money import normalize_text
root=ROOT
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--sources',type=Path,default=root/'artifacts/audits/municipal_m0_v1/sources')
parser.add_argument('--output',type=Path,default=root/'artifacts/audits/municipal_m0_v1/analysis.json.gz')
args=parser.parse_args()
def read(name): return json.loads(gzip.decompress((args.sources/(name+'.json.gz')).read_bytes()))
sigep=read('sigep')
dane={r['attributes']['MPIO_CDPMP']:r['attributes'] for r in read('divipola')['features'] if r['attributes']['MPIO_TIPO']=='MUNICIPIO'}
directory=read('directory')['rows']
bogota=get_groups()['bogota_executive']
bogota_names={ds:{n for member in bogota.members for n in project_buyer_names(member,ds)} for ds in ('p6dx-8zbt','jbjy-vk9h')}
bogota_evidence=json.loads((ROOT/'artifacts/audits/bogota_group_membership_v1.json').read_text())
bogota_accounts={ds:{(r.get('entidad',r.get('nombre_entidad')),str(r['codigo_entidad']),str(r['nit_entidad'])) for r in evidence['rows']} for ds,evidence in bogota_evidence['sources'].items()}
data={'p6dx-8zbt':read('processes')['rows'],'jbjy-vk9h':read('contracts')['rows']}
assert sum(int(r['unique_ids']) for r in data['p6dx-8zbt'])==int(read('process_distinct')['rows'][0]['unique_processes'])
assert read('contracts')['counts_after'][0]['physical_rows']==read('contracts')['counts_after'][0]['unique_contracts']
def fiscal(n):
    # Strip explicitly separated check digit only. No unbounded zero/DV guessing.
    return re.sub(r'[ .]','',str(n).split('-')[0])
by_nit=defaultdict(list)
for r in sigep:
    r['divipola']=r['DIVIPOLA DEPARTAMENTO'].zfill(2)+r['DIVIPOLA MUNICIPIO'].zfill(3)
    by_nit[fiscal(r['NIT'])].append(r)
by_account=defaultdict(list)
for r in directory: by_account[str(r['codigo_entidad'])].append(r)
def root_head(name): return EntityResolver._official_matches_institutional_family(name,'municipality')
def contains_town(name,code):
    hay=name_tokens(name); needle=name_tokens(dane[code]['MPIO_CNMBRE'])
    return any(hay[i:i+len(needle)]==needle for i in range(len(hay)-len(needle)+1))
classified=[]
for ds,rows in data.items():
    for row in rows:
        name=row.get('entidad',row.get('nombre_entidad'))
        regs=[r for r in by_account[str(row['codigo_entidad'])] if name_tokens(r['nombre_entidad'])==name_tokens(name) and fiscal(r['nit_entidad'])==fiscal(row['nit_entidad'])]
        codes={m[1] for r in regs if (m:=re.search(r'-(\d{5})$',r.get('c_digo_ubicaci_n','')))}
        sigs=by_nit[fiscal(row['nit_entidad'])]
        matched=[]
        for s in sigs:
            same=name_tokens(s['NOMBRE'])==name_tokens(name)
            code=s['divipola']
            roots=(code in dane and code in codes and root_head(s['NOMBRE']) and root_head(name) and contains_town(name,code) and contains_town(s['NOMBRE'],code))
            if regs and code in codes and (same or roots): matched.append(s)
        scopes={(s['SUBORDEN'],s['divipola']) for s in matched}
        scope=next(iter(scopes)) if len(scopes)==1 else None
        basis='SIGEP_DIRECTORY_NAME_FISCAL_AND_TERRITORY'
        if name in bogota_names[ds] and (name,str(row['codigo_entidad']),str(row['nit_entidad'])) in bogota_accounts[ds]:
            # Affiliation follows the accredited, declared executive registry.
            # Geography and fiscal evidence never manufacture this membership.
            scope=('Distrital','11001')
            basis='DECLARED_BOGOTA_EXECUTIVE_REGISTRY_V1'
        if scope and scope[0] in ('Municipal','Distrital') and scope[1] in dane:
            status='ACCREDITED_MUNICIPAL_AFFILIATION'
        elif scope or row.get('ordenentidad',row.get('orden')) == 'Nacional':
            status='ACCREDITED_OTHER_AFFILIATION'
        else:
            status='UNRESOLVED_AFFILIATION'
        classified.append(dict(dataset=ds,account=str(row['codigo_entidad']),name=name,nit_evidence=str(row['nit_entidad']),physical_rows=int(row['physical_rows']),unique_ids=int(row.get('unique_ids',row['physical_rows'])),status=status,municipality=scope[1] if scope else None,affiliation_basis=basis,sigep_codes=sorted(s['CÓDIGO SIGEP'] for s in matched),source_order=row.get('ordenentidad',row.get('orden'))))
resolver=EntityResolver(root/'app/data/aliases_db.json')
records=json.loads((root/'app/data/aliases_db.json').read_text())
by_canonical={r['official_name']:r for r in records}
source_names={ds:{r['name'] for r in classified if r['dataset']==ds} for ds in data}
normalized_sources={ds:{n:normalize_text(n) for n in names} for ds,names in source_names.items()}
reach_cache={}
def reach(c,ds):
    if (c,ds) not in reach_cache:
        binding=project_buyer_names(c,ds)
        reach_cache[c,ds]=set(binding)&source_names[ds] if binding else {n for n,value in normalized_sources[ds].items() if normalize_text(c) in value}
    return reach_cache[c,ds]
municipal=[r for r in classified if r['status']=='ACCREDITED_MUNICIPAL_AFFILIATION']
by_town=defaultdict(list)
for r in municipal: by_town[r['municipality']].append(r)
covered=set()
canonical_reach=[]
for canonical,rec in by_canonical.items():
    reached={ds:reach(canonical,ds) for ds in data}
    hits=[r for r in classified if r['name'] in reached[r['dataset']]]
    territories={r['municipality'] for r in hits}
    accounts={r['account'] for r in hits}
    if hits and all(r['status']=='ACCREDITED_MUNICIPAL_AFFILIATION' for r in hits) and len(territories)==1 and len(accounts)==1:
        aliases=[]
        for alias in rec.get('aliases',[]):
            resolution=resolver.resolve_entidad(alias)
            if resolution.value==canonical and resolution.method=='exact':
                aliases=[alias]
                break
        if not aliases: continue
        covered.update((r['dataset'],r['account'],r['name']) for r in hits)
        canonical_reach.append(dict(canonical=canonical,alias=aliases[0],municipality=next(iter(territories)),account=next(iter(accounts)),source_names={ds:sorted(ns) for ds,ns in reached.items()}))
claims=defaultdict(set)
for unit in canonical_reach:
    for ds,names in unit['source_names'].items():
        for name in names: claims[ds,name].add(unit['canonical'])
disputed={key for key,values in claims.items() if len(values)>1}
canonical_reach=[u for u in canonical_reach if not any((ds,n) in disputed for ds,names in u['source_names'].items() for n in names)]
covered={(ds,u['account'],name) for u in canonical_reach for ds,names in u['source_names'].items() for name in names}
for r in classified: r['covered']=(r['dataset'],r['account'],r['name']) in covered
summary={'sigep_rows':len(sigep),'sigep_suborder':dict(Counter(r['SUBORDEN'] for r in sigep)),'divipola_municipalities':len(dane),'accredited_active_municipal_territories':len(by_town),'recognized_municipal_territories':len({r['municipality'] for r in municipal if r['covered']}),'accredited_recognized_buyer_units':len(canonical_reach),'strata_disposition':dict(Counter(r['status'] for r in classified)),'volumes':{}}
summary['disputed_source_claims']=[dict(dataset=ds,name=name,canonicals=sorted(claims[ds,name])) for ds,name in sorted(disputed)]
for ds in data:
    summary['volumes'][ds]={status:{'physical_rows':sum(r['physical_rows'] for r in classified if r['dataset']==ds and r['status']==status),'unique_ids':sum(r['unique_ids'] for r in classified if r['dataset']==ds and r['status']==status),'covered_physical_rows':sum(r['physical_rows'] for r in classified if r['dataset']==ds and r['status']==status and r['covered']),'covered_unique_ids':sum(r['unique_ids'] for r in classified if r['dataset']==ds and r['status']==status and r['covered'])} for status in sorted({r['status'] for r in classified})}
output=json.dumps({'summary':summary,'strata':classified,'recognized_units':canonical_reach},ensure_ascii=False,indent=2).encode()
args.output.write_bytes(gzip.compress(output,mtime=0) if args.output.suffix=='.gz' else output)
print(json.dumps(summary,ensure_ascii=False,indent=2),flush=True)
