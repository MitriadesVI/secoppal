"""Proxy de capacidad: rasgos que el parse legado detecta y el contrato Foundation no hospeda."""
import json, sys, re, socket
from datetime import date
from pathlib import Path
SP=Path(sys.argv[1])
socket.socket.connect=lambda *a,**k: (_ for _ in ()).throw(AssertionError("no network"))
from app.core.query_router import QueryRouter
from app.core.entity_resolver import EntityResolver, department_mention_resolution
from app.utils.money import normalize_text
from pathlib import Path as _P; qr=QueryRouter(); er=EntityResolver(_P("app/data/aliases_db.json"))
P=json.load(open(SP/'probe_out.json'))
def features(q):
    p=qr.parse(q, today=date(2026,9,30)).params
    f=[]
    if p.get('ordering_signal'): f.append('ORDER:'+p['ordering_signal'])
    mt=p.get('municipal_territory') or {}
    if mt.get('status') in ('resolved','ambiguous') and not p.get('entidad'): f.append('CITY')
    fd,fh=p.get('fecha_desde'),p.get('fecha_hasta')
    if fd or fh:
        whole_year = fd and fh and fd[:4]==fh[:4] and fd[5:]=='01-01' and fh[5:]=='12-31'
        if not whole_year: f.append('TIME_NOT_YEAR')
    raw=normalize_text(q)
    if re.search(r"\b(costa|caribe|eje cafetero|region)\b", raw): f.append('REGION')
    if re.search(r"\b(hoy|ahora|actualmente|este momento|ultimos?|recientes?)\b", raw): f.append('RELATIVE_TIME')
    if re.search(r"\bsmmlv\b", raw): f.append('SMMLV')
    if re.search(r"\b(cuantos?|cuanto suman|total(es)?)\b", raw): f.append('AGG')
    ent=p.get('entidad')
    if ent:
        r=er.resolve_entidad(ent)
        ok = r.method in {"exact","exact_official","exact_normalized"} or r.method.startswith("strict_")
        if not ok: f.append(f'BUYER_UNRESOLVED:{r.method}')
    return f
res={}
for o in P:
    if o.get('eligible') and not o.get('owner'):
        res[o['i']]=features(o['q'])
json.dump(res, open(SP/'capacity_out.json','w'), ensure_ascii=False)
import collections
by=collections.defaultdict(collections.Counter); tot=collections.Counter()
for o in P:
    if o['i'] in res:
        tot[o['gold']]+=1
        fs=res[o['i']]
        by[o['gold']]['ANY' if fs else 'CLEAN']+=1
        for x in fs: by[o['gold']][x.split(':')[0]]+=1
for g in tot: print(g, tot[g], dict(by[g].most_common()))
