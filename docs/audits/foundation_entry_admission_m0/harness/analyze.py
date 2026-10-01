import json, sys, collections
SP=sys.argv[1]
out=json.load(open(SP+'/probe_out.json'))
errs=[o for o in out if 'error' in o]
print("errors", len(errs)); [print(" ", o['i'], o['q'][:60], o['error']) for o in errs[:5]]
def decision(o):
    if 'error' in o: return 'ERROR'
    if not o['eligible']: return 'LEX'
    if o['owner']: return 'OWN:'+o['owner']
    return 'ADMIT'
def base(g): return g.split('|')
for subset in ('real','synth'):
    rows=[o for o in out if (not str(o['i']).startswith('x'))==(subset=='real')]
    tab=collections.defaultdict(collections.Counter)
    for o in rows: tab[o['gold']][decision(o)]+=1
    print(f"\n== {subset} ({len(rows)})")
    for g,c in sorted(tab.items(), key=lambda kv:-sum(kv[1].values())):
        print(f"  {g:6} n={sum(c.values()):3}  ", dict(c.most_common()))
    # owner breakdown when eligible regardless (what the veto would say)
    own=collections.defaultdict(collections.Counter)
    for o in rows:
        if 'error' in o: continue
        own[o['gold']][o['owner'] or '-']+=1
    print("  -- owner veto if lexical passed:")
    for g,c in sorted(own.items(), key=lambda kv:-sum(kv[1].values())):
        print(f"  {g:6}", dict(c.most_common()))
