import json,sys
r=[json.loads(l) for l in open("/private/tmp/claude-501/-Users-rodrigoortiz-Documents-secoppal/771a1a89-ee94-4b92-b2de-eef37cce6788/scratchpad/gold_followups/synthetic_for_labelers/cases.jsonl")]
def c(ir):
    if ir is None: return "IR=null"
    s=f"{ir['operation']}|{ir['universe']}"+(f"+{ir.get('universes')}" if ir.get('universes') else "")+f"|{ir['view']}|pv={ir['product_view']}|vs={ir['view_source']}"
    cs=[]
    for x in ir['conditions']:
        cs.append(f"{x['id']}:{x['dimension']}.{x['operator']}{'' if x['polarity']=='POSITIVE' else '(NEG)'}={x['value_hint']}"+(f"@{x['level']}" if x['level'] else "")+(f"[{x['unit']}]" if x['unit'] else ""))
    s+=" ; "+" , ".join(cs)
    if ir.get('order'): s+=f" ; ORDER={ir['order']}"
    if ir.get('cardinality'): s+=f" ; CARD={ir['cardinality']}"
    return s
a,b=int(sys.argv[1]),int(sys.argv[2])
for x in r[a:b]:
    p=x['prior']
    print(f"{x['gold_id']} «{x['followup_text']}» gap={p['gap_seconds']} own={p['owner']} st={p['state']} ch={p['chain']} pend={p['pending']}\n   {c(p['ir'])}")
