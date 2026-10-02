import json
C=json.load(open('omission_probe_results__res_cond.json')); A=json.load(open('omission_probe_results__res.json')); B=json.load(open('omission_probe_results__res_B.json'))
Ls=['L0_empty','L1_closed_min(art/prep/conj)','L1b_closed_full(+clit/aux/interrog/quant)','L2_STOPWORDS','L3_STOPWORDS+RESIDUAL_FW','L4_union_all_found','L3x_existing_as_is(comparators_ignorable)']
sh=lambda L:L.split('_')[0]
def row(*c): print('| '+' | '.join(str(x) for x in c)+' |')
print('### lexicon sizes')
lx=C['lexicons']
for k,v in lx['sizes'].items(): row(k,v,lx['only_in_one_lexicon'][k])
print(lx['variants']); print(lx['closed_min_vs_STOPWORDS']); print(lx['protected_inside_lexicons'])
labs=list(lx['sizes'])
print('### intersections (top few)')
for a in labs[:6]: print(a, {b:lx['pairwise_intersection'][a][b] for b in labs})
print('### curve A/B/C')
for V,R in (('C',C),('B',B),('A',A)):
    for c in R['sensitivity_curve']: row(V,sh(c['L']),c['recall_any_residual_pct'],c['recall_attributable_pct'],c['fp_clean_pct'],c['fp_ir_limit_pct'])
print('### detection by dim (C)')
d=C['detection']['by_dimension']
for k,v in sorted(d.items()):
    row(k,v['n'],*[f"{v[L]['any']}/{v[L]['attrib']}" for L in Ls])
print('### geometric')
g=C['geometric_gold_quote_coverage']
for k,v in sorted(g.items()):
    if k.startswith('__DIM__') or k=='__ALL__' or True:
        row(k,v['n'],v['cited'],v['partial_clip'],v['full_uncited'],*[f"{v['flag_partial'][L]}/{v['flag_full'][L]}" for L in Ls[:6]])
print('### cost C', C['population_SEARCH_by_slice_kind'])
for name in ('cost_CLEAN_CORRECT','cost_CORRECT_UNIVERSE_UNCITED','cost_IR_LIMIT'):
    cc=C[name]; print(name, cc['n'])
    for L in Ls:
        x=cc['by_L'][L]; row(sh(L),x['false_positive_records'],x['fp_rate_pct'],x['fp_with_protected_token'],x['fp_only_protected_tokens'],x['fp_solely_due_to_Y1(TIME clip)'],x['fp_solely_due_to_any_gold_quoted_clip'])
print('### oracle'); 
print('clean ids', {sh(L):v['fp_rate_pct'] for L,v in C['oracle_on_clean_ids'].items()})
o=C['oracle_private_REAL+35']; print(o['n'],{sh(L):(v['fp_records'],v['fp_rate_pct']) for L,v in o['by_L'].items()})
for k,v in C['oracle_synthetic_slices'].items(): print(k,v['n'],{sh(L):(x['fp_records'],x['fp_rate_pct']) for L,x in v['by_L'].items()})
print('### IR split'); 
for k,v in C['IR_limit_split'].items(): print(k, v, C['IR_limit_oracle_comparison'][k])
print('### Y1'); print(json.dumps(C['Y1'])[:1800])
