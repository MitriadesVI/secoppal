"""SB self-check: schema m2a-followup-gold/2, closed enums, quotes, §2.3 range convention, prior untouched."""
import hashlib, json, os, re

S = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CASES = os.path.join(S, 'synthetic_for_labelers', 'cases.jsonl')
LABELS = os.path.join(S, 'synth_labeler_SB', 'labels_v2.jsonl')
OUT = os.path.join(S, 'synth_labeler_SB', 'self_check.json')

E = dict(
    turn_kind={'DELTA', 'NEW_SEARCH', 'ANSWER_TO_CLARIFICATION', 'PROTOCOL', 'OTHER_OPERATION', 'UNCLEAR'},
    m2a_scope={'IN', 'OUT_ANCHOR_NOT_SEARCH', 'OUT_OPERATION_CHANGE', 'OUT_REFERENT_EXTENSIONAL', 'OUT_RELATIVE_TO_PREVIOUS'},
    op={'SET', 'CLEAR', 'KEEP', 'SET_ORDER', 'SET_UNIVERSE', 'ANSWER'},
    slot={'TIME', 'VALUE.lower', 'VALUE.upper', 'VALUE', 'GEOGRAPHY', 'BUYER', 'ACTOR', 'TOPIC', 'STATE', 'MODALITY',
          'CONTRACT_TYPE_OR_NATURE', 'ORDER', 'CARDINALITY', 'UNIVERSE'},
    disposition={'EXECUTE', 'CLARIFY', 'REASK', 'NEW_SEARCH', 'HANDOFF_LEGACY'},
    ambiguity={'CLEAR', 'AMBIGUOUS'}, confidence={'HIGH', 'MEDIUM', 'LOW'},
    effects={'VALUE_RANGE_REPLACED', 'VIEW_REDERIVED', 'TOPIC_REPLACED', 'UNIVERSE_CHANGED'},
    echo={'CARRIED', 'YEAR_INHERITED', 'UNIT_INHERITED', 'VIEW_DERIVED', 'VALUE_RANGE_REPLACED', 'TOPIC_REPLACED',
          'TOPN_CARRIED', 'CONTEXT_EXPIRED'},
    gap={'SET', 'SELECT', 'PV', 'OPSWITCH', 'UNIT', 'COUNTERPART', 'MONTHYEAR', 'VCOMP', 'UNIVTIME', 'GEOBUYER',
         'ENADELANTE', 'TTL', 'TOPIC', 'B5RANGE', 'ACTORTIME', 'FIRMADOS', 'TOTALES', 'YEARRANGE', 'COMPARATOR_FORM',
         'REGION_VOCAB', 'SMMLV', 'UNIVERSE_FRAGMENT'},
    year_source={'STATED', 'PREVIOUS_ACCEPTED_TIME', 'CLOCK_INFERRED', None},
    unit_source={'STATED', 'PREVIOUS_ACCEPTED_VALUE', 'SIBLING_IN_RANGE', None},
    unit={'MILLONES', 'MIL_MILLONES', 'SMMLV', 'PESOS', None},
    universe={'CONTRACTS', 'PROCESSES', 'UNSPECIFIED', 'DUAL'}, view={'RECORDS', 'OPPORTUNITIES'},
    product_view={None, 'HISTORICAL_PROCESSES', 'CONTRACTS', 'CURRENT_OPPORTUNITIES'}, view_source={'USER', 'DERIVED', None},
    operation={'SEARCH', 'COUNT', 'SUM_VALUE', 'GROUP_BY', 'RANK_SUPPLIERS', 'PROFILE_BUYER', 'PROFILE_ACTOR', 'FOLLOW', 'EXPORT'},
    level={'DEPARTMENT', 'MUNICIPALITY', 'REGION', 'UNKNOWN'},
)
OPS_BY_DIM = {
    'TOPIC': {'contains', 'whole_token_folded'}, 'BUYER': {'is'}, 'ACTOR': {'is'}, 'GEOGRAPHY': {'is'},
    'TIME': {'during_year', 'during_month', 'between_dates', 'relative_period'},
    'VALUE': {'greater_than', 'greater_or_equal', 'less_than', 'less_or_equal'},
    'STATE': {'is', 'family'}, 'MODALITY': {'is', 'family'}, 'CONTRACT_TYPE_OR_NATURE': {'is'},
    'ORDER': {'value_desc', 'value_asc', 'date_desc', 'date_asc'}, 'CARDINALITY': {'top_n'},
}
INPUT_FIELDS = ['gold_id', 'slice', 'followup_text', 'followup_sha256', 'source_kind', 'prior']
LABEL_FIELDS = ['turn_kind', 'm2a_scope', 'ops', 'resulting_ir', 'effects', 'expected_disposition', 'disposition_reason',
                'expected_echo', 'ambiguity', 'alternatives', 'confidence', 'gap_codes', 'labeler', 'notes']

errors, warnings = [], []
def err(g, m): errors.append(f'{g}: {m}')
def warn(g, m): warnings.append(f'{g}: {m}')

cases = {json.loads(l)['gold_id']: json.loads(l) for l in open(CASES, encoding='utf-8')}
recs = [json.loads(l) for l in open(LABELS, encoding='utf-8')]
ids = [r['gold_id'] for r in recs]
if len(recs) != 162: err('*', f'{len(recs)} records')
if ids != sorted(ids): err('*', 'not sorted by gold_id')
if len(set(ids)) != len(ids): err('*', 'duplicate gold_id')
if set(ids) != set(cases): err('*', 'id set differs from cases')

def check_ir(g, ir, where):
    for k in ['operation', 'universe', 'view', 'product_view', 'view_source', 'conditions', 'order', 'cardinality']:
        if k not in ir: err(g, f'{where}: missing {k}')
    if ir['operation'] not in E['operation']: err(g, f'{where}: operation {ir["operation"]}')
    if ir['universe'] not in E['universe']: err(g, f'{where}: universe')
    if ir['universe'] == 'DUAL' and ir.get('universes') != ['PROCESSES', 'CONTRACTS']: err(g, f'{where}: DUAL without universes')
    if ir['universe'] != 'DUAL' and 'universes' in ir: err(g, f'{where}: universes outside DUAL')
    if ir['view'] not in E['view']: err(g, f'{where}: view')
    if ir['product_view'] not in E['product_view']: err(g, f'{where}: product_view')
    if ir['view_source'] not in E['view_source']: err(g, f'{where}: view_source')
    if ir['operation'] == 'SEARCH':
        if (ir['universe'] == 'UNSPECIFIED') != (ir['product_view'] is not None): err(g, f'{where}: product_view iff UNSPECIFIED')
    ids_ = set()
    for c in ir['conditions']:
        if c['id'] in ids_: err(g, f'{where}: dup cond id {c["id"]}')
        ids_.add(c['id'])
        d = c['dimension']
        if d not in OPS_BY_DIM: err(g, f'{where}: dim {d}'); continue
        if c['operator'] not in OPS_BY_DIM[d]: err(g, f'{where}: operator {c["operator"]} not in {d}')
        if c['polarity'] not in ('POSITIVE', 'NEGATIVE'): err(g, f'{where}: polarity')
        if (d == 'GEOGRAPHY') != (c['level'] is not None): err(g, f'{where}: level iff GEOGRAPHY ({c["id"]})')
        if c['level'] is not None and c['level'] not in E['level']: err(g, f'{where}: level value')
        if c['unit'] not in E['unit']: err(g, f'{where}: unit value')
        if d != 'VALUE' and c['unit'] is not None: err(g, f'{where}: unit outside VALUE')
    if ir['order'] is not None:
        if ir['order'].get('operator') not in OPS_BY_DIM['ORDER'] or ir['order'].get('polarity') != 'POSITIVE': err(g, f'{where}: order shape')
    if ir['cardinality'] is not None:
        if ir['cardinality'].get('operator') != 'top_n' or not str(ir['cardinality'].get('value_hint', '')).isdigit(): err(g, f'{where}: cardinality shape')
        if ir['order'] is None and ir['operation'] == 'SEARCH': err(g, f'{where}: CARDINALITY_WITHOUT_ORDER')

new_gaps = {}
for r in recs:
    g = r['gold_id']; c = cases.get(g)
    if c is None: continue
    for k in INPUT_FIELDS:
        if json.dumps(r.get(k), sort_keys=True, ensure_ascii=False) != json.dumps(c[k], sort_keys=True, ensure_ascii=False):
            err(g, f'input field {k} altered')
    if hashlib.sha256(r['followup_text'].encode('utf-8')).hexdigest() != r['followup_sha256']:
        warn(g, 'followup_sha256 does not match sha256(followup_text) utf-8 (input as given; untouched)')
    for k in LABEL_FIELDS:
        if k not in r: err(g, f'missing {k}')
    extra = set(r) - set(INPUT_FIELDS) - set(LABEL_FIELDS)
    if extra: err(g, f'extra fields {extra}')
    if r['labeler'] != 'SB': err(g, 'labeler')
    for f, en in [('turn_kind', 'turn_kind'), ('m2a_scope', 'm2a_scope'), ('expected_disposition', 'disposition'),
                  ('ambiguity', 'ambiguity'), ('confidence', 'confidence')]:
        if r[f] not in E[en]: err(g, f'{f}={r[f]}')
    for x in r['effects']:
        if x not in E['effects']: err(g, f'effect {x}')
    for x in r['expected_echo']:
        if x not in E['echo']: err(g, f'echo {x}')
    for x in r['gap_codes']:
        if x not in E['gap']: err(g, f'gap {x}')
    if (r['expected_disposition'] == 'EXECUTE') != (r['disposition_reason'] is None): err(g, 'disposition_reason iff not EXECUTE')
    if (r['ambiguity'] == 'AMBIGUOUS') != bool(r['alternatives']): err(g, 'alternatives iff AMBIGUOUS')
    if ('VALUE_RANGE_REPLACED' in r['effects']) != ('VALUE_RANGE_REPLACED' in r['expected_echo']): err(g, 'VALUE_RANGE_REPLACED effect/echo mismatch')
    if ('TOPIC_REPLACED' in r['effects']) != ('TOPIC_REPLACED' in r['expected_echo']): err(g, 'TOPIC_REPLACED effect/echo mismatch')
    if r['expected_disposition'] in ('EXECUTE',) and 'VIEW_REDERIVED' in r['effects'] and 'VIEW_DERIVED' not in r['expected_echo']:
        err(g, 'VIEW_REDERIVED executed without VIEW_DERIVED echo')
    p = r['prior']
    # rule 2.2.7
    if p['owner'] not in ('FOUNDATION_SEARCH',) and r['m2a_scope'] != 'OUT_ANCHOR_NOT_SEARCH': err(g, 'owner≠FOUNDATION_SEARCH needs OUT_ANCHOR_NOT_SEARCH')
    if p['owner'].startswith('LEGACY:') and r['expected_disposition'] != 'HANDOFF_LEGACY': err(g, 'legacy owner needs HANDOFF_LEGACY')
    # F11
    if p['gap_seconds'] is not None and p['gap_seconds'] > 1800:
        if not (r['expected_disposition'] == 'CLARIFY' and r['disposition_reason'] == 'CONTEXT_EXPIRED_CONFIRM'): err(g, 'F11 expired not CLARIFY')
        if 'CONTEXT_EXPIRED' not in r['expected_echo']: err(g, 'F11 echo missing')
    elif r['disposition_reason'] == 'CONTEXT_EXPIRED_CONFIRM': err(g, 'CONTEXT_EXPIRED_CONFIRM without expiry')
    # F15
    if r['m2a_scope'] == 'OUT_RELATIVE_TO_PREVIOUS' and r['expected_disposition'] != 'CLARIFY': err(g, 'F15 must CLARIFY')
    # ops
    for i, o in enumerate(r['ops']):
        w = f'ops[{i}]'
        if o['op'] not in E['op']: err(g, f'{w} op')
        if o['slot'] not in E['slot']: err(g, f'{w} slot')
        if not o['quote'] or o['quote'] not in r['followup_text']: err(g, f'{w} quote not exact substring: {o["quote"]!r}')
        for k in ['refers_to_result_set', 'depends_on_prior_value']:
            if not isinstance(o[k], bool): err(g, f'{w} {k} not bool')
        inh = o['inherited']
        if inh['year_source'] not in E['year_source'] or inh['unit_source'] not in E['unit_source']: err(g, f'{w} inherited enum')
        base = o['slot'].split('.')[0]
        if o['op'] == 'CLEAR':
            if o['operator'] is not None or o['polarity'] is not None: warn(g, f'{w} CLEAR with operator/polarity')
            if o['slot'] in ('VALUE.lower', 'VALUE.upper'): err(g, f'{w} §2.3 CLEAR must use slot VALUE')
        else:
            if base in OPS_BY_DIM and o['operator'] not in OPS_BY_DIM[base] and not (o['op'] == 'KEEP' and o['operator'] is None):
                err(g, f'{w} operator {o["operator"]} not valid for {base}')
            if o['polarity'] not in ('POSITIVE', 'NEGATIVE') and base not in ('UNIVERSE',): err(g, f'{w} polarity')
            if o['value_hint'] in (None, '') and o['op'] != 'SET_ORDER': err(g, f'{w} value_hint empty')
            if base == 'GEOGRAPHY' and o['level'] not in E['level']: err(g, f'{w} GEOGRAPHY level')
            if base != 'GEOGRAPHY' and o['level'] is not None: err(g, f'{w} level outside GEOGRAPHY')
            clar = r['expected_disposition'] == 'CLARIFY'
            if base == 'TIME' and inh['year_source'] is None and not clar: err(g, f'{w} TIME without year_source')
            if base == 'VALUE' and inh['unit_source'] is None and not clar: err(g, f'{w} VALUE without unit_source')
        if base != 'TIME' and inh['year_source'] is not None: err(g, f'{w} year_source outside TIME')
        if base != 'VALUE' and inh['unit_source'] is not None: err(g, f'{w} unit_source outside VALUE')
        if o['op'] == 'KEEP' and not o['depends_on_prior_value'] and o['slot'] != 'UNIVERSE': warn(g, f'{w} KEEP without depends_on_prior_value')
        if o['op'] == 'SET_ORDER' and o['slot'] != 'ORDER': err(g, f'{w} SET_ORDER slot')
        if o['op'] == 'SET_UNIVERSE' and o['slot'] != 'UNIVERSE': err(g, f'{w} SET_UNIVERSE slot')
    # §2.3 range convention
    vops = [o for o in r['ops'] if o['slot'].startswith('VALUE') and o['op'] == 'SET']
    by_quote = {}
    for o in vops: by_quote.setdefault(o['quote'], []).append(o)
    for q, os_ in by_quote.items():
        is_range = bool(re.match(r'^(entre|de \d)', q)) and (' y ' in q or ' a ' in q) or re.search(r'\d\s*-\s*\d', q)
        if is_range:
            sl = sorted(o['slot'] for o in os_)
            if sl != ['VALUE.lower', 'VALUE.upper']: err(g, f'§2.3 range {q!r} must be exactly lower+upper')
            for o in os_:
                exp = 'greater_or_equal' if o['slot'] == 'VALUE.lower' else 'less_or_equal'
                if o['operator'] != exp: err(g, f'§2.3 range {q!r} {o["slot"]} operator {o["operator"]}')
        elif len(os_) > 1 and sorted(o['slot'] for o in os_) != ['VALUE.lower', 'VALUE.upper']:
            err(g, f'§2.3 duplicated VALUE op on quote {q!r}')
    for o in r['ops']:
        if o['op'] == 'CLEAR' and o['slot'] == 'VALUE':
            if sum(1 for x in r['ops'] if x['op'] == 'CLEAR' and x['slot'].startswith('VALUE')) != 1: err(g, '§2.3 one CLEAR VALUE only')
    # resulting_ir
    if r['resulting_ir'] is None:
        if r['expected_disposition'] == 'EXECUTE': err(g, 'resulting_ir null with EXECUTE')
        warn(g, 'resulting_ir null')
    else:
        check_ir(g, r['resulting_ir'], 'resulting_ir')
        if r['expected_disposition'] == 'EXECUTE' and r['resulting_ir']['operation'] == 'SEARCH':
            for cnd in r['resulting_ir']['conditions']:
                if str(cnd['value_hint']).startswith('UNRESOLVED') and r['turn_kind'] != 'ANSWER_TO_CLARIFICATION':
                    err(g, f'EXECUTE with UNRESOLVED value {cnd["id"]}')
    if p['ir'] is not None: check_ir(g, p['ir'], 'prior.ir (read-only check)')
    for m in re.findall(r'NEW_GAP:\s*([A-Z0-9_]+)', r['notes']):
        new_gaps.setdefault(m, []).append(g)

res = {'validator': 'synth_labeler_SB/self_check.py', 'records': len(recs), 'errors': errors, 'warnings': warnings,
       'error_count': len(errors), 'warning_count': len(warnings), 'new_gaps': new_gaps,
       'checks': ['162 records, sorted, ids == cases', 'input fields (incl. prior) byte-equal to cases.jsonl',
                  'closed enums (turn_kind, m2a_scope, op, slot, disposition, ambiguity, confidence, effects, echo, gap_codes, inherited, unit, level, product_view)',
                  'quote is exact substring of followup_text', 'operator within dimension table (C0 §5.1.3)',
                  '§2.3: "entre/de X a Y" = exactly VALUE.lower(greater_or_equal)+VALUE.upper(less_or_equal) same quote; CLEAR VALUE single op',
                  '§2.4: year_source on TIME ops, unit_source on VALUE ops (null only with CLARIFY or CLEAR)',
                  '§2.2.7 owner≠FOUNDATION_SEARCH -> OUT_ANCHOR_NOT_SEARCH; LEGACY -> HANDOFF_LEGACY', 'F11 gap>1800s -> CLARIFY CONTEXT_EXPIRED_CONFIRM + echo',
                  'F15 scope -> CLARIFY', 'disposition_reason iff not EXECUTE', 'alternatives iff AMBIGUOUS',
                  'resulting_ir shape: GEOGRAPHY level, unit only on VALUE, CARDINALITY needs ORDER, product_view iff UNSPECIFIED',
                  'effect/echo coherence for VALUE_RANGE_REPLACED, TOPIC_REPLACED, VIEW_REDERIVED']}
json.dump(res, open(OUT, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
print('errors', len(errors), 'warnings', len(warnings))
for e in errors: print('E', e)
for w in warnings[:40]: print('W', w)
print(json.dumps(new_gaps, ensure_ascii=False))
