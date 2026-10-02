"""SB self-check for m2a-followup-gold/2.1 + ADJUDICATED_OVERRIDES A1-A7 (kit_v2_2)."""
import hashlib, json, os, re

HERE = os.path.dirname(os.path.abspath(__file__))
G = os.path.dirname(os.path.dirname(HERE))
CASES = os.path.join(G, 'sintetico_para_etiquetadores_v2_3', 'cases.jsonl')
LABELS = os.path.join(HERE, 'labels_v2_3.jsonl')
OUT = os.path.join(HERE, 'self_check.json')

E = dict(
    turn_kind={'DELTA', 'NEW_SEARCH', 'ANSWER_TO_CLARIFICATION', 'PROTOCOL', 'OTHER_OPERATION', 'UNCLEAR'},
    scope={'IN', 'IN_NEW_SEARCH', 'IN_PROTOCOL', 'OUT_NO_ANCHOR', 'OUT_ANCHOR_NOT_SEARCH', 'OUT_OPERATION_CHANGE',
           'OUT_REFERENT_EXTENSIONAL', 'OUT_RELATIVE_TO_PREVIOUS'},
    disp={'EXECUTE', 'NEW_SEARCH', 'CLARIFY', 'REASK', 'INFORM', 'HANDOFF_LEGACY'},
    op={'SET', 'CLEAR', 'KEEP', 'SET_ORDER', 'SET_UNIVERSE', 'ANSWER', 'CANCEL_PENDING'},
    slot={'TIME', 'VALUE.lower', 'VALUE.upper', 'VALUE', 'GEOGRAPHY', 'BUYER', 'ACTOR', 'TOPIC', 'STATE', 'MODALITY',
          'CONTRACT_TYPE_OR_NATURE', 'ORDER', 'CARDINALITY', 'UNIVERSE', None},
    ys={'STATED', 'PREVIOUS_ACCEPTED_TIME', 'CLOCK_INFERRED', 'RELATIVE_TO_CLOCK', None},
    us={'STATED', 'SIBLING_IN_RANGE', 'PREVIOUS_ACCEPTED_VALUE', None},
    unit={'PESOS', 'MILES', 'MILLONES', 'MIL_MILLONES', 'BILLONES', 'SMMLV', None},
    universe={'CONTRACTS', 'PROCESSES', 'BOTH', 'UNSPECIFIED'}, view={'RECORDS', 'OPPORTUNITIES'},
    pv={None, 'HISTORICAL_PROCESSES', 'CONTRACTS', 'CURRENT_OPPORTUNITIES'}, vs={'USER', 'DERIVED', None},
    level={'DEPARTMENT', 'MUNICIPALITY', 'REGION', 'UNKNOWN'},
    effects={'VALUE_RANGE_REPLACED', 'TOPIC_REPLACED', 'POLARITY_INVERTED', 'VIEW_REDERIVED', 'UNIVERSE_CHANGED', 'TOPN_CARRIED',
             'TOPN_CAPPED', 'ORDER_DIRECTION_DEFAULTED', 'PENDING_CANCELLED', 'CONTEXT_RESET'},
    echo={'CARRIED', 'UNIT_INHERITED', 'UNIT_SHARED_IN_RANGE', 'YEAR_INHERITED', 'YEAR_CLOCK_INFERRED', 'RELATIVE_WINDOW_RESOLVED',
          'VALUE_RANGE_REPLACED', 'TOPIC_REPLACED', 'POLARITY_INVERTED', 'UNIVERSE_CHANGED', 'TOPN_CARRIED', 'TOPN_CAPPED',
          'ORDER_DIRECTION_DEFAULTED', 'CONTEXT_RESET', 'VIEW_DERIVED', 'VIEW_REDERIVED', 'CONTEXT_EXPIRED'},
    # §8 minus UNIT_IMPLAUSIBLE (A1) plus RANGE_NO_INTERIOR (A1)
    gap={'SET', 'PV', 'VCOMP', 'UNIT', 'MONTHYEAR', 'YEARRANGE', 'TTL', 'TOPIC', 'COUNTERPART', 'REGION_VOCAB', 'SMMLV',
         'POLARITY_SAME_VALUE', 'POLARITY_OTHER_VALUE', 'CLEAR_ABSENT', 'EMPTY_AFTER_DELTA', 'TOPN_CAP', 'ORDER_DIRECTION',
         'UNIVERSE_FRAGMENT', 'PENDING_REJECTED', 'ANY_OPTION', 'ANSWER_PLUS_EDIT', 'STATE_UNIVERSE', 'NO_ANCHOR',
         'PRIOR_NOT_EXECUTED', 'GEOBUYER', 'UNIVTIME', 'SELECT', 'ENADELANTE', 'B5RANGE', 'COMPARATOR_FORM',
         'NEGATED_COMPARATOR', 'RELATIVE_ANCHOR', 'OPPORTUNITY_STATE', 'PAGE_VS_TOPN', 'OPSWITCH', 'TOTALES', 'FIRMADOS',
         'ACTORTIME', 'PENDING_OPSWITCH', 'RANGE_NO_INTERIOR'},
)
LIVE_GAPS = {'RANGE_NO_INTERIOR', 'ENADELANTE', 'B5RANGE', 'COMPARATOR_FORM', 'NEGATED_COMPARATOR', 'OPPORTUNITY_STATE', 'PAGE_VS_TOPN'}
REASONS = {('CLARIFY', r) for r in ['CONTEXT_EXPIRED_CONFIRM', 'BUYER_OR_PLACE_AFTER_ACTOR', 'REGION_NOT_IN_LIST', 'RELATIVE_TO_PREVIOUS_VALUE',
            'UNIT_AMBIGUOUS', 'MONTH_YEAR_AMBIGUOUS', 'EMPTY_SEARCH_AFTER_DELTA', 'STATE_NOT_IN_UNIVERSE', 'TERRITORY_VS_CARRIED_BUYER',
            'NO_ANCHOR', 'ANCHOR_UNREADABLE', 'TARGET_NOT_ADDRESSABLE', 'VALUE_WITHOUT_COMPARATOR', 'SMMLV_YEAR_AMBIGUOUS']} | \
          {('REASK', r) for r in ['OPTION_NOT_CHOSEN', 'ANSWER_EDITS_OTHER_SLOT', 'PENDING_UNANSWERED', 'ANSWER_UNRESOLVED']} | \
          {('INFORM', r) for r in ['CLEAR_ON_EMPTY_SLOT', 'REFERENT_EXTENSIONAL']}
PREFIX = {('CLARIFY', 'UNRESOLVED_VALUE:'), ('INFORM', 'OPERATION_CHANGE:'), ('HANDOFF_LEGACY', 'ANCHOR_NOT_SEARCH:')}
RETIRED = {'SMMLV_UNSUPPORTED', 'INHERITED_UNIT_IMPLAUSIBLE', 'TOPN_CAP_EXCEEDED', 'SELF_CONTAINED_REQUEST', 'REFERENT_UNRESOLVED',
           'MONTH_YEAR_MULTI_YEAR_PRIOR', 'MONTH_YEAR_RELATIVE_PRIOR'}
OPS_BY_DIM = {'TOPIC': {'contains', 'whole_token_folded'}, 'BUYER': {'is'}, 'ACTOR': {'is'}, 'GEOGRAPHY': {'is'},
              'TIME': {'during_year', 'during_month', 'between_dates', 'relative_period'},
              'VALUE': {'greater_than', 'greater_or_equal', 'less_than', 'less_or_equal'}, 'STATE': {'is', 'family'},
              'MODALITY': {'is', 'family'}, 'CONTRACT_TYPE_OR_NATURE': {'is'},
              'ORDER': {'value_desc', 'value_asc', 'date_desc', 'date_asc'}, 'CARDINALITY': {'top_n'}}
IR_KEYS = {'operation', 'aspect', 'universe', 'view', 'product_view', 'view_source', 'conditions', 'logic', 'order', 'cardinality', 'host_notice'}
INPUT = ['schema', 'gold_id', 'slice', 'followup_text', 'followup_sha256', 'source_kind', 'input_kind', 'prior']
LABEL = ['turn_kind', 'm2a_scope', 'ops', 'resulting_ir', 'alternatives', 'expected_disposition', 'disposition_reason', 'effects',
         'expected_echo', 'ambiguity', 'confidence', 'gap_codes', 'labeler', 'notes']
SCALE = {'PESOS': 1, 'MILES': 1e3, 'MILLONES': 1e6, 'MIL_MILLONES': 1e9, 'BILLONES': 1e12}

errors, warnings = [], []
def err(g, m): errors.append(f'{g}: {m}')
def warn(g, m): warnings.append(f'{g}: {m}')

def check_ir(g, ir, w):
    if set(ir) != IR_KEYS: err(g, f'{w}: keys {sorted(set(ir) ^ IR_KEYS)}')
    if ir['universe'] not in E['universe'] or ir['view'] not in E['view'] or ir['product_view'] not in E['pv'] or ir['view_source'] not in E['vs']:
        err(g, f'{w}: universe/view enums')
    if ir['operation'] == 'SEARCH':
        if ir['aspect'] is not None: err(g, f'{w}: aspect with SEARCH')
        if (ir['universe'] == 'UNSPECIFIED') != (ir['product_view'] is not None): err(g, f'{w}: product_view iff UNSPECIFIED')
        if ir['universe'] == 'UNSPECIFIED' and ir['view_source'] is None: err(g, f'{w}: UNSPECIFIED without view_source')
        if ir['universe'] != 'UNSPECIFIED' and ir['view_source'] != 'USER': err(g, f'{w}: said universe needs view_source USER')
    ids = set()
    for c in ir['conditions']:
        if set(c) != {'id', 'dimension', 'operator', 'polarity', 'value_hint', 'level', 'unit'}: err(g, f'{w}: cond keys')
        if c['id'] in ids: err(g, f'{w}: dup id')
        ids.add(c['id'])
        if c['operator'] not in OPS_BY_DIM.get(c['dimension'], ()): err(g, f'{w}: operator {c["operator"]}/{c["dimension"]}')
        if (c['dimension'] == 'GEOGRAPHY') != (c['level'] is not None): err(g, f'{w}: level iff GEOGRAPHY')
        if c['unit'] not in E['unit'] or (c['dimension'] != 'VALUE' and c['unit'] is not None): err(g, f'{w}: unit')
        if c['dimension'] == 'VALUE' and c['unit'] is None: err(g, f'{w}: VALUE without unit')
    o = ir['order']
    if o is not None and (set(o) != {'operator', 'direction_source'} or o['operator'] not in OPS_BY_DIM['ORDER'] or o['direction_source'] not in ('STATED', 'DEFAULTED')):
        err(g, f'{w}: order shape')
    k = ir['cardinality']
    if k is not None:
        if set(k) != {'operator', 'n', 'n_requested'} or not (1 <= k['n'] <= 100) or k['n'] != min(k['n_requested'], 100): err(g, f'{w}: cardinality')
        if o is None: err(g, f'{w}: cardinality without order')
    # A1/F2: never lower >= upper (same comparable scale)
    lo = [c for c in ir['conditions'] if c['dimension'] == 'VALUE' and c['polarity'] == 'POSITIVE' and c['operator'].startswith('greater')]
    up = [c for c in ir['conditions'] if c['dimension'] == 'VALUE' and c['polarity'] == 'POSITIVE' and c['operator'].startswith('less')]
    def num(c):
        m = re.match(r'^([\d.]+)', c['value_hint'].replace('.', '') if c['unit'] == 'PESOS' else c['value_hint'])
        return float(m.group(1)) * SCALE[c['unit']] if m and c['unit'] in SCALE else None
    for a in lo:
        for b in up:
            if num(a) is not None and num(b) is not None and (num(a) > num(b) or (num(a) == num(b) and (a['operator'] == 'greater_than' or b['operator'] == 'less_than'))):
                err(g, f'{w}: VALUE empty interval (A8 criterion)')
            elif num(a) is not None and num(b) is not None and num(a) == num(b) and w == 'resulting_ir' and 'singleton explícito' not in r['notes']:
                err(g, f'{w}: VALUE single point generated by follow-up (A8.1)')

recs = [json.loads(l) for l in open(LABELS, encoding='utf-8')]
cases = {json.loads(l)['gold_id']: json.loads(l) for l in open(CASES, encoding='utf-8')}
ids = [r['gold_id'] for r in recs]
if len(recs) != 191 or ids != sorted(ids) or set(ids) != set(cases): err('*', 'record set / order')

for r in recs:
    globals()['r'] = r
    g = r['gold_id']; c = cases[g]; p = r['prior']; d = r['expected_disposition']; why = r['disposition_reason']; sc = r['m2a_scope']
    for k in INPUT:
        if json.dumps(r[k], sort_keys=True, ensure_ascii=False) != json.dumps(c[k], sort_keys=True, ensure_ascii=False): err(g, f'input {k} altered')
    if list(r) != INPUT + LABEL: err(g, 'field set/order')
    if hashlib.sha256(r['followup_text'].encode()).hexdigest() != r['followup_sha256']: warn(g, 'sha256 mismatch (input)')
    if r['labeler'] != 'SB': err(g, 'labeler')
    for f, en in [('turn_kind', 'turn_kind'), ('m2a_scope', 'scope'), ('expected_disposition', 'disp')]:
        if r[f] not in E[en]: err(g, f'{f} enum')
    if r['ambiguity'] not in ('CLEAR', 'AMBIGUOUS') or r['confidence'] not in ('HIGH', 'MEDIUM', 'LOW'): err(g, 'ambiguity/confidence')
    for x in r['gap_codes']:
        if x not in E['gap']: err(g, f'gap {x}')
    if set(r['gap_codes']) & LIVE_GAPS and r['ambiguity'] != 'AMBIGUOUS': err(g, 'live gap without AMBIGUOUS')
    for x in r['effects']:
        if x not in E['effects']: err(g, f'effect {x}')
    for x in r['expected_echo']:
        if x not in E['echo']: err(g, f'echo {x}')
    # reasons
    if d in ('EXECUTE', 'NEW_SEARCH'):
        if why is not None: err(g, 'reason must be null')
    else:
        if why in RETIRED: err(g, f'retired reason {why}')
        if (d, why) not in REASONS and not any(d == dd and str(why).startswith(px) for dd, px in PREFIX): err(g, f'reason {d}/{why} not in closed list')
    # §5.2 precedence
    tk = r['turn_kind']; ev = r['input_kind'] == 'EVENT'
    if p['state'] in ('NONE', 'UNREADABLE') and tk != 'NEW_SEARCH': exp = 'OUT_NO_ANCHOR'
    elif p['owner'].startswith('LEGACY:'): exp = 'OUT_ANCHOR_NOT_SEARCH'
    elif tk == 'PROTOCOL' or ev: exp = 'IN_PROTOCOL'
    elif tk == 'OTHER_OPERATION' and sc == 'OUT_OPERATION_CHANGE': exp = 'OUT_OPERATION_CHANGE'
    elif sc == 'OUT_REFERENT_EXTENSIONAL': exp = sc
    elif sc == 'OUT_RELATIVE_TO_PREVIOUS': exp = sc
    elif tk == 'NEW_SEARCH': exp = 'IN_NEW_SEARCH'
    else: exp = 'IN'
    if sc != exp: err(g, f'§5.2 scope {sc} expected {exp}')
    scope_disp = {'OUT_NO_ANCHOR': {'CLARIFY'}, 'OUT_ANCHOR_NOT_SEARCH': {'HANDOFF_LEGACY'}, 'OUT_OPERATION_CHANGE': {'INFORM'},
                  'OUT_REFERENT_EXTENSIONAL': {'INFORM'}, 'OUT_RELATIVE_TO_PREVIOUS': {'CLARIFY'}, 'IN_NEW_SEARCH': {'NEW_SEARCH'}}
    if sc in scope_disp and d not in scope_disp[sc]: err(g, f'disposition {d} for {sc}')
    if d == 'HANDOFF_LEGACY' and not p['owner'].startswith('LEGACY:'): err(g, 'HANDOFF_LEGACY without legacy owner')
    if sc == 'OUT_RELATIVE_TO_PREVIOUS' and why != 'RELATIVE_TO_PREVIOUS_VALUE': err(g, 'F15/A5 reason')
    # resulting_ir nullness §5.2
    rir = r['resulting_ir']
    must_null = sc in ('OUT_NO_ANCHOR', 'OUT_OPERATION_CHANGE', 'OUT_REFERENT_EXTENSIONAL', 'OUT_RELATIVE_TO_PREVIOUS') or \
        (sc == 'IN' and (d in ('REASK', 'INFORM') or (d == 'CLARIFY' and why != 'CONTEXT_EXPIRED_CONFIRM'))) or \
        (sc == 'IN_PROTOCOL' and d == 'INFORM')
    if must_null and rir is not None: err(g, 'resulting_ir must be null (§5.2)')
    if not must_null and rir is None: err(g, 'resulting_ir must not be null (§5.2)')
    if rir is not None: check_ir(g, rir, 'resulting_ir')
    for i, a in enumerate(r['alternatives']): check_ir(g, a, f'alternatives[{i}]')
    if why in ('BUYER_OR_PLACE_AFTER_ACTOR', 'TERRITORY_VS_CARRIED_BUYER') and len(r['alternatives']) != 2: err(g, 'F12/F30 needs 2 alternatives')
    # F11
    if p['gap_seconds'] is not None and p['gap_seconds'] > 1800 and sc == 'IN' and why != 'CONTEXT_EXPIRED_CONFIRM': err(g, 'F11')
    if why == 'CONTEXT_EXPIRED_CONFIRM' and not (p['gap_seconds'] or 0) > 1800: err(g, 'CONTEXT_EXPIRED without expiry')
    # ops
    ops = r['ops']
    for i, o in enumerate(ops):
        w = f'ops[{i}]'
        if set(o) != {'op', 'slot', 'operator', 'polarity', 'level', 'quote', 'value_hint', 'unresolved', 'refers_to_result_set',
                      'depends_on_prior_value', 'inherited'}: err(g, f'{w} keys')
        if o['op'] not in E['op'] or o['slot'] not in E['slot']: err(g, f'{w} op/slot enum')
        if (o['slot'] is None) != (o['op'] == 'CANCEL_PENDING'): err(g, f'{w} O2 slot null iff CANCEL_PENDING')
        if o['quote'] is None:
            if not ev: err(g, f'{w} quote null outside EVENT')
        elif o['quote'] not in r['followup_text']: err(g, f'{w} quote not substring: {o["quote"]!r}')
        base = (o['slot'] or '').split('.')[0]
        inh = o['inherited']
        if inh['year_source'] not in E['ys'] or inh['unit_source'] not in E['us']: err(g, f'{w} inherited enum')
        if inh['year_source'] and not (base == 'TIME' and o['op'] in ('SET', 'ANSWER')): err(g, f'{w} O6 year_source scope')
        if inh['unit_source'] and not (base == 'VALUE' and o['op'] in ('SET', 'ANSWER')): err(g, f'{w} O7 unit_source scope')
        if o['op'] in ('SET', 'ANSWER') and base == 'TIME' and not inh['year_source'] and not o['unresolved']: err(g, f'{w} TIME without year_source')
        if o['op'] in ('SET', 'ANSWER') and base == 'VALUE' and not inh['unit_source'] and not o['unresolved']: err(g, f'{w} VALUE without unit_source')
        if o['op'] == 'KEEP' and (inh['year_source'] or inh['unit_source']): err(g, f'{w} O5 KEEP inherited')
        if o['op'] not in ('CLEAR', 'CANCEL_PENDING', 'SET_UNIVERSE', 'KEEP') and base in OPS_BY_DIM and o['operator'] not in OPS_BY_DIM[base]:
            err(g, f'{w} operator')
        if o['op'] == 'CLEAR' and o['slot'] in ('VALUE.lower', 'VALUE.upper'): err(g, f'{w} O4 CLEAR uses VALUE')
        if inh['year_source'] == 'RELATIVE_TO_CLOCK' and o['operator'] != 'relative_period': err(g, f'{w} O6 RELATIVE_TO_CLOCK')
        if o['operator'] == 'relative_period' and inh['year_source'] != 'RELATIVE_TO_CLOCK': err(g, f'{w} relative_period source')
        if inh['unit_source'] == 'PREVIOUS_ACCEPTED_VALUE' and p['ir']:
            units = {x['unit'] for x in p['ir']['conditions'] if x['dimension'] == 'VALUE'}
            if len(units) != 1: err(g, f'{w} F5: prior VALUE units {units}')
        if inh['year_source'] == 'PREVIOUS_ACCEPTED_TIME' and p['ir']:
            t = [x for x in p['ir']['conditions'] if x['dimension'] == 'TIME' and x['polarity'] == 'POSITIVE']
            if len(t) != 1: err(g, f'{w} F7/A5: needs exactly one TIME referent')
            elif t[0]['operator'] != 'during_year' and 'RELATIVE_ANCHOR' not in r['gap_codes']: err(g, f'{w} F7 single-year prior')
        if o['unresolved'] and d in ('EXECUTE', 'NEW_SEARCH'): err(g, f'{w} unresolved with {d}')
    if d in ('CLARIFY', 'REASK', 'INFORM') and sc == 'IN' and why not in ('CONTEXT_EXPIRED_CONFIRM', 'SMMLV_YEAR_AMBIGUOUS') and not any(o['unresolved'] for o in ops):
        err(g, '§5.2: blocking op must be unresolved')
    # O4 ranges
    byq = {}
    for o in ops:
        if o['op'] == 'SET' and o['slot'].startswith('VALUE'): byq.setdefault(o['quote'], []).append(o)
    for q, os_ in byq.items():
        if re.match(r'^(entre\b|de \d)', q or ''):
            if sorted(x['slot'] for x in os_) != ['VALUE.lower', 'VALUE.upper'] or any(
                    x['operator'] != ('greater_or_equal' if x['slot'] == 'VALUE.lower' else 'less_or_equal') for x in os_):
                err(g, f'O4 range {q!r}')
    # O13 / O15
    if tk == 'NEW_SEARCH' and any(o['op'] != 'CANCEL_PENDING' for o in ops): err(g, 'O13 NEW_SEARCH ops')
    if (tk == 'PROTOCOL' or sc in ('OUT_OPERATION_CHANGE', 'OUT_REFERENT_EXTENSIONAL')) and ops: err(g, 'O15 ops must be []')
    if ev and tk == 'ANSWER_TO_CLARIFICATION' and not (len(ops) == 1 and ops[0]['op'] == 'ANSWER' and ops[0]['quote'] is None): err(g, 'O11')
    # pending rules
    if p['state'] == 'PENDING' and p['gap_seconds'] <= 1800 and sc == 'IN':
        if not any(o['op'] in ('ANSWER', 'CANCEL_PENDING') for o in ops) and why != 'PENDING_UNANSWERED': err(g, 'F29')
        if any(o['op'] == 'ANSWER' for o in ops) and any(o['op'] in ('SET', 'CLEAR') for o in ops) and why != 'ANSWER_EDITS_OTHER_SLOT': err(g, 'F25')
    if tk == 'ANSWER_TO_CLARIFICATION' and p['state'] != 'PENDING': err(g, 'ANSWER without PENDING')
    # F18/F19/F20/F21/F28
    if len(ops) == 1 and ops[0]['op'] == 'CLEAR' and p['ir'] and ops[0]['slot'] not in ('ORDER', 'CARDINALITY', 'UNIVERSE') and \
            not any(x['dimension'] == ops[0]['slot'].split('.')[0] for x in p['ir']['conditions']) and why != 'CLEAR_ON_EMPTY_SLOT': err(g, 'F18')
    if rir is not None and rir['operation'] == 'SEARCH' and not rir['conditions']: err(g, 'F19: empty search')
    if rir is not None and rir['cardinality'] and rir['cardinality']['n_requested'] > 100 and d != 'EXECUTE': err(g, 'F20')
    if any(o['op'] == 'SET_ORDER' and o['quote'] and o['quote'].strip() == 'por fecha' for o in ops) and rir and rir['order']['direction_source'] != 'DEFAULTED':
        err(g, 'F21')
    # A2 SMMLV
    if rir is not None:
        sm = any(x['unit'] == 'SMMLV' for x in rir['conditions'] if x['dimension'] == 'VALUE')
        t = [x for x in rir['conditions'] if x['dimension'] == 'TIME' and x['polarity'] == 'POSITIVE']
        if sm and d == 'EXECUTE' and not (len(t) == 1 and t[0]['operator'] == 'during_year'): err(g, 'A2: SMMLV executed without single year')
    if why == 'SMMLV_YEAR_AMBIGUOUS' and p['ir'] and not any(x.get('unit') == 'SMMLV' for x in p['ir']['conditions']): err(g, 'A2 reason without SMMLV')
    if 'SMMLV' in r['gap_codes'] and why == 'SMMLV_YEAR_AMBIGUOUS':
        t = [x for x in p['ir']['conditions'] if x['dimension'] == 'TIME']
        if not t and r['ambiguity'] != 'AMBIGUOUS': err(g, 'A2: SMMLV without TIME must be AMBIGUOUS')
    if 'RELATIVE_ANCHOR' in r['gap_codes'] and sc not in ('IN', 'OUT_RELATIVE_TO_PREVIOUS'): err(g, 'A5 scope')
    # echo coherence (§7)
    if d not in ('EXECUTE', 'NEW_SEARCH') and why != 'CONTEXT_EXPIRED_CONFIRM' and r['expected_echo']: err(g, '§7 echo must be []')
    if why == 'CONTEXT_EXPIRED_CONFIRM' and r['expected_echo'] != ['CONTEXT_EXPIRED']: err(g, '§7 expired echo')
    for e in ['VALUE_RANGE_REPLACED', 'TOPIC_REPLACED', 'POLARITY_INVERTED', 'UNIVERSE_CHANGED', 'TOPN_CARRIED', 'TOPN_CAPPED', 'ORDER_DIRECTION_DEFAULTED', 'CONTEXT_RESET']:
        if d in ('EXECUTE', 'NEW_SEARCH') and (e in r['effects']) != (e in r['expected_echo']): err(g, f'§7 {e} effect/echo')
    if 'RANGE_NO_INTERIOR' in r['gap_codes'] and r['ambiguity'] != 'AMBIGUOUS': err(g, 'A1 RANGE_NO_INTERIOR must be AMBIGUOUS')
    if 'adjudicated_by: ["A8.1"]' in r['notes']:
        if r['ambiguity'] != 'CLEAR' or 'RANGE_NO_INTERIOR' in r['gap_codes'] or d != 'EXECUTE' or 'VALUE_RANGE_REPLACED' not in r['effects']:
            err(g, 'A8.1 case must be EXECUTE/CLEAR, no RANGE_NO_INTERIOR, with VALUE_RANGE_REPLACED')
    if 'adjudicated_by: ["A9.1"]' in r['notes'] and tk != 'DELTA': err(g, 'A9.1 => DELTA')
    if 'adjudicated_by: ["A8"]' in r['notes']:
        if r['ambiguity'] != 'CLEAR' or 'RANGE_NO_INTERIOR' in r['gap_codes'] or d != 'EXECUTE' or 'VALUE_RANGE_REPLACED' not in r['effects']:
            err(g, 'A8 adjudicated case must be EXECUTE/CLEAR, no RANGE_NO_INTERIOR, with VALUE_RANGE_REPLACED')

res = {'validator': 'etiquetas_SB_v2_3/self_check_v2_3.py', 'schema': 'm2a-followup-gold/2.1 + A1-A9.1', 'records': len(recs),
       'error_count': len(errors), 'warning_count': len(warnings), 'errors': errors, 'warnings': warnings,
       'checks': ['A8.1: ningún resulting_ir con cota inferior = superior generado por follow-up; casos A8.1 => EXECUTE/CLEAR/VALUE_RANGE_REPLACED; A9.1 => DELTA', 'A8: intervalo vacío = inferior > superior, o iguales con algún operador exclusivo; casos adjudicados por A8 => EXECUTE/CLEAR/VALUE_RANGE_REPLACED', '191 registros ordenados; campos de entrada byte-idénticos; orden de campos; labeler SB',
                  'listas cerradas §1, §3, §4, §6, §7, §8 (sin UNIT_IMPLAUSIBLE, con RANGE_NO_INTERIOR), §5.3 (sin SMMLV_UNSUPPORTED / INHERITED_UNIT_IMPLAUSIBLE, con SMMLV_YEAR_AMBIGUOUS)',
                  'precedencia de m2a_scope §5.2 y disposición por alcance; HANDOFF_LEGACY sólo con dueño legacy',
                  'nulidad de resulting_ir §5.2; forma de IR §3 (claves cerradas, unidad, nivel, orden, cardinalidad n<=100)',
                  'ops O2/O4/O5/O6/O7/O11/O13/O15; quote subcadena exacta (null sólo EVENT); unresolved en la operación que bloquea',
                  'F5 (unidad única), F7/A5 (un referente TIME), F11, F18, F19, F20, F21, F25, F29, A1 (sin inferior>=superior; RANGE_NO_INTERIOR => AMBIGUOUS), A2 (SMMLV ejecuta sólo con año único)',
                  'gaps vivos => AMBIGUOUS; coherencia efecto/eco §7; F12/F30 con dos alternativas']}
json.dump(res, open(OUT, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
print('errors', len(errors), 'warnings', len(warnings))
for e in errors: print('E', e)
for w in warnings: print('W', w)
