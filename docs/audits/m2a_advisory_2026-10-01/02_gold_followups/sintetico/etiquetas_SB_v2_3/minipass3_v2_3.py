"""SB mini-pass 3 (A8.1, A9.1): only SF091, SF095, SF134, SF187. Others byte-identical to v2.2."""
import copy, json, os

HERE = os.path.dirname(os.path.abspath(__file__))
SIN = os.path.dirname(HERE)
src = open(os.path.join(SIN, 'etiquetas_SB_v2_1', 'build_labels_v2_1.py'), encoding='utf-8').read()
ns = {}; exec(src[src.index('# ---------------- derived effects'):src.index('# ---------------- emit')], ns); derive = ns['derive']

lines = open(os.path.join(SIN, 'etiquetas_SB_v2_2', 'labels_v2_2.jsonl'), encoding='utf-8').read().splitlines()
recs = [json.loads(l) for l in lines]; R = {r['gold_id']: r for r in recs}; BEFORE = copy.deepcopy(R)
LOG = {}

def P(g): return copy.deepcopy(R[g]['prior']['ir'])
def nid(ir): return 'c%d' % (max([int(c['id'][1:]) for c in ir['conditions']] or [0]) + 1)
def add(ir, dim, op, vh, unit=None):
    ir['conditions'].append({'id': nid(ir), 'dimension': dim, 'operator': op, 'polarity': 'POSITIVE', 'value_hint': vh, 'level': None, 'unit': unit}); return ir
def drop(ir, dim): ir['conditions'] = [c for c in ir['conditions'] if c['dimension'] != dim]; return ir

def a81(g, op, vh, calc):
    ir = add(drop(P(g), 'VALUE'), 'VALUE', op, vh, 'MILLONES')
    R[g].update(resulting_ir=ir, alternatives=[], ambiguity='CLEAR', confidence='HIGH',
                gap_codes=[x for x in R[g]['gap_codes'] if x != 'RANGE_NO_INTERIOR'],
                notes=f'A8.1 — {calc} => intervalo de un solo punto GENERADO por el follow-up (el usuario no pidió un valor exacto): '
                      'cuenta como contradictorio; la cota nueva sustituye la restricción VALUE completa, anunciado (VALUE_RANGE_REPLACED). adjudicated_by: ["A8.1"]')
    LOG[g] = ('A8.1', calc)

a81('SF091', 'less_or_equal', '300 millones',
    'cota previa: inferior >= 300 millones (greater_or_equal, inclusiva, MILLONES); follow-up «hasta 300 millones»: superior <= 300 millones '
    '(less_or_equal, inclusiva, unidad dicha MILLONES); conservar la opuesta daría [300, 300]')
a81('SF095', 'greater_or_equal', '200 millones',
    'cota previa: superior <= 200 millones (less_or_equal, inclusiva, MILLONES); follow-up «desde 200 millones»: inferior >= 200 millones '
    '(greater_or_equal, inclusiva por D4, unidad dicha MILLONES); conservar la opuesta daría [200, 200]')

# SF134: A9.1 no aplica (TOPIC nuevo) -> rige A9 -> NEW_SEARCH (sin cambio de etiqueta)
R['SF134']['notes'] = ('A9.1 — leído sin historial, «contratos de vigilancia» trae UNIVERSE (CONTRACTS) + TOPIC «vigilancia». El ancla tiene '
                       'TOPIC «papelería»: «vigilancia» es un TOPIC NUEVO, así que A9.1 no aplica y rige A9: el turno forma por sí solo una SEARCH '
                       'admisible y no lleva marca de continuación => NEW_SEARCH. F22 no aplica (no es tema desnudo: trae universo).')
LOG['SF134'] = ('A9.1 no aplica -> A9', 'dimensiones sin historial: UNIVERSE + TOPIC; TOPIC nuevo (vigilancia ≠ papelería) => A9 => NEW_SEARCH')

# SF187: A9.1 aplica -> DELTA
ir = P('SF187'); ir.update(universe='CONTRACTS', view='RECORDS', product_view=None, view_source='USER'); add(ir, 'TIME', 'during_year', '2024')
op = lambda o, slot, q, operator, vh, ys=None: {'op': o, 'slot': slot, 'operator': operator, 'polarity': 'POSITIVE', 'level': None, 'quote': q,
     'value_hint': vh, 'unresolved': False, 'refers_to_result_set': False, 'depends_on_prior_value': False,
     'inherited': {'year_source': ys, 'unit_source': None}}
R['SF187'].update(turn_kind='DELTA', m2a_scope='IN', ops=[op('SET_UNIVERSE', 'UNIVERSE', 'contratos', None, 'CONTRACTS'),
                  op('SET', 'TIME', 'de 2024', 'during_year', '2024', 'STATED')],
                  resulting_ir=ir, alternatives=[], expected_disposition='EXECUTE', disposition_reason=None, ambiguity='CLEAR', confidence='HIGH',
                  gap_codes=['UNIVERSE_FRAGMENT'],
                  notes='A9.1 — leído sin historial, «contratos de 2024» trae sólo UNIVERSE (CONTRACTS) + TIME (2024): ni TOPIC nuevo, ni ACTOR nuevo, '
                        'ni operación nueva, con una SEARCH activa como ancla => DELTA aunque pase la prueba aislada de A9. Se conservan tema '
                        '(pavimentación) y comprador (Alcaldía de Montería) por F1. adjudicated_by: ["A9.1"]')
LOG['SF187'] = ('A9.1', 'dimensiones sin historial: UNIVERSE + TIME; sin TOPIC/ACTOR/operación nuevos => DELTA')

for g in LOG:
    e, ec = derive(R[g]); R[g]['effects'] = e; R[g]['expected_echo'] = ec
out_lines = [l if r['gold_id'] not in LOG else json.dumps(R[r['gold_id']], ensure_ascii=False) for l, r in zip(lines, recs)]
open(os.path.join(HERE, 'labels_v2_3.jsonl'), 'w', encoding='utf-8').write('\n'.join(out_lines) + '\n')
assert sum(1 for a, b in zip(lines, out_lines) if a != b) <= 4

def short(r):
    ir = r['resulting_ir']
    irs = 'null' if ir is None else ir['universe'] + ': ' + '; '.join(f"{c['dimension']} {c['operator']} {c['value_hint']}" for c in ir['conditions'])
    return f"{r['turn_kind']} · {r['expected_disposition']} · {r['ambiguity']} · effects {r['effects']} · IR {irs}"
md = ['# Mini-pasada 3 SB (v2.3) — A8.1 y A9.1', '',
      'Base: `etiquetas_SB_v2_2/labels_v2_2.jsonl`. Sólo se revisan SF091, SF095, SF134 y SF187; las otras 187 líneas son byte-idénticas a v2.2.', '',
      '| id | regla | cálculo / razonamiento | ¿cambia la disposición? | ¿cambia la IR? | antes (v2.2) | ahora (v2.3) |', '|---|---|---|---|---|---|---|']
for g in sorted(LOG):
    a, b = BEFORE[g], R[g]
    md.append(f"| {g} | {LOG[g][0]} | {LOG[g][1]} | {'sí' if a['expected_disposition'] != b['expected_disposition'] else 'no'} | "
              f"{'sí' if a['resulting_ir'] != b['resulting_ir'] else 'no'} | {short(a)} | {short(b)} |")
md += ['', 'Validador: `self_check_v2_3.py` -> `self_check.json` (controles de v2.2 + A8.1: ningún `resulting_ir` con un único punto generado por '
       'follow-up; casos A8.1 => EXECUTE/CLEAR/VALUE_RANGE_REPLACED; A9.1 => DELTA).']
open(os.path.join(HERE, 'MINIPASS3_REPORT.md'), 'w', encoding='utf-8').write('\n'.join(md) + '\n')
print('\n'.join(md))
