"""SB mini-pass (A8, A9) over labels_v2_1 -> labels_v2_2. Only the listed cases are touched."""
import copy, json, os

HERE = os.path.dirname(os.path.abspath(__file__))
V21 = os.path.join(os.path.dirname(HERE), 'etiquetas_SB_v2_1')
src = open(os.path.join(V21, 'build_labels_v2_1.py'), encoding='utf-8').read()
ns = {}
exec(src[src.index('# ---------------- derived effects'):src.index('# ---------------- emit')], ns)
derive = ns['derive']

recs = [json.loads(l) for l in open(os.path.join(V21, 'labels_v2_1.jsonl'), encoding='utf-8')]
R = {r['gold_id']: r for r in recs}
BEFORE = copy.deepcopy(R)
REVIEW = ['SF051', 'SF053', 'SF063', 'SF090', 'SF091', 'SF094', 'SF095', 'SF099', 'SF103', 'SF126', 'SF132', 'SF134',
          'SF182', 'SF183', 'SF185', 'SF187']
SCAN_ADDED = ['SF170']
LOG = {}

def P(g): return copy.deepcopy(R[g]['prior']['ir'])
def nid(ir): return 'c%d' % (max([int(c['id'][1:]) for c in ir['conditions']] or [0]) + 1)
def add(ir, dim, op, vh, pol='POSITIVE', level=None, unit=None):
    ir['conditions'].append({'id': nid(ir), 'dimension': dim, 'operator': op, 'polarity': pol, 'value_hint': vh, 'level': level, 'unit': unit}); return ir
def drop(ir, dim): ir['conditions'] = [c for c in ir['conditions'] if c['dimension'] != dim]; return ir
def fresh(u, pv=None, vs='USER'):
    return {'operation': 'SEARCH', 'aspect': None, 'universe': u, 'view': 'RECORDS', 'product_view': pv, 'view_source': vs,
            'conditions': [], 'logic': None, 'order': None, 'cardinality': None, 'host_notice': []}

def patch(g, criterion, calc, **kw):
    r = R[g]
    r.update(kw)
    LOG[g] = (criterion, calc)

def a8_replace(g, new_dim_op, vh, unit, calc, extra_gaps=()):
    ir = drop(P(g), 'VALUE'); add(ir, 'VALUE', new_dim_op, vh, unit=unit)
    gaps = [x for x in R[g]['gap_codes'] if x != 'RANGE_NO_INTERIOR'] + [x for x in extra_gaps if x not in R[g]['gap_codes']]
    patch(g, 'A8', calc, resulting_ir=ir, alternatives=[], ambiguity='CLEAR', confidence='HIGH', gap_codes=gaps,
          notes=f'A8 — {calc} => intervalo vacío: la cota nueva sustituye la restricción VALUE completa, anunciado (VALUE_RANGE_REPLACED). adjudicated_by: ["A8"]')

def a8_keep(g, new_op, vh, unit, calc):
    ir = add(P(g), 'VALUE', new_op, vh, unit=unit)
    gaps = [x for x in R[g]['gap_codes'] if x != 'RANGE_NO_INTERIOR']
    patch(g, 'A8', calc, resulting_ir=ir, alternatives=[], ambiguity='CLEAR', confidence='HIGH', gap_codes=gaps,
          notes=f'A8 — {calc} => punto único con ambos operadores inclusivos: NO es vacío; la cota opuesta se conserva (regla general por lado). Decidido por A8 (criterio formal: excepción NO aplicable).')

# ---------------- A8: rango ----------------
a8_replace('SF090', 'less_or_equal', '200 millones', 'MILLONES',
           'opuesta conservada: inferior >= 500 millones (incl.); nueva superior <= 200 millones (incl.); unidad MILLONES (dicha); 500 > 200')
a8_keep('SF091', 'less_or_equal', '300 millones', 'MILLONES',
        'opuesta conservada: inferior >= 300 millones (incl.); nueva superior <= 300 millones (incl.); unidad MILLONES; 300 = 300 y ambos inclusivos')
a8_replace('SF094', 'greater_than', '400 millones', 'MILLONES',
           'opuesta conservada: superior <= 200 millones (incl.); nueva inferior > 400 millones (excl.); unidad MILLONES; 400 > 200')
a8_keep('SF095', 'greater_or_equal', '200 millones', 'MILLONES',
        'opuesta conservada: superior <= 200 millones (incl.); nueva inferior >= 200 millones (incl., «desde» D4); unidad MILLONES; 200 = 200 y ambos inclusivos')
# SF095: the new lower is added next to the kept upper (a8_keep adds it); same for SF091 (upper added next to kept lower).
a8_replace('SF099', 'greater_than', '350 millones', 'MILLONES',
           'la inferior previa (>= 100) se sustituye por su lado; opuesta conservada: superior <= 300 millones (incl.); nueva inferior > 350 millones (excl.); unidad MILLONES; 350 > 300')
a8_replace('SF103', 'greater_or_equal', '350 millones', 'MILLONES',
           'opuesta conservada: superior <= 300 millones (incl.); nueva inferior >= 350 (incl.) con unidad heredada MILLONES (F5: única unidad previa); 350 > 300')
a8_replace('SF182', 'less_or_equal', '90 millones', 'MILLONES',
           'opuesta conservada: inferior >= 150 millones (incl.); nueva superior <= 90 millones (incl.); unidad MILLONES (dicha); 150 > 90')
a8_replace('SF183', 'less_or_equal', '90 millones', 'MILLONES',
           'opuesta conservada: inferior >= 150 millones (incl.); nueva superior <= 90 (incl.) con unidad heredada MILLONES (F5); 150 > 90')
# barrido: SF170 (no estaba marcado)
patch('SF170', 'A8 (barrido)', 'inferior >= 200 millones (incl.) frente a nueva superior <= 900 SMMLV (incl.): MILLONES frente a SMMLV, no comparables según A8',
      ambiguity='AMBIGUOUS', confidence='MEDIUM', gap_codes=['SMMLV', 'RANGE_NO_INTERIOR'],
      notes='A8 — barrido: inferior >= 200 millones (incl., MILLONES) y nueva superior <= 900 SMMLV (incl., SMMLV, dicha). A8 declara no comparables millones frente a SMMLV => no adjudicado, AMBIGUOUS. Mejor lectura sin cambio: se conservan las dos cotas (regla general por lado); con el salario de 2025 (año único, A2) 900 SMMLV supera ampliamente 200 millones, así que el rango tendría interior. Alternativa: CLARIFY. Uso RANGE_NO_INTERIOR como el gap vivo más cercano (no hay código para «montos no comparables»).')
# SF051: F25 REASK se mantiene (A8 no se llega a aplicar)
patch('SF051', 'A9 + A8 (informativo)', '«pero» es marca de continuación (A9) -> no es autosuficiente; respuesta + edición -> F25 REASK. A8 informativo: < 40 millones (excl.) frente a > 150 millones (excl.) sería vacío, pero el turno no se ejecuta.',
      notes=R['SF051']['notes'] + ' Mini-pasada A8/A9: «pero» = marca explícita de continuación => DELTA/respuesta, no búsqueda nueva; F25 se mantiene. A8 sólo informativo (150 > 40: vacío; no se aplica porque la disposición es REASK).')
patch('SF053', 'A9', 'Historial borrado: «los de la Alcaldía de Piedecuesta de enero a junio de 2025» es elíptico («los de» sin núcleo) -> necesita el previo -> DELTA; respuesta + edición de BUYER -> F25 REASK (sin cambio).',
      notes=R['SF053']['notes'] + ' Mini-pasada A9: elipsis («los de» sin sustantivo) => no autosuficiente; sin cambio.')

# ---------------- A9: búsqueda nueva frente a delta ----------------
r = R['SF063']
patch('SF063', 'A9 + A7/F23', 'Historial borrado: X = «dotación de la Gobernación del Cauca en 2025» forma SEARCH admisible (UNSPECIFIED + PV1). «no, mejor» es el rechazo de F23/A7, no una marca de continuación de A9: F23 dice que con consulta completa es búsqueda nueva.',
      alternatives=[], ambiguity='CLEAR', confidence='MEDIUM',
      notes='A9: X pasa la prueba estructural (tema + comprador + año -> UNSPECIFIED, PV1). El rechazo «no, mejor» cancela la pendiente (A7/F23); no lo leo como marca de continuación de A9 («y…», «ahora…», «sólo…», «pero…»). Duda residual: si «mejor» contara como marca de continuación, ganaría DELTA sobre el marco pendiente. CANCEL_PENDING se conserva (O12).')
ir = fresh('UNSPECIFIED', 'HISTORICAL_PROCESSES', 'DERIVED')
add(ir, 'TOPIC', 'contains', 'alquiler de maquinaria pesada'); add(ir, 'TIME', 'during_year', '2023')
patch('SF126', 'A9', 'Historial borrado: «alquiler de maquinaria pesada en 2023» = tema + año -> SEARCH admisible (UNSPECIFIED + PV1); sin marca de continuación; no es tema desnudo (F22 no aplica: trae año).',
      turn_kind='NEW_SEARCH', m2a_scope='IN_NEW_SEARCH', ops=[], resulting_ir=ir, alternatives=[], expected_disposition='NEW_SEARCH',
      disposition_reason=None, ambiguity='CLEAR', confidence='MEDIUM', gap_codes=['UNIVERSE_FRAGMENT'],
      notes='A9: autosuficiente (tema + año, universo no dicho -> PV1 HISTORICAL_PROCESSES) y sin marca de continuación => NEW_SEARCH. F22 sólo cubre el tema desnudo.')
patch('SF132', 'A9', '«y…» es marca explícita de continuación -> DELTA (sin cambio). La ambigüedad que queda no es de A9 sino de significado («licitaciones» = procesos o oportunidades).',
      notes=R['SF132']['notes'] + ' Mini-pasada A9: «y…» => DELTA confirmado; la ambigüedad sigue siendo de significado, no de autosuficiencia.')
patch('SF134', 'A9', 'Historial borrado: «contratos de vigilancia» -> SEARCH admisible (CONTRACTS + TOPIC); sin marca de continuación -> NEW_SEARCH.',
      alternatives=[], ambiguity='CLEAR', confidence='HIGH',
      notes='A9: autosuficiente (universo explícito + tema), sin marca de continuación => NEW_SEARCH; la alternativa delta queda descartada por la prueba.')
patch('SF185', 'A9', 'Historial borrado: «mantenimiento de parques de la Alcaldía de Duitama» -> SEARCH admisible (UNSPECIFIED + PV6); sin marca de continuación; no es tema desnudo (trae comprador).',
      alternatives=[], ambiguity='CLEAR', confidence='MEDIUM',
      notes='A9: autosuficiente (tema + comprador, universo no dicho -> PV6 CURRENT_OPPORTUNITIES), sin marca de continuación => NEW_SEARCH.')
ir = fresh('CONTRACTS'); add(ir, 'TIME', 'during_year', '2024')
patch('SF187', 'A9', 'Historial borrado: «contratos de 2024» -> SEARCH admisible (CONTRACTS + TIME 2024, con una condición, F19 no aplica); sin marca de continuación -> NEW_SEARCH.',
      turn_kind='NEW_SEARCH', m2a_scope='IN_NEW_SEARCH', ops=[], resulting_ir=ir, alternatives=[], expected_disposition='NEW_SEARCH',
      disposition_reason=None, ambiguity='CLEAR', confidence='MEDIUM', gap_codes=['UNIVERSE_FRAGMENT'],
      notes='A9: autosuficiente (universo explícito + año) y sin marca de continuación => NEW_SEARCH (todos los contratos de 2024). Contraste: SF186 «y los contratos de 2024?» lleva «y…» => DELTA.')

# ---------------- derive + emit ----------------
for g in LOG:
    eff, echo = derive(R[g]); R[g]['effects'] = eff; R[g]['expected_echo'] = echo
with open(os.path.join(HERE, 'labels_v2_2.jsonl'), 'w', encoding='utf-8') as fh:
    for r in recs: fh.write(json.dumps(r, ensure_ascii=False) + '\n')
untouched_changed = [r['gold_id'] for r in recs if r['gold_id'] not in LOG and r != BEFORE[r['gold_id']]]
assert not untouched_changed, untouched_changed
assert set(LOG) == set(REVIEW) | set(SCAN_ADDED), set(LOG) ^ (set(REVIEW) | set(SCAN_ADDED))

def short(r):
    ir = r['resulting_ir']
    if ir is None: irs = 'null'
    else:
        irs = f"{ir['universe']}" + (f"/{ir['product_view']}" if ir['product_view'] else '') + ': ' + '; '.join(
            f"{c['dimension']} {c['operator']}{' NEG' if c['polarity'] == 'NEGATIVE' else ''} {c['value_hint']}" for c in ir['conditions'])
    return f"{r['turn_kind']} · {r['expected_disposition']}{'/' + r['disposition_reason'] if r['disposition_reason'] else ''} · {r['ambiguity']} · IR {irs}"
out = ['# Mini-pasada SB v2.2 — A8 (rango contradictorio) y A9 (autosuficiencia estructural)', '',
       'Base: `etiquetas_SB_v2_1/labels_v2_1.jsonl`. Sólo se tocan los 16 casos pedidos más SF170 (hallado en el barrido de VALUE por lado).',
       'Los demás 174 registros son byte-idénticos a v2.1 (lo comprueba `minipass_v2_2.py`). `effects`/`expected_echo` se rederivan con §6/§7.', '',
       '## Barrido A8 sobre VALUE por lado (SF085–SF108, SF170)', '',
       '- Cumplen el criterio de vacío y ya estaban marcados: SF090, SF094, SF099, SF103 (más SF182 y SF183 fuera del rango barrido).',
       '- Punto único con ambos operadores inclusivos (no vacío): SF091, SF095.',
       '- SF100: daría vacío (< 90 frente a >= 100), pero ya lo decide la instrucción explícita «cambia el valor a…» (F2/O4). Sin cambio.',
       '- SF105: > 900 MILES frente a <= 200 MILLONES: comparables (900.000 < 200.000.000), tiene interior. Sin cambio.',
       '- **SF170 (no estaba marcado):** >= 200 MILLONES frente a <= 900 SMMLV: A8 los declara no comparables => AMBIGUOUS. Se incluye.',
       '- Ningún otro caso (SF085–SF089, SF092, SF093, SF096–SF098, SF101, SF102, SF104, SF106–SF108) tiene cotas opuestas en conflicto.', '',
       '## Casos revisados', '', '| id | criterio | ¿cambia la disposición? | ¿cambia la IR? | antes (v2.1) | ahora (v2.2) |', '|---|---|---|---|---|---|']
for g in sorted(LOG):
    a, b = BEFORE[g], R[g]
    dch = (a['expected_disposition'], a['disposition_reason']) != (b['expected_disposition'], b['disposition_reason'])
    ich = a['resulting_ir'] != b['resulting_ir']
    out.append(f"| {g} | {LOG[g][0]} | {'sí' if dch else 'no'} | {'sí' if ich else 'no'} | {short(a)} | {short(b)} |")
out += ['', '## Cálculo o razonamiento por caso', '']
for g in sorted(LOG):
    out.append(f"- **{g}** ({LOG[g][0]}): {LOG[g][1]}")
out += ['', '## Validador', '', '`self_check_v2_2.py` -> `self_check.json`: los mismos controles de v2.1 más el criterio formal de A8 (inferior > superior, o iguales con algún',
        'operador exclusivo, prohibido en `resulting_ir` y en `alternatives`) y la forma de los casos adjudicados por A8 (EXECUTE, CLEAR, sin RANGE_NO_INTERIOR, con VALUE_RANGE_REPLACED).']
open(os.path.join(HERE, 'MINIPASS_REPORT.md'), 'w', encoding='utf-8').write('\n'.join(out) + '\n')
print('\n'.join(out))
