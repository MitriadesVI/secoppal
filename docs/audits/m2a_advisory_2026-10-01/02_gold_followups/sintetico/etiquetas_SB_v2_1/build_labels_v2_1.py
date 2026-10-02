"""SB labeler (blind), round m2a-followup-gold/2.1 (kit_v2_2: A1-A7 + F1-F30 + SCHEMA_v2_1).
Hand-written label per case; effects/expected_echo are DERIVED mechanically (SCHEMA §6/§7)."""
import copy, json, os

HERE = os.path.dirname(os.path.abspath(__file__))
G = os.path.dirname(os.path.dirname(HERE))
CASES = os.path.join(G, 'sintetico_para_etiquetadores_v2_3', 'cases.jsonl')
OLD = os.path.join(G, 'sintetico', 'etiquetas_SB', 'labels_v2.jsonl')
OUT = os.path.join(HERE, 'labels_v2_1.jsonl')

cases, order = {}, []
for line in open(CASES, encoding='utf-8'):
    r = json.loads(line); cases[r['gold_id']] = r; order.append(r['gold_id'])
LAB, CHANGE = {}, {}

# ---------------- helpers ----------------
def O(op, slot, quote, operator=None, pol='POSITIVE', vh=None, level=None, rrs=False, dpv=False, ys=None, us=None, unres=False):
    if op in ('CLEAR', 'CANCEL_PENDING'):
        pol = None
    if op == 'KEEP':
        ys = us = None
    return {'op': op, 'slot': slot, 'operator': operator, 'polarity': pol, 'level': level, 'quote': quote,
            'value_hint': vh, 'unresolved': unres, 'refers_to_result_set': rrs, 'depends_on_prior_value': dpv,
            'inherited': {'year_source': ys, 'unit_source': us}}

def P(gid):
    ir = cases[gid]['prior']['ir']
    return copy.deepcopy(ir) if ir is not None else None

def nid(ir):
    return 'c%d' % (max([int(c['id'][1:]) for c in ir['conditions']] or [0]) + 1)

def add(ir, dim, op, vh, pol='POSITIVE', level=None, unit=None):
    ir['conditions'].append({'id': nid(ir), 'dimension': dim, 'operator': op, 'polarity': pol,
                             'value_hint': vh, 'level': level, 'unit': unit})
    return ir

def rep(ir, dim, **kw):
    hits = [c for c in ir['conditions'] if c['dimension'] == dim]
    assert len(hits) == 1, (dim, ir)
    hits[0].update(kw); return ir

def cid(ir, i, **kw):
    [c for c in ir['conditions'] if c['id'] == i][0].update(kw); return ir

def drop(ir, dim=None, i=None):
    ir['conditions'] = [c for c in ir['conditions'] if not ((dim and c['dimension'] == dim) or (i and c['id'] == i))]
    return ir

def order_(ir, op, src='STATED'):
    ir['order'] = None if op is None else {'operator': op, 'direction_source': src}; return ir

def card(ir, n, req=None):
    ir['cardinality'] = None if n is None else {'operator': 'top_n', 'n': n, 'n_requested': req if req is not None else n}
    return ir

def uni(ir, u, view='RECORDS'):
    ir.update(universe=u, view=view, product_view=None, view_source='USER'); return ir

def fresh(u, view='RECORDS', pv=None, vs='USER'):
    return {'operation': 'SEARCH', 'aspect': None, 'universe': u, 'view': view, 'product_view': pv, 'view_source': vs,
            'conditions': [], 'logic': None, 'order': None, 'cardinality': None, 'host_notice': []}

def L(gid, ops, ir, turn_kind='DELTA', scope='IN', disp='EXECUTE', reason=None, alts=(), amb='CLEAR', conf='HIGH',
      gaps=(), notes='', change=None):
    assert gid not in LAB, gid
    LAB[gid] = dict(turn_kind=turn_kind, m2a_scope=scope, ops=ops, resulting_ir=ir, alternatives=list(alts),
                    expected_disposition=disp, disposition_reason=reason, ambiguity=amb, confidence=conf,
                    gap_codes=list(gaps), labeler='SB', notes=notes)
    if change:
        CHANGE[gid] = change

NS = dict(turn_kind='NEW_SEARCH', scope='IN_NEW_SEARCH', disp='NEW_SEARCH')
EXP = dict(disp='CLARIFY', reason='CONTEXT_EXPIRED_CONFIRM')
F15 = dict(scope='OUT_RELATIVE_TO_PREVIOUS', disp='CLARIFY', reason='RELATIVE_TO_PREVIOUS_VALUE')
EXT = dict(turn_kind='OTHER_OPERATION', scope='OUT_REFERENT_EXTENSIONAL', disp='INFORM', reason='REFERENT_EXTENSIONAL')
PROT = dict(turn_kind='PROTOCOL', scope='IN_PROTOCOL')
REL6 = 'últimos 6 meses (2026-04-01..2026-10-01)'
Y2026 = 'este año (2026-01-01..2026-12-31)'

# ======================= SF001-SF014 =======================
L('SF001', [O('SET', 'TOPIC', 'papelería', 'contains', 'NEGATIVE', 'papelería')],
  add(P('SF001'), 'TOPIC', 'contains', 'papelería', 'NEGATIVE'), gaps=['POLARITY_OTHER_VALUE'],
  notes='F17: exclusión sin positivo en la ranura: se añade.')
L('SF002', [O('SET', 'TOPIC', 'papelería', 'contains', vh='papelería')], add(P('SF002'), 'TOPIC', 'contains', 'papelería'),
  gaps=['TOPIC'], notes='Tema sobre ranura vacía: se añade (F3 no reemplaza nada).')
L('SF003', [O('SET', 'GEOGRAPHY', 'Eje Cafetero', 'is', 'NEGATIVE', 'Eje Cafetero', 'REGION', rrs=True)],
  add(P('SF003'), 'GEOGRAPHY', 'is', 'Eje Cafetero', 'NEGATIVE', 'REGION'), gaps=['REGION_VOCAB', 'POLARITY_OTHER_VALUE'],
  notes='Eje Cafetero está en la lista cerrada (F13). «los del» -> O8 true; F4 re-consulta.')
L('SF004', [O('SET', 'GEOGRAPHY', 'Eje Cafetero', 'is', vh='Eje Cafetero', level='REGION', rrs=True)],
  add(P('SF004'), 'GEOGRAPHY', 'is', 'Eje Cafetero', level='REGION'), gaps=['REGION_VOCAB', 'SET'])
L('SF005', [O('SET', 'BUYER', 'SENA', 'is', 'NEGATIVE', 'SENA')], add(P('SF005'), 'BUYER', 'is', 'SENA', 'NEGATIVE'),
  gaps=['POLARITY_OTHER_VALUE'])
L('SF006', [O('SET', 'BUYER', 'SENA', 'is', vh='SENA')], add(P('SF006'), 'BUYER', 'is', 'SENA'))
L('SF007', [O('SET', 'MODALITY', 'contratación directa', 'is', 'NEGATIVE', 'contratación directa')],
  add(P('SF007'), 'MODALITY', 'is', 'contratación directa', 'NEGATIVE'), gaps=['POLARITY_OTHER_VALUE'])
L('SF008', [O('SET', 'MODALITY', 'contratación directa', 'is', vh='contratación directa')],
  add(P('SF008'), 'MODALITY', 'is', 'contratación directa'))
L('SF009', [O('SET', 'STATE', 'adjudicados', 'is', 'NEGATIVE', 'awarded', rrs=True)],
  add(P('SF009'), 'STATE', 'is', 'awarded', 'NEGATIVE'), gaps=['POLARITY_OTHER_VALUE'],
  notes='C4: adjudicado = STATE is awarded; existe en procesos (universo citado), sin ACTOR.')
L('SF010', [O('SET', 'STATE', 'adjudicados', 'is', vh='awarded', rrs=True)], add(P('SF010'), 'STATE', 'is', 'awarded'))
L('SF011', [O('SET', 'GEOGRAPHY', 'Cali', 'is', 'NEGATIVE', 'Cali', 'MUNICIPALITY', rrs=True)],
  rep(P('SF011'), 'GEOGRAPHY', polarity='NEGATIVE'), gaps=['POLARITY_SAME_VALUE'],
  notes='F16: negar el único positivo vigente de la ranura lo invierte, anunciado. D7: «de Cali» = GEOGRAPHY.',
  change='etiqueta igual; la ambigüedad desaparece por F16 (AMBIGUOUS -> CLEAR)')
L('SF012', [O('SET', 'BUYER', 'Alcaldía de Pasto', 'is', 'NEGATIVE', 'Alcaldía de Pasto')],
  rep(P('SF012'), 'BUYER', polarity='NEGATIVE'), gaps=['POLARITY_SAME_VALUE'],
  notes='F16.', change='igual; AMBIGUOUS -> CLEAR por F16')
alt = add(P('SF013'), 'VALUE', 'less_or_equal', '500 millones', unit='MILLONES')
L('SF013', [O('SET', 'VALUE.upper', 'más de 500 millones', 'greater_than', 'NEGATIVE', '500 millones', rrs=True, us='STATED')],
  add(P('SF013'), 'VALUE', 'greater_than', '500 millones', 'NEGATIVE', unit='MILLONES'),
  alts=[alt], amb='AMBIGUOUS', conf='MEDIUM', gaps=['NEGATED_COMPARATOR'],
  notes='O4 fija el lado (VALUE.upper). La forma del operador (greater_than NEGATIVE vs less_or_equal POSITIVE, alternativa) sigue viva: NEGATED_COMPARATOR.')
L('SF014', [O('SET', 'TIME', 'de 2023', 'during_year', 'NEGATIVE', '2023', rrs=True, ys='STATED')],
  add(P('SF014'), 'TIME', 'during_year', '2023', 'NEGATIVE'), gaps=['POLARITY_OTHER_VALUE'])

# ======================= SF015-SF024 order / cardinality =======================
L('SF015', [O('SET_ORDER', 'ORDER', 'más caros', 'value_desc', rrs=True), O('SET', 'CARDINALITY', '5', 'top_n', vh='5', rrs=True)],
  order_(card(P('SF015'), 5), 'value_desc'))
L('SF016', [O('SET_ORDER', 'ORDER', 'más caros', 'value_desc', rrs=True)], order_(P('SF016'), 'value_desc'), conf='MEDIUM',
  notes='§15: plural sin cifra -> sin CARDINALITY, aunque diga «solo».')
L('SF017', [O('SET_ORDER', 'ORDER', 'mayor valor', 'value_desc', rrs=True), O('SET', 'CARDINALITY', 'el', 'top_n', vh='1', rrs=True)],
  order_(card(P('SF017'), 1), 'value_desc'), conf='MEDIUM', notes='§15: superlativo singular -> CARDINALITY 1. F4: re-consulta.')
L('SF018', [O('SET_ORDER', 'ORDER', 'por fecha, primero los más recientes', 'date_desc', rrs=True)], order_(P('SF018'), 'date_desc'),
  notes='F10: se conserva n=10.')
L('SF019', [O('SET_ORDER', 'ORDER', 'por fecha, primero los más antiguos', 'date_asc', rrs=True)], order_(P('SF019'), 'date_asc'),
  notes='F10.')
L('SF020', [O('SET_ORDER', 'ORDER', 'más recientes', 'date_desc', rrs=True), O('SET', 'CARDINALITY', '20', 'top_n', vh='20', rrs=True)],
  order_(card(P('SF020'), 20), 'date_desc'))
L('SF021', [O('SET_ORDER', 'ORDER', 'mayor valor', 'value_desc', rrs=True), O('SET', 'CARDINALITY', '250', 'top_n', vh='250', rrs=True)],
  order_(card(P('SF021'), 100, 250), 'value_desc'), gaps=['TOPN_CAP'],
  notes='F20/A3: ejecuta con 100 y anuncia TOPN_CAPPED.', change='cardinalidad 250 -> n=100/n_requested=250; AMBIGUOUS -> CLEAR (A3/F20)')
L('SF022', [O('SET_ORDER', 'ORDER', 'mayor valor', 'value_desc', rrs=True), O('SET', 'CARDINALITY', '100', 'top_n', vh='100', rrs=True)],
  order_(card(P('SF022'), 100), 'value_desc'), gaps=['TOPN_CAP'])
L('SF023', [O('CLEAR', 'ORDER', 'quita el orden'), O('CLEAR', 'CARDINALITY', 'quiero verlos todos', rrs=True)],
  order_(card(P('SF023'), None), None))
L('SF024', [O('CLEAR', 'CARDINALITY', 'no solo 10, todos', rrs=True),
            O('KEEP', 'ORDER', 'déjalos por valor', 'value_desc', vh='valor descendente (el orden vigente)', rrs=True, dpv=True)],
  card(P('SF024'), None), notes='O5: KEEP del orden vigente, inherited null.')

# ======================= SF025-SF038 time =======================
L('SF025', [O('SET', 'TIME', 'este año', 'relative_period', vh=Y2026, ys='RELATIVE_TO_CLOCK')], add(P('SF025'), 'TIME', 'relative_period', Y2026))
L('SF026', [O('SET', 'TIME', 'de este mes', 'relative_period', vh='este mes (2026-10-01..2026-10-31)', rrs=True, ys='RELATIVE_TO_CLOCK')],
  add(P('SF026'), 'TIME', 'relative_period', 'este mes (2026-10-01..2026-10-31)'))
L('SF027', [O('SET', 'TIME', 'de los últimos 6 meses', 'relative_period', vh=REL6, ys='RELATIVE_TO_CLOCK')],
  add(P('SF027'), 'TIME', 'relative_period', REL6))
L('SF028', [O('SET', 'TIME', 'los últimos 3 meses', 'relative_period', vh='últimos 3 meses (2026-07-01..2026-10-01)', ys='RELATIVE_TO_CLOCK')],
  rep(P('SF028'), 'TIME', operator='relative_period', value_hint='últimos 3 meses (2026-07-01..2026-10-01)'))
L('SF029', [O('SET', 'TIME', 'los últimos 12 meses', 'relative_period', vh='últimos 12 meses (2025-10-01..2026-10-01)', ys='RELATIVE_TO_CLOCK')],
  rep(P('SF029'), 'TIME', value_hint='últimos 12 meses (2025-10-01..2026-10-01)'))
L('SF030', [O('SET', 'TIME', 'el año en curso', 'relative_period', vh='año en curso (2026-01-01..2026-12-31)', ys='RELATIVE_TO_CLOCK')],
  rep(P('SF030'), 'TIME', operator='relative_period', value_hint='año en curso (2026-01-01..2026-12-31)'))
L('SF031', [O('SET', 'TIME', 'marzo', 'during_month', vh='marzo (año sin resolver)', unres=True)], None,
  disp='CLARIFY', reason='MONTH_YEAR_AMBIGUOUS', gaps=['MONTHYEAR'],
  notes='F7: TIME previo plurianual -> CLARIFY. Lectura descartada: marzo de cada año 2023-2025.',
  change='resulting_ir -> null (SCHEMA §5.2: CLARIFY no F11); razón MONTH_YEAR_MULTI_YEAR_PRIOR -> MONTH_YEAR_AMBIGUOUS; AMBIGUOUS -> CLEAR')
L('SF032', [O('SET', 'TIME', 'marzo', 'during_month', vh='2024-03', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF032'), 'TIME', operator='during_month', value_hint='2024-03'), gaps=['MONTHYEAR'])
L('SF033', [O('SET', 'TIME', 'en octubre', 'during_month', vh='octubre (año sin resolver)', unres=True)], None,
  disp='CLARIFY', reason='MONTH_YEAR_AMBIGUOUS', gaps=['MONTHYEAR'], notes='F7: TIME previo relativo.',
  change='resulting_ir -> null (§5.2); razón -> MONTH_YEAR_AMBIGUOUS')
L('SF034', [O('SET', 'TIME', 'en abril', 'during_month', vh='2026-04', ys='CLOCK_INFERRED')], add(P('SF034'), 'TIME', 'during_month', '2026-04'),
  gaps=['MONTHYEAR'])
L('SF035', [O('SET', 'TIME', 'en diciembre', 'during_month', vh='2025-12', ys='CLOCK_INFERRED')], add(P('SF035'), 'TIME', 'during_month', '2025-12'),
  gaps=['MONTHYEAR'])
L('SF036', [O('SET', 'TIME', 'este año', 'relative_period', vh=Y2026, ys='RELATIVE_TO_CLOCK')],
  rep(P('SF036'), 'TIME', operator='relative_period', value_hint=Y2026))
L('SF037', [O('SET', 'TIME', 'el mes siguiente', 'during_month', vh='2024-04', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF037'), 'TIME', value_hint='2024-04'), gaps=['RELATIVE_ANCHOR'],
  notes='A5: único referente temporal activo (2024-03, mes) -> sucesor 2024-04, EXECUTE anunciado. La lectura por reloj (noviembre de 2026, futuro) no es plausible.',
  change='CLARIFY/OUT_RELATIVE_TO_PREVIOUS -> EXECUTE/IN (A5)')
alt = rep(P('SF038'), 'TIME', value_hint='2025')
L('SF038', [O('SET', 'TIME', 'el año anterior', 'during_year', vh='2023', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF038'), 'TIME', value_hint='2023'), alts=[alt], amb='AMBIGUOUS', conf='MEDIUM', gaps=['RELATIVE_ANCHOR'],
  notes='A5: referente único (2024) -> predecesor 2023, EXECUTE. Alternativa: leído respecto del reloj («el año pasado») da 2025; si se admite, A5 manda CLARIFY / RELATIVE_TO_PREVIOUS_VALUE (X18).',
  change='CLARIFY/OUT_RELATIVE_TO_PREVIOUS -> EXECUTE/IN 2023 (A5)')

# ======================= SF039-SF046 relative =======================
L('SF039', [O('SET', 'VALUE.lower', 'bájalo un poco', 'greater_or_equal', vh='bájalo un poco', dpv=True, us='PREVIOUS_ACCEPTED_VALUE', unres=True)], None, **F15,
  change='resulting_ir -> null (§5.2 / O14)')
L('SF040', [O('SET', 'VALUE.lower', 'el doble', 'greater_or_equal', vh='el doble', dpv=True, us='PREVIOUS_ACCEPTED_VALUE', unres=True)], None, **F15,
  change='resulting_ir -> null (§5.2 / O14: sin aritmética)')
L('SF041', [O('SET', 'TOPIC', 'vigilancia', 'contains', vh='vigilancia'),
            O('KEEP', 'BUYER', 'el mismo comprador', 'is', vh='Alcaldía de Ibagué', dpv=True)],
  rep(P('SF041'), 'TOPIC', value_hint='vigilancia'), gaps=['TOPIC'])
L('SF042', [O('SET', 'TIME', 'los seis meses siguientes', 'between_dates', vh='2024-07-01..2024-12-31', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF042'), 'TIME', value_hint='2024-07-01..2024-12-31'), gaps=['RELATIVE_ANCHOR'],
  notes='A5: referente único (2024-01-01..2024-06-30, ventana de 6 meses) -> sucesor.', change='CLARIFY -> EXECUTE (A5)')
L('SF043', [O('SET', 'VALUE.upper', 'súbelo un poquito', 'less_or_equal', vh='súbelo un poquito', dpv=True, us='PREVIOUS_ACCEPTED_VALUE', unres=True)], None, **F15,
  change='resulting_ir -> null (§5.2)')
L('SF044', [O('SET', 'VALUE.lower', 'el triple', 'greater_or_equal', vh='el triple', dpv=True, us='PREVIOUS_ACCEPTED_VALUE', unres=True)], None, **F15,
  change='resulting_ir -> null (§5.2)')
L('SF045', [O('SET', 'TIME', 'un trimestre antes', 'between_dates', vh='2024-11-01..2025-01-31', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF045'), 'TIME', value_hint='2024-11-01..2025-01-31'), gaps=['RELATIVE_ANCHOR'],
  notes='A5: referente único (2025-02-01..2025-04-30, un trimestre) -> trimestre predecesor.', change='CLARIFY -> EXECUTE (A5)')
L('SF046', [O('SET', 'VALUE.lower', 'la mitad de eso', 'greater_or_equal', vh='la mitad de eso', dpv=True, us='PREVIOUS_ACCEPTED_VALUE', unres=True),
            O('SET', 'VALUE.upper', 'la mitad de eso', 'less_or_equal', vh='la mitad de eso', dpv=True, us='PREVIOUS_ACCEPTED_VALUE', unres=True)],
  None, **F15, conf='MEDIUM', notes='F15. «eso» puede ser una cota o el rango: dos operaciones sin resolver.',
  change='resulting_ir -> null (§5.2)')

# ======================= SF047-SF064 pending (TIME «el primer semestre») =======================
def ns(u, *conds):
    ir = fresh(u)
    for c in conds: add(ir, *c[:3], **(c[3] if len(c) > 3 else {}))
    return ir
H1 = '2025-01-01..2025-06-30'
L('SF047', [], ns('CONTRACTS', ('TOPIC', 'contains', 'alquiler de oficinas'), ('BUYER', 'is', 'Alcaldía de Piedecuesta'), ('TIME', 'between_dates', H1)),
  **NS, gaps=['UNIVERSE_FRAGMENT'], notes='A4/A7: consulta autosuficiente; cancela la pendiente.')
L('SF048', [], ns('CONTRACTS', ('TOPIC', 'contains', 'alquiler de oficinas'), ('BUYER', 'is', 'Gobernación de Santander'), ('TIME', 'between_dates', H1)),
  **NS, gaps=['UNIVERSE_FRAGMENT'], notes='A4/A7.')
L('SF049', [], ns('CONTRACTS', ('TOPIC', 'contains', 'alquiler de oficinas'), ('BUYER', 'is', 'Alcaldía de Piedecuesta'), ('TIME', 'during_year', '2024')),
  **NS, gaps=['UNIVERSE_FRAGMENT'], notes='A4/A7.')
L('SF050', [], ns('CONTRACTS', ('TOPIC', 'contains', 'alquiler de oficinas'), ('BUYER', 'is', 'Alcaldía de Piedecuesta'), ('TIME', 'during_year', '2024')),
  **NS, gaps=['UNIVERSE_FRAGMENT'], notes='A4/A7; el reask previo (asks=2) no cambia la lectura.')
L('SF051', [O('ANSWER', 'TIME', 'de enero a junio de 2025', 'between_dates', vh=H1, ys='STATED'),
            O('SET', 'VALUE.upper', 'menos de 40 millones', 'less_than', vh='40 millones', us='STATED', unres=True)], None,
  turn_kind='ANSWER_TO_CLARIFICATION', disp='REASK', reason='ANSWER_EDITS_OTHER_SLOT', gaps=['ANSWER_PLUS_EDIT'],
  notes='F25: respuesta + edición de otra ranura -> REASK nombrando la salida. (Además, <40 con >150 dejaría el rango sin interior: RANGE_NO_INTERIOR, no se llega a aplicar.)',
  change='EXECUTE -> REASK/ANSWER_EDITS_OTHER_SLOT (F25)')
L('SF052', [], ns('PROCESSES', ('TOPIC', 'contains', 'alquiler de oficinas'), ('BUYER', 'is', 'Alcaldía de Piedecuesta'), ('TIME', 'between_dates', H1)),
  **NS, gaps=['UNIVERSE_FRAGMENT'], notes='A4/A7.')
L('SF053', [O('SET', 'BUYER', 'Alcaldía de Piedecuesta', 'is', vh='Alcaldía de Piedecuesta', rrs=True, unres=True),
            O('ANSWER', 'TIME', 'de enero a junio de 2025', 'between_dates', vh=H1, ys='STATED')], None,
  turn_kind='ANSWER_TO_CLARIFICATION', disp='REASK', reason='ANSWER_EDITS_OTHER_SLOT', conf='MEDIUM',
  gaps=['ANSWER_PLUS_EDIT', 'UNIVERSE_FRAGMENT'],
  notes='Anafórico («los de»): no es autosuficiente (A4) -> respuesta + edición de BUYER -> F25 REASK.',
  change='EXECUTE -> REASK (F25); AMBIGUOUS -> CLEAR (A4: anafórico no es autosuficiente)')
L('SF054', [O('SET', 'TIME', 'de enero a junio de 2025', 'between_dates', vh=H1, ys='STATED')],
  rep(P('SF054'), 'TIME', operator='between_dates', value_hint=H1))
L('SF055', [], ns('CONTRACTS', ('TOPIC', 'contains', 'interventoría'), ('BUYER', 'is', 'Alcaldía de Floridablanca'),
                  ('VALUE', 'greater_than', '150 millones', {'unit': 'MILLONES'}), ('TIME', 'between_dates', H1)),
  **NS, gaps=['UNIVERSE_FRAGMENT'], notes='A7: consulta autosuficiente cancela la pendiente; la IR coincide con la de responderla.',
  change='AMBIGUOUS -> CLEAR (A7)')
L('SF056', [O('ANSWER', 'TIME', 'de enero a junio de 2025', 'between_dates', vh=H1, ys='STATED')],
  add(P('SF056'), 'TIME', 'between_dates', H1), turn_kind='ANSWER_TO_CLARIFICATION')
L('SF057', [], ns('CONTRACTS', ('TOPIC', 'contains', 'alquiler de oficinas'), ('BUYER', 'is', 'Alcaldía de Piedecuesta'), ('TIME', 'between_dates', H1)),
  **NS, notes='«busca» + consulta autosuficiente.')
L('SF058', [O('ANSWER', 'BUYER', None, 'is', vh='Alcaldía de San Pedro (Valle del Cauca)')],
  add(P('SF058'), 'BUYER', 'is', 'Alcaldía de San Pedro (Valle del Cauca)'), turn_kind='ANSWER_TO_CLARIFICATION', scope='IN_PROTOCOL',
  notes='O11: evento de opción o1.', change='m2a_scope IN -> IN_PROTOCOL (input_kind EVENT)')
L('SF059', [O('ANSWER', 'BUYER', None, 'is', vh='Alcaldía de Soledad (Atlántico)')],
  add(P('SF059'), 'BUYER', 'is', 'Alcaldía de Soledad (Atlántico)'), turn_kind='ANSWER_TO_CLARIFICATION', scope='IN_PROTOCOL',
  gaps=['COUNTERPART'], notes='O11: opción o1 (comprador) de F12.',
  change='resulting_ir: valor X ahora disponible en pending (Alcaldía de Soledad); scope -> IN_PROTOCOL')
L('SF060', [O('ANSWER', 'GEOGRAPHY', None, 'is', vh='Soledad (Atlántico)', level='MUNICIPALITY')],
  add(P('SF060'), 'GEOGRAPHY', 'is', 'Soledad (Atlántico)', level='MUNICIPALITY'), turn_kind='ANSWER_TO_CLARIFICATION', scope='IN_PROTOCOL',
  gaps=['COUNTERPART'], notes='O11: opción o2 (lugar) de F12.',
  change='resulting_ir: GEOGRAPHY Soledad MUNICIPALITY (antes UNRESOLVED/UNKNOWN); scope -> IN_PROTOCOL')
L('SF061', [O('ANSWER', 'BUYER', 'cualquiera, el que sea', 'is', vh='cualquiera de las opciones', unres=True)], None,
  turn_kind='ANSWER_TO_CLARIFICATION', disp='REASK', reason='OPTION_NOT_CHOSEN', gaps=['ANY_OPTION'],
  notes='F24: el producto no elige; REASK una vez.', change='EXECUTE (OR de opciones) -> REASK/OPTION_NOT_CHOSEN (F24)')
L('SF062', [], None, turn_kind='OTHER_OPERATION', scope='OUT_OPERATION_CHANGE', disp='INFORM', reason='OPERATION_CHANGE:SUM_VALUE',
  gaps=['PENDING_OPSWITCH', 'TOTALES'],
  notes='§5.2 #4 precede a F29 (que sólo rige dentro de IN). El comprador «Santa Rosa» seguiría sin resolver para la suma.',
  change='HANDOFF_LEGACY -> INFORM/OPERATION_CHANGE:SUM_VALUE (§5.2: con ancla Foundation nada va a legacy); resulting_ir -> null')
ir_ns = fresh('UNSPECIFIED', pv='HISTORICAL_PROCESSES', vs='DERIVED')
add(ir_ns, 'TOPIC', 'contains', 'dotación'); add(ir_ns, 'BUYER', 'is', 'Gobernación del Cauca'); add(ir_ns, 'TIME', 'during_year', '2025')
alt = P('SF063'); rep(alt, 'TOPIC', value_hint='dotación'); rep(alt, 'BUYER', value_hint='Gobernación del Cauca'); add(alt, 'TIME', 'during_year', '2025')
L('SF063', [O('CANCEL_PENDING', None, 'no, mejor')], ir_ns, turn_kind='NEW_SEARCH', scope='IN_NEW_SEARCH', disp='NEW_SEARCH',
  alts=[alt], amb='AMBIGUOUS', conf='LOW', gaps=['PENDING_REJECTED', 'UNIVERSE_FRAGMENT'],
  notes='A7/F23: el rechazo cancela la pendiente. X = tema + comprador + año, interpretable sin el previo (A4: autosuficiente aunque no cite universo) -> búsqueda nueva UNSPECIFIED, PV1. Alternativa: X como fragmento -> delta sobre el marco pendiente (conserva CONTRACTS y >150 millones). CANCEL_PENDING se conserva por O12 aunque O13 pide ops=[] en NEW_SEARCH.',
  change='DELTA/EXECUTE -> NEW_SEARCH UNSPECIFIED (A4 + A7); la lectura anterior pasa a alternativa')
L('SF064', [O('ANSWER', 'TIME', 'de enero a junio de 2025', 'between_dates', vh=H1, ys='STATED')],
  add(P('SF064'), 'TIME', 'between_dates', H1), turn_kind='ANSWER_TO_CLARIFICATION', **EXP, gaps=['TTL'],
  notes='§5.3: previo PENDING vencido -> gana F11. Tensión X15 declarada (no resuelta).')

# ======================= SF065-SF072 extensional =======================
for g, n in [('SF065', 'Selección posicional.'), ('SF066', '«ese contrato» sobre un conjunto de varios: fila sin identificar.'),
             ('SF067', 'Abrir la tercera fila.'), ('SF070', 'Atributo de la primera fila.'), ('SF071', 'Atributo de la última fila.'),
             ('SF072', 'Subconjunto posicional; sin ORDER en el ancla no cabe CARDINALITY.')]:
    L(g, [], None, **EXT, conf='MEDIUM' if g in ('SF066', 'SF072') else 'HIGH', notes=n,
      change='HANDOFF_LEGACY/CLARIFY -> INFORM/REFERENT_EXTENSIONAL; resulting_ir -> null (§5.2 #5)')
alt = add(P('SF068'), 'BUYER', 'is', 'Alcaldía de Cali')
L('SF068', [O('SET', 'GEOGRAPHY', 'Cali', 'is', vh='Cali', level='MUNICIPALITY', rrs=True)],
  add(P('SF068'), 'GEOGRAPHY', 'is', 'Cali', level='MUNICIPALITY'), alts=[alt], amb='AMBIGUOUS', conf='MEDIUM', gaps=['GEOBUYER', 'SET'],
  notes='F4 re-consulta. D7: «de Cali» = GEOGRAPHY (AMBIGUOUS + EXECUTE). Cali está dentro del territorio del comprador (Valle): contención, X12 vivo; no se aplica F30.')
alt = add(P('SF069'), 'BUYER', 'is', 'Alcaldía de Cali')
L('SF069', [O('SET', 'GEOGRAPHY', 'Cali', 'is', vh='Cali', level='MUNICIPALITY', rrs=True)],
  add(P('SF069'), 'GEOGRAPHY', 'is', 'Cali', level='MUNICIPALITY'), alts=[alt], amb='AMBIGUOUS', conf='MEDIUM', gaps=['GEOBUYER'],
  notes='Igual que SF068 (D7, X12).')

# ======================= SF073-SF084 universe / view =======================
L('SF073', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES', rrs=True)], uni(P('SF073'), 'PROCESSES'), gaps=['UNIVTIME'])
L('SF074', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS')], uni(P('SF074'), 'CONTRACTS'))
L('SF075', [O('SET_UNIVERSE', 'UNIVERSE', 'oportunidades', vh='PROCESSES/OPPORTUNITIES')], uni(P('SF075'), 'PROCESSES', 'OPPORTUNITIES'),
  gaps=['UNIVTIME'], notes='UNIVTIME cerrado por F1: TIME 2025 se conserva.', change='AMBIGUOUS -> CLEAR (UNIVTIME cerrado por F1)')
L('SF076', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS', rrs=True)], uni(P('SF076'), 'CONTRACTS'))
L('SF077', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES', rrs=True)], uni(P('SF077'), 'PROCESSES'))
L('SF078', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES', rrs=True), O('SET', 'STATE', 'abiertos', 'family', vh='open', rrs=True)],
  add(uni(P('SF078'), 'PROCESSES'), 'STATE', 'family', 'open'))
L('SF079', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS', rrs=True),
            O('SET', 'STATE', 'abiertos', 'family', vh='open', rrs=True, unres=True)], None,
  disp='CLARIFY', reason='STATE_NOT_IN_UNIVERSE', gaps=['STATE_UNIVERSE'],
  notes='F27: «open» no existe en contratos (§19.1); se ofrece cambiar de universo.',
  change='resulting_ir -> null (§5.2); AMBIGUOUS -> CLEAR (F27)')
L('SF080', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES'), O('SET', 'MODALITY', 'mínima cuantía', 'is', vh='mínima cuantía')],
  add(uni(P('SF080'), 'PROCESSES'), 'MODALITY', 'is', 'mínima cuantía'))
L('SF081', [O('SET', 'TIME', '2025', 'during_year', vh='2025', ys='STATED')], add(P('SF081'), 'TIME', 'during_year', '2025'), gaps=['PV'],
  notes='F9: vista elegida por botón (PROTOCOL:view), no se recalcula.')
ir = add(P('SF082'), 'TIME', 'during_year', '2025'); ir['product_view'] = 'HISTORICAL_PROCESSES'
L('SF082', [O('SET', 'TIME', '2025', 'during_year', vh='2025', ys='STATED')], ir, gaps=['PV'], notes='F9: PV6 -> PV1.')
L('SF083', [O('SET', 'TIME', '2025', 'during_year', vh='2025', ys='STATED')], add(P('SF083'), 'TIME', 'during_year', '2025'))
alt = uni(P('SF084'), 'PROCESSES', 'OPPORTUNITIES')
L('SF084', [O('SET_UNIVERSE', 'UNIVERSE', 'convocatorias', vh='PROCESSES/OPPORTUNITIES'),
            O('SET', 'STATE', 'siguen recibiendo ofertas', 'family', vh='open')],
  add(uni(P('SF084'), 'PROCESSES', 'OPPORTUNITIES'), 'STATE', 'family', 'open'), alts=[alt], amb='AMBIGUOUS', conf='MEDIUM',
  gaps=['OPPORTUNITY_STATE'],
  notes='O7 + A1 (léxico «reciben ofertas» = open). Si el estado es redundante con la lente de oportunidad (OPPORTUNITY_STATE, vivo), la alternativa no lo añade.',
  change='CLEAR -> AMBIGUOUS (OPPORTUNITY_STATE vivo en §8)')

# ======================= SF085-SF108 value (A1, A2, F5, F6) =======================
def RNI(gid, keep_ops, new_ir, lit_ir, extra_notes='', change=None, gaps=('RANGE_NO_INTERIOR',)):
    L(gid, keep_ops, new_ir, alts=[], amb='AMBIGUOUS', conf='LOW', gaps=list(gaps),
      notes='A1: tras aplicar la cota por lado, inferior >= superior: no adjudicado. Mejor lectura: la cota nueva sustituye la restricción (intención de cambio, EXECUTE, anunciado). Alternativa: CLARIFY (sin IR). La lectura F2 literal por lado (conservar las dos cotas) deja un rango vacío que el propio §9-F2 prohíbe, por eso no va en alternatives. ' + extra_notes,
      change=change)

L('SF085', [O('SET', 'VALUE.lower', 'más de 80 millones', 'greater_than', vh='80 millones', rrs=True, us='STATED')],
  add(P('SF085'), 'VALUE', 'greater_than', '80 millones', unit='MILLONES'))
L('SF086', [O('SET', 'VALUE.upper', 'máximo 300 millones', 'less_or_equal', vh='300 millones', us='STATED')],
  add(P('SF086'), 'VALUE', 'less_or_equal', '300 millones', unit='MILLONES'))
L('SF087', [O('SET', 'VALUE.lower', 'entre 50 y 120 millones', 'greater_or_equal', vh='50 millones', us='SIBLING_IN_RANGE'),
            O('SET', 'VALUE.upper', 'entre 50 y 120 millones', 'less_or_equal', vh='120 millones', us='STATED')],
  add(add(P('SF087'), 'VALUE', 'greater_or_equal', '50 millones', unit='MILLONES'), 'VALUE', 'less_or_equal', '120 millones', unit='MILLONES'),
  gaps=['UNIT'], notes='F6. Tensión X16 (anuncio de unidad compartida): el eco derivado lleva UNIT_SHARED_IN_RANGE.')
L('SF088', [O('SET', 'VALUE.lower', 'mínimo 250 millones', 'greater_or_equal', vh='250 millones', us='STATED')], rep(P('SF088'), 'VALUE', value_hint='250 millones'))
L('SF089', [O('SET', 'VALUE.upper', 'hasta 400 millones', 'less_or_equal', vh='400 millones', us='STATED')],
  add(P('SF089'), 'VALUE', 'less_or_equal', '400 millones', unit='MILLONES'))
RNI('SF090', [O('SET', 'VALUE.upper', 'no más de 200 millones', 'less_or_equal', vh='200 millones', us='STATED')],
    add(drop(P('SF090'), 'VALUE'), 'VALUE', 'less_or_equal', '200 millones', unit='MILLONES'),
    add(P('SF090'), 'VALUE', 'less_or_equal', '200 millones', unit='MILLONES'),
    change='CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1 retira la sustitución automática); IR igual')
RNI('SF091', [O('SET', 'VALUE.upper', 'hasta 300 millones', 'less_or_equal', vh='300 millones', us='STATED')],
    add(drop(P('SF091'), 'VALUE'), 'VALUE', 'less_or_equal', '300 millones', unit='MILLONES'),
    add(P('SF091'), 'VALUE', 'less_or_equal', '300 millones', unit='MILLONES'), 'Rango de un solo punto (300).',
    change='CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1); IR igual')
L('SF092', [O('SET', 'VALUE.upper', 'máximo 150 millones', 'less_or_equal', vh='150 millones', us='STATED')], rep(P('SF092'), 'VALUE', value_hint='150 millones'))
L('SF093', [O('SET', 'VALUE.lower', 'mayores de 50 millones', 'greater_than', vh='50 millones', us='STATED')],
  add(P('SF093'), 'VALUE', 'greater_than', '50 millones', unit='MILLONES'))
RNI('SF094', [O('SET', 'VALUE.lower', 'más de 400 millones', 'greater_than', vh='400 millones', us='STATED')],
    add(drop(P('SF094'), 'VALUE'), 'VALUE', 'greater_than', '400 millones', unit='MILLONES'),
    add(P('SF094'), 'VALUE', 'greater_than', '400 millones', unit='MILLONES'),
    change='CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1); IR igual')
RNI('SF095', [O('SET', 'VALUE.lower', 'desde 200 millones', 'greater_or_equal', vh='200 millones', us='STATED')],
    add(drop(P('SF095'), 'VALUE'), 'VALUE', 'greater_or_equal', '200 millones', unit='MILLONES'),
    add(P('SF095'), 'VALUE', 'greater_or_equal', '200 millones', unit='MILLONES'), 'Rango de un solo punto (200).',
    change='CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1); IR igual')
L('SF096', [O('SET', 'VALUE.lower', 'mínimo 150 millones', 'greater_or_equal', vh='150 millones', us='STATED')], cid(P('SF096'), 'c3', value_hint='150 millones'))
L('SF097', [O('SET', 'VALUE.upper', 'hasta 250 millones', 'less_or_equal', vh='250 millones', us='STATED')], cid(P('SF097'), 'c4', value_hint='250 millones'))
L('SF098', [O('SET', 'VALUE.lower', 'entre 400 y 600 millones', 'greater_or_equal', vh='400 millones', us='SIBLING_IN_RANGE'),
            O('SET', 'VALUE.upper', 'entre 400 y 600 millones', 'less_or_equal', vh='600 millones', us='STATED')],
  cid(cid(P('SF098'), 'c3', value_hint='400 millones'), 'c4', value_hint='600 millones'), gaps=['UNIT'],
  notes='Los dos lados se reemplazan; sin VALUE_RANGE_REPLACED (§6).')
lit = cid(P('SF099'), 'c3', operator='greater_than', value_hint='350 millones')
RNI('SF099', [O('SET', 'VALUE.lower', 'más de 350 millones', 'greater_than', vh='350 millones', us='STATED')],
    add(drop(P('SF099'), 'VALUE'), 'VALUE', 'greater_than', '350 millones', unit='MILLONES'), lit,
    change='CLEAR -> AMBIGUOUS + RANGE_NO_INTERIOR (A1); IR igual')
L('SF100', [O('CLEAR', 'VALUE', 'cambia el valor a'), O('SET', 'VALUE.upper', 'menos de 90 millones', 'less_than', vh='90 millones', us='STATED')],
  add(drop(P('SF100'), 'VALUE'), 'VALUE', 'less_than', '90 millones', unit='MILLONES'),
  notes='O4: «cambia el valor a…» = CLEAR(VALUE) + SET.', change='ops: se añade CLEAR(VALUE) por O4; IR igual')
L('SF101', [O('SET', 'VALUE.lower', 'mínimo 350', 'greater_or_equal', vh='350 millones', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
  rep(P('SF101'), 'VALUE', value_hint='350 millones'), gaps=['UNIT'])
L('SF102', [O('SET', 'VALUE.lower', 'mínimo 400', 'greater_or_equal', vh='400 (unidad sin resolver)', unres=True)], None,
  disp='CLARIFY', reason='UNIT_AMBIGUOUS', gaps=['UNIT'],
  notes='F5/A1: el VALUE previo tiene dos unidades (MILLONES, MIL_MILLONES): no hay unidad única inequívoca.',
  change='resulting_ir -> null; AMBIGUOUS -> CLEAR (A1: «única unidad activa inequívoca en el VALUE previo»)')
lit = add(P('SF103'), 'VALUE', 'greater_or_equal', '350 millones', unit='MILLONES')
RNI('SF103', [O('SET', 'VALUE.lower', 'mejor mínimo 350', 'greater_or_equal', vh='350 millones', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
    add(drop(P('SF103'), 'VALUE'), 'VALUE', 'greater_or_equal', '350 millones', unit='MILLONES'), lit,
    'F5 hereda MILLONES (única unidad); >=350 con <=300 no tiene interior. F26 retirada (A1).', gaps=['UNIT', 'RANGE_NO_INTERIOR'],
    change='texto nuevo (antes «mínimo 800» sobre SMMLV): caso distinto; EXECUTE >=350 millones, AMBIGUOUS + RANGE_NO_INTERIOR (A1)')
L('SF104', [O('SET', 'VALUE.lower', 'ahora mínimo 650', 'greater_or_equal', vh='650 pesos', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
  rep(P('SF104'), 'VALUE', value_hint='650 pesos'), conf='MEDIUM', gaps=['UNIT'],
  notes='F5/A1: única unidad PESOS -> 650 pesos; A1 suprime la cláusula de magnitud absurda, así que se ejecuta.',
  change='AMBIGUOUS -> CLEAR (A1 retira F26 / magnitud absurda)')
L('SF105', [O('SET', 'VALUE.lower', 'más de 900 mil pesos', 'greater_than', vh='900 mil pesos', us='STATED')],
  add(P('SF105'), 'VALUE', 'greater_than', '900 mil pesos', unit='MILES'),
  notes='§3: con palabra de escala «mil» la unidad es MILES.', change='unit PESOS -> MILES (lista cerrada §3)')
L('SF106', [O('SET', 'VALUE.lower', 'entre 450 y 900', 'greater_or_equal', vh='450 millones', dpv=True, us='PREVIOUS_ACCEPTED_VALUE'),
            O('SET', 'VALUE.upper', 'entre 450 y 900', 'less_or_equal', vh='900 millones', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
  add(rep(P('SF106'), 'VALUE', value_hint='450 millones'), 'VALUE', 'less_or_equal', '900 millones', unit='MILLONES'), gaps=['UNIT'])
L('SF107', [O('SET', 'VALUE.lower', 'entre 200 y 500 millones', 'greater_or_equal', vh='200 millones', us='SIBLING_IN_RANGE'),
            O('SET', 'VALUE.upper', 'entre 200 y 500 millones', 'less_or_equal', vh='500 millones', us='STATED')],
  add(add(P('SF107'), 'VALUE', 'greater_or_equal', '200 millones', unit='MILLONES'), 'VALUE', 'less_or_equal', '500 millones', unit='MILLONES'),
  gaps=['UNIT'])
L('SF108', [O('SET', 'VALUE.lower', 'mínimo 2 mil millones', 'greater_or_equal', vh='2 mil millones', us='STATED')],
  rep(P('SF108'), 'VALUE', value_hint='2 mil millones', unit='MIL_MILLONES'))

# ======================= SF109-SF116 clears =======================
L('SF109', [O('CLEAR', 'TIME', 'sin importar el año')], drop(P('SF109'), 'TIME'))
L('SF110', [O('CLEAR', 'GEOGRAPHY', 'no importa el departamento')], drop(P('SF110'), 'GEOGRAPHY'))
L('SF111', [O('CLEAR', 'STATE', 'da igual el estado')], drop(P('SF111'), 'STATE'))
L('SF112', [O('CLEAR', 'BUYER', 'de cualquier entidad')], drop(P('SF112'), 'BUYER'))
L('SF113', [{**O('CLEAR', 'MODALITY', 'quita el filtro de modalidad'), 'unresolved': True}], None, disp='INFORM', reason='CLEAR_ON_EMPTY_SLOT',
  gaps=['CLEAR_ABSENT'], notes='F18.', change='EXECUTE (no-op) -> INFORM/CLEAR_ON_EMPTY_SLOT (F18); AMBIGUOUS -> CLEAR')
for g, q in [('SF114', 'mejor sin año'), ('SF116', 'ya no importa el comprador')]:
    slot = 'TIME' if g == 'SF114' else 'BUYER'
    L(g, [{**O('CLEAR', slot, q), 'unresolved': True}], None, disp='CLARIFY', reason='EMPTY_SEARCH_AFTER_DELTA',
      alts=[drop(P(g), slot)], amb='AMBIGUOUS', conf='MEDIUM', gaps=['EMPTY_AFTER_DELTA'],
      notes='F19: sólo queda el universo (CONTRACTS). Tensión X9: si conservar el universo cuenta como «condición», se ejecutaría (alternativa).',
      change='EXECUTE -> CLARIFY/EMPTY_SEARCH_AFTER_DELTA (F19); resulting_ir -> null')
L('SF115', [O('CLEAR', 'TOPIC', 'olvida el tema')], drop(P('SF115'), 'TOPIC'))

# ======================= SF117-SF122 order =======================
L('SF117', [O('SET_ORDER', 'ORDER', 'del más barato al más caro', 'value_asc', rrs=True)], order_(P('SF117'), 'value_asc'))
L('SF118', [O('SET_ORDER', 'ORDER', 'los más antiguos primero', 'date_asc', rrs=True)], order_(P('SF118'), 'date_asc'))
L('SF119', [O('SET_ORDER', 'ORDER', 'por fecha, los más recientes primero', 'date_desc', rrs=True)], order_(P('SF119'), 'date_desc'))
L('SF120', [O('SET', 'TOPIC', 'ambulancia', 'contains', vh='ambulancia', rrs=True),
            O('SET_ORDER', 'ORDER', 'del más caro al más barato', 'value_desc', rrs=True)],
  order_(add(P('SF120'), 'TOPIC', 'contains', 'ambulancia'), 'value_desc'))
L('SF121', [O('SET', 'TOPIC', 'ambulancia', 'contains', vh='ambulancia', rrs=True)], add(P('SF121'), 'TOPIC', 'contains', 'ambulancia'))
L('SF122', [O('SET_ORDER', 'ORDER', 'de menor a mayor valor', 'value_asc')], order_(P('SF122'), 'value_asc'))

# ======================= SF123-SF134 territory / fragments =======================
def f30(gid, ops, buyer_dim_new, extra=None, notes=''):
    a1 = drop(P(gid), 'BUYER'); a2 = P(gid)
    for a in (a1, a2):
        for c in buyer_dim_new: add(a, *c[:3], **(c[3] if len(c) > 3 else {}))
    L(gid, ops, None, disp='CLARIFY', reason='TERRITORY_VS_CARRIED_BUYER', alts=[a1, a2], gaps=['GEOBUYER'], notes=notes,
      change='EXECUTE (AMBIGUOUS) -> CLARIFY/TERRITORY_VS_CARRIED_BUYER con dos alternativas (F30); resulting_ir -> null')
f30('SF123', [O('SET', 'GEOGRAPHY', 'región Caribe', 'is', vh='Caribe', level='REGION', unres=True)],
    [('GEOGRAPHY', 'is', 'Caribe', {'level': 'REGION'})], notes='F30: Gobernación de Nariño (Pacífica) arrastrada + región Caribe. Alternativas: cambiar de comprador / buscar en ambos.')
L('SF124', [], ns('CONTRACTS', ('TOPIC', 'contains', 'mantenimiento de ascensores'), ('GEOGRAPHY', 'is', 'Caribe', {'level': 'REGION'})),
  **NS, gaps=['UNIVERSE_FRAGMENT', 'REGION_VOCAB'], notes='A4: autosuficiente.')
L('SF125', [O('SET', 'TIME', '2023', 'during_year', vh='2023', ys='STATED')], rep(P('SF125'), 'TIME', value_hint='2023'))
alt = fresh('UNSPECIFIED', pv='HISTORICAL_PROCESSES', vs='DERIVED')
add(alt, 'TOPIC', 'contains', 'alquiler de maquinaria pesada'); add(alt, 'TIME', 'during_year', '2023')
L('SF126', [O('SET', 'TOPIC', 'alquiler de maquinaria pesada', 'contains', vh='alquiler de maquinaria pesada'),
            O('SET', 'TIME', 'en 2023', 'during_year', vh='2023', ys='STATED')],
  rep(rep(P('SF126'), 'TOPIC', value_hint='alquiler de maquinaria pesada'), 'TIME', value_hint='2023'),
  alts=[alt], amb='AMBIGUOUS', conf='LOW', gaps=['UNIVERSE_FRAGMENT', 'TOPIC'],
  notes='A4: tema + año sin marco de petición (sin dueño ni universo); leído como continuación (F22 extendido). Alternativa: autosuficiente -> UNSPECIFIED + PV1.')
L('SF127', [O('SET', 'BUYER', 'Gobernación del Cauca', 'is', vh='Gobernación del Cauca')], rep(P('SF127'), 'BUYER', value_hint='Gobernación del Cauca'))
L('SF128', [], ns('CONTRACTS', ('TOPIC', 'contains', 'transporte escolar'), ('BUYER', 'is', 'Gobernación del Cauca')), **NS, gaps=['UNIVERSE_FRAGMENT'])
L('SF129', [O('SET', 'VALUE.lower', 'pasen de 50 millones', 'greater_than', vh='50 millones', rrs=True, us='STATED')],
  add(P('SF129'), 'VALUE', 'greater_than', '50 millones', unit='MILLONES'))
L('SF130', [], ns('CONTRACTS', ('TOPIC', 'contains', 'dotación de ambulancias'), ('VALUE', 'greater_than', '50 millones', {'unit': 'MILLONES'})),
  **NS, gaps=['UNIVERSE_FRAGMENT'])
L('SF131', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES')], uni(P('SF131'), 'PROCESSES'))
alt = uni(P('SF132'), 'PROCESSES', 'OPPORTUNITIES')
L('SF132', [O('KEEP', 'UNIVERSE', 'licitaciones', vh='PROCESSES')], P('SF132'), alts=[alt], amb='AMBIGUOUS', conf='LOW',
  notes='O6/D3: «licitaciones» coloquial = procesos, mismo universo -> KEEP, la IR no cambia. Alternativa: licitaciones como oportunidades. Ningún gap_code de §8 cubre «sustantivo de universo que repite el vigente».')
alt = rep(P('SF134'), 'TOPIC', value_hint='vigilancia')
L('SF134', [], ns('CONTRACTS', ('TOPIC', 'contains', 'vigilancia')), **NS, alts=[alt], amb='AMBIGUOUS', conf='LOW', gaps=['UNIVERSE_FRAGMENT'],
  notes='A4: «contratos de vigilancia» se interpreta solo -> búsqueda nueva. Alternativa: delta que reemplaza el tema y conserva la Alcaldía de Zipaquirá.')
L('SF133', [O('SET', 'TOPIC', 'vigilancia', 'contains', vh='vigilancia')], rep(P('SF133'), 'TOPIC', value_hint='vigilancia'), gaps=['TOPIC'],
  notes='F22.')

# ======================= SF135-SF144 =======================
L('SF135', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], rep(P('SF135'), 'TIME', value_hint='2024'),
  gaps=['PRIOR_NOT_EXECUTED'], notes='A6: interpretación aceptada refinable.')
L('SF136', [O('SET', 'VALUE.lower', 'superen los 100 millones', 'greater_than', vh='100 millones', rrs=True, us='STATED')],
  add(P('SF136'), 'VALUE', 'greater_than', '100 millones', unit='MILLONES'), gaps=['PRIOR_NOT_EXECUTED', 'SET'],
  notes='A6: «los que» es referente semántico (F4, re-consulta), no a filas.')
L('SF137', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], None, scope='OUT_NO_ANCHOR', disp='CLARIFY', reason='NO_ANCHOR',
  gaps=['NO_ANCHOR'], notes='F28.', change='m2a_scope OUT_ANCHOR_NOT_SEARCH -> OUT_NO_ANCHOR; resulting_ir -> null; AMBIGUOUS -> CLEAR (F28)')
L('SF138', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], rep(P('SF138'), 'TIME', value_hint='2024'), **EXP, gaps=['TTL'])
L('SF139', [O('SET', 'GEOGRAPHY', 'Palmira', 'is', vh='Palmira', level='MUNICIPALITY', rrs=True)], rep(P('SF139'), 'GEOGRAPHY', value_hint='Palmira'),
  **EXP, gaps=['TTL'])
L('SF140', [O('SET_ORDER', 'ORDER', 'por fecha', 'date_desc', rrs=True)], order_(P('SF140'), 'date_desc', 'DEFAULTED'), **EXP,
  gaps=['TTL', 'ORDER_DIRECTION'], notes='F21: más reciente primero, DEFAULTED.', change='AMBIGUOUS -> CLEAR (F21)')
L('SF141', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], rep(P('SF141'), 'TIME', value_hint='2024'), gaps=['TTL'])
L('SF142', [O('SET', 'GEOGRAPHY', 'Bogotá', 'is', vh='Bogotá', level='MUNICIPALITY')], None, scope='OUT_NO_ANCHOR', disp='CLARIFY',
  reason='ANCHOR_UNREADABLE', notes='§5.2 #1.', change='m2a_scope IN -> OUT_NO_ANCHOR')
L('SF143', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], rep(P('SF143'), 'TIME', value_hint='2024'),
  scope='OUT_ANCHOR_NOT_SEARCH', disp='HANDOFF_LEGACY', reason='ANCHOR_NOT_SEARCH:COUNT', notes='resulting_ir informativa.')
L('SF144', [O('SET', 'TIME', 'de 2024', 'during_year', vh='2024', rrs=True, ys='STATED')], add(P('SF144'), 'TIME', 'during_year', '2024'),
  scope='OUT_ANCHOR_NOT_SEARCH', disp='HANDOFF_LEGACY', reason='ANCHOR_NOT_SEARCH:RANK_SUPPLIERS', notes='resulting_ir informativa.')

# ======================= SF145-SF154 protocol / events / op change =======================
L('SF145', [], P('SF145'), **PROT, notes='Paginación: IR igual al previo.', change='m2a_scope -> IN_PROTOCOL')
L('SF146', [], P('SF146'), **PROT, change='m2a_scope -> IN_PROTOCOL')
alt = card(P('SF147'), 20)
L('SF147', [], P('SF147'), **PROT, alts=[alt], amb='AMBIGUOUS', conf='MEDIUM', gaps=['PAGE_VS_TOPN'],
  notes='«otros diez más»: paginación (lectura elegida) o cardinalidad (top 20 por valor, alternativa). PAGE_VS_TOPN vivo.',
  change='m2a_scope -> IN_PROTOCOL; CLEAR -> AMBIGUOUS (PAGE_VS_TOPN vivo)')
L('SF148', [], P('SF148'), **PROT, disp='CLARIFY', reason='UNRESOLVED_VALUE:UNIVERSE', conf='MEDIUM',
  notes='§5.2 #3: paginar un ancla dual exige aclarar qué universo. El código más cercano de §5.3 es UNRESOLVED_VALUE:<DIM>; falta uno propio (p. ej. PAGE_DUAL_ANCHOR).',
  change='EXECUTE -> CLARIFY (§5.2 #3, ancla dual al paginar)')
L('SF149', [], order_(P('SF149'), 'value_desc'), **PROT, notes='Botón de orden (EVENT): O15 ops=[]; resulting_ir = lo que produce.',
  change='ops SET_ORDER -> [] (O15); turn_kind DELTA -> PROTOCOL; scope -> IN_PROTOCOL')
L('SF150', [], order_(P('SF150'), 'date_desc'), **PROT, change='ops -> [] (O15); PROTOCOL / IN_PROTOCOL')
L('SF151', [], drop(P('SF151'), 'BUYER'), **PROT, notes='Botón de quitar filtro.', change='ops -> [] (O15); PROTOCOL / IN_PROTOCOL')
L('SF152', [], drop(P('SF152'), 'TIME'), **PROT, change='ops -> [] (O15); PROTOCOL / IN_PROTOCOL')
L('SF153', [], None, turn_kind='OTHER_OPERATION', scope='OUT_OPERATION_CHANGE', disp='INFORM', reason='OPERATION_CHANGE:FOLLOW', gaps=['OPSWITCH'],
  change='HANDOFF_LEGACY -> INFORM/OPERATION_CHANGE:FOLLOW (§5.2 #4); resulting_ir -> null')
L('SF154', [], None, turn_kind='OTHER_OPERATION', scope='OUT_OPERATION_CHANGE', disp='INFORM', reason='OPERATION_CHANGE:FOLLOW', gaps=['OPSWITCH'],
  notes='Alerta sobre procesos nuevos: cambio de operación.', change='HANDOFF_LEGACY -> INFORM (§5.2 #4); resulting_ir -> null; AMBIGUOUS -> CLEAR')

# ======================= SF155-SF162 =======================
f30('SF155', [O('SET', 'GEOGRAPHY', 'Cali', 'is', vh='Cali', level='MUNICIPALITY', unres=True),
              O('SET', 'TIME', 'solo 2024', 'during_year', vh='2024', ys='STATED')],
    [('GEOGRAPHY', 'is', 'Cali', {'level': 'MUNICIPALITY'}), ('TIME', 'during_year', '2024')],
    notes='F30: Alcaldía de Bello (Antioquia) arrastrada + Cali.')
L('SF156', [O('SET', 'BUYER', 'Gobernación de Risaralda', 'is', vh='Gobernación de Risaralda'), O('CLEAR', 'TIME', 'sin importar el año')],
  drop(rep(P('SF156'), 'BUYER', value_hint='Gobernación de Risaralda'), 'TIME'))
L('SF157', [O('SET', 'VALUE.lower', 'más de 100 millones', 'greater_than', vh='100 millones', us='STATED'),
            O('SET', 'TIME', 'de 2023', 'during_year', vh='2023', ys='STATED')],
  add(add(P('SF157'), 'VALUE', 'greater_than', '100 millones', unit='MILLONES'), 'TIME', 'during_year', '2023'))
L('SF158', [O('CLEAR', 'GEOGRAPHY', 'olvida el lugar'), O('SET', 'BUYER', 'Universidad de Antioquia', 'is', vh='Universidad de Antioquia', rrs=True)],
  add(drop(P('SF158'), 'GEOGRAPHY'), 'BUYER', 'is', 'Universidad de Antioquia'))
L('SF159', [O('SET', 'STATE', 'adjudicados', 'is', vh='awarded', rrs=True),
            O('SET', 'GEOGRAPHY', 'Magdalena Medio', 'is', vh='Magdalena Medio', level='REGION', unres=True)], None,
  disp='CLARIFY', reason='REGION_NOT_IN_LIST', gaps=['REGION_VOCAB'], notes='F13.', change='resulting_ir -> null (§5.2)')
L('SF160', [O('SET', 'MODALITY', 'mínima cuantía', 'is', vh='mínima cuantía'), O('SET_ORDER', 'ORDER', 'los más recientes', 'date_desc', rrs=True)],
  order_(add(P('SF160'), 'MODALITY', 'is', 'mínima cuantía'), 'date_desc'))
L('SF161', [O('SET_ORDER', 'ORDER', 'más caros', 'value_desc', rrs=True), O('SET', 'CARDINALITY', '5', 'top_n', vh='5', rrs=True),
            O('SET', 'TIME', 'de este año', 'relative_period', vh=Y2026, ys='RELATIVE_TO_CLOCK')],
  order_(card(add(P('SF161'), 'TIME', 'relative_period', Y2026), 5), 'value_desc'))
alt = add(add(P('SF162'), 'BUYER', 'is', 'Alcaldía de Soledad', 'NEGATIVE'), 'TIME', 'during_year', '2024')
L('SF162', [O('SET', 'GEOGRAPHY', 'Soledad', 'is', 'NEGATIVE', 'Soledad', 'MUNICIPALITY', rrs=True),
            O('SET', 'TIME', 'en 2024', 'during_year', vh='2024', ys='STATED')],
  add(add(P('SF162'), 'GEOGRAPHY', 'is', 'Soledad', 'NEGATIVE', 'MUNICIPALITY'), 'TIME', 'during_year', '2024'),
  alts=[alt], amb='AMBIGUOUS', conf='MEDIUM', gaps=['POLARITY_OTHER_VALUE', 'GEOBUYER'],
  notes='F17: valor distinto (Soledad) contenido en el positivo (Atlántico): se suma la exclusión. X7 (valor contenido) declarado. D7: alternativa comprador.')

# ======================= SF163-SF191 (nuevos) =======================
a1 = add(P('SF163'), 'BUYER', 'is', 'Alcaldía de Soacha'); a2 = add(P('SF163'), 'GEOGRAPHY', 'is', 'Soacha', level='MUNICIPALITY')
L('SF163', [O('SET', 'GEOGRAPHY', 'Soacha', 'is', vh='Soacha', level='MUNICIPALITY', unres=True)], None, disp='CLARIFY',
  reason='BUYER_OR_PLACE_AFTER_ACTOR', alts=[a1, a2], gaps=['COUNTERPART'],
  notes='F12: «con X» tras ACTOR, X = comprador o lugar. La ranura de la operación es provisional (GEOGRAPHY); queda sin resolver.')
L('SF164', [O('SET', 'BUYER', 'Alcaldía de Soacha', 'is', vh='Alcaldía de Soacha')], add(P('SF164'), 'BUYER', 'is', 'Alcaldía de Soacha'),
  gaps=['COUNTERPART'], notes='Cabeza institucional («Alcaldía de»): comprador sin duda; F12 no aplica.')
L('SF165', [O('SET', 'CONTRACT_TYPE_OR_NATURE', 'prestación de servicios', 'is', vh='prestación de servicios')],
  add(P('SF165'), 'CONTRACT_TYPE_OR_NATURE', 'is', 'prestación de servicios'), gaps=['SMMLV'],
  notes='A2 (C2 del contrato): tipo que califica al universo. SMMLV con año único 2025 -> ejecutable, el host convierte y anuncia.')
L('SF166', [O('SET', 'TIME', '2023', 'during_year', vh='2023', ys='STATED')], rep(P('SF166'), 'TIME', value_hint='2023'), gaps=['SMMLV'],
  notes='A2: cambia el año único -> se recalcula 500 SMMLV con el salario de 2023 (eco de la conversión; no hay clase de eco propia en §7).')
L('SF167', [O('SET', 'VALUE.lower', 'mejor mínimo 800', 'greater_or_equal', vh='800 SMMLV', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
  rep(P('SF167'), 'VALUE', value_hint='800 SMMLV'), gaps=['SMMLV', 'UNIT'], notes='A2 + F5: hereda SMMLV (única unidad); año único 2025.')
L('SF168', [O('SET', 'CONTRACT_TYPE_OR_NATURE', 'prestación de servicios', 'is', vh='prestación de servicios')], None,
  disp='CLARIFY', reason='SMMLV_YEAR_AMBIGUOUS', gaps=['SMMLV'],
  notes='A2: la interpretación resultante tiene SMMLV con TIME plurianual (2023-2025) -> CLARIFY. Ninguna operación del turno lo causa (unresolved=false en todas): la duda viene arrastrada del previo.')
alt = add(P('SF169'), 'CONTRACT_TYPE_OR_NATURE', 'is', 'prestación de servicios')
L('SF169', [O('SET', 'CONTRACT_TYPE_OR_NATURE', 'prestación de servicios', 'is', vh='prestación de servicios')], None,
  disp='CLARIFY', reason='SMMLV_YEAR_AMBIGUOUS', alts=[alt], amb='AMBIGUOUS', conf='LOW', gaps=['SMMLV'],
  notes='A2: SMMLV sin ningún TIME no está adjudicado. Mejor lectura: no hay año autoritativo -> CLARIFY con el código de año de A2. Alternativa: EXECUTE con el salario del año en curso (idea de la F14 retirada).')
L('SF170', [O('SET', 'VALUE.upper', 'hasta 900 SMMLV', 'less_or_equal', vh='900 SMMLV', us='STATED')],
  add(P('SF170'), 'VALUE', 'less_or_equal', '900 SMMLV', unit='SMMLV'), gaps=['SMMLV'],
  notes='A2: año único 2025; 900 SMMLV de 2025 supera 200 millones, el rango tiene interior. Unidades distintas por cota, ambas dichas.')
L('SF171', [O('SET_ORDER', 'ORDER', 'por fecha', 'date_desc', rrs=True)], order_(P('SF171'), 'date_desc', 'DEFAULTED'), gaps=['ORDER_DIRECTION'],
  notes='F21.')
L('SF172', [O('SET_ORDER', 'ORDER', 'por fecha', 'date_desc', rrs=True)], order_(P('SF172'), 'date_desc', 'DEFAULTED'), gaps=['ORDER_DIRECTION'],
  notes='F10 + F21 (X11 compatibles): se conserva n=10, más recientes primero.')
alt = rep(P('SF173'), 'TIME', value_hint='2025')
L('SF173', [O('SET', 'TIME', 'el año anterior', 'during_year', vh='2023', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF173'), 'TIME', value_hint='2023'), alts=[alt], amb='AMBIGUOUS', conf='MEDIUM', gaps=['RELATIVE_ANCHOR'],
  notes='A5, como SF038: predecesor de 2024. Alternativa reloj (2025) -> CLARIFY.')
L('SF174', [O('SET', 'TIME', 'el mes siguiente', 'during_month', vh='2025-04', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF174'), 'TIME', value_hint='2025-04'), gaps=['RELATIVE_ANCHOR'], notes='A5: sucesor de 2025-03.')
alt = rep(P('SF175'), 'TIME', operator='during_year', value_hint='2021')
L('SF175', [O('SET', 'TIME', 'el año anterior', 'during_year', vh='el año anterior', dpv=True, unres=True)], None, **F15,
  alts=[alt], amb='AMBIGUOUS', conf='MEDIUM', gaps=['RELATIVE_ANCHOR'],
  notes='A5: el referente activo es un rango de tres años: no hay granularidad compatible (año) -> CLARIFY. Alternativa: año anterior al rango (2021).')
L('SF176', [O('SET', 'TIME', 'el mes siguiente', 'during_month', vh='el mes siguiente', dpv=True, unres=True)], None, **F15,
  gaps=['RELATIVE_ANCHOR'], notes='A5: sin referente temporal activo -> CLARIFY. La lectura por reloj (noviembre de 2026) sería futura.')
alt = None
L('SF177', [O('SET', 'TIME', 'el año anterior', 'during_year', vh='2025', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF177'), 'TIME', operator='during_year', value_hint='2025'), amb='AMBIGUOUS', conf='LOW', gaps=['RELATIVE_ANCHOR'],
  notes='A5: referente único = ventana relativa que cae entera en 2026 -> año anterior 2025; la lectura por reloj también da 2025 (no hay «otro resultado»). Alternativa: CLARIFY / RELATIVE_TO_PREVIOUS_VALUE porque la granularidad (ventana de meses vs año) no es compatible (resulting_ir null).')
L('SF178', [O('SET', 'VALUE.lower', 'más de 100 millones', 'greater_than', vh='100 millones', us='STATED')],
  add(P('SF178'), 'VALUE', 'greater_than', '100 millones', unit='MILLONES'), gaps=['PRIOR_NOT_EXECUTED'], notes='A6: delta semántico refinable.')
L('SF179', [], None, **PROT, disp='INFORM', reason='REFERENT_EXTENSIONAL', gaps=['PRIOR_NOT_EXECUTED'],
  notes='A6: «más resultados» necesita página y no hay resultados. §5.3 no tiene un código INFORM para «sin conjunto de resultados»; se usa REFERENT_EXTENSIONAL como el más cercano.')
L('SF180', [], None, **EXT, gaps=['PRIOR_NOT_EXECUTED'], notes='A6: no hay filas a las que referirse.')
L('SF181', [], None, turn_kind='OTHER_OPERATION', scope='OUT_OPERATION_CHANGE', disp='INFORM', reason='OPERATION_CHANGE:EXPORT',
  gaps=['PRIOR_NOT_EXECUTED', 'OPSWITCH'], notes='EXPORT de lo mostrado: cambio de operación y, además (A6), no hay nada mostrado.')
RNI('SF182', [O('SET', 'VALUE.upper', 'máximo 90 millones', 'less_or_equal', vh='90 millones', us='STATED')],
    add(drop(P('SF182'), 'VALUE'), 'VALUE', 'less_or_equal', '90 millones', unit='MILLONES'),
    add(P('SF182'), 'VALUE', 'less_or_equal', '90 millones', unit='MILLONES'))
RNI('SF183', [O('SET', 'VALUE.upper', 'máximo 90', 'less_or_equal', vh='90 millones', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
    add(drop(P('SF183'), 'VALUE'), 'VALUE', 'less_or_equal', '90 millones', unit='MILLONES'),
    add(P('SF183'), 'VALUE', 'less_or_equal', '90 millones', unit='MILLONES'), 'F5 hereda MILLONES; F26 retirada.', gaps=['UNIT', 'RANGE_NO_INTERIOR'])
L('SF184', [], ns('CONTRACTS', ('TOPIC', 'contains', 'mantenimiento de parques'), ('BUYER', 'is', 'Alcaldía de Duitama')), **NS, gaps=['UNIVERSE_FRAGMENT'])
alt = rep(rep(P('SF185'), 'TOPIC', value_hint='mantenimiento de parques'), 'BUYER', value_hint='Alcaldía de Duitama')
ir = fresh('UNSPECIFIED', pv='CURRENT_OPPORTUNITIES', vs='DERIVED'); add(ir, 'TOPIC', 'contains', 'mantenimiento de parques'); add(ir, 'BUYER', 'is', 'Alcaldía de Duitama')
L('SF185', [], ir, **NS, alts=[alt], amb='AMBIGUOUS', conf='LOW', gaps=['UNIVERSE_FRAGMENT'],
  notes='A4: tema + comprador forman una petición que se interpreta sola (sin universo -> UNSPECIFIED, PV6). Alternativa: delta (F22/F1) que reemplaza tema y comprador y conserva CONTRACTS.')
L('SF186', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'), O('SET', 'TIME', 'de 2024', 'during_year', vh='2024', ys='STATED')],
  add(uni(P('SF186'), 'CONTRACTS'), 'TIME', 'during_year', '2024'), gaps=['UNIVERSE_FRAGMENT'], notes='«y los … ?» elíptico -> delta.')
alt = ns('CONTRACTS', ('TIME', 'during_year', '2024'))
L('SF187', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'), O('SET', 'TIME', 'de 2024', 'during_year', vh='2024', ys='STATED')],
  add(uni(P('SF187'), 'CONTRACTS'), 'TIME', 'during_year', '2024'), alts=[alt], amb='AMBIGUOUS', conf='MEDIUM', gaps=['UNIVERSE_FRAGMENT'],
  notes='A4: universo + año sin condición de contenido: depende del previo -> delta. Alternativa: búsqueda nueva de todos los contratos de 2024.')
L('SF188', [O('ANSWER', 'TIME', 'de abril a junio de 2025', 'between_dates', vh='2025-04-01..2025-06-30', ys='STATED')],
  add(P('SF188'), 'TIME', 'between_dates', '2025-04-01..2025-06-30'), turn_kind='ANSWER_TO_CLARIFICATION', notes='A7: respuesta compatible.')
L('SF189', [O('ANSWER', 'BUYER', 'la del Caribe', 'is', vh='Alcaldía de Villanueva (región Caribe: La Guajira o Bolívar)', unres=True)], None,
  turn_kind='ANSWER_TO_CLARIFICATION', disp='REASK', reason='ANSWER_UNRESOLVED', gaps=['REGION_VOCAB'],
  notes='A7: tras aplicar la respuesta quedan dos opciones del Caribe (La Guajira, Bolívar) -> REASK.')
L('SF190', [], ns('PROCESSES', ('TOPIC', 'contains', 'dotación escolar'), ('BUYER', 'is', 'Gobernación del Chocó'), ('STATE', 'family', 'open')),
  **NS, gaps=['UNIVERSE_FRAGMENT'], notes='A7: consulta autosuficiente cancela la pendiente.')
L('SF191', [O('CANCEL_PENDING', None, 'no, mejor'),
            O('SET', 'VALUE.lower', 'pasen de 200 millones', 'greater_than', vh='200 millones', rrs=True, us='STATED')],
  rep(P('SF191'), 'VALUE', value_hint='200 millones'), gaps=['PENDING_REJECTED'],
  notes='A7: «no, mejor X» con X fragmento: cancela la pregunta y aplica X sobre el marco pendiente (el semestre/trimestre sin año se descarta con la pregunta).')

# ---------------- derived effects / echo (SCHEMA §6/§7) ----------------
SIDE = {'greater_than': 'lower', 'greater_or_equal': 'lower', 'less_than': 'upper', 'less_or_equal': 'upper'}
def vside(c):
    s = SIDE[c['operator']]
    if c['polarity'] == 'NEGATIVE': s = 'upper' if s == 'lower' else 'lower'
    return s

def derive(rec):
    p = rec['prior']; pir = p['ir']; rir = rec['resulting_ir']; ops = rec['ops']; d = rec['expected_disposition']
    eff = []
    compute = rir is not None and (d in ('EXECUTE', 'NEW_SEARCH') or rec['disposition_reason'] == 'CONTEXT_EXPIRED_CONFIRM') \
        and rec['m2a_scope'] not in ('OUT_ANCHOR_NOT_SEARCH',)
    if compute and pir is not None and rec['turn_kind'] != 'NEW_SEARCH':
        slots = {o['slot'] for o in ops}
        pv = [c for c in pir['conditions'] if c['dimension'] == 'VALUE']
        rv = [c for c in rir['conditions'] if c['dimension'] == 'VALUE']
        for c in pv:
            side = vside(c)
            if not any(vside(x) == side for x in rv) and ('VALUE.' + side) not in slots and 'VALUE' not in slots - {'VALUE'} \
                    and any(o['slot'].startswith('VALUE') for o in ops):
                eff.append('VALUE_RANGE_REPLACED'); break
        if 'VALUE_RANGE_REPLACED' not in eff and any(o['op'] == 'CLEAR' and o['slot'] == 'VALUE' for o in ops):
            for c in pv:
                if not any(o['slot'] == 'VALUE.' + vside(c) for o in ops):
                    eff.append('VALUE_RANGE_REPLACED'); break
        ptop = [c for c in pir['conditions'] if c['dimension'] == 'TOPIC' and c['polarity'] == 'POSITIVE']
        for o in ops:
            if o['op'] == 'SET' and o['slot'] == 'TOPIC' and o['polarity'] == 'POSITIVE' and ptop and ptop[0]['value_hint'] != o['value_hint']:
                eff.append('TOPIC_REPLACED')
            if o['op'] == 'SET' and o['polarity'] == 'NEGATIVE' and o['slot'] not in ('VALUE.lower', 'VALUE.upper'):
                pos = [c for c in pir['conditions'] if c['dimension'] == o['slot'] and c['polarity'] == 'POSITIVE']
                if len(pos) == 1 and pos[0]['value_hint'] == o['value_hint']:
                    eff.append('POLARITY_INVERTED')
        if pir['view_source'] == 'DERIVED' and rir['product_view'] != pir['product_view']:
            eff.append('VIEW_REDERIVED')
        if any(o['op'] == 'SET_UNIVERSE' for o in ops) and rir['universe'] != pir['universe']:
            eff.append('UNIVERSE_CHANGED')
        if pir['cardinality'] and rir['cardinality'] and any(o['op'] == 'SET_ORDER' for o in ops) and 'CARDINALITY' not in slots:
            eff.append('TOPN_CARRIED')
    if compute:
        if rir['cardinality'] and rir['cardinality']['n_requested'] > 100: eff.append('TOPN_CAPPED')
        if rir['order'] and rir['order']['direction_source'] == 'DEFAULTED': eff.append('ORDER_DIRECTION_DEFAULTED')
    if any(o['op'] == 'CANCEL_PENDING' for o in ops) or (rec['turn_kind'] == 'NEW_SEARCH' and p['state'] == 'PENDING'):
        eff.append('PENDING_CANCELLED')
    if rec['m2a_scope'] == 'IN_NEW_SEARCH' and p['owner'] == 'FOUNDATION_SEARCH' and p['state'] in ('EXECUTED', 'EXECUTED_EMPTY', 'NOT_EXECUTED'):
        eff.append('CONTEXT_RESET')
    eff = list(dict.fromkeys(eff))
    # echo
    echo = []
    if rec['disposition_reason'] == 'CONTEXT_EXPIRED_CONFIRM':
        echo = ['CONTEXT_EXPIRED']
    elif d in ('EXECUTE', 'NEW_SEARCH') and rir is not None:
        if rec['turn_kind'] in ('DELTA', 'ANSWER_TO_CLARIFICATION') and pir is not None:
            touched = set()
            for o in ops:
                if o['slot']: touched.add(o['slot'].split('.')[0])
            surv = any(c['dimension'] not in touched and c in rir['conditions'] for c in pir['conditions'])
            surv |= bool(pir['order']) and 'ORDER' not in touched and rir['order'] == pir['order']
            surv |= bool(pir['cardinality']) and 'CARDINALITY' not in touched and rir['cardinality'] == pir['cardinality']
            surv |= pir['universe'] != 'UNSPECIFIED' and 'UNIVERSE' not in touched
            if surv: echo.append('CARRIED')
        src = [(o['inherited']['unit_source'], o['inherited']['year_source']) for o in ops]
        if any(u == 'PREVIOUS_ACCEPTED_VALUE' for u, _ in src): echo.append('UNIT_INHERITED')
        if any(u == 'SIBLING_IN_RANGE' for u, _ in src): echo.append('UNIT_SHARED_IN_RANGE')
        if any(y == 'PREVIOUS_ACCEPTED_TIME' for _, y in src): echo.append('YEAR_INHERITED')
        if any(y == 'CLOCK_INFERRED' for _, y in src): echo.append('YEAR_CLOCK_INFERRED')
        if any(y == 'RELATIVE_TO_CLOCK' for _, y in src): echo.append('RELATIVE_WINDOW_RESOLVED')
        for e in ['VALUE_RANGE_REPLACED', 'TOPIC_REPLACED', 'POLARITY_INVERTED', 'UNIVERSE_CHANGED', 'TOPN_CARRIED', 'TOPN_CAPPED',
                  'ORDER_DIRECTION_DEFAULTED', 'CONTEXT_RESET']:
            if e in eff: echo.append(e)
        if rir['view_source'] == 'DERIVED': echo.append('VIEW_DERIVED')
        if 'VIEW_REDERIVED' in eff: echo.append('VIEW_REDERIVED')
    return eff, echo

# ---------------- emit ----------------
missing = [g for g in order if g not in LAB]; extra = [g for g in LAB if g not in cases]
assert not missing and not extra, (missing, extra)
INPUT = ['schema', 'gold_id', 'slice', 'followup_text', 'followup_sha256', 'source_kind', 'input_kind', 'prior']
with open(OUT, 'w', encoding='utf-8') as fh:
    for gid in sorted(order):
        rec = {k: copy.deepcopy(cases[gid][k]) for k in INPUT}
        lab = LAB[gid]
        rec.update(turn_kind=lab['turn_kind'], m2a_scope=lab['m2a_scope'], ops=lab['ops'], resulting_ir=lab['resulting_ir'],
                   alternatives=lab['alternatives'], expected_disposition=lab['expected_disposition'],
                   disposition_reason=lab['disposition_reason'])
        eff, echo = derive(rec)
        rec.update(effects=eff, expected_echo=echo, ambiguity=lab['ambiguity'], confidence=lab['confidence'],
                   gap_codes=lab['gap_codes'], labeler='SB', notes=lab['notes'])
        fh.write(json.dumps(rec, ensure_ascii=False) + '\n')
json.dump(CHANGE, open(os.path.join(HERE, 'change_rules.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
print('wrote', len(order))
