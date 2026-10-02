"""SB labeler (blind) - builds labels_v2.jsonl from cases.jsonl. Labels are hand-written per case below."""
import copy, json, os

S = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CASES = os.path.join(S, 'synthetic_for_labelers', 'cases.jsonl')
OUT = os.path.join(S, 'synth_labeler_SB', 'labels_v2.jsonl')

cases = {}
order = []
for line in open(CASES, encoding='utf-8'):
    r = json.loads(line)
    cases[r['gold_id']] = r
    order.append(r['gold_id'])

LAB = {}

# ---------------- helpers ----------------
def O(op, slot, quote, operator=None, pol='POSITIVE', vh=None, level=None, rrs=False, dpv=False, ys=None, us=None):
    if op == 'CLEAR':
        pol = None
    return {'op': op, 'slot': slot, 'operator': operator, 'polarity': pol, 'level': level, 'quote': quote,
            'value_hint': vh, 'refers_to_result_set': rrs, 'depends_on_prior_value': dpv,
            'inherited': {'year_source': ys, 'unit_source': us}}

def P(gid):
    ir = cases[gid]['prior']['ir']
    return copy.deepcopy(ir) if ir is not None else None

def nid(ir):
    n = 0
    for c in ir['conditions']:
        n = max(n, int(c['id'][1:]))
    return f'c{n+1}'

def add(ir, dim, op, vh, pol='POSITIVE', level=None, unit=None):
    ir['conditions'].append({'id': nid(ir), 'dimension': dim, 'operator': op, 'polarity': pol,
                             'value_hint': vh, 'level': level, 'unit': unit})
    return ir

def rep(ir, dim, **kw):
    """replace the (single) condition of a dimension in place, keeping its id"""
    hits = [c for c in ir['conditions'] if c['dimension'] == dim]
    assert len(hits) == 1, (dim, ir)
    hits[0].update(kw)
    return ir

def drop(ir, dim=None, cid=None):
    ir['conditions'] = [c for c in ir['conditions']
                        if not ((dim and c['dimension'] == dim) or (cid and c['id'] == cid))]
    return ir

def order_(ir, op):
    ir['order'] = None if op is None else {'operator': op, 'polarity': 'POSITIVE'}
    return ir

def card(ir, n):
    ir['cardinality'] = None if n is None else {'operator': 'top_n', 'value_hint': str(n)}
    return ir

def uni(ir, u, view='RECORDS'):
    ir['universe'] = u
    ir['view'] = view
    ir['product_view'] = None
    ir['view_source'] = 'USER'
    ir.pop('universes', None)
    return ir

def fresh(u, view='RECORDS'):
    return {'operation': 'SEARCH', 'universe': u, 'view': view, 'product_view': None, 'view_source': 'USER',
            'conditions': [], 'order': None, 'cardinality': None}

def L(gid, ops, ir, turn_kind='DELTA', scope='IN', effects=(), disp='EXECUTE', reason=None, echo=('CARRIED',),
      amb='CLEAR', alts=(), conf='HIGH', gaps=(), notes=''):
    assert gid not in LAB, gid
    LAB[gid] = dict(turn_kind=turn_kind, m2a_scope=scope, ops=ops, resulting_ir=ir, effects=list(effects),
                    expected_disposition=disp, disposition_reason=reason, expected_echo=list(echo),
                    ambiguity=amb, alternatives=list(alts), confidence=conf, gap_codes=list(gaps),
                    labeler='SB', notes=notes)

EXP = dict(disp='CLARIFY', reason='CONTEXT_EXPIRED_CONFIRM', echo=('CONTEXT_EXPIRED',))
F15 = dict(scope='OUT_RELATIVE_TO_PREVIOUS', disp='CLARIFY', reason='RELATIVE_TO_PREVIOUS_VALUE', echo=())
HAND_LEG = dict(scope='OUT_ANCHOR_NOT_SEARCH', disp='HANDOFF_LEGACY', reason='ANCHOR_OWNER_LEGACY', echo=())
EXT = dict(turn_kind='OTHER_OPERATION', scope='OUT_REFERENT_EXTENSIONAL', disp='HANDOFF_LEGACY',
           reason='REFERENT_EXTENSIONAL', echo=())
PROT = dict(turn_kind='PROTOCOL', scope='OUT_OPERATION_CHANGE', echo=())

# ---------------- SF001-SF014: exclusions / restrictions ----------------
L('SF001', [O('SET', 'TOPIC', 'papelería', 'contains', 'NEGATIVE', 'papelería')],
  add(P('SF001'), 'TOPIC', 'contains', 'papelería', 'NEGATIVE'),
  notes='Exclusión de tema sin tema previo: se añade un átomo TOPIC NEGATIVE; no hay TOPIC previo que reemplazar (F3 no aplica).')
L('SF002', [O('SET', 'TOPIC', 'papelería', 'contains', 'POSITIVE', 'papelería')],
  add(P('SF002'), 'TOPIC', 'contains', 'papelería'),
  notes='Tema nuevo sin tema previo: se añade.')
L('SF003', [O('SET', 'GEOGRAPHY', 'Eje Cafetero', 'is', 'NEGATIVE', 'Eje Cafetero', 'REGION', rrs=True)],
  add(P('SF003'), 'GEOGRAPHY', 'is', 'Eje Cafetero', 'NEGATIVE', 'REGION'),
  notes='Eje Cafetero está en la lista cerrada de regiones del producto (Caldas, Quindío, Risaralda): F13 no aclara. «los del» apunta al conjunto mostrado; F4: re-consulta.')
L('SF004', [O('SET', 'GEOGRAPHY', 'Eje Cafetero', 'is', 'POSITIVE', 'Eje Cafetero', 'REGION', rrs=True)],
  add(P('SF004'), 'GEOGRAPHY', 'is', 'Eje Cafetero', 'POSITIVE', 'REGION'),
  notes='Región en lista cerrada; F4: re-consulta con la condición añadida.')
L('SF005', [O('SET', 'BUYER', 'SENA', 'is', 'NEGATIVE', 'SENA')],
  add(P('SF005'), 'BUYER', 'is', 'SENA', 'NEGATIVE'),
  notes='SENA es sigla de entidad compradora (contrato §4 BUYER).')
L('SF006', [O('SET', 'BUYER', 'SENA', 'is', 'POSITIVE', 'SENA')],
  add(P('SF006'), 'BUYER', 'is', 'SENA'))
L('SF007', [O('SET', 'MODALITY', 'contratación directa', 'is', 'NEGATIVE', 'contratación directa')],
  add(P('SF007'), 'MODALITY', 'is', 'contratación directa', 'NEGATIVE'))
L('SF008', [O('SET', 'MODALITY', 'contratación directa', 'is', 'POSITIVE', 'contratación directa')],
  add(P('SF008'), 'MODALITY', 'is', 'contratación directa'))
L('SF009', [O('SET', 'STATE', 'adjudicados', 'is', 'NEGATIVE', 'awarded', rrs=True)],
  add(P('SF009'), 'STATE', 'is', 'awarded', 'NEGATIVE'),
  notes='C4: adjudicado es estado SECOP exacto -> STATE is. Universo PROCESSES citado, sin ACTOR: no aplica la política ACTOR×STATE.')
L('SF010', [O('SET', 'STATE', 'adjudicados', 'is', 'POSITIVE', 'awarded', rrs=True)],
  add(P('SF010'), 'STATE', 'is', 'awarded'))
ir = P('SF011'); rep(ir, 'GEOGRAPHY', polarity='NEGATIVE')
L('SF011', [O('SET', 'GEOGRAPHY', 'Cali', 'is', 'NEGATIVE', 'Cali', 'MUNICIPALITY', rrs=True)], ir,
  amb='AMBIGUOUS', conf='MEDIUM', gaps=['GEOBUYER'],
  alts=['CLARIFY: el previo ya se limitaba a Cali; excluir Cali contradice el ancla (¿invertir el filtro o error del usuario?)',
        'BUYER NEGATIVE Alcaldía de Cali (de <lugar> como comprador), conservando GEOGRAPHY Cali'],
  notes='Se niega exactamente el único valor positivo de la ranura: la lectura literal sustituye Cali POSITIVE por Cali NEGATIVE (si se sumara, el AND sería vacío). D7: «de Cali» sin cabeza institucional = GEOGRAPHY. NEW_GAP: POLARITY_FLIP_SAME_VALUE (negar el valor positivo vigente de la misma ranura: ¿invertir, o aclarar?).')
ir = P('SF012'); rep(ir, 'BUYER', polarity='NEGATIVE')
L('SF012', [O('SET', 'BUYER', 'Alcaldía de Pasto', 'is', 'NEGATIVE', 'Alcaldía de Pasto')], ir,
  amb='AMBIGUOUS', conf='MEDIUM',
  alts=['CLARIFY: el previo era exactamente la Alcaldía de Pasto; excluirla contradice el ancla'],
  notes='Mismo patrón que SF011 en BUYER: el resultado son procesos de aseo de cualquier comprador salvo la Alcaldía de Pasto. NEW_GAP: POLARITY_FLIP_SAME_VALUE.')
L('SF013', [O('SET', 'VALUE.upper', 'más de 500 millones', 'greater_than', 'NEGATIVE', '500 millones', rrs=True, us='STATED')],
  add(P('SF013'), 'VALUE', 'greater_than', '500 millones', 'NEGATIVE', unit='MILLONES'),
  conf='MEDIUM', gaps=['VCOMP'],
  notes='Exclusión de una cota inferior: NOT(valor > 500 millones) = valor <= 500 millones. Se transcribe con polaridad NEGATIVE (E2: negación de átomo = polarity) y se etiqueta la ranura por el lado efectivo (VALUE.upper). Equivalente ejecutable: less_or_equal 500 millones.')
L('SF014', [O('SET', 'TIME', 'de 2023', 'during_year', 'NEGATIVE', '2023', rrs=True, ys='STATED')],
  add(P('SF014'), 'TIME', 'during_year', '2023', 'NEGATIVE'),
  notes='Sin TIME previo; se excluye un año.')

# ---------------- SF015-SF024: order / cardinality ----------------
ir = order_(card(P('SF015'), 5), 'value_desc')
L('SF015', [O('SET_ORDER', 'ORDER', 'más caros', 'value_desc', rrs=True),
            O('SET', 'CARDINALITY', '5', 'top_n', vh='5', rrs=True)], ir,
  notes='Cifra explícita 5 (<= tope 100). F4: re-consulta.')
L('SF016', [O('SET_ORDER', 'ORDER', 'más caros', 'value_desc', rrs=True)], order_(P('SF016'), 'value_desc'),
  conf='MEDIUM',
  notes='§15/ADR-103 enm.1: plural sin cifra -> ORDER sin CARDINALITY. «solo» no aporta cifra.')
L('SF017', [O('SET_ORDER', 'ORDER', 'mayor valor', 'value_desc', rrs=True),
            O('SET', 'CARDINALITY', 'el', 'top_n', vh='1', rrs=True)],
  order_(card(P('SF017'), 1), 'value_desc'),
  conf='MEDIUM',
  notes='Superlativo singular «el de mayor valor» -> ORDER value_desc + CARDINALITY 1 (§15). La cita de cardinalidad es el número gramatical (artículo «el»). F4: re-consulta, no se escoge del conjunto mostrado.')
ir = order_(P('SF018'), 'date_desc')
L('SF018', [O('SET_ORDER', 'ORDER', 'por fecha, primero los más recientes', 'date_desc', rrs=True)], ir,
  echo=('CARRIED', 'TOPN_CARRIED'),
  notes='F10: se conserva top_n 10 y se reordena por fecha descendente (los 10 más recientes), anunciado.')
ir = order_(P('SF019'), 'date_asc')
L('SF019', [O('SET_ORDER', 'ORDER', 'por fecha, primero los más antiguos', 'date_asc', rrs=True)], ir,
  echo=('CARRIED', 'TOPN_CARRIED'),
  notes='F10: se conserva top_n 10, orden por fecha ascendente.')
L('SF020', [O('SET_ORDER', 'ORDER', 'más recientes', 'date_desc', rrs=True),
            O('SET', 'CARDINALITY', '20', 'top_n', vh='20', rrs=True)],
  order_(card(P('SF020'), 20), 'date_desc'))
L('SF021', [O('SET_ORDER', 'ORDER', 'mayor valor', 'value_desc', rrs=True),
            O('SET', 'CARDINALITY', '250', 'top_n', vh='250', rrs=True)],
  order_(card(P('SF021'), 250), 'value_desc'),
  amb='AMBIGUOUS', conf='LOW',
  alts=['CLARIFY (TOP_N_ABOVE_MAX): C0 P8 dice «por encima, aclarar»'],
  notes='P8 (C0 §11 / ADR-103): «Máximo 100, anunciado («te muestro los 100 de mayor valor»); por encima, aclarar». El ejemplo de la propia regla («los 500 más caros») se responde con 100 anunciados, así que la lectura elegida es EXECUTE con tope 100 anunciado; la IR conserva lo pedido (250) y el host lo capa. Clase de eco no listada: TOPN_CAPPED. NEW_GAP: TOPN_CAP_ABOVE_MAX (P8 es internamente contradictoria: tope anunciado vs aclarar).')
L('SF022', [O('SET_ORDER', 'ORDER', 'mayor valor', 'value_desc', rrs=True),
            O('SET', 'CARDINALITY', '100', 'top_n', vh='100', rrs=True)],
  order_(card(P('SF022'), 100), 'value_desc'),
  notes='100 = tope P8, dentro del límite.')
L('SF023', [O('CLEAR', 'ORDER', 'quita el orden'), O('CLEAR', 'CARDINALITY', 'quiero verlos todos', rrs=True)],
  order_(card(P('SF023'), None), None),
  notes='Dos remociones citadas: orden y top-N.')
L('SF024', [O('CLEAR', 'CARDINALITY', 'no solo 10, todos', rrs=True),
            O('KEEP', 'ORDER', 'déjalos por valor', 'value_desc', vh='valor descendente (como el previo)', rrs=True, dpv=True)],
  card(P('SF024'), None),
  notes='Se quita el top-N; el orden por valor se mantiene (KEEP: la dirección descendente sale del previo).')

# ---------------- SF025-SF038: time ----------------
L('SF025', [O('SET', 'TIME', 'este año', 'relative_period', vh='este año (2026)', ys='CLOCK_INFERRED')],
  add(P('SF025'), 'TIME', 'relative_period', 'este año (2026)'),
  notes='§4: «este año» = relative_period; resuelto con el reloj (hoy 2026-10-01). No es herencia F7, por eso no lleva YEAR_INHERITED.')
L('SF026', [O('SET', 'TIME', 'de este mes', 'relative_period', vh='este mes (octubre de 2026)', rrs=True, ys='CLOCK_INFERRED')],
  add(P('SF026'), 'TIME', 'relative_period', 'este mes (octubre de 2026)'),
  notes='Hoy 2026-10-01: «este mes» es octubre de 2026 (recién iniciado; el resultado puede ser casi vacío, pero el significado es claro).')
L('SF027', [O('SET', 'TIME', 'de los últimos 6 meses', 'relative_period', vh='últimos 6 meses (hasta 2026-10-01)', ys='CLOCK_INFERRED')],
  add(P('SF027'), 'TIME', 'relative_period', 'últimos 6 meses (hasta 2026-10-01)'))
L('SF028', [O('SET', 'TIME', 'los últimos 3 meses', 'relative_period', vh='últimos 3 meses (hasta 2026-10-01)', ys='CLOCK_INFERRED')],
  rep(P('SF028'), 'TIME', operator='relative_period', value_hint='últimos 3 meses (hasta 2026-10-01)'),
  notes='Reemplaza TIME 2024 (una sola ranura TIME).')
L('SF029', [O('SET', 'TIME', 'los últimos 12 meses', 'relative_period', vh='últimos 12 meses (hasta 2026-10-01)', ys='CLOCK_INFERRED')],
  rep(P('SF029'), 'TIME', operator='relative_period', value_hint='últimos 12 meses (hasta 2026-10-01)'))
L('SF030', [O('SET', 'TIME', 'el año en curso', 'relative_period', vh='año en curso (2026)', ys='CLOCK_INFERRED')],
  rep(P('SF030'), 'TIME', operator='relative_period', value_hint='año en curso (2026)'))
L('SF031', [O('SET', 'TIME', 'marzo', 'during_month', vh='marzo (año sin resolver: 2023, 2024 o 2025)', ys=None)],
  rep(P('SF031'), 'TIME', operator='during_month', value_hint='marzo (año sin resolver: 2023, 2024 o 2025)'),
  disp='CLARIFY', reason='MONTH_YEAR_MULTI_YEAR_PRIOR', echo=(), amb='AMBIGUOUS', conf='HIGH', gaps=['MONTHYEAR'],
  alts=['marzo de cada año 2023-2025 («solo» como restricción dentro del rango)'],
  notes='F7: el TIME previo abarca varios años -> CLARIFY. year_source queda null porque no hay año único que heredar.')
L('SF032', [O('SET', 'TIME', 'marzo', 'during_month', vh='marzo de 2024', ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF032'), 'TIME', operator='during_month', value_hint='2024-03'),
  echo=('CARRIED', 'YEAR_INHERITED'),
  notes='F7: hereda el único año activo (2024), anunciado.')
L('SF033', [O('SET', 'TIME', 'en octubre', 'during_month', vh='octubre (año sin resolver: ventana relativa previa)', ys=None)],
  rep(P('SF033'), 'TIME', operator='during_month', value_hint='octubre (año sin resolver)'),
  disp='CLARIFY', reason='MONTH_YEAR_RELATIVE_PRIOR', echo=(), gaps=['MONTHYEAR'],
  notes='F7: el TIME previo es una ventana relativa -> CLARIFY (aunque la ventana de 6 meses hasta 2026-10-01 sólo roza octubre de 2026).')
L('SF034', [O('SET', 'TIME', 'en abril', 'during_month', vh='abril de 2026', ys='CLOCK_INFERRED')],
  add(P('SF034'), 'TIME', 'during_month', '2026-04'),
  echo=('CARRIED', 'YEAR_INHERITED'),
  notes='F7 sin TIME previo: ocurrencia más reciente no posterior a hoy (2026-10-01) = abril de 2026, anunciado.')
L('SF035', [O('SET', 'TIME', 'en diciembre', 'during_month', vh='diciembre de 2025', ys='CLOCK_INFERRED')],
  add(P('SF035'), 'TIME', 'during_month', '2025-12'),
  echo=('CARRIED', 'YEAR_INHERITED'),
  notes='F7 sin TIME previo: diciembre más reciente no posterior a hoy = diciembre de 2025.')
L('SF036', [O('SET', 'TIME', 'este año', 'relative_period', vh='este año (2026)', ys='CLOCK_INFERRED')],
  rep(P('SF036'), 'TIME', operator='relative_period', value_hint='este año (2026)'))
L('SF037', [O('SET', 'TIME', 'el mes siguiente', 'during_month', vh='abril de 2024', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF037'), 'TIME', operator='during_month', value_hint='2024-04'), **F15)
L('SF038', [O('SET', 'TIME', 'el año anterior', 'during_year', vh='2023', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF038'), 'TIME', value_hint='2023'), **F15, amb='AMBIGUOUS', conf='MEDIUM',
  alts=['«el año anterior» = año pasado respecto del reloj (2025): SET TIME during_year 2025, CLOCK_INFERRED, EXECUTE'],
  notes='Lectura principal: anterior al año vigente del ancla (2024 -> 2023) -> F15. NEW_GAP: ANTERIOR_PRIOR_VS_CLOCK (no hay regla que decida si «anterior» se ancla al valor previo o al reloj).')

# ---------------- SF039-SF046: relative operators (F15) ----------------
L('SF039', [O('SET', 'VALUE.lower', 'bájalo un poco', 'greater_or_equal', vh='menos de 200 millones (cantidad sin precisar)', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
  rep(P('SF039'), 'VALUE', value_hint='UNRESOLVED: algo menos de 200 millones'), **F15,
  notes='F15. resulting_ir conserva la forma; el valor no es determinable («un poco»).')
L('SF040', [O('SET', 'VALUE.lower', 'el doble', 'greater_or_equal', vh='200 millones', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
  rep(P('SF040'), 'VALUE', value_hint='200 millones'), **F15)
ir = P('SF041'); rep(ir, 'TOPIC', value_hint='vigilancia')
L('SF041', [O('SET', 'TOPIC', 'vigilancia', 'contains', vh='vigilancia'),
            O('KEEP', 'BUYER', 'el mismo comprador', 'is', vh='Alcaldía de Ibagué', dpv=True)], ir,
  effects=['TOPIC_REPLACED'], echo=('CARRIED', 'TOPIC_REPLACED'),
  notes='F3: tema nuevo reemplaza al previo. «el mismo comprador» es KEEP explícito (depende del valor previo).')
L('SF042', [O('SET', 'TIME', 'los seis meses siguientes', 'between_dates', vh='julio a diciembre de 2024', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF042'), 'TIME', value_hint='julio a diciembre de 2024'), **F15)
L('SF043', [O('SET', 'VALUE.upper', 'súbelo un poquito', 'less_or_equal', vh='algo más de 50 millones (cantidad sin precisar)', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
  rep(P('SF043'), 'VALUE', value_hint='UNRESOLVED: algo más de 50 millones'), **F15)
L('SF044', [O('SET', 'VALUE.lower', 'el triple', 'greater_or_equal', vh='90 millones', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
  rep(P('SF044'), 'VALUE', value_hint='90 millones'), **F15)
L('SF045', [O('SET', 'TIME', 'un trimestre antes', 'between_dates', vh='noviembre de 2024 a enero de 2025', dpv=True, ys='PREVIOUS_ACCEPTED_TIME')],
  rep(P('SF045'), 'TIME', value_hint='noviembre de 2024 a enero de 2025'), **F15)
ir = P('SF046')
ir['conditions'][2]['value_hint'] = '50 millones'; ir['conditions'][3]['value_hint'] = '150 millones'
L('SF046', [O('SET', 'VALUE.lower', 'la mitad de eso', 'greater_or_equal', vh='50 millones', dpv=True, us='PREVIOUS_ACCEPTED_VALUE'),
            O('SET', 'VALUE.upper', 'la mitad de eso', 'less_or_equal', vh='150 millones', dpv=True, us='PREVIOUS_ACCEPTED_VALUE')],
  ir, **F15, conf='MEDIUM',
  notes='F15. «eso» puede apuntar a una cota o al rango entero; se etiqueta la mitad de ambas cotas (50-150 millones).')

# ---------------- SF047-SF064: pending clarifications ----------------
def ns_alquiler(gid, uni_, buyer, time_op, time_vh, extra_notes=''):
    ir = fresh(uni_)
    add(ir, 'TOPIC', 'contains', 'alquiler de oficinas')
    add(ir, 'BUYER', 'is', buyer)
    add(ir, 'TIME', time_op, time_vh)
    return ir

t = cases['SF047']['followup_text']
L('SF047', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'alquiler de oficinas', 'contains', vh='alquiler de oficinas'),
            O('SET', 'BUYER', 'Alcaldía de Piedecuesta', 'is', vh='Alcaldía de Piedecuesta'),
            O('SET', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED')],
  ns_alquiler('SF047', 'CONTRACTS', 'Alcaldía de Piedecuesta', 'between_dates', 'enero a junio de 2025'),
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), conf='MEDIUM', gaps=['UNIVERSE_FRAGMENT'],
  notes='Petición autocontenida (sustantivo de universo + tema + comprador + periodo con año) que cambia tema y comprador: abandona la aclaración pendiente y no arrastra el VALUE previo.')
L('SF048', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'alquiler de oficinas', 'contains', vh='alquiler de oficinas'),
            O('SET', 'BUYER', 'Gobernación de Santander', 'is', vh='Gobernación de Santander'),
            O('SET', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED')],
  ns_alquiler('SF048', 'CONTRACTS', 'Gobernación de Santander', 'between_dates', 'enero a junio de 2025'),
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), conf='MEDIUM', gaps=['UNIVERSE_FRAGMENT'],
  notes='Como SF047 con otro comprador.')
L('SF049', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'alquiler de oficinas', 'contains', vh='alquiler de oficinas'),
            O('SET', 'BUYER', 'Alcaldía de Piedecuesta', 'is', vh='Alcaldía de Piedecuesta'),
            O('SET', 'TIME', 'en 2024', 'during_year', vh='2024', ys='STATED')],
  ns_alquiler('SF049', 'CONTRACTS', 'Alcaldía de Piedecuesta', 'during_year', '2024'),
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), conf='MEDIUM', gaps=['UNIVERSE_FRAGMENT'],
  notes='Petición autocontenida; abandona la aclaración pendiente.')
L('SF050', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'alquiler de oficinas', 'contains', vh='alquiler de oficinas'),
            O('SET', 'BUYER', 'Alcaldía de Piedecuesta', 'is', vh='Alcaldía de Piedecuesta'),
            O('SET', 'TIME', 'en 2024', 'during_year', vh='2024', ys='STATED')],
  ns_alquiler('SF050', 'CONTRACTS', 'Alcaldía de Piedecuesta', 'during_year', '2024'),
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), conf='MEDIUM', gaps=['UNIVERSE_FRAGMENT'],
  notes='Mismo texto que SF049; difiere sólo en la cadena (un reask previo, 70 s). El reask no cambia la lectura: petición autocontenida.')
ir = P('SF051'); drop(ir, 'VALUE')
add(ir, 'TIME', 'between_dates', 'enero a junio de 2025')
add(ir, 'VALUE', 'less_than', '40 millones', unit='MILLONES')
L('SF051', [O('ANSWER', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED'),
            O('SET', 'VALUE.upper', 'menos de 40 millones', 'less_than', vh='40 millones', us='STATED')], ir,
  turn_kind='ANSWER_TO_CLARIFICATION', effects=['VALUE_RANGE_REPLACED'], echo=('CARRIED', 'VALUE_RANGE_REPLACED'), conf='MEDIUM',
  notes='Responde la aclaración pendiente (periodo con año) y además cambia el valor: <40 millones con >150 millones deja el rango sin interior -> F2 sustituye la restricción entera, anunciado.')
ir = ns_alquiler('SF052', 'PROCESSES', 'Alcaldía de Piedecuesta', 'between_dates', 'enero a junio de 2025')
L('SF052', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES'),
            O('SET', 'TOPIC', 'alquiler de oficinas', 'contains', vh='alquiler de oficinas'),
            O('SET', 'BUYER', 'Alcaldía de Piedecuesta', 'is', vh='Alcaldía de Piedecuesta'),
            O('SET', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED')], ir,
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), conf='MEDIUM', gaps=['UNIVERSE_FRAGMENT'],
  notes='Petición autocontenida sobre procesos.')
ir = P('SF053'); rep(ir, 'BUYER', value_hint='Alcaldía de Piedecuesta'); add(ir, 'TIME', 'between_dates', 'enero a junio de 2025')
L('SF053', [O('SET', 'BUYER', 'Alcaldía de Piedecuesta', 'is', vh='Alcaldía de Piedecuesta', rrs=True),
            O('ANSWER', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED')], ir,
  turn_kind='ANSWER_TO_CLARIFICATION', conf='MEDIUM', amb='AMBIGUOUS',
  alts=['NEW_SEARCH: contratos de la Alcaldía de Piedecuesta en ene-jun 2025 sin arrastrar tema ni valor'],
  notes='Fragmento anafórico («los de») sin sustantivo de universo: delta que responde el periodo pendiente y reemplaza el comprador; F1 conserva tema (interventoría) y valor (>150 millones).')
ir = P('SF054'); rep(ir, 'TIME', operator='between_dates', value_hint='enero a junio de 2025')
L('SF054', [O('SET', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED')], ir)
ir = fresh('CONTRACTS'); add(ir, 'TOPIC', 'contains', 'interventoría'); add(ir, 'BUYER', 'is', 'Alcaldía de Floridablanca')
add(ir, 'VALUE', 'greater_than', '150 millones', unit='MILLONES'); add(ir, 'TIME', 'between_dates', 'enero a junio de 2025')
L('SF055', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'interventoría', 'contains', vh='interventoría'),
            O('SET', 'BUYER', 'Alcaldía de Floridablanca', 'is', vh='Alcaldía de Floridablanca'),
            O('SET', 'VALUE.lower', 'más de 150 millones', 'greater_than', vh='150 millones', us='STATED'),
            O('SET', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED')], ir,
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), amb='AMBIGUOUS', conf='MEDIUM',
  gaps=['UNIVERSE_FRAGMENT'],
  alts=['ANSWER_TO_CLARIFICATION / EXECUTE: reformulación completa que responde el periodo pendiente; la IR resultante es idéntica'],
  notes='Repite la búsqueda completa añadiendo el periodo con año. Ambas lecturas producen la misma IR; difieren sólo en la disposición. NEW_GAP: SELF_CONTAINED_RESTATEMENT_DURING_PENDING.')
ir = P('SF056'); add(ir, 'TIME', 'between_dates', 'enero a junio de 2025')
L('SF056', [O('ANSWER', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED')], ir,
  turn_kind='ANSWER_TO_CLARIFICATION',
  notes='Respuesta directa a PERIOD_WITHOUT_YEAR.')
ir = ns_alquiler('SF057', 'CONTRACTS', 'Alcaldía de Piedecuesta', 'between_dates', 'enero a junio de 2025')
L('SF057', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'alquiler de oficinas', 'contains', vh='alquiler de oficinas'),
            O('SET', 'BUYER', 'Alcaldía de Piedecuesta', 'is', vh='Alcaldía de Piedecuesta'),
            O('SET', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED')], ir,
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(),
  notes='Verbo de búsqueda explícito («busca») + petición autocontenida.')
L('SF058', [O('ANSWER', 'BUYER', 'Alcaldía de San Pedro (Valle del Cauca)', 'is', vh='Alcaldía de San Pedro (Valle del Cauca)')],
  rep(P('SF058'), 'BUYER', value_hint='Alcaldía de San Pedro (Valle del Cauca)'),
  turn_kind='ANSWER_TO_CLARIFICATION',
  notes='Texto con forma de etiqueta de opción de la aclaración BUYER (3 opciones): respuesta por evento.')
L('SF059', [O('ANSWER', 'BUYER', 'Como comprador', 'is', vh='UNRESOLVED: el valor X ofrecido en la aclaración, como comprador', dpv=True)],
  add(P('SF059'), 'BUYER', 'is', 'UNRESOLVED: valor X de la aclaración pendiente'),
  turn_kind='ANSWER_TO_CLARIFICATION', conf='MEDIUM', gaps=['COUNTERPART'],
  notes='F12: respuesta por evento a la opción «comprador». El valor X no está en prior (sólo dimensión y razón), así que resulting_ir lo deja como marcador. NEW_GAP: PENDING_VALUE_NOT_IN_PRIOR (prior.pending no transporta el valor en disputa).')
L('SF060', [O('ANSWER', 'GEOGRAPHY', 'Como lugar', 'is', vh='UNRESOLVED: el valor X ofrecido en la aclaración, como lugar', level='UNKNOWN', dpv=True)],
  add(P('SF060'), 'GEOGRAPHY', 'is', 'UNRESOLVED: valor X de la aclaración pendiente', level='UNKNOWN'),
  turn_kind='ANSWER_TO_CLARIFICATION', conf='MEDIUM', gaps=['COUNTERPART'],
  notes='F12: opción «lugar». level UNKNOWN porque el valor X no viaja en prior. NEW_GAP: PENDING_VALUE_NOT_IN_PRIOR.')
L('SF061', [O('ANSWER', 'BUYER', 'cualquiera, el que sea', 'is', vh='Santa Rosa (cualquiera de las 3 opciones ofrecidas)', dpv=True)],
  rep(P('SF061'), 'BUYER', value_hint='Santa Rosa (OR de las 3 opciones ofrecidas)'),
  turn_kind='ANSWER_TO_CLARIFICATION', amb='AMBIGUOUS', conf='LOW',
  alts=['CLEAR BUYER (no le importa el comprador)', 'REASK: la respuesta no elige una opción'],
  notes='Lectura principal: acepta todas las candidatas (OR de los 3 compradores «Santa Rosa»). NEW_GAP: ANY_OPTION_ANSWER (no hay regla para «cualquiera» como respuesta a una aclaración de opciones).')
ir = P('SF062'); ir['operation'] = 'SUM_VALUE'; ir['view_source'] = None; ir['product_view'] = None
L('SF062', [], ir, turn_kind='OTHER_OPERATION', scope='OUT_OPERATION_CHANGE', disp='HANDOFF_LEGACY',
  reason='OPERATION_HANDOFF:SUM_VALUE', echo=(), amb='AMBIGUOUS', conf='MEDIUM', gaps=['OPSWITCH', 'TOTALES'],
  alts=['CLARIFY/REASK: el comprador «Santa Rosa» sigue ambiguo y la suma también lo necesita'],
  notes='Cambio de operación (SUM_VALUE·TOTAL) en plena aclaración BUYER: fuera de M2a. La IR conserva las condiciones; el aspecto TOTAL no cabe en la forma de prior.ir (se omite). El dueño destino debe resolver todavía el comprador ambiguo.')
ir = P('SF063'); rep(ir, 'TOPIC', value_hint='dotación'); rep(ir, 'BUYER', value_hint='Gobernación del Cauca')
add(ir, 'TIME', 'during_year', '2025')
L('SF063', [O('SET', 'TOPIC', 'dotación', 'contains', vh='dotación'),
            O('SET', 'BUYER', 'Gobernación del Cauca', 'is', vh='Gobernación del Cauca'),
            O('SET', 'TIME', 'en 2025', 'during_year', vh='2025', ys='STATED')], ir,
  effects=['TOPIC_REPLACED'], echo=('CARRIED', 'TOPIC_REPLACED'), amb='AMBIGUOUS', conf='LOW', gaps=['TOPIC'],
  alts=['NEW_SEARCH: «no, mejor …» abandona el ancla; UNSPECIFIED + TOPIC + BUYER + TIME 2025 -> PV1 HISTORICAL_PROCESSES, sin el VALUE >150 millones'],
  notes='Tras dos reasks el usuario rechaza («no, mejor») y reformula sin sustantivo de universo. Lectura por F1: delta que reemplaza tema y comprador, fija 2025 y conserva CONTRACTS y VALUE. NEW_GAP: PENDING_ABANDONED_BY_DELTA (no hay regla para un rechazo que reformula durante una aclaración pendiente).')
ir = P('SF064'); add(ir, 'TIME', 'between_dates', 'enero a junio de 2025')
L('SF064', [O('ANSWER', 'TIME', 'de enero a junio de 2025', 'between_dates', vh='enero a junio de 2025', ys='STATED')], ir,
  turn_kind='ANSWER_TO_CLARIFICATION', gaps=['TTL'], **EXP,
  notes='F11: 2640 s > 30 min -> CLARIFY CONTEXT_EXPIRED_CONFIRM; resulting_ir = continuar respondiendo el periodo pendiente.')

# ---------------- SF065-SF072: extensional referents ----------------
L('SF065', [], P('SF065'), **EXT, conf='MEDIUM', gaps=['SELECT'],
  notes='Selección posicional de un registro mostrado: referente extensional, fuera de M2a. resulting_ir = ancla sin cambios.')
L('SF066', [], P('SF066'), turn_kind='OTHER_OPERATION', scope='OUT_REFERENT_EXTENSIONAL', disp='CLARIFY',
  reason='REFERENT_UNRESOLVED', echo=(), amb='AMBIGUOUS', conf='MEDIUM', gaps=['SELECT'],
  alts=['HANDOFF_LEGACY si el turno previo mostró un único contrato o hay uno en foco'],
  notes='Pregunta de atributo sobre «ese contrato» con un conjunto de varios resultados: el referente no se identifica.')
L('SF067', [], P('SF067'), **EXT, gaps=['SELECT'], notes='Abrir el tercer registro mostrado.')
ir = add(P('SF068'), 'GEOGRAPHY', 'is', 'Cali', level='MUNICIPALITY')
L('SF068', [O('SET', 'GEOGRAPHY', 'Cali', 'is', vh='Cali', level='MUNICIPALITY', rrs=True)], ir,
  amb='AMBIGUOUS', conf='MEDIUM', gaps=['GEOBUYER', 'SET'],
  alts=['BUYER is Alcaldía de Cali (reemplazando o sumando comprador)', 'filtrar sólo el conjunto mostrado (contrario a F4)'],
  notes='«de esos» apunta al conjunto mostrado; F4: re-consulta con la condición añadida. D7: «de Cali» sin cabeza institucional = GEOGRAPHY (AMBIGUOUS + EXECUTE).')
ir = add(P('SF069'), 'GEOGRAPHY', 'is', 'Cali', level='MUNICIPALITY')
L('SF069', [O('SET', 'GEOGRAPHY', 'Cali', 'is', vh='Cali', level='MUNICIPALITY', rrs=True)], ir,
  amb='AMBIGUOUS', conf='MEDIUM', gaps=['GEOBUYER'],
  alts=['BUYER is Alcaldía de Cali'],
  notes='Igual que SF068 sin «de esos». D7: GEOGRAPHY dominante, AMBIGUOUS + EXECUTE.')
L('SF070', [], P('SF070'), **EXT, gaps=['SELECT'], notes='Atributo (valor) del primer registro mostrado.')
L('SF071', [], P('SF071'), **EXT, conf='MEDIUM', gaps=['SELECT'], notes='Atributo (firmante) del último registro mostrado.')
L('SF072', [], P('SF072'), **EXT, amb='AMBIGUOUS', conf='MEDIUM', gaps=['SELECT'],
  alts=['CARDINALITY top_n 2 sobre el orden vigente (no hay ORDER en el ancla, y CARDINALITY exige ORDER)'],
  notes='Subconjunto posicional del conjunto mostrado: referente extensional.')

# ---------------- SF073-SF084: universe / view ----------------
L('SF073', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES', rrs=True)], uni(P('SF073'), 'PROCESSES'),
  effects=['UNIVERSE_CHANGED'], notes='Cambio de universo; tema, comprador y año se conservan (F1).')
L('SF074', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS')], uni(P('SF074'), 'CONTRACTS'),
  effects=['UNIVERSE_CHANGED'])
L('SF075', [O('SET_UNIVERSE', 'UNIVERSE', 'oportunidades', vh='PROCESSES/OPPORTUNITIES')],
  uni(P('SF075'), 'PROCESSES', 'OPPORTUNITIES'),
  effects=['UNIVERSE_CHANGED'], amb='AMBIGUOUS', conf='MEDIUM', gaps=['UNIVTIME'],
  alts=['CLARIFY: oportunidades vigentes con TIME 2025 (año ya cerrado a 2026-10-01) casi seguro da vacío; ¿quitar el año?'],
  notes='O3/O7: «oportunidades» = PROCESSES + OPPORTUNITIES. F1 conserva TIME 2025 porque no se citó su remoción.')
L('SF076', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS', rrs=True)], uni(P('SF076'), 'CONTRACTS'),
  effects=['UNIVERSE_CHANGED'], notes='De ancla dual a sólo contratos.')
L('SF077', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES', rrs=True)], uni(P('SF077'), 'PROCESSES'),
  effects=['UNIVERSE_CHANGED'], notes='De ancla dual a sólo procesos.')
ir = add(uni(P('SF078'), 'PROCESSES'), 'STATE', 'family', 'open')
L('SF078', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES', rrs=True),
            O('SET', 'STATE', 'abiertos', 'family', vh='open', rrs=True)], ir,
  effects=['UNIVERSE_CHANGED'], notes='C4: «abierto» = STATE family open.')
ir = add(uni(P('SF079'), 'CONTRACTS'), 'STATE', 'family', 'open')
L('SF079', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS', rrs=True),
            O('SET', 'STATE', 'abiertos', 'family', vh='open', rrs=True)], ir,
  effects=['UNIVERSE_CHANGED'], disp='CLARIFY', reason='STATE_NOT_IN_UNIVERSE', echo=(), amb='AMBIGUOUS', conf='LOW',
  alts=['EXECUTE con «abiertos» leído como contratos vigentes / en ejecución (STATE in_execution)',
        'EXECUTE en PROCESSES (el usuario confunde universo)'],
  notes='§19.1: «open» es PROCESS_ONLY (no existe en contratos); C0 §5.6 bloquea con STATE_NOT_IN_VIEW. El estado no se traduce en silencio (§19 principio) -> CLARIFY. NEW_GAP: STATE_NOT_IN_TARGET_UNIVERSE (estado de procesos pedido sobre contratos en un delta, sin ACTOR).')
ir = add(uni(P('SF080'), 'PROCESSES'), 'MODALITY', 'is', 'mínima cuantía')
L('SF080', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES'),
            O('SET', 'MODALITY', 'mínima cuantía', 'is', vh='mínima cuantía')], ir,
  effects=['UNIVERSE_CHANGED'])
L('SF081', [O('SET', 'TIME', '2025', 'during_year', vh='2025', ys='STATED')],
  add(P('SF081'), 'TIME', 'during_year', '2025'),
  notes='F9: la vista CONTRACTS la eligió el usuario por botón (view_source USER, cadena PROTOCOL:option): no se recalcula.')
ir = add(P('SF082'), 'TIME', 'during_year', '2025'); ir['product_view'] = 'HISTORICAL_PROCESSES'
L('SF082', [O('SET', 'TIME', '2025', 'during_year', vh='2025', ys='STATED')], ir,
  effects=['VIEW_REDERIVED'], echo=('CARRIED', 'VIEW_DERIVED'),
  notes='F9: vista derivada (PV6) se recalcula tras el delta: TIME presente -> PV1 HISTORICAL_PROCESSES, anunciado.')
L('SF083', [O('SET', 'TIME', '2025', 'during_year', vh='2025', ys='STATED')],
  add(P('SF083'), 'TIME', 'during_year', '2025'), notes='Universo PROCESSES dicho por el usuario: sin vista que recalcular.')
ir = add(uni(P('SF084'), 'PROCESSES', 'OPPORTUNITIES'), 'STATE', 'family', 'open')
L('SF084', [O('SET_UNIVERSE', 'UNIVERSE', 'convocatorias', vh='PROCESSES/OPPORTUNITIES'),
            O('SET', 'STATE', 'siguen recibiendo ofertas', 'family', vh='open')], ir,
  effects=['UNIVERSE_CHANGED'], conf='MEDIUM',
  notes='«convocatorias» encabezando = O7 (oportunidad). «siguen recibiendo ofertas» está en el léxico PRE_AWARD open (§18) -> STATE family open (A1). Sin ACTOR, sin política ACTOR×STATE.')

# ---------------- SF085-SF108: value ----------------
def V(gid):
    return P(gid)
L('SF085', [O('SET', 'VALUE.lower', 'más de 80 millones', 'greater_than', vh='80 millones', rrs=True, us='STATED')],
  add(V('SF085'), 'VALUE', 'greater_than', '80 millones', unit='MILLONES'))
L('SF086', [O('SET', 'VALUE.upper', 'máximo 300 millones', 'less_or_equal', vh='300 millones', us='STATED')],
  add(V('SF086'), 'VALUE', 'less_or_equal', '300 millones', unit='MILLONES'), notes='D4: «máximo» inclusivo.')
ir = add(add(V('SF087'), 'VALUE', 'greater_or_equal', '50 millones', unit='MILLONES'), 'VALUE', 'less_or_equal', '120 millones', unit='MILLONES')
L('SF087', [O('SET', 'VALUE.lower', 'entre 50 y 120 millones', 'greater_or_equal', vh='50 millones', us='SIBLING_IN_RANGE'),
            O('SET', 'VALUE.upper', 'entre 50 y 120 millones', 'less_or_equal', vh='120 millones', us='STATED')], ir,
  notes='§2.3 rango; F6 unidad dicha una vez cubre las dos cotas.')
L('SF088', [O('SET', 'VALUE.lower', 'mínimo 250 millones', 'greater_or_equal', vh='250 millones', us='STATED')],
  rep(V('SF088'), 'VALUE', value_hint='250 millones'), notes='F2: cota inferior nueva reemplaza la inferior.')
L('SF089', [O('SET', 'VALUE.upper', 'hasta 400 millones', 'less_or_equal', vh='400 millones', us='STATED')],
  add(V('SF089'), 'VALUE', 'less_or_equal', '400 millones', unit='MILLONES'),
  notes='F2: cota del lado que faltaba completa el rango (100-400 millones).')
ir = drop(V('SF090'), 'VALUE'); add(ir, 'VALUE', 'less_or_equal', '200 millones', unit='MILLONES')
L('SF090', [O('SET', 'VALUE.upper', 'no más de 200 millones', 'less_or_equal', vh='200 millones', us='STATED')], ir,
  effects=['VALUE_RANGE_REPLACED'], echo=('CARRIED', 'VALUE_RANGE_REPLACED'),
  notes='>=500 y <=200: rango vacío -> F2 sustituye la restricción entera, anunciado.')
ir = drop(V('SF091'), 'VALUE'); add(ir, 'VALUE', 'less_or_equal', '300 millones', unit='MILLONES')
L('SF091', [O('SET', 'VALUE.upper', 'hasta 300 millones', 'less_or_equal', vh='300 millones', us='STATED')], ir,
  effects=['VALUE_RANGE_REPLACED'], echo=('CARRIED', 'VALUE_RANGE_REPLACED'),
  notes='>=300 y <=300: rango de un solo punto (sin interior) -> F2 sustituye la restricción entera.')
L('SF092', [O('SET', 'VALUE.upper', 'máximo 150 millones', 'less_or_equal', vh='150 millones', us='STATED')],
  rep(V('SF092'), 'VALUE', value_hint='150 millones'), notes='F2: superior nueva reemplaza la superior.')
L('SF093', [O('SET', 'VALUE.lower', 'mayores de 50 millones', 'greater_than', vh='50 millones', us='STATED')],
  add(V('SF093'), 'VALUE', 'greater_than', '50 millones', unit='MILLONES'),
  notes='F2: completa el rango (>50 y <=200 millones).')
ir = drop(V('SF094'), 'VALUE'); add(ir, 'VALUE', 'greater_than', '400 millones', unit='MILLONES')
L('SF094', [O('SET', 'VALUE.lower', 'más de 400 millones', 'greater_than', vh='400 millones', us='STATED')], ir,
  effects=['VALUE_RANGE_REPLACED'], echo=('CARRIED', 'VALUE_RANGE_REPLACED'), notes='>400 y <=200: vacío -> sustitución entera.')
ir = drop(V('SF095'), 'VALUE'); add(ir, 'VALUE', 'greater_or_equal', '200 millones', unit='MILLONES')
L('SF095', [O('SET', 'VALUE.lower', 'desde 200 millones', 'greater_or_equal', vh='200 millones', us='STATED')], ir,
  effects=['VALUE_RANGE_REPLACED'], echo=('CARRIED', 'VALUE_RANGE_REPLACED'),
  notes='D4: «desde» inclusivo. >=200 y <=200: un solo punto -> sustitución entera.')
ir = V('SF096'); ir['conditions'][2]['value_hint'] = '150 millones'
L('SF096', [O('SET', 'VALUE.lower', 'mínimo 150 millones', 'greater_or_equal', vh='150 millones', us='STATED')], ir,
  notes='Rango 150-300 millones.')
ir = V('SF097'); ir['conditions'][3]['value_hint'] = '250 millones'
L('SF097', [O('SET', 'VALUE.upper', 'hasta 250 millones', 'less_or_equal', vh='250 millones', us='STATED')], ir,
  notes='Rango 100-250 millones.')
ir = V('SF098'); ir['conditions'][2]['value_hint'] = '400 millones'; ir['conditions'][3]['value_hint'] = '600 millones'
L('SF098', [O('SET', 'VALUE.lower', 'entre 400 y 600 millones', 'greater_or_equal', vh='400 millones', us='SIBLING_IN_RANGE'),
            O('SET', 'VALUE.upper', 'entre 400 y 600 millones', 'less_or_equal', vh='600 millones', us='STATED')], ir,
  conf='MEDIUM',
  notes='F2 por lado: las dos cotas se reemplazan a la vez y el rango final tiene interior, así que no se marca VALUE_RANGE_REPLACED (F2 lo reserva para rango sin interior o instrucción explícita de cambio). Duda: §2.3 habla de «restricción sustituida entera», lo que podría leerse como que sí lleva el efecto.')
ir = drop(V('SF099'), 'VALUE'); add(ir, 'VALUE', 'greater_than', '350 millones', unit='MILLONES')
L('SF099', [O('SET', 'VALUE.lower', 'más de 350 millones', 'greater_than', vh='350 millones', us='STATED')], ir,
  effects=['VALUE_RANGE_REPLACED'], echo=('CARRIED', 'VALUE_RANGE_REPLACED'), notes='>350 y <=300: vacío -> sustitución entera.')
ir = drop(V('SF100'), 'VALUE'); add(ir, 'VALUE', 'less_than', '90 millones', unit='MILLONES')
L('SF100', [O('SET', 'VALUE.upper', 'menos de 90 millones', 'less_than', vh='90 millones', us='STATED')], ir,
  effects=['VALUE_RANGE_REPLACED'], echo=('CARRIED', 'VALUE_RANGE_REPLACED'),
  notes='F2: instrucción explícita «cambia el valor a…» sustituye la restricción (además <90 con >=100 sería vacío).')
L('SF101', [O('SET', 'VALUE.lower', 'mínimo 350', 'greater_or_equal', vh='350 millones', us='PREVIOUS_ACCEPTED_VALUE')],
  rep(V('SF101'), 'VALUE', value_hint='350 millones'), echo=('CARRIED', 'UNIT_INHERITED'),
  notes='F5: la condición previa modificada tiene una única unidad (MILLONES): se hereda, anunciado.')
ir = V('SF102'); ir['conditions'][2].update(value_hint='400 (unidad sin resolver: MILLONES o MIL_MILLONES)', unit=None)
L('SF102', [O('SET', 'VALUE.lower', 'mínimo 400', 'greater_or_equal', vh='400 (unidad sin resolver)', us=None)], ir,
  disp='CLARIFY', reason='UNIT_AMBIGUOUS', echo=(), amb='AMBIGUOUS', conf='MEDIUM', gaps=['UNIT'],
  alts=['EXECUTE heredando MILLONES de la cota inferior que se modifica (lectura de F5 por lado): 400 millones a 5 mil millones'],
  notes='La restricción VALUE previa tiene dos unidades activas (MILLONES en la inferior, MIL_MILLONES en la superior): F5 -> CLARIFY. Si F5 se leyera por lado, la inferior tiene una sola unidad y heredaría MILLONES.')
ir = V('SF103'); rep(ir, 'VALUE', value_hint='800 SMMLV')
L('SF103', [O('SET', 'VALUE.lower', 'mínimo 800', 'greater_or_equal', vh='800 SMMLV', us='PREVIOUS_ACCEPTED_VALUE')], ir,
  disp='CLARIFY', reason='SMMLV_UNSUPPORTED', echo=(), conf='MEDIUM', gaps=['SMMLV', 'UNIT'],
  notes='F5 hereda SMMLV (única unidad previa); F14: monto en SMMLV no ejecutable -> CLARIFY SMMLV_UNSUPPORTED (default provisional).')
ir = V('SF104'); rep(ir, 'VALUE', value_hint='650 pesos')
L('SF104', [O('SET', 'VALUE.lower', 'mínimo 650', 'greater_or_equal', vh='650 pesos', us='PREVIOUS_ACCEPTED_VALUE')], ir,
  echo=('CARRIED', 'UNIT_INHERITED'), amb='AMBIGUOUS', conf='LOW', gaps=['UNIT'],
  alts=['CLARIFY: heredar PESOS da 650 pesos (escala inverosímil frente a 40.000.000 pesos)', '650 millones (escala implícita)'],
  notes='F5 literal: única unidad previa PESOS -> 650 pesos (rango >=650 pesos, más ancho que el previo). NEW_GAP: UNIT_INHERITANCE_IMPLAUSIBLE_SCALE (F5 no contempla que la unidad heredada vuelva absurdo el monto).')
ir = add(V('SF105'), 'VALUE', 'greater_than', '900 mil pesos', unit='PESOS')
L('SF105', [O('SET', 'VALUE.lower', 'más de 900 mil pesos', 'greater_than', vh='900 mil pesos', us='STATED')], ir,
  conf='MEDIUM',
  notes='F2: completa el rango con la inferior (>900.000 pesos y <=200 millones). Unidades distintas por cota, ambas dichas.')
ir = V('SF106'); rep(ir, 'VALUE', value_hint='450 millones'); add(ir, 'VALUE', 'less_or_equal', '900 millones', unit='MILLONES')
L('SF106', [O('SET', 'VALUE.lower', 'entre 450 y 900', 'greater_or_equal', vh='450 millones', us='PREVIOUS_ACCEPTED_VALUE'),
            O('SET', 'VALUE.upper', 'entre 450 y 900', 'less_or_equal', vh='900 millones', us='PREVIOUS_ACCEPTED_VALUE')], ir,
  echo=('CARRIED', 'UNIT_INHERITED'), conf='MEDIUM',
  notes='F5: única unidad previa MILLONES, heredada por las dos cotas (no hay unidad dicha, F6 no aplica). Inferior reemplazada, superior nueva.')
ir = add(add(V('SF107'), 'VALUE', 'greater_or_equal', '200 millones', unit='MILLONES'), 'VALUE', 'less_or_equal', '500 millones', unit='MILLONES')
L('SF107', [O('SET', 'VALUE.lower', 'entre 200 y 500 millones', 'greater_or_equal', vh='200 millones', us='SIBLING_IN_RANGE'),
            O('SET', 'VALUE.upper', 'entre 200 y 500 millones', 'less_or_equal', vh='500 millones', us='STATED')], ir,
  notes='F6.')
ir = V('SF108'); rep(ir, 'VALUE', value_hint='2 mil millones', unit='MIL_MILLONES')
L('SF108', [O('SET', 'VALUE.lower', 'mínimo 2 mil millones', 'greater_or_equal', vh='2 mil millones', us='STATED')], ir)

# ---------------- SF109-SF116: clears ----------------
L('SF109', [O('CLEAR', 'TIME', 'sin importar el año')], drop(P('SF109'), 'TIME'))
L('SF110', [O('CLEAR', 'GEOGRAPHY', 'no importa el departamento')], drop(P('SF110'), 'GEOGRAPHY'))
L('SF111', [O('CLEAR', 'STATE', 'da igual el estado')], drop(P('SF111'), 'STATE'))
L('SF112', [O('CLEAR', 'BUYER', 'de cualquier entidad')], drop(P('SF112'), 'BUYER'))
L('SF113', [O('CLEAR', 'MODALITY', 'quita el filtro de modalidad')], P('SF113'),
  amb='AMBIGUOUS', conf='LOW',
  alts=['CLARIFY/aviso: no había filtro de modalidad; nada que quitar'],
  notes='Remoción de una ranura vacía: la IR no cambia. El producto debería avisar que no había ese filtro. NEW_GAP: CLEAR_ABSENT_SLOT.')
L('SF114', [O('CLEAR', 'TIME', 'mejor sin año')], drop(P('SF114'), 'TIME'),
  amb='AMBIGUOUS', conf='MEDIUM',
  alts=['CLARIFY: la búsqueda queda sin ninguna condición (todos los contratos de SECOP)'],
  notes='Tras la remoción la IR no tiene condiciones. NEW_GAP: EMPTY_IR_AFTER_CLEAR (no hay regla para un delta que deja la búsqueda sin filtros).')
L('SF115', [O('CLEAR', 'TOPIC', 'olvida el tema')], drop(P('SF115'), 'TOPIC'))
L('SF116', [O('CLEAR', 'BUYER', 'ya no importa el comprador')], drop(P('SF116'), 'BUYER'),
  amb='AMBIGUOUS', conf='MEDIUM',
  alts=['CLARIFY: la búsqueda queda sin ninguna condición'],
  notes='Igual que SF114. NEW_GAP: EMPTY_IR_AFTER_CLEAR.')

# ---------------- SF117-SF122: order ----------------
L('SF117', [O('SET_ORDER', 'ORDER', 'del más barato al más caro', 'value_asc', rrs=True)], order_(P('SF117'), 'value_asc'))
L('SF118', [O('SET_ORDER', 'ORDER', 'los más antiguos primero', 'date_asc', rrs=True)], order_(P('SF118'), 'date_asc'))
L('SF119', [O('SET_ORDER', 'ORDER', 'por fecha, los más recientes primero', 'date_desc', rrs=True)], order_(P('SF119'), 'date_desc'),
  notes='Reemplaza value_desc.')
ir = order_(add(P('SF120'), 'TOPIC', 'contains', 'ambulancia'), 'value_desc')
L('SF120', [O('SET', 'TOPIC', 'ambulancia', 'contains', vh='ambulancia', rrs=True),
            O('SET_ORDER', 'ORDER', 'del más caro al más barato', 'value_desc', rrs=True)], ir,
  notes='Sin TOPIC previo: se añade. F4: re-consulta.')
L('SF121', [O('SET', 'TOPIC', 'ambulancia', 'contains', vh='ambulancia', rrs=True)],
  add(P('SF121'), 'TOPIC', 'contains', 'ambulancia'))
L('SF122', [O('SET_ORDER', 'ORDER', 'de menor a mayor valor', 'value_asc')], order_(P('SF122'), 'value_asc'),
  notes='Reemplaza date_desc.')

# ---------------- SF123-SF134: fragment vs self-contained ----------------
ir = add(P('SF123'), 'GEOGRAPHY', 'is', 'Caribe', level='REGION')
L('SF123', [O('SET', 'GEOGRAPHY', 'región Caribe', 'is', vh='Caribe', level='REGION')], ir,
  amb='AMBIGUOUS', conf='LOW', gaps=['GEOBUYER'],
  alts=['La región sustituye el foco geográfico del comprador: CLEAR BUYER Gobernación de Nariño + GEOGRAPHY Caribe',
        'CLARIFY: ¿conservar la Gobernación de Nariño (Pacífica) o cambiarla por la región Caribe?'],
  notes='F1 literal: se añade GEOGRAPHY y se conserva BUYER (sólo una remoción citada lo quita). Con un comprador de Nariño el resultado es casi vacío; «y para X?» suele contrastar. Región en lista cerrada.')
ir = fresh('CONTRACTS'); add(ir, 'TOPIC', 'contains', 'mantenimiento de ascensores'); add(ir, 'GEOGRAPHY', 'is', 'Caribe', level='REGION')
L('SF124', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'mantenimiento de ascensores', 'contains', vh='mantenimiento de ascensores'),
            O('SET', 'GEOGRAPHY', 'región Caribe', 'is', vh='Caribe', level='REGION')], ir,
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), conf='MEDIUM', gaps=['UNIVERSE_FRAGMENT'],
  notes='Petición autocontenida (sustantivo de universo + tema + lugar): no arrastra comprador ni año.')
L('SF125', [O('SET', 'TIME', '2023', 'during_year', vh='2023', ys='STATED')], rep(P('SF125'), 'TIME', value_hint='2023'))
ir = P('SF126'); rep(ir, 'TOPIC', value_hint='alquiler de maquinaria pesada'); rep(ir, 'TIME', value_hint='2023')
L('SF126', [O('SET', 'TOPIC', 'alquiler de maquinaria pesada', 'contains', vh='alquiler de maquinaria pesada'),
            O('SET', 'TIME', 'en 2023', 'during_year', vh='2023', ys='STATED')], ir,
  effects=['TOPIC_REPLACED'], echo=('CARRIED', 'TOPIC_REPLACED'), amb='AMBIGUOUS', conf='LOW',
  gaps=['UNIVERSE_FRAGMENT', 'TOPIC'],
  alts=['NEW_SEARCH: tema + año sin universo -> UNSPECIFIED, PV1 HISTORICAL_PROCESSES, sin la Alcaldía de Tunja'],
  notes='Sin sustantivo de universo se lee como delta: F3 reemplaza el tema, se reemplaza el año y se conserva el comprador. Criterio no escrito en contrato ni adenda: fragmento sin universo = delta; con universo = nueva búsqueda.')
L('SF127', [O('SET', 'BUYER', 'Gobernación del Cauca', 'is', vh='Gobernación del Cauca')],
  rep(P('SF127'), 'BUYER', value_hint='Gobernación del Cauca'))
ir = fresh('CONTRACTS'); add(ir, 'TOPIC', 'contains', 'transporte escolar'); add(ir, 'BUYER', 'is', 'Gobernación del Cauca')
L('SF128', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'transporte escolar', 'contains', vh='transporte escolar'),
            O('SET', 'BUYER', 'Gobernación del Cauca', 'is', vh='Gobernación del Cauca')], ir,
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), conf='MEDIUM', gaps=['UNIVERSE_FRAGMENT'],
  notes='Petición autocontenida.')
L('SF129', [O('SET', 'VALUE.lower', 'pasen de 50 millones', 'greater_than', vh='50 millones', rrs=True, us='STATED')],
  add(P('SF129'), 'VALUE', 'greater_than', '50 millones', unit='MILLONES'),
  notes='«pasar de» = estrictamente mayor.')
ir = fresh('CONTRACTS'); add(ir, 'TOPIC', 'contains', 'dotación de ambulancias'); add(ir, 'VALUE', 'greater_than', '50 millones', unit='MILLONES')
L('SF130', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'dotación de ambulancias', 'contains', vh='dotación de ambulancias'),
            O('SET', 'VALUE.lower', 'más de 50 millones', 'greater_than', vh='50 millones', us='STATED')], ir,
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), conf='MEDIUM', gaps=['UNIVERSE_FRAGMENT'],
  notes='Petición autocontenida: no arrastra ICBF ni Chocó.')
L('SF131', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES')], uni(P('SF131'), 'PROCESSES'), effects=['UNIVERSE_CHANGED'])
L('SF132', [O('KEEP', 'UNIVERSE', 'licitaciones', vh='PROCESSES')], P('SF132'),
  amb='AMBIGUOUS', conf='LOW', gaps=['UNIVERSE_FRAGMENT'],
  alts=['MODALITY is licitación pública (contra O6/DA-2bis)', 'PROCESSES/OPPORTUNITIES (licitaciones como oportunidades)'],
  notes='O6/D3: «licitaciones» coloquial no es modalidad; nombra procesos, el mismo universo del ancla -> KEEP, la IR no cambia. El usuario probablemente espera algo distinto de lo ya mostrado.')
ir = rep(P('SF133'), 'TOPIC', value_hint='vigilancia')
L('SF133', [O('SET', 'TOPIC', 'vigilancia', 'contains', vh='vigilancia')], ir,
  effects=['TOPIC_REPLACED'], echo=('CARRIED', 'TOPIC_REPLACED'), notes='F3: tema desnudo como fragmento reemplaza el tema.')
ir = fresh('CONTRACTS'); add(ir, 'TOPIC', 'contains', 'vigilancia')
L('SF134', [O('SET_UNIVERSE', 'UNIVERSE', 'contratos', vh='CONTRACTS'),
            O('SET', 'TOPIC', 'vigilancia', 'contains', vh='vigilancia')], ir,
  turn_kind='NEW_SEARCH', disp='NEW_SEARCH', reason='SELF_CONTAINED_REQUEST', echo=(), amb='AMBIGUOUS', conf='LOW',
  gaps=['UNIVERSE_FRAGMENT'],
  alts=['DELTA: TOPIC_REPLACED conservando la Alcaldía de Zipaquirá (el sustantivo «contratos» repite el universo vigente)'],
  notes='Con sustantivo de universo es una petición autocontenida (todos los contratos de vigilancia). Criterio no escrito: ver SF126.')

# ---------------- SF135-SF144: anchor states / TTL / owners ----------------
L('SF135', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], rep(P('SF135'), 'TIME', value_hint='2024'),
  conf='MEDIUM', notes='Ancla NOT_EXECUTED pero con IR legible de Foundation SEARCH: el delta se aplica y se ejecuta.')
L('SF136', [O('SET', 'VALUE.lower', 'superen los 100 millones', 'greater_than', vh='100 millones', rrs=True, us='STATED')],
  add(P('SF136'), 'VALUE', 'greater_than', '100 millones', unit='MILLONES'), conf='MEDIUM',
  notes='«los que» apunta a un conjunto que nunca se mostró (ancla NOT_EXECUTED); F4 re-consulta igual.')
ir = P('SF137'); add(ir, 'TIME', 'during_year', '2024'); ir['product_view'] = 'HISTORICAL_PROCESSES'
L('SF137', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], ir,
  scope='OUT_ANCHOR_NOT_SEARCH', disp='CLARIFY', reason='NO_ANCHOR', effects=['VIEW_REDERIVED'], echo=(),
  amb='AMBIGUOUS', conf='LOW',
  alts=['HANDOFF_LEGACY (regla §2.2.7 literal: owner ≠ FOUNDATION_SEARCH)', 'EXECUTE como búsqueda nueva UNSPECIFIED + 2024 -> PV1'],
  notes='No hay turno previo (owner NONE, state NONE): el fragmento no tiene ancla; se pregunta qué buscar. La regla 2.2.7 manda HANDOFF_LEGACY para owner ≠ FOUNDATION_SEARCH, pero NONE no es legacy. NEW_GAP: NO_ANCHOR_FRAGMENT.')
L('SF138', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], rep(P('SF138'), 'TIME', value_hint='2024'),
  gaps=['TTL'], **EXP, notes='F11: 1860 s > 30 min.')
ir = rep(P('SF139'), 'GEOGRAPHY', value_hint='Palmira')
L('SF139', [O('SET', 'GEOGRAPHY', 'Palmira', 'is', vh='Palmira', level='MUNICIPALITY', rrs=True)], ir,
  gaps=['TTL'], **EXP, notes='F11: 5 h. Continuar: Palmira reemplaza a Cali (D7: GEOGRAPHY).')
L('SF140', [O('SET_ORDER', 'ORDER', 'por fecha', 'date_desc', rrs=True)], order_(P('SF140'), 'date_desc'),
  gaps=['TTL'], **EXP, conf='MEDIUM', amb='AMBIGUOUS',
  alts=['date_asc (más antiguo primero)', 'al continuar, preguntar la dirección del orden'],
  notes='F11: 13 h. «por fecha» sin dirección: se toma date_desc (más reciente primero) como lectura por defecto. NEW_GAP: ORDER_DIRECTION_UNSTATED.')
L('SF141', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], rep(P('SF141'), 'TIME', value_hint='2024'),
  notes='F11: 1740 s < 30 min -> continuación.')
L('SF142', [O('SET', 'GEOGRAPHY', 'Bogotá', 'is', vh='Bogotá', level='MUNICIPALITY')], None,
  disp='CLARIFY', reason='ANCHOR_UNREADABLE', echo=(), conf='MEDIUM',
  notes='prior.ir es null (ancla ilegible): no se puede componer la IR resultante. NEW_GAP: ANCHOR_UNREADABLE (sin regla de disposición para ancla Foundation sin IR legible).')
L('SF143', [O('SET', 'TIME', '2024', 'during_year', vh='2024', ys='STATED')], rep(P('SF143'), 'TIME', value_hint='2024'),
  **HAND_LEG, notes='§2.2.7: ancla LEGACY:COUNT.')
L('SF144', [O('SET', 'TIME', 'de 2024', 'during_year', vh='2024', rrs=True, ys='STATED')],
  add(P('SF144'), 'TIME', 'during_year', '2024'), **HAND_LEG, notes='§2.2.7: ancla LEGACY:RANK_SUPPLIERS.')

# ---------------- SF145-SF154: protocol / buttons / operation change ----------------
L('SF145', [], P('SF145'), **PROT, notes='Paginación (capa 1, PROTOCOL): la IR no cambia.')
L('SF146', [], P('SF146'), **PROT, notes='Paginación tras otra paginación.')
L('SF147', [], P('SF147'), **PROT, conf='MEDIUM',
  notes='«otros diez más» = página siguiente sobre el orden vigente; no es CARDINALITY.')
L('SF148', [], P('SF148'), **PROT, conf='MEDIUM', notes='Paginación sobre ancla dual (procesos y contratos).')
L('SF149', [O('SET_ORDER', 'ORDER', 'Mayor valor', 'value_desc')], order_(P('SF149'), 'value_desc'),
  conf='MEDIUM', notes='Etiqueta del botón [Mayor valor] (P7, M2): orden por valor descendente, sin cardinalidad.')
L('SF150', [O('SET_ORDER', 'ORDER', 'Más recientes', 'date_desc')], order_(P('SF150'), 'date_desc'), conf='MEDIUM')
L('SF151', [O('CLEAR', 'BUYER', 'Quitar: Alcaldía de Soledad')], drop(P('SF151'), 'BUYER'),
  notes='Etiqueta de chip de remoción: remoción citada.')
L('SF152', [O('CLEAR', 'TIME', 'Quitar filtro de 2025')], drop(P('SF152'), 'TIME'))
ir = P('SF153'); ir['operation'] = 'FOLLOW'; ir['view_source'] = None
L('SF153', [], ir, turn_kind='OTHER_OPERATION', scope='OUT_OPERATION_CHANGE', disp='HANDOFF_LEGACY',
  reason='OPERATION_HANDOFF:FOLLOW', echo=(), gaps=['OPSWITCH'],
  notes='FOLLOW (Radar) sobre la búsqueda vigente: fuera de M2a.')
ir = P('SF154'); ir['operation'] = 'FOLLOW'; ir['universe'] = 'PROCESSES'; ir['view_source'] = None
L('SF154', [O('SET_UNIVERSE', 'UNIVERSE', 'procesos', vh='PROCESSES')], ir,
  turn_kind='OTHER_OPERATION', scope='OUT_OPERATION_CHANGE', disp='HANDOFF_LEGACY',
  reason='OPERATION_HANDOFF:FOLLOW', echo=(), amb='AMBIGUOUS', conf='MEDIUM', gaps=['OPSWITCH', 'UNIVTIME'],
  alts=['La alerta de procesos nuevos no debería arrastrar TIME 2025 (los nuevos serán de 2026)'],
  notes='Alerta sobre procesos nuevos «así» (mismas condiciones), universo cambiado a procesos. F1 conserva TIME 2025, que choca con «nuevos».')

# ---------------- SF155-SF162: compound deltas ----------------
ir = add(add(P('SF155'), 'GEOGRAPHY', 'is', 'Cali', level='MUNICIPALITY'), 'TIME', 'during_year', '2024')
L('SF155', [O('SET', 'GEOGRAPHY', 'Cali', 'is', vh='Cali', level='MUNICIPALITY'),
            O('SET', 'TIME', 'solo 2024', 'during_year', vh='2024', ys='STATED')], ir,
  amb='AMBIGUOUS', conf='LOW', gaps=['GEOBUYER'],
  alts=['Cali sustituye a la Alcaldía de Bello como comprador (BUYER Alcaldía de Cali)', 'CLEAR BUYER + GEOGRAPHY Cali'],
  notes='F1 literal: «en Cali» (A4: en + lugar = GEOGRAPHY) se añade y se conserva la Alcaldía de Bello; el resultado sería contratos de Bello ejecutados en Cali (casi vacío).')
ir = drop(rep(P('SF156'), 'BUYER', value_hint='Gobernación de Risaralda'), 'TIME')
L('SF156', [O('SET', 'BUYER', 'Gobernación de Risaralda', 'is', vh='Gobernación de Risaralda'),
            O('CLEAR', 'TIME', 'sin importar el año')], ir)
ir = add(add(P('SF157'), 'VALUE', 'greater_than', '100 millones', unit='MILLONES'), 'TIME', 'during_year', '2023')
L('SF157', [O('SET', 'VALUE.lower', 'más de 100 millones', 'greater_than', vh='100 millones', us='STATED'),
            O('SET', 'TIME', 'de 2023', 'during_year', vh='2023', ys='STATED')], ir)
ir = add(drop(P('SF158'), 'GEOGRAPHY'), 'BUYER', 'is', 'Universidad de Antioquia')
L('SF158', [O('CLEAR', 'GEOGRAPHY', 'olvida el lugar'),
            O('SET', 'BUYER', 'Universidad de Antioquia', 'is', vh='Universidad de Antioquia', rrs=True)], ir)
ir = add(add(P('SF159'), 'STATE', 'is', 'awarded'), 'GEOGRAPHY', 'is', 'Magdalena Medio', level='REGION')
L('SF159', [O('SET', 'STATE', 'adjudicados', 'is', vh='awarded', rrs=True),
            O('SET', 'GEOGRAPHY', 'Magdalena Medio', 'is', vh='Magdalena Medio', level='REGION')], ir,
  disp='CLARIFY', reason='REGION_NOT_IN_LIST', echo=(), gaps=['REGION_VOCAB'],
  notes='F13: «Magdalena Medio» no está en la lista cerrada (es sub-departamental; colombia_geography.py lo excluye expresamente) -> CLARIFY. El STATE adjudicado sobre PROCESSES citado sin ACTOR es ejecutable.')
ir = order_(add(P('SF160'), 'MODALITY', 'is', 'mínima cuantía'), 'date_desc')
L('SF160', [O('SET', 'MODALITY', 'mínima cuantía', 'is', vh='mínima cuantía'),
            O('SET_ORDER', 'ORDER', 'los más recientes', 'date_desc', rrs=True)], ir)
ir = order_(card(add(P('SF161'), 'TIME', 'relative_period', 'este año (2026)'), 5), 'value_desc')
L('SF161', [O('SET_ORDER', 'ORDER', 'más caros', 'value_desc', rrs=True),
            O('SET', 'CARDINALITY', '5', 'top_n', vh='5', rrs=True),
            O('SET', 'TIME', 'de este año', 'relative_period', vh='este año (2026)', ys='CLOCK_INFERRED')], ir)
ir = add(add(P('SF162'), 'GEOGRAPHY', 'is', 'Soledad', 'NEGATIVE', 'MUNICIPALITY'), 'TIME', 'during_year', '2024')
L('SF162', [O('SET', 'GEOGRAPHY', 'Soledad', 'is', 'NEGATIVE', 'Soledad', 'MUNICIPALITY', rrs=True),
            O('SET', 'TIME', 'en 2024', 'during_year', vh='2024', ys='STATED')], ir,
  amb='AMBIGUOUS', conf='MEDIUM', gaps=['GEOBUYER'],
  alts=['BUYER NEGATIVE Alcaldía de Soledad'],
  notes='La exclusión de un municipio dentro del departamento positivo (Atlántico) se suma como átomo NEGATIVE; no reemplaza al positivo (sólo se reemplaza el positivo cuando se niega el mismo valor, SF011). D7: GEOGRAPHY dominante.')

# ---------------- emit ----------------
missing = [g for g in order if g not in LAB]
extra = [g for g in LAB if g not in cases]
assert not missing and not extra, (missing, extra)
with open(OUT, 'w', encoding='utf-8') as fh:
    for gid in sorted(order):
        rec = copy.deepcopy(cases[gid])
        rec.update(LAB[gid])
        fh.write(json.dumps(rec, ensure_ascii=False) + '\n')
print('wrote', len(order), OUT)
