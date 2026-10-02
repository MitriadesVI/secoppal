import json, collections
R=[json.loads(l) for l in open('labels_v2_1.jsonl',encoding='utf-8')]
old={json.loads(l)['gold_id']:json.loads(l) for l in open('../etiquetas_SB/labels_v2.jsonl',encoding='utf-8')}
CH=json.load(open('change_rules.json',encoding='utf-8'))
sc=json.load(open('self_check.json',encoding='utf-8'))
C=collections.Counter
def tbl(t,c):
    s=f"\n### {t}\n\n| valor | n |\n|---|---|\n"
    for k,v in sorted(c.items(),key=lambda x:(-x[1],str(x[0]))): s+=f"| {k} | {v} |\n"
    return s
def sig(ir, vh=False):
    if ir is None: return None
    u={'DUAL':'BOTH'}.get(ir['universe'],ir['universe'])
    conds=sorted((c['dimension'],c['operator'],c['polarity'],c['level'],c['unit'])+((c['value_hint'],) if vh else ()) for c in ir['conditions'])
    o=ir['order']['operator'] if ir['order'] else None
    k=ir['cardinality']; k=(k.get('n') or int(k.get('value_hint'))) if k else None
    return (ir['operation'],u,ir['view'],ir['product_view'],tuple(conds),o,k)
rows=[]
for r in R:
    g=r['gold_id']; a=old.get(g)
    if not a: continue
    dch=(a['expected_disposition'],a['disposition_reason'])!=(r['expected_disposition'],r['disposition_reason'])
    ich=sig(a['resulting_ir'])!=sig(r['resulting_ir']); vch=(not ich) and sig(a['resulting_ir'],True)!=sig(r['resulting_ir'],True)
    if not (dch or ich or vch): continue
    if g in CH: rule=CH[g]
    elif a['disposition_reason']=='SELF_CONTAINED_REQUEST': rule='sólo código: §5.3 (NEW_SEARCH lleva razón null)'+('; value_hint en formato §3' if vch else '')
    elif a['disposition_reason']=='ANCHOR_OWNER_LEGACY': rule='sólo código: §5.3 ANCHOR_NOT_SEARCH:<op>'
    elif vch: rule='sólo formato de value_hint (§3: fechas absolutas, AAAA-MM-DD..AAAA-MM-DD)'
    else: rule='?'
    kind='disposición' if dch and a['expected_disposition']!=r['expected_disposition'] else ('resulting_ir' if ich else ('razón' if dch else 'formato'))
    rows.append((g,kind,f"{a['expected_disposition']}/{a['disposition_reason']}",f"{r['expected_disposition']}/{r['disposition_reason']}",rule))
other=[(g,CH[g]) for g in sorted(CH) if g not in {x[0] for x in rows}]
GROUPS={
 'A1 — rango sin interior tras la cota por lado (RANGE_NO_INTERIOR)':['SF090','SF091','SF094','SF095','SF099','SF103','SF182','SF183'],
 'A5 — referente temporal relativo: al valor previo o al reloj / granularidad compatible (RELATIVE_ANCHOR, X18)':['SF038','SF173','SF175','SF177'],
 'A4 — frontera de autosuficiencia: fragmento vs consulta nueva (UNIVERSE_FRAGMENT)':['SF126','SF134','SF185','SF187'],
 'A4 + A7 — «no, mejor X» con X que se interpreta sola (PENDING_REJECTED + UNIVERSE_FRAGMENT)':['SF063'],
 'A2 — SMMLV sin ningún TIME (SMMLV)':['SF169'],
 'X9 — qué cuenta como «sin condición» en F19 (EMPTY_AFTER_DELTA)':['SF114','SF116'],
 'X12 + contrato D7 — territorio contenido en el del comprador / «de <lugar>» comprador o lugar (GEOBUYER)':['SF068','SF069'],
 'X7 — negar un valor contenido en el positivo vigente (POLARITY_OTHER_VALUE + GEOBUYER)':['SF162'],
 'Forma de la cota negada (NEGATED_COMPARATOR, vivo)':['SF013'],
 'Estado redundante con la lente de oportunidad (OPPORTUNITY_STATE, vivo)':['SF084'],
 'Paginación vs cardinalidad (PAGE_VS_TOPN, vivo)':['SF147'],
 'Sustantivo de universo que repite el vigente («licitaciones» sobre procesos): sin gap_code en §8':['SF132'],
}
amb=sorted(r['gold_id'] for r in R if r['ambiguity']=='AMBIGUOUS')
assert sorted(i for v in GROUPS.values() for i in v)==amb, set(amb)^set(i for v in GROUPS.values() for i in v)
out=f"""# Etiquetador SB (ciego) — slice sintético, ronda `m2a-followup-gold/2.1`

191 registros en `labels_v2_1.jsonl` (ordenados por `gold_id`, `labeler = "SB"`, los campos de entrada byte-idénticos).
Normas: kit_v2_2 (ADJUDICATED_OVERRIDES A1–A7 > SCHEMA_v2_1 > CONTRACT_ADDENDUM_FOLLOWUPS_v2), contrato M1 v1.4–v1.9.
Hoy = 2026-10-01. Validador: `self_check_v2_1.py` -> `self_check.json`: **{sc['error_count']} errores, {sc['warning_count']} avisos**.
Generador: `build_labels_v2_1.py` (etiqueta escrita a mano por caso); motivo por cambio: `change_rules.json`.

`effects` y `expected_echo` **no** se etiquetaron a mano: los calcula `build_labels_v2_1.py` mecánicamente con §6/§7
(van en el registro porque el formato los lista; el validador oficial los recalcula).

## Distribuciones
{tbl('turn_kind',C(r['turn_kind'] for r in R))}{tbl('m2a_scope',C(r['m2a_scope'] for r in R))}{tbl('expected_disposition',C(r['expected_disposition'] for r in R))}{tbl('expected_disposition / disposition_reason',C(f"{r['expected_disposition']} / {r['disposition_reason']}" for r in R))}{tbl('ops por ranura',C(str(o['slot']) for r in R for o in r['ops']))}{tbl('ops por op · ranura',C(f"{o['op']} · {o['slot']}" for r in R for o in r['ops']))}
Ops: {sum(len(r['ops']) for r in R)} en total; `unresolved` {sum(o['unresolved'] for r in R for o in r['ops'])}; `refers_to_result_set` {sum(o['refers_to_result_set'] for r in R for o in r['ops'])}; `depends_on_prior_value` {sum(o['depends_on_prior_value'] for r in R for o in r['ops'])}.
{tbl('effects (derivado)',C(e for r in R for e in r['effects']) + C({'(vacío)':sum(1 for r in R if not r['effects'])}))}{tbl('expected_echo (derivado)',C(e for r in R for e in r['expected_echo']) + C({'(vacío)':sum(1 for r in R if not r['expected_echo'])}))}{tbl('ambiguity',C(r['ambiguity'] for r in R))}{tbl('confidence',C(r['confidence'] for r in R))}{tbl('gap_codes',C(g for r in R for g in r['gap_codes']))}
## Cambios respecto de mi ronda anterior (162 comunes)

Disposición, razón o `resulting_ir` (comparada por significado: universo, vista, condiciones, orden, n). {len(rows)} registros.

| id | tipo | antes | ahora | regla que lo causa |
|---|---|---|---|---|
"""
for g,k,a,b,rule in rows: out+=f"| {g} | {k} | {a} | {b} | {rule} |\n"
out+=f"\nOtros cambios (sólo alcance, ambigüedad u ops, misma disposición e IR): {len(other)}.\n\n| id | cambio y regla |\n|---|---|\n"
for g,t in other: out+=f"| {g} | {t} |\n"
out+=f"\nCasos nuevos SF163–SF191: 29, etiquetados desde cero.\n\n## AMBIGUOUS ({len(amb)}), agrupados por la regla que falta\n"
for k,v in GROUPS.items(): out+=f"\n- **{k}**: {', '.join(v)}"
out+="""

## Dónde no alcanzaron el esquema o las reglas

1. **INFORM sin conjunto de resultados (A6).** «muéstrame más resultados» sobre un previo NOT_EXECUTED (SF179) debe ser `INFORM`,
   pero §5.3 no tiene un código para «no hay resultados que paginar». Usé `REFERENT_EXTENSIONAL`, el más cercano. Falta un código propio.
2. **Paginar un ancla dual (§5.2 #3).** El esquema manda `CLARIFY` (SF148), pero §5.3 no tiene código. Usé `UNRESOLVED_VALUE:UNIVERSE`
   (UNIVERSE no es una dimensión del contrato). Falta un código propio.
3. **O12 frente a O13.** «No, mejor X» con X autosuficiente (SF063) es `NEW_SEARCH`: O13 pide `ops = []` y O12 pide `CANCEL_PENDING`.
   Conservé el `CANCEL_PENDING`; el validador lo admite.
4. **SMMLV con TIME plurianual arrastrado (SF168).** §5.2 pide `unresolved = true` en la operación que bloquea, pero aquí no la causa
   ninguna operación del turno (la duda viene del previo). Todas quedan en `unresolved = false`.
5. **A1 y la lectura literal por lado.** En los ocho casos RANGE_NO_INTERIOR, la lectura «conservar las dos cotas» deja un rango vacío
   que §9-F2 prohíbe. Por eso la alternativa es `CLARIFY`, sin IR, y va en `notes`, no en `alternatives`. El derivado marca
   `VALUE_RANGE_REPLACED` porque una cota desaparece sin operación en su lado, aunque A1 retiró el inciso (a): si la mejor lectura es
   la sustitución, hace falta ese anuncio.
6. **`alternatives` sólo admite IR.** Las alternativas de disposición con la misma IR (CLARIFY o REASK frente a EXECUTE) no tienen forma
   propia; van en `notes`.
7. **`UNIVERSE_CHANGED` y la vista.** El efecto se define por `universe`; pasar de PROCESSES/RECORDS a PROCESSES/OPPORTUNITIES
   (SF084) no lo dispara, aunque cambia lo recuperado.
8. **Recálculo SMMLV al cambiar de año (A2, SF166).** A2 exige un «eco de la conversión», pero §7 no tiene esa clase.
9. **Unidad MILES.** «900 mil pesos» pasa a `MILES` por la lista de §3 (SF105); antes era `PESOS`.
10. **Tensiones declaradas sin resolver:** X7 (SF162), X9 (SF114, SF116), X12 (SF068, SF069), X15 (SF064: gana F11 por §5.3), X16 (SF087, SF098, SF107: el eco derivado
    lleva UNIT_SHARED_IN_RANGE), X18 (SF038, SF173). X8, X13 y X17 no aparecen en este slice.
11. **F29 frente a cambio de operación (SF062).** La precedencia §5.2 (#4 antes de IN) da `INFORM / OPERATION_CHANGE:SUM_VALUE`
    aunque la pregunta BUYER siga abierta. Ninguna regla dice si el comprador ambiguo se arrastra al nuevo dueño.
"""
open('REPORT.md','w',encoding='utf-8').write(out); print(out)
