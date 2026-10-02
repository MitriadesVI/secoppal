import json, sys, hashlib, pathlib, collections
P, G = sys.argv[1], sys.argv[2]
L = lambda p: {r["gold_id"]: r for r in map(json.loads, open(p).read().splitlines()) if r}
SL = {"REAL": (f"{P}/labeler_A_v2_3/labels_v2_3.jsonl", f"{P}/labeler_B_v2_3/labels_v2_3.jsonl", f"{G}/comparacion_final/real_v2_3/summary.json"),
      "SYNTHETIC": (f"{G}/sintetico/etiquetas_SA_v2_3/labels_v2_3.jsonl", f"{G}/sintetico/etiquetas_SB_v2_3/labels_v2_3.jsonl", f"{G}/comparacion_final/sintetico_v2_3/summary.json")}
LEDGER = [
 ("G-FU-01", "contradictory_value_range", "Cota VALUE nueva que choca con la opuesta: si conservarla da intervalo vacío, se sustituye la restricción completa y se anuncia (A8).", "M2a", "REPLACE_VALUE_CONSTRAINT + anuncio", "M2a", "NO (adjudicado A8)", "Gold M2a sellado con los casos adjudicados", {"adj": "A8"}),
 ("G-FU-01b", "single_point_value_range", "Cotas iguales y ambas inclusivas: A8 literal conserva la opuesta y queda «exactamente X». Los etiquetadores leían sustitución antes de la regla.", "M2a", "REPLACE_VALUE_CONSTRAINT + anuncio (A8.1); singleton pedido explícitamente sigue válido", "M2a", "NO (adjudicado A8.1)", "Gold M2a sellado", {"adj": "A8.1"}),
 ("G-FU-02", "new_search_vs_delta", "Frontera búsqueda nueva / delta por prueba estructural de autosuficiencia (A9).", "M2a", "prueba estructural; la marca de continuación gana", "M2a", "NO (adjudicado A9)", "Gold M2a sellado", {"adj": "A9"}),
 ("G-FU-02b", "lone_condition_passes_selfsufficiency", "Una condición suelta que pasa la prueba A9 se vuelve búsqueda nueva y pierde el contexto; «no, mejor X»; fragmento con sustantivo de universo.", "M2a", "DELTA si sólo aporta condiciones o modificadores sin TOPIC, ACTOR ni operación nueva (A9.1)", "M2a", "NO (adjudicado A9.1)", "Gold M2a sellado", {"adj": "A9.1"}),
 ("G-FU-03", "clarification_with_alternatives", "Pregunta con dos lecturas (comprador / lugar; cambiar de comprador / ambos): PendingClarification{ambiguous_span, alternatives:[CandidatePatch]} en vez de «doble ranura».", "M2a (forma del tipo)", "CLARIFY con las alternativas", "M2a diseño", "NO (decidido: candidate_patches[])", "diseño v2.2 y esquema del Gold usan PendingClarification.candidate_patches[]", {"gaps": ["COUNTERPART"], "reasons": ["BUYER_OR_PLACE_AFTER_ACTOR"]}),
 ("G-FU-04", "no_result_set_after_source_failure", "Tras fallo de fuente existe semantic_ref pero no result_set_ref: «ver más», fila o exportar no tienen a qué referirse.", "protocolo", "INFORM (delta semántico sí se ejecuta)", "implementación del protocolo M2a", "NO", "código de razón propio al implementar paginación sobre Foundation", {"gaps": ["PRIOR_NOT_EXECUTED"], "ids": ["SF179"]}),
 ("G-FU-05", "paging_dual_anchor_and_page_vs_topn", "Paginar un ancla dual (procesos y contratos); «otros diez más»: página o cardinalidad.", "protocolo", "CLARIFY", "implementación del protocolo / M2b", "NO", "regla y código de razón al implementar paginación", {"gaps": ["PAGE_VS_TOPN"], "reasons": ["UNRESOLVED_VALUE:UNIVERSE"]}),
 ("G-FU-06", "smmlv_conversion", "Eco y procedencia de la conversión SMMLV; SMMLV sin año o con varios años; montos no comparables (millones frente a SMMLV).", "E-VALUE / presentación", "CLARIFY si no hay año único; M2a conserva unit, año, cantidad fuente y COP convertido", "E-VALUE", "NO", "autoridad de conversión por vigencia con catálogo y Gold de E-VALUE en verde", {"gaps": ["SMMLV", "RANGE_NO_INTERIOR"], "reasons": ["SMMLV_YEAR_AMBIGUOUS"]}),
 ("G-FU-07", "territory_inside_buyer", "Territorio nuevo frente a un comprador arrastrado (contenido, contenedor u otro): composición BUYER + GEO.", "composición de autoridades", "CLARIFY", "E-GEO / E-BUYER", "NO", "Gold de composición de autoridades en verde", {"gaps": ["GEOBUYER"], "reasons": ["TERRITORY_VS_CARRIED_BUYER"]}),
 ("G-FU-08", "relative_temporal_referent", "«El año anterior», «el mes siguiente»: referente temporal único y transformable se resuelve; ventana relativa o varios referentes aclaran.", "E-TIME", "resolver con referente único; si no, CLARIFY", "E-TIME", "NO", "aritmética de periodo sobre TimeValue previo, con Gold de E-TIME", {"gaps": ["RELATIVE_ANCHOR"], "ids": ["SF177"]}),
 ("G-FU-09", "negation_over_sets", "Negar un valor cuando la ranura tiene varios positivos o un valor contenido; exclusión sobre conjuntos.", "M2b", "fail-closed: CLARIFY", "M2b", "NO", "álgebra de conjuntos por ranura en Conversation IR", {"gaps": ["POLARITY_SAME_VALUE", "POLARITY_OTHER_VALUE"]}),
 ("G-FU-10", "observed_executor", "Falta en el esquema de observación un valor para «búsqueda atendida por legacy».", "esquema de observación", "observed_executor = LEGACY_SEARCH", "esquema del Gold", "NO", "añadir el valor al enum de lo observado", {}),
 ("G-FU-11", "followup_after_non_search_anchor", "Follow-up tras PROFILE_ACTOR / COUNT / RANK_SUPPLIERS: qué operación resulta, palabra sola tras conteo o ranking, selección de candidato.", "M3", "HANDOFF_LEGACY", "M3 (Gold de M3)", "NO", "las capacidades producen y consumen la misma Conversation IR", {"scopes": ["OUT_ANCHOR_NOT_SEARCH"], "gaps": ["OPSWITCH", "ACTORTIME", "TOTALES", "FIRMADOS", "SELECT"]}),
 ("G-FU-12", "context_expired_with_pending", "Contexto vencido con una pregunta pendiente: qué se ofrece primero.", "M2b", "CLARIFY / CONTEXT_EXPIRED_CONFIRM (gana F11)", "M2b", "NO", "regla de precedencia F11 frente a F29", {"gaps": ["TTL"]}),
 ("G-FU-13", "empty_search_after_delta", "Delta que deja la búsqueda sin condiciones; qué cuenta como «condición».", "M2b", "CLARIFY", "M2b", "NO", "definición de búsqueda mínima admisible", {"gaps": ["EMPTY_AFTER_DELTA"]}),
 ("G-FU-14", "extensional_referents", "Referentes a filas o subconjuntos de lo mostrado («el segundo», «ese contrato»); re-consulta frente a lo mostrado.", "M2b", "INFORM", "M2b", "NO", "result_set_ref con identidad de fila", {"scopes": ["OUT_REFERENT_EXTENSIONAL"], "gaps": ["SET"]}),
 ("G-FU-15", "relative_non_temporal", "Relativos a un valor previo no temporales («bájalo», «el doble», «pero no tanto»).", "M2b", "CLARIFY", "M2b", "NO", "operadores relativos sobre ExecutableValue", {"scopes": ["OUT_RELATIVE_TO_PREVIOUS"]}),
 ("G-FU-16", "operation_change_on_search_anchor", "Follow-up que pide otra operación (seguir, exportar, sumar) sobre una búsqueda Foundation; con o sin pendiente.", "M3", "INFORM", "M3", "NO", "manifiestos de capacidad sobre Accepted IR", {"scopes": ["OUT_OPERATION_CHANGE"], "gaps": ["PENDING_OPSWITCH"]}),
 ("G-FU-17", "no_or_unreadable_anchor", "Fragmento sin búsqueda previa o con ancla ilegible.", "M2a (fail-closed)", "CLARIFY: pedir la búsqueda completa", "M2a protocolo", "NO", "casos como regresión de fail-closed", {"scopes": ["OUT_NO_ANCHOR"], "gaps": ["NO_ANCHOR"]}),
 ("G-FU-18", "value_surface_forms", "Formas del contrato M1 sin regla: comparador desordenado o negado, cota sin comparador en rango, unidad compartida. («en adelante» ya está decidido: GTE en money_comparators.py:99.)", "contrato M1 / E-VALUE", "mejor lectura marcada AMBIGUOUS", "contrato M1 + E-VALUE", "NO", "enmienda del contrato Gold M1", {"gaps": ["COMPARATOR_FORM", "B5RANGE", "NEGATED_COMPARATOR", "UNIT", "VCOMP"]}),
 ("G-FU-19", "view_and_universe_interplay", "Vista derivada, tiempo arrastrado al cambiar de universo, estado frente a lente de oportunidad.", "Product View / F", "recalcular sólo si era derivada; anunciar", "ola F (Product View)", "NO", "tabla PV adjudicada para follow-ups", {"gaps": ["PV", "UNIVTIME", "OPPORTUNITY_STATE"]}),
 ("G-FU-20", "undo", "«Volver atrás»: deshacer el último paso / volver a la búsqueda inicial, como protocolo.", "idea", "no existe", "sin decidir", "NO", "decisión de producto", {}),
 ("G-FU-21", "prior_tie_break", "Un mismo texto con varios turnos previos distintos en las trazas (baterías de replay).", "método del Gold", "previo dominante o primero cronológico, declarado", "método del Gold", "NO", "regla de desempate en selection_rule", {"ids": ["R0083", "R0304"]}),
]
def nconds(ir): return len((ir or {}).get("conditions") or [])
ledger_examples = collections.defaultdict(list); out = {}
for name, (fa, fb, sm) in SL.items():
    A, B = L(fa), L(fb); dis = set(json.load(open(sm))["primary_disagreement_ids"])
    gold, front = [], []
    for i in sorted(A):
        a, b = A[i], B[i]
        sc = a["m2a_scope"] if a["m2a_scope"] == b["m2a_scope"] else f'{a["m2a_scope"]}|{b["m2a_scope"]}'
        amb = sorted(x for r, x in ((a, "A"), (b, "B")) if r.get("ambiguity") == "AMBIGUOUS")
        gaps = set(a.get("gap_codes") or []) | set(b.get("gap_codes") or [])
        reasons = {str(r.get("disposition_reason") or "") for r in (a, b)}
        lone = any(r.get("turn_kind") == "NEW_SEARCH" and nconds(r.get("resulting_ir")) <= 1 for r in (a, b))
        def _adj(r):
            t = str(r.get("notes", "")) + str(r.get("adjudicated_by", ""))
            return {x for x in ("A8.1", "A9.1", "A8", "A9") if "adjudicated_by" in t and f'"{x}"' in t}
        adj = sorted(_adj(a) | _adj(b))
        matched = []
        for gid, _n, _d, _s, _i, _o, _b, _e, m in LEDGER:
            if "adj" in m:
                if m["adj"] in adj: ledger_examples[gid].append(i)
                continue
            hit = (i in m.get("ids", [])) or (gaps & set(m.get("gaps", []))) or (sc in m.get("scopes", [])) or any(x.startswith(tuple(m.get("reasons", ["\0"]))) for x in reasons) or (m.get("lone_new_search") and lone)
            if hit: matched.append(gid)
        forced = []
        in_scope = sc in ("IN", "IN_NEW_SEARCH", "IN_PROTOCOL")
        CLASSIFY = {"R0075": ("M2A_GOLD", "F1: vocabulario cerrado, SET reemplaza; la unión es G-FU-09 (M2b)"),
                    "SF017": ("M2A_GOLD", "F4: re-consulta; superlativo singular = ORDER + CARDINALITY 1"),
                    "SF063": ("M2A_GOLD", "A7 + A9; A9.1 no aplica (TOPIC y BUYER nuevos); ops O12/O13 se fija en el esquema"),
                    "R0205": ("M2A_GOLD", "A8.1 + contrato vigente de D-MONEY: app/core/money_comparators.py:99 mapea «en adelante» a GTE (>=); la lectura principal de A y B ya es >= 500 M; ENADELANTE cerrado por contrato existente"),
                    "SF132": ("G-FU-19", "ambigüedad viva licitaciones = procesos u oportunidades; Product View")}
        cls = CLASSIFY.get(i)
        is_gold = in_scope and i not in dis and not amb and not forced
        if cls and cls[0] == "M2A_GOLD": is_gold = True
        if cls and cls[0] != "M2A_GOLD": is_gold = False; matched = [cls[0]]
        rec = {"gold_id": i, "slice": f"{name}_FOLLOWUP", "m2a_scope": sc, "ambiguous_by": amb,
               "primary_agreement": i not in dis, "adjudicated_by": adj,
               "expected_disposition": a["expected_disposition"] if a["expected_disposition"] == b["expected_disposition"] else None,
               "label_A": a, "label_B": b}
        if cls: rec["orchestrator_classification"] = {"destination": cls[0], "rule": cls[1]}
        if is_gold: gold.append(rec)
        else:
            why = ("DISPUTED_PRIMARY" if i in dis else "PENDING_DECISION" if forced else "AMBIGUOUS" if amb else "OUT_OF_M2A_SCOPE")
            gl = [g for g in matched if g not in ("G-FU-01b", "G-FU-02b", "G-FU-03")] or ["G-FU-90"]
            rec.update({"frontier_reason": why, "gap_ids": gl, "interim_disposition": rec["expected_disposition"]})
            front.append(rec)
            for g in gl: ledger_examples[g].append(i)
    out[name] = (gold, front)
def dump(path, rows):
    body = "\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows); pathlib.Path(path).write_text(body)
    return hashlib.sha256(body.encode()).hexdigest()
pub = f"{G}/gold_candidato"; priv = f"{P}/GOLD_CANDIDATE"
idx = {"sealed": False, "schema": "m2a-followup-gold/2.1", "rules": "contrato M1 v1.4 + F1-F30 + A1-A9 + A8.1 + A9.1",
       "note": "Candidato NO sellado. Cada registro conserva las dos etiquetas; nada reconciliado. M2A_GOLD = en alcance, acuerdo primario, sin ambigüedad viva ni decisión pendiente.", "slices": {}}
for name, (gold, front) in out.items():
    base = priv if name == "REAL" else pub
    hg = dump(f"{base}/M2A_GOLD_{name.lower()}.jsonl", gold); hf = dump(f"{base}/FOLLOWUP_FRONTIER_{name.lower()}.jsonl", front)
    idx["slices"][name] = {"M2A_GOLD": {"n": len(gold), "sha256": hg, "ids": [r["gold_id"] for r in gold],
                                         "by_disposition": dict(collections.Counter(str(r["expected_disposition"]) for r in gold))},
                           "FOLLOWUP_FRONTIER": {"n": len(front), "sha256": hf, "by_reason": dict(collections.Counter(r["frontier_reason"] for r in front)),
                                                 "records": [{"gold_id": r["gold_id"], "frontier_reason": r["frontier_reason"], "gap_ids": r["gap_ids"], "m2a_scope": r["m2a_scope"], "interim_disposition": r["interim_disposition"]} for r in front]}}
    print(name, "GOLD", len(gold), "FRONTIER", len(front), dict(collections.Counter(r["frontier_reason"] for r in front)))
pathlib.Path(f"{pub}/M2A_GOLD_AND_FRONTIER_INDEX.json").write_text(json.dumps(idx, ensure_ascii=False, indent=1))
rows = []; md = ["# FOLLOWUP_GAPS_LEDGER", "", "Huecos de follow-ups con comportamiento provisional, dueño y criterio de salida. `example_ids` sale de los candidatos (dos etiquetados ciegos).", "", "| gap_id | hueco | alcance | provisional | dueño | bloquea M2a | salida | casos |", "|---|---|---|---|---|---|---|---|"]
for gid, n, d, s, i, o, b, e, m in LEDGER + [("G-FU-90", "unclassified", "Ambiguos o en disputa sin código de hueco asignado (debe quedar vacío antes del sello).", "por clasificar", "mejor lectura AMBIGUOUS", "adjudicación", "NO", "clasificar al adjudicar", {})]:
    ex = sorted(set(ledger_examples.get(gid, [])))
    rows.append({"gap_id": gid, "name": n, "example_ids": ex, "description": d, "scope": s, "interim_behavior": i, "owner_stage": o, "blocks_m2a": b, "exit_criterion": e})
    md.append(f"| {gid} | **{n}** — {d} | {s} | {i} | {o} | {b} | {e} | {len(ex)}: {', '.join(ex[:12])}{' …' if len(ex) > 12 else ''} |")
pathlib.Path(f"{pub}/FOLLOWUP_GAPS_LEDGER.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows))
pathlib.Path(f"{pub}/FOLLOWUP_GAPS_LEDGER.md").write_text("\n".join(md) + "\n")
for r in rows: print(r["gap_id"], r["name"], len(r["example_ids"]), r["blocks_m2a"])
