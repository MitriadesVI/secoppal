#!/usr/bin/env python3
"""v2_2 -> v2_3: copia los 164 casos sin tocar e INSERTA SF165+ (forma esquema 2.1). Sin etiquetas."""
import json, os, hashlib
HERE = os.path.dirname(os.path.abspath(__file__)); OUT = os.path.dirname(HERE); SRC = os.path.join(os.path.dirname(OUT), "v2_2")
base = open(os.path.join(SRC, "followup_synthetic_cases.jsonl"), encoding="utf-8").read().splitlines()
intent = open(os.path.join(SRC, "AUTHOR_INTENT.jsonl"), encoding="utf-8").read().splitlines()

def C(dim, op, hint, level=None, unit=None, pol="POSITIVE"): return {"dimension": dim, "operator": op, "polarity": pol, "value_hint": hint, "level": level, "unit": unit}
TP = lambda h: C("TOPIC", "contains", h); B = lambda h: C("BUYER", "is", h); A = lambda h: C("ACTOR", "is", h)
GD = lambda h: C("GEOGRAPHY", "is", h, "DEPARTMENT"); GR = lambda h: C("GEOGRAPHY", "is", h, "REGION")
TY = lambda y: C("TIME", "during_year", str(y)); TM = lambda m: C("TIME", "during_month", m)
TR = lambda h: C("TIME", "between_dates", h); TREL = lambda h: C("TIME", "relative_period", h)
VL = lambda h, u: C("VALUE", "greater_or_equal", h, unit=u); VU = lambda h, u: C("VALUE", "less_or_equal", h, unit=u)
CT = lambda h: C("CONTRACT_TYPE_OR_NATURE", "is", h)
def IR(conds, univ="CONTRACTS", order=None, top=None):
    cs = [dict(c, id=f"c{i+1}") for i, c in enumerate(conds)]
    cs = [{k: c[k] for k in ("id", "dimension", "operator", "polarity", "value_hint", "level", "unit")} for c in cs]
    return {"operation": "SEARCH", "aspect": None, "universe": univ, "view": "RECORDS", "product_view": None, "view_source": "USER",
            "conditions": cs, "logic": None, "order": {"operator": order, "direction_source": "STATED"} if order else None,
            "cardinality": {"operator": "top_n", "n": top, "n_requested": top} if top else None, "host_notice": []}
def PRI(ir, gap=45, state="EXECUTED", pending=None, chain=None, owner="FOUNDATION_SEARCH"):
    return {"gold_id": None, "selection_rule": "SYNTHETIC_AUTHORED", "chain": chain or [], "gap_seconds": gap, "owner": owner, "state": state, "ir": ir, "pending": pending}
def PEND(reason, span, slots, val, opts=(), asks=1):
    return {"reason": reason, "span": span, "candidate_slots": slots, "candidate_value": val,
            "options": [{"option_id": f"o{i+1}", "slot": s, "value_hint": v} for i, (s, v) in enumerate(opts)], "asks": asks}

NEW = []
def case(grp, key, text, prior, ptext, trait, rule, twin=None, kind="TEXT", note=""):
    NEW.append(dict(grp=grp, key=key, text=text, prior=prior, ptext=ptext, trait=trait, rule=rule, twin=twin, kind=kind, note=note))

# ---------------------------------------------------------------- SMMLV (A2)
G = "SMMLV (A2)"
sm = [TP("dotación"), B("Alcaldía de Tunja"), VL("500 SMMLV", "SMMLV"), TY(2025)]
p = PRI(IR(sm), 40); t = "contratos de dotación de la Alcaldía de Tunja de al menos 500 SMMLV en 2025"
case(G, "SM01", "y que sean de prestación de servicios", p, t, "previo en SMMLV con un solo año + follow-up que añade otra condición (conserva SMMLV y año)", "A2", twin="SM02")
case(G, "SM02", "y en 2023?", p, t, "mismo previo; el follow-up CAMBIA el año: el monto en SMMLV se recalcula sobre la vigencia nueva", "A2", twin="SM01")
case(G, "SM03", "mejor mínimo 800", p, t, "cota sin unidad sobre previo en SMMLV: hereda SMMLV (única unidad activa)", "A2/F5", twin="SM02")
p = PRI(IR([TP("dotación"), B("Alcaldía de Tunja"), VL("500 SMMLV", "SMMLV"), TR("2023-01-01..2025-12-31")]), 52)
case(G, "SM04", "y que sean de prestación de servicios", p, "contratos de dotación de la Alcaldía de Tunja de al menos 500 SMMLV de 2023 a 2025", "previo en SMMLV con TIME de varios años: SMMLV_YEAR_AMBIGUOUS", "A2", twin="SM01")
p = PRI(IR([TP("dotación"), B("Alcaldía de Tunja"), VL("500 SMMLV", "SMMLV")]), 52)
case(G, "SM05", "y que sean de prestación de servicios", p, "contratos de dotación de la Alcaldía de Tunja de al menos 500 SMMLV (sin periodo)", "previo en SMMLV sin TIME: caso no adjudicado (gap SMMLV)", "A2", twin="SM01")
p = PRI(IR([TP("suministro de papelería"), B("Gobernación de Caldas"), VL("200 millones", "MILLONES"), TY(2025)]), 38)
case(G, "SM06", "hasta 900 SMMLV", p, "contratos de suministro de papelería de la Gobernación de Caldas de al menos 200 millones en 2025", "follow-up cita SMMLV sobre previo en millones: quedan dos unidades en el VALUE", "A2/F5")

# ---------------------------------------------------------------- F21
G = "Orden por fecha sin dirección (F21)"
p = PRI(IR([TP("mantenimiento de ascensores"), B("Gobernación de Santander"), TY(2025)]), 33); t = "contratos de mantenimiento de ascensores de la Gobernación de Santander en 2025"
case(G, "ORF1", "ordénalos por fecha", p, t, "orden por fecha sin dirección, sin top-N (más reciente primero, DEFAULTED)", "F21", twin="ORF2")
p = PRI(IR([TP("mantenimiento de ascensores"), B("Gobernación de Santander"), TY(2025)], order="value_desc", top=10), 33)
case(G, "ORF2", "ahora ordénalos por fecha", p, "los 10 contratos de mayor valor de mantenimiento de ascensores de la Gobernación de Santander en 2025", "orden por fecha sin dirección con top-N arrastrado (conserva N)", "F10/F21", twin="ORF1")

# ---------------------------------------------------------------- A5
G = "Referente temporal relativo (A5)"
p = PRI(IR([TP("vigilancia"), B("Alcaldía de Floridablanca"), TY(2024)]), 29); t = "contratos de vigilancia de la Alcaldía de Floridablanca en 2024"
case(G, "TEM1", "el año anterior", p, t, "«el año anterior» con un único año activo (resuelve a 2023)", "A5", twin="TEM3")
p = PRI(IR([TP("aseo"), B("Alcaldía de Pitalito"), TM("2025-03")]), 31)
case(G, "TEM2", "el mes siguiente", p, "contratos de aseo de la Alcaldía de Pitalito en marzo de 2025", "«el mes siguiente» con un único mes activo (resuelve a abril de 2025)", "A5")
p = PRI(IR([TP("vigilancia"), B("Alcaldía de Floridablanca"), TR("2022-01-01..2024-12-31")]), 29)
case(G, "TEM3", "el año anterior", p, "contratos de vigilancia de la Alcaldía de Floridablanca de 2022 a 2024", "mismo texto con TIME previo de varios años: no hay referente único", "A5", twin="TEM1")
p = PRI(IR([TP("transporte escolar"), B("Gobernación del Cesar")]), 36)
case(G, "TEM4", "el mes siguiente", p, "contratos de transporte escolar de la Gobernación del Cesar (sin periodo)", "referente temporal relativo sin TIME previo", "A5")
p = PRI(IR([TP("obras de acueducto"), B("Gobernación de Sucre"), TREL("últimos 6 meses (2026-04-01..2026-10-01)")]), 41)
case(G, "TEM5", "el año anterior", p, "contratos de obras de acueducto de la Gobernación de Sucre en los últimos 6 meses", "referente temporal relativo con ventana relativa previa (cabe leerlo contra el reloj)", "A5", twin="TEM1")

# ---------------------------------------------------------------- A6
G = "Fuente fallida (A6)"
p = PRI(IR([TP("dotación hospitalaria"), B("Gobernación de Bolívar")]), 24, state="NOT_EXECUTED"); t = "contratos de dotación hospitalaria de la Gobernación de Bolívar (aceptada; la fuente falló, sin pregunta pendiente)"
case(G, "NEX1", "y que valgan más de 100 millones", p, t, "previo aceptado cuya fuente falló + delta semántico (se aplica y ejecuta)", "A6", twin="NEX3")
case(G, "NEX2", "muéstrame más resultados", p, t, "previo cuya fuente falló + «ver más» (no hay página)", "A6")
case(G, "NEX3", "ábreme el segundo", p, t, "previo cuya fuente falló + referente de fila (no hay filas)", "A6", twin="NEX1")
case(G, "NEX4", "pásame eso a un archivo de Excel", p, t, "previo cuya fuente falló + exportar lo mostrado (nada mostrado)", "A6")

# ---------------------------------------------------------------- A1
G = "Cota contradictoria (A1)"
p = PRI(IR([TP("interventoría de obras"), B("Universidad del Valle"), VL("150 millones", "MILLONES")]), 47); t = "contratos de interventoría de obras de la Universidad del Valle de al menos 150 millones"
case(G, "RNG1", "máximo 90 millones", p, t, "cota superior nueva con unidad dicha, menor que la inferior vigente (rango sin interior)", "A1", twin="RNG2")
case(G, "RNG2", "máximo 90", p, t, "igual, con unidad heredada (única unidad activa)", "A1/F5", twin="RNG1")

# ---------------------------------------------------------------- A4
G = "Autosuficiencia (A4)"
p = PRI(IR([TP("vigilancia"), B("Alcaldía de Tunja")]), 52); t = "contratos de vigilancia de la Alcaldía de Tunja"
case(G, "AUT1", "contratos de mantenimiento de parques de la Alcaldía de Duitama", p, t, "petición autosuficiente CON sustantivo de universo", "A4", twin="AUT2")
case(G, "AUT2", "mantenimiento de parques de la Alcaldía de Duitama", p, t, "la misma petición SIN sustantivo de universo (también autosuficiente; vista por defecto)", "A4", twin="AUT1")
p = PRI(IR([TP("pavimentación"), B("Alcaldía de Montería")], "PROCESSES"), 44); t = "procesos de pavimentación de la Alcaldía de Montería"
case(G, "AUT3", "y los contratos de 2024?", p, t, "fragmento con sustantivo de universo que SÍ depende del previo («y los…?»)", "A4", twin="AUT4")
case(G, "AUT4", "contratos de 2024", p, t, "el mismo fragmento sin «y los…?»: frontera de dependencia sin regla (UNIVERSE_FRAGMENT)", "A4", twin="AUT3")

# ---------------------------------------------------------------- A7
G = "Aclaración pendiente (A7)"
FR = [TP("suministro de combustible"), B("Alcaldía de Girardot"), C("VALUE", "greater_than", "80 millones", unit="MILLONES")]
pt = "contratos de suministro de combustible de la Alcaldía de Girardot por más de 80 millones en el segundo trimestre (¿de qué año?)"
pend = PEND("UNRESOLVED_VALUE:TIME", "el segundo trimestre", ["TIME"], "segundo trimestre (abril a junio), año sin decir")
p = PRI(IR(FR), 21, state="PENDING", pending=pend)
case(G, "PEN1", "de abril a junio de 2025", p, pt, "respuesta compatible: modifica el marco pendiente", "A7")
VL4 = [("BUYER", "Alcaldía de Villanueva (La Guajira)"), ("BUYER", "Alcaldía de Villanueva (Bolívar)"), ("BUYER", "Alcaldía de Villanueva (Casanare)"), ("BUYER", "Alcaldía de Villanueva (Santander)")]
p2 = PRI(IR([TP("aseo")]), 19, state="PENDING", pending=PEND("UNRESOLVED_VALUE:BUYER", "Villanueva", ["BUYER"], "Villanueva", VL4))
case(G, "PEN2", "la del Caribe", p2, "contratos de aseo de Villanueva (4 entidades posibles)", "respuesta compatible que deja el marco aún ambiguo (dos candidatas): REASK", "A7")
case(G, "PEN3", "procesos de dotación escolar de la Gobernación del Chocó que estén abiertos", p, pt, "consulta autosuficiente: cancela la pendiente y es búsqueda nueva", "A7")
case(G, "PEN4", "no, mejor solo los que pasen de 200 millones", p, pt, "«no, mejor X» con X fragmento: cancela y aplica X sobre el marco pendiente", "A7")

# ---------------------------------------------------------------- emit
sha = lambda s: hashlib.sha256(s.encode()).hexdigest()
n0 = len(base); ids = {}
for i, c in enumerate(NEW): c["id"] = f"SF{n0 + 1 + i:03d}"; ids[c["key"]] = c["id"]
with open(os.path.join(OUT, "followup_synthetic_cases.jsonl"), "w", encoding="utf-8") as f:
    for l in base: f.write(l + "\n")
    for c in NEW:
        f.write(json.dumps({"schema": "m2a-followup-gold/2.1", "gold_id": c["id"], "slice": "SYNTHETIC_FOLLOWUP", "followup_text": c["text"],
                            "followup_sha256": sha(c["text"]), "source_kind": "SYNTHETIC", "input_kind": c["kind"], "prior": c["prior"]}, ensure_ascii=False) + "\n")
with open(os.path.join(OUT, "AUTHOR_INTENT.jsonl"), "w", encoding="utf-8") as f:
    for l in intent: f.write(l + "\n")
    for c in NEW:
        f.write(json.dumps({"gold_id": c["id"], "key": c["key"], "class": "v2.3 · " + c["grp"], "feature_exercised": c["trait"], "rule": c["rule"],
                            "twin_id": ids.get(c["twin"]) if c["twin"] else None, "prior_as_natural_text": c["ptext"], "note": c["note"]}, ensure_ascii=False) + "\n")
by = {}
for c in NEW: by.setdefault(c["grp"], []).append(c["id"])
json.dump(by, open(os.path.join(HERE, "new_ids_by_frontier.json"), "w"), ensure_ascii=False, indent=1)
print(n0 + len(NEW), by)
