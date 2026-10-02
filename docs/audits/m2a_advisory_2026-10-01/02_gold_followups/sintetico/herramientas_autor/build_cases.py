#!/usr/bin/env python3
"""Author tool: builds followup_synthetic_cases.jsonl + AUTHOR_INTENT.jsonl.
Contains NO label fields. Do not give this file or AUTHOR_INTENT to labelers."""
import hashlib, json, os, sys
OUT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------- builders
def T_year(y): return ("TIME", "during_year", str(y), None, None)
def T_month(h): return ("TIME", "during_month", h, None, None)
def T_range(h): return ("TIME", "between_dates", h, None, None)
def T_rel(h): return ("TIME", "relative_period", h, None, None)
def VAL(op, hint, unit="MILLONES"):
    return ("VALUE", {">": "greater_than", ">=": "greater_or_equal", "<": "less_than", "<=": "less_or_equal"}[op], hint, None, unit)
def B(n): return ("BUYER", "is", n, None, None)
def A(n): return ("ACTOR", "is", n, None, None)
def TP(n): return ("TOPIC", "contains", n, None, None)
def GD(n): return ("GEOGRAPHY", "is", n, "DEPARTMENT", None)
def GM(n): return ("GEOGRAPHY", "is", n, "MUNICIPALITY", None)
def GR(n): return ("GEOGRAPHY", "is", n, "REGION", None)
def GU(n): return ("GEOGRAPHY", "is", n, "UNKNOWN", None)
def ST(h, op="is"): return ("STATE", op, h, None, None)
def MD(h, op="is"): return ("MODALITY", op, h, None, None)
def CT(h): return ("CONTRACT_TYPE_OR_NATURE", "is", h, None, None)
def NEG(c): return c + ("NEG",)

def _cond(i, c):
    pol = "NEGATIVE" if len(c) > 5 else "POSITIVE"
    return {"id": f"c{i}", "dimension": c[0], "operator": c[1], "polarity": pol,
            "value_hint": c[2], "level": c[3], "unit": c[4]}

TERMINAL = {"closed", "awarded", "terminated", "cancelled", "in_execution", "liquidated", "signed"}
def derive_pv(conds, order):
    pos = [c for c in conds if len(c) == 5]
    dims = {c[0] for c in pos}
    if "ACTOR" in dims: return "CONTRACTS"
    if "TIME" in dims: return "HISTORICAL_PROCESSES"
    if any(c[0] == "STATE" and c[2] in TERMINAL for c in pos): return "HISTORICAL_PROCESSES"
    if "CONTRACT_TYPE_OR_NATURE" in dims: return "CONTRACTS"
    if order and order.startswith("value"): return "HISTORICAL_PROCESSES"
    return "CURRENT_OPPORTUNITIES"

def IR(conds, univ="CONTRACTS", order=None, top=None, pv=None, view_source=None, view="RECORDS", operation="SEARCH"):
    order_o = {"operator": order, "polarity": "POSITIVE"} if order else None
    card = {"operator": "top_n", "value_hint": str(top)} if top else None
    d = {"operation": operation, "universe": None, "view": view, "product_view": None, "view_source": None,
         "conditions": [_cond(i + 1, c) for i, c in enumerate(conds)], "order": order_o, "cardinality": card}
    if univ is None:
        d["universe"] = "UNSPECIFIED"
        d["product_view"] = pv or derive_pv(conds, order)
        d["view_source"] = view_source or "DERIVED"
    elif univ == "DUAL":
        d["universe"] = "DUAL"; d["universes"] = ["PROCESSES", "CONTRACTS"]; d["view_source"] = "USER"
    else:
        d["universe"] = univ; d["view_source"] = "USER"
    return d

def PRIOR(ir, gap=60, owner="FOUNDATION_SEARCH", state="EXECUTED", chain=None, pending=None):
    return {"gold_id": None, "selection_rule": "SYNTHETIC_AUTHORED", "chain": chain or [], "gap_seconds": gap,
            "owner": owner, "state": state, "ir": ir, "pending": pending}

CASES = []
def case(cls, key, text, prior, ptext, trait, twin=None, note=""):
    CASES.append(dict(cls=cls, key=key, text=text, prior=prior, ptext=ptext, trait=trait, twin=twin, note=note))

def P(ptext, conds, univ="CONTRACTS", gap=60, **kw):
    ir_kw = {k: kw.pop(k) for k in list(kw) if k in ("order", "top", "pv", "view_source", "view", "operation")}
    return PRIOR(IR(conds, univ, **ir_kw), gap=gap, **kw), ptext

# ============================================================ 1 POLARIDAD (14)
C = "Polaridad negativa"
pr, pt = P("contratos de la Gobernación de Boyacá en 2025", [B("Gobernación de Boyacá"), T_year(2025)], gap=45)
case(C, "POL01", "y que no sean de papelería", pr, pt, "TOPIC NEGATIVE sin tema previo", twin="POL02")
case(C, "POL02", "y que sean de papelería", pr, pt, "TOPIC POSITIVE (gemelo de POL01)", twin="POL01")
pr, pt = P("contratos de pavimentación en 2024", [TP("pavimentación"), T_year(2024)], gap=80, univ="CONTRACTS")
case(C, "POL03", "menos los del Eje Cafetero", pr, pt, "GEOGRAPHY NEGATIVE con región de la lista cerrada", twin="POL04")
case(C, "POL04", "solo los del Eje Cafetero", pr, pt, "GEOGRAPHY POSITIVE región (gemelo)", twin="POL03")
pr, pt = P("contratos de capacitación docente en 2025", [TP("capacitación docente"), T_year(2025)], gap=30)
case(C, "POL05", "pero que no sean del SENA", pr, pt, "BUYER NEGATIVE", twin="POL06")
case(C, "POL06", "pero que sean del SENA", pr, pt, "BUYER POSITIVE (gemelo)", twin="POL05")
pr, pt = P("procesos de consultoría ambiental en 2024", [TP("consultoría ambiental"), T_year(2024)], univ="PROCESSES", gap=95)
case(C, "POL07", "sin contratación directa", pr, pt, "MODALITY NEGATIVE", twin="POL08")
case(C, "POL08", "solo contratación directa", pr, pt, "MODALITY POSITIVE (gemelo)", twin="POL07")
pr, pt = P("procesos de dotación escolar de la Gobernación del Valle del Cauca", [TP("dotación escolar"), B("Gobernación del Valle del Cauca")], univ="PROCESSES", gap=20)
case(C, "POL09", "solo los que no estén adjudicados", pr, pt, "STATE NEGATIVE (awarded)", twin="POL10")
case(C, "POL10", "solo los adjudicados", pr, pt, "STATE POSITIVE awarded (gemelo)", twin="POL09")
pr, pt = P("contratos de mantenimiento vial en Cali en 2024", [TP("mantenimiento vial"), GM("Cali"), T_year(2024)], gap=70)
case(C, "POL11", "sin los de Cali", pr, pt, "negación de GEOGRAPHY ya arrastrada en positivo (debe aclarar)")
pr, pt = P("procesos de aseo de la Alcaldía de Pasto", [TP("aseo"), B("Alcaldía de Pasto")], univ="PROCESSES", gap=55)
case(C, "POL12", "excluye a la Alcaldía de Pasto", pr, pt, "negación de BUYER ya arrastrado en positivo (debe aclarar)")
pr, pt = P("contratos de interventoría de la Universidad Nacional de Colombia", [TP("interventoría"), B("Universidad Nacional de Colombia")], gap=100)
case(C, "POL13", "excluye los de más de 500 millones", pr, pt, "VALUE NEGATIVE (comparador negado)")
pr, pt = P("contratos de aseo de la Alcaldía de Montería", [TP("aseo"), B("Alcaldía de Montería")], gap=40)
case(C, "POL14", "sin contar los de 2023", pr, pt, "TIME NEGATIVE sin TIME previo")

# ============================================================ 2 TOP-N (10)
C = "Top-N"
pr, pt = P("contratos de construcción de vías de la Gobernación de Santander en 2024", [TP("construcción de vías"), B("Gobernación de Santander"), T_year(2024)], gap=50)
case(C, "TOP01", "dame solo los 5 más caros", pr, pt, "cifra explícita con orden", twin="TOP02")
case(C, "TOP02", "dame solo los más caros", pr, pt, "plural sin cifra (gemelo de TOP01)", twin="TOP01")
case(C, "TOP03", "cuál es el de mayor valor?", pr, pt, "superlativo singular (N=1)", twin="TOP02")
pr, pt = P("los 10 contratos más caros de suministro de combustible de la Alcaldía de Cartagena", [TP("suministro de combustible"), B("Alcaldía de Cartagena")], order="value_desc", top=10, gap=35)
case(C, "TOP04", "ahora ordénalos por fecha, primero los más recientes", pr, pt, "orden nuevo con top-N arrastrado (F10), más reciente", twin="TOP05")
case(C, "TOP05", "ahora ordénalos por fecha, primero los más antiguos", pr, pt, "orden nuevo con top-N arrastrado (F10), más antiguo (gemelo)", twin="TOP04")
pr, pt = P("contratos de mantenimiento de ascensores de la Universidad de Antioquia", [TP("mantenimiento de ascensores"), B("Universidad de Antioquia")], gap=65)
case(C, "TOP06", "los 20 más recientes", pr, pt, "top-N con orden por fecha")
pr, pt = P("contratos de transporte escolar en Boyacá en 2025", [TP("transporte escolar"), GD("Boyacá"), T_year(2025)], gap=25)
case(C, "TOP07", "dame los 250 de mayor valor", pr, pt, "N por encima del tope de 100", twin="TOP08")
case(C, "TOP08", "dame los 100 de mayor valor", pr, pt, "N exactamente en el tope de 100 (gemelo)", twin="TOP07")
pr, pt = P("los 10 contratos más recientes de papelería de la Alcaldía de Manizales", [TP("papelería"), B("Alcaldía de Manizales")], order="date_desc", top=10, gap=42)
case(C, "TOP09", "quita el orden, quiero verlos todos", pr, pt, "CLEAR de orden con cardinalidad arrastrada")
pr, pt = P("los 10 contratos de mayor valor de dotación hospitalaria de la Gobernación de Nariño", [TP("dotación hospitalaria"), B("Gobernación de Nariño")], order="value_desc", top=10, gap=38)
case(C, "TOP10", "no solo 10, todos, pero déjalos por valor", pr, pt, "CLEAR sólo de la cardinalidad; el orden se conserva")

# ============================================================ 3 TIEMPO RELATIVO (12)
C = "Tiempo relativo"
pr, pt = P("contratos de aseo de la Alcaldía de Barranquilla", [TP("aseo"), B("Alcaldía de Barranquilla")], gap=33)
case(C, "REL01", "y este año?", pr, pt, "«este año» como SET TIME sin TIME previo")
pr, pt = P("contratos de dotación del ICBF", [TP("dotación"), B("ICBF")], gap=48)
case(C, "REL02", "los de este mes", pr, pt, "«este mes» como SET TIME relativo")
pr, pt = P("contratos de mantenimiento de la Universidad de Antioquia", [TP("mantenimiento"), B("Universidad de Antioquia")], gap=75)
case(C, "REL03", "de los últimos 6 meses", pr, pt, "«últimos N meses» sin TIME previo")
pr, pt = P("contratos de vigilancia de la Alcaldía de Cartagena en 2024", [TP("vigilancia"), B("Alcaldía de Cartagena"), T_year(2024)], gap=52)
case(C, "REL04", "y los últimos 3 meses?", pr, pt, "relativo sobre TIME arrastrado de un año")
pr, pt = P("contratos de obras viales del Departamento del Cesar en los últimos 3 meses", [TP("obras viales"), GD("Cesar"), T_rel("últimos 3 meses")], gap=61)
case(C, "REL05", "mejor los últimos 12 meses", pr, pt, "relativo sobre relativo arrastrado")
pr, pt = P("contratos de alimentación escolar de la Alcaldía de Soacha en marzo de 2025", [TP("alimentación escolar"), B("Alcaldía de Soacha"), T_month("2025-03")], gap=44)
case(C, "REL06", "mejor el año en curso", pr, pt, "relativo sobre un mes arrastrado")
pr, pt = P("contratos de interventoría de la Gobernación del Tolima de 2023 a 2025", [TP("interventoría"), B("Gobernación del Tolima"), T_range("2023 a 2025")], gap=58)
case(C, "REL07", "solo marzo", pr, pt, "mes sin año con TIME previo de varios años (debe aclarar)", twin="REL08")
pr, pt = P("contratos de interventoría de la Gobernación del Tolima en 2024", [TP("interventoría"), B("Gobernación del Tolima"), T_year(2024)], gap=58)
case(C, "REL08", "solo marzo", pr, pt, "mes sin año con un único año activo (hereda) (gemelo de REL07)", twin="REL07")
pr, pt = P("contratos de reparación de acueductos de la Gobernación de La Guajira en los últimos 6 meses", [TP("reparación de acueductos"), B("Gobernación de La Guajira"), T_rel("últimos 6 meses")], gap=47)
case(C, "REL09", "y en octubre?", pr, pt, "mes sin año con previo relativo (debe aclarar)")
pr, pt = P("contratos de señalización vial de la Alcaldía de Bucaramanga", [TP("señalización vial"), B("Alcaldía de Bucaramanga")], gap=39)
case(C, "REL10", "y en abril?", pr, pt, "mes sin año sin TIME previo: más reciente no posterior a hoy (abril 2026)", twin="REL11")
case(C, "REL11", "y en diciembre?", pr, pt, "mes sin año sin TIME previo, mes posterior a hoy -> diciembre de 2025 (gemelo)", twin="REL10")
pr, pt = P("contratos de software de la Gobernación del Atlántico de 2022 a 2024", [TP("software"), B("Gobernación del Atlántico"), T_range("2022 a 2024")], gap=66)
case(C, "REL12", "mejor este año", pr, pt, "«este año» reemplazando un rango de varios años")

# ============================================================ 4 D7 RELATIVO AL PREVIO (10)
C = "Relativo al valor previo (D7)"
pr, pt = P("contratos de aseo de la Alcaldía de Soledad en marzo de 2024", [TP("aseo"), B("Alcaldía de Soledad"), T_month("2024-03")], gap=30)
case(C, "D701", "y el mes siguiente?", pr, pt, "operador relativo sobre mes previo")
pr, pt = P("contratos de dotación de la Gobernación de Caldas en 2024", [TP("dotación"), B("Gobernación de Caldas"), T_year(2024)], gap=41)
case(C, "D702", "el año anterior", pr, pt, "operador relativo sobre año previo")
pr, pt = P("contratos de suministro de medicamentos del Hospital Universitario del Valle de más de 200 millones", [TP("suministro de medicamentos"), B("Hospital Universitario del Valle"), VAL(">=", "200 millones")], gap=72)
case(C, "D703", "bájalo un poco", pr, pt, "operador relativo sobre un valor")
pr, pt = P("contratos de vigilancia de la Alcaldía de Yopal de al menos 100 millones", [TP("vigilancia"), B("Alcaldía de Yopal"), VAL(">=", "100 millones")], gap=36)
case(C, "D704", "el doble", pr, pt, "multiplicador sobre valor previo")
pr, pt = P("contratos de aseo de la Alcaldía de Ibagué", [TP("aseo"), B("Alcaldía de Ibagué")], gap=54)
case(C, "D705", "ahora vigilancia, pero con el mismo comprador", pr, pt, "referencia «el mismo comprador» al valor previo")
pr, pt = P("contratos de papelería del SENA de enero a junio de 2024", [TP("papelería"), B("SENA"), T_range("enero a junio de 2024")], gap=49)
case(C, "D706", "los seis meses siguientes", pr, pt, "ventana siguiente sobre rango previo")
pr, pt = P("contratos de mantenimiento de la Alcaldía de Villavicencio de hasta 50 millones", [TP("mantenimiento"), B("Alcaldía de Villavicencio"), VAL("<=", "50 millones")], gap=60)
case(C, "D707", "súbelo un poquito", pr, pt, "operador relativo sobre cota superior")
pr, pt = P("contratos de consultoría de la Gobernación de Sucre de al menos 30 millones", [TP("consultoría"), B("Gobernación de Sucre"), VAL(">=", "30 millones")], gap=31)
case(C, "D708", "el triple", pr, pt, "multiplicador sobre valor previo")
pr, pt = P("contratos de obra de la Alcaldía de Cúcuta de febrero a abril de 2025", [TP("obra"), B("Alcaldía de Cúcuta"), T_range("febrero a abril de 2025")], gap=43)
case(C, "D709", "un trimestre antes", pr, pt, "ventana anterior sobre rango previo")
pr, pt = P("contratos de transporte de la Gobernación de Córdoba entre 100 y 300 millones", [TP("transporte"), B("Gobernación de Córdoba"), VAL(">=", "100 millones"), VAL("<=", "300 millones")], gap=57)
case(C, "D710", "la mitad de eso", pr, pt, "fracción sobre rango previo")

# ============================================================ 5 RESPUESTA A ACLARACION (18)
C = "Respuesta a aclaración"
FR = [TP("interventoría"), B("Alcaldía de Floridablanca"), VAL(">", "150 millones")]
PEND_T = {"dimension": "TIME", "reason": "PERIOD_WITHOUT_YEAR", "options": 0}
fr_text = "contratos de interventoría de la Alcaldía de Floridablanca por más de 150 millones en el primer semestre (pregunta abierta: ¿de qué año?)"
def pend_prior(chain=None, gap=25):
    return PRIOR(IR(FR, "CONTRACTS"), gap=gap, state="PENDING", pending=PEND_T, chain=chain)
pp = pend_prior()
case(C, "ACL01", "contratos de alquiler de oficinas de la Alcaldía de Piedecuesta de enero a junio de 2025", pp, fr_text, "respuesta que cambia tema y comprador a la vez (R5 F2)")
case(C, "ACL02", "contratos de alquiler de oficinas de la Gobernación de Santander de enero a junio de 2025", pp, fr_text, "respuesta que cambia tema y comprador por una gobernación (R5 F4)", twin="ACL01")
case(C, "ACL03", "contratos de alquiler de oficinas de la Alcaldía de Piedecuesta en 2024", pp, fr_text, "respuesta con tema, comprador nuevos y otro periodo (R5 F5)")
case(C, "ACL04", "contratos de alquiler de oficinas de la Alcaldía de Piedecuesta en 2024", pend_prior(chain=["PROTOCOL:reask"], gap=70), fr_text, "igual a ACL03 tras una repregunta (R5 F6)", twin="ACL03")
case(C, "ACL05", "pero de menos de 40 millones de enero a junio de 2025", pp, fr_text, "respuesta que además invierte el valor del frame (R5 F7)")
case(C, "ACL06", "procesos de alquiler de oficinas de la Alcaldía de Piedecuesta de enero a junio de 2025", pp, fr_text, "búsqueda completa nueva con otro universo (R5 F8)")
case(C, "ACL07", "los de la Alcaldía de Piedecuesta de enero a junio de 2025", pp, fr_text, "respuesta sin tema que cambia el comprador (R5)")
ex_ir = PRIOR(IR(FR + [T_year(2024)], "CONTRACTS"), gap=50, state="EXECUTED")
case(C, "ACL08", "mejor de enero a junio de 2025", ex_ir, "mismo frame ya ejecutado en 2024, sin pregunta abierta", "follow-up directo sin pregunta abierta (R5 F1/F3)")
case(C, "ACL09", "contratos de interventoría de la Alcaldía de Floridablanca por más de 150 millones de enero a junio de 2025", pp, fr_text, "respuesta que repite el frame con el periodo (R5)")
case(C, "ACL10", "de enero a junio de 2025", pp, fr_text, "respuesta sólo con el periodo (R5)")
case(C, "ACL11", "busca contratos de alquiler de oficinas de la Alcaldía de Piedecuesta de enero a junio de 2025", pp, fr_text, "salida «busca …» que nombra la pregunta (R5)")
ABUY = [TP("aseo"), GU("San Pedro")]
pend_b = {"dimension": "BUYER", "reason": "AMBIGUOUS_HOST_DIMENSION:BUYER", "options": 3}
pb = PRIOR(IR([TP("aseo"), B("San Pedro")], "CONTRACTS"), gap=18, state="PENDING", pending=pend_b)
case(C, "ACL12", "Alcaldía de San Pedro (Valle del Cauca)", pb, "contratos de aseo de San Pedro (3 entidades posibles, pregunta abierta)", "opción elegida por evento (etiqueta de botón) entre 3 compradores")
pend_f12 = {"dimension": "BUYER", "reason": "AMBIGUOUS_HOST_DIMENSION:BUYER|GEOGRAPHY", "options": 2}
pf = PRIOR(IR([A("Inversiones Cóndor del Sur S.A.S.")], "CONTRACTS"), gap=22, state="PENDING", pending=pend_f12)
case(C, "ACL13", "Como comprador", pf, "contratos de Inversiones Cóndor del Sur S.A.S. con Soledad (¿comprador o lugar?)", "opción F12 por evento: comprador", twin="ACL14")
case(C, "ACL14", "Como lugar", pf, "contratos de Inversiones Cóndor del Sur S.A.S. con Soledad (¿comprador o lugar?)", "opción F12 por evento: lugar (gemelo)", twin="ACL13")
pb3 = PRIOR(IR([TP("dotación"), B("Santa Rosa")], "CONTRACTS"), gap=27, state="PENDING", pending=pend_b)
case(C, "ACL15", "cualquiera, el que sea", pb3, "contratos de dotación de Santa Rosa (3 entidades posibles)", "respuesta que no resuelve la opción")
case(C, "ACL16", "mejor dime cuánto suman en total", pb3, "contratos de dotación de Santa Rosa (3 entidades posibles)", "respuesta que cambia de operación sin resolver", twin="ACL15")
pr2 = PRIOR(IR(FR, "CONTRACTS"), gap=44, state="PENDING", pending=PEND_T, chain=["PROTOCOL:reask", "PROTOCOL:reask"])
case(C, "ACL17", "no, mejor dotación de la Gobernación del Cauca en 2025", pr2, fr_text + " (ya repreguntada dos veces)", "segundo cambio de marco tras una repregunta")
pexp = PRIOR(IR(FR, "CONTRACTS"), gap=2640, state="EXPIRED", pending=PEND_T)
case(C, "ACL18", "de enero a junio de 2025", pexp, fr_text + " (sin respuesta durante 44 minutos)", "respuesta a una pregunta pendiente caducada", twin="ACL10")

# ============================================================ 6 REFERENTES (8)
C = "Referentes ordinales y de fila"
pr, pt = P("contratos de vigilancia de la Gobernación del Atlántico en 2025 (lista de 10 mostrada)", [TP("vigilancia"), B("Gobernación del Atlántico"), T_year(2025)], gap=28)
case(C, "REF01", "el segundo", pr, pt, "ordinal sobre fila mostrada")
case(C, "REF02", "ese contrato, de qué fecha es?", pr, pt, "deíctico «ese contrato» sobre una fila")
case(C, "REF03", "ábreme el tercero", pr, pt, "ordinal con verbo de abrir")
pr, pt = P("contratos de servicios de salud de la Gobernación del Valle del Cauca (lista mostrada)", [TP("servicios de salud"), B("Gobernación del Valle del Cauca")], gap=34)
case(C, "REF04", "de esos, los que sean de Cali", pr, pt, "«de esos, los de…» sobre el conjunto mostrado", twin="REF05")
case(C, "REF05", "los que sean de Cali", pr, pt, "re-consulta con lugar sin «de esos» (F4) (gemelo)", twin="REF04")
case(C, "REF06", "el primero de la lista, cuánto vale?", pr, pt, "ordinal + pregunta sobre atributo de la fila")
case(C, "REF07", "y ese último, quién lo firmó?", pr, pt, "«ese último» como fila")
case(C, "REF08", "los dos primeros", pr, pt, "ordinal plural de filas")

# ============================================================ 7 UNIVERSO (12)
C = "Cambio explícito de universo"
pr, pt = P("contratos de aseo de la Alcaldía de Soledad en 2025", [TP("aseo"), B("Alcaldía de Soledad"), T_year(2025)], gap=40)
case(C, "UNI01", "y ahora los procesos", pr, pt, "contratos -> procesos con condiciones arrastradas")
pr, pt = P("procesos de dotación de la Gobernación de Córdoba", [TP("dotación"), B("Gobernación de Córdoba")], univ="PROCESSES", gap=52)
case(C, "UNI02", "mejor en contratos", pr, pt, "procesos -> contratos")
pr, pt = P("contratos de papelería del SENA en 2025", [TP("papelería"), B("SENA"), T_year(2025)], gap=29)
case(C, "UNI03", "mejor muéstrame las oportunidades", pr, pt, "cambio a oportunidades")
pr, pt = P("contratos y procesos de mantenimiento vial de la Gobernación de Boyacá (respuesta dual)", [TP("mantenimiento vial"), B("Gobernación de Boyacá")], univ="DUAL", gap=36)
case(C, "UNI04", "solo los contratos", pr, pt, "ancla dual se queda con contratos", twin="UNI05")
case(C, "UNI05", "solo los procesos", pr, pt, "ancla dual se queda con procesos (gemelo)", twin="UNI04")
pr, pt = P("contratos de obra civil de la Alcaldía de Neiva", [TP("obra civil"), B("Alcaldía de Neiva")], gap=46)
case(C, "UNI06", "los procesos que están abiertos", pr, pt, "universo dicho + STATE open (existe sólo en procesos)", twin="UNI07")
pr7, pt7 = P("procesos de obra civil de la Alcaldía de Neiva", [TP("obra civil"), B("Alcaldía de Neiva")], univ="PROCESSES", gap=46)
case(C, "UNI07", "los contratos que están abiertos", pr7, pt7, "universo dicho contratos + STATE open (no existe en contratos) (gemelo)", twin="UNI06")
pr, pt = P("contratos de transporte escolar de la Gobernación de Nariño", [TP("transporte escolar"), B("Gobernación de Nariño")], gap=38)
case(C, "UNI08", "procesos, pero solo de mínima cuantía", pr, pt, "universo dicho + MODALITY")
bpr, bpt = P("señalización vial en Boyacá (vista elegida con el botón [Contratos])", [TP("señalización vial"), GD("Boyacá")], univ=None, pv="CONTRACTS", view_source="USER", chain=["PROTOCOL:option"], gap=15)
case(C, "UNI09", "y solo 2025", bpr, bpt, "vista elegida por botón no se recalcula tras añadir TIME", twin="UNI10")
dpr, dpt = P("señalización vial en Boyacá (vista derivada, oportunidades vigentes)", [TP("señalización vial"), GD("Boyacá")], univ=None, gap=15)
case(C, "UNI10", "y solo 2025", dpr, dpt, "vista derivada sí se recalcula al añadir TIME (VIEW_REDERIVED) (gemelo)", twin="UNI09")
upr, upt = P("procesos de señalización vial en Boyacá (universo dicho)", [TP("señalización vial"), GD("Boyacá")], univ="PROCESSES", gap=15)
case(C, "UNI11", "y solo 2025", upr, upt, "universo dicho por el usuario no se recalcula al añadir TIME", twin="UNI10")
pr, pt = P("procesos de pavimentación de la Alcaldía de Montería", [TP("pavimentación"), B("Alcaldía de Montería")], univ="PROCESSES", gap=33)
case(C, "UNI12", "mejor las convocatorias que siguen recibiendo ofertas", pr, pt, "cambio a oportunidades con estado de apertura")

# ============================================================ 8 VALUE POR LADO (16)
C = "VALUE por lado"
base = [TP("ambulancias"), B("Gobernación de Antioquia")]
def vp(vals, gap=45):
    txt = "contratos de ambulancias de la Gobernación de Antioquia" + (" con valor " + " y ".join(f"{'≥' if v[1].startswith('greater') else '≤'} {v[2]}" for v in vals) if vals else "")
    return PRIOR(IR(base + vals, "CONTRACTS"), gap=gap), txt
lo = lambda h: VAL(">=", h); up = lambda h: VAL("<=", h)
p, t = vp([]);                         case(C, "VAL01", "los de más de 80 millones", p, t, "previo sin valor, nueva cota inferior")
case(C, "VAL02", "máximo 300 millones", p, t, "previo sin valor, nueva cota superior")
case(C, "VAL03", "entre 50 y 120 millones", p, t, "previo sin valor, rango nuevo")
p, t = vp([lo("100 millones")]);       case(C, "VAL04", "mínimo 250 millones", p, t, "previo inferior; nueva inferior reemplaza la inferior")
case(C, "VAL05", "hasta 400 millones", p, t, "previo inferior; nueva superior con interior completa el rango", twin="VAL06")
p6, t6 = vp([lo("500 millones")]);     case(C, "VAL06", "no más de 200 millones", p6, t6, "previo inferior; nueva superior SIN interior: sustituye entera (gemelo)", twin="VAL05")
p, t = vp([lo("300 millones")]);       case(C, "VAL07", "hasta 300 millones", p, t, "previo inferior; nueva superior igual a la inferior (un solo punto)")
p, t = vp([up("200 millones")]);       case(C, "VAL08", "máximo 150 millones", p, t, "previo superior; nueva superior reemplaza la superior")
case(C, "VAL09", "mayores de 50 millones", p, t, "previo superior; nueva inferior con interior completa el rango", twin="VAL10")
case(C, "VAL10", "más de 400 millones", p, t, "previo superior; nueva inferior SIN interior: sustituye entera (gemelo)", twin="VAL09")
case(C, "VAL11", "desde 200 millones", p, t, "previo superior; nueva inferior igual (un solo punto)")
p, t = vp([lo("100 millones"), up("300 millones")])
case(C, "VAL12", "mínimo 150 millones", p, t, "previo ambos; nueva inferior con interior reemplaza la inferior")
case(C, "VAL13", "hasta 250 millones", p, t, "previo ambos; nueva superior con interior reemplaza la superior")
case(C, "VAL14", "entre 400 y 600 millones", p, t, "previo ambos; rango nuevo disjunto reemplaza ambas")
case(C, "VAL15", "de más de 350 millones", p, t, "previo ambos; nueva inferior sin interior (supera la superior)", twin="VAL12")
case(C, "VAL16", "cambia el valor a menos de 90 millones", p, t, "instrucción explícita de cambio: sustituye la restricción")

# ============================================================ 9 HERENCIA DE UNIDAD (8)
C = "Herencia de unidad"
hb = [TP("estudios y diseños"), B("Alcaldía de Medellín")]
def hp(vals, txt, gap=50): return PRIOR(IR(hb + vals, "CONTRACTS"), gap=gap), "contratos de estudios y diseños de la Alcaldía de Medellín " + txt
p, t = hp([lo("200 millones")], "de al menos 200 millones")
case(C, "HER01", "mejor mínimo 350", p, t, "unidad única previa: hereda millones", twin="HER08")
p, t = hp([lo("200 millones"), VAL("<=", "5 mil millones", "MIL_MILLONES")], "entre 200 millones y 5 mil millones")
case(C, "HER02", "mínimo 400", p, t, "previo con dos unidades activas: debe aclarar")
p, t = hp([VAL(">=", "500 SMMLV", "SMMLV")], "de al menos 500 SMMLV")
case(C, "HER03", "mínimo 800", p, t, "previo en SMMLV (no ejecutable hoy)")
p, t = hp([VAL(">=", "40.000.000 pesos", "PESOS")], "de al menos 40.000.000 pesos")
case(C, "HER04", "ahora mínimo 650", p, t, "previo en pesos sin escala")
p, t = hp([up("200 millones")], "de hasta 200 millones")
case(C, "HER05", "más de 900 mil pesos", p, t, "cifra bajo el piso con unidad propia")
p, t = hp([lo("300 millones")], "de al menos 300 millones")
case(C, "HER06", "entre 450 y 900", p, t, "rango sin unidad: dos herencias en la misma frase")
p, t = hp([], "")
case(C, "HER07", "entre 200 y 500 millones", p, t, "unidad dicha una vez cubre las dos cotas", twin="HER06")
p, t = hp([lo("200 millones")], "de al menos 200 millones")
case(C, "HER08", "mejor mínimo 2 mil millones", p, t, "unidad propia distinta a la del previo (gemelo de HER01)", twin="HER01")

# ============================================================ 10 CLEAR (8)
C = "CLEAR distinto de VALUE"
pr, pt = P("contratos de aseo de la Alcaldía de Montería en 2024", [TP("aseo"), B("Alcaldía de Montería"), T_year(2024)], gap=40)
case(C, "CLR01", "sin importar el año", pr, pt, "CLEAR TIME con otras condiciones que quedan")
pr, pt = P("contratos de dotación hospitalaria en el Meta en 2025", [TP("dotación hospitalaria"), GD("Meta"), T_year(2025)], gap=36)
case(C, "CLR02", "no importa el departamento", pr, pt, "CLEAR GEOGRAPHY")
pr, pt = P("procesos de mantenimiento de la Alcaldía de Tunja abiertos", [TP("mantenimiento"), B("Alcaldía de Tunja"), ST("open", "family")], univ="PROCESSES", gap=44)
case(C, "CLR03", "da igual el estado", pr, pt, "CLEAR STATE")
pr, pt = P("contratos de vigilancia de la Alcaldía de Pereira en 2024", [TP("vigilancia"), B("Alcaldía de Pereira"), T_year(2024)], gap=29)
case(C, "CLR04", "de cualquier entidad", pr, pt, "CLEAR BUYER con otras condiciones que quedan")
pr, pt = P("contratos de software de la Universidad Nacional en 2025", [TP("software"), B("Universidad Nacional de Colombia"), T_year(2025)], gap=51)
case(C, "CLR05", "quita el filtro de modalidad", pr, pt, "quitar una condición que no existe")
pr, pt = P("contratos en 2024", [T_year(2024)], gap=20)
case(C, "CLR06", "mejor sin año", pr, pt, "quitar la única condición (queda búsqueda sin filtros)", twin="CLR01")
pr, pt = P("contratos de obra de la Gobernación de Bolívar en Cartagena en 2024", [TP("obra"), B("Gobernación de Bolívar"), GM("Cartagena"), T_year(2024)], gap=60)
case(C, "CLR07", "olvida el tema", pr, pt, "CLEAR TOPIC")
pr, pt = P("contratos de la Gobernación de Boyacá", [B("Gobernación de Boyacá")], gap=33)
case(C, "CLR08", "ya no importa el comprador", pr, pt, "quitar el único filtro (BUYER)", twin="CLR04")

# ============================================================ 11 ORDEN (6)
C = "Orden"
pr, pt = P("contratos de fotocopiado de la Alcaldía de Armenia", [TP("fotocopiado"), B("Alcaldía de Armenia")], gap=31)
case(C, "ORD01", "ordénalos del más barato al más caro", pr, pt, "orden ascendente por valor")
pr, pt = P("contratos de asfaltado de la Gobernación de Caldas en 2024", [TP("asfaltado"), B("Gobernación de Caldas"), T_year(2024)], gap=47)
case(C, "ORD02", "los más antiguos primero", pr, pt, "orden ascendente por fecha")
pr, pt = P("contratos de reparación de colegios de la Alcaldía de Bogotá, ordenados por valor", [TP("reparación de colegios"), B("Alcaldía de Bogotá")], order="value_desc", gap=39)
case(C, "ORD03", "mejor por fecha, los más recientes primero", pr, pt, "orden nuevo sobre orden previo")
pr, pt = P("contratos de la Alcaldía de Manizales en 2025", [B("Alcaldía de Manizales"), T_year(2025)], gap=26)
case(C, "ORD04", "solo los que mencionen ambulancia, del más caro al más barato", pr, pt, "orden + filtro por token", twin="ORD05")
case(C, "ORD05", "solo los que mencionen ambulancia", pr, pt, "filtro por token sin orden (gemelo)", twin="ORD04")
pr, pt = P("contratos de insumos de oficina de la Universidad del Valle, los más recientes primero", [TP("insumos de oficina"), B("Universidad del Valle")], order="date_desc", gap=53)
case(C, "ORD06", "de menor a mayor valor", pr, pt, "orden por valor ascendente sobre orden por fecha")

# ============================================================ 12 BUSQUEDA NUEVA CON HISTORIA (12)
C = "Búsqueda nueva con historia"
pr, pt = P("contratos de aseo de la Gobernación de Nariño en 2025", [TP("aseo"), B("Gobernación de Nariño"), T_year(2025)], gap=58)
case(C, "NEW01", "y para la región Caribe?", pr, pt, "fragmento de GEOGRAPHY (región)", twin="NEW02")
case(C, "NEW02", "contratos de mantenimiento de ascensores en la región Caribe", pr, pt, "consulta completa con GEOGRAPHY (gemelo)", twin="NEW01")
pr, pt = P("contratos de vigilancia de la Alcaldía de Tunja en 2025", [TP("vigilancia"), B("Alcaldía de Tunja"), T_year(2025)], gap=62)
case(C, "NEW03", "y en 2023?", pr, pt, "fragmento de TIME", twin="NEW04")
case(C, "NEW04", "alquiler de maquinaria pesada en 2023", pr, pt, "consulta completa con TIME (gemelo)", twin="NEW03")
pr, pt = P("contratos de transporte de la Gobernación de Antioquia", [TP("transporte"), B("Gobernación de Antioquia")], gap=37)
case(C, "NEW05", "y la Gobernación del Cauca?", pr, pt, "fragmento de BUYER", twin="NEW06")
case(C, "NEW06", "contratos de transporte escolar de la Gobernación del Cauca", pr, pt, "consulta completa con BUYER (gemelo)", twin="NEW05")
pr, pt = P("contratos de suministro de alimentos del ICBF en Chocó", [TP("suministro de alimentos"), B("ICBF"), GD("Chocó")], gap=49)
case(C, "NEW07", "los que pasen de 50 millones", pr, pt, "fragmento de VALUE", twin="NEW08")
case(C, "NEW08", "contratos de dotación de ambulancias de más de 50 millones", pr, pt, "consulta completa con VALUE (gemelo)", twin="NEW07")
pr, pt = P("contratos de aseo de la Alcaldía de Girardot", [TP("aseo"), B("Alcaldía de Girardot")], gap=43)
case(C, "NEW09", "y los procesos?", pr, pt, "fragmento con sustantivo de universo y sin contenido")
pr9, pt9 = P("procesos de aseo de la Alcaldía de Girardot", [TP("aseo"), B("Alcaldía de Girardot")], univ="PROCESSES", gap=43)
case(C, "NEW10", "y las licitaciones?", pr9, pt9, "fragmento con «licitaciones» coloquial (no es modalidad) y sin contenido")
pr, pt = P("contratos de papelería de la Alcaldía de Zipaquirá", [TP("papelería"), B("Alcaldía de Zipaquirá")], gap=41)
case(C, "NEW11", "vigilancia", pr, pt, "tema solo tras búsqueda con comprador", twin="NEW12")
case(C, "NEW12", "contratos de vigilancia", pr, pt, "consulta completa con tema (gemelo)", twin="NEW11")

# ============================================================ 13 ANCLA INVALIDA (10)
C = "Ancla inválida"
fr_i = [TP("aseo"), B("Alcaldía de Ibagué"), T_year(2025)]
pr = PRIOR(IR(fr_i), gap=40, state="NOT_EXECUTED"); pt = "contratos de aseo de la Alcaldía de Ibagué en 2025 (el turno falló por tiempo de espera y no se ejecutó)"
case(C, "INV01", "y en 2024?", pr, pt, "ancla no ejecutada (timeout)")
pr = PRIOR(IR([TP("vigilancia"), B("Gobernación de Sucre")]), gap=25, state="NOT_EXECUTED"); pt = "contratos de vigilancia de la Gobernación de Sucre (rechazado antes de ejecutar)"
case(C, "INV02", "los que superen los 100 millones", pr, pt, "ancla no ejecutada (bloqueada)")
pr = PRIOR(IR([], None, pv="CURRENT_OPPORTUNITIES"), gap=None, owner="NONE", state="NONE"); pt = "(conversación nueva: sin búsqueda previa)"
case(C, "INV03", "y en 2024?", pr, pt, "ancla vacía (sin búsqueda previa)")
pr = PRIOR(IR(fr_i), gap=1860, state="EXPIRED"); pt = "contratos de aseo de la Alcaldía de Ibagué en 2025 (hace 31 min)"
case(C, "INV04", "y en 2024?", pr, pt, "ancla vencida a 31 minutos (F11)", twin="INV07")
pr = PRIOR(IR([TP("dotación"), B("Gobernación del Valle del Cauca"), GM("Cali")]), gap=18000, state="EXPIRED"); pt = "contratos de dotación de la Gobernación del Valle en Cali (hace 5 h)"
case(C, "INV05", "solo los de Palmira", pr, pt, "ancla vencida a 5 horas")
pr = PRIOR(IR([TP("papelería"), B("SENA")], order="value_desc"), gap=46800, state="EXPIRED"); pt = "contratos de papelería del SENA por valor (hace 13 h)"
case(C, "INV06", "ahora ordénalos por fecha", pr, pt, "ancla vencida a 13 horas")
pr = PRIOR(IR(fr_i), gap=1740, state="EXECUTED"); pt = "contratos de aseo de la Alcaldía de Ibagué en 2025 (hace 29 min)"
case(C, "INV07", "y en 2024?", pr, pt, "ancla vigente a 29 minutos (gemelo de INV04)", twin="INV04")
pr = PRIOR(None, gap=70, state="EXECUTED"); pt = "(ancla guardada pero ilegible)"
case(C, "INV08", "y en Bogotá?", pr, pt, "ancla ilegible (IR no decodificable)")
pr = PRIOR(IR([B("Alcaldía de Montería"), T_year(2025)], None, operation="COUNT", pv=None), gap=35, owner="LEGACY:COUNT"); pr["ir"]["product_view"] = None; pr["ir"]["universe"] = "CONTRACTS"; pr["ir"]["view_source"] = None
pt = "cuántos contratos tiene la Alcaldía de Montería en 2025 (respondido por el motor de conteo)"
case(C, "INV09", "y en 2024?", pr, pt, "ancla de dueño legacy (conteo)")
pr = PRIOR(IR([B("Alcaldía de Pereira")], "CONTRACTS", operation="RANK_SUPPLIERS"), gap=44, owner="LEGACY:RANK_SUPPLIERS"); pr["ir"]["view_source"] = None
pt = "principales proveedores de la Alcaldía de Pereira (motor de proveedores)"
case(C, "INV10", "ahora los de 2024", pr, pt, "ancla de dueño legacy (proveedores)")

# ============================================================ 14 PROTOCOLO (10)
C = "Protocolo sobre resultado Foundation"
pr, pt = P("contratos de mantenimiento vial de la Gobernación del Huila (primera página)", [TP("mantenimiento vial"), B("Gobernación del Huila")], gap=22)
case(C, "PRO01", "muéstrame los siguientes", pr, pt, "página tras búsqueda")
pr, pt = P("contratos de mantenimiento vial de la Gobernación del Huila en 2025 (resultado de un delta, página 1 ya vista)", [TP("mantenimiento vial"), B("Gobernación del Huila"), T_year(2025)], gap=19, chain=["PROTOCOL:page"])
case(C, "PRO02", "página siguiente", pr, pt, "página tras delta y tras una página previa")
pr, pt = P("contratos de dotación de la Alcaldía de Ibagué por valor (resultado tras cambio de orden)", [TP("dotación"), B("Alcaldía de Ibagué")], order="value_desc", gap=24)
case(C, "PRO03", "dame otros diez más", pr, pt, "página tras cambio de orden")
pr, pt = P("contratos y procesos de aseo de la Alcaldía de Soacha (respuesta dual)", [TP("aseo"), B("Alcaldía de Soacha")], univ="DUAL", gap=27)
case(C, "PRO04", "sigue mostrando más", pr, pt, "página sobre ancla dual")
pr, pt = P("contratos de vigilancia de la Gobernación de Cundinamarca en 2025", [TP("vigilancia"), B("Gobernación de Cundinamarca"), T_year(2025)], gap=16)
case(C, "PRO05", "Mayor valor", pr, pt, "botón de orden «Mayor valor»", twin="PRO06")
case(C, "PRO06", "Más recientes", pr, pt, "botón de orden «Más recientes» (gemelo)", twin="PRO05")
pr, pt = P("contratos de papelería de la Alcaldía de Soledad en 2025", [TP("papelería"), B("Alcaldía de Soledad"), T_year(2025)], gap=21)
case(C, "PRO07", "Quitar: Alcaldía de Soledad", pr, pt, "botón de quitar filtro de comprador")
case(C, "PRO08", "Quitar filtro de 2025", pr, pt, "botón de quitar filtro de periodo", twin="PRO07")
case(C, "PRO09", "guárdame esta búsqueda en el radar", pr, pt, "mensaje de guardar en radar")
case(C, "PRO10", "avísame cuando salgan procesos nuevos así", pr, pt, "mensaje de alerta (radar), con universo distinto", twin="PRO09")

# ============================================================ 15 DOS O MAS DIMENSIONES (8)
C = "Dos o más dimensiones"
pr, pt = P("contratos de vigilancia de la Alcaldía de Bello", [TP("vigilancia"), B("Alcaldía de Bello")], gap=45)
case(C, "MUL01", "y en Cali, solo 2024", pr, pt, "SET GEOGRAPHY + SET TIME")
pr, pt = P("contratos de aseo de la Alcaldía de Pereira en 2024", [TP("aseo"), B("Alcaldía de Pereira"), T_year(2024)], gap=34)
case(C, "MUL02", "ahora la Gobernación de Risaralda, sin importar el año", pr, pt, "SET BUYER + CLEAR TIME")
pr, pt = P("contratos de mantenimiento de la Gobernación de Santander", [TP("mantenimiento"), B("Gobernación de Santander")], gap=53)
case(C, "MUL03", "con más de 100 millones y de 2023", pr, pt, "SET VALUE + SET TIME")
pr, pt = P("contratos de consultoría en Antioquia", [TP("consultoría"), GD("Antioquia")], gap=28)
case(C, "MUL04", "olvida el lugar, mejor los de la Universidad de Antioquia", pr, pt, "CLEAR GEOGRAPHY + SET BUYER")
pr, pt = P("procesos de dotación escolar en 2025", [TP("dotación escolar"), T_year(2025)], univ="PROCESSES", gap=36)
case(C, "MUL05", "solo los adjudicados, del Magdalena Medio", pr, pt, "SET STATE + región fuera de la lista cerrada (debe aclarar)")
pr, pt = P("procesos de obras de la Gobernación del Meta", [TP("obras"), B("Gobernación del Meta")], univ="PROCESSES", gap=42)
case(C, "MUL06", "de mínima cuantía y los más recientes", pr, pt, "SET MODALITY + SET_ORDER")
pr, pt = P("contratos de suministro de combustible de la Gobernación de Boyacá", [TP("suministro de combustible"), B("Gobernación de Boyacá")], gap=39)
case(C, "MUL07", "los 5 más caros de este año", pr, pt, "ORDER + top-N + TIME relativo")
pr, pt = P("contratos de aseo en el Atlántico", [TP("aseo"), GD("Atlántico")], gap=47)
case(C, "MUL08", "sin los de Soledad y en 2024", pr, pt, "negación de lugar + SET TIME")

# ---------------------------------------------------------------- emit
def sha(s): return hashlib.sha256(s.encode("utf-8")).hexdigest()
def main():
    ids = {}
    for i, c in enumerate(CASES, 1):
        c["id"] = f"SF{i:03d}"; ids[c["key"]] = c["id"]
    assert len(CASES) == 162, len(CASES)
    assert len({c["key"] for c in CASES}) == 162
    with open(os.path.join(OUT, "followup_synthetic_cases.jsonl"), "w", encoding="utf-8") as f, \
         open(os.path.join(OUT, "AUTHOR_INTENT.jsonl"), "w", encoding="utf-8") as g:
        for c in CASES:
            rec = {"gold_id": c["id"], "slice": "SYNTHETIC_FOLLOWUP", "followup_text": c["text"],
                   "followup_sha256": sha(c["text"]), "source_kind": "SYNTHETIC", "prior": c["prior"]}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            g.write(json.dumps({"gold_id": c["id"], "key": c["key"], "class": c["cls"], "feature_exercised": c["trait"],
                                "twin_id": ids.get(c["twin"]) if c["twin"] else None,
                                "prior_as_natural_text": c["ptext"], "note": c["note"]}, ensure_ascii=False) + "\n")
    from collections import Counter
    cnt = Counter(c["cls"] for c in CASES)
    json.dump(cnt, open(os.path.join(OUT, "author_tools", "class_counts.json"), "w"), ensure_ascii=False, indent=1)
    print(cnt, sum(cnt.values()))
main()
