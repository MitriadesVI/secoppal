#!/usr/bin/env python3
"""v2_1 -> v2_2: conforma la FORMA de los campos de entrada al esquema m2a-followup-gold/2.1. No escribe etiquetas."""
import json, os, copy, hashlib
HERE = os.path.dirname(os.path.abspath(__file__)); OUT = os.path.dirname(HERE); SRC = os.path.join(os.path.dirname(OUT), "v2_1")
rs = [json.loads(l) for l in open(os.path.join(SRC, "followup_synthetic_cases.jsonl"), encoding="utf-8")]
intent = [json.loads(l) for l in open(os.path.join(SRC, "AUTHOR_INTENT.jsonl"), encoding="utf-8")]
orig = {r["gold_id"]: copy.deepcopy(r) for r in rs}

TIME_HINT = {"2023 a 2025": "2023-01-01..2025-12-31", "2022 a 2024": "2022-01-01..2024-12-31",
             "enero a junio de 2024": "2024-01-01..2024-06-30", "febrero a abril de 2025": "2025-02-01..2025-04-30",
             "últimos 3 meses": "últimos 3 meses (2026-07-01..2026-10-01)", "últimos 6 meses": "últimos 6 meses (2026-04-01..2026-10-01)"}
EVENT_IDS = {"SF058", "SF059", "SF060", "SF149", "SF150", "SF151", "SF152"}
INSTALL = {}   # gold_id -> list of field-change tags
def tag(g, t):
    INSTALL.setdefault(g, [])
    if t not in INSTALL[g]: INSTALL[g].append(t)

def conv_ir(g, ir):
    if ir is None: return None
    n = {"operation": ir["operation"], "aspect": "TOTAL" if ir["operation"] == "COUNT" else None, "universe": ir["universe"], "view": ir["view"],
         "product_view": ir["product_view"], "view_source": ir["view_source"], "conditions": [], "logic": None,
         "order": None, "cardinality": None, "host_notice": []}
    if ir["operation"] == "COUNT": tag(g, "ir.aspect")
    tag(g, "ir.logic/host_notice/aspect (claves nuevas)")
    if ir["universe"] == "DUAL":
        n["universe"] = "BOTH"; tag(g, "ir.universe DUAL->BOTH (y se retira ir.universes)")
    for c in ir["conditions"]:
        c = dict(c)
        if c["dimension"] == "TIME" and c["value_hint"] in TIME_HINT:
            c["value_hint"] = TIME_HINT[c["value_hint"]]; tag(g, "ir.conditions.value_hint TIME completo (fechas absolutas)")
        n["conditions"].append(c)
    if ir["order"]:
        n["order"] = {"operator": ir["order"]["operator"], "direction_source": "STATED"}; tag(g, "ir.order {operator,direction_source}")
    if ir["cardinality"]:
        k = int(ir["cardinality"]["value_hint"])
        n["cardinality"] = {"operator": "top_n", "n": k, "n_requested": k}; tag(g, "ir.cardinality {n,n_requested}")
    return n

OPT = {
 "SF058": ("UNRESOLVED_VALUE:BUYER", "San Pedro", ["BUYER"], "San Pedro",
           [("BUYER", "Alcaldía de San Pedro (Valle del Cauca)"), ("BUYER", "Alcaldía de San Pedro de Urabá (Antioquia)"), ("BUYER", "Alcaldía de San Pedro (Sucre)")]),
 "SF059": ("BUYER_OR_PLACE_AFTER_ACTOR", "Soledad", ["BUYER", "GEOGRAPHY"], "Soledad", [("BUYER", "Alcaldía de Soledad (Atlántico)"), ("GEOGRAPHY", "Soledad (Atlántico)")]),
 "SF061": ("UNRESOLVED_VALUE:BUYER", "Santa Rosa", ["BUYER"], "Santa Rosa",
           [("BUYER", "Alcaldía de Santa Rosa de Cabal (Risaralda)"), ("BUYER", "Alcaldía de Santa Rosa de Osos (Antioquia)"), ("BUYER", "Alcaldía de Santa Rosa (Cauca)")]),
}
OPT["SF060"] = OPT["SF059"]; OPT["SF062"] = OPT["SF061"]
def conv_pending(g, r):
    old = r["prior"]["pending"]
    if g in OPT:
        reason, span, slots, val, opts = OPT[g]
        return {"reason": reason, "span": span, "candidate_slots": slots, "candidate_value": val,
                "options": [{"option_id": f"o{i+1}", "slot": s, "value_hint": v} for i, (s, v) in enumerate(opts)], "asks": 1}
    assert old["dimension"] == "TIME"
    asks = 1 + r["prior"]["chain"].count("PROTOCOL:reask")
    return {"reason": "UNRESOLVED_VALUE:TIME", "span": old["span"], "candidate_slots": ["TIME"],
            "candidate_value": "primer semestre (enero a junio), año sin decir", "options": [], "asks": asks}

def reid(ir):
    for i, c in enumerate(ir["conditions"]): c["id"] = f"c{i+1}"

out = []
for r in rs:
    g = r["gold_id"]; p = r["prior"]
    if g == "SF103":     # reescritura autorizada: previo en SMMLV ya no es un estado alcanzable (F14)
        r["followup_text"] = "mejor mínimo 350"
        r["followup_sha256"] = hashlib.sha256(r["followup_text"].encode()).hexdigest()
        for c in p["ir"]["conditions"]:
            if c["dimension"] == "VALUE":
                c.update(operator="less_or_equal", value_hint="300 millones", unit="MILLONES")
        tag(g, "followup_text + sha (reescrito)"); tag(g, "prior.ir VALUE (SMMLV>=500 -> <=300 millones)")
    # state EXPIRED
    if p["state"] == "EXPIRED":
        p["state"] = "PENDING" if g == "SF064" else "EXECUTED"; tag(g, "prior.state EXPIRED->" + p["state"])
    if p["state"] == "NONE":
        p["ir"] = None; tag(g, "prior.ir [] -> null (state NONE)")
    elif p["state"] == "EXECUTED" and p["ir"] is None:
        p["state"] = "UNREADABLE"; tag(g, "prior.state EXECUTED -> UNREADABLE")
    if g in ("SF058", "SF061", "SF062"):
        p["ir"]["conditions"] = [c for c in p["ir"]["conditions"] if c["dimension"] != "BUYER"]; reid(p["ir"])
        tag(g, "prior.ir sin la condición BUYER ambigua (span abierto fuera de la IR)")
    p["ir"] = conv_ir(g, p["ir"])
    if p["pending"] is not None:
        old = p["pending"]; p["pending"] = conv_pending(g, r); tag(g, "prior.pending (forma 2.1: reason/span/candidate_slots/candidate_value/options/asks)")
    if p["chain"] == ["PROTOCOL:option"] and p["ir"]["view_source"] == "USER" and p["ir"]["universe"] == "UNSPECIFIED":
        p["chain"] = ["PROTOCOL:view"]; tag(g, "prior.chain option->view")
    p["owner"] = p["owner"]
    r2 = {"schema": "m2a-followup-gold/2.1"}
    for k in ("gold_id", "slice", "followup_text", "followup_sha256", "source_kind"): r2[k] = r[k]
    r2["input_kind"] = "EVENT" if g in EVENT_IDS else "TEXT"; tag(g, "input_kind=" + r2["input_kind"]); tag(g, "schema")
    r2["prior"] = {k: p[k] for k in ("gold_id", "selection_rule", "chain", "gap_seconds", "owner", "state", "ir", "pending")}
    out.append(r2)

with open(os.path.join(OUT, "followup_synthetic_cases.jsonl"), "w", encoding="utf-8") as f:
    for r in out: f.write(json.dumps(r, ensure_ascii=False) + "\n")
json.dump({"changes": INSTALL}, open(os.path.join(HERE, "changes_by_id.json"), "w"), ensure_ascii=False, indent=1)
# intent
FLAG = json.load(open(os.path.join(HERE, "flags.json")))
with open(os.path.join(OUT, "AUTHOR_INTENT.jsonl"), "w", encoding="utf-8") as f:
    for it in intent:
        g = it["gold_id"]
        if g == "SF103":
            it["feature_exercised"] = "unidad heredada (única, millones) deja el rango sin interior: F26 (no F2); previo con sólo cota superior"
            it["prior_as_natural_text"] = "contratos de estudios y diseños de la Alcaldía de Medellín de hasta 300 millones"
            it["twin_id"] = "SF101"; it["key"] = "HER03"
            it["note"] = "v2.2: reescrito; antes previo en SMMLV (no alcanzable bajo F14)"
        if g in FLAG: it["v2_2_flag"] = FLAG[g]
        f.write(json.dumps(it, ensure_ascii=False) + "\n")
print(len(out))
