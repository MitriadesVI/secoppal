#!/usr/bin/env python3
"""Validador de FORMA de los campos de entrada contra m2a-followup-gold/2.1 (enums cerrados). No valida etiquetas."""
import json, os, re, hashlib, sys
OUT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
E = dict(
 slice={"REAL_FOLLOWUP", "SYNTHETIC_FOLLOWUP"}, source_kind={"ORGANIC", "SCRIPTED", "SYNTHETIC"}, input_kind={"TEXT", "EVENT"},
 sel={"IMMEDIATE_SUBSTANTIVE", "DOMINANT_IN_REPLAY", "CHAIN_LABELED", "SYNTHETIC_AUTHORED"},
 chain={"PROTOCOL:option", "PROTOCOL:page", "PROTOCOL:view", "PROTOCOL:reask"},
 owner={"FOUNDATION_SEARCH", "LEGACY:PROFILE_ACTOR", "LEGACY:PROFILE_BUYER", "LEGACY:COUNT", "LEGACY:SUM_VALUE", "LEGACY:GROUP_BY", "LEGACY:RANK_SUPPLIERS", "LEGACY:FOLLOW", "NONE"},
 state={"EXECUTED", "EXECUTED_EMPTY", "NOT_EXECUTED", "PENDING", "UNREADABLE", "NONE"},
 universe={"CONTRACTS", "PROCESSES", "BOTH", "UNSPECIFIED"}, view={"RECORDS", "OPPORTUNITIES"},
 pv={"HISTORICAL_PROCESSES", "CURRENT_OPPORTUNITIES", "CONTRACTS"}, vs={"USER", "DERIVED", None},
 op={"SEARCH", "COUNT", "SUM_VALUE", "GROUP_BY", "RANK_SUPPLIERS", "PROFILE_BUYER", "PROFILE_ACTOR", "FOLLOW", "EXPORT"},
 aspect={"TOTAL", "BY_MODALITY", "BY_YEAR", "BY_BUYER", "BY_SUPPLIER", "BY_STATE", "BY_TYPE"},
 unit={"PESOS", "MILES", "MILLONES", "MIL_MILLONES", "BILLONES", "SMMLV"}, level={"DEPARTMENT", "MUNICIPALITY", "REGION", "UNKNOWN"},
 order={"value_desc", "value_asc", "date_desc", "date_asc"}, dsrc={"STATED", "DEFAULTED"}, pol={"POSITIVE", "NEGATIVE"},
 slot={"TIME", "VALUE.lower", "VALUE.upper", "VALUE", "GEOGRAPHY", "BUYER", "ACTOR", "TOPIC", "STATE", "MODALITY", "CONTRACT_TYPE_OR_NATURE", "ORDER", "CARDINALITY", "UNIVERSE"},
 reason={"CONTEXT_EXPIRED_CONFIRM", "BUYER_OR_PLACE_AFTER_ACTOR", "REGION_NOT_IN_LIST", "SMMLV_UNSUPPORTED", "RELATIVE_TO_PREVIOUS_VALUE", "UNIT_AMBIGUOUS",
         "INHERITED_UNIT_IMPLAUSIBLE", "MONTH_YEAR_AMBIGUOUS", "EMPTY_SEARCH_AFTER_DELTA", "STATE_NOT_IN_UNIVERSE", "TERRITORY_VS_CARRIED_BUYER", "NO_ANCHOR",
         "ANCHOR_UNREADABLE", "TARGET_NOT_ADDRESSABLE", "VALUE_WITHOUT_COMPARATOR"})
OPS = {"TOPIC": {"contains", "whole_token_folded"}, "BUYER": {"is"}, "ACTOR": {"is"}, "GEOGRAPHY": {"is"},
       "TIME": {"during_year", "during_month", "between_dates", "relative_period"},
       "VALUE": {"greater_than", "greater_or_equal", "less_than", "less_or_equal"}, "STATE": {"is", "family"},
       "MODALITY": {"is", "family"}, "CONTRACT_TYPE_OR_NATURE": {"is", "family"}}
TOP = ["schema", "gold_id", "slice", "followup_text", "followup_sha256", "source_kind", "input_kind", "prior"]
PRI = ["gold_id", "selection_rule", "chain", "gap_seconds", "owner", "state", "ir", "pending"]
IRK = ["operation", "aspect", "universe", "view", "product_view", "view_source", "conditions", "logic", "order", "cardinality", "host_notice"]
CK = ["id", "dimension", "operator", "polarity", "value_hint", "level", "unit"]
PK = ["reason", "span", "candidate_slots", "candidate_value", "options", "asks"]
errs = []
def bad(g, m): errs.append({"gold_id": g, "error": m})
def inn(g, name, v, s):
    if v not in s: bad(g, f"{name}={v!r} fuera de la lista cerrada")
def keys(g, name, d, ks):
    if list(d) != ks and set(d) != set(ks): bad(g, f"{name}: claves {sorted(set(d) ^ set(ks))} sobran/faltan")
n = 0
for l in open(os.path.join(OUT, "followup_synthetic_cases.jsonl"), encoding="utf-8"):
    r = json.loads(l); g = r["gold_id"]; n += 1
    keys(g, "registro", r, TOP)
    if r.get("schema") != "m2a-followup-gold/2.1": bad(g, "schema")
    if r["followup_sha256"] != hashlib.sha256(r["followup_text"].encode()).hexdigest(): bad(g, "sha no coincide con el texto")
    inn(g, "slice", r["slice"], E["slice"]); inn(g, "source_kind", r["source_kind"], E["source_kind"]); inn(g, "input_kind", r["input_kind"], E["input_kind"])
    p = r["prior"]; keys(g, "prior", p, PRI)
    inn(g, "selection_rule", p["selection_rule"], E["sel"]); inn(g, "owner", p["owner"], E["owner"]); inn(g, "state", p["state"], E["state"])
    for c in p["chain"]: inn(g, "chain", c, E["chain"])
    if p["gap_seconds"] is None:
        if p["owner"] != "NONE": bad(g, "gap_seconds null sólo con owner NONE")
    elif not isinstance(p["gap_seconds"], int) or p["gap_seconds"] < 0: bad(g, "gap_seconds no entero >=0")
    if p["owner"] == "NONE" and (p["state"] != "NONE" or p["ir"] is not None or p["gap_seconds"] is not None): bad(g, "owner NONE incoherente")
    if p["state"] == "NONE" and p["owner"] != "NONE": bad(g, "state NONE exige owner NONE")
    if (p["ir"] is None) != (p["state"] in ("UNREADABLE", "NONE")): bad(g, "ir null sólo con UNREADABLE o NONE (y viceversa)")
    if (p["pending"] is not None) != (p["state"] == "PENDING"): bad(g, "pending sí y sólo sí state PENDING")
    ir = p["ir"]
    if ir:
        keys(g, "ir", ir, IRK); inn(g, "operation", ir["operation"], E["op"]); inn(g, "universe", ir["universe"], E["universe"])
        inn(g, "view", ir["view"], E["view"]); inn(g, "view_source", ir["view_source"], E["vs"])
        if (ir["universe"] == "UNSPECIFIED") != (ir["product_view"] is not None): bad(g, "product_view sí y sólo sí UNSPECIFIED")
        if ir["product_view"] is not None: inn(g, "product_view", ir["product_view"], E["pv"])
        if ir["universe"] != "UNSPECIFIED" and p["owner"] == "FOUNDATION_SEARCH" and ir["view_source"] != "USER": bad(g, "universo dicho exige view_source USER")
        if ir["universe"] == "UNSPECIFIED" and ir["view_source"] not in ("USER", "DERIVED"): bad(g, "UNSPECIFIED exige view_source")
        if ir["aspect"] is not None:
            inn(g, "aspect", ir["aspect"], E["aspect"])
            if ir["operation"] not in ("COUNT", "SUM_VALUE", "GROUP_BY"): bad(g, "aspect prohibido fuera de COUNT/SUM_VALUE/GROUP_BY")
        if not isinstance(ir["host_notice"], list): bad(g, "host_notice no es lista")
        ids = []
        for c in ir["conditions"]:
            keys(g, "condición", c, CK); ids.append(c["id"])
            if c["dimension"] not in OPS: bad(g, f"dimensión {c['dimension']}"); continue
            inn(g, "operator", c["operator"], OPS[c["dimension"]]); inn(g, "polarity", c["polarity"], E["pol"])
            if (c["dimension"] == "GEOGRAPHY") != (c["level"] is not None): bad(g, "level sí y sólo sí GEOGRAPHY")
            if c["level"] is not None: inn(g, "level", c["level"], E["level"])
            if (c["dimension"] == "VALUE") != (c["unit"] is not None): bad(g, "unit sí y sólo sí VALUE")
            if c["unit"] is not None: inn(g, "unit", c["unit"], E["unit"])
            # A2 (v2_3): un previo EXECUTED de Foundation PUEDE llevar SMMLV; la comprobación F14 se retira
            if c["dimension"] == "TIME" and c["operator"] == "between_dates" and not re.match(r"^\d{4}-\d\d-\d\d\.\.\d{4}-\d\d-\d\d$", c["value_hint"]): bad(g, "between_dates sin AAAA-MM-DD..AAAA-MM-DD")
            if c["dimension"] == "TIME" and c["operator"] == "relative_period" and not re.search(r"\(\d{4}-\d\d-\d\d\.\.\d{4}-\d\d-\d\d\)", c["value_hint"]): bad(g, "relativo sin fechas absolutas")
            if c["dimension"] == "VALUE" and not re.search(r"\d", c["value_hint"]): bad(g, "VALUE sin cifra")
        if len(set(ids)) != len(ids): bad(g, "ids de condición repetidos")
        vs = [c for c in ir["conditions"] if c["dimension"] == "VALUE"]
        o = ir["order"]
        if o is not None:
            if set(o) != {"operator", "direction_source"}: bad(g, "order: claves")
            else: inn(g, "order.operator", o["operator"], E["order"]); inn(g, "direction_source", o["direction_source"], E["dsrc"])
        k = ir["cardinality"]
        if k is not None:
            if set(k) != {"operator", "n", "n_requested"} or k.get("operator") != "top_n": bad(g, "cardinality: forma")
            else:
                if not (isinstance(k["n"], int) and 1 <= k["n"] <= 100): bad(g, "n fuera de 1..100")
                if k["n_requested"] != k["n"] and not (k["n_requested"] > 100 and k["n"] == 100): bad(g, "n_requested distinto de n sin tope")
            if o is None: bad(g, "cardinality exige order")
    pe = p["pending"]
    if pe is not None:
        keys(g, "pending", pe, PK); inn(g, "pending.reason", pe["reason"], E["reason"] | {x for x in [pe["reason"]] if re.match(r"^UNRESOLVED_VALUE:[A-Z_]+$", x)})
        if not 1 <= len(pe["candidate_slots"]) <= 2: bad(g, "candidate_slots debe tener 1 o 2 ranuras")
        for s in pe["candidate_slots"]: inn(g, "candidate_slot", s, E["slot"])
        if not isinstance(pe["asks"], int) or pe["asks"] < 1: bad(g, "asks")
        for i, op in enumerate(pe["options"]):
            if set(op) != {"option_id", "slot", "value_hint"}: bad(g, "option: claves")
            else: inn(g, "option.slot", op["slot"], E["slot"])
        if pe["span"] is not None and ir and any(pe["span"].lower() == c["value_hint"].lower() for c in ir["conditions"]): bad(g, "el span abierto está en prior.ir.conditions")
        if pe["span"] is None and pe["reason"] != "CONTEXT_EXPIRED_CONFIRM": bad(g, "span null sólo con CONTEXT_EXPIRED_CONFIRM")
res = {"schema": "m2a-followup-gold/2.1 (campos de entrada)", "cases": n, "errors": len(errs), "details": errs, "valid": not errs}
json.dump(res, open(os.path.join(OUT, "form_check.json"), "w"), ensure_ascii=False, indent=1)
print(n, "errores:", len(errs)); [print(e) for e in errs[:20]]
