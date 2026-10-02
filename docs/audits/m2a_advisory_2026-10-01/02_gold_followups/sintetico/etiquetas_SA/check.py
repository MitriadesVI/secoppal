import json
B="/private/tmp/claude-501/-Users-rodrigoortiz-Documents-secoppal/771a1a89-ee94-4b92-b2de-eef37cce6788/scratchpad/gold_followups/"
cases={json.loads(l)["gold_id"]:json.loads(l) for l in open(B+"synthetic_for_labelers/cases.jsonl")}
recs=[json.loads(l) for l in open(B+"synth_labeler_SA/labels_v2.jsonl")]
E=dict(
 turn_kind={"DELTA","NEW_SEARCH","ANSWER_TO_CLARIFICATION","PROTOCOL","OTHER_OPERATION","UNCLEAR"},
 m2a_scope={"IN","OUT_ANCHOR_NOT_SEARCH","OUT_OPERATION_CHANGE","OUT_REFERENT_EXTENSIONAL","OUT_RELATIVE_TO_PREVIOUS"},
 op={"SET","CLEAR","KEEP","SET_ORDER","SET_UNIVERSE","ANSWER"},
 slot={"TIME","VALUE.lower","VALUE.upper","VALUE","GEOGRAPHY","BUYER","ACTOR","TOPIC","STATE","MODALITY","CONTRACT_TYPE_OR_NATURE","ORDER","CARDINALITY","UNIVERSE"},
 effects={"VALUE_RANGE_REPLACED","VIEW_REDERIVED","TOPIC_REPLACED","UNIVERSE_CHANGED"},
 disp={"EXECUTE","CLARIFY","REASK","NEW_SEARCH","HANDOFF_LEGACY"},
 echo={"CARRIED","YEAR_INHERITED","UNIT_INHERITED","VIEW_DERIVED","VALUE_RANGE_REPLACED","TOPIC_REPLACED","TOPN_CARRIED","CONTEXT_EXPIRED"},
 amb={"CLEAR","AMBIGUOUS"},conf={"HIGH","MEDIUM","LOW"},
 gaps={"SET","SELECT","PV","OPSWITCH","UNIT","COUNTERPART","MONTHYEAR","VCOMP","UNIVTIME","GEOBUYER","ENADELANTE","TTL","TOPIC","B5RANGE","ACTORTIME","FIRMADOS","TOTALES","YEARRANGE","COMPARATOR_FORM","REGION_VOCAB","SMMLV","UNIVERSE_FRAGMENT"},
 ys={None,"STATED","PREVIOUS_ACCEPTED_TIME","CLOCK_INFERRED"},us={None,"STATED","PREVIOUS_ACCEPTED_VALUE","SIBLING_IN_RANGE"},
 dim={"TIME","VALUE","GEOGRAPHY","BUYER","ACTOR","TOPIC","STATE","MODALITY","CONTRACT_TYPE_OR_NATURE","SOURCE","UNSUPPORTED"},
 unit={None,"MILLONES","MIL_MILLONES","SMMLV","PESOS"},
 opers={"TOPIC":{"contains","whole_token_folded"},"BUYER":{"is"},"ACTOR":{"is"},"GEOGRAPHY":{"is"},"TIME":{"during_year","during_month","between_dates","relative_period"},
        "VALUE":{"greater_than","greater_or_equal","less_than","less_or_equal"},"STATE":{"is","family"},"MODALITY":{"is","family"},"CONTRACT_TYPE_OR_NATURE":{"is","family"}},
 ord={"value_desc","value_asc","date_desc","date_asc"},
 universe={"CONTRACTS","PROCESSES","UNSPECIFIED","DUAL"},
)
errs=[]
def e(g,m): errs.append(f"{g}: {m}")
def chk_ir(g,ir,where):
    if ir is None: return
    if ir["universe"] not in E["universe"]: e(g,f"{where} universe")
    if (ir["universe"]=="DUAL")!=("universes" in ir): e(g,f"{where} DUAL/universes")
    ids=[c["id"] for c in ir["conditions"]]
    if len(set(ids))!=len(ids): e(g,f"{where} dup ids")
    for c in ir["conditions"]:
        if c["dimension"] not in E["dim"]: e(g,f"{where} dim {c['dimension']}")
        if c["operator"] not in E["opers"].get(c["dimension"],{c["operator"]}): e(g,f"{where} operator {c}")
        if c["polarity"] not in("POSITIVE","NEGATIVE"): e(g,f"{where} pol")
        if (c["dimension"]=="GEOGRAPHY")!=(c["level"] is not None): e(g,f"{where} level")
        if c["level"] not in (None,"DEPARTMENT","MUNICIPALITY","REGION","UNKNOWN"): e(g,f"{where} level val")
        if c["unit"] not in E["unit"]: e(g,f"{where} unit")
        if (c["dimension"]=="VALUE")!=(c["unit"] is not None): e(g,f"{where} VALUE unit presence {c}")
    if ir["order"] and ir["order"]["operator"] not in E["ord"]: e(g,f"{where} order op")
    if ir["cardinality"] and not ir["order"]: e(g,f"{where} cardinality without order")
    if ir["universe"]=="UNSPECIFIED" and ir["operation"]=="SEARCH" and not ir["product_view"]: e(g,f"{where} UNSPECIFIED needs product_view")
    if ir["universe"]!="UNSPECIFIED" and ir["product_view"]: e(g,f"{where} product_view with explicit universe")
    vs=[c for c in ir["conditions"] if c["dimension"]=="VALUE" and c["polarity"]=="POSITIVE"]
    lo=[c for c in vs if c["operator"].startswith("greater")]; up=[c for c in vs if c["operator"].startswith("less")]
    if len(lo)>1 or len(up)>1: e(g,f"{where} >1 bound per side")
for r in recs:
    g=r["gold_id"]; c=cases[g]
    for k in ["gold_id","slice","followup_text","followup_sha256","source_kind","prior"]:
        if r[k]!=c[k]: e(g,f"original field altered: {k}")
    for k,v in [("turn_kind","turn_kind"),("m2a_scope","m2a_scope"),("expected_disposition","disp"),("ambiguity","amb"),("confidence","conf")]:
        if r[k] not in E[v]: e(g,f"{k} enum {r[k]}")
    if r["labeler"]!="SA": e(g,"labeler")
    for x in r["effects"]:
        if x not in E["effects"]: e(g,f"effect {x}")
    for x in r["expected_echo"]:
        if x not in E["echo"]: e(g,f"echo {x}")
    for x in r["gap_codes"]:
        if x not in E["gaps"]: e(g,f"gap {x}")
    if r["ambiguity"]=="AMBIGUOUS" and not r["alternatives"]: e(g,"AMBIGUOUS without alternatives")
    if r["expected_disposition"]!="EXECUTE" and not r["disposition_reason"] and r["expected_disposition"]!="NEW_SEARCH": e(g,"missing disposition_reason")
    if r["disposition_reason"] and r["expected_disposition"]=="EXECUTE": e(g,"reason with EXECUTE")
    if r["expected_disposition"]=="CLARIFY" and r["disposition_reason"]=="CONTEXT_EXPIRED_CONFIRM" and "CONTEXT_EXPIRED" not in r["expected_echo"]: e(g,"expired echo")
    p=c["prior"]
    if p["gap_seconds"] is not None and p["gap_seconds"]>1800 and r["disposition_reason"]!="CONTEXT_EXPIRED_CONFIRM": e(g,"F11 gap>1800 without CONTEXT_EXPIRED_CONFIRM")
    if p["owner"].startswith("LEGACY") and (r["m2a_scope"]!="OUT_ANCHOR_NOT_SEARCH" or r["expected_disposition"]!="HANDOFF_LEGACY"): e(g,"rule 7")
    chk_ir(g,r["resulting_ir"],"resulting_ir"); chk_ir(g,p["ir"],"prior.ir")
    ops=r["ops"]
    for o in ops:
        if o["op"] not in E["op"]: e(g,"op enum")
        if o["slot"] not in E["slot"]: e(g,f"slot {o['slot']}")
        if o["op"]!="CLEAR" or True:
            if o["quote"] not in r["followup_text"]: e(g,f"quote not substring: {o['quote']!r}")
        if o["op"]=="CLEAR" and not o["quote"]: e(g,"CLEAR without quote")
        i=o["inherited"]
        if i["year_source"] not in E["ys"] or i["unit_source"] not in E["us"]: e(g,"inherited enum")
        if o["slot"]=="TIME" and o["op"]!="CLEAR" and i["year_source"] is None and r["resulting_ir"] is not None: e(g,"TIME op without year_source")
        if o["slot"].startswith("VALUE") and o["op"]!="CLEAR" and i["unit_source"] is None and r["resulting_ir"] is not None: e(g,"VALUE op without unit_source")
        if o["slot"]=="VALUE": 
            if o["op"]!="CLEAR": e(g,"slot VALUE only with CLEAR (§2.3)")
    # §2.3 range
    vo=[o for o in ops if o["slot"].startswith("VALUE") and o["op"]!="CLEAR"]
    q={}
    for o in vo: q.setdefault(o["quote"],[]).append(o)
    for qt,l in q.items():
        if len(l)==2:
            sl={o["slot"]:o for o in l}
            if set(sl)!={"VALUE.lower","VALUE.upper"} or sl["VALUE.lower"]["operator"]!="greater_or_equal" or sl["VALUE.upper"]["operator"]!="less_or_equal": e(g,"range convention §2.3")
        elif len(l)>2: e(g,"range >2 ops")
        else:
            o=l[0]
            if (o["slot"]=="VALUE.lower")!=o["operator"].startswith("greater"): e(g,"slot/operator side mismatch")
    # F-structure
    if r["turn_kind"]=="NEW_SEARCH" and (ops or r["expected_disposition"]!="NEW_SEARCH"): e(g,"NEW_SEARCH shape")
    if r["m2a_scope"]!="IN" and r["turn_kind"]=="DELTA" and r["m2a_scope"] not in("OUT_RELATIVE_TO_PREVIOUS","OUT_ANCHOR_NOT_SEARCH"): e(g,"delta scope")
    # light reducer consistency
    ir=r["resulting_ir"]
    if ir and r["turn_kind"] in("DELTA","ANSWER_TO_CLARIFICATION") and r["m2a_scope"]=="IN":
        for o in ops:
            if o["op"] in("SET","ANSWER") and not o["slot"].startswith("VALUE") and o["slot"] not in("CARDINALITY",):
                dim=o["slot"]
                if not any(cd["dimension"]==dim and cd["value_hint"]==o["value_hint"] or (cd["dimension"]==dim and cd["polarity"]==o["polarity"] and cd["operator"]==o["operator"]) for cd in ir["conditions"]): e(g,f"reducer: SET {dim} {o['value_hint']} not in resulting_ir")
            if o["op"]=="CLEAR" and o["slot"] in E["dim"] and any(cd["dimension"]==o["slot"] for cd in ir["conditions"]): e(g,f"reducer: CLEAR {o['slot']} still present")
            if o["op"]=="CLEAR" and o["slot"]=="ORDER" and ir["order"]: e(g,"reducer CLEAR ORDER")
            if o["op"]=="CLEAR" and o["slot"]=="CARDINALITY" and ir["cardinality"]: e(g,"reducer CLEAR CARD")
        # untouched dims preserved
        cleared={o["slot"] for o in ops if o["op"]=="CLEAR"}
        touched={o["slot"].split(".")[0] for o in ops}
        pi=c["prior"]["ir"]
        for cd in pi["conditions"]:
            if cd["dimension"] not in touched and not any(cd2["dimension"]==cd["dimension"] and cd2["value_hint"]==cd["value_hint"] for cd2 in ir["conditions"]):
                e(g,f"reducer: untouched prior {cd['dimension']} lost")
        if "ORDER" not in touched and ir["order"]!=pi["order"] and "CARDINALITY" not in cleared: e(g,"reducer order changed without op")
        if "CARDINALITY" not in touched and ir["cardinality"]!=pi["cardinality"]: e(g,"reducer cardinality changed without op")
        if "UNIVERSE" not in touched and (ir["universe"]!=pi["universe"] or ir["view"]!=pi["view"]): e(g,"reducer universe changed without op")
res={"total":len(recs),"ids_sorted":[r["gold_id"] for r in recs]==sorted(cases),"errors":errs,"ok":not errs}
json.dump(res,open(B+"synth_labeler_SA/self_check.json","w"),ensure_ascii=False,indent=1)
print(len(recs),len(errs)); print("\n".join(errs[:80]))
