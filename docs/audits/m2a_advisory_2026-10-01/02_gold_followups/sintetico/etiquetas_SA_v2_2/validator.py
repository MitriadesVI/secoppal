import json
G="/Users/rodrigoortiz/Documents/secoppal-asesoria-2026-10-01/para_git/docs/audits/m2a_advisory_2026-10-01/02_gold_followups/"
OUT=G+"sintetico/etiquetas_SA_v2_2/"
cases={json.loads(l)["gold_id"]:json.loads(l) for l in open(G+"sintetico_para_etiquetadores_v2_3/cases.jsonl")}
recs=[json.loads(l) for l in open(OUT+"labels_v2_2.jsonl")]
S=lambda *a:set(a)
E=dict(
 turn_kind=S("DELTA","NEW_SEARCH","ANSWER_TO_CLARIFICATION","PROTOCOL","OTHER_OPERATION","UNCLEAR"),
 scope=S("IN","IN_NEW_SEARCH","IN_PROTOCOL","OUT_NO_ANCHOR","OUT_ANCHOR_NOT_SEARCH","OUT_OPERATION_CHANGE","OUT_REFERENT_EXTENSIONAL","OUT_RELATIVE_TO_PREVIOUS"),
 op=S("SET","CLEAR","KEEP","SET_ORDER","SET_UNIVERSE","ANSWER","CANCEL_PENDING"),
 slot=S("TIME","VALUE.lower","VALUE.upper","VALUE","GEOGRAPHY","BUYER","ACTOR","TOPIC","STATE","MODALITY","CONTRACT_TYPE_OR_NATURE","ORDER","CARDINALITY","UNIVERSE",None),
 disp=S("EXECUTE","NEW_SEARCH","CLARIFY","REASK","INFORM","HANDOFF_LEGACY"),
 amb=S("CLEAR","AMBIGUOUS"),conf=S("HIGH","MEDIUM","LOW"),
 reasons=S("CONTEXT_EXPIRED_CONFIRM","BUYER_OR_PLACE_AFTER_ACTOR","REGION_NOT_IN_LIST","RELATIVE_TO_PREVIOUS_VALUE","UNIT_AMBIGUOUS","MONTH_YEAR_AMBIGUOUS","EMPTY_SEARCH_AFTER_DELTA","STATE_NOT_IN_UNIVERSE","TERRITORY_VS_CARRIED_BUYER","NO_ANCHOR","ANCHOR_UNREADABLE","TARGET_NOT_ADDRESSABLE","VALUE_WITHOUT_COMPARATOR","OPTION_NOT_CHOSEN","ANSWER_EDITS_OTHER_SLOT","PENDING_UNANSWERED","ANSWER_UNRESOLVED","CLEAR_ON_EMPTY_SLOT","REFERENT_EXTENSIONAL","SMMLV_YEAR_AMBIGUOUS"),
 reason_prefix=("UNRESOLVED_VALUE:","OPERATION_CHANGE:","ANCHOR_NOT_SEARCH:"),
 gaps=S("SET","PV","VCOMP","UNIT","MONTHYEAR","YEARRANGE","TTL","TOPIC","COUNTERPART","REGION_VOCAB","SMMLV","POLARITY_SAME_VALUE","POLARITY_OTHER_VALUE","CLEAR_ABSENT","EMPTY_AFTER_DELTA","TOPN_CAP","ORDER_DIRECTION","UNIVERSE_FRAGMENT","PENDING_REJECTED","ANY_OPTION","ANSWER_PLUS_EDIT","STATE_UNIVERSE","NO_ANCHOR","PRIOR_NOT_EXECUTED","GEOBUYER","UNIVTIME","SELECT","ENADELANTE","B5RANGE","COMPARATOR_FORM","NEGATED_COMPARATOR","RELATIVE_ANCHOR","OPPORTUNITY_STATE","PAGE_VS_TOPN","OPSWITCH","TOTALES","FIRMADOS","ACTORTIME","PENDING_OPSWITCH","RANGE_NO_INTERIOR"),
 ys=S(None,"STATED","PREVIOUS_ACCEPTED_TIME","CLOCK_INFERRED","RELATIVE_TO_CLOCK"),us=S(None,"STATED","PREVIOUS_ACCEPTED_VALUE","SIBLING_IN_RANGE"),
 dim=S("TIME","VALUE","GEOGRAPHY","BUYER","ACTOR","TOPIC","STATE","MODALITY","CONTRACT_TYPE_OR_NATURE","SOURCE","UNSUPPORTED"),
 unit=S(None,"PESOS","MILES","MILLONES","MIL_MILLONES","BILLONES","SMMLV"),
 opers={"TOPIC":S("contains","whole_token_folded"),"BUYER":S("is"),"ACTOR":S("is"),"GEOGRAPHY":S("is"),"TIME":S("during_year","during_month","between_dates","relative_period"),
        "VALUE":S("greater_than","greater_or_equal","less_than","less_or_equal"),"STATE":S("is","family"),"MODALITY":S("is","family"),"CONTRACT_TYPE_OR_NATURE":S("is","family")},
 ordop=S("value_desc","value_asc","date_desc","date_asc"),
 universe=S("CONTRACTS","PROCESSES","BOTH","UNSPECIFIED"),
 regions=S("Amazonia","Andina","Orinoquia","Caribe","Pacifica","Insular","Eje Cafetero","Santanderes","Tolima Grande"),
)
IRKEYS=S("operation","aspect","universe","view","product_view","view_source","conditions","logic","order","cardinality","host_notice")
errs=[]
def e(g,m): errs.append(f"{g}: {m}")
def chk_ir(g,ir,w):
    if ir is None: return
    if set(ir)!=IRKEYS: e(g,f"{w} keys {set(ir)^IRKEYS}")
    if ir["universe"] not in E["universe"]: e(g,f"{w} universe")
    if (ir["universe"]=="UNSPECIFIED")!=(ir["product_view"] is not None): e(g,f"{w} product_view iff UNSPECIFIED")
    if ir["operation"]=="SEARCH" and ir["aspect"] is not None: e(g,f"{w} aspect with SEARCH")
    ids=[c["id"] for c in ir["conditions"]]
    if len(set(ids))!=len(ids): e(g,f"{w} dup ids")
    for c in ir["conditions"]:
        if c["dimension"] not in E["dim"]: e(g,f"{w} dim")
        if c["operator"] not in E["opers"].get(c["dimension"],{c["operator"]}): e(g,f"{w} operator {c}")
        if (c["dimension"]=="GEOGRAPHY")!=(c["level"] is not None): e(g,f"{w} level {c}")
        if c["unit"] not in E["unit"] or (c["dimension"]=="VALUE")!=(c["unit"] is not None): e(g,f"{w} unit {c}")
        if c["dimension"]=="GEOGRAPHY" and c["level"]=="REGION" and c["value_hint"] not in E["regions"]: pass
    o=ir["order"]
    if o and (o["operator"] not in E["ordop"] or o["direction_source"] not in("STATED","DEFAULTED")): e(g,f"{w} order")
    ca=ir["cardinality"]
    if ca:
        if not o: e(g,f"{w} cardinality without order")
        if not(1<=ca["n"]<=100) or ca["n"]!=min(ca["n_requested"],100): e(g,f"{w} cardinality n/n_requested (F20)")
    vp=[c for c in ir["conditions"] if c["dimension"]=="VALUE" and c["polarity"]=="POSITIVE"]
    if len([c for c in vp if c["operator"].startswith("greater")])>1 or len([c for c in vp if c["operator"].startswith("less")])>1: e(g,f"{w} >1 bound per side")
    ts=[c for c in ir["conditions"] if c["dimension"]=="TIME" and c["polarity"]=="POSITIVE"]
    for c in ts:
        if c["operator"]=="between_dates" and ".." not in c["value_hint"]: e(g,f"{w} between_dates not ISO range")
        if c["operator"]=="relative_period" and "(" not in c["value_hint"]: e(g,f"{w} relative without absolute dates")
    if any(c["unit"]=="SMMLV" for c in ir["conditions"]):
        # A2: single year needed to execute; flagged later at record level
        pass
def years_of(ir):
    ts=[c for c in ir["conditions"] if c["dimension"]=="TIME" and c["polarity"]=="POSITIVE"]
    if len(ts)!=1: return None
    c=ts[0]
    if c["operator"]=="during_year": return 1
    if c["operator"]=="during_month": return 1
    if c["operator"]=="between_dates":
        a,b=c["value_hint"].split("..")
        return 1 if a[:4]==b[:4] else 2
    return None
for r in recs:
    g=r["gold_id"]; c=cases[g]; p=c["prior"]
    for k in ["schema","gold_id","slice","followup_text","followup_sha256","source_kind","input_kind","prior"]:
        if r[k]!=c[k]: e(g,f"input field altered {k}")
    if r["labeler"]!="SA": e(g,"labeler")
    if r["turn_kind"] not in E["turn_kind"]: e(g,"turn_kind")
    if r["m2a_scope"] not in E["scope"]: e(g,"scope")
    if r["expected_disposition"] not in E["disp"]: e(g,"disp")
    if r["ambiguity"] not in E["amb"] or r["confidence"] not in E["conf"]: e(g,"amb/conf")
    rs=r["disposition_reason"]
    if r["expected_disposition"] in("EXECUTE","NEW_SEARCH"):
        if rs is not None: e(g,"reason with EXECUTE/NEW_SEARCH")
    else:
        if rs is None or not (rs in E["reasons"] or rs.startswith(E["reason_prefix"])): e(g,f"reason not in closed list {rs}")
    for x in r["gap_codes"]:
        if x not in E["gaps"]: e(g,f"gap {x}")
    if r["effects"] or r["expected_echo"]: e(g,"effects/expected_echo are derived: must be empty")
    if r["ambiguity"]=="AMBIGUOUS" and not r["notes"]: e(g,"AMBIGUOUS needs notes")
    chk_ir(g,r["resulting_ir"],"resulting_ir")
    for a in r["alternatives"]: chk_ir(g,a,"alternatives")
    if r["disposition_reason"] in("BUYER_OR_PLACE_AFTER_ACTOR","TERRITORY_VS_CARRIED_BUYER") and (len(r["alternatives"])!=2 or r["resulting_ir"] is not None): e(g,"two-option clarify shape")
    ops=r["ops"]
    for o in ops:
        if o["op"] not in E["op"] or o["slot"] not in E["slot"]: e(g,"op/slot enum")
        if (o["slot"] is None)!=(o["op"]=="CANCEL_PENDING"): e(g,"slot null iff CANCEL_PENDING")
        if o["quote"] is None:
            if r["input_kind"]!="EVENT": e(g,"null quote without EVENT")
        elif o["quote"] not in r["followup_text"]: e(g,f"quote not substring {o['quote']!r}")
        if o["op"]=="CLEAR" and not o["quote"]: e(g,"CLEAR without quote")
        i=o["inherited"]
        if i["year_source"] not in E["ys"] or i["unit_source"] not in E["us"]: e(g,"inherited enum")
        if o["op"]=="KEEP" and (i["year_source"] or i["unit_source"]): e(g,"KEEP inherits (O5)")
        if o["slot"]=="TIME" and o["op"] in("SET","ANSWER") and not o["unresolved"] and i["year_source"] is None: e(g,"TIME op without year_source")
        if o["slot"] in("VALUE.lower","VALUE.upper") and o["op"] in("SET","ANSWER") and not o["unresolved"] and i["unit_source"] is None: e(g,"VALUE op without unit_source")
        if o["slot"]=="TIME" and o["operator"]=="relative_period" and i["year_source"]!="RELATIVE_TO_CLOCK": e(g,"relative_period needs RELATIVE_TO_CLOCK")
        if o["slot"]=="VALUE" and o["op"]!="CLEAR": e(g,"slot VALUE only with CLEAR")
        if o["op"]=="CLEAR" and o["slot"]=="VALUE" and not any(x["op"]=="SET" and x["slot"].startswith("VALUE.") for x in ops) and False: pass
        if o["unresolved"] and r["resulting_ir"] is not None and r["expected_disposition"]!="CLARIFY": e(g,"unresolved but resulting_ir present")
    if r["turn_kind"]=="NEW_SEARCH" and (ops or r["m2a_scope"]!="IN_NEW_SEARCH" or r["expected_disposition"]!="NEW_SEARCH"): e(g,"NEW_SEARCH shape (O13)")
    if r["m2a_scope"]=="IN_NEW_SEARCH" and r["turn_kind"]!="NEW_SEARCH": e(g,"IN_NEW_SEARCH w/o NEW_SEARCH")
    if r["m2a_scope"] in("IN_PROTOCOL",) and r["turn_kind"]=="PROTOCOL" and ops: e(g,"O15 protocol ops must be []")
    if r["m2a_scope"] in("OUT_OPERATION_CHANGE","OUT_REFERENT_EXTENSIONAL") and ops: e(g,"O15 ops must be []")
    # scope precedence
    st=p["state"]
    if st in("NONE","UNREADABLE") and r["turn_kind"]!="NEW_SEARCH":
        if r["m2a_scope"]!="OUT_NO_ANCHOR" or r["expected_disposition"]!="CLARIFY" or r["disposition_reason"] not in("NO_ANCHOR","ANCHOR_UNREADABLE") or r["resulting_ir"] is not None: e(g,"rows 1 / F28")
    elif p["owner"].startswith("LEGACY:"):
        if r["m2a_scope"]!="OUT_ANCHOR_NOT_SEARCH" or r["expected_disposition"]!="HANDOFF_LEGACY" or not (r["disposition_reason"] or "").startswith("ANCHOR_NOT_SEARCH:"): e(g,"row 2")
    elif r["turn_kind"]=="PROTOCOL" or r["input_kind"]=="EVENT":
        if r["m2a_scope"]!="IN_PROTOCOL": e(g,"row 3")
    elif r["m2a_scope"]=="IN" and r["expected_disposition"]=="HANDOFF_LEGACY": e(g,"HANDOFF only for legacy owner")
    if r["m2a_scope"]=="OUT_OPERATION_CHANGE" and not (r["expected_disposition"]=="INFORM" and r["disposition_reason"].startswith("OPERATION_CHANGE:")): e(g,"row 4")
    if r["m2a_scope"]=="OUT_REFERENT_EXTENSIONAL" and not (r["expected_disposition"]=="INFORM" and r["disposition_reason"]=="REFERENT_EXTENSIONAL"): e(g,"row 5")
    if r["m2a_scope"]=="OUT_RELATIVE_TO_PREVIOUS" and not (r["expected_disposition"]=="CLARIFY" and r["disposition_reason"]=="RELATIVE_TO_PREVIOUS_VALUE" and r["resulting_ir"] is None): e(g,"row 6")
    # F11
    gs=p["gap_seconds"]
    if gs is not None and gs>1800 and r["m2a_scope"] in("IN",) and r["disposition_reason"]!="CONTEXT_EXPIRED_CONFIRM": e(g,"F11")
    if r["disposition_reason"]=="CONTEXT_EXPIRED_CONFIRM" and (not(gs and gs>1800) or r["resulting_ir"] is None): e(g,"F11 shape")
    # F29: pending + no answer/cancel/new -> REASK
    if st=="PENDING" and r["m2a_scope"]=="IN" and r["disposition_reason"]!="CONTEXT_EXPIRED_CONFIRM":
        kinds={o["op"] for o in ops}
        if not (kinds&{"ANSWER","CANCEL_PENDING"}) and r["turn_kind"]!="NEW_SEARCH" and r["disposition_reason"]!="PENDING_UNANSWERED": e(g,"F29")
    if r["disposition_reason"]=="ANSWER_EDITS_OTHER_SLOT":
        if not any(o["op"]=="ANSWER" for o in ops) or len([o for o in ops if o["op"]!="ANSWER"])<1: e(g,"F25 shape")
    if st=="PENDING" and "CANCEL_PENDING" in {o["op"] for o in ops} and r["turn_kind"] not in("DELTA","NEW_SEARCH"): e(g,"F23 kind")
    # F19
    ir=r["resulting_ir"]
    if ir and r["expected_disposition"]=="EXECUTE" and not ir["conditions"]: e(g,"F19 empty search executed")
    # F21
    if ir and ir["order"] and ir["order"]["direction_source"]=="DEFAULTED" and ir["order"]["operator"]!="date_desc": e(g,"F21")
    # F10
    if ir and r["expected_disposition"] in("EXECUTE","CLARIFY") and r["m2a_scope"]=="IN" and p["ir"] and p["ir"]["cardinality"] and any(o["op"]=="SET_ORDER" for o in ops) and not any(o["slot"]=="CARDINALITY" for o in ops):
        if not ir["cardinality"] or ir["cardinality"]["n"]!=p["ir"]["cardinality"]["n"]: e(g,"F10")
    # F5 / F6
    for o in ops:
        if o["inherited"]["unit_source"]=="PREVIOUS_ACCEPTED_VALUE" and p["ir"]:
            us={c["unit"] for c in p["ir"]["conditions"] if c["dimension"]=="VALUE"}
            if len(us)!=1: e(g,"F5 inherited with non-single unit")
    # F7
    for o in ops:
        if o["inherited"]["year_source"]=="PREVIOUS_ACCEPTED_TIME" and o["operator"]=="during_month":
            if not p["ir"] or years_of(p["ir"])!=1: e(g,"F7 year inherited without single prior year")
    # A2
    if ir and r["expected_disposition"]=="EXECUTE" and any(c["unit"]=="SMMLV" for c in ir["conditions"]) and years_of(ir)!=1: e(g,"A2 SMMLV executed without single year")
    # F13
    if ir:
        for c in ir["conditions"]:
            if c["level"]=="REGION" and c["value_hint"] not in E["regions"] and r["disposition_reason"]!="REGION_NOT_IN_LIST": e(g,"F13")
    # F16 / F17 (light): negative SET whose value equals the only positive of slot -> positive must be gone
    if ir and p["ir"] and r["expected_disposition"]=="EXECUTE" and r["turn_kind"]!="PROTOCOL":
        for o in ops:
            if o["op"]=="SET" and o["polarity"]=="NEGATIVE":
                dim=o["slot"].split(".")[0]
                pos=[c for c in p["ir"]["conditions"] if c["dimension"]==dim and c["polarity"]=="POSITIVE"]
                same=[c for c in pos if c["value_hint"]==o["value_hint"]]
                if len(pos)==1 and same and any(c["dimension"]==dim and c["polarity"]=="POSITIVE" for c in ir["conditions"]): e(g,"F16 positive not inverted")
        # F1 reducer-lite
        touched={o["slot"].split(".")[0] for o in ops if o["slot"]}
        cancelled="CANCEL_PENDING" in {o["op"] for o in ops}
        for cd in p["ir"]["conditions"]:
            dim=cd["dimension"]
            if dim not in touched and not any(x["dimension"]==dim and x["value_hint"]==cd["value_hint"] and x["polarity"]==cd["polarity"] and x["operator"]==cd["operator"] for x in ir["conditions"]): e(g,f"F1 untouched prior {dim} lost")
        for o in ops:
            if o["op"]=="CLEAR" and o["slot"] in E["dim"] and not any(x["slot"].startswith(o["slot"]+".") for x in ops) and any(x["dimension"]==o["slot"] for x in ir["conditions"]): e(g,f"CLEAR {o['slot']} present")
            if o["op"] in("SET","ANSWER") and o["slot"] not in("VALUE.lower","VALUE.upper","CARDINALITY","ORDER","UNIVERSE") and o["slot"] and not o["unresolved"]:
                if not any(x["dimension"]==o["slot"] and x["polarity"]==o["polarity"] and x["operator"]==o["operator"] for x in ir["conditions"]): e(g,f"SET {o['slot']} not reflected")
        if "ORDER" not in touched and ir["order"]!=p["ir"]["order"] and not any(o["op"]=="CLEAR" and o["slot"]=="CARDINALITY" for o in ops): e(g,"order changed without op")
        if "CARDINALITY" not in touched and ir["cardinality"]!=p["ir"]["cardinality"] and not any(o["op"]=="CLEAR" and o["slot"]=="ORDER" for o in ops): e(g,"cardinality changed without op")
        if "UNIVERSE" not in touched and r["m2a_scope"]=="IN" and r["turn_kind"] in("DELTA","ANSWER_TO_CLARIFICATION") and (ir["universe"],ir["view"])!=(p["ir"]["universe"],p["ir"]["view"]) and not any(o["slot"]=="UNIVERSE" for o in ops): e(g,"universe changed without op")
        # F9
        if p["ir"]["view_source"]=="USER" and ir["universe"]==p["ir"]["universe"] and ir["product_view"]!=p["ir"]["product_view"]: e(g,"F9 USER view changed")
    # VALUE per-side O4
    vo=[o for o in ops if o["slot"] in("VALUE.lower","VALUE.upper") and o["op"]=="SET" and not o["unresolved"]]
    q={}
    for o in vo: q.setdefault(o["quote"],[]).append(o)
    for qt,l in q.items():
        if len(l)==2:
            sl={o["slot"]:o for o in l}
            if set(sl)!={"VALUE.lower","VALUE.upper"} or sl["VALUE.lower"]["operator"]!="greater_or_equal" or sl["VALUE.upper"]["operator"]!="less_or_equal": e(g,"O4 range")
        elif len(l)==1:
            o=l[0]
            if o["polarity"]=="POSITIVE" and (o["slot"]=="VALUE.lower")!=o["operator"].startswith("greater"): e(g,"O4 side/operator")
            if o["polarity"]=="NEGATIVE" and (o["slot"]=="VALUE.upper")!=o["operator"].startswith("greater"): e(g,"O4 negated side")
    # A1: resulting never has lower>=upper silently: flag must be RANGE_NO_INTERIOR when alt exists
    if r["ambiguity"]=="AMBIGUOUS" and r["resulting_ir"] is None and not r["alternatives"] and False: pass
SC={"PESOS":1,"MILES":1e3,"MILLONES":1e6,"MIL_MILLONES":1e9,"BILLONES":1e12}
import re
def num(v):
    m=re.match(r"[\d\.]+",v); t=m.group(0); 
    return float(t.replace(".","")) if t.count(".")>1 else float(t)
for r in recs:
    ir=r["resulting_ir"]
    if ir and r["expected_disposition"]=="EXECUTE":
        vs=[c for c in ir["conditions"] if c["dimension"]=="VALUE" and c["polarity"]=="POSITIVE"]
        lo=[c for c in vs if c["operator"].startswith("greater")]; up=[c for c in vs if c["operator"].startswith("less")]
        if lo and up and lo[0]["unit"]!="SMMLV" and up[0]["unit"]!="SMMLV":
            a=num(lo[0]["value_hint"])*SC[lo[0]["unit"]]; b=num(up[0]["value_hint"])*SC[up[0]["unit"]]
            if a>b or (a==b and (lo[0]["operator"]=="greater_than" or up[0]["operator"]=="less_than")): e(r["gold_id"],"A8 empty interval in resulting_ir")
        if lo and up and "SMMLV" in (lo[0]["unit"],up[0]["unit"]) and lo[0]["unit"]!=up[0]["unit"] and r["ambiguity"]!="AMBIGUOUS": e(r["gold_id"],"A8 not comparable must be AMBIGUOUS")
    if "RANGE_NO_INTERIOR" in r["gap_codes"]: e(r["gold_id"],"RANGE_NO_INTERIOR must be gone after A8")
    if "A8" in r["notes"] and r["ambiguity"]!="CLEAR" and r["gold_id"]!="SF170": e(r["gold_id"],"A8 adjudicated must be CLEAR")
v1={json.loads(l)["gold_id"]:json.loads(l) for l in open(G+"sintetico/etiquetas_SA_v2_1/labels_v2_1.jsonl")}
REV={"SF051","SF053","SF063","SF090","SF091","SF094","SF095","SF099","SF103","SF126","SF132","SF134","SF182","SF183","SF185","SF187","SF170"}
for r in recs:
    if r["gold_id"] not in REV and r!=v1[r["gold_id"]]: e(r["gold_id"],"unreviewed record changed vs v2.1")
res={"schema":"m2a-followup-gold/2.1","total":len(recs),"ids_sorted":[x["gold_id"] for x in recs]==sorted(cases),"errors":errs,"ok":not errs}
json.dump(res,open(OUT+"self_check.json","w"),ensure_ascii=False,indent=1)
print(len(recs),len(errs)); print("\n".join(errs[:80]))
