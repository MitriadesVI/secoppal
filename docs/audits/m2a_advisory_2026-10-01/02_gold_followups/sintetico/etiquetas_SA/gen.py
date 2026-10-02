import json, copy
BASE="/private/tmp/claude-501/-Users-rodrigoortiz-Documents-secoppal/771a1a89-ee94-4b92-b2de-eef37cce6788/scratchpad/gold_followups/"
CASES=[json.loads(l) for l in open(BASE+"synthetic_for_labelers/cases.jsonl")]
BY={c["gold_id"]:c for c in CASES}

# ---------- IR helpers ----------
def C(i,dim,op,vh,pol="POSITIVE",level=None,unit=None):
    return {"id":i,"dimension":dim,"operator":op,"polarity":pol,"value_hint":vh,"level":level,"unit":unit}
def nid(ir):
    n=max([int(c["id"][1:]) for c in ir["conditions"]]+[0]); return f"c{n+1}"
def add(ir,dim,op,vh,pol="POSITIVE",level=None,unit=None):
    ir["conditions"].append(C(nid(ir),dim,op,vh,pol,level,unit)); return ir
def rep(ir,dim,op,vh,pol="POSITIVE",level=None,unit=None):
    # replace all conditions of dim in place (keep id of first), else add
    idx=[i for i,c in enumerate(ir["conditions"]) if c["dimension"]==dim]
    if not idx: return add(ir,dim,op,vh,pol,level,unit)
    first=ir["conditions"][idx[0]]["id"]
    new=C(first,dim,op,vh,pol,level,unit)
    ir["conditions"][idx[0]]=new
    for i in reversed(idx[1:]): del ir["conditions"][i]
    return ir
def rm(ir,dim):
    ir["conditions"]=[c for c in ir["conditions"] if c["dimension"]!=dim]; return ir
def setvalue(ir,conds):
    """replace all VALUE conditions by list of (operator,vh,unit); reuse ids."""
    old=[c["id"] for c in ir["conditions"] if c["dimension"]=="VALUE"]
    pos=next((i for i,c in enumerate(ir["conditions"]) if c["dimension"]=="VALUE"),len(ir["conditions"]))
    ir["conditions"]=[c for c in ir["conditions"] if c["dimension"]!="VALUE"]
    new=[]
    for k,(op,vh,u) in enumerate(conds):
        i=old[k] if k<len(old) else None
        new.append((i,op,vh,u))
    out=[]
    for i,op,vh,u in new:
        if i is None:
            i=nid({"conditions":ir["conditions"]+out})
        out.append(C(i,"VALUE",op,vh,"POSITIVE",None,u))
    ir["conditions"][pos:pos]=out; return ir
def P(i): return copy.deepcopy(BY[i]["prior"]["ir"])

def O(op,slot,quote,vh,operator=None,pol="POSITIVE",level=None,r=False,dep=False,ys=None,us=None):
    if op=="CLEAR": pol=None
    return {"op":op,"slot":slot,"operator":operator,"polarity":pol,"level":level,"quote":quote,"value_hint":vh,
            "refers_to_result_set":r,"depends_on_prior_value":dep,"inherited":{"year_source":ys,"unit_source":us}}

LABELS={}
def L(gid,kind,scope,ops,ir,eff,disp,reason,amb="CLEAR",alts=None,conf="HIGH",gaps=None,notes="",echo=None,echo_x=None,uni_note=False):
    LABELS[gid]=dict(turn_kind=kind,m2a_scope=scope,ops=ops,resulting_ir=ir,effects=eff,expected_disposition=disp,
        disposition_reason=reason,expected_echo=echo,_echo_x=echo_x or [],ambiguity=amb,alternatives=alts or [],confidence=conf,
        gap_codes=gaps or [],labeler="SA",notes=notes+(" ECHO_EXTRA: UNIVERSE_CHANGED (not in closed echo list)." if uni_note else ""))
D="DELTA"; IN="IN"; EX="EXECUTE"
def alt(desc): return [desc]

# ---------- 001-014 polarity / exclusion ----------
i="SF001"; ir=P(i); add(ir,"TOPIC","contains","papelería","NEGATIVE")
L(i,D,IN,[O("SET","TOPIC","que no sean de papelería","papelería","contains","NEGATIVE")],ir,[],EX,None,notes="Negated topic added; buyer and year carried.")
i="SF002"; ir=P(i); add(ir,"TOPIC","contains","papelería")
L(i,D,IN,[O("SET","TOPIC","que sean de papelería","papelería","contains")],ir,[],EX,None,notes="Positive twin of SF001.")
i="SF003"; ir=P(i); add(ir,"GEOGRAPHY","is","Eje Cafetero","NEGATIVE","REGION")
L(i,D,IN,[O("SET","GEOGRAPHY","los del Eje Cafetero","Eje Cafetero","is","NEGATIVE","REGION",r=True)],ir,[],EX,None,gaps=["REGION_VOCAB"],notes="Eje Cafetero is in the closed region list (F13). Negative region exclusion.")
i="SF004"; ir=P(i); add(ir,"GEOGRAPHY","is","Eje Cafetero","POSITIVE","REGION")
L(i,D,IN,[O("SET","GEOGRAPHY","los del Eje Cafetero","Eje Cafetero","is","POSITIVE","REGION",r=True)],ir,[],EX,None,gaps=["REGION_VOCAB"],notes="Positive twin of SF003.")
i="SF005"; ir=P(i); add(ir,"BUYER","is","SENA","NEGATIVE")
L(i,D,IN,[O("SET","BUYER","que no sean del SENA","SENA","is","NEGATIVE")],ir,[],EX,None,notes="SENA is a named buyer (sigla); prior has no ACTOR so F12 does not apply.")
i="SF006"; ir=P(i); add(ir,"BUYER","is","SENA")
L(i,D,IN,[O("SET","BUYER","que sean del SENA","SENA","is")],ir,[],EX,None,notes="Positive twin of SF005.")
i="SF007"; ir=P(i); add(ir,"MODALITY","is","contratación directa","NEGATIVE")
L(i,D,IN,[O("SET","MODALITY","sin contratación directa","contratación directa","is","NEGATIVE")],ir,[],EX,None,notes="Named modality excluded.")
i="SF008"; ir=P(i); add(ir,"MODALITY","is","contratación directa")
L(i,D,IN,[O("SET","MODALITY","solo contratación directa","contratación directa","is")],ir,[],EX,None,notes="Positive twin of SF007; 'solo' is a narrowing marker, not a removal.")
i="SF009"; ir=P(i); add(ir,"STATE","is","awarded","NEGATIVE")
L(i,D,IN,[O("SET","STATE","que no estén adjudicados","awarded","is","NEGATIVE",r=True)],ir,[],EX,None,notes="Negated awarded state on PROCESSES (explicit universe, no ACTOR). D4: negative state does not trigger PV2.")
i="SF010"; ir=P(i); add(ir,"STATE","is","awarded")
L(i,D,IN,[O("SET","STATE","los adjudicados","awarded","is",r=True)],ir,[],EX,None,notes="Positive twin of SF009. PROCESSES + awarded without ACTOR: no redirect.")
i="SF011"; ir=P(i); rep(ir,"GEOGRAPHY","is","Cali","NEGATIVE","MUNICIPALITY")
L(i,D,IN,[O("SET","GEOGRAPHY","sin los de Cali","Cali","is","NEGATIVE","MUNICIPALITY",r=True)],ir,[],EX,None,amb="AMBIGUOUS",
  alts=["Positive Cali kept AND negative Cali added (empty by conjunction)","CLEAR GEOGRAPHY (user wants to drop the Cali filter)"],conf="MEDIUM",gaps=["SET","GEOBUYER"],
  notes="Same-value negation over an accepted positive: read as polarity flip of the GEOGRAPHY slot (all mantenimiento vial 2024 except Cali). Rule missing: how SET with opposite polarity on an occupied slot composes (flip vs coexist vs clear).")
i="SF012"; ir=P(i); rep(ir,"BUYER","is","Alcaldía de Pasto","NEGATIVE")
L(i,D,IN,[O("SET","BUYER","excluye a la Alcaldía de Pasto","Alcaldía de Pasto","is","NEGATIVE")],ir,[],EX,None,amb="AMBIGUOUS",
  alts=["Positive buyer kept AND negative added (empty by conjunction)","CLEAR BUYER"],conf="MEDIUM",gaps=["SET"],
  notes="Same-value negation over accepted positive buyer: polarity flip. Rule missing: SET with opposite polarity on an occupied slot.")
i="SF013"; ir=P(i); add(ir,"VALUE","greater_than","500 millones","NEGATIVE",None,"MILLONES")
L(i,D,IN,[O("SET","VALUE.lower","excluye los de más de 500 millones","500 millones","greater_than","NEGATIVE",r=True,us="STATED")],ir,[],EX,None,amb="AMBIGUOUS",
  alts=["VALUE less_or_equal 500 millones POSITIVE (equivalent normalised form)"],conf="MEDIUM",
  notes="NEW_GAP: representation of a negated value comparator (NEGATIVE greater_than vs POSITIVE less_or_equal); contract E2/D4 says negation of an atom = polarity NEGATIVE, so I keep NEGATIVE greater_than (slot VALUE.lower by operator).")
i="SF014"; ir=P(i); add(ir,"TIME","during_year","2023","NEGATIVE")
L(i,D,IN,[O("SET","TIME","sin contar los de 2023","2023","during_year","NEGATIVE",r=True,ys="STATED")],ir,[],EX,None,notes="Negated year; year stated.")

# ---------- 015-024 order / cardinality ----------
i="SF015"; ir=P(i); ir["order"]={"operator":"value_desc","polarity":"POSITIVE"}; ir["cardinality"]={"operator":"top_n","value_hint":"5"}
L(i,D,IN,[O("SET_ORDER","ORDER","más caros","más caros","value_desc",r=True),O("SET","CARDINALITY","5","5","top_n",r=True)],ir,[],EX,None,notes="Re-query (F4) with value_desc + top 5; conditions carried.")
i="SF016"; ir=P(i); ir["order"]={"operator":"value_desc","polarity":"POSITIVE"}
L(i,D,IN,[O("SET_ORDER","ORDER","más caros","más caros","value_desc",r=True)],ir,[],EX,None,notes="Plural superlative without figure: ORDER only, no CARDINALITY (ADR-103 enm. 1).")
i="SF017"; ir=P(i); ir["order"]={"operator":"value_desc","polarity":"POSITIVE"}; ir["cardinality"]={"operator":"top_n","value_hint":"1"}
L(i,D,IN,[O("SET_ORDER","ORDER","el de mayor valor","mayor valor","value_desc",r=True),O("SET","CARDINALITY","el de mayor valor","1","top_n",r=True)],ir,[],EX,None,
  amb="AMBIGUOUS",alts=["Question about the already displayed rows (no re-query)"],conf="MEDIUM",
  notes="Singular superlative -> ORDER + CARDINALITY 1 (v1.5 / ADR-103 enm.1). Interrogative form could be read as a question about shown rows; F4 says re-query.")
i="SF018"; ir=P(i); ir["order"]={"operator":"date_desc","polarity":"POSITIVE"}
L(i,D,IN,[O("SET_ORDER","ORDER","ordénalos por fecha, primero los más recientes","más recientes","date_desc",r=True)],ir,[],EX,None,echo=["CARRIED","TOPN_CARRIED"],notes="F10: top 10 kept, reordered most recent first.")
i="SF019"; ir=P(i); ir["order"]={"operator":"date_asc","polarity":"POSITIVE"}
L(i,D,IN,[O("SET_ORDER","ORDER","ordénalos por fecha, primero los más antiguos","más antiguos","date_asc",r=True)],ir,[],EX,None,echo=["CARRIED","TOPN_CARRIED"],notes="F10: top 10 kept, oldest first.")
i="SF020"; ir=P(i); ir["order"]={"operator":"date_desc","polarity":"POSITIVE"}; ir["cardinality"]={"operator":"top_n","value_hint":"20"}
L(i,D,IN,[O("SET_ORDER","ORDER","más recientes","más recientes","date_desc",r=True),O("SET","CARDINALITY","20","20","top_n",r=True)],ir,[],EX,None,notes="Figure -> CARDINALITY 20 (<=100 cap).")
i="SF021"; ir=P(i); ir["order"]={"operator":"value_desc","polarity":"POSITIVE"}; ir["cardinality"]={"operator":"top_n","value_hint":"250"}
L(i,D,IN,[O("SET_ORDER","ORDER","de mayor valor","mayor valor","value_desc",r=True),O("SET","CARDINALITY","250","250","top_n",r=True)],ir,[],"CLARIFY","TOPN_CAP_EXCEEDED",amb="AMBIGUOUS",
  alts=["EXECUTE clamped to 100, announced ('te muestro los 100 de mayor valor')"],conf="MEDIUM",
  notes="NEW_GAP: P8 text in C0 says 'Máximo 100, anunciado; por encima, aclarar' (clamp-and-announce vs clarify). I read: <=100 executes, >100 clarifies; ADR-103 says 'tope 100 anunciado'. resulting_ir keeps the requested 250.")
i="SF022"; ir=P(i); ir["order"]={"operator":"value_desc","polarity":"POSITIVE"}; ir["cardinality"]={"operator":"top_n","value_hint":"100"}
L(i,D,IN,[O("SET_ORDER","ORDER","de mayor valor","mayor valor","value_desc",r=True),O("SET","CARDINALITY","100","100","top_n",r=True)],ir,[],EX,None,notes="Exactly the cap (P8 max 100): executes.")
i="SF023"; ir=P(i); ir["order"]=None; ir["cardinality"]=None
L(i,D,IN,[O("CLEAR","ORDER","quita el orden",None),O("CLEAR","CARDINALITY","verlos todos",None,r=True)],ir,[],EX,None,notes="Order removal cited; 'todos' cites removal of the limit (CARDINALITY needs ORDER anyway).")
i="SF024"; ir=P(i); ir["cardinality"]=None
L(i,D,IN,[O("CLEAR","CARDINALITY","no solo 10, todos",None,r=True),O("KEEP","ORDER","déjalos por valor","valor (value_desc)","value_desc",r=True,dep=True)],ir,[],EX,None,
  notes="Limit dropped, value order kept (direction inherited from prior value_desc).")

# ---------- 025-046 time ----------
def tm(i,op,vh_ir,quote,vh,txt,ys,eff=None,amb="CLEAR",alts=None,conf="HIGH",gaps=None,notes="",r=False,dep=False,echo=None,base=None):
    ir=base if base is not None else P(i); rep(ir,"TIME",op,vh_ir)
    L(i,D,IN,[O("SET","TIME",quote,vh,op,r=r,dep=dep,ys=ys)],ir,eff or [],EX,None,amb,alts,conf,gaps,notes,echo)
tm("SF025","relative_period","este año","este año","este año (2026)",None,"CLOCK_INFERRED",notes="Relative window resolved on 2026-10-01; no prior TIME. Contract lists 'este año' under relative_period.")
tm("SF026","relative_period","este mes","los de este mes","este mes (octubre de 2026)",None,"CLOCK_INFERRED",r=True,notes="Relative window ('este mes'), not a month-without-year (F7 does not apply).")
tm("SF027","relative_period","últimos 6 meses","de los últimos 6 meses","últimos 6 meses (desde 2026-04-01)",None,"CLOCK_INFERRED",notes="Relative window.")
tm("SF028","relative_period","últimos 3 meses","los últimos 3 meses","últimos 3 meses",None,"CLOCK_INFERRED",notes="Replaces prior year 2024 with a relative window.")
tm("SF029","relative_period","últimos 12 meses","los últimos 12 meses","últimos 12 meses",None,"CLOCK_INFERRED",notes="Replaces prior 'últimos 3 meses'.")
tm("SF030","relative_period","año en curso","el año en curso","año en curso (2026)",None,"CLOCK_INFERRED",notes="Replaces prior March 2025 with the current year (relative_period).")
i="SF031"; ir=P(i)
L(i,D,IN,[O("SET","TIME","solo marzo","marzo (año por resolver)","during_month",dep=True,ys=None)],None,[],"CLARIFY","MONTH_YEAR_AMBIGUOUS",conf="HIGH",gaps=["MONTHYEAR"],
  notes="F7: prior TIME spans several years (2023 a 2025) -> CLARIFY which year. resulting_ir null: year not determinable.")
tm("SF032","during_month","2024-03","solo marzo","marzo de 2024",None,"PREVIOUS_ACCEPTED_TIME",dep=True,gaps=["MONTHYEAR"],echo=["CARRIED","YEAR_INHERITED"],notes="F7: single active year 2024 inherited; replaces during_year 2024 by the month.")
i="SF033"
L(i,D,IN,[O("SET","TIME","en octubre","octubre (año por resolver)","during_month",dep=True,ys=None)],None,[],"CLARIFY","MONTH_YEAR_AMBIGUOUS",gaps=["MONTHYEAR"],
  notes="F7: prior TIME is a relative window -> CLARIFY. resulting_ir null.")
tm("SF034","during_month","2026-04","en abril","abril de 2026",None,"CLOCK_INFERRED",gaps=["MONTHYEAR"],echo=["CARRIED","YEAR_INHERITED"],notes="F7: no prior TIME -> most recent April not after 2026-10-01 = 2026, announced.")
tm("SF035","during_month","2025-12","en diciembre","diciembre de 2025",None,"CLOCK_INFERRED",gaps=["MONTHYEAR"],echo=["CARRIED","YEAR_INHERITED"],notes="F7: December 2026 is after today -> most recent occurrence not after today is December 2025, announced.")
tm("SF036","relative_period","este año","este año","este año (2026)",None,"CLOCK_INFERRED",notes="Replaces between_dates 2022 a 2024.")
# F15 relative
i="SF037"; ir=P(i); rep(ir,"TIME","during_month","2024-04")
L(i,D,"OUT_RELATIVE_TO_PREVIOUS",[O("SET","TIME","el mes siguiente","abril de 2024","during_month",dep=True,ys="PREVIOUS_ACCEPTED_TIME")],ir,[],"CLARIFY","RELATIVE_TO_PREVIOUS_VALUE",
  notes="F15. resulting_ir shows what it would be if continued (April 2024).",gaps=["MONTHYEAR"],echo=[])
i="SF038"; ir=P(i); rep(ir,"TIME","during_year","2023")
L(i,D,"OUT_RELATIVE_TO_PREVIOUS",[O("SET","TIME","el año anterior","2023","during_year",dep=True,ys="PREVIOUS_ACCEPTED_TIME")],ir,[],"CLARIFY","RELATIVE_TO_PREVIOUS_VALUE",notes="F15. Would be 2023 if continued.",echo=[])
i="SF039"
L(i,D,"OUT_RELATIVE_TO_PREVIOUS",[O("SET","VALUE.lower","bájalo un poco","menos que 200 millones (monto no especificado)","greater_or_equal",dep=True,us="PREVIOUS_ACCEPTED_VALUE")],None,[],"CLARIFY","RELATIVE_TO_PREVIOUS_VALUE",
  notes="F15. Amount of the decrease is unspecified -> resulting_ir not determinable (null).",echo=[])
i="SF040"; ir=P(i); setvalue(ir,[("greater_or_equal","200 millones","MILLONES")])
L(i,D,"OUT_RELATIVE_TO_PREVIOUS",[O("SET","VALUE.lower","el doble","200 millones","greater_or_equal",dep=True,us="PREVIOUS_ACCEPTED_VALUE")],ir,[],"CLARIFY","RELATIVE_TO_PREVIOUS_VALUE",
  amb="AMBIGUOUS",alts=["Double of what: the lower bound (>=200 millones) or something else"],conf="MEDIUM",notes="F15. If continued: lower bound doubled to 200 millones.",echo=[])
i="SF041"; ir=P(i); rep(ir,"TOPIC","contains","vigilancia")
L(i,D,IN,[O("SET","TOPIC","vigilancia","vigilancia","contains"),O("KEEP","BUYER","el mismo comprador","Alcaldía de Ibagué","is",dep=True)],ir,["TOPIC_REPLACED"],EX,None,echo=["CARRIED","TOPIC_REPLACED"],gaps=["TOPIC"],
  notes="F3: topic replaced and announced; buyer explicitly kept (KEEP).")
i="SF042"; ir=P(i); rep(ir,"TIME","between_dates","julio a diciembre de 2024")
L(i,D,"OUT_RELATIVE_TO_PREVIOUS",[O("SET","TIME","los seis meses siguientes","julio a diciembre de 2024","between_dates",r=True,dep=True,ys="PREVIOUS_ACCEPTED_TIME")],ir,[],"CLARIFY","RELATIVE_TO_PREVIOUS_VALUE",notes="F15. If continued: the six months after Jan-Jun 2024.",echo=[])
i="SF043"
L(i,D,"OUT_RELATIVE_TO_PREVIOUS",[O("SET","VALUE.upper","súbelo un poquito","más que 50 millones (monto no especificado)","less_or_equal",dep=True,us="PREVIOUS_ACCEPTED_VALUE")],None,[],"CLARIFY","RELATIVE_TO_PREVIOUS_VALUE",
  notes="F15. Amount unspecified -> resulting_ir null.",echo=[])
i="SF044"; ir=P(i); setvalue(ir,[("greater_or_equal","90 millones","MILLONES")])
L(i,D,"OUT_RELATIVE_TO_PREVIOUS",[O("SET","VALUE.lower","el triple","90 millones","greater_or_equal",dep=True,us="PREVIOUS_ACCEPTED_VALUE")],ir,[],"CLARIFY","RELATIVE_TO_PREVIOUS_VALUE",notes="F15. If continued: lower bound tripled to 90 millones.",echo=[])
i="SF045"; ir=P(i); rep(ir,"TIME","between_dates","noviembre de 2024 a enero de 2025")
L(i,D,"OUT_RELATIVE_TO_PREVIOUS",[O("SET","TIME","un trimestre antes","noviembre de 2024 a enero de 2025","between_dates",dep=True,ys="PREVIOUS_ACCEPTED_TIME")],ir,[],"CLARIFY","RELATIVE_TO_PREVIOUS_VALUE",
  amb="AMBIGUOUS",alts=["Shift of the window back one quarter (read here)","The quarter preceding the window's start (nov 2024 a ene 2025 as well, but length-3 interpretation)"],conf="LOW",
  notes="F15. 'Un trimestre antes' is vague; the best reading shifts the Feb-Apr 2025 window back three months. resulting_ir is low confidence.",echo=[])
i="SF046"; ir=P(i); setvalue(ir,[("greater_or_equal","50 millones","MILLONES"),("less_or_equal","150 millones","MILLONES")])
L(i,D,"OUT_RELATIVE_TO_PREVIOUS",[O("SET","VALUE.lower","la mitad de eso","50 millones","greater_or_equal",r=True,dep=True,us="PREVIOUS_ACCEPTED_VALUE"),O("SET","VALUE.upper","la mitad de eso","150 millones","less_or_equal",r=True,dep=True,us="PREVIOUS_ACCEPTED_VALUE")],ir,[],"CLARIFY","RELATIVE_TO_PREVIOUS_VALUE",
  amb="AMBIGUOUS",alts=["Only the upper bound halved (100-150 millones)","Halve nothing determinable ('eso' unspecified)"],conf="LOW",notes="F15. Both bounds halved is my best reading of 'eso'.",echo=[])

# ---------- 047-064 pending period ----------
def newsearch(i,universe,view,conds,vs="USER",pv=None,notes="",amb="CLEAR",alts=None,conf="HIGH",echo=None,gaps=None,uni=None):
    ir={"operation":"SEARCH","universe":universe,"view":view,"product_view":pv,"view_source":vs,"conditions":conds,"order":None,"cardinality":None}
    if uni: ir["universes"]=uni
    L(i,"NEW_SEARCH",IN,[],ir,[],"NEW_SEARCH",None,amb,alts,conf,gaps,notes+" NEW_SEARCH: ops [] (self-contained; nothing composed with the anchor).",echo if echo is not None else [])
for i,tp,by in [("SF047","alquiler de oficinas","Alcaldía de Piedecuesta"),("SF048","alquiler de oficinas","Gobernación de Santander")]:
    newsearch(i,"CONTRACTS","RECORDS",[C("c1","TOPIC","contains",tp),C("c2","BUYER","is",by),C("c3","TIME","between_dates","enero a junio de 2025")],notes="Complete restatement with its own year; pending period question of the prior is abandoned.")
for i in ["SF049","SF050"]:
    newsearch(i,"CONTRACTS","RECORDS",[C("c1","TOPIC","contains","alquiler de oficinas"),C("c2","BUYER","is","Alcaldía de Piedecuesta"),C("c3","TIME","during_year","2024")],notes="Complete restatement. SF050 differs only in chain PROTOCOL:reask, which does not change the label.")
i="SF051"; ir=P(i); setvalue(ir,[("less_than","40 millones","MILLONES")]); add(ir,"TIME","between_dates","enero a junio de 2025")
L(i,D,IN,[O("SET","VALUE.upper","menos de 40 millones","menos de 40 millones","less_than",us="STATED"),O("SET","TIME","de enero a junio de 2025","enero a junio de 2025","between_dates",ys="STATED")],ir,["VALUE_RANGE_REPLACED"],EX,None,
  amb="AMBIGUOUS",alts=["ANSWER_TO_CLARIFICATION (the period resolves the pending year question) plus a SET"],conf="MEDIUM",gaps=["SET"],echo=["CARRIED","VALUE_RANGE_REPLACED"],
  notes="Pending TIME is resolved by the stated period. F2: new upper bound (<40M) leaves the old lower bound (>150M) with no interior -> restriction replaced whole, announced.")
newsearch("SF052","PROCESSES","RECORDS",[C("c1","TOPIC","contains","alquiler de oficinas"),C("c2","BUYER","is","Alcaldía de Piedecuesta"),C("c3","TIME","between_dates","enero a junio de 2025")],notes="Explicit universe 'procesos'.")
i="SF053"; ir=P(i); rep(ir,"BUYER","is","Alcaldía de Piedecuesta"); add(ir,"TIME","between_dates","enero a junio de 2025")
L(i,D,IN,[O("SET","BUYER","los de la Alcaldía de Piedecuesta","Alcaldía de Piedecuesta","is",r=True),O("SET","TIME","de enero a junio de 2025","enero a junio de 2025","between_dates",ys="STATED")],ir,[],EX,None,
  amb="AMBIGUOUS",alts=["NEW_SEARCH (self-contained buyer+period, universe unspecified)"],conf="MEDIUM",echo=["CARRIED"],
  notes="Anaphoric 'los de' keeps the pending base (interventoría, >150M); buyer replaced, period supplied (pending resolved).")
i="SF054"; ir=P(i); rep(ir,"TIME","between_dates","enero a junio de 2025")
L(i,D,IN,[O("SET","TIME","de enero a junio de 2025","enero a junio de 2025","between_dates",ys="STATED")],ir,[],EX,None,notes="Replaces during_year 2024 (c4) with the stated period.")
i="SF055"; ir=P(i); add(ir,"TIME","between_dates","enero a junio de 2025")
L(i,"NEW_SEARCH",IN,[],ir,[],"NEW_SEARCH",None,amb="AMBIGUOUS",alts=["ANSWER_TO_CLARIFICATION (identical resulting IR: prior pending base + the period)"],conf="MEDIUM",
  notes="Full restatement of the pending query with the year: self-contained so NEW_SEARCH; resulting IR equals the prior pending base plus TIME. NEW_SEARCH: ops [].",echo=[])
i="SF056"; ir=P(i); add(ir,"TIME","between_dates","enero a junio de 2025")
L(i,"ANSWER_TO_CLARIFICATION",IN,[O("ANSWER","TIME","de enero a junio de 2025","enero a junio de 2025","between_dates",ys="STATED")],ir,[],EX,None,echo=["CARRIED"],notes="Answer to the pending PERIOD_WITHOUT_YEAR question (period stated with its year).")
newsearch("SF057","CONTRACTS","RECORDS",[C("c1","TOPIC","contains","alquiler de oficinas"),C("c2","BUYER","is","Alcaldía de Piedecuesta"),C("c3","TIME","between_dates","enero a junio de 2025")],notes="Imperative 'busca' + complete query.")
i="SF058"; ir=P(i); rep(ir,"BUYER","is","Alcaldía de San Pedro (Valle del Cauca)")
L(i,"ANSWER_TO_CLARIFICATION",IN,[O("ANSWER","BUYER","Alcaldía de San Pedro (Valle del Cauca)","Alcaldía de San Pedro (Valle del Cauca)","is")],ir,[],EX,None,echo=["CARRIED"],notes="Fragment is the label of an option offered by the pending BUYER clarification: answer by event.")
for i,qt,role in [("SF059","Como comprador","BUYER"),("SF060","Como lugar","GEOGRAPHY")]:
    L(i,"ANSWER_TO_CLARIFICATION",IN,[O("ANSWER",role,qt,"(span pendiente no registrado en prior.ir)","is",level=("MUNICIPALITY" if role=="GEOGRAPHY" else None))],None,[],EX,None,
      amb="AMBIGUOUS",alts=["resulting_ir = prior ACTOR condition kept + the pending span added as "+role],conf="LOW",
      notes="Option label answering the F12-type BUYER|GEOGRAPHY question. prior.ir does not contain the pending span's text, so the resulting condition value is not determinable -> resulting_ir null. The ACTOR condition must be kept (F1).",echo=["CARRIED"],gaps=["COUNTERPART"])
i="SF061"
L(i,"ANSWER_TO_CLARIFICATION",IN,[],P(i),[],"REASK","NON_SELECTION",conf="HIGH",echo=[],notes="Reply selects no option; the pending BUYER question remains. resulting_ir = anchor unchanged (still pending).")
i="SF062"; ir=P(i); ir["operation"]="SUM_VALUE"
L(i,"OTHER_OPERATION","OUT_OPERATION_CHANGE",[],ir,[],"HANDOFF_LEGACY","OPERATION_SWITCH:SUM_VALUE",amb="AMBIGUOUS",alts=["REASK: resolve the pending BUYER ambiguity before any new operation"],conf="MEDIUM",gaps=["OPSWITCH","TOTALES"],
  notes="'cuánto suman en total' = SUM_VALUE (Contract Activity), outside M2a; the pending buyer ambiguity (Santa Rosa) is also unresolved.",echo=[])
i="SF063"
newsearch(i,"UNSPECIFIED","RECORDS",[C("c1","TOPIC","contains","dotación"),C("c2","BUYER","is","Gobernación del Cauca"),C("c3","TIME","during_year","2025")],vs="DERIVED",pv="HISTORICAL_PROCESSES",
  amb="AMBIGUOUS",alts=["DELTA over the pending base: topic/buyer replaced, year answered, universe CONTRACTS and VALUE >150M carried"],conf="MEDIUM",echo=["VIEW_DERIVED"],gaps=["PV"],
  notes="'no, mejor' restarts with a self-contained topic+buyer+year; universe not named -> UNSPECIFIED, PV1 (TIME present) -> HISTORICAL_PROCESSES, announced. Rule missing: whether 'no, mejor X' drops the unmentioned conditions (value, universe).")
i="SF064"; ir=P(i); add(ir,"TIME","between_dates","enero a junio de 2025")
L(i,"ANSWER_TO_CLARIFICATION",IN,[O("ANSWER","TIME","de enero a junio de 2025","enero a junio de 2025","between_dates",ys="STATED")],ir,[],"CLARIFY","CONTEXT_EXPIRED_CONFIRM",echo=["CONTEXT_EXPIRED"],gaps=["TTL"],
  notes="F11: 2640 s > 1800 s -> continue-or-reset confirmation; resulting_ir is what continuing would give.")

# ---------- 065-072 referents ----------
for i,q in [("SF065","el segundo"),("SF066","ese contrato, de qué fecha es?"),("SF067","ábreme el tercero"),("SF070","el primero de la lista, cuánto vale?"),("SF071","y ese último, quién lo firmó?"),("SF072","los dos primeros")]:
    L(i,"OTHER_OPERATION","OUT_REFERENT_EXTENSIONAL",[],P(i),[],"HANDOFF_LEGACY","REFERENT_EXTENSIONAL",gaps=["SELECT"],echo=[],
      notes="References a row/position of the displayed list (extensional), not a search condition; search anchor unchanged (resulting_ir = prior.ir).")
for i,q in [("SF068","de esos, los que sean de Cali"),("SF069","los que sean de Cali")]:
    ir=P(i); add(ir,"GEOGRAPHY","is","Cali","POSITIVE","MUNICIPALITY")
    L(i,D,IN,[O("SET","GEOGRAPHY",q,"Cali","is",level="MUNICIPALITY",r=True)],ir,[],EX,None,amb="AMBIGUOUS",alts=["BUYER Alcaldía de Cali"],conf="MEDIUM",gaps=["GEOBUYER"],echo=["CARRIED"],
      notes="F4: 're-query with the condition added', not a filter over shown rows. D7: 'de <lugar>' = GEOGRAPHY dominant, AMBIGUOUS, EXECUTE.")

# ---------- 073-084 universe ----------
def uni(i,q,u,view,eff,extra_ops=None,extra_edit=None,notes="",disp=EX,reason=None,amb="CLEAR",alts=None,conf="HIGH",r=False,gaps=None,echo=None):
    ir=P(i); ir["universe"]=u; ir["view"]=view; ir["view_source"]="USER"; ir["product_view"]=None
    ir.pop("universes",None)
    if extra_edit: extra_edit(ir)
    ops=[O("SET_UNIVERSE","UNIVERSE",q,u if view=="RECORDS" else "PROCESSES (oportunidades)",r=r)]+(extra_ops or [])
    L(i,D,IN,ops,ir,eff,disp,reason,amb,alts,conf,gaps,notes,echo,uni_note=("UNIVERSE_CHANGED" in eff))
uni("SF073","los procesos","PROCESSES","RECORDS",["UNIVERSE_CHANGED"],gaps=["UNIVERSE_FRAGMENT"],notes="Universe user-set; F9: no view recalculation.")
uni("SF074","en contratos","CONTRACTS","RECORDS",["UNIVERSE_CHANGED"],gaps=["UNIVERSE_FRAGMENT"],notes="Universe switch to CONTRACTS.")
uni("SF075","las oportunidades","PROCESSES","OPPORTUNITIES",["UNIVERSE_CHANGED"],amb="AMBIGUOUS",alts=["Drop the 2025 TIME (opportunities are current)"],conf="MEDIUM",gaps=["UNIVERSE_FRAGMENT"],
    notes="'oportunidades' = PROCESSES + OPPORTUNITIES (O3/O7). TIME 2025 is carried by F1 even though it fits current opportunities poorly.")
uni("SF076","solo los contratos","CONTRACTS","RECORDS",["UNIVERSE_CHANGED"],r=True,notes="DUAL anchor narrowed to CONTRACTS.")
uni("SF077","solo los procesos","PROCESSES","RECORDS",["UNIVERSE_CHANGED"],r=True,notes="DUAL anchor narrowed to PROCESSES.")
i="SF078"
uni(i,"los procesos","PROCESSES","RECORDS",["UNIVERSE_CHANGED"],extra_ops=[O("SET","STATE","que están abiertos","open","family")],extra_edit=lambda ir:add(ir,"STATE","family","open"),notes="Universe PROCESSES + STATE family open (open exists in procesos, §19).")
i="SF079"
uni(i,"los contratos","CONTRACTS","RECORDS",["UNIVERSE_CHANGED"],extra_ops=[O("SET","STATE","que están abiertos","open","family")],extra_edit=lambda ir:add(ir,"STATE","family","open"),
    disp="CLARIFY",reason="STATE_NOT_IN_UNIVERSE",amb="AMBIGUOUS",alts=["EXECUTE reading 'abiertos' as active contracts (en ejecución)"],conf="MEDIUM",
    notes="NEW_GAP: `open` is process-only (§19: no existe en contratos) and §19 says process states are never translated to contract states; no rule for an explicit CONTRACTS universe + process-only state without ACTOR. I clarify.")
uni("SF080","procesos","PROCESSES","RECORDS",["UNIVERSE_CHANGED"],extra_ops=[O("SET","MODALITY","solo de mínima cuantía","mínima cuantía","is")],extra_edit=lambda ir:add(ir,"MODALITY","is","mínima cuantía"),notes="Universe PROCESSES + named modality.")

# ---------- 081-083 view ----------
i="SF081"; ir=P(i); add(ir,"TIME","during_year","2025")
L(i,D,IN,[O("SET","TIME","solo 2025","2025","during_year",ys="STATED")],ir,[],EX,None,gaps=["PV"],echo=["CARRIED"],notes="F9: view was chosen by button (USER): not recalculated, stays CONTRACTS even with TIME.")
i="SF082"; ir=P(i); add(ir,"TIME","during_year","2025"); ir["product_view"]="HISTORICAL_PROCESSES"
L(i,D,IN,[O("SET","TIME","solo 2025","2025","during_year",ys="STATED")],ir,["VIEW_REDERIVED"],EX,None,gaps=["PV"],echo=["CARRIED","VIEW_DERIVED"],notes="F9: derived view recalculated: PV1 (TIME present) -> HISTORICAL_PROCESSES, announced.")
i="SF083"; ir=P(i); add(ir,"TIME","during_year","2025")
L(i,D,IN,[O("SET","TIME","solo 2025","2025","during_year",ys="STATED")],ir,[],EX,None,gaps=["PV"],echo=["CARRIED"],notes="Universe explicit PROCESSES (USER): not recalculated.")
i="SF084"; ir=P(i); ir["view"]="OPPORTUNITIES"; add(ir,"STATE","family","open")
L(i,D,IN,[O("SET_UNIVERSE","UNIVERSE","las convocatorias","PROCESSES (oportunidades)"),O("SET","STATE","que siguen recibiendo ofertas","open","family")],ir,["UNIVERSE_CHANGED"],EX,None,amb="AMBIGUOUS",
  alts=["STATE family open is redundant with the opportunity lens; only SET_UNIVERSE"],conf="MEDIUM",gaps=["UNIVERSE_FRAGMENT"],uni_note=True,
  notes="'convocatorias' heading = O7 opportunity lens (view OPPORTUNITIES); 'siguen recibiendo ofertas' cited -> STATE family open (§18 open class).")

# ---------- 085-108 value ----------
def val(i,quote,sides,final,eff=None,amb="CLEAR",alts=None,conf="HIGH",gaps=None,notes="",r=False,disp=EX,reason=None,echo=None):
    ir=P(i); setvalue(ir,final)
    ops=[]
    for slot,operator,vh,us in sides:
        ops.append(O("SET",slot,quote,vh,operator,r=r,dep=(us=="PREVIOUS_ACCEPTED_VALUE"),us=us))
    ech=echo
    if ech is None:
        ech=[] if disp!=EX else ["CARRIED"]
        if any(s[3] in("PREVIOUS_ACCEPTED_VALUE","SIBLING_IN_RANGE") for s in sides) and disp==EX: ech.append("UNIT_INHERITED")
        if eff and "VALUE_RANGE_REPLACED" in eff and disp==EX: ech.append("VALUE_RANGE_REPLACED")
    L(i,D,IN,ops,ir,eff or [],disp,reason,amb,alts,conf,gaps,notes,ech)
M="MILLONES"
val("SF085","los de más de 80 millones",[("VALUE.lower","greater_than","más de 80 millones","STATED")],[("greater_than","80 millones",M)],r=True,notes="Exclusive 'más de' -> greater_than.")
val("SF086","máximo 300 millones",[("VALUE.upper","less_or_equal","máximo 300 millones","STATED")],[("less_or_equal","300 millones",M)],notes="'máximo' inclusive (D4).")
val("SF087","entre 50 y 120 millones",[("VALUE.lower","greater_or_equal","50 millones","SIBLING_IN_RANGE"),("VALUE.upper","less_or_equal","120 millones","STATED")],[("greater_or_equal","50 millones",M),("less_or_equal","120 millones",M)],gaps=["B5RANGE","UNIT"],notes="§2.3: two SETs sharing the quote; F6 unit from sibling.")
val("SF088","mínimo 250 millones",[("VALUE.lower","greater_or_equal","mínimo 250 millones","STATED")],[("greater_or_equal","250 millones",M)],notes="Lower replaced (100 -> 250).")
val("SF089","hasta 400 millones",[("VALUE.upper","less_or_equal","hasta 400 millones","STATED")],[("greater_or_equal","100 millones",M),("less_or_equal","400 millones",M)],notes="F2: missing upper side completes the range.")
val("SF090","no más de 200 millones",[("VALUE.upper","less_or_equal","no más de 200 millones","STATED")],[("less_or_equal","200 millones",M)],eff=["VALUE_RANGE_REPLACED"],notes="F2: upper 200 < prior lower 500 -> empty; restriction replaced whole, announced.")
val("SF091","hasta 300 millones",[("VALUE.upper","less_or_equal","hasta 300 millones","STATED")],[("less_or_equal","300 millones",M)],eff=["VALUE_RANGE_REPLACED"],notes="F2: single-point range [300,300] -> no interior; replaced whole, announced.")
val("SF092","máximo 150 millones",[("VALUE.upper","less_or_equal","máximo 150 millones","STATED")],[("less_or_equal","150 millones",M)],notes="Upper replaced (200 -> 150).")
val("SF093","mayores de 50 millones",[("VALUE.lower","greater_than","mayores de 50 millones","STATED")],[("greater_than","50 millones",M),("less_or_equal","200 millones",M)],gaps=["COMPARATOR_FORM"],notes="'mayores de' read as greater_than (exclusive). Lower side completes the range (50,200].")
val("SF094","más de 400 millones",[("VALUE.lower","greater_than","más de 400 millones","STATED")],[("greater_than","400 millones",M)],eff=["VALUE_RANGE_REPLACED"],notes="F2: lower 400 > prior upper 200 -> empty; replaced whole.")
val("SF095","desde 200 millones",[("VALUE.lower","greater_or_equal","desde 200 millones","STATED")],[("greater_or_equal","200 millones",M)],eff=["VALUE_RANGE_REPLACED"],notes="F2: [200,200] single point -> replaced whole.")
val("SF096","mínimo 150 millones",[("VALUE.lower","greater_or_equal","mínimo 150 millones","STATED")],[("greater_or_equal","150 millones",M),("less_or_equal","300 millones",M)],notes="Lower replaced, upper kept.")
val("SF097","hasta 250 millones",[("VALUE.upper","less_or_equal","hasta 250 millones","STATED")],[("greater_or_equal","100 millones",M),("less_or_equal","250 millones",M)],notes="Upper replaced, lower kept.")
val("SF098","entre 400 y 600 millones",[("VALUE.lower","greater_or_equal","400 millones","SIBLING_IN_RANGE"),("VALUE.upper","less_or_equal","600 millones","STATED")],[("greater_or_equal","400 millones",M),("less_or_equal","600 millones",M)],eff=["VALUE_RANGE_REPLACED"],
    gaps=["B5RANGE","UNIT"],amb="AMBIGUOUS",alts=["No VALUE_RANGE_REPLACED effect (plain side-wise replacement)"],conf="MEDIUM",
    notes="Both prior bounds (100-300) replaced, so the whole restriction is substituted; I flag the effect per §2.3 ('se sustituyó entera'). Rule missing: whether a side-wise replacement of both sides counts as VALUE_RANGE_REPLACED.")
val("SF099","de más de 350 millones",[("VALUE.lower","greater_than","más de 350 millones","STATED")],[("greater_than","350 millones",M)],eff=["VALUE_RANGE_REPLACED"],notes="F2: lower 350 > prior upper 300 -> empty; replaced whole.")
val("SF100","cambia el valor a menos de 90 millones",[("VALUE.upper","less_than","menos de 90 millones","STATED")],[("less_than","90 millones",M)],eff=["VALUE_RANGE_REPLACED"],notes="F2: explicit change instruction replaces the whole restriction; lower bound's removal is not an op.")
val("SF101","mínimo 350",[("VALUE.lower","greater_or_equal","350 millones","PREVIOUS_ACCEPTED_VALUE")],[("greater_or_equal","350 millones",M)],gaps=["UNIT"],notes="F5: unit inherited (single active unit MILLONES) and announced.")
i="SF102"
L(i,D,IN,[O("SET","VALUE.lower","mínimo 400","400 (unidad por resolver)","greater_or_equal",dep=True,us=None)],None,[],"CLARIFY","UNIT_AMBIGUOUS",gaps=["UNIT"],echo=[],
  notes="F5: prior VALUE carries two units (millones and mil millones) -> no single active unit -> CLARIFY. resulting_ir null (unit not determinable).")
i="SF103"; ir=P(i); setvalue(ir,[("greater_or_equal","800 SMMLV","SMMLV")])
L(i,D,IN,[O("SET","VALUE.lower","mínimo 800","800 SMMLV","greater_or_equal",dep=True,us="PREVIOUS_ACCEPTED_VALUE")],ir,[],"CLARIFY","SMMLV_UNSUPPORTED",gaps=["SMMLV","UNIT"],echo=[],
  notes="F5 inherits SMMLV from prior; F14: SMMLV amounts are not executable -> CLARIFY SMMLV_UNSUPPORTED (provisional default).")
val("SF104","mínimo 650",[("VALUE.lower","greater_or_equal","650 pesos","PREVIOUS_ACCEPTED_VALUE")],[("greater_or_equal","650 pesos","PESOS")],gaps=["UNIT"],amb="AMBIGUOUS",alts=["CLARIFY: 650 pesos is implausible next to the prior 40.000.000 pesos"],conf="MEDIUM",
    notes="F5 applied literally: single prior unit PESOS inherited -> '650 pesos', announced. Implausible magnitude; no plausibility rule exists.")
i="SF105"; ir=P(i); setvalue(ir,[("greater_than","900 mil pesos","PESOS"),("less_or_equal","200 millones",M)])
ir["conditions"]=[ir["conditions"][0],ir["conditions"][1]]+sorted([c for c in ir["conditions"] if c["dimension"]=="VALUE"],key=lambda c:c["operator"]!="greater_than")
L(i,D,IN,[O("SET","VALUE.lower","más de 900 mil pesos","900 mil pesos","greater_than",us="STATED")],ir,[],EX,None,gaps=["UNIT"],echo=["CARRIED"],
  notes="Different unit stated on the new lower bound; scales compared by amount (900.000 pesos < 200 millones) so a valid interior exists; upper kept in its own unit.")
val("SF106","entre 450 y 900",[("VALUE.lower","greater_or_equal","450 millones","PREVIOUS_ACCEPTED_VALUE"),("VALUE.upper","less_or_equal","900 millones","PREVIOUS_ACCEPTED_VALUE")],[("greater_or_equal","450 millones",M),("less_or_equal","900 millones",M)],gaps=["B5RANGE","UNIT"],
    notes="F5: no unit stated for either bound; prior has a single unit (MILLONES) -> inherited for both, announced. Lower replaced (300 -> 450), upper completes.")
val("SF107","entre 200 y 500 millones",[("VALUE.lower","greater_or_equal","200 millones","SIBLING_IN_RANGE"),("VALUE.upper","less_or_equal","500 millones","STATED")],[("greater_or_equal","200 millones",M),("less_or_equal","500 millones",M)],gaps=["B5RANGE","UNIT"],notes="F6: unit from sibling.")
val("SF108","mínimo 2 mil millones",[("VALUE.lower","greater_or_equal","2 mil millones","STATED")],[("greater_or_equal","2 mil millones","MIL_MILLONES")],gaps=["UNIT"],notes="Unit stated (mil millones) differs from prior's; replaces lower.")

# ---------- 109-116 clear ----------
def clr(i,dim,slot,quote,notes,disp=EX,amb="CLEAR",alts=None,conf="HIGH",gaps=None,reason=None):
    ir=P(i); rm(ir,dim)
    L(i,D,IN,[O("CLEAR",slot,quote,None)],ir,[],disp,reason,amb,alts,conf,gaps,notes)
clr("SF109","TIME","TIME","sin importar el año","Cited removal of the year.")
clr("SF110","GEOGRAPHY","GEOGRAPHY","no importa el departamento","Cited removal of the department.")
clr("SF111","STATE","STATE","da igual el estado","Cited removal of the state.")
clr("SF112","BUYER","BUYER","de cualquier entidad","'cualquier entidad' cites removal of the buyer filter.")
i="SF113"
L(i,D,IN,[O("CLEAR","MODALITY","quita el filtro de modalidad",None)],P(i),[],EX,None,amb="AMBIGUOUS",alts=["CLARIFY/inform: there is no modality filter to remove"],conf="MEDIUM",
  notes="NEW_GAP: CLEAR on a slot that is empty in the anchor (no-op). I execute the anchor unchanged; the product should say there was nothing to remove.",echo=["CARRIED"])
clr("SF114","TIME","TIME","sin año","Removing the only condition leaves a bare CONTRACTS search (universe user-set).",amb="AMBIGUOUS",alts=["CLARIFY: refusing an unfiltered all-contracts search"],conf="MEDIUM")
clr("SF115","TOPIC","TOPIC","olvida el tema","Cited removal of the topic.")
clr("SF116","BUYER","BUYER","ya no importa el comprador","Removing the only condition leaves a bare CONTRACTS search.",amb="AMBIGUOUS",alts=["CLARIFY: refusing an unfiltered all-contracts search"],conf="MEDIUM")

# ---------- 117-122 order ----------
def ordr(i,q,vh,op,notes,extra_ops=None,extra_edit=None,r=True,amb="CLEAR",alts=None,conf="HIGH",echo=None):
    ir=P(i); ir["order"]={"operator":op,"polarity":"POSITIVE"}
    if extra_edit: extra_edit(ir)
    L(i,D,IN,[O("SET_ORDER","ORDER",q,vh,op,r=r)]+(extra_ops or []),ir,[],EX,None,amb,alts,conf,None,notes,echo)
ordr("SF117","ordénalos del más barato al más caro","más barato primero","value_asc","Ascending by value.")
ordr("SF118","los más antiguos primero","más antiguos primero","date_asc","Oldest first.")
ordr("SF119","por fecha, los más recientes primero","más recientes primero","date_desc","Replaces value_desc.")
ordr("SF120","del más caro al más barato","más caro primero","value_desc","Topic + order composed.",extra_ops=[O("SET","TOPIC","solo los que mencionen ambulancia","ambulancia","contains",r=True)],extra_edit=lambda ir:add(ir,"TOPIC","contains","ambulancia"))
LABELS["SF120"]["ops"]=[LABELS["SF120"]["ops"][1],LABELS["SF120"]["ops"][0]]
i="SF121"; ir=P(i); add(ir,"TOPIC","contains","ambulancia")
L(i,D,IN,[O("SET","TOPIC","solo los que mencionen ambulancia","ambulancia","contains",r=True)],ir,[],EX,None,notes="Topic added.")
ordr("SF122","de menor a mayor valor","menor a mayor valor","value_asc","Replaces date_desc.",r=False)

# ---------- 123-134 restatements ----------
i="SF123"; ir=P(i); add(ir,"GEOGRAPHY","is","Caribe","POSITIVE","REGION")
L(i,D,IN,[O("SET","GEOGRAPHY","la región Caribe","Caribe","is",level="REGION")],ir,[],EX,None,amb="AMBIGUOUS",alts=["User is switching territory: also CLEAR BUYER (Gobernación de Nariño is not in Caribe)"],conf="MEDIUM",gaps=["REGION_VOCAB"],echo=["CARRIED"],
  notes="Caribe is in the closed list. F1: buyer Gobernación de Nariño is not removed (no cited removal), so the result is likely empty; flagged.")
newsearch("SF124","CONTRACTS","RECORDS",[C("c1","TOPIC","contains","mantenimiento de ascensores"),C("c2","GEOGRAPHY","is","Caribe",level="REGION")],gaps=["REGION_VOCAB"],notes="Complete restatement; prior buyer/year dropped.")
i="SF125"; ir=P(i); rep(ir,"TIME","during_year","2023")
L(i,D,IN,[O("SET","TIME","en 2023","2023","during_year",ys="STATED")],ir,[],EX,None,notes="Year replaced.")
newsearch("SF126","UNSPECIFIED","RECORDS",[C("c1","TOPIC","contains","alquiler de maquinaria pesada"),C("c2","TIME","during_year","2023")],vs="DERIVED",pv="HISTORICAL_PROCESSES",echo=["VIEW_DERIVED"],gaps=["PV"],
  notes="Complete topic+year without universe noun -> UNSPECIFIED, PV1 -> HISTORICAL_PROCESSES, announced; buyer dropped.",amb="AMBIGUOUS",alts=["DELTA: topic and year replaced, buyer Alcaldía de Tunja and universe CONTRACTS carried"],conf="MEDIUM")
i="SF127"; ir=P(i); rep(ir,"BUYER","is","Gobernación del Cauca")
L(i,D,IN,[O("SET","BUYER","la Gobernación del Cauca","Gobernación del Cauca","is")],ir,[],EX,None,notes="Buyer replaced.")
newsearch("SF128","CONTRACTS","RECORDS",[C("c1","TOPIC","contains","transporte escolar"),C("c2","BUYER","is","Gobernación del Cauca")],notes="Complete restatement; differs from SF127 in dropping/replacing the topic.")
i="SF129"; ir=P(i); add(ir,"VALUE","greater_than","50 millones","POSITIVE",None,M)
L(i,D,IN,[O("SET","VALUE.lower","los que pasen de 50 millones","pasen de 50 millones","greater_than",r=True,us="STATED")],ir,[],EX,None,gaps=["COMPARATOR_FORM"],echo=["CARRIED"],notes="'pasen de' = exceeds -> greater_than.")
newsearch("SF130","CONTRACTS","RECORDS",[C("c1","TOPIC","contains","dotación de ambulancias"),C("c2","VALUE","greater_than","50 millones",unit=M)],notes="Complete restatement.")
i="SF131"
uni(i,"los procesos","PROCESSES","RECORDS",["UNIVERSE_CHANGED"],gaps=["UNIVERSE_FRAGMENT"],notes="'y los procesos?' switches the universe; same conditions.")
i="SF132"
L(i,D,IN,[O("KEEP","UNIVERSE","las licitaciones","PROCESSES","is")],P(i),[],EX,None,gaps=["UNIVERSE_FRAGMENT"],echo=["CARRIED"],
  notes="O6/D3: colloquial 'licitaciones' is the PROCESSES noun, NOT a MODALITY; universe already PROCESSES -> KEEP, no new condition.")
i="SF133"; ir=P(i); rep(ir,"TOPIC","contains","vigilancia")
L(i,D,IN,[O("SET","TOPIC","vigilancia","vigilancia","contains")],ir,["TOPIC_REPLACED"],EX,None,amb="AMBIGUOUS",alts=["NEW_SEARCH: bare topic -> UNSPECIFIED + Product View Policy (D2), dropping buyer Zipaquirá"],conf="MEDIUM",gaps=["TOPIC"],echo=["CARRIED","TOPIC_REPLACED"],
  notes="F3: a new TOPIC replaces the previous one. Rule missing: bare noun after a search = topic replace vs new search.")
newsearch("SF134","CONTRACTS","RECORDS",[C("c1","TOPIC","contains","vigilancia")],gaps=["TOPIC"],notes="Universe noun + topic: self-contained; buyer dropped.")

# ---------- 135-144 state/ttl/owner ----------
i="SF135"; ir=P(i); rep(ir,"TIME","during_year","2024")
L(i,D,IN,[O("SET","TIME","en 2024","2024","during_year",ys="STATED")],ir,[],EX,None,amb="AMBIGUOUS",alts=["REASK/CLARIFY: the previous search was never executed"],conf="MEDIUM",
  notes="NEW_GAP: prior.state NOT_EXECUTED (anchor accepted but never shown) is not covered by F1-F15. I continue from the accepted IR.")
i="SF136"; ir=P(i); add(ir,"VALUE","greater_than","100 millones","POSITIVE",None,M)
L(i,D,IN,[O("SET","VALUE.lower","los que superen los 100 millones","superen los 100 millones","greater_than",r=True,us="STATED")],ir,[],EX,None,amb="AMBIGUOUS",alts=["REASK/CLARIFY: the previous search was never executed"],conf="MEDIUM",gaps=["COMPARATOR_FORM"],
  notes="NEW_GAP: prior.state NOT_EXECUTED not covered. 'superen' = exceeds -> greater_than.")
i="SF137"
ir=P(i); add(ir,"TIME","during_year","2024"); ir["product_view"]="HISTORICAL_PROCESSES"
L(i,"UNCLEAR","OUT_ANCHOR_NOT_SEARCH",[O("SET","TIME","en 2024","2024","during_year",ys="STATED")],ir,[],"CLARIFY","NO_ANCHOR",amb="AMBIGUOUS",alts=["NEW_SEARCH with only a year (all processes 2024)"],conf="MEDIUM",
  notes="No previous search to continue (state NONE). A bare year is not a meaningful search; ask what to search. resulting_ir shows the degenerate TIME-only search.")
i="SF138"; ir=P(i); rep(ir,"TIME","during_year","2024")
L(i,D,IN,[O("SET","TIME","en 2024","2024","during_year",ys="STATED")],ir,[],"CLARIFY","CONTEXT_EXPIRED_CONFIRM",echo=["CONTEXT_EXPIRED"],gaps=["TTL"],notes="F11: 1860 s > 1800 s (just over 30 min) -> confirm continue-or-reset.")
i="SF139"; ir=P(i); rep(ir,"GEOGRAPHY","is","Palmira","POSITIVE","MUNICIPALITY")
L(i,D,IN,[O("SET","GEOGRAPHY","los de Palmira","Palmira","is",level="MUNICIPALITY",r=True)],ir,[],"CLARIFY","CONTEXT_EXPIRED_CONFIRM",amb="AMBIGUOUS",alts=["BUYER Alcaldía de Palmira"],conf="MEDIUM",echo=["CONTEXT_EXPIRED"],gaps=["TTL","GEOBUYER"],
  notes="F11: 18000 s -> confirmation. If continued: GEOGRAPHY replaced Cali -> Palmira (D7 dominant reading).")
i="SF140"; ir=P(i); ir["order"]={"operator":"date_desc","polarity":"POSITIVE"}
L(i,D,IN,[O("SET_ORDER","ORDER","ordénalos por fecha","por fecha (más recientes primero)","date_desc",r=True)],ir,[],"CLARIFY","CONTEXT_EXPIRED_CONFIRM",amb="AMBIGUOUS",alts=["date_asc (oldest first)"],conf="MEDIUM",echo=["CONTEXT_EXPIRED"],gaps=["TTL"],
  notes="F11: 46800 s -> confirmation. NEW_GAP: 'por fecha' with no direction; default most-recent-first assumed.")
i="SF141"; ir=P(i); rep(ir,"TIME","during_year","2024")
L(i,D,IN,[O("SET","TIME","en 2024","2024","during_year",ys="STATED")],ir,[],EX,None,gaps=["TTL"],echo=["CARRIED"],notes="F11 boundary: 1740 s < 1800 s -> still a continuation.")
i="SF142"
L(i,"UNCLEAR","OUT_ANCHOR_NOT_SEARCH",[O("SET","GEOGRAPHY","en Bogotá","Bogotá","is",level="MUNICIPALITY")],None,[],"CLARIFY","ANCHOR_UNREADABLE",conf="MEDIUM",
  notes="The anchor IR is unreadable (prior.ir null), so there is nothing to compose; ask what to search in Bogotá. resulting_ir null.")
i="SF143"; ir=P(i); rep(ir,"TIME","during_year","2024")
L(i,D,"OUT_ANCHOR_NOT_SEARCH",[O("SET","TIME","en 2024","2024","during_year",ys="STATED")],ir,[],"HANDOFF_LEGACY","ANCHOR_OWNER_LEGACY:COUNT",echo=[],notes="Rule 7: owner LEGACY:COUNT. resulting_ir still labeled (COUNT with year replaced).")
i="SF144"; ir=P(i); add(ir,"TIME","during_year","2024")
L(i,D,"OUT_ANCHOR_NOT_SEARCH",[O("SET","TIME","los de 2024","2024","during_year",r=True,ys="STATED")],ir,[],"HANDOFF_LEGACY","ANCHOR_OWNER_LEGACY:RANK_SUPPLIERS",echo=[],notes="Rule 7: owner LEGACY:RANK_SUPPLIERS. resulting_ir: ranking restricted to 2024.")

# ---------- 145-154 protocol / buttons / ops ----------
for i,q in [("SF145","muéstrame los siguientes"),("SF146","página siguiente"),("SF147","dame otros diez más"),("SF148","sigue mostrando más")]:
    L(i,"PROTOCOL","OUT_OPERATION_CHANGE",[],P(i),[],EX,None,echo=[],conf=("MEDIUM" if i=="SF147" else "HIGH"),
      amb=("AMBIGUOUS" if i=="SF147" else "CLEAR"),alts=(["DELTA: top 10 more via CARDINALITY"] if i=="SF147" else []),
      notes="Pagination (layer 1, deterministic); search IR unchanged. NEW_GAP: no m2a_scope value for PROTOCOL turns; I use OUT_OPERATION_CHANGE (PROTOCOL is a distinct operation, §2).")
i="SF149"; ir=P(i); ir["order"]={"operator":"value_desc","polarity":"POSITIVE"}
L(i,D,IN,[O("SET_ORDER","ORDER","Mayor valor","mayor valor","value_desc")],ir,[],EX,None,echo=["CARRIED"],notes="Button label [Mayor valor] (ADR-103 enm.3: M2 delta). Event, not free text.")
i="SF150"; ir=P(i); ir["order"]={"operator":"date_desc","polarity":"POSITIVE"}
L(i,D,IN,[O("SET_ORDER","ORDER","Más recientes","más recientes","date_desc")],ir,[],EX,None,echo=["CARRIED"],notes="Button label for most recent first.")
i="SF151"; ir=P(i); rm(ir,"BUYER")
L(i,D,IN,[O("CLEAR","BUYER","Quitar: Alcaldía de Soledad",None)],ir,[],EX,None,echo=["CARRIED"],notes="Removal chip: cited removal of the buyer.")
i="SF152"; ir=P(i); rm(ir,"TIME")
L(i,D,IN,[O("CLEAR","TIME","Quitar filtro de 2025",None)],ir,[],EX,None,echo=["CARRIED"],notes="Removal chip: cited removal of the year.")
i="SF153"; ir=P(i); ir["operation"]="FOLLOW"
L(i,"OTHER_OPERATION","OUT_OPERATION_CHANGE",[],ir,[],"HANDOFF_LEGACY","OPERATION_SWITCH:FOLLOW",gaps=["OPSWITCH"],echo=[],notes="FOLLOW (Radar) over the current search conditions.")
i="SF154"; ir=P(i); ir["operation"]="FOLLOW"; ir["universe"]="PROCESSES"
L(i,"OTHER_OPERATION","OUT_OPERATION_CHANGE",[],ir,[],"HANDOFF_LEGACY","OPERATION_SWITCH:FOLLOW",amb="AMBIGUOUS",alts=["FOLLOW keeping universe CONTRACTS"],conf="MEDIUM",gaps=["OPSWITCH"],echo=[],
  notes="FOLLOW over new processes: 'procesos nuevos así' names PROCESSES with the same conditions.")

# ---------- 155-162 compound ----------
i="SF155"; ir=P(i); add(ir,"GEOGRAPHY","is","Cali","POSITIVE","MUNICIPALITY"); add(ir,"TIME","during_year","2024")
L(i,D,IN,[O("SET","GEOGRAPHY","en Cali","Cali","is",level="MUNICIPALITY"),O("SET","TIME","solo 2024","2024","during_year",ys="STATED")],ir,[],EX,None,amb="AMBIGUOUS",
  alts=["Switch territory: replace BUYER Alcaldía de Bello with Cali (CLEAR BUYER)"],conf="MEDIUM",echo=["CARRIED"],
  notes="F1: buyer Alcaldía de Bello kept (no cited removal), so GEOGRAPHY Cali + buyer Bello is likely empty; flagged.")
i="SF156"; ir=P(i); rep(ir,"BUYER","is","Gobernación de Risaralda"); rm(ir,"TIME")
L(i,D,IN,[O("SET","BUYER","la Gobernación de Risaralda","Gobernación de Risaralda","is"),O("CLEAR","TIME","sin importar el año",None)],ir,[],EX,None,echo=["CARRIED"],notes="Buyer replaced and year removed (cited).")
i="SF157"; ir=P(i); add(ir,"VALUE","greater_than","100 millones","POSITIVE",None,M); add(ir,"TIME","during_year","2023")
L(i,D,IN,[O("SET","VALUE.lower","más de 100 millones","más de 100 millones","greater_than",us="STATED"),O("SET","TIME","de 2023","2023","during_year",ys="STATED")],ir,[],EX,None,echo=["CARRIED"],
  notes="F12 not applicable: prior has BUYER, no ACTOR; 'con' introduces a value.")
i="SF158"; ir=P(i); rm(ir,"GEOGRAPHY"); add(ir,"BUYER","is","Universidad de Antioquia")
L(i,D,IN,[O("CLEAR","GEOGRAPHY","olvida el lugar",None),O("SET","BUYER","los de la Universidad de Antioquia","Universidad de Antioquia","is",r=True)],ir,[],EX,None,echo=["CARRIED"],notes="Place removed (cited); institutional buyer added.")
i="SF159"; ir=P(i); add(ir,"STATE","is","awarded"); add(ir,"GEOGRAPHY","is","Magdalena Medio","POSITIVE","REGION")
L(i,D,IN,[O("SET","STATE","solo los adjudicados","awarded","is",r=True),O("SET","GEOGRAPHY","del Magdalena Medio","Magdalena Medio","is",level="REGION")],ir,[],"CLARIFY","REGION_NOT_IN_LIST",gaps=["REGION_VOCAB"],echo=[],
  notes="F13: Magdalena Medio is not in the closed region list (Amazonia, Andina, Orinoquia, Caribe, Pacifica, Insular, Eje Cafetero, Santanderes, Tolima Grande) -> CLARIFY. resulting_ir shows the best reading.")
i="SF160"; ir=P(i); add(ir,"MODALITY","is","mínima cuantía"); ir["order"]={"operator":"date_desc","polarity":"POSITIVE"}
L(i,D,IN,[O("SET","MODALITY","de mínima cuantía","mínima cuantía","is"),O("SET_ORDER","ORDER","los más recientes","más recientes","date_desc",r=True)],ir,[],EX,None,echo=["CARRIED"],notes="Named modality + order. (C8: 'mayor cuantía' would be ORDER; 'mínima cuantía' is a modality.)")
i="SF161"; ir=P(i); add(ir,"TIME","relative_period","este año"); ir["order"]={"operator":"value_desc","polarity":"POSITIVE"}; ir["cardinality"]={"operator":"top_n","value_hint":"5"}
L(i,D,IN,[O("SET_ORDER","ORDER","los 5 más caros","más caros","value_desc",r=True),O("SET","CARDINALITY","5","5","top_n",r=True),O("SET","TIME","de este año","este año (2026)","relative_period",ys="CLOCK_INFERRED")],ir,[],EX,None,echo=["CARRIED"],
  notes="Order + top 5 + relative window resolved on 2026-10-01.")
i="SF162"; ir=P(i); add(ir,"GEOGRAPHY","is","Soledad","NEGATIVE","MUNICIPALITY"); add(ir,"TIME","during_year","2024")
L(i,D,IN,[O("SET","GEOGRAPHY","sin los de Soledad","Soledad","is","NEGATIVE","MUNICIPALITY",r=True),O("SET","TIME","en 2024","2024","during_year",ys="STATED")],ir,[],EX,None,amb="AMBIGUOUS",
  alts=["BUYER NEGATIVE Alcaldía de Soledad","Negative Soledad replaces the positive Atlántico (single GEOGRAPHY slot)"],conf="MEDIUM",gaps=["GEOBUYER","SET"],echo=["CARRIED"],
  notes="Soledad is a municipality inside the positive department Atlántico: exclusion coexists with the positive department (different value). Rule missing: negative on an occupied slot with a different value (coexist vs replace).")

# ---------- assemble ----------
def echo_auto(gid,lab):
    if lab["expected_echo"] is not None: return lab["expected_echo"]
    if lab["expected_disposition"]!="EXECUTE": return []
    pir=BY[gid]["prior"]["ir"]; rir=lab["resulting_ir"]
    e=[]
    key=lambda c:(c["dimension"],c["operator"],c["polarity"],c["value_hint"],c["level"],c["unit"])
    pk={key(c) for c in pir["conditions"]}
    if any(key(c) in pk for c in rir["conditions"]) or (rir["order"] and rir["order"]==pir["order"]) or (rir["cardinality"] and rir["cardinality"]==pir["cardinality"]): e.append("CARRIED")
    ef=lab["effects"]
    if "TOPIC_REPLACED" in ef: e.append("TOPIC_REPLACED")
    if "VALUE_RANGE_REPLACED" in ef: e.append("VALUE_RANGE_REPLACED")
    if "VIEW_REDERIVED" in ef: e.append("VIEW_DERIVED")
    ys=[o["inherited"]["year_source"] for o in lab["ops"]]; us=[o["inherited"]["unit_source"] for o in lab["ops"]]
    if any(y in("PREVIOUS_ACCEPTED_TIME","CLOCK_INFERRED") and o["operator"]=="during_month" for y,o in zip(ys,lab["ops"])): e.append("YEAR_INHERITED")
    if any(u in("PREVIOUS_ACCEPTED_VALUE","SIBLING_IN_RANGE") for u in us): e.append("UNIT_INHERITED")
    return e
out=[]
for c in CASES:
    g=c["gold_id"]; lab=copy.deepcopy(LABELS[g])
    lab["expected_echo"]=echo_auto(g,lab)
    lab.pop("_echo_x")
    rec=copy.deepcopy(c)
    for k in ["turn_kind","m2a_scope","ops","resulting_ir","effects","expected_disposition","disposition_reason","expected_echo","ambiguity","alternatives","confidence","gap_codes","labeler","notes"]:
        rec[k]=lab[k]
    out.append(rec)
out.sort(key=lambda r:r["gold_id"])
missing=[c["gold_id"] for c in CASES if c["gold_id"] not in LABELS]
assert not missing,missing
import os
with open(BASE+"synth_labeler_SA/labels_v2.jsonl","w") as f:
    for r in out: f.write(json.dumps(r,ensure_ascii=False)+"\n")
print(len(out))
