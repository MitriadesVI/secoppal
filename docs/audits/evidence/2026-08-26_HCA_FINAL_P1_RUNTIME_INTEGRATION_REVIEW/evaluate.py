from pathlib import Path
import json
import re

ROOT = Path(__file__).parent
def read(path):
    return json.loads((ROOT / path).read_text())

summary = {"corpora": {}, "gates": {}, "details": {}}
for name in ("hca", "hca-final-p1", "negation-v1", "time-v2", "control-language-v1", "actor-hygiene-v1"):
    b, f = [read(f"{version}-{name}/verdicts.json") for version in ("base", "feature")]
    vb, vf = [{v["id"]: v for v in data["verdicts"]} for data in (b, f)]
    assert vb.keys() == vf.keys()
    broken = [k for k in vb if vb[k]["status"] == "PASS" and vf[k]["status"] != "PASS"]
    fixed = [k for k in vb if vb[k]["status"] != "PASS" and vf[k]["status"] == "PASS"]
    summary["corpora"][name] = {"base": {k: b[k] for k in ("cases", "pass", "fail", "error")},
        "feature": {k: f[k] for k in ("cases", "pass", "fail", "error")},
        "fixed": fixed, "broken": broken, "same_sha": b["corpus_sha256"] == f["corpus_sha256"]}
    summary["gates"][name + "_no_regressions"] = not broken and f["error"] == 0
    if name == "hca":
        summary["hca_failures"] = [{"id": v["id"], "fails": v["fails"]} for v in f["verdicts"] if v["status"] != "PASS"]

b, f = [read(version + "-source-evidence.json") for version in ("base", "feature")]
tb, tf = [{(t["id"], t["turn"]): t for t in data["turns"]} for data in (b, f)]
def last_query(t):
    return [s for s in t["source_calls"] if s["kind"] == "query"][-1]
def params(k):
    return tf[(k, 1)]["params"] or {}
def calls_semantic(calls):
    # Actor aggregates execute concurrently; only scheduling order may differ.
    return {"queries": [c for c in calls if c["kind"] != "aggregate"],
            "aggregates": sorted(json.dumps(c, sort_keys=True, ensure_ascii=False) for c in calls if c["kind"] == "aggregate")}
def unchanged(k):
    def semantic(value):
        if isinstance(value, dict):
            return {key: semantic(v) for key, v in value.items() if key != "selection_token"}
        if isinstance(value, list):
            return [semantic(v) for v in value]
        return value
    differences = []
    for key in tb:
        if key[0] != k:
            continue
        for field in ("params", "needs_clarification", "source_calls"):
            if semantic(tb[key][field]) != semantic(tf[key][field]):
                differences.append({"turn": key[1], "field": field, "base": tb[key][field], "feature": tf[key][field]})
    return differences

boundary_ids = ["HCA-STATE-003", "HCA-SOURCE-002", "HCA-ENTITY-004", "HCA-ACTOR-004", "HCA-TOPIC-007",
    "HCA-ENTITY-001", "HCA-FOLLOWUP-007", "HCA-META-001B", "HCA-TOPIC-012", "HCA-MODALITY-002",
    "HCA-MODALITY-012", "HCA-TYPO-003", "BUYER-ACTOR-PREEXISTING", "BUYER-FONDO-PREEXISTING",
    "MONEY-REFERENCE", "STATE-OPPORTUNITY"]
for k in boundary_ids:
    delta = unchanged(k)
    summary["details"][k] = delta
    summary["gates"][k + "_unchanged"] = not delta

for k, amount in (("HCA-MONEY-020", 100000), ("HCA-MONEY-026", 1000000), ("MONEY-ACCENT", 1000000),
                  ("MONEY-2M", 2000000), ("MONEY-500M", 500000000)):
    summary["gates"][k] = params(k).get("valor_min") == amount and f"valor_del_contrato >= {amount}" in last_query(tf[k, 1])["where"]
for k in ("MONEY-YEAR", "MONEY-NIT", "MONEY-COUNT", "MONEY-BARE", "MONEY-BARE-EXACT"):
    summary["gates"][k] = not any(params(k).get(x) for x in ("valor_min", "valor_max"))
for k in ("MONEY-HALF", "MONEY-HALF-EXACT"):
    t = tf[k, 1]
    summary["gates"][k] = bool(t["needs_clarification"]) and not t["source_calls"] and not any(params(k).get(x) for x in ("valor_min", "valor_max"))
    summary["details"][k] = t
for k in ("BUYER-COMPLETE", "BUYER-LOCAL", "BUYER-TOPIC"):
    summary["gates"][k] = bool(params(k).get("entidad_resolved")) and bool(tf[k, 1]["source_calls"]) and not tf[k, 1]["needs_clarification"]
summary["gates"]["HCA-ENTITY-005"] = bool(tf["HCA-ENTITY-005", 1]["needs_clarification"]) and not tf["HCA-ENTITY-005", 1]["source_calls"] and "alcaldia" not in params("HCA-ENTITY-005").get("objeto", [])
for k in ("HCA-STATE-005", "HCA-STATE-007", "STATE-OPPORTUNITY", "STATE-DESIERTOS"):
    summary["details"][k + "_source"] = {v: last_query(ts[k, 1]) for v, ts in (("base", tb), ("feature", tf))}
summary["gates"]["HCA-STATE-005"] = "estado_del_procedimiento = 'Publicado'" in last_query(tf["HCA-STATE-005", 1])["where"] and all(v not in last_query(tf["HCA-STATE-005", 1])["where"] for v in ("Borrador", "En aprobación", "Aprobado"))
summary["gates"]["HCA-STATE-007"] = params("HCA-STATE-007").get("estado_family") == "contrato_activo" and "estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')" in last_query(tf["HCA-STATE-007", 1])["where"]
summary["gates"]["state_catalog_identical"] = b["estado_families"] == f["estado_families"]
summary["gates"]["state_projection_property"] = len(f["state_properties"]) == 11 and all(p["pass"] for p in f["state_properties"])
summary["gates"]["desiertos_same_root"] = tb["STATE-DESIERTOS", 1]["params"].get("estado_family") == "no_procede" and "estado_del_procedimiento IN ('Cancelado', 'Suspendido')" in last_query(tf["STATE-DESIERTOS", 1])["where"]
for k in ("HCA-FOLLOWUP-016", "FOLLOWUP-ICBF", "FOLLOWUP-SENA"):
    before, after = [last_query(tf[k, n]) for n in (1, 2)]
    summary["gates"][k + "_where"] = before["where"] == after["where"] and before["soql"] != after["soql"]
    summary["details"][k] = {v: [last_query(ts[k, n]) for n in (1, 2)] for v, ts in (("base", tb), ("feature", tf))}
    if k != "HCA-FOLLOWUP-016":
        summary["gates"][k + "_explicit"] = k.split("-")[-1] in after["where"]
summary["gates"]["followup_obra_money_cali"] = all(x in last_query(tf["HCA-FOLLOWUP-016", 2])["where"] for x in ("Cali", "300000000", "tipo_de_contrato = 'Obra'"))
summary["gates"]["followup_persisted_derived_absent"] = "entidad_ecosystem" in tb["HCA-FOLLOWUP-016", 1]["persisted"] and "entidad_ecosystem" not in tf["HCA-FOLLOWUP-016", 1]["persisted"]

summary["modality_deltas"] = []
state_fields = {"estado_contrato", "estado_contrato_adicional", "estado_del_procedimiento", "estado_de_apertura_del_proceso"}
for mb, mf in zip(b["modality"], f["modality"]):
    assert mb["q"] == mf["q"]
    changes = {k: {"base": mb[k], "feature": mf[k]} for k in mb if mb[k] != mf[k]}
    param_changes = {k: [mb["params"].get(k), mf["params"].get(k)] for k in mb["params"].keys() | mf["params"].keys() if mb["params"].get(k) != mf["params"].get(k)}
    if changes:
        summary["modality_deltas"].append({"q": mb["q"], "fields": list(changes), "param_changes": param_changes,
            "source_before": mb["soql"], "source_after": mf["soql"]})
    normalized_source = mf["soql"].replace("estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')", "estado_contrato = 'En ejecución'")
    summary["gates"]["modality:" + mb["q"]] = all(mb[k] == mf[k] for k in ("interpretation", "needs_llm", "route_reason")) and set(param_changes).issubset(state_fields) and mb["soql"] == normalized_source

summary["gates"]["query_plan_loc"] = b["query_plan_loc"] == f["query_plan_loc"] == 299
summary["source_behavior_deltas_all_hca"] = []
for key in tb:
    if not key[0].startswith("HCA-"):
        continue
    if calls_semantic(tb[key]["source_calls"]) != calls_semantic(tf[key]["source_calls"]) or tb[key]["needs_clarification"] != tf[key]["needs_clarification"]:
        summary["source_behavior_deltas_all_hca"].append({"id": key[0], "turn": key[1]})
summary["failed_gates"] = [k for k, v in summary["gates"].items() if not v]
(ROOT / "comparison.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1))
print(json.dumps({"corpora": summary["corpora"], "failed_gates": summary["failed_gates"], "modality_deltas": [{k: v for k, v in d.items() if not k.startswith("source_")} for d in summary["modality_deltas"]]}, ensure_ascii=False, indent=1))
