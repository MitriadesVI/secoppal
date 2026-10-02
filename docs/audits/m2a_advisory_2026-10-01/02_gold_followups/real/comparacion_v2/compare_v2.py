import json, collections, pathlib, sys, unicodedata
base = pathlib.Path(__file__).resolve().parent.parent
da, db, out = sys.argv[1], sys.argv[2], sys.argv[3]
def load(d): return {r["gold_id"]: r for r in map(json.loads, (base/d/"labels_v2.jsonl").read_text().splitlines()) if r}
A, B = load(da), load(db)
def fold(s):
    s = unicodedata.normalize("NFKD", str(s or "").lower()); return " ".join("".join(c for c in s if not unicodedata.combining(c)).split())
def ir_struct(ir):
    if not ir: return None
    conds = sorted((c.get("dimension"), c.get("operator"), c.get("polarity")) for c in ir.get("conditions") or [])
    o = ir.get("order"); k = ir.get("cardinality")
    return json.dumps({"op": ir.get("operation"), "universe": ir.get("universe"), "conds": conds,
                       "order": (o or {}).get("operator") if isinstance(o, dict) else o,
                       "card": bool(k)}, sort_keys=True)
def ir_values(ir):
    if not ir: return None
    return sorted((c.get("dimension"), c.get("operator"), fold(c.get("value_hint"))) for c in ir.get("conditions") or [])
def sig(r):
    ops = r.get("ops") or []
    return {"turn_kind": r.get("turn_kind"), "m2a_scope": r.get("m2a_scope"),
            "expected_disposition": r.get("expected_disposition"),
            "disposition_reason_class": fold(r.get("disposition_reason")).split(":")[0][:12],
            "prior_owner": (r.get("prior") or {}).get("owner"),
            "prior_ir_struct": ir_struct((r.get("prior") or {}).get("ir")),
            "ops": sorted((o.get("op"), o.get("slot")) for o in ops),
            "resulting_ir_struct": ir_struct(r.get("resulting_ir")),
            "resulting_ir_values": ir_values(r.get("resulting_ir")),
            "effects": sorted(r.get("effects") or []),
            "expected_echo": sorted(r.get("expected_echo") or [])}
fields = ["turn_kind","m2a_scope","expected_disposition","prior_owner","prior_ir_struct","ops","resulting_ir_struct","resulting_ir_values","effects","expected_echo"]
cnt = collections.Counter(); ids_by = collections.defaultdict(list); rows = []
ids = sorted(set(A) & set(B))
for i in ids:
    a, b = sig(A[i]), sig(B[i])
    d = [f for f in fields if a[f] != b[f]]
    for f in d: cnt[f] += 1; ids_by[f].append(i)
    if d: rows.append({"gold_id": i, "fields": d, "A": {f: a[f] for f in d if f not in ("resulting_ir_values",)}, "B": {f: b[f] for f in d if f not in ("resulting_ir_values",)}})
primary = ["expected_disposition", "resulting_ir_struct"]
pd = sorted({r["gold_id"] for r in rows if set(r["fields"]) & set(primary)})
o = {"n_A": len(A), "n_B": len(B), "n_common": len(ids), "by_field": dict(cnt), "ids_by_field": dict(ids_by),
     "primary": primary, "primary_disagreement_ids": pd, "primary_agreement": round(1 - len(pd)/max(1, len(ids)), 4)}
(base/out).mkdir(exist_ok=True)
(base/out/"summary.json").write_text(json.dumps(o, ensure_ascii=False, indent=1))
(base/out/"disagreements.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows))
print(json.dumps({k: v for k, v in o.items() if k != "ids_by_field"}, ensure_ascii=False, indent=1))
for f in ("expected_disposition","turn_kind","m2a_scope","prior_owner","resulting_ir_struct","ops"): print(f, ids_by.get(f))
for r in rows:
    if r["gold_id"] in pd and "expected_disposition" in r["fields"]: print(r["gold_id"], r["A"].get("expected_disposition"), r["B"].get("expected_disposition"))
