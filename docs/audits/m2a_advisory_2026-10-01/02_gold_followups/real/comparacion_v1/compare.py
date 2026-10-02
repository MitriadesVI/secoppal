import json, collections, pathlib
base = pathlib.Path(__file__).resolve().parent.parent
def load(n):
    return {r["gold_id"]: r for r in map(json.loads, (base/f"labeler_{n}"/"labels.jsonl").read_text().splitlines()) if r}
A, B = load("A"), load("B")
ids = sorted(set(A) | set(B))
def norm(s): return " ".join((s or "").lower().split())
def sig(r):
    d = r.get("deltas") or []
    return {
        "turn_kind": r.get("turn_kind"),
        "expected_disposition": r.get("expected_disposition"),
        "prior_text": norm(r.get("prior_text")),
        "followup_text": norm(r.get("followup_text")),
        "delta_dim_op": sorted({(x.get("dimension"), x.get("delta_op")) for x in d}),
        "delta_dims": sorted({x.get("dimension") for x in d}),
        "delta_operators": sorted({(x.get("dimension"), x.get("operator")) for x in d}),
        "polarity": sorted({(x.get("dimension"), x.get("polarity")) for x in d}),
        "needs_referent": any(x.get("needs_referent") for x in d),
        "resulting_ir_null": r.get("resulting_ir") is None,
        "resulting_operation": (r.get("resulting_ir") or {}).get("operation"),
    }
fields = ["turn_kind","expected_disposition","prior_text","followup_text","delta_dims","delta_dim_op","delta_operators","polarity","needs_referent","resulting_ir_null","resulting_operation"]
count = collections.Counter(); rows = []; by_field = collections.defaultdict(list)
for i in ids:
    if i not in A or i not in B:
        rows.append({"gold_id": i, "missing_in": "A" if i not in A else "B"}); continue
    a, b = sig(A[i]), sig(B[i])
    diff = {f: {"A": a[f], "B": b[f]} for f in fields if a[f] != b[f]}
    for f in diff: count[f] += 1; by_field[f].append(i)
    if diff:
        rows.append({"gold_id": i, "fields": sorted(diff), "detail": {k: v for k, v in diff.items() if k not in ("prior_text","followup_text")},
                     "confidence": {"A": A[i].get("confidence"), "B": B[i].get("confidence")}})
core = ["turn_kind","expected_disposition","delta_dim_op"]
core_dis = sorted({r["gold_id"] for r in rows if set(r.get("fields", [])) & set(core)})
out = {"n_A": len(A), "n_B": len(B), "n_common": len(set(A)&set(B)),
       "records_with_any_disagreement": len(rows), "by_field": dict(count),
       "ids_by_field": {k: v for k, v in by_field.items()},
       "core_fields": core, "core_disagreement_ids": core_dis, "core_agreement": 1 - len(core_dis)/max(1,len(set(A)&set(B)))}
(base/"comparison"/"disagreements.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows))
(base/"comparison"/"summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
print(json.dumps(out, ensure_ascii=False, indent=1))
for r in rows:
    if r["gold_id"] in core_dis: print(r["gold_id"], json.dumps({k: r["detail"][k] for k in core if k in r["detail"]}, ensure_ascii=False))
