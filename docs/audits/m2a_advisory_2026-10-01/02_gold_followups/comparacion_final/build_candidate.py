import json, sys, hashlib, pathlib, collections
fa, fb, summ, out_jsonl, out_index, slice_name = sys.argv[1:7]
L = lambda p: {r["gold_id"]: r for r in map(json.loads, open(p).read().splitlines()) if r}
A, B = L(fa), L(fb); S = json.load(open(summ)); dis = set(S["primary_disagreement_ids"])
rows, idx, c = [], [], collections.Counter()
for i in sorted(A):
    a, b = A[i], B[i]
    amb = sorted({x for r, x in ((a, "A"), (b, "B")) if r.get("ambiguity") == "AMBIGUOUS"})
    gaps = sorted(set(a.get("gap_codes") or []) | set(b.get("gap_codes") or []))
    status = "DISPUTED_PRIMARY" if i in dis else ("AGREED_AMBIGUOUS" if amb else "AGREED")
    scope = a.get("m2a_scope") if a.get("m2a_scope") == b.get("m2a_scope") else f'{a.get("m2a_scope")}|{b.get("m2a_scope")}'
    c[status] += 1
    rows.append({"gold_id": i, "slice": slice_name, "candidate_status": status, "m2a_scope": scope,
                 "expected_disposition": a.get("expected_disposition") if a.get("expected_disposition") == b.get("expected_disposition") else None,
                 "ambiguous_by": amb, "gap_codes_union": gaps, "label_A": a, "label_B": b})
    idx.append({"gold_id": i, "candidate_status": status, "m2a_scope": scope,
                "disposition_A": a.get("expected_disposition"), "disposition_B": b.get("expected_disposition"),
                "ambiguous_by": amb, "gap_codes_union": gaps})
body = "\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows)
pathlib.Path(out_jsonl).write_text(body)
pathlib.Path(out_index).write_text(json.dumps({"slice": slice_name, "n": len(rows), "status": dict(c),
    "sha256_candidate": hashlib.sha256(body.encode()).hexdigest(), "sealed": False,
    "note": "Candidato NO sellado. Conserva las dos etiquetas por registro; ninguna discrepancia fue reconciliada.",
    "records": idx}, ensure_ascii=False, indent=1))
ins = [r for r in rows if r["m2a_scope"] in ("IN", "IN_NEW_SEARCH", "IN_PROTOCOL")]
print(slice_name, dict(c), "| en alcance:", len(ins), collections.Counter(r["candidate_status"] for r in ins))
