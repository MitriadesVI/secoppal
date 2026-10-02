#!/usr/bin/env python3
"""Word 3-gram Jaccard of each synthetic follow-up vs every REAL Gold text (private, in memory only) and vs the
public Gold jsonl (informational + also gated). Writes only scores and gold_ids, never REAL text."""
import json, os, re, unicodedata, sys, glob
OUT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REAL = os.path.expanduser("~/Documents/tools/secoppal-diagnostics/m1_search_gold/sealed_v1/gold_v1.jsonl")
PUB = "/Users/rodrigoortiz/Documents/secoppal-m1/docs/audits/m1_search_foundation/gold"
THR, N = 0.6, 3

def norm(s):
    s = unicodedata.normalize("NFD", s.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.findall(r"[a-z0-9ñ]+", s)
def grams(s):
    w = ["<s>"] + norm(s) + ["</s>"]          # padding so 1-2 word fragments still yield trigrams
    return {tuple(w[i:i + N]) for i in range(max(1, len(w) - N + 1))}
def jac(a, b):
    return len(a & b) / len(a | b) if a | b else 0.0

corpus = []
for l in open(REAL, encoding="utf-8"):
    r = json.loads(l); corpus.append(("REAL", r["gold_id"], grams(r["text"])))
for p in sorted(glob.glob(PUB + "/*.jsonl")):
    for l in open(p, encoding="utf-8"):
        r = json.loads(l); corpus.append(("PUBLIC", r["gold_id"], grams(r["text"])))
n_real = sum(1 for c in corpus if c[0] == "REAL")

# personal-data patterns
PAT = [("long_digit_run", re.compile(r"(?<!\d)\d{9,10}(?!\d)")),
       ("dotted_nit_like", re.compile(r"(?<!\d)\d{3}(?:\.\d{3}){2,3}(?:-\d)?(?!\d)")),
       ("id_marker", re.compile(r"\b(nit|c\.?\s?c\.?|cedula|cédula|identificaci[oó]n|tarjeta de identidad|pasaporte)\b", re.I)),
       ("email", re.compile(r"[\w.]+@[\w.]+")), ("phone", re.compile(r"\b3\d{9}\b"))]
def strings(o):
    if isinstance(o, str): yield o
    elif isinstance(o, dict):
        for v in o.values(): yield from strings(v)
    elif isinstance(o, list):
        for v in o: yield from strings(v)

rows, rejected, pii = [], [], []
for l in open(os.path.join(OUT, "followup_synthetic_cases.jsonl"), encoding="utf-8"):
    r = json.loads(l); g = grams(r["followup_text"])
    best = {"REAL": (0.0, None), "PUBLIC": (0.0, None)}
    for kind, gid, gg in corpus:
        j = jac(g, gg)
        if j > best[kind][0]: best[kind] = (j, gid)
    rows.append({"gold_id": r["gold_id"], "max_jaccard_real": round(best["REAL"][0], 3), "nearest_real_id": best["REAL"][1],
                 "max_jaccard_public": round(best["PUBLIC"][0], 3), "nearest_public_id": best["PUBLIC"][1]})
    if best["REAL"][0] >= THR or best["PUBLIC"][0] >= THR: rejected.append(r["gold_id"])
    for s in strings({"t": r["followup_text"], "p": r["prior"]}):
        for name, rx in PAT:
            if rx.search(s): pii.append({"gold_id": r["gold_id"], "pattern": name})
res = {"method": f"word {N}-gram Jaccard (accent/case-folded, <s>/</s> padded), threshold >= {THR} rejects",
       "corpus": {"real_texts": n_real, "public_texts": len(corpus) - n_real},
       "cases": len(rows), "rejected_count": len(rejected), "rejected": rejected,
       "max_over_all_cases_real": max(x["max_jaccard_real"] for x in rows),
       "max_over_all_cases_public": max(x["max_jaccard_public"] for x in rows),
       "personal_data_scan": {"patterns": [p[0] for p in PAT], "hits": pii, "clean": not pii},
       "per_case": rows}
json.dump(res, open(os.path.join(OUT, "similarity_check.json"), "w"), ensure_ascii=False, indent=1)
print(res["corpus"], "rejected", rejected, "maxreal", res["max_over_all_cases_real"], "maxpub", res["max_over_all_cases_public"], "pii", pii)
top = sorted(rows, key=lambda x: -max(x["max_jaccard_real"], x["max_jaccard_public"]))[:12]
for t in top: print(t)
