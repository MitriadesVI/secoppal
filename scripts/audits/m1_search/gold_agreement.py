"""Inter-labeler agreement for the M1 SEARCH Gold (contract §8, Phase A gate 2).

Usage: python scripts/audits/m1_search/gold_agreement.py LABELS_A_DIR LABELS_B_DIR OUT_DIR [--normalize]
--normalize applies the mechanical contract v1.3 rules (gold_adjudicate.normalize) to both sides
and compares UNSUPPORTED by span only (C8), universes only inside SEARCH (C7).
Each dir holds *.jsonl Gold files (validated by gold_schema.py). Only gold_ids present in both
are compared. Writes OUT_DIR/agreement.json (aggregates, no text) and OUT_DIR/disagreements.jsonl
(both labels per disagreeing case; contains text, so OUT_DIR must live outside git for REAL).
"""

from __future__ import annotations

import json
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


def _fold(text: str | None) -> str:
    text = unicodedata.normalize("NFKD", (text or "").lower())
    return " ".join("".join(ch for ch in text if not unicodedata.combining(ch)).split())


def _load(directory: Path) -> dict[str, dict]:
    records = {}
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                record = json.loads(line)
                records[record["gold_id"]] = record
    return records


def _search_requests(record: dict) -> list[dict]:
    if record.get("operation") == "MULTI":
        return [r for job in record.get("jobs", []) for r in job.get("requests", [])]
    return list(record.get("requests") or [])


def _universes(record: dict) -> tuple:
    return tuple(sorted((r.get("universe"), r.get("view")) for r in _search_requests(record)))


NORMALIZE = False


def _conditions(record: dict) -> Counter:
    """Multiset of (dimension, operator, polarity, folded quote) over all requests, deduplicated
    per request so a condition repeated in a dual SHARES_SCOPE pair counts once."""
    seen = Counter()
    for request in _search_requests(record):
        local = set()
        for cond in request.get("conditions", []):
            operator = "*" if NORMALIZE and cond.get("dimension") == "UNSUPPORTED" else cond.get("operator")
            local.add((cond.get("dimension"), operator, cond.get("polarity"), _fold(cond.get("quote"))))
        seen.update(local)
    return seen


def _dims(record: dict) -> Counter:
    return Counter((d, p) for (d, _o, p, _q) in _conditions(record).elements())


def _kappa(pairs: list[tuple[str, str]]) -> float | None:
    if not pairs:
        return None
    n = len(pairs)
    observed = sum(a == b for a, b in pairs) / n
    left = Counter(a for a, _ in pairs)
    right = Counter(b for _, b in pairs)
    expected = sum(left[k] * right.get(k, 0) for k in left) / (n * n)
    return None if expected == 1 else round((observed - expected) / (1 - expected), 4)


def main(dir_a: str, dir_b: str, out_dir: str) -> int:
    a, b = _load(Path(dir_a)), _load(Path(dir_b))
    if NORMALIZE:
        from gold_adjudicate import normalize
        a = {k: normalize(v)[0] for k, v in a.items()}
        b = {k: normalize(v)[0] for k, v in b.items()}
    common = sorted(set(a) & set(b))
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    fields = defaultdict(lambda: [0, 0])  # field -> [agree, total]
    per_dim = defaultdict(lambda: Counter())  # dim -> tp/fp/fn on exact condition tuples
    op_pairs = []
    disagreements = []
    for gid in common:
        ra, rb = a[gid], b[gid]
        diffs = []
        checks = {
            "operation": (ra.get("operation"), rb.get("operation")),
            "aspect": (ra.get("aspect"), rb.get("aspect")),
            "turn_context": (ra.get("turn_context"), rb.get("turn_context")),
            "universes": ((_universes(ra), _universes(rb)) if not NORMALIZE
                          or {ra.get("operation"), rb.get("operation")} <= {"SEARCH", "MULTI"}
                          else (None, None)),
            "product_view": (ra.get("product_view"), rb.get("product_view")),
            "expected_disposition": (ra.get("expected_disposition"), rb.get("expected_disposition")),
            "ambiguity": (ra.get("ambiguity"), rb.get("ambiguity")),
        }
        both_search = ra.get("operation") in {"SEARCH", "MULTI"} and rb.get("operation") in {"SEARCH", "MULTI"}
        if both_search:
            checks["dimensions_polarity"] = (sorted(_dims(ra).elements()), sorted(_dims(rb).elements()))
            checks["conditions_exact"] = (sorted(_conditions(ra).elements()), sorted(_conditions(rb).elements()))
            ca, cb = _conditions(ra), _conditions(rb)
            for key in (ca | cb):
                dim = key[0]
                inter = min(ca[key], cb[key])
                per_dim[dim]["tp"] += inter
                per_dim[dim]["fn"] += ca[key] - inter  # in A, not B
                per_dim[dim]["fp"] += cb[key] - inter  # in B, not A
        op_pairs.append((ra.get("operation"), rb.get("operation")))
        for name, (va, vb) in checks.items():
            fields[name][1] += 1
            if va == vb:
                fields[name][0] += 1
            else:
                diffs.append(name)
        if diffs:
            disagreements.append({"gold_id": gid, "slice": ra.get("slice"), "fields": diffs,
                                  "text": ra.get("text"), "A": ra, "B": rb})
    summary = {
        "compared": len(common),
        "only_in_A": len(set(a) - set(b)),
        "only_in_B": len(set(b) - set(a)),
        "field_agreement": {k: {"agree": v[0], "total": v[1],
                                "rate": round(v[0] / v[1], 4) if v[1] else None}
                            for k, v in sorted(fields.items())},
        "operation_kappa": _kappa(op_pairs),
        "condition_agreement_by_dimension": {
            dim: {**dict(c), "f1": round(2 * c["tp"] / (2 * c["tp"] + c["fp"] + c["fn"]), 4)
                  if (c["tp"] + c["fp"] + c["fn"]) else None}
            for dim, c in sorted(per_dim.items())
        },
        "disagreements": len(disagreements),
        "disagreements_by_field": dict(Counter(f for d in disagreements for f in d["fields"])),
        "disagreements_by_slice": dict(Counter(d["slice"] for d in disagreements)),
    }
    (out / "agreement.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    with (out / "disagreements.jsonl").open("w", encoding="utf-8") as handle:
        for item in disagreements:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(json.dumps({k: summary[k] for k in ("compared", "operation_kappa", "disagreements")}))
    return 0


if __name__ == "__main__":
    NORMALIZE = "--normalize" in sys.argv
    sys.path.insert(0, str(Path(__file__).parent))
    sys.exit(main(*[arg for arg in sys.argv[1:] if arg != "--normalize"][:3]))
