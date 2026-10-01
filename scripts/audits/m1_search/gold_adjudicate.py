"""M1-B adjudication: labels A (+ labels B where adjudicated) → sealed Gold v1.

Usage: python scripts/audits/m1_search/gold_adjudicate.py LABELS_A_DIR LABELS_B_DIR OUT_DIR
Applies the mechanical rules of contract v1.3 (C1, C2, C7) to every record and the per-case
decisions listed in DECISIONS below (log: docs/audits/m1_search_foundation/M1_B_ADJUDICATION.md).
Writes OUT_DIR/gold_v1.jsonl (contains text; REAL lives outside git) and OUT_DIR/SEAL.json.
The same `normalize` is importable so synthetic slices get identical treatment.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import sys
from pathlib import Path

GOLD_SCHEMA_VERSION = "m1-search-gold/1.3"
_ROLE_MARKER = re.compile(
    r"^(?:(?:del|de la|de los|al|el|la|los)\s+)?(?:proveedor(?:es)?|contratista(?:s)?|empresa|nit|c\.?c\.?|"
    r"c[eé]dula)\s*(?:n[oº°.]*\s*)?[:\-]?\s+", re.IGNORECASE)


def _strip_marker(quote: str) -> str:
    stripped = _ROLE_MARKER.sub("", quote, count=1)
    return stripped if stripped.strip() else quote


def _requests(record: dict):
    yield from record.get("requests") or []
    for job in record.get("jobs") or []:
        yield from job.get("requests") or []


def normalize(record: dict) -> tuple[dict, list[str]]:
    """Mechanical contract v1.3 rules; returns (record, applied rule ids)."""
    out = copy.deepcopy(record)
    applied: list[str] = []
    for request in _requests(out):
        for cond in request.get("conditions", []):
            if cond.get("dimension") == "ACTOR":
                new = _strip_marker(cond.get("quote") or "")
                if new != cond.get("quote"):
                    cond["quote"] = new
                    applied.append("C1")
    for request in _requests(out):
        for cond in request.get("conditions", []):
            if cond.get("dimension") == "CONTRACT_TYPE_OR_NATURE":
                if (cond.get("quote") or "").strip().lower() in {"obra", "obras"}:
                    cond.update(dimension="TOPIC", operator="whole_token_folded")  # v1.4 D2 (K1)
                    applied.append("D2")
                elif cond.get("operator") != "is":
                    cond["operator"] = "is"  # v1.4 D3
                    applied.append("D3")
    op = out.get("operation")
    if op == "FOLLOWUP_DELTA" and out.get("turn_context") != "FOLLOWUP_FRAGMENT":
        out["turn_context"] = "FOLLOWUP_FRAGMENT"
        applied.append("C2")
    if op == "PROTOCOL" and out.get("turn_context") != "SELF_CONTAINED":
        out["turn_context"] = "SELF_CONTAINED"
        applied.append("C2")
    for job in out.get("jobs") or []:  # contract v1.4 D9: non-SEARCH jobs follow C7
        if job.get("operation") != "SEARCH":
            for request in job.get("requests") or []:
                if (request.get("universe"), request.get("view"), request.get("universe_quote")) != (
                        "UNSPECIFIED", "RECORDS", None):
                    request.update(universe="UNSPECIFIED", view="RECORDS", universe_quote=None)
                    applied.append("C7")
    if op not in {"SEARCH", "MULTI"} and out.get("requests"):
        kept = []
        for request in out["requests"]:
            if not request.get("conditions"):
                applied.append("C7")
                continue
            if (request.get("universe"), request.get("view"), request.get("universe_quote")) != (
                    "UNSPECIFIED", "RECORDS", None):
                request.update(universe="UNSPECIFIED", view="RECORDS", universe_quote=None)
                applied.append("C7")
            kept.append(request)
        out["requests"] = kept
    if op not in {"SEARCH", "MULTI"} and out.get("product_view") is not None:
        out["product_view"] = None
        applied.append("B1")
    return out, sorted(set(applied))


def _set_clear(record: dict) -> None:
    record["ambiguity"] = "CLEAR"
    record["alternatives"] = []


def _state_family(record: dict) -> None:
    for request in _requests(record):
        for cond in request.get("conditions", []):
            if cond["dimension"] == "STATE":
                cond["operator"] = "family"


def _drop_time(record: dict) -> None:
    for request in _requests(record):
        drop = {c["id"] for c in request["conditions"] if c["dimension"] == "TIME"}
        request["conditions"] = [c for c in request["conditions"] if c["id"] not in drop]
        ids = [c["id"] for c in request["conditions"]]
        request["logic"] = ids[0] if len(ids) == 1 else f"AND({','.join(ids)})"


# gold_id -> (source, patch functions, rule, rationale)
DECISIONS = {
    "R0069": ("B", [_state_family], "A2+C4", "interventoría sin objeto = tipo; «activos» = familia abierta"),
    "R0191": ("B", [_drop_time], "C3", "«hoy» junto a estado no es TIME"),
    "R0216": ("B", [_state_family, _set_clear], "B6+C4", "municipio sujeto = BUYER; «en concurso» = familia abierta"),
    "R0346": ("B", [], "§6", "nombre desnudo = PROFILE_ACTOR ejecutable"),
    "R0524": ("B", [], "§3", "sin sustantivo de universo = UNSPECIFIED (PV6, mismo efecto)"),
    "R0553": ("B", [], "B6", "municipio sujeto que contrata = BUYER"),
    "R0219": ("A", [_set_clear], "—", "Soledad sin homónimo relevante"),
    "R0361": ("A", [_set_clear], "—", "número tipo cédula tras «contratos de» = ACTOR"),
    "R0377": ("A", [_set_clear], "—", "CLARIFY basta; no hay lecturas alternativas enumerables"),
    "R0389": ("A", [_set_clear], "B5", "CLARIFY por monto sin comparador, no ambigüedad"),
    "R0415": ("A", [_set_clear], "C1", "persona natural nombrada tras marcador de rol"),
    "R0435": ("A", [_set_clear], "C2", "fragmento de botón de desambiguación"),
    "R0510": ("A", [_set_clear], "C7", "ranking por valor, lectura única"),
    "R0511": ("A", [_set_clear], "C7", "ranking por valor, lectura única"),
    "R0525": ("A", [_set_clear], "B3", "«qué ha hecho X» = PROFILE_BUYER"),
    "R0342": ("A", [_set_clear], "C6+B4", "sustantivo explícito manda; «celebrado» = estado signed"),
}


# Synthetic slices (NEIGHBOR, MINIMAL_PAIR, ADVERSARIAL): A = author (sonnet), B = opus-B.
SYNTH_FROM_B = {"N013", "N019", "N033", "N051", "N053", "N062", "N083", "N098", "N099", "N102",
                "X021", "X035", "X037", "X038", "X041", "X049", "X051", "X057", "X072", "X079",
                "X082", "X091"}
SYNTH_CLEAR = {"N026", "N065", "P084", "X000", "X040"}


def adjudicate_synthetic(a: dict[str, dict], b: dict[str, dict]) -> list[dict]:
    rows = []
    for gid in sorted(a):
        source = "B" if gid in SYNTH_FROM_B else "A"
        record, _ = normalize(b[gid] if source == "B" else a[gid])
        if gid in SYNTH_CLEAR:
            _set_clear(record)
        record["labeler"] = f"adjudicated:{source}"
        record["double_labeled"] = gid in b
        rows.append(record)
    return rows


def _load(directory: Path) -> dict[str, dict]:
    records = {}
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                record = json.loads(line)
                records[record["gold_id"]] = record
    return records


def main(dir_a: str, dir_b: str, out_dir: str) -> int:
    a, b = _load(Path(dir_a)), _load(Path(dir_b))
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rows, log = [], {}
    for gid in sorted(a):
        decision = DECISIONS.get(gid)
        base = b[gid] if decision and decision[0] == "B" else a[gid]
        record, applied = normalize(base)
        if decision:
            for patch in decision[1]:
                patch(record)
            applied.append(f"DECISION:{decision[2]}")
        record["labeler"] = "adjudicated:" + ("B" if decision and decision[0] == "B" else "A")
        record["double_labeled"] = gid in b
        if applied:
            log[gid] = applied
        rows.append(record)
    path = out / "gold_v1.jsonl"
    path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")
    seal = {
        "schema_version": GOLD_SCHEMA_VERSION,
        "records": len(rows),
        "double_labeled": sum(r["double_labeled"] for r in rows),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "decisions": {gid: {"source": d[0], "rule": d[2], "why": d[3]} for gid, d in DECISIONS.items()},
        "normalized": {gid: rules for gid, rules in log.items()},
    }
    (out / "SEAL.json").write_text(json.dumps(seal, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: seal[k] for k in ("records", "double_labeled", "sha256")}),
          "normalized:", len(log))
    return 0


def main_synthetic(dir_a: str, dir_b: str, out_dir: str) -> int:
    rows = adjudicate_synthetic(_load(Path(dir_a)), _load(Path(dir_b)))
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for slice_name, file_name in (("NEIGHBOR", "neighbor"), ("MINIMAL_PAIR", "minimal_pair"),
                                  ("ADVERSARIAL", "adversarial")):
        part = [r for r in rows if r["slice"] == slice_name]
        (out / f"{file_name}.jsonl").write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in part), encoding="utf-8")
    print(json.dumps({"records": len(rows), "from_B": len(SYNTH_FROM_B), "cleared": len(SYNTH_CLEAR)}))
    return 0


if __name__ == "__main__":
    if sys.argv[1] == "--synthetic":
        sys.exit(main_synthetic(*sys.argv[2:5]))
    sys.exit(main(*sys.argv[1:4]))
