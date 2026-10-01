"""M1-B adjudication: labels A (+ labels B where adjudicated) → sealed Gold v1.

Usage: python scripts/audits/m1_search/gold_adjudicate.py LABELS_A_DIR LABELS_B_DIR OUT_DIR
       python scripts/audits/m1_search/gold_adjudicate.py --selftest   # the v1.7/v1.8/v1.9 host-policy rules of `normalize`
Applies the mechanical rules of contract v1.3 (C1, C2, C7) to every record and the per-case
decisions listed in DECISIONS below (log: docs/audits/m1_search_foundation/M1_B_ADJUDICATION.md).
Writes OUT_DIR/gold_v1.jsonl (contains text; REAL lives outside git) and OUT_DIR/SEAL.json.
The same `normalize` is importable so synthetic slices get identical treatment.

Gold v1.8/v1.9 note: the MINIMAL_PAIR records P176-P181 (ACTOR x STATE) and P182+ (v1.9) were authored by the
orchestrator directly in minimal_pair.jsonl and have no labels A/B; the sealed files are canonical, `--synthetic`
does not regenerate them.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import sys
from pathlib import Path

from gold_schema import (ACTOR_AWARDED_REDIRECT, ACTOR_PROCESSES_AWARDED_REDIRECT, ACTOR_PROCESSES_REDIRECT,
                         ACTOR_TIME_NOTICE, AWARDED, CONTRACT_STATE, NOTICE_FIELD, PREAWARD_REASON, REDIRECT_FIELD,
                         SCHEMA_VERSION, actor_state_policy, actor_time_applies, clarifies, needs_redirect,
                         required_redirect)

GOLD_SCHEMA_VERSION = SCHEMA_VERSION  # single source: gold_schema.py (v1.9)
_ROLE_MARKER = re.compile(
    r"^(?:(?:del|de la|de los|al|el|la|los)\s+)?(?:proveedor(?:es)?|contratista(?:s)?|empresa)\s*[:\-]?\s+",
    re.IGNORECASE)
# v1.6: document-type markers belong INSIDE an ACTOR quote (the authority decides NIT vs CC from it).
_DOC_MARKER_BEFORE = re.compile(r"(?:\b(?:nit|c[eé]dula(?:\s+de\s+ciudadan[ií]a)?|c\.?\s?c\.?)\s*(?:n[oº°.]*\s*)?[:\-]?\s*)$",
                                re.IGNORECASE)


def _strip_marker(quote: str) -> str:
    stripped = _ROLE_MARKER.sub("", quote, count=1)
    return stripped if stripped.strip() else quote


def _requests(record: dict):
    yield from record.get("requests") or []
    for job in record.get("jobs") or []:
        yield from job.get("requests") or []


def _search_requests(record: dict):
    """Requests the host policy applies to: those of a SEARCH record or of a SEARCH job inside a MULTI."""
    if record.get("operation") == "SEARCH":
        yield from record.get("requests") or []
    for job in record.get("jobs") or []:
        if job.get("operation") == "SEARCH":
            yield from job.get("requests") or []


def normalize(record: dict) -> tuple[dict, list[str]]:
    """Mechanical contract rules (v1.3 C1/C2/C7, v1.7 P-ACT-2, v1.8/v1.9 P-ACT-3, v1.9 P-ACT-4); returns (record, applied rule ids)."""
    out = copy.deepcopy(record)
    applied: list[str] = []
    text = out.get("text") or ""
    for request in _requests(out):
        for cond in request.get("conditions", []):
            if cond.get("dimension") == "ACTOR":
                new = _strip_marker(cond.get("quote") or "")
                if new != cond.get("quote"):
                    cond["quote"] = new
                    applied.append("C1")
                at = text.find(new) if new else -1
                if at > 0 and re.search(r"\d{5,}", new):
                    marker = _DOC_MARKER_BEFORE.search(text[:at])
                    if marker and marker.group(0).strip():
                        cond["quote"] = text[marker.start():at] + new
                        applied.append("C1-DOC")
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
    for request in _search_requests(out):  # v1.7 P-ACT-2 / v1.8-v1.9 P-ACT-3: only genuine SEARCH requests (after C7)
        policy = actor_state_policy(request)
        if policy == "PENDING":
            continue  # no rule adjudicated: never invent a label
        if clarifies(policy):
            if request.pop(REDIRECT_FIELD, None) is not None:
                applied.append("P-ACT-3")
            continue
        expected = required_redirect(request)
        if expected is not None and request.get(REDIRECT_FIELD) != expected:
            request[REDIRECT_FIELD] = dict(expected)
            applied.append("P-ACT-2" if policy is None else "P-ACT-3")
        if actor_time_applies(request) and request.get(NOTICE_FIELD) != ACTOR_TIME_NOTICE:
            request[NOTICE_FIELD] = ACTOR_TIME_NOTICE  # v1.9 P-ACT-4
            applied.append("P-ACT-4")
    if op == "SEARCH":  # record level (single SEARCH): product_view and disposition follow the policy of its requests
        requests = out.get("requests") or []
        policies = {actor_state_policy(q) for q in requests}
        unspecified = [q for q in requests if q.get("universe") == "UNSPECIFIED"]
        wants_contracts = any(actor_state_policy(q) in {AWARDED, CONTRACT_STATE} or actor_time_applies(q)
                              for q in unspecified)
        if wants_contracts and out.get("product_view") != "CONTRACTS":
            out["product_view"] = "CONTRACTS"
            applied.append("P-ACT-3")
        if any(clarifies(p) for p in policies):
            if (out.get("expected_disposition"), out.get("disposition_reason")) != ("CLARIFY", PREAWARD_REASON):
                out["expected_disposition"], out["disposition_reason"] = "CLARIFY", PREAWARD_REASON
                applied.append("P-ACT-3")
        elif (policies & {AWARDED, CONTRACT_STATE} or any(actor_time_applies(q) for q in requests)) \
                and out.get("expected_disposition") != "EXECUTE" and out.get("disposition_reason") == PREAWARD_REASON:
            # a v1.8 "asks" label on a state that EXISTS in contracts (v1.9): it executes; only that stale reason is cleared
            out["expected_disposition"], out["disposition_reason"] = "EXECUTE", None
            applied.append("P-ACT-3")
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


def selftest() -> int:
    """`normalize` must REPAIR a record that violates the v1.7/v1.8/v1.9 host-policy rules (and leave a correct one alone)."""
    def request(universe, *conds, redirect=None, notice=None):
        built = [{"id": f"c{at + 1}", "dimension": dim, "operator": "is", "polarity": "POSITIVE", "quote": "q",
                  "value_hint": hint, "level": None} for at, (dim, hint) in enumerate(conds)]
        out = {"request_id": "q1", "universe": universe, "view": "RECORDS",
               "universe_quote": None if universe == "UNSPECIFIED" else "u", "conditions": built,
               "logic": "AND(" + ",".join(c["id"] for c in built) + ")"}
        if redirect:
            out[REDIRECT_FIELD] = redirect
        if notice:
            out[NOTICE_FIELD] = notice
        return out

    def record(req, pv="CONTRACTS", disposition="EXECUTE", reason=None, op="SEARCH"):
        return {"gold_id": "T", "operation": op, "turn_context": "SELF_CONTAINED", "requests": [req],
                "product_view": pv, "expected_disposition": disposition, "disposition_reason": reason}

    actor, awarded, year = ("ACTOR", "X SAS"), ("STATE", "awarded"), ("TIME", "2024")
    clarify = {"disposition": "CLARIFY", "reason": PREAWARD_REASON}
    cases = [  # (label, input record, check on the normalized record)
        ("awarded: redirect + CONTRACTS restored over PV2", record(request("UNSPECIFIED", actor, awarded), pv="HISTORICAL_PROCESSES"),
         lambda r: r["requests"][0].get(REDIRECT_FIELD) == ACTOR_AWARDED_REDIRECT and r["product_view"] == "CONTRACTS"
         and r["expected_disposition"] == "EXECUTE"),
        ("pre-award: EXECUTE becomes CLARIFY/ACTOR_STATE_INCOMPATIBLE", record(request("UNSPECIFIED", actor, ("STATE", "published"))),
         lambda r: (r["expected_disposition"], r["disposition_reason"]) == ("CLARIFY", PREAWARD_REASON)
         and REDIRECT_FIELD not in r["requests"][0]),
        ("pre-award: a redirect is removed", record(request("UNSPECIFIED", actor, ("STATE", "open"), redirect=ACTOR_AWARDED_REDIRECT),
                                                    **clarify),
         lambda r: REDIRECT_FIELD not in r["requests"][0]),
        ("ACTOR alone: untouched (PV3, no redirect)", record(request("UNSPECIFIED", actor)),
         lambda r: REDIRECT_FIELD not in r["requests"][0] and r["expected_disposition"] == "EXECUTE"),
        ("explicit CONTRACTS ACTOR+awarded: untouched", record(request("CONTRACTS", actor, awarded), pv=None),
         lambda r: REDIRECT_FIELD not in r["requests"][0] and r["product_view"] is None),
        ("v1.7: PROCESSES+ACTOR gets the processes redirect", record(request("PROCESSES", actor), pv=None),
         lambda r: r["requests"][0].get(REDIRECT_FIELD) == ACTOR_PROCESSES_REDIRECT),
        # ---- v1.9
        ("v1.9 contract state (terminated): CONTRACTS, EXECUTE, no redirect, never PV2", record(request("UNSPECIFIED", actor, ("STATE", "terminated")), pv="HISTORICAL_PROCESSES"),
         lambda r: REDIRECT_FIELD not in r["requests"][0] and r["product_view"] == "CONTRACTS"
         and r["expected_disposition"] == "EXECUTE"),
        ("v1.9 draft: the v1.8 'asks' label is repaired to EXECUTE on CONTRACTS", record(request("UNSPECIFIED", actor, ("STATE", "draft")), **clarify),
         lambda r: (r["expected_disposition"], r["disposition_reason"], r["product_view"]) == ("EXECUTE", None, "CONTRACTS")
         and REDIRECT_FIELD not in r["requests"][0]),
        ("v1.9 contract state: an unrelated CLARIFY reason is NOT cleared", record(request("UNSPECIFIED", actor, ("STATE", "cancelled")), disposition="CLARIFY", reason="OTHER"),
         lambda r: (r["expected_disposition"], r["disposition_reason"]) == ("CLARIFY", "OTHER")),
        ("v1.9 desierto: asks, no redirect", record(request("UNSPECIFIED", actor, ("STATE", "other:desierto"))),
         lambda r: (r["expected_disposition"], r["disposition_reason"]) == ("CLARIFY", PREAWARD_REASON)
         and REDIRECT_FIELD not in r["requests"][0]),
        ("v1.9 PROCESSES+ACTOR+draft: asks and the v1.7 redirect is removed (never translated)", record(request("PROCESSES", actor, ("STATE", "draft"), redirect=ACTOR_PROCESSES_REDIRECT), pv=None),
         lambda r: (r["expected_disposition"], r["disposition_reason"]) == ("CLARIFY", PREAWARD_REASON)
         and REDIRECT_FIELD not in r["requests"][0] and r["product_view"] is None),
        ("v1.9 PROCESSES+ACTOR+awarded: v1.7 notice gains the APPROXIMATION effect", record(request("PROCESSES", actor, awarded, redirect=ACTOR_PROCESSES_REDIRECT), pv=None),
         lambda r: r["requests"][0].get(REDIRECT_FIELD) == ACTOR_PROCESSES_AWARDED_REDIRECT
         and r["expected_disposition"] == "EXECUTE"),
        ("v1.9 PROCESSES+ACTOR (no state): v1.7 object unchanged", record(request("PROCESSES", actor, redirect=ACTOR_PROCESSES_REDIRECT), pv=None),
         lambda r: r["requests"][0].get(REDIRECT_FIELD) == ACTOR_PROCESSES_REDIRECT),
        ("v1.9 ACTOR+TIME: signature-date notice + CONTRACTS over PV1", record(request("UNSPECIFIED", actor, year), pv="HISTORICAL_PROCESSES"),
         lambda r: r["requests"][0].get(NOTICE_FIELD) == ACTOR_TIME_NOTICE and r["product_view"] == "CONTRACTS"
         and REDIRECT_FIELD not in r["requests"][0]),
        ("v1.9 ACTOR+TIME on a non-SEARCH operation: untouched", record(request("UNSPECIFIED", actor, year), pv=None, op="COUNT"),
         lambda r: NOTICE_FIELD not in r["requests"][0]),
        ("v1.9 pending (closed): never invented", record(request("UNSPECIFIED", actor, ("STATE", "closed")), pv="HISTORICAL_PROCESSES"),
         lambda r: REDIRECT_FIELD not in r["requests"][0] and r["product_view"] == "HISTORICAL_PROCESSES"
         and r["expected_disposition"] == "EXECUTE"),
        ("v1.9 pending PROCESSES+ACTOR+cancelled: no v1.7 redirect invented", record(request("PROCESSES", actor, ("STATE", "cancelled")), pv=None),
         lambda r: REDIRECT_FIELD not in r["requests"][0] and r["expected_disposition"] == "EXECUTE"),
        ("v1.9 pending (awarded+TIME): never invented", record(request("UNSPECIFIED", actor, awarded, year), pv="HISTORICAL_PROCESSES"),
         lambda r: REDIRECT_FIELD not in r["requests"][0] and NOTICE_FIELD not in r["requests"][0]
         and r["product_view"] == "HISTORICAL_PROCESSES"),
    ]
    failures = 0
    for label, rec, check in cases:
        out, _ = normalize(rec)
        again, _ = normalize(out)
        good = bool(check(out)) and again == out  # repaired AND idempotent
        failures += not good
        print(("ok   " if good else "FAIL ") + label)
    print(f"selftest: {len(cases) - failures}/{len(cases)} passed", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    if sys.argv[1] == "--selftest":
        sys.exit(selftest())
    if sys.argv[1] == "--synthetic":
        sys.exit(main_synthetic(*sys.argv[2:5]))
    sys.exit(main(*sys.argv[1:4]))
