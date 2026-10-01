"""Validator for the M1 SEARCH Gold v1 (docs/audits/m1_search_foundation/M1_0_SEARCH_CONTRACT.md §8).

Usage: python scripts/audits/m1_search/gold_schema.py FILE.jsonl [FILE.jsonl ...]
Exit 0 when every record is valid; otherwise prints one line per error and exits 1.
Stdlib only, so it runs from any worktree without the app.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys

SLICES = {"REAL", "SYNTHETIC", "ADVERSARIAL", "MINIMAL_PAIR", "NEIGHBOR"}
TURN_CONTEXTS = {"SELF_CONTAINED", "FOLLOWUP_FRAGMENT", "NEEDS_HISTORY"}
OPERATIONS = {"SEARCH", "COUNT", "SUM_VALUE", "GROUP_BY", "RANK_SUPPLIERS", "PROFILE_BUYER",
              "PROFILE_ACTOR", "FOLLOW", "EXPORT", "PROTOCOL", "FOLLOWUP_DELTA", "OUT_OF_DOMAIN",
              "MULTI"}
ASPECT_OPERATIONS = {"COUNT", "SUM_VALUE", "GROUP_BY"}
ASPECTS = {"TOTAL", "BY_MODALITY", "BY_YEAR", "BY_BUYER", "BY_SUPPLIER", "BY_STATE", "BY_TYPE"}
UNIVERSES = {"PROCESSES", "CONTRACTS", "UNSPECIFIED"}
VIEWS = {"RECORDS", "OPPORTUNITIES"}
PRODUCT_VIEWS = {"CURRENT_OPPORTUNITIES", "HISTORICAL_PROCESSES", "CONTRACTS"}
DISPOSITIONS = {"EXECUTE", "CLARIFY", "DECLINE"}
POLARITIES = {"POSITIVE", "NEGATIVE"}
OPERATORS = {
    "TOPIC": {"contains", "whole_token_folded"},
    "BUYER": {"is"},
    "ACTOR": {"is"},
    "GEOGRAPHY": {"is"},
    "TIME": {"during_year", "during_month", "between_dates", "relative_period"},
    "VALUE": {"greater_than", "greater_or_equal", "less_than", "less_or_equal"},
    "STATE": {"is", "family"},
    "MODALITY": {"family", "is"},
    "CONTRACT_TYPE_OR_NATURE": {"is", "family"},
    "ORDER": {"value_desc", "value_asc", "date_desc", "date_asc"},
    "SOURCE": {"is"},
    "UNSUPPORTED": None,  # free short label
}
GEO_LEVELS = {"DEPARTMENT", "MUNICIPALITY", "REGION", "UNKNOWN"}
STATE_HINT = re.compile(r"(open|published|closed|awarded|terminated|in_execution|cancelled|draft|in_evaluation|signed|liquidated|other:.+)\Z")
LOGIC_TOKEN = re.compile(r"AND|OR|NOT|[A-Za-z0-9_.-]+|\(|\)|,")


def _parse_logic(text: str) -> tuple[str, list[str]]:
    """Return (normalized, atoms); raises ValueError on a malformed expression."""
    tokens = [t for t in LOGIC_TOKEN.findall(text.replace(" ", ""))]
    if "".join(tokens) != text.replace(" ", ""):
        raise ValueError("LOGIC_BAD_CHARS")
    pos = 0
    atoms: list[str] = []

    def node() -> None:
        nonlocal pos
        if pos >= len(tokens):
            raise ValueError("LOGIC_TRUNCATED")
        tok = tokens[pos]
        pos += 1
        if tok in {"AND", "OR", "NOT"}:
            if pos >= len(tokens) or tokens[pos] != "(":
                raise ValueError("LOGIC_EXPECTED_PAREN")
            pos += 1
            count = 0
            while True:
                node()
                count += 1
                if pos >= len(tokens):
                    raise ValueError("LOGIC_TRUNCATED")
                if tokens[pos] == ",":
                    pos += 1
                    continue
                if tokens[pos] == ")":
                    pos += 1
                    break
                raise ValueError("LOGIC_UNEXPECTED_TOKEN")
            if tok == "NOT" and count != 1:
                raise ValueError("LOGIC_NOT_ARITY")
            if tok in {"AND", "OR"} and count < 1:
                raise ValueError("LOGIC_GROUP_ARITY")
        elif tok in {"(", ")", ","}:
            raise ValueError("LOGIC_UNEXPECTED_TOKEN")
        else:
            atoms.append(tok)

    node()
    if pos != len(tokens):
        raise ValueError("LOGIC_TRAILING")
    return text, atoms


def _check_requests(requests, text, where, errors) -> set[str]:
    universes = set()
    if not isinstance(requests, list) or not requests:
        errors.append(f"{where}: REQUESTS_REQUIRED")
        return universes
    ids = set()
    for request in requests:
        rid = request.get("request_id")
        if not rid or rid in ids:
            errors.append(f"{where}: BAD_OR_DUPLICATE_REQUEST_ID {rid}")
        ids.add(rid)
        universe = request.get("universe")
        universes.add(universe)
        if universe not in UNIVERSES:
            errors.append(f"{where}/{rid}: BAD_UNIVERSE {universe}")
        view = request.get("view")
        if view not in VIEWS:
            errors.append(f"{where}/{rid}: BAD_VIEW {view}")
        if view == "OPPORTUNITIES" and universe != "PROCESSES":
            errors.append(f"{where}/{rid}: OPPORTUNITIES_ONLY_ON_PROCESSES")
        quote = request.get("universe_quote")
        if universe == "UNSPECIFIED":
            if quote is not None:
                errors.append(f"{where}/{rid}: UNSPECIFIED_HAS_UNIVERSE_QUOTE")
        elif not quote or (text is not None and quote not in text):
            errors.append(f"{where}/{rid}: UNIVERSE_QUOTE_NOT_IN_TEXT {quote!r}")
        conditions = request.get("conditions", [])
        cids = []
        for cond in conditions:
            cid = cond.get("id")
            cids.append(cid)
            dim = cond.get("dimension")
            if dim not in OPERATORS:
                errors.append(f"{where}/{rid}/{cid}: BAD_DIMENSION {dim}")
                continue
            allowed = OPERATORS[dim]
            op = cond.get("operator")
            if allowed is not None and op not in allowed:
                errors.append(f"{where}/{rid}/{cid}: BAD_OPERATOR {dim}.{op}")
            if allowed is None and (not isinstance(op, str) or not op.strip()):
                errors.append(f"{where}/{rid}/{cid}: UNSUPPORTED_NEEDS_LABEL")
            if cond.get("polarity") not in POLARITIES:
                errors.append(f"{where}/{rid}/{cid}: BAD_POLARITY")
            q = cond.get("quote")
            if not q or (text is not None and q not in text):
                errors.append(f"{where}/{rid}/{cid}: QUOTE_NOT_IN_TEXT {q!r}")
            if dim == "GEOGRAPHY" and cond.get("level") not in GEO_LEVELS:
                errors.append(f"{where}/{rid}/{cid}: GEOGRAPHY_LEVEL_REQUIRED")
            if dim != "GEOGRAPHY" and cond.get("level") not in (None, ""):
                errors.append(f"{where}/{rid}/{cid}: LEVEL_ONLY_FOR_GEOGRAPHY")
            if dim == "STATE" and not STATE_HINT.match(str(cond.get("value_hint") or "")):
                errors.append(f"{where}/{rid}/{cid}: BAD_STATE_HINT {cond.get('value_hint')!r}")
            if dim == "TOPIC" and op == "whole_token_folded" and cond.get("polarity") == "POSITIVE":
                if q and q.strip().lower() not in {"obra", "obras"}:
                    errors.append(f"{where}/{rid}/{cid}: WHOLE_TOKEN_ONLY_FOR_OBRA")
        if len(set(cids)) != len(cids):
            errors.append(f"{where}/{rid}: DUPLICATE_CONDITION_ID")
        logic = request.get("logic")
        if conditions:
            try:
                _, atoms = _parse_logic(logic or "")
            except ValueError as exc:
                errors.append(f"{where}/{rid}: {exc} {logic!r}")
            else:
                if sorted(atoms) != sorted(cids):
                    errors.append(f"{where}/{rid}: LOGIC_ATOMS_MISMATCH {logic!r}")
        elif logic not in (None, ""):
            errors.append(f"{where}/{rid}: LOGIC_WITHOUT_CONDITIONS")
    return universes


def validate(record: dict) -> list[str]:
    errors: list[str] = []
    gid = record.get("gold_id") or "?"
    if record.get("slice") not in SLICES:
        errors.append(f"{gid}: BAD_SLICE")
    text = record.get("text")
    sha = record.get("text_sha256")
    if text is not None:
        if hashlib.sha256(text.encode("utf-8")).hexdigest() != sha:
            errors.append(f"{gid}: TEXT_SHA_MISMATCH")
    elif not sha:
        errors.append(f"{gid}: TEXT_OR_SHA_REQUIRED")
    if record.get("turn_context") not in TURN_CONTEXTS:
        errors.append(f"{gid}: BAD_TURN_CONTEXT")
    op = record.get("operation")
    if op not in OPERATIONS:
        errors.append(f"{gid}: BAD_OPERATION {op}")
    aspect = record.get("aspect")
    if op in ASPECT_OPERATIONS:
        if not (aspect in ASPECTS or (isinstance(aspect, str) and aspect.startswith("OTHER:"))):
            errors.append(f"{gid}: ASPECT_REQUIRED {aspect}")
    elif aspect is not None and op != "MULTI":
        errors.append(f"{gid}: ASPECT_ONLY_FOR_ANALYTIC")
    universes: set[str] = set()
    if op == "SEARCH":
        universes |= _check_requests(record.get("requests"), text, gid, errors)
    elif op == "MULTI":
        jobs = record.get("jobs")
        if not isinstance(jobs, list) or len(jobs) < 2:
            errors.append(f"{gid}: MULTI_NEEDS_JOBS")
        else:
            for at, job in enumerate(jobs):
                if job.get("operation") not in OPERATIONS - {"MULTI"}:
                    errors.append(f"{gid}/job{at}: BAD_JOB_OPERATION")
                if job.get("operation") == "SEARCH":
                    universes |= _check_requests(job.get("requests"), text, f"{gid}/job{at}", errors)
    elif record.get("requests"):
        # Non-SEARCH operations may still carry conditions (e.g. RANK_SUPPLIERS of a buyer).
        universes |= _check_requests(record.get("requests"), text, gid, errors)
    pv = record.get("product_view")
    if "UNSPECIFIED" in universes:
        if pv not in PRODUCT_VIEWS:
            errors.append(f"{gid}: PRODUCT_VIEW_REQUIRED")
    elif pv is not None:
        errors.append(f"{gid}: PRODUCT_VIEW_ONLY_FOR_UNSPECIFIED")
    disp = record.get("expected_disposition")
    if disp not in DISPOSITIONS:
        errors.append(f"{gid}: BAD_DISPOSITION")
    if disp in {"CLARIFY", "DECLINE"} and not record.get("disposition_reason"):
        errors.append(f"{gid}: DISPOSITION_REASON_REQUIRED")
    if record.get("ambiguity") not in {"CLEAR", "AMBIGUOUS"}:
        errors.append(f"{gid}: BAD_AMBIGUITY")
    if record.get("ambiguity") == "AMBIGUOUS" and not record.get("alternatives"):
        errors.append(f"{gid}: AMBIGUOUS_NEEDS_ALTERNATIVES")
    if not record.get("labeler"):
        errors.append(f"{gid}: LABELER_REQUIRED")
    return errors


def main(paths: list[str]) -> int:
    all_errors: list[str] = []
    seen: dict[str, str] = {}
    total = 0
    for path in paths:
        with open(path, encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                total += 1
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    all_errors.append(f"{path}:{line_no}: BAD_JSON {exc}")
                    continue
                gid = record.get("gold_id")
                if gid in seen:
                    all_errors.append(f"{path}:{line_no}: DUPLICATE_GOLD_ID {gid} (first in {seen[gid]})")
                seen[gid] = f"{path}:{line_no}"
                all_errors.extend(f"{path}:{line_no}: {e}" for e in validate(record))
    for error in all_errors:
        print(error)
    print(f"records={total} errors={len(all_errors)}", file=sys.stderr)
    return 1 if all_errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
