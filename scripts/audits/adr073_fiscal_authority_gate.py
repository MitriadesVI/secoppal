#!/usr/bin/env python3
"""ADR-073 R1: audit every fiscal entry and preserve Schema 2 NIT authority.

The frozen directory/D0 policy treats a bare nine-digit NIT as a complete
document. A ten-digit raw may be 9+DV only when its DV validates; a leading
zero is the separately declared census normalization. Module 11 alone never
licenses a 9-to-8 reinterpretation.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from app.core.actor_identity import compute_check_digit

ROOT = Path(__file__).resolve().parents[2]
SIDECAR = ROOT / "app/data/buyer_source_names.json"
FROZEN_SCHEMA2 = ROOT / "tests/fixtures/adr073_fiscal_r1/schema2_nit_authorities.json"
OUT = ROOT / "artifacts/audits/adr073_fiscal_r1"
SCHEMA2_SOURCE = "buyer_source_names.json#schema2:identity_key"
# The census report declares exactly this leading-zero normalization.
LEADING_ZERO_AUTHORIZED = {"0899999061": "899999061"}

D0_CANONICALS = (
    "UNIATLANTICO", "UNIVERSIDAD DE CÓRDOBA", "UNIVERSIDAD DE LA AMAZONIA",
    "UNIVERSIDAD DEL PACÍFICO",
    "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA",
    "UNIVERSIDAD POPULAR DEL CESAR",
)


def expected_form(raw: str) -> dict[str, Any]:
    """Only the forms explicitly distinguished by the frozen census/D0 policy."""
    if not isinstance(raw, str) or not raw.isascii() or not raw.isdigit():
        return {"reason": "UNRECOGNIZED_RAW_FORM", "base": None,
                "declared": None, "computed": None, "split": False}
    if len(raw) == 9:
        base, declared, split, reason = raw, None, False, "NAKED_9_IS_COMPLETE_NIT"
    elif raw in LEADING_ZERO_AUTHORIZED:
        base, declared, split, reason = (
            LEADING_ZERO_AUTHORIZED[raw], None, False, "CENSUS_LEADING_ZERO"
        )
    elif raw.startswith("0"):
        return {"reason": "UNDECLARED_LEADING_ZERO", "base": None,
                "declared": None, "computed": None, "split": False}
    elif len(raw) == 10 and compute_check_digit(raw[:9]) == int(raw[-1]):
        base, declared, split, reason = raw[:9], raw[-1], True, "VALIDATED_10_TO_9_DV"
    elif len(raw) == 10:
        base, declared, split, reason = raw, None, False, "INVALID_10_DIGIT_DV"
    else:
        return {"reason": "UNRECOGNIZED_RAW_LENGTH", "base": None,
                "declared": None, "computed": None, "split": False}
    return {"reason": reason, "base": base, "declared": declared,
            "computed": compute_check_digit(base), "split": split}


def audit_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for unit in payload["buyer_units"]:
        canonical = unit["canonical_name"]
        for index, entry in enumerate(unit["fiscal_evidence"]):
            forms = entry["raw_forms"]
            policy = expected_form(forms[0]) if len(forms) == 1 else {
                "reason": "MULTIPLE_RAW_FORMS_REQUIRE_REVIEW", "base": None,
                "declared": None, "computed": None, "split": False,
            }
            digit = entry["check_digit"]
            actual = (entry["base_number"], digit["declared"],
                      digit["computed"], digit["split_validated"])
            expected = (policy["base"], policy["declared"],
                        policy["computed"], policy["split"])
            if policy["base"] is None:
                status = "REVIEW"
            elif actual == expected:
                status = {
                    "NAKED_9_IS_COMPLETE_NIT": "PRESERVED",
                    "CENSUS_LEADING_ZERO": "VALID_LEADING_ZERO_NORMALIZATION",
                    "VALIDATED_10_TO_9_DV": "VALID_10_TO_9_DV_SPLIT",
                    "INVALID_10_DIGIT_DV": "INVALID_10_DIGIT_PRESERVED",
                }[policy["reason"]]
            elif (policy["reason"] == "NAKED_9_IS_COMPLETE_NIT"
                  and entry["base_number"] == forms[0][:8]
                  and digit["declared"] == forms[0][-1]
                  and digit["split_validated"] is True):
                status = "UNAUTHORIZED_9_TO_8_SPLIT"
            else:
                status = "REVIEW"
            if entry.get("scheme") != "NIT":
                status = "REVIEW"
            rows.append({
                "canonical_name": canonical,
                "evidence_index": index,
                "kind": entry["kind"],
                "scheme": entry.get("scheme"),
                "source": entry["source"],
                "raw_forms": forms,
                "current_base_number": entry["base_number"],
                "raw_length": len(forms[0]) if len(forms) == 1 else None,
                "split_validated": digit["split_validated"],
                "declared_dv": digit["declared"],
                "computed_dv": digit["computed"],
                "normalization_reason": policy["reason"],
                "expected_base_number": policy["base"],
                "expected_declared_dv": policy["declared"],
                "expected_computed_dv": policy["computed"],
                "expected_split_validated": policy["split"],
                "shared": entry["shared"],
                "observed_at": entry["observed_at"],
                "evidence": entry["evidence"],
                "outcome": entry.get("outcome"),
                "outcome_note": entry.get("outcome_note"),
                "status": status,
            })
    return rows


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
                    encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


def _counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(sorted(Counter(row["status"] for row in rows).items()))


def _baseline_key(row: dict[str, Any]) -> tuple[str, int]:
    return row["canonical_name"], row["evidence_index"]


def _fiscal_parity(payload: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    frozen = json.loads(FROZEN_SCHEMA2.read_text(encoding="utf-8"))
    authorities = {row["canonical_name"]: row for row in frozen["authorities"]}
    by_canonical = {unit["canonical_name"]: unit for unit in payload["buyer_units"]}
    audit_index = {_baseline_key(row): row for row in rows}
    comparisons = []
    for canonical, previous in authorities.items():
        unit = by_canonical.get(canonical)
        attestations = [(index, entry) for index, entry in enumerate(unit["fiscal_evidence"])
                        if entry["kind"] == "ACCREDITATION"] if unit else []
        entry = attestations[0][1] if len(attestations) == 1 else None
        audit = audit_index.get((canonical, attestations[0][0])) if entry else None
        raw_preserved = bool(entry and entry["raw_forms"] == [previous["schema2_nit"]])
        authority_preserved = bool(
            raw_preserved and audit and entry["scheme"] == "NIT" and audit["status"] in {
                "PRESERVED", "VALID_10_TO_9_DV_SPLIT", "INVALID_10_DIGIT_PRESERVED"
            } and entry["source"] == SCHEMA2_SOURCE
            and entry["evidence"] == previous["evidence"]
            and entry["observed_at"] == previous["observed_at"]
        )
        comparisons.append({
            "canonical": canonical,
            "schema2_nit": previous["schema2_nit"],
            "schema3_accreditation": entry,
            "raw_preserved": raw_preserved,
            "normalization_class": audit["status"] if audit else "REVIEW",
            "authority_preserved": authority_preserved,
        })
    all_accreditations = [row for row in rows if row["kind"] == "ACCREDITATION"]
    return {
        "schema2_nit_authorities": len(authorities),
        "schema3_accreditations": len(all_accreditations),
        "preserved": sum(item["authority_preserved"] for item in comparisons),
        "comparisons": comparisons,
        "pass": (len(authorities) == len(all_accreditations) == len(comparisons)
                 and all(item["authority_preserved"] for item in comparisons)),
    }


def verify(payload: dict[str, Any], rows: list[dict[str, Any]], before: list[dict[str, Any]]) -> dict:
    before_index = {_baseline_key(row): row for row in before}
    after_index = {_baseline_key(row): row for row in rows}
    protected = ("kind", "scheme", "source", "raw_forms", "shared", "observed_at",
                 "evidence", "outcome", "outcome_note")
    unchanged = (before_index.keys() == after_index.keys()
                 and all(all(before_index[key][field] == after_index[key][field]
                             for field in protected) for key in before_index))
    fiscal = _fiscal_parity(payload, rows)
    units = {unit["canonical_name"]: unit for unit in payload["buyer_units"]}
    d0 = []
    frozen = {row["canonical_name"]: row for row in
              json.loads(FROZEN_SCHEMA2.read_text(encoding="utf-8"))["authorities"]}
    for canonical in D0_CANONICALS:
        unit = units[canonical]
        attested = [entry for entry in unit["fiscal_evidence"] if entry["kind"] == "ACCREDITATION"]
        absent = [entry for entry in unit["fiscal_evidence"]
                  if entry["kind"] == "SOURCE_OBSERVATION"
                  and entry["source"] == "SECOP_DIRECTORY" and entry["outcome"] == "NOT_FOUND"]
        nit = frozen[canonical]["schema2_nit"]
        d0.append({"canonical": canonical, "schema2_nit": nit,
                   "accreditation_raw_preserved": len(attested) == 1 and attested[0]["raw_forms"] == [nit],
                   "accreditation_authority_preserved": any(
                       item["canonical"] == canonical and item["authority_preserved"]
                       for item in fiscal["comparisons"]),
                   "directory_not_found_preserved": len(absent) == 1})
    counts = _counts(rows)
    observations = sum(row["kind"] == "SOURCE_OBSERVATION" for row in rows)
    pass_gate = (len(rows) == len(before) == 612 and observations == 594
                 and unchanged and fiscal["pass"]
                 and counts.get("UNAUTHORIZED_9_TO_8_SPLIT", 0) == 0
                 and counts.get("REVIEW", 0) == 0
                 and all(all(value for key, value in item.items() if key.endswith("_preserved"))
                         for item in d0))
    return {
        "entries_before": len(before), "entries_after": len(rows),
        "source_observations_audited": observations,
        "source_facts_unchanged": unchanged,
        "before_status_counts": _counts(before),
        "after_status_counts": counts,
        "unauthorized_9_to_8_before": _counts(before).get("UNAUTHORIZED_9_TO_8_SPLIT", 0),
        "unauthorized_9_to_8_after": counts.get("UNAUTHORIZED_9_TO_8_SPLIT", 0),
        "review_after": counts.get("REVIEW", 0),
        "fiscal_authority": fiscal,
        "d0_concrete_authorities": d0,
        "verdict": "PASS" if pass_gate else "FAIL",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-before", action="store_true")
    args = parser.parse_args()
    payload = json.loads(SIDECAR.read_text(encoding="utf-8"))
    rows = audit_entries(payload)
    before_path = OUT / "fiscal_evidence_audit_before.jsonl"
    if args.snapshot_before:
        if before_path.exists():
            raise SystemExit("before matrix already frozen")
        _write_jsonl(before_path, rows)
        _write_json(OUT / "fiscal_evidence_audit_before_summary.json",
                    {"candidate_before": "8fd906de4d5c7950d9f424504bd18b5ebef906ac",
                     "entries": len(rows), "kinds": dict(Counter(row["kind"] for row in rows)),
                     "status_counts": _counts(rows)})
        print(json.dumps(_counts(rows)))
        return 0
    before = _read_jsonl(before_path)
    result = verify(payload, rows, before)
    _write_jsonl(OUT / "fiscal_evidence_audit_after.jsonl", rows)
    _write_json(OUT / "fiscal_authority_parity.json", result)
    print(json.dumps({key: result[key] for key in
                      ("entries_after", "source_observations_audited", "after_status_counts",
                       "unauthorized_9_to_8_after", "review_after", "verdict")}))
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
