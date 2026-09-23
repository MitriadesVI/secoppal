#!/usr/bin/env python3
"""Apply the bounded R1 fiscal representation repair to the Schema 3 sidecar."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.audits.adr073_fiscal_authority_gate import SIDECAR, audit_entries


def main() -> None:
    payload = json.loads(SIDECAR.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 3:
        raise ValueError("R1 requires the frozen Schema 3 candidate")
    projection_before = [
        (unit["canonical_name"], unit["source_names"], unit["projection_evidence"])
        for unit in payload["buyer_units"]
    ]
    rows = audit_entries(payload)
    if len(rows) != 612:
        raise ValueError("R1 fiscal entry count changed")
    repairable = []
    for row in rows:
        if row["status"] == "UNAUTHORIZED_9_TO_8_SPLIT":
            repairable.append(row)
        elif row["status"] == "REVIEW" and (
            row["canonical_name"] == "SECRETARÍA DISTRITAL DE CULTURA, RECREACIÓN Y DEPORTE"
            and row["raw_forms"] == ["0899999061"]
            and row["normalization_reason"] == "CENSUS_LEADING_ZERO"
        ):
            repairable.append(row)
        elif row["status"] == "REVIEW":
            raise ValueError(f"R1 requires adjudication: {row['canonical_name']}")
    by_canonical = {unit["canonical_name"]: unit for unit in payload["buyer_units"]}
    for row in repairable:
        entry = by_canonical[row["canonical_name"]]["fiscal_evidence"][row["evidence_index"]]
        entry["base_number"] = row["expected_base_number"]
        entry["check_digit"] = {
            "declared": row["expected_declared_dv"],
            "computed": row["expected_computed_dv"],
            "split_validated": row["expected_split_validated"],
        }
    projection_after = [
        (unit["canonical_name"], unit["source_names"], unit["projection_evidence"])
        for unit in payload["buyer_units"]
    ]
    if projection_after != projection_before:
        raise AssertionError("R1 changed a projection or canonical")
    after = audit_entries(payload)
    if any(row["status"] in {"UNAUTHORIZED_9_TO_8_SPLIT", "REVIEW"} for row in after):
        raise AssertionError("R1 left an unadjudicated fiscal form")
    if repairable:
        temp = Path(f"{SIDECAR}.r1-tmp")
        temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                        encoding="utf-8")
        temp.replace(SIDECAR)
    print(json.dumps({"entries": len(after), "repaired": len(repairable),
                      "accreditations": sum(row["kind"] == "ACCREDITATION" for row in after),
                      "observations": sum(row["kind"] == "SOURCE_OBSERVATION" for row in after)}))


if __name__ == "__main__":
    main()
