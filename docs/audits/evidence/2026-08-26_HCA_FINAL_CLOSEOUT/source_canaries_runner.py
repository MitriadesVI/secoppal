"""Canarios read-only y acotados contra Socrata para el cierre HCA.

Reutiliza SoQL ya generado por el fresh challenge, limita cada GET a cinco
filas, no guarda filas ni secretos y no toca stores de producción.
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


EVIDENCE = Path(__file__).resolve().parent
ROOT = EVIDENCE.parents[3]
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from scripts.audits.hca_r4_1_live_precheck import SocrataProbe  # noqa: E402


DATASET_IDS = {"procesos": "p6dx-8zbt", "contratos": "jbjy-vk9h"}


def fold(value: Any) -> str:
    raw = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join(
        "".join(ch for ch in raw if not unicodedata.combining(ch)).lower().split()
    )


def as_number(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def limited(soql: str, n: int = 5) -> str:
    if re.search(r"\bLIMIT\s+\d+\s*$", soql, re.I):
        return re.sub(r"\bLIMIT\s+\d+\s*$", f"LIMIT {n}", soql, flags=re.I)
    return f"{soql} LIMIT {n}"


def where(soql: str) -> str:
    match = re.search(r"\bWHERE\s+(.*?)\s+ORDER BY\b", soql, re.I | re.S)
    return " ".join((match.group(1) if match else "").split())


def object_has(row: dict[str, Any], term: str, dataset: str) -> bool:
    fields = (
        ("objeto_del_contrato", "descripcion_del_proceso")
        if dataset == "contratos"
        else ("nombre_del_procedimiento", "descripci_n_del_procedimiento")
    )
    haystack = " ".join(fold(row.get(field)) for field in fields)
    return fold(term) in haystack


def record_for(case: str, turn: int = 1) -> dict[str, Any]:
    for line in (EVIDENCE / "fresh-run" / "results.jsonl").read_text().splitlines():
        row = json.loads(line)
        if row["case"] == case and row["turn"] == turn:
            return row
    raise KeyError((case, turn))


def run_live(
    probe: SocrataProbe,
    *,
    canary_id: str,
    record: dict[str, Any],
    validate: Callable[[dict[str, Any]], list[str]],
) -> tuple[dict[str, Any], list[dict[str, Any]] | None]:
    dataset = record["params"]["dataset"]
    query = limited(record["soql"])
    outcome = probe.query(
        DATASET_IDS[dataset], query, label=canary_id, timeout_s=30.0, max_attempts=1
    )
    rows = outcome.get("rows")
    violations: list[dict[str, Any]] = []
    if rows is not None:
        for index, row in enumerate(rows):
            problems = validate(row)
            if problems:
                violations.append({"row_index": index, "problems": problems})
    attempt = outcome["attempts"][-1]
    summary = {
        "id": canary_id,
        "query_text": record["q"],
        "dataset": dataset,
        "dataset_id": DATASET_IDS[dataset],
        "status": attempt["status"],
        "latency_s": attempt.get("latency_s"),
        "row_count": None if rows is None else len(rows),
        "hard_filter_violations": len(violations),
        "violation_details": violations,
        "soql": query,
    }
    if attempt["status"] != "ok":
        summary["error"] = attempt.get("error")
    return summary, rows


def main() -> None:
    settings = get_settings()
    probe = SocrataProbe(settings.datos_gov_domain, settings.secop_app_token)
    canaries: list[dict[str, Any]] = []
    observed_rows: list[tuple[str, dict[str, Any]]] = []

    rec = record_for("FRESH-MONEY-001")
    summary, rows = run_live(
        probe,
        canary_id="LIVE-MONEY-SMALL",
        record=rec,
        validate=lambda row: [
            problem
            for problem, ok in (
                ("valor_del_contrato < 75000", (as_number(row.get("valor_del_contrato")) or -1) >= 75000),
                ("fecha_de_firma fuera de 2025", str(row.get("fecha_de_firma") or "").startswith("2025-")),
                ("objeto no contiene cuadernos", object_has(row, "cuadernos", "contratos")),
            )
            if not ok
        ],
    )
    canaries.append(summary)
    observed_rows.extend(("contratos", row) for row in (rows or []))

    rec = record_for("FRESH-STATE-001")
    summary, rows = run_live(
        probe,
        canary_id="LIVE-PUBLISHED-EXACT",
        record=rec,
        validate=lambda row: [
            problem
            for problem, ok in (
                ("departamento_entidad != Huila", fold(row.get("departamento_entidad")) == "huila"),
                ("estado_del_procedimiento != Publicado", fold(row.get("estado_del_procedimiento")) == "publicado"),
                ("estado_de_apertura_del_proceso != Abierto", fold(row.get("estado_de_apertura_del_proceso")) == "abierto"),
                ("objeto no contiene señalizacion", object_has(row, "senalizacion", "procesos")),
            )
            if not ok
        ],
    )
    canaries.append(summary)
    observed_rows.extend(("procesos", row) for row in (rows or []))

    rec = record_for("FRESH-STATE-002")
    summary, rows = run_live(
        probe,
        canary_id="LIVE-OPPORTUNITY-FAMILY",
        record=rec,
        validate=lambda row: [
            problem
            for problem, ok in (
                ("departamento_entidad != Nariño", fold(row.get("departamento_entidad")) == "narino"),
                ("estado_de_apertura_del_proceso != Cerrado", fold(row.get("estado_de_apertura_del_proceso")) == "cerrado"),
                ("estado_del_procedimiento != Seleccionado", fold(row.get("estado_del_procedimiento")) == "seleccionado"),
                ("objeto no contiene dotacion", object_has(row, "dotacion", "procesos")),
                ("objeto no contiene escolar", object_has(row, "escolar", "procesos")),
            )
            if not ok
        ],
    )
    expected_opportunity_states = {
        "Publicado", "Borrador", "En aprobación", "Aprobado", "Abierto"
    }
    actual_opportunity_states = set(rec["params"].get("estado_del_procedimiento") or [])
    actual_opening_states = set(
        rec["params"].get("estado_de_apertura_del_proceso") or []
    )
    opportunity_semantic_ok = (
        rec["params"].get("estado_family") == "oferta_abierta"
        and actual_opportunity_states <= expected_opportunity_states
        and bool(actual_opportunity_states)
        and "Cerrado" not in actual_opening_states
    )
    summary["semantic_contract_violations"] = 0 if opportunity_semantic_ok else 1
    summary["semantic_contract_details"] = [] if opportunity_semantic_ok else [{
        "expected": "oferta_abierta projected to the authorized open funnel",
        "actual": {
            "estado_family": rec["params"].get("estado_family"),
            "estado_de_apertura_del_proceso": sorted(actual_opening_states),
            "estado_del_procedimiento": sorted(actual_opportunity_states),
        },
        "finding": "Declared oferta_abierta but queried Cerrado/Seleccionado.",
    }]
    canaries.append(summary)
    observed_rows.extend(("procesos", row) for row in (rows or []))

    rec = record_for("FRESH-BUYER-001")
    buyer = fold(rec["params"]["entidad_resolved"])
    summary, rows = run_live(
        probe,
        canary_id="LIVE-COMPLETE-BUYER",
        record=rec,
        validate=lambda row: [
            problem
            for problem, ok in (
                ("nombre_entidad no coincide con buyer", buyer in fold(row.get("nombre_entidad"))),
                ("fecha_de_firma fuera de 2025", str(row.get("fecha_de_firma") or "").startswith("2025-")),
                ("objeto no contiene parques", object_has(row, "parques", "contratos")),
            )
            if not ok
        ],
    )
    canaries.append(summary)
    observed_rows.extend(("contratos", row) for row in (rows or []))

    t1 = record_for("FRESH-FOLLOWUP-001", 1)
    t2 = record_for("FRESH-FOLLOWUP-001", 2)
    summary, rows = run_live(
        probe,
        canary_id="LIVE-FOLLOWUP-ORDERING",
        record=t2,
        validate=lambda row: [
            problem
            for problem, ok in (
                ("departamento != Meta", fold(row.get("departamento")) == "meta"),
                ("tipo_de_contrato != Obra", fold(row.get("tipo_de_contrato")) == "obra"),
                ("fecha_de_firma fuera de 2025", str(row.get("fecha_de_firma") or "").startswith("2025-")),
            )
            if not ok
        ],
    )
    summary["where_t1"] = where(t1["soql"])
    summary["where_t2"] = where(t2["soql"])
    summary["where_equal"] = summary["where_t1"] == summary["where_t2"]
    if not summary["where_equal"]:
        summary["hard_filter_violations"] += 1
        summary["violation_details"].append({"structural": "WHERE changed"})
    canaries.append(summary)
    observed_rows.extend(("contratos", row) for row in (rows or []))

    reference_field = {"procesos": "referencia_del_proceso", "contratos": "referencia_del_contrato"}
    reference: tuple[str, str] | None = None
    for dataset, row in observed_rows:
        value = str(row.get(reference_field[dataset]) or "").strip()
        if value:
            reference = (dataset, value)
            break
    if reference is None:
        seed = probe.query(
            DATASET_IDS["procesos"],
            "SELECT referencia_del_proceso WHERE referencia_del_proceso IS NOT NULL LIMIT 1",
            label="LIVE-DIRECT-LOOKUP-SEED",
            timeout_s=30.0,
            max_attempts=1,
        )
        seed_rows = seed.get("rows") or []
        if seed_rows and seed_rows[0].get("referencia_del_proceso"):
            reference = ("procesos", str(seed_rows[0]["referencia_del_proceso"]))

    if reference is None:
        canaries.append({
            "id": "LIVE-DIRECT-LOOKUP",
            "status": "inconclusive",
            "row_count": None,
            "hard_filter_violations": 0,
            "reason": "No live reference was available from the bounded canaries or seed.",
        })
    else:
        dataset, value = reference
        field = reference_field[dataset]
        escaped = value.replace("'", "''")
        query = f"SELECT {field} WHERE UPPER({field}) = UPPER('{escaped}') LIMIT 5"
        outcome = probe.query(
            DATASET_IDS[dataset], query, label="LIVE-DIRECT-LOOKUP", timeout_s=30.0, max_attempts=1
        )
        lookup_rows = outcome.get("rows")
        violations = 0 if lookup_rows is None else sum(
            fold(row.get(field)) != fold(value) for row in lookup_rows
        )
        attempt = outcome["attempts"][-1]
        canaries.append({
            "id": "LIVE-DIRECT-LOOKUP",
            "query_text": f"lookup exacto de referencia real observada ({dataset})",
            "dataset": dataset,
            "dataset_id": DATASET_IDS[dataset],
            "status": attempt["status"],
            "latency_s": attempt.get("latency_s"),
            "row_count": None if lookup_rows is None else len(lookup_rows),
            "reference": value,
            "hard_filter_violations": violations,
            "soql": query,
            **({"error": attempt.get("error")} if attempt["status"] != "ok" else {}),
        })

    live_ok = sum(item.get("status") == "ok" for item in canaries)
    total_violations = sum(int(item.get("hard_filter_violations") or 0) for item in canaries)
    semantic_violations = sum(
        int(item.get("semantic_contract_violations") or 0) for item in canaries
    )
    status = (
        "PASS"
        if live_ok == len(canaries)
        and total_violations == 0
        and semantic_violations == 0
        else "LIVE_SOURCE_INCONCLUSIVE"
        if live_ok < len(canaries)
        else "FAIL"
    )
    output = {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "method": "Six bounded read-only Socrata GET canaries; LIMIT 5; one attempt each; rows not persisted.",
        "status": status,
        "cases": len(canaries),
        "live_ok": live_ok,
        "hard_filter_violations": total_violations,
        "semantic_contract_violations": semantic_violations,
        "canaries": canaries,
    }
    (EVIDENCE / "source_canaries.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({key: output[key] for key in ("status", "cases", "live_ok", "hard_filter_violations")}, indent=2))


if __name__ == "__main__":
    main()
