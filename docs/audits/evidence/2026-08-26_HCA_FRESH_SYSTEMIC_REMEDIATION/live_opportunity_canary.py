"""Canario vivo acotado para la proyección dataset-aware de ``vigente``.

Genera el SoQL desde el runtime actual, hace un único GET read-only con LIMIT 5
y no persiste filas. El contrato semántico se valida aparte de los filtros que
la fuente efectivamente aplicó.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from app.core.query_router import QueryRouter  # noqa: E402
from app.core.soql_builder import SoQLBuilder  # noqa: E402
from scripts.audits.hca_r4_1_live_precheck import SocrataProbe  # noqa: E402


QUERY = "oportunidades vigentes de dotación escolar en Nariño"
AUTHORIZED_STATES = {"Publicado", "Borrador", "En aprobación", "Aprobado", "Abierto"}


def fold(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join(
        "".join(char for char in normalized if not unicodedata.combining(char))
        .lower()
        .split()
    )


def limited(soql: str) -> str:
    return re.sub(r"\bLIMIT\s+\d+\s*$", "LIMIT 5", soql, flags=re.IGNORECASE)


def run() -> dict:
    params = QueryRouter().parse(QUERY).params
    builder = SoQLBuilder()
    dataset_id = builder.dataset_id_for(params["dataset"])
    soql = limited(builder.build(dataset_id, params))

    settings = get_settings()
    probe = SocrataProbe(settings.datos_gov_domain, settings.secop_app_token)
    outcome = probe.query(
        dataset_id,
        soql,
        label="HCA-FRESH-LIVE-OPPORTUNITY",
        timeout_s=30.0,
        max_attempts=1,
    )
    rows = outcome.get("rows")
    hard_violations: list[dict] = []
    authorized_folded = {fold(value) for value in AUTHORIZED_STATES}
    for index, row in enumerate(rows or []):
        problems: list[str] = []
        if fold(row.get("departamento_entidad")) != "narino":
            problems.append("departamento_entidad != Nariño")
        if fold(row.get("estado_del_procedimiento")) not in authorized_folded:
            problems.append("estado_del_procedimiento fuera de oferta_abierta")
        if fold(row.get("estado_de_apertura_del_proceso")) != "abierto":
            problems.append("estado_de_apertura_del_proceso != Abierto")
        haystack = " ".join(
            (
                fold(row.get("nombre_del_procedimiento")),
                fold(row.get("descripci_n_del_procedimiento")),
            )
        )
        for term in ("dotacion", "escolar"):
            if term not in haystack:
                problems.append(f"objeto no contiene {term}")
        if problems:
            hard_violations.append({"row_index": index, "problems": problems})

    actual_states = set(params.get("estado_del_procedimiento") or [])
    opening_states = set(params.get("estado_de_apertura_del_proceso") or [])
    semantic_ok = (
        params.get("estado_family") == "oferta_abierta"
        and bool(actual_states)
        and actual_states <= AUTHORIZED_STATES
        and "Seleccionado" not in actual_states
        and "Cerrado" not in opening_states
    )
    attempt = outcome["attempts"][-1]
    live_ok = attempt.get("status") == "ok"
    rows_seen = 0 if rows is None else len(rows)
    predicate_observed = live_ok and rows_seen > 0
    status = (
        "PASS"
        if predicate_observed and not hard_violations and semantic_ok
        else "LIVE_INCONCLUSIVE"
        if not predicate_observed
        else "FAIL"
    )
    return {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "method": "One bounded read-only Socrata GET; LIMIT 5; one attempt; rows not persisted.",
        "query": QUERY,
        "status": status,
        "source_status": attempt.get("status"),
        "source_available": live_ok,
        "rows_seen": rows_seen,
        "predicate_observed": predicate_observed,
        "semantic_check_executed": live_ok,
        "latency_s": attempt.get("latency_s"),
        "row_count": None if rows is None else len(rows),
        "state_projection": {
            "estado_family": params.get("estado_family"),
            "estado_de_apertura_del_proceso": params.get(
                "estado_de_apertura_del_proceso"
            )
            or [],
            "estado_del_procedimiento": params.get("estado_del_procedimiento") or [],
        },
        "hard_filter_violations": len(hard_violations),
        "semantic_contract_violations": 0 if semantic_ok else 1,
        "violation_details": hard_violations,
        "soql": soql,
        **({"error": attempt.get("error")} if not live_ok else {}),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default=None)
    args = parser.parse_args()
    report = run()
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.json:
        Path(args.json).write_text(output, encoding="utf-8")
    print(output, end="")  # noqa: T201 - CLI de auditoría
    raise SystemExit(0 if report["status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
