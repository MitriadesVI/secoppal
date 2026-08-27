"""Canario vivo: el que atrapó la contradicción, repetido contra la fuente real.

El cierre HCA midió esto sobre `oportunidades vigentes de dotación escolar en
Nariño`:

    hard_filter_violations:        0     ← el SoQL es válido y la fuente lo respeta
    semantic_contract_violations:  1     ← pero pregunta el universo equivocado

Esa asimetría es el punto entero de la campaña: un filtro duro correcto no
prueba que la pregunta lo fuera. Aquí se comprueban las dos cosas por separado,
y ambas se derivan del FRAME, no de constantes escritas a mano:

``hard_filter_violations``
    cada fila devuelta satisface cada filtro que el frame proyectó.

``semantic_contract_violations``
    los filtros proyectados corresponden a la familia que el frame declara.

Read-only, cinco filas por consulta, sin guardar filas ni secretos, sin tocar
stores de producción.

Uso:
    .venv/bin/python docs/audits/evidence/2026-08-26_HCA_FRESH_SYSTEMIC_REMEDIATION/live_canary.py \
        [--json <salida>]
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

EVIDENCE = Path(__file__).resolve().parent
ROOT = EVIDENCE.parents[3]
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402
from app.core.estado_families import ESTADO_FAMILIES  # noqa: E402
from scripts.audits.hca_r4_1_live_precheck import SocrataProbe  # noqa: E402

DATASET_IDS = {"procesos": "p6dx-8zbt", "contratos": "jbjy-vk9h"}

_STATE_FIELDS = (
    "estado_del_procedimiento",
    "estado_de_apertura_del_proceso",
    "estado_contrato",
)


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


def record_for(case: str, turn: int = 1) -> dict[str, Any]:
    for line in (EVIDENCE / "fresh-run" / "results.jsonl").read_text().splitlines():
        row = json.loads(line)
        if row["case"] == case and row["turn"] == turn:
            return row
    raise KeyError((case, turn))


def hard_filter_problems(row: dict[str, Any], params: dict[str, Any], dataset: str) -> list[str]:
    """Cada filtro que el frame proyectó tiene que cumplirse en la fila."""
    problems: list[str] = []

    for field in _STATE_FIELDS:
        expected = params.get(field)
        if not expected:
            continue
        allowed = {fold(value) for value in expected}
        if fold(row.get(field)) not in allowed:
            problems.append(f"{field}={row.get(field)!r} fuera de {sorted(expected)}")

    department = params.get("departamento_resolved")
    if department:
        observed = {fold(row.get("departamento_entidad")), fold(row.get("departamento"))}
        if fold(department) not in observed:
            problems.append(f"departamento {sorted(observed)} != {department!r}")

    low, high = params.get("valor_min"), params.get("valor_max")
    if low is not None or high is not None:
        field = "valor_del_contrato" if dataset == "contratos" else "precio_base"
        value = as_number(row.get(field))
        if value is None:
            problems.append(f"{field} no numérico ({row.get(field)!r})")
        else:
            if low is not None and value < low:
                problems.append(f"{field}={value} < valor_min={low}")
            if high is not None and value > high:
                problems.append(f"{field}={value} > valor_max={high}")

    object_fields = (
        ("objeto_del_contrato", "descripcion_del_proceso")
        if dataset == "contratos"
        else ("nombre_del_procedimiento", "descripci_n_del_procedimiento")
    )
    haystack = " ".join(fold(row.get(field)) for field in object_fields)
    for term in params.get("objeto") or []:
        if isinstance(term, list):
            if not any(fold(option) in haystack for option in term):
                problems.append(f"objeto no contiene ninguna de {term}")
        elif fold(term) not in haystack:
            problems.append(f"objeto no contiene {term!r}")

    return problems


def semantic_contract_problems(params: dict[str, Any], dataset: str) -> list[str]:
    """Los filtros proyectados tienen que ser los de la familia declarada."""
    family = params.get("estado_family")
    if not family:
        return []
    declared = ESTADO_FAMILIES.get(str(family), {}).get(dataset, {})
    problems: list[str] = []
    for field in _STATE_FIELDS:
        projected = set(params.get(field) or [])
        if not projected:
            continue
        authorized = set(declared.get(field) or [])
        stray = projected - authorized
        if stray:
            problems.append(
                f"{field}={sorted(projected)} no pertenece a "
                f"{family}.{dataset} ({sorted(authorized) or 'sin proyección'})"
            )
    if not any(params.get(field) for field in _STATE_FIELDS):
        problems.append(f"declara estado_family={family!r} sin proyectar ningún estado")
    return problems


def run_canary(probe: SocrataProbe, canary_id: str, case: str, turn: int = 1) -> dict[str, Any]:
    record = record_for(case, turn)
    params = record["params"]
    dataset = params["dataset"]
    query = limited(record["soql"])
    outcome = probe.query(
        DATASET_IDS[dataset], query, label=canary_id, timeout_s=30.0, max_attempts=1
    )
    rows = outcome.get("rows")
    attempt = outcome["attempts"][-1]

    violations: list[dict[str, Any]] = []
    for index, row in enumerate(rows or []):
        problems = hard_filter_problems(row, params, dataset)
        if problems:
            violations.append({"row_index": index, "problems": problems})

    semantic = semantic_contract_problems(params, dataset)
    source_available = attempt["status"] == "ok"
    rows_seen = 0 if rows is None else len(rows)
    predicate_observed = source_available and rows_seen > 0
    semantic_check_executed = source_available
    case_verdict = (
        "PASS"
        if predicate_observed and not violations and not semantic
        else "LIVE_INCONCLUSIVE"
        if not predicate_observed
        else "FAIL"
    )
    summary = {
        "id": canary_id,
        "case": case,
        "query_text": record["q"],
        "dataset": dataset,
        "status": attempt["status"],
        "verdict": case_verdict,
        "source_available": source_available,
        "rows_seen": rows_seen,
        "predicate_observed": predicate_observed,
        "semantic_check_executed": semantic_check_executed,
        "latency_s": attempt.get("latency_s"),
        "row_count": None if rows is None else len(rows),
        "projected_state": {
            field: params.get(field) for field in _STATE_FIELDS if params.get(field)
        },
        "estado_family": params.get("estado_family"),
        "valor_min": params.get("valor_min"),
        "valor_max": params.get("valor_max"),
        "hard_filter_violations": len(violations),
        "violation_details": violations,
        "semantic_contract_violations": len(semantic),
        "semantic_contract_details": semantic,
        "soql": query,
    }
    if attempt["status"] != "ok":
        summary["error"] = attempt.get("error")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default=None)
    args = parser.parse_args()

    settings = get_settings()
    probe = SocrataProbe(settings.datos_gov_domain, settings.secop_app_token)

    canaries = [
        # El canario que produjo la contradicción: familia oferta_abierta con
        # filtros Cerrado/Seleccionado.
        run_canary(probe, "LIVE-OPPORTUNITY-FAMILY", "FRESH-STATE-002"),
        # El rango pequeño que antes viajaba como techo sin piso.
        run_canary(probe, "LIVE-MONEY-SMALL-RANGE", "FRESH-MONEY-003"),
        # Comprador completo tras rutear el verbo de compra a contratos.
        run_canary(probe, "LIVE-BUYER-PURCHASE-VERB", "FRESH-CROSS-004"),
    ]

    reachable = [row for row in canaries if row["source_available"]]
    observed = [row for row in canaries if row["predicate_observed"]]
    report = {
        "gate": "LIVE_CANARY",
        "completed_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "read_only": True,
        "row_limit_per_query": 5,
        "canaries_run": len(canaries),
        "canaries_reachable": len(reachable),
        "canaries_nonvacuous": len(observed),
        "rows_seen": sum(row["rows_seen"] for row in reachable),
        "hard_filter_violations": sum(row["hard_filter_violations"] for row in reachable),
        "semantic_contract_violations": sum(
            row["semantic_contract_violations"] for row in reachable
        ),
        "verdict": (
            "PASS"
            if len(observed) == len(canaries)
            and not any(row["hard_filter_violations"] for row in reachable)
            and not any(row["semantic_contract_violations"] for row in reachable)
            else "FAIL"
            if observed and (
                any(row["hard_filter_violations"] for row in reachable)
                or any(row["semantic_contract_violations"] for row in reachable)
            )
            else "LIVE_INCONCLUSIVE"
        ),
        "canaries": canaries,
    }
    text = json.dumps(report, ensure_ascii=False, indent=1)
    if args.json:
        Path(args.json).write_text(text + "\n", encoding="utf-8")
    print(text)  # noqa: T201 - CLI de auditoría


if __name__ == "__main__":
    main()
