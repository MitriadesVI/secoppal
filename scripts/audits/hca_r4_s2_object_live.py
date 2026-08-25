#!/usr/bin/env python3
"""Bounded live verification for HCA-R4-S2 OBJECT exclusion.

Read-only: issues Socrata GET requests against the two runtime datasets and
writes a local JSON evidence artifact when ``--output`` is provided.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from app.config import get_settings  # noqa: E402
from app.core.soql_builder import (  # noqa: E402
    SoQLBuilder,
    project_object_term_variants,
)


DATASETS = {
    SoQLBuilder.PROCESOS_DATASET: {
        "label": "procesos",
        "id_field": "id_del_proceso",
    },
    SoQLBuilder.CONTRATOS_DATASET: {
        "label": "contratos",
        "id_field": "id_contrato",
    },
}
POSITIVE_TERM = "alimentacion"
EXCLUDED_TERM = "transporte"
SYNTHETIC_ABSENT_TERM = "zznegobjectr4s220260825"
COMPARISON_SCOPE = {
    "departamento_resolved": "Atlántico",
    "fecha_desde": "2025-01-01",
    "fecha_hasta": "2026-08-24",
}


def _fold(value: object) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "").casefold())
    return "".join(
        char for char in normalized if not unicodedata.combining(char)
    )


class Probe:
    def __init__(self, domain: str, app_token: str | None) -> None:
        self.domain = domain
        self.headers = {"X-App-Token": app_token} if app_token else {}
        self.session = requests.Session()
        self.operations: list[dict[str, Any]] = []

    def get(
        self,
        dataset_id: str,
        soql: str,
        *,
        label: str,
        timeout_s: float = 60.0,
    ) -> list[dict[str, Any]] | None:
        started = time.perf_counter()
        url = f"https://{self.domain}/resource/{dataset_id}.json"
        try:
            response = self.session.get(
                url,
                params={"$query": soql},
                headers=self.headers,
                timeout=timeout_s,
            )
            response.raise_for_status()
            rows = response.json()
            operation = {
                "label": label,
                "dataset": dataset_id,
                "soql": soql,
                "status": "ok",
                "latency_s": round(time.perf_counter() - started, 4),
                "row_count": len(rows),
            }
        except Exception as exc:  # noqa: BLE001 - evidence records exact class
            rows = None
            operation = {
                "label": label,
                "dataset": dataset_id,
                "soql": soql,
                "status": "error",
                "latency_s": round(time.perf_counter() - started, 4),
                "error": f"{type(exc).__name__}: {str(exc)[:500]}",
            }
        self.operations.append(operation)
        return rows


def _count(rows: list[dict[str, Any]] | None) -> int | None:
    if not rows:
        return None
    return int(next(iter(rows[0].values())))


def _row_has_term(row: dict[str, Any], fields: tuple[str, str], term: str) -> bool:
    variants = tuple(_fold(value) for value in project_object_term_variants(term))
    return any(
        variant in _fold(row.get(field))
        for field in fields
        for variant in variants
    )


def _sample(rows: list[dict[str, Any]], fields: tuple[str, str], id_field: str) -> list[dict[str, Any]]:
    return [
        {
            "id": row.get(id_field),
            fields[0]: str(row.get(fields[0]) or "")[:300],
            fields[1]: str(row.get(fields[1]) or "")[:300],
        }
        for row in rows[:10]
    ]


def verify_dataset(
    probe: Probe,
    builder: SoQLBuilder,
    dataset_id: str,
) -> dict[str, Any]:
    spec = builder.SPECS[dataset_id]
    fields = (spec.object_name, str(spec.description))
    id_field = DATASETS[dataset_id]["id_field"]
    positive_params = {
        **COMPARISON_SCOPE,
        "objeto": [POSITIVE_TERM],
        "limit": 100,
    }
    excluded_params = {
        **COMPARISON_SCOPE,
        "objeto": [POSITIVE_TERM],
        "objeto_exclude": [EXCLUDED_TERM],
        "limit": 100,
    }
    positive_soql = builder.build(dataset_id, positive_params)
    excluded_soql = builder.build(dataset_id, excluded_params)
    positive_count_soql = builder.build_count(dataset_id, positive_params)
    excluded_count_soql = builder.build_count(dataset_id, excluded_params)

    positive_rows = probe.get(
        dataset_id, positive_soql, label=f"{dataset_id}:positive_fetch",
    )
    excluded_rows = probe.get(
        dataset_id, excluded_soql, label=f"{dataset_id}:excluded_fetch",
    )
    positive_count = _count(probe.get(
        dataset_id, positive_count_soql, label=f"{dataset_id}:positive_count",
    ))
    excluded_count = _count(probe.get(
        dataset_id, excluded_count_soql, label=f"{dataset_id}:excluded_count",
    ))

    positive_violations = [
        row for row in (positive_rows or [])
        if _row_has_term(row, fields, EXCLUDED_TERM)
    ]
    excluded_violations = [
        row for row in (excluded_rows or [])
        if _row_has_term(row, fields, EXCLUDED_TERM)
    ]

    negative_only_where = builder.build_count(
        dataset_id,
        {"objeto_exclude": [SYNTHETIC_ABSENT_TERM]},
    ).split(" WHERE ", 1)[1]
    null_base = f"({fields[0]} IS NULL OR {fields[1]} IS NULL)"
    null_select = f"SELECT {id_field}, {fields[0]}, {fields[1]}"
    null_positive_soql = f"{null_select} WHERE {null_base} LIMIT 100"
    null_excluded_soql = (
        f"{null_select} WHERE {null_base} AND ({negative_only_where}) LIMIT 100"
    )
    null_positive_rows = probe.get(
        dataset_id,
        null_positive_soql,
        label=f"{dataset_id}:null_candidates",
    )
    null_excluded_rows = probe.get(
        dataset_id,
        null_excluded_soql,
        label=f"{dataset_id}:null_candidates_with_exclusion",
    )
    null_positive_ids = {
        row.get(id_field) for row in (null_positive_rows or [])
    }
    null_excluded_ids = {
        row.get(id_field) for row in (null_excluded_rows or [])
    }
    missing_null_ids = sorted(
        str(value) for value in null_positive_ids - null_excluded_ids
    )

    completed = all(value is not None for value in (
        positive_rows,
        excluded_rows,
        positive_count,
        excluded_count,
        null_positive_rows,
        null_excluded_rows,
    ))
    passed = bool(
        completed
        and not excluded_violations
        and excluded_count <= positive_count
        and not missing_null_ids
    )
    return {
        "dataset": dataset_id,
        "label": DATASETS[dataset_id]["label"],
        "fields": fields,
        "comparison": {
            "scope": COMPARISON_SCOPE,
            "positive_term": POSITIVE_TERM,
            "excluded_term": EXCLUDED_TERM,
            "positive": {
                "soql": positive_soql,
                "count_soql": positive_count_soql,
                "total_count": positive_count,
                "fetched_rows": len(positive_rows or []),
                "rows_containing_excluded_term": len(positive_violations),
                "matching_samples": _sample(
                    positive_violations, fields, id_field,
                ),
            },
            "with_exclusion": {
                "soql": excluded_soql,
                "count_soql": excluded_count_soql,
                "total_count": excluded_count,
                "fetched_rows": len(excluded_rows or []),
                "violations": len(excluded_violations),
                "violation_samples": _sample(
                    excluded_violations, fields, id_field,
                ),
            },
        },
        "null_cases": {
            "synthetic_absent_term": SYNTHETIC_ABSENT_TERM,
            "candidate_soql": null_positive_soql,
            "excluded_soql": null_excluded_soql,
            "candidate_count": len(null_positive_rows or []),
            "survivor_count": len(null_excluded_rows or []),
            "missing_ids": missing_null_ids,
            "samples": _sample(null_excluded_rows or [], fields, id_field),
            "not_applicable_reason": (
                "No NULL rows observed in the governed fields in this live snapshot."
                if completed and not null_positive_rows
                else None
            ),
        },
        "status": "PASS" if passed else ("INCONCLUSIVE" if not completed else "FAIL"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    settings = get_settings()
    probe = Probe(settings.datos_gov_domain, settings.secop_app_token)
    builder = SoQLBuilder()
    started = datetime.now(timezone.utc)
    datasets = {
        dataset_id: verify_dataset(probe, builder, dataset_id)
        for dataset_id in DATASETS
    }
    statuses = {item["status"] for item in datasets.values()}
    verdict = (
        "PASS" if statuses == {"PASS"}
        else "LIVE_SOURCE_INCONCLUSIVE" if "INCONCLUSIVE" in statuses
        else "FAIL"
    )
    artifact = {
        "schema": "secoppal.hca_r4_s2_object_live.v1",
        "started_at_utc": started.isoformat(),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "domain": settings.datos_gov_domain,
            "authenticated_with_app_token": bool(settings.secop_app_token),
            "read_only": True,
        },
        "datasets": datasets,
        "transport_operations": probe.operations,
        "verdict": {"HCA_R4_S2_OBJECT_LIVE": verdict},
    }
    rendered = json.dumps(artifact, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        output = args.output if args.output.is_absolute() else REPO / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
        print(output)
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
