#!/usr/bin/env python3
"""HCA-R4.1: precheck de fuente para negación dimensional V1.

Este runner es deliberadamente externo al runtime. Solo ejecuta GET contra
Socrata y reproducciones locales de parser/follow-up; no construye ni activa
soporte de negación.

Uso:
    .venv/bin/python scripts/audits/hca_r4_1_live_precheck.py \
      --output artifacts/audits/hca_r4_1_live_precheck_2026-08-24.json
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from app.config import get_settings
from app.core.followup_engine import detect_and_merge
from app.core.query_frame import frame_from_params
from app.core.query_router import QueryRouter, _CITY_TO_ENTITY, _COMMON_CITIES
from app.core.soql_builder import SoQLBuilder


DATASETS = {
    "p6dx-8zbt": {
        "label": "procesos",
        "id_field": "id_del_proceso",
        "object_fields": (
            "nombre_del_procedimiento",
            "descripci_n_del_procedimiento",
        ),
        "city_field": "ciudad_entidad",
        "department_field": "departamento_entidad",
    },
    "jbjy-vk9h": {
        "label": "contratos",
        "id_field": "id_contrato",
        "object_fields": ("objeto_del_contrato", "descripcion_del_proceso"),
        "city_field": "ciudad",
        "department_field": "departamento",
    },
}
SYNTHETIC_TERM = "zznegprecheck20260824"
PRESENT_TERM = "servicios"
ETAPA6_P90_GATE_S = 15.0
ETAPA6_OBSERVED_P90_S = 11.6


def _fold(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join(
        "".join(char for char in text if not unicodedata.combining(char))
        .lower()
        .split()
    )


def _pct(numerator: int, denominator: int) -> float:
    return round(100.0 * numerator / denominator, 8) if denominator else 0.0


def _nearest_rank(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def _summary(samples: list[dict[str, Any]], variant: str) -> dict[str, Any]:
    selected = [sample for sample in samples if sample["variant"] == variant]
    values = sorted(
        float(sample["latency_s"])
        for sample in selected
        if sample["status"] == "ok"
    )
    return {
        "attempts": len(selected),
        "successes": len(values),
        "failures": len(selected) - len(values),
        "min_s": round(min(values), 4) if values else None,
        "p50_s": round(statistics.median(values), 4) if values else None,
        "p90_s_nearest_rank": round(_nearest_rank(values, 0.90), 4)
        if values
        else None,
        "max_s": round(max(values), 4) if values else None,
    }


class SocrataProbe:
    """Transporte sin retries ocultos; cada intento queda en el artefacto."""

    def __init__(self, domain: str, app_token: str | None) -> None:
        self.domain = domain
        self.headers = {"X-App-Token": app_token} if app_token else {}
        self.session = requests.Session()
        self.operations: list[dict[str, Any]] = []

    def request_once(
        self,
        dataset_id: str,
        query: str,
        *,
        timeout_s: float,
        label: str,
        record: bool = True,
    ) -> dict[str, Any]:
        url = f"https://{self.domain}/resource/{dataset_id}.json"
        started = time.perf_counter()
        try:
            response = self.session.get(
                url,
                params={"$query": query},
                headers=self.headers,
                timeout=timeout_s,
            )
            latency = time.perf_counter() - started
            response.raise_for_status()
            rows = response.json()
            outcome = {
                "status": "ok",
                "latency_s": round(latency, 4),
                "row_count": len(rows),
                "response_bytes": len(response.content),
                "rows": rows,
            }
        except Exception as exc:  # la clase exacta queda registrada
            latency = time.perf_counter() - started
            outcome = {
                "status": "timeout"
                if isinstance(exc, requests.Timeout)
                else "error",
                "latency_s": round(latency, 4),
                "error": f"{type(exc).__name__}: {str(exc)[:300]}",
            }
        if record:
            self.operations.append(
                {
                    "label": label,
                    "dataset_id": dataset_id,
                    "query": query,
                    "timeout_s": timeout_s,
                    **{key: value for key, value in outcome.items() if key != "rows"},
                }
            )
        return outcome

    def query(
        self,
        dataset_id: str,
        query: str,
        *,
        label: str,
        timeout_s: float = 60.0,
        max_attempts: int = 2,
    ) -> dict[str, Any]:
        attempts: list[dict[str, Any]] = []
        for attempt_no in range(1, max_attempts + 1):
            outcome = self.request_once(
                dataset_id,
                query,
                timeout_s=timeout_s,
                label=f"{label}:attempt_{attempt_no}",
            )
            attempts.append(outcome)
            if outcome["status"] == "ok":
                return {
                    "query": query,
                    "attempts": [
                        {key: value for key, value in item.items() if key != "rows"}
                        for item in attempts
                    ],
                    "rows": outcome["rows"],
                }
            if attempt_no < max_attempts:
                time.sleep(0.5)
        return {
            "query": query,
            "attempts": [
                {key: value for key, value in item.items() if key != "rows"}
                for item in attempts
            ],
            "rows": None,
        }

    def count(self, dataset_id: str, query: str, *, label: str) -> tuple[int, dict[str, Any]]:
        result = self.query(dataset_id, query, label=label)
        rows = result["rows"]
        if not rows:
            raise RuntimeError(f"measurement did not complete: {label}")
        return int(rows[0]["n"]), result


def _like(field: str, term: str) -> str:
    return (
        f"unaccent(upper({field})) LIKE "
        f"unaccent(upper('%{term}%'))"
    )


def measure_object_nulls(probe: SocrataProbe) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for dataset_id, spec in DATASETS.items():
        f1, f2 = spec["object_fields"]
        counts: dict[str, int] = {}
        evidence: dict[str, Any] = {}
        query_map = {
            "total": "SELECT count(*) AS n",
            "f1_empty": f"SELECT count(*) AS n WHERE {f1} = ''",
            "f2_empty": f"SELECT count(*) AS n WHERE {f2} = ''",
            "both_null": (
                f"SELECT count(*) AS n WHERE {f1} IS NULL AND {f2} IS NULL"
            ),
            "f1_null_only": (
                f"SELECT count(*) AS n WHERE {f1} IS NULL AND {f2} IS NOT NULL"
            ),
            "f2_null_only": (
                f"SELECT count(*) AS n WHERE {f1} IS NOT NULL AND {f2} IS NULL"
            ),
        }
        for key, query in query_map.items():
            counts[key], evidence[key] = probe.count(
                dataset_id,
                query,
                label=f"object_nulls:{dataset_id}:{key}",
            )
        f1_null = counts["both_null"] + counts["f1_null_only"]
        f2_null = counts["both_null"] + counts["f2_null_only"]
        any_null = (
            counts["both_null"]
            + counts["f1_null_only"]
            + counts["f2_null_only"]
        )
        result[dataset_id] = {
            "label": spec["label"],
            "total": counts["total"],
            "fields": {
                f1: {
                    "null": f1_null,
                    "null_pct": _pct(f1_null, counts["total"]),
                    "empty": counts["f1_empty"],
                    "empty_pct": _pct(counts["f1_empty"], counts["total"]),
                },
                f2: {
                    "null": f2_null,
                    "null_pct": _pct(f2_null, counts["total"]),
                    "empty": counts["f2_empty"],
                    "empty_pct": _pct(counts["f2_empty"], counts["total"]),
                },
            },
            "null_combination": {
                "both_null": counts["both_null"],
                "f1_null_only": counts["f1_null_only"],
                "f2_null_only": counts["f2_null_only"],
                "neither_null": counts["total"] - any_null,
                "any_null": any_null,
                "any_null_pct": _pct(any_null, counts["total"]),
            },
            "evidence": evidence,
        }
    return result


def verify_null_safe_form(
    probe: SocrataProbe,
    object_metrics: dict[str, Any],
) -> dict[str, Any]:
    verification: dict[str, Any] = {}
    for dataset_id, spec in DATASETS.items():
        f1, f2 = spec["object_fields"]
        ident = spec["id_field"]
        synthetic_p1 = _like(f1, SYNTHETIC_TERM)
        synthetic_p2 = _like(f2, SYNTHETIC_TERM)
        null_base = f"({f1} IS NULL OR {f2} IS NULL)"
        current = f"NOT ({synthetic_p1} OR {synthetic_p2})"
        null_safe = (
            f"NOT (coalesce({synthetic_p1}, false) OR "
            f"coalesce({synthetic_p2}, false))"
        )
        a_queries = {
            "null_candidates_containing_synthetic_term": (
                f"SELECT count(*) AS n WHERE {null_base} "
                f"AND ({synthetic_p1} OR {synthetic_p2})"
            ),
            "current_survivors": (
                f"SELECT count(*) AS n WHERE {null_base} AND {current}"
            ),
            "null_safe_survivors": (
                f"SELECT count(*) AS n WHERE {null_base} AND {null_safe}"
            ),
        }
        a_counts: dict[str, int] = {}
        a_evidence: dict[str, Any] = {}
        for key, query in a_queries.items():
            a_counts[key], a_evidence[key] = probe.count(
                dataset_id,
                query,
                label=f"null_safe_a:{dataset_id}:{key}",
            )

        present_p1 = _like(f1, PRESENT_TERM)
        present_p2 = _like(f2, PRESENT_TERM)
        # En procesos se prefiere una fila con f1 NULL para probar TRUE OR NULL;
        # en contratos no existen NULL y se usa cualquier coincidencia real.
        candidate_where = (
            f"{f1} IS NULL AND ({present_p1} OR {present_p2})"
            if object_metrics[dataset_id]["fields"][f1]["null"]
            else f"({present_p1} OR {present_p2})"
        )
        candidate = probe.query(
            dataset_id,
            f"SELECT {ident} WHERE {candidate_where} LIMIT 1",
            label=f"null_safe_b:{dataset_id}:candidate",
        )
        if not candidate["rows"]:
            raise RuntimeError(f"no positive control candidate for {dataset_id}")
        candidate_id = candidate["rows"][0][ident]
        base = f"{ident} = '{candidate_id}'"
        present_current = f"NOT ({present_p1} OR {present_p2})"
        present_null_safe = (
            f"NOT (coalesce({present_p1}, false) OR "
            f"coalesce({present_p2}, false))"
        )
        b_queries = {
            "candidate_contains_term": (
                f"SELECT count(*) AS n WHERE {base} AND "
                f"({present_p1} OR {present_p2})"
            ),
            "current_survivors": (
                f"SELECT count(*) AS n WHERE {base} AND {present_current}"
            ),
            "null_safe_survivors": (
                f"SELECT count(*) AS n WHERE {base} AND {present_null_safe}"
            ),
        }
        b_counts: dict[str, int] = {}
        b_evidence: dict[str, Any] = {"candidate_query": candidate}
        for key, query in b_queries.items():
            b_counts[key], b_evidence[key] = probe.count(
                dataset_id,
                query,
                label=f"null_safe_b:{dataset_id}:{key}",
            )

        expected_null_candidates = object_metrics[dataset_id]["null_combination"][
            "any_null"
        ]
        a_pass = (
            a_counts["null_candidates_containing_synthetic_term"] == 0
            and a_counts["null_safe_survivors"] == expected_null_candidates
            and (
                expected_null_candidates == 0
                or a_counts["current_survivors"] < expected_null_candidates
            )
        )
        b_pass = (
            b_counts["candidate_contains_term"] == 1
            and b_counts["current_survivors"] == 0
            and b_counts["null_safe_survivors"] == 0
        )
        verification[dataset_id] = {
            "label": spec["label"],
            "synthetic_absent_term": SYNTHETIC_TERM,
            "present_term": PRESENT_TERM,
            "positive_control_id": candidate_id,
            "current_form": current,
            "null_safe_form": null_safe,
            "a_nonmatching_null_rows_are_preserved": {
                "pass": a_pass,
                "counts": a_counts,
                "evidence": a_evidence,
            },
            "b_matching_rows_remain_excluded": {
                "pass": b_pass,
                "counts": b_counts,
                "evidence": b_evidence,
            },
        }
    return verification


def measure_geography(
    probe: SocrataProbe,
    object_metrics: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    unknown_markers = (
        "defin",
        "desconoc",
        "inform",
        "no aplica",
        "no disponible",
        "sin ciudad",
        "pendiente",
        "por determinar",
        "n/a",
    )
    known_city_terms = sorted({_fold(value) for value in _COMMON_CITIES})
    for dataset_id, spec in DATASETS.items():
        city = spec["city_field"]
        department = spec["department_field"]
        total = object_metrics[dataset_id]["total"]
        null_count, null_ev = probe.count(
            dataset_id,
            f"SELECT count(*) AS n WHERE {city} IS NULL",
            label=f"geography:{dataset_id}:city_null",
        )
        empty_count, empty_ev = probe.count(
            dataset_id,
            f"SELECT count(*) AS n WHERE {city} = ''",
            label=f"geography:{dataset_id}:city_empty",
        )
        no_def_count, no_def_ev = probe.count(
            dataset_id,
            f"SELECT count(*) AS n WHERE {city} = 'No Definido'",
            label=f"geography:{dataset_id}:city_no_definido",
        )
        grouped = probe.query(
            dataset_id,
            (
                f"SELECT {city} AS value, count(*) AS n GROUP BY {city} "
                "ORDER BY n DESC LIMIT 5000"
            ),
            label=f"geography:{dataset_id}:city_groups",
            timeout_s=90.0,
        )
        if grouped["rows"] is None:
            raise RuntimeError(f"city group measurement failed for {dataset_id}")
        values = [
            (row.get("value"), int(row["n"]))
            for row in grouped["rows"]
        ]
        sentinels = [
            {"value": value, "count": count, "pct": _pct(count, total)}
            for value, count in values
            if any(marker in _fold(value) for marker in unknown_markers)
        ]
        collisions = []
        seen: set[tuple[str, str]] = set()
        for term in known_city_terms:
            for raw_value, count in values:
                normalized_value = _fold(raw_value)
                key = (term, normalized_value)
                if (
                    term != normalized_value
                    and term in normalized_value
                    and key not in seen
                ):
                    seen.add(key)
                    collisions.append(
                        {
                            "query_term": term,
                            "matched_source_value": raw_value,
                            "count": count,
                        }
                    )

        by_department: dict[str, Any] = {}
        for term in ("Cali", "San Andres", "Cartagena"):
            query = (
                f"SELECT {city} AS city, {department} AS department, "
                f"count(*) AS n WHERE {_like(city, term)} "
                "GROUP BY city, department ORDER BY n DESC LIMIT 100"
            )
            by_department[term] = probe.query(
                dataset_id,
                query,
                label=f"geography:{dataset_id}:collision:{_fold(term)}",
                timeout_s=90.0,
            )

        department_no_def, department_ev = probe.count(
            dataset_id,
            f"SELECT count(*) AS n WHERE {department} = 'No Definido'",
            label=f"geography:{dataset_id}:department_no_definido",
        )
        result[dataset_id] = {
            "label": spec["label"],
            "total": total,
            "city_field": city,
            "city_unknowns": {
                "null": {"count": null_count, "pct": _pct(null_count, total)},
                "empty": {"count": empty_count, "pct": _pct(empty_count, total)},
                "no_definido": {
                    "exact_value": "No Definido",
                    "count": no_def_count,
                    "pct": _pct(no_def_count, total),
                },
                "other_sentinel_variants": [
                    item for item in sentinels if item["value"] != "No Definido"
                ],
            },
            "city_grouping": {
                "group_count": len(values),
                "sum": sum(count for _, count in values),
                "sentinel_like_values": sentinels,
                "evidence": grouped,
            },
            "substring_collisions_against_current_city_lexicon": collisions,
            "requested_collision_groups_by_department": by_department,
            "department_design_figure_check": {
                "field": department,
                "sentinel": "No Definido",
                "count": department_no_def,
                "pct": _pct(department_no_def, total),
                "design_pct": 6.61 if spec["label"] == "procesos" else 2.01,
                "reproduced_at_two_decimals": round(
                    _pct(department_no_def, total), 2
                )
                == (6.61 if spec["label"] == "procesos" else 2.01),
                "evidence": department_ev,
            },
            "evidence": {
                "city_null": null_ev,
                "city_empty": empty_ev,
                "city_no_definido": no_def_ev,
            },
        }
    return result


def reproduce_city_to_entity_hazard() -> dict[str, Any]:
    router = QueryRouter()
    single_text = "contratos en atlantico menos barranquilla"
    t1_text = "contratos en atlántico"
    t2_text = "menos barranquilla"
    single = router.parse(single_text).params
    t1 = router.parse(t1_text).params
    t2 = router.parse(t2_text).params
    mutation_trace: dict[str, Any] = {}
    intent, merged = detect_and_merge(
        t2_text,
        t2,
        frame_from_params(t1),
        mutation_trace=mutation_trace,
    )
    material_fields = (
        "dataset",
        "dataset_explicit",
        "departamento_resolved",
        "ciudad",
        "entidad",
        "entidad_resolved",
        "entidad_resolution",
        "objeto",
        "objeto_exclude",
    )

    def pick(params: dict[str, Any]) -> dict[str, Any]:
        return {key: params[key] for key in material_fields if key in params}

    mapped = _CITY_TO_ENTITY.get("barranquilla")
    confirmed = bool(
        mapped
        and single.get("ciudad") == "Barranquilla"
        and single.get("entidad_resolved") == mapped
        and intent == "change_scope"
        and merged.get("ciudad") == "Barranquilla"
        and merged.get("entidad_resolved") == mapped
        and "departamento_resolved"
        in mutation_trace.get("transition_cleared_fields", [])
        and "departamento_resolved" not in merged
    )
    return {
        "status": "CONFIRMED" if confirmed else "NOT_REPRODUCED",
        "mapping": {"barranquilla": mapped},
        "single_turn": {"text": single_text, "params": pick(single)},
        "followup": {
            "t1": {"text": t1_text, "params": pick(t1)},
            "t2": {"text": t2_text, "params": pick(t2)},
            "intent": intent,
            "merged_params": pick(merged),
            "mutation_trace": mutation_trace,
        },
        "layer": {
            "activation": "QueryRouter.parse step 10.5 city detection -> _CITY_TO_ENTITY",
            "scope_replacement": (
                "FollowupClassifier change_scope -> followup_mutations "
                "normalize_scope_transition"
            ),
        },
        "contaminated_positive_fields": [
            "ciudad",
            "entidad",
            "entidad_resolved",
            "entidad_resolution",
        ],
        "minimum_expected_fix_surface": [
            (
                "Bind negative spans before QueryRouter city detection; a negated "
                "city cannot populate ciudad or trigger _CITY_TO_ENTITY."
            ),
            (
                "Classify follow-ups from positive residue so a negated city cannot "
                "trigger change_scope or normalize_scope_transition."
            ),
        ],
    }


def benchmark_temporal(probe: SocrataProbe, fetch_rounds: int, count_rounds: int) -> dict[str, Any]:
    dataset_id = "p6dx-8zbt"
    spec = SoQLBuilder.SPECS[dataset_id]
    date_field = spec.date
    base_range = (
        f"{date_field} >= '2024-01-01T00:00:00.000' AND "
        f"{date_field} < '2027-01-01T00:00:00.000'"
    )
    exclusion_hole = (
        f"(({date_field} >= '2024-01-01T00:00:00.000' AND "
        f"{date_field} < '2025-01-01T00:00:00.000') OR "
        f"({date_field} >= '2026-01-01T00:00:00.000' AND "
        f"{date_field} < '2027-01-01T00:00:00.000'))"
    )
    select_fields = ", ".join(spec.select_fields)
    fetch_queries = {
        "A": (
            f"SELECT {select_fields} WHERE {base_range} "
            f"ORDER BY {date_field} DESC LIMIT 25"
        ),
        "B": (
            f"SELECT {select_fields} WHERE {exclusion_hole} "
            f"ORDER BY {date_field} DESC LIMIT 25"
        ),
    }
    count_queries = {
        "A": f"SELECT count(*) AS n WHERE {base_range}",
        "B": f"SELECT count(*) AS n WHERE {exclusion_hole}",
    }

    def rounds(
        shape: str,
        queries: dict[str, str],
        number: int,
        timeout_s: float,
    ) -> list[dict[str, Any]]:
        samples: list[dict[str, Any]] = []
        orders = [("A", "B"), ("B", "A")]
        for round_no in range(1, number + 1):
            order = orders[(round_no - 1) % 2]
            for position, variant in enumerate(order, 1):
                outcome = probe.request_once(
                    dataset_id,
                    queries[variant],
                    timeout_s=timeout_s,
                    label=f"temporal:{shape}:round_{round_no}:{variant}",
                )
                sample = {
                    "round": round_no,
                    "order": "".join(order),
                    "position": position,
                    "variant": variant,
                    **{key: value for key, value in outcome.items() if key != "rows"},
                }
                if outcome["status"] == "ok" and shape == "count":
                    sample["count"] = int(outcome["rows"][0]["n"])
                samples.append(sample)
            time.sleep(0.35)
        return samples

    fetch_samples = rounds("fetch", fetch_queries, fetch_rounds, 20.0)
    count_samples = rounds("count", count_queries, count_rounds, 30.0)
    fetch_summary = {variant: _summary(fetch_samples, variant) for variant in ("A", "B")}
    count_summary = {variant: _summary(count_samples, variant) for variant in ("A", "B")}
    all_success = all(
        summary[variant]["failures"] == 0
        for summary in (fetch_summary, count_summary)
        for variant in ("A", "B")
    )
    b_under_gate = all(
        summary["B"]["p90_s_nearest_rank"] is not None
        and summary["B"]["p90_s_nearest_rank"] < ETAPA6_P90_GATE_S
        for summary in (fetch_summary, count_summary)
    )
    status = "SAFE" if all_success and b_under_gate else "INCONCLUSIVE"
    return {
        "status": status,
        "exploratory_only": True,
        "window": "[2024-01-01, 2027-01-01)",
        "excluded_hole": "[2025-01-01, 2026-01-01)",
        "query_shapes": {
            "fetch": fetch_queries,
            "count": count_queries,
        },
        "fetch": {"samples": fetch_samples, "summary": fetch_summary},
        "count": {"samples": count_samples, "summary": count_summary},
        "etapa6_authority": {
            "document": "docs/PERFIL_LATENCIA_2026-06-28.md",
            "gate_p90_lt_s": ETAPA6_P90_GATE_S,
            "observed_p90_s": ETAPA6_OBSERVED_P90_S,
        },
        "comparability": {
            "fetch": (
                "Misma proyección, ORDER BY y LIMIT de Search, pero medición HTTP "
                "directa: no incluye parser, follow-up, fallback, count lazy ni render."
            ),
            "count": (
                "Agregado fuente-a-fuente sobre toda la ventana; no es latencia "
                "end-to-end de un turno."
            ),
            "gate": (
                "El gate de Etapa 6 es end-to-end p90 < 15 s; 11,6 s es el "
                "resultado observado, no el umbral. La comparación es orientativa."
            ),
        },
    }


def _git(command: list[str]) -> str:
    return subprocess.check_output(command, cwd=REPO, text=True).strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--fetch-rounds", type=int, default=8)
    parser.add_argument("--count-rounds", type=int, default=4)
    args = parser.parse_args(argv)

    started = datetime.now(timezone.utc)
    settings = get_settings()
    probe = SocrataProbe(settings.datos_gov_domain, settings.secop_app_token)
    object_metrics = measure_object_nulls(probe)
    null_safe = verify_null_safe_form(probe, object_metrics)
    geography = measure_geography(probe, object_metrics)
    hazard = reproduce_city_to_entity_hazard()
    temporal = benchmark_temporal(probe, args.fetch_rounds, args.count_rounds)

    null_safe_verified = all(
        dataset["a_nonmatching_null_rows_are_preserved"]["pass"]
        and dataset["b_matching_rows_remain_excluded"]["pass"]
        for dataset in null_safe.values()
    )
    current_null_safe = all(
        dataset["a_nonmatching_null_rows_are_preserved"]["counts"][
            "current_survivors"
        ]
        == object_metrics[dataset_id]["null_combination"]["any_null"]
        for dataset_id, dataset in null_safe.items()
    )
    city_safe = not any(
        dataset["substring_collisions_against_current_city_lexicon"]
        for dataset in geography.values()
    )
    transport_failures = [
        operation
        for operation in probe.operations
        if operation["status"] != "ok"
    ]
    overall = "PASS" if temporal["status"] == "SAFE" else "PARTIAL"
    artifact = {
        "schema": "secoppal.hca_r4_1_live_precheck.v1",
        "started_at_utc": started.isoformat(),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "domain": settings.datos_gov_domain,
            "authenticated_with_app_token": bool(settings.secop_app_token),
            "datasets": list(DATASETS),
            "transport_failures_observed": len(transport_failures),
        },
        "authority": {
            "runtime_base": "9ecf3be0e1b5acdda31fc8fe6ead6073df3f137f",
            "head_at_measurement": _git(["git", "rev-parse", "HEAD"]),
            "runtime_app_diff_from_base": bool(
                _git(["git", "diff", "--name-only", "9ecf3be", "--", "app"])
            ),
        },
        "object_null_rate": object_metrics,
        "object_null_semantics": null_safe,
        "geography_sentinels": geography,
        "city_substring_collisions": {
            dataset_id: dataset[
                "substring_collisions_against_current_city_lexicon"
            ]
            for dataset_id, dataset in geography.items()
        },
        "city_to_entity_negative_hazard": hazard,
        "temporal_negation_transport": temporal,
        "verdict": {
            "HCA_R4_1_LIVE_PRECHECK": overall,
            "CURRENT_OBJECT_EXCLUDE_NULL_SAFE": "YES"
            if current_null_safe
            else "NO",
            "NULL_SAFE_FORM_VERIFIED": "YES" if null_safe_verified else "NO",
            "CITY_NEGATION_SAFE_WITH_CURRENT_MATCHING": "YES"
            if city_safe
            else "NO",
            "CITY_TO_ENTITY_NEGATIVE_HAZARD": hazard["status"],
            "TEMPORAL_NEGATION_TRANSPORT": temporal["status"],
            "ADR_064_FACTUAL_CHANGES_REQUIRED": "YES",
            "FINAL": "READY_FOR_HCA_R4_NEGATION_IMPLEMENTATION"
            if (
                overall == "PASS"
                and null_safe_verified
                and not current_null_safe
                and not city_safe
                and hazard["status"] == "CONFIRMED"
            )
            else "NEGATION_DESIGN_NEEDS_REVISION",
        },
        "transport_operations": probe.operations,
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
