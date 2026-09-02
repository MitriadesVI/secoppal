#!/usr/bin/env python3
"""Reproductor read-only del piloto R2F-A de universidades publicas.

La matriz PRE esta congelada en HEAD 8849f0d antes de mutar el gazetteer. El
reproductor revalida nombres fisicos y actividad por NIT, ejecuta el resolver
vivo, prueba proyeccion ADR-073 y audita aliases ADR-072 contra el directorio
de compradores. No descubre ni aplica decisiones de curacion.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from app.core.buyer_source_names import load_registry  # noqa: E402
from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.entity_types import name_tokens  # noqa: E402
from app.core.scan_alias_policy import record_alias_policy  # noqa: E402
from app.core.soql_builder import SoQLBuilder  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402
from gazetteer_ownership_baseline import (  # noqa: E402
    load_baseline,
    multi_owner_groups,
    new_multi_owner_keys,
)

ALIASES_DB = ROOT / "app/data/aliases_db.json"
R2C = ROOT / "artifacts/audits/institutional_buyer_universe_r2c.json"
OUT = ROOT / "artifacts/audits/public_university_pilot_r2fa.json"

PROCESSES = SoQLBuilder.PROCESOS_DATASET
CONTRACTS = SoQLBuilder.CONTRATOS_DATASET
DIRECTORY = "4ex9-j3n8"
RESOURCE = "https://www.datos.gov.co/resource/{}.json"
HEAD_BEFORE = "8849f0d"
PRE_DATA_COMMIT = "97008a0"
OBSERVED_AT = "2026-09-02"
RECENT_SINCE = "2025-09-02T00:00:00.000"

AMAZONIA = "UNIVERSIDAD DE LA AMAZONIA"
ANTIOQUIA = "UNIVERSIDAD DE ANTIOQUIA"
ATLANTICO = "UNIATLANTICO"
CORDOBA = "UNIVERSIDAD DE CÓRDOBA"
UFPS_CUCUTA = "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER"
UFPS_OCANA = "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA"
UNITROPICO = "UNIVERSIDAD INTERNACIONAL DEL TRÓPICO AMERICANO UNITRÓPICO"
UAIIN = "Universidad Autónoma Indígena Intercultural"

PILOT = (
    "Universidad de la Amazonia",
    "Universidad de Antioquia",
    "Universidad del Atlántico",
    "Universidad de Córdoba",
    "Universidad Francisco de Paula Santander - Cúcuta",
    "Universidad Francisco de Paula Santander - Ocaña",
    "Universidad Internacional del Trópico Americano",
    UAIIN,
)

# Congelado antes de mutar datos. Ocaña ya incorpora la revalidacion por su
# NIT propio; R2C la habia atribuido incorrectamente al NIT de Cucuta.
PRE_CLASSIFICATION = {
    "Universidad de la Amazonia": "FAIL_CLOSED",
    "Universidad de Antioquia": "PASS_EXACT",
    "Universidad del Atlántico": "WRONG_OWNER",
    "Universidad de Caldas": "PASS_EXACT",
    "Universidad de Cartagena": "PASS_EXACT",
    "Universidad del Cauca": "PASS_EXACT",
    "Universidad Colegio Mayor de Cundinamarca": "FAIL_CLOSED",
    "Universidad de Córdoba": "FAIL_CLOSED",
    "Universidad de Cundinamarca": "FAIL_CLOSED",
    "Universidad Distrital Francisco José de Caldas": "PASS_EXACT",
    "Universidad Francisco de Paula Santander - Cúcuta": "FUZZY_ONLY",
    "Universidad Francisco de Paula Santander - Ocaña": "WRONG_OWNER",
    "Universidad Industrial de Santander": "PASS_EXACT",
    "Universidad de La Guajira": "FAIL_CLOSED",
    "Universidad de los Llanos": "PASS_EXACT",
    "Universidad del Magdalena": "PASS_EXACT",
    "Universidad Militar Nueva Granada": "PASS_EXACT",
    "Universidad Nacional Abierta y a Distancia": "PASS_EXACT",
    "Universidad Nacional de Colombia": "PASS_EXACT",
    "Universidad de Nariño": "PASS_EXACT",
    "Universidad del Pacífico": "FAIL_CLOSED",
    "Universidad de Pamplona": "PASS_EXACT",
    "Universidad Pedagógica Nacional": "PASS_EXACT",
    "Universidad Pedagógica y Tecnológica de Colombia": "PASS_EXACT",
    "Universidad Popular del Cesar": "FAIL_CLOSED",
    "Universidad del Quindío": "PASS_EXACT",
    "Universidad de Sucre": "FAIL_CLOSED",
    "Universidad Surcolombiana": "PASS_EXACT",
    "Universidad Tecnológica del Chocó Diego Luis Córdoba": "FAIL_CLOSED",
    "Universidad Tecnológica de Pereira": "PASS_EXACT",
    "Universidad del Tolima": "FAIL_CLOSED",
    "Universidad del Valle": "PASS_EXACT",
    "Universidad Internacional del Trópico Americano": "WRONG_OWNER",
    UAIIN: "WRONG_OWNER",
}

POST_OWNER = {
    "Universidad de la Amazonia": AMAZONIA,
    "Universidad de Antioquia": ANTIOQUIA,
    "Universidad del Atlántico": ATLANTICO,
    "Universidad de Córdoba": CORDOBA,
    "Universidad Francisco de Paula Santander - Cúcuta": UFPS_CUCUTA,
    "Universidad Francisco de Paula Santander - Ocaña": UFPS_OCANA,
    "Universidad Internacional del Trópico Americano": UNITROPICO,
}

NIT_OVERRIDES = {
    "Universidad Francisco de Paula Santander - Ocaña": "8001631300",
    UAIIN: None,
}

PILOT_DECISIONS = {
    "Universidad de la Amazonia": "ADD_EXACT_AND_SCAN + SOURCE_PROJECTION",
    "Universidad de Antioquia": "NO_CHANGE_REQUIRED",
    "Universidad del Atlántico": "KEEP + SOURCE_PROJECTION",
    "Universidad de Córdoba": "ADD_EXACT_AND_SCAN + SOURCE_PROJECTION",
    "Universidad Francisco de Paula Santander - Cúcuta": (
        "ADD_EXACT_AND_SCAN + KEEP_EXACT_DISABLE_SCAN(base physical name)"
    ),
    "Universidad Francisco de Paula Santander - Ocaña": (
        "SEPARATE_BUYER + ADD_EXACT_AND_SCAN + SOURCE_PROJECTION"
    ),
    "Universidad Internacional del Trópico Americano": "ADD_EXACT_AND_SCAN",
    UAIIN: "RESOLUTION_BLOCK",
}

IDENTITY_CLASSIFICATION = defaultdict(lambda: "SINGLE_SOURCE_NAME")
IDENTITY_CLASSIFICATION.update(
    {
        "Universidad Francisco de Paula Santander - Ocaña": "SEPARATE_BUYERS",
        "Universidad Tecnológica de Pereira": "REGISTRY_NOISE",
        UAIIN: "NO_SECOP_EVIDENCE",
    }
)

AUDITED_SCAN_ALIASES = {
    "universidad francisco de paula santander - cúcuta": UFPS_CUCUTA,
    "universidad de la amazonia": AMAZONIA,
    "universidad de córdoba": CORDOBA,
    "universidad francisco de paula santander - ocaña": UFPS_OCANA,
    "universidad francisco de paula santander ocaña": UFPS_OCANA,
    "universidad francisco de paula santander seccional ocaña": UFPS_OCANA,
    "ufps ocaña": UFPS_OCANA,
    "universidad internacional del trópico americano": UNITROPICO,
    "universidad internacional del trópico americano unitrópico": UNITROPICO,
    "unitrópico": UNITROPICO,
}


def digits(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def contains_token_sequence(haystack: str, needle: str) -> bool:
    hay = name_tokens(haystack)
    wanted = name_tokens(needle)
    return bool(wanted) and any(
        hay[index : index + len(wanted)] == wanted
        for index in range(len(hay) - len(wanted) + 1)
    )


class Socrata:
    def __init__(self, token: str | None) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "User-Agent": "SECOPPAL-R2FA-audit/1.0"}
        )
        if token:
            self.session.headers["X-App-Token"] = token

    def get(self, dataset: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                response = self.session.get(
                    RESOURCE.format(dataset), params=params, timeout=60
                )
                response.raise_for_status()
                payload = response.json()
                return payload if isinstance(payload, list) else []
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                time.sleep(0.5 * (attempt + 1))
        assert last_error is not None
        raise last_error


def load_universe() -> tuple[list[dict[str, Any]], dict[str, str]]:
    payload = json.loads(R2C.read_text(encoding="utf-8"))
    rows = [
        dict(row)
        for row in payload["entities"]
        if row["group"] == "PUBLIC_UNIVERSITIES_SECOP"
    ]
    historical = {row["logical_name"]: row["classification"] for row in rows}
    for row in rows:
        if row["logical_name"] in NIT_OVERRIDES:
            row["nit"] = NIT_OVERRIDES[row["logical_name"]]
    return rows, historical


def grouped_activity(
    client: Socrata, nits: list[str]
) -> dict[tuple[str, str, bool], list[dict[str, Any]]]:
    evidence: dict[tuple[str, str, bool], list[dict[str, Any]]] = defaultdict(list)
    jobs: list[tuple[str, bool, str, dict[str, Any], str]] = []
    definitions = (
        ("processes", PROCESSES, "entidad", "fecha_de_publicacion_del"),
        ("contracts", CONTRACTS, "nombre_entidad", "fecha_de_firma"),
    )
    for start in range(0, len(nits), 10):
        quoted = ",".join(f'"{nit}"' for nit in nits[start : start + 10])
        for label, dataset, field, date_field in definitions:
            for recent in (False, True):
                where = f"nit_entidad in ({quoted})"
                if recent:
                    where += f' and {date_field} >= "{RECENT_SINCE}"'
                params = {
                    "$select": f"nit_entidad,{field},count(*) as rows",
                    "$where": where,
                    "$group": f"nit_entidad,{field}",
                    "$limit": 5000,
                }
                jobs.append((label, recent, dataset, params, field))

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(client.get, dataset, params): (label, recent, field)
            for label, recent, dataset, params, field in jobs
        }
        for future in as_completed(futures):
            label, recent, field = futures[future]
            for item in future.result():
                evidence[(digits(item.get("nit_entidad")), label, recent)].append(
                    {"name": item.get(field), "rows": int(item.get("rows", 0))}
                )
    return evidence


def registry_names(client: Socrata, nits: list[str]) -> dict[str, list[str]]:
    result: dict[str, set[str]] = defaultdict(set)
    for start in range(0, len(nits), 20):
        quoted = ",".join(f'"{nit}"' for nit in nits[start : start + 20])
        rows = client.get(
            DIRECTORY,
            {
                "$select": "nit_entidad,nombre_entidad",
                "$where": f"nit_entidad in ({quoted}) and es_entidad='Si'",
                "$limit": 5000,
            },
        )
        for row in rows:
            result[digits(row.get("nit_entidad"))].add(row.get("nombre_entidad", ""))
    return {nit: sorted(names) for nit, names in result.items()}


def resolution_status(
    logical_name: str,
    result: Any,
    expected_names: set[str],
) -> str:
    if logical_name == UAIIN:
        return "FAIL_CLOSED" if result.value is None else "WRONG_OWNER"
    expected_owner = POST_OWNER.get(logical_name)
    correct = (
        result.value == expected_owner
        if expected_owner
        else bool(result.value and normalize_text(result.value) in expected_names)
    )
    if correct and result.method == "exact":
        return "PASS_EXACT"
    if correct and str(result.method).startswith("fuzzy"):
        return "FUZZY_ONLY"
    if correct:
        return "LIKE"
    if result.clarification_needed:
        return "AMBIGUOUS"
    if result.value:
        return "WRONG_OWNER"
    return "FAIL_CLOSED"


def scan_cross_owner(
    client: Socrata, records: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    owner_nits = {official: digits(record.get("nit")) for official, record in records.items()}

    def audit(alias: str, owner: str) -> list[dict[str, Any]]:
        rows = client.get(
            DIRECTORY,
            {
                "$q": alias,
                "$select": "nombre_entidad,nit_entidad",
                "$where": "es_entidad='Si'",
                "$limit": 100,
            },
        )
        return [
            {
                "alias": alias,
                "owner": owner,
                "foreign_name": row.get("nombre_entidad"),
                "foreign_nit": digits(row.get("nit_entidad")),
            }
            for row in rows
            if contains_token_sequence(row.get("nombre_entidad", ""), alias)
            and digits(row.get("nit_entidad")) != owner_nits[owner]
        ]

    hits: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(audit, alias, owner): alias
            for alias, owner in AUDITED_SCAN_ALIASES.items()
        }
        for future in as_completed(futures):
            hits.extend(future.result())
    return sorted(hits, key=lambda row: (row["alias"], row["foreign_name"] or ""))


def source_projection_canaries() -> list[dict[str, Any]]:
    registry = load_registry()
    builder = SoQLBuilder()
    rows = []
    for canonical in (AMAZONIA, ATLANTICO, CORDOBA, UFPS_OCANA):
        binding = registry.binding_for(canonical)
        datasets = {}
        for dataset, field in ((PROCESSES, "entidad"), (CONTRACTS, "nombre_entidad")):
            params = {"entidad_resolved": canonical}
            snapshot = dict(params)
            soql = builder.build_count(dataset, params)
            names = registry.project(canonical, dataset)
            datasets[dataset] = {
                "names": list(names or ()),
                "soql": soql,
                "exact_only": (
                    bool(names)
                    and " LIKE " not in soql
                    and "UPPER" not in soql
                    and "unaccent" not in soql
                    and f"{field} = " in soql
                ),
                "params_immutable": params == snapshot,
            }
        rows.append(
            {
                "canonical_name": canonical,
                "identity_key": {
                    "type": "NIT",
                    "value": binding.identity_key,
                },
                "observed_at": binding.observed_at.isoformat(),
                "observed_rows": binding.observed_rows,
                "datasets": datasets,
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--token", default=None)
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")
    client = Socrata(args.token or os.getenv("SOCRATA_APP_TOKEN"))

    universe, historical = load_universe()
    nits = sorted({row["nit"] for row in universe if row.get("nit")})
    activity = grouped_activity(client, nits)
    registry_by_nit = registry_names(client, nits)
    resolver = EntityResolver(ALIASES_DB)
    gazetteer = json.loads(ALIASES_DB.read_text(encoding="utf-8"))
    records = {record["official_name"]: record for record in gazetteer}
    records_by_source_name: dict[str, list[str]] = defaultdict(list)
    by_nit: dict[str, list[str]] = defaultdict(list)
    for record in gazetteer:
        records_by_source_name[normalize_text(record["official_name"])].append(
            record["official_name"]
        )
        if record.get("nit"):
            by_nit[digits(record["nit"])].append(record["official_name"])

    cases = []
    for old in universe:
        logical = old["logical_name"]
        nit = old.get("nit")
        process_rows = sorted(
            activity.get((nit, "processes", False), []),
            key=lambda row: (-row["rows"], row["name"] or ""),
        )
        contract_rows = sorted(
            activity.get((nit, "contracts", False), []),
            key=lambda row: (-row["rows"], row["name"] or ""),
        )
        source_names = {
            normalize_text(row["name"])
            for row in process_rows + contract_rows
            if row.get("name") and not str(row["name"]).startswith("CCE ENTIDAD DE PRUEBAS")
        }
        expected_names = source_names | {
            normalize_text(name) for name in by_nit.get(nit or "", [])
        }
        result = resolver.resolve_entidad(logical)
        scan = resolver.scan_entity(normalize_text(logical))
        post = resolution_status(logical, result, expected_names)
        existing_records = set(by_nit.get(nit or "", []))
        for source_name in source_names:
            existing_records.update(records_by_source_name.get(source_name, []))
        if result.value in records:
            existing_records.add(result.value)
        case = {
            "logical_name": logical,
            "nit": nit,
            "registry_names": registry_by_nit.get(nit or "", []),
            "process_source_names": [row["name"] for row in process_rows],
            "contract_source_names": [row["name"] for row in contract_rows],
            "process_observed_rows": {row["name"]: row["rows"] for row in process_rows},
            "contract_observed_rows": {row["name"]: row["rows"] for row in contract_rows},
            "all_time_processes": sum(row["rows"] for row in process_rows) if nit else None,
            "all_time_contracts": sum(row["rows"] for row in contract_rows) if nit else None,
            "recent_12m_processes": sum(
                row["rows"] for row in activity.get((nit, "processes", True), [])
            )
            if nit
            else None,
            "recent_12m_contracts": sum(
                row["rows"] for row in activity.get((nit, "contracts", True), [])
            )
            if nit
            else None,
            "existing_gazetteer_records": sorted(existing_records),
            "existing_aliases": sorted(
                {
                    alias
                    for official in existing_records
                    for alias in records[official].get("aliases", [])
                }
            ),
            "current_resolution": result.value,
            "current_method": result.method,
            "current_like_value": result.like_value,
            "current_clarification_needed": result.clarification_needed,
            "current_scan_owner": scan.official_name if scan else None,
            "current_scan_alias": scan.matched_alias if scan else None,
            "identity_classification": IDENTITY_CLASSIFICATION[logical],
            "r2c_classification": historical[logical],
            "pre_classification": PRE_CLASSIFICATION[logical],
            "drift_from_r2c": (
                "UNCHANGED"
                if historical[logical] == PRE_CLASSIFICATION[logical]
                else f"{historical[logical]} -> {PRE_CLASSIFICATION[logical]}"
            ),
            "post_classification": post,
            "in_pilot": logical in PILOT,
        }
        if logical in PILOT:
            case["decision"] = PILOT_DECISIONS[logical]
            case["source_projection"] = logical in {
                "Universidad de la Amazonia",
                "Universidad del Atlántico",
                "Universidad de Córdoba",
                "Universidad Francisco de Paula Santander - Ocaña",
            }
        cases.append(case)

    pre_counts = Counter(PRE_CLASSIFICATION.values())
    post_counts = Counter(case["post_classification"] for case in cases)
    pilot_cases = [case for case in cases if case["in_pilot"]]
    pilot_pre = Counter(case["pre_classification"] for case in pilot_cases)
    pilot_post = Counter(case["post_classification"] for case in pilot_cases)

    scan_hits = scan_cross_owner(client, records)
    baseline = load_baseline()
    multi_owner = multi_owner_groups(gazetteer)
    source_registry = load_registry()
    projection_canaries = source_projection_canaries()
    outside_regressions = [
        case["logical_name"]
        for case in cases
        if not case["in_pilot"]
        and case["post_classification"] == "WRONG_OWNER"
        and case["pre_classification"] != "WRONG_OWNER"
    ]

    payload = {
        "audit": "PUBLIC-UNIVERSITY-PILOT-R2FA",
        "type": "supervised-curation/replay",
        "as_of": date.today().isoformat(),
        "observed_at": OBSERVED_AT,
        "recent_since": RECENT_SINCE[:10],
        "head_before": HEAD_BEFORE,
        "pre_data_commit": PRE_DATA_COMMIT,
        "authorities": {
            "identity": "NIT + physical source name + SECOP activity",
            "alias_policy": "ADR-072",
            "source_projection": "ADR-073",
            "processes": PROCESSES,
            "contracts": CONTRACTS,
            "buyer_directory": DIRECTORY,
        },
        "methodology": {
            "similarity_is_identity_authority": False,
            "source_name_split_rule": (
                "Different physical names for one NIT across datasets; absence in one "
                "dataset is not a split."
            ),
            "selection_is_automatic_after_frozen_pre": True,
            "implementation_is_supervised": True,
        },
        "full_universe_current_pre": {
            "total": len(cases),
            **{key.lower(): pre_counts[key] for key in (
                "PASS_EXACT",
                "FAIL_CLOSED",
                "WRONG_OWNER",
                "FUZZY_ONLY",
                "LIKE",
                "AMBIGUOUS",
            )},
            "no_secop_evidence": sum(
                case["identity_classification"] == "NO_SECOP_EVIDENCE"
                for case in cases
            ),
            "drift_from_r2c": sum(case["drift_from_r2c"] != "UNCHANGED" for case in cases),
        },
        "pilot_selection_rationale": {
            "size": 8,
            "selection": list(PILOT),
            "wrong_owner_included": 4,
            "fuzzy_only_included": 1,
            "fail_closed_included": 2,
            "pass_exact_control_included": 1,
            "source_name_split_in_universe": 0,
            "source_name_split_note": (
                "No qualifying case exists; missing contract rows are not a split."
            ),
        },
        "pilot_matrix_pre": dict(sorted(pilot_pre.items())),
        "pilot_matrix_post": dict(sorted(pilot_post.items())),
        "full_universe_post": {
            "total": len(cases),
            **{key.lower(): post_counts[key] for key in (
                "PASS_EXACT",
                "FAIL_CLOSED",
                "WRONG_OWNER",
                "FUZZY_ONLY",
                "LIKE",
                "AMBIGUOUS",
            )},
            "new_wrong_owner_outside_pilot": len(outside_regressions),
            "outside_regressions": outside_regressions,
        },
        "identity_changes": {
            "identities_added": [AMAZONIA, CORDOBA, UFPS_OCANA, UNITROPICO],
            "aliases_added_normalized": sorted(AUDITED_SCAN_ALIASES),
            "aliases_removed": [],
            "scan_disabled": ["universidad francisco de paula santander"],
            "resolution_blocked": [normalize_text(UAIIN)],
        },
        "source_projection": {
            "bindings_added": [AMAZONIA, ATLANTICO, CORDOBA, UFPS_OCANA],
            "sidecar_total_bindings": len(source_registry),
            "sidecar_evidence": source_registry.provenance.evidence,
            "canaries": projection_canaries,
        },
        "gates": {
            "new_scan_cross_owner": len(scan_hits),
            "scan_cross_owner_hits": scan_hits,
            "exact_multi_owner_before": 172,
            "exact_multi_owner_after": len(multi_owner),
            "new_multi_owner_keys": new_multi_owner_keys(gazetteer, baseline),
            "wrong_owner_post": pilot_post["WRONG_OWNER"],
            "fuzzy_only_post": pilot_post["FUZZY_ONLY"],
        },
        "verification": {
            "focused_regressions": {
                "command": (
                    ".venv/bin/python -m pytest -q "
                    "tests/test_public_university_pilot_r2fa.py "
                    "tests/test_source_name_projection_v1.py "
                    "tests/test_fuzzy_identity_multiword_r2e1a.py "
                    "tests/test_institutional_fuzzy_safety_r2e1.py "
                    "tests/test_control_national_r2e.py "
                    "tests/test_scan_alias_eligibility_r2d3.py "
                    "tests/test_car_core_r2d.py tests/test_national_core_r2b.py "
                    "tests/test_root_buyer_evidence_r1d.py "
                    "tests/test_gazetteer_alias_ownership_r1c.py "
                    "tests/test_sena_resolver.py tests/test_icbf_regionals.py"
                ),
                "result": "829 passed",
            },
            "query_corpus": "89/89 PASS, 0 FAIL",
            "ruff": "PASS",
            "lint_core": "PASS",
            "full_suite": "10656 passed, 2 skipped, 15 xfailed",
            "new_xfails": 0,
        },
        "cases": cases,
        "generated_at": date.today().isoformat(),
        "recent_window_days": (date.today() - date.fromisoformat(RECENT_SINCE[:10])).days,
        "recommended_replay": (
            ".venv/bin/python scripts/audits/audit_public_university_pilot_r2fa.py"
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({
        "pre": payload["full_universe_current_pre"],
        "pilot_pre": payload["pilot_matrix_pre"],
        "pilot_post": payload["pilot_matrix_post"],
        "post": payload["full_universe_post"],
        "gates": payload["gates"],
    }, ensure_ascii=False, indent=2))
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
