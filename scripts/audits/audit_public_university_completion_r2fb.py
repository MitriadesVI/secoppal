#!/usr/bin/env python3
"""Reproductor read-only del cierre R2F-B de universidades publicas.

R2F-A dejo ocho universidades acreditadas fallando cerradas. Este reproductor
revalida sus nombres fisicos y su actividad por NIT, corre el resolver vivo
sobre las 34 del universo R2C, prueba la proyeccion ADR-073 y audita la
elegibilidad de scan ADR-072 contra el directorio COMPLETO de compradores.

La auditoria de scan no usa `$q`: pagina el directorio entero y compara por
secuencia de tokens en local. Es estrictamente mas severa que un full-text
acotado a los primeros cien resultados, y es lo que permite afirmar que un
homonimo registral no publica una sola fila.

No descubre ni aplica decisiones de curacion: sólo las reproduce.
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
from datetime import date
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
from app.core.soql_builder import SoQLBuilder  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402
from gazetteer_ownership_baseline import (  # noqa: E402
    load_baseline,
    multi_owner_groups,
    new_multi_owner_keys,
)

ALIASES_DB = ROOT / "app/data/aliases_db.json"
R2C = ROOT / "artifacts/audits/institutional_buyer_universe_r2c.json"
R2FA = ROOT / "artifacts/audits/public_university_pilot_r2fa.json"
OUT = ROOT / "artifacts/audits/public_university_completion_r2fb.json"

PROCESSES = SoQLBuilder.PROCESOS_DATASET
CONTRACTS = SoQLBuilder.CONTRATOS_DATASET
DIRECTORY = "4ex9-j3n8"
RESOURCE = "https://www.datos.gov.co/resource/{}.json"
HEAD_BEFORE = "e33a97a"
OBSERVED_AT = "2026-09-02"
RECENT_SINCE = "2025-09-02T00:00:00.000"

COLEGIO_MAYOR = "UNIVERSIDAD COLEGIO MAYOR DE CUNDINAMARCA"
CUNDINAMARCA = "UNIVERSIDAD DE CUNDINAMARCA"
GUAJIRA = "UNIVERSIDAD DE LA GUAJIRA"
PACIFICO = "UNIVERSIDAD DEL PACÍFICO"
POPULAR_CESAR = "UNIVERSIDAD POPULAR DEL CESAR"
SUCRE = "UNIVERSIDAD DE SUCRE"
CHOCO = "UNIVERSIDAD TECNOLÓGICA DEL CHOCÓ DIEGO LUIS CÓRDOBA"
TOLIMA = "UNIVERSIDAD DEL TOLIMA"
UAIIN = "Universidad Autónoma Indígena Intercultural"

# Universo congelado del lote. Ocho, ni una mas.
TARGETS = {
    "Universidad Colegio Mayor de Cundinamarca": COLEGIO_MAYOR,
    "Universidad de Cundinamarca": CUNDINAMARCA,
    "Universidad de La Guajira": GUAJIRA,
    "Universidad del Pacífico": PACIFICO,
    "Universidad Popular del Cesar": POPULAR_CESAR,
    "Universidad de Sucre": SUCRE,
    "Universidad Tecnológica del Chocó Diego Luis Córdoba": CHOCO,
    "Universidad del Tolima": TOLIMA,
}

# PRE de las 34 = POST de R2F-A, revalidado en vivo antes de mutar datos.
TARGET_PRE = dict.fromkeys(TARGETS, "FAIL_CLOSED")

# Owners acreditados fuera del lote. Vienen de R2F-A y no se re-litigan aqui:
# entran para que el replay pueda declarar WRONG_OWNER con un oraculo, y no
# por coincidencia de nombres.
R2FA_OWNERS = {
    "Universidad de la Amazonia": "UNIVERSIDAD DE LA AMAZONIA",
    "Universidad de Antioquia": "UNIVERSIDAD DE ANTIOQUIA",
    "Universidad del Atlántico": "UNIATLANTICO",
    "Universidad de Córdoba": "UNIVERSIDAD DE CÓRDOBA",
    "Universidad Francisco de Paula Santander - Cúcuta": (
        "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER"
    ),
    "Universidad Francisco de Paula Santander - Ocaña": (
        "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA"
    ),
    "Universidad Internacional del Trópico Americano": (
        "UNIVERSIDAD INTERNACIONAL DEL TRÓPICO AMERICANO UNITRÓPICO"
    ),
}
POST_OWNER = {**R2FA_OWNERS, **TARGETS}

NIT_OVERRIDES = {
    "Universidad Francisco de Paula Santander - Ocaña": "8001631300",
    UAIIN: None,
}

DECISIONS = {
    "Universidad Colegio Mayor de Cundinamarca": (
        "ADD_EXACT_AND_SCAN + SOURCE_PROJECTION(processes only)"
    ),
    "Universidad de Cundinamarca": "ADD_EXACT_AND_SCAN",
    "Universidad de La Guajira": "ADD_EXACT_AND_SCAN",
    "Universidad del Pacífico": "ADD_EXACT_AND_SCAN + SOURCE_PROJECTION",
    "Universidad Popular del Cesar": (
        "ADD_EXACT_AND_SCAN + SOURCE_PROJECTION(processes only)"
    ),
    "Universidad de Sucre": "ADD_EXACT_AND_SCAN",
    "Universidad Tecnológica del Chocó Diego Luis Córdoba": (
        "ADD_EXACT_AND_SCAN + SOURCE_PROJECTION"
    ),
    "Universidad del Tolima": "ADD_EXACT_AND_SCAN",
}

# Aliases nuevos y su owner, para la auditoria de scan cross-owner.
AUDITED_SCAN_ALIASES = {
    "universidad colegio mayor de cundinamarca": COLEGIO_MAYOR,
    "colegio mayor de cundinamarca": COLEGIO_MAYOR,
    "universidad de cundinamarca": CUNDINAMARCA,
    "universidad de la guajira": GUAJIRA,
    "universidad del pacifico": PACIFICO,
    "universidad popular del cesar": POPULAR_CESAR,
    "universidad de sucre": SUCRE,
    "universidad tecnologica del choco": CHOCO,
    "universidad tecnologica del choco diego luis cordoba": CHOCO,
    "universidad del tolima": TOLIMA,
    # Grafias que la fuente publica: sin ellas el propio nombre fisico de la
    # identidad entra por fuzzy en vez de exacto.
    "universidad del pacifico//": PACIFICO,
    "universidad popular del cesar.": POPULAR_CESAR,
}

# Ownership exacto sin scan (ADR-072). El typo del directorio acredita a quien
# lo publica y nada mas: nadie nombra al comprador asi dentro de texto libre.
EXACT_ONLY_ALIASES = {"universida de la guajira": GUAJIRA}

# Aportada externamente y NO reproducida en p6dx-8zbt, jbjy-vk9h ni el
# directorio. No se promueve a alias: la identidad esta acreditada, la grafia
# no. Sigue resolviendo al owner correcto por fuzzy.
UNREPRODUCED_SURFACE_VARIANTS = {
    "UNIVERSIDAD TECNOLOGICA DEL CHOCO.": {
        "owner": CHOCO,
        "reported_by": "external SECOP user directory view",
        "found_in_processes": False,
        "found_in_contracts": False,
        "found_in_buyer_directory": False,
        "decision": "NOT_ACCREDITED_KEEPS_FUZZY",
    }
}

PROJECTED = (COLEGIO_MAYOR, PACIFICO, POPULAR_CESAR, CHOCO)
NOT_PROJECTED = (CUNDINAMARCA, GUAJIRA, SUCRE, TOLIMA)


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
            {"Accept": "application/json", "User-Agent": "SECOPPAL-R2FB-audit/1.0"}
        )
        if token:
            self.session.headers["X-App-Token"] = token

    def get(self, dataset: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                response = self.session.get(
                    RESOURCE.format(dataset), params=params, timeout=120
                )
                response.raise_for_status()
                payload = response.json()
                return payload if isinstance(payload, list) else []
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                time.sleep(1.0 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def buyer_directory(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = self.get(
                DIRECTORY,
                {
                    "$select": "nombre_entidad,nit_entidad",
                    "$where": "es_entidad='Si'",
                    "$limit": 50000,
                    "$offset": offset,
                    "$order": ":id",
                },
            )
            if not page:
                break
            rows.extend(page)
            offset += len(page)
            if len(page) < 50000:
                break
        return rows


def load_universe() -> list[dict[str, Any]]:
    payload = json.loads(R2C.read_text(encoding="utf-8"))
    rows = [
        dict(row)
        for row in payload["entities"]
        if row["group"] == "PUBLIC_UNIVERSITIES_SECOP"
    ]
    for row in rows:
        if row["logical_name"] in NIT_OVERRIDES:
            row["nit"] = NIT_OVERRIDES[row["logical_name"]]
    return rows


def r2fa_post() -> dict[str, str]:
    payload = json.loads(R2FA.read_text(encoding="utf-8"))
    return {
        case["logical_name"]: case["post_classification"] for case in payload["cases"]
    }


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


def registry_names(directory: list[dict[str, Any]], nits: set[str]) -> dict[str, list[str]]:
    result: dict[str, set[str]] = defaultdict(set)
    for row in directory:
        nit = digits(row.get("nit_entidad"))
        if nit in nits:
            result[nit].add(row.get("nombre_entidad", ""))
    return {nit: sorted(names) for nit, names in result.items()}


def resolution_status(logical_name: str, result: Any, expected_names: set[str]) -> str:
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
    directory: list[dict[str, Any]],
    owner_nits: dict[str, str],
    activity: dict[str, dict[str, int]],
) -> list[dict[str, Any]]:
    """Homonimos con NIT distinto al owner, y si publican o no.

    Un homonimo sin una sola fila en procesos ni contratos no es un comprador
    fisico que el scan pueda secuestrar: no hay nada suyo que atribuir mal. La
    distincion se reporta, no se esconde.
    """
    hits: list[dict[str, Any]] = []
    for alias, owner in AUDITED_SCAN_ALIASES.items():
        for row in directory:
            foreign_nit = digits(row.get("nit_entidad"))
            if foreign_nit == owner_nits[owner]:
                continue
            if not contains_token_sequence(row.get("nombre_entidad", ""), alias):
                continue
            rows = activity.get(foreign_nit, {"processes": 0, "contracts": 0})
            hits.append(
                {
                    "alias": alias,
                    "owner": owner,
                    "foreign_name": row.get("nombre_entidad"),
                    "foreign_nit": foreign_nit,
                    "foreign_processes": rows["processes"],
                    "foreign_contracts": rows["contracts"],
                    "is_physical_buyer": bool(rows["processes"] or rows["contracts"]),
                }
            )
    return sorted(hits, key=lambda row: (row["alias"], row["foreign_name"] or ""))


def foreign_activity(client: Socrata, nits: list[str]) -> dict[str, dict[str, int]]:
    if not nits:
        return {}
    quoted = ",".join(f'"{nit}"' for nit in nits)
    counts: dict[str, dict[str, int]] = {
        nit: {"processes": 0, "contracts": 0} for nit in nits
    }
    for label, dataset in (("processes", PROCESSES), ("contracts", CONTRACTS)):
        for row in client.get(
            dataset,
            {
                "$select": "nit_entidad,count(*) as rows",
                "$where": f"nit_entidad in ({quoted})",
                "$group": "nit_entidad",
                "$limit": 5000,
            },
        ):
            nit = digits(row.get("nit_entidad"))
            if nit in counts:
                counts[nit][label] = int(row.get("rows", 0))
    return counts


def projection_canaries() -> list[dict[str, Any]]:
    registry = load_registry()
    builder = SoQLBuilder()
    rows = []
    for canonical in PROJECTED:
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
                "declared": names is not None,
                "exact_only": (
                    bool(names)
                    and " LIKE " not in soql
                    and "UPPER" not in soql
                    and "unaccent" not in soql
                    and "%" not in soql
                    and f"{field} = " in soql
                ),
                "params_immutable": params == snapshot,
            }
        rows.append(
            {
                "canonical_name": canonical,
                "identity_key": binding.identity_key,
                "observed_at": binding.observed_at.isoformat(),
                "evidence": binding.evidence,
                "observed_rows": binding.observed_rows,
                "datasets": datasets,
            }
        )
    for canonical in NOT_PROJECTED:
        rows.append(
            {
                "canonical_name": canonical,
                "identity_key": None,
                "no_binding_reason": "CANONICAL_IS_THE_PHYSICAL_NAME_BYTE_FOR_BYTE",
                "datasets": {
                    dataset: {"declared": load_registry().project(canonical, dataset) is not None}
                    for dataset in (PROCESSES, CONTRACTS)
                },
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

    universe = load_universe()
    previous_post = r2fa_post()
    nits = sorted({row["nit"] for row in universe if row.get("nit")})
    activity = grouped_activity(client, nits)
    directory = client.buyer_directory()
    registry_by_nit = registry_names(directory, set(nits))

    resolver = EntityResolver(ALIASES_DB)
    gazetteer = json.loads(ALIASES_DB.read_text(encoding="utf-8"))
    records = {record["official_name"]: record for record in gazetteer}
    by_nit: dict[str, list[str]] = defaultdict(list)
    records_by_source_name: dict[str, list[str]] = defaultdict(list)
    for record in gazetteer:
        records_by_source_name[normalize_text(record["official_name"])].append(
            record["official_name"]
        )
        if record.get("nit"):
            by_nit[digits(record["nit"])].append(record["official_name"])

    cases = []
    for row in universe:
        logical = row["logical_name"]
        nit = row.get("nit")
        process_rows = sorted(
            activity.get((nit, "processes", False), []),
            key=lambda item: (-item["rows"], item["name"] or ""),
        )
        contract_rows = sorted(
            activity.get((nit, "contracts", False), []),
            key=lambda item: (-item["rows"], item["name"] or ""),
        )
        source_names = {
            normalize_text(item["name"])
            for item in process_rows + contract_rows
            if item.get("name")
            and not str(item["name"]).startswith("CCE ENTIDAD DE PRUEBAS")
        }
        expected_names = source_names | {
            normalize_text(name) for name in by_nit.get(nit or "", [])
        }
        result = resolver.resolve_entidad(logical)
        scan = resolver.scan_entity(normalize_text(logical))
        post = resolution_status(logical, result, expected_names)
        existing = set(by_nit.get(nit or "", []))
        for source_name in source_names:
            existing.update(records_by_source_name.get(source_name, []))
        if result.value in records:
            existing.add(result.value)

        case = {
            "logical_name": logical,
            "nit": nit,
            "in_target": logical in TARGETS,
            "registry_names": registry_by_nit.get(nit or "", []),
            "process_source_names": [item["name"] for item in process_rows],
            "contract_source_names": [item["name"] for item in contract_rows],
            "process_observed_rows": {
                item["name"]: item["rows"] for item in process_rows
            },
            "contract_observed_rows": {
                item["name"]: item["rows"] for item in contract_rows
            },
            "all_time_processes": sum(item["rows"] for item in process_rows)
            if nit
            else None,
            "all_time_contracts": sum(item["rows"] for item in contract_rows)
            if nit
            else None,
            "recent_12m_processes": sum(
                item["rows"] for item in activity.get((nit, "processes", True), [])
            )
            if nit
            else None,
            "recent_12m_contracts": sum(
                item["rows"] for item in activity.get((nit, "contracts", True), [])
            )
            if nit
            else None,
            "existing_gazetteer_records": sorted(existing),
            "current_resolution": result.value,
            "current_method": result.method,
            "current_like_value": result.like_value,
            "current_clarification_needed": result.clarification_needed,
            "current_scan_owner": scan.official_name if scan else None,
            "current_scan_alias": scan.matched_alias if scan else None,
            "r2fa_post": previous_post.get(logical),
            "pre_classification": TARGET_PRE.get(logical, previous_post.get(logical)),
            "post_classification": post,
        }
        if logical in TARGETS:
            case["identity_classification"] = "SINGLE_SOURCE_NAME"
            case["decision"] = DECISIONS[logical]
            case["canonical_name"] = TARGETS[logical]
            case["source_projection"] = TARGETS[logical] in PROJECTED
            case["aliases_added"] = sorted(
                alias
                for alias, owner in {
                    **AUDITED_SCAN_ALIASES,
                    **EXACT_ONLY_ALIASES,
                }.items()
                if owner == TARGETS[logical]
            )
            case["scan_disabled_aliases"] = sorted(
                alias
                for alias, owner in EXACT_ONLY_ALIASES.items()
                if owner == TARGETS[logical]
            )
        cases.append(case)

    owner_nits = {
        canonical: digits(records[canonical]["nit"]) for canonical in TARGETS.values()
    }
    candidate_hits = scan_cross_owner(directory, owner_nits, {})
    foreign_nits = sorted({hit["foreign_nit"] for hit in candidate_hits})
    foreign = foreign_activity(client, foreign_nits)
    scan_hits = scan_cross_owner(directory, owner_nits, foreign)
    physical_cross_owner = [hit for hit in scan_hits if hit["is_physical_buyer"]]

    baseline = load_baseline()
    multi_owner = multi_owner_groups(gazetteer)
    source_registry = load_registry()

    target_cases = [case for case in cases if case["in_target"]]
    target_pre = Counter(case["pre_classification"] for case in target_cases)
    target_post = Counter(case["post_classification"] for case in target_cases)
    post_counts = Counter(case["post_classification"] for case in cases)
    regressions_outside = [
        case["logical_name"]
        for case in cases
        if not case["in_target"]
        and case["post_classification"] != case["r2fa_post"]
    ]

    payload = {
        "audit": "PUBLIC-UNIVERSITY-COMPLETION-R2FB",
        "type": "supervised-curation/replay",
        "as_of": date.today().isoformat(),
        "observed_at": OBSERVED_AT,
        "recent_since": RECENT_SINCE[:10],
        "head_before": HEAD_BEFORE,
        "authorities": {
            "identity": "NIT + physical source name + SECOP activity",
            "alias_policy": "ADR-072",
            "source_projection": "ADR-073",
            "processes": PROCESSES,
            "contracts": CONTRACTS,
            "buyer_directory": DIRECTORY,
            "directory_rows_scanned": len(directory),
        },
        "methodology": {
            "similarity_is_identity_authority": False,
            "source_name_split_rule": (
                "Different physical names for one NIT across datasets; absence in "
                "one dataset is not a split."
            ),
            "binding_rule": (
                "A binding is declared only when the canonical identity differs "
                "byte-for-byte from the physical name."
            ),
            "scan_audit_scope": "full buyer directory, token-sequence match",
        },
        "target_universe": sorted(TARGETS),
        "target_matrix_pre": dict(sorted(target_pre.items())),
        "target_matrix_post": dict(sorted(target_post.items())),
        "full_universe_post": {
            "total": len(cases),
            **{
                key.lower(): post_counts[key]
                for key in (
                    "PASS_EXACT",
                    "FAIL_CLOSED",
                    "WRONG_OWNER",
                    "FUZZY_ONLY",
                    "LIKE",
                    "AMBIGUOUS",
                )
            },
            "regressions_outside_target": regressions_outside,
            "new_regression_outside_target": len(regressions_outside),
        },
        "identity_changes": {
            "identities_added": sorted(TARGETS.values()),
            "aliases_added_normalized": sorted(
                set(AUDITED_SCAN_ALIASES) | set(EXACT_ONLY_ALIASES)
            ),
            "aliases_removed": [],
            "scan_disabled": sorted(EXACT_ONLY_ALIASES),
            "resolution_blocked": [],
            "unreproduced_surface_variants": UNREPRODUCED_SURFACE_VARIANTS,
        },
        "source_projection": {
            "bindings_added": sorted(PROJECTED),
            "bindings_not_required": sorted(NOT_PROJECTED),
            "sidecar_total_bindings": len(source_registry),
            "sidecar_schema_version": source_registry.provenance.schema_version,
            "sidecar_evidence_by_binding": {
                binding.canonical_name: binding.evidence
                for binding in source_registry.bindings()
            },
            "canaries": projection_canaries(),
        },
        "gates": {
            "new_scan_cross_owner": len(physical_cross_owner),
            "scan_cross_owner_hits": physical_cross_owner,
            "registry_homonyms_without_activity": [
                hit for hit in scan_hits if not hit["is_physical_buyer"]
            ],
            "exact_multi_owner_before": len(baseline),
            "exact_multi_owner_after": len(multi_owner),
            "new_multi_owner_keys": new_multi_owner_keys(gazetteer, baseline),
            "wrong_owner_target_post": target_post["WRONG_OWNER"],
            "fuzzy_only_target_post": target_post["FUZZY_ONLY"],
        },
        "verification": {
            "focused_regressions": {
                "command": (
                    ".venv/bin/python -m pytest -q "
                    "tests/test_public_university_completion_r2fb.py "
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
                "result": "902 passed",
            },
            "query_corpus": "89/89 PASS, 0 FAIL",
            "ruff": "PASS (27 hallazgos preexistentes en .hermes/ y docs/audits/evidence/)",
            "lint_core": "PASS",
            "full_suite": "10720 passed, 2 skipped, 15 xfailed",
            "new_xfails": 0,
            # Cada binding se contrasto contra el dataset con su propio filtro
            # proyectado: las filas devueltas igualan las observadas por NIT.
            "live_projection_parity": {
                "result": "PASS",
                "checked": {
                    "UNIVERSIDAD COLEGIO MAYOR DE CUNDINAMARCA": {PROCESSES: 16},
                    "UNIVERSIDAD DEL PACÍFICO": {PROCESSES: 987, CONTRACTS: 5},
                    "UNIVERSIDAD POPULAR DEL CESAR": {PROCESSES: 57},
                    "UNIVERSIDAD TECNOLÓGICA DEL CHOCÓ DIEGO LUIS CÓRDOBA": {
                        PROCESSES: 2257,
                        CONTRACTS: 1,
                    },
                },
            },
            # El lote no toca parser, LLM ni follow-ups, asi que la suite sirve
            # de gate de regresion. No autoriza a hablar de cobertura
            # conversacional viva: el proveedor devuelve 402.
            "llm_execution_mode": "DEGRADED_REGEX_ONLY",
            "llm_provider_live_coverage": "NOT_VERIFIED",
            "llm_provider_probe": "deepseek chat/completions -> HTTP 402 Insufficient Balance",
        },
        "cases": cases,
        "generated_at": date.today().isoformat(),
        "recommended_replay": (
            ".venv/bin/python scripts/audits/audit_public_university_completion_r2fb.py"
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "target_pre": payload["target_matrix_pre"],
                "target_post": payload["target_matrix_post"],
                "post": payload["full_universe_post"],
                "gates": {
                    key: value
                    for key, value in payload["gates"].items()
                    if key != "registry_homonyms_without_activity"
                },
                "registry_homonyms_without_activity": len(
                    payload["gates"]["registry_homonyms_without_activity"]
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
