#!/usr/bin/env python3
"""Auditor R2E: universo de control nacional (4 raíces + 2 relacionados).

Reproduce, contra el HEAD vivo:

* la identidad SECOP II de cada comprador por NIT (nombres físicos y volumen);
* la matriz de resolución de nombres institucionales, formas humanas y siglas;
* el riesgo de scan de cada alias auditado, cruzado por SECUENCIA COMPLETA DE
  TOKENS contra el directorio oficial de compradores ``4ex9-j3n8``;
* los canarios padre/relacionado y de control territorial;
* las puertas de colisión y el smoke de dataset con el ``WHERE`` del runtime.

La actividad transaccional decide si un comprador contenedor es un dueño ajeno
real o un registro duplicado de la misma institución: un NIT sin procesos ni
contratos no puede secuestrar nada.  Lee el gazetteer y el resolver; no los
modifica.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
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

from app.core.actor_identity import compute_check_digit, normalize_identity  # noqa: E402
from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.entity_types import name_tokens  # noqa: E402
from app.core.scan_alias_policy import record_alias_policy  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402
from gazetteer_ownership_baseline import (  # noqa: E402
    load_baseline,
    multi_owner_groups,
    new_multi_owner_keys,
)

DB = ROOT / "app/data/aliases_db.json"
OUT = ROOT / "artifacts/audits/control_national_r2e_candidates.json"

BUYER_DIRECTORY_ID = "4ex9-j3n8"
PROCESS_DATASET_ID = "p6dx-8zbt"
CONTRACT_DATASET_ID = "jbjy-vk9h"
RESOURCE = "https://www.datos.gov.co/resource/{}.json"
RECENT_SINCE = "2025-09-01T00:00:00.000"

CGR = "CGR-NIVEL CENTRAL"
AGR = "AUDITORIA GENERAL DE LA REPUBLICA"
PGN = "PROCURADURIA GENERAL DE LA NACION"
DP = "DEFENSORÍA DEL PUEBLO"
FBS = "FONDO DE BIENESTAR SOCIAL DE LA CONTRALORIA GENERAL DE LA REPUBLICA"
IEMP = "INSTITUTO DE ESTUDIOS DEL MINISTERIO PUBLICO."

UNIVERSE = (
    {
        "code": "CGR",
        "logical_name": "Contraloría General de la República",
        "institutional_role": "CONTROL_ROOT",
        "nit": "899999067",
        "expected_physical_name": CGR,
    },
    {
        "code": "AGR",
        "logical_name": "Auditoría General de la República",
        "institutional_role": "CONTROL_ROOT",
        "nit": "830065741",
        "expected_physical_name": AGR,
    },
    {
        "code": "PGN",
        "logical_name": "Procuraduría General de la Nación",
        "institutional_role": "CONTROL_ROOT",
        "nit": "899999119",
        "expected_physical_name": PGN,
    },
    {
        "code": "DP",
        "logical_name": "Defensoría del Pueblo",
        "institutional_role": "CONTROL_ROOT",
        "nit": "800186061",
        "expected_physical_name": DP,
    },
    {
        "code": "FBS",
        "logical_name": "Fondo de Bienestar Social de la Contraloría General de la República",
        "institutional_role": "RELATED_BUYER",
        "parent_concept": "Contraloría General de la República",
        "nit": "800252683",
        "expected_physical_name": FBS,
    },
    {
        "code": "IEMP",
        "logical_name": "Instituto de Estudios del Ministerio Público",
        "institutional_role": "RELATED_BUYER",
        "parent_concept": "Procuraduría General de la Nación",
        "nit": "830015728",
        "expected_physical_name": IEMP,
    },
)

HUMAN_FORMS = {
    "la Contraloría General de la República": CGR,
    "Contraloría General": CGR,
    "la Contraloría": CGR,
    "Contraloría": CGR,
    "la Auditoría General de la República": AGR,
    "Auditoría General": AGR,
    "la Auditoría General": AGR,
    "la Procuraduría General de la Nación": PGN,
    "Procuraduría General": PGN,
    "la Procuraduría": PGN,
    "Procuraduría": PGN,
    "la Defensoría del Pueblo": DP,
    "la Defensoría": DP,
    "Defensoría": DP,
    "Defensoría Pública": DP,
    "el Fondo de Bienestar Social de la Contraloría General de la República": FBS,
    "Fondo de Bienestar Social de la CGR": FBS,
    "el Instituto de Estudios del Ministerio Público": IEMP,
}

ACRONYMS = {"CGR": CGR, "AGR": AGR, "PGN": PGN, "DP": DP, "IEMP": IEMP}

# Alias auditado -> (owner, decisión, exact_eligible, scan_eligible)
AUDITED_ALIASES = {
    "contraloría": (CGR, "KEEP_EXACT_DISABLE_SCAN", True, False),
    "la contraloría": (CGR, "KEEP_EXACT_DISABLE_SCAN", True, False),
    "contraloría general": (CGR, "KEEP_EXACT_DISABLE_SCAN", True, False),
    "la contraloría general": (CGR, "KEEP_EXACT_DISABLE_SCAN", True, False),
    "contraloría general de la república": (CGR, "KEEP", True, True),
    "la contraloría general de la república": (CGR, "KEEP", True, True),
    "cgr": (CGR, "KEEP", True, True),
    "defensoría": (DP, "KEEP_EXACT_DISABLE_SCAN", True, False),
    "la defensoría": (DP, "KEEP_EXACT_DISABLE_SCAN", True, False),
    "defensoría del pueblo": (DP, "KEEP", True, True),
    "defensoría pública": (DP, "KEEP", True, True),
    "dp": (DP, "KEEP", True, False),
    "auditoría general de la república": (AGR, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "la auditoría general de la república": (AGR, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "auditoría general": (AGR, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "la auditoría general": (AGR, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "agr": (AGR, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "procuraduría general de la nación": (PGN, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "la procuraduría general de la nación": (PGN, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "procuraduría general": (PGN, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "la procuraduría general": (PGN, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "procuraduría": (PGN, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "la procuraduría": (PGN, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "pgn": (PGN, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "fondo de bienestar social de la contraloría general de la república": (
        FBS, "ACCEPT_REGISTRY_VERIFIED", True, True,
    ),
    "el fondo de bienestar social de la contraloría general de la república": (
        FBS, "ACCEPT_REGISTRY_VERIFIED", True, True,
    ),
    "fondo de bienestar social de la cgr": (FBS, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "fondo de bienestar social cgr": (FBS, "ACCEPT_REGISTRY_VERIFIED", True, True),
    "instituto de estudios del ministerio público": (
        IEMP, "ACCEPT_REGISTRY_VERIFIED", True, True,
    ),
    "el instituto de estudios del ministerio público": (
        IEMP, "ACCEPT_REGISTRY_VERIFIED", True, True,
    ),
    "iemp": (IEMP, "ACCEPT_REGISTRY_VERIFIED", True, True),
}

# Frases medidas y rechazadas: no identifican un comprador único.
REJECTED_CANDIDATES = {
    "ministerio público": "OVER_GENERIC_ALIAS — concepto institucional (PGN + Defensoría + personerías)",
    "instituto de estudios": "OVER_GENERIC_ALIAS — cabeza genérica sin identidad",
    "fondo de bienestar social": "OVER_GENERIC_ALIAS — 9 fondos físicos distintos",
    "fondo de bienestar social de la contraloría": "OVER_GENERIC_ALIAS — 4 fondos territoriales",
    "bienestar social": "OVER_GENERIC_ALIAS — 12 compradores físicos distintos",
    "auditoría": "OVER_GENERIC_ALIAS — función, no identidad",
}

BLOCKED_PHRASES = (
    "Ministerio Público",
    "fondo de bienestar social",
    "Fondo de Bienestar Social de la Contraloría",
)

TERRITORIAL_CANARIES = (
    "contratos de la contraloria departamental de bolivar",
    "procesos de la contraloria general de santander",
    "contratos de la contraloria general de caldas",
    "contratos de la contraloria general de medellin",
    "contratos de la contraloria municipal de ibague",
    "contratos de la defensoria civil de nobsa",
    "contratos de la personeria municipal de soledad",
    "contratos del fondo de bienestar social de la contraloria departamental de bolivar",
)

PARENT_RELATED_CANARIES = (
    ("CGR_VS_FONDO", "contratos del fondo de bienestar social de la contraloria general de la republica", FBS),
    ("CGR_VS_FONDO", "licitaciones de la contraloria general de la republica", CGR),
    ("PGN_VS_IEMP", "contratos del instituto de estudios del ministerio publico", IEMP),
    ("PGN_VS_IEMP", "contratos de la procuraduria general de la nacion", PGN),
)

SMOKE_PHRASES = (
    ("Contraloría General de la República", CGR),
    ("Auditoría General de la República", AGR),
    ("Procuraduría General de la Nación", PGN),
    ("Defensoría del Pueblo", DP),
    ("Fondo de Bienestar Social de la Contraloría General de la República", FBS),
    ("Instituto de Estudios del Ministerio Público", IEMP),
)

NATIONAL_OWNERS = frozenset({CGR, AGR, PGN, DP, FBS, IEMP})


def identity_key(raw_nit: str | None) -> str | None:
    """Colapsa NIT con y sin dígito de verificación a una identidad única."""
    if not raw_nit:
        return None
    match = normalize_identity(raw_nit)
    key = getattr(match, "key", None)
    return getattr(key, "base_number", None)


def contains_token_sequence(haystack: list[str], needle: list[str]) -> bool:
    if not needle or len(needle) > len(haystack):
        return False
    return any(
        haystack[i:i + len(needle)] == needle
        for i in range(len(haystack) - len(needle) + 1)
    )


class Socrata:
    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "SECOPPAL-R2E-audit/1.0",
                "X-App-Token": token,
            }
        )

    def get(self, dataset: str, params: dict[str, Any], tries: int = 6) -> list[dict]:
        last_error: str | None = None
        for attempt in range(1, tries + 1):
            try:
                response = self.session.get(
                    RESOURCE.format(dataset), params=params, timeout=120
                )
                payload = response.json()
                if response.status_code == 200 and isinstance(payload, list):
                    return payload
                last_error = f"HTTP {response.status_code}: {str(payload)[:160]}"
            except (requests.RequestException, ValueError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(1.5 * attempt)
        raise RuntimeError(f"{dataset} unavailable: {last_error}")

    def buyer_directory(self) -> list[dict]:
        rows: list[dict] = []
        offset = 0
        while True:
            page = self.get(
                BUYER_DIRECTORY_ID,
                {
                    "$select": "codigo_entidad,nombre_entidad,nit_entidad,esta_activa",
                    "$where": "es_entidad='Si'",
                    "$order": "codigo_entidad",
                    "$limit": 50_000,
                    "$offset": offset,
                },
            )
            rows.extend(page)
            if len(page) < 50_000:
                return rows
            offset += 50_000

    def names_by_nit(self, nits: list[str]) -> dict[str, dict[str, list[dict]]]:
        """Nombres físicos y volumen (histórico y 12 meses) por NIT."""
        if not nits:
            return {}
        quoted = ",".join(f"'{value}'" for value in dict.fromkeys(nits))
        evidence: dict[str, dict[str, list[dict]]] = defaultdict(
            lambda: {"procesos": [], "contratos": []}
        )
        for label, dataset, field, date_field in (
            ("procesos", PROCESS_DATASET_ID, "entidad", "fecha_de_publicacion_del"),
            ("contratos", CONTRACT_DATASET_ID, "nombre_entidad", "fecha_de_firma"),
        ):
            base = self.get(
                dataset,
                {
                    "$select": f"nit_entidad,{field},count(*) as rows",
                    "$where": f"nit_entidad in ({quoted})",
                    "$group": f"nit_entidad,{field}",
                    "$limit": 5_000,
                },
            )
            recent = self.get(
                dataset,
                {
                    "$select": f"nit_entidad,{field},count(*) as rows",
                    "$where": (
                        f"nit_entidad in ({quoted}) and {date_field} >= '{RECENT_SINCE}'"
                    ),
                    "$group": f"nit_entidad,{field}",
                    "$limit": 5_000,
                },
            )
            recent_index = {
                (row["nit_entidad"], row[field]): int(row["rows"]) for row in recent
            }
            for row in base:
                key = identity_key(row["nit_entidad"])
                if key is None:
                    continue
                evidence[key][label].append(
                    {
                        "published_nit": row["nit_entidad"],
                        "name": row[field],
                        "all_time_rows": int(row["rows"]),
                        "recent_12m_rows": recent_index.get(
                            (row["nit_entidad"], row[field]), 0
                        ),
                    }
                )
        return dict(evidence)

    def like_rows(self, dataset: str, field: str, value: str) -> int:
        """Filas que devuelve el ``WHERE`` de entidad que emite el runtime."""
        escaped = value.replace("'", "''")
        where = (
            f"unaccent(UPPER({field})) LIKE unaccent(UPPER('%{escaped}%'))"
        )
        rows = self.get(
            dataset, {"$select": "count(*) as rows", "$where": where, "$limit": 1}
        )
        return int(rows[0]["rows"]) if rows else 0


def audit_identity(client: Socrata) -> list[dict]:
    nits: list[str] = []
    for entity in UNIVERSE:
        digit = compute_check_digit(entity["nit"])
        nits.append(entity["nit"])
        if digit is not None:
            nits.append(f"{entity['nit']}{digit}")
    evidence = client.names_by_nit(nits)

    rows: list[dict] = []
    for entity in UNIVERSE:
        found = evidence.get(entity["nit"], {"procesos": [], "contratos": []})
        process_names = [item["name"] for item in found["procesos"]]
        contract_names = [item["name"] for item in found["contratos"]]
        published = sorted(
            {item["published_nit"] for group in found.values() for item in group}
        )
        distinct = {normalize_text(name) for name in process_names + contract_names}
        if not distinct:
            shape = "NO_SECOP_EVIDENCE"
        elif len(distinct) > 1:
            shape = "MULTIPLE_PHYSICAL_SAME_IDENTITY"
        else:
            shape = "SINGLE_PHYSICAL_NAME"
        rows.append(
            {
                **entity,
                "published_nit_variants": published,
                "process_source_names": sorted(process_names),
                "contract_source_names": sorted(contract_names),
                "all_time_processes": sum(i["all_time_rows"] for i in found["procesos"]),
                "all_time_contracts": sum(i["all_time_rows"] for i in found["contratos"]),
                "recent_processes": sum(i["recent_12m_rows"] for i in found["procesos"]),
                "recent_contracts": sum(i["recent_12m_rows"] for i in found["contratos"]),
                "expected_name_all_time_processes": sum(
                    i["all_time_rows"] for i in found["procesos"]
                    if normalize_text(i["name"]) == normalize_text(entity["expected_physical_name"])
                ),
                "expected_name_recent_processes": sum(
                    i["recent_12m_rows"] for i in found["procesos"]
                    if normalize_text(i["name"]) == normalize_text(entity["expected_physical_name"])
                ),
                "expected_name_all_time_contracts": sum(
                    i["all_time_rows"] for i in found["contratos"]
                    if normalize_text(i["name"]) == normalize_text(entity["expected_physical_name"])
                ),
                "expected_name_recent_contracts": sum(
                    i["recent_12m_rows"] for i in found["contratos"]
                    if normalize_text(i["name"]) == normalize_text(entity["expected_physical_name"])
                ),
                "secop_identity_shape": shape,
                "identity_verified": normalize_text(entity["expected_physical_name"])
                in distinct,
            }
        )
    return rows


def classify(result, expected: str, exact_owners: set[str]) -> str:
    if result.value == expected:
        if result.method == "exact" and expected in exact_owners:
            return "PASS_EXACT"
        if str(result.method).startswith("fuzzy"):
            return "FUZZY_ONLY"
        return "PARTIAL"
    if result.value:
        return "WRONG_OWNER"
    if str(result.method) in {"root_alias_ambiguous"}:
        return "AMBIGUOUS"
    if result.clarification_needed:
        return "FAIL_CLOSED"
    return "MISSING"


def audit_resolution(resolver: EntityResolver, owners: dict[str, set[str]]) -> dict:
    def probe(query: str, expected: str) -> dict:
        result = resolver.resolve_entidad(query)
        return {
            "query": query,
            "expected_owner": expected,
            "resolved_owner": result.value,
            "method": result.method,
            "classification": classify(
                result, expected, owners.get(normalize_text(query), set())
            ),
        }

    institutional = [
        probe(entity["logical_name"], entity["expected_physical_name"])
        for entity in UNIVERSE
    ]
    return {
        "institutional_names": institutional,
        "human_forms": [probe(q, e) for q, e in sorted(HUMAN_FORMS.items())],
        "acronyms": [probe(q, e) for q, e in sorted(ACRONYMS.items())],
        "matrix": {
            status: sum(1 for row in institutional if row["classification"] == status)
            for status in (
                "PASS_EXACT", "FAIL_CLOSED", "WRONG_OWNER", "FUZZY_ONLY",
                "AMBIGUOUS", "PARTIAL", "MISSING",
            )
        },
    }


def audit_alias_safety(
    resolver: EntityResolver,
    buyers: list[dict],
    activity: dict[str, dict],
    gazetteer: list[dict],
) -> list[dict]:
    tokenized = [
        (name_tokens(row.get("nombre_entidad") or ""), row.get("nombre_entidad") or "",
         identity_key(row.get("nit_entidad")))
        for row in buyers
    ]
    owner_identity = {
        entity["expected_physical_name"]: entity["nit"] for entity in UNIVERSE
    }
    exact_owners: dict[str, set[str]] = defaultdict(set)
    for record in gazetteer:
        for alias in record.get("aliases", []):
            exact_owners[normalize_text(alias)].add(record["official_name"])

    rows: list[dict] = []
    for alias, (owner, decision, exact_ok, scan_ok) in AUDITED_ALIASES.items():
        key = normalize_text(alias)
        want = name_tokens(alias)
        own_names: list[str] = []
        cross: list[dict] = []
        for tokens, name, ident in tokenized:
            if not contains_token_sequence(tokens, want):
                continue
            if ident == owner_identity[owner]:
                own_names.append(name)
                continue
            counts = activity.get(ident or "", {})
            active = bool(counts.get("processes", 0) or counts.get("contracts", 0))
            scan = resolver.scan_entity(normalize_text(name))
            cross.append(
                {
                    "physical_name": name,
                    "identity": ident,
                    "secop_processes": counts.get("processes", 0),
                    "secop_contracts": counts.get("contracts", 0),
                    "active_buyer": active,
                    "scan_owner": scan.official_name if scan else None,
                    "scan_hijacked_by_this_owner": bool(scan and scan.official_name == owner),
                }
            )
        result = resolver.resolve_entidad(alias)
        indexed = {
            entry_alias
            for entries in resolver._scan_index.values()
            for entry_alias, _ in entries
        }
        rows.append(
            {
                "alias": alias,
                "owner": owner,
                "decision": decision,
                "exact_eligible": exact_ok,
                "scan_eligible": scan_ok,
                "exact_owner_count": len(exact_owners.get(key, set())),
                "own_physical_names": sorted(set(own_names)),
                "other_physical_buyers_containing_alias": len(cross),
                "scan_cross_owner_candidates": sum(
                    1 for item in cross
                    if item["active_buyer"] and item["scan_hijacked_by_this_owner"]
                ),
                "inactive_registry_duplicates": sum(
                    1 for item in cross
                    if not item["active_buyer"] and item["scan_hijacked_by_this_owner"]
                ),
                "cross_owner_detail": cross,
                "observed_resolution": {"owner": result.value, "method": result.method},
                "observed_scan_eligible": key in indexed,
                "safety_class": (
                    "SAFE_EXACT_SAFE_SCAN" if exact_ok and scan_ok
                    else "SAFE_EXACT_DISABLE_SCAN" if exact_ok
                    else "UNSAFE_EXACT"
                ),
            }
        )
    return rows


def audit_canaries(resolver: EntityResolver) -> dict:
    territorial = []
    for text in TERRITORIAL_CANARIES:
        scan = resolver.scan_entity(normalize_text(text))
        owner = scan.official_name if scan else None
        territorial.append(
            {
                "text": text,
                "scan_owner": owner,
                "matched_alias": scan.matched_alias if scan else None,
                "national_root_hijack": owner in NATIONAL_OWNERS,
            }
        )
    parent = []
    for group, text, expected in PARENT_RELATED_CANARIES:
        scan = resolver.scan_entity(normalize_text(text))
        owner = scan.official_name if scan else None
        parent.append(
            {
                "group": group,
                "text": text,
                "expected_owner": expected,
                "scan_owner": owner,
                "matched_alias": scan.matched_alias if scan else None,
                "pass": owner == expected,
            }
        )
    blocked = []
    for phrase in BLOCKED_PHRASES:
        result = resolver.resolve_entidad(phrase)
        blocked.append(
            {
                "phrase": phrase,
                "owner": result.value,
                "method": result.method,
                "fail_closed": result.value is None and result.clarification_needed,
            }
        )
    return {
        "territorial_control": territorial,
        "parent_related": parent,
        "blocked_phrases": blocked,
        "territorial_non_hijack": not any(
            row["national_root_hijack"] for row in territorial
        ),
    }


def audit_smoke(client: Socrata, resolver: EntityResolver) -> list[dict]:
    rows = []
    for phrase, expected in SMOKE_PHRASES:
        result = resolver.resolve_entidad(phrase)
        owner = result.value
        processes = contracts = None
        if owner:
            processes = client.like_rows(PROCESS_DATASET_ID, "entidad", owner)
            contracts = client.like_rows(CONTRACT_DATASET_ID, "nombre_entidad", owner)
        rows.append(
            {
                "phrase": phrase,
                "expected_physical_name": expected,
                "resolved_owner": owner,
                "method": result.method,
                "runtime_where": (
                    "unaccent(UPPER(<entidad>)) LIKE unaccent(UPPER('%<owner>%'))"
                ),
                "process_rows": processes,
                "contract_rows": contracts,
                "zero_row": bool(owner) and not (processes or contracts),
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--label", default="POST", help="PRE | POST")
    parser.add_argument(
        "--gazetteer",
        type=Path,
        default=DB,
        help="Gazetteer a auditar; permite reproducir el estado PRE desde git show",
    )
    args = parser.parse_args()

    if args.env_file.exists():
        load_dotenv(args.env_file, override=False)
    token = os.getenv("SECOP_APP_TOKEN")
    if not token:
        raise SystemExit("SECOP_APP_TOKEN is required; pass --env-file .env")

    client = Socrata(token)
    gazetteer = json.loads(args.gazetteer.read_text(encoding="utf-8"))
    resolver = EntityResolver(args.gazetteer)
    owners: dict[str, set[str]] = defaultdict(set)
    for record in gazetteer:
        for alias in record.get("aliases", []):
            owners[normalize_text(alias)].add(record["official_name"])

    identity = audit_identity(client)
    buyers = client.buyer_directory()

    # Actividad de todo comprador que contiene algún alias auditado.
    tokenized = [
        (name_tokens(row.get("nombre_entidad") or ""), identity_key(row.get("nit_entidad")))
        for row in buyers
    ]
    touched: set[str] = set()
    for alias in AUDITED_ALIASES:
        want = name_tokens(alias)
        for tokens, ident in tokenized:
            if ident and contains_token_sequence(tokens, want):
                touched.add(ident)
    probe_nits: list[str] = []
    for base in sorted(touched):
        probe_nits.append(base)
        digit = compute_check_digit(base)
        if digit is not None:
            probe_nits.append(f"{base}{digit}")
    activity: dict[str, dict] = {}
    for start in range(0, len(probe_nits), 60):
        chunk = probe_nits[start:start + 60]
        found = client.names_by_nit(chunk)
        for base, groups in found.items():
            entry = activity.setdefault(base, {"processes": 0, "contracts": 0})
            entry["processes"] += sum(i["all_time_rows"] for i in groups["procesos"])
            entry["contracts"] += sum(i["all_time_rows"] for i in groups["contratos"])

    aliases = audit_alias_safety(resolver, buyers, activity, gazetteer)
    resolution = audit_resolution(resolver, owners)
    canaries = audit_canaries(resolver)
    smoke = audit_smoke(client, resolver)

    policy_records = {
        record["official_name"]: {
            "scan_disabled_aliases": sorted(record_alias_policy(record)[0]),
            "resolution_blocked_aliases": sorted(record_alias_policy(record)[1]),
        }
        for record in gazetteer
        if record.get("scan_disabled_aliases") or record.get("resolution_blocked_aliases")
    }

    payload = {
        "audit": "CONTROL-NATIONAL-R2E",
        "label": args.label,
        "type": "discovery + remediation verification",
        "as_of": date.today().isoformat(),
        "recent_since": RECENT_SINCE[:10],
        "authority": {
            "state_structure": (
                "https://www.funcionpublica.gov.co/eva/gestornormativo/"
                "manual-estado/organismos-control.php"
            ),
            "processes": PROCESS_DATASET_ID,
            "contracts": CONTRACT_DATASET_ID,
            "buyer_directory": BUYER_DIRECTORY_ID,
        },
        "adr": "ADR-072",
        "gazetteer": str(args.gazetteer.relative_to(ROOT) if args.gazetteer.is_relative_to(ROOT) else args.gazetteer),
        "methodology": {
            "containment": "secuencia completa de tokens normalizados, no substring",
            "identity": "NIT normalizado sin dígito de verificación",
            "cross_owner_rule": (
                "un comprador contenedor sólo cuenta como dueño ajeno si tiene "
                "actividad transaccional en SECOP II; los registros con cédula y "
                "el nombre de la propia institución son duplicados registrales"
            ),
            "buyer_directory_size": len(buyers),
        },
        "universe": {
            "control_roots": sum(
                1 for e in UNIVERSE if e["institutional_role"] == "CONTROL_ROOT"
            ),
            "related_buyers": sum(
                1 for e in UNIVERSE if e["institutional_role"] == "RELATED_BUYER"
            ),
            "total": len(UNIVERSE),
        },
        "identity": identity,
        "resolution": resolution,
        "alias_policy_records": policy_records,
        "alias_safety": aliases,
        "rejected_alias_candidates": REJECTED_CANDIDATES,
        "canaries": canaries,
        "dataset_smoke": smoke,
        "gates": {
            "exact_multi_owner": len(multi_owner_groups(gazetteer)),
            "new_multi_owner_keys": new_multi_owner_keys(gazetteer, load_baseline()),
            "new_scan_cross_owner": sum(
                row["scan_cross_owner_candidates"] for row in aliases
            ),
            "inactive_registry_duplicates": sum(
                row["inactive_registry_duplicates"] for row in aliases
            ),
            "territorial_non_hijack": canaries["territorial_non_hijack"],
            "parent_related_pass": all(row["pass"] for row in canaries["parent_related"]),
            "recent_volume": {
                "processes": sum(row["recent_processes"] for row in identity),
                "contracts": sum(row["recent_contracts"] for row in identity),
            },
        },
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(payload["gates"], ensure_ascii=False, indent=2))
    print(json.dumps(payload["resolution"]["matrix"], ensure_ascii=False))
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
