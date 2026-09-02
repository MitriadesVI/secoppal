#!/usr/bin/env python3
"""Auditor R2D.2: seguridad de aliases liberados por D6.

Reconstruye exactamente ``FREED_BY_D6`` (rechazado por la regla OLD y
aceptado por B'), lo cruza por secuencias completas de tokens contra el
directorio físico de compradores SECOP II y ejecuta una muestra adversarial
por ``QueryRouter`` bajo la regla viva y la regla OLD.

La fuente remota es el directorio oficial ``SECOP II - Contacto Entidades y
Proveedores`` (4ex9-j3n8), no una búsqueda por parecido. El código y el NIT
publicados identifican al comprador físico; ``normalize_identity`` colapsa el
mismo NIT con o sin dígito de verificación.

No modifica runtime ni ``aliases_db.json``. Sólo escribe el artefacto pedido.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.actor_identity import compute_check_digit, normalize_identity  # noqa: E402
from app.core.entity_resolver import EntityResolver, GAZETTEER_CIUDADES  # noqa: E402
from app.core.generic_buyer import department_root_buyer_target  # noqa: E402
from app.core.query_router import QueryRouter  # noqa: E402
from app.data.departamentos import DEPARTAMENTOS  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

DB = ROOT / "app/data/aliases_db.json"
R2D1 = ROOT / "artifacts/audits/d6_department_suffix_r2d1.json"
OUT = ROOT / "artifacts/audits/freed_alias_safety_r2d2.json"

BUYER_DIRECTORY_ID = "4ex9-j3n8"
BUYER_DIRECTORY_URL = f"https://www.datos.gov.co/resource/{BUYER_DIRECTORY_ID}.json"
TERMINAL_SANTA_MARTA_NIT = "891701694"
PROCESS_DATASET_ID = "p6dx-8zbt"
CONTRACT_DATASET_ID = "jbjy-vk9h"

# Cabezas que declaran una clase institucional, no una materia. ``salud``,
# ``ambiente`` o ``infraestructura`` son fragmentos funcionales, no cabezas.
INSTITUTIONAL_HEADS = frozenset(
    {
        "administracion",
        "aerocivil",
        "agencia",
        "alcaldia",
        "armada",
        "asamblea",
        "autoridad",
        "banco",
        "caja",
        "camara",
        "canal",
        "centro",
        "colegio",
        "comision",
        "concejo",
        "consejo",
        "contraloria",
        "corporacion",
        "defensoria",
        "departamento",
        "direccion",
        "distrito",
        "ejercito",
        "empresa",
        "empresas",
        "entidad",
        "escuela",
        "ese",
        "establecimiento",
        "federacion",
        "financiera",
        "fiscalia",
        "fondo",
        "fuerza",
        "fundacion",
        "gobernacion",
        "gobierno",
        "hospital",
        "icbf",
        "institucion",
        "instituto",
        "junta",
        "licorera",
        "loteria",
        "ministerio",
        "municipio",
        "notaria",
        "oficina",
        "personeria",
        "policia",
        "procuraduria",
        "secretaria",
        "sena",
        "servicio",
        "servicios",
        "sociedad",
        "superintendencia",
        "terminal",
        "tribunal",
        "unidad",
        "universidad",
    }
)

GOVERNMENT_HEADS = frozenset(
    {"alcaldia", "gobernacion", "gobierno", "municipio", "distrito", "departamento"}
)
LOCATION_WRAPPERS = frozenset({"de", "del", "la", "el", "los", "las", "en", "al", "d", "c", "dc"})

CANARIES = (
    {
        "id": "SECRETARIA_GENERAL_BOGOTA",
        "phrase": "Secretaría General Alcaldía Mayor Bogotá",
        "gazetteer_owner": "SECRETARÍA GENERAL DE LA ALCALDÍA MAYOR DE BOGOTÁ",
    },
    {
        "id": "EAAB",
        "phrase": "Empresa de Acueducto y Alcantarillado de Bogotá",
        "gazetteer_owner": "EMPRESA DE ACUEDUCTO Y ALCANTARILLADO DE BOGOTÁ - E.S.P.",
    },
    {
        "id": "ICCU",
        "phrase": "Instituto de Caminos y Construcciones de Cundinamarca",
        "gazetteer_owner": ("INSTITUTO DE CAMINOS Y CONSTRUCCIONES DE CUNDINAMARCA - ICCU"),
    },
    {
        "id": "CANAL_TELEANTIOQUIA",
        "phrase": "Canal Teleantioquia",
        "gazetteer_owner": "SOCIEDAD TELEVISION DE ANTIOQUIA LTDA",
    },
    {
        "id": "TERMINAL_SANTA_MARTA",
        "phrase": "Terminal de Transportes de Santa Marta Magdalena",
        "gazetteer_owner": "ALCALDÍA DISTRITAL DE SANTA MARTA",
    },
    {
        "id": "ESE_META",
        "phrase": "ESE del departamento de Meta",
        "gazetteer_owner": "EMPRESA SOCIAL DEL ESTADO DEL DEPARTAMENTO DEL META E.S.E",
    },
)


def catalog_norm(value: str | None) -> str:
    """Normalización léxica de comparación, conservando fronteras de token."""
    decomposed = unicodedata.normalize("NFKD", value or "")
    folded = "".join(char for char in decomposed if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^A-Z0-9]+", " ", folded.upper()).split())


def nit_base(value: str | None) -> str | None:
    """NIT sin DV, usando la autoridad de identidad que ya vive en runtime."""
    match = normalize_identity(value or "", document_type="NIT", source=BUYER_DIRECTORY_ID)
    return match.base_number


def token_sequence(haystack: tuple[str, ...], needle: tuple[str, ...]) -> bool:
    """Contención por tokens completos; nunca substring crudo."""
    if not needle or len(needle) > len(haystack):
        return False
    return any(
        haystack[index : index + len(needle)] == needle
        for index in range(len(haystack) - len(needle) + 1)
    )


def _old_reject(resolver: EntityResolver, alias: str, full_text: str) -> bool:
    """Réplica exacta de D6 OLD, anterior a R2D.1."""
    stripped = alias.strip()
    root = department_root_buyer_target(stripped)
    if root and root in DEPARTAMENTOS:
        return False

    is_geo = any(key == stripped or stripped.endswith(key) for key in DEPARTAMENTOS)
    if not is_geo:
        return False

    first = stripped.split()[0] if stripped else ""
    if first in resolver._ENTITY_QUALIFIERS:
        return False

    position = full_text.find(stripped)
    if position > 0:
        words = full_text[:position].rstrip().split()
        if words:
            if words[-1] in resolver._ENTITY_QUALIFIERS:
                return False
            if words[-1] == "de" and len(words) >= 2 and words[-2] in resolver._ENTITY_QUALIFIERS:
                return False
    return True


def fetch_buyer_directory(app_token: str, tries: int = 8) -> list[dict[str, Any]]:
    """Descarga completa de compradores físicos registrados en SECOP II."""
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "SECOPPAL-R2D2-audit/1.0",
            "X-App-Token": app_token,
        }
    )
    result: list[dict[str, Any]] = []
    offset = 0
    page_size = 50_000
    while True:
        params = {
            "$select": (
                "codigo_entidad,nombre_entidad,nit_entidad,es_entidad,esta_activa,"
                "departamento,ciudad"
            ),
            "$where": "es_entidad='Si'",
            "$order": "codigo_entidad",
            "$limit": page_size,
            "$offset": offset,
        }
        last_error: str | None = None
        for attempt in range(1, tries + 1):
            try:
                response = session.get(BUYER_DIRECTORY_URL, params=params, timeout=90)
                payload = response.json()
                if response.status_code == 200 and isinstance(payload, list):
                    break
                last_error = f"HTTP {response.status_code}: {str(payload)[:160]}"
            except (requests.RequestException, ValueError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(2 * attempt)
        else:
            raise RuntimeError(f"buyer directory unavailable: {last_error}")

        result.extend(payload)
        if len(payload) < page_size:
            return result
        offset += page_size


def fetch_identity_by_nit(
    app_token: str, nit: str, tries: int = 8
) -> dict[str, list[dict[str, Any]]]:
    """Evidencia transaccional SECOP II exacta por NIT, sin parecido textual."""
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "SECOPPAL-R2D2-audit/1.0",
            "X-App-Token": app_token,
        }
    )
    check_digit = compute_check_digit(nit)
    variants = (nit, f"{nit}{check_digit}" if check_digit is not None else nit)
    quoted = ",".join(f"'{value}'" for value in dict.fromkeys(variants))
    evidence: dict[str, list[dict[str, Any]]] = {}
    for label, dataset, field in (
        ("processes", PROCESS_DATASET_ID, "entidad"),
        ("contracts", CONTRACT_DATASET_ID, "nombre_entidad"),
    ):
        params = {
            "$select": f"nit_entidad,{field},count(*) as rows",
            "$where": f"nit_entidad in ({quoted})",
            "$group": f"nit_entidad,{field}",
            "$limit": 5_000,
        }
        last_error: str | None = None
        for attempt in range(1, tries + 1):
            try:
                response = session.get(
                    f"https://www.datos.gov.co/resource/{dataset}.json",
                    params=params,
                    timeout=90,
                )
                payload = response.json()
                if response.status_code == 200 and isinstance(payload, list):
                    evidence[label] = payload
                    break
                last_error = f"HTTP {response.status_code}: {str(payload)[:160]}"
            except (requests.RequestException, ValueError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(2 * attempt)
        else:
            raise RuntimeError(f"{dataset} identity lookup unavailable: {last_error}")
    return evidence


def enrich_directory(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        normalized = catalog_norm(str(row.get("nombre_entidad") or ""))
        row["_normalized"] = normalized
        row["_tokens"] = tuple(normalized.split())
        row["_nit_base"] = nit_base(str(row.get("nit_entidad") or ""))


def derive_location_tokens(gazetteer: list[dict[str, Any]]) -> frozenset[str]:
    """Vocabulario conservador de lugares derivado de autoridades locales."""
    tokens: set[str] = set()
    for value in (*DEPARTAMENTOS, *DEPARTAMENTOS.values(), *GAZETTEER_CIUDADES):
        tokens.update(catalog_norm(value).split())

    government_heads = GOVERNMENT_HEADS | {"alcaldia", "gobernacion"}
    phrases = [str(record["official_name"]) for record in gazetteer]
    phrases.extend(str(alias) for record in gazetteer for alias in record.get("aliases", []))
    for phrase in phrases:
        words = catalog_norm(phrase).lower().split()
        for index, word in enumerate(words):
            if word not in government_heads:
                continue
            # Igual que R1C: sólo el primer token de contenido tras la
            # cabeza. Seguir consumiendo convertiría materias como
            # ``infraestructura`` o ``salud`` en falsos topónimos.
            candidate = next(
                (item for item in words[index + 1 :] if item not in LOCATION_WRAPPERS),
                None,
            )
            if candidate and candidate not in INSTITUTIONAL_HEADS and len(candidate) > 2:
                tokens.add(candidate.upper())
    return frozenset(tokens)


def institution_heads(value: str) -> list[str]:
    return sorted(set(catalog_norm(value).lower().split()) & INSTITUTIONAL_HEADS)


def is_location_only(alias: str, location_tokens: frozenset[str]) -> bool:
    if institution_heads(alias):
        return False
    words = catalog_norm(alias).split()
    content = [word for word in words if word.lower() not in LOCATION_WRAPPERS]
    return bool(content) and all(word in location_tokens for word in content)


def owner_directory_identities(
    owners: set[str], directory: list[dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in directory:
        by_name[str(row["_normalized"])].append(row)
    return {owner: by_name.get(catalog_norm(owner), []) for owner in sorted(owners)}


def compact_buyer(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "code": row.get("codigo_entidad"),
        "nit": row.get("nit_entidad"),
        "nit_base": row.get("_nit_base"),
        "physical_name": row.get("nombre_entidad"),
        "active": row.get("esta_activa") == "Si",
        "department": row.get("departamento"),
        "city": row.get("ciudad"),
    }


def distinct_buyers(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Una identidad por NIT base; sin NIT, el código físico manda."""
    chosen: dict[str, dict[str, Any]] = {}
    for row in rows:
        identity = str(row.get("_nit_base") or row.get("codigo_entidad") or row.get("_normalized"))
        incumbent = chosen.get(identity)
        if incumbent is None or (
            row.get("esta_activa") == "Si" and incumbent.get("esta_activa") != "Si"
        ):
            chosen[identity] = row
    return sorted(
        chosen.values(),
        key=lambda row: (
            row.get("esta_activa") != "Si",
            str(row.get("_normalized")),
            str(row.get("codigo_entidad") or ""),
        ),
    )


def classify_alias(
    *,
    alias: str,
    owners: list[str],
    other_buyers: list[dict[str, Any]],
    location_only: bool,
    heads: list[str],
) -> tuple[str, str]:
    """Devuelve (RISK_CLASS, RISK_LEVEL); señales no equivalen a veredicto."""
    token_count = len(catalog_norm(alias).split())
    if len(owners) != 1:
        return "REVIEW", "REVIEW"

    if other_buyers:
        owner_heads = set(institution_heads(owners[0]))
        foreign_heads = {
            head
            for buyer in other_buyers
            for head in institution_heads(str(buyer.get("nombre_entidad") or ""))
        }
        if location_only:
            risk_class = "GENERIC_LOCATION_ALIAS"
        elif owner_heads & GOVERNMENT_HEADS and foreign_heads - GOVERNMENT_HEADS:
            risk_class = "PARENT_ALIAS_IN_CHILD_NAME"
        elif set(heads) - GOVERNMENT_HEADS:
            risk_class = "CHILD_ALIAS_IN_UNRELATED_NAME"
        else:
            risk_class = "INSTITUTIONAL_FRAGMENT"
        risk_level = "HIGH" if token_count <= 3 and not heads else "MEDIUM"
        return risk_class, risk_level

    if location_only:
        return "GENERIC_LOCATION_ALIAS", "MEDIUM"
    return "SAFE_SPECIFIC_ALIAS", "SAFE"


def build_alias_records(
    resolver: EntityResolver,
    gazetteer: list[dict[str, Any]],
    directory: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    aliases = sorted(resolver.owners_by_alias)
    freed = [
        alias
        for alias in aliases
        if _old_reject(resolver, alias, alias) and not resolver._is_unqualified_dept(alias, alias)
    ]
    location_tokens = derive_location_tokens(gazetteer)
    all_owners = {owner for alias in freed for owner in resolver.owners_by_alias.get(alias, set())}
    directory_by_owner = owner_directory_identities(all_owners, directory)

    alias_by_tokens = {tuple(catalog_norm(alias).split()): alias for alias in freed}
    lengths = sorted({len(tokens) for tokens in alias_by_tokens})
    containing: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for buyer in directory:
        tokens = buyer["_tokens"]
        for length in lengths:
            if length > len(tokens):
                break
            for index in range(len(tokens) - length + 1):
                alias = alias_by_tokens.get(tokens[index : index + length])
                if alias:
                    containing[alias].append(buyer)

    records: list[dict[str, Any]] = []
    other_rows_by_alias: dict[str, list[dict[str, Any]]] = {}
    for alias in freed:
        owners = sorted(resolver.owners_by_alias.get(alias, set()))
        owner_names = {catalog_norm(owner) for owner in owners}
        owner_nits = {
            str(row["_nit_base"])
            for owner in owners
            for row in directory_by_owner.get(owner, [])
            if row.get("_nit_base")
        }
        candidates = [
            row
            for row in containing.get(alias, [])
            if row.get("_normalized") not in owner_names
            and not (row.get("_nit_base") and str(row["_nit_base"]) in owner_nits)
        ]
        other_buyers = distinct_buyers(candidates)
        other_rows_by_alias[alias] = other_buyers
        heads = institution_heads(alias)
        location_only = is_location_only(alias, location_tokens)
        risk_class, risk_level = classify_alias(
            alias=alias,
            owners=owners,
            other_buyers=other_buyers,
            location_only=location_only,
            heads=heads,
        )
        records.append(
            {
                "alias": alias,
                "current_owners": owners,
                "owner_count": len(owners),
                "owner_directory_identities": {
                    owner: [compact_buyer(row) for row in directory_by_owner.get(owner, [])]
                    for owner in owners
                },
                "token_count": len(catalog_norm(alias).split()),
                "institutional_heads": heads,
                "contains_institutional_head": bool(heads),
                "location_only": location_only,
                "other_physical_buyer_count": len(other_buyers),
                "other_active_physical_buyer_count": sum(
                    row.get("esta_activa") == "Si" for row in other_buyers
                ),
                "other_physical_buyers_containing_alias": [
                    compact_buyer(row) for row in other_buyers[:10]
                ],
                "risk_class": risk_class,
                "risk_level": risk_level,
            }
        )
    return records, other_rows_by_alias


def router_probe(router: QueryRouter, phrase: str) -> dict[str, Any]:
    params = router.parse(f"contratos de {phrase}").params
    return {
        "entity": params.get("entidad_resolved"),
        "department": params.get("departamento_resolved"),
        "method": (params.get("entidad_resolution") or {}).get("method"),
        "clarification": params.get("clarification_needed"),
    }


def scan_probe(resolver: EntityResolver, phrase: str) -> dict[str, Any] | None:
    result = resolver.scan_entity(normalize_text(f"contratos de {phrase}"))
    if result is None:
        return None
    return {
        "owner": result.official_name,
        "matched_alias": result.matched_alias,
        "method": result.method,
    }


def dual_rule_probe(
    resolver: EntityResolver,
    router: QueryRouter,
    phrase: str,
) -> dict[str, Any]:
    live_rule = EntityResolver._is_unqualified_dept
    live = router_probe(router, phrase)
    live_scan = scan_probe(resolver, phrase)
    try:
        EntityResolver._is_unqualified_dept = _old_reject
        old = router_probe(router, phrase)
        old_scan = scan_probe(resolver, phrase)
    finally:
        EntityResolver._is_unqualified_dept = live_rule
    return {
        "phrase": phrase,
        "query": f"contratos de {phrase}",
        "live": live,
        "live_scan": live_scan,
        "old": old,
        "old_scan": old_scan,
    }


def expected_directory_owner(
    gazetteer_owner: str,
    directory: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized = catalog_norm(gazetteer_owner)
    return [compact_buyer(row) for row in directory if row["_normalized"] == normalized]


def terminal_identities(directory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    matches = [
        row
        for row in directory
        if re.sub(r"\D", "", str(row.get("nit_entidad") or ""))
        in {TERMINAL_SANTA_MARTA_NIT, f"{TERMINAL_SANTA_MARTA_NIT}2"}
    ]
    return [compact_buyer(row) for row in distinct_buyers(matches)]


def build_canaries(
    resolver: EntityResolver,
    router: QueryRouter,
    directory: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    terminal = terminal_identities(directory)
    records = []
    for canary in CANARIES:
        probe = dual_rule_probe(resolver, router, str(canary["phrase"]))
        verified = (
            terminal
            if canary["id"] == "TERMINAL_SANTA_MARTA"
            else expected_directory_owner(str(canary["gazetteer_owner"]), directory)
        )
        verified_matches_gazetteer = any(
            catalog_norm(str(owner.get("physical_name") or ""))
            == catalog_norm(str(canary["gazetteer_owner"]))
            for owner in verified
        )
        if canary["id"] == "TERMINAL_SANTA_MARTA":
            live_entity = probe["live"]["entity"]
            if live_entity == canary["gazetteer_owner"]:
                verdict = "ALIAS_HIJACK"
            elif any(
                catalog_norm(str(owner.get("physical_name") or ""))
                == catalog_norm(str(live_entity or ""))
                for owner in terminal
            ):
                verdict = "REMEDIATED_SPECIFIC_IDENTITY"
            else:
                verdict = "FAIL_CLOSED"
        elif verified_matches_gazetteer and probe["live"]["entity"] == canary["gazetteer_owner"]:
            verdict = "VERIFIED_SECOP_OWNER"
        elif (
            verified_matches_gazetteer
            and (probe.get("live_scan") or {}).get("owner") == canary["gazetteer_owner"]
            and probe["live"]["entity"] is None
        ):
            verdict = "VERIFIED_OWNER_SCAN_ROUTER_FAIL_CLOSED"
        else:
            verdict = "REVIEW"
        records.append(
            {
                **canary,
                "verified_secop_owners": verified,
                "verified_secop_owner_matches_gazetteer": (verified_matches_gazetteer),
                "probe": probe,
                "verdict": verdict,
            }
        )
    return records


def choose_adversarial_rows(
    alias_records: list[dict[str, Any]],
    other_rows: dict[str, list[dict[str, Any]]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Todos los compradores activos por alias HIGH y uno por MEDIUM."""
    chosen = []
    for record in alias_records:
        if record["risk_level"] not in {"HIGH", "MEDIUM"}:
            continue
        candidates = [
            row
            for row in other_rows.get(str(record["alias"]), [])
            if row.get("esta_activa") == "Si"
        ]
        limit = len(candidates) if record["risk_level"] == "HIGH" else 1
        chosen.extend((record, row) for row in candidates[:limit])
    return chosen


def same_logical_identity(
    resolver: EntityResolver,
    resolved_owner: str,
    buyer: dict[str, Any],
    directory: list[dict[str, Any]],
) -> bool:
    """Prueba conservadora de que registro y gazetteer nombran al mismo comprador."""
    buyer_name = str(buyer.get("nombre_entidad") or "")
    buyer_norm = normalize_text(buyer_name)
    if buyer_norm == normalize_text(resolved_owner):
        return True
    if buyer_norm in resolver.aliases_by_official.get(resolved_owner, set()):
        return True

    buyer_tokens = tuple(catalog_norm(buyer_name).split())
    owner_tokens = set(catalog_norm(resolved_owner).split())
    ignorable_remainder = owner_tokens | {"SA", "LTDA", "ESP", "EICE"}
    for alias in resolver.aliases_by_official.get(resolved_owner, set()):
        alias_tokens = tuple(catalog_norm(alias).split())
        if len(alias_tokens) < 3 or not token_sequence(buyer_tokens, alias_tokens):
            continue
        start = next(
            index
            for index in range(len(buyer_tokens) - len(alias_tokens) + 1)
            if buyer_tokens[index : index + len(alias_tokens)] == alias_tokens
        )
        remainder = buyer_tokens[:start] + buyer_tokens[start + len(alias_tokens) :]
        if set(remainder) <= ignorable_remainder:
            return True

    buyer_nit = buyer.get("_nit_base")
    if buyer_nit:
        owner_norm = catalog_norm(resolved_owner)
        if any(
            row.get("_normalized") == owner_norm and row.get("_nit_base") == buyer_nit
            for row in directory
        ):
            return True

    # Corrige ruido registral obvio (``ES PECIAL``) sin usar parecido para
    # asignar un dueño: sólo evita declarar dos veces la misma razón social.
    return fuzz.ratio(catalog_norm(resolved_owner), catalog_norm(buyer_name)) >= 97


def adversarial_matrix(
    resolver: EntityResolver,
    router: QueryRouter,
    alias_records: list[dict[str, Any]],
    other_rows: dict[str, list[dict[str, Any]]],
    directory: list[dict[str, Any]],
    terminal_buyers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    freed = {str(record["alias"]) for record in alias_records}
    result = []
    for record, buyer in choose_adversarial_rows(alias_records, other_rows):
        phrase = str(buyer.get("nombre_entidad") or "")
        probe = dual_rule_probe(resolver, router, phrase)
        live_owner = probe["live"]["entity"]
        current_owners = set(record["current_owners"])
        scan_alias = (probe.get("live_scan") or {}).get("matched_alias")
        activated = scan_alias in freed and live_owner in current_owners
        if live_owner is None:
            outcome = "FAIL_CLOSED"
        elif same_logical_identity(resolver, str(live_owner), buyer, directory):
            outcome = "CORRECT_OWNER"
        else:
            outcome = "WRONG_OWNER"
        new_wrong = bool(
            outcome == "WRONG_OWNER" and activated and probe["old"]["entity"] != live_owner
        )
        result.append(
            {
                "alias": record["alias"],
                "current_owners": record["current_owners"],
                "risk_class": record["risk_class"],
                "expected_secop_buyer": compact_buyer(buyer),
                "probe": probe,
                "d6_activated_current_owner": activated,
                "new_wrong_owner_confirmed": new_wrong,
                "outcome": outcome,
            }
        )

    terminal_record = next(
        (
            record
            for record in alias_records
            if record["alias"] == "santa marta magdalena"
        ),
        {
            "alias": "santa marta magdalena",
            "current_owners": ["ALCALDÍA DISTRITAL DE SANTA MARTA"],
            "risk_class": "GENERIC_LOCATION_ALIAS",
        },
    )
    if terminal_buyers:
        probe = dual_rule_probe(
            resolver, router, "Terminal de Transportes de Santa Marta Magdalena"
        )
        live_owner = probe["live"]["entity"]
        scan_alias = (probe.get("live_scan") or {}).get("matched_alias")
        activated = bool(
            scan_alias in freed
            and live_owner in set(terminal_record["current_owners"])
        )
        if live_owner is None:
            outcome = "FAIL_CLOSED"
        elif same_logical_identity(
            resolver, str(live_owner), terminal_buyers[0], directory
        ):
            outcome = "CORRECT_OWNER"
        else:
            outcome = "WRONG_OWNER"
        result.append(
            {
                "alias": terminal_record["alias"],
                "current_owners": terminal_record["current_owners"],
                "risk_class": terminal_record["risk_class"],
                "expected_secop_buyer": terminal_buyers[0],
                "probe": probe,
                "d6_activated_current_owner": activated,
                "new_wrong_owner_confirmed": bool(
                    outcome == "WRONG_OWNER"
                    and activated
                    and probe["old"]["entity"] != live_owner
                ),
                "outcome": outcome,
                "canary": "TERMINAL_SANTA_MARTA",
            }
        )
    return result


def confirmed_wrong_owners(
    adversarial: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, tuple[str, ...]], dict[str, Any]] = {}
    for record in adversarial:
        if not record["new_wrong_owner_confirmed"]:
            continue
        key = (str(record["alias"]), tuple(record["current_owners"]))
        item = grouped.setdefault(
            key,
            {
                "alias": record["alias"],
                "current_owners": record["current_owners"],
                "risk_class": record["risk_class"],
                "verified_secop_buyers_hijacked": [],
                "examples": [],
                "root_causes": ["OVER_GENERIC_ALIAS", "SCAN_SUBSTRING_POLICY"],
            },
        )
        buyer = record["expected_secop_buyer"]
        identity = buyer.get("nit_base") or buyer.get("code") or buyer.get("physical_name")
        if identity not in {
            found.get("nit_base") or found.get("code") or found.get("physical_name")
            for found in item["verified_secop_buyers_hijacked"]
        }:
            item["verified_secop_buyers_hijacked"].append(buyer)
        item["examples"].append(
            {
                "query": record["probe"]["query"],
                "live_owner": record["probe"]["live"]["entity"],
                "old_owner": record["probe"]["old"]["entity"],
                "matched_freed_alias": (record["probe"].get("live_scan") or {}).get(
                    "matched_alias"
                ),
            }
        )
        if record.get("canary") == "TERMINAL_SANTA_MARTA":
            item["root_causes"].append("MISSING_SPECIFIC_IDENTITY")
    return sorted(grouped.values(), key=lambda item: str(item["alias"]))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()

    if args.env_file:
        load_dotenv(args.env_file, override=False)
    token = os.getenv("SECOP_APP_TOKEN")
    if not token:
        raise SystemExit("SECOP_APP_TOKEN is required; pass --env-file .env")

    gazetteer = json.loads(DB.read_text(encoding="utf-8"))
    r2d1 = json.loads(R2D1.read_text(encoding="utf-8"))
    resolver = EntityResolver(DB)
    router = QueryRouter(entity_resolver=resolver)
    directory = fetch_buyer_directory(token)
    enrich_directory(directory)
    terminal_transaction_evidence = fetch_identity_by_nit(token, TERMINAL_SANTA_MARTA_NIT)

    alias_records, other_rows = build_alias_records(resolver, gazetteer, directory)
    retired_freed_aliases = sorted(
        alias
        for alias in resolver.resolution_blocked_aliases
        if _old_reject(resolver, alias, alias)
        and not resolver._is_unqualified_dept(alias, alias)
    )
    canaries = build_canaries(resolver, router, directory)
    terminal_buyers = terminal_identities(directory)
    terminal_alias = next(
        (
            record
            for record in alias_records
            if record["alias"] == "santa marta magdalena"
        ),
        None,
    )
    if terminal_alias is not None:
        terminal_alias["risk_level"] = "HIGH"
        terminal_alias["adversarial_cross_owner_evidence"] = {
            "phrase": "Terminal de Transportes de Santa Marta Magdalena",
            "verified_secop_buyers": terminal_buyers,
        }
    adversarial = adversarial_matrix(
        resolver,
        router,
        alias_records,
        other_rows,
        directory,
        terminal_buyers,
    )
    confirmed = confirmed_wrong_owners(adversarial)
    terminal_probe = dual_rule_probe(
        resolver, router, "Terminal de Transportes de Santa Marta Magdalena"
    )
    terminal_live_owner = terminal_probe["live"]["entity"]
    terminal_scan_alias = (terminal_probe.get("live_scan") or {}).get("matched_alias")
    terminal_identity_present = any(
        catalog_norm(owner) == catalog_norm("CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA.")
        for owner in resolver.aliases_by_official
    )

    risk_counts = Counter(str(record["risk_level"]) for record in alias_records)
    class_counts = Counter(str(record["risk_class"]) for record in alias_records)
    outcomes = Counter(str(record["outcome"]) for record in adversarial)
    payload = {
        "audit": "FREED-ALIAS-SAFETY-R2D2",
        "type": "discovery/read-only",
        "as_of": date.today().isoformat(),
        "head_expected": "cafe9fc09237dd22a64dc643ba0491f9303dd6e6",
        "runtime_changed": False,
        "aliases_db_changed": False,
        "universe": {
            "total_aliases": len(resolver.owners_by_alias),
            "old_rejected": r2d1["BASELINE"]["REJECTED_ALIASES"],
            "b_prime_rejected": r2d1["B_PRIME_EVIDENCE"]["REJECTED_ALIASES"],
            "freed_aliases_expected": r2d1["B_PRIME_EVIDENCE"]["FREED_VS_CURRENT"],
            "freed_aliases_reconstructed": len(alias_records),
            "freed_aliases_retired_by_policy": retired_freed_aliases,
            "freed_aliases_reconstructed_plus_retired": (
                len(alias_records) + len(retired_freed_aliases)
            ),
        },
        "secop_buyer_directory": {
            "dataset_id": BUYER_DIRECTORY_ID,
            "url": BUYER_DIRECTORY_URL,
            "filter": "es_entidad='Si'",
            "physical_buyers": len(directory),
            "active_physical_buyers": sum(row.get("esta_activa") == "Si" for row in directory),
            "identity_key": "normalized NIT base; physical code when NIT is unusable",
            "containment": "complete normalized token sequence",
        },
        "risk": dict(sorted(risk_counts.items())),
        "risk_classes": dict(sorted(class_counts.items())),
        "aliases_with_other_physical_buyers": sum(
            bool(record["other_physical_buyer_count"]) for record in alias_records
        ),
        "alias_records": alias_records,
        "non_car_canaries": canaries,
        "santa_marta_terminal": {
            "query_phrase": "Terminal de Transportes de Santa Marta Magdalena",
            "secop_directory_identities": terminal_buyers,
            "secop_transaction_evidence": terminal_transaction_evidence,
            "institutional_identity": {
                "name": "CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA.",
                "nit": "891701694-2",
                "authority": "official 2024 management report",
                "url": (
                    "https://terminaldesantamarta.com/imagenes/documentos/"
                    "INFORME_DE_GESTI%C3%93N_2024_CTSM.pdf"
                ),
            },
            "gazetteer_specific_identity_present": terminal_identity_present,
            "current_owner": terminal_live_owner,
            "matched_alias": terminal_scan_alias,
            "alias_assessment": "GENERIC_LOCATION_ALIAS",
            "verdict": (
                "ALIAS_HIJACK"
                if terminal_live_owner == "ALCALDÍA DISTRITAL DE SANTA MARTA"
                else "REMEDIATED_SPECIFIC_IDENTITY"
                if terminal_identity_present
                else "FAIL_CLOSED"
            ),
        },
        "adversarial_sample": {
            "count": len(adversarial),
            "outcomes": dict(sorted(outcomes.items())),
            "records": adversarial,
        },
        "new_wrong_owner_confirmed": confirmed,
        "root_causes": sorted({cause for finding in confirmed for cause in finding["root_causes"]}),
        "r2d1_verdict": ("REMEDIATION_REQUIRED" if confirmed else "PASS_CONFIRMED"),
        "status": "PASS",
    }
    if payload["universe"]["freed_aliases_reconstructed_plus_retired"] != 2_104:
        raise RuntimeError(
            "FREED_BY_D6 reconstruction plus explicitly retired aliases "
            "drifted from 2,104"
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "FREED_ALIASES": len(alias_records),
                "BUYER_DIRECTORY": len(directory),
                "RISK": payload["risk"],
                "RISK_CLASSES": payload["risk_classes"],
                "ALIASES_WITH_OTHER_BUYERS": payload["aliases_with_other_physical_buyers"],
                "ADVERSARIAL": payload["adversarial_sample"]["outcomes"],
                "OUTPUT": str(args.output),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
