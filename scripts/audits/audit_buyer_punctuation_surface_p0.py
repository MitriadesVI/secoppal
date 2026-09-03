#!/usr/bin/env python3
"""Audita superficies fisicas puntuadas de compradores conocidos por SECOPPAL.

Discovery read-only: consulta el directorio de compradores, procesos y contratos;
no modifica aliases, normalizacion ni resolver. La identidad se acredita por NIT
declarado/proyectado o por nombre exacto y univoco en el directorio. Nunca usa
fuzzy para decidir quien es quien.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import string
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.entity_types import name_tokens  # noqa: E402
from app.core.scan_alias_policy import record_alias_policy  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

ALIASES_DB = ROOT / "app/data/aliases_db.json"
SOURCE_NAMES = ROOT / "app/data/buyer_source_names.json"
OUT = ROOT / "artifacts/audits/buyer_punctuation_surface_p0.json"

DIRECTORY = "4ex9-j3n8"
PROCESSES = "p6dx-8zbt"
CONTRACTS = "jbjy-vk9h"
RESOURCE = "https://www.datos.gov.co/resource/{}.json"
EXPECTED_HEAD = "0c4df013cbf52a71adfcea8b22e7f642e21dace2"
OBSERVED_AT = "2026-09-02"
RECENT_SINCE = (date.fromisoformat(OBSERVED_AT) - timedelta(days=365)).isoformat()

DATASETS = {
    "directory": (DIRECTORY, "nombre_entidad", None),
    "processes": (PROCESSES, "entidad", "fecha_de_publicacion_del"),
    "contracts": (CONTRACTS, "nombre_entidad", "fecha_de_firma"),
}

CANARIES = {
    "UNIVERSIDAD DEL PACIFICO//": {
        "owner": "UNIVERSIDAD DEL PACÍFICO",
        "expected": "EXACT_SAME_OWNER",
        "expected_scan_eligible": True,
        "must_be_reproduced": True,
    },
    "UNIVERSIDAD POPULAR DEL CESAR.": {
        "owner": "UNIVERSIDAD POPULAR DEL CESAR",
        "expected": "EXACT_SAME_OWNER",
        "expected_scan_eligible": True,
        "must_be_reproduced": True,
    },
    "UNIVERSIDA DE LA GUAJIRA": {
        "owner": "UNIVERSIDAD DE LA GUAJIRA",
        "expected": "EXACT_SAME_OWNER",
        "expected_scan_eligible": False,
        "must_be_reproduced": True,
    },
    "UNIVERSIDAD TECNOLOGICA DEL CHOCO.": {
        "owner": "UNIVERSIDAD TECNOLÓGICA DEL CHOCÓ DIEGO LUIS CÓRDOBA",
        "expected": None,
        "expected_scan_eligible": None,
        "must_be_reproduced": False,
    },
}


def digits(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


def nit_equivalent(left: str, right: str) -> bool:
    """Equivalencia acotada base/digito de verificacion, nunca fuzzy."""
    if left == right:
        return True
    shorter, longer = sorted((left, right), key=len)
    return len(shorter) >= 8 and len(longer) == len(shorter) + 1 and longer.startswith(shorter)


def punctuation_char(char: str) -> bool:
    return char in string.punctuation or unicodedata.category(char).startswith("P")


def contains_punctuation(value: str) -> bool:
    return any(punctuation_char(char) for char in value)


def punctuation_shape(value: str) -> dict[str, Any]:
    stripped = value.rstrip()
    positions = [index for index, char in enumerate(value) if punctuation_char(char)]
    repeated = any(
        punctuation_char(value[index]) and punctuation_char(value[index + 1])
        for index in range(len(value) - 1)
    )
    malformed = any(value.count(left) != value.count(right) for left, right in "() [] {}".split())
    trailing = bool(stripped and punctuation_char(stripped[-1]))
    internal = any(index < len(stripped) - 1 for index in positions)
    signals = []
    if trailing:
        signals.append("TRAILING_PUNCTUATION")
    if internal:
        signals.append("INTERNAL_PUNCTUATION")
    if repeated:
        signals.append("REPEATED_PUNCTUATION")
    if malformed:
        signals.append("TYPO_OR_MALFORMED")
    if repeated:
        primary = "REPEATED_PUNCTUATION"
    elif malformed:
        primary = "TYPO_OR_MALFORMED"
    elif trailing:
        primary = "TRAILING_PUNCTUATION"
    elif internal:
        primary = "INTERNAL_PUNCTUATION"
    else:
        primary = "OTHER"
    return {"primary": primary, "signals": signals or ["OTHER"]}


def current_key(value: str) -> str:
    return normalize_text(value)


def strip_trailing_key(value: str) -> str:
    key = normalize_text(value)
    while key and punctuation_char(key[-1]):
        key = key[:-1].rstrip()
    return key


def collapse_trailing_key(value: str) -> str:
    key = normalize_text(value)
    if not key:
        return key
    end = len(key)
    start = end
    while start and punctuation_char(key[start - 1]):
        start -= 1
    suffix = key[start:end]
    if len(suffix) < 2:
        return key
    collapsed = []
    for char in suffix:
        if not collapsed or collapsed[-1] != char:
            collapsed.append(char)
    return key[:start] + "".join(collapsed)


def strip_all_key(value: str) -> str:
    key = normalize_text(value)
    return " ".join(
        "".join(" " if punctuation_char(char) else char for char in key).split()
    )


STRATEGIES: dict[str, Callable[[str], str]] = {
    "CURRENT": current_key,
    "STRIP_TRAILING": strip_trailing_key,
    "COLLAPSE_TRAILING": collapse_trailing_key,
    "STRIP_ALL": strip_all_key,
}


class Socrata:
    def __init__(self, token: str | None) -> None:
        self.headers = {
            "Accept": "application/json",
            "User-Agent": "SECOPPAL-BUYER-PUNCTUATION-P0/1.0",
        }
        if token:
            self.headers["X-App-Token"] = token

    def get(self, dataset: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                response = requests.get(
                    RESOURCE.format(dataset),
                    params=params,
                    headers=self.headers,
                    timeout=180,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("Socrata response is not a list")
                return payload
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                time.sleep(attempt + 1)
        assert last_error is not None
        raise last_error

    def directory(self) -> list[dict[str, Any]]:
        return self.get(
            DIRECTORY,
            {
                "$select": "nombre_entidad,nit_entidad",
                "$where": "es_entidad='Si'",
                "$limit": 50000,
                "$order": ":id",
            },
        )


def alias_ownership(records: list[dict[str, Any]]) -> dict[str, set[str]]:
    owners: dict[str, set[str]] = defaultdict(set)
    for record in records:
        official = record["official_name"]
        owners[current_key(official)].add(official)
        for alias in record.get("aliases", []):
            owners[current_key(alias)].add(official)
    return owners


def authoritative_nits(
    records: list[dict[str, Any]], source_payload: dict[str, Any]
) -> tuple[dict[str, set[str]], dict[tuple[str, str], set[str]]]:
    by_owner: dict[str, set[str]] = defaultdict(set)
    evidence: dict[tuple[str, str], set[str]] = defaultdict(set)
    for record in records:
        nit = digits(record.get("nit"))
        if nit:
            owner = record["official_name"]
            by_owner[owner].add(nit)
            evidence[(owner, nit)].add("aliases_db.nit")
    for binding in source_payload["bindings"]:
        identity = binding["identity_key"]
        if identity["type"] != "NIT":
            continue
        owner = binding["canonical_name"]
        nit = digits(identity["value"])
        by_owner[owner].add(nit)
        evidence[(owner, nit)].add("buyer_source_names.identity_key")
    return by_owner, evidence


def accredited_owner_nits(
    records: list[dict[str, Any]],
    directory: list[dict[str, Any]],
    ownership: dict[str, set[str]],
    source_payload: dict[str, Any],
) -> tuple[
    dict[str, set[str]],
    dict[tuple[str, str], set[str]],
    dict[tuple[str, str], str],
    list[dict[str, Any]],
]:
    by_owner, evidence = authoritative_nits(records, source_payload)
    declared = {owner: set(nits) for owner, nits in by_owner.items()}
    canonical_nit = {
        (owner, nit): nit for owner, nits in by_owner.items() for nit in nits
    }
    authoritative_owners = set(by_owner)
    rejected = []
    for row in directory:
        surface = str(row.get("nombre_entidad") or "")
        nit = digits(row.get("nit_entidad"))
        owners = ownership.get(current_key(surface), set())
        if len(owners) != 1 or not nit:
            continue
        owner = next(iter(owners))
        if owner in authoritative_owners:
            equivalents = [
                known for known in declared[owner] if nit_equivalent(nit, known)
            ]
            if not equivalents:
                rejected.append(
                    {
                        "canonical_name": owner,
                        "foreign_nit": nit,
                        "directory_name": surface,
                        "reason": "DECLARED_NIT_HAS_PRECEDENCE_OVER_REGISTRY_HOMONYM",
                    }
                )
                continue
            logical_nit = min(equivalents, key=len)
            by_owner[owner].add(nit)
            canonical_nit[(owner, nit)] = logical_nit
            evidence[(owner, logical_nit)].add(
                "directory.exact_name+nIT_base_check_digit_equivalence"
            )
            continue
        by_owner[owner].add(nit)
        canonical_nit[(owner, nit)] = nit
        evidence[(owner, nit)].add("directory.exact_unique_name")
    return by_owner, evidence, canonical_nit, rejected


def batches(values: list[str], size: int = 40) -> list[list[str]]:
    return [values[start : start + size] for start in range(0, len(values), size)]


def activity_queries(
    client: Socrata, nits: list[str]
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:
    jobs: list[tuple[str, str, dict[str, Any], str]] = []
    for batch in batches(nits):
        quoted = ",".join(f'"{nit}"' for nit in batch)
        for label, (dataset, name_field, date_field) in DATASETS.items():
            if label == "directory":
                continue
            jobs.append(
                (
                    label,
                    dataset,
                    {
                        "$select": f"nit_entidad,{name_field},count(*) as rows",
                        "$where": f"nit_entidad in ({quoted})",
                        "$group": f"nit_entidad,{name_field}",
                        "$limit": 50000,
                    },
                    name_field,
                )
            )
            jobs.append(
                (
                    f"recent_{label}",
                    dataset,
                    {
                        "$select": "nit_entidad,count(*) as rows",
                        "$where": (
                            f"nit_entidad in ({quoted}) and "
                            f'{date_field} >= "{RECENT_SINCE}T00:00:00.000"'
                        ),
                        "$group": "nit_entidad",
                        "$limit": 50000,
                    },
                    "",
                )
            )

    groups: list[dict[str, Any]] = []
    counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "processes": 0,
            "contracts": 0,
            "recent_processes": 0,
            "recent_contracts": 0,
        }
    )
    with ThreadPoolExecutor(max_workers=6) as pool:
        pending = {
            pool.submit(client.get, dataset, params): (label, name_field)
            for label, dataset, params, name_field in jobs
        }
        for future in as_completed(pending):
            label, name_field = pending[future]
            for row in future.result():
                nit = digits(row.get("nit_entidad"))
                amount = int(row.get("rows", 0))
                if label.startswith("recent_"):
                    counts[nit][label] += amount
                else:
                    counts[nit][label] += amount
                    groups.append(
                        {
                            "source": label,
                            "nit": nit,
                            "name": str(row.get(name_field) or ""),
                            "rows": amount,
                        }
                    )
    return groups, dict(counts)


def count_foreign_activity(
    client: Socrata, foreign_nits: list[str]
) -> dict[str, dict[str, int]]:
    if not foreign_nits:
        return {}
    counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"processes": 0, "contracts": 0}
    )
    jobs = []
    for batch in batches(foreign_nits):
        quoted = ",".join(f'"{nit}"' for nit in batch)
        for label, (dataset, _field, _date) in DATASETS.items():
            if label == "directory":
                continue
            jobs.append(
                (
                    label,
                    dataset,
                    {
                        "$select": "nit_entidad,count(*) as rows",
                        "$where": f"nit_entidad in ({quoted})",
                        "$group": "nit_entidad",
                        "$limit": 50000,
                    },
                )
            )
    with ThreadPoolExecutor(max_workers=6) as pool:
        pending = {
            pool.submit(client.get, dataset, params): label
            for label, dataset, params in jobs
        }
        for future in as_completed(pending):
            label = pending[future]
            for row in future.result():
                counts[digits(row.get("nit_entidad"))][label] += int(
                    row.get("rows", 0)
                )
    return dict(counts)


def attribute_owner(
    nit: str,
    surface: str,
    owners_by_nit: dict[str, set[str]],
    ownership: dict[str, set[str]],
) -> str | None:
    candidates = owners_by_nit.get(nit, set())
    if len(candidates) == 1:
        return next(iter(candidates))
    exact = candidates & ownership.get(current_key(surface), set())
    return next(iter(exact)) if len(exact) == 1 else None


def resolution_classification(result: Any, expected_owner: str) -> str:
    if result.value == expected_owner and result.method == "exact":
        return "EXACT_SAME_OWNER"
    if result.value == expected_owner and str(result.method).startswith("fuzzy"):
        return "FUZZY_SAME_OWNER"
    if result.value is not None:
        return "WRONG_OWNER"
    return "FAIL_CLOSED"


def token_key(value: str) -> tuple[str, ...]:
    return tuple(name_tokens(value))


def strategy_index(
    records: list[dict[str, Any]], transform: Callable[[str], str]
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    owners: dict[str, set[str]] = defaultdict(set)
    current_inputs: dict[str, set[str]] = defaultdict(set)
    for record in records:
        owner = record["official_name"]
        for value in [owner, *record.get("aliases", [])]:
            key = transform(value)
            if not key:
                continue
            owners[key].add(owner)
            current_inputs[key].add(current_key(value))
    return owners, current_inputs


def normalization_simulation(
    records: list[dict[str, Any]], affected: list[dict[str, Any]]
) -> dict[str, Any]:
    output = {}
    for label, transform in STRATEGIES.items():
        owners, current_inputs = strategy_index(records, transform)
        new_multi = [
            {
                "key": key,
                "owners": sorted(key_owners),
                "merged_current_keys": sorted(current_inputs[key]),
            }
            for key, key_owners in owners.items()
            if len(key_owners) > 1 and len(current_inputs[key]) > 1
        ]
        gains = []
        wrong_risks = []
        for case in affected:
            candidates = owners.get(transform(case["physical_name"]), set())
            if candidates == {case["canonical_name"]}:
                gains.append(case["surface_id"])
            elif candidates and candidates != {case["canonical_name"]}:
                wrong_risks.append(
                    {
                        "surface_id": case["surface_id"],
                        "expected_owner": case["canonical_name"],
                        "candidate_owners": sorted(candidates),
                    }
                )
        output[label] = {
            "exact_gain": len(gains) if label != "CURRENT" else 0,
            "exact_gain_surface_ids": gains if label != "CURRENT" else [],
            "new_collisions": len(new_multi) if label != "CURRENT" else 0,
            "new_multi_owner_keys": len(new_multi) if label != "CURRENT" else 0,
            "new_multi_owner_key_details": new_multi if label != "CURRENT" else [],
            "wrong_owner_risks": len(wrong_risks),
            "wrong_owner_risk_details": wrong_risks,
        }
    return output


def recommendation_for(case: dict[str, Any]) -> str:
    if case["current_classification"] == "EXACT_SAME_OWNER":
        return "ALREADY_COVERED"
    if case["current_classification"] == "WRONG_OWNER":
        return "DO_NOT_ADD"
    if case["alias_exact_candidate_foreign_owners"]:
        return "DO_NOT_ADD"
    if case["active_foreign_same_token_nits"] or case["other_token_owners"]:
        return "NEEDS_REVIEW"
    if (
        case["punctuation"]["primary"] == "TYPO_OR_MALFORMED"
        or set(case["sources"]) == {"directory"}
    ):
        return "SAFE_EXACT_ONLY"
    return "SAFE_EXACT_AND_SCAN"


def decision_for(
    affected_identities: int, total_identities: int, simulation: dict[str, Any]
) -> tuple[str, str]:
    if affected_identities <= 20 or affected_identities <= total_identities * 0.03:
        return "CURATED_ALIAS_FIX", "At most 20 identities or 3% of checked identities."
    safe = [
        simulation[name]["new_collisions"] == 0
        and simulation[name]["wrong_owner_risks"] == 0
        and simulation[name]["exact_gain"] > 0
        for name in ("STRIP_TRAILING", "COLLAPSE_TRAILING")
    ]
    if any(safe):
        return (
            "DEDICATED_SURFACE_NORMALIZER_DISCOVERY",
            "Affected volume is material and B or C gains exact matches without new risk.",
        )
    return (
        "KEEP_EXPLICIT_ALIASES",
        "Normalization simulation introduces collisions/wrong-owner risk or no safe gain.",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--token", default=None)
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    records = json.loads(ALIASES_DB.read_text(encoding="utf-8"))
    source_payload = json.loads(SOURCE_NAMES.read_text(encoding="utf-8"))
    resolver = EntityResolver(ALIASES_DB)
    ownership = alias_ownership(records)
    client = Socrata(args.token or os.getenv("SOCRATA_APP_TOKEN"))

    directory = client.directory()
    owner_nits, nit_evidence, canonical_nit, rejected_homonyms = (
        accredited_owner_nits(records, directory, ownership, source_payload)
    )
    owners_by_nit: dict[str, set[str]] = defaultdict(set)
    for owner, nits in owner_nits.items():
        for nit in nits:
            owners_by_nit[nit].add(owner)
    known_nits = sorted(owners_by_nit)
    activity_groups, activity = activity_queries(client, known_nits)
    identity_activity: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {
            "processes": 0,
            "contracts": 0,
            "recent_processes": 0,
            "recent_contracts": 0,
        }
    )
    for owner, raw_nits in owner_nits.items():
        for raw_nit in raw_nits:
            logical_nit = canonical_nit[(owner, raw_nit)]
            for metric, amount in activity.get(raw_nit, {}).items():
                identity_activity[(owner, logical_nit)][metric] += amount

    surfaces: dict[tuple[str, str, str], dict[str, Any]] = {}
    unattributed = Counter()

    def add_surface(source: str, nit: str, name: str, rows: int) -> None:
        owner = attribute_owner(nit, name, owners_by_nit, ownership)
        if owner is None:
            unattributed[source] += 1
            return
        logical_nit = canonical_nit[(owner, nit)]
        key = (owner, logical_nit, name)
        item = surfaces.setdefault(
            key,
            {
                "canonical_name": owner,
                "nit": logical_nit,
                "physical_name": name,
                "sources": {},
            },
        )
        item["sources"][source] = item["sources"].get(source, 0) + rows

    for row in directory:
        add_surface(
            "directory",
            digits(row.get("nit_entidad")),
            str(row.get("nombre_entidad") or ""),
            1,
        )
    for row in activity_groups:
        add_surface(row["source"], row["nit"], row["name"], row["rows"])

    punctuated = [item for item in surfaces.values() if contains_punctuation(item["physical_name"])]
    token_owners: dict[tuple[str, ...], set[str]] = defaultdict(set)
    for record in records:
        owner = record["official_name"]
        for value in [owner, *record.get("aliases", [])]:
            if token_key(value):
                token_owners[token_key(value)].add(owner)
    directory_by_tokens: dict[tuple[str, ...], list[dict[str, str]]] = defaultdict(list)
    for row in directory:
        tokens = token_key(str(row.get("nombre_entidad") or ""))
        if tokens:
            directory_by_tokens[tokens].append(
                {
                    "nit": digits(row.get("nit_entidad")),
                    "name": str(row.get("nombre_entidad") or ""),
                }
            )

    foreign_nits = sorted(
        {
            row["nit"]
            for item in punctuated
            for row in directory_by_tokens.get(token_key(item["physical_name"]), [])
            if row["nit"] and not nit_equivalent(row["nit"], item["nit"])
        }
    )
    foreign_activity = count_foreign_activity(client, foreign_nits)

    cases = []
    for item in punctuated:
        surface = item["physical_name"]
        owner = item["canonical_name"]
        result = resolver.resolve_entidad(surface)
        classification = resolution_classification(result, owner)
        exact_candidates = ownership.get(current_key(surface), set())
        other_owners = token_owners.get(token_key(surface), set()) - {owner}
        foreign_rows = [
            row
            for row in directory_by_tokens.get(token_key(surface), [])
            if not nit_equivalent(row["nit"], item["nit"])
        ]
        active_foreign = sorted(
            {
                row["nit"]
                for row in foreign_rows
                if sum(foreign_activity.get(row["nit"], {}).values()) > 0
            }
        )
        process_rows = item["sources"].get("processes", 0)
        contract_rows = item["sources"].get("contracts", 0)
        identity_counts = identity_activity[(owner, item["nit"])]
        surface_id = f"{owner}|{item['nit']}|{surface}"
        case = {
            "surface_id": surface_id,
            **item,
            "punctuation": punctuation_shape(surface),
            "current_classification": classification,
            "current_resolution": result.value,
            "current_method": result.method,
            "alias_exact_candidates": sorted(exact_candidates),
            "alias_exact_candidate_foreign_owners": sorted(exact_candidates - {owner}),
            "scan_owner": (
                scan.official_name
                if (scan := resolver.scan_entity(current_key(surface)))
                else None
            ),
            "other_token_owners": sorted(other_owners),
            "same_token_foreign_directory_rows": foreign_rows,
            "active_foreign_same_token_nits": active_foreign,
            "activity": {
                "processes": process_rows,
                "contracts": contract_rows,
                "recent_processes": identity_counts["recent_processes"],
                "recent_contracts": identity_counts["recent_contracts"],
                "total": process_rows + contract_rows,
            },
            "nit_evidence": sorted(nit_evidence[(owner, item["nit"])]),
        }
        case["recommendation"] = recommendation_for(case)
        cases.append(case)

    cases.sort(key=lambda row: (row["canonical_name"], row["nit"], row["physical_name"]))
    affected = [row for row in cases if row["current_classification"] != "EXACT_SAME_OWNER"]
    affected_pairs = {(row["canonical_name"], row["nit"]) for row in affected}
    affected_nits = {row["nit"] for row in affected}
    logical_owners_by_nit: dict[str, set[str]] = defaultdict(set)
    for owner, raw_nits in owner_nits.items():
        for raw_nit in raw_nits:
            logical_owners_by_nit[canonical_nit[(owner, raw_nit)]].add(owner)
    current_counts = Counter(row["current_classification"] for row in cases)
    recommendation_counts = Counter(row["recommendation"] for row in cases)
    category_counts = Counter(row["punctuation"]["primary"] for row in cases)
    simulation = normalization_simulation(records, affected)
    decision, decision_reason = decision_for(len(affected_nits), len(records), simulation)

    activity_by_nit: dict[str, dict[str, Any]] = {}
    for owner, nit in affected_pairs:
        counts = identity_activity[(owner, nit)]
        item = activity_by_nit.setdefault(
            nit,
            {
                "nit": nit,
                "canonical_names": set(),
                "processes": 0,
                "contracts": 0,
                "recent_processes": 0,
                "recent_contracts": 0,
                "affected_surfaces": 0,
            },
        )
        item["canonical_names"].add(owner)
        # Un NIT compartido se mide una sola vez; no se suma por canonical.
        for metric in (
            "processes",
            "contracts",
            "recent_processes",
            "recent_contracts",
        ):
            item[metric] = max(item[metric], counts[metric])
        item["affected_surfaces"] += sum(
            row["canonical_name"] == owner and row["nit"] == nit for row in affected
        )
    activity_rows = []
    for item in activity_by_nit.values():
        names = sorted(item.pop("canonical_names"))
        item["canonical_names"] = names
        item["canonical_name"] = names[0] if len(names) == 1 else None
        all_known_names = sorted(logical_owners_by_nit[item["nit"]])
        item["known_canonical_names_on_nit"] = all_known_names
        item["activity_scope"] = (
            "SHARED_NIT_UPPER_BOUND"
            if len(all_known_names) > 1
            else "SINGLE_KNOWN_IDENTITY_NIT"
        )
        item["total"] = item["processes"] + item["contracts"]
        item["recent_total"] = item["recent_processes"] + item["recent_contracts"]
        activity_rows.append(item)
    top_50 = sorted(
        activity_rows,
        key=lambda row: (-row["total"], -row["recent_total"], row["nit"]),
    )[:50]

    canaries = []
    for surface, oracle in CANARIES.items():
        matching = [
            row
            for row in surfaces.values()
            if row["physical_name"] == surface
            and row["canonical_name"] == oracle["owner"]
        ]
        reproduced = bool(matching)
        observed_sources = sorted(
            {source for row in matching for source in row["sources"]}
        )
        result = resolver.resolve_entidad(surface) if reproduced else None
        classification = (
            resolution_classification(result, oracle["owner"]) if result else None
        )
        scan = resolver.scan_entity(current_key(surface)) if reproduced else None
        scan_eligible = bool(scan and scan.official_name == oracle["owner"])
        passed = reproduced == oracle["must_be_reproduced"]
        if oracle["expected"] is not None:
            passed = passed and classification == oracle["expected"]
        if oracle["expected_scan_eligible"] is not None:
            passed = passed and scan_eligible == oracle["expected_scan_eligible"]
        canaries.append(
            {
                "physical_name": surface,
                **oracle,
                "reproduced": reproduced,
                "observed_sources": observed_sources,
                "current_classification": classification,
                "current_method": result.method if result else None,
                "scan_eligible": scan_eligible,
                "pass": passed,
            }
        )

    scan_disabled = set()
    for record in records:
        disabled, _blocked = record_alias_policy(record)
        scan_disabled.update((record["official_name"], alias) for alias in disabled)

    payload = {
        "audit": "BUYER_PUNCTUATION_SURFACE_P0",
        "status": "PASS" if all(row["pass"] for row in canaries) else "PARTIAL",
        "type": "DISCOVERY_READ_ONLY",
        "as_of": OBSERVED_AT,
        "recent_since": RECENT_SINCE,
        "expected_head": EXPECTED_HEAD,
        "authorities": {
            "identity": "declared/projected NIT, else exact unique directory name",
            "identity_uses_fuzzy": False,
            "alias_policy": "ADR-072",
            "source_projection": "ADR-073 (read as evidence only; not confused with input resolution)",
            "buyer_directory": DIRECTORY,
            "processes": PROCESSES,
            "contracts": CONTRACTS,
        },
        "methodology": {
            "known_canonical_identities_checked": len(records),
            "canonical_identities_with_accredited_nit": len(owner_nits),
            "logical_owner_nit_pairs_accredited": len(
                {
                    (owner, canonical_nit[(owner, nit)])
                    for owner, nits in owner_nits.items()
                    for nit in nits
                }
            ),
            "directory_rows_scanned": len(directory),
            "physical_surfaces_checked": len(surfaces),
            "unattributed_source_groups": dict(sorted(unattributed.items())),
            "rejected_registry_homonyms": rejected_homonyms,
            "dedup_key": "canonical_name + accredited NIT",
            "normalization_is_simulated_only": True,
        },
        "summary": {
            "physical_surfaces_checked": len(surfaces),
            "logical_identities_checked": len(records),
            "logical_owner_nit_pairs_accredited": len(
                {
                    (owner, canonical_nit[(owner, nit)])
                    for owner, nits in owner_nits.items()
                    for nit in nits
                }
            ),
            "punctuated_surfaces": len(cases),
            "affected_identities": len(affected_nits),
            "affected_physical_surfaces": len(affected),
            "current": {
                key: current_counts[key]
                for key in (
                    "EXACT_SAME_OWNER",
                    "FUZZY_SAME_OWNER",
                    "WRONG_OWNER",
                    "FAIL_CLOSED",
                )
            },
            "punctuation_categories": dict(sorted(category_counts.items())),
            "recommendations": {
                key: recommendation_counts[key]
                for key in (
                    "SAFE_EXACT_AND_SCAN",
                    "SAFE_EXACT_ONLY",
                    "ALREADY_COVERED",
                    "NEEDS_REVIEW",
                    "DO_NOT_ADD",
                )
            },
        },
        "normalization_simulation": simulation,
        "decision": decision,
        "decision_reason": decision_reason,
        "r2fb_canaries": canaries,
        "top_50_affected_by_activity": top_50,
        "punctuated_surface_cases": cases,
        "affected_surface_ids": [row["surface_id"] for row in affected],
        "scan_disabled_aliases_observed": [
            {"canonical_name": owner, "alias": alias}
            for owner, alias in sorted(scan_disabled)
        ],
        "runtime_changed": False,
        "aliases_changed": False,
        "recommended_replay": (
            ".venv/bin/python scripts/audits/audit_buyer_punctuation_surface_p0.py"
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    print(json.dumps({"decision": decision, "canaries": canaries}, ensure_ascii=False, indent=2))
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
