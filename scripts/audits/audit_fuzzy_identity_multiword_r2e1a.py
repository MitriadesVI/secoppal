#!/usr/bin/env python3
"""Auditor R2E.1A: territorios de más de una palabra en la puerta del fuzzy.

R2E.1 comparaba el territorio pedido token a token contra un conjunto plano de
claves del catálogo. El catálogo, sin embargo, está indexado por FRASE: `norte
de santander`, `san andres y providencia`, `puerto carreño`, `santa marta`,
`distrito capital de bogota`. Este auditor mide la discordancia:

* clasifica las claves de ``DEPARTAMENTOS`` y ``GAZETTEER_CIUDADES``;
* corre un banco adversarial donde el candidato pierde el territorio pedido;
* barre TODA la superficie real del gazetteer —cada nombre oficial y cada alias
  como consulta, contra los tres primeros candidatos del fuzzy— y cuenta los
  veredictos que cambian frente a la regla de R2E.1, que es como se mide un
  falso rechazo;
* congela las tres superficies de nombre del NIT 899999083 (registro, procesos,
  contratos) para clasificar la deuda del DAPRE sin inferir cronología.

``--baseline`` evalúa con la regla token-a-token de R2E.1 sobre el código vivo.
Lee el gazetteer y el resolver; no los modifica.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz, process

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.entity_resolver import (  # noqa: E402
    GAZETTEER_CIUDADES,
    TERRITORY_PHRASES,
    EntityResolver,
)
from app.core.entity_types import name_tokens  # noqa: E402
from app.core.fuzzy_identity_guard import (  # noqa: E402
    FILLER_TOKENS,
    candidate_keeps_requested_identity,
    requested_identity_tokens,
)
from app.data.departamentos import DEPARTAMENTOS  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

DB = ROOT / "app/data/aliases_db.json"
OUT = ROOT / "artifacts/audits/fuzzy_identity_multiword_r2e1a.json"

BUYER_DIRECTORY_ID = "4ex9-j3n8"
PROCESS_DATASET_ID = "p6dx-8zbt"
CONTRACT_DATASET_ID = "jbjy-vk9h"
RESOURCE = "https://www.datos.gov.co/resource/{}.json"

DAPRE_NIT = "899999083"
DAPRE_GAZETTEER_NAME = "DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA"

# Regla de R2E.1: el territorio se comparaba token a token.
R2E1_TERRITORY_TOKENS = frozenset(DEPARTAMENTOS) | frozenset(GAZETTEER_CIUDADES)

# Banco adversarial: el candidato pierde el territorio que la frase nombra.
# ``expected`` es el veredicto correcto de la puerta, no del resolver.
MULTIWORD_BANK = (
    ("norte de santander vs santander", "contraloria departamental de norte de santander",
     "contraloria general de santander", "CONTRALORIA GENERAL DE SANTANDER", False),
    ("san andres", "contraloria de san andres",
     "contraloria", "CGR-NIVEL CENTRAL", False),
    ("puerto carreño", "contraloria de puerto carreno",
     "contraloria", "CGR-NIVEL CENTRAL", False),
    ("santa marta vs magdalena", "contraloria de santa marta",
     "contraloria general del magdalena", "CONTRALORIA GENERAL DEL DEPARTAMENTO DEL MAGDALENA", False),
    ("valle del cauca vs cauca", "contraloria departamental del valle del cauca",
     "contraloria del cauca", "CONTRALORIA GENERAL DEL CAUCA", False),
    ("santander vs norte de santander", "contraloria de santander",
     "gobernacion norte de santander", "Gobernación Norte de Santander*", False),
    ("distrito capital vs otro dueño", "contraloria del distrito capital de bogota",
     "bogota distrito capital", "UNIVERSIDAD PEDAGÓGICA NACIONAL", False),
    ("san andres providencia", "contraloria de san andres y providencia",
     "contraloria general", "CGR-NIVEL CENTRAL", False),
    # Cabeza distinta: los rechaza la regla de cabeza de R2E.1, no la
    # territorial.  Se conservan en el banco porque su veredicto no puede
    # cambiar al tocar el territorio.
    ("head mismatch gobernacion", "contraloria de norte de santander",
     "gobernacion norte de santander", "Gobernación Norte de Santander*", False),
    ("head mismatch valle", "contraloria del valle del cauca",
     "el valle", "GOBERNACION DEL VALLE DEL CAUCA - SECRETARIA DE EDUCACION", False),
    # Canarios positivos: el candidato SÍ conserva el territorio pedido.
    ("keep norte de santander", "gobernacion de norte de santander",
     "gobernacion norte de santander", "Gobernación Norte de Santander*", True),
    ("keep san andres", "gobernacion de san andres",
     "san andres", "GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA", True),
    ("keep valle abreviado", "gobernacion del valle del cauca",
     "el valle", "GOBERNACION DEL VALLE DEL CAUCA - SECRETARIA DE EDUCACION", True),
    ("keep bogota por distrito capital", "contraloria del distrito capital de bogota",
     "contraloria del distrito capital", "CONTRALORIA DE BOGOTA", True),
    ("keep bogota simple", "contraloria del distrito capital",
     "contraloria de bogota", "CONTRALORIA DE BOGOTA", True),
    ("keep santa marta", "contraloria de santa marta",
     "contraloria de santa marta", "CONTRALORIA DISTRITAL DE SANTA MARTA", True),
    ("keep sin territorio", "contraloria general de la republica de colombia",
     "la contraloria general de la republica", "CGR-NIVEL CENTRAL", True),
)

WRAPPERS = frozenset({"el", "la", "los", "las", "de", "del", "en", "al", "y", "e"})
PREFIXES = frozenset({"depto", "departamento", "dpto"})


def guard_r2e1(query: str, alias: str, official: str) -> bool:
    """La puerta tal como quedó en R2E.1, para medir el delta."""
    requested = requested_identity_tokens(query)
    if not requested:
        return True
    evidence = set(name_tokens(alias)) | set(name_tokens(official))
    if requested[0] not in evidence:
        return False
    return all(t in evidence for t in requested if t in R2E1_TERRITORY_TOKENS)


def guard_live(query: str, alias: str, official: str) -> bool:
    return candidate_keeps_requested_identity(query, alias, official, TERRITORY_PHRASES)


def classify_catalog() -> dict[str, Any]:
    def classify(key: str) -> str:
        tokens = name_tokens(key)
        if len(tokens) == 1:
            return "SINGLE_TOKEN"
        core = [t for t in tokens if t not in WRAPPERS and t not in PREFIXES]
        if tokens[0] in WRAPPERS or tokens[0] in PREFIXES:
            return "WRAPPER_FORM" if len(core) == 1 else "WRAPPER_MULTIWORD"
        return "MULTIWORD"

    def core_phrase(key: str) -> tuple[str, ...]:
        return tuple(t for t in name_tokens(key) if t not in WRAPPERS and t not in PREFIXES)

    out: dict[str, Any] = {"catalogs": {}, "multiword_cores": {}, "proxy_keys": []}
    cores: dict[tuple[str, ...], set[str]] = {}
    for label, catalog in (("DEPARTAMENTOS", DEPARTAMENTOS), ("GAZETTEER_CIUDADES", GAZETTEER_CIUDADES)):
        counts: dict[str, int] = {}
        for key, canonical in catalog.items():
            counts[classify(key)] = counts.get(classify(key), 0) + 1
            core = core_phrase(key)
            if len(core) > 1:
                cores.setdefault(core, set()).add(canonical)
            if not set(name_tokens(key)) & set(name_tokens(canonical)):
                out["proxy_keys"].append({"key": key, "canonical": canonical})
        out["catalogs"][label] = {"keys": len(catalog), "classes": counts}

    for core, canonicals in sorted(cores.items()):
        invisible = [t for t in core if t not in R2E1_TERRITORY_TOKENS]
        out["multiword_cores"][" ".join(core)] = {
            "canonicals": sorted(canonicals),
            "tokens_invisible_to_r2e1_rule": invisible,
            "fully_invisible": len(invisible) == len(core),
        }
    return out


def run_bank(guard) -> list[dict[str, Any]]:
    rows = []
    for label, query, alias, official, expected in MULTIWORD_BANK:
        verdict = guard(query, alias, official)
        rows.append({
            "case": label,
            "query": query,
            "alias": alias,
            "official": official,
            "expected": expected,
            "verdict": verdict,
            "correct": verdict == expected,
        })
    return rows


def sweep_real_surface(resolver: EntityResolver, records: list[dict]) -> dict[str, Any]:
    """Cada nombre oficial y cada alias como consulta, contra el top-3 del fuzzy."""
    queries = set()
    for record in records:
        queries.add(record["official_name"])
        queries.update(record["aliases"])
    choices = list(resolver.choice_to_official.keys())

    evaluated = 0
    changed: list[dict[str, Any]] = []
    for query in sorted(queries):
        normalized = normalize_text(query)
        if normalized in resolver.alias_index:
            continue  # Tier 1: la puerta no interviene
        for match, score, _ in process.extract(
            normalized, choices, scorer=fuzz.WRatio, limit=3
        ):
            if score < resolver.FUZZY_CUTOFF_ENTIDAD:
                break
            official = resolver.choice_to_official[match]
            evaluated += 1
            before = guard_r2e1(normalized, match, official)
            after = guard_live(normalized, match, official)
            if before != after:
                changed.append({
                    "query": query, "alias": match, "official": official,
                    "score": score, "r2e1": before, "live": after,
                })
    return {
        "queries": len(queries),
        "triples_evaluated": evaluated,
        "verdict_changes": changed,
        "new_false_rejections": sum(1 for c in changed if c["r2e1"] and not c["live"]),
        "new_acceptances": sum(1 for c in changed if not c["r2e1"] and c["live"]),
    }


def dapre_surfaces() -> dict[str, Any]:
    load_dotenv(ROOT / ".env")
    token = os.getenv("APP_TOKEN")
    headers = {"X-App-Token": token} if token else {}

    def get(dataset: str, params: dict[str, Any]) -> list[dict]:
        response = requests.get(
            RESOURCE.format(dataset), params=params, headers=headers, timeout=180
        )
        response.raise_for_status()
        return response.json()

    variants = [DAPRE_NIT] + [f"{DAPRE_NIT}{d}" for d in range(10)]
    quoted = ",".join(f"'{v}'" for v in variants)

    registry = [
        {"published_nit": row["nit_entidad"], "name": row["nombre_entidad"],
         "active": row.get("esta_activa")}
        for row in get(BUYER_DIRECTORY_ID, {
            "$select": "nombre_entidad,nit_entidad,esta_activa",
            "$where": f"es_entidad='Si' and nit_entidad in ({quoted})",
            "$limit": 200,
        })
    ]
    surfaces = {}
    for key, dataset, field in (
        ("process_source_names", PROCESS_DATASET_ID, "entidad"),
        ("contract_source_names", CONTRACT_DATASET_ID, "nombre_entidad"),
    ):
        surfaces[key] = [
            {"published_nit": row["nit_entidad"], "name": row[field], "rows": int(row["rows"])}
            for row in get(dataset, {
                "$select": f"nit_entidad,{field},count(*) as rows",
                "$where": f"nit_entidad in ({quoted})",
                "$group": f"nit_entidad,{field}",
                "$limit": 500,
            })
        ]

    def norm_names(items, key="name"):
        return {normalize_text(item[key]) for item in items}

    gazetteer_name = normalize_text(DAPRE_GAZETTEER_NAME)
    present_in = [
        label for label, names in (
            ("registry", norm_names(registry)),
            ("processes", norm_names(surfaces["process_source_names"])),
            ("contracts", norm_names(surfaces["contract_source_names"])),
        ) if gazetteer_name in names
    ]
    return {
        "nit": DAPRE_NIT,
        "gazetteer_official_name": DAPRE_GAZETTEER_NAME,
        "registry_names": registry,
        **surfaces,
        "gazetteer_name_present_in": present_in,
        "classification": (
            "SOURCE_NAME_SPLIT" if not present_in else "SOURCE_NAME_ALIGNED"
        ),
        "note": (
            "Tres superficies distintas bajo un mismo NIT base. No se afirma "
            "cronología: la evidencia disponible es un corte, no una serie."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default="POST")
    parser.add_argument("--baseline", action="store_true",
                        help="evalúa el banco con la regla token-a-token de R2E.1")
    parser.add_argument("--skip-network", action="store_true")
    parser.add_argument("--output", type=Path, default=OUT)
    args = parser.parse_args(argv)

    resolver = EntityResolver(DB)
    records = json.loads(DB.read_text(encoding="utf-8"))
    guard = guard_r2e1 if args.baseline else guard_live

    bank = run_bank(guard)
    payload: dict[str, Any] = {
        "label": args.label,
        "baseline_mode": args.baseline,
        "filler_tokens": sorted(FILLER_TOKENS),
        "catalog": classify_catalog(),
        "multiword_bank": bank,
        "real_surface": sweep_real_surface(resolver, records),
    }
    if not args.baseline:
        # La matriz PRE viaja en el mismo artefacto: el delta se lee sin
        # reconstruir la regla anterior a mano.
        pre_bank = run_bank(guard_r2e1)
        payload["pre"] = {
            "rule": "R2E.1 — territorio comparado token a token",
            "multiword_bank": pre_bank,
            "gates": {
                "MULTIWORD_TERRITORY_LOSS": sum(
                    1 for row in pre_bank if not row["expected"] and row["verdict"]
                ),
                "BANK_FALSE_REJECTIONS": sum(
                    1 for row in pre_bank if row["expected"] and not row["verdict"]
                ),
            },
        }
    if not args.skip_network:
        payload["dapre"] = dapre_surfaces()

    losses = [row for row in bank if not row["expected"] and row["verdict"]]
    rejections = [row for row in bank if row["expected"] and not row["verdict"]]
    payload["gates"] = {
        "MULTIWORD_TERRITORY_LOSS": len(losses),
        "BANK_FALSE_REJECTIONS": len(rejections),
        "NEW_FALSE_REJECTIONS": payload["real_surface"]["new_false_rejections"],
        "REAL_SURFACE_VERDICT_CHANGES": len(payload["real_surface"]["verdict_changes"]),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[{args.label}] " + " · ".join(f"{k}={v}" for k, v in payload["gates"].items()))
    print(f"escrito: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
