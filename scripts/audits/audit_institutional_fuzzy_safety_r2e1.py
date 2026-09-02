#!/usr/bin/env python3
"""Auditor R2E.1: seguridad del fuzzy institucional.

Separa causalmente los dos dueños ajenos que R2E dejó medidos y abiertos:

* **A — alias sobre-genérico.**  ``la república`` pertenecía al Banco de la
  República.  El auditor cruza la frase, por SECUENCIA COMPLETA DE TOKENS,
  contra el directorio oficial de compradores ``4ex9-j3n8`` y cuenta cuántas
  identidades distintas la contienen.
* **B — fail-open del fuzzy sobre identidad ausente.**  Las contralorías
  territoriales existen en SECOP II con NIT propio y actividad, pero no están
  en el gazetteer; Tier 3 devolvía el vecino léxico que quedara.  El auditor
  verifica cada identidad por NIT y mide qué dueño entrega el resolver.

``--baseline`` reproduce el estado PRE contra el código vivo: restaura el alias
retirado en memoria y neutraliza la puerta del fuzzy.  Así la matriz PRE se
mide, no se cita.

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

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core import entity_resolver as resolver_module  # noqa: E402
from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.entity_types import name_tokens  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

DB = ROOT / "app/data/aliases_db.json"
OUT = ROOT / "artifacts/audits/institutional_fuzzy_safety_r2e1.json"

BUYER_DIRECTORY_ID = "4ex9-j3n8"
PROCESS_DATASET_ID = "p6dx-8zbt"
CONTRACT_DATASET_ID = "jbjy-vk9h"
RESOURCE = "https://www.datos.gov.co/resource/{}.json"

BANREP = "BANCO DE LA REPUBLICA"
DAPRE = "DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA"
CGR = "CGR-NIVEL CENTRAL"
OVERGENERIC_ALIAS = "la república"

REPUBLIC_ADVERSARIAL = (
    "Presidencia de la República",
    "Vicepresidencia de la República",
    "Auditoría General de la República",
    "Contraloría General de la República",
    "Senado de la República",
    "Congreso de la República",
    "Cámara de Representantes",
    "Departamento Administrativo de la Presidencia de la República",
    "Fondo de Bienestar Social de la Contraloría General de la República",
)

BANREP_POSITIVE_CANARIES = (
    "Banco de la República",
    "Banrep",
    "Banco Central de Colombia",
    "Banco emisor",
    "BR",
    "banrepública",
    "el emisor",
    "Banco de la República de Colombia",
)

# Banco físico: cada entrada verificada por NIT contra SECOP II.
TERRITORIAL_CONTROL = (
    ("Contraloría Departamental de Bolívar", "890480306"),
    ("Contraloría General de Caldas", "800077897"),
    ("Contraloría General de Santander", "890201705"),
    ("Contraloría Municipal de Ibagué", "890706867"),
    ("Contraloría General de Medellín", "811026988"),
    ("Contraloría Departamental del Huila", "800169790"),
    ("Contraloría Municipal de Pasto", "800156682"),
    ("Contraloría Departamental del Atlántico", "890103037"),
    ("Contraloría Distrital de Cartagena", "800194000"),
    ("Contraloría Municipal de Bucaramanga", "890211142"),
    ("Contraloría Departamental del Valle del Cauca", "800090735"),
    ("Contraloría Distrital de Barranquilla", "800022773"),
    ("Contraloría Departamental de Nariño", "800157830"),
    ("Contraloría Municipal de Neiva", "800095147"),
)

NATIONAL_SIX = {
    "Contraloría General de la República": CGR,
    "Auditoría General de la República": "AUDITORIA GENERAL DE LA REPUBLICA",
    "Procuraduría General de la Nación": "PROCURADURIA GENERAL DE LA NACION",
    "Defensoría del Pueblo": "DEFENSORÍA DEL PUEBLO",
    "Fondo de Bienestar Social de la Contraloría General de la República":
        "FONDO DE BIENESTAR SOCIAL DE LA CONTRALORIA GENERAL DE LA REPUBLICA",
    "Instituto de Estudios del Ministerio Público":
        "INSTITUTO DE ESTUDIOS DEL MINISTERIO PUBLICO.",
}

PARENT_RELATED = (
    ("contratos del fondo de bienestar social de la contraloria general de la republica",
     "FONDO DE BIENESTAR SOCIAL DE LA CONTRALORIA GENERAL DE LA REPUBLICA"),
    ("licitaciones de la contraloria general de la republica", CGR),
    ("contratos del instituto de estudios del ministerio publico",
     "INSTITUTO DE ESTUDIOS DEL MINISTERIO PUBLICO."),
    ("contratos de la procuraduria general de la nacion",
     "PROCURADURIA GENERAL DE LA NACION"),
)

TERRITORIAL_SCAN_TEXTS = (
    "contratos de la contraloria departamental de bolivar",
    "contratos de la contraloria general de santander",
    "contratos de la contraloria general de caldas",
    "contratos de la contraloria general de medellin",
    "contratos de la contraloria municipal de ibague",
    "contratos de la defensoria civil de nobsa",
    "contratos de la personeria municipal de soledad",
    "contratos del fondo de bienestar social de la contraloria departamental de bolivar",
)


# ── SECOP II ────────────────────────────────────────────────────────────────

class Socrata:
    def __init__(self) -> None:
        load_dotenv(ROOT / ".env")
        token = os.getenv("APP_TOKEN")
        self.headers = {"X-App-Token": token} if token else {}

    def get(self, dataset: str, params: dict[str, Any]) -> list[dict]:
        response = requests.get(
            RESOURCE.format(dataset), params=params, headers=self.headers, timeout=180
        )
        response.raise_for_status()
        return response.json()

    def buyer_directory(self) -> list[dict]:
        rows: list[dict] = []
        offset = 0
        while True:
            page = self.get(BUYER_DIRECTORY_ID, {
                "$select": "nombre_entidad,nit_entidad,esta_activa",
                "$where": "es_entidad='Si'",
                "$limit": 50000,
                "$offset": offset,
            })
            rows.extend(page)
            if len(page) < 50000:
                return rows
            offset += 50000

    def activity_by_nit(self, nits: list[str]) -> dict[str, list[dict]]:
        """Nombres físicos y volumen por NIT, probando base y base+dígito."""
        variants = {nit: [nit] + [f"{nit}{d}" for d in range(10)] for nit in nits}
        flat = sorted({v for values in variants.values() for v in values})
        quoted = ",".join(f"'{v}'" for v in flat)
        published: dict[str, dict[str, dict[str, int]]] = {}
        for dataset, field, key in (
            (PROCESS_DATASET_ID, "entidad", "procesos"),
            (CONTRACT_DATASET_ID, "nombre_entidad", "contratos"),
        ):
            for row in self.get(dataset, {
                "$select": f"nit_entidad,{field},count(*) as rows",
                "$where": f"nit_entidad in ({quoted})",
                "$group": f"nit_entidad,{field}",
                "$limit": 5000,
            }):
                entry = published.setdefault(row["nit_entidad"], {}).setdefault(
                    row[field], {"procesos": 0, "contratos": 0}
                )
                entry[key] = int(row["rows"])
        out: dict[str, list[dict]] = {}
        for nit, candidates in variants.items():
            names: list[dict] = []
            for candidate in candidates:
                for physical, counts in published.get(candidate, {}).items():
                    names.append({
                        "published_nit": candidate,
                        "physical_name": physical,
                        **counts,
                    })
            out[nit] = sorted(names, key=lambda item: -item["procesos"])
        return out


def contains_token_sequence(haystack: str, needle: str) -> bool:
    hay, want = name_tokens(haystack), name_tokens(needle)
    if not want or len(want) > len(hay):
        return False
    return any(hay[i:i + len(want)] == want for i in range(len(hay) - len(want) + 1))


# ── medición ────────────────────────────────────────────────────────────────

def build_resolver(baseline: bool) -> EntityResolver:
    """Resolver del HEAD vivo; en modo baseline, el estado PRE de R2E.1."""
    if not baseline:
        return EntityResolver(DB)

    records = json.loads(DB.read_text(encoding="utf-8"))
    for record in records:
        if record["official_name"] == BANREP:
            record["aliases"] = [*record["aliases"], OVERGENERIC_ALIAS]
            record.pop("resolution_blocked_aliases", None)
    scratch = Path(os.environ.get("TMPDIR", "/tmp")) / "r2e1_baseline_aliases.json"
    scratch.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")

    resolver = EntityResolver(scratch)
    # PRE no tenía la puerta del fuzzy: se neutraliza, no se borra.
    resolver._keeps_requested_identity = lambda *_args, **_kwargs: True  # noqa: SLF001
    return resolver


def measure(resolver: EntityResolver) -> dict[str, Any]:
    republic = []
    for phrase in REPUBLIC_ADVERSARIAL:
        result = resolver.resolve_entidad(phrase)
        hit = resolver.scan_entity(normalize_text(f"contratos de {phrase}"))
        republic.append({
            "query": phrase,
            "resolve_owner": result.value,
            "resolve_method": result.method,
            "scan_owner": hit.official_name if hit else None,
            "scan_alias": hit.matched_alias if hit else None,
            "reaches_banrep": BANREP in {result.value, hit.official_name if hit else None},
        })

    canaries = []
    for phrase in BANREP_POSITIVE_CANARIES:
        result = resolver.resolve_entidad(phrase)
        hit = resolver.scan_entity(normalize_text(f"contratos del {phrase}"))
        canaries.append({
            "query": phrase,
            "resolve_owner": result.value,
            "resolve_method": result.method,
            "scan_owner": hit.official_name if hit else None,
        })

    territorial = []
    for phrase, nit in TERRITORIAL_CONTROL:
        result = resolver.resolve_entidad(phrase)
        hit = resolver.scan_entity(normalize_text(f"contratos de la {phrase}"))
        if result.value is None and result.method == "like":
            verdict = "FAIL_CLOSED_LITERAL"
        elif result.value is None:
            verdict = "FAIL_CLOSED"
        else:
            verdict = "WRONG_OWNER_FUZZY" if result.method.startswith("fuzzy") else "EXACT_KNOWN"
        territorial.append({
            "natural_query": phrase,
            "nit": nit,
            "current_result": result.value,
            "current_method": result.method,
            "like_value": result.like_value,
            "scan_owner": hit.official_name if hit else None,
            "verdict": verdict,
        })

    national = {
        phrase: {
            "owner": resolver.resolve_entidad(phrase).value,
            "method": resolver.resolve_entidad(phrase).method,
            "expected": expected,
        }
        for phrase, expected in NATIONAL_SIX.items()
    }
    parent = [
        {
            "text": text,
            "owner": (hit := resolver.scan_entity(text)) and hit.official_name,
            "expected": expected,
        }
        for text, expected in PARENT_RELATED
    ]
    scan_territorial = [
        {"text": text, "owner": (hit := resolver.scan_entity(text)) and hit.official_name}
        for text in TERRITORIAL_SCAN_TEXTS
    ]

    return {
        "republic_adversarial": republic,
        "banrep_positive_canaries": canaries,
        "territorial_control": territorial,
        "national_six": national,
        "parent_related": parent,
        "territorial_scan": scan_territorial,
        "gates": {
            "BANREP_CROSS_OWNER": sum(1 for row in republic if row["reaches_banrep"]),
            "CONTROL_TERRITORIAL_WRONG_OWNER": sum(
                1 for row in territorial if row["verdict"] == "WRONG_OWNER_FUZZY"
            ),
            "BANREP_POSITIVE_LOST": sum(
                1 for row in canaries if row["resolve_owner"] != BANREP
            ),
            "NATIONAL_SIX_OK": sum(
                1 for data in national.values() if data["owner"] == data["expected"]
            ),
            "PARENT_RELATED_OK": sum(
                1 for row in parent if row["owner"] == row["expected"]
            ),
            "TERRITORIAL_SCAN_HIJACKS": sum(
                1 for row in scan_territorial if row["owner"] is not None
            ),
        },
    }


def literal_projection(
    territorial: list[dict], identity: dict[str, list[dict]]
) -> list[dict]:
    """Si el filtro literal del Tier 4 alcanza al comprador real.

    Reproduce en local ``unaccent(UPPER(entidad)) LIKE unaccent(UPPER('%X%'))``:
    la equivalencia Ñ→N de ``unaccent`` se verificó en vivo contra
    ``p6dx-8zbt`` antes de plegarla aquí.
    """
    rows = []
    for case in territorial:
        like = normalize_text(case["like_value"] or "").upper()
        names = identity.get(case["nit"], [])
        reached = [
            name for name in names
            if like and like in normalize_text(name["physical_name"]).upper()
        ]
        rows.append({
            "natural_query": case["natural_query"],
            "like_value": case["like_value"],
            "physical_names": [name["physical_name"] for name in names],
            "reaches_own_buyer": bool(reached),
            "procesos": sum(name["procesos"] for name in reached),
            "contratos": sum(name["contratos"] for name in reached),
        })
    return rows


def overgeneric_evidence(directory: list[dict]) -> dict[str, Any]:
    hits = [
        row for row in directory
        if contains_token_sequence(row.get("nombre_entidad") or "", OVERGENERIC_ALIAS)
    ]
    identities: dict[str, list[str]] = {}
    for row in hits:
        identities.setdefault(row["nit_entidad"], []).append(row["nombre_entidad"])
    return {
        "alias": OVERGENERIC_ALIAS,
        "physical_names_containing": len(hits),
        "distinct_published_nits": len(identities),
        "identities": {nit: sorted(set(names)) for nit, names in sorted(identities.items())},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default="POST")
    parser.add_argument("--baseline", action="store_true",
                        help="reproduce el estado PRE contra el código vivo")
    parser.add_argument("--skip-network", action="store_true")
    parser.add_argument("--output", type=Path, default=OUT)
    args = parser.parse_args(argv)

    payload: dict[str, Any] = {
        "label": args.label,
        "baseline_mode": args.baseline,
        **measure(build_resolver(args.baseline)),
    }
    if not args.baseline:
        # La matriz PRE viaja en el mismo artefacto: el delta se lee sin
        # reconstruir el estado anterior a mano.
        payload["pre"] = measure(build_resolver(baseline=True))

    if not args.skip_network:
        client = Socrata()
        directory = client.buyer_directory()
        payload["buyer_directory_size"] = len(directory)
        payload["overgeneric_alias_evidence"] = overgeneric_evidence(directory)
        payload["territorial_identity"] = client.activity_by_nit(
            [nit for _, nit in TERRITORIAL_CONTROL]
        )
        payload["literal_projection"] = literal_projection(
            payload["territorial_control"], payload["territorial_identity"]
        )
        payload["gates"]["LITERAL_PROJECTION_REACHES_OWN_BUYER"] = sum(
            1 for row in payload["literal_projection"] if row["reaches_own_buyer"]
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    gates = payload["gates"]
    print(f"[{args.label}] " + " · ".join(f"{k}={v}" for k, v in gates.items()))
    print(f"escrito: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
