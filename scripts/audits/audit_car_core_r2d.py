#!/usr/bin/env python3
"""Auditor read-only R2D del núcleo oficial de 33 CAR/CDS.

Congela, en un solo artefacto, la evidencia que autoriza (o bloquea) cada
comprador antes de mutar el gazetteer:

* la identidad SECOP II por NIT — nombre físico y volumen, sin recortes;
* el riesgo de cada sigla, medido contra el catálogo real de compradores y
  contra el ownership del gazetteer, no contra la intuición;
* la matriz de resolución vigente y la causa raíz de cada resolución insegura.

No importa ni modifica componentes de runtime, y no escribe en ``aliases_db``.
Cormagdalena queda fuera del universo: es un ente corporativo especial del
orden nacional (Ley 161 de 1994), no una CAR.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

DB = ROOT / "app/data/aliases_db.json"
OUT = ROOT / "artifacts/audits/car_core_r2d_candidates.json"
PROC_URL = "https://www.datos.gov.co/resource/p6dx-8zbt.json"
CONT_URL = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
RECENT_SINCE = "2025-09-01T00:00:00.000"

# ── Universo oficial ────────────────────────────────────────────────────
# Autoridad: Ministerio de Ambiente — 26 CAR + 7 CDS = 33.
# https://www.minambiente.gov.co/eleccion-car/
# El NIT es la llave de identidad; se verifica contra SECOP II en cada corrida.
# (acronym, official_institutional_name, class, nit, seat_department, jurisdiction)
UNIVERSE_33: list[tuple[str, str, str, str, str, str]] = [
    ("CAM", "Corporación Autónoma Regional del Alto Magdalena", "CAR",
     "800255580", "Huila", "Huila"),
    ("CAR", "Corporación Autónoma Regional de Cundinamarca", "CAR",
     "899999062", "Cundinamarca", "Cundinamarca"),
    ("CARDER", "Corporación Autónoma Regional de Risaralda", "CAR",
     "891410354", "Risaralda", "Risaralda"),
    ("CARDIQUE", "Corporación Autónoma Regional del Canal del Dique", "CAR",
     "800254453", "Bolívar", "Norte de Bolívar"),
    ("CARSUCRE", "Corporación Autónoma Regional de Sucre", "CAR",
     "823000050", "Sucre", "Sucre"),
    ("CAS", "Corporación Autónoma Regional de Santander", "CAR",
     "804000292", "Santander", "Santander"),
    ("CDA", "Corporación para el Desarrollo Sostenible del Norte y Oriente Amazónico", "CDS",
     "838000009", "Guaviare", "Guainía, Guaviare y Vaupés"),
    ("CDMB", "Corporación Autónoma Regional para la Defensa de la Meseta de Bucaramanga", "CAR",
     "890201573", "Santander", "Meseta de Bucaramanga"),
    ("CODECHOCÓ", "Corporación Autónoma Regional para el Desarrollo Sostenible del Chocó", "CDS",
     "899999238", "Chocó", "Chocó"),
    ("CORALINA", "Corporación para el Desarrollo Sostenible del Archipiélago de San Andrés, "
                 "Providencia y Santa Catalina", "CDS",
     "827000031", "San Andrés, Providencia y Santa Catalina", "Archipiélago"),
    ("CORANTIOQUIA", "Corporación Autónoma Regional del Centro de Antioquia", "CAR",
     "811000231", "Antioquia", "Centro de Antioquia"),
    ("CORMACARENA", "Corporación para el Desarrollo Sostenible del Área de Manejo Especial "
                    "La Macarena", "CDS",
     "822000091", "Meta", "Meta"),
    ("CORNARE", "Corporación Autónoma Regional de las Cuencas de los Ríos Negro y Nare", "CAR",
     "890985138", "Antioquia", "Oriente antioqueño"),
    ("CORPAMAG", "Corporación Autónoma Regional del Magdalena", "CAR",
     "800099287", "Magdalena", "Magdalena"),
    ("CORPOAMAZONIA", "Corporación para el Desarrollo Sostenible del Sur de la Amazonia", "CDS",
     "800252844", "Putumayo", "Amazonas, Caquetá y Putumayo"),
    ("CORPOBOYACÁ", "Corporación Autónoma Regional de Boyacá", "CAR",
     "800252843", "Boyacá", "Boyacá"),
    ("CORPOCALDAS", "Corporación Autónoma Regional de Caldas", "CAR",
     "890803005", "Caldas", "Caldas"),
    ("CORPOCESAR", "Corporación Autónoma Regional del Cesar", "CAR",
     "892301483", "Cesar", "Cesar"),
    ("CORPOCHIVOR", "Corporación Autónoma Regional de Chivor", "CAR",
     "800252037", "Boyacá", "Suroriente de Boyacá"),
    ("CORPOGUAJIRA", "Corporación Autónoma Regional de La Guajira", "CAR",
     "892115314", "La Guajira", "La Guajira"),
    ("CORPOGUAVIO", "Corporación Autónoma Regional del Guavio", "CAR",
     "832000171", "Cundinamarca", "Región del Guavio"),
    ("CORPOMOJANA", "Corporación para el Desarrollo Sostenible de La Mojana y el San Jorge", "CDS",
     "823000077", "Sucre", "La Mojana y San Jorge"),
    ("CORPONARIÑO", "Corporación Autónoma Regional de Nariño", "CAR",
     "891222322", "Nariño", "Nariño"),
    ("CORPONOR", "Corporación Autónoma Regional de la Frontera Nororiental", "CAR",
     "890505253", "Norte de Santander", "Norte de Santander"),
    ("CORPORINOQUIA", "Corporación Autónoma Regional de la Orinoquia", "CAR",
     "832000283", "Casanare", "Orinoquia"),
    ("CORPOURABÁ", "Corporación para el Desarrollo Sostenible del Urabá", "CDS",
     "890907748", "Antioquia", "Urabá"),
    ("CORTOLIMA", "Corporación Autónoma Regional del Tolima", "CAR",
     "890704536", "Tolima", "Tolima"),
    ("CRA", "Corporación Autónoma Regional del Atlántico", "CAR",
     "802000339", "Atlántico", "Atlántico"),
    ("CRC", "Corporación Autónoma Regional del Cauca", "CAR",
     "891501885", "Cauca", "Cauca"),
    ("CRQ", "Corporación Autónoma Regional del Quindío", "CAR",
     "8900004478", "Quindío", "Quindío"),
    ("CSB", "Corporación Autónoma Regional del Sur de Bolívar", "CAR",
     "806000327", "Bolívar", "Sur de Bolívar"),
    ("CVC", "Corporación Autónoma Regional del Valle del Cauca", "CAR",
     "890399002", "Valle del Cauca", "Valle del Cauca"),
    ("CVS", "Corporación Autónoma Regional de los Valles del Sinú y del San Jorge", "CAR",
     "891000627", "Córdoba", "Córdoba"),
]

# Cormagdalena: identidad separada, nunca dentro de las 33.
CORMAGDALENA = {
    "acronym": "CORMAGDALENA",
    "official_institutional_name":
        "Corporación Autónoma Regional del Río Grande de la Magdalena",
    "nit": "829000127",
    "secop_physical_name": "CORPORACIÓN AUTÓNOMA DEL RÍO GRANDE DE LA MAGDALENA**",
    "classification": "SPECIAL_AUTONOMOUS_CORPORATION",
    "scope": "OUTSIDE_R2D_CORE",
    "authority": "Ley 161 de 1994 — ente corporativo especial del orden nacional (EICE)",
}


# ── Causa raíz y decisión, congeladas tras el discovery ─────────────────
# La causa es por qué la consulta era insegura; el mecanismo (fuzzy fail-open,
# colisión de sigla, secuestro territorial) es común y no se repite por fila.
ROOT_CAUSE: dict[str, str] = {
    **{a: "IDENTITY_MISSING" for a in (
        "CAM", "CARDIQUE", "CARSUCRE", "CDA", "CODECHOCÓ", "CORALINA", "CORANTIOQUIA",
        "CORPAMAG", "CORPOCALDAS", "CORPOCESAR", "CORPOGUAJIRA", "CORPOGUAVIO",
        "CORPOMOJANA", "CORPOURABÁ", "CRA", "CSB", "CVS")},
    **{a: "IDENTITY_PRESENT_ALIAS_GAP" for a in (
        "CORNARE", "CORPONARIÑO", "CORPONOR", "CORPORINOQUIA", "CORTOLIMA", "CRC")},
    "CORPOCHIVOR": "WRONG_ALIAS_OWNER",
}

# Siglas retenidas fuera del gazetteer: cada una nombra a un comprador SECOP II
# activo que no es la corporación. La entidad entra igual por nombre.
BLOCKED_ACRONYMS: dict[str, str] = {
    "CDA": "SENA REGIONAL CUNDINAMARCA Grupo Administrativo CDA; centros de diagnóstico automotor",
    "CRA": "Comisión de Regulación de Agua Potable y Saneamiento Básico; abrevia 'carrera'",
    "CRC": "Comisión de Regulación de Comunicaciones",
}

DECISION: dict[str, str] = {
    **{a: "ACCEPT_REGISTRY_VERIFIED" for a in (
        "CAM", "CARDIQUE", "CARSUCRE", "CODECHOCÓ", "CORALINA", "CORANTIOQUIA", "CORPAMAG",
        "CORPOCALDAS", "CORPOCESAR", "CORPOGUAJIRA", "CORPOGUAVIO", "CORPOMOJANA",
        "CORPOURABÁ", "CSB", "CVS")},
    **{a: "ACCEPT_ALIAS_RECONCILIATION" for a in (
        "CORNARE", "CORPONARIÑO", "CORPONOR", "CORPORINOQUIA", "CORTOLIMA")},
    "CORPOCHIVOR": "REMOVE_WRONG_ALIAS",
    **{a: "BLOCK_AMBIGUOUS" for a in BLOCKED_ACRONYMS},
    **{a: "UNCHANGED_ALREADY_VERIFIED" for a in (
        "CAR", "CARDER", "CAS", "CDMB", "CORMACARENA", "CORPOAMAZONIA", "CORPOBOYACÁ",
        "CRQ", "CVC")},
}

def fetch(base: str, params: dict, tries: int = 8) -> list[dict]:
    """GET con reintentos: datos.gov.co devuelve 503 transitorios con frecuencia."""
    url = base + "?" + urllib.parse.urlencode(params)
    last: Exception | None = None
    for attempt in range(tries):
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": "SECOPPAL-R2D-audit/1.0"})
            with urllib.request.urlopen(request, timeout=90) as response:
                payload = json.load(response)
            return payload if isinstance(payload, list) else []
        except Exception as exc:
            last = exc
            time.sleep(2.0 * (attempt + 1))
    assert last is not None
    raise last


def catalog_norm(value: str) -> str:
    """Normalización de comparación léxica: sin tildes, sin puntuación, en tokens."""
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(c for c in value if not unicodedata.combining(c)).upper()
    return " ".join(re.sub(r"[^A-Z0-9]+", " ", value).split())


def digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def buyer_catalog() -> dict[tuple[str, str], dict]:
    """Catálogo completo de compradores SECOP II (procesos + contratos).

    Es la única forma honesta de medir si una sigla identifica *inequívocamente*
    a una corporación: se compara contra todos los compradores que existen, no
    contra los que uno recuerda.
    """
    catalog: dict[tuple[str, str], dict] = {}
    for base, field in ((PROC_URL, "entidad"), (CONT_URL, "nombre_entidad")):
        offset = 0
        while True:
            rows = fetch(base, {"$select": f"nit_entidad,{field},count(*) as rows",
                                "$group": f"nit_entidad,{field}",
                                "$order": f"nit_entidad,{field}",
                                "$limit": 10000, "$offset": offset})
            for row in rows:
                key = (digits(row.get("nit_entidad", "")), catalog_norm(row.get(field, "")))
                entry = catalog.setdefault(key, {"raw": row.get(field), "rows": 0})
                entry["rows"] += int(row.get("rows", 0))
            if len(rows) < 10000:
                break
            offset += 10000
    return catalog


def identity_by_nit(nits: list[str]) -> dict[str, dict]:
    """Nombres físicos y volúmenes por NIT, sin filtro de similitud.

    Filtrar los nombres de un NIT por parecido al nombre buscado esconde
    justamente lo que hay que ver: un comprador partido en varios nombres.
    """
    evidence: dict[str, dict] = {
        nit: {"proceso_source_names": [], "contrato_source_names": [],
              "all_time_procesos": 0, "all_time_contratos": 0,
              "recent_12m_procesos": 0, "recent_12m_contratos": 0}
        for nit in nits
    }
    quoted = ",".join(f'"{n}"' for n in nits)
    for dataset, base, field, date_field in (
        ("procesos", PROC_URL, "entidad", "fecha_de_publicacion_del"),
        ("contratos", CONT_URL, "nombre_entidad", "fecha_de_firma"),
    ):
        for recent in (False, True):
            where = f"nit_entidad in ({quoted})"
            if recent:
                where += f' and {date_field} >= "{RECENT_SINCE}"'
            rows = fetch(base, {"$select": f"nit_entidad,{field},count(*) as rows",
                                "$where": where, "$group": f"nit_entidad,{field}",
                                "$limit": 5000})
            for row in rows:
                nit = digits(row.get("nit_entidad", ""))
                if nit not in evidence:
                    continue
                count = int(row.get("rows", 0))
                prefix = "recent_12m" if recent else "all_time"
                evidence[nit][f"{prefix}_{dataset}"] += count
                if not recent:
                    names = evidence[nit][f"{dataset[:-1]}_source_names"]
                    if row.get(field) not in names:
                        names.append(row.get(field))
    return evidence


def acronym_risk(acronym: str, owner_nit: str, catalog: dict, gazetteer: list[dict]) -> dict:
    """Mide la superficie de riesgo de una sigla; no la juzga."""
    key = catalog_norm(acronym)
    token_hits = [
        {"nit": nit, "name": entry["raw"], "rows": entry["rows"]}
        for (nit, name), entry in catalog.items() if key in name.split()
    ]
    foreign = sorted((h for h in token_hits if h["nit"] != owner_nit),
                     key=lambda h: -h["rows"])
    exact_owners: list[str] = []
    token_owners: set[str] = set()
    for record in gazetteer:
        for alias in record.get("aliases", []):
            alias_key = normalize_text(alias)
            if alias_key == normalize_text(acronym):
                exact_owners.append(record["official_name"])
            if normalize_text(acronym) in alias_key.split():
                token_owners.add(record["official_name"])
    return {
        "acronym": acronym,
        "secop_token_buyers": len(token_hits),
        "secop_foreign_buyers": foreign[:5],
        "secop_foreign_buyer_count": len({h["nit"] for h in foreign}),
        "gazetteer_exact_owners": sorted(set(exact_owners)),
        "gazetteer_token_owners": sorted(token_owners),
    }


def resolution_probes(resolver: EntityResolver, gazetteer: list[dict],
                      queries: list[str], expected: set[str]) -> tuple[list[dict], str]:
    alias_owners: dict[str, set[str]] = defaultdict(set)
    for record in gazetteer:
        for alias in record.get("aliases", []):
            alias_owners[normalize_text(alias)].add(record["official_name"])
    probes, statuses = [], []
    for query in queries:
        result = resolver.resolve_entidad(query)
        value = result.value
        correct = bool(value and normalize_text(value) in expected)
        owners = alias_owners.get(normalize_text(query), set())
        if correct and result.method == "exact" and value in owners:
            status = "PASS_EXACT"
        elif correct and str(result.method).startswith("fuzzy"):
            status = "FUZZY_ONLY"
        elif correct:
            status = "PARTIAL"
        elif value:
            status = "WRONG_OWNER"
        else:
            status = "FAIL_CLOSED"
        statuses.append(status)
        probes.append({"query": query, "resolved_owner": value,
                       "resolution_method": result.method, "classification": status,
                       "exact_alias_owners": sorted(owners)})
    precedence = ["WRONG_OWNER", "FUZZY_ONLY", "FAIL_CLOSED", "PARTIAL", "PASS_EXACT"]
    final = next((s for s in precedence if s in statuses), "REVIEW")
    if len(set(statuses)) > 1 and set(statuses) <= {"PASS_EXACT", "PARTIAL"}:
        final = "PARTIAL"
    return probes, final


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=OUT)
    args = parser.parse_args()

    gazetteer = json.loads(DB.read_text(encoding="utf-8"))
    resolver = EntityResolver(DB)
    by_official = {normalize_text(r["official_name"]): r for r in gazetteer}

    catalog = buyer_catalog()
    evidence = identity_by_nit([row[3] for row in UNIVERSE_33])

    entities = []
    for acronym, official, klass, nit, seat, jurisdiction in UNIVERSE_33:
        found = evidence[nit]
        physical = sorted({*found["proceso_source_names"], *found["contrato_source_names"]})
        expected = {normalize_text(name) for name in physical}
        existing = [by_official[k] for k in expected if k in by_official]
        probes, classification = resolution_probes(
            resolver, gazetteer, [acronym, official], expected)
        if len(physical) == 0:
            shape = "NO_SECOP_II_EVIDENCE"
        elif len(physical) > 1:
            shape = "SOURCE_NAME_SPLIT"
        else:
            shape = "SINGLE_PHYSICAL_NAME"
        entities.append({
            "acronym": acronym,
            "official_institutional_name": official,
            "class": klass,
            "nit": nit,
            "seat_department": seat,
            "jurisdiction": jurisdiction,
            "natural_queries": [acronym, official],
            "proceso_source_names": found["proceso_source_names"],
            "contrato_source_names": found["contrato_source_names"],
            "physical_names": physical,
            "all_time_procesos": found["all_time_procesos"],
            "all_time_contratos": found["all_time_contratos"],
            "recent_12m_procesos": found["recent_12m_procesos"],
            "recent_12m_contratos": found["recent_12m_contratos"],
            "secop_identity_shape": shape,
            "existing_gazetteer_records": [r["official_name"] for r in existing],
            "existing_aliases": sorted({a for r in existing for a in r.get("aliases", [])}),
            "acronym_risk": acronym_risk(acronym, nit, catalog, gazetteer),
            "resolution_probes": probes,
            "classification": classification,
            "root_cause": ROOT_CAUSE.get(acronym, "NONE_ALREADY_VERIFIED"),
            "acronym_decision": ("BLOCKED" if acronym in BLOCKED_ACRONYMS else "ADMITTED"),
            "acronym_block_reason": BLOCKED_ACRONYMS.get(acronym),
            "decision": DECISION[acronym],
        })

    matrix: dict[str, int] = defaultdict(int)
    for entity in entities:
        matrix[entity["classification"]] += 1

    payload = {
        "audit": "CAR-CORE-R2D",
        "type": "discovery/read-only",
        "as_of": date.today().isoformat(),
        "recent_since": RECENT_SINCE[:10],
        "secop_authority": {"processes": "p6dx-8zbt", "contracts": "jbjy-vk9h"},
        "official_sources": [
            {"scope": "car_cds_33", "authority": "Ministerio de Ambiente",
             "url": "https://www.minambiente.gov.co/eleccion-car/"},
            {"scope": "cormagdalena", "authority": "Ley 161 de 1994",
             "url": "https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=68533"},
        ],
        "universe": {
            "car_cds": len(UNIVERSE_33),
            "car": sum(1 for row in UNIVERSE_33 if row[2] == "CAR"),
            "cds": sum(1 for row in UNIVERSE_33 if row[2] == "CDS"),
            "cormagdalena": CORMAGDALENA,
            "excluded": ["urban environmental authorities (7)", "Cormagdalena"],
        },
        "methodology": {
            "identity_order": ["MinAmbiente universe", "SECOP II NIT", "physical source name"],
            "volume_note": "Counts by NIT; processes and contracts remain separate.",
            "recent_date_fields": {"processes": "fecha_de_publicacion_del",
                                   "contracts": "fecha_de_firma"},
            "acronym_note": "Risk is measured against the full SECOP II buyer catalog "
                            "and gazetteer ownership, never assumed from length.",
        },
        "buyer_catalog_size": len(catalog),
        "matrix_scope": "state of app/data/aliases_db.json at run time",
        "matrix": dict(matrix),
        "volume": {
            "recent_12m_processes": sum(e["recent_12m_procesos"] for e in entities),
            "recent_12m_contracts": sum(e["recent_12m_contratos"] for e in entities),
            "all_time_processes": sum(e["all_time_procesos"] for e in entities),
            "all_time_contracts": sum(e["all_time_contratos"] for e in entities),
        },
        "entities": entities,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                           encoding="utf-8")
    print(json.dumps({"matrix": payload["matrix"], "volume": payload["volume"],
                      "buyer_catalog_size": payload["buyer_catalog_size"]},
                     ensure_ascii=False, indent=2))
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
