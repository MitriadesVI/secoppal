"""One-shot: append 20 Bogotá local alcaldías to aliases_db.json.

ENTITY-COVERAGE-BOGOTA-LOCAL-001 — adds local mayors missing from
the top-500 gazetteer.  Each is a distinct public entity that manages
its own SECOP procurement, even though most share NIT 899999061
(Distrito Capital).

Run:  python scripts/add_bogota_local_alcaldias.py
Backup already created at app/data/aliases_db.backup.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent

# ── Canonical data from SECOP II (www.datos.gov.co, p6dx-8zbt) ──────────
# Official name = EXACT name in SECOP so unaccent(LIKE) matches work.
# Aliases = normalized forms WITHOUT standalone generic tokens (recall
# ENTITY-MISRESOLUTION-CATASTROPHIC — "alcaldía" solo no debe resolver).
ALCALDIAS_LOCALES: list[dict] = [
    {
        "official_name": "ALCALDIA LOCAL DE USAQUEN",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de usaquen", "alcaldia local usaquen",
            "alcaldia usaquen", "usaquen localidad",
            "localidad de usaquen", "localidad usaquen",
        ],
    },
    {
        "official_name": "ALCALDÍA LOCAL DE CHAPINERO - FONDO DE DESARROLLO LOCAL",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de chapinero", "alcaldia local chapinero",
            "alcaldia chapinero", "chapinero localidad",
            "localidad de chapinero", "localidad chapinero",
        ],
    },
    {
        "official_name": "FONDO DE DESARROLLO LOCAL DE SANTA FE",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de santa fe", "alcaldia local santa fe",
            "alcaldia santa fe", "santa fe localidad",
            "localidad de santa fe", "localidad santa fe",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE SAN CRISTOBAL",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de san cristobal", "alcaldia local san cristobal",
            "alcaldia san cristobal", "san cristobal localidad",
            "localidad de san cristobal", "localidad san cristobal",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE USME",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de usme", "alcaldia local usme",
            "alcaldia usme", "usme localidad",
            "localidad de usme", "localidad usme",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE TUNJUELITO",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de tunjuelito", "alcaldia local tunjuelito",
            "alcaldia tunjuelito", "tunjuelito localidad",
            "localidad de tunjuelito", "localidad tunjuelito",
        ],
    },
    {
        "official_name": "FONDO DE DESARROLLO LOCAL DE BOSA",
        "departamento": "BOGOTA D.C.",
        "nit": "8999990615",
        "aliases": [
            "alcaldia local de bosa", "alcaldia local bosa",
            "alcaldia bosa", "bosa localidad",
            "localidad de bosa", "localidad bosa",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE KENNEDY",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de kennedy", "alcaldia local kennedy",
            "alcaldia kennedy", "kennedy localidad",
            "localidad de kennedy", "localidad kennedy",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE FONTIBON",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de fontibon", "alcaldia local fontibon",
            "alcaldia fontibon", "fontibon localidad",
            "localidad de fontibon", "localidad fontibon",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE ENGATIVÁ",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de engativa", "alcaldia local engativa",
            "alcaldia engativa", "engativa localidad",
            "localidad de engativa", "localidad engativa",
        ],
    },
    {
        "official_name": "ALCALDÍA LOCAL DE SUBA",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de suba", "alcaldia local suba",
            "alcaldia suba", "suba localidad",
            "localidad de suba", "localidad suba",
        ],
    },
    {
        "official_name": "ALCALDÍA LOCAL DE BARRIOS UNIDOS",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de barrios unidos", "alcaldia local barrios unidos",
            "alcaldia barrios unidos", "barrios unidos localidad",
            "localidad de barrios unidos", "localidad barrios unidos",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE TEUSAQUILLO(oficial)",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de teusaquillo", "alcaldia local teusaquillo oficial",
            "alcaldia teusaquillo", "teusaquillo localidad",
            "localidad de teusaquillo", "localidad teusaquillo",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE MARTIRES",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de los martires", "alcaldia local los martires",
            "alcaldia los martires", "los martires localidad",
            "localidad de los martires", "localidad los martires",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE ANTONIO NARIÑO",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de antonio nariño", "alcaldia local antonio narino",
            "alcaldia antonio nariño", "antonio nariño localidad",
            "localidad de antonio nariño", "localidad antonio narino",
        ],
    },
    {
        "official_name": "CENTRAL ADMINISTRATIVA Y CONTABLE  PUENTE ARANDA+",
        "departamento": "BOGOTA D.C.",
        "nit": "830039548",
        "aliases": [
            "alcaldia local de puente aranda", "alcaldia local puente aranda",
            "alcaldia puente aranda", "puente aranda localidad",
            "localidad de puente aranda", "localidad puente aranda",
        ],
    },
    {
        "official_name": "FONDO DE DESARROLLO LOCAL DE LA CANDELARIA",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de la candelaria", "alcaldia local la candelaria",
            "alcaldia la candelaria", "la candelaria localidad",
            "localidad de la candelaria", "localidad la candelaria",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL RAFAEL URIBE URIBE",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de rafael uribe uribe", "alcaldia local rafael uribe uribe",
            "alcaldia rafael uribe uribe", "rafael uribe uribe localidad",
            "localidad de rafael uribe uribe", "localidad rafael uribe uribe",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE CIUDAD BOLIVAR //",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de ciudad bolivar", "alcaldia local ciudad bolivar",
            "alcaldia ciudad bolivar", "ciudad bolivar localidad",
            "localidad de ciudad bolivar", "localidad ciudad bolivar",
        ],
    },
    {
        "official_name": "ALCALDIA LOCAL DE SUMAPAZ",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
        "aliases": [
            "alcaldia local de sumapaz", "alcaldia local sumapaz",
            "alcaldia sumapaz", "sumapaz localidad",
            "localidad de sumapaz", "localidad sumapaz",
        ],
    },
]


def main() -> None:
    json_path = PROJECT / "app" / "data" / "aliases_db.json"

    with json_path.open("r", encoding="utf-8") as fh:
        data: list[dict] = json.load(fh)

    existing_officials = {rec["official_name"] for rec in data}
    added = 0
    skipped = 0

    for entry in ALCALDIAS_LOCALES:
        if entry["official_name"] in existing_officials:
            print(f"SKIP (ya existe): {entry['official_name']}")
            skipped += 1
            continue

        # Build record in the same shape as existing entries
        record = {
            "official_name": entry["official_name"],
            "departamento": entry["departamento"],
            "aliases": entry["aliases"],
        }
        # NIT is optional in the schema but present in some entries
        if entry.get("nit"):
            record["nit"] = entry["nit"]

        data.append(record)
        print(f"ADD: {entry['official_name']}")
        added += 1

    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)

    print(f"\nResultado: {added} agregadas, {skipped} skipped, {len(data)} total en gazetteer")
    print(f"Backup: app/data/aliases_db.backup.json")


if __name__ == "__main__":
    main()
