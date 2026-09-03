"""RETIRADO — one-shot ya aplicado. No volver a ejecutar.

ENTITY-COVERAGE-BOGOTA-LOCAL-001 — añadió al gazetteer las 20 alcaldías locales
de Bogotá que faltaban en el top-500. Cada una es una entidad pública distinta
que gestiona su propia contratación, aunque casi todas compartan el NIT 899999061
del Distrito Capital.

Se conserva como registro de procedencia del lote, no como herramienta. La tabla
quedó corregida en la Etapa 1 (ADR-074): la entrada de Puente Aranda declaraba
la central contable del Ejército como alcaldía local. Un replay hoy no puede
reproducir ese daño —el applier sólo AÑADE registros nuevos y saltaría los 20 por
nombre—, pero tampoco es una fuente de verdad: el gazetteer evolucionó desde
entonces y esta tabla es una foto de 2026-06. La autoridad es
``app/data/aliases_db.json``.

La lección está en la nota original que gobernaba la elección de nombres:
"official_name = EXACT name in SECOP so unaccent(LIKE) matches work". Optimizaba
por que la cadena EXISTIERA en SECOP, no por que perteneciera al comprador
pedido. ADR-074 define la BuyerUnit como unidad lógica acreditada precisamente
para que esa confusión no pueda repetirse.
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
        # CORREGIDO (Etapa 1 / ADR-074, BUYER-PUENTE-ARANDA-HIJACK).
        # Esta entrada declaraba como official_name
        # "CENTRAL ADMINISTRATIVA Y CONTABLE  PUENTE ARANDA+", NIT 830039548:
        # la central contable del EJÉRCITO. Se eligió por contener el token de
        # la localidad, siguiendo la nota de arriba —"official_name = EXACT name
        # in SECOP"—, que optimiza por que la cadena exista y no por que el
        # dueño sea el correcto. Los otros 19 registros llevan el NIT distrital
        # 899999061; este llevaba otro, y el propio dato ya contradecía el
        # binding. Verificado en vivo: la alcaldía local publica
        # ALCALDIA LOCAL DE PUENTE ARANDA-FONDO DE DESARROLLO LOCAL
        # (2.967 procesos / 3.685 contratos) y el CENAC publica
        # CENAC PUENTE ARANDA+ (2.968 / 1.457). Son dos BuyerUnits, no una.
        "official_name": "ALCALDIA LOCAL DE PUENTE ARANDA-FONDO DE DESARROLLO LOCAL",
        "departamento": "BOGOTA D.C.",
        "nit": "899999061",
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


RETIRED_REASON = (
    "add_bogota_local_alcaldias.py está RETIRADO: el lote "
    "ENTITY-COVERAGE-BOGOTA-LOCAL-001 ya se aplicó y su tabla es una foto de "
    "2026-06, no la verdad actual. La autoridad es app/data/aliases_db.json. "
    "Si necesitas modificar alcaldías locales, hazlo con evidencia viva y con "
    "los canarios de tests/test_buyer_unit_integrity_p02.py delante."
)


def main() -> None:
    raise SystemExit(RETIRED_REASON)


def _apply_once_historical() -> None:
    """Cuerpo original del applier. Se conserva para leer qué hizo el lote."""
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
