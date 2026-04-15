#!/usr/bin/env python3
"""
SECOPAL — SECOP Dataset Discovery
====================================

Consulta ambos datasets de SECOP II y descubre los valores reales
de todos los campos enum (estados, modalidades, tipos de contrato, etc.)

Uso:
    python scripts/discover_secop.py

Output:
    - Imprime todos los valores por consola
    - Guarda resultado en app/data/secop_field_values.json

Esto se corre UNA vez (o cuando sospeches que SECOP cambió valores).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

try:
    from sodapy import Socrata
except ImportError:
    print("ERROR: pip install sodapy")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DOMAIN = "www.datos.gov.co"
APP_TOKEN = os.environ.get("SECOP_APP_TOKEN")

PROCESOS = "p6dx-8zbt"
CONTRATOS = "jbjy-vk9h"

OUTPUT_FILE = Path("app/data/secop_field_values.json")

# Fields to discover per dataset
DISCOVERY = {
    PROCESOS: {
        "name": "Procesos de Contratación",
        "fields": [
            "estado_de_apertura_del_proceso",
            "estado_del_procedimiento",
            "modalidad_de_contratacion",
            "tipo_de_contrato",
            "departamento_entidad",
        ],
    },
    CONTRATOS: {
        "name": "Contratos Electrónicos",
        "fields": [
            "estado_contrato",
            "modalidad_de_contratacion",
            "tipo_de_contrato",
            "departamento",
        ],
    },
}


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def discover_field(client: Socrata, dataset_id: str, field: str) -> list[dict]:
    """Get unique values and counts for a field."""
    try:
        results = client.get(
            dataset_id,
            query=f"""
                SELECT {field}, COUNT(*) as total
                GROUP BY {field}
                ORDER BY total DESC
                LIMIT 100
            """,
        )
        return [
            {"value": r.get(field, "NULL"), "count": int(r.get("total", 0))}
            for r in results
        ]
    except Exception as e:
        print(f"    ERROR querying {field}: {e}")
        return []


def discover_sample_values(client: Socrata, dataset_id: str) -> dict:
    """Get a few sample rows to see field names and data shapes."""
    try:
        rows = client.get(dataset_id, limit=3)
        if rows:
            return {
                "available_columns": sorted(rows[0].keys()),
                "sample_row": rows[0],
            }
    except Exception as e:
        print(f"    ERROR getting sample: {e}")
    return {}


def main():
    client = Socrata(DOMAIN, app_token=APP_TOKEN, timeout=60)
    all_results: dict = {}

    for dataset_id, config in DISCOVERY.items():
        name = config["name"]
        print(f"\n{'='*70}")
        print(f"  DATASET: {name} ({dataset_id})")
        print(f"{'='*70}")

        dataset_results: dict = {"name": name, "dataset_id": dataset_id, "fields": {}}

        # Discover each enum field
        for field in config["fields"]:
            print(f"\n  📊 {field}:")
            print(f"  {'─'*50}")

            values = discover_field(client, dataset_id, field)
            dataset_results["fields"][field] = values

            for item in values:
                val = item["value"]
                count = item["count"]
                print(f"    {count:>10,}  │  {val}")

            if not values:
                print(f"    (sin resultados)")

        # Get sample row to see all available columns
        print(f"\n  📋 Columnas disponibles:")
        print(f"  {'─'*50}")
        sample = discover_sample_values(client, dataset_id)
        dataset_results["sample"] = sample

        if sample.get("available_columns"):
            for col in sample["available_columns"]:
                print(f"    • {col}")

        all_results[dataset_id] = dataset_results

    # Save to JSON
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*70}")
    print(f"  Resultados guardados en: {OUTPUT_FILE}")
    print(f"{'='*70}")

    # Print actionable summary
    print(f"\n\n{'='*70}")
    print(f"  RESUMEN PARA ACTUALIZAR SECOPAL")
    print(f"{'='*70}")

    for dataset_id, data in all_results.items():
        print(f"\n  Dataset: {data['name']}")
        for field, values in data["fields"].items():
            if not values:
                continue
            top_values = [v["value"] for v in values[:10]]
            print(f"\n    {field}:")
            for v in top_values:
                print(f"      \"{v}\"")


if __name__ == "__main__":
    main()
