#!/usr/bin/env python3
"""Inspecciona el dataset SECOP Integrado (rpmr-utcd).

Uso:
  python scripts/inspect_secop_integrado_dataset.py [--sample N]
"""

import argparse
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sodapy import Socrata
from app.config import get_settings


def main():
    parser = argparse.ArgumentParser(description="Inspeccionar SECOP Integrado (rpmr-utcd)")
    parser.add_argument("--sample", type=int, default=20, help="Tamaño de muestra")
    args = parser.parse_args()

    settings = get_settings()
    client = Socrata(settings.datos_gov_domain, settings.secop_app_token, timeout=30)
    domain = settings.datos_gov_domain
    dataset_id = "rpmr-utcd"

    metadata = client.get_metadata(dataset_id)
    print("=" * 80)
    print("SECOP INTEGRADO — rpmr-utcd")
    print("=" * 80)
    print(f"URL humana: https://{domain}/Estad-sticas-Nacionales/SECOP-Integrado/{dataset_id}/about_data")
    print(f"Dataset ID: {dataset_id}")
    print(f"Nombre: {metadata.get('name')}")
    print(f"Descripción: {metadata.get('description', '')[:500]}")
    print(f"Actualizado: {metadata.get('rowsUpdatedAt')}")
    print(f"Filas (download count): {metadata.get('downloadCount')}")
    print()

    # Columnas
    print(f"Columnas ({len(metadata.get('columns', []))}):")
    for col in metadata.get("columns", []):
        print(f"  {col['fieldName']:<42s} {col.get('dataTypeName', '?'):<16s} {(col.get('description', '') or '')[:100]}")
    print()

    # Conteo total
    try:
        count_result = client.get(dataset_id, select="COUNT(*)", limit=1)
        total = int(count_result[0]["COUNT"]) if count_result else 0
        print(f"Total estimado de registros: {total:,}")
    except Exception as e:
        print(f"No se pudo obtener COUNT(*): {e}")
        total = 0
    print()

    # Conteo por origen
    print("--- Conteo por origen ---")
    try:
        results = client.get(dataset_id, select="origen, COUNT(origen)", group="origen", limit=10)
        for r in results:
            print(f"  {r.get('origen', 'N/A'):30s} {r.get('COUNT_origen', '?'):>10s}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Conteo por año de firma
    print("--- Conteo por año de firma ---")
    try:
        results = client.get(dataset_id,
                           select="date_extract_y(fecha_de_firma_del_contrato) as year, COUNT(*)",
                           group="year",
                           order="year DESC",
                           limit=20)
        for r in results:
            print(f"  {r.get('year', 'N/A'):10s} {r.get('COUNT', '?'):>10s}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Conteo por estado
    print("--- Conteo por estado del proceso ---")
    try:
        results = client.get(dataset_id,
                           select="estado_del_proceso, COUNT(estado_del_proceso)",
                           group="estado_del_proceso",
                           limit=20)
        for r in results:
            count = r.get(f"COUNT_estado_del_proceso", "?")
            estado = r.get("estado_del_proceso", "N/A")
            print(f"  {estado[:50]:52s} {count:>10s}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Muestra general (20 filas)
    print(f"--- Muestra general ({args.sample} filas) ---")
    try:
        results = client.get(dataset_id, limit=args.sample, order="fecha_de_firma_del_contrato DESC")
        print(f"  Obtenidas: {len(results)} filas")
        for i, row in enumerate(results):
            print(f"\n  Fila {i+1}:")
            for k, v in sorted(row.items()):
                val_str = str(v)[:120]
                print(f"    {k}: {val_str}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Muestra SECOP I
    print("--- Muestra origen SECOP I ---")
    try:
        results = client.get(dataset_id,
                           where="origen = 'SECOP I'",
                           limit=args.sample,
                           order="fecha_de_firma_del_contrato DESC")
        print(f"  Obtenidas: {len(results)} filas")
        for i, row in enumerate(results):
            print(f"\n  Fila {i+1}:")
            for k, v in sorted(row.items()):
                val_str = str(v)[:120]
                print(f"    {k}: {val_str}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Muestra SECOP II
    print("--- Muestra origen SECOP II ---")
    try:
        results = client.get(dataset_id,
                           where="origen = 'SECOP II'",
                           limit=args.sample,
                           order="fecha_de_firma_del_contrato DESC")
        print(f"  Obtenidas: {len(results)} filas")
        for i, row in enumerate(results):
            print(f"\n  Fila {i+1}:")
            for k, v in sorted(row.items()):
                val_str = str(v)[:120]
                print(f"    {k}: {val_str}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Guardar fixtures para tests
    fixtures_dir = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    try:
        sample_results = client.get(dataset_id, limit=50, order="fecha_de_firma_del_contrato DESC")
        fixture_path = fixtures_dir / "secop_integrado_sample.json"
        with open(fixture_path, "w", encoding="utf-8") as f:
            json.dump(sample_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"Fixture guardado: {fixture_path} ({len(sample_results)} registros)")

        # También guardar sample SECOP I y SECOP II
        s1 = client.get(dataset_id, where="origen = 'SECOP I'", limit=20, order="fecha_de_firma_del_contrato DESC")
        s1_path = fixtures_dir / "secop_integrado_secop1_sample.json"
        with open(s1_path, "w", encoding="utf-8") as f:
            json.dump(s1, f, ensure_ascii=False, indent=2, default=str)
        print(f"Fixture SECOP I guardado: {s1_path} ({len(s1)} registros)")

        s2 = client.get(dataset_id, where="origen = 'SECOP II'", limit=20, order="fecha_de_firma_del_contrato DESC")
        s2_path = fixtures_dir / "secop_integrado_secop2_sample.json"
        with open(s2_path, "w", encoding="utf-8") as f:
            json.dump(s2, f, ensure_ascii=False, indent=2, default=str)
        print(f"Fixture SECOP II guardado: {s2_path} ({len(s2)} registros)")
    except Exception as e:
        print(f"Error guardando fixtures: {e}")

    client.close()
    print("\nInspección completada.")


if __name__ == "__main__":
    main()
