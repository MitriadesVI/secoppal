#!/usr/bin/env python3
"""Inspecciona el dataset SECOP I Procesos (f789-7hwg).

Uso:
  python scripts/inspect_secop1_procesos_dataset.py [--sample N]
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
    parser = argparse.ArgumentParser(description="Inspeccionar SECOP I Procesos (f789-7hwg)")
    parser.add_argument("--sample", type=int, default=20, help="Tamaño de muestra")
    args = parser.parse_args()

    settings = get_settings()
    client = Socrata(settings.datos_gov_domain, settings.secop_app_token, timeout=30)
    domain = settings.datos_gov_domain
    dataset_id = "f789-7hwg"

    metadata = client.get_metadata(dataset_id)
    print("=" * 80)
    print("SECOP I PROCESOS — f789-7hwg")
    print("=" * 80)
    print(f"URL humana: https://{domain}/Estad-sticas-Nacionales/SECOP-I-Procesos-de-Compra-P-blica/{dataset_id}/about_data")
    print(f"Dataset ID: {dataset_id}")
    print(f"Nombre: {metadata.get('name')}")
    print(f"Descripción: {metadata.get('description', '')[:500]}")
    print(f"Actualizado: {metadata.get('rowsUpdatedAt')}")
    print(f"Filas (download count): {metadata.get('downloadCount')}")
    print()

    # Columnas
    print(f"Columnas ({len(metadata.get('columns', []))}):")
    for col in metadata.get("columns", []):
        print(f"  {col['fieldName']:<44s} {col.get('dataTypeName', '?'):<16s} {(col.get('description', '') or '')[:90]}")
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

    # Conteo por año de firma
    print("--- Conteo por año de firma del contrato ---")
    try:
        results = client.get(dataset_id,
                           select="anno_firma_contrato, COUNT(*)",
                           group="anno_firma_contrato",
                           order="anno_firma_contrato DESC",
                           limit=20)
        for r in results:
            print(f"  {r.get('anno_firma_contrato', 'N/A'):10s} {r.get('COUNT', '?'):>10s}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Conteo por año de cargue
    print("--- Conteo por año de cargue en SECOP ---")
    try:
        results = client.get(dataset_id,
                           select="date_extract_y(fecha_de_cargue_en_el_secop) as year, COUNT(*)",
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
                           limit=30)
        for r in results:
            count = r.get(f"COUNT_estado_del_proceso", "?")
            estado = r.get("estado_del_proceso", "N/A")
            print(f"  {estado[:50]:52s} {count:>10s}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Conteo por modalidad
    print("--- Conteo por modalidad de contratación ---")
    try:
        results = client.get(dataset_id,
                           select="modalidad_de_contratacion, COUNT(modalidad_de_contratacion)",
                           group="modalidad_de_contratacion",
                           limit=30)
        for r in results:
            count = r.get(f"COUNT_modalidad_de_contratacion", "?")
            mod = r.get("modalidad_de_contratacion", "N/A")
            print(f"  {mod[:60]:62s} {count:>10s}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Top 10 entidades
    print("--- Top 10 entidades ---")
    try:
        results = client.get(dataset_id,
                           select="nombre_entidad, COUNT(nombre_entidad)",
                           group="nombre_entidad",
                           order="COUNT(nombre_entidad) DESC",
                           limit=10)
        for r in results:
            count = r.get(f"COUNT_nombre_entidad", "?")
            ent = r.get("nombre_entidad", "N/A")
            print(f"  {ent[:60]:62s} {count:>10s}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Top 10 departamentos
    print("--- Top 10 departamentos ---")
    try:
        results = client.get(dataset_id,
                           select="departamento_entidad, COUNT(departamento_entidad)",
                           group="departamento_entidad",
                           order="COUNT(departamento_entidad) DESC",
                           limit=10)
        for r in results:
            count = r.get(f"COUNT_departamento_entidad", "?")
            dep = r.get("departamento_entidad", "N/A")
            print(f"  {dep[:40]:42s} {count:>10s}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Procesos sin contrato (sin fecha de firma)
    print("--- Procesos sin contrato (sin fecha de firma) ---")
    try:
        results = client.get(dataset_id,
                           select="COUNT(*)",
                           where="fecha_de_firma_del_contrato IS NULL",
                           limit=1)
        total_sin = int(results[0]["COUNT"]) if results else 0
        print(f"  Sin fecha de firma: {total_sin:,} de {total:,} ({100*total_sin/total:.1f}%)" if total else f"  Sin fecha de firma: {total_sin:,}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Muestra general
    print(f"--- Muestra general ({args.sample} filas) ---")
    try:
        results = client.get(dataset_id, limit=args.sample, order="fecha_de_cargue_en_el_secop DESC")
        print(f"  Obtenidas: {len(results)} filas")
        for i, row in enumerate(results):
            print(f"\n  Fila {i+1}:")
            # Mostrar campos clave primero, luego el resto
            key_fields = [
                "numero_de_proceso", "numero_de_contrato", "nombre_entidad",
                "estado_del_proceso", "modalidad_de_contratacion",
                "cuantia_proceso", "cuantia_contrato",
                "fecha_de_cargue_en_el_secop", "fecha_de_firma_del_contrato",
                "departamento_entidad", "nom_razon_social_contratista",
            ]
            for k in key_fields:
                if k in row:
                    print(f"    {k}: {row[k]}")
            # Resto
            for k, v in sorted(row.items()):
                if k not in key_fields:
                    val_str = str(v)[:100]
                    if val_str.strip():
                        print(f"    {k}: {val_str}")
    except Exception as e:
        print(f"  Error: {e}")
    print()

    # Guardar fixture
    fixtures_dir = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    try:
        sample_results = client.get(dataset_id, limit=50, order="fecha_de_cargue_en_el_secop DESC")
        fixture_path = fixtures_dir / "secop1_procesos_sample.json"
        with open(fixture_path, "w", encoding="utf-8") as f:
            json.dump(sample_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"Fixture guardado: {fixture_path} ({len(sample_results)} registros)")
    except Exception as e:
        print(f"Error guardando fixture: {e}")

    client.close()
    print("\nInspección completada.")


if __name__ == "__main__":
    main()
