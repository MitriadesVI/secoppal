#!/usr/bin/env python3
"""
SECOPPAL — SENA Entity Audit

Read-only audit of all SENA entities in SECOP II (procesos + contratos).
Classifies each entity into: national, national_secretaria_general,
national_direccion_general, regional_parent, regional_admin_group,
training_center, intercentros, needs_review.

Output: docs/gazetteer/SENA_ENTITY_AUDIT.md
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter
from datetime import date
from pathlib import Path

# Ensure project root on path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.config import get_settings
from app.core.secop_client import SecopClient

OUTPUT_DIR = _PROJECT_ROOT / "docs" / "gazetteer"
OUTPUT_PATH = OUTPUT_DIR / "SENA_ENTITY_AUDIT.md"

DEPARTAMENTOS_COLOMBIA = {
    "amazonas", "antioquia", "arauca", "atlantico", "bolivar",
    "boyaca", "caldas", "caqueta", "casanare", "cauca", "cesar",
    "choco", "cordoba", "cundinamarca", "guainia", "guaviare",
    "huila", "laguajira", "magdalena", "meta", "narino",
    "nortedesantander", "putumayo", "quindio", "risaralda",
    "sanandres", "santander", "sucre", "tolima", "valle",
    "vaupes", "vichada", "bogota", "distritocapital",
}


def classify_sena_entity(name: str, dept: str | None) -> str:
    """Classify a SENA entity by its official name pattern."""
    upper = name.upper()

    # National levels
    if "SECRETARIA GENERAL" in upper:
        return "national_secretaria_general"
    if "DIRECCIÓN GENERAL" in upper or "DIRECCION GENERAL" in upper:
        return "national_direccion_general"

    # Regional admin groups
    if "SENA REGIONAL" in upper and ("GRUPO" in upper or "GRUPO DE APOYO" in upper or "GRUPO ADMINISTRATIVO" in upper):
        return "regional_admin_group"

    # Regional parent
    if "SENA REGIONAL" in upper:
        return "regional_parent"

    # Intercentros
    if "INTERCENTROS" in upper:
        return "intercentros"

    # Training centers
    if "CENTRO" in upper and "SENA" in upper:
        # Check if it's a real training center vs. admin group
        center_keywords = ["CENTRO AGROPECUARIO", "CENTRO DE", "CENTRO INDUSTRIAL",
                          "CENTRO TECNOLOGICO", "CENTRO DE FORMACION", "CENTRO COMERCIAL",
                          "CENTRO DE SERVICIOS", "CENTRO DE DISEÑO", "CENTRO DE ELECTRICIDAD"]
        if any(kw in upper for kw in center_keywords):
            return "training_center"

    # Other regional with SENA
    if "SENA" in upper:
        if not dept or dept in ("No Definido", ""):
            return "needs_review"
        return "regional_parent"

    return "needs_review"


def extract_department(name: str, dept_field: str | None) -> str | None:
    """Try to extract department from entity name."""
    if dept_field and dept_field not in ("No Definido", ""):
        return dept_field

    upper = name.upper()
    # Check for department mentions in name
    for colombian_dept in [
        "AMAZONAS", "ANTIOQUIA", "ARAUCA", "ATLANTICO", "BOLIVAR",
        "BOYACA", "CALDAS", "CAQUETA", "CASANARE", "CAUCA", "CESAR",
        "CHOCO", "CORDOBA", "CUNDINAMARCA", "GUAINIA", "GUAVIARE",
        "HULA", "HUILA", "LAGUAJIRA", "MAGDALENA", "META", "NARINO",
        "NORTE DE SANTANDER", "PUTUMAYO", "QUINDIO", "RISARALDA",
        "SAN ANDRES", "SANTANDER", "SUCRE", "TOLIMA", "VALLE",
        "VAUPES", "VICHADA", "BOGOTA", "DISTRITO CAPITAL",
    ]:
        if colombian_dept in upper:
            return colombian_dept.title()

    return None


def main():
    settings = get_settings()
    client = SecopClient(
        domain=settings.datos_gov_domain,
        app_token=settings.secop_app_token,
        timeout=settings.secop_timeout_seconds,
    )

    print("Auditando entidades SENA en SECOP II...", file=sys.stderr)

    # Query procesos
    print("  [1/2] Consultando procesos...", file=sys.stderr)
    procesos = client.query("p6dx-8zbt", """
        SELECT entidad, departamento_entidad, COUNT(*) AS total
        WHERE UPPER(entidad) LIKE '%SENA%'
        GROUP BY entidad, departamento_entidad
        ORDER BY total DESC
        LIMIT 200
    """)

    # Query contratos
    print("  [2/2] Consultando contratos...", file=sys.stderr)
    contratos = client.query("jbjy-vk9h", """
        SELECT nombre_entidad, departamento, COUNT(*) AS total
        WHERE UPPER(nombre_entidad) LIKE '%SENA%'
        GROUP BY nombre_entidad, departamento
        ORDER BY total DESC
        LIMIT 200
    """)

    # Merge by name
    entity_map: dict[str, dict] = {}
    for row in procesos:
        name = (row.get("entidad") or "").strip()
        if not name:
            continue
        dept = (row.get("departamento_entidad") or "").strip()
        total = int(row.get("total", 0))
        key = name.upper()
        if key not in entity_map:
            entity_map[key] = {
                "official_name": name,
                "departamento": dept or "No Definido",
                "total_procesos": total,
                "total_contratos": 0,
                "source_datasets": ["procesos"],
            }
        else:
            entity_map[key]["total_procesos"] += total
            if "procesos" not in entity_map[key]["source_datasets"]:
                entity_map[key]["source_datasets"].append("procesos")

    for row in contratos:
        name = (row.get("nombre_entidad") or "").strip()
        if not name:
            continue
        dept = (row.get("departamento") or "").strip()
        total = int(row.get("total", 0))
        key = name.upper()
        if key not in entity_map:
            entity_map[key] = {
                "official_name": name,
                "departamento": dept or "No Definido",
                "total_procesos": 0,
                "total_contratos": total,
                "source_datasets": ["contratos"],
            }
        else:
            entity_map[key]["total_contratos"] += total
            if "contratos" not in entity_map[key]["source_datasets"]:
                entity_map[key]["source_datasets"].append("contratos")

    # Sort by volume
    entities = sorted(
        entity_map.values(),
        key=lambda e: e["total_procesos"] + e["total_contratos"],
        reverse=True,
    )

    # Classify
    classified: dict[str, list[dict]] = {
        "national_secretaria_general": [],
        "national_direccion_general": [],
        "regional_parent": [],
        "regional_admin_group": [],
        "training_center": [],
        "intercentros": [],
        "needs_review": [],
    }
    for entity in entities:
        classification = classify_sena_entity(
            entity["official_name"], entity.get("departamento")
        )
        bucket = classified.setdefault(classification, [])
        bucket.append(entity)

    # Also attempt to extract dept for needs_review
    for entity in classified["needs_review"]:
        extracted = extract_department(
            entity["official_name"], entity.get("departamento")
        )
        if extracted and entity.get("departamento", "No Definido") == "No Definido":
            entity["departamento"] = extracted

    total = len(entities)
    total_vol = sum(e["total_procesos"] + e["total_contratos"] for e in entities)

    # Count unique departments covered
    depts_covered = {e.get("departamento", "No Definido") for e in entities
                     if e.get("departamento", "No Definido") != "No Definido"}

    # === Generate report ===
    lines = [
        f"# SENA Entity Audit — {date.today().isoformat()}",
        "",
        "**Estado:** Read-only audit contra SECOP II live",
        "**Fuentes:** Procesos (p6dx-8zbt) + Contratos (jbjy-vk9h)",
        "",
        "---",
        "",
        "## Resumen",
        "",
        f"| Metrica | Valor |",
        f"|---------|-------|",
        f"| Total entidades SENA unicas | {total} |",
        f"| Volumen total (procesos + contratos) | {total_vol:,} |",
        f"| Departamentos cubiertos | {len(depts_covered)} |",
        f"| Clasificaciones | {len([c for c,v in classified.items() if v])} tipos con datos |",
        "",
        "## Clasificacion por Tipo",
        "",
        "| Tipo | Cantidad | Volumen total | Descripcion |",
        "|------|----------|---------------|-------------|",
    ]
    type_descriptions = {
        "national_secretaria_general": "SENA Secretaria General (nivel nacional)",
        "national_direccion_general": "SENA Direccion General (nivel nacional)",
        "regional_parent": "SENA Regional (entidad principal por departamento)",
        "regional_admin_group": "Grupo Administrativo Regional",
        "training_center": "Centro de Formacion SENA",
        "intercentros": "Grupo Intercentros",
        "needs_review": "Requiere revision (nombre atipico o sin departamento)",
    }
    for ctype in ["national_secretaria_general", "national_direccion_general",
                   "regional_parent", "regional_admin_group", "training_center",
                   "intercentros", "needs_review"]:
        entries = classified.get(ctype, [])
        if entries:
            vol = sum(e["total_procesos"] + e["total_contratos"] for e in entries)
            desc = type_descriptions.get(ctype, "")
            lines.append(f"| {ctype} | {len(entries)} | {vol:,} | {desc} |")

    lines.extend([
        "",
        "## Top 20 por Volumen",
        "",
        "| # | Volumen | Nombre Oficial | Departamento | Clasificacion |",
        "|---|---------|---------------|-------------|---------------|",
    ])
    for i, entity in enumerate(entities[:20], 1):
        vol = entity["total_procesos"] + entity["total_contratos"]
        name = entity["official_name"][:70]
        dept = entity.get("departamento", "N/A")[:25]
        cls = classify_sena_entity(entity["official_name"], entity.get("departamento"))
        lines.append(f"| {i} | {vol:>7} | {name} | {dept} | {cls} |")

    # Regional parents by dept
    regional_parents = classified.get("regional_parent", [])
    lines.extend([
        "",
        "## Regionales por Departamento",
        "",
        "| Departamento | Cantidad | Volumen |",
        "|--------------|----------|---------|",
    ])
    dept_summary: dict[str, dict] = {}
    for e in regional_parents:
        d = e.get("departamento", "Otro")
        if d not in dept_summary:
            dept_summary[d] = {"count": 0, "volume": 0}
        dept_summary[d]["count"] += 1
        dept_summary[d]["volume"] += e["total_procesos"] + e["total_contratos"]
    for d in sorted(dept_summary.keys()):
        info = dept_summary[d]
        lines.append(f"| {d} | {info['count']} | {info['volume']:,} |")

    # Needs review
    needs_review = classified.get("needs_review", [])
    if needs_review:
        lines.extend([
            "",
            "## Needs Review",
            "",
            "| # | Volumen | Nombre Oficial | Departamento |",
            "|---|---------|---------------|-------------|",
        ])
        for i, entity in enumerate(needs_review[:30], 1):
            vol = entity["total_procesos"] + entity["total_contratos"]
            name = entity["official_name"][:70]
            dept = entity.get("departamento", "N/A")[:25]
            lines.append(f"| {i} | {vol:>7} | {name} | {dept} |")

    lines.extend([
        "",
        "## Observaciones",
        "",
        "- SENA tiene entidades regionales que pueden tener nombre distinto en procesos vs contratos",
        "- Algunas regionales aparecen como `SENA REGIONAL {DEPT}` y otras como `SENA {DEPT}//`",
        "- Los grupos administrativos (Grupo Administrativo CBA, CDA, etc.) son variantes internas",
        "- Centros de formacion (Centro Agropecuario, Centro Industrial, etc.) existen por regional",
        "- Intercentros son grupos administrativos multi-centro",
        "- NIT base frecuente: 899999034, pero varía por regional y centro",
        "- `needs_review` incluye entidades sin departamento o con nombres atípicos",
        "",
        "## Recomendaciones",
        "",
        "1. Agrupar SENA + departamento como ecosistema, no como entidad unica",
        "2. No fusionar los nombres oficiales — cada resultado conserva su official_name real",
        "3. Para busqueda regional, usar LIKE con contains_all (SENA + DEPARTAMENTO)",
        "4. Para macro-region, expandir a lista de departamentos con LIKE compuesto",
        "5. Secretaria General y Direccion General son entidades nacionales aparte",
        "",
    ])

    output = "\n".join(lines)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(output, encoding="utf-8")
    print(f"\nAudit guardado en {OUTPUT_PATH}", file=sys.stderr)
    print(f"Total entidades SENA: {total}", file=sys.stderr)
    print(f"Departamentos cubiertos: {len(depts_covered)}", file=sys.stderr)
    print(f"Needs review: {len(needs_review)}", file=sys.stderr)


if __name__ == "__main__":
    main()
