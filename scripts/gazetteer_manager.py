#!/usr/bin/env python3
"""
SECOPPAL Gazetteer Manager — CLI for incremental gazetteer expansion.

Usage:
    python scripts/gazetteer_manager.py diff --top 1000 [--format markdown|json]
    python scripts/gazetteer_manager.py append --from-list PATH [--aliases heuristic|llm]
    python scripts/gazetteer_manager.py validate [--tests TEST_PATH]
    python scripts/gazetteer_manager.py stats [--details]

Phases:
    G1 — diff + stats (read-only, no modification of aliases_db.json)
    G2 — heuristic alias generator
    G3 — validate / collision scan
    G4 — Lote 00: alias hygiene
    G5 — Lote 01: expansion mixta
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

# Ensure project root is on the path so app imports work
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from app.config import get_settings
from app.core.secop_client import SecopClient

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATASETS = {
    "procesos": {
        "id": "p6dx-8zbt",
        "entity_col": "entidad",
        "dept_col": "departamento_entidad",
        "vol_col": "COUNT(*)",
    },
    "contratos": {
        "id": "jbjy-vk9h",
        "entity_col": "nombre_entidad",
        "dept_col": "departamento",
        "vol_col": "COUNT(*)",
    },
}

DEFAULT_TOP = 1000
DEFAULT_ALIASES_DB = _PROJECT_ROOT / "app" / "data" / "aliases_db.json"

# Tokens que NUNCA deben aparecer como alias unico
FORBIDDEN_GENERIC_ALIASES: set[str] = {
    "alcaldia", "alcaldía",
    "municipio",
    "gobernacion", "gobernación",
    "ministerio",
    "secretaria", "secretaría",
    "corporacion", "corporación",
    "hospital",
    "fondo",
    "instituto",
    "empresa",
    "entidad",
    "distrito",
    "agencia",
}

FORBIDDEN_WITH_ARTICLE: set[str] = {
    f"el {t}" for t in FORBIDDEN_GENERIC_ALIASES
} | {
    f"la {t}" for t in FORBIDDEN_GENERIC_ALIASES
} | {
    f"los {t}" for t in FORBIDDEN_GENERIC_ALIASES
} | {
    f"las {t}" for t in FORBIDDEN_GENERIC_ALIASES
}

ALL_FORBIDDEN = FORBIDDEN_GENERIC_ALIASES | FORBIDDEN_WITH_ARTICLE

# Deuda histórica de aliases reclamados por más de una entidad. El runtime sólo
# puede honrar a UNA: `EntityResolver._load_aliases` sobrescribe (gana la última
# del JSON) y `_scan_index` desempata por longitud (gana la primera). Quién de
# las dos responde no es una decisión de nadie.
#
# El baseline sale de GAZETTEER_ALIAS_OWNERSHIP_R1C tras retirar los 114 aliases
# con ownership demostrablemente ajeno: 217 grupos antes, 173 después. No se
# convierte el backlog en un fallo global —rompería el baseline por deuda
# conocida— pero cualquier entrada nueva que lo aumente falla. El tope BAJA
# cuando se salda deuda; no sube.
EXACT_MULTI_OWNER_BASELINE = 173

# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------


def infer_entity_type(official_name: str) -> str:
    """Infer entity type from the official SECOP name."""
    upper = official_name.upper()

    if "ALCALDIA" in upper and "MUNICIPAL" in upper:
        return "alcaldia_municipal"
    if "ESE" in upper or ("HOSPITAL" in upper and "ESE" not in upper):
        return "ese_hospital"
    if "SENA" in upper and "REGIONAL" in upper:
        return "sena_regional"
    if "ICBF" in upper and "REGIONAL" in upper:
        return "icbf_regional"
    if "SECRETARIA" in upper or "SECRETARÍA" in upper:
        return "secretaria"
    if "FONDO" in upper:
        return "fondo"
    if "CORPORACION" in upper or "CORPORACIÓN" in upper:
        if "AUTONOMA" in upper or "AUTÓNOMA" in upper:
            return "corporacion_autonoma"
        return "corporacion"
    if "MINISTERIO" in upper:
        return "ministerio"
    if "SUPERINTENDENCIA" in upper:
        return "superintendencia"
    if "GOBERNACION" in upper or "GOBERNACIÓN" in upper:
        return "gobernacion"
    if "ALCALDIA" in upper:
        return "alcaldia_municipal"
    if "HOSPITAL" in upper:
        return "ese_hospital"
    if "INSTITUTO" in upper:
        return "instituto"

    return "otro"


def compute_priority(entity: dict, entities_per_dept: dict[str, int]) -> str:
    """P0 = top volume, P1 = dept with <5 entities, P2 = rest."""
    dept = entity.get("departamento", "N/A")
    if entities_per_dept.get(dept, 0) < 5:
        return "P1"
    return "P2"


# ---------------------------------------------------------------------------
# Fetch entities from SECOP
# ---------------------------------------------------------------------------


def fetch_top_entities(
    client: SecopClient, top_n: int = DEFAULT_TOP
) -> list[dict]:
    """Fetch top N entities from both datasets, merge by official_name."""
    entity_map: dict[str, dict] = {}

    for ds_name, ds_info in DATASETS.items():
        print(f"[fetch] Consultando {ds_name}...", file=sys.stderr)
        query = (
            f"SELECT {ds_info['entity_col']}, {ds_info['dept_col']}, "
            f"{ds_info['vol_col']} AS total "
            f"GROUP BY {ds_info['entity_col']}, {ds_info['dept_col']} "
            f"ORDER BY total DESC LIMIT {top_n}"
        )
        rows = client.query(ds_info["id"], query)

        for row in rows:
            name = (row.get(ds_info["entity_col"]) or "").strip()
            if not name:
                continue
            dept = (row.get(ds_info["dept_col"]) or "").strip()
            total = int(row.get("total", 0))

            key = name.upper()
            if key not in entity_map:
                entity_map[key] = {
                    "official_name": name,
                    "departamento": dept,
                    "total_procesos": 0,
                    "total_contratos": 0,
                    "source_datasets": [],
                }
            entity_map[key]["total_procesos" if ds_name == "procesos" else "total_contratos"] += total
            if ds_name not in entity_map[key]["source_datasets"]:
                entity_map[key]["source_datasets"].append(ds_name)

    entities = sorted(
        entity_map.values(),
        key=lambda e: e["total_procesos"] + e["total_contratos"],
        reverse=True,
    )[:top_n]

    print(f"[fetch] {len(entities)} entidades unicas encontradas", file=sys.stderr)
    return entities


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


def do_diff(
    client: SecopClient,
    aliases_db: list[dict],
    top_n: int = DEFAULT_TOP,
    fmt: str = "markdown",
) -> str:
    """Compare gazetteer against SECOP live, return gap report."""
    entities = fetch_top_entities(client, top_n=top_n)

    existing_officials = {e.get("official_name", "").upper() for e in aliases_db}

    # Compute entities per dept for priority
    dept_counts: dict[str, int] = {}
    for e in aliases_db:
        d = e.get("departamento", "N/A")
        dept_counts[d] = dept_counts.get(d, 0) + 1

    missing = []
    for entity in entities:
        if entity["official_name"].upper() not in existing_officials:
            entity["entity_type"] = infer_entity_type(entity["official_name"])
            entity["priority"] = compute_priority(entity, dept_counts)
            entity["volume"] = entity["total_procesos"] + entity["total_contratos"]
            missing.append(entity)

    # Count how many are in coverage
    total_from_secop = {e["official_name"].upper() for e in entities}
    covered = len(total_from_secop & existing_officials)
    total = len(entities)

    if fmt == "json":
        return json.dumps(
            {
                "fetched": total,
                "covered": covered,
                "gap": len(missing),
                "entities": missing,
            },
            ensure_ascii=False,
            indent=2,
        )

    # Markdown format
    lines = [
        f"# Gap Report — Gazetteer vs SECOP Top-{top_n}",
        f"**Fecha:** {date.today().isoformat()}",
        f"**Gazetteer actual:** {len(aliases_db)} entidades",
        f"**SECOP top-{top_n}:** {total} entidades unicas",
        f"**Cubiertas:** {covered}/{total} ({100*covered//total:.0f}%)",
        f"**Faltantes:** {len(missing)}",
        "",
        "## Top 50 Faltantes por Volumen",
        "",
        "| # | Volumen | Nombre Oficial | Departamento | Tipo | Prioridad |",
        "|---|--------|----------------|-------------|------|-----------|",
    ]

    for i, entity in enumerate(missing[:50], 1):
        vol = entity["volume"]
        name = entity["official_name"][:65]
        dept = entity.get("departamento", "N/A")[:25]
        etype = entity.get("entity_type", "?")
        prio = entity.get("priority", "P2")
        lines.append(f"| {i} | {vol:>7} | {name} | {dept} | {etype} | {prio} |")

    lines.append("")
    lines.append(f"## Departamentos con <5 Entidades ({len([d for d,c in dept_counts.items() if c < 5])})")
    lines.append("")
    lines.append("| Departamento | Entidades en gazetteer |")
    lines.append("|-------------|----------------------|")
    for dept, count in sorted(dept_counts.items()):
        if count < 5:
            lines.append(f"| {dept} | {count} |")

    lines.append("")
    lines.append(f"## Por Tipo de Entidad Faltante")
    lines.append("")
    lines.append("| Tipo | Cantidad | Heuristico? |")
    lines.append("|------|----------|-------------|")
    types_that_need_llm = {"otro", "instituto", "corporacion"}
    type_counts: dict[str, int] = {}
    for e in missing:
        t = e.get("entity_type", "otro")
        type_counts[t] = type_counts.get(t, 0) + 1
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        heuristic = "NO (LLM)" if t in types_that_need_llm else "SI"
        lines.append(f"| {t} | {c} | {heuristic} |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Heuristic alias generation
# ---------------------------------------------------------------------------


def generate_heuristic_aliases(
    official_name: str,
    entity_type: str | None = None,
    departamento: str | None = None,
) -> list[str]:
    """
    Generate natural-language aliases from patterns.
    Returns a list of lowercase, deduplicated aliases.
    """
    if entity_type is None:
        entity_type = infer_entity_type(official_name)

    name_upper = official_name.upper()
    aliases: set[str] = set()

    # Always include the official name in lowercase
    aliases.add(official_name.lower().strip())

    def _extract_name(upper_name: str, prefixes: list[str]) -> str | None:
        """Extract the core name after removing known prefixes."""
        for prefix in prefixes:
            if upper_name.startswith(prefix):
                rest = upper_name.removeprefix(prefix).strip()
                # Remove trailing suffixes like "BOLIVAR", "//", "(oficial)", "- FONDO..."
                for suffix in [" //", " (OFICIAL)", " - FONDO DE DESARROLLO LOCAL"]:
                    if rest.endswith(suffix):
                        rest = rest[: -len(suffix)]
                rest = rest.strip()
                if rest:
                    return rest.lower()
        return None

    def _normalize(name: str) -> str:
        """Remove diacritics for alias matching."""
        import unicodedata
        return unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")

    if entity_type == "alcaldia_municipal":
        name = _extract_name(name_upper, [
            "ALCALDIA MUNICIPAL DE ", "ALCALDIA MUNICIPAL ",
            "ALCALDIA DE ", "ALCALDIA ",
            "MUNICIPIO DE ", "MUNICIPIO ",
        ])
        if name:
            patterns = [
                f"alcaldia de {name}", f"alcaldia {name}",
                f"municipio de {name}", f"municipio {name}",
                f"alcaldia municipal de {name}",
                f"{name} alcaldia", f"{name} municipio",
            ]
            for p in patterns:
                aliases.add(p)
                aliases.add(_normalize(p))

    elif entity_type == "ese_hospital":
        # Try ESE HOSPITAL X, ESE X, HOSPITAL X
        for prefix in ["ESE HOSPITAL ", "HOSPITAL ", "ESE ", "CLINICA ", "CENTRO DE SALUD "]:
            name = _extract_name(name_upper, [prefix])
            if name:
                base = name
                for p in [f"ese {base}", f"hospital {base}", f"clinica {base}", f"centro de salud {base}"]:
                    aliases.add(p)
                    aliases.add(_normalize(p))

    elif entity_type == "sena_regional":
        for prefix in ["SENA REGIONAL ", "SENA ", "SERVICIO NACIONAL DE APRENDIZAJE SENA REGIONAL ", "SERVICIO NACIONAL DE APRENDIZAJE SENA "]:
            region = _extract_name(name_upper, [prefix])
            if region:
                region = region.replace("grupo administrativo", "").replace("grupo de apoyo", "").replace("grupo administrativo mixto", "").replace("grupo de apoyo administrativo mixto", "")
                import re as _re
                region = _re.sub(r'\s+', ' ', region).strip()
                # Strip trailing/leading short codes like "CBA", "CIAA" (only 2-4 char tokens, not real region names)
                region = _re.sub(r'\s+[a-z0-9]{2,4}$', '', region)
                region = _re.sub(r'^[a-z0-9]{2,4}\s+', '', region).strip()
                for p in [
                    f"sena {region}", f"sena regional {region}",
                    f"servicio nacional de aprendizaje {region}",
                    f"regional {region} sena",
                ]:
                    aliases.add(p)
                    aliases.add(_normalize(p))

    elif entity_type == "icbf_regional":
        for prefix in ["ICBF REGIONAL ", "ICBF "]:
            region = _extract_name(name_upper, [prefix])
            if region:
                for p in [
                    f"icbf {region}", f"icbf regional {region}",
                    f"instituto colombiano de bienestar familiar {region}",
                    f"bienestar familiar {region}",
                ]:
                    aliases.add(p)
                    aliases.add(_normalize(p))

    elif entity_type == "secretaria":
        for prefix in ["SECRETARIA DE ", "SECRETARÍA DE ", "SECRETARIA ", "SECRETARÍA "]:
            name = _extract_name(name_upper, [prefix])
            if name:
                for p in [
                    f"secretaria de {name}", f"secretaria {name}",
                    f"secretaria de {_normalize(name)}",
                ]:
                    aliases.add(p)

    elif entity_type == "fondo":
        for prefix in ["FONDO DE ", "FONDO "]:
            name = _extract_name(name_upper, [prefix])
            if name:
                for p in [
                    f"fondo de {name}", f"fondo {name}",
                    f"{name} fondo",
                ]:
                    aliases.add(p)
                    aliases.add(_normalize(p))

    elif entity_type in ("corporacion_autonoma", "corporacion"):
        for prefix in ["CORPORACION AUTONOMA REGIONAL DE ", "CORPORACION AUTONOMA DE ",
                       "CORPORACION AUTÓNOMA REGIONAL DE ", "CORPORACIÓN AUTÓNOMA REGIONAL DE ",
                       "CORPORACION DE ", "CORPORACION "]:
            name = _extract_name(name_upper, [prefix])
            if name:
                for p in [
                    f"corporacion {name}", f"corp {name}",
                    f"autoridad ambiental {name}",
                    f"corporacion autonoma {name}",
                ]:
                    aliases.add(p)
                    aliases.add(_normalize(p))

    elif entity_type == "ministerio":
        for prefix in ["MINISTERIO DE ", "MINISTERIO "]:
            name = _extract_name(name_upper, [prefix])
            if name:
                for p in [
                    f"ministerio de {name}", f"ministerio {name}",
                    f"min {name}",
                ]:
                    aliases.add(p)
                    aliases.add(_normalize(p))

    elif entity_type == "superintendencia":
        for prefix in ["SUPERINTENDENCIA NACIONAL DE ", "SUPERINTENDENCIA DE ",
                       "SUPERINTENDENCIA NACIONAL ", "SUPERINTENDENCIA "]:
            name = _extract_name(name_upper, [prefix])
            if name:
                for p in [
                    f"superintendencia de {name}", f"superintendencia {name}",
                    f"sup {name}",
                ]:
                    aliases.add(p)
                    aliases.add(_normalize(p))

    elif entity_type == "gobernacion":
        for prefix in ["GOBERNACION DEL ", "GOBERNACIÓN DEL ",
                       "GOBERNACION DE ", "GOBERNACIÓN DE ",
                       "GOBERNACION ", "GOBERNACIÓN ",
                       "DEPARTAMENTO DE ", "DEPARTAMENTO "]:
            name = _extract_name(name_upper, [prefix])
            if name:
                for p in [
                    f"gobernacion de {name}", f"gobernacion {name}",
                    f"gobernacion del {name}",
                    f"departamento de {name}",
                    f"{name} gobernacion",
                ]:
                    aliases.add(p)
                    aliases.add(_normalize(p))

    # Filter out forbidden single-generic aliases
    filtered = set()
    for alias in aliases:
        alias_stripped = alias.strip().lower()
        if alias_stripped in ALL_FORBIDDEN:
            continue
        filtered.add(alias_stripped)

    return sorted(filtered)


# ---------------------------------------------------------------------------
# Append
# ---------------------------------------------------------------------------


def do_append(
    aliases_db_path: Path,
    from_list_path: Path,
    alias_mode: str = "heuristic",
) -> dict:
    """Append entities from a list JSON file to the gazetteer."""
    # Load existing gazetteer
    with aliases_db_path.open("r", encoding="utf-8") as f:
        data: list[dict] = json.load(f)

    existing_officials = {e.get("official_name", "").upper() for e in data}

    # Load new entities
    with from_list_path.open("r", encoding="utf-8") as f:
        new_entities: list[dict] = json.load(f)

    added = 0
    skipped = 0
    for entity in new_entities:
        name = entity.get("official_name", "")
        if name.upper() in existing_officials:
            print(f"  SKIP: {name[:60]}")
            skipped += 1
            continue

        # Infer entity_type if not provided
        if "entity_type" not in entity:
            entity["entity_type"] = infer_entity_type(name)

        # Generate aliases if not provided
        if not entity.get("aliases"):
            if alias_mode == "llm":
                print(f"  LLM MODE no implementado aun para: {name[:60]}")
                entity["aliases"] = [name.lower().strip()]
            else:
                entity["aliases"] = generate_heuristic_aliases(
                    name, entity.get("entity_type"), entity.get("departamento")
                )
                print(f"  ADD ({entity.get('entity_type', '?')}: {len(entity['aliases'])} aliases): {name[:60]}")

        # Ensure source_datasets
        if "source_datasets" not in entity:
            entity["source_datasets"] = entity.get("source_datasets", ["procesos"])

        data.append(entity)
        existing_officials.add(name.upper())
        added += 1

    # Backup + write
    backup_path = aliases_db_path.with_suffix(f".json.bak.{date.today().isoformat()}")
    shutil.copy2(aliases_db_path, backup_path)
    print(f"  Backup: {backup_path}")

    with aliases_db_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return {"added": added, "skipped": skipped, "total": len(data)}

def do_validate(
    aliases_db_path: Path,
    new_entities: list[dict] | None = None,
    test_path: str | None = None,
    full_scan: bool = False,
) -> dict:
    """Validate gazetteer integrity and optionally run tests.

    Args:
        aliases_db_path: Path to aliases_db.json
        new_entities: List of entities to check for collision
        test_path: Path to pytest tests to run
        full_scan: Scan EVERY alias of EVERY entity against the resolver
    """
    with aliases_db_path.open("r", encoding="utf-8") as f:
        data: list[dict] = json.load(f)

    errors: list[str] = []
    warnings: list[str] = []

    # 1. Check each entity has >= 3 aliases
    for entity in data:
        alias_count = len(entity.get("aliases", []))
        if alias_count < 3:
            warnings.append(
                f"WARN: {entity['official_name'][:60]} tiene solo {alias_count} aliases"
            )

    # 2. Check for forbidden generic aliases
    for entity in data:
        for alias in entity.get("aliases", []):
            alias_stripped = alias.strip().lower()
            if alias_stripped in ALL_FORBIDDEN:
                warnings.append(
                    f"WARN: '{alias_stripped}' es alias generico peligroso en {entity['official_name'][:60]}"
                )

    # 3. Collision scan (new entities vs existing)
    if new_entities:
        from app.core.entity_resolver import EntityResolver

        resolver = EntityResolver(aliases_db_path)
        for entity in new_entities:
            for alias in entity.get("aliases", []):
                result = resolver.scan_entity(alias)
                if result and result.official_name != entity["official_name"]:
                    errors.append(
                        f"COLISION: alias '{alias}' resuelve a "
                        f"'{result.official_name}' en vez de '{entity['official_name']}'"
                    )

    # 4. Full collision scan: check every alias of every entity
    if full_scan:
        from app.core.entity_resolver import EntityResolver

        print(f"  Full scan: cargando resolver...", file=sys.stderr)
        resolver = EntityResolver(aliases_db_path)
        total = len(data)
        collisions = 0
        for i, entity in enumerate(data):
            name = entity.get("official_name", "?")
            aliases = entity.get("aliases", [])
            for alias in aliases:
                result = resolver.scan_entity(alias)
                if result and result.official_name != name:
                    # Check if it's a self-collision (alias matches a different entity)
                    # First check if the alias IS the official name (legitimate self-ref)
                    if alias.lower().strip() == name.lower().strip():
                        continue
                    collisions += 1
                    errors.append(
                        f"COLISION: '{alias}' resuelve a "
                        f"'{result.official_name}' (esperaba '{name}')"
                    )
            if (i + 1) % 100 == 0:
                print(f"  Full scan: {i+1}/{total} entidades, {collisions} colisiones", file=sys.stderr)
        print(f"  Full scan completo: {total} entidades, {collisions} colisiones", file=sys.stderr)

    # 5. Ownership múltiple exacto: dos entidades reclamando la misma frase.
    #    No lo cubre el collision scan: `scan_entity` devuelve un solo ganador,
    #    así que una colisión cuyo ganador coincide con el dueño esperado pasa
    #    invisible. Este chequeo mira el dato, no la resolución.
    from app.utils.money import normalize_text

    owners_by_alias: dict[str, set[str]] = {}
    for entity in data:
        for alias in entity.get("aliases", []):
            key = normalize_text(alias)
            if key:
                owners_by_alias.setdefault(key, set()).add(entity["official_name"])
    multi_owner = {k: sorted(v) for k, v in owners_by_alias.items() if len(v) > 1}
    if len(multi_owner) > EXACT_MULTI_OWNER_BASELINE:
        errors.append(
            f"EXACT_MULTI_OWNER: {len(multi_owner)} grupos, baseline "
            f"{EXACT_MULTI_OWNER_BASELINE}. Un alias nuevo quedó reclamado por "
            f"más de una entidad; el runtime sólo puede honrar a una."
        )

    # 6. Check official_name is not empty
    for i, entity in enumerate(data):
        if not entity.get("official_name", "").strip():
            errors.append(f"ERROR: Entidad #{i} tiene official_name vacio")

    # 7. Run pytest if path provided
    test_result = None
    if test_path:
        result = subprocess.run(
            ["pytest", test_path, "-q", "--tb=short"],
            capture_output=True, text=True, timeout=120,
        )
        test_result = {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
        if result.returncode != 0:
            errors.append(f"TESTS FAILED ({result.returncode})")

    return {
        "total_entities": len(data),
        "total_aliases": sum(len(e.get("aliases", [])) for e in data),
        "exact_multi_owner_groups": len(multi_owner),
        "exact_multi_owner_baseline": EXACT_MULTI_OWNER_BASELINE,
        "errors": errors,
        "warnings": warnings,
        "test_result": test_result,
    }


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


def do_stats(aliases_db: list[dict], details: bool = False) -> str:
    """Generate gazetteer statistics."""
    total_entities = len(aliases_db)
    total_aliases = sum(len(e.get("aliases", [])) for e in aliases_db)
    with_aliases = sum(1 for e in aliases_db if e.get("aliases"))
    avg_aliases = total_aliases / with_aliases if with_aliases else 0

    # Department distribution
    dept_counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for e in aliases_db:
        d = e.get("departamento", "N/A")
        dept_counts[d] = dept_counts.get(d, 0) + 1
        t = infer_entity_type(e.get("official_name", ""))
        type_counts[t] = type_counts.get(t, 0) + 1
        for s in e.get("source_datasets", []):
            source_counts[s] = source_counts.get(s, 0) + 1

    # Coverage estimation (approximate — doesn't query SECOP)
    depts_with_lt5 = sum(1 for c in dept_counts.values() if c < 5)

    lines = [
        "# Gazetteer Stats",
        f"**Fecha:** {date.today().isoformat()}",
        "",
        f"| Metrica | Valor |",
        f"|---------|-------|",
        f"| Total entidades | {total_entities} |",
        f"| Total aliases | {total_aliases} |",
        f"| Promedio aliases/entidad | {avg_aliases:.1f} |",
        f"| Entidades sin aliases | {total_entities - with_aliases} |",
        f"| Departamentos cubiertos | {len(dept_counts)} |",
        f"| Departamentos con <5 entidades | {depts_with_lt5} |",
        "",
        f"| Fuente | Entidades |",
        f"|--------|-----------|",
    ]
    for s, c in sorted(source_counts.items()):
        lines.append(f"| {s} | {c} |")

    if details:
        lines.append("")
        lines.append("## Por Tipo de Entidad")
        lines.append("")
        lines.append("| Tipo | Cantidad |")
        lines.append("|------|----------|")
        for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
            lines.append(f"| {t} | {c} |")

        lines.append("")
        lines.append("## Top 10 Departamentos")
        lines.append("")
        lines.append("| Departamento | Entidades |")
        lines.append("|--------------|-----------|")
        for dept, count in sorted(dept_counts.items(), key=lambda x: -x[1])[:10]:
            lines.append(f"| {dept} | {count} |")

        lines.append("")
        lines.append("## Departamentos con <5 Entidades")
        lines.append("")
        lines.append("| Departamento | Entidades |")
        lines.append("|--------------|-----------|")
        for dept, count in sorted(dept_counts.items()):
            if count < 5:
                lines.append(f"| {dept} | {count} |")

        lines.append("")
        lines.append("## Top 10 Entidades por Aliases")
        lines.append("")
        lines.append("| # | Nombre | Aliases | Volumen |")
        lines.append("|---|--------|---------|---------|")
        sorted_by_aliases = sorted(aliases_db, key=lambda e: -len(e.get("aliases", [])))
        for i, e in enumerate(sorted_by_aliases[:10], 1):
            vol = e.get("total_procesos", 0) + e.get("total_contratos", 0)
            lines.append(f"| {i} | {e['official_name'][:60]} | {len(e.get('aliases', []))} | {vol:,} |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="SECOPPAL Gazetteer Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  diff         Compare gazetteer vs SECOP live — gap report
  append       Add entities to gazetteer (with backup)
  validate     Run integrity checks + optional pytest
  stats        Gazetteer statistics
  hygiene      Lote 00: remove dangerous generic aliases

Examples:
  python scripts/gazetteer_manager.py diff --top 1000
  python scripts/gazetteer_manager.py stats --details
  python scripts/gazetteer_manager.py validate --tests tests/test_entity_resolver.py
  python scripts/gazetteer_manager.py append --from-list scripts/lote_01.json --aliases heuristic
  python scripts/gazetteer_manager.py hygiene
        """,
    )
    sub = parser.add_subparsers(dest="command")

    # diff
    p_diff = sub.add_parser("diff", help="Compare gazetteer vs SECOP live")
    p_diff.add_argument("--top", type=int, default=DEFAULT_TOP, help=f"Top N entities (default: {DEFAULT_TOP})")
    p_diff.add_argument("--format", choices=["markdown", "json"], default="markdown", help="Output format")
    p_diff.add_argument("--output", type=str, default=None, help="Save output to file")

    # append
    p_append = sub.add_parser("append", help="Add entities to gazetteer")
    p_append.add_argument("--from-list", type=str, required=True, help="Path to JSON list of entities")
    p_append.add_argument("--aliases", choices=["heuristic", "llm"], default="heuristic", help="Alias generation mode")

    # validate
    p_valid = sub.add_parser("validate", help="Run integrity checks")
    p_valid.add_argument("--tests", type=str, default=None, help="pytest path (e.g. tests/test_entity_resolver.py)")
    p_valid.add_argument("--from-list", type=str, default=None, help="Check collision against new entities")
    p_valid.add_argument("--full-scan", action="store_true", help="Scan EVERY alias against ALL entities (slow but thorough)")

    # stats
    p_stats = sub.add_parser("stats", help="Gazetteer statistics")
    p_stats.add_argument("--details", action="store_true", help="Detailed breakdown")
    p_stats.add_argument("--output", type=str, default=None, help="Save output to file")

    # hygiene
    p_hygiene = sub.add_parser("hygiene", help="Lote 00: remove dangerous generic aliases")
    p_hygiene.add_argument("--dry-run", action="store_true", help="Show what would be removed without modifying")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Resolve paths
    settings = get_settings()
    aliases_db_path = _PROJECT_ROOT / settings.secop_alias_db_path

    # Load aliases_db.json
    if not aliases_db_path.exists():
        print(f"ERROR: no se encuentra {aliases_db_path}", file=sys.stderr)
        sys.exit(1)

    with aliases_db_path.open("r", encoding="utf-8") as f:
        aliases_data = json.load(f)

    # --- diff ---
    if args.command == "diff":
        client = SecopClient(
            domain=settings.datos_gov_domain,
            app_token=settings.secop_app_token,
            timeout=settings.secop_timeout_seconds,
        )
        result = do_diff(client, aliases_data, top_n=args.top, fmt=args.format)
        if args.output:
            out_path = Path(args.output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(result, encoding="utf-8")
            print(f"Diff guardado en {out_path}", file=sys.stderr)
        else:
            print(result)

    # --- append ---
    elif args.command == "append":
        from_list_path = Path(args.from_list)
        if not from_list_path.exists():
            print(f"ERROR: {from_list_path} no existe", file=sys.stderr)
            sys.exit(1)

        result = do_append(aliases_db_path, from_list_path, alias_mode=args.aliases)
        print(f"\nResultado: {result['added']} agregadas, {result['skipped']} skipped, {result['total']} total")

    # --- validate ---
    elif args.command == "validate":
        new_entities = None
        if args.from_list:
            fp = Path(args.from_list)
            if fp.exists():
                new_entities = json.loads(fp.read_text(encoding="utf-8"))
        result = do_validate(aliases_db_path, new_entities=new_entities, test_path=args.tests, full_scan=args.full_scan)

        print(f"Entidades: {result['total_entities']}")
        print(f"Aliases total: {result['total_aliases']:,}")
        print(
            f"Aliases con dueño múltiple: {result['exact_multi_owner_groups']} "
            f"(baseline {result['exact_multi_owner_baseline']})"
        )
        print(f"Errores: {len(result['errors'])}")
        for e in result["errors"]:
            print(f"  [ERROR] {e}")
        print(f"Warnings: {len(result['warnings'])}")
        for w in result["warnings"]:
            print(f"  [WARN] {w}")

        if result.get("test_result"):
            print(f"\nTests: returncode={result['test_result']['returncode']}")
            if result["test_result"]["returncode"] != 0:
                print(result["test_result"]["stdout"])

    # --- stats ---
    elif args.command == "stats":
        result = do_stats(aliases_data, details=args.details)
        if args.output:
            out_path = Path(args.output)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(result, encoding="utf-8")
            print(f"Stats guardados en {out_path}", file=sys.stderr)
        else:
            print(result)

    # --- hygiene ---
    elif args.command == "hygiene":
        removed_count = 0
        affected_entities = 0
        would_deplete = []

        for entity in aliases_data:
            original = entity.get("aliases", [])
            filtered = [
                a for a in original
                if a.strip().lower() not in ALL_FORBIDDEN
            ]
            if len(filtered) < 3:
                would_deplete.append((entity["official_name"][:60], len(original), len(filtered)))
                continue  # Don't remove if it leaves <3
            removed = len(original) - len(filtered)
            if removed > 0:
                affected_entities += 1
                removed_count += removed
                if not args.dry_run:
                    entity["aliases"] = filtered

        # Write if not dry-run
        if not args.dry_run and (removed_count > 0 or would_deplete):
            backup_path = aliases_db_path.with_suffix(f".json.bak.{date.today().isoformat()}")
            shutil.copy2(aliases_db_path, backup_path)
            print(f"Backup: {backup_path}", file=sys.stderr)
            with aliases_db_path.open("w", encoding="utf-8") as f:
                json.dump(aliases_data, f, ensure_ascii=False, indent=2)

        print(f"Aliases removidos: {removed_count}")
        print(f"Entidades afectadas: {affected_entities}")
        if args.dry_run:
            print("(dry-run — no se guardaron cambios)")
        if would_deplete:
            print(f"\nEntidades que quedarian con <3 aliases (NO se tocaron):")
            for name, orig, new in would_deplete:
                print(f"  {name}: {orig} → {new}")


if __name__ == "__main__":
    main()
