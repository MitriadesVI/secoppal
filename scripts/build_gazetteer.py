#!/usr/bin/env python3
"""
SECOPAL — Gazetteer Builder
============================

Extrae entidades únicas de SECOP II (ambos datasets), genera aliases
con DeepSeek, y produce aliases_db.json para el EntityResolver.

Uso:
    # Paso 1: Extraer entidades únicas de SECOP
    python scripts/build_gazetteer.py extract --top 500

    # Paso 2: Generar aliases con DeepSeek (requiere DEEPSEEK_API_KEY)
    python scripts/build_gazetteer.py generate-aliases

    # Paso 3: Todo junto
    python scripts/build_gazetteer.py full --top 500

Output: app/data/aliases_db.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

try:
    from sodapy import Socrata
except ImportError:
    print("ERROR: sodapy is required. Install with: pip install sodapy")
    sys.exit(1)

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATOS_GOV_DOMAIN = "www.datos.gov.co"
PROCESOS_DATASET = "p6dx-8zbt"
CONTRATOS_DATASET = "jbjy-vk9h"

OUTPUT_DIR = Path("app/data")
RAW_ENTITIES_FILE = OUTPUT_DIR / "entidades_raw.json"
ALIASES_DB_FILE = OUTPUT_DIR / "aliases_db.json"

ALIAS_GENERATION_PROMPT = """
Para la entidad oficial de contratación pública colombiana SECOP II:

Nombre oficial: "{entidad}"
Departamento: "{departamento}"

Genera una lista JSON de todas las formas como un colombiano podría
referirse a esta entidad en lenguaje natural cuando busca licitaciones
o contratos. Incluye:

- Nombre completo sin siglas
- Siglas y abreviaciones comunes
- Nombres coloquiales o informales
- Variaciones con y sin tildes
- Variaciones con y sin artículos (el, la, del, de la)
- Si es un municipio: "alcaldía de [nombre]", "municipio de [nombre]"
- Si es un departamento/gobernación: "gobernación de [nombre]", "gobierno de [nombre]"
- Si tiene siglas conocidas (SENA, ICBF, etc.): incluir solo la sigla

NO incluyas:
- El nombre oficial exacto (ya lo tenemos)
- Variaciones triviales solo de mayúsculas/minúsculas
- Entidades diferentes que tengan nombre parecido

Responde SOLO con un array JSON de strings, sin explicación.
Ejemplo: ["el sena", "servicio de aprendizaje", "sena colombia"]
""".strip()

# ---------------------------------------------------------------------------
# Paso 1: Extraer entidades únicas
# ---------------------------------------------------------------------------


def extract_entities(app_token: str | None, top_n: int = 500) -> list[dict]:
    """
    Consulta ambos datasets de SECOP y extrae las entidades únicas
    ordenadas por volumen de procesos (las más activas primero).
    """
    client = Socrata(DATOS_GOV_DOMAIN, app_token=app_token, timeout=60)

    print(f"[1/3] Consultando entidades del dataset de Procesos ({PROCESOS_DATASET})...")
    procesos_entities = client.get(
        PROCESOS_DATASET,
        query=f"""
            SELECT entidad, departamento_entidad, COUNT(*) as total
            GROUP BY entidad, departamento_entidad
            ORDER BY total DESC
            LIMIT {top_n}
        """,
    )
    print(f"       → {len(procesos_entities)} entidades encontradas en Procesos")

    print(f"[2/3] Consultando entidades del dataset de Contratos ({CONTRATOS_DATASET})...")
    contratos_entities = client.get(
        CONTRATOS_DATASET,
        query=f"""
            SELECT nombre_entidad, departamento, COUNT(*) as total
            GROUP BY nombre_entidad, departamento
            ORDER BY total DESC
            LIMIT {top_n}
        """,
    )
    print(f"       → {len(contratos_entities)} entidades encontradas en Contratos")

    # Merge: usar nombre como key, acumular totales
    entity_map: dict[str, dict] = {}

    for row in procesos_entities:
        name = (row.get("entidad") or "").strip()
        if not name:
            continue
        dept = (row.get("departamento_entidad") or "").strip()
        total = int(row.get("total", 0))

        key = name.upper()
        if key not in entity_map:
            entity_map[key] = {
                "official_name": name,
                "departamento": dept,
                "total_procesos": total,
                "total_contratos": 0,
                "source_datasets": ["procesos"],
            }
        else:
            entity_map[key]["total_procesos"] += total
            if "procesos" not in entity_map[key]["source_datasets"]:
                entity_map[key]["source_datasets"].append("procesos")

    for row in contratos_entities:
        name = (row.get("nombre_entidad") or "").strip()
        if not name:
            continue
        dept = (row.get("departamento") or "").strip()
        total = int(row.get("total", 0))

        key = name.upper()
        if key not in entity_map:
            entity_map[key] = {
                "official_name": name,
                "departamento": dept,
                "total_procesos": 0,
                "total_contratos": total,
                "source_datasets": ["contratos"],
            }
        else:
            entity_map[key]["total_contratos"] += total
            if "contratos" not in entity_map[key]["source_datasets"]:
                entity_map[key]["source_datasets"].append("contratos")

    # Sort by combined volume
    entities = sorted(
        entity_map.values(),
        key=lambda e: e["total_procesos"] + e["total_contratos"],
        reverse=True,
    )[:top_n]

    print(f"[3/3] Merged: {len(entities)} entidades únicas (top {top_n} por volumen)")

    # Save raw
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with RAW_ENTITIES_FILE.open("w", encoding="utf-8") as f:
        json.dump(entities, f, ensure_ascii=False, indent=2)
    print(f"       → Guardado en {RAW_ENTITIES_FILE}")

    return entities


# ---------------------------------------------------------------------------
# Paso 2: Generar aliases con DeepSeek
# ---------------------------------------------------------------------------


def generate_aliases(
    entities: list[dict],
    api_key: str,
    model: str = "deepseek-chat",
    batch_delay: float = 0.5,
) -> list[dict]:
    """
    Genera aliases para cada entidad usando DeepSeek.
    Agrega el campo "aliases" a cada registro.
    """
    if OpenAI is None:
        print("ERROR: openai package required. Install with: pip install openai")
        sys.exit(1)

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1")

    total = len(entities)
    print(f"\nGenerando aliases para {total} entidades con DeepSeek ({model})...")

    for idx, entity in enumerate(entities):
        name = entity["official_name"]
        dept = entity.get("departamento", "")

        # Skip if aliases already exist (for resume capability)
        if entity.get("aliases"):
            print(f"  [{idx+1}/{total}] SKIP (ya tiene aliases): {name[:60]}")
            continue

        prompt = ALIAS_GENERATION_PROMPT.format(entidad=name, departamento=dept)

        try:
            response = client.chat.completions.create(
                model=model,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_content = response.choices[0].message.content or "[]"

            # Clean markdown fences if present
            cleaned = raw_content.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[-1]
                cleaned = cleaned.rsplit("```", 1)[0]
            cleaned = cleaned.strip()

            aliases = json.loads(cleaned)
            if not isinstance(aliases, list):
                aliases = []

            # Normalize: lowercase, strip, deduplicate
            aliases = list({a.lower().strip() for a in aliases if isinstance(a, str) and a.strip()})

            # Add normalized official name as alias
            aliases.append(name.lower().strip())

            entity["aliases"] = aliases
            print(f"  [{idx+1}/{total}] {len(aliases)} aliases: {name[:50]}...")

        except json.JSONDecodeError as e:
            print(f"  [{idx+1}/{total}] JSON ERROR for {name[:50]}: {e}")
            entity["aliases"] = [name.lower().strip()]

        except Exception as e:
            print(f"  [{idx+1}/{total}] API ERROR for {name[:50]}: {e}")
            entity["aliases"] = [name.lower().strip()]

        # Save progress after every entity (resume safety)
        with ALIASES_DB_FILE.open("w", encoding="utf-8") as f:
            json.dump(entities, f, ensure_ascii=False, indent=2)

        # Rate limit
        time.sleep(batch_delay)

    print(f"\n→ Aliases generados y guardados en {ALIASES_DB_FILE}")
    return entities


# ---------------------------------------------------------------------------
# Paso 3: Validación y estadísticas
# ---------------------------------------------------------------------------


def print_stats(entities: list[dict]) -> None:
    total = len(entities)
    with_aliases = sum(1 for e in entities if e.get("aliases"))
    total_aliases = sum(len(e.get("aliases", [])) for e in entities)
    avg_aliases = total_aliases / with_aliases if with_aliases else 0

    departamentos = {e.get("departamento", "N/A") for e in entities}

    print("\n" + "=" * 60)
    print("ESTADÍSTICAS DEL GAZETTEER")
    print("=" * 60)
    print(f"Total entidades:        {total}")
    print(f"Con aliases generados:  {with_aliases}")
    print(f"Total aliases:          {total_aliases}")
    print(f"Promedio aliases/entidad: {avg_aliases:.1f}")
    print(f"Departamentos cubiertos: {len(departamentos)}")
    print()

    # Top 10 by volume
    print("Top 10 entidades por volumen:")
    for i, e in enumerate(entities[:10], 1):
        vol = e.get("total_procesos", 0) + e.get("total_contratos", 0)
        aliases_count = len(e.get("aliases", []))
        print(f"  {i:2}. [{vol:>6} proc] [{aliases_count:>3} aliases] {e['official_name'][:60]}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="SECOPAL Gazetteer Builder")
    sub = parser.add_subparsers(dest="command")

    # Extract
    p_extract = sub.add_parser("extract", help="Extract unique entities from SECOP")
    p_extract.add_argument("--top", type=int, default=500, help="Top N entities by volume")

    # Generate aliases
    p_aliases = sub.add_parser("generate-aliases", help="Generate aliases with DeepSeek")
    p_aliases.add_argument("--model", default="deepseek-chat", help="DeepSeek model")

    # Full pipeline
    p_full = sub.add_parser("full", help="Extract + generate aliases")
    p_full.add_argument("--top", type=int, default=500, help="Top N entities")
    p_full.add_argument("--model", default="deepseek-chat", help="DeepSeek model")

    # Stats
    sub.add_parser("stats", help="Print gazetteer statistics")

    args = parser.parse_args()

    app_token = os.environ.get("SECOP_APP_TOKEN")
    deepseek_key = os.environ.get("DEEPSEEK_API_KEY")

    if args.command == "extract":
        entities = extract_entities(app_token, top_n=args.top)
        print_stats(entities)

    elif args.command == "generate-aliases":
        if not deepseek_key:
            print("ERROR: DEEPSEEK_API_KEY environment variable required")
            sys.exit(1)
        if not RAW_ENTITIES_FILE.exists():
            print(f"ERROR: Run 'extract' first. {RAW_ENTITIES_FILE} not found.")
            sys.exit(1)
        with RAW_ENTITIES_FILE.open("r") as f:
            entities = json.load(f)
        entities = generate_aliases(entities, api_key=deepseek_key, model=args.model)
        print_stats(entities)

    elif args.command == "full":
        if not deepseek_key:
            print("ERROR: DEEPSEEK_API_KEY environment variable required")
            sys.exit(1)
        entities = extract_entities(app_token, top_n=args.top)
        entities = generate_aliases(entities, api_key=deepseek_key, model=args.model)
        print_stats(entities)

    elif args.command == "stats":
        if not ALIASES_DB_FILE.exists():
            print(f"No gazetteer found at {ALIASES_DB_FILE}")
            sys.exit(1)
        with ALIASES_DB_FILE.open("r") as f:
            entities = json.load(f)
        print_stats(entities)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
