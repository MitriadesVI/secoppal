from __future__ import annotations

import json
from pathlib import Path

from openai import OpenAI

from app.config import get_settings

INPUT_PATH = Path("scripts/top_entities.json")
OUTPUT_PATH = Path("app/data/aliases_db.generated.json")

PROMPT_TEMPLATE = """
Para la entidad oficial de SECOP "{entity}" genera un array JSON con formas naturales
en que un colombiano podria referirse a ella. Incluye abreviaturas, sinonimos,
formas con y sin articulos, y variantes sin tildes.

Responde solo con un array JSON de strings.
""".strip()


def iter_entities(payload: dict) -> list[str]:
    names: set[str] = set()
    for row in payload.get("procesos", []):
        entity = row.get("entidad")
        if entity:
            names.add(entity)
    for row in payload.get("contratos", []):
        entity = row.get("nombre_entidad")
        if entity:
            names.add(entity)
    return sorted(names)


def main() -> None:
    settings = get_settings()
    if not settings.deepseek_api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is required to generate aliases.")
    if not INPUT_PATH.exists():
        raise RuntimeError(f"Missing input file: {INPUT_PATH}")

    client = OpenAI(api_key=settings.deepseek_api_key, base_url="https://api.deepseek.com/v1")
    payload = json.loads(INPUT_PATH.read_text(encoding="utf-8"))

    generated: list[dict] = []
    for entity in iter_entities(payload):
        response = client.chat.completions.create(
            model=settings.deepseek_model,
            temperature=0,
            messages=[
                {"role": "system", "content": "Genera aliases para entity linking SECOP."},
                {"role": "user", "content": PROMPT_TEMPLATE.format(entity=entity)},
            ],
        )
        aliases = json.loads(response.choices[0].message.content)
        generated.append({"official_name": entity, "aliases": aliases})
        print(f"Generated aliases for {entity}")

    OUTPUT_PATH.write_text(json.dumps(generated, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"Saved aliases to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

