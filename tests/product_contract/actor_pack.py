"""Pack mínimo de colisiones de actores (plan §9.1–9.2, F2.5).

JSONL congelado → builder real (``build_actor_alias_index``) → SQLite temporal →
resolver real. Nunca se commitea el índice de 662 MB ni se lee de forma ambiental.
Las claves que empiezan por ``_`` documentan la forma de colisión y no llegan al builder.
"""
from __future__ import annotations

import json
from pathlib import Path

from app.data.actor_alias_index import build_actor_alias_index

PACK_FIELDS = ("tipodocproveedor", "documento_proveedor", "proveedor_adjudicado",
               "occurrences", "first_seen", "last_seen")
BUILT_AT = "2026-09-12T00:00:00+00:00"


def load_pack(path: Path) -> list[dict]:
    rows: list[dict] = []
    for number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        record = json.loads(line)
        missing = [f for f in PACK_FIELDS if f not in record]
        extra = [k for k in record if k not in PACK_FIELDS and not k.startswith("_")]
        if missing or extra:
            raise ValueError(f"{path}:{number}: missing={missing} unexpected={extra}")
        if "_shape" not in record:
            raise ValueError(f"{path}:{number}: every pack row must declare its collision _shape")
        rows.append(record)
    return rows


def build_pack_index(rows: list[dict], target: Path, *, source_dataset: str) -> Path:
    build_actor_alias_index(
        [{k: v for k, v in row.items() if not k.startswith("_")} for row in rows],
        target, source_dataset=source_dataset, built_at=BUILT_AT,
    )
    return Path(target)
