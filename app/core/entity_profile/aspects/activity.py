"""Process activity and modality distribution from one server-side aggregate."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core.entity_profile.aspects.common import as_optional_int, build_where
from app.core.entity_profile.models import PROCESS_DATASET


MODALITY_CARDINALITY_LIMIT = 100
TOP_MODALITIES = 3


@dataclass(frozen=True, slots=True)
class ProcessActivity:
    """``process_count`` es ``None`` cuando la fuente no lo acreditó.

    ``as_int`` devolvía 0 ante cualquier valor no numérico, así que un agregado
    ilegible se presentaba como "publicó 0 procesos" — un hecho, no un hueco.
    """

    process_count: int | None
    modalities: tuple[tuple[str, int, float], ...]
    truncated: bool


def build_process_activity_query(builder: Any, params: dict) -> str:
    spec, where = build_where(builder, PROCESS_DATASET, params)
    modality = spec.modality
    return (
        f"SELECT {modality}, count(*) AS process_count WHERE {where} "
        f"GROUP BY {modality} "
        f"ORDER BY process_count DESC, {modality} ASC "
        f"LIMIT {MODALITY_CARDINALITY_LIMIT}"
    )


def parse_process_activity(rows: list[dict]) -> ProcessActivity:
    counts = [as_optional_int(row.get("process_count")) for row in rows]
    total = None if any(count is None for count in counts) else sum(counts)
    ranked: list[tuple[str, int, float]] = []
    for row, count in zip(rows, counts):
        raw = row.get("modalidad_de_contratacion")
        if raw in (None, "") or count is None:
            continue
        share = count / total if total else 0.0
        ranked.append((str(raw), count, share))
    ranked.sort(key=lambda item: (-item[1], item[0]))
    return ProcessActivity(
        process_count=total,
        modalities=tuple(ranked[:TOP_MODALITIES]),
        truncated=len(rows) >= MODALITY_CARDINALITY_LIMIT,
    )
