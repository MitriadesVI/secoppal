"""Agrupación pura y determinista de eventos seleccionables del Radar.

Este módulo no conoce SQLite, preferencias, reloj ni canales. Convierte los
eventos elegibles de una ventana en una línea lógica por ``result_key`` y
conserva todos los ids/secuencias que explican esa línea.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re
from typing import Iterable


KIND_PRIORITY = {
    "new": 0,
    "changed_state": 1,
    "changed_value": 2,
    "disappeared": 3,
}


@dataclass(frozen=True)
class SelectableRadarEvent:
    event_id: str
    event_seq: int
    kind: str
    result_key: str | None = None
    dataset: str | None = None
    title: str | None = None
    entity: str | None = None
    value: str | None = None
    status: str | None = None
    prev_status: str | None = None
    prev_value: str | None = None
    urlproceso: str | None = None
    referencia: str | None = None
    # ``radar_events`` todavía no tiene esta representación. El campo permite
    # expresar el contrato sin derivar confirmación de una mera ausencia.
    disappeared_confirmed: bool = False

    @property
    def grouping_key(self) -> tuple[str, str]:
        key = (self.result_key or "").strip()
        if key:
            return ("result_key", key)
        return ("event_id", self.event_id)


@dataclass(frozen=True)
class GroupedRadarEvent:
    result_key: str | None
    kind: str
    title: str | None
    entity: str | None
    value: str | None
    status: str | None
    prev_status: str | None
    prev_value: str | None
    urlproceso: str | None
    referencia: str | None
    event_ids: tuple[str, ...]
    event_seqs: tuple[int, ...]

    @property
    def representative_seq(self) -> int:
        return self.event_seqs[0]

    def as_payload_item(self) -> dict:
        return {
            "result_key": self.result_key,
            "kind": self.kind,
            "title": self.title,
            "entity": self.entity,
            "value": self.value,
            "status": self.status,
            "prev_status": self.prev_status,
            "prev_value": self.prev_value,
            "urlproceso": self.urlproceso,
            "referencia": self.referencia,
            "event_ids": list(self.event_ids),
            "event_seqs": list(self.event_seqs),
        }


def _latest_nonempty(events: list[SelectableRadarEvent], field: str) -> str | None:
    for event in reversed(events):
        value = getattr(event, field)
        if value is not None and str(value).strip():
            return value
    return None


_NON_NUMERIC = re.compile(r"[^0-9,.-]+")


def numeric_value(value: str | None) -> Decimal | None:
    """Parsea valores comunes sin alterar la representación congelada.

    Acepta separadores colombianos (``1.234.567,89``) e internacionales
    (``1,234,567.89``). Una entrada ambigua o no numérica simplemente no tiene
    clave numérica y se ordena después de los números.
    """
    if value is None:
        return None
    raw = _NON_NUMERIC.sub("", str(value).strip())
    if not raw or raw in {"-", ".", ",", "-.", "-,"}:
        return None
    if "." in raw and "," in raw:
        decimal_separator = "," if raw.rfind(",") > raw.rfind(".") else "."
        thousands_separator = "." if decimal_separator == "," else ","
        raw = raw.replace(thousands_separator, "")
        if decimal_separator == ",":
            raw = raw.replace(",", ".")
    elif "." in raw or "," in raw:
        separator = "." if "." in raw else ","
        parts = raw.split(separator)
        if all(len(part) == 3 for part in parts[1:]):
            raw = "".join(parts)
        elif len(parts[-1]) in (1, 2):
            raw = "".join(parts[:-1]) + "." + parts[-1]
        elif separator == ",":
            raw = raw.replace(",", ".")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def grouped_sort_key(group: GroupedRadarEvent) -> tuple:
    number = numeric_value(group.value)
    return (
        KIND_PRIORITY.get(group.kind, len(KIND_PRIORITY)),
        number is None,
        -number if number is not None else Decimal(0),
        group.representative_seq,
    )


def sort_grouped_events(groups: Iterable[GroupedRadarEvent]) -> list[GroupedRadarEvent]:
    return sorted(groups, key=grouped_sort_key)


def group_events(events: Iterable[SelectableRadarEvent]) -> list[GroupedRadarEvent]:
    """Colapsa por clave y devuelve un orden total independiente del input."""
    ordered = sorted(events, key=lambda e: (e.event_seq, e.event_id))
    buckets: dict[tuple[str, str], list[SelectableRadarEvent]] = {}
    for event in ordered:
        if event.event_seq <= 0:
            raise ValueError(f"event_seq inválido: {event.event_seq!r}")
        buckets.setdefault(event.grouping_key, []).append(event)

    groups: list[GroupedRadarEvent] = []
    for bucket in buckets.values():
        first, last = bucket[0], bucket[-1]
        kind = min(bucket, key=lambda e: (KIND_PRIORITY.get(e.kind, 99), e.event_seq)).kind
        groups.append(
            GroupedRadarEvent(
                result_key=(first.result_key or None),
                kind=kind,
                title=_latest_nonempty(bucket, "title"),
                entity=_latest_nonempty(bucket, "entity"),
                value=last.value,
                status=last.status,
                prev_status=first.prev_status,
                prev_value=first.prev_value,
                urlproceso=_latest_nonempty(bucket, "urlproceso"),
                referencia=_latest_nonempty(bucket, "referencia"),
                event_ids=tuple(e.event_id for e in bucket),
                event_seqs=tuple(e.event_seq for e in bucket),
            )
        )
    return sort_grouped_events(groups)


__all__ = [
    "GroupedRadarEvent",
    "KIND_PRIORITY",
    "SelectableRadarEvent",
    "group_events",
    "grouped_sort_key",
    "numeric_value",
    "sort_grouped_events",
]
