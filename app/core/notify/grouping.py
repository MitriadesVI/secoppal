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

# El mismo dataset llega con dos nombres según quién escribió el evento: el
# alias humano o el id Socrata. Sin esta tabla, ``procesos`` y ``p6dx-8zbt``
# serían dos identidades distintas —el mismo proceso partido en dos líneas— y
# el id crudo terminaría impreso en el mensaje.
_DATASET_ALIASES = {
    "procesos": "procesos",
    "p6dx-8zbt": "procesos",
    "contratos": "contratos",
    "jbjy-vk9h": "contratos",
}


def normalize_dataset(value: str | None) -> str:
    clean = (value or "").strip()
    return _DATASET_ALIASES.get(clean.casefold(), clean.casefold())


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
    observed_at: str | None = None
    modality: str | None = None
    closing_date: str | None = None
    contractor: str | None = None
    signature_date: str | None = None
    # Extracto breve del objeto/descripción contractual (dato público, no
    # razonamiento generado ni "node signal"). Congelado tal cual lo entregue
    # el caller — este módulo no decide qué fragmento mostrar.
    object_excerpt: str | None = None
    # ``radar_events`` todavía no tiene esta representación. El campo permite
    # expresar el contrato sin derivar confirmación de una mera ausencia.
    disappeared_confirmed: bool = False

    @property
    def grouping_key(self) -> tuple[str, str, str]:
        # El ``dataset`` es parte de la identidad, no un adorno del item: un radar
        # de procesos seguidos puede traer una fila de ``procesos`` y otra de
        # ``contratos`` con el mismo ``result_key``. Colapsarlas mostraría el
        # contrato como si fuera el estado del proceso.
        key = (self.result_key or "").strip()
        if key:
            return ("result_key", normalize_dataset(self.dataset), key)
        return ("event_id", "", self.event_id)


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
    dataset: str | None = None
    observed_at: str | None = None
    modality: str | None = None
    closing_date: str | None = None
    contractor: str | None = None
    signature_date: str | None = None
    object_excerpt: str | None = None

    @property
    def representative_seq(self) -> int:
        return self.event_seqs[0]

    def as_payload_item(self) -> dict:
        return {
            "dataset": self.dataset,
            "result_key": self.result_key,
            "kind": self.kind,
            "title": self.title,
            "entity": self.entity,
            "value": self.value,
            "status": self.status,
            "prev_status": self.prev_status,
            "prev_value": self.prev_value,
            "modality": self.modality,
            "closing_date": self.closing_date,
            "contractor": self.contractor,
            "signature_date": self.signature_date,
            "object_excerpt": self.object_excerpt,
            "observed_at": self.observed_at,
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
                # Descriptivo y relativamente estable como title/entity/referencia
                # (no un hecho volátil como closing_date/modality): si un evento
                # posterior del mismo bucket no trae excerpt, no hay motivo para
                # perder la explicación que un evento anterior sí trajo.
                object_excerpt=_latest_nonempty(bucket, "object_excerpt"),
                event_ids=tuple(e.event_id for e in bucket),
                event_seqs=tuple(e.event_seq for e in bucket),
                # ``or None``: un evento sin dataset sigue sin tenerlo. Ausencia
                # es ``None`` en todo este módulo, no una cadena vacía.
                dataset=normalize_dataset(last.dataset) or None,
                observed_at=last.observed_at,
                # Exactamente el último evento, sin ``_latest_nonempty``: si la
                # fuente dejó de reportar el cierre, el grupo tampoco lo reporta.
                # Rellenar con el dato viejo fingiría que la fuente todavía lo
                # afirma —y esa fecha es justo la que la gente usaría para decidir.
                modality=last.modality,
                closing_date=last.closing_date,
                contractor=last.contractor,
                signature_date=last.signature_date,
            )
        )
    return sort_grouped_events(groups)


__all__ = [
    "GroupedRadarEvent",
    "KIND_PRIORITY",
    "SelectableRadarEvent",
    "group_events",
    "grouped_sort_key",
    "normalize_dataset",
    "numeric_value",
    "sort_grouped_events",
]
