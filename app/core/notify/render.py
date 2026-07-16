"""Payload congelado y render textual puro para notificaciones del Radar."""
from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Iterable, Mapping

from app.core.notify.grouping import GroupedRadarEvent, sort_grouped_events
from app.core.notify.schema import CHANNELS


PAYLOAD_VERSION = "p1b-payload-v1"


@dataclass(frozen=True)
class RenderedMessage:
    text: str
    item_count: int
    truncated_count: int
    links_missing: int


def build_payload(
    groups: Iterable[GroupedRadarEvent],
    *,
    principal_id: str,
    endpoint_id: str,
    radar_id: str,
    radar_name: str,
    channel: str,
    created_at: str,
    max_items: int,
) -> dict:
    if channel not in CHANNELS:
        raise ValueError(f"Canal no soportado: {channel!r}")
    if isinstance(max_items, bool) or not isinstance(max_items, int) or max_items <= 0:
        raise ValueError("max_items debe ser un entero positivo")
    ordered = sort_grouped_events(groups)
    items = [group.as_payload_item() for group in ordered]
    rendered = min(max_items, len(items))
    return {
        "version": PAYLOAD_VERSION,
        "principal_id": principal_id,
        "endpoint_id": endpoint_id,
        "radar_id": radar_id,
        "radar_name": radar_name,
        "channel": channel,
        "created_at": created_at,
        "items": items,
        "total_items": len(items),
        "rendered_items": rendered,
        "truncated_count": len(items) - rendered,
        "links_missing": sum(1 for item in items if not _clean(item.get("urlproceso"))),
    }


def serialize_payload(payload: Mapping) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


_SPACE = re.compile(r"\s+")


def _clean(value) -> str:
    if value is None:
        return ""
    return _SPACE.sub(" ", str(value)).strip()


_KIND_LABEL = {
    "new": "Nuevo",
    "changed_state": "Cambio de estado",
    "changed_value": "Cambio de valor",
    "disappeared": "Desaparecido",
}


def _item_line(item: Mapping) -> str:
    label = _KIND_LABEL.get(_clean(item.get("kind")), "Novedad")
    subject = next(
        (value for value in (
            _clean(item.get("title")),
            _clean(item.get("entity")),
            _clean(item.get("referencia")),
            _clean(item.get("result_key")),
        ) if value),
        "Proceso sin título",
    )
    details: list[str] = []
    prev_status, status = _clean(item.get("prev_status")), _clean(item.get("status"))
    prev_value, value = _clean(item.get("prev_value")), _clean(item.get("value"))
    if prev_status and status and prev_status != status:
        details.append(f"{prev_status} → {status}")
    elif status:
        details.append(status)
    if prev_value and value and prev_value != value:
        details.append(f"{prev_value} → {value}")
    elif value:
        details.append(value)
    url = _clean(item.get("urlproceso"))
    if url:
        details.append(url)
    suffix = f" — {' · '.join(details)}" if details else ""
    return f"• {label}: {subject}{suffix}"


def render_payload(payload: Mapping, *, channel: str | None = None) -> RenderedMessage:
    selected_channel = channel or _clean(payload.get("channel"))
    if selected_channel not in CHANNELS:
        raise ValueError(f"Canal no soportado: {selected_channel!r}")
    items = list(payload.get("items") or [])
    visible_count = int(payload.get("rendered_items", len(items)))
    if visible_count < 0 or visible_count > len(items):
        raise ValueError("rendered_items no corresponde al payload")
    truncated = len(items) - visible_count
    radar_name = _clean(payload.get("radar_name")) or "Radar"
    lines = [f"Radar: {radar_name}", ""]
    lines.extend(_item_line(item) for item in items[:visible_count])
    if truncated:
        lines.append(f"• Y {truncated} más")
    text = "\n".join(lines).rstrip()
    return RenderedMessage(
        text=text,
        item_count=visible_count,
        truncated_count=truncated,
        links_missing=sum(1 for item in items if not _clean(item.get("urlproceso"))),
    )


__all__ = [
    "PAYLOAD_VERSION",
    "RenderedMessage",
    "build_payload",
    "render_payload",
    "serialize_payload",
]
