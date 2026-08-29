"""Proyección pura del feed histórico a la bandeja Radar (R5A)."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from app.core.radar.detail import is_opportunity_event
from app.core.radar.receipts import ACTIVE


@dataclass(frozen=True, slots=True)
class InboxProjection:
    visible_events: tuple[Mapping[str, Any], ...]
    unread_event_ids: frozenset[str]

    @property
    def unread_count(self) -> int:
        return len(self.unread_event_ids)


def project_inbox(
    events: Iterable[Mapping[str, Any]],
    *,
    viewed_event_ids: Iterable[str] = (),
    dispositions: Mapping[tuple[str, str], str] | None = None,
) -> InboxProjection:
    """Oculta resultados suprimidos y deriva ``sin revisar`` por receipt viewed.

    La entrada sigue siendo el feed raw de ``RadarStore.get_feed``. Un baseline
    es siempre visible y nunca cuenta como oportunidad sin revisar.
    """
    viewed = frozenset(str(event_id) for event_id in viewed_event_ids)
    current = dispositions or {}
    visible: list[Mapping[str, Any]] = []
    unread: set[str] = set()
    for event in events:
        if _is_suppressed(event, current):
            continue
        visible.append(event)
        event_id = str(event.get("id") or "").strip()
        if (
            event_id
            and is_opportunity_event(event.get("kind"))
            and event_id not in viewed
        ):
            unread.add(event_id)
    return InboxProjection(tuple(visible), frozenset(unread))


def _is_suppressed(
    event: Mapping[str, Any], dispositions: Mapping[tuple[str, str], str],
) -> bool:
    radar_id = str(event.get("radar_id") or "").strip()
    result_key = str(event.get("result_key") or "").strip()
    if not radar_id or not result_key:
        return False
    return dispositions.get((radar_id, result_key), ACTIVE) != ACTIVE


__all__ = ["InboxProjection", "project_inbox"]
