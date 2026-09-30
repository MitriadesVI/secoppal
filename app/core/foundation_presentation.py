"""Presentación de un turno Foundation: tarjetas y texto por canal.

Los grupos publicados conservan las filas exactas de la fuente (la evidencia).
Aquí sólo se proyectan a la forma de tarjeta que leen la web, los canales de
texto y la exportación, con la misma convención del camino legacy: ``rows`` son
tarjetas y ``results`` filas crudas. Ninguna fila se añade, quita o reordena.
"""

from __future__ import annotations

from datetime import datetime

from app.core.contract_universe import label as universe_label, stamp
from app.core.formatter import Formatter
from app.core.foundation_messages import SOURCE_UNAVAILABLE

_LABELS = {"processes": "Procesos", "contracts": "Contratos"}
# Sólo la web pinta tarjetas aparte; los canales de texto las necesitan en el mensaje.
_CARD_CHANNELS = frozenset({"streamlit"})
# Una hoja mezclaría procesos y contratos sin decir cuál es cuál: se bloquea, no se adivina.
DUAL_EXPORT_UNSUPPORTED = ("La exportación a Excel todavía no separa procesos y contratos de una misma "
                           "búsqueda. Pide cada uno por separado para exportarlo.")


def group_cards(formatter: Formatter, groups: list[dict], *,
                reference_instant: datetime | None) -> list[list[dict]]:
    """Una lista de tarjetas por grupo, alineada con ``groups``."""
    return [formatter.display_rows(group["rows"], group["dataset"], reference_instant=reference_instant)
            for group in groups]


def export_fields(groups: list[dict]) -> dict:
    """Lo que la exportación necesita saber del turno: un sello o un bloqueo explícito."""
    if len(groups) > 1:
        return {"export_notice": DUAL_EXPORT_UNSUPPORTED}
    if groups and groups[0]["universe"] == "contracts":
        # La proyección siempre compila el universo celebrado; el Excel verifica este sello.
        return {"export_params": stamp({"dataset": "contratos"})}
    return {}


def shown_cards(cards: list[dict], *, channel: str, limit: int) -> list[dict]:
    """Las tarjetas que el turno muestra de verdad: todas en la web, ``limit`` en texto."""
    return cards if channel in _CARD_CHANNELS else cards[:limit]


def text_card_lines(cards: list[dict], *, channel: str, limit: int) -> list[str]:
    """Las tarjetas dentro del mensaje, sólo para canales sin tarjetas propias."""
    if channel in _CARD_CHANNELS or not cards:
        return []
    return ["", *Formatter._grouped_card_lines(cards[:limit])]


def _total_label(group: dict) -> str:
    if group["total_quality"] == "exact":
        return f"{group['total']:,}"
    return f"al menos {group['total']:,}"


def _group_lines(group: dict, cards: list[dict], *, channel: str, limit: int) -> list[str]:
    label = _LABELS.get(group["universe"], "Resultados")
    if group["state"] == "source_error":
        return [f"{label}: {SOURCE_UNAVAILABLE.get(group['technical_reason'], SOURCE_UNAVAILABLE['source_error'])}"]
    if not cards:
        return [f"{label}: ninguna fila cumple todas las condiciones."]
    shown = shown_cards(cards, channel=channel, limit=limit)
    head = f"{label}: encontré {_total_label(group)}."
    if group["fallback_attempted"]:
        head += f" Te muestro {len(shown)} (muestra sin orden garantizado)."
    elif group["total_quality"] != "exact" or len(shown) < group["total"]:
        head += f" Te muestro los {len(shown)} más recientes."
    lines = [head]
    if group["universe"] == "contracts":
        lines.append(universe_label())
    return lines + text_card_lines(shown, channel=channel, limit=limit)


def response_text(groups: list[dict], cards_by_group: list[list[dict]], *, channel: str, limit: int) -> str:
    """El texto visible del turno; nunca identificadores crudos ni códigos."""
    blocks = [_group_lines(group, cards, channel=channel, limit=limit)
              for group, cards in zip(groups, cards_by_group)]
    return "\n\n".join("\n".join(block) for block in blocks)
