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


def shown_card_groups(cards_by_group: list[list[dict]], *, channel: str, limit: int) -> list[list[dict]]:
    """Presupuesto global de texto, repartido por turnos y con prefijos estables."""
    if channel in _CARD_CHANNELS:
        return cards_by_group
    quotas = [0] * len(cards_by_group)
    remaining = min(max(0, limit), sum(len(cards) for cards in cards_by_group))
    while remaining:
        for index, cards in enumerate(cards_by_group):
            if quotas[index] < len(cards):
                quotas[index] += 1
                remaining -= 1
                if not remaining:
                    break
    return [cards[:quota] for cards, quota in zip(cards_by_group, quotas)]


def shown_cards(cards: list[dict], *, channel: str, limit: int) -> list[dict]:
    """La misma política de publicación para el fallback de un solo grupo."""
    return shown_card_groups([cards], channel=channel, limit=limit)[0]


def text_card_lines(cards: list[dict], *, channel: str, limit: int) -> list[str]:
    """Las tarjetas dentro del mensaje, sólo para canales sin tarjetas propias."""
    if channel in _CARD_CHANNELS or not cards:
        return []
    return ["", *Formatter._grouped_card_lines(shown_cards(cards, channel=channel, limit=limit))]


def _total_label(group: dict) -> str:
    if group["total_quality"] == "exact":
        return f"{group['total']:,}"
    return f"al menos {group['total']:,}"


def _group_lines(group: dict, cards: list[dict], *, channel: str) -> list[str]:
    label = _LABELS.get(group["universe"], "Resultados")
    if group["state"] == "source_error":
        return [f"{label}: {SOURCE_UNAVAILABLE.get(group['technical_reason'], SOURCE_UNAVAILABLE['source_error'])}"]
    if not group["rows"]:
        return [f"{label}: ninguna fila cumple todas las condiciones."]
    head = f"{label}: encontré {_total_label(group)}."
    if group["fallback_attempted"]:
        head += f" Te muestro {len(cards)} (muestra sin orden garantizado)."
    elif not cards:
        head += " Te muestro 0 por el límite de tarjetas del mensaje."
    else:
        head += f" Te muestro los {len(cards)} más recientes."
    lines = [head]
    if group["universe"] == "contracts":
        lines.append(universe_label())
    if channel not in _CARD_CHANNELS and cards:
        lines += ["", *Formatter._grouped_card_lines(cards)]
    return lines


def response_text(groups: list[dict], cards_by_group: list[list[dict]], *, channel: str, limit: int) -> str:
    """El texto visible del turno; nunca identificadores crudos ni códigos."""
    shown = shown_card_groups(cards_by_group, channel=channel, limit=limit)
    blocks = [_group_lines(group, cards, channel=channel)
              for group, cards in zip(groups, shown)]
    return "\n\n".join("\n".join(block) for block in blocks)
