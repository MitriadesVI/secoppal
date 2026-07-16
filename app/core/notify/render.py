"""Payload congelado y render textual puro para notificaciones del Radar.

Dos versiones conviven a propósito. ``v1`` es lo que ya está persistido en
``payload_json`` de batches reales: su texto no puede cambiar, porque el mensaje
que se entregue mañana tiene que ser el que se congeló ayer. ``v2`` es el
contrato nuevo, con la metadata que R2B-1B1 congeló en ``radar_events``.

El despacho por versión es explícito y falla cerrado: un payload sin versión, o
con una versión que este código no conoce, no se renderiza. Adivinar "debe ser
el vigente" convertiría un payload corrupto en un mensaje plausible.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import json
import re
from typing import Iterable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.notify.grouping import GroupedRadarEvent, sort_grouped_events
from app.core.notify.schema import CHANNELS


PAYLOAD_VERSION_V1 = "p1b-payload-v1"
PAYLOAD_VERSION_V2 = "p1b-payload-v2"
PAYLOAD_VERSION = PAYLOAD_VERSION_V2

# Política interna conservadora, no un límite publicado por ningún proveedor:
# todavía no hay proveedor conectado que pueda decirnos el suyo. Lo que estos
# números garantizan es que un render nunca produce algo que haya que partir en
# dos mensajes —partir es lo que rompe la idempotencia del outbox—.
CHANNEL_BYTE_BUDGETS = {
    "telegram": 3800,
    "whatsapp": 3800,
    "email": 12000,
}

# Un canal soportado sin presupuesto sería un límite silencioso; que reviente al
# importar es mejor que descubrirlo con un mensaje de 40 KB en producción.
assert set(CHANNEL_BYTE_BUDGETS) == set(CHANNELS), "Todo canal necesita presupuesto"

# Cotas por campo para que un solo dato no se coma el mensaje entero.
FIELD_LIMITS = {
    "radar_name": 120,
    "title": 220,
    "entity": 120,
    "referencia": 80,
    "status": 90,
    "value": 90,
    "modality": 100,
    "contractor": 140,
    "result_key": 120,
    "dataset": 40,
}


@dataclass(frozen=True)
class RenderedMessage:
    text: str
    item_count: int
    truncated_count: int
    links_missing: int
    byte_count: int = 0
    budget_bytes: int | None = None
    links_omitted_due_to_budget: int = 0


def _zone(name) -> ZoneInfo:
    """Fail closed: sin timezone válida no hay ``Observado`` honesto.

    Caer a UTC mostraría las 15:30 a alguien que vio el evento a las 10:30.
    """
    text = _clean(name)
    if not text:
        raise ValueError("El payload no trae timezone")
    try:
        return ZoneInfo(text)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"Timezone inválida: {text!r}") from exc


def build_payload(
    groups: Iterable[GroupedRadarEvent],
    *,
    principal_id: str,
    endpoint_id: str,
    radar_id: str,
    radar_name: str,
    channel: str,
    timezone: str,
    created_at: str,
    max_items: int,
) -> dict:
    """Congela el conjunto completo de items. Siempre produce ``v2``."""
    if channel not in CHANNELS:
        raise ValueError(f"Canal no soportado: {channel!r}")
    if isinstance(max_items, bool) or not isinstance(max_items, int) or max_items <= 0:
        raise ValueError("max_items debe ser un entero positivo")
    _zone(timezone)
    ordered = sort_grouped_events(groups)
    items = [group.as_payload_item() for group in ordered]
    # ``rendered_items``/``truncated_count`` no se congelan: cuántos items se ven
    # depende también del presupuesto del canal, y eso solo se sabe al renderizar.
    # Congelar un número que el render puede desmentir es peor que no tenerlo.
    return {
        "version": PAYLOAD_VERSION_V2,
        "principal_id": principal_id,
        "endpoint_id": endpoint_id,
        "radar_id": radar_id,
        "radar_name": radar_name,
        "channel": channel,
        "timezone": timezone,
        "created_at": created_at,
        "max_items": max_items,
        "items": items,
        "total_items": len(items),
        "links_missing": sum(1 for item in items if not _clean(item.get("urlproceso"))),
    }


def serialize_payload(payload: Mapping) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


_SPACE = re.compile(r"\s+")


def _clean(value) -> str:
    if value is None:
        return ""
    return _SPACE.sub(" ", str(value)).strip()


def truncate_field(value: str, limit: int) -> str:
    """Recorta por caracteres completos. Nunca parte una secuencia Unicode."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit debe ser un entero positivo")
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _field(item: Mapping, name: str) -> str:
    value = _clean(item.get(name))
    if not value:
        return ""
    return truncate_field(value, FIELD_LIMITS[name])


# ---------------------------------------------------------------------------
# v1 — congelado. Su texto es un contrato con los batches ya persistidos.
# ---------------------------------------------------------------------------

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


def _render_v1(payload: Mapping, channel: str) -> RenderedMessage:
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
        byte_count=len(text.encode("utf-8")),
        budget_bytes=None,
    )


# ---------------------------------------------------------------------------
# v2 — lenguaje factual. Cada etiqueta dice quién afirma el dato y cuándo.
# ---------------------------------------------------------------------------

_KIND_LABEL_V2 = {
    "new": "🆕 Nueva coincidencia",
    "changed_state": "🔄 Cambio de estado",
    "changed_value": "💰 Cambio de valor",
}

_DATASET_LABEL = {"procesos": "Proceso", "contratos": "Contrato"}

_MONTHS_ES = (
    "ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "oct", "nov", "dic",
)


def _format_date(value: date) -> str:
    return f"{value.day} {_MONTHS_ES[value.month - 1]} {value.year}"


def _format_datetime(value: datetime) -> str:
    return f"{_format_date(value.date())}, {value:%H:%M}"


def _format_source_instant(raw) -> str:
    """Presenta una fecha de la fuente sin cambiarle el significado.

    No se le inventa zona horaria a una fecha naive ni se calculan días
    restantes: el dato es lo que la fuente reportó, no una promesa de vigencia.
    """
    text = _clean(raw)
    if not text:
        return ""
    if "T" not in text and " " not in text:
        try:
            return _format_date(date.fromisoformat(text))
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return truncate_field(text, 60)
    return _format_datetime(parsed)


def _format_observed_at(raw, zone: ZoneInfo) -> str:
    """``observed_at`` sí es nuestro reloj: si trae offset, se convierte."""
    text = _clean(raw)
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return truncate_field(text, 60)
    if parsed.tzinfo is not None and parsed.utcoffset() is not None:
        parsed = parsed.astimezone(zone)
    return _format_datetime(parsed)


def _transition(prev_raw, current_raw, limit: int) -> str:
    prev, current = _clean(prev_raw), _clean(current_raw)
    if prev and current and prev != current:
        return f"{truncate_field(prev, limit)} → {truncate_field(current, limit)}"
    return truncate_field(current, limit) if current else ""


def _subject(item: Mapping) -> str:
    return _field(item, "title") or _field(item, "result_key") or "Resultado sin título"


def _dataset_qualifier(item: Mapping) -> str:
    raw = _clean(item.get("dataset"))
    if not raw:
        return ""
    return _DATASET_LABEL.get(raw.casefold(), truncate_field(raw, FIELD_LIMITS["dataset"]))


# Orden determinista de sacrificio cuando un item no cabe: primero lo accesorio,
# al final la transición que da nombre a la novedad.
_OPTIONAL_DROP_ORDER = (
    "observed", "referencia", "signature", "closing", "modality",
    "contractor", "entity", "url", "status", "value",
)
_DROP_SEQUENCE = tuple(
    frozenset(_OPTIONAL_DROP_ORDER[:index])
    for index in range(len(_OPTIONAL_DROP_ORDER) + 1)
)


def _item_lines(
    item: Mapping, *, zone: ZoneInfo, dropped: frozenset[str]
) -> tuple[list[str], bool]:
    """Bloque de un item y si su URL quedó visible."""
    label = _KIND_LABEL_V2.get(_clean(item.get("kind")), "Novedad")
    qualifier = _dataset_qualifier(item)
    lines = [f"{label} — {qualifier}" if qualifier else label, _subject(item)]

    if "entity" not in dropped and (entity := _field(item, "entity")):
        lines.append(f"Entidad: {entity}")
    if "referencia" not in dropped and (referencia := _field(item, "referencia")):
        lines.append(f"Referencia: {referencia}")
    if "modality" not in dropped and (modality := _field(item, "modality")):
        lines.append(f"Modalidad: {modality}")
    if "value" not in dropped and (
        value := _transition(item.get("prev_value"), item.get("value"), FIELD_LIMITS["value"])
    ):
        lines.append(f"Valor reportado: {value}")
    if "status" not in dropped and (
        status := _transition(item.get("prev_status"), item.get("status"), FIELD_LIMITS["status"])
    ):
        lines.append(f"Estado reportado: {status}")
    if "closing" not in dropped and (closing := _format_source_instant(item.get("closing_date"))):
        # Nunca "Fecha de cierre": una fila puede estar en el Radar y no estar
        # vigente. Lo único que sabemos es qué reportó la fuente.
        lines.append(f"Recepción de ofertas reportada: {closing}")
    if "contractor" not in dropped and (contractor := _field(item, "contractor")):
        lines.append(f"Contratista reportado: {contractor}")
    if "signature" not in dropped and (
        signature := _format_source_instant(item.get("signature_date"))
    ):
        lines.append(f"Firma reportada: {signature}")
    if "observed" not in dropped and (
        observed := _format_observed_at(item.get("observed_at"), zone)
    ):
        lines.append(f"Observado: {observed}")

    url_shown = False
    if "url" not in dropped and (url := _clean(item.get("urlproceso"))):
        # La URL no se trunca: media URL no lleva a ninguna parte y aparenta
        # que sí. O cabe entera o no se muestra.
        lines.append(f"Ver en SECOP: {url}")
        url_shown = True
    return lines, url_shown


def _count_line(total: int) -> str:
    return "1 novedad" if total == 1 else f"{total} novedades"


def _compose(radar_name: str, total: int, blocks: list[list[str]], hidden: int) -> str:
    lines = [f"🔔 Radar: {radar_name}", _count_line(total)]
    for block in blocks:
        lines.append("")
        lines.extend(block)
    if hidden > 0:
        lines.append("")
        lines.append(f"Y {hidden} novedades más no mostradas")
    return "\n".join(lines).rstrip()


def _fits(text: str, budget: int) -> bool:
    # UTF-8, no caracteres: "Licitación pública 🔔" ocupa más bytes que letras.
    return len(text.encode("utf-8")) <= budget


def _render_v2(payload: Mapping, channel: str) -> RenderedMessage:
    zone = _zone(payload.get("timezone"))
    items = list(payload.get("items") or [])
    total = int(payload.get("total_items", len(items)))
    if total != len(items):
        raise ValueError("total_items no corresponde al payload")
    max_items = payload.get("max_items")
    if isinstance(max_items, bool) or not isinstance(max_items, int) or max_items <= 0:
        raise ValueError("max_items debe ser un entero positivo")
    budget = CHANNEL_BYTE_BUDGETS[channel]
    radar_name = truncate_field(
        _clean(payload.get("radar_name")) or "Radar", FIELD_LIMITS["radar_name"]
    )

    blocks: list[list[str]] = []
    links_omitted = 0
    for item in items[:max_items]:
        placed: tuple[list[str], bool] | None = None
        for dropped in _DROP_SEQUENCE:
            lines, url_shown = _item_lines(item, zone=zone, dropped=dropped)
            if _fits(_compose(radar_name, total, blocks + [lines], total - len(blocks) - 1), budget):
                placed = (lines, url_shown)
                break
        if placed is None:
            # Ni el bloque mínimo cabe: este item y los siguientes cuentan como
            # no mostrados. Cortar el prefijo mantiene el orden y el determinismo.
            # Sus URL existen y no se ven por espacio, así que cuentan igual que
            # la URL sacrificada dentro de un item visible. Solo hasta
            # ``max_items``: más allá la omisión es preferencia, no presupuesto.
            links_omitted += sum(
                1 for hidden_item in items[len(blocks):max_items]
                if _clean(hidden_item.get("urlproceso"))
            )
            break
        blocks.append(placed[0])
        if not placed[1] and _clean(item.get("urlproceso")):
            links_omitted += 1

    text = _compose(radar_name, total, blocks, total - len(blocks))
    return RenderedMessage(
        text=text,
        item_count=len(blocks),
        truncated_count=total - len(blocks),
        links_missing=sum(1 for item in items if not _clean(item.get("urlproceso"))),
        byte_count=len(text.encode("utf-8")),
        budget_bytes=budget,
        links_omitted_due_to_budget=links_omitted,
    )


def render_payload(payload: Mapping, *, channel: str | None = None) -> RenderedMessage:
    # El canal es un hecho congelado, no una preferencia del caller: el payload se
    # armó para un canal y su presupuesto. Dejar que un argumento lo sustituya
    # dejaría pasar un mensaje de 12.000 bytes por Telegram bajo el presupuesto
    # de email. ``channel`` solo puede confirmar lo congelado, nunca completarlo
    # ni contradecirlo.
    payload_channel = _clean(payload.get("channel"))
    if payload_channel not in CHANNELS:
        raise ValueError(f"Canal del payload no soportado: {payload_channel!r}")
    if channel is not None and channel != payload_channel:
        raise ValueError(
            f"El canal solicitado {channel!r} no coincide con "
            f"el canal congelado {payload_channel!r}"
        )
    selected_channel = payload_channel
    version = payload.get("version")
    if version == PAYLOAD_VERSION_V1:
        return _render_v1(payload, selected_channel)
    if version == PAYLOAD_VERSION_V2:
        return _render_v2(payload, selected_channel)
    raise ValueError(f"Versión de payload no soportada: {version!r}")


__all__ = [
    "CHANNEL_BYTE_BUDGETS",
    "FIELD_LIMITS",
    "PAYLOAD_VERSION",
    "PAYLOAD_VERSION_V1",
    "PAYLOAD_VERSION_V2",
    "RenderedMessage",
    "build_payload",
    "render_payload",
    "serialize_payload",
    "truncate_field",
]
