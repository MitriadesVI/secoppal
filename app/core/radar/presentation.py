"""Presentación humana de hechos Radar sin alterar su persistencia UTC.

Dos relojes distintos pasan por aquí y NO se pueden leer con la misma función:

* Lo que **Radar persiste** (``ts``, ``last_run_at``, heartbeats) se escribe en
  UTC; un naive heredado se interpreta conservadoramente como UTC. Eso es
  ``parse_persisted_timestamp``.
* Lo que **SECOP publica** dentro de un resultado (``fecha_de_recepcion_de``)
  llega sin offset y ya es hora local colombiana. Leerlo como UTC lo adelantaría
  cinco horas — mostraría las 11:00 donde la entidad publicó las 16:00. Eso es
  ``parse_secop_timestamp``.

Las dos comparten ``COLOMBIA_TZ``, que sigue definida una sola vez, y se
declaran juntas para que la diferencia esté a la vista de quien elija una.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Literal
from zoneinfo import ZoneInfo

COLOMBIA_TZ = ZoneInfo("America/Bogota")
SCHEDULER_HEARTBEAT_MAX_AGE = timedelta(hours=7)

SecopTimestampKind = Literal["civil_date", "instant", "unreadable"]


def parse_secop_timestamp(
    raw: str | None,
) -> tuple[SecopTimestampKind, date | datetime | None]:
    """Lee una fecha publicada por SECOP en la zona en que fue publicada.

    Devuelve QUÉ se leyó, no solo el valor, porque las dos formas se muestran
    distinto y confundirlas afirma algo falso:

    * ``civil_date`` — un día sin hora ("2026-09-02"). No se promueve a
      medianoche: eso daría por vencido, desde las 00:00, un plazo que corre
      todo el día.
    * ``instant`` — un momento con hora. Un texto sin offset se ancla en
      ``COLOMBIA_TZ`` (America/Bogota no observa horario de verano, así que no
      hay huecos ni solapamientos que resolver); uno con offset explícito se
      respeta y se convierte.
    * ``unreadable`` — no se adivina ningún formato ni se aplica locale. Un
      texto que no sabemos leer se reporta como tal; nunca se muestra crudo ni
      se trata como ausente.

    El orden importa: la fecha civil se intenta primero, porque
    ``datetime.fromisoformat`` acepta "2026-09-02" y le inventaría las 00:00.
    """
    if raw is None:
        return "unreadable", None
    text = str(raw).strip()
    if not text:
        return "unreadable", None
    try:
        return "civil_date", date.fromisoformat(text)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return "unreadable", None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return "instant", parsed.replace(tzinfo=COLOMBIA_TZ)
    return "instant", parsed.astimezone(COLOMBIA_TZ)


def parse_persisted_timestamp(raw: str | None) -> datetime | None:
    """Lee ISO persistido; un legacy naive se interpreta conservadoramente UTC."""
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def format_colombia_timestamp(raw: str | None) -> str:
    parsed = parse_persisted_timestamp(raw)
    if parsed is None:
        return "nunca" if not raw else "fecha desconocida"
    return parsed.astimezone(COLOMBIA_TZ).strftime("%Y-%m-%d %H:%M")


def format_relative_timestamp(
    raw: str | None, *, now: datetime | None = None,
) -> str:
    parsed = parse_persisted_timestamp(raw)
    if parsed is None:
        return "sin registro"
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    seconds = max(0, int((current - parsed).total_seconds()))
    if seconds < 60:
        return "hace un momento"
    if seconds < 3600:
        return f"hace {seconds // 60} min"
    if seconds < 86400:
        return f"hace {seconds // 3600} h"
    return f"hace {seconds // 86400} d"


def scheduler_heartbeat_recent(state, *, now: datetime | None = None) -> bool:
    if state is None:
        return False
    heartbeat = parse_persisted_timestamp(state.last_heartbeat_at)
    if heartbeat is None:
        return False
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    age = current - heartbeat
    return timedelta(0) <= age <= SCHEDULER_HEARTBEAT_MAX_AGE


def timeline_label(run) -> str:
    """Etiqueta de corrida basada en metadatos formales, nunca en heurística."""
    when = format_colombia_timestamp(run.started_at)
    if run.status == "error":
        return f"⚠️ {when} · error · cobertura desconocida"
    if run.run_kind == "baseline":
        count = int(run.baseline_count or 0)
        return f"📡 {when} · Línea base · {count} resultados bajo vigilancia"
    if run.run_kind != "review":
        return f"◷ {when} · revisión histórica · cobertura desconocida"
    if run.new_count or run.changed_count:
        return (
            f"🆕 {when} · {run.new_count} nuevos · "
            f"{run.changed_count} cambios"
        )
    if run.coverage_status == "COMPLETE":
        return f"✓ {when} · sin novedades · cobertura completa"
    if run.coverage_status in {"PARTIAL_DISCOVERY", "PARTIAL_MAINTENANCE"}:
        return f"◐ {when} · sin novedades en cobertura revisada · cobertura parcial"
    return f"◷ {when} · revisión · cobertura desconocida"
