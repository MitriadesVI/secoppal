"""Presentación humana de hechos Radar sin alterar su persistencia UTC.

Dos relojes distintos pasan por aquí y NO se pueden leer con la misma función:

* Lo que **Radar persiste** (``ts``, ``last_run_at``, heartbeats) se escribe en
  UTC; un naive heredado se interpreta conservadoramente como UTC. Eso es
  ``parse_persisted_timestamp``.
* Lo que **SECOP publica** como fecha dentro de un resultado
  (``fecha_de_recepcion_de``, ``fecha_de_publicacion_del``) no es un instante:
  son columnas Socrata ``calendar_date``. Solo se puede leer el día civil. Eso
  es ``parse_secop_calendar_date``.

``COLOMBIA_TZ`` sigue definida una sola vez y las dos lecturas se declaran
juntas para que la diferencia esté a la vista de quien elija una: la zona
convierte instantes persistidos y sitúa el día de hoy, nunca le pone hora a una
fecha de SECOP.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

COLOMBIA_TZ = ZoneInfo("America/Bogota")
SCHEDULER_HEARTBEAT_MAX_AGE = timedelta(hours=7)

def parse_secop_calendar_date(raw: str | None) -> date | None:
    """Día civil de una fecha publicada por SECOP, o ``None`` si no se lee.

    ``fecha_de_recepcion_de`` y ``fecha_de_publicacion_del`` son columnas
    Socrata ``calendar_date`` (auditoría O1B.1, 2026-08-17), materializadas
    como ``YYYY-MM-DDT00:00:00.000``. Esa parte horaria es el relleno del tipo,
    no una hora de cierre observada: las 62 lecturas reales disponibles en
    ``radar_seen_results.last_snapshot_json`` son medianoche sin excepción.

    Por eso la función devuelve un ``date`` y no un instante. Anclar el texto a
    ``COLOMBIA_TZ`` y mostrar la hora resultante presentaría "cierra a las
    00:00" —una precisión que la fuente no tiene y una afirmación falsa sobre
    el plazo—. El resto del repo ya lee esta columna así: el estado operativo
    recorta a ``[:10]`` y el SoQL compara contra un límite de día civil.

    No se adivina ningún formato ni se aplica locale: un texto que no empieza
    por una fecha ISO válida devuelve ``None``, nunca una fecha inventada.

    La zona horaria no participa en esta lectura. Su papel, con la misma
    ``COLOMBIA_TZ``, es situar el día de HOY para comparar contra él.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


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
