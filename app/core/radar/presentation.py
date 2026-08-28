"""Presentación humana de hechos Radar sin alterar su persistencia UTC."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

COLOMBIA_TZ = ZoneInfo("America/Bogota")
SCHEDULER_HEARTBEAT_MAX_AGE = timedelta(hours=7)


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
