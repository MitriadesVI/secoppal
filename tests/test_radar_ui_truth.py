"""Verdad de presentación Radar: zona horaria, timeline y heartbeat."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from app.core.radar.presentation import (
    COLOMBIA_TZ,
    SCHEDULER_HEARTBEAT_MAX_AGE,
    format_colombia_timestamp,
    parse_secop_calendar_date,
    scheduler_heartbeat_recent,
    timeline_label,
)


def test_colombia_timezone_is_resolvable_and_conversion_is_explicit():
    assert ZoneInfo("America/Bogota") == COLOMBIA_TZ
    assert (
        format_colombia_timestamp("2026-08-28T15:00:00+00:00")
        == "2026-08-28 10:00"
    )


def test_scheduler_recent_threshold_matches_the_six_hour_timer():
    now = datetime(2026, 8, 28, 15, 0, tzinfo=timezone.utc)
    recent = SimpleNamespace(
        last_heartbeat_at=(now - timedelta(hours=6, minutes=59)).isoformat()
    )
    stale = SimpleNamespace(
        last_heartbeat_at=(now - timedelta(hours=7, minutes=1)).isoformat()
    )

    assert SCHEDULER_HEARTBEAT_MAX_AGE == timedelta(hours=7)
    assert scheduler_heartbeat_recent(recent, now=now)
    assert not scheduler_heartbeat_recent(stale, now=now)
    assert not scheduler_heartbeat_recent(None, now=now)


def test_timeline_uses_formal_baseline_and_conservative_legacy_copy():
    baseline = SimpleNamespace(
        started_at="2026-08-28T15:00:00+00:00", status="ok",
        run_kind="baseline", baseline_count=98, new_count=98,
        changed_count=0, coverage_status="PARTIAL_DISCOVERY",
    )
    legacy = SimpleNamespace(
        started_at="2026-08-28T15:00:00+00:00", status="ok",
        run_kind=None, baseline_count=None, new_count=98,
        changed_count=0, coverage_status=None,
    )

    assert "Línea base · 98 resultados bajo vigilancia" in timeline_label(baseline)
    assert "98 nuevos" not in timeline_label(baseline)
    assert "revisión histórica · cobertura desconocida" in timeline_label(legacy)


def test_streamlit_no_longer_calls_scheduler_roadmap():
    source = (Path(__file__).parents[1] / "streamlit_app.py").read_text()
    radar_surface = source[source.index("# Radar tab"):]
    assert "scheduler (roadmap)" not in radar_surface
    assert "needs_attention" in radar_surface
    assert "format_colombia_timestamp" in radar_surface


def test_tzdata_is_declared_for_linux_without_system_zoneinfo():
    requirements = (Path(__file__).parents[1] / "requirements.txt").read_text()
    assert any(line.startswith("tzdata") for line in requirements.splitlines())


# ── R1: los dos relojes de la pestaña Radar ─────────────────────────────────
#
# Radar escribe INSTANTES en UTC (ts, last_run_at, heartbeats). SECOP publica
# DÍAS: fecha_de_recepcion_de es una columna Socrata calendar_date, según la
# auditoría O1B.1 del 2026-08-17. Mezclarlas produce dos errores distintos, y
# cada helper existe para no cometer el suyo.


def test_secop_calendar_date_reads_the_day_and_drops_the_padding():
    """La medianoche del ISO es relleno del tipo, no una hora de cierre.

    Las lecturas reales de la columna (radar_seen_results.last_snapshot_json)
    son todas 'T00:00:00.000'. Mostrar esa hora afirmaría que el proceso cierra
    a medianoche, que es falso y además accionable en la dirección equivocada.
    """
    from datetime import date

    assert parse_secop_calendar_date("2026-09-02T00:00:00.000") == date(2026, 9, 2)
    assert parse_secop_calendar_date("2026-09-02") == date(2026, 9, 2)


def test_secop_calendar_date_ignores_any_clock_or_offset_it_receives():
    """Aunque llegara una hora, el tipo de la columna es lo que manda.

    Este es el caso que hace falta fijar: anclar 'T16:00' a America/Bogota
    parece más informativo y es precisamente la precisión que la fuente no
    sostiene.
    """
    from datetime import date

    assert parse_secop_calendar_date("2026-09-02T16:00:00.000") == date(2026, 9, 2)
    assert parse_secop_calendar_date("2026-09-02T21:00:00+00:00") == date(2026, 9, 2)


def test_unreadable_secop_date_is_none_never_a_guess():
    for basura in (None, "", "   ", "no aplica", "02/09/2026", "2026-13-45"):
        assert parse_secop_calendar_date(basura) is None


def test_the_two_readers_answer_different_questions_and_do_not_overlap():
    """El helper de lo persistido sigue leyendo instantes, y debe seguir así.

    Un naive persistido por Radar SÍ es UTC; aplicarle la lectura de día civil
    perdería la hora que ahí sí existe. La separación es el punto.
    """
    assert format_colombia_timestamp("2026-08-28T15:00:00+00:00") == "2026-08-28 10:00"
    assert COLOMBIA_TZ == ZoneInfo("America/Bogota")
