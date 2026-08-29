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
    parse_secop_timestamp,
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


def test_secop_naive_timestamp_is_colombian_wall_time_not_utc():
    """El contraste que hace peligroso reutilizar el helper equivocado.

    ``format_colombia_timestamp`` lee un naive como UTC —correcto, porque lo
    que Radar persiste se escribe en UTC—. Un ``fecha_de_recepcion_de`` de
    SECOP también llega sin offset, pero YA es hora local: pasarlo por el
    primer helper adelantaría el cierre cinco horas.
    """
    crudo = "2026-09-02T16:00:00.000"

    kind, momento = parse_secop_timestamp(crudo)

    assert kind == "instant"
    assert momento.tzinfo is COLOMBIA_TZ
    assert momento.strftime("%Y-%m-%d %H:%M") == "2026-09-02 16:00"
    # El helper de lo persistido daría otra hora sobre el MISMO texto.
    assert format_colombia_timestamp(crudo) == "2026-09-02 11:00"


def test_secop_timestamp_with_offset_is_converted_to_colombia():
    kind, momento = parse_secop_timestamp("2026-09-02T21:00:00+00:00")
    assert kind == "instant"
    assert momento.strftime("%Y-%m-%d %H:%M") == "2026-09-02 16:00"


def test_secop_civil_date_is_not_promoted_to_midnight():
    """Un día sin hora se conserva como día: medianoche sería una hora inventada."""
    kind, valor = parse_secop_timestamp("2026-09-02")
    assert kind == "civil_date"
    assert valor.isoformat() == "2026-09-02"
    assert not isinstance(valor, datetime)


def test_unreadable_secop_timestamp_is_reported_not_guessed():
    for basura in (None, "", "   ", "no aplica", "02/09/2026"):
        assert parse_secop_timestamp(basura) == ("unreadable", None)
