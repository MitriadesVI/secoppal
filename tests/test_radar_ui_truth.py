"""Verdad de presentación Radar: zona horaria, timeline y heartbeat."""
from __future__ import annotations

import ast
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


# ── R2: dónde vive Guardar, y qué NO se convirtió en un CRM ─────────────────
#
# El escritorio tiene que demostrar una sola cosa: guardar, volver a encontrar,
# abrir, quitar. Cada botón de más ("me interesa", "preparando", "descartar")
# promete un pipeline comercial que no existe y que el plan V2.1 §9.4 excluye
# explícitamente de V1.


def _streamlit_source() -> str:
    return (Path(__file__).parents[1] / "streamlit_app.py").read_text()


def _function_node(name: str) -> ast.FunctionDef:
    arbol = ast.parse(_streamlit_source())
    for node in ast.walk(arbol):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} no existe en streamlit_app.py")


def _button_labels(name: str) -> set[str]:
    """Etiquetas literales de los botones que esa función pinta."""
    etiquetas = set()
    for node in ast.walk(_function_node(name)):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in {"button", "link_button", "form_submit_button"}:
            continue
        if node.args and isinstance(node.args[0], ast.Constant):
            etiquetas.add(node.args[0].value)
    return etiquetas


def test_the_workspace_tab_exists_next_to_search_and_radar():
    source = _streamlit_source()
    assert '"🗂 Mi escritorio"' in source
    assert "_render_escritorio()" in source
    # No se creó un router nuevo ni una carpeta pages/.
    assert not (Path(__file__).parents[1] / "pages").exists()


def test_save_lives_in_the_opportunity_detail_not_in_the_feed_card():
    """El feed se mantiene escaneable: Ver oportunidad → Guardar."""
    assert "Guardar" in _button_labels("_render_workspace_action")
    assert "_render_workspace_action" in {
        node.func.id
        for node in ast.walk(_function_node("_render_opportunity_detail"))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert _button_labels("_render_event_card") == {"Ver oportunidad"}


def test_the_detail_offers_only_save_and_secop():
    assert _button_labels("_render_workspace_action") <= {
        "Guardar", "✓ Guardada", "Quitar de guardadas", "Inicia sesión para guardar",
    }
    assert _button_labels("_render_opportunity_detail") <= {
        "← Volver a movimientos", "Abrir en SECOP",
    }


def test_the_workspace_card_offers_only_secop_and_remove():
    """R2 no es un CRM: sin estados, sin pin, sin notas, sin compartir."""
    assert _button_labels("_render_workspace_card") == {"Abrir en SECOP", "Quitar"}


def test_no_commercial_pipeline_verbs_reached_the_ui():
    source = _streamlit_source()
    for prohibido in ("Me interesa", "Preparando", "No es relevante", "Descartar",
                      "Preparar oferta", "Analizar pliego"):
        assert prohibido not in source


def test_no_parallel_persistence_table_was_introduced():
    """La autoridad ya existía: workspace_items."""
    for archivo in ("streamlit_app.py", "app/core/workspace_store.py",
                    "app/core/radar/workspace.py"):
        fuente = (Path(__file__).parents[1] / archivo).read_text()
        for tabla in ("saved_opportunities", "saved_radar_results",
                      "favorites", "bookmarks"):
            assert tabla not in fuente, f"{tabla} en {archivo}"


def test_anonymous_users_cannot_save_and_no_anonymous_user_id_is_invented():
    fuente = _streamlit_source()
    cuerpo = ast.get_source_segment(fuente, _function_node("_workspace_user_id"))
    assert "is_anonymous" in cuerpo
    assert "ensure_user" in cuerpo
    # Ni un user_id fabricado ni un guardado temporal escondido. Se mira el
    # AST y no el texto: un literal "anonymous" en el código es el defecto, una
    # mención en un comentario que explica por qué no existe, no.
    literales = {
        node.value
        for node in ast.walk(ast.parse(fuente))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert "anonymous" not in literales
