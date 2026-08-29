"""R1 — la oportunidad que un movimiento del Radar representa.

Lo que estos tests protegen no es el formato: es que la ficha solo AFIRME lo
que el evento observó. Cada caso de ausencia existe porque la alternativa
—rellenar con la publicación, con el estado de hoy, o con una URL construida—
produce una pantalla verosímil y falsa.
"""
from __future__ import annotations

import ast
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.radar.detail import (
    OPPORTUNITY_EVENT_KINDS,
    ClosingFact,
    build_opportunity_detail,
    is_opportunity_event,
    observed_secop_url,
    resolve_dataset_id,
)
from app.core.radar.store import EventRecord, RadarStore
from app.core.soql_builder import SoQLBuilder

NOW = datetime(2026, 8, 29, 15, 0, tzinfo=timezone.utc)  # 10:00 en Bogotá
PROC = SoQLBuilder.PROCESOS_DATASET
CONT = SoQLBuilder.CONTRATOS_DATASET


def _event(**overrides) -> dict:
    base = {
        "id": "ev-1",
        "kind": "new",
        "radar_name": "Mantenimiento Costa",
        "result_key": "CO1.NTC.111",
        "dataset": "procesos",
        "ts": "2026-08-28T15:00:00+00:00",
        "title": "Mantenimiento preventivo y correctivo de vías",
        "object_excerpt": "Mantenimiento de la malla vial urbana del municipio.",
        "entity": "ALCALDÍA DE SOLEDAD",
        "department": "Atlántico",
        "city": "Soledad",
        "value": "1250000000",
        "status": "Abierto",
        "modality": "Licitación pública",
        # Forma real de la columna: calendar_date materializado a medianoche.
        # Las 62 lecturas disponibles en radar_seen_results son así, sin
        # excepción. La medianoche es relleno del tipo, no una hora de cierre.
        "closing_date": "2026-09-02T00:00:00.000",
        "referencia": "LP-004-2026",
        "urlproceso": "https://community.secop.gov.co/Public/Tendering/x",
    }
    base.update(overrides)
    return base


# ── Feed: qué puede abrirse y qué no ────────────────────────────────────────


def test_baseline_is_not_an_opportunity():
    """La línea base es un hecho operativo del radar, no un proceso.

    Ofrecerle "Ver oportunidad" prometería una ficha que no existe: su título
    es un recuento ("Empezó a vigilar · N resultados"), no un objeto contractual.
    """
    assert not is_opportunity_event("baseline")
    assert "baseline" not in OPPORTUNITY_EVENT_KINDS
    baseline = _event(kind="baseline", result_key=None, title="Empezó a vigilar · 98")
    assert build_opportunity_detail(baseline, now=NOW) is None


@pytest.mark.parametrize(
    "kind", ["new", "changed_state", "changed_value", "disappeared", "followed"]
)
def test_individual_movements_open_a_detail(kind):
    assert is_opportunity_event(kind)
    detail = build_opportunity_detail(_event(kind=kind), now=NOW)
    assert detail is not None
    assert detail.kind == kind


def test_identity_comes_from_the_event_never_from_its_position():
    """La navegación usa el id del evento y su result_key, no un índice."""
    detail = build_opportunity_detail(_event(), now=NOW)
    assert detail.event_id == "ev-1"
    assert detail.result_key == "CO1.NTC.111"
    assert detail.dataset_id == PROC


def test_movement_without_stable_identity_offers_no_detail():
    """Sin result_key no se identifica inequívocamente el proceso (§12)."""
    assert build_opportunity_detail(_event(result_key=None), now=NOW) is None
    assert build_opportunity_detail(_event(result_key="  "), now=NOW) is None
    assert build_opportunity_detail(_event(id=None), now=NOW) is None


def test_dataset_is_resolved_from_either_name_or_id_and_fails_closed():
    """El runner guarda el nombre; follows guarda el id. Ambos se leen.

    Lo que NO se hace es caer a procesos ante un dataset desconocido: ese
    default le afirmaría a una fila ajena un cierre de ofertas que nadie
    observó, porque solo procesos expone ese campo.
    """
    assert resolve_dataset_id("procesos") == PROC
    assert resolve_dataset_id(PROC) == PROC
    assert resolve_dataset_id(CONT) == CONT
    assert resolve_dataset_id("zzzz-9999") is None
    assert resolve_dataset_id(None) is None

    ajeno = build_opportunity_detail(_event(dataset="zzzz-9999"), now=NOW)
    assert ajeno.dataset_id is None
    assert ajeno.closing.supported is False


# ── Ficha: los campos que el detalle afirma ─────────────────────────────────


def test_detail_projects_every_observed_field():
    detail = build_opportunity_detail(_event(), now=NOW)

    assert detail.title == "Mantenimiento preventivo y correctivo de vías"
    assert detail.object_excerpt.startswith("Mantenimiento de la malla vial")
    assert detail.entity == "ALCALDÍA DE SOLEDAD"
    assert detail.location == "Soledad, Atlántico"
    assert detail.value_display == "$1,250,000,000"
    assert detail.modality == "Licitación pública"
    assert detail.source_status == "Abierto"
    assert detail.reference == "LP-004-2026"
    assert detail.secop_url == "https://community.secop.gov.co/Public/Tendering/x"
    assert detail.closing.display == "2026-09-02"


def test_location_does_not_repeat_a_city_that_is_its_own_department():
    assert build_opportunity_detail(
        _event(city="Bogotá D.C.", department="Bogotá D.C."), now=NOW
    ).location == "Bogotá D.C."
    assert build_opportunity_detail(
        _event(city=None, department="Huila"), now=NOW
    ).location == "Huila"


def test_contract_movement_shows_signature_not_a_closing_date():
    """Contratos no tiene cierre de ofertas: no se dice "sin fecha de cierre"."""
    detail = build_opportunity_detail(
        _event(
            dataset=CONT, closing_date=None, signature_date="2026-03-11T00:00:00.000",
            contractor="UNIÓN TEMPORAL VÍAS 2026",
        ),
        now=NOW,
    )
    # Día civil, no el ISO crudo: la firma es otra fecha de SECOP y mostrar su
    # "T00:00:00.000" exhibiría una hora que la columna tampoco tiene.
    assert detail.signature_date == "2026-03-11"
    assert detail.contractor == "UNIÓN TEMPORAL VÍAS 2026"
    assert detail.closing.supported is False
    assert detail.closing.is_known is False


def test_change_movements_carry_what_changed():
    cambio_estado = build_opportunity_detail(
        _event(kind="changed_state", prev_status="Borrador"), now=NOW
    )
    cambio_valor = build_opportunity_detail(
        _event(kind="changed_value", prev_value="900000000"), now=NOW
    )
    assert cambio_estado.previous_status == "Borrador"
    assert cambio_valor.previous_value_display == "$900,000,000"


# ── Ausencias: lo que NO se inventa ─────────────────────────────────────────


def test_missing_closing_date_is_reported_as_unknown_not_replaced():
    """Sin cierre no se promueve la publicación ni se afirma "sin cierre"."""
    detail = build_opportunity_detail(_event(closing_date=None), now=NOW)
    assert detail.closing.supported is True   # procesos SÍ expone el campo
    assert detail.closing.is_known is False   # pero este evento no lo trae
    assert detail.closing.relation == "unknown"
    assert detail.closing.days_remaining is None


def test_unreadable_closing_text_is_not_displayed():
    detail = build_opportunity_detail(_event(closing_date="no aplica"), now=NOW)
    assert detail.closing.is_known is False
    assert detail.closing.relation == "unknown"


def test_missing_modality_status_and_value_are_absent_not_placeholders():
    detail = build_opportunity_detail(
        _event(modality=None, status="   ", value="", entity=None), now=NOW
    )
    assert detail.modality is None
    assert detail.source_status is None
    assert detail.value_display is None
    assert detail.entity is None


def test_zero_or_unparseable_value_is_omitted_instead_of_sin_valor():
    """``format_cop`` rotula 0 como "Sin valor"; en un detalle eso es ruido."""
    assert build_opportunity_detail(_event(value="0"), now=NOW).value_display is None


# ── URL: solo la observada ──────────────────────────────────────────────────


def test_missing_url_yields_no_link_and_no_constructed_one():
    """Nunca se arma una URL desde el id o la referencia (§8)."""
    detail = build_opportunity_detail(_event(urlproceso=None), now=NOW)
    assert detail.secop_url is None
    assert detail.has_secop_link is False
    # Ni el id ni la referencia se filtran a la URL por otra vía.
    assert "CO1.NTC.111" not in json.dumps(detail.secop_url)


def test_socrata_url_object_is_unwrapped_not_stringified():
    detail = build_opportunity_detail(
        _event(urlproceso={"url": "https://secop/abc", "description": "x"}), now=NOW
    )
    assert detail.secop_url == "https://secop/abc"


def test_non_http_url_text_is_rejected():
    """Un valor heredado que no es una URL no puede renderizarse como enlace."""
    assert observed_secop_url("{'url': 'https://x'}") is None
    assert observed_secop_url("javascript:alert(1)") is None
    assert observed_secop_url("   ") is None
    assert observed_secop_url({"url": ""}) is None
    assert observed_secop_url("https://secop/ok") == "https://secop/ok"


# ── El cierre es un calendar_date, no un instante ───────────────────────────
#
# Autoridad: docs/audits/2026-08-17_O1B_CURRENT_OPPORTUNITY_RETRIEVAL.md,
# sección "O1B.1 — frontera calendar_date". fecha_de_recepcion_de es una
# columna Socrata calendar_date materializada a 00:00:00, y el resto del
# runtime ya la lee así (process_operational_state recorta a [:10]; el SoQL
# compara contra un límite de día civil). Estos tests impiden que la vista de
# Radar vuelva a prometer una hora de cierre que la fuente no da.


@pytest.mark.parametrize(
    "crudo",
    [
        "2026-09-02",                 # día puro
        "2026-09-02T00:00:00.000",    # la forma real, medianoche de relleno
        "2026-09-02T16:00:00.000",    # aunque trajera hora, sigue siendo el día
        "2026-09-02T21:00:00+00:00",  # ni un offset la convierte en instante
    ],
)
def test_closing_calendar_date_never_exposes_clock_precision(crudo):
    """Guardián del contrato: ninguna forma del dato produce una hora visible.

    El caso peligroso es el tercero. Anclar "T16:00" a America/Bogota y
    mostrarlo parece más informativo y es una afirmación que la columna no
    sostiene; el cuarto es el mismo error por la vía del offset. Todos colapsan
    al día porque el tipo de la columna es lo que manda, no el texto que llegue.
    """
    closing = build_opportunity_detail(_event(closing_date=crudo), now=NOW).closing

    assert closing.display == "2026-09-02"
    assert ":" not in closing.display
    assert "00:00" not in closing.display
    assert "16:00" not in closing.display


def test_closing_day_stays_today_for_the_whole_colombian_day():
    """Un plazo fechado hoy no vence a las 00:00.

    Promover el día a medianoche y comparar instantes lo daría por pasado desde
    el primer segundo del propio día en que todavía se puede presentar oferta.
    """
    manana_temprano = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)   # 07:00 CO
    noche = datetime(2026, 9, 2, 23, 30, tzinfo=timezone.utc)            # 18:30 CO
    for instante in (manana_temprano, noche):
        closing = build_opportunity_detail(
            _event(closing_date="2026-09-02T00:00:00.000"), now=instante
        ).closing
        assert closing.relation == "today"
        assert closing.days_remaining == 0


def test_closing_relation_flips_at_the_colombian_day_boundary_not_the_utc_one():
    """El corte es 05:00 UTC, como fija O1B.1 — no la medianoche UTC.

    A las 04:59Z del día 3 en Colombia todavía es día 2: un cierre fechado el 2
    sigue siendo de hoy. Comparar contra el día UTC lo habría vencido cinco
    horas antes.
    """
    aun_dia_2 = datetime(2026, 9, 3, 4, 59, tzinfo=timezone.utc)
    ya_dia_3 = datetime(2026, 9, 3, 5, 1, tzinfo=timezone.utc)

    assert build_opportunity_detail(
        _event(closing_date="2026-09-02T00:00:00.000"), now=aun_dia_2
    ).closing.relation == "today"
    assert build_opportunity_detail(
        _event(closing_date="2026-09-02T00:00:00.000"), now=ya_dia_3
    ).closing.relation == "past"


def test_closing_relation_counts_whole_days():
    futuro = build_opportunity_detail(_event(), now=NOW)
    pasado = build_opportunity_detail(
        _event(closing_date="2026-08-01T00:00:00.000"), now=NOW
    )
    assert (futuro.closing.relation, futuro.closing.days_remaining) == ("future", 4)
    assert (pasado.closing.relation, pasado.closing.days_remaining) == ("past", -28)


def test_calendar_date_is_read_as_written_never_shifted_by_a_timezone():
    """El día es el que la fuente escribió; no se reubica convirtiendo zonas.

    Distingue dos implementaciones que en casi todo el dominio coinciden: leer
    el componente de día (correcto para un calendar_date) frente a parsear un
    instante, convertirlo a Colombia y tomar SU día. Con este valor las dos
    difieren — convertir lo movería al 2 de septiembre — y la columna no
    autoriza esa reubicación: no denota un instante que se pueda trasladar.
    """
    closing = build_opportunity_detail(
        _event(closing_date="2026-09-03T03:00:00+00:00"), now=NOW
    ).closing
    assert closing.display == "2026-09-03"


def test_every_known_closing_display_is_a_bare_iso_day():
    """Invariante de la clase, no de un caso: si hay display, es un día ISO."""
    for crudo in ("2026-09-02", "2026-09-02T00:00:00.000", "2026-01-01T09:30:00",
                  "2025-12-31T23:59:59.999"):
        closing = build_opportunity_detail(_event(closing_date=crudo), now=NOW).closing
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", closing.display), crudo


def test_closing_fact_default_asserts_nothing():
    vacio = ClosingFact()
    assert (vacio.supported, vacio.display, vacio.relation) == (False, None, "unknown")
    assert vacio.is_known is False


# ── Snapshot: el detalle histórico no se contamina con el estado de hoy ─────


def test_detail_reads_the_frozen_event_not_the_current_state(tmp_path):
    """Abrir un movimiento antiguo no puede mostrar el SECOP de hoy.

    El evento es un hecho fechado; ``radar_seen_results`` es estado mutable.
    Si el detalle leyera de ahí, la ficha de un cambio de ayer aparecería con
    el valor de hoy y nadie podría auditar qué fue lo que cambió.
    """
    store = RadarStore(tmp_path / "radar.db")
    rid = store.create_radar(
        name="Costa", owner="u1",
        query_frame_json=json.dumps({"dataset": "procesos", "topic": ["vias"]}),
    )
    evento = _event(kind="changed_value", prev_value="900000000")
    store.record_events([{
        "radar_id": rid, "owner": "u1", "radar_name": "Costa",
        **{k: v for k, v in evento.items() if k != "id"},
    }])
    guardado = store.get_feed("u1")[0]

    detail = build_opportunity_detail(guardado, now=NOW)
    assert detail.value_display == "$1,250,000,000"
    assert detail.previous_value_display == "$900,000,000"
    assert detail.location == "Soledad, Atlántico"
    assert detail.closing.display == "2026-09-02"


def test_get_event_is_scoped_to_its_owner(tmp_path):
    """Un id adivinado no puede abrir el movimiento de otra persona."""
    store = RadarStore(tmp_path / "radar.db")
    rid = store.create_radar(
        name="Costa", owner="u1",
        query_frame_json=json.dumps({"dataset": "procesos", "topic": ["vias"]}),
    )
    store.record_events([{
        "radar_id": rid, "owner": "u1", "radar_name": "Costa",
        **{k: v for k, v in _event().items() if k != "id"},
    }])
    guardado = store.get_feed("u1")[0]

    assert store.get_event("u1", guardado["id"])["result_key"] == "CO1.NTC.111"
    assert store.get_event("u2", guardado["id"]) is None
    assert store.get_event("u1", "no-existe") is None
    assert store.get_event("u1", "") is None


def test_location_survives_a_full_run_materialization(tmp_path):
    """La ubicación llega al feed por la ruta productiva, no solo por record_events."""
    store = RadarStore(tmp_path / "radar.db")
    rid = store.create_radar(
        name="Costa", owner="u1",
        query_frame_json=json.dumps({"dataset": "procesos", "topic": ["vias"]}),
    )
    claim = store.try_acquire_run_lease(rid)
    assert claim.acquired
    store.materialize_successful_run(
        radar_id=rid,
        token=claim.token,
        run=_run_record(),
        seen=[],
        events=[EventRecord(
            kind="new", ts="2026-08-28T15:00:00+00:00", dataset="procesos",
            result_key="CO1.NTC.111", title="Vías", entity="ALCALDÍA",
            department="Atlántico", city="Soledad", value="1000",
            status="Abierto", modality="Licitación pública",
        )],
    )
    guardado = store.get_feed("u1")[0]
    assert (guardado["department"], guardado["city"]) == ("Atlántico", "Soledad")
    assert build_opportunity_detail(guardado, now=NOW).location == "Soledad, Atlántico"


def _run_record():
    from app.core.radar.store import RunRecord, new_run_id

    return RunRecord(
        id=new_run_id(), started_at="2026-08-28T15:00:00+00:00",
        finished_at="2026-08-28T15:00:05+00:00", status="ok", total_count=1,
        new_count=1, changed_count=0, digest_json="{}",
    )


# ── Consumidor Streamlit: el feed y el detalle ──────────────────────────────


def _radar_surface() -> str:
    source = (Path(__file__).parents[1] / "streamlit_app.py").read_text()
    return source[source.index("# Radar tab"):]


def test_streamlit_feed_offers_the_detail_only_for_opportunities():
    surface = _radar_surface()
    # El baseline retorna antes de llegar a las acciones de la tarjeta.
    tarjeta = surface[surface.index("def _render_event_card"):
                      surface.index("def _render_detail_header")]
    assert 'if kind == "baseline":' in tarjeta
    assert tarjeta.index('if kind == "baseline":') < tarjeta.index("Ver oportunidad")
    assert "is_opportunity_event(kind)" in tarjeta
    # La identidad de navegación es el id del evento, nunca su posición.
    assert 'st.session_state["_open_event"] = event_id' in tarjeta


def test_streamlit_detail_uses_the_shared_authorities():
    surface = _radar_surface()
    assert "build_opportunity_detail" in surface
    assert "observed_secop_url" in surface
    # El detalle no puede construir un enlace: solo publica el observado.
    assert "OpportunityDetail/Index" not in surface


def test_streamlit_does_not_expose_internal_vocabulary_in_the_detail():
    """Copy de producto: el usuario no lee result_key ni kinds crudos (§16)."""
    surface = _radar_surface()
    detalle = surface[surface.index("def _render_opportunity_detail"):]
    for jerga in ("result_key:", "snapshot", "run_kind", "coverage", "event kind"):
        assert jerga not in detalle


def test_streamlit_shows_no_dead_buttons_for_deferred_actions():
    """§13: lo que todavía no funciona se anuncia, no se pinta como botón."""
    tree = ast.parse((Path(__file__).parents[1] / "streamlit_app.py").read_text())
    etiquetas = {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "button"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    }
    for pendiente in ("Guardar", "Descartar", "No es relevante"):
        assert pendiente not in etiquetas
