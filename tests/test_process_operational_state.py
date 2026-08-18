"""M2/PROCESS-OP-STATE-001 — estado operativo del proceso en la tarjeta.

Casos A–J del bloque M2. Todos deterministas: el instante de referencia se
inyecta, nunca se lee el reloj.

Contexto medido en la fuente (2026-08-17, dataset p6dx-8zbt, 9.018.738 filas):
``estado_de_apertura_del_proceso='Abierto'`` cubre 7.888.926 filas (87,5%) y de
esas el 65,7% ya NO admite ofertas. La tarjeta mostraba ese campo como estado.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.core.formatter import Formatter
from app.core.process_operational_state import (
    ProcessOperationalSummary,
    summarize_process_operational_state,
)
from app.core.soql_builder import SoQLBuilder

PROCESOS_ID = "p6dx-8zbt"
CONTRATOS_ID = "jbjy-vk9h"

REF = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)


def _row(**over):
    """Fila de proceso mínima; los casos sobreescriben las señales."""
    base = {
        "id_del_proceso": "CO1.NTC.0001",
        "entidad": "ALCALDIA DE PRUEBA",
        "nombre_del_procedimiento": "MANTENIMIENTO DE VIAS",
        "precio_base": "1000000",
        "urlproceso": "https://community.secop.gov.co/x",
        "fecha_de_publicacion_del": "2026-06-01T00:00:00.000",
    }
    base.update(over)
    return base


def _summary(**over) -> ProcessOperationalSummary:
    return summarize_process_operational_state(_row(**over), reference_instant=REF)


# ── Caso A — Abierto + Seleccionado ─────────────────────────────────────────
# El 60,1% de los "Abierto" del dataset real cae aquí.
def test_case_a_open_selected_is_not_presented_as_open():
    s = _summary(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Seleccionado",
    )
    assert s.state == "selected"
    assert s.label == "Seleccionado"
    # El estado principal NO puede ser simplemente "Abierto".
    assert "Abierto" not in s.label
    # El crudo se preserva y se hace visible porque contradice.
    assert s.raw_state == "Abierto"
    assert s.show_raw_state is True
    assert s.icon != "🟢"


# ── Caso B — Abierto + Evaluación ───────────────────────────────────────────
def test_case_b_open_evaluation():
    s = _summary(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Evaluación",
    )
    assert s.state == "evaluation"
    assert s.label == "En evaluación"
    assert s.raw_state == "Abierto"
    assert s.show_raw_state is True


# ── Caso C — Abierto + plazo vencido, sin selección ─────────────────────────
# Fixture de borde deliberado: en producción su cobertura es casi nula (solo
# 3.475 de 2.700.581 filas "Abierto+Publicado" traen fecha de recepción, y la
# mayoría de esas la traen en el futuro). No se optimiza para este caso.
def test_case_c_open_past_deadline_loses_the_green_claim():
    s = _summary(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Publicado",
        fase="Presentación de oferta",
        fecha_de_recepcion_de="2026-05-01T10:00:00.000",  # anterior a REF
    )
    # La etapa canónica del Truth Layer no se altera...
    assert s.state == "offer_phase_observed"
    # ...pero pierde la afirmación de vigencia.
    assert s.deadline_passed is True
    assert s.label == "Plazo de recepción reportado vencido"
    assert s.icon != "🟢"


def test_case_c_unknown_temporal_relation_never_counts_as_expired():
    """Sin fecha de recepción no se infiere vencimiento. Es el caso real común."""
    s = _summary(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Publicado",
        fase="Presentación de oferta",
    )
    assert s.state == "offer_phase_observed"
    assert s.deadline_passed is False
    assert "plazo vencido" not in s.label


# ── Caso D — recepción de ofertas real ──────────────────────────────────────
def test_case_d_real_open_process_is_not_degraded():
    s = _summary(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Publicado",
        fase="Presentación de oferta",
        fecha_de_recepcion_de="2026-09-30T10:00:00.000",  # futura
    )
    assert s.state == "offer_phase_observed"
    assert s.label == "Recepción de ofertas · límite 2026-09-30"
    assert s.icon == "🟢"
    assert s.deadline_passed is False
    # No hay discrepancia material: nada de segunda línea redundante (§12).
    assert s.show_raw_state is False


# ── Caso E — evidencia insuficiente ─────────────────────────────────────────
def test_case_e_insufficient_evidence_falls_back_conservatively():
    s = _summary(estado_de_apertura_del_proceso="Abierto")
    assert s.state == "unknown"
    assert s.is_conclusive is False
    assert s.label == "Estado por verificar"
    # unknown NUNCA se convierte en una categoría.
    assert s.label not in {"Recepción de ofertas", "Seleccionado", "Publicado"}
    # El crudo sigue disponible: es lo único que hay.
    assert s.raw_state == "Abierto"
    assert s.show_raw_state is True


def test_case_e_no_signals_at_all_stays_unknown_without_raw_line():
    s = summarize_process_operational_state(_row(), reference_instant=REF)
    assert s.state == "unknown"
    assert s.raw_state == ""
    assert s.show_raw_state is False


# ── Caso F — cancelado ──────────────────────────────────────────────────────
def test_case_f_cancelled():
    s = _summary(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Cancelado",
    )
    assert s.state == "cancelled"
    assert s.label == "Cancelado"
    assert s.icon == "🔴"
    assert s.show_raw_state is True


# ── Caso G — borrador / prepublicación ──────────────────────────────────────
def test_case_g_draft():
    s = _summary(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Borrador",
    )
    assert s.state == "draft"
    assert s.label == "Borrador"
    assert s.show_raw_state is True


def test_case_g_approval_stage():
    s = _summary(estado_del_procedimiento="En aprobación")
    assert s.state == "approval"
    assert s.label == "En aprobación"


# ── Discrepancia inversa y política de segunda línea ────────────────────────
def test_closed_opening_over_offer_phase_is_also_a_discrepancy():
    """No se pinta verde una recepción que SECOP reporta cerrada."""
    s = _summary(
        estado_de_apertura_del_proceso="Cerrado",
        estado_del_procedimiento="Publicado",
        fase="Presentación de oferta",
    )
    assert s.state == "offer_phase_observed"
    assert s.show_raw_state is True
    assert s.raw_state == "Cerrado"


def test_published_with_closed_opening_is_not_noise():
    """'Publicado' no afirma recepción activa: no contradice a 'Cerrado'."""
    s = _summary(
        estado_de_apertura_del_proceso="Cerrado",
        estado_del_procedimiento="Publicado",
    )
    assert s.state == "published"
    assert s.show_raw_state is False


def test_uncatalogued_opening_value_is_shown_instead_of_dropped():
    """Un valor de apertura fuera de {Abierto, Cerrado} no se descarta en silencio.

    Hueco detectado por mutación: la rama catch-all de la política de
    discrepancia no estaba cubierta por ningún caso.
    """
    s = _summary(
        estado_de_apertura_del_proceso="Publicado",
        estado_del_procedimiento="Publicado",
        fase="Presentación de oferta",
    )
    assert s.state == "offer_phase_observed"
    assert s.raw_state == "Publicado"
    assert s.show_raw_state is True


def test_published_keeps_its_own_label_not_abierto():
    s = _summary(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Publicado",
    )
    assert s.state == "published"
    assert s.label == "Publicado"
    assert s.show_raw_state is False  # sin ruido redundante


# ── Contrato del payload público ────────────────────────────────────────────
def test_public_payload_carries_no_truth_layer_internals():
    payload = _summary(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Seleccionado",
    ).as_dict()
    assert set(payload) == {
        "state", "label", "icon", "raw_state", "show_raw_state", "deadline_passed",
    }
    for leaked in ("reason_codes", "quality_summary", "evidence", "process_resolution",
                   "visibility", "temporal", "conflict_codes"):
        assert leaked not in payload


def test_real_socrata_url_object_does_not_break_the_observer():
    """Regresión: SECOP entrega ``urlproceso`` como objeto, no como escalar.

    El transporte T1A rechaza no-escalares. Con la fila real cruda el observer
    lanzaba TypeError y el estado operativo desaparecía de TODA tarjeta de
    proceso — invisible para fixtures sintéticos con urlproceso de tipo str.
    """
    row = _row(
        estado_de_apertura_del_proceso="Abierto",
        estado_del_procedimiento="Seleccionado",
        fase="Presentación de oferta",
        estado_resumen="Presentación de oferta",
        urlproceso={"url": "https://community.secop.gov.co/x?noticeUID=CO1.NTC.9872684"},
    )
    s = summarize_process_operational_state(row, reference_instant=REF)
    assert s.state == "selected"
    assert s.show_raw_state is True
    # La fila original conserva su objeto intacto.
    assert isinstance(row["urlproceso"], dict)


def test_card_survives_real_socrata_url_object():
    rows = _rows([_row(estado_de_apertura_del_proceso="Abierto",
                       estado_del_procedimiento="Evaluación",
                       urlproceso={"url": "https://community.secop.gov.co/x"})])
    assert rows[0]["estado_op"] == "evaluation"


def test_summary_is_deterministic_and_pure():
    row = _row(estado_de_apertura_del_proceso="Abierto",
               estado_del_procedimiento="Evaluación")
    before = dict(row)
    a = summarize_process_operational_state(row, reference_instant=REF)
    b = summarize_process_operational_state(row, reference_instant=REF)
    assert a == b
    assert row == before  # no muta la fila


def test_reference_instant_must_be_timezone_aware():
    with pytest.raises(ValueError):
        summarize_process_operational_state(
            _row(), reference_instant=datetime(2026, 8, 17, 12, 0)
        )


# ── Transporte SoQL ─────────────────────────────────────────────────────────
def test_soql_select_transports_estado_resumen():
    fields = SoQLBuilder.SPECS[PROCESOS_ID].select_fields
    assert "estado_resumen" in fields
    # Los ejes decisivos ya viajaban.
    for f in ("estado_del_procedimiento", "fase", "estado_de_apertura_del_proceso",
              "fecha_de_recepcion_de"):
        assert f in fields


def test_soql_does_not_transport_deferred_resolution_fields():
    """process_resolution queda diferido: no se transportan sus 4 campos."""
    fields = SoQLBuilder.SPECS[PROCESOS_ID].select_fields
    for f in ("adjudicado", "fecha_adjudicacion", "id_adjudicacion", "id_del_portafolio"):
        assert f not in fields


def test_contracts_select_untouched():
    fields = SoQLBuilder.SPECS[CONTRATOS_ID].select_fields
    assert "estado_resumen" not in fields
    assert fields[0] == "id_contrato"


def test_soql_where_and_order_unchanged_by_m2():
    """El SELECT crece; el WHERE/ORDER no se tocan (paridad §29)."""
    params = {"intent_type": "opportunity_search", "entidad_resolved": "ALCALDIA DE CALI"}
    q = SoQLBuilder().build(PROCESOS_ID, params)
    where = q.split(" WHERE ", 1)[1]
    assert "estado_resumen" not in where  # el campo nuevo no filtra nada
    assert "estado_de_apertura_del_proceso = 'Abierto'" in where  # STATE-PRIORITY-001 intacto


# ── Integración con la tarjeta ──────────────────────────────────────────────
def _rows(items, dataset_id=PROCESOS_ID):
    return Formatter().to_rows(items, dataset_id, {}, reference_instant=REF)


def test_card_row_keeps_raw_estado_and_adds_operational_fields():
    rows = _rows([_row(estado_de_apertura_del_proceso="Abierto",
                       estado_del_procedimiento="Seleccionado")])
    row = rows[0]
    # `estado` sigue siendo el crudo: lo consume el radar/follows.
    assert row["estado"] == "Abierto"
    assert row["estado_op"] == "selected"
    assert row["estado_op_label"] == "Seleccionado"
    assert row["estado_op_mostrar_raw"] is True


def test_card_text_shows_operational_state_and_preserves_raw():
    rows = _rows([_row(estado_de_apertura_del_proceso="Abierto",
                       estado_del_procedimiento="Seleccionado")])
    text = Formatter().format_whatsapp(rows, dataset_id=PROCESOS_ID, total_count=1)
    assert "Seleccionado" in text
    assert "SECOP reporta apertura: Abierto" in text
    # Nunca acusa a la fuente.
    for accusatory in ("equivocado", "miente", "incorrecto", "falso"):
        assert accusatory not in text.lower()


def test_card_text_has_no_redundant_second_line_when_aligned():
    rows = _rows([_row(estado_de_apertura_del_proceso="Abierto",
                       estado_del_procedimiento="Publicado",
                       fase="Presentación de oferta")])
    text = Formatter().format_whatsapp(rows, dataset_id=PROCESOS_ID, total_count=1)
    assert "Publicado · Vigencia por verificar" in text
    assert "Recepción de ofertas" not in text
    assert "SECOP reporta" not in text


def test_telegram_card_also_shows_operational_state():
    rows = _rows([_row(estado_de_apertura_del_proceso="Abierto",
                       estado_del_procedimiento="Evaluación")])
    text = Formatter().format_telegram(rows, dataset_id=PROCESOS_ID, total_count=1)
    assert "En evaluación" in text
    assert "SECOP reporta apertura: Abierto" in text


def test_truth_layer_name_never_leaks_to_the_card():
    rows = _rows([_row(estado_de_apertura_del_proceso="Abierto",
                       estado_del_procedimiento="Seleccionado")])
    text = Formatter().format_whatsapp(rows, dataset_id=PROCESOS_ID, total_count=1)
    for internal in ("truth", "lifecycle", "reason_code", "sentinel", "offer_phase_observed",
                     "quality", "PROCESS_"):
        assert internal.lower() not in text.lower()


# ── Caso H — paginación ─────────────────────────────────────────────────────
def test_case_h_pagination_pages_both_get_operational_state():
    page1 = [_row(id_del_proceso=f"CO1.NTC.{i}",
                  estado_de_apertura_del_proceso="Abierto",
                  estado_del_procedimiento="Seleccionado") for i in range(3)]
    page2 = [_row(id_del_proceso=f"CO1.NTC.{i}",
                  estado_de_apertura_del_proceso="Abierto",
                  estado_del_procedimiento="Evaluación") for i in range(10, 13)]
    r1, r2 = _rows(page1), _rows(page2)
    assert [r["estado_op"] for r in r1] == ["selected"] * 3
    assert [r["estado_op"] for r in r2] == ["evaluation"] * 3
    # El resumen se recalcula sobre las filas visibles: no se serializa el
    # assessment ni se persiste nada en el buffer.
    assert all(r["estado_op_mostrar_raw"] for r in r1 + r2)


# ── Caso I — direct lookup ──────────────────────────────────────────────────
def test_case_i_direct_lookup_single_row():
    text, rows = Formatter().format_for_channel(
        [_row(estado_de_apertura_del_proceso="Abierto",
              estado_del_procedimiento="Cancelado")],
        PROCESOS_ID, "whatsapp", params={}, total_count=1, reference_instant=REF,
    )
    assert rows[0]["estado_op"] == "cancelled"
    assert "Cancelado" in text
    assert "SECOP reporta apertura: Abierto" in text


# ── Caso J — dual dataset: contratos intactos ───────────────────────────────
def _con_row():
    return {"objeto_del_contrato": "REPUESTOS", "nombre_entidad": "DISTRITO",
            "valor_del_contrato": "175000000", "estado_contrato": "En ejecución",
            "fecha_de_firma": "2026-05-19T00:00:00.000", "urlproceso": "http://c1"}


def test_case_j_contract_rows_have_no_operational_state():
    rows = _rows([_con_row()], dataset_id=CONTRATOS_ID)
    row = rows[0]
    assert row["estado"] == "En ejecución"
    for k in ("estado_op", "estado_op_label", "estado_op_icono", "estado_op_mostrar_raw"):
        assert k not in row


def test_case_j_contract_card_text_unchanged():
    rows = _rows([_con_row()], dataset_id=CONTRATOS_ID)
    text = Formatter().format_whatsapp(rows, dataset_id=CONTRATOS_ID, total_count=1)
    assert "En ejecución" in text
    assert "SECOP reporta" not in text


def test_case_j_dual_dataset_grouped_single_reference_instant():
    from app.core.workflow.dual_dataset import DatasetGroupResult

    def _grp(name, rows, total):
        return DatasetGroupResult(name, "ok", rows, [],
                                  {"total_count": total, "candidate_count": len(rows),
                                   "validated_count": len(rows), "candidate_pool_full": False},
                                  "SELECT ...", None, {})

    groups = {
        "procesos": _grp("procesos", [_row(estado_de_apertura_del_proceso="Abierto",
                                           estado_del_procedimiento="Seleccionado")], 214),
        "contratos": _grp("contratos", [_con_row()], 5202),
    }
    text, displayed = Formatter().format_grouped(
        groups, "whatsapp", reference_instant=REF
    )
    assert displayed == {"procesos": 1, "contratos": 1}
    assert "Seleccionado" in text
    assert "SECOP reporta apertura: Abierto" in text
    assert "En ejecución" in text  # el contrato conserva su estado crudo
