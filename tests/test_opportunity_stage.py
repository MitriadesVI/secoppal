"""Tests del clasificador puro OpportunityStage (OPP-STAGE-001, Fase 2A)."""

import copy

from app.core.opportunity_stage import (
    OpportunityStageAssessment,
    classify_opportunity_stage,
)


def _row(estado=None, apertura=None, fase=None, **extra):
    r = {}
    if estado is not None:
        r["estado_del_procedimiento"] = estado
    if apertura is not None:
        r["estado_de_apertura_del_proceso"] = apertura
    if fase is not None:
        r["fase"] = fase
    r.update(extra)
    return r


# 1
def test_published_open_offer_is_offer_phase_high():
    a = classify_opportunity_stage(_row("Publicado", "Abierto", "Presentación de oferta"))
    assert a.stage == "offer_phase_indicated"
    assert a.confidence == "high"
    assert "phase_offer_submission" in a.reasons


# 2
def test_offer_phase_but_opening_closed_is_conflict():
    a = classify_opportunity_stage(_row("Publicado", "Cerrado", "Presentación de oferta"))
    assert a.stage == "conflicting_signals"
    assert "offer_phase_but_opening_closed" in a.conflicts


# 3
def test_selected_state_but_offer_phase_is_conflict():
    a = classify_opportunity_stage(_row("Seleccionado", "Abierto", "Presentación de oferta"))
    assert a.stage == "conflicting_signals"
    assert "offer_phase_but_state_post_offer" in a.conflicts


# 4 — borradores suelen tener fase NULL → igual deben ser pre-oferta (OR)
def test_draft_with_null_phase_is_pre_offer():
    a = classify_opportunity_stage(_row("Borrador", fase=None))
    assert a.stage == "pre_offer_signal"
    assert a.confidence == "high"
    assert "procedure_state_draft" in a.reasons


# 5
def test_published_observations_is_pre_offer():
    a = classify_opportunity_stage(_row("Publicado", "Abierto", "Presentación de observaciones"))
    assert a.stage == "pre_offer_signal"
    assert "phase_observations" in a.reasons


# 6
def test_published_selection_is_post_offer():
    a = classify_opportunity_stage(_row("Publicado", "Abierto", "Selección de ofertas"))
    assert a.stage == "post_offer_signal"
    assert "phase_offer_selection" in a.reasons


# 7 — fase extranjera no catalogada → unknown, preservando raw_phase
def test_foreign_phase_is_unknown_preserves_raw():
    a = classify_opportunity_stage(_row("Publicado", "Abierto", "Estimate Phase"))
    assert a.stage == "unknown"
    assert a.raw_phase == "Estimate Phase"
    assert "phase_unrecognized" in a.reasons


# 8 — mayúsculas/tildes no cambian la categoría
def test_case_and_accent_insensitive():
    base = classify_opportunity_stage(_row("Publicado", "Abierto", "Presentación de oferta"))
    variant = classify_opportunity_stage(_row("PUBLICADO", "abierto", "PRESENTACION DE OFERTA"))
    assert base.stage == variant.stage == "offer_phase_indicated"


# 9 — pureza: no muta el input
def test_input_not_mutated():
    row = _row("Publicado", "Abierto", "Presentación de oferta")
    snapshot = copy.deepcopy(row)
    classify_opportunity_stage(row)
    assert row == snapshot


# 10 — fila de contratos / sin campos de proceso → unknown, sin excepción
def test_non_process_row_is_unknown_no_exception():
    a = classify_opportunity_stage({"id_contrato": "X", "valor_del_contrato": "100"})
    assert a.stage == "unknown"
    assert "no_process_signals" in a.reasons


def test_non_dict_is_unknown_no_exception():
    assert classify_opportunity_stage(None).stage == "unknown"  # type: ignore[arg-type]


# 11 — fases parecidas pero opuestas NO colisionan (catálogo, no substring)
def test_offer_vs_selection_do_not_collide():
    offer = classify_opportunity_stage(_row("Publicado", "Abierto", "Presentación de oferta"))
    selection = classify_opportunity_stage(_row("Publicado", "Abierto", "Selección de ofertas"))
    assert offer.stage == "offer_phase_indicated"
    assert selection.stage == "post_offer_signal"


# 12 — reasons/conflicts deterministas y estables
def test_reasons_are_deterministic():
    row = _row("Seleccionado", "Cerrado", "Presentación de oferta")
    a = classify_opportunity_stage(row)
    b = classify_opportunity_stage(dict(row))
    assert a == b
    assert isinstance(a.reasons, tuple) and isinstance(a.conflicts, tuple)


# Extra — el resultado es del tipo correcto
def test_returns_assessment():
    assert isinstance(
        classify_opportunity_stage(_row("Publicado")), OpportunityStageAssessment
    )


# Normalización con espacios extra / puntuación
def test_whitespace_and_punctuation_normalized():
    a = classify_opportunity_stage(_row("Publicado", "Abierto", "  Presentación   de  oferta "))
    assert a.stage == "offer_phase_indicated"


# Señales parciales — fase de oferta sin estado ni apertura → medium, no high
def test_offer_phase_only_is_medium():
    a = classify_opportunity_stage(_row(fase="Presentación de oferta"))
    assert a.stage == "offer_phase_indicated"
    assert a.confidence == "medium"


# Señal insuficiente → unknown/low con razón explícita
def test_insufficient_signals_is_unknown_low():
    a = classify_opportunity_stage(_row("Abierto"))  # estado 'Abierto' sin fase
    assert a.stage == "unknown"
    assert a.confidence == "low"
    assert a.reasons  # nunca vacío


# Anomalía apertura-abierta en post-oferta: razón observada, NO conflicto, y medium
def test_post_offer_open_aperture_is_observed_not_conflict():
    a = classify_opportunity_stage(_row("Publicado", "Abierto", "Selección de ofertas"))
    assert a.stage == "post_offer_signal"
    assert "opening_state_open_observed" in a.reasons
    assert a.conflicts == ()
    assert a.confidence == "medium"  # señales no plenamente alineadas


def test_post_offer_state_closed_aperture_is_high():
    a = classify_opportunity_stage(_row("Seleccionado", "Cerrado"))
    assert a.stage == "post_offer_signal"
    assert a.confidence == "high"


# Acepta un Mapping que no es dict (no debe degradar a unknown)
def test_accepts_non_dict_mapping():
    from types import MappingProxyType
    row = MappingProxyType({
        "estado_del_procedimiento": "Publicado",
        "estado_de_apertura_del_proceso": "Abierto",
        "fase": "Presentación de oferta",
    })
    assert classify_opportunity_stage(row).stage == "offer_phase_indicated"


# estado="Abierto" real (apertura abierta) es oferta de alta confianza
def test_open_state_open_opening_offer_phase_is_high():
    a = classify_opportunity_stage(_row("Abierto", "Abierto", "Presentación de oferta"))
    assert a.stage == "offer_phase_indicated"
    assert a.confidence == "high"
    assert "procedure_state_open" in a.reasons


# estado="Abierto" residual (apertura cerrada) + fase oferta → conflicto
def test_open_state_closed_opening_offer_phase_is_conflict():
    a = classify_opportunity_stage(_row("Abierto", "Cerrado", "Presentación de oferta"))
    assert a.stage == "conflicting_signals"
    assert "offer_phase_but_opening_closed" in a.conflicts
