"""Commit 4: validador puro de frases sobre filas (PhraseMatch).

Casos sintéticos + muestras de development. NO se mira el holdout al diseñar.
"""
from __future__ import annotations

import dataclasses

import pytest

from app.core.phrase_spec import PhraseSpec
from app.core.phrase_validator import PhraseMatch, validate_phrase

SPEC = PhraseSpec(
    canonical="manual de contratacion",
    ordered_anchors=("manual", "contratacion"),
    variants=("manuales de contratacion",),
    max_gap_tokens=2,
)


def v(objeto="", descripcion=""):
    return validate_phrase(SPEC, objeto, descripcion)


# ── direct ───────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "objeto",
    [
        "Elaboración del MANUAL DE CONTRATACIÓN del municipio",
        "actualización de los MANUALES DE CONTRATACIÓN institucionales",
        "ajuste del manual interno de contratación de la ESE",
        "ACTUALIZACION DEL MANUAL DE\nCONTRATACIÓN ADAPTADO",  # texto sucio
    ],
)
def test_direct_matches(objeto):
    m = v(objeto=objeto)
    assert m.level == "direct"
    assert m.field == "objeto"


# ── weak: anclas en el mismo campo pero demasiado separadas ──────────────────
def test_weak_when_anchors_too_far():
    m = v(objeto="actualizacion del manual de funciones para apoyar procesos de contratacion")
    assert m.level == "weak"
    assert m.gap_tokens is not None and m.gap_tokens > SPEC.max_gap_tokens


# ── none ─────────────────────────────────────────────────────────────────────
def test_none_when_anchors_split_across_fields():
    # 'manual' en objeto, 'contratacion' en descripcion → NO es frase
    m = v(objeto="actualizacion del manual institucional", descripcion="apoyo a la contratacion")
    assert m.level == "none"
    assert m.field == "none"


def test_none_when_anchor_missing():
    assert v(objeto="elaboracion del manual de funciones").level == "none"


def test_none_when_order_inverted():
    assert v(objeto="procesos de contratacion y elaboracion del manual de funciones").level == "none"


# ── preferencia de campo y contrato del dataclass ────────────────────────────
def test_objeto_preferred_over_descripcion_on_tie():
    m = v(objeto="manual de contratacion", descripcion="manual de contratacion")
    assert m.field == "objeto" and m.level == "direct"


def test_phrasematch_is_frozen():
    with pytest.raises(dataclasses.FrozenInstanceError):
        v(objeto="manual de contratacion").level = "none"  # type: ignore[misc]


def test_purity_no_action_interpretation():
    # la acción (elaboracion/actualizacion) NO afecta la validación de la frase
    con_accion = v(objeto="elaboracion del manual de contratacion")
    sin_accion = v(objeto="el manual de contratacion vigente")
    assert con_accion.level == sin_accion.level == "direct"
