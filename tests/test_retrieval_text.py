"""Invariantes puras de normalize_retrieval_text (Commit 2)."""
from __future__ import annotations

import pytest

from app.core.retrieval_text import normalize_retrieval_text as n


def test_none_and_empty():
    assert n(None) == ""
    assert n("") == ""
    assert n("   ") == ""


def test_lowercase_and_accents():
    assert n("CONTRATACIÓN") == "contratacion"
    assert n("Diseño") == "diseno"


def test_newlines_tabs_and_repeated_spaces():
    assert n("MANUAL DE\nCONTRATACIÓN") == "manual de contratacion"
    assert n("manual\tde\tcontratacion") == "manual de contratacion"
    assert n("manual   de    contratacion") == "manual de contratacion"


def test_punctuation_becomes_space():
    assert n("Manual;  interno de contratación") == "manual interno de contratacion"
    assert n("manual, de. contratacion:") == "manual de contratacion"


def test_alphanumerics_preserved_not_shredded():
    # guiones/barras -> espacio; números se conservan (no se trituran)
    assert n("contrato 045-2024") == "contrato 045 2024"
    assert n("manual/de/contratacion") == "manual de contratacion"


def test_idempotent():
    for s in ["MANUAL DE\nCONTRATACIÓN", "Manual;  interno", "contrato 045-2024", "  x  ", None]:
        once = n(s)
        assert n(once) == once


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("ELABORACIÓN DEL MANUAL DE CONTRATACIÓN", "elaboracion del manual de contratacion"),
        ("ACTUALIZACIÓN DEL MANUAL INTERNO DE CONTRATACIÓN", "actualizacion del manual interno de contratacion"),
    ],
)
def test_representative_objeto_samples(raw, expected):
    # muestras de DEVELOPMENT (formato real de SECOP), nunca del holdout
    assert n(raw) == expected
