"""Tests MORPHO-UNEVEN-001 — cobertura morfológica ≥3 variantes por rubro."""
from __future__ import annotations


def test_roob_to_variants_has_enough_coverage():
    """Al menos 50 roots con ≥1 variant (post-Sprint 5 expansion)."""
    from app.data.morphological_variants import ROOT_TO_VARIANTS

    assert len(ROOT_TO_VARIANTS) >= 50, f"Expected >=50 roots, got {len(ROOT_TO_VARIANTS)}"


def test_rubros_frecuentes_tienen_variantes():
    """15 rubros frecuentes tienen ≥3 variantes morfológicas cada uno."""
    from app.data.morphological_variants import ROOT_TO_VARIANTS

    expected_rubros = [
        "turismo", "equipo", "salud", "educacion", "alimentacion",
        "seguridad", "tecnologia", "agricultura", "ambiente", "vivienda",
        "sede", "uniforme", "insumo", "medicamento", "laboratorio",
    ]
    for rubro in expected_rubros:
        variants = ROOT_TO_VARIANTS.get(rubro, frozenset())
        assert len(variants) >= 2, (
            f"Rubro '{rubro}' tiene solo {len(variants)} variantes: {sorted(variants)}. "
            f"Se esperaban ≥3."
        )


def test_existing_rubros_preserved():
    """Rubros existentes no pierden sus variantes."""
    from app.data.morphological_variants import ROOT_TO_VARIANTS

    # cultura should still have ≥15 variants
    cultura_vars = ROOT_TO_VARIANTS.get("cultura", frozenset())
    assert len(cultura_vars) >= 15, f"cultura tiene {len(cultura_vars)} variantes, expected ≥15"

    # alimentacion_escolar (PAE)
    ae_vars = ROOT_TO_VARIANTS.get("alimentacion_escolar", frozenset())
    assert len(ae_vars) >= 6, f"alimentacion_escolar tiene {len(ae_vars)}, expected ≥6"
