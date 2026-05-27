"""Tests for app.core.contextual_suggestions."""
from __future__ import annotations

import pytest
from datetime import date, timedelta

from app.core.contextual_suggestions import (
    compute_contextual_filters,
    detect_test_or_duplicate,
    apply_refinement,
    _adaptive_monto_thresholds,
    _round_to_nice,
)


# ── Fixtures ───────────────────────────────────────────────────────────

def _make_row_procesos(
    nombre="Mantenimiento vial",
    referencia="LP-2026-001",
    estado="Abierto",
    fecha="2026-03-15T00:00:00",
    precio=800_000_000,
    entidad="MEDELLÍN",
    ciudad="Medellín",
):
    return {
        "nombre_del_procedimiento": nombre,
        "referencia_del_proceso": referencia,
        "estado_de_apertura_del_proceso": estado,
        "fecha_de_publicacion_del": fecha,
        "precio_base": precio,
        "entidad": entidad,
        "ciudad_entidad": ciudad,
    }


def _make_row_contratos(
    objeto="Suministro equipo",
    referencia="CD-08-2026-100",
    estado="En ejecución",
    fecha="2026-04-10T00:00:00",
    valor=2_000_000_000,
    entidad="SENA",
    contratista="Proveedor SAS",
):
    return {
        "objeto_del_contrato": objeto,
        "referencia_del_contrato": referencia,
        "estado_contrato": estado,
        "fecha_de_firma": fecha,
        "valor_del_contrato": valor,
        "nombre_entidad": entidad,
        "proveedor_adjudicado": contratista,
    }


# ═══════════════════════════════════════════════════════════════════════
# Test: detect_test_or_duplicate
# ═══════════════════════════════════════════════════════════════════════

class TestDetectTestOrDuplicate:

    def test_detect_copia_in_referencia(self):
        row = _make_row_procesos(referencia="0070007831 copia")
        result = detect_test_or_duplicate(row, "p6dx-8zbt")
        assert result == "posible duplicado"

    def test_detect_prueba_in_nombre(self):
        row = _make_row_procesos(nombre="IP-prueba-010-2024")
        result = detect_test_or_duplicate(row, "p6dx-8zbt")
        assert result == "posible proceso de prueba"

    def test_detect_test_in_nombre(self):
        row = _make_row_procesos(nombre="Proceso test servicios")
        result = detect_test_or_duplicate(row, "p6dx-8zbt")
        assert result == "posible test"

    def test_detect_xxx_placeholder(self):
        row = _make_row_procesos(referencia="SI-CD--2026XXXX")
        result = detect_test_or_duplicate(row, "p6dx-8zbt")
        assert result == "placeholder no rellenado"

    def test_detect_placeholder_word(self):
        row = _make_row_contratos(objeto="placeholder servicio medico")
        result = detect_test_or_duplicate(row, "jbjy-vk9h")
        assert result == "placeholder no rellenado"

    def test_detect_incomplete_reference_si_dash(self):
        row = _make_row_procesos(referencia="SI-")
        result = detect_test_or_duplicate(row, "p6dx-8zbt")
        assert result == "referencia incompleta"

    def test_detect_incomplete_reference_cd_dash(self):
        row = _make_row_procesos(referencia="CD-")
        result = detect_test_or_duplicate(row, "p6dx-8zbt")
        assert result == "referencia incompleta"

    def test_no_detect_normal_text(self):
        row = _make_row_procesos(
            nombre="Mantenimiento vial rural",
            referencia="LP-2026-001",
        )
        result = detect_test_or_duplicate(row, "p6dx-8zbt")
        assert result is None

    def test_no_detect_empty_row(self):
        row = {}
        result = detect_test_or_duplicate(row, "p6dx-8zbt")
        assert result is None

    def test_no_detect_copia_in_normal_word(self):
        """'Copia' embebida en palabra normal no debe detectarse."""
        row = _make_row_contratos(objeto="Fotocopiadora industrial")
        result = detect_test_or_duplicate(row, "jbjy-vk9h")
        # "fotocopiadora" contiene "copia" pero con \b debe fallar
        # Wait, actually "copia" IS in "fotocopiadora"... let's see.
        # If the regex is \bcopia\b in IGNORECASE:
        # "fotocopiadora" → the \b before 'c' won't match because 'c' is
        # preceded by 'o' (word char), so it won't match. Correct.
        assert result is None

    def test_contratos_searches_correct_fields(self):
        """detect_test_or_duplicate must search objeto_del_contrato, 
        not nombre_del_procedimiento for contratos."""
        row = _make_row_contratos(
            objeto="servicio normal",
            referencia="CD-08-2026-test",
        )
        result = detect_test_or_duplicate(row, "jbjy-vk9h")
        assert result == "posible test"

    def test_returns_first_match_only(self):
        row = _make_row_procesos(
            nombre="prueba de copia test",
            referencia="normal",
        )
        result = detect_test_or_duplicate(row, "p6dx-8zbt")
        # First match in nombre is "prueba"
        assert result == "posible proceso de prueba"


# ═══════════════════════════════════════════════════════════════════════
# Test: compute_contextual_filters
# ═══════════════════════════════════════════════════════════════════════

class TestComputeContextualFilters:

    def test_returns_empty_for_empty_results(self):
        result = compute_contextual_filters([], "p6dx-8zbt", {})
        assert result == {}

    def test_returns_estado_options(self):
        results = [
            _make_row_procesos(estado="Abierto"),
            _make_row_procesos(estado="Abierto"),
            _make_row_procesos(estado="Cerrado"),
            _make_row_procesos(estado="Adjudicado"),
        ]
        filters = compute_contextual_filters(results, "p6dx-8zbt", {})
        assert "estado" in filters
        estados = [o["value"] for o in filters["estado"]]
        assert estados[0] == "Abierto"  # most frequent first
        assert len(filters["estado"]) == 3

    def test_skips_estado_when_user_already_filtered(self):
        results = [_make_row_procesos(estado="Abierto")] * 5
        params = {"estado": "Abierto"}
        filters = compute_contextual_filters(results, "p6dx-8zbt", params)
        assert "estado" not in filters

    def test_skips_entity_when_user_already_filtered(self):
        results = [_make_row_procesos(entidad="MEDELLÍN")] * 5
        params = {"entidad_resolved": "MEDELLÍN"}
        filters = compute_contextual_filters(results, "p6dx-8zbt", params)
        assert "entidad" not in filters

    def test_includes_ciudad_when_depto_filtered_but_not_ciudad(self):
        results = [
            _make_row_procesos(ciudad="Medellín"),
            _make_row_procesos(ciudad="Medellín"),
            _make_row_procesos(ciudad="Itagüí"),
        ]
        params = {"departamento_resolved": "Antioquia"}
        filters = compute_contextual_filters(results, "p6dx-8zbt", params)
        assert "ciudad" in filters

    def test_skips_ciudad_when_already_filtered(self):
        results = [_make_row_procesos(ciudad="Medellín")] * 5
        params = {"departamento_resolved": "Antioquia", "ciudad": "Medellín"}
        filters = compute_contextual_filters(results, "p6dx-8zbt", params)
        assert "ciudad" not in filters

    def test_max_3_options_per_category(self):
        results = [
            _make_row_procesos(entidad=f"ENTIDAD_{i}")
            for i in range(10)
        ]
        filters = compute_contextual_filters(results, "p6dx-8zbt", {})
        assert len(filters.get("entidad", [])) <= 3

    def test_skips_category_with_less_than_2_options(self):
        results = [_make_row_procesos(estado="Abierto")] * 10
        filters = compute_contextual_filters(results, "p6dx-8zbt", {})
        assert "estado" not in filters  # only 1 distinct value

    def test_contratos_uses_correct_fields(self):
        results = [
            _make_row_contratos(estado="En ejecución"),
            _make_row_contratos(estado="En ejecución"),
            _make_row_contratos(estado="Liquidado"),
            _make_row_contratos(estado="Cerrado"),
        ]
        filters = compute_contextual_filters(results, "jbjy-vk9h", {})
        assert "estado" in filters
        assert filters["estado"][0]["value"] == "En ejecución"
        assert "entidad" not in filters  # only 1 entity (SENA)

    def test_monto_skipped_when_user_already_filtered(self):
        results = [_make_row_procesos(precio=800_000_000)] * 5
        params = {"valor_min": 500_000_000}
        filters = compute_contextual_filters(results, "p6dx-8zbt", params)
        assert "monto" not in filters

    def test_fecha_skipped_when_user_already_filtered(self):
        results = [_make_row_procesos()] * 5
        params = {"fecha_desde": "2026-01-01"}
        filters = compute_contextual_filters(results, "p6dx-8zbt", params)
        assert "fecha" not in filters

    def test_monto_adapts_to_small_values(self):
        # Result set con valores 100M-400M: NO debe sugerir ">500M" (hardcoded viejo)
        # debe sugerir umbrales relativos al set real
        results = [
            _make_row_procesos(precio=v)
            for v in (100_000_000, 150_000_000, 200_000_000, 250_000_000,
                      300_000_000, 350_000_000, 400_000_000, 450_000_000)
        ]
        filters = compute_contextual_filters(results, "p6dx-8zbt", {})
        assert "monto" in filters
        thresholds = [opt["value"] for opt in filters["monto"]]
        # Ningún threshold debe exceder el máximo del set
        assert max(thresholds) <= 500_000_000
        # Y debe haber al menos uno por debajo de la mediana (útil para refinar)
        assert min(thresholds) < 300_000_000

    def test_monto_adapts_to_large_values(self):
        # Result set con valores 1MM-50MM: debe sugerir umbrales en MM, no hardcoded 500M
        results = [
            _make_row_procesos(precio=v)
            for v in (1_000_000_000, 5_000_000_000, 10_000_000_000,
                      15_000_000_000, 20_000_000_000, 30_000_000_000,
                      40_000_000_000, 50_000_000_000)
        ]
        filters = compute_contextual_filters(results, "p6dx-8zbt", {})
        assert "monto" in filters
        thresholds = [opt["value"] for opt in filters["monto"]]
        # Mínimo threshold debe ser >= 1MM (no irse a millones)
        assert min(thresholds) >= 1_000_000_000


class TestAdaptiveMontoHelpers:
    def test_round_to_nice_floors_to_finescale(self):
        # Escala fina {1, 2, 3, 5, 7} por década, siempre floor
        assert _round_to_nice(1_200_000) == 1_000_000      # 1.2 → 1
        assert _round_to_nice(2_800_000) == 2_000_000      # 2.8 → 2
        assert _round_to_nice(4_500_000) == 3_000_000      # 4.5 → 3
        assert _round_to_nice(6_000_000) == 5_000_000      # 6.0 → 5
        assert _round_to_nice(8_500_000) == 7_000_000      # 8.5 → 7
        assert _round_to_nice(450_000_000) == 300_000_000  # 4.5 → 3

    def test_round_to_nice_never_exceeds_input(self):
        # Invariante crítico: floor garantiza nice ≤ value
        for v in (1.1e9, 3.7e8, 9.9e6, 250_000):
            assert _round_to_nice(v) <= v

    def test_round_to_nice_handles_zero_and_negative(self):
        assert _round_to_nice(0) == 0.0
        assert _round_to_nice(-100) == 0.0

    def test_adaptive_thresholds_returns_empty_when_too_few_values(self):
        assert _adaptive_monto_thresholds([100, 200, 300]) == []

    def test_adaptive_thresholds_dedupes_and_sorts(self):
        values = sorted([200_000_000] * 10)
        thresholds = _adaptive_monto_thresholds(values)
        assert len(thresholds) <= 1  # todos los percentiles colapsan a 200M

    def test_adaptive_thresholds_caps_at_max(self):
        # Ningún threshold debe exceder el max real del set
        values = sorted([100_000_000, 200_000_000, 300_000_000, 400_000_000, 500_000_000])
        thresholds = _adaptive_monto_thresholds(values)
        assert all(t <= 500_000_000 for t in thresholds)


# ═══════════════════════════════════════════════════════════════════════
# Test: apply_refinement
# ═══════════════════════════════════════════════════════════════════════

class TestApplyRefinement:

    def test_merge_estado_preserves_context(self):
        params = {"departamento": "Antioquia", "valor_min": 600_000_000}
        result = apply_refinement(params, "estado", "Abierto")
        assert result["departamento"] == "Antioquia"
        assert result["valor_min"] == 600_000_000
        assert result["estado"] == "Abierto"

    def test_fecha_30d_converts_to_fecha_desde(self):
        today = date.today()
        result = apply_refinement({}, "fecha", "30d")
        expected = (today - timedelta(days=30)).isoformat()
        assert result["fecha_desde"] == expected

    def test_fecha_90d_converts_to_fecha_desde(self):
        today = date.today()
        result = apply_refinement({}, "fecha", "90d")
        expected = (today - timedelta(days=90)).isoformat()
        assert result["fecha_desde"] == expected

    def test_fecha_2026_converts_to_fecha_desde(self):
        result = apply_refinement({}, "fecha", "2026")
        assert result["fecha_desde"] == "2026-01-01"

    def test_monto_sets_valor_min(self):
        result = apply_refinement({}, "monto", 500_000_000)
        assert result["valor_min"] == 500_000_000

    def test_entidad_sets_entidad_resolved(self):
        result = apply_refinement({}, "entidad", "MEDELLÍN")
        assert result["entidad_resolved"] == "MEDELLÍN"

    def test_ciudad_sets_ciudad(self):
        result = apply_refinement({}, "ciudad", "Itagüí")
        assert result["ciudad"] == "Itagüí"

    def test_does_not_mutate_original(self):
        original = {"estado": "Cerrado"}
        result = apply_refinement(original, "estado", "Abierto")
        assert original["estado"] == "Cerrado"  # unchanged
        assert result["estado"] == "Abierto"
