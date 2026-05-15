"""Tests para app/core/narrator.py

Cubre:
- Normalización de todos los formatos de monto (7 formatos del plan)
- extract_monetary_values + extract_real_monetary_values
- validate_grounding: pass, fail, sin cifras, sin datos monetarios
- Tolerancia ±1%
- NarratorHandler.narrate_with_grounding con mock del LLM
"""

from __future__ import annotations

import pytest

from app.core.narrator import (
    NarratorHandler,
    extract_monetary_values,
    extract_real_monetary_values,
    validate_grounding,
    _parse_amount_str,
)


# ---------------------------------------------------------------------------
# _parse_amount_str
# ---------------------------------------------------------------------------

class TestParseAmountStr:
    def test_pesos_con_puntos(self):
        # $750.000.000
        assert _parse_amount_str("750.000.000", "") == pytest.approx(750_000_000)

    def test_millones_sufijo(self):
        # $750M
        assert _parse_amount_str("750", "M") == pytest.approx(750_000_000)

    def test_millones_palabra(self):
        assert _parse_amount_str("750", "millones") == pytest.approx(750_000_000)

    def test_cop_sufijo_ignorado(self):
        # 750M COP — COP no es un multiplicador
        assert _parse_amount_str("750", "M") == pytest.approx(750_000_000)

    def test_mm_sufijo(self):
        # $1.5MM
        assert _parse_amount_str("1.5", "MM") == pytest.approx(1_500_000_000)

    def test_mil_millones_sufijo(self):
        assert _parse_amount_str("1.5", "mil millones") == pytest.approx(1_500_000_000)

    def test_coma_como_decimal(self):
        # 1,5 millones
        assert _parse_amount_str("1,5", "millones") == pytest.approx(1_500_000)

    def test_coma_como_miles(self):
        # 1,500 (anglosajón) = 1500
        assert _parse_amount_str("1,500", "") == pytest.approx(1500)

    def test_punto_decimal_con_sufijo(self):
        assert _parse_amount_str("1.5", "millones") == pytest.approx(1_500_000)

    def test_sin_sufijo_numero_grande(self):
        assert _parse_amount_str("750000000", "") == pytest.approx(750_000_000)

    def test_mil_sufijo(self):
        assert _parse_amount_str("500", "mil") == pytest.approx(500_000)


# ---------------------------------------------------------------------------
# extract_monetary_values
# ---------------------------------------------------------------------------

class TestExtractMonetaryValues:
    def test_formato_pesos_colombianos(self):
        vals = extract_monetary_values("El contrato fue por $750.000.000")
        assert 750_000_000 in vals

    def test_formato_m(self):
        vals = extract_monetary_values("valor de $750M")
        assert 750_000_000 in vals

    def test_formato_millones(self):
        vals = extract_monetary_values("$750 millones")
        assert 750_000_000 in vals

    def test_formato_mm(self):
        vals = extract_monetary_values("contrato de $1.5MM")
        assert 1_500_000_000 in vals

    def test_formato_mil_millones(self):
        vals = extract_monetary_values("$1.5 mil millones")
        assert 1_500_000_000 in vals

    def test_multiple_cifras(self):
        vals = extract_monetary_values("contratos por $100M y $200M")
        assert 100_000_000 in vals
        assert 200_000_000 in vals

    def test_sin_cifras(self):
        vals = extract_monetary_values("contratos de obra vial en Atlántico")
        assert len(vals) == 0

    def test_ignora_valores_pequeños(self):
        # Números menores a 1000 son ruido (años, IDs, etc.)
        vals = extract_monetary_values("año 2024, contrato número 15")
        assert not any(v < 1_000 for v in vals)


# ---------------------------------------------------------------------------
# extract_real_monetary_values
# ---------------------------------------------------------------------------

class TestExtractRealMonetaryValues:
    def test_precio_base(self):
        rows = [{"precio_base": "750000000"}]
        vals = extract_real_monetary_values(rows)
        assert 750_000_000 in vals

    def test_valor_contrato(self):
        rows = [{"valor_del_contrato": "1500000000"}]
        vals = extract_real_monetary_values(rows)
        assert 1_500_000_000 in vals

    def test_float_en_campo(self):
        rows = [{"precio_base": 750000000.0}]
        vals = extract_real_monetary_values(rows)
        assert 750_000_000.0 in vals

    def test_campo_none_ignorado(self):
        rows = [{"precio_base": None}]
        vals = extract_real_monetary_values(rows)
        assert len(vals) == 0

    def test_multiple_rows(self):
        rows = [
            {"precio_base": "100000000"},
            {"precio_base": "200000000"},
        ]
        vals = extract_real_monetary_values(rows)
        assert 100_000_000 in vals
        assert 200_000_000 in vals


# ---------------------------------------------------------------------------
# validate_grounding
# ---------------------------------------------------------------------------

class TestValidateGrounding:
    def _row(self, precio: float) -> dict:
        return {"precio_base": str(precio)}

    def test_cifra_exacta_pasa(self):
        rows = [self._row(750_000_000)]
        assert validate_grounding("contrato de $750.000.000", rows) is True

    def test_cifra_dentro_tolerancia_pasa(self):
        # 749.800.000 ≈ 750M con ±1%
        rows = [self._row(749_800_000)]
        assert validate_grounding("contrato de $750M", rows) is True

    def test_cifra_fuera_tolerancia_falla(self):
        rows = [self._row(750_000_000)]
        assert validate_grounding("contrato de $760M", rows) is False

    def test_sin_cifras_narrativa_pasa(self):
        rows = [self._row(750_000_000)]
        assert validate_grounding("Se encontraron contratos en Atlántico", rows) is True

    def test_sin_datos_monetarios_rows_pasa(self):
        rows = [{"nombre_del_procedimiento": "obra vial"}]
        assert validate_grounding("contrato de $750M", rows) is True

    def test_tolerancia_exactamente_1_porciento_pasa(self):
        rows = [self._row(750_000_000)]
        # 757.500.000 = 750M * 1.01
        assert validate_grounding("$757.500.000", rows) is True

    def test_tolerancia_sobre_1_porciento_falla(self):
        rows = [self._row(750_000_000)]
        # 758M > 750M * 1.01
        assert validate_grounding("$758M", rows) is False

    def test_multiples_cifras_una_invalida(self):
        rows = [self._row(100_000_000), self._row(200_000_000)]
        # 300M no existe en los rows
        assert validate_grounding("contratos por $100M y $300M", rows) is False

    def test_multiples_cifras_todas_validas(self):
        rows = [self._row(100_000_000), self._row(200_000_000)]
        assert validate_grounding("contratos por $100M y $200M", rows) is True


# ---------------------------------------------------------------------------
# NarratorHandler — tests con mock
# ---------------------------------------------------------------------------

class TestNarratorHandler:
    def _make_handler(self):
        return NarratorHandler(api_key=None)

    def test_disabled_sin_api_key(self):
        h = self._make_handler()
        assert h.enabled is False

    def test_narrate_returns_none_when_disabled(self):
        h = self._make_handler()
        result = h.narrate([], "contratos en Atlántico")
        assert result is None

    def test_narrate_with_grounding_returns_none_when_disabled(self):
        h = self._make_handler()
        result = h.narrate_with_grounding([], "contratos en Atlántico")
        assert result is None

    def test_narrate_with_grounding_uses_first_attempt(self, monkeypatch):
        """Si el primer intento pasa grounding, lo retorna sin reintentar."""
        h = NarratorHandler(api_key="fake-key")
        rows = [{"precio_base": "750000000"}]

        call_count = {"n": 0}

        def mock_narrate(self_inner, *args, **kwargs):
            call_count["n"] += 1
            return "El contrato fue por $750.000.000"

        monkeypatch.setattr(NarratorHandler, "narrate", mock_narrate)
        result = h.narrate_with_grounding(rows, "contratos", total_count=1)

        assert result == "El contrato fue por $750.000.000"
        assert call_count["n"] == 1

    def test_narrate_with_grounding_retries_on_fail(self, monkeypatch):
        """Si el primer intento falla grounding, reintenta con strict=True."""
        h = NarratorHandler(api_key="fake-key")
        rows = [{"precio_base": "750000000"}]

        attempts: list[bool] = []

        def mock_narrate(self_inner, *args, **kwargs):
            strict = kwargs.get("strict", False)
            attempts.append(strict)
            if not strict:
                return "El contrato fue por $900M"  # cifra inventada
            return "El contrato fue por $750.000.000"  # cifra correcta

        monkeypatch.setattr(NarratorHandler, "narrate", mock_narrate)
        result = h.narrate_with_grounding(rows, "contratos", total_count=1)

        assert result == "El contrato fue por $750.000.000"
        assert attempts == [False, True]

    def test_narrate_with_grounding_fallback_on_double_fail(self, monkeypatch):
        """Si ambos intentos fallan grounding, retorna None."""
        h = NarratorHandler(api_key="fake-key")
        rows = [{"precio_base": "750000000"}]

        def mock_narrate(self_inner, *args, **kwargs):
            return "El contrato fue por $999M"

        monkeypatch.setattr(NarratorHandler, "narrate", mock_narrate)
        result = h.narrate_with_grounding(rows, "contratos", total_count=1)

        assert result is None
