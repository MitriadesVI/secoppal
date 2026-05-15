"""Tests for the spell correction module."""
from __future__ import annotations

import pytest

from app.utils.spell_correction import correct_query


class TestCorrectCommonTypos:
    """Typos in procurement terms should be corrected."""

    def test_pretacion_to_prestacion(self):
        assert correct_query("pretacion") == "prestacion"

    def test_ccontratos_to_contratos(self):
        assert correct_query("ccontratos") == "contratos"

    def test_alcladia_to_alcaldia(self):
        assert correct_query("alcladia") == "alcaldia"


class TestCleansSpecialChars:
    """Garbage chars embedded in words should be stripped, then corrected."""

    def test_servid_equals_cios(self):
        assert correct_query("servid=cios") == "servicios"


class TestPreservesCorrectWords:
    """Words already in the vocabulary should pass through unchanged."""

    def test_construccion_unchanged(self):
        assert correct_query("construccion") == "construccion"

    def test_prestacion_unchanged(self):
        assert correct_query("prestacion") == "prestacion"

    def test_interventoria_unchanged(self):
        assert correct_query("interventoria") == "interventoria"


class TestPreservesShortWords:
    """Words of 3 or fewer characters should never be corrected."""

    def test_de(self):
        assert correct_query("de") == "de"

    def test_en(self):
        assert correct_query("en") == "en"

    def test_la(self):
        assert correct_query("la") == "la"

    def test_por(self):
        assert correct_query("por") == "por"


class TestPreservesUnknownWords:
    """Proper nouns and unknown words far from the vocabulary should be left alone."""

    def test_valledupar(self):
        assert correct_query("valledupar") == "valledupar"

    def test_bogota(self):
        assert correct_query("bogota") == "bogota"

    def test_antioquia(self):
        assert correct_query("antioquia") == "antioquia"


class TestFullQueryCorrection:
    """End-to-end query correction."""

    def test_full_query(self):
        result = correct_query("ccontratos por pretacion de servid=cios")
        assert result == "contratos por prestacion de servicios"

    def test_mixed_correct_and_typos(self):
        result = correct_query("construccion y manteniminto de infraestrucutra")
        # construccion is correct, manteniminto->mantenimiento, infraestrucutra->infraestructura
        assert "construccion" in result
        assert "mantenimiento" in result
        assert "infraestructura" in result

    def test_preserves_numbers(self):
        result = correct_query("contratos por 5000000")
        assert "5000000" in result
        assert "contratos" in result

class TestFeedbackTypos:
    """Typos reales observados en feedback.jsonl (1.3)."""

    def test_alcadia_to_alcaldia(self):
        assert correct_query("alcadia") == "alcaldia"

    def test_licitasciones_to_licitaciones(self):
        result = correct_query("licitasciones")
        assert result == "licitaciones"

    def test_consutoria_to_consultoria(self):
        assert correct_query("consutoria") == "consultoria"

    def test_manteniminto_to_mantenimiento(self):
        assert correct_query("manteniminto") == "mantenimiento"

    def test_infraestrucutra_to_infraestructura(self):
        assert correct_query("infraestrucutra") == "infraestructura"

    def test_gobernacion_unchanged(self):
        assert correct_query("gobernacion") == "gobernacion"


class TestNewVocabularyTerms:
    """Nuevos terminos agregados en 1.3 deben reconocerse correctamente."""

    def test_vial_unchanged(self):
        assert correct_query("vial") == "vial"

    def test_destronque_unchanged(self):
        assert correct_query("destronque") == "destronque"

    def test_mobiliario_unchanged(self):
        assert correct_query("mobiliario") == "mobiliario"

    def test_hidrografia_unchanged(self):
        assert correct_query("hidrografia") == "hidrografia"

    def test_agroforestales_unchanged(self):
        assert correct_query("agroforestales") == "agroforestales"

    def test_convocatoria_unchanged(self):
        assert correct_query("convocatoria") == "convocatoria"

    def test_ferreas_unchanged(self):
        assert correct_query("ferreas") == "ferreas"

    def test_vocab_size_above_80(self):
        from app.utils.spell_correction import VOCABULARY
        assert len(VOCABULARY) >= 80, f"Vocabulary only has {len(VOCABULARY)} terms"
