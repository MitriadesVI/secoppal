"""Tests for the spell correction module."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from app.utils.money import normalize_text
from app.utils.spell_correction import (
    PROTECTED_MODALITY_VOCABULARY,
    PROTECTED_STATE_VOCABULARY,
    PROTECTED_VOCABULARY,
    correct_query,
)

_LEXICON_PATH = (
    Path(__file__).resolve().parent.parent
    / "app"
    / "data"
    / "modality_lexicon_v1.yaml"
)


def _normalized_modality_phrases() -> set[str]:
    payload = yaml.safe_load(_LEXICON_PATH.read_text(encoding="utf-8"))
    return {
        normalize_text(phrase)
        for rule in payload["rules"]
        for phrase in rule.get("phrases", [])
    }


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

    @pytest.mark.parametrize(
        "word",
        [
            "proveedor",
            "proveedores",
            "publica",
            "publicas",
            "publico",
            "publicos",
            "publicado",
            "publicada",
            "publicados",
            "publicadas",
            "concurso",
            "concursos",
        ],
    )
    def test_semantically_distinct_domain_forms_are_preserved(self, word):
        assert correct_query(word) == word


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

    @pytest.mark.parametrize(
        "raw",
        [
            "solicitud de información a proveedores",
            "licitación pública de alimentación",
            "concurso de méritos abierto de interventoría",
            "procesos publicados de alimentación",
            "licitación publicada",
            "licitación pública publicada",
            "contratación pública",
            "contratación directa publicada",
            "concurso público de méritos",
            "proveedores publicados",
            "proveedor de alimentación",
            "solicitud a proveedor",
        ],
    )
    def test_domain_context_is_identity_preserved_after_normalization(self, raw):
        canonical = normalize_text(raw)
        assert correct_query(canonical) == canonical

    def test_accented_input_is_not_the_router_identity_test(self):
        assert correct_query("pública") == "pública"
        assert correct_query(normalize_text("pública")) == "publica"


class TestConservativeCorrection:
    @pytest.mark.parametrize(
        ("typo", "expected"),
        [
            ("publcia", "publica"),
            ("dircta", "directa"),
            ("abirto", "abierto"),
        ],
    )
    def test_unique_single_edit_typo_is_corrected(self, typo, expected):
        assert correct_query(typo) == expected

    def test_single_edit_tie_is_fail_closed(self):
        assert correct_query("proveedors") == "proveedors"


class TestStateVocabularyProtection:
    @pytest.mark.parametrize(
        "word",
        ["abierta", "abiertas", "abierto", "abiertos"],
    )
    def test_open_gender_and_number_are_preserved(self, word):
        assert correct_query(word) == word

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("licitaciones abiertas", "licitaciones abiertas"),
            (
                "licitación pública abierta",
                "licitacion publica abierta",
            ),
        ],
    )
    def test_open_query_matrix_matches_router_input(self, raw, expected):
        assert correct_query(normalize_text(raw)) == expected


class TestModalityLexiconProtection:
    def test_all_normalized_modality_phrases_are_identity_preserved(self):
        aliases = _normalized_modality_phrases()
        changed = {
            alias: correct_query(alias)
            for alias in aliases
            if correct_query(alias) != alias
        }
        assert len(aliases) == 106
        assert changed == {}

    def test_protected_token_metric_is_stable(self):
        assert len(PROTECTED_MODALITY_VOCABULARY) == 96
        assert len(PROTECTED_STATE_VOCABULARY) > 0
        assert PROTECTED_MODALITY_VOCABULARY <= PROTECTED_VOCABULARY
        assert PROTECTED_STATE_VOCABULARY <= PROTECTED_VOCABULARY

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
