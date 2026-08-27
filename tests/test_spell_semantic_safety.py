"""INV-SPELL-CORRECTION-CANNOT-CREATE-SEMANTIC-DIMENSION.

Una corrección ortográfica arregla cómo se escribió una palabra; no decide de
qué habla la consulta. El caso que motiva estas pruebas —``otra`` → ``obra``—
no se cierra comprobando esas dos palabras: se cierra comprobando que **ninguna**
palabra del lenguaje funcional puede ser reescrita, porque cualquiera de ellas
está a una distancia de edición de algún término del dominio.
"""
from __future__ import annotations

import pytest

from app.data.stopwords import STOPWORDS
from app.utils.spell_correction import VOCABULARY, _correct_word, correct_query


@pytest.mark.parametrize("word", sorted(STOPWORDS))
def test_functional_language_is_never_rewritten(word: str) -> None:
    """Ninguna palabra vacía puede convertirse en un término del dominio."""
    assert _correct_word(word) == word


def test_no_functional_word_survives_as_a_domain_topic() -> None:
    """La comprobación de arriba, pero sobre la frase entera."""
    for phrase in (
        "quitalo todo y empezamos otra vez",
        "muestrame otras opciones",
        "dame otra cosa",
    ):
        corrected = correct_query(phrase)
        assert "obra" not in corrected.split(), (
            f"'{phrase}' fabricó un tema contractual: '{corrected}'"
        )


# ── La protección no puede costar las correcciones reales ────────────────

@pytest.mark.parametrize(
    ("typo", "expected"),
    [
        ("obrra", "obra"),
        ("mantenimeinto", "mantenimiento"),
        ("alcaldiaa", "alcaldia"),
        ("saludd", "salud"),
    ],
)
def test_real_typos_are_still_corrected(typo: str, expected: str) -> None:
    assert _correct_word(typo) == expected


def test_garbage_characters_are_still_cleaned() -> None:
    assert correct_query("servid=cios profesionales").split()[0] == "servicios"


def test_protection_does_not_shrink_the_correction_targets() -> None:
    """El vocabulario de DESTINO no cambia: solo se prohíbe el origen vacío.

    ``obra`` sigue siendo un destino legítimo —"obrra" se corrige— aunque
    ``otra`` haya dejado de ser un origen válido para llegar a él.
    """
    assert "obra" in VOCABULARY
    assert "otra" in STOPWORDS
