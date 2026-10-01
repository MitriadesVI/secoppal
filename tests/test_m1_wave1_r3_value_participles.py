"""M1 wave 1 R3 (ZEUS2 P2-4): «300 millones o mayores firmados en 2025» is a bound again.

R2 made an adjectival suffix («o/y mayor|menor|superior|inferior») a bound only at the end of
the text, before punctuation or a connector. The plural form followed by a participle
(«o mayores firmados en 2025», «o superiores adjudicados», «o mayores (firmados)») was a
bound in the base and in R1, and R2 asked: a regression. The continuation after the suffix is
now also a participle or a finite verb of a closed morphological class
(``money_suffix_event``); a noun, an article or an adjective still asks («y mayor plazo»,
«o mayor duración», «o mayores vigentes»).

The property is suffixes x continuations. No polarity is ever inverted: every case reads the
bound of the suffix's own family or nothing.

M1 wave 1 VALUE PARITY: the legacy scanner (``money_polarity``, ``QueryRouter``) is byte-identical to
``main``; what it should read is the E-VALUE contract
(``docs/audits/m1_wave1/E_VALUE_REGRESSION_CASES.jsonl``). This file keeps the half that exercises the
quote-route reader (``money_comparators.read_comparators``) and ``money_suffix_event`` directly.
"""
from __future__ import annotations

import itertools

import pytest

from app.core import money_comparators as mc
from app.core import money_suffix_event as mse
from app.core.money_comparators import GTE, LTE, read_comparators

HEAD = "contratos de la Alcaldía de Galapa de 300 millones"
SUFFIXES = list(mc._ADJECTIVE_SUFFIXES)
OPPOSITE = {GTE: LTE, LTE: GTE}

AR_STEMS = ["firm", "adjudic", "celebr", "public", "otorg", "contrat", "perfeccion", "regist",
            "cancel", "suspend", "liquid", "ejecut"]
ER_IR_STEMS = ["vend", "recib", "cumpl", "suscrib", "perdid", "decid"]
PARTICIPLES = (
    [f"{stem}{ending}" for stem in AR_STEMS for ending in ("ado", "ada", "ados", "adas")]
    + [f"{stem}{ending}" for stem in ER_IR_STEMS for ending in ("ido", "ida", "idos", "idas")]
    + ["suscritos", "hechos", "abiertas", "escritos"]
)
VERBS = ["firmaron", "suscribieron", "adjudicaron", "se", "son", "sean", "están", "estén", "hayan",
         "han", "fueron", "tienen"]
# Nouns an adjective modifies, plus the nouns that merely LOOK like participles.
NOUNS = [
    "plazo", "plazos", "cuantía", "valor", "monto", "tamaño", "duración", "riesgo", "experiencia",
    "impacto", "presupuesto", "calidad", "contratos", "vigencia", "importe", "cifra", "vigentes",
    "recurrentes", "importantes", "relevantes",
    # nouns that only LOOK like participles (a control list written here, not read from the module)
    "estado", "estados", "mercado", "grado", "resultado", "ganado", "senado", "abogado", "sentido",
    "partido", "contenido", "lado", "cuidado", "condado", "pasado",
]
SEPARATORS = [" ", "  ", " - ", " – ", " (", "/", " / "]
CLOSERS = ["", ")", " en 2025", " por la Alcaldía", ", en 2025"]


def _families(text: str) -> tuple:
    return read_comparators(text).families


def test_the_word_class_is_what_the_orchestrator_asked_for():
    assert all(mse.is_event_word(word) for word in PARTICIPLES + VERBS)
    assert not any(mse.is_event_word(word) for word in NOUNS)
    for word in ("firmados", "FIRMADOS", "Adjudicadas", "vendidos"):
        assert mse.is_event_word(word), word


# ── the property: every suffix x every participle / verb is a bound of its own family ────────

@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_a_participle_or_verb_after_the_suffix_is_the_bound_of_the_suffix(phrase, family):
    bad = []
    for word, separator, closer in itertools.product(PARTICIPLES + VERBS, SEPARATORS, CLOSERS):
        text = f"{HEAD} {phrase}{separator}{word}{closer}"
        families = _families(text)
        if family not in families or OPPOSITE[family] in families:
            bad.append((text, families))
    assert not bad, bad[:5]


@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_an_adverb_before_the_participle_is_not_in_the_class_and_asks(phrase, family):
    """Closed class, fail-closed: only the NEXT word counts; «ya firmados» asks."""
    text = f"{HEAD} {phrase} ya firmados en 2025"
    assert _families(text) == ()


@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_a_noun_or_a_non_verbal_adjective_after_the_suffix_still_asks(phrase, family):
    bad = []
    for noun, closer in itertools.product(NOUNS, ["", " en 2025", " del contrato"]):
        text = f"{HEAD} {phrase} {noun}{closer}"
        if _families(text) != ():
            bad.append((text, _families(text)))
    assert not bad, bad[:5]


@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_a_participle_separated_by_a_noun_is_not_read(phrase, family):
    """The participle must be the NEXT word: «y mayor plazo firmados» is a noun phrase."""
    text = f"{HEAD} {phrase} plazo firmados en 2025"
    assert _families(text) == ()


@pytest.mark.parametrize("phrase,family", [(p, f) for p, f in SUFFIXES if p.startswith("o ")])
@pytest.mark.parametrize("accent_phrase", ["ó", "Ó", "O"])
def test_the_accented_or_capitalised_conjunction_is_read_like_the_plain_one(phrase, family, accent_phrase):
    text = f"{HEAD} {accent_phrase}{phrase[1:]} firmados"
    assert _families(text) == (family,), text


def test_the_closed_class_matches_what_the_lookahead_accepts():
    """The regex used by both consumers and ``is_event_word`` are one class (no drift)."""
    import re

    for word in PARTICIPLES + VERBS + NOUNS + ["firmó", "vigentes", "plazo", "ado", "dado"]:
        in_regex = bool(re.match(mse.LOOKAHEAD, f" {word}", re.IGNORECASE))
        assert in_regex == mse.is_event_word(word), word
