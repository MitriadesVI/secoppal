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
"""
from __future__ import annotations

import itertools
from datetime import date
from pathlib import Path

import pytest

from app.core import money_comparators as mc
from app.core import money_suffix_event as mse
from app.core.entity_resolver import EntityResolver
from app.core.money_comparators import GTE, LTE, read_comparators
from app.core.money_polarity import MoneyRelation, resolve_money_polarity
from app.core.query_router import QueryRouter

TODAY = date(2026, 10, 1)
HEAD = "contratos de la Alcaldía de Galapa de 300 millones"
SUFFIXES = list(mc._ADJECTIVE_SUFFIXES)
BOUND = {GTE: MoneyRelation.LOWER_BOUND, LTE: MoneyRelation.UPPER_BOUND}
OPPOSITE = {GTE: MoneyRelation.UPPER_BOUND, LTE: MoneyRelation.LOWER_BOUND}
FIELD = {GTE: "valor_min", LTE: "valor_max"}
OTHER_FIELD = {GTE: "valor_max", LTE: "valor_min"}

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


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _relation(text: str) -> MoneyRelation:
    decisions = [d for d in resolve_money_polarity(text) if "300" in d.amount_spans[0]]
    assert len(decisions) == 1, (text, decisions)
    return decisions[0].relation


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
        relation, families = _relation(text), _families(text)
        if relation is not BOUND[family] or family not in families or OPPOSITE[family] in families:
            bad.append((text, relation, families))
    assert not bad, bad[:5]


@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_a_reflexive_verb_phrase_after_the_suffix_is_a_bound_too(phrase, family):
    for word in ("firmaron", "firmados", "adjudicadas"):
        text = f"{HEAD} {phrase} se {word} en 2025"
        assert _relation(text) is BOUND[family], text


@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_an_adverb_before_the_participle_is_not_in_the_class_and_asks(phrase, family):
    """Closed class, fail-closed: only the NEXT word counts; «ya firmados» asks."""
    text = f"{HEAD} {phrase} ya firmados en 2025"
    assert _relation(text) is MoneyRelation.UNKNOWN
    assert _families(text) == ()


@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_a_noun_or_a_non_verbal_adjective_after_the_suffix_still_asks(phrase, family):
    bad = []
    for noun, closer in itertools.product(NOUNS, ["", " en 2025", " del contrato"]):
        text = f"{HEAD} {phrase} {noun}{closer}"
        if _relation(text) is not MoneyRelation.UNKNOWN or _families(text) != ():
            bad.append((text, _relation(text), _families(text)))
    assert not bad, bad[:5]


@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_a_participle_separated_by_a_noun_is_not_read(phrase, family):
    """The participle must be the NEXT word: «y mayor plazo firmados» is a noun phrase."""
    text = f"{HEAD} {phrase} plazo firmados en 2025"
    assert _relation(text) is MoneyRelation.UNKNOWN
    assert _families(text) == ()


@pytest.mark.parametrize("phrase,family", [(p, f) for p, f in SUFFIXES if p.startswith("o ")])
@pytest.mark.parametrize("accent_phrase", ["ó", "Ó", "O"])
def test_the_accented_or_capitalised_conjunction_is_read_like_the_plain_one(phrase, family, accent_phrase):
    text = f"{HEAD} {accent_phrase}{phrase[1:]} firmados"
    assert _relation(text) is BOUND[family], text
    assert _families(text) == (family,), text


# ── the router: the cases of the finding ───────────────────────────────────────────────────

@pytest.mark.parametrize("amount,field", [
    ("de 300 millones o mayores firmados en 2025", "valor_min"),
    ("de 300 millones o superiores adjudicados en 2025", "valor_min"),
    ("de 300 millones o menores firmados en 2025", "valor_max"),
    ("de 300 millones o inferiores suscritos este año", "valor_max"),
    ("de 300 millones o mayores (firmados)", "valor_min"),
    ("de 300 millones o mayores - firmados", "valor_min"),
    ("de 300 millones ó mayores firmados", "valor_min"),
    ("de 300 millones o mayor firmados", "valor_min"),
    ("de 300 millones y menores celebrados", "valor_max"),
    ("de 300 millones o mayores/firmados", "valor_min"),
    ("de 300 millones o mayores 🙂", "valor_min"),
    ("de 300 millones o menores 👍", "valor_max"),
])
def test_the_router_executes_the_inclusive_bound_again(router, amount, field):
    params = router.parse(f"contratos de la Alcaldía de Galapa {amount}", today=TODAY).params
    assert params.get(field) == 300_000_000, amount
    other = "valor_max" if field == "valor_min" else "valor_min"
    assert other not in params, f"{amount}: the opposite polarity must never appear"
    assert not params.get("money_clarification"), amount


@pytest.mark.parametrize("amount", [
    "de 300 millones y mayor plazo",
    "de 300 millones y menores plazos",
    "de 300 millones o mayor duración",
    "de 300 millones o mayor cuantía",
    "de 300 millones o menor valor",
    "de 300 millones o mayores Galapa",
])
def test_the_router_still_asks_when_a_noun_follows(router, amount):
    params = router.parse(f"contratos de la Alcaldía de Galapa {amount}", today=TODAY).params
    assert params.get("money_clarification"), amount
    assert "valor_min" not in params and "valor_max" not in params, amount


def test_the_closed_class_matches_what_the_lookahead_accepts():
    """The regex used by both consumers and ``is_event_word`` are one class (no drift)."""
    import re

    for word in PARTICIPLES + VERBS + NOUNS + ["firmó", "vigentes", "plazo", "ado", "dado"]:
        in_regex = bool(re.match(mse.LOOKAHEAD, f" {word}", re.IGNORECASE))
        assert in_regex == mse.is_event_word(word), word
