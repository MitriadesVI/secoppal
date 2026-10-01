"""M1 wave 1 R2 (ZEUS P2-2 and P3): comparative adjectives are bounds only when nothing modifies.

R1 made «300 millones o mayor» an inclusive lower bound. The same suffix read
«contratos de 300 millones y mayor plazo» as ``valor_min = 300000000``: «mayor» there is
an adjective of «plazo», not a bound on the amount. The rule is closed: an adjectival
suffix («o/y mayor|menor|superior|inferior», singular or plural) is a bound only at the end
of the text, before punctuation, or before a connector of ``SUFFIX_CONNECTORS``. Any other
continuation does not read it, and the amount stays without an operator (asked).

The property enumerates the suffix lexicon x the continuations; the lexicon is read from
``money_comparators`` so a new suffix is covered without editing this file.

P3: ``money_polarity`` read «más o menos 300 millones» as an upper bound. An approximation
is not a bound: it is asked (the legacy «por 300 millones» band is untouched).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.core import money_comparators as mc
from app.core.entity_resolver import EntityResolver
from app.core.money_comparators import GTE, LTE, read_comparators
from app.core.money_polarity import MoneyRelation, has_money_relation_before, resolve_money_polarity
from app.core.query_router import QueryRouter

TODAY = date(2026, 10, 1)
HEAD = "contratos de la Alcaldía de Galapa de 300 millones"

ADJECTIVE_SUFFIXES = list(mc._ADJECTIVE_SUFFIXES)           # (phrase, family)
QUANTIFIER_SUFFIXES = list(mc._QUANTIFIER_SUFFIXES)
BOUND = {GTE: MoneyRelation.LOWER_BOUND, LTE: MoneyRelation.UPPER_BOUND}
# Nouns an adjective can modify, money nouns included: «mayor valor» is a noun phrase too.
NOUNS = [
    "plazo", "cuantía", "cuantia", "valor", "monto", "tamaño", "duración", "riesgo", "experiencia",
    "impacto", "presupuesto", "calidad", "contratos", "vigencia", "importe", "cifra",
]
PUNCTUATION = [".", ",", ";", ":", "?", "!", ")", " .", " ,"]


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _relation(text: str) -> MoneyRelation:
    decisions = [d for d in resolve_money_polarity(text) if "300" in d.amount_spans[0]]
    assert len(decisions) == 1, (text, decisions)
    return decisions[0].relation


def _families(text: str) -> tuple:
    return read_comparators(text).families


def test_the_lexicon_has_both_sides_for_every_adjective():
    families = {family for _, family in ADJECTIVE_SUFFIXES}
    assert families == {GTE, LTE}
    stems = {phrase.split()[1] for phrase, _ in ADJECTIVE_SUFFIXES}
    assert stems == {
        "mayor", "mayores", "superior", "superiores", "menor", "menores", "inferior", "inferiores",
    }
    assert {phrase.split()[0] for phrase, _ in ADJECTIVE_SUFFIXES} == {"o", "y"}


@pytest.mark.parametrize("phrase,family", ADJECTIVE_SUFFIXES)
def test_the_suffix_is_a_bound_at_the_end_of_the_text(phrase, family):
    text = f"{HEAD} {phrase}"
    assert _relation(text) is BOUND[family], text
    assert _families(text) == (family,), text


@pytest.mark.parametrize("phrase,family", ADJECTIVE_SUFFIXES)
@pytest.mark.parametrize("punctuation", PUNCTUATION)
def test_the_suffix_is_a_bound_before_punctuation(phrase, family, punctuation):
    text = f"{HEAD} {phrase}{punctuation}"
    assert _relation(text) is BOUND[family], text
    assert _families(text) == (family,), text


# Connectors that are themselves comparator heads («desde», «hasta») add their own reading in the
# comparator reader (R1 behaviour, unchanged); «y» after the suffix makes the polarity scanner look
# for a second cardinal operand and, finding none, ask (R1 behaviour, unchanged: fail-closed).
HEAD_CONNECTORS = {"desde", "hasta"}


@pytest.mark.parametrize("phrase,family", ADJECTIVE_SUFFIXES)
@pytest.mark.parametrize("connector", [c for c in mc.SUFFIX_CONNECTORS if c not in {"de", "a", "que"}])
def test_the_suffix_is_a_bound_before_a_connector(phrase, family, connector):
    text = f"{HEAD} {phrase} {connector} obras"
    assert family in _families(text), text
    if connector not in HEAD_CONNECTORS:
        assert _families(text) == (family,), text
    if connector == "y":
        assert _relation(text) in {BOUND[family], MoneyRelation.UNKNOWN}, text
    else:
        assert _relation(text) is BOUND[family], text


@pytest.mark.parametrize("phrase,family", ADJECTIVE_SUFFIXES)
@pytest.mark.parametrize("connector", ["de", "a", "que"])
def test_before_the_linking_connectors_the_suffix_is_still_never_an_unbounded_noun_phrase(
    phrase, family, connector,
):
    """«o mayor de/a/que …» is the suffix or a head with its link: either way a bound, never nothing."""
    text = f"{HEAD} {phrase} {connector} la fecha"
    assert _relation(text) in {MoneyRelation.LOWER_BOUND, MoneyRelation.UPPER_BOUND}, text
    assert _families(text), text


@pytest.mark.parametrize("phrase,family", ADJECTIVE_SUFFIXES)
@pytest.mark.parametrize("noun", NOUNS)
def test_a_noun_after_the_adjective_means_it_is_not_a_bound(phrase, family, noun):
    text = f"{HEAD} {phrase} {noun}"
    assert _relation(text) is MoneyRelation.UNKNOWN, text
    assert _families(text) == (), text
    assert read_comparators(text).amount_text().count("300") == 1


@pytest.mark.parametrize("phrase,family", ADJECTIVE_SUFFIXES)
@pytest.mark.parametrize("article", ["el", "la", "los", "las", "un", "una", "su"])
def test_an_article_after_the_adjective_is_a_noun_phrase_not_a_connector(phrase, family, article):
    text = f"{HEAD} {phrase} {article} plazo"
    assert _relation(text) is MoneyRelation.UNKNOWN, text
    assert _families(text) == (), text


@pytest.mark.parametrize("phrase,family", ADJECTIVE_SUFFIXES)
def test_the_orchestrator_probe_shapes(phrase, family):
    # «… de 300 millones y mayor plazo» / «… y menor plazo»: the ZEUS P2-2 repros, with every suffix.
    text = f"{HEAD} {phrase} plazo"
    assert _relation(text) is MoneyRelation.UNKNOWN


@pytest.mark.parametrize("phrase,family", QUANTIFIER_SUFFIXES)
@pytest.mark.parametrize("tail", ["", " plazo", " contratos", ".", " de valor"])
def test_the_quantifier_suffixes_are_unchanged_by_this_rule(phrase, family, tail):
    text = f"{HEAD} {phrase}{tail}"
    assert _relation(text) is BOUND[family], text


@pytest.mark.parametrize("text,family", [
    (f"{HEAD} o mayor", GTE),
    (f"{HEAD} o superior", GTE),
    (f"{HEAD} y superior", GTE),
    (f"{HEAD} o menor", LTE),
    (f"{HEAD} o inferior", LTE),
    (f"{HEAD} o más de valor", GTE),
    (f"{HEAD} o más a la fecha", GTE),
    (f"{HEAD} o menos de valor", LTE),
    ("contratos de 300 millones o mayor firmados en 2025".replace("firmados en 2025", "en 2025"), GTE),
    ("contratos de 300 millones o mayor, de aseo", GTE),
])
def test_the_inclusive_suffixes_of_r1_are_kept(text, family):
    assert _relation(text) is BOUND[family], text
    assert _families(text) == (family,), text


def test_a_participle_after_the_suffix_is_a_bound_since_r3():
    """R2 asked «o mayor firmados …»; R3 reads the participle as a report (test_m1_wave1_r3_*)."""
    text = f"{HEAD} o mayor firmados en 2025"
    assert _relation(text) is MoneyRelation.LOWER_BOUND
    assert _families(text) == (GTE,)


def test_the_connectors_are_a_closed_list_of_words_that_cannot_start_a_noun_phrase():
    assert set(mc.SUFFIX_CONNECTORS) >= {"y", "o", "en", "por", "para", "con", "sin", "desde", "hasta"}
    assert not {"el", "la", "los", "las", "un", "una", "su", "mi", "plazo", "valor"} & set(mc.SUFFIX_CONNECTORS)


# ── through the router: the amount stays without an operator and is asked ───────────────────

@pytest.mark.parametrize("phrase", ["y mayor plazo", "y menor plazo", "o mayor plazo", "o menor valor", "y mayor cuantía"])
def test_the_router_asks_instead_of_inventing_a_bound(router, phrase):
    params = router.parse(f"{HEAD} {phrase}", today=TODAY).params
    assert params.get("money_clarification"), phrase
    assert "valor_min" not in params and "valor_max" not in params, phrase


@pytest.mark.parametrize("phrase,field", [("o mayor", "valor_min"), ("o superior", "valor_min"),
                                          ("o menor", "valor_max"), ("y menor", "valor_max")])
def test_the_router_keeps_the_inclusive_suffixes(router, phrase, field):
    params = router.parse(f"{HEAD} {phrase}", today=TODAY).params
    assert params.get(field) == 300_000_000, phrase
    assert not params.get("money_clarification")


# ── P3: an approximation is not a bound ────────────────────────────────────────────────────

APPROXIMATIONS = [
    "más o menos 300 millones",
    "mas o menos 300 millones",
    "Más o menos 300 millones",
    "más o menos de 300 millones",
    "de más o menos 300 millones",
    "más o menos 300 millones o menos",
    "más o menos 300 millones o más",
    "más o menos 300 millones o mayor",
]


@pytest.mark.parametrize("amount", APPROXIMATIONS)
def test_more_or_less_is_not_a_bound_in_the_polarity_scanner(amount):
    text = f"contratos de la Alcaldía de Galapa de {amount}"
    decisions = [d for d in resolve_money_polarity(text) if "300" in d.amount_spans[0]]
    assert [d.relation for d in decisions] == [MoneyRelation.UNKNOWN], text


@pytest.mark.parametrize("amount", APPROXIMATIONS)
def test_more_or_less_is_not_a_bound_in_the_comparator_reader_either(amount):
    families = [f for f in _families(f"contratos de la Alcaldía de Galapa de {amount}") if f]
    # Only an EXPLICIT suffix after the amount («… o menos») may be read; the approximation never.
    assert families in ([], [LTE], [GTE]), amount
    if amount.endswith("300 millones"):
        assert families == []


@pytest.mark.parametrize("amount", APPROXIMATIONS)
def test_the_router_asks_for_an_approximation(router, amount):
    params = router.parse(f"contratos de la Alcaldía de Galapa de {amount}", today=TODAY).params
    assert params.get("money_clarification"), amount
    assert "valor_min" not in params and "valor_max" not in params, amount


def test_a_real_comparator_next_to_the_word_menos_is_untouched():
    for text, relation in [
        ("contratos de menos de 300 millones", MoneyRelation.UPPER_BOUND),
        ("contratos de no menos de 300 millones", MoneyRelation.LOWER_BOUND),
        ("contratos de más de 300 millones", MoneyRelation.LOWER_BOUND),
        ("contratos de por lo menos 300 millones", MoneyRelation.LOWER_BOUND),
        ("contratos de menos de 50 millones o más de 300 millones", None),
        ("contratos de 300 millones o menos", MoneyRelation.UPPER_BOUND),
        ("contratos de 300 millones o más", MoneyRelation.LOWER_BOUND),
    ]:
        decisions = resolve_money_polarity(text)
        if relation is not None:
            assert [d.relation for d in decisions] == [relation], text
        else:
            assert {d.relation for d in decisions} == {MoneyRelation.UPPER_BOUND, MoneyRelation.LOWER_BOUND}


def test_the_legacy_approximation_por_is_not_touched():
    assert _relation("contratos de la Alcaldía de Galapa por 300 millones") is MoneyRelation.APPROXIMATE
    assert has_money_relation_before("contratos de la Alcaldía de Galapa por ")
    assert has_money_relation_before("contratos de la Alcaldía de Galapa de más o menos ")


def test_the_router_still_gives_the_legacy_band_for_por(router):
    params = router.parse("contratos de la Alcaldía de Galapa por 300 millones", today=TODAY).params
    assert (params.get("valor_min"), params.get("valor_max")) == (240_000_000, 360_000_000)
