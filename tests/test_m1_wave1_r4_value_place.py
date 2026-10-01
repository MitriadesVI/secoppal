"""M1 wave 1 R4 (orchestrator amendment): the last VALUE regression of wave 1 is closed.

«de 300 millones o mayores Galapa» (a bound followed by the NAME of the place asked about) executed
the inclusive bound in the base; wave 1 made it ask because a bare name cannot be told from a noun
(«o mayores plazos») without a lexicon. The lexicon already exists: the gazetteer / geography /
buyer-mention catalogs. A continuation that STARTS with a name they recognise ends the bound
(``money_suffix_place``), nothing is copied. A noun that is not a place still asks.

Properties: adjective suffix x {recognised place or buyer, control noun}, on both consumers
(``money_polarity`` and ``read_comparators``) and through the router. The polarity is never inverted.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.core import money_comparators as mc
from app.core import money_suffix_place as msp
from app.core.entity_resolver import EntityResolver
from app.core.money_comparators import GTE, LTE, read_comparators
from app.core.money_polarity import MoneyRelation, resolve_money_polarity
from app.core.municipal_territory import get_municipal_territories
from app.core.query_router import QueryRouter
from app.data.departamentos import DEPARTAMENTOS_OFICIALES

TODAY = date(2026, 10, 1)
HEAD = "contratos de la Alcaldía de Galapa de 300 millones"
SUFFIXES = list(mc._ADJECTIVE_SUFFIXES)
BOUND = {GTE: MoneyRelation.LOWER_BOUND, LTE: MoneyRelation.UPPER_BOUND}
OPPOSITE = {GTE: MoneyRelation.UPPER_BOUND, LTE: MoneyRelation.LOWER_BOUND}
FIELD = {GTE: "valor_min", LTE: "valor_max"}

_territories = get_municipal_territories()
# Every 25th municipality of the gazetteer (deterministic) plus the ones the orchestrator named.
MUNICIPALITIES = sorted({t.municipality for t in _territories.by_code.values()})[::25] + [
    "Galapa", "Barranquilla", "Soledad", "Bogotá", "Cali", "Medellín",
]
DEPARTMENTS = list(DEPARTAMENTOS_OFICIALES)
REGIONS = ["Caribe", "Eje Cafetero", "región Caribe", "costa atlántica", "Pacífica"]
BUYER_MENTIONS = [
    "Alcaldía de Soledad", "la Alcaldía de Soledad", "Alcaldía Municipal de Galapa",
    "Gobernación del Atlántico", "la Gobernación de Antioquia", "Hospital San Rafael",
    "Universidad Nacional",
]
# Control nouns: written here, not read from the module; none of them is a place.
NOUNS = [
    "plazo", "plazos", "cuantía", "valor", "monto", "tamaño", "duración", "riesgo", "experiencia",
    "impacto", "presupuesto", "calidad", "contratos", "vigencia", "importe", "cifra", "vigentes",
    "recurrentes", "importantes", "relevantes", "precio", "tiempo", "garantía", "anticipo",
    "adiciones", "pagos", "ofertas", "entidades", "procesos", "cantidad", "número", "nivel",
]


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _relation(text: str) -> MoneyRelation:
    decisions = [d for d in resolve_money_polarity(text) if "300" in d.amount_spans[0]]
    assert len(decisions) == 1, (text, decisions)
    return decisions[0].relation


def _families(text: str) -> tuple:
    return read_comparators(text).families


def _variants(name: str):
    yield name
    yield name.lower()
    yield name.upper()


# ── the orchestrator's cases, through the router ─────────────────────────────────────────────

@pytest.mark.parametrize("amount,field", [
    ("de 300 millones o mayores Galapa", "valor_min"),
    ("de 300 millones o mayores galapa", "valor_min"),
    ("de 300 millones o superiores Barranquilla", "valor_min"),
    ("de 300 millones o menores en Soledad", "valor_max"),
    ("de 300 millones o menores Soledad", "valor_max"),
    ("de 300 millones o inferiores Barranquilla", "valor_max"),
    ("de 300 millones o mayores Alcaldía de Soledad", "valor_min"),
    ("de 300 millones o mayores Atlántico", "valor_min"),
])
def test_the_router_executes_the_bound_when_a_place_follows(router, amount, field):
    params = router.parse(f"contratos de la Alcaldía de Galapa {amount}", today=TODAY).params
    assert params.get(field) == 300_000_000, amount
    other = "valor_max" if field == "valor_min" else "valor_min"
    assert other not in params, f"{amount}: the opposite polarity must never appear"
    assert not params.get("money_clarification"), amount


def test_a_proper_noun_after_the_plural_suffix_keeps_the_bound(router):
    """Was xfail(strict) in R3 (the only VALUE regression of wave 1 against the base)."""
    params = router.parse("contratos de la Alcaldía de Galapa de 300 millones o mayores Galapa", today=TODAY).params
    assert params.get("valor_min") == 300_000_000


def test_the_name_stays_for_the_buyer_and_geography_authorities(router):
    """The suffix is consumed, the name is not: the buyer / territory authority still reads it."""
    params = router.parse("contratos de aseo de 300 millones o superiores Barranquilla", today=TODAY).params
    assert params.get("valor_min") == 300_000_000
    assert params.get("ciudad") == "Barranquilla"
    assert params.get("entidad_resolved")
    assert "barranquilla" not in " ".join(params.get("objeto") or []).lower()
    spans = [d.source_span for d in resolve_money_polarity("de 300 millones o superiores Barranquilla")]
    assert all("barranquilla" not in span.lower() for span in spans)


@pytest.mark.parametrize("amount", [
    "de 300 millones y mayor plazo",
    "de 300 millones y menores plazos",
    "de 300 millones o mayor duración",
    "de 300 millones o mayor cuantía",
    "de 300 millones o menor valor",
    "de 300 millones o mayores plazos de ejecución",
])
def test_a_noun_after_the_suffix_still_asks(router, amount):
    params = router.parse(f"contratos de la Alcaldía de Galapa {amount}", today=TODAY).params
    assert params.get("money_clarification"), amount
    assert "valor_min" not in params and "valor_max" not in params, amount


# ── the property: suffix x place, on both consumers ──────────────────────────────────────────

@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_a_municipality_department_or_buyer_after_the_suffix_is_the_bound(phrase, family):
    for name in (*MUNICIPALITIES, *DEPARTMENTS, *REGIONS, *BUYER_MENTIONS):
        for variant in _variants(name):
            for tail in ("", " en 2025", " por favor"):
                text = f"{HEAD} {phrase} {variant}{tail}"
                assert _relation(text) is BOUND[family], text
                assert _families(text) == (family,), text
                assert _relation(text) is not OPPOSITE[family], text


@pytest.mark.parametrize("phrase,family", SUFFIXES)
def test_a_control_noun_after_the_suffix_is_not_a_bound(phrase, family):
    for noun in NOUNS:
        for variant in _variants(noun):
            text = f"{HEAD} {phrase} {variant}"
            assert _relation(text) is MoneyRelation.UNKNOWN, text
            assert _families(text) == (), text


def test_no_control_noun_is_recognised_as_a_place():
    for noun in NOUNS:
        for variant in _variants(noun):
            assert msp.names_a_place(variant) is False, variant
            assert msp.names_a_place(f"{variant} de ejecución") is False, variant


def test_the_consumers_agree_on_every_place_and_noun():
    for phrase, family in SUFFIXES:
        for name in (*MUNICIPALITIES[:30], *DEPARTMENTS, *BUYER_MENTIONS, *NOUNS):
            text = f"{HEAD} {phrase} {name}"
            polarity_reads = _relation(text) is BOUND[family]
            comparators_read = _families(text) == (family,)
            assert polarity_reads == comparators_read, text


def test_the_predicate_is_fail_closed():
    for text in ("", "   ", "en", "y", "123", "...", "xyzzyq", "plazo de Galapa"):
        assert msp.names_a_place(text) is False, repr(text)
    assert msp.names_a_place("Galapa") is True
    assert msp.names_a_place("  galapa  ") is True
    assert msp.names_a_place("la Alcaldía de Soledad") is True


def test_the_predicate_asks_the_catalogs_and_copies_no_list():
    """No string constant of the module (docstrings aside) is a place name: the catalogs are asked."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(msp))
    constants = {
        node.value.casefold()
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    } - {ast.get_docstring(tree).casefold()}
    known = {name.casefold() for name in (*MUNICIPALITIES, *DEPARTMENTS)}
    assert not (constants & known), constants & known
    from app.core.geo_authority import department_of, municipal_candidates

    assert municipal_candidates("Galapa") and department_of("Atlántico")
    assert not municipal_candidates("plazos") and not department_of("plazos")


def test_a_negated_suffix_with_a_place_never_inverts():
    for text in (f"{HEAD} no o mayores Galapa", f"{HEAD} o mayores Galapa", f"{HEAD} o menores Galapa"):
        relation = _relation(text)
        if "menores" in text:
            assert relation in (MoneyRelation.UPPER_BOUND, MoneyRelation.UNKNOWN), text
        else:
            assert relation in (MoneyRelation.LOWER_BOUND, MoneyRelation.UNKNOWN), text
