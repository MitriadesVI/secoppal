"""Frozen human oracle for MPV2; expectations precede runtime changes."""
from pathlib import Path

import pytest
import yaml
from hypothesis import given, settings, strategies as st

from app.core.money_polarity import MoneyRelation, resolve_money_polarity
from app.core.query_router import QueryRouter


CORPUS = yaml.safe_load(
    (Path(__file__).parent / "fixtures/money_polarity_corpus_v1.yaml").read_text()
)["cases"]


def test_corpus_structure():
    assert 90 <= len(CORPUS) <= 120
    assert len({case["id"] for case in CORPUS}) == len(CORPUS)
    assert {case["category"] for case in CORPUS} == {
        "lower", "upper", "negated", "range", "approximate", "unknown", "adversarial",
    }


@pytest.mark.parametrize("case", CORPUS, ids=lambda case: case["id"])
def test_money_polarity_corpus(case):
    params = QueryRouter().parse(case["query"]).params
    assert (params.get("valor_min"), params.get("valor_max")) == (
        case["valor_min"], case["valor_max"],
    ), params
    assert bool(params.get("money_clarification")) == (case["relation"] == "UNKNOWN")


PROPERTY_SETTINGS = settings(max_examples=40, derandomize=True, deadline=None)
AMOUNTS = st.integers(min_value=1, max_value=5000)
LOWER = st.sampled_from(["más", "mayor", "supera", "supere", "excede", "rebasa", "superior"])
UPPER = st.sampled_from(["menos", "menor", "inferior", "hasta", "máximo"])


def _extract(text):
    return QueryRouter()._extract_amounts(text, {})[0]


@PROPERTY_SETTINGS
@given(AMOUNTS, LOWER, st.sampled_from([" ", " de ", " a ", " que los "]))
def test_lower_connector_omission_preserves_direction(amount, comparator, connector):
    params = _extract(f"{comparator}{connector}{amount} millones")
    assert params == {"valor_min": amount * 1_000_000}


@PROPERTY_SETTINGS
@given(AMOUNTS, UPPER, st.sampled_from([" ", " de "]))
def test_upper_cannot_produce_lower(amount, comparator, connector):
    assert _extract(f"{comparator}{connector}{amount} millones") == {"valor_max": amount * 1_000_000}


@PROPERTY_SETTINGS
@given(AMOUNTS, LOWER, st.sampled_from(["no ", "que no ", "no sea ", "que no sean "]))
def test_negating_lower_family_inverts_relation(amount, comparator, negation):
    text = f"{negation}{comparator} {amount} millones"
    decisions = resolve_money_polarity(text)
    assert len(decisions) == 1
    assert decisions[0].negated is True
    assert decisions[0].relation is MoneyRelation.UPPER_BOUND
    assert _extract(text) == {"valor_max": amount * 1_000_000}


@PROPERTY_SETTINGS
@given(AMOUNTS, st.sampled_from(["", "de ", "con ", "cerca de ", "rondan "]))
def test_unknown_amount_has_no_executable_bound(amount, prefix):
    params = _extract(f"contratos {prefix}{amount} millones")
    assert params.get("money_clarification")
    assert "valor_min" not in params and "valor_max" not in params


@PROPERTY_SETTINGS
@given(AMOUNTS, AMOUNTS, st.sampled_from(["entre {a} y {b}", "de {a} a {b}", "desde {a} hasta {b}"]))
def test_range_is_atomic_and_unit_inheritance_preserves_scale(a, b, template):
    params = _extract(template.format(a=a, b=b) + " millones")
    assert params == {"valor_min": min(a, b) * 1_000_000, "valor_max": max(a, b) * 1_000_000}


@PROPERTY_SETTINGS
@given(AMOUNTS, LOWER, st.sampled_from(["contratos", "procesos", "resultados", "obras"]))
def test_interposed_noun_breaks_comparator_amount_binding(amount, comparator, noun):
    params = _extract(f"{comparator} {noun} de {amount} millones")
    assert params.get("money_clarification")
    assert "valor_min" not in params and "valor_max" not in params


@PROPERTY_SETTINGS
@given(AMOUNTS, st.sampled_from(["entre {a} y un millón y medio", "más de {a} y menos de un millón y medio"]))
def test_partial_conversion_cannot_leave_one_side(amount, template):
    params = _extract(template.format(a=f"{amount} millones"))
    assert params.get("money_clarification")
    assert "valor_min" not in params and "valor_max" not in params
