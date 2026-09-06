"""Frozen human oracle for MPV2; expectations precede runtime changes."""
from pathlib import Path

import pytest
import yaml

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
