from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

import pytest

from backend.app.core.query_parser import QueryParser


class StubLLM:
    def __init__(self, payload):
        self._payload = payload
        self.calls = []

    def generate(self, prompt: str):
        self.calls.append(prompt)
        return self._payload


def test_parser_uses_llm_response_when_valid():
    client = StubLLM(
        json.dumps(
            {
                "entity": "contracts",
                "filters": {"buyer": "Bogotá"},
                "metrics": ["total_amount"],
            }
        )
    )
    parser = QueryParser(llm_client=client)

    params = parser.parse("Contratos de Bogotá")

    assert params.entity == "contracts"
    assert params.filters["buyer"] == "Bogotá"
    assert params.metrics == ["total_amount"]
    assert params.raw_query == "Contratos de Bogotá"


def test_parser_falls_back_to_heuristics_when_llm_invalid():
    client = StubLLM("not-json")
    parser = QueryParser(llm_client=client)

    params = parser.parse("Contratos de la entidad Ministerio de Salud 2022")

    assert params.entity == "agencies"  # heuristics detect "entidad"
    assert params.filters["year"] == 2022
    assert "buyer" in params.filters


def test_heuristic_detects_supplier_and_limit():
    parser = QueryParser()

    params = parser.parse("Top 5 contratos del proveedor ACME Corp 2024")

    assert params.entity == "suppliers"
    assert params.filters["supplier"] == "ACME Corp"
    assert params.filters["year"] == 2024
    assert params.limit == 5


@pytest.mark.parametrize(
    "query,expected_entity",
    [
        ("Quiero saber proveedores en 2023", "suppliers"),
        ("Información de la entidad nacional", "agencies"),
        ("Total contratos", "contracts"),
    ],
)
def test_heuristic_entity_detection(query, expected_entity):
    parser = QueryParser()
    params = parser.parse(query)
    assert params.entity == expected_entity
