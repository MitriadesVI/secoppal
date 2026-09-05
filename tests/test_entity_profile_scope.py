from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

from app.core.entity_profile.models import ScopeType
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter


CORPUS = Path("tests/fixtures/entity_profile_corpus_v1.yaml")


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _case_params(router: QueryRouter, text: str) -> dict:
    return router.parse(text, today=date(2026, 9, 5)).params


def test_entity_profile_single_turn_corpus_has_exactly_forty_cases(router):
    payload = yaml.safe_load(CORPUS.read_text(encoding="utf-8"))
    cases = payload["single_turn"]
    assert len(cases) == 40

    failures: list[str] = []
    for case in cases:
        turn = classify_entity_profile_turn(case["text"], _case_params(router, case["text"]))
        actual = {
            "detected": turn.detected,
            "focus": turn.focus,
            "scope": turn.scope.scope_type.value if turn.scope else None,
            "clarification": bool(turn.clarification),
        }
        for key, expected in case.items():
            if key in {"id", "text"}:
                continue
            if actual.get(key) != expected:
                failures.append(f"{case['id']} {key}: {actual.get(key)!r} != {expected!r}")
    assert failures == []


def test_baranoa_buyer_unit_gets_product_recognized_metadata_only(router):
    query = "analiza la alcaldia de baranoa"
    turn = classify_entity_profile_turn(query, _case_params(router, query))
    assert turn.scope is not None and turn.scope.scope_type is ScopeType.BUYER_UNIT
    assert turn.scope.buyer_units == ("ALCALDIA MUNICIPAL DE BARANOA",)
    assert len(turn.scope.territory_context) == 1
    context = turn.scope.territory_context[0]
    assert context.divipola == "08078"
    assert context.knowledge_status == "PRODUCT_RECOGNIZED"
    assert context.institution_id.startswith("logical:")


@pytest.mark.parametrize("name", ["Baranoa", "Bucaramanga", "Yumbo", "Barranquilla"])
def test_naked_municipality_is_never_a_buyer(name, router):
    query = f"analiza {name}"
    turn = classify_entity_profile_turn(query, _case_params(router, query))
    assert turn.scope is None
    assert turn.clarification
    assert f"Alcaldía de {name}" in turn.clarification
    assert f"territorio institucional de {name}" in turn.clarification


def test_bogota_uses_declared_group_membership(router):
    query = "perfil de la alcaldia mayor de bogota"
    turn = classify_entity_profile_turn(query, _case_params(router, query))
    assert turn.scope is not None and turn.scope.scope_type is ScopeType.BUYER_GROUP
    assert turn.scope.buyer_group_id == "bogota_executive"
    assert len(turn.scope.buyer_units) == 38


def test_candidate_knowledge_can_never_be_a_product_scope(monkeypatch):
    params = {
        "entidad_resolved": "CANDIDATE",
        "entidad_resolution": {
            "confidence": "medium",
            "kind": "territorial_institution_candidate",
        },
    }
    turn = classify_entity_profile_turn("analiza CANDIDATE", params)
    assert turn.detected is False
    assert turn.scope is None
