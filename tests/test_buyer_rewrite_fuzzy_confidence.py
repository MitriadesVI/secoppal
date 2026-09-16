"""Global fuzzy rewrite parity is separate from A2 buyer admission."""
from datetime import date
from pathlib import Path

import pytest

from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.turn import profile_query_params
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter


@pytest.fixture(scope="module")
def router():
    return QueryRouter(entity_resolver=EntityResolver(Path("app/data/aliases_db.json")))


def test_fuzzy_rewrite_preserves_global_base_confidence(router):
    params = router.parse("analiza la alcaldía de San Juan", today=date(2026, 9, 15)).params
    resolution = params["entidad_resolution"]
    assert resolution["metadata"]["resolution_method"].startswith("fuzzy")
    assert resolution["confidence"] == "high"
    assert resolve_profile_scope(profile_query_params(params)).scope is not None


@pytest.mark.parametrize("text", [
    "analiza la alcaldía de Barranquilla", "analiza la alcaldía de Galapa",
    "analiza la gobernación del atlántico", "analiza la alcaldía de Santa Marta",
])
def test_exact_rewrites_keep_high_confidence(router, text):
    resolution = router.parse(text, today=date(2026, 9, 15)).params["entidad_resolution"]
    assert resolution["metadata"]["resolution_method"] == "exact"
    assert resolution["confidence"] == "high"
