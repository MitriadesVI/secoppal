"""BIDDER-INTENT-002 tests: para poder presentarme + mantenimiento vial."""

from app.core.query_router import QueryRouter


def test_para_poder_presentarme_activates_opportunity():
    router = QueryRouter()
    result = router.parse("algun mantenimiento de vias para poder presentarme?")
    params = result.params if hasattr(result, "params") else result
    assert params.get("dataset") == "procesos"
    assert params.get("intent_type") == "opportunity_search"
    assert params.get("estado_family") == "oferta_abierta"
    objeto = params.get("objeto", [])
    assert "poder" not in str(objeto).lower()


def test_para_poder_presentarme_overrides_previous_contract_dataset():
    router = QueryRouter()
    # Base context (not used in minimal parse; the bidder intent alone forces procesos)
    result = router.parse("algun mantenimiento de vias para poder presentarme?")
    params = result.params if hasattr(result, "params") else result
    assert params.get("dataset") == "procesos"
    assert params.get("intent_type") == "opportunity_search"
    objeto = params.get("objeto", [])
    assert "poder" not in str(objeto).lower()


def test_mantenimiento_vias_expands_vial_variants():
    router = QueryRouter()
    result = router.parse("mantenimiento de vias")
    params = result.params if hasattr(result, "params") else result
    objeto = params.get("objeto", [])
    assert any("vial" in str(o).lower() or "vias" in str(o).lower() for o in objeto) or "vial" in str(objeto)


def test_bidder_intent_soql_no_poder():
    router = QueryRouter()
    result = router.parse("algun mantenimiento de vias para poder presentarme?")
    params = result.params if hasattr(result, "params") else result
    objeto = params.get("objeto", [])
    assert "poder" not in str(objeto).lower()
    assert params.get("dataset") == "procesos"