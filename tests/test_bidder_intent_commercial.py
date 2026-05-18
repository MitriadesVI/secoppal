"""BIDDER-INTENT-003: commercial verbs must not pollute objeto in opportunity queries."""

from app.core.query_router import QueryRouter


def test_quiero_vender_carpas_opportunity():
    router = QueryRouter()
    result = router.parse("quiero vender carpas, alguna oportunidad?")
    params = result.params if hasattr(result, "params") else result
    assert params.get("dataset") == "procesos"
    assert params.get("intent_type") == "opportunity_search"
    objeto = params.get("objeto", [])
    assert "carpas" in str(objeto).lower() or "carpa" in str(objeto).lower()
    assert "vender" not in str(objeto).lower()


def test_vender_not_in_soql():
    router = QueryRouter()
    result = router.parse("quiero vender carpas, alguna oportunidad?")
    params = result.params if hasattr(result, "params") else result
    # The important guarantee is that "vender" was scrubbed before SoQL
    objeto = params.get("objeto", [])
    assert "vender" not in str(objeto).lower()
    assert params.get("dataset") == "procesos"
    assert params.get("estado_family") == "oferta_abierta"


def test_vendo_uniformes_opportunity():
    router = QueryRouter()
    result = router.parse("vendo uniformes, hay oportunidades?")
    params = result.params if hasattr(result, "params") else result
    objeto = params.get("objeto", [])
    assert "uniformes" in str(objeto).lower()
    assert "vendo" not in str(objeto).lower()


def test_ofrezco_insumos_medicos_opportunity():
    router = QueryRouter()
    result = router.parse("ofrezco insumos médicos, alguna convocatoria?")
    params = result.params if hasattr(result, "params") else result
    objeto = params.get("objeto", [])
    assert "insumos" in str(objeto).lower() or "medicos" in str(objeto).lower()
    assert "ofrezco" not in str(objeto).lower()


def test_soy_proveedor_de_carpas():
    router = QueryRouter()
    result = router.parse("soy proveedor de carpas, hay procesos abiertos?")
    params = result.params if hasattr(result, "params") else result
    objeto = params.get("objeto", [])
    assert "carpas" in str(objeto).lower() or "carpa" in str(objeto).lower()
    assert "proveedor" not in str(objeto).lower()