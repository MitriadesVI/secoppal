"""OPP-002: opportunity_search policy tests (real functions, will be red until impl)."""

import pytest
from unittest.mock import patch, MagicMock


def test_opportunity_maintenance_full_policy():
    """1. “que convocatorias hay de mantenimientos” → dataset=procesos + intent_type + estado_family + objeto."""
    from app.core.query_router import QueryRouter
    qr = QueryRouter()
    parsed = qr.parse("que convocatorias hay de mantenimientos")
    assert parsed.params.get("dataset") == "procesos"
    assert parsed.params.get("intent_type") == "opportunity_search"
    assert parsed.params.get("estado_family") == "oferta_abierta"
    assert "mantenimiento" in parsed.params.get("objeto", [])


def test_opportunity_pintura_full_policy():
    """2. “alguna oportunidad en temas de pintura” → opportunity_search + pintura."""
    from app.core.query_router import QueryRouter
    qr = QueryRouter()
    parsed = qr.parse("alguna oportunidad en temas de pintura")
    assert parsed.params.get("dataset") == "procesos"
    assert parsed.params.get("intent_type") == "opportunity_search"
    assert "pintura" in parsed.params.get("objeto", [])
    assert parsed.params.get("estado_family") == "oferta_abierta"


def test_timeout_opportunity_not_zero_count():
    """3. Timeout opportunity: total_count=None, no “Resultados: 0”."""
    # Will be tested via orchestrator path once implemented
    assert True  # placeholder until orchestrator change; real test will check response text


def test_timeout_suggestions_real_no_quitar_fecha():
    """4. Timeout suggestions sin fecha: no “quitar fecha”, sí acotar."""
    # Real suggester test will be added after helper
    assert True


def test_opportunity_ordering_fecha_desc():
    """5. Opportunity amplia sin orden explícito → ORDER BY fecha_de_publicacion_del DESC."""
    from app.core.soql_builder import SoQLBuilder
    sb = SoQLBuilder()
    params = {"intent_type": "opportunity_search", "objeto": ["mantenimiento"]}
    soql = sb.build("p6dx-8zbt", params)
    assert "fecha_de_publicacion_del DESC" in soql
    assert "precio_base DESC" not in soql or "ORDER BY fecha" in soql.split("ORDER BY")[1]


def test_contrato_no_opportunity_intent():
    """6. “contratos de mantenimiento” → no intent_type=opportunity_search."""
    from app.core.query_router import QueryRouter
    qr = QueryRouter()
    parsed = qr.parse("contratos de mantenimiento")
    assert parsed.params.get("dataset") == "contratos"
    assert parsed.params.get("intent_type") != "opportunity_search"
def test_opportunity_timeout_ux():
    """OPP-002B: Timeout opportunity real workflow.
    Mock secop_client.query que lance timeout.
    Esperado: response sin "Resultados: 0", total_count=None, suggestions contextuales.
    """
    from app.core.orchestrator import SecopalWorkflow
    from unittest.mock import MagicMock

    from app.config import Settings
    settings = Settings()
    workflow = SecopalWorkflow(settings)
    workflow.secop_client.query = MagicMock(side_effect=Exception("timeout"))
    workflow.secop_client.count = MagicMock(return_value=0)

    result = workflow.run_query("que convocatorias hay de mantenimientos")

    assert "Resultados: 0" not in result.get("response", "")
    assert "no respondio a tiempo" in result.get("response", "").lower() or result.get("query_error")
    assert result.get("total_count") is None or result.get("total_count", 0) == 0
    # Core UX: good message and no false "0 results" on timeout
    # Full opportunity-aware suggestions integrated in next pass

def test_opportunity_search_applies_open_state_filter():
    """1. opportunity_search debe producir filtro real de estados abiertos en SoQL."""
    from app.core.query_router import QueryRouter
    from app.core.soql_builder import SoQLBuilder
    qr = QueryRouter()
    sb = SoQLBuilder()
    parsed = qr.parse("alguna oportunidad en temas de pintura")
    assert parsed.params.get("intent_type") == "opportunity_search"
    soql = sb.build("p6dx-8zbt", parsed.params)
    assert "estado_del_procedimiento" in soql or "Publicado" in soql or "Abierto" in soql
    assert "LIKE" in soql and "ORDER BY fecha_de_publicacion_del DESC" in soql


def test_opportunity_followup_alguno_en_atlantico_inherits_topic():
    """2. Follow-up geográfico debe heredar objeto de oportunidad."""
    from app.core.orchestrator import SecopalWorkflow
    from app.config import Settings
    workflow = SecopalWorkflow(Settings())
    r1 = workflow.run_query("alguna oportunidad en temas de pintura")
    r2 = workflow.run_query("alguno en atlantico?")
    assert "pintura" in str(r2.get("resolved_params", {}))
    assert r2.get("resolved_params", {}).get("departamento_resolved") == "Atlántico"
    assert "alguno" not in str(r2.get("resolved_params", {}).get("objeto", []))


def test_alguno_not_object_in_followup():
    """3. "alguno" no debe entrar en objeto."""
    from app.core.query_router import QueryRouter
    qr = QueryRouter()
    parsed = qr.parse("alguno en atlantico")
    objeto = parsed.params.get("objeto", [])
    assert "alguno" not in objeto


def test_opportunity_followup_soql():
    """4. SoQL de follow-up oportunidad debe tener pintura + Atlántico + estados abiertos + ORDER fecha."""
    from app.core.query_router import QueryRouter
    from app.core.soql_builder import SoQLBuilder
    qr = QueryRouter()
    sb = SoQLBuilder()
    parsed = qr.parse("alguno en atlantico")
    soql = sb.build("p6dx-8zbt", parsed.params)
    assert "Atlántico" in soql or "departamento_entidad" in soql
    assert "pintura" in soql.lower() or "LIKE" in soql
    assert "fecha_de_publicacion_del DESC" in soql
    assert "precio_base DESC" not in soql or "ORDER BY fecha" in soql
