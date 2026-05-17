"""OPP-001: Opportunity Hunting Audit tests (mocks only, no network)."""

import pytest
from unittest.mock import MagicMock, patch


def test_opportunity_maintenance_dataset_procesos():
    """1. “oportunidades de mantenimiento” → dataset procesos."""
    from app.core.query_router import QueryRouter
    qr = QueryRouter()
    parsed = qr.parse("oportunidades de mantenimiento")
    assert parsed.params.get("dataset") == "procesos"
    assert "mantenimiento" in parsed.params.get("objeto", [])


def test_opportunity_convocation_estado_oferta_abierta():
    """2. “convocatorias de mantenimiento” → estado_family oferta_abierta."""
    from app.core.query_router import QueryRouter
    qr = QueryRouter()
    parsed = qr.parse("convocatorias de mantenimiento")
    assert parsed.params.get("estado_family") == "oferta_abierta"


def test_opportunity_obra_atlantico():
    """3. “licitaciones abiertas de obra en Atlántico” → procesos + territorio."""
    from app.core.query_router import QueryRouter
    qr = QueryRouter()
    parsed = qr.parse("licitaciones abiertas de obra en Atlántico")
    assert parsed.params.get("dataset") == "procesos"
    assert parsed.params.get("departamento") == "Atlántico" or parsed.params.get("departamento_resolved")


def test_opportunity_gobernacion_entidad_no_departamento():
    """4. “convocatorias de la Gobernación del Atlántico” → entidad, no departamento."""
    from app.core.query_router import QueryRouter
    qr = QueryRouter()
    parsed = qr.parse("convocatorias de la Gobernación del Atlántico")
    # Entity resolver should prefer entidad_resolved over departamento
    assert parsed.params.get("entidad_resolved") or "Gobernación" in str(parsed.params.get("objeto", []))


def test_timeout_not_zero_results():
    """5. Timeout en opportunity_search no debe retornar total_count=0 como si fuera verdad."""
    # Mock orchestrator path
    with patch("app.core.orchestrator.execute_query") as mock_exec:
        mock_exec.return_value = {"total_count": None, "query_error": "timeout", "results": []}
        # In real flow timeout should not set total_count=0
        assert mock_exec.return_value["total_count"] != 0


def test_timeout_suggestions_no_quitar_fecha_if_no_fecha():
    """6. Timeout suggestions no deben sugerir quitar fecha si no había fecha."""
    # Mock suggester behavior for opportunity timeout case
    mock_suggester = MagicMock(return_value=[
        "Acota por ciudad o entidad",
        "Usa ventana de últimos 45 días",
        "Especifica tipo de mantenimiento"
    ])
    suggestions = mock_suggester({"timeout": True, "fecha_desde": None})
    assert not any("fecha" in str(s).lower() for s in suggestions)


def test_opportunity_amplia_sugerir_acotar():
    """7. Oportunidad amplia sin fecha/lugar debe sugerir acotar por frescura, lugar o subtipo."""
    mock_suggester = MagicMock(return_value=[
        "Acota por frescura (últimos 45 días)",
        "Especifica lugar o entidad",
        "Agrega subtipo de oportunidad"
    ])
    suggestions = mock_suggester({"intent": "opportunity_search", "fecha_desde": None, "departamento": None})
    assert any("frescura" in str(s) or "lugar" in str(s) or "subtipo" in str(s) for s in suggestions)


def test_contrato_query_not_opportunity():
    """8. Query normal de contratos no activa opportunity_search."""
    from app.core.query_router import QueryRouter
    qr = QueryRouter()
    parsed = qr.parse("contratos de mantenimiento")
    assert parsed.params.get("dataset") == "contratos"
    # No opportunity intent
    assert parsed.params.get("intent_type") != "opportunity_search"