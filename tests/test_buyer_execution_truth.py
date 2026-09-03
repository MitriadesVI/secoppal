"""El contexto de respuesta sólo describe los filtros realmente ejecutados."""
from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def test_geography_does_not_inject_buyers_after_the_fetch(monkeypatch):
    calls = []
    advised = []
    workflow = SecopalWorkflow(Settings(deepseek_api_key=None))
    row = {
        "id_contrato": "CO-TRUTH-1", "nombre_entidad": "COMPRADOR INDEPENDIENTE",
        "objeto_del_contrato": "Aseo", "valor_del_contrato": "1000000",
        "fecha_de_firma": "2026-01-01", "ciudad": "Bogotá",
        "departamento": "Distrito Capital de Bogotá",
    }
    def query(dataset, soql, **kwargs):
        calls.append(soql)
        return [dict(row)]
    def advisor(context, **kwargs):
        advised.append(dict(context["resolved_params"]))
        return "Resultado instrumentado"
    workflow.secop_client.query = query
    workflow.secop_client.count = lambda *args, **kwargs: 1
    workflow.secop_client.aggregate = lambda *args, **kwargs: []
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None
    monkeypatch.setattr("app.core.workflow.handlers.normal_flow.build_advisor_response", advisor)
    result = workflow.run_query("contratos de aseo en bogota en 2026")
    assert calls and advised
    assert result["soql_query"] in calls
    assert not advised[-1].get("entidad_ecosystem")
    assert not result["resolved_params"].get("entidad_ecosystem")
