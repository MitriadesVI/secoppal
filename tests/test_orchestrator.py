from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def test_workflow_runs_end_to_end_with_fake_secop_response() -> None:
    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = lambda dataset_id, soql_query: [  # type: ignore[method-assign]
        {
            "nombre_del_procedimiento": "Mantenimiento vial en corredor principal",
            "entidad": "DEPARTAMENTO DE ATLANTICO",
            "precio_base": "750000000",
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": "2026-04-01T00:00:00.000",
            "urlproceso": "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.TEST.1",
        }
    ]

    result = workflow.run_query(
        "licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas",
        channel="streamlit",
    )

    assert result["dataset_id"] == "p6dx-8zbt"
    assert "precio_base >= 500000000" in result["soql_query"]
    assert result["rows"][0]["entidad"] == "DEPARTAMENTO DE ATLANTICO"
    assert "Encontre 1 resultados" in result["response"]
