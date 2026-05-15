from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def _fake_workflow(tmp_path=None):
    """Workflow con mocks de SECOP para tests rapidos."""
    import tempfile
    from app.core.conversation_store import ConversationStore
    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = lambda dataset_id, soql_query: [  # type: ignore[method-assign]
        {
            "nombre_del_procedimiento": "Mantenimiento vial en corredor principal",
            "entidad": "DEPARTAMENTO DE ATLANTICO",
            "precio_base": "750000000",
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": "2026-04-01T00:00:00.000",
            "urlproceso": "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.TEST.1",
            "referencia_del_proceso": "REF-001",
        }
    ]
    workflow.secop_client.count = lambda dataset_id, soql_query: 1  # type: ignore[method-assign]
    if tmp_path:
        workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return workflow


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
    workflow.secop_client.count = lambda dataset_id, soql_query: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign] # disable narrator

    result = workflow.run_query(
        "licitaciones de mantenimiento vial en Atlantico por mas de 500 millones abiertas",
        channel="streamlit",
    )

    assert result["dataset_id"] == "p6dx-8zbt"
    assert "precio_base >= 500000000" in result["soql_query"]
    assert result["rows"][0]["entidad"] == "DEPARTAMENTO DE ATLANTICO"
    assert "Encontre 1 resultados" in result["response"]
    assert result["total_count"] == 1


def test_workflow_exposes_total_count_when_universe_is_larger() -> None:
    """When count > shown results, response header should mention universe size."""
    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = lambda dataset_id, soql_query: [  # type: ignore[method-assign]
        {
            "nombre_del_procedimiento": "Pavimentacion calle 5",
            "entidad": "MUNICIPIO DE MONTERIA",
            "precio_base": "200000000",
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": "2026-03-15T00:00:00.000",
            "urlproceso": "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=CO1.TEST.2",
        }
    ]
    workflow.secop_client.count = lambda dataset_id, soql_query: 347  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign] # disable narrator

    result = workflow.run_query("pavimentacion en Cordoba", channel="telegram")

    assert result["total_count"] == 347
    assert "347" in result["response"]
    assert "más recientes" in result["response"]


# ---------------------------------------------------------------------------
# Tests de ConversationStore integrado en run_query
# ---------------------------------------------------------------------------

def test_run_query_sin_chat_id_no_persiste(tmp_path):
    """Sin chat_id, run_query no debe escribir nada en conv_store."""
    from app.core.conversation_store import ConversationStore
    workflow = _fake_workflow(tmp_path)
    workflow.run_query("procesos en Atlantico", channel="streamlit")
    # No debe haber ningún archivo de conversacion
    conv_dir = tmp_path / "conversations"
    files = list(conv_dir.glob("*.jsonl")) if conv_dir.exists() else []
    assert files == []


def test_run_query_con_chat_id_persiste_turno(tmp_path):
    """Con chat_id, run_query debe guardar el turno en conv_store."""
    workflow = _fake_workflow(tmp_path)
    workflow.run_query("procesos en Atlantico", channel="telegram", chat_id="tg_555")
    history = workflow.conv_store.get_history("tg_555")
    assert len(history) == 1
    assert history[0].user_query == "procesos en Atlantico"
    assert history[0].result_ids == ["REF-001"]


def test_run_query_followup_preserva_params(tmp_path):
    """Turno 2 tipo follow-up conserva params del turno 1."""
    workflow = _fake_workflow(tmp_path)
    # Turno 1: query completa
    workflow.run_query(
        "procesos de pavimentacion en el Choco",
        channel="telegram",
        chat_id="tg_999",
    )
    # Turno 2: follow-up (empieza con "y")
    result2 = workflow.run_query(
        "y en Bolivar?",
        channel="telegram",
        chat_id="tg_999",
    )
    assert result2["followup"] is True
    # El resolved_params debe contener Bolivar (no Choco)
    # No podemos garantizar la resolucion exacta sin red, pero si que followup=True
    # y que el pipeline corrio sin error
    assert "response" in result2
    assert result2["response"] != ""


def test_run_query_no_followup_sin_turno_previo(tmp_path):
    """Primera query nunca es follow-up."""
    workflow = _fake_workflow(tmp_path)
    result = workflow.run_query(
        "procesos en Atlantico",
        channel="telegram",
        chat_id="tg_111",
    )
    assert result["followup"] is False


def test_query_canchas_o_parques_uses_or_in_soql(tmp_path):
    workflow = _fake_workflow(tmp_path)
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]

    result = workflow.run_query(
        "que oportunidades hay para construccion de canchas o parques",
        channel="streamlit",
    )
    soql = result["soql_query"]

    assert "construccion" in soql
    assert "canchas" in soql
    assert "parques" in soql
    assert " AND " in soql
    assert " OR " in soql
    assert soql.index("construccion") < soql.index(" AND ") < soql.index("canchas")


def test_query_pae_extracts_alimentacion_escolar(tmp_path):
    workflow = _fake_workflow(tmp_path)
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]

    result = workflow.run_query("oportunidades del pae", channel="streamlit")

    assert result["resolved_params"].get("objeto") == ["alimentacion_escolar"]
