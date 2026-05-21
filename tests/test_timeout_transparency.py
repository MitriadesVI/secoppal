from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def test_timeout_does_not_claim_zero_results(tmp_path) -> None:
    workflow = SecopalWorkflow(
        Settings(secop_feedback_path=tmp_path / "feedback.jsonl")
    )
    workflow.secop_client.query = lambda *args, **kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
        TimeoutError("SECOP timed out")
    )
    workflow.secop_client.count = lambda dataset_id, soql: 1  # type: ignore[method-assign]

    result = workflow.run_query(
        "contratos de mantenimiento en antioquia",
        channel="streamlit",
    )

    response = result.get("response", "").lower()
    assert result["risk_flag_timeout"] is True
    assert result["total_count"] is None
    assert "no puedo confirmar si hay o no resultados" in response
    assert "no respondio" in response
    assert "0 resultados" not in response
    assert "no encontre resultados" not in response
