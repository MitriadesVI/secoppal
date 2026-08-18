"""Money V2 / M7 — transparencia contextual del filtro económico."""

from __future__ import annotations

from app.core.query_router import QueryRouter
from app.core.response_policy import (
    _build_money_filter_interpretation,
    build_advisor_response,
)


router = QueryRouter()


def _context(params: dict) -> dict:
    return {
        "user_query": "test",
        "resolved_params": params,
        "total_count": 1,
        "rows": [{
            "titulo": "Resultado",
            "entidad": "Entidad",
            "valor": "$2,000,000,000",
            "estado": "",
            "fecha": "2026-01-01",
            "contratista": "",
            "url": "",
        }],
        "universe_insights": None,
        "suggestions": [],
        "dataset_id": "",
        "channel": "streamlit",
        "needs_clarification": False,
        "clarification_reason": "",
        "degraded": False,
        "degraded_hint": "",
        "query_error": "",
        "timeout_suggestions": [],
    }


def test_process_filter_names_precio_base() -> None:
    text = _build_money_filter_interpretation({
        "dataset": "procesos", "valor_min": 2_000_000_000,
    })
    assert text == "precio base del proceso ≥ $2,000,000,000"


def test_contract_filter_names_valor_del_contrato() -> None:
    text = _build_money_filter_interpretation({
        "dataset": "contratos", "valor_min": 2_000_000_000,
    })
    assert text == "valor del contrato ≥ $2,000,000,000"


def test_smmlv_filter_shows_source_year_and_cop() -> None:
    params = router.parse(
        "contratos de alimentacion mayores a 1500 SMMLV en 2026"
    ).params
    text = _build_money_filter_interpretation(params)
    assert "valor del contrato ≥ 1.500 SMMLV (2026)" in (text or "")
    assert "≈ $2,626,357,500" in (text or "")


def test_assumed_smmlv_year_is_disclosed(monkeypatch) -> None:
    monkeypatch.setattr("app.core.query_router.current_colombian_year", lambda: 2026)
    params = router.parse(
        "contratos de alimentacion mayores a 1000 SMMLV"
    ).params
    text = _build_money_filter_interpretation(params)
    assert "2026 · año civil actual asumido" in (text or "")


def test_clear_response_includes_one_contextual_economic_section() -> None:
    params = {
        "dataset": "procesos",
        "objeto": ["alimentacion"],
        "valor_min": 2_000_000_000,
    }
    response = build_advisor_response(_context(params))
    assert response.count("Filtro económico:") == 1
    assert "precio base del proceso ≥ $2,000,000,000" in response


def test_no_money_filter_adds_no_economic_section() -> None:
    response = build_advisor_response(_context({
        "dataset": "contratos", "objeto": ["alimentacion"],
    }))
    assert "Filtro económico:" not in response
