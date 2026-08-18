"""Money V2 / M6 — SMMLV con autoridad anual explícita."""

from __future__ import annotations

from datetime import datetime

import pytest

from app.core.followup_engine import FollowupMerger
from app.core.followup_engine import detect_and_merge
from app.core.query_frame import frame_from_params, params_from_frame
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.data.smmlv import BOGOTA_TZ, current_colombian_year, smmlv_cop
from app.utils.money import money_to_cop, smmlv_to_cop


router = QueryRouter()
builder = SoQLBuilder()


def test_authoritative_catalog_and_bogota_clock() -> None:
    assert smmlv_cop(2024) == 1_300_000
    assert smmlv_cop(2025) == 1_423_500
    assert smmlv_cop(2026) == 1_750_905
    assert smmlv_cop(2023) is None
    instant = datetime(2026, 1, 1, 0, 30, tzinfo=BOGOTA_TZ)
    assert current_colombian_year(instant) == 2026


@pytest.mark.parametrize("alias", ["SMMLV", "SMLMV", "SMLV", "salarios mínimos"])
def test_aliases_resolve_with_explicit_year(alias: str) -> None:
    params = router.parse(
        f"contratos de alimentacion mayores a 1000 {alias} en 2026"
    ).params
    assert params.get("valor_min") == 1_750_905_000
    metadata = params["money_metadata"]
    assert metadata["reference_year"] == 2026
    assert metadata["reference_year_assumed"] is False
    assert metadata["source_values"] == {"valor_min": "1000"}


def test_verbal_smmlv_uses_current_bogota_year_and_discloses_assumption(monkeypatch) -> None:
    monkeypatch.setattr("app.core.query_router.current_colombian_year", lambda: 2026)
    params = router.parse(
        "contratos de alimentacion de mas de mil salarios minimos"
    ).params
    assert params.get("valor_min") == 1_750_905_000
    metadata = params["money_metadata"]
    assert metadata["reference_year"] == 2026
    assert metadata["reference_year_assumed"] is True
    assert metadata["year_authority"] == "current_bogota_year"


def test_smmlv_range_projects_numeric_cop_to_where() -> None:
    params = router.parse(
        "contratos de alimentacion entre 500 y 1500 SMMLV en 2026"
    ).params
    assert params.get("valor_min") == 875_452_500
    assert params.get("valor_max") == 2_626_357_500
    soql = builder.build(builder.dataset_id_for(params["dataset"]), params)
    assert "valor_del_contrato >= 875452500" in soql
    assert "valor_del_contrato <= 2626357500" in soql


def test_process_smmlv_filter_targets_precio_base() -> None:
    params = router.parse(
        "procesos de alimentacion mayores a 1000 SMMLV en 2026"
    ).params
    soql = builder.build(builder.dataset_id_for(params["dataset"]), params)
    assert params.get("valor_min") == 1_750_905_000
    assert "precio_base >= 1750905000" in soql


def test_uncatalogued_explicit_year_is_not_guessed() -> None:
    query = "contratos de alimentacion mayores a 1000 SMMLV en 2023"
    params = router.parse(query).params
    assert params.get("valor_min") is None
    plan = compute_query_plan(params, query, enabled_rules={"P16"})
    assert plan.should_execute is False
    assert "No tengo un valor oficial" in (plan.clarification_question or "")


def test_multi_year_smmlv_is_blocked_before_query_execution() -> None:
    query = "contratos de alimentacion entre 2024 y 2026 mayores a 1000 SMMLV"
    params = router.parse(query).params
    assert params.get("valor_min") is None
    plan = compute_query_plan(params, query, enabled_rules={"P16"})
    assert plan.should_execute is False
    assert plan.reason == "smmlv_year_scope_unsupported"
    assert "un solo año" in (plan.clarification_question or "")


def test_followup_assumed_year_reuses_single_year_context(monkeypatch) -> None:
    monkeypatch.setattr("app.core.query_router.current_colombian_year", lambda: 2026)
    previous = frame_from_params(
        router.parse("contratos de alimentacion en 2025").params
    )
    current = frame_from_params(
        router.parse("ahora mayores a 1500 SMMLV").params
    )
    merged = FollowupMerger.merge(previous, current, "refine_filter")
    params = params_from_frame(merged)
    assert params.get("valor_min") == 2_135_250_000
    metadata = params["money_metadata"]
    assert metadata["reference_year"] == 2025
    assert metadata["year_authority"] == "inherited_query_date_scope"


def test_gated_followup_path_preserves_and_rebases_money_metadata(monkeypatch) -> None:
    monkeypatch.setattr("app.core.query_router.current_colombian_year", lambda: 2026)
    previous = frame_from_params(
        router.parse("contratos de alimentacion en 2025").params
    )
    current = router.parse("ahora mayores a 1500 SMMLV").params
    intent, params = detect_and_merge(
        "ahora mayores a 1500 SMMLV", current, previous
    )
    assert intent == "refine_filter"
    assert params.get("valor_min") == 2_135_250_000
    assert params["money_metadata"]["reference_year"] == 2025


def test_plain_cop_followup_clears_stale_smmlv_metadata() -> None:
    previous = frame_from_params(
        router.parse(
            "contratos de alimentacion mayores a 1000 SMMLV en 2026"
        ).params
    )
    current = router.parse("ahora mayores a dos mil millones").params
    _, params = detect_and_merge(
        "ahora mayores a dos mil millones", current, previous
    )
    assert params.get("valor_min") == 2_000_000_000
    assert params.get("money_metadata") is None


def test_followup_over_multi_year_context_is_explicitly_blocked(monkeypatch) -> None:
    monkeypatch.setattr("app.core.query_router.current_colombian_year", lambda: 2026)
    previous = frame_from_params(
        router.parse("contratos de alimentacion entre 2024 y 2026").params
    )
    current = frame_from_params(
        router.parse("ahora mayores a 1500 SMMLV").params
    )
    merged_params = params_from_frame(
        FollowupMerger.merge(previous, current, "refine_filter")
    )
    plan = compute_query_plan(
        merged_params, "ahora mayores a 1500 SMMLV", enabled_rules={"P16"}
    )
    assert plan.should_execute is False
    assert plan.params["money_metadata"]["status"] == "unsupported_multi_year_context"


def test_scalar_money_api_never_hides_smmlv_year() -> None:
    assert money_to_cop("mil salarios minimos") is None
    assert smmlv_to_cop("mil salarios minimos", 2026) == (1_750_905_000, 1000)
