"""Monetary planning gate, extracted before the MPV2 behavior change."""
from app.core.query_plan_types import QueryPlan


def money_query_gate(params: dict, risk_flags: list[str], signals: list) -> QueryPlan | None:
    metadata = params.get("money_metadata")
    metadata_message = metadata.get("clarification_message") if isinstance(metadata, dict) else None
    clarification = params.get("money_clarification") or metadata_message
    if not clarification:
        return None
    return QueryPlan(
        should_execute=False,
        params=params,
        risk_flags=risk_flags + ["smmlv_year_scope_unsupported"],
        clarification_question=str(clarification),
        reason="smmlv_year_scope_unsupported",
        signals=signals,
    )
