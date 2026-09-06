"""P16 blocks uncertain money; only unsupported SMMLV scope names SMMLV."""
from app.core.query_plan_types import QueryPlan


def money_query_gate(params: dict, risk_flags: list[str], signals: list) -> QueryPlan | None:
    metadata = params.get("money_metadata")
    metadata_message = metadata.get("clarification_message") if isinstance(metadata, dict) else None
    clarification = params.get("money_clarification") or metadata_message
    if not clarification:
        return None
    smmlv_scope = isinstance(metadata, dict) and metadata.get("unit") == "SMMLV" and (
        str(metadata.get("status", "")).startswith("unsupported_") or metadata_message
    )
    reason = "smmlv_year_scope_unsupported" if smmlv_scope else "money_interpretation_uncertain"
    if not smmlv_scope:
        params.pop("valor_min", None)
        params.pop("valor_max", None)
    return QueryPlan(
        should_execute=False,
        params=params,
        risk_flags=risk_flags + [reason],
        clarification_question=str(clarification),
        reason=reason,
        signals=signals,
    )
