"""Validate contract evidence without changing the recorded transport outcome."""
from dataclasses import replace

from app.core.entity_profile.aspects.values import parse_contract_values
from app.core.entity_profile.models import SourceCall


def section_evidence_calls(
    rows: dict[str, list[dict]], calls: dict[str, SourceCall],
) -> dict[str, SourceCall]:
    """A successful HTTP response is necessary, but not sufficient, for a fact."""
    call = calls.get("contract_values")
    if call is None or call.status != "SUCCESS":
        return calls
    parsed = parse_contract_values(rows.get("contract_values", []))
    reason = "INVALID_RESPONSE" if parsed is None else None
    if parsed is not None and parsed.contract_count == 0 and any(
        calls.get(key) and calls[key].status == "SUCCESS" and rows.get(key)
        for key in ("recent_contracts", "supplier_count", "supplier_value")
    ):
        reason = "INCONSISTENT_RESPONSE"
    if reason is None:
        return calls
    return {**calls, "contract_values": replace(call, status=reason)}
