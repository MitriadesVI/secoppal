"""Trace diagnóstico de un turno Foundation para el expander de Streamlit.

Proyección de sólo lectura sobre campos que el resultado YA lleva: no ejecuta,
no consulta, no reinterpreta. Los códigos internos pueden aparecer aquí porque
esto es diagnóstico de dogfood; la respuesta pública nunca los muestra.
"""

from __future__ import annotations

_RECEIPT_FIELDS = ("call_id", "parent_call_id", "request_id", "operation", "dataset", "reason",
                   "status", "attempts", "rows_received", "count_value", "error_type")
_GROUP_FIELDS = ("request_id", "scope_id", "dataset", "universe", "state", "total", "total_quality",
                 "fallback_attempted", "technical_reason", "error_type", "count_error_type", "warnings")
_PRODUCER_FIELDS = ("producer_version", "provider_model_reported", "provider_latency_ms", "provider_calls",
                    "provider_error", "prompt_sha256", "foundation_latency_ms", "source_calls")


def _quotes(evidence) -> list[str]:
    return [span.get("quote", "") for span in evidence or () if isinstance(span, dict)]


def project(result: dict) -> dict | None:
    """Los campos del trace, o None si el turno no es de Foundation."""
    if not isinstance(result, dict) or not result.get("foundation_mode"):
        return None
    acceptance = result.get("foundation_acceptance") or {}
    proposal = acceptance.get("proposal") or {}
    telemetry = result.get("foundation_telemetry") or {}
    return {
        "terminal": result.get("foundation_terminal"),
        "plan_decision": result.get("foundation_plan_decision"),
        "internal_reason": result.get("foundation_internal_reason"),
        "producer": {key: telemetry.get(key) for key in _PRODUCER_FIELDS if key in telemetry},
        "requests": [{"request_id": r.get("request_id"), "scope_id": r.get("scope_id"),
                      "subject": r.get("subject"), "universe": r.get("universe_claim"),
                      "expression": r.get("condition_expression")}
                     for r in proposal.get("requests") or ()],
        "conditions": [{"condition_id": c.get("condition_id"), "request_ids": c.get("request_ids"),
                        "dimension": c.get("dimension"), "operator": c.get("operator"),
                        "polarity": c.get("polarity"), "role": c.get("role"), "claim": c.get("claim"),
                        "evidence": _quotes(c.get("evidence")), "unsupported": c.get("unsupported_label")}
                       for c in proposal.get("conditions") or ()],
        "resolutions": list(acceptance.get("resolutions") or ()),
        "policies": list(acceptance.get("policies") or ()),
        "h7": result.get("foundation_universe"),
        "datasets": list(result.get("dataset_scope") or ()),
        "groups": [{**{key: group.get(key) for key in _GROUP_FIELDS}, "rows": len(group.get("rows") or ())}
                   for group in result.get("groups") or ()],
        "plan": [{key: item.get(key) for key in ("request_id", "dataset", "operation", "soql")}
                 for item in result.get("foundation_plan") or ()],
        "receipts": [{key: receipt.get(key) for key in _RECEIPT_FIELDS}
                     for receipt in result.get("source_call_receipts") or () if isinstance(receipt, dict)],
        "fallback": result.get("foundation_fallback"),
    }


def render(st, view: dict | None) -> None:
    """Pinta el trace dentro del expander ya abierto; nada si el turno no es Foundation."""
    if not view:
        return
    st.markdown(f"**Foundation:** terminal `{view['terminal']}` · plan `{view['plan_decision']}`"
                + (f" · motivo `{view['internal_reason']}`" if view["internal_reason"] else ""))
    producer = view["producer"]
    if producer:
        st.markdown(f"**Productor:** v{producer.get('producer_version')} · "
                    f"`{producer.get('provider_model_reported')}` · "
                    f"{producer.get('provider_latency_ms')} ms proveedor · "
                    f"{producer.get('foundation_latency_ms')} ms turno · "
                    f"{producer.get('source_calls')} llamadas a SECOP"
                    + (f" · error `{producer['provider_error']}`" if producer.get("provider_error") else ""))
    if view["requests"]:
        st.markdown("**Requests:**")
        st.json(view["requests"], expanded=False)
    if view["conditions"]:
        st.markdown("**Conditions:**")
        st.dataframe(view["conditions"], use_container_width=True)
    if view["resolutions"]:
        st.markdown("**Resoluciones del host:**")
        st.json(view["resolutions"], expanded=False)
    if view["h7"]:
        st.markdown("**H7 (universo):**")
        st.json(view["h7"], expanded=False)
    if view["groups"]:
        st.markdown(f"**Grupos** ({', '.join(view['datasets'])}):")
        st.dataframe(view["groups"], use_container_width=True)
    for item in view["plan"]:
        st.markdown(f"**SoQL {item['operation']}** · `{item['request_id']}` · `{item['dataset']}`")
        st.code(item["soql"] or "", language="sql")
    if view["receipts"]:
        st.markdown("**Recibos de llamadas a la fuente:**")
        st.dataframe(view["receipts"], use_container_width=True)
    if view["fallback"]:
        st.markdown("**Fallback básico:**")
        st.json(view["fallback"], expanded=False)
