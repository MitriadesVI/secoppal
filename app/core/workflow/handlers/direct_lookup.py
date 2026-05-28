"""Direct-lookup handler — CO1.XXX.YYY ids, SECOP URLs, entity references.

Three kinds of lookups feed into two execution branches:

- ``id`` / ``url`` → equality query against ``id_del_proceso`` /
  ``id_contrato``. Tries the prefix-routed dataset first, then the
  other one as fallback.
- ``entity_ref`` → LIKE query against both datasets (``LP-001``,
  ``CD-08-2026-5534`` etc.).

Each branch in turn has three terminal states:
- 0 matches → friendly "not found" message.
- 1 match   → rich detail formatted by the standard channel formatter.
- 2+ matches (entity_ref only) → numbered disambiguation list.

Moved here in Paso 7D from the inline branch in
``SecopalWorkflow.run_query``.

Persistence
-----------
Legacy direct_lookup paths log a feedback trace but do NOT append a
conversation turn. That asymmetry is intentional — lookups are "go
fetch this specific thing" operations that shouldn't pollute follow-up
context. Preserved here: each branch calls ``deps.feedback.log_trace``
directly instead of ``persist_trace_and_turn``.

Contract
--------
Returns a dict for every kind/state combination matched. Returns
``None`` when ``detect_direct_lookup`` doesn't fire (so the next
handler in the chain can take the request) or when an unexpected
exception bubbles up (the legacy ``except Exception`` fallthrough,
which mirrors letting run_query continue to the normal pipeline).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.core.direct_lookup import (
    build_entity_ref_soql,
    dataset_for_id,
    detect_direct_lookup,
    normalize_entity_ref,
)
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.results import empty_result, tabular_result

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


logger = logging.getLogger(__name__)


def _log_trace(deps: "WorkflowDeps", ctx: "HandlerContext", result: dict) -> dict:
    """Direct-lookup persistence: trace only, never conv_store.append_turn."""
    trace_id = deps.feedback.log_trace(
        ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity,
    )
    result["trace_id"] = trace_id
    return result


def handle_direct_lookup(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict | None:
    """Try to resolve user_query as a SECOP id/URL/entity_ref. Returns
    None to fall through to the normal pipeline."""
    lookup_value, lookup_kind = detect_direct_lookup(ctx.user_query)
    if lookup_value is None:
        return None

    try:
        # ── Branch: entity_ref → LIKE query across both datasets ─
        if lookup_kind == "entity_ref":
            return _handle_entity_ref(deps, ctx, lookup_value)

        # ── Branch: id or url → equality query ──────────────────
        return _handle_id_or_url(deps, ctx, lookup_value, lookup_kind)
    except Exception as exc:
        logger.warning("direct_lookup failed for %s: %s", lookup_value, exc)
        # Fall through to normal pipeline
        return None


def _handle_entity_ref(deps: "WorkflowDeps", ctx: "HandlerContext", lookup_value: str) -> dict:
    normalized = normalize_entity_ref(lookup_value)
    datasets_to_search = [SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET]
    all_results: list[dict] = []
    soql = ""
    for ds_id in datasets_to_search:
        soql = build_entity_ref_soql(ds_id, normalized, limit=5)
        try:
            rows = deps.secop_client.query(ds_id, soql)
            for r in rows:
                r["_dataset_id"] = ds_id
            all_results.extend(rows)
        except Exception:
            continue

    match_count = len(all_results)

    # 0 results — friendly message
    if match_count == 0:
        return _log_trace(deps, ctx, empty_result(
            response=(
                f"🔍 No encontré la referencia `{lookup_value}` en SECOP II.\n\n"
                f"Puede que:\n"
                f"• La referencia esté mal escrita.\n"
                f"• El proceso sea de SECOP I (anterior a 2019).\n"
                f"• El proceso aún no esté publicado en datos.gov.co.\n\n"
                f"💡 Intenta con el número electrónico CO1.XXX.YYY si lo tienes."
            ),
            route_reason="direct_lookup_entity_ref",
            intent_type="direct_lookup",
            soql_query=soql,
        ))

    # 1 result — rich detail (same as id/url lookup)
    if match_count == 1:
        hit = all_results[0]
        ds = hit.pop("_dataset_id")
        hit_results = [hit]
        response, rows = deps.formatter.format_for_channel(
            hit_results, ds, ctx.channel,
            params={"intent_type": "direct_lookup", "route_reason": "direct_lookup_entity_ref"},
            total_count=1,
        )
        return _log_trace(deps, ctx, tabular_result(
            response=response,
            rows=rows,
            results=hit_results,
            dataset_id=ds,
            soql_query=soql,
            parsed_params={"direct_lookup_ref": lookup_value, "route_reason": "direct_lookup_entity_ref"},
            resolved_params={"direct_lookup_ref": lookup_value},
            route_reason="direct_lookup_entity_ref",
            total_count=1,
            intent_type="direct_lookup",
        ))

    # 2+ results — disambiguation list
    candidates: list[str] = []
    for i, r in enumerate(all_results[:5], 1):
        ds = r.get("_dataset_id")
        id_ = r.get("id_del_proceso") or r.get("id_contrato", "")
        entidad = (r.get("entidad") or r.get("nombre_entidad", "") or "?")[:60]
        objeto = (r.get("nombre_del_procedimiento") or r.get("objeto_del_contrato", "") or "Sin titulo")[:80]
        estado = r.get("estado_de_apertura_del_proceso") or r.get("estado_contrato", "?")
        tipo = "Contrato" if ds == "jbjy-vk9h" else "Proceso"
        candidates.append(
            f"{i}. [{tipo}] `{id_}` — {objeto}\n"
            f"   🏛️ {entidad} | 📌 {estado}"
        )

    disambig_response = (
        f"🔍 Encontré {match_count} resultados para la referencia `{lookup_value}`:\n\n"
        + "\n\n".join(candidates)
        + f"\n\nResponde con el número (1-{min(match_count, 5)}) o pega el ID CO1.XXX.YYY para ver el detalle completo."
    )

    return _log_trace(deps, ctx, empty_result(
        response=disambig_response,
        route_reason="entity_ref_disambiguation",
        intent_type="direct_lookup",
        soql_query=soql,
        parsed_params={"direct_lookup_ref": lookup_value, "route_reason": "entity_ref_disambiguation"},
        resolved_params={"direct_lookup_ref": lookup_value},
        total_count=match_count,
    ))


def _handle_id_or_url(
    deps: "WorkflowDeps", ctx: "HandlerContext", lookup_value: str, lookup_kind: str,
) -> dict:
    lookup_dataset = dataset_for_id(lookup_value)
    datasets_to_try: list[str] = (
        [lookup_dataset] if lookup_dataset
        else [SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET]
    )

    hit_results: list[dict] = []
    hit_dataset = ""
    soql = ""
    for ds_id in datasets_to_try:
        spec = SoQLBuilder.SPECS[ds_id]
        id_field = "id_del_proceso" if ds_id == SoQLBuilder.PROCESOS_DATASET else "id_contrato"
        fields = ", ".join(spec.select_fields)
        soql = (
            f"SELECT {fields} "
            f"WHERE {id_field} = '{SoQLBuilder._escape(lookup_value)}' "
            f"LIMIT 1"
        )
        try:
            rows = deps.secop_client.query(ds_id, soql)
            if rows:
                hit_results = rows
                hit_dataset = ds_id
                break
        except Exception:
            continue

    route_reason = f"direct_lookup_{lookup_kind}"

    if not hit_results:
        return _log_trace(deps, ctx, empty_result(
            response=(
                f"🔍 No encontré el proceso con ID `{lookup_value}` en SECOP II.\n\n"
                f"Puede que:\n"
                f"• El ID esté mal escrito.\n"
                f"• El proceso sea de SECOP I (anterior a 2019).\n"
                f"• El proceso aún no esté publicado en datos.gov.co."
            ),
            route_reason=route_reason,
            intent_type="direct_lookup",
            soql_query=soql,
        ))

    # Format result with rich detail
    response, rows = deps.formatter.format_for_channel(
        hit_results, hit_dataset, ctx.channel,
        params={"intent_type": "direct_lookup", "route_reason": route_reason},
        total_count=1,
    )
    return _log_trace(deps, ctx, tabular_result(
        response=response,
        rows=rows,
        results=hit_results,
        dataset_id=hit_dataset,
        soql_query=soql,
        parsed_params={"direct_lookup_id": lookup_value, "route_reason": route_reason},
        resolved_params={"direct_lookup_id": lookup_value},
        route_reason=route_reason,
        total_count=1,
        intent_type="direct_lookup",
    ))
