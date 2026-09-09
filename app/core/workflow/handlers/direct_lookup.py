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
import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from app.core.deadline import (
    MIN_USEFUL_FETCH_SECONDS,
    current_deadline,
    record_deadline,
)
from app.core.direct_lookup import (
    DirectLookupRequest,
    DirectLookupResolution,
    build_entity_ref_soql,
    dataset_for_id,
    deduplicate_lookup_candidates,
    detect_direct_lookup,
    resolve_reference_candidates,
)
from app.core.formatter import _estado_raw_nota, _estado_visible
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.helpers import _client_for
from app.core.workflow.results import empty_result, tabular_result

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


logger = logging.getLogger(__name__)


def _log_trace(deps: "WorkflowDeps", ctx: "HandlerContext", result: dict) -> dict:
    """Direct-lookup persistence: trace only, never conv_store.append_turn."""
    if result.get("dataset_id") == "jbjy-vk9h":
        from app.core.contract_universe import POLICY_VERSION
        for key in ("parsed_params", "resolved_params"):
            result.setdefault(key, {}).update(contract_universe="source_records",
                                               contract_universe_version=POLICY_VERSION)
    trace_id = deps.feedback.log_trace(
        ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity,
    )
    result["trace_id"] = trace_id
    return result


def handle_direct_lookup(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict | None:
    """Try to resolve user_query as a SECOP id/URL/entity_ref. Returns
    None to fall through to the normal pipeline."""
    request = detect_direct_lookup(ctx.user_query)
    if not isinstance(request, DirectLookupRequest):
        return None
    lookup_value = request.lookup_value
    lookup_kind = request.lookup_kind

    try:
        # ── Branch: entity_ref → LIKE query across both datasets ─
        if lookup_kind == "entity_ref":
            return _handle_entity_ref(deps, ctx, request)

        # ── Branch: id or url → equality query ──────────────────
        return _handle_id_or_url(deps, ctx, lookup_value, lookup_kind)
    except Exception as exc:
        logger.warning("direct_lookup failed for %s: %s", lookup_value, exc)
        # Fall through to normal pipeline
        return None


_DATASETS = (SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET)
_POOL_LIMITS = (10, 20, 40)


def _probe_budget_s(deps: "WorkflowDeps") -> float | None:
    """Presupuesto para la PRÓXIMA probe de direct_lookup (Etapa 6 H2).

    Igual que el fetch normal: ``min(timeout fuente, restante − reserva)``. Devuelve
    ``None`` cuando no hay deadline activo (tests/legacy) → sin acotar. Antes, este path
    probaba hasta 12 variantes en serie SIN presupuesto → un turno llegó a 67s.
    """
    dl = current_deadline()
    if dl is None:
        return None
    src = getattr(deps.secop_client, "timeout", 15) or 15
    return dl.fetch_budget_s(src)


def _is_timeout(exc: Exception) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        detail = f"{type(current).__name__} {current}".lower()
        if isinstance(current, TimeoutError) or "timeout" in detail or "timed out" in detail:
            return True
        current = current.__cause__ or current.__context__
    return False


def _query_attempt(
    deps: "WorkflowDeps",
    dataset: str,
    mode: str,
    soql: str,
    limit: int,
    budget_s: float | None = None,
) -> tuple[list[dict] | None, dict]:
    attempt = {
        "dataset": dataset,
        "mode": mode,
        "status": "completed",
        "error_type": None,
        "timeout": False,
        "limit": limit,
        "row_count": 0,
    }
    try:
        # Acotado al presupuesto restante del turno (H2): el probe no puede colgar 60s.
        client = (
            _client_for(deps.secop_client, max(1, math.ceil(budget_s)))
            if budget_s is not None else deps.secop_client
        )
        rows = client.query(dataset, soql)
        attempt["row_count"] = len(rows)
        return rows, attempt
    except Exception as exc:
        attempt.update({
            "status": "failed",
            "error_type": type(exc).__name__,
            "timeout": _is_timeout(exc),
        })
        return None, attempt


def _budget_skipped_attempt(dataset: str, mode: str, limit: int) -> dict:
    """Probe NO iniciada por presupuesto agotado (corte honesto, no es fallo de fuente)."""
    return {
        "dataset": dataset, "mode": mode, "status": "skipped",
        "error_type": None, "timeout": False, "limit": limit,
        "row_count": 0, "skipped_due_to_budget": True,
    }


def _attach_lookup_metadata(
    result: dict,
    status: str,
    reason: str,
    attempts: list[dict],
) -> dict:
    successful = [
        dataset for dataset in _DATASETS
        if any(a["dataset"] == dataset and a["status"] == "completed" for a in attempts)
    ]
    failed = [
        dataset for dataset in _DATASETS
        if any(a["dataset"] == dataset and a["status"] == "failed" for a in attempts)
    ]
    result.update({
        "direct_lookup_status": status,
        "direct_lookup_reason": reason,
        "successful_sources": successful,
        "failed_sources": failed,
        "search_attempts": attempts,
        "source_partial": bool(successful and failed),
    })
    return result


def _finish(
    deps: "WorkflowDeps",
    ctx: "HandlerContext",
    result: dict,
    status: str,
    reason: str,
    attempts: list[dict],
) -> dict:
    return _log_trace(
        deps, ctx, _attach_lookup_metadata(result, status, reason, attempts),
    )


def _terminal_absence(
    deps: "WorkflowDeps",
    ctx: "HandlerContext",
    *,
    lookup_value: str,
    route_reason: str,
    soql: str,
    attempts: list[dict],
    confirmed_reason: str,
    budget_cut: bool = False,
) -> dict:
    completed = any(a["status"] == "completed" for a in attempts)
    failed = any(a["status"] == "failed" for a in attempts)
    if budget_cut:
        # Se agotó el presupuesto del turno antes de descartar todas las variantes: NO se
        # puede afirmar "no encontré" (no se exploró todo) — corte honesto por tiempo.
        status, reason = "not_confirmed", "deadline_exhausted"
        response = (
            f"⏱️ No pude confirmar la referencia `{lookup_value}` en el tiempo disponible. "
            "Intenta de nuevo o pega el ID CO1.XXX.YYY para una búsqueda directa."
        )
    elif not completed:
        status, reason = "source_unavailable", "all_sources_failed"
        response = "⚠️ No pude consultar SECOP II en este momento. Intenta de nuevo más tarde."
    elif failed:
        status, reason = "not_confirmed", "partial_source_failure"
        response = (
            f"⚠️ No pude confirmar la referencia `{lookup_value}` porque una fuente de "
            "SECOP II no respondió. Intenta de nuevo más tarde."
        )
    else:
        status, reason = "not_found", confirmed_reason
        response = (
            f"🔍 No encontré la referencia `{lookup_value}` en las fuentes consultadas de SECOP II."
        )
    return _finish(deps, ctx, empty_result(
        response=response,
        route_reason=route_reason,
        intent_type="direct_lookup",
        soql_query=soql,
    ), status, reason, attempts)


def _search_reference(
    deps: "WorkflowDeps", request: DirectLookupRequest,
) -> tuple[DirectLookupResolution, list[dict], str, bool]:
    pools: dict[str, dict[str, list[dict]]] = {"exact": {}, "tolerant": {}}
    attempts: list[dict] = []
    soql = ""
    budget_cut = False
    resolution = resolve_reference_candidates(request, [])

    for mode in ("exact", "tolerant"):
        active = set(_DATASETS)
        limits = _POOL_LIMITS if request.entity_qualifier_raw else _POOL_LIMITS[:1]
        for limit in limits:
            for dataset in _DATASETS:
                if dataset not in active:
                    continue
                # H2: cortar las variantes seriales cuando el presupuesto del turno se agota.
                budget = _probe_budget_s(deps)
                if budget is not None and budget < MIN_USEFUL_FETCH_SECONDS:
                    attempts.append(_budget_skipped_attempt(dataset, mode, limit))
                    record_deadline(
                        direct_lookup_skipped_due_to_budget=True,
                        deadline_exhausted=True, deadline_stage="direct_lookup",
                    )
                    budget_cut = True
                    return resolution, attempts, soql, budget_cut
                soql = build_entity_ref_soql(
                    dataset,
                    request.reference_normalized,
                    limit=limit,
                    exact=mode == "exact",
                )
                rows, attempt = _query_attempt(deps, dataset, mode, soql, limit, budget_s=budget)
                attempts.append(attempt)
                if rows is None:
                    active.remove(dataset)
                    continue
                pools[mode][dataset] = [
                    {**row, "_dataset_id": dataset} for row in rows
                ]
                if len(rows) < limit:
                    active.remove(dataset)

            candidates = [
                row
                for search_mode in ("exact", "tolerant")
                for dataset in _DATASETS
                for row in pools[search_mode].get(dataset, [])
            ]
            resolution = resolve_reference_candidates(request, candidates)
            if resolution.candidates:
                return resolution, attempts, soql, budget_cut
            if not active:
                break

    return resolution, attempts, soql, budget_cut


def _handle_entity_ref(
    deps: "WorkflowDeps", ctx: "HandlerContext", request: DirectLookupRequest,
) -> dict:
    lookup_value = request.reference_raw
    resolution, attempts, soql, budget_cut = _search_reference(deps, request)
    match_count = len(resolution.candidates)

    if match_count == 0:
        return _terminal_absence(
            deps, ctx,
            lookup_value=lookup_value,
            route_reason="direct_lookup_entity_ref",
            soql=soql,
            attempts=attempts,
            confirmed_reason=resolution.outcome_reason,
            budget_cut=budget_cut,
        )

    # 1 result — rich detail (same as id/url lookup)
    if match_count == 1:
        hit = dict(resolution.candidates[0])
        ds = hit.pop("_dataset_id")
        hit_results = [hit]
        response, rows = deps.formatter.format_for_channel(
            hit_results, ds, ctx.channel,
            params={"intent_type": "direct_lookup", "route_reason": "direct_lookup_entity_ref"},
            total_count=1,
        )
        return _finish(deps, ctx, tabular_result(
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
        ), resolution.outcome, resolution.outcome_reason, attempts)

    # 2+ results — disambiguation list. Un solo instante para todos los procesos
    # visibles; contratos y fallbacks conservan el estado raw anterior.
    reference_instant_utc = datetime.now(timezone.utc)
    candidates: list[str] = []
    for i, r in enumerate(resolution.candidates[:5], 1):
        ds = r.get("_dataset_id")
        id_ = r.get("id_del_proceso") or r.get("id_contrato", "")
        entidad = (r.get("entidad") or r.get("nombre_entidad", "") or "?")[:60]
        objeto = (r.get("nombre_del_procedimiento") or r.get("objeto_del_contrato", "") or "Sin titulo")[:80]
        estado = r.get("estado_de_apertura_del_proceso") or r.get("estado_contrato", "?")
        nota_estado = ""
        if ds == SoQLBuilder.PROCESOS_DATASET:
            try:
                cards = deps.formatter.to_rows(
                    [dict(r)], ds,
                    {"intent_type": "direct_lookup", "route_reason": "entity_ref_disambiguation"},
                    reference_instant=reference_instant_utc,
                )
                if cards:
                    estado = _estado_visible(cards[0]) or estado
                    nota_estado = _estado_raw_nota(cards[0])
            except Exception:
                # Best-effort: una fila legacy o formatter parcial conserva el
                # texto raw previo sin romper toda la desambiguación.
                pass
        tipo = "Contrato" if ds == "jbjy-vk9h" else "Proceso"
        candidate = (
            f"{i}. [{tipo}] `{id_}` — {objeto}\n"
            f"   🏛️ {entidad} | 📌 {estado}"
        )
        if nota_estado:
            candidate += f"\n   {nota_estado}"
        candidates.append(candidate)

    disambig_response = (
        f"🔍 Encontré {match_count} resultados para la referencia `{lookup_value}`:\n\n"
        + "\n\n".join(candidates)
        + f"\n\nResponde con el número (1-{min(match_count, 5)}) o pega el ID CO1.XXX.YYY para ver el detalle completo."
    )

    return _finish(deps, ctx, empty_result(
        response=disambig_response,
        route_reason="entity_ref_disambiguation",
        intent_type="direct_lookup",
        soql_query=soql,
        parsed_params={"direct_lookup_ref": lookup_value, "route_reason": "entity_ref_disambiguation"},
        resolved_params={"direct_lookup_ref": lookup_value},
        total_count=match_count,
    ), resolution.outcome, resolution.outcome_reason, attempts)


def _handle_id_or_url(
    deps: "WorkflowDeps", ctx: "HandlerContext", lookup_value: str, lookup_kind: str,
) -> dict:
    lookup_dataset = dataset_for_id(lookup_value)
    datasets_to_try = list(_DATASETS)
    if lookup_dataset in datasets_to_try:
        datasets_to_try.remove(lookup_dataset)
        datasets_to_try.insert(0, lookup_dataset)

    hit_results: list[dict] = []
    attempts: list[dict] = []
    soql = ""
    budget_cut = False
    for dataset in datasets_to_try:
        budget = _probe_budget_s(deps)
        if budget is not None and budget < MIN_USEFUL_FETCH_SECONDS:
            attempts.append(_budget_skipped_attempt(dataset, "uid", 1))
            record_deadline(
                direct_lookup_skipped_due_to_budget=True,
                deadline_exhausted=True, deadline_stage="direct_lookup",
            )
            budget_cut = True
            break
        spec = SoQLBuilder.SPECS[dataset]
        id_field = "id_del_proceso" if dataset == SoQLBuilder.PROCESOS_DATASET else "id_contrato"
        fields = ", ".join(spec.select_fields)
        soql = (
            f"SELECT {fields} "
            f"WHERE {id_field} = '{SoQLBuilder._escape(lookup_value)}' "
            f"LIMIT 1"
        )
        rows, attempt = _query_attempt(deps, dataset, "uid", soql, 1, budget_s=budget)
        attempts.append(attempt)
        if rows:
            hit_results.extend({**row, "_dataset_id": dataset} for row in rows)

    route_reason = f"direct_lookup_{lookup_kind}"

    if not hit_results:
        return _terminal_absence(
            deps, ctx,
            lookup_value=lookup_value,
            route_reason=route_reason,
            soql=soql,
            attempts=attempts,
            confirmed_reason="uid_not_found",
            budget_cut=budget_cut,
        )

    unique_hits = deduplicate_lookup_candidates(hit_results)
    hit = dict(unique_hits[0])
    hit_dataset = hit.pop("_dataset_id")
    hit.update({
        "match_type": "exact_uid",
        "reference_match": "uid",
        "entity_match": "not_requested",
        "confidence": "high",
    })
    formatted_results = [hit]

    # Format result with rich detail
    response, rows = deps.formatter.format_for_channel(
        formatted_results, hit_dataset, ctx.channel,
        params={"intent_type": "direct_lookup", "route_reason": route_reason},
        total_count=1,
    )
    return _finish(deps, ctx, tabular_result(
        response=response,
        rows=rows,
        results=formatted_results,
        dataset_id=hit_dataset,
        soql_query=soql,
        parsed_params={"direct_lookup_id": lookup_value, "route_reason": route_reason},
        resolved_params={"direct_lookup_id": lookup_value},
        route_reason=route_reason,
        total_count=1,
        intent_type="direct_lookup",
    ), "exact", "uid_match", attempts)
