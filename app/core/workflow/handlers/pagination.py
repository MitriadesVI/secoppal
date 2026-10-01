"""Pagination handler — "muestrame mas" and synonyms.

Two sub-branches share this handler because they're both triggered by
the same predicate (``is_pagination_phrase(user_query)``):

1. **Hit**: history exists for the chat. Re-issue the previous turn's
   SoQL with ``offset += limit`` so the user gets the next page.
   Returns a tabular result with ``route_reason="pagination_more"``.

2. **Miss**: chat has no history. We must NOT fall through to the
   normal flow — a parsed "muestrame mas" with no scope would hit
   WHERE 1=1. Instead return a clarification asking for context.
   ``route_reason="pagination_no_context"``.

Moved here in Paso 7B from the inline branch in
``SecopalWorkflow.run_query``. The hit path uses ``tabular_result`` +
``persist_trace_and_turn`` from the staged modules; the miss path uses
``empty_result``.

Contract
--------
Returns a dict for both sub-branches. Returns ``None`` only when the
predicate doesn't match (no chat_id, or query is not a pagination
phrase) — that signals the caller to keep walking the chain.

If the SoQL execution raises in the hit path, we fall through to the
miss branch's clarification rather than crashing. This mirrors the
pre-refactor behavior (the original code had a bare ``except Exception:
pass`` that effectively did the same.
"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING

from app.core import pagination_blocked
from app.core.conversation_store import is_pagination_phrase, normalize_pagination_text
from app.core.direct_responses import pagination_no_history_response
from app.core.pagination_buffer import (
    MAX_CANDIDATE_ROWS,
    PaginationBuffer,
)
from app.core.phrase_ranking import validate_rank_and_dedupe
from app.core.pagination_buffer import pagination_fingerprint
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.helpers import _query_first_then_count
from app.core.workflow.persistence import persist_trace_and_turn
from app.core.workflow.results import empty_result, tabular_result

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps

# Claves de estado A-buffer que viven en parsed_params pero NO son params de
# SECOP (SoQL/observer las ignoran). Se quitan antes de construir el SoQL.
_DISPLAY_OFFSET_KEY = "_display_offset"
_SHOWN_TOTAL_KEY = "_shown_total"
_PAGE_FP_KEY = "_page_fingerprint"
# ``normal_flow`` puede añadir esta clave DESPUES de ejecutar/rankear para la
# respuesta asesora. No pertenecio a la fuente de pagina 1 y no debe cambiar la
# repesca de pagina 2.
_POST_QUERY_ONLY_KEYS = frozenset({"entidad_ecosystem"})
# Tamaño de ventana SECOP por defecto (coincide con el default de SoQLBuilder).
_DEFAULT_SECOP_LIMIT = 50

# Etapa 8A: versión del CONTRATO de paginación (orden + ranking + validación). Si cambia
# la semántica de cómo se ordena/valida/rankea una página, BUMPEAR → el fingerprint del
# cursor cambia → un cursor de un deploy anterior NO se reutiliza (se reinicia desde la
# página 1). Protege contra "continuar una página vieja" tras un cambio de código.
_PAGINATION_CURSOR_KEYS = (_DISPLAY_OFFSET_KEY, _SHOWN_TOTAL_KEY, _PAGE_FP_KEY)


def _page_fingerprint(params: dict) -> str:
    """Huella de la consulta que define la página (sin claves de cursor), versionada por
    el contrato de paginación. Dos turnos con MISMA consulta y MISMO contrato comparten
    huella; un cambio de consulta/dataset/orden/contrato la cambia → cursor incompatible."""
    return pagination_fingerprint(params)


def _fetch_window(
    deps: "WorkflowDeps", params: dict,
) -> tuple[str, str, int, bool, list[dict], list[dict], dict[str, float | int]]:
    """Pesca la ventana SECOP de ``params`` y reconstruye el bloque ordenado.

    Devuelve (dataset_id, soql, total_count, bloque_rankeado). El bloque es la
    lista COMPLETA tras la ruta canonica de validacion+ranking+dedupe (sin
    cortar a 25) — sobre ella se cortan las rebanadas de cada pagina.
    """
    ds_id = SoQLBuilder.dataset_id_for(params.get("dataset"))
    soql = deps.soql_builder.build(ds_id, params)
    total, results, _timings, approximate = _query_first_then_count(
        deps.secop_client,
        ds_id,
        deps.soql_builder.build_count(ds_id, params),
        soql,
        limit=int(params.get("limit", _DEFAULT_SECOP_LIMIT) or _DEFAULT_SECOP_LIMIT),
    )
    validation_started = time.perf_counter()
    ranked = validate_rank_and_dedupe(results, params, ds_id)
    metrics: dict[str, float | int] = {
        "pagination_validation_ms": round((time.perf_counter() - validation_started) * 1000, 2),
        "candidate_count": len(results),
        "validated_count": len(ranked.ranked_results),
        "quarantined_count": len(ranked.unvalidated_candidates),
    }
    return ds_id, soql, total, approximate, results, ranked.ranked_results, metrics


def _requested_group(text: str) -> str | None:
    """Reconoce solo paginacion con selector de grupo, sin filtros nuevos."""
    normalized = normalize_pagination_text(text)
    patterns = {
        "procesos": {"mas procesos", "muestrame mas procesos", "otros procesos"},
        "contratos": {"mas contratos", "muestrame mas contratos", "otros contratos"},
    }
    for group, values in patterns.items():
        if normalized in values:
            return group
    return None


def _clean_fetch_params(params: dict) -> dict:
    return {
        key: value for key, value in params.items()
        if key not in _PAGINATION_CURSOR_KEYS and key not in _POST_QUERY_ONLY_KEYS
    }


def _group_for_params(params: dict) -> str:
    return "contratos" if params.get("dataset") == "contratos" else "procesos"


def _sum_metrics(
    current: dict[str, float | int], update: dict[str, float | int],
) -> dict[str, float | int]:
    return {key: current.get(key, 0) + update.get(key, 0) for key in set(current) | set(update)}


def handle_pagination(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict | None:
    """Pagination bypass — next page when there's history, clarification when not.

    A-buffer: la página 1 (normal_flow) muestra ``bloque[0:25]`` de una ventana
    SECOP. Cada "muestrame mas" re-pesca la MISMA ventana, reconstruye el
    bloque determinísticamente y corta ``bloque[display_offset : +25]``. Cuando
    el bloque se agota, salta a la siguiente ventana (``offset += limit``). Así
    no se solapan páginas (rebanadas disjuntas del mismo bloque) ni se pierden
    filas (los 25 cortados de la página 1 salen en la 2), y los analytics del
    primer turno (observer/BM25/filtros) quedan intactos: se siguen calculando
    sobre las 50.
    """
    requested_group = _requested_group(ctx.user_query)
    admitted_buffer = ctx.tabular_referent.buffer if ctx.tabular_referent else None
    if not ctx.chat_id or not (is_pagination_phrase(ctx.user_query) or requested_group or admitted_buffer is not None):
        return None

    history = [] if ctx.tabular_referent else deps.conv_store.get_history(ctx.chat_id, last_n=1)
    if history and not ctx.tabular_referent and pagination_blocked.is_blocked(history[-1]):
        # The last turn asked a question instead of executing: there is no page to continue.
        # Not persisted as a turn, so the blocked frame (and its open question) stays the last.
        result = empty_result(
            response=pagination_blocked.MESSAGE, route_reason="pagination_blocked_turn",
            needs_clarification=True, intent_type="pagination_more", followup=True,
        )
        result["trace_id"] = deps.feedback.log_trace(
            ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity,
        )
        return result
    if ctx.tabular_referent or history:
        prev_params = (dict(ctx.tabular_referent.params) if ctx.tabular_referent
                       else dict(history[-1].parsed_params))

        from app.core.contract_universe import compatible
        if not compatible(prev_params):
            return persist_trace_and_turn(deps, ctx, empty_result(
                response="El universo contractual de esa página es anterior a la política vigente. Repite la búsqueda.",
                route_reason="contract_universe_review_required", needs_clarification=True,
                parsed_params=prev_params, resolved_params=prev_params,
            ))

        # Dual-dataset: "más" tras una búsqueda dual es ambiguo. No paginar uno solo
        # en silencio: preguntar cuál (primera versión; "más procesos"/"más contratos"
        # colapsan vía el dual-collapse del follow-up).
        scope = prev_params.get("dataset_scope")
        if isinstance(scope, list) and len(scope) >= 2 and requested_group is None:
            return persist_trace_and_turn(deps, ctx, empty_result(
                response=("Esta búsqueda trae dos grupos. ¿Quieres ver más "
                          "*procesos* o más *contratos*?"),
                route_reason="pagination_dual_clarify",
                intent_type="pagination_more",
                parsed_params=prev_params,
                resolved_params=prev_params,
                soql_query="",
                dataset_id="",
                total_count=0,
                followup=True,
            ))

        try:
            display_count = max(1, int(deps.formatter.max_results or 25))
            if admitted_buffer is not None:
                buffer = admitted_buffer
                group = buffer.group
                fetch_params = dict(buffer.query_params)
                current_fp = buffer.query_fingerprint
            elif requested_group:
                group = requested_group
                buffer = deps.conv_store.get_pagination_buffer(ctx.chat_id, group, None)
                if buffer is not None:
                    fetch_params = dict(buffer.query_params)
                    current_fp = buffer.query_fingerprint
                elif isinstance(scope, list) and len(scope) >= 2:
                    from app.core.workflow.dual_dataset import params_for_dataset
                    fetch_params = params_for_dataset(_clean_fetch_params(prev_params), group)
                    current_fp = _page_fingerprint(fetch_params)
                else:
                    fetch_params = _clean_fetch_params(prev_params)
                    if _group_for_params(fetch_params) != group:
                        raise LookupError("no buffer for requested group")
                    current_fp = _page_fingerprint(fetch_params)
            else:
                fetch_params = _clean_fetch_params(prev_params)
                group = _group_for_params(fetch_params)
                current_fp = _page_fingerprint(fetch_params)
                buffer = deps.conv_store.get_pagination_buffer(
                    ctx.chat_id, group, current_fp,
                )

            ranking_metrics: dict[str, float | int] = {
                "pagination_validation_ms": 0.0,
                "candidate_count": 0,
                "validated_count": 0,
                "quarantined_count": 0,
            }
            if buffer is not None and not compatible(buffer.query_params):
                raise ValueError("El universo contractual del buffer requiere una nueva búsqueda")
            buffer_hit = buffer is not None
            soql = (
                deps.soql_builder.build(buffer.dataset_id, buffer.query_params)
                if buffer is not None else ""
            )

            # TTL/fingerprint miss: reconstruir desde pagina 1, no continuar un cursor viejo.
            if buffer is None:
                fetch_params["offset"] = 0
                fetch_params.setdefault("limit", _DEFAULT_SECOP_LIMIT)
                ds_id, soql, total, approximate, candidates, ranked, ranking_metrics = _fetch_window(
                    deps, fetch_params,
                )
                limit = int(fetch_params.get("limit", _DEFAULT_SECOP_LIMIT) or _DEFAULT_SECOP_LIMIT)
                buffer = PaginationBuffer.create(
                    query_fingerprint=current_fp,
                    group=group,
                    dataset_id=ds_id,
                    query_params=fetch_params,
                    candidate_buffer=candidates,
                    validated_ranked_buffer=ranked,
                    display_cursor=0,
                    source_cursor=limit,
                    source_candidate_total=max(int(total or 0), len(candidates)),
                    source_total_approximate=approximate,
                )

            # Refill solo cuando el snapshot no alcanza para completar la pagina.
            while (
                len(buffer.validated_ranked_buffer) - buffer.display_cursor < display_count
                and not buffer.source_exhausted
                and len(buffer.candidate_buffer) < MAX_CANDIDATE_ROWS
            ):
                refill_params = dict(buffer.query_params)
                limit = int(refill_params.get("limit", _DEFAULT_SECOP_LIMIT) or _DEFAULT_SECOP_LIMIT)
                refill_params["offset"] = buffer.source_cursor
                ds_id, soql, total, approximate, candidates, ranked, refill_metrics = _fetch_window(
                    deps, refill_params,
                )
                ranking_metrics = _sum_metrics(ranking_metrics, refill_metrics)
                next_cursor = buffer.source_cursor + limit
                buffer = buffer.extend(
                    candidate_rows=candidates,
                    validated_rows=ranked,
                    next_source_cursor=next_cursor,
                    source_candidate_total=max(int(total or 0), buffer.source_candidate_total),
                    source_total_approximate=(approximate and len(candidates) >= limit),
                )
                if not candidates:
                    break

            slice_rows, advanced = buffer.take(display_count)
            if not slice_rows:
                capacity_reached = (
                    not buffer.source_exhausted
                    and len(buffer.candidate_buffer) >= MAX_CANDIDATE_ROWS
                )
                route_reason = (
                    "pagination_capacity_reached" if capacity_reached else "pagination_exhausted"
                )
                message = (
                    "Alcancé el límite temporal del buffer (600 candidatos). "
                    "Refina la búsqueda para seguir sin mezclar resultados."
                    if capacity_reached else
                    f"Ya te mostré los {buffer.displayed_so_far} resultados "
                    "disponibles para esta búsqueda. No hay más."
                )
                exhausted = empty_result(
                    response=message,
                    route_reason=route_reason,
                    intent_type="pagination_more",
                    parsed_params=prev_params,
                    resolved_params=prev_params,
                    soql_query=soql,
                    dataset_id=buffer.dataset_id,
                    total_count=buffer.source_candidate_total,
                    followup=True,
                )
                exhausted.update({
                    "source_candidate_total": buffer.source_candidate_total,
                    "validated_total_known": buffer.validated_total_known,
                    "displayed_so_far": buffer.displayed_so_far,
                    "source_total_approximate": buffer.source_total_approximate,
                })
                return persist_trace_and_turn(deps, ctx, exhausted)

            response, rows = deps.formatter.format_for_channel(
                slice_rows, buffer.dataset_id, ctx.channel,
                params=buffer.query_params, total_count=buffer.source_candidate_total,
            )
            display_from = buffer.displayed_so_far + 1
            display_to = buffer.displayed_so_far + len(rows)
            from app.core.order_semantics import ordering_label as _ordering_label
            ordering_label = _ordering_label(buffer.query_params)
            source_label = (
                f"{advanced.source_candidate_total}+"
                if advanced.source_total_approximate
                else str(advanced.source_candidate_total)
            )
            header = (
                f"📄 Resultados mostrados {display_from}-{display_to}; "
                f"{advanced.validated_total_known} validados conocidos de "
                f"{source_label} candidatos SECOP "
                f"(manteniendo orden por {ordering_label})\n\n"
            )
            pag_params = {
                **advanced.query_params,
                _DISPLAY_OFFSET_KEY: buffer.displayed_so_far,
                _SHOWN_TOTAL_KEY: advanced.displayed_so_far,
                _PAGE_FP_KEY: current_fp,   # E8A: huella para validar el próximo "más"
            }
            deps.conv_store.save_pagination_buffer(ctx.chat_id, advanced)
            result = tabular_result(
                response=header + response,
                rows=rows,
                results=slice_rows,
                dataset_id=advanced.dataset_id,
                soql_query=soql,
                parsed_params=pag_params,
                resolved_params=pag_params,
                route_reason="pagination_more",
                total_count=advanced.source_candidate_total,
                intent_type="pagination_more",
                followup=True,
            )
            known_candidates = len(advanced.candidate_buffer)
            result.update({
                **ranking_metrics,
                "candidate_count": known_candidates,
                "validated_count": advanced.validated_total_known,
                "quarantined_count": max(0, known_candidates - advanced.validated_total_known),
                "source_candidate_total": advanced.source_candidate_total,
                "validated_total_known": advanced.validated_total_known,
                "displayed_so_far": advanced.displayed_so_far,
                "pagination_buffer_hit": buffer_hit,
                "source_total_approximate": advanced.source_total_approximate,
            })
            return persist_trace_and_turn(deps, ctx, result)
        except Exception:
            # Fall through to the no-history clarification — the previous
            # implementation had an equivalent bare except that swallowed
            # SECOP errors during paginate-more.
            pass

    # No history (or hit branch failed): ask for context, don't WHERE 1=1.
    return empty_result(
        response=pagination_no_history_response(),
        route_reason="pagination_no_context",
        needs_clarification=True,
        intent_type="pagination_more",
    )
