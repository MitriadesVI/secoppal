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

from typing import TYPE_CHECKING

from app.core.conversation_store import is_pagination_phrase
from app.core.direct_responses import pagination_no_history_response
from app.core.formatter import rank_and_dedupe
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
# Tamaño de ventana SECOP por defecto (coincide con el default de SoQLBuilder).
_DEFAULT_SECOP_LIMIT = 50


def _fetch_window(deps: "WorkflowDeps", params: dict) -> tuple[str, str, int, list[dict]]:
    """Pesca la ventana SECOP de ``params`` y reconstruye el bloque ordenado.

    Devuelve (dataset_id, soql, total_count, bloque_rankeado). El bloque es la
    lista COMPLETA tras BM25+reorder+dedupe (sin cortar a 25) — sobre ella se
    cortan las rebanadas de cada página.
    """
    ds_id = SoQLBuilder.dataset_id_for(params.get("dataset"))
    soql = deps.soql_builder.build(ds_id, params)
    total, results, _timings, _approx = _query_first_then_count(
        deps.secop_client,
        ds_id,
        deps.soql_builder.build_count(ds_id, params),
        soql,
        limit=int(params.get("limit", _DEFAULT_SECOP_LIMIT) or _DEFAULT_SECOP_LIMIT),
    )
    ranked = rank_and_dedupe(results, ds_id, params)
    return ds_id, soql, total, ranked


def handle_pagination(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict | None:
    """Pagination bypass — next page when there's history, clarification when not.

    A-buffer: la página 1 (normal_flow) muestra ``bloque[0:25]`` de una ventana
    SECOP de 50. Cada "muestrame mas" re-pesca la MISMA ventana, reconstruye el
    bloque determinísticamente y corta ``bloque[display_offset : +25]``. Cuando
    el bloque se agota, salta a la siguiente ventana (``offset += limit``). Así
    no se solapan páginas (rebanadas disjuntas del mismo bloque) ni se pierden
    filas (los 25 cortados de la página 1 salen en la 2), y los analytics del
    primer turno (observer/BM25/filtros) quedan intactos: se siguen calculando
    sobre las 50.
    """
    if not ctx.chat_id or not is_pagination_phrase(ctx.user_query):
        return None

    history = deps.conv_store.get_history(ctx.chat_id, last_n=1)
    if history:
        prev_params = dict(history[-1].parsed_params)
        display_count = max(1, int(deps.formatter.max_results or 25))
        secop_offset = int(prev_params.get("offset", 0) or 0)
        secop_limit = int(prev_params.get("limit", _DEFAULT_SECOP_LIMIT) or _DEFAULT_SECOP_LIMIT)
        prev_display_offset = int(prev_params.get(_DISPLAY_OFFSET_KEY, 0) or 0)
        prev_shown_total = prev_params.get(_SHOWN_TOTAL_KEY)

        # Params de fetch: copia limpia (sin las claves internas), con la
        # ventana SECOP explícita del turno previo.
        fetch_params = {
            k: v for k, v in prev_params.items()
            if k not in (_DISPLAY_OFFSET_KEY, _SHOWN_TOTAL_KEY)
        }
        fetch_params["offset"] = secop_offset
        fetch_params["limit"] = secop_limit

        try:
            ds_id, soql, total, ranked = _fetch_window(deps, fetch_params)

            if prev_shown_total is None:
                # El turno previo fue la búsqueda inicial: mostró bloque[0:25].
                prev_shown_total = min(len(ranked), display_count)
                prev_display_offset = 0

            next_display_offset = prev_display_offset + display_count
            if next_display_offset >= len(ranked):
                # Bloque agotado → avanzar a la siguiente ventana SECOP.
                secop_offset += secop_limit
                fetch_params["offset"] = secop_offset
                ds_id, soql, total, ranked = _fetch_window(deps, fetch_params)
                next_display_offset = 0

            slice_rows = ranked[next_display_offset:next_display_offset + display_count]
            if not slice_rows:
                # No hay más resultados en todo el universo.
                return persist_trace_and_turn(deps, ctx, empty_result(
                    response=(
                        f"Ya te mostré los {prev_shown_total} resultados "
                        "disponibles para esta búsqueda. No hay más."
                    ),
                    route_reason="pagination_exhausted",
                    intent_type="pagination_more",
                    parsed_params=prev_params,
                    resolved_params=prev_params,
                    soql_query=soql,
                    dataset_id=ds_id,
                    total_count=total,
                    followup=True,
                ))

            response, rows = deps.formatter.format_for_channel(
                slice_rows, ds_id, ctx.channel,
                params=fetch_params, total_count=total,
            )
            display_from = prev_shown_total + 1
            display_to = prev_shown_total + len(rows)
            ordering_label = (
                "de mayor valor"
                if fetch_params.get("ordering_signal") == "valor_desc"
                else "más recientes"
            )
            header = (
                f"📄 Resultados {display_from}-{display_to} de {total} "
                f"(manteniendo orden por {ordering_label})\n\n"
            )
            pag_params = {
                **fetch_params,
                _DISPLAY_OFFSET_KEY: next_display_offset,
                _SHOWN_TOTAL_KEY: prev_shown_total + len(rows),
            }
            result = tabular_result(
                response=header + response,
                rows=rows,
                results=slice_rows,
                dataset_id=ds_id,
                soql_query=soql,
                parsed_params=pag_params,
                resolved_params=pag_params,
                route_reason="pagination_more",
                total_count=total,
                intent_type="pagination_more",
                followup=True,
            )
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
