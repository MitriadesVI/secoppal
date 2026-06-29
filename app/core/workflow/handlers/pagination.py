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

from app.core.conversation_store import is_pagination_phrase
from app.core.direct_responses import pagination_no_history_response
from app.core.phrase_ranking import validate_rank_and_dedupe
from app.core.query_fingerprint import query_fingerprint
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
_PAGINATION_CONTRACT_VERSION = "e8b.1-1"
_PAGINATION_CURSOR_KEYS = (_DISPLAY_OFFSET_KEY, _SHOWN_TOTAL_KEY, _PAGE_FP_KEY)


def _page_fingerprint(params: dict) -> str:
    """Huella de la consulta que define la página (sin claves de cursor), versionada por
    el contrato de paginación. Dos turnos con MISMA consulta y MISMO contrato comparten
    huella; un cambio de consulta/dataset/orden/contrato la cambia → cursor incompatible."""
    return query_fingerprint(params, extra={"pagination_contract": _PAGINATION_CONTRACT_VERSION})


def _fetch_window(
    deps: "WorkflowDeps", params: dict,
) -> tuple[str, str, int, list[dict], dict[str, float | int]]:
    """Pesca la ventana SECOP de ``params`` y reconstruye el bloque ordenado.

    Devuelve (dataset_id, soql, total_count, bloque_rankeado). El bloque es la
    lista COMPLETA tras la ruta canonica de validacion+ranking+dedupe (sin
    cortar a 25) — sobre ella se cortan las rebanadas de cada pagina.
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
    validation_started = time.perf_counter()
    ranked = validate_rank_and_dedupe(results, params, ds_id)
    metrics: dict[str, float | int] = {
        "pagination_validation_ms": round((time.perf_counter() - validation_started) * 1000, 2),
        "candidate_count": len(results),
        "validated_count": len(ranked.ranked_results),
        "quarantined_count": len(ranked.unvalidated_candidates),
    }
    return ds_id, soql, total, ranked.ranked_results, metrics


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
    if not ctx.chat_id or not is_pagination_phrase(ctx.user_query):
        return None

    history = deps.conv_store.get_history(ctx.chat_id, last_n=1)
    if history:
        prev_params = dict(history[-1].parsed_params)

        # Dual-dataset: "más" tras una búsqueda dual es ambiguo. No paginar uno solo
        # en silencio: preguntar cuál (primera versión; "más procesos"/"más contratos"
        # colapsan vía el dual-collapse del follow-up).
        scope = prev_params.get("dataset_scope")
        if isinstance(scope, list) and len(scope) >= 2:
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

        display_count = max(1, int(deps.formatter.max_results or 25))
        secop_offset = int(prev_params.get("offset", 0) or 0)
        secop_limit = int(prev_params.get("limit", _DEFAULT_SECOP_LIMIT) or _DEFAULT_SECOP_LIMIT)
        prev_display_offset = int(prev_params.get(_DISPLAY_OFFSET_KEY, 0) or 0)
        prev_shown_total = prev_params.get(_SHOWN_TOTAL_KEY)

        # Params de fetch: copia limpia (sin las claves internas), con la
        # ventana SECOP explícita del turno previo.
        fetch_params = {
            k: v for k, v in prev_params.items()
            if k not in _PAGINATION_CURSOR_KEYS and k not in _POST_QUERY_ONLY_KEYS
        }
        fetch_params["offset"] = secop_offset
        fetch_params["limit"] = secop_limit

        # Etapa 8A — FINGERPRINT GUARD: si el cursor previo trae una huella y NO coincide
        # con la de la consulta actual (cambió consulta/dataset/orden/contrato de
        # paginación, p.ej. tras un deploy), el cursor es INCOMPATIBLE → no se continúa la
        # página vieja: se reinicia desde la página 1.
        current_fp = _page_fingerprint(fetch_params)
        prev_fp = prev_params.get(_PAGE_FP_KEY)
        cursor_incompatible = bool(prev_fp) and prev_fp != current_fp
        if cursor_incompatible:
            secop_offset = 0
            fetch_params["offset"] = 0

        try:
            ds_id, soql, total, ranked, ranking_metrics = _fetch_window(deps, fetch_params)

            if cursor_incompatible:
                # Cursor de otro contrato/consulta: re-mostrar la página 1 desde cero.
                next_display_offset = 0
                prev_shown_total = 0
            else:
                if prev_shown_total is None:
                    # El turno previo fue la búsqueda inicial: mostró bloque[0:25].
                    prev_shown_total = min(len(ranked), display_count)
                    prev_display_offset = 0

                next_display_offset = prev_display_offset + display_count
                if next_display_offset >= len(ranked):
                    # Bloque agotado → avanzar a la siguiente ventana SECOP.
                    secop_offset += secop_limit
                    fetch_params["offset"] = secop_offset
                    ds_id, soql, total, ranked, next_metrics = _fetch_window(deps, fetch_params)
                    ranking_metrics = {
                        key: ranking_metrics[key] + next_metrics[key]
                        for key in ranking_metrics
                    }
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
                _PAGE_FP_KEY: current_fp,   # E8A: huella para validar el próximo "más"
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
            result.update(ranking_metrics)
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
