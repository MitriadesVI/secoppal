"""Orquestación dual-dataset (Commit 2): ejecutar procesos y contratos como
búsquedas HERMANAS independientes y devolver resultados estructurados.

NO presenta (eso es Commit 3), NO persiste conversación, NO corre follow-up, NO
llama al workflow completo ni reordena entre grupos. Cada hijo recorre las MISMAS
capas (build → execute → validar/rankear con `rank_candidate_pool`) y conserva sus
propios conteos. Sin ranking global: procesos y contratos son momentos jurídicos
distintos y no se mezclan.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from app.core.phrase_ranking import validate_rank_and_dedupe
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.helpers import _query_first_then_count
from app.core.workflow.result_ownership import owns_execution_and_presentation

_DATASET_NAME_BY_ID = {"p6dx-8zbt": "procesos", "jbjy-vk9h": "contratos"}

# Campos de estado/clasificación específicos de un esquema: NO deben cruzar al
# hijo del otro dataset (versión dual del frame-leak).
_PROCESOS_ONLY_KEYS = (
    "estado_de_apertura_del_proceso", "estado_del_procedimiento",
    "estado_family", "estado_field", "estado", "intent_type",
)
_CONTRATOS_ONLY_KEYS = ("estado_contrato", "estado_contrato_adicional",
                        "contract_universe", "contract_universe_version")

_CANDIDATE_LIMIT = 200
_DEFAULT_LIMIT = 50


def params_for_dataset(common: dict, dataset_name: str) -> dict:
    """Adapta los params comunes para UN dataset hijo:
    copia, fija `dataset` escalar, elimina `dataset_scope`/`dataset_explicit` y
    purga los filtros exclusivos del OTRO esquema. Conserva objeto, objeto_phrases,
    entidad, lugar, fechas y valores (el builder mapea la fecha al campo correcto).
    """
    p = dict(common)
    p["dataset"] = dataset_name
    p.pop("dataset_scope", None)
    p.pop("dataset_explicit", None)
    drop = _CONTRATOS_ONLY_KEYS if dataset_name == "procesos" else _PROCESOS_ONLY_KEYS
    for key in drop:
        p.pop(key, None)
    from app.core.contract_universe import stamp
    return stamp(p)


@dataclass(frozen=True)
class DatasetGroupResult:
    dataset: str
    status: str  # "ok" | "error"
    rows: list[dict]
    unvalidated_candidates: list[dict]
    counts: dict
    soql: str
    error: str | None
    timings_ms: dict = field(default_factory=dict)
    candidate_rows: list[dict] = field(default_factory=list)


def _counts(rows: list[dict], ranked, candidate_count: int, limit: int, total: int, approx: bool) -> dict:
    return {
        "candidate_count": candidate_count,
        "candidate_pool_full": candidate_count >= limit,
        "validated_count": len(ranked.ranked_results),
        "unvalidated_count": len(ranked.unvalidated_candidates),
        "total_count": total,
        "total_count_approximate": approx,
        "phrase_counts": ranked.phrase_counts,
    }


def execute_dataset_child(dataset_name: str, common_params: dict, deps) -> DatasetGroupResult:
    """Ejecuta UN dataset hijo y devuelve su resultado estructurado. Puro: sin
    persistir, sin follow-up, sin formatear, sin tocar estado global."""
    child = params_for_dataset(common_params, dataset_name)
    dataset_id = SoQLBuilder.dataset_id_for(dataset_name)
    limit = _CANDIDATE_LIMIT if child.get("objeto_phrases") else _DEFAULT_LIMIT
    child.setdefault("limit", limit)
    limit = int(child["limit"])
    try:
        soql = deps.soql_builder.build(dataset_id, child)
        count_soql = deps.soql_builder.build_count(dataset_id, child)
        total, rows, timings, approx = _query_first_then_count(
            deps.secop_client, dataset_id, count_soql, soql, limit=limit,
            call_reason=f"dual_child:{dataset_name}",
        )
    except Exception as exc:
        return DatasetGroupResult(dataset_name, "error", [], [], {}, "", str(exc)[:120], {})
    ranked = validate_rank_and_dedupe(rows, child, dataset_id)
    return DatasetGroupResult(
        dataset_name, "ok", ranked.ranked_results, ranked.unvalidated_candidates,
        _counts(rows, ranked, len(rows), limit, total, approx), soql, None, timings,
        list(rows),
    )


def _primary_group_from_state(name: str, result: dict, state) -> DatasetGroupResult:
    """Arma el grupo del dataset PRIMARIO desde el estado del pipeline Burr (ya
    corrió build/execute/rank). No re-ejecuta nada."""
    err = result.get("query_error") or state.get("query_error")
    rows = state.get("ranked_results") or state.get("results") or []
    unval = state.get("unvalidated_candidates") or []
    candidate_count = int(state.get("candidate_count", len(state.get("results") or [])) or 0)
    limit = _CANDIDATE_LIMIT if state.get("resolved_params", {}).get("objeto_phrases") else _DEFAULT_LIMIT
    counts = {
        "candidate_count": candidate_count,
        "candidate_pool_full": bool(state.get("candidate_pool_full", candidate_count >= limit)),
        "validated_count": len(rows),
        "unvalidated_count": len(unval),
        "total_count": int(state.get("total_count", 0) or 0),
        "total_count_approximate": bool(state.get("total_count_approximate", False)),
        "phrase_counts": state.get("phrase_counts"),
    }
    return DatasetGroupResult(
        name, "error" if err else "ok", rows, unval, counts,
        state.get("soql_query", "") or "", (str(err)[:120] if err else None), {},
        list(state.get("results") or []),
    )


def build_dataset_groups(deps, result: dict, state) -> dict[str, DatasetGroupResult] | None:
    """Si el turno pidió ambos datasets, devuelve {nombre: DatasetGroupResult}.
    El primario sale del estado del pipeline; el secundario corre por
    `execute_dataset_child`. None si no es dual o no debe ejecutarse.
    """
    # Cinturón: aunque un llamador futuro olvide la guarda, un resultado que ya
    # tiene dueño terminal no se expande. La condición es de PROPIEDAD del
    # resultado, no la ausencia de una palabra en params.
    if owns_execution_and_presentation(result, state):
        return None
    rp = result.get("resolved_params") or state.get("resolved_params") or {}
    scope = rp.get("dataset_scope")
    if not (isinstance(scope, list) and len(scope) >= 2):
        return None
    if (result.get("query_plan", {}) or {}).get("should_execute") is False:
        return None
    # A clarification raised after planning (contract universe conflict in
    # resolve_entities) blocks the whole turn: the secondary child is not an
    # exception to it. It used to run with the half the primary refused.
    if result.get("needs_clarification") or state.get("needs_clarification"):
        return None

    primary_name = _DATASET_NAME_BY_ID.get(state.get("dataset_id", ""), "procesos")
    other_name = "contratos" if primary_name == "procesos" else "procesos"

    primary = _primary_group_from_state(primary_name, result, state)
    other = execute_dataset_child(other_name, rp, deps)
    return {primary.dataset: primary, other.dataset: other}
