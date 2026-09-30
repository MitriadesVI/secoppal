"""Labeled basic search when the Foundation provider is down (ADR-102 §12).

Contract: docs/audits/foundation_v3_promotion/BASIC_FALLBACK_CONTRACT.md. The only
authority is the product's existing deterministic parser (QueryRouter.parse,
never the llm_parse node), and only a closed subset of its output may execute.
Anything outside that subset makes no source call.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.core.foundation_guard import raw_source_rows
from app.core.foundation_presentation import shown_cards, text_card_lines
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError
from app.core.workflow.results import empty_result
from app.utils.money import normalize_text

MATERIAL = frozenset({"dataset", "objeto", "entidad_resolved", "departamento_resolved",
                      "fecha_desde", "fecha_hasta", "contract_universe", "contract_universe_version"})
METADATA = frozenset({"dataset_explicit", "entidad", "entidad_resolution", "departamento",
                      "departamento_resolution", "_temporal_replace_scope"})
ANCHORS = ("objeto", "entidad_resolved", "departamento_resolved")
UNIVERSE_NOUNS = frozenset({"contrato", "contratos", "proceso", "procesos", "licitacion", "licitaciones",
                            "oportunidad", "oportunidades", "convocatoria", "convocatorias"})
FALLBACK_MODE = "legacy_deterministic_basic"
NOT_SAFE = "BASIC_FALLBACK_NOT_SAFE"
LABEL = "Búsqueda básica — modo degradado"


def assess(parsed) -> tuple[dict | None, str | None]:
    """(params, None) when the parse fits the closed contract; (None, detail) otherwise."""
    params = {key: value for key, value in parsed.params.items() if value not in (None, "", [], {}, False)}
    evidence = parsed.semantic_evidence
    if parsed.needs_llm:
        return None, "NEEDS_LLM"
    if parsed.negation_requests:
        return None, "NEGATION"
    if evidence is not None and evidence.analytic_controls:
        return None, "ANALYTIC_CONTROL"
    foreign = sorted(set(params) - MATERIAL - METADATA)
    if foreign:
        return None, "UNSUPPORTED_KEYS:" + ",".join(foreign)
    if params.get("dataset") not in ("procesos", "contratos"):
        return None, "NO_DATASET"
    for raw_key in ("entidad", "departamento"):
        if params.get(raw_key) and not params.get(f"{raw_key}_resolved"):
            return None, f"UNRESOLVED_{raw_key.upper()}"
    if not any(params.get(key) for key in ANCHORS):
        return None, "NO_ANCHOR"
    topic = {normalize_text(str(term)) for term in params.get("objeto", [])}
    if topic & UNIVERSE_NOUNS:
        return None, "STRUCTURAL_TOPIC"
    residual = {normalize_text(word) for word in (evidence.material_residual if evidence else ())}
    if not residual <= topic:
        return None, "RESIDUAL_OUTSIDE_QUERY"
    return params, None


def _filters(params: dict) -> dict:
    return {key: value for key, value in (
        ("dataset", params.get("dataset")), ("entidad", params.get("entidad_resolved")),
        ("departamento", params.get("departamento_resolved")),
        ("tema", ", ".join(map(str, params.get("objeto", []))) or None),
        ("fecha", " a ".join(filter(None, (params.get("fecha_desde"), params.get("fecha_hasta")))) or None),
    ) if value}


def _telemetry(*, safe: bool, detail: str | None, filters: dict, calls: int) -> dict:
    return {"fallback_outcome": "EXECUTED" if safe else NOT_SAFE, "degraded": safe, "degradation_source": "foundation_provider_error", "fallback_mode": FALLBACK_MODE,
            "fallback_attempted": True, "fallback_safe": safe, "fallback_unsafe_detail": detail,
            "fallback_filters": filters, "fallback_source_calls": calls, "fallback_label_shown": safe,
            "advanced_provider_calls": 1, "second_llm_calls": 0}


def _not_safe(detail: str) -> dict:
    result = empty_result(
        response=("El intérprete avanzado no respondió y tu consulta tiene condiciones que la búsqueda básica "
                  "no puede conservar, así que no ejecuté ninguna búsqueda. Puedes reintentar en unos segundos."),
        route_reason="foundation_provider_error", needs_clarification=True, total_count=None)
    result.update(state="provider_error", groups=[], rows=[], total_quality="not_applicable",
                  foundation_fallback=_telemetry(safe=False, detail=detail, filters={}, calls=0))
    return result


def run_basic_fallback(workflow, ctx) -> dict:
    """A labeled deterministic search, or a no-call message when the contract does not hold."""
    parsed = workflow.deps.query_router.parse(ctx.user_query, today=ctx.today)
    params, unsafe = assess(parsed)
    if unsafe is not None:
        return _not_safe(unsafe)
    builder = workflow.soql_builder
    dataset_id = SoQLBuilder.dataset_id_for(params["dataset"])
    try:
        soql, count_soql = builder.build(dataset_id, params), builder.build_count(dataset_id, params)
    except (UnsafeGlobalQueryError, ValueError, KeyError, TypeError):
        return _not_safe("BUILDER_REFUSED")
    from app.core.source_call_receipts import source_calls_snapshot
    from app.core.workflow.helpers import _query_first_then_count

    before = len(source_calls_snapshot() or ())
    filters = _filters(params)
    lines = [LABEL, "", "El intérprete avanzado no respondió. Ejecuté una búsqueda determinista limitada con "
             "estos filtros:", *(f"- {name}: {value}" for name, value in filters.items()), ""]
    try:
        # `results` is evidence: the transport rows, exactly as a Foundation search publishes them.
        with raw_source_rows():
            total, rows, _timings, approximate = _query_first_then_count(
                workflow.secop_client, dataset_id, count_soql, soql, call_reason="foundation_basic_fallback")
        error = None
    except Exception as exc:  # noqa: BLE001 - a source failure is reported, never read as zero
        total, rows, approximate, error = None, [], False, type(exc.__cause__ or exc).__name__
    # One clock read per turn, the same authority the Foundation search uses.
    instant = getattr(workflow, "foundation_reference_instant", None) or datetime.now(ZoneInfo("America/Bogota"))
    cards = workflow.formatter.display_rows(rows, dataset_id, reference_instant=instant)
    if error is None:
        shown = shown_cards(cards, channel=ctx.channel, limit=workflow.formatter.max_results)
        lines.append(f"Resultados: encontré {'al menos ' if approximate else ''}{total}; te muestro {len(shown)}.")
        lines += text_card_lines(shown, channel=ctx.channel, limit=workflow.formatter.max_results)
    else:
        lines.append("SECOP no permitió completar la búsqueda básica. No afirmo que no existan resultados.")
    lines += ["", "Puede no conservar relaciones complejas, exclusiones o matices. Puedes reintentar la "
              "interpretación avanzada."]
    calls = len(source_calls_snapshot() or ()) - before
    result = empty_result(response="\n".join(lines), route_reason="foundation_basic_fallback",
                          parsed_params=params, resolved_params=params, soql_query=soql,
                          dataset_id=dataset_id, total_count=total)
    result.update(state="source_error" if error else "degraded_basic", rows=cards, results=rows, groups=[],
                  result_row_count=len(rows), total_count_approximate=approximate, degraded=True,
                  query_error="SOURCE_ERROR" if error else "",
                  outcome="SOURCE_ERROR" if error else ("RESULTS" if rows else "NO_RESULTS"),
                  foundation_fallback=_telemetry(safe=True, detail=None, filters=filters, calls=calls))
    return result
