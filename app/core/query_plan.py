"""QueryPlan — deterministically decide what to do BEFORE executing a query.

Pure function: same input → same output. No I/O, no SECOP, no DB.
Consumes DialogueSignal instances from dialogue_policy and applies
9 derivation rules (P1..P9) in strict order.

Architecture constraint (G8): this module MUST stay under 300 LoC.
"""

from __future__ import annotations

import calendar
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date as Date

from app.core.dialogue_policy import classify, DialogueSignal

# ── All known rules — used as default when enabled_rules is None ──────────────
ALL_RULES = {"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9"}

# ── Entity markers that preserve entity when present in raw_query (P9) ───────
_ENTITY_MARKERS = (
    "alcaldía de", "alcaldia de",
    "gobernación de", "gobernacion de",
    "distrito de",
    "municipio de",
    "secretaría de", "secretaria de",
    "entidad ",
)


@dataclass
class QueryPlan:
    """Result of compute_query_plan. Dict-serializable via dataclasses.asdict."""

    should_execute: bool
    params: dict  # modified params (deep copy, never mutates input)
    missing_slots: list[str] = field(default_factory=list)
    risk_flags: list[str] = field(default_factory=list)
    clarification_question: str | None = None
    reason: str = ""  # "command_incomplete", "valid_query", ...
    signals: list[DialogueSignal] = field(default_factory=list)


def compute_query_plan(
    params: dict,
    raw_query: str,
    context: dict | None = None,
    enabled_rules: set[str] | None = None,
) -> QueryPlan:
    """Produce a QueryPlan from parsed params and raw user text.

    Pure function. No side effects. No mutation of input params.

    Args:
        params: parsed params dict from query_router. NOT mutated.
        raw_query: original user text.
        context: optional dict with keys:
            - prev_params: dict | None
            - prev_intent_type: str | None
            - turn_index: int
            - today: date | None (defaults to date.today())
        enabled_rules: subset of ALL_RULES to apply. None = all rules.

    Returns:
        QueryPlan with deep-copied params.
    """
    working_params = deepcopy(params)
    rules_to_apply = enabled_rules if enabled_rules is not None else ALL_RULES
    ctx = context or {}
    today = ctx.get("today") or Date.today()

    all_signals = classify(raw_query, params, context)
    reason_parts: list[str] = []
    risk_flags: list[str] = []

    # ── P1: command_incomplete ────────────────────────────────────────────
    if "P1" in rules_to_apply:
        for sig in all_signals:
            if sig.kind == "command_incomplete":
                slot = sig.payload.get("slot", "criterio")
                return QueryPlan(
                    should_execute=False,
                    params=working_params,
                    missing_slots=[slot],
                    risk_flags=risk_flags,
                    clarification_question=f"¿A qué {slot} quieres filtrar?",
                    reason="command_incomplete",
                    signals=all_signals,
                )

    # ── P2: non_query_followup ────────────────────────────────────────────
    if "P2" in rules_to_apply:
        for sig in all_signals:
            if sig.kind == "non_query_followup":
                risk_flags.append("complaint_detected")
                return QueryPlan(
                    should_execute=False,
                    params=working_params,
                    risk_flags=risk_flags,
                    clarification_question=(
                        "Parece que algo no funcionó. "
                        "¿Quieres que diagnostiquemos por qué no apareció "
                        "el resultado anterior?"
                    ),
                    reason="non_query_followup",
                    signals=all_signals,
                )

    # ── P3: city_mention_as_topic ─────────────────────────────────────────
    if "P3" in rules_to_apply:
        for sig in all_signals:
            if sig.kind == "city_mention_as_topic":
                city = sig.payload["city"]
                token = sig.payload["token_in_objeto"]
                objeto: list[str] = working_params.get("objeto", [])

                if working_params.get("ciudad") and working_params["ciudad"] != city:
                    risk_flags.append("ambiguous_city")
                    reason_parts.append("ambiguous_city_skipped")
                else:
                    working_params["ciudad"] = city
                    new_objeto = [
                        t for t in objeto
                        if t.lower().strip() != token.lower().strip()
                    ]
                    working_params["objeto"] = new_objeto
                    reason_parts.append("city_extracted_to_scope")

                    if not new_objeto:
                        return QueryPlan(
                            should_execute=False,
                            params=working_params,
                            missing_slots=["topic"],
                            risk_flags=risk_flags,
                            clarification_question=(
                                f"Detecté '{city}' como ciudad. "
                                "¿Qué tema o rubro buscas?"
                            ),
                            reason="city_extracted_topic_missing",
                            signals=all_signals,
                        )
                break

    # ── P9: bare_city_no_entity_promotion ─────────────────────────────────
    if "P9" in rules_to_apply:
        ciudad_set = bool(working_params.get("ciudad"))
        entidad_set = bool(
            working_params.get("entidad") or working_params.get("entidad_resolved")
        )
        if ciudad_set and entidad_set:
            query_lower = raw_query.lower().strip()
            has_marker = any(m in query_lower for m in _ENTITY_MARKERS)
            if not has_marker:
                working_params.pop("entidad", None)
                working_params.pop("entidad_resolved", None)
                working_params.pop("entidad_resolution", None)
                risk_flags.append("entity_autopromotion_purged")
                reason_parts.append("entity_autopromotion_purged")

    # ── P4: implicit_opportunity ──────────────────────────────────────────
    if "P4" in rules_to_apply:
        for sig in all_signals:
            if sig.kind == "implicit_opportunity":
                intent = working_params.get("intent_type")
                if not intent or intent == "history_search":
                    working_params["intent_type"] = "opportunity_search"
                    if not working_params.get("estado_family"):
                        working_params["estado_family"] = "oferta_abierta"
                    reason_parts.append("implicit_opportunity_promoted")
                break

    # ── P5: temporal_month ────────────────────────────────────────────────
    if "P5" in rules_to_apply:
        for sig in all_signals:
            if sig.kind == "temporal_month":
                p = sig.payload
                month = p["month"]
                year = p["year"]
                last_day = calendar.monthrange(year, month)[1]
                desde = f"{year}-{month:02d}-01"
                hasta = f"{year}-{month:02d}-{last_day:02d}"

                if working_params.get("fecha_desde") or working_params.get("fecha_hasta"):
                    risk_flags.append("temporal_conflict")
                else:
                    working_params["fecha_desde"] = desde
                    working_params["fecha_hasta"] = hasta
                    reason_parts.append(f"temporal_month_{month:02d}_{year}")
                break

    # ── P6: number_phrase_scrub ───────────────────────────────────────────
    if "P6" in rules_to_apply:
        for sig in all_signals:
            if sig.kind == "number_phrase_scrub":
                tokens_to_scrub = sig.payload.get("tokens_to_scrub", [])
                objeto: list[str] = working_params.get("objeto", [])
                scrub_set = {x.lower().strip() for x in tokens_to_scrub}
                new_objeto = [t for t in objeto if t.lower().strip() not in scrub_set]
                working_params["objeto"] = new_objeto
                if not new_objeto:
                    risk_flags.append("topic_emptied_by_scrub")
                reason_parts.append("number_phrase_scrubbed")
                break

    # ── P7: discourse_filler ──────────────────────────────────────────────
    if "P7" in rules_to_apply:
        for sig in all_signals:
            if sig.kind == "discourse_filler":
                tokens_to_scrub = sig.payload.get("tokens_to_scrub", [])
                objeto: list[str] = working_params.get("objeto", [])
                scrub_set = {x.lower().strip() for x in tokens_to_scrub}
                new_objeto = [t for t in objeto if t.lower().strip() not in scrub_set]
                working_params["objeto"] = new_objeto
                reason_parts.append("filler_scrubbed")
                break

    # ── P8: invalid_value_range ───────────────────────────────────────────
    if "P8" in rules_to_apply:
        vmin = working_params.get("valor_min")
        vmax = working_params.get("valor_max")
        if vmin is not None and vmax is not None and vmin > vmax:
            risk_flags.append("invalid_range")
            return QueryPlan(
                should_execute=False,
                params=working_params,
                risk_flags=risk_flags,
                clarification_question=(
                    "El rango de valor que diste es inválido "
                    "(mínimo > máximo). ¿Cuál querías?"
                ),
                reason="invalid_value_range",
                signals=all_signals,
            )

    # ── Default: valid query ──────────────────────────────────────────────
    if not reason_parts:
        reason_parts.append("valid_query")

    return QueryPlan(
        should_execute=True,
        params=working_params,
        risk_flags=risk_flags,
        reason=";".join(reason_parts),
        signals=all_signals,
    )
