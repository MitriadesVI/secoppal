"""JSON-safe Streamlit projection of already-accredited Entity Profile facts."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.contract_universe import label, universe_from_params
from app.core.entity_profile.effective_scope import (
    build_effective_scope,
    scope_filter_lines,
)
from app.core.entity_profile.frame_render import (
    build_compiled_scope,
    render_compiled_scope,
)
from app.core.entity_profile.models import (
    CONTRACT_DATASET,
    PROCESS_DATASET,
    EntityProfile,
    RecentRecord,
    SupplierMetric,
)
from app.core.entity_profile.render import _contract_noun, _money, profile_next_steps
from app.core.entity_profile.supplier_rankings import section_ranking


ENTITY_PROFILE_VIEW_KEY = "entity_profile_view"
ENTITY_PROFILE_VIEW_KIND = "entity_profile"
ENTITY_PROFILE_VIEW_SCHEMA = 1
_TITLE_LIMIT = 120
_SOURCE_LABELS = {
    PROCESS_DATASET: "SECOP II Procesos",
    CONTRACT_DATASET: "SECOP II Contratos",
}


def project_entity_profile(profile: EntityProfile) -> dict:
    """Project display fields without parsing text or deriving new metrics."""
    noun = _contract_noun(profile)
    period, scope_lines = _scope(profile)
    return {
        "kind": ENTITY_PROFILE_VIEW_KIND,
        "schema_version": ENTITY_PROFILE_VIEW_SCHEMA,
        "focus": profile.focus,
        "header": {
            "display_name": profile.identity.display_name,
            "period": period,
            "territory": _territory(profile),
        },
        "kpis": [
            _count_slot("process_count", "Procesos publicados", profile.activity.fact("process_count")),
            _count_slot("contract_count", noun.capitalize() + "s", profile.activity.fact("contract_count")),
            _money_slot(
                "total_contract_value",
                "Valor contratado" if noun == "contrato" else "Valor de registros",
                profile.values.fact("total_contract_value"),
            ),
            _money_slot("average_contract_value", f"Promedio por {noun}", profile.values.fact("average_contract_value")),
        ],
        "modalities": _modalities(profile),
        "suppliers": {
            "by_value": _supplier(profile, "value", noun),
            "by_count": _supplier(profile, "count", noun),
        },
        "recent": {
            "process": _recent(profile.recent.fact("processes")),
            "contract": _recent(profile.recent.fact("contracts")),
            "contract_label": noun.capitalize() + " reciente",
        },
        "sources": _sources(profile),
        "universe": _universe(profile, noun),
        "scope": scope_lines,
        "warnings": list(dict.fromkeys(profile.warnings)),
        "next_steps": profile_next_steps(profile),
    }


def _valid_count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _count_slot(key: str, label_text: str, value: object) -> dict:
    if not _valid_count(value):
        return {"key": key, "label": label_text, "status": "unavailable", "display": "No disponible"}
    return {"key": key, "label": label_text, "status": "available", "display": f"{value:,}"}


def _valid_money(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float, Decimal)):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return False
    return numeric == numeric and numeric not in (float("inf"), float("-inf"))


def _money_slot(key: str, label_text: str, value: object) -> dict:
    if not _valid_money(value):
        return {"key": key, "label": label_text, "status": "unavailable", "display": "No disponible"}
    return {"key": key, "label": label_text, "status": "available", "display": _money(value)}


def _modalities(profile: EntityProfile) -> list[dict]:
    rows = profile.modalities.fact("top_modalities")
    if not isinstance(rows, (tuple, list)):
        return []
    projected = []
    for row in rows:
        if not isinstance(row, (tuple, list)) or len(row) != 3:
            continue
        name, count, share = row
        if not isinstance(name, str) or not _valid_count(count):
            continue
        try:
            numeric_share = float(share)
        except (TypeError, ValueError, OverflowError):
            numeric_share = float("nan")
        share_display = (
            f"{numeric_share:.0%}"
            if numeric_share == numeric_share
            and numeric_share not in (float("inf"), float("-inf"))
            and 0 <= numeric_share <= 1
            else "No disponible"
        )
        projected.append({"name": name, "count": f"{count:,}", "share": share_display})
    return projected


def _supplier(profile: EntityProfile, ranking: str, noun: str) -> dict | None:
    decision = section_ranking(profile.suppliers, ranking)
    if not decision.accredited:
        return None
    key = "top_by_contract_value" if ranking == "value" else "top_by_contract_count"
    rows = profile.suppliers.fact(key)
    if not isinstance(rows, (tuple, list)) or not rows:
        return None
    supplier = rows[0]
    if not isinstance(supplier, SupplierMetric) or not supplier.observed_name:
        return None
    value = (
        _money(supplier.total_contract_value)
        if ranking == "value"
        else f"{supplier.contract_count:,} {noun}s"
    )
    return {"name": supplier.observed_name, "value": value}


def _recent(rows: object) -> dict | None:
    if not isinstance(rows, (tuple, list)) or not rows or not isinstance(rows[0], RecentRecord):
        return None
    row = rows[0]
    display_title = row.title.strip()
    excerpt = (
        display_title
        if len(display_title) <= _TITLE_LIMIT
        else display_title[: _TITLE_LIMIT - 1].rstrip() + "…"
    )
    return {"excerpt": excerpt, "full_title": row.title, "date": row.date, "url": row.url}


def _territory(profile: EntityProfile) -> str | None:
    if not profile.scope.territory_context or profile.scope.buyer_group_id is not None:
        return None
    territory = profile.scope.territory_context[0]
    return f"{territory.municipality.title()}, {territory.department.title()}"


def _scope(profile: EntityProfile) -> tuple[str | None, list[str]]:
    plan = profile.plan
    if plan is not None and hasattr(plan, "source_plans"):
        compiled = build_compiled_scope(plan)
        period = compiled.sources[0].period if compiled.shares_one_period and compiled.sources else None
        return period, [
            line for line in render_compiled_scope(compiled)
            if not line.startswith("Fuentes: ")
        ]
    effective = build_effective_scope(profile)
    if effective is None:
        return None, []
    lines = list(effective.basis_lines)
    lines.extend(scope_filter_lines(effective))
    return effective.period, lines


def _datasets(profile: EntityProfile) -> tuple[str, ...]:
    plan = profile.plan
    if plan is None:
        return ()
    if hasattr(plan, "source_plans"):
        available = {source.dataset_id for source in plan.planned_sources}
    else:
        available = set(plan.executed_datasets)
    return tuple(
        dataset_id for dataset_id in (PROCESS_DATASET, CONTRACT_DATASET)
        if dataset_id in available
    )


def _sources(profile: EntityProfile) -> list[dict]:
    return [
        {"id": dataset_id, "label": _SOURCE_LABELS[dataset_id]}
        for dataset_id in dict.fromkeys(_datasets(profile))
        if dataset_id in _SOURCE_LABELS
    ]


def _universe(profile: EntityProfile, noun: str) -> dict | None:
    if CONTRACT_DATASET not in _datasets(profile):
        return None
    plan = profile.plan
    if hasattr(plan, "source_plans"):
        from app.core.entity_profile.frame_values import SourceKind

        universe = plan.source_plans[SourceKind.CONTRACTS].predicates.contract_universe
        text = label(universe)
    else:
        text = label(universe_from_params(plan.plan_for(CONTRACT_DATASET).params))
    return {"noun": noun, "label": text}


__all__ = [
    "ENTITY_PROFILE_VIEW_KEY",
    "ENTITY_PROFILE_VIEW_KIND",
    "ENTITY_PROFILE_VIEW_SCHEMA",
    "project_entity_profile",
]
