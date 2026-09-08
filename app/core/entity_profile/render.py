"""Concise, source-labelled product response for Entity Analysis V1."""
from __future__ import annotations

from app.core.analytics.base import format_currency
from app.core.entity_profile.effective_scope import (
    build_effective_scope,
    scope_filter_lines,
)
from app.core.entity_profile.models import EntityProfile, RecentRecord, SupplierMetric
from app.core.entity_profile.source_request import RequestedSources


_SOURCES = {
    "p6dx-8zbt": "SECOP II Procesos",
    "jbjy-vk9h": "SECOP II Contratos",
}

_SCOPE_NOUN = {
    RequestedSources.PROCESSES: "procesos",
    RequestedSources.CONTRACTS: "contratos",
    RequestedSources.BOTH: "procesos y contratos",
}


def _money(value: object) -> str:
    try:
        return f"${format_currency(float(value))}"
    except (TypeError, ValueError):
        return "valor no disponible"


def _count(value: object, noun: str) -> str:
    """Un conteo que la fuente no acreditó se dice, no se imprime como 0."""
    if not isinstance(value, int) or isinstance(value, bool):
        return f"número de {noun} no disponible"
    return f"{value:,} {noun}"


def _period(profile: EntityProfile) -> str:
    period = profile.temporal.fact("period")
    if isinstance(period, str):
        return period
    scope = profile.scope
    if scope.time_from and scope.time_to and scope.time_from[:4] == scope.time_to[:4]:
        return scope.time_from[:4]
    return "histórico disponible"


def _source(dataset_id: str) -> str:
    return _SOURCES.get(dataset_id, dataset_id)


def _overview(profile: EntityProfile) -> list[str]:
    lines: list[str] = []
    processes = profile.activity.fact("process_count")
    contracts = profile.activity.fact("contract_count")
    total = profile.values.fact("total_contract_value")
    average = profile.values.fact("average_contract_value")
    if processes is not None:
        lines.append(
            f"• Publicó {int(processes):,} procesos en el periodo. "
            f"[{_source('p6dx-8zbt')}]"
        )
    if contracts is not None:
        sentence = f"• Se observan {int(contracts):,} contratos"
        if total is not None:
            sentence += f" por {_money(total)}"
        if average is not None:
            sentence += f"; promedio {_money(average)}"
        lines.append(f"{sentence}. [{_source('jbjy-vk9h')}]")

    modalities = profile.modalities.fact("top_modalities") or ()
    if modalities:
        rendered = ", ".join(
            f"{name} ({share:.0%})" for name, _, share in modalities
        )
        lines.append(
            f"• Sus modalidades de proceso más frecuentes fueron {rendered}. "
            f"[{_source('p6dx-8zbt')}]"
        )

    by_value = profile.suppliers.fact("top_by_contract_value") or ()
    by_count = profile.suppliers.fact("top_by_contract_count") or ()
    if by_value and all(item.total_contract_value is not None for item in by_value):
        supplier = by_value[0]
        lines.append(
            f"• Principal contratista por valor: {supplier.observed_name} "
            f"({_money(supplier.total_contract_value)}). "
            f"[{_source('jbjy-vk9h')}]"
        )
    if by_count:
        supplier = by_count[0]
        lines.append(
            f"• Principal contratista por número de contratos: "
            f"{supplier.observed_name} ({_count(supplier.contract_count, 'contratos')}). "
            f"[{_source('jbjy-vk9h')}]"
        )

    recent_processes = profile.recent.fact("processes") or ()
    recent_contracts = profile.recent.fact("contracts") or ()
    if recent_processes:
        lines.append(
            f"• Proceso reciente: {_short(recent_processes[0].title)}. "
            f"[{_source('p6dx-8zbt')}]"
        )
    if recent_contracts:
        lines.append(
            f"• Contrato reciente: {_short(recent_contracts[0].title)}. "
            f"[{_source('jbjy-vk9h')}]"
        )
    return lines[:8]


def _supplier_lines(title: str, suppliers: tuple[SupplierMetric, ...], metric: str) -> list[str]:
    if not suppliers:
        return []
    if metric == "value" and any(item.total_contract_value is None for item in suppliers):
        title = "Proveedores observados; ranking por valor no acreditado"
    lines = [title]
    for supplier in suppliers:
        value = (
            _count(supplier.contract_count, "contratos")
            if metric == "count"
            else _money(supplier.total_contract_value)
        )
        lines.append(f"• {supplier.observed_name}: {value}")
    return lines


def _recent_lines(title: str, rows: tuple[RecentRecord, ...]) -> list[str]:
    if not rows:
        return []
    lines = [title]
    for row in rows:
        dated = f" ({row.date})" if row.date else ""
        lines.append(f"• {_short(row.title)}{dated}")
    return lines


def _focused(profile: EntityProfile) -> list[str]:
    if profile.focus == "suppliers":
        return [
            *_supplier_lines(
                "Principales contratistas por número de contratos",
                profile.suppliers.fact("top_by_contract_count") or (),
                "count",
            ),
            *_supplier_lines(
                "Principales contratistas por valor contratado",
                profile.suppliers.fact("top_by_contract_value") or (),
                "value",
            ),
        ]
    if profile.focus == "modalities":
        modalities = profile.modalities.fact("top_modalities") or ()
        return [
            f"• {name}: {count:,} procesos ({share:.1%})."
            for name, count, share in modalities
        ]
    if profile.focus == "values":
        total = profile.values.fact("total_contract_value")
        average = profile.values.fact("average_contract_value")
        lines = []
        if total is not None:
            lines.append(f"• Valor total reportado de contratos: {_money(total)}.")
        if average is not None:
            lines.append(f"• Valor promedio reportado por contrato: {_money(average)}.")
        return lines
    if profile.focus == "recent":
        return [
            *_recent_lines("Procesos recientes", profile.recent.fact("processes") or ()),
            *_recent_lines("Contratos recientes", profile.recent.fact("contracts") or ()),
        ]
    if profile.focus == "temporal":
        periods = profile.temporal.fact("periods") or ()
        lines = []
        for period in periods[-10:]:
            parts = []
            if period.process_count is not None:
                parts.append(f"{period.process_count:,} procesos publicados")
            if period.contract_count is not None:
                parts.append(_count(period.contract_count, "contratos"))
            if period.contract_value is not None:
                parts.append(_money(period.contract_value))
            lines.append(f"• {period.period}: {', '.join(parts)}.")
        return lines
    return _overview(profile)


def profile_fact_lines(profile: EntityProfile) -> list[str]:
    """Los HECHOS de un perfil, sin encabezado ni alcance.

    V1.2-B compone su respuesta desde el plan compilado y no desde el scope
    legacy, pero los hechos se dicen igual: la forma de una línea de proveedor,
    de modalidad o de monto es la misma decisión de producto, y duplicarla
    sería tener dos maneras de contar lo mismo. Esto es exactamente lo que
    `render_entity_profile` ya usaba; sólo deja de ser privado.
    """
    return _overview(profile) if profile.focus == "overview" else _focused(profile)


def _short(text: str, limit: int = 90) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else f"{text[:limit - 3]}..."


def render_entity_profile(profile: EntityProfile) -> str:
    """Render supported facts and name failed aspects without turning them into zero.

    El bloque de alcance sale del plan EJECUTADO, nunca del foco ni del scope
    pedido: es lo que hace que el texto no pueda hablar de una fuente que no se
    consultó (INV-RENDERED-SCOPE-EQUALS-EXECUTED-SCOPE).
    """
    scope = build_effective_scope(profile)
    blocks: list[str] = [
        scope.title if scope else
        f"{profile.identity.display_name.upper()} — {_period(profile)}"
    ]
    if scope:
        blocks.extend(_scope_lines(scope))
    if profile.scope.territory_context and profile.scope.buyer_group_id is None:
        territory = profile.scope.territory_context[0]
        blocks.append(
            f"Territorio: {territory.municipality.title()}, "
            f"{territory.department.title()} (DIVIPOLA {territory.divipola})."
        )
    facts = _overview(profile) if profile.focus == "overview" else _focused(profile)
    if facts:
        blocks.append("\n".join(facts))
    blocks.extend(profile.warnings)
    if profile.focus == "overview":
        blocks.append(_next_steps(scope))
    return "\n\n".join(blocks)


def _scope_lines(scope) -> list[str]:
    lines = list(scope.basis_lines)
    if not lines and scope.period != "todo el histórico disponible":
        lines.append(f"Periodo: {scope.period}.")
    # INV-FILTER-IN-SQL-IMPLIES-FILTER-DECLARED: todo filtro material que el
    # plan ejecutó se declara aquí, con su forma.
    lines.extend(scope_filter_lines(scope))
    if scope.narrowed:
        lines.append(
            "Esta vista sólo se puede responder sobre "
            f"{_SCOPE_NOUN[scope.effective_sources]}."
        )
    return ["\n".join(lines)] if lines else []


def _next_steps(scope) -> str:
    """No se ofrece profundizar en una fuente que este perfil no cubre."""
    if scope is not None and scope.effective_sources is RequestedSources.CONTRACTS:
        return "Puedes pedirme sus proveedores, valores o contratos recientes."
    if scope is not None and scope.effective_sources is RequestedSources.PROCESSES:
        return "Puedes pedirme sus modalidades, evolución o procesos recientes."
    return (
        "Puedes pedirme sus proveedores, modalidades, evolución o contratos recientes."
    )
