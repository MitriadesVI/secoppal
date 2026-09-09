"""Concise, source-labelled product response for Entity Analysis V1."""
from __future__ import annotations

from app.core.analytics.base import format_currency
from app.core.entity_profile.effective_scope import (
    build_effective_scope,
    scope_filter_lines,
)
from app.core.entity_profile.models import EntityProfile, RecentRecord, SupplierMetric
from app.core.entity_profile.source_request import RequestedSources
from app.core.entity_profile.supplier_rankings import section_ranking


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


def _contract_noun(profile: EntityProfile) -> str:
    """Choose generated labels from the executed plan before inserting source data."""
    plan = profile.plan
    if plan is not None:
        if hasattr(plan, 'source_plans'):
            from app.core.entity_profile.frame_values import SourceKind
            contract = plan.source_plans.get(SourceKind.CONTRACTS)
            records = contract and contract.predicates.contract_universe == 'source_records'
        else:
            contract = plan.plan_for('jbjy-vk9h')
            records = contract and contract.params.get('contract_universe') == 'source_records'
        if records:
            return 'registro'
    return 'contrato'


def _overview(profile: EntityProfile) -> list[str]:
    noun = _contract_noun(profile)
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
        sentence = f"• Se observan {int(contracts):,} {noun}s"
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
    if by_value and section_ranking(profile.suppliers, 'value').accredited:
        supplier = by_value[0]
        lines.append(
            f"• Principal contratista por valor: {supplier.observed_name} "
            f"({_money(supplier.total_contract_value)}). "
            f"[{_source('jbjy-vk9h')}]"
        )
    if by_count and section_ranking(profile.suppliers, 'count').accredited:
        supplier = by_count[0]
        lines.append(
            f"• Principal contratista por número de {noun}s: "
            f"{supplier.observed_name} ({_count(supplier.contract_count, noun + 's')}). "
            f"[{_source('jbjy-vk9h')}]"
        )

    recent_processes = profile.recent.fact("processes") or ()
    recent_contracts = profile.recent.fact("contracts") or ()
    if recent_processes:
        lines.append(
            f"• Proceso reciente: {recent_processes[0].title}. "
            f"[{_source('p6dx-8zbt')}]"
        )
    if recent_contracts:
        lines.append(
            f"• {noun.capitalize()} reciente: {recent_contracts[0].title}. "
            f"[{_source('jbjy-vk9h')}]"
        )
    return lines[:8]


def _supplier_lines(
    title: str, suppliers: tuple[SupplierMetric, ...], metric: str, *, accredited: bool, noun: str = "contrato",
) -> list[str]:
    if not suppliers:
        return []
    if not accredited:
        label = 'valor' if metric == 'value' else f'número de {noun}s'
        title = f"Proveedores observados; ranking por {label} no acreditado"
    lines = [title]
    for supplier in suppliers:
        value = (
            _count(supplier.contract_count, noun + "s")
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
        lines.append(f"• {row.title}{dated}")
    return lines


def _focused(profile: EntityProfile) -> list[str]:
    noun = _contract_noun(profile)
    if profile.focus == "suppliers":
        return [
            *_supplier_lines(
                f"Principales contratistas por número de {noun}s",
                profile.suppliers.fact("top_by_contract_count") or (),
                "count",
                accredited=section_ranking(profile.suppliers, 'count').accredited, noun=noun,
            ),
            *_supplier_lines(
                "Principales contratistas por valor contratado",
                profile.suppliers.fact("top_by_contract_value") or (),
                "value",
                accredited=section_ranking(profile.suppliers, 'value').accredited,
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
            lines.append(f"• Valor total reportado de {noun}s: {_money(total)}.")
        if average is not None:
            lines.append(f"• Valor promedio reportado por {noun}: {_money(average)}.")
        return lines
    if profile.focus == "recent":
        return [
            *_recent_lines("Procesos recientes", profile.recent.fact("processes") or ()),
            *_recent_lines(f"{noun.capitalize()}s recientes", profile.recent.fact("contracts") or ()),
        ]
    if profile.focus == "temporal":
        periods = profile.temporal.fact("periods") or ()
        lines = []
        for period in periods[-10:]:
            parts = []
            if period.process_count is not None:
                parts.append(f"{period.process_count:,} procesos publicados")
            if period.contract_count is not None:
                parts.append(_count(period.contract_count, noun + "s"))
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
        blocks.extend(_scope_lines(scope, _contract_noun(profile)))
    if profile.plan and 'jbjy-vk9h' in profile.plan.executed_datasets:
        from app.core.contract_universe import label, universe_from_params
        contract_params = profile.plan.plan_for('jbjy-vk9h').params
        blocks.append(label(universe_from_params(contract_params)))
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
        blocks.append(_next_steps(scope, _contract_noun(profile)))
    response = "\n\n".join(blocks)
    return response


def _scope_lines(scope, noun="contrato") -> list[str]:
    lines = list(scope.basis_lines)
    if not lines and scope.period != "todo el histórico disponible":
        lines.append(f"Periodo: {scope.period}.")
    # INV-FILTER-IN-SQL-IMPLIES-FILTER-DECLARED: todo filtro material que el
    # plan ejecutó se declara aquí, con su forma.
    lines.extend(scope_filter_lines(scope))
    if scope.narrowed:
        lines.append(
            "Esta vista sólo se puede responder sobre "
            f"{_SCOPE_NOUN[scope.effective_sources].replace('contratos', noun + 's')}."
        )
    return ["\n".join(lines)] if lines else []


def _next_steps(scope, noun="contrato") -> str:
    """No se ofrece profundizar en una fuente que este perfil no cubre."""
    if scope is not None and scope.effective_sources is RequestedSources.CONTRACTS:
        return f"Puedes pedirme sus proveedores, valores o {noun}s recientes."
    if scope is not None and scope.effective_sources is RequestedSources.PROCESSES:
        return "Puedes pedirme sus modalidades, evolución o procesos recientes."
    return (
        f"Puedes pedirme sus proveedores, modalidades, evolución o {noun}s recientes."
    )
