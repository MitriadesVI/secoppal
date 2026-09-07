"""El alcance visible se deriva del plan compilado, no de la petición.

`INV-RENDERED-SCOPE-EQUALS-EXECUTED-SCOPE`, esta vez como consecuencia
estructural y no como afirmación: las líneas salen de los mismos
`SourcePredicates` de los que salió el WHERE, así que un filtro no puede estar
en el SQL y faltar en el alcance.

Lo que esto cierra del hallazgo F4:

* AND y OR se leen distinto porque el árbol conserva su forma.
* Una exclusión de modalidad aparece como exclusión, no desaparece.
* Dos fuentes con periodos distintos se rotulan por separado; no se presenta
  un encabezado común que sugiera una ventana compartida que no existe.
* Un filtro de valor sobre dos fuentes nombra sus dos magnitudes: el
  presupuesto de un proceso y el valor de un contrato no son la misma cifra.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.core.entity_profile.frame_compiler import ProfilePlan
from app.core.entity_profile.frame_values import (
    Period,
    Selection,
    SourceKind,
    StateExpression,
    TopicAll,
    TopicAny,
    TopicNode,
    TopicNot,
    TopicPhrase,
    TopicTerm,
    TopicTrue,
    ValueConstraint,
)


_MONTHS = (
    "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
    "agosto", "septiembre", "octubre", "noviembre", "diciembre",
)

_EVENT_VERB = {
    SourceKind.PROCESSES: "publicación",
    SourceKind.CONTRACTS: "firma",
}

_LAST_DAY = {1: 31, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31,
             11: 30, 12: 31}


@dataclass(frozen=True, slots=True)
class SourceScope:
    """El alcance material de UNA fuente, en el orden en que se declara."""

    source: SourceKind
    period: str
    basis: str
    lines: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CompiledScope:
    buyer: str
    focus: str
    planned_sources: tuple[SourceKind, ...]
    sources: tuple[SourceScope, ...]
    limitations: tuple[str, ...] = ()

    @property
    def shares_one_period(self) -> bool:
        periods = {scope.period for scope in self.sources}
        return len(periods) == 1


def build_compiled_scope(plan: ProfilePlan) -> CompiledScope:
    """Plan -> alcance visible. No se lee texto, params ni scope legacy."""
    planned = tuple(s for s in SourceKind if s in plan.planned_sources)
    scopes = tuple(
        SourceScope(
            source=source,
            period=humanize_period(
                plan.source_plans[source].predicates.temporal.period,
            ),
            basis=_EVENT_VERB[source],
            lines=_filter_lines(
                plan.source_plans[source].predicates,
                name_magnitude=_two_magnitudes(plan, planned),
            ),
        )
        for source in planned
    )
    buyer = ""
    if plan.source_plans:
        buyer = next(iter(plan.source_plans.values())).where
    return CompiledScope(
        buyer=buyer, focus=plan.focus, planned_sources=planned, sources=scopes,
        limitations=plan.limitations,
    )


def render_compiled_scope(scope: CompiledScope) -> tuple[str, ...]:
    """Una línea por hecho declarado del universo consultado."""
    lines: list[str] = [
        "Fuentes: " + ", ".join(s.noun for s in scope.planned_sources) + ".",
    ]
    for source_scope in scope.sources:
        prefix = (
            "" if len(scope.sources) == 1
            else f"{source_scope.source.noun.capitalize()} — "
        )
        lines.append(
            f"{prefix}{source_scope.period}, según fecha de "
            f"{source_scope.basis}.",
        )
        lines.extend(f"{prefix}{line}" for line in source_scope.lines)
    lines.extend(scope.limitations)
    return tuple(lines)


def humanize_period(period: Period) -> str:
    """Un mes se dice como mes; nunca se reduce a su año."""
    if period.is_unbounded:
        return "Todo el histórico disponible"
    start, end = _parts(period.lower), _parts(period.upper)
    if start and end and start[0] == end[0]:
        if start[1:] == (1, 1) and end[1:] == (12, 31):
            return str(start[0])
        if start[1] == end[1] and start[2] == 1 and end[2] == _last_day(*end[:2]):
            return f"{_MONTHS[end[1] - 1].capitalize()} de {end[0]}"
    if period.lower and period.upper:
        return f"Del {period.lower} al {period.upper}"
    if period.lower:
        return f"Desde {period.lower}"
    return f"Hasta {period.upper}"


def describe_topic(node: TopicNode) -> str | None:
    """AND y OR no pueden leerse igual; una exclusión se nombra exclusión."""
    if isinstance(node, TopicTrue):
        return None
    if isinstance(node, TopicTerm):
        return node.term
    if isinstance(node, TopicPhrase):
        return f"«{node.canonical}»"
    if isinstance(node, TopicAll):
        parts = [describe_topic(child) for child in node.children]
        return " y ".join(p for p in parts if p) or None
    if isinstance(node, TopicAny):
        parts = [describe_topic(child) for child in node.children]
        joined = " o ".join(p for p in parts if p)
        return f"({joined})" if joined else None
    if isinstance(node, TopicNot):
        inner = describe_topic(node.operand)
        return f"excepto {inner}" if inner else None
    return None


def _filter_lines(predicates, *, name_magnitude: bool) -> tuple[str, ...]:
    lines: list[str] = []
    topic = describe_topic(predicates.topic)
    if topic:
        lines.append(f"Tema: {topic}.")
    state = _state_line(predicates.current_state)
    if state:
        lines.append(state)
    modality = _selection_line("Modalidad", predicates.modality, labels=True)
    if modality:
        lines.append(modality)
    contract_type = _selection_line(
        "Tipo de contrato", predicates.contract_type, labels=False,
    )
    if contract_type:
        lines.append(contract_type)
    value = _value_line(predicates.value, name_magnitude=name_magnitude)
    if value:
        lines.append(value)
    return tuple(lines)


def _state_line(state: StateExpression) -> str:
    if state.is_true:
        return ""
    described = "; ".join(
        f"{p.field}: {', '.join(p.values)}" for p in state.predicates
    )
    return f"Estado actual: {described}."


def _selection_line(label: str, selection: Selection, *, labels: bool) -> str:
    if selection.is_empty:
        return ""
    parts: list[str] = []
    if selection.include:
        parts.append(", ".join(_labels(selection.include) if labels
                               else selection.include))
    if selection.exclude:
        excluded = _labels(selection.exclude) if labels else selection.exclude
        parts.append("excepto " + ", ".join(excluded))
    return f"{label}: {'; '.join(parts)}."


def _value_line(value: ValueConstraint, *, name_magnitude: bool) -> str:
    if value.is_unbounded:
        return ""
    if value.lower is not None and value.upper is not None:
        span = f"entre {_money(value.lower)} y {_money(value.upper)}"
    elif value.lower is not None:
        span = f"desde {_money(value.lower)}"
    else:
        span = f"hasta {_money(value.upper)}"
    label = value.magnitude.capitalize() if name_magnitude else "Valor"
    return f"{label}: {span}."


def _labels(ids: tuple[str, ...]) -> tuple[str, ...]:
    """Los nombres visibles los publica el catálogo de modalidad, no este módulo."""
    try:
        from app.core.modality_models import load_catalog

        catalog = load_catalog()
    except Exception:
        return ids
    resolved = []
    for procedure_id in ids:
        classification = catalog.by_id(procedure_id)
        resolved.append(
            getattr(classification, "display_name", None) or procedure_id,
        )
    return tuple(dict.fromkeys(resolved))


def _two_magnitudes(plan: ProfilePlan, planned: tuple[SourceKind, ...]) -> bool:
    bounded = [
        source for source in planned
        if not plan.source_plans[source].predicates.value.is_unbounded
    ]
    return len(bounded) > 1


def _money(amount: Decimal) -> str:
    from app.core.analytics.base import format_currency

    return f"${format_currency(float(amount))}"


def _parts(value: str | None) -> tuple[int, int, int] | None:
    try:
        year, month, day = (int(part) for part in value.split("-"))
    except (AttributeError, ValueError):
        return None
    return year, month, day


def _last_day(year: int, month: int) -> int:
    if month == 2:
        leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        return 29 if leap else 28
    return _LAST_DAY.get(month, 31)
