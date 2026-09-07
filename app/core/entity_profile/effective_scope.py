"""Lo que REALMENTE se ejecutó, en la forma en que el render puede decirlo.

INV-RENDERED-SCOPE-EQUALS-EXECUTED-SCOPE.

El render describía el análisis a partir del scope y del foco, que son lo que
se pidió, no lo que se hizo. Así un perfil de contratos podía titularse con un
periodo anual porque la sección temporal no se había ejecutado, y hablar de
procesos que nadie consultó.

Esta instantánea se deriva del plan de ejecución y es la ÚNICA fuente del
bloque "qué analizamos". Si el plan no consultó procesos, aquí no hay procesos
que contar.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.entity_profile.material_filters import MaterialFilters, filters_for
from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET
from app.core.entity_profile.source_request import RequestedSources


_MONTHS = (
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
)

_SOURCE_TITLE = {
    RequestedSources.PROCESSES: "SECOP II Procesos",
    RequestedSources.CONTRACTS: "SECOP II Contratos",
    RequestedSources.BOTH: "actividad contractual",
}

_SOURCE_NOUN = {
    PROCESS_DATASET: "procesos",
    CONTRACT_DATASET: "contratos",
}

_EVENT_VERB = {
    PROCESS_DATASET: "Publicados",
    CONTRACT_DATASET: "Firmados",
}

_LAST_DAY = {1: 31, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31,
             11: 30, 12: 31}


@dataclass(frozen=True, slots=True)
class EffectiveScopeSnapshot:
    buyer: str
    requested_sources: RequestedSources
    effective_sources: RequestedSources | None
    period: str
    basis_lines: tuple[str, ...] = ()
    # Los filtros EJECUTADOS por cada fuente, leídos de los params que el
    # builder convirtió en WHERE. No se copian de la petición: copiarla fue
    # exactamente lo que dejó fuera estado, exclusiones, modalidad, tipo y
    # valor, y lo que hizo indistinguibles AND y OR.
    filters: tuple[MaterialFilters, ...] = ()

    @property
    def topic(self) -> tuple[str, ...]:
        """Compatibilidad: vista plana del tema de la primera fuente."""
        return self.filters[0].topic.flat_terms if self.filters else ()

    @property
    def declares_two_magnitudes(self) -> bool:
        """Dos fuentes con filtro de valor miden cosas distintas."""
        with_value = [f for f in self.filters if f.has_value_filter]
        return len(with_value) > 1

    @property
    def title(self) -> str:
        label = _SOURCE_TITLE.get(self.effective_sources)
        if label is None:
            return f"{self.buyer.upper()} — sin fuentes consultadas"
        return f"{self.buyer.upper()} — {label}"

    @property
    def narrowed(self) -> bool:
        """El foco acotó la fuente: hay que decirlo, no ocultarlo."""
        return (
            self.effective_sources is not None
            and self.effective_sources is not self.requested_sources
        )


def _parts(value: str | None) -> tuple[int, int, int] | None:
    try:
        year, month, day = (int(part) for part in value.split("-"))
    except (AttributeError, ValueError):
        return None
    return year, month, day


def humanize_period(time_from: str | None, time_to: str | None) -> str:
    """Un mes se dice como mes; nunca se reduce a su año."""
    start, end = _parts(time_from), _parts(time_to)
    if start and end and start[0] == end[0]:
        if start[1:] == (1, 1) and end[1:] == (12, 31):
            return str(start[0])
        if start[1] == end[1] and start[2] == 1 and end[2] == _last_day(*end[:2]):
            return f"del 1 al {end[2]} de {_MONTHS[end[1] - 1]} de {end[0]}"
    if time_from and time_to:
        return f"del {time_from} al {time_to}"
    if time_from:
        return f"desde {time_from}"
    if time_to:
        return f"hasta {time_to}"
    return "todo el histórico disponible"


def _last_day(year: int, month: int) -> int:
    if month == 2:
        leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        return 29 if leap else 28
    return _LAST_DAY.get(month, 31)


def _basis_lines(
    effective: RequestedSources | None, period: str, has_period: bool,
) -> tuple[str, ...]:
    if effective is None or not has_period:
        return ()
    if effective is RequestedSources.BOTH:
        return (
            f"Procesos: {period}, según fecha de publicación.",
            f"Contratos: {period}, según fecha de firma.",
        )
    dataset = (
        PROCESS_DATASET
        if effective is RequestedSources.PROCESSES
        else CONTRACT_DATASET
    )
    return (f"{_EVENT_VERB[dataset]} {period}.",)


def build_effective_scope(profile: object) -> EffectiveScopeSnapshot | None:
    """Deriva la instantánea del plan; sin plan no se afirma ningún alcance."""
    plan = getattr(profile, "plan", None)
    if plan is None:
        return None
    request = plan.request
    period = humanize_period(request.time_from, request.time_to)
    executed = plan.executed_datasets
    filters = tuple(
        filters_for(plan.plan_for(dataset_id))
        for dataset_id in (PROCESS_DATASET, CONTRACT_DATASET)
        if dataset_id in executed
    )
    return EffectiveScopeSnapshot(
        buyer=profile.identity.display_name,
        requested_sources=request.sources,
        effective_sources=plan.effective_sources,
        period=period,
        basis_lines=_basis_lines(
            plan.effective_sources, period, request.has_period,
        ),
        filters=filters,
    )


def scope_filter_lines(snapshot: EffectiveScopeSnapshot) -> tuple[str, ...]:
    """Cada filtro material declarado una vez, nombrando su fuente si difiere.

    Con dos fuentes, un filtro idéntico se declara una sola vez; uno que difiere
    —el valor, que mide presupuesto en procesos y valor pactado en contratos—
    se declara por fuente para no presentar dos magnitudes bajo una etiqueta.
    """
    if not snapshot.filters:
        return ()
    name_magnitude = snapshot.declares_two_magnitudes
    described = [
        (filters.dataset_id, filters.describe(name_magnitude=name_magnitude))
        for filters in snapshot.filters
    ]
    if len(described) == 1:
        return described[0][1]
    shared = [line for line in described[0][1] if all(
        line in lines for _, lines in described[1:]
    )]
    lines = list(shared)
    for dataset_id, source_lines in described:
        for line in source_lines:
            if line not in shared:
                lines.append(f"{_SOURCE_NOUN[dataset_id].capitalize()} — {line}")
    return tuple(lines)
