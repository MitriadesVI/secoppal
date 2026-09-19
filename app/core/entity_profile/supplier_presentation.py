"""Presentación adaptativa de SUPPLIERS: decir la operación que se ejecutó.

La vista V1 presentaba siempre las dos listas de cinco. V1.1 presenta lo que el
plan realmente ejecutó: la métrica pedida, el corte exacto que la autoridad
acreditó, el empate que ese corte reveló y el dato secundario sólo cuando
existe. La jerarquía del contrato §13 es el orden de las líneas — respuesta
pedida, luego el estado — y el alcance (comprador y periodo) ya viaja en el
encabezado del perfil.

Dos consecuencias deliberadas:

* En una métrica única no se emite el dato de la otra: ese ranking no se pidió
  a la fuente (§10), y presentarlo sería hablar de una llamada que no existió.
* Un empate en el corte del primer lugar NUNCA se presenta como un líder único
  (§12): se nombra el valor observado y el empate, sin enumerar un empate que
  puede ser ilimitado.
"""
from __future__ import annotations

from collections.abc import Callable

from app.core.entity_profile.models import EntityProfile
from app.core.entity_profile.supplier_ranking_request import (
    DEFAULT_SUPPLIER_REQUEST,
    SupplierRankingMetric,
    SupplierRankingRequest,
)
from app.core.entity_profile.supplier_rankings import (
    RankingAccreditation,
    RankingWindow,
    section_ranking,
    section_window,
)

_FACT_KEY = {
    "count": "top_by_contract_count",
    "value": "top_by_contract_value",
}
_RANKING_LABEL = {
    "count": "número de contratos",
    "value": "valor contratado",
}


def supplier_request_of(profile: EntityProfile) -> SupplierRankingRequest:
    """La petición que el plan ejecutó; sin plan (perfiles V1), la vista default."""
    request = getattr(profile.plan, "supplier_ranking", None)
    return request if isinstance(request, SupplierRankingRequest) else DEFAULT_SUPPLIER_REQUEST


def requested_rankings(request: SupplierRankingRequest) -> tuple[str, ...]:
    """Los rankings que la métrica ejecuta, en el orden estable del sistema."""
    return tuple(
        ranking for ranking in ("count", "value") if request.metric.asks(ranking)
    )


def supplier_lines(
    profile: EntityProfile,
    noun: str,
    *,
    money: Callable[[object], str],
    count: Callable[[object, str], str],
) -> list[str]:
    """Las líneas de hechos de la sección suppliers, ya adaptadas al request."""
    request = supplier_request_of(profile)
    lines: list[str] = []
    for ranking in requested_rankings(request):
        decision = section_ranking(profile.suppliers, ranking)
        window = section_window(profile.suppliers, ranking)
        visible = window.visible if window is not None else _fact_rows(profile, ranking)
        if request.limit == 1 and request.metric is not SupplierRankingMetric.BOTH:
            lines.extend(
                _single_lines(ranking, visible, window, decision, noun, money=money, count=count)
            )
        else:
            lines.extend(
                _listed_lines(ranking, visible, decision, request, noun, money=money, count=count)
            )
    lines.extend(_tie_lines(profile, request, noun, money=money, count=count))
    answered = [
        profile.suppliers.fact(_FACT_KEY[ranking])
        for ranking in requested_rankings(request)
    ]
    if answered and not any(answered) and all(
        value is not None for value in answered
    ):
        lines.append(f"• No encontré proveedores con {noun}s que cumplan estos filtros.")
    return lines


def supplier_view(
    profile: EntityProfile,
    noun: str,
    *,
    money: Callable[[object], str],
    count: Callable[[object, str], str],
) -> dict:
    """La proyección JSON de los rankings ejecutados, sin derivar métricas."""
    request = supplier_request_of(profile)
    rows: dict[str, list[dict]] = {}
    ties: list[dict] = []
    for ranking in requested_rankings(request):
        window = section_window(profile.suppliers, ranking)
        visible = window.visible if window is not None else _fact_rows(profile, ranking)
        if not visible:
            continue
        rows[ranking] = [
            {
                "name": supplier.observed_name,
                "counts": count(supplier.contract_count, f"{noun}s"),
                "value": money(supplier.total_contract_value),
            }
            for supplier in visible
        ]
        if window is not None and window.tied:
            ties.append({
                "ranking": ranking,
                "position": request.limit,
                "boundary": _boundary_display(ranking, window, money=money, count=count),
            })
    return {
        "metric": request.metric.value,
        "limit": request.limit,
        "rows": rows,
        "ties": ties,
    }


def _fact_rows(profile: EntityProfile, ranking: str) -> tuple:
    return tuple(profile.suppliers.fact(_FACT_KEY[ranking]) or ())


def _boundary_display(
    ranking: str, window: RankingWindow,
    *, money, count,
) -> str:
    if ranking == "count":
        return count(window.boundary_value, "contratos")
    return money(window.boundary_value)


def _single_lines(
    ranking, visible, window, decision, noun, *, money, count,
) -> list[str]:
    """El escalar del primer puesto, o el empate que impide afirmarlo."""
    title = (
        "Proveedor con más contratos" if ranking == "count"
        else "Proveedor con mayor valor contratado"
    )
    if not visible:
        return [title]
    if window is not None and window.tied:
        # §12: el mayor valor observado y el empate, sin nombrar un líder único.
        return [title, f"• {_tie_first_sentence(ranking, window, noun, money=money, count=count)}"]
    if not decision.accredited or (window is not None and window.uncertain):
        # Sin cifra acreditable en el borde no se afirma un primer puesto; la
        # advertencia de la sección ya dice por qué.
        return [
            *_listed_lines(
                ranking, visible, decision,
                _single_request(ranking), noun, money=money, count=count,
            ),
        ]
    supplier = visible[0]
    primary = (
        count(supplier.contract_count, f"{noun}s") if ranking == "count"
        else money(supplier.total_contract_value)
    )
    return [title, f"• {supplier.observed_name}: {primary}"]


def _single_request(ranking: str) -> SupplierRankingRequest:
    return SupplierRankingRequest(SupplierRankingMetric(ranking), 1)


def _listed_lines(
    ranking, visible, decision: RankingAccreditation, request, noun, *,
    money, count,
) -> list[str]:
    if request.metric is SupplierRankingMetric.BOTH:
        title = (
            f"Principales contratistas por número de {noun}s" if ranking == "count"
            else "Principales contratistas por valor contratado"
        )
    else:
        title = (
            f"Proveedores con más contratos (top {request.limit})" if ranking == "count"
            else f"Proveedores con mayor valor contratado (top {request.limit})"
        )
    if not decision.accredited:
        label = "valor" if ranking == "value" else f"número de {noun}s"
        title = f"Proveedores observados; ranking por {label} no acreditado"
    lines = [title]
    for index, supplier in enumerate(visible, start=1):
        primary = (
            count(supplier.contract_count, f"{noun}s") if ranking == "count"
            else money(supplier.total_contract_value)
        )
        if request.metric is SupplierRankingMetric.BOTH:
            lines.append(f"• {supplier.observed_name}: {primary}")
        else:
            lines.append(f"{index}. {supplier.observed_name}: {primary}")
    return lines


def _tie_first_sentence(ranking, window, noun, *, money, count) -> str:
    observed = _boundary_display(ranking, window, money=money, count=count)
    if ranking == "count":
        return (
            f"El mayor número observado es {observed} y existe empate en el "
            "primer lugar."
        )
    return (
        f"El mayor valor observado es {observed} y existe empate en el primer lugar."
    )


def _tie_lines(profile, request, noun, *, money, count) -> list[str]:
    """El empate en el corte, declarado sin decidir un orden que la fuente no dio."""
    lines: list[str] = []
    for ranking in requested_rankings(request):
        if request.limit == 1 and request.metric is not SupplierRankingMetric.BOTH:
            continue
        window = section_window(profile.suppliers, ranking)
        if window is None or not window.tied:
            continue
        lines.append(
            f"• Empate en el corte del top {request.limit} por "
            f"{_RANKING_LABEL[ranking]}: el orden no decide un puesto único."
        )
    return lines
