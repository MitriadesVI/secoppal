"""One accreditation decision per supplier query; never join or reorder TOPs."""
from dataclasses import dataclass

from app.core.entity_profile.models import EntityProfileSection, SupplierMetric


@dataclass(frozen=True, slots=True)
class RankingAccreditation:
    available: bool
    empty: bool
    counts_valid: bool
    amounts_valid: bool
    accredited: bool


@dataclass(frozen=True, slots=True)
class RankingWindow:
    """El corte VISIBLE de un ranking, con su testigo de empate aparte.

    El SQL pide ``limit + 1`` filas y la ejecución las trae todas: la fila
    extra nunca es un resultado (§12). Sólo decide si el corte cae dentro de un
    empate — el valor del último visible igual al del siguiente. Sin ella, un
    empate real se presentaría como un liderazgo único.
    """

    ranking: str
    visible: tuple[SupplierMetric, ...]
    extra: SupplierMetric | None
    boundary_value: float | int | None

    @property
    def tied(self) -> bool:
        """¿El corte cae dentro de un empate, con valores comparables?"""
        if self.extra is None or self.boundary_value is None:
            return False
        return ranking_value(self.extra, self.ranking) == self.boundary_value

    @property
    def uncertain(self) -> bool:
        """El borde no es comparable: ni unicidad ni empate se pueden afirmar."""
        return self.extra is not None and (
            self.boundary_value is None
            or ranking_value(self.extra, self.ranking) is None
        )


def ranking_value(supplier: SupplierMetric, ranking: str) -> float | int | None:
    """La cifra que ordena ese ranking, tal como quedó acreditada (o None)."""
    if ranking == "count":
        return supplier.contract_count
    return supplier.total_contract_value


def window_ranking(
    suppliers: tuple[SupplierMetric, ...], limit: int, *, ranking: str,
) -> RankingWindow:
    """Corta el ranking a ``limit`` y separa la fila testigo.

    El corte se hace con el entero que el request ya validó; aquí se vuelve a
    comprobar porque esta función también la usa la ruta legacy, que no lleva
    request.
    """
    if ranking not in ("count", "value"):
        raise ValueError("UNKNOWN_SUPPLIER_RANKING")
    if isinstance(limit, bool) or type(limit) is not int or limit < 1:
        raise ValueError("SUPPLIER_LIMIT_OUT_OF_RANGE")
    visible = tuple(suppliers[:limit])
    extra = suppliers[limit] if len(suppliers) > limit else None
    boundary = ranking_value(visible[-1], ranking) if visible else None
    return RankingWindow(ranking, visible, extra, boundary)


def accredit_ranking(
    ranking: str, suppliers: tuple[SupplierMetric, ...], *, available: bool,
) -> RankingAccreditation:
    counts = available and all(item.contract_count is not None for item in suppliers)
    amounts = available and all(item.total_contract_value is not None for item in suppliers)
    return RankingAccreditation(
        available, available and not suppliers, counts, amounts,
        counts if ranking == 'count' else amounts,
    )


def section_ranking(section: EntityProfileSection, ranking: str) -> RankingAccreditation:
    decision = section.fact(f'{ranking}_ranking_accreditation')
    if isinstance(decision, RankingAccreditation):
        return decision
    # The legacy assembler has no accreditation fact. Use the same policy for
    # its existing facts; Frame always supplies the explicit decision above.
    key = 'top_by_contract_count' if ranking == 'count' else 'top_by_contract_value'
    suppliers = section.fact(key)
    return accredit_ranking(ranking, suppliers or (), available=suppliers is not None)


def section_window(section: EntityProfileSection, ranking: str) -> RankingWindow | None:
    """La ventana del ranking si la sección la publicó; ``None`` en V1 puro."""
    window = section.fact(f'{ranking}_window')
    return window if isinstance(window, RankingWindow) else None


def ranking_warnings(ranking: str, decision: RankingAccreditation) -> tuple[str, ...]:
    label = 'cantidad' if ranking == 'count' else 'valor'
    if not decision.available:
        return (f'No se obtuvo el TOP por {label}; no se interpreta como un resultado vacío.',)
    warnings = []
    if not decision.amounts_valid:
        warnings.append(
            f'Hay montos de proveedores no acreditables en el TOP por {label}. '
            'Se conservan las identidades y los conteos válidos.'
        )
    if not decision.counts_valid:
        warnings.append(f'Hay conteos de proveedores no acreditables en el TOP por {label}.')
    if not decision.accredited:
        metric = 'número de contratos' if ranking == 'count' else 'valor'
        warnings.append(f'El ranking por {metric} no está acreditado.')
    return tuple(warnings)
