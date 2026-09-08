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
