"""Evidence-backed SECOP projection for canonical contractual classes."""
from __future__ import annotations

from dataclasses import dataclass


_INTERADMIN_JUSTIFICATIONS = (
    "Contratos o convenios Interadministrativos (valor cero)",
    "Contratos o convenios Interadministrativos (con valor)",
)
_CONTRACT_INTERADMIN_JUSTIFICATIONS = (
    "Contratos interadministrativos",
)
_SOLIDARITY_JUSTIFICATIONS = (
    "Convenios Solidarios Regimen articulo 92 Ley 2166 de 2021",
    "Law2166of2021_Article95_SolidarityAgreements",
)

_EXACT_TYPES = {
    "prestacion_servicios": "Prestación de servicios",
    "obra": "Obra",
    "suministro": "Suministros",
    "arrendamiento_inmuebles": "Arrendamiento de inmuebles",
    "arrendamiento_muebles": "Arrendamiento de muebles",
    "compraventa": "Compraventa",
}


@dataclass(frozen=True, slots=True)
class ContractTypeProjection:
    clause: str = ""
    applied_ids: tuple[str, ...] = ()


def _quote(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value.replace(chr(39), chr(39) * 2)}'" for value in values)


def _in(field: str, values: tuple[str, ...]) -> str:
    return f"{field} IN ({_quote(values)})"


def _contains_any(fields: tuple[str, ...], phrase: str) -> str:
    escaped = phrase.replace("'", "''")
    conditions = (
        f"unaccent(UPPER({field})) LIKE unaccent(UPPER('%{escaped}%'))"
        for field in fields
    )
    return f"({' OR '.join(conditions)})"


def _clause_for(
    canonical_id: str,
    *,
    type_field: str,
    justification_field: str,
    text_fields: tuple[str, ...],
) -> str:
    exact = _EXACT_TYPES.get(canonical_id)
    if exact:
        return f"{type_field} = '{exact}'"
    if canonical_id == "arrendamiento":
        return _in(
            type_field,
            ("Arrendamiento de inmuebles", "Arrendamiento de muebles"),
        )
    if canonical_id == "convenio_solidario":
        return _in(justification_field, _SOLIDARITY_JUSTIFICATIONS)
    if canonical_id == "convenio_interadministrativo":
        return (
            f"({_in(justification_field, _INTERADMIN_JUSTIFICATIONS)} AND "
            f"{_contains_any(text_fields, 'convenio')})"
        )
    if canonical_id == "contrato_interadministrativo":
        exact = _in(justification_field, _CONTRACT_INTERADMIN_JUSTIFICATIONS)
        combined = (
            f"({_in(justification_field, _INTERADMIN_JUSTIFICATIONS)} AND "
            f"{_contains_any(text_fields, 'contrato interadministrativo')})"
        )
        return f"({exact} OR {combined})"
    if canonical_id == "convenio_asociacion":
        decree = (
            f"({type_field} = 'Decreto 092 de 2017' OR "
            f"{justification_field} = 'Decreto 092 de 2017')"
        )
        return f"({decree} AND {_contains_any(text_fields, 'convenio')})"
    if canonical_id == "convenio":
        solidarity = _in(justification_field, _SOLIDARITY_JUSTIFICATIONS)
        interadmin = (
            f"({_in(justification_field, _INTERADMIN_JUSTIFICATIONS)} AND "
            f"{_contains_any(text_fields, 'convenio')})"
        )
        association = (
            f"((({type_field} = 'Decreto 092 de 2017') OR "
            f"({justification_field} = 'Decreto 092 de 2017')) AND "
            f"{_contains_any(text_fields, 'convenio')})"
        )
        cooperation = f"{type_field} = 'Acuerdo de cooperación'"
        return f"({solidarity} OR {interadmin} OR {association} OR {cooperation})"
    raise ValueError(f"unknown contract type id: {canonical_id}")


def project_contract_types(
    include_ids: list[str] | tuple[str, ...],
    *,
    type_field: str,
    justification_field: str,
    text_fields: tuple[str, ...],
) -> ContractTypeProjection:
    ids = tuple(dict.fromkeys(str(item) for item in include_ids if item))
    clauses = tuple(
        _clause_for(
            item,
            type_field=type_field,
            justification_field=justification_field,
            text_fields=text_fields,
        )
        for item in ids
    )
    if not clauses:
        return ContractTypeProjection()
    return ContractTypeProjection(
        clause=f"({' OR '.join(clauses)})",
        applied_ids=ids,
    )
