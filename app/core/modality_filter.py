"""Value object ejecutable de modalidad (M3B-1).

``ModalityFilter`` es el ÚNICO contrato ejecutable de modalidad: dos conjuntos
de ``procedure_ids`` (incluir / excluir) ya resueltos por M2. Es inmutable,
JSON-nativo en su proyección, y NO conoce status, issues, aliases, familias,
valores raw de Socrata ni datasets. No consulta el catálogo ni reexpande
familias: los ``procedure_ids`` que recibe son autoridad final.
"""
from __future__ import annotations

from dataclasses import dataclass

_INCLUDE_KEY = "modalidad_include_procedure_ids"
_EXCLUDE_KEY = "modalidad_exclude_procedure_ids"


def _coerce_ids(values: object, field_name: str) -> tuple[str, ...]:
    """Valida, deduplica y ordena una colección de procedure_ids.

    Acepta cualquier iterable de strings no vacíos. Rechaza ``bool`` como ID
    (aunque sea subtipo de ``int``), valores no-string y strings vacíos o solo
    de espacios. El resultado es determinista: ordenado y sin duplicados.
    """
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{field_name} debe ser una colección de IDs, no {type(values).__name__}")
    try:
        items = list(values)  # type: ignore[arg-type]
    except TypeError as exc:
        raise ValueError(f"{field_name} debe ser iterable de procedure_ids") from exc

    seen: set[str] = set()
    for value in items:
        if isinstance(value, bool):
            raise ValueError(f"{field_name}: bool no es un procedure_id válido")
        if not isinstance(value, str):
            raise ValueError(
                f"{field_name}: procedure_id debe ser str (recibido {type(value).__name__})"
            )
        if not value or not value.strip():
            raise ValueError(f"{field_name}: procedure_id vacío no permitido")
        seen.add(value)
    return tuple(sorted(seen))


@dataclass(frozen=True)
class ModalityFilter:
    """Filtro ejecutable inmutable. Objeto vacío ⇔ ausencia de filtro."""

    include_procedure_ids: tuple[str, ...] = ()
    exclude_procedure_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        include = _coerce_ids(self.include_procedure_ids, _INCLUDE_KEY)
        exclude = _coerce_ids(self.exclude_procedure_ids, _EXCLUDE_KEY)
        overlap = set(include) & set(exclude)
        if overlap:
            raise ValueError(
                f"include y exclude no pueden solaparse: {sorted(overlap)}"
            )
        object.__setattr__(self, "include_procedure_ids", include)
        object.__setattr__(self, "exclude_procedure_ids", exclude)

    def is_empty(self) -> bool:
        """True si no hay ningún ID de inclusión ni de exclusión."""
        return not self.include_procedure_ids and not self.exclude_procedure_ids

    def to_params(self) -> dict[str, list[str]]:
        """Proyección JSON-nativa. Nunca escribe claves vacías ni ``modalidad``."""
        params: dict[str, list[str]] = {}
        if self.include_procedure_ids:
            params[_INCLUDE_KEY] = list(self.include_procedure_ids)
        if self.exclude_procedure_ids:
            params[_EXCLUDE_KEY] = list(self.exclude_procedure_ids)
        return params

    @classmethod
    def from_params(cls, params: dict) -> "ModalityFilter":
        """Reconstruye desde params. Claves ausentes ⇒ vacío. No-lista ⇒ error."""
        include = _require_list(params, _INCLUDE_KEY)
        exclude = _require_list(params, _EXCLUDE_KEY)
        return cls(include_procedure_ids=include, exclude_procedure_ids=exclude)


def _require_list(params: dict, key: str) -> tuple[str, ...]:
    if key not in params:
        return ()
    value = params[key]
    if not isinstance(value, list):
        raise ValueError(f"{key} debe ser una lista (recibido {type(value).__name__})")
    return tuple(value)
