"""Proyección cerrada de procedure_ids canónicos a raws por dataset."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from app.core.modality_filter import ModalityFilter
from app.core.modality_models import ModalityCatalog, load_catalog

_VALID_DATASETS = frozenset({"procesos", "contratos"})


class ModalityDatasetProjectionError(ValueError):
    """El filtro canónico no puede proyectarse de forma segura."""


@dataclass(frozen=True)
class DatasetModalityProjection:
    include_raw_values: tuple[str, ...]
    exclude_raw_values: tuple[str, ...]
    include_requested: bool

    @property
    def include_unsatisfiable(self) -> bool:
        return self.include_requested and not self.include_raw_values


@lru_cache(maxsize=1)
def _default_catalog() -> ModalityCatalog:
    return load_catalog()


def _catalog(catalog: ModalityCatalog | None) -> ModalityCatalog:
    return catalog if catalog is not None else _default_catalog()


def _validate_dataset(dataset: str) -> None:
    if type(dataset) is not str or dataset not in _VALID_DATASETS:  # noqa: E721
        raise ModalityDatasetProjectionError(
            "dataset debe ser exactamente 'procesos' o 'contratos' "
            f"(recibido {dataset!r})"
        )


def _raw_values(
    procedure_ids: tuple[str, ...],
    dataset: str,
    catalog: ModalityCatalog,
) -> tuple[str, ...]:
    values: set[str] = set()
    for procedure_id in procedure_ids:
        procedure = catalog.by_id(procedure_id)
        if procedure is None:
            raise ModalityDatasetProjectionError(
                f"procedure_id desconocido: {procedure_id!r}"
            )
        values.update(procedure.dataset_values.get(dataset, ()))
    return tuple(sorted(values))


def project_modality_filter(
    modality_filter: ModalityFilter,
    dataset: str,
    *,
    catalog: ModalityCatalog | None = None,
) -> DatasetModalityProjection:
    """Proyecta solo los IDs pedidos; nunca reexpande familias ni aliases."""
    if not isinstance(modality_filter, ModalityFilter):
        raise ModalityDatasetProjectionError(
            "modality_filter debe ser ModalityFilter"
        )
    _validate_dataset(dataset)
    if modality_filter.is_empty():
        return DatasetModalityProjection((), (), False)
    selected_catalog = _catalog(catalog)
    return DatasetModalityProjection(
        include_raw_values=_raw_values(
            modality_filter.include_procedure_ids, dataset, selected_catalog
        ),
        exclude_raw_values=_raw_values(
            modality_filter.exclude_procedure_ids, dataset, selected_catalog
        ),
        include_requested=bool(modality_filter.include_procedure_ids),
    )


def procedure_id_for_raw_value(
    dataset: str,
    raw_value: str,
    *,
    catalog: ModalityCatalog | None = None,
) -> str | None:
    """Traduce un raw exacto a su único procedure_id, o ``None``."""
    _validate_dataset(dataset)
    if not isinstance(raw_value, str):
        raise ModalityDatasetProjectionError(
            f"raw_value debe ser str (recibido {type(raw_value).__name__})"
        )
    matches = [
        procedure.procedure_id
        for procedure in _catalog(catalog).procedures
        if raw_value in procedure.dataset_values.get(dataset, ())
    ]
    if len(matches) > 1:
        raise ModalityDatasetProjectionError(
            f"raw value ambiguo en dataset={dataset}: {raw_value!r}"
        )
    return matches[0] if matches else None
