from __future__ import annotations

from types import MappingProxyType

import pytest

from app.core.modality_dataset_projection import (
    ModalityDatasetProjectionError,
    procedure_id_for_raw_value,
    project_modality_filter,
)
from app.core.modality_filter import ModalityFilter
from app.core.modality_models import ModalityCatalog, ProcedureClassification


def test_projection_uses_only_dataset_raw_values_sorted_unique():
    projection = project_modality_filter(
        ModalityFilter(
            include_procedure_ids=(
                "licitacion_publica_obra",
                "licitacion_publica",
            ),
            exclude_procedure_ids=("contratacion_directa",),
        ),
        "procesos",
    )

    assert projection.include_raw_values == (
        "Licitación pública",
        "Licitación pública Obra Publica",
    )
    assert projection.exclude_raw_values == ("Contratación directa",)
    assert projection.include_requested is True
    assert projection.include_unsatisfiable is False


def test_projection_does_not_reexpand_specific_procedure():
    projection = project_modality_filter(
        ModalityFilter(
            include_procedure_ids=(
                "seleccion_abreviada_subasta_inversa",
            )
        ),
        "contratos",
    )
    assert projection.include_raw_values == (
        "Selección abreviada subasta inversa",
    )


def test_unknown_id_fails_closed():
    with pytest.raises(
        ModalityDatasetProjectionError, match="procedure_id desconocido"
    ):
        project_modality_filter(
            ModalityFilter(include_procedure_ids=("no_existe",)),
            "procesos",
        )


@pytest.mark.parametrize("dataset", ["", "Procesos", "radar", None])
def test_invalid_dataset_fails_closed(dataset):
    with pytest.raises(ModalityDatasetProjectionError):
        project_modality_filter(
            ModalityFilter(), dataset  # type: ignore[arg-type]
        )


def test_inapplicable_include_is_explicit_zero_set():
    projection = project_modality_filter(
        ModalityFilter(
            include_procedure_ids=(
                "solicitud_informacion_proveedores",
            )
        ),
        "contratos",
    )
    assert projection.include_requested is True
    assert projection.include_raw_values == ()
    assert projection.include_unsatisfiable is True


def test_partially_applicable_include_keeps_applicable_branch():
    projection = project_modality_filter(
        ModalityFilter(
            include_procedure_ids=(
                "licitacion_publica",
                "solicitud_informacion_proveedores",
            )
        ),
        "contratos",
    )
    assert projection.include_raw_values == ("Licitación pública",)
    assert projection.include_unsatisfiable is False


def test_inapplicable_exclude_excludes_nothing():
    projection = project_modality_filter(
        ModalityFilter(
            exclude_procedure_ids=(
                "solicitud_informacion_proveedores",
            )
        ),
        "contratos",
    )
    assert projection.exclude_raw_values == ()
    assert projection.include_unsatisfiable is False


def test_raw_value_maps_only_by_exact_dataset_value():
    assert (
        procedure_id_for_raw_value(
            "procesos", "Licitación pública"
        )
        == "licitacion_publica"
    )
    assert (
        procedure_id_for_raw_value(
            "procesos", "Licitacion Publica"
        )
        is None
    )
    assert (
        procedure_id_for_raw_value(
            "contratos", "Solicitud de información a los Proveedores"
        )
        is None
    )


def _procedure(procedure_id: str, raw: str) -> ProcedureClassification:
    return ProcedureClassification(
        procedure_id=procedure_id,
        display_name=procedure_id,
        legal_selection_family=None,
        legal_classification_status="not_applicable",
        retrieval_family="test",
        procedure_variant=None,
        legal_regime="no_aplica",
        record_kind="platform_test",
        submission_mode="no_aplica",
        user_searchable=False,
        explicit_intent_required=False,
        parent_expansion_policy="never",
        dataset_values=MappingProxyType({"procesos": (raw,)}),
        evidence_confidence="alta",
    )


def test_raw_collision_fails_even_if_catalog_constructor_is_synthetic():
    catalog = ModalityCatalog(
        version=1,
        procedures=(_procedure("a", "RAW"), _procedure("b", "RAW")),
    )
    with pytest.raises(
        ModalityDatasetProjectionError, match="raw value ambiguo"
    ):
        procedure_id_for_raw_value(
            "procesos", "RAW", catalog=catalog
        )
