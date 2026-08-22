"""Natural contractual-class corpus through parser, frame and SoQL."""
from __future__ import annotations

import pytest

from app.core.query_frame import frame_from_params, params_from_frame
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


router = QueryRouter()
builder = SoQLBuilder()


@pytest.mark.parametrize(
    ("query", "contract_type", "topic"),
    [
        ("convenios de la alcaldia de barranquilla en 2026", "convenio", []),
        ("convenios de regimen especial de barranquilla en 2026", "convenio", []),
        ("convenios de regimen especial por mas de 2 mil millones", "convenio", []),
        (
            "contratos de prestacion de servicios de adulto mayor",
            "prestacion_servicios",
            ["adulto_mayor"],
        ),
        ("contratos de obra por licitacion publica", "obra", []),
        (
            "contratos interadministrativos de la gobernacion del atlantico",
            "contrato_interadministrativo",
            [],
        ),
        ("convenios de adulto mayor", "convenio", ["adulto_mayor"]),
    ],
)
def test_natural_contractual_class_corpus(
    query: str, contract_type: str, topic: list[str],
) -> None:
    parsed = router.parse(query)

    assert parsed.params["contract_type_include"] == [contract_type]
    assert parsed.params.get("objeto", []) == topic
    assert parsed.contract_type_interpretation is not None
    assert parsed.contract_type_interpretation.consumed_spans


def test_convenio_modality_money_buyer_and_time_all_compose() -> None:
    params = router.parse(
        "muestrame convenios de regimen especial de alcaldia de barranquilla "
        "por mas de 2000 millones en 2026"
    ).params
    soql = builder.build(SoQLBuilder.PROCESOS_DATASET, params)

    assert params["contract_type_include"] == ["convenio"]
    assert params["modalidad_include_procedure_ids"] == [
        "regimen_especial",
        "regimen_especial_con_ofertas",
    ]
    assert params["valor_min"] == 2_000_000_000
    assert params["fecha_desde"] == "2026-01-01"
    assert "BARRANQUILLA" in params["entidad_resolved"]
    assert params.get("objeto", []) == []
    assert "precio_base >= 2000000000" in soql
    assert "modalidad_de_contratacion IN" in soql
    assert "justificaci_n_modalidad_de" in soql
    assert "%convenios%" not in soql


@pytest.mark.parametrize(
    ("dataset", "dataset_id", "justification_field"),
    [
        ("procesos", SoQLBuilder.PROCESOS_DATASET, "justificaci_n_modalidad_de"),
        ("contratos", SoQLBuilder.CONTRATOS_DATASET, "justificacion_modalidad_de"),
    ],
)
def test_convenio_projection_is_explicit_per_dataset(
    dataset: str, dataset_id: str, justification_field: str,
) -> None:
    params = {
        "dataset": dataset,
        "contract_type_include": ["convenio"],
    }
    soql = builder.build(dataset_id, params)

    assert justification_field in soql
    assert "Contratos o convenios Interadministrativos" in soql
    assert "Convenios Solidarios" in soql
    assert "Decreto 092 de 2017" in soql
    assert "tipo_de_contrato = 'Acuerdo de cooperación'" in soql


@pytest.mark.parametrize(
    ("query", "expected_predicate"),
    [
        (
            "contratos de prestacion de servicios de adulto mayor",
            "tipo_de_contrato = 'Prestación de servicios'",
        ),
        ("contratos de obra por licitacion publica", "tipo_de_contrato = 'Obra'"),
        ("contratos de suministro de alimentos", "tipo_de_contrato = 'Suministros'"),
        (
            "contratos de arrendamiento de inmuebles",
            "tipo_de_contrato = 'Arrendamiento de inmuebles'",
        ),
        ("contratos de compraventa de vehiculos", "tipo_de_contrato = 'Compraventa'"),
    ],
)
def test_exact_contract_types_project_to_source_values(
    query: str, expected_predicate: str,
) -> None:
    params = router.parse(query).params
    soql = builder.build(builder.dataset_id_for(params["dataset"]), params)

    assert expected_predicate in soql


def test_contract_type_round_trips_through_query_frame() -> None:
    params = router.parse("convenios de adulto mayor").params

    assert params_from_frame(frame_from_params(params))["contract_type_include"] == [
        "convenio"
    ]


def test_thematic_control_case_stays_thematic() -> None:
    params = router.parse(
        "contratos adulto mayor en cartagena por mas de 1000 millones en 2025"
    ).params

    assert params["objeto"] == ["adulto_mayor"]
    assert "contract_type_include" not in params


def test_plural_obras_control_remains_thematic() -> None:
    params = router.parse("contratos de obras").params

    assert params["objeto"] == ["obras"]
    assert "contract_type_include" not in params
