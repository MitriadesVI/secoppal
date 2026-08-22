"""SEARCH-SH1 audit corpus for the existing modality dimension."""
from __future__ import annotations

import pytest

from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


router = QueryRouter()
builder = SoQLBuilder()


@pytest.mark.parametrize(
    ("query", "expected_ids"),
    [
        ("licitacion", ["licitacion_publica", "licitacion_publica_obra"]),
        ("licitacion publica", ["licitacion_publica", "licitacion_publica_obra"]),
        (
            "seleccion abreviada",
            [
                "seleccion_abreviada_menor_cuantia",
                "seleccion_abreviada_menor_cuantia_sin_manifestacion",
                "seleccion_abreviada_subasta_inversa",
            ],
        ),
        (
            "seleccion abreviada de menor cuantia",
            ["seleccion_abreviada_menor_cuantia"],
        ),
        ("subasta inversa", ["seleccion_abreviada_subasta_inversa"]),
        ("minima cuantia", ["minima_cuantia"]),
        (
            "concurso de meritos",
            ["concurso_meritos_abierto", "concurso_meritos_precalificacion"],
        ),
        (
            "contratacion directa",
            ["contratacion_directa", "contratacion_directa_con_ofertas"],
        ),
        (
            "regimen especial",
            ["regimen_especial", "regimen_especial_con_ofertas"],
        ),
        ("regimen especial con ofertas", ["regimen_especial_con_ofertas"]),
        ("regimen especial sin ofertas", ["regimen_especial"]),
        ("acuerdo marco", ["acuerdo_marco_precio_secop"]),
        ("AMP", ["acuerdo_marco_precio_secop"]),
        (
            "enajenacion de bienes",
            ["enajenacion_sobre_cerrado", "enajenacion_subasta"],
        ),
    ],
)
def test_modality_language_is_consumed_and_projects_to_both_datasets(
    query: str, expected_ids: list[str],
) -> None:
    parsed = router.parse(query)

    assert parsed.params["modalidad_include_procedure_ids"] == expected_ids
    assert parsed.params.get("objeto", []) == []
    assert parsed.modality_interpretation is not None
    assert parsed.modality_interpretation.consumed_spans
    assert not parsed.modality_interpretation.residual_text.strip()

    for dataset in ("procesos", "contratos"):
        params = dict(parsed.params, dataset=dataset)
        soql = builder.build(builder.dataset_id_for(dataset), params)
        assert "modalidad_de_contratacion" in soql
        assert (
            "modalidad_de_contratacion IN" in soql
            or "modalidad_de_contratacion =" in soql
        )


def test_convenio_and_regimen_especial_do_not_collapse_dimensions() -> None:
    parsed = router.parse("convenios de regimen especial")

    assert parsed.params["contract_type_include"] == ["convenio"]
    assert parsed.params["modalidad_include_procedure_ids"] == [
        "regimen_especial",
        "regimen_especial_con_ofertas",
    ]
    assert parsed.params.get("objeto", []) == []
