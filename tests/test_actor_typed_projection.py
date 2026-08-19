"""INV-TYPED-PROJECTION — la llave de identidad llega entera al WHERE.

AP1 declaró que un actor es ``(document_type, base_number)``. AP2-B llevó
el número a la ejecución y dejó el tipo fuera, así que dos identidades
distintas emitían SoQL idéntico y la llave quedaba decorativa justo donde
importa. Este archivo congela la corrección y —tan importante— el límite
que la corrección NO alcanza.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.actor_document_types import (
    ORGANIZATION_SOURCE_TYPES,
    PERSONAL_SOURCE_TYPES,
    UNDETERMINED_SOURCE_TYPES,
    incompatible_source_types,
    is_personal_source_type,
)
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder

CONTRACT = json.loads(
    Path("tests/fixtures/actor_sources/secop2_contratos_source_contract.json")
    .read_text(encoding="utf-8")
)
FINDINGS = CONTRACT["typed_projection_findings"]

router = QueryRouter()
builder = SoQLBuilder()


def where_for(text: str) -> str:
    params = dict(router.parse(text).params)
    soql = builder.build(builder.dataset_id_for(params.get("dataset")), params)
    return soql.split(" WHERE ", 1)[1].split(" ORDER BY")[0]


# ---------------------------------------------------------------------------
# La incoherencia que se cierra.
# ---------------------------------------------------------------------------


def test_two_identities_over_one_number_no_longer_emit_the_same_query():
    """El defecto exacto: antes de esto, ambos WHERE eran idénticos."""
    as_cedula = where_for("contratos de la cedula 13848603")
    as_nit = where_for("contratos del NIT 13848603")

    assert as_cedula != as_nit
    assert "documento_proveedor = '13848603'" in as_cedula
    assert "documento_proveedor = '13848603'" in as_nit


def test_a_nit_identity_excludes_personal_document_types():
    where = where_for("contratos del NIT 13848603")
    assert "tipodocproveedor" in where
    for personal in PERSONAL_SOURCE_TYPES:
        assert f"'{personal}'" in where


def test_a_cedula_identity_excludes_the_nit_type():
    where = where_for("contratos de la cedula 13848603")
    assert "tipodocproveedor NOT IN ('NIT')" in where


# ---------------------------------------------------------------------------
# Excluir lo contradictorio, no exigir lo idéntico.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("undetermined", UNDETERMINED_SOURCE_TYPES)
def test_undetermined_source_types_are_never_excluded(undetermined):
    """"No Definido" y "Otro" no contradicen nada: no afirman nada.

    Excluirlos borraría los 4 contratos de La Previsora publicados como
    "Otro" y 10.073 filas del universo — cambiaría falsas atribuciones por
    omisiones silenciosas, que es el mismo defecto con otro signo.
    """
    for text in ("contratos del NIT 13848603", "contratos de la cedula 13848603"):
        assert f"'{undetermined}'" not in where_for(text)


def test_the_measured_cost_of_a_strict_filter_is_recorded_in_the_contract():
    """La decisión no es de gusto: está medida contra la fuente."""
    assert FINDINGS["documento_860002400_por_tipo"]["Otro"] == 4
    assert sum(FINDINGS["filas_con_documento_utilizable_y_tipo_indeterminado"].values()) > 10_000


def test_an_untyped_identity_does_not_narrow_the_universe():
    """Sin evidencia de qué clase de actor es, nada es incompatible.

    Recortar aquí sería inferir el tipo — exactamente lo que INV-NO-LENGTH
    prohíbe. La ambigüedad se declara en el render, no se resuelve sola.
    """
    assert "tipodocproveedor" not in where_for("contratos de 13848603")
    assert incompatible_source_types("UNKNOWN") == ()
    assert incompatible_source_types(None) == ()


def test_nulls_survive_the_type_filter():
    """``NOT IN`` a secas descarta los nulos en silencio.

    Hoy la columna no tiene ninguno, pero una omisión silenciosa futura es
    justo lo que este bloque existe para cerrar.
    """
    where = where_for("contratos del NIT 13848603")
    assert "tipodocproveedor IS NULL OR" in where
    assert CONTRACT["fields"]["document_type"]["null_count"] == 0


# ---------------------------------------------------------------------------
# El límite: el filtro no sustituye las otras defensas.
# ---------------------------------------------------------------------------


def test_the_filter_cannot_separate_what_the_source_misclassified():
    """De las 192 filas "Cédula" bajo 13848603, una es una unión temporal.

    El error está en el dato publicado, así que ningún predicado por tipo
    la aparta. Por eso el perfil sigue listando los nombres observados y
    advirtiendo que pueden ser agrupaciones distintas: son dos defensas
    para dos fallas, no una en lugar de la otra.
    """
    limits = " ".join(FINDINGS["findings"]).lower()
    assert "union temporal de recargas 2022" in limits
    assert "no sustituye" in limits


def test_the_observed_names_block_warns_about_groups():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from app.core.actor_profile.models import ActorProfileResult, ObservedName
    from app.core.actor_profile.render import render_actor_profile

    profile = ActorProfileResult(
        queried_at=datetime(2026, 8, 18, 14, 32, tzinfo=ZoneInfo("America/Bogota")),
        identity={"document_type": "CC", "base_number": "13848603"},
        dataset_id=SoQLBuilder.CONTRATOS_DATASET,
        observed_names=(
            ObservedName("PERSONA NATURAL", "13848603", "Cédula de Ciudadanía", 191),
            ObservedName("UNION TEMPORAL DE RECARGAS 2022", "13848603",
                         "Cédula de Ciudadanía", 1),
        ),
    )
    text = render_actor_profile(profile)
    assert "uniones temporales o consorcios" in text


def test_an_untyped_mixed_profile_offers_the_way_out():
    """Cuando el tipo no se declaró, precisar es del usuario y sirve de verdad."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from app.core.actor_profile.models import ActorProfileResult, ObservedName
    from app.core.actor_profile.render import render_actor_profile

    profile = ActorProfileResult(
        queried_at=datetime(2026, 8, 18, 14, 32, tzinfo=ZoneInfo("America/Bogota")),
        identity={"document_type": "UNKNOWN", "base_number": "13848603"},
        dataset_id=SoQLBuilder.CONTRATOS_DATASET,
        observed_names=(
            ObservedName("PERSONA NATURAL", "13848603", "Cédula de Ciudadanía", 191),
            ObservedName("UNIÓN TEMPORAL MIGRACIÓN COLOMBIA 2021", "13848603", "NIT", 1),
        ),
    )
    text = render_actor_profile(profile)
    assert "más de un tipo de documento" in text
    assert "separo la huella" in text


def test_a_typed_mixed_profile_does_not_ask_for_what_it_already_has():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from app.core.actor_profile.models import ActorProfileResult, ObservedName
    from app.core.actor_profile.render import render_actor_profile

    profile = ActorProfileResult(
        queried_at=datetime(2026, 8, 18, 14, 32, tzinfo=ZoneInfo("America/Bogota")),
        identity={"document_type": "CC", "base_number": "13848603"},
        dataset_id=SoQLBuilder.CONTRATOS_DATASET,
        observed_names=(
            ObservedName("A", "13848603", "Cédula de Ciudadanía", 191),
            ObservedName("B", "13848603", "Otro", 1),
        ),
    )
    assert "separo la huella" not in render_actor_profile(profile)


# ---------------------------------------------------------------------------
# El catálogo de tipos es el de la fuente, no uno inventado.
# ---------------------------------------------------------------------------


def test_every_source_type_in_the_catalog_was_observed_in_the_dataset():
    observed = set(CONTRACT["fields"]["document_type"]["observed_values"])
    catalog = set(PERSONAL_SOURCE_TYPES) | set(ORGANIZATION_SOURCE_TYPES) | set(
        UNDETERMINED_SOURCE_TYPES
    )
    assert catalog == observed


@pytest.mark.parametrize("value", ["CC", "Cédula de Ciudadanía", "cedula de ciudadania"])
def test_personal_types_are_recognised_in_both_vocabularies(value):
    """El perfil ve el vocabulario de AP1 y el de la fuente a la vez."""
    assert is_personal_source_type(value) is True


@pytest.mark.parametrize("value", ["NIT", "Otro", "No Definido", None, ""])
def test_non_personal_values_are_not_treated_as_personal(value):
    assert is_personal_source_type(value) is False
