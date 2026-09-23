"""Los cuatro bindings del dogfood valen lo que vale su artefacto (ADR-073).

Un binding es una afirmación sobre la fuente: «ESTA identidad publica ESTOS
nombres físicos en ESTE dataset». Aquí no se vuelve a consultar SECOP —eso lo
hace el reproductor read-only— pero sí se comprueba que lo escrito en el sidecar
**se deriva** del artefacto que lo acredita y no de otra cosa.

La regla que estos casos protegen es la de «ningún cambio de semántica»: el
binding contiene exactamente las formas que el fallback LIKE ya alcanzaba. Una
forma física REAL que el LIKE no alcanza hoy queda declarada en el artefacto y
FUERA del binding, porque incorporarla ampliaría el universo en silencio y eso
es una decisión de producto (BUYER_GROUP), no de proyección de nombres.
"""
from pathlib import Path
import json

import pytest

from app.core.buyer_source_names import load_registry, project_buyer_names

ARTIFACT = Path("artifacts/audits/dogfood_buyer_coverage_adr073.json")

COHORT = (
    "MUNICIPIO DE SOLEDAD",
    "MUNICIPIO DE GALAPA.",
    "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
    "ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS",
)


@pytest.fixture(scope="module")
def artifact() -> dict:
    return json.loads(ARTIFACT.read_text())


@pytest.fixture(scope="module")
def observations(artifact) -> dict:
    return {item["canonical_name"]: item for item in artifact["observations"]}


@pytest.mark.parametrize("canonical", COHORT)
def test_each_binding_is_derived_from_its_own_evidence(canonical, observations):
    observation = observations[canonical]
    binding = load_registry().binding_for(canonical)
    assert binding is not None, f"{canonical} no tiene binding"
    assert binding.projection_evidence.evidence == ARTIFACT.as_posix()
    # El artefacto histórico declara el NIT; Schema 3 lo conserva como
    # acreditación fiscal, separada de la identidad operacional BuyerUnit.
    declared = observation["identity_key"]
    assert declared["type"] == "NIT"
    assert any(declared["value"] in entry.raw_forms for entry in binding.fiscal_attestations)
    for dataset, names in observation["proposed_binding"].items():
        assert binding.names_for(dataset) == tuple(names)
        assert project_buyer_names(canonical, dataset) == tuple(names)


@pytest.mark.parametrize("canonical", COHORT)
def test_the_binding_preserves_the_universe_the_fallback_already_returned(
    canonical, observations,
):
    """Equivalencia MEDIDA, no supuesta: mismas filas y ningún NIT ajeno."""
    observation = observations[canonical]
    assert observation["status"] == "READY"
    assert observation["semantics_preserved"] is True
    for dataset, data in observation["datasets"].items():
        measured = data["benchmark"]
        # `is True`: un LIKE que no contestó deja None, y None no acredita nada.
        assert measured["same_universe"] is True, (canonical, dataset)
        assert measured["like_rows"] == measured["exact_rows"]
        assert not data["foreign_nits_reached_by_like"], (
            f"{canonical}/{dataset}: el fallback alcanza NITs ajenos, "
            "el binding exacto NO es equivalente"
        )


@pytest.mark.parametrize("canonical", COHORT)
def test_the_binding_never_exceeds_what_the_fallback_reached(canonical, observations):
    """El binding no puede AMPLIAR el universo, ni siquiera con formas reales."""
    observation = observations[canonical]
    for dataset, data in observation["datasets"].items():
        bound = set(observation["proposed_binding"][dataset])
        assert bound == set(data["forms_reached_by_like"])
        assert bound.isdisjoint(data["forms_not_reached_by_like"])


def test_cartagena_declares_its_other_bodies_instead_of_absorbing_them(observations):
    """Cuatro nombres bajo un NIT no son un comprador: son una pregunta abierta.

    DATT, la Alcaldía de la Localidad Industrial y de la Bahía y el Fondo
    Especial de Bomberos publican bajo el NIT del distrito. El fallback no los
    alcanza hoy, así que el binding tampoco: quedan DECLARADOS en el artefacto
    como candidatos a una decisión BUYER_GROUP que nadie ha tomado.
    """
    cartagena = observations["ALCALDÍA DEL DISTRITO TURÍSTICO Y CULTURAL DE CARTAGENA DE INDIAS"]
    declared = cartagena["declared_not_bound"]
    assert declared, "Cartagena publica otros cuerpos; el artefacto debe nombrarlos"
    for dataset, names in declared.items():
        assert "DEPARTAMENTO ADMINISTRATIVO DE TRÁNSITO Y TRANSPORTE DATT" in names
        assert project_buyer_names(cartagena["canonical_name"], dataset) is not None
        for name in names:
            assert name not in project_buyer_names(cartagena["canonical_name"], dataset)


def test_malambo_stays_out_because_it_has_no_identity_to_key_a_binding(artifact):
    """Sin identidad canónica no hay clave, y un nombre físico «que parece
    correcto» es exactamente lo que ADR-073 prohíbe escribir."""
    excluded = {item["phrase"]: item for item in artifact["excluded"]}
    malambo = excluded["alcaldia de malambo"]
    assert malambo["reason"] == "NO_CANONICAL_IDENTITY"
    assert malambo["resolution"]["value"] is None
    assert load_registry().binding_for("MUNICIPIO DE MALAMBO") is None


def test_the_measured_binding_is_faster_than_the_fallback_it_replaces(observations):
    """La razón de existir del binding. Se afirma lo medido, no una expectativa."""
    for canonical in COHORT:
        for dataset, data in observations[canonical]["datasets"].items():
            measured = data["benchmark"]
            assert measured["like_timeouts"] == 0, (canonical, dataset)
            assert measured["exact_ms_median"] < measured["like_ms_median"], (
                canonical, dataset, measured["like_ms_median"], measured["exact_ms_median"],
            )
