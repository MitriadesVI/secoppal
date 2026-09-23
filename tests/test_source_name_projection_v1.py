"""SOURCE_NAME_PROJECTION_V1 — ADR-073.

La identidad canónica y el string que cada dataset guarda son autoridades
distintas. Estos tests fijan las tres reglas y, sobre todo, impiden que la
comparación se relaje: bajo `LIKE` la grafía de un dataset alcanza la del otro
por substring y el defecto vuelve sin que ningún test caiga.
"""

from __future__ import annotations

from tests.sql_oracle_properties import assert_identity

from datetime import date
import json
import re

import pytest

from app.core.buyer_source_names import (
    SCHEMA_VERSION,
    BuyerSourceNameError,
    load_registry,
    parse_registry,
    project_buyer_names,
)
from app.core.soql_builder import SoQLBuilder

DAPRE_CANONICAL = "DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA"
PROCESOS = SoQLBuilder.PROCESOS_DATASET
CONTRATOS = SoQLBuilder.CONTRATOS_DATASET


@pytest.fixture
def builder() -> SoQLBuilder:
    return SoQLBuilder()


def _fake_projector(mapping: dict[tuple[str, str], tuple[str, ...]]):
    def _project(canonical_name: str, dataset_id: str):
        return mapping.get((canonical_name, dataset_id))
    return _project


# =====================================================================
# A. SYNTHETIC SOURCE SPLIT — la puntuación es deliberada
# =====================================================================

def test_process_source_name_does_not_satisfy_contracts(builder, monkeypatch):
    """El nombre de procesos NO puede satisfacer contratos.

    "TEST BUYER.." contiene a "TEST BUYER." como substring: bajo LIKE este
    test pasaría con la implementación equivocada. Bajo igualdad exacta el
    filtro de contratos sólo acepta su propia grafía.
    """
    monkeypatch.setattr(
        "app.core.soql_builder.project_buyer_names",
        _fake_projector({
            ("TEST BUYER", PROCESOS): ("TEST BUYER..",),
            ("TEST BUYER", CONTRATOS): ("TEST BUYER.",),
        }),
    )
    params = {"entidad_resolved": "TEST BUYER"}

    procesos = builder.build_count(PROCESOS, dict(params))
    contratos = builder.build_count(CONTRATOS, dict(params))

    assert "entidad = 'TEST BUYER..'" in procesos
    assert "nombre_entidad = 'TEST BUYER.'" in contratos
    # La grafía de procesos no aparece en el filtro de contratos.
    assert "TEST BUYER..'" not in contratos


def test_bound_clause_carries_no_relaxation(builder, monkeypatch):
    """Mutación 5: `%`, UPPER() o folding en la rama con binding deben caer."""
    monkeypatch.setattr(
        "app.core.soql_builder.project_buyer_names",
        _fake_projector({("TEST BUYER", PROCESOS): ("TEST BUYER..",)}),
    )
    soql = builder.build_count(PROCESOS, {"entidad_resolved": "TEST BUYER"})

    assert soql.endswith("WHERE entidad = 'TEST BUYER..'")
    assert "%" not in soql
    assert "LIKE" not in soql
    assert "UPPER" not in soql
    assert "unaccent" not in soql


# =====================================================================
# B. MULTI-NAME
# =====================================================================

def test_multiple_source_names_emit_explicit_or(builder, monkeypatch):
    monkeypatch.setattr(
        "app.core.soql_builder.project_buyer_names",
        _fake_projector({("TEST BUYER", PROCESOS): ("NAME A", "NAME B")}),
    )
    soql = builder.build_count(PROCESOS, {"entidad_resolved": "TEST BUYER"})

    assert "(entidad = 'NAME A' OR entidad = 'NAME B')" in soql
    assert " IN (" not in soql


def test_single_source_name_emits_bare_equality(builder, monkeypatch):
    monkeypatch.setattr(
        "app.core.soql_builder.project_buyer_names",
        _fake_projector({("TEST BUYER", PROCESOS): ("NAME A",)}),
    )
    soql = builder.build_count(PROCESOS, {"entidad_resolved": "TEST BUYER"})

    assert soql.endswith("WHERE entidad = 'NAME A'")


def test_source_name_quote_is_escaped(builder, monkeypatch):
    """El único tratamiento permitido sobre el string: duplicar la comilla."""
    monkeypatch.setattr(
        "app.core.soql_builder.project_buyer_names",
        _fake_projector({("TEST BUYER", PROCESOS): ("O'HIGGINS",)}),
    )
    soql = builder.build_count(PROCESOS, {"entidad_resolved": "TEST BUYER"})

    assert "entidad = 'O''HIGGINS'" in soql


# =====================================================================
# C. ENTIDAD_LIKE — nunca se proyecta
# =====================================================================

EXPECTED_LIKE_BOLIVAR = (
    "SELECT count(*) WHERE unaccent(UPPER(entidad)) LIKE "
    "unaccent(UPPER('%CONTRALORIA DEPARTAMENTAL DE BOLIVAR%'))"
)


def test_entidad_like_keeps_literal_filter(builder):
    soql = builder.build_count(
        PROCESOS, {"entidad_like": "CONTRALORIA DEPARTAMENTAL DE BOLIVAR"},
    )
    assert soql == EXPECTED_LIKE_BOLIVAR


def test_entidad_like_holding_a_bound_name_is_still_not_projected(builder):
    """Mutación 3: proyectar `entidad_like` debe hacer caer este canario.

    El valor de `entidad_like` es AQUÍ el canónico ligado. Ese detalle es lo
    que hace útil al canario: si se probara con una cadena sin binding, la
    proyección devolvería `None` por ausencia y el guard quedaría sin
    ejercer, pasando el test aunque el guard se hubiera borrado.

    `entidad_like` significa "palabras del usuario sin identidad acreditada".
    Que el texto coincida con un canónico no lo convierte en identidad: quien
    acredita es el resolver, y no lo hizo.
    """
    soql = builder.build_count(PROCESOS, {"entidad_like": DAPRE_CANONICAL})

    assert soql == (
        "SELECT count(*) WHERE unaccent(UPPER(entidad)) LIKE "
        f"unaccent(UPPER('%{DAPRE_CANONICAL}%'))"
    )
    assert "= 'DAPRE'" not in soql


def test_entidad_like_wins_over_a_bound_resolved_identity(builder):
    """Con ambos campos gana `entidad_like`, igual que antes del cambio."""
    soql = builder.build_count(PROCESOS, {
        "entidad_like": "CONTRALORIA DEPARTAMENTAL DE BOLIVAR",
        "entidad_resolved": DAPRE_CANONICAL,
    })
    assert soql == EXPECTED_LIKE_BOLIVAR
    assert "DAPRE" not in soql


# =====================================================================
# D. NEUTRALIDAD — identidades sin binding, byte-for-byte
# =====================================================================

UNBOUND = [
    "CONTRALORÍA GENERAL DE LA REPÚBLICA",
    "AUDITORÍA GENERAL DE LA REPÚBLICA",
    "PROCURADURÍA GENERAL DE LA NACIÓN",
    "DEFENSORÍA DEL PUEBLO",
    "CORPORACIÓN AUTÓNOMA REGIONAL DE CUNDINAMARCA",
    "GOBERNACIÓN DE ANTIOQUIA",
    "SERVICIO NACIONAL DE APRENDIZAJE",
    # Soacha es SOURCE_NAME_SPLIT confirmado pero NO está ligado en V1:
    # debe comportarse como cualquier identidad sin binding.
    "MUNICIPIO DE SOACHA.",
]


@pytest.mark.parametrize("canonical", UNBOUND)
@pytest.mark.parametrize("dataset,field", [(PROCESOS, "entidad"), (CONTRATOS, "nombre_entidad")])
def test_unbound_identity_is_byte_identical_to_pre(builder, canonical, dataset, field):
    soql = builder.build_count(dataset, {"entidad_resolved": canonical})
    expected = (
        f"SELECT count(*) WHERE unaccent(UPPER({field})) LIKE "
        f"unaccent(UPPER('%{canonical}%'))"
    )
    assert_identity(soql, dataset, expected.split(" WHERE ", 1)[1])


def test_ecosystem_branch_still_fires_when_no_entity_value(builder):
    """El guard `not entidad_value` no se movió al separar los campos."""
    soql = builder.build_count(PROCESOS, {
        "entidad_ecosystem": ["ALCALDIA DE CALI", "MUNICIPIO DE CALI"],
        "estado": "Activo",
    })
    assert "ALCALDIA DE CALI" in soql
    assert "MUNICIPIO DE CALI" in soql


def test_ecosystem_branch_still_yields_to_a_bound_identity(builder):
    """Con `entidad_resolved` presente el ecosystem sigue sin disparar."""
    soql = builder.build_count(PROCESOS, {
        "entidad_resolved": DAPRE_CANONICAL,
        "entidad_ecosystem": ["ALCALDIA DE CALI"],
    })
    assert soql == "SELECT count(*) WHERE entidad = 'DAPRE'"
    assert "CALI" not in soql


# =====================================================================
# E. INMUTABILIDAD DE PARAMS
# =====================================================================

@pytest.mark.parametrize("dataset", [PROCESOS, CONTRATOS])
def test_projection_never_mutates_params(builder, dataset):
    """`entidad_resolved` sigue siendo la identidad canónica después de armar.

    Si el builder sustituyera el canónico por el nombre físico, el follow-up
    heredaría "DAPRE" como identidad y el historial diría algo que el usuario
    nunca pidió.
    """
    params = {"entidad_resolved": DAPRE_CANONICAL}
    snapshot = json.dumps(params, sort_keys=True, ensure_ascii=False)

    builder.build_count(dataset, params)
    builder.build(dataset, params)

    assert json.dumps(params, sort_keys=True, ensure_ascii=False) == snapshot
    assert params["entidad_resolved"] == DAPRE_CANONICAL


# =====================================================================
# F/G. DUAL DATASET + DAPRE
# =====================================================================

def test_dapre_projects_physical_name_in_both_datasets(builder):
    assert builder.build_count(PROCESOS, {"entidad_resolved": DAPRE_CANONICAL}) == (
        "SELECT count(*) WHERE entidad = 'DAPRE'"
    )
    assert_identity(
        builder.build_count(CONTRATOS, {"entidad_resolved": DAPRE_CANONICAL}),
        CONTRATOS, "nombre_entidad = 'DAPRE'",
    )


def test_dapre_projection_reaches_analytics_builders(builder):
    """build_count no es el único consumidor: analytics comparte el WHERE."""
    for method in ("build_top_entities", "build_value_stats", "build_top_modalities"):
        soql = getattr(builder, method)(PROCESOS, {"entidad_resolved": DAPRE_CANONICAL})
        assert "entidad = 'DAPRE'" in soql, method


# Los cuatro splits que V1 confirmó y dejó en REVIEW. ADR-074 acreditó después
# el merge atómico y sus bindings exactos; quedan aquí como guarda acumulativa.
V1_VERIFIED_CANONICALS = (
    "MUNICIPIO DE SOACHA",
    "MUNICIPIO DE YOPAL",
    "FONDO FINANCIERO DISTRITAL DE SALUD",
    "UNIDAD ADMINISTRATIVA ESPECIAL DE SERVICIOS PÚBLICOS",
)


def test_sidecar_keeps_v1_dapre_and_accumulates_reviewed_bindings():
    """El sidecar es acumulativo; DAPRE y los merges posteriores conviven.

    El oráculo es de pertenencia, no de censo: afirmar un total obliga a cada
    lote posterior a editar este archivo, y ese conteo nunca fue lo que V1
    quiso proteger. Lo que V1 protege es que DAPRE siga proyectando; el lote
    ADR-074 protege que los cuatro splits ya acreditados también lo hagan.
    """
    registry = load_registry()
    assert registry.project(DAPRE_CANONICAL, PROCESOS) == ("DAPRE",)
    assert registry.project("MUNICIPIO DE SOACHA.", PROCESOS) is None
    for canonical in V1_VERIFIED_CANONICALS:
        assert registry.binding_for(canonical) is not None, canonical
        assert registry.project(canonical, PROCESOS)
        assert registry.project(canonical, CONTRATOS)


def test_project_buyer_names_is_accent_and_case_insensitive_on_the_lookup():
    """La BÚSQUEDA por identidad tolera grafía; el RESULTADO es exacto."""
    assert project_buyer_names(DAPRE_CANONICAL.lower(), PROCESOS) == ("DAPRE",)
    assert project_buyer_names(
        "DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPUBLICA", PROCESOS,
    ) == ("DAPRE",)


def test_unknown_identity_returns_none_not_empty_tuple():
    assert project_buyer_names("ENTIDAD QUE NO EXISTE", PROCESOS) is None
    assert project_buyer_names("", PROCESOS) is None


def test_bound_identity_in_unbound_dataset_returns_none():
    registry = parse_registry(_sidecar(
        canonical_name="SOLO PROCESOS", source_names={PROCESOS: ["SOLO P"]},
    ))
    assert registry.project("SOLO PROCESOS", PROCESOS) == ("SOLO P",)
    assert registry.project("SOLO PROCESOS", CONTRATOS) is None


# =====================================================================
# VALIDACIÓN DEL SIDECAR — fail fast, nunca silently malformed
# =====================================================================

# La procedencia de proyección vive en projection_evidence por BuyerUnit.
_ROOT_KEYS = ("schema_version", "authority")
_PROJECTION_KEYS = ("observed_at", "evidence", "observed_rows")
_UNSET = object()


def _sidecar(**overrides):
    """Un sidecar Schema 3 válido de una BuyerUnit.

    Que el caso base sea válido es lo que da valor a los negativos: si al
    payload le faltara metadata de raíz, cada caso reventaría en la validación
    de procedencia antes de llegar a su causa declarada, y la parametrización
    probaría una sola cosa doce veces.
    """
    root = {
        "schema_version": SCHEMA_VERSION,
        "authority": "ADR-073",
    }
    record = {
        "canonical_name": "X",
        "source_names": {PROCESOS: ["NAME"]},
        "projection_evidence": {
            "observed_at": "2026-09-02",
            "evidence": "artifacts/audits/source_name_projection_v1.json",
        },
        "fiscal_evidence": [],
    }
    for key, value in overrides.items():
        target = root if key in _ROOT_KEYS else (
            record["projection_evidence"] if key in _PROJECTION_KEYS else record
        )
        if value is _UNSET:
            target.pop(key, None)
        else:
            target[key] = value
    return {**root, "buyer_units": [record]}


def test_the_negative_baseline_is_actually_valid():
    """Control positivo: sin esto los negativos no prueban lo que dicen."""
    registry = parse_registry(_sidecar())
    assert len(registry) == 1
    assert registry.provenance.schema_version == SCHEMA_VERSION


@pytest.mark.parametrize("payload,expected_fragment", [
    # --- contratos ya vigentes en V1 ---
    (_sidecar(canonical_name=""), "canonical_name"),
    (_sidecar(canonical_name=_UNSET), "canonical_name"),
    (_sidecar(identity_key={}), "identity_key"),
    (_sidecar(source_names={}), "source_names"),
    (_sidecar(source_names={"no-existe": ["NAME"]}), "dataset desconocido"),
    (_sidecar(source_names={PROCESOS: []}), "lista vacía"),
    (_sidecar(source_names={PROCESOS: [""]}), PROCESOS),
    (_sidecar(source_names={PROCESOS: ["NAME", "name"]}), "duplicados"),
    (_sidecar(source_names={PROCESOS: "NAME"}), "deben venir en una lista"),
    # --- contratos de metadata que ADR-073 exigía y V1 no validaba ---
    (_sidecar(schema_version=_UNSET), "schema_version"),
    # Schema 1 es el que declaraba procedencia acumulativa en la raíz: leerlo
    # con las reglas de hoy resucitaría exactamente el defecto que V2 cierra.
    (_sidecar(schema_version=1), "schema_version"),
    (_sidecar(schema_version=2), "schema_version"),
    (_sidecar(schema_version="3"), "schema_version"),
    (_sidecar(schema_version=True), "schema_version"),
    (_sidecar(evidence=_UNSET), "evidence"),
    (_sidecar(evidence=""), "evidence"),
    (_sidecar(evidence="   "), "evidence"),
    (_sidecar(evidence=17128), "evidence"),
    (_sidecar(authority=""), "authority"),
    (_sidecar(observed_at=_UNSET), "observed_at"),
    (_sidecar(observed_at=""), "observed_at"),
    (_sidecar(observed_at="2026-99-99"), "observed_at"),
    (_sidecar(observed_at="2026-02-30"), "observed_at"),
    (_sidecar(observed_at="02/09/2026"), "observed_at"),
    (_sidecar(observed_at="2026-9-2"), "observed_at"),
    # `date.fromisoformat` acepta estas dos y NO son YYYY-MM-DD: la forma
    # declarada la impone la regex, no el parser. Sin estos casos, quitar
    # la regex no haría caer ningún test.
    (_sidecar(observed_at="20260902"), "YYYY-MM-DD"),
    (_sidecar(observed_at="2026-W36-3"), "YYYY-MM-DD"),
    (_sidecar(observed_at=20260902), "observed_at"),
    # --- observed_rows: evidencia, pero coherente ---
    (_sidecar(observed_rows={"no-existe": 1}), "dataset desconocido"),
    (_sidecar(observed_rows={CONTRATOS: 1}), "no declara nombres"),
    (_sidecar(observed_rows={PROCESOS: -1}), "observed_rows"),
    (_sidecar(observed_rows={PROCESOS: "17128"}), "observed_rows"),
    (_sidecar(observed_rows={PROCESOS: True}), "observed_rows"),
    (_sidecar(observed_rows=[1, 2]), "observed_rows"),
    # --- raíz ---
    ({"schema_version": SCHEMA_VERSION, "buyer_units": "no-es-lista"}, "buyer_units"),
    ({}, "schema_version"),
    # Un `evidence` de raíz no se tolera en silencio: aceptarlo dejaría que un
    # lote futuro volviera a declarar una procedencia que no acredita a los
    # bindings anteriores, que es el defecto que V2 vino a cerrar.
    (
        {**_sidecar(), "evidence": "artifacts/audits/public_university_pilot_r2fa.json"},
        "'evidence' de raíz no existe en schema 3",
    ),
])
def test_malformed_sidecar_fails_loudly(payload, expected_fragment):
    """Falla, y falla por la razón declarada — no por otra que llegó antes."""
    with pytest.raises(BuyerSourceNameError, match=re.escape(expected_fragment)):
        parse_registry(payload)


def test_observed_rows_is_optional():
    """Ausente es válido: es evidencia, no contrato de proyección."""
    registry = parse_registry(_sidecar(observed_rows=_UNSET))
    assert registry.binding_for("X").projection_evidence.observed_rows == {}


def test_observed_metadata_is_not_query_semantics(builder):
    """La metadata no entra al WHERE por ninguna vía."""
    soql = builder.build_count(PROCESOS, {"entidad_resolved": DAPRE_CANONICAL})
    assert soql == "SELECT count(*) WHERE entidad = 'DAPRE'"
    assert "2026" not in soql
    assert "17128" not in soql


def test_provenance_is_exposed_for_audit():
    registry = load_registry()
    assert registry.provenance.schema_version == SCHEMA_VERSION
    assert registry.provenance.authority == "ADR-073"
    assert not hasattr(registry.provenance, "evidence")
    binding = registry.binding_for(DAPRE_CANONICAL)
    assert binding.projection_evidence.observed_at == date(2026, 9, 2)
    assert binding.projection_evidence.observed_rows == {PROCESOS: 17128, CONTRATOS: 4351}


# =====================================================================
# PROVENANCE V2 — la adjudicación, no la mera presencia del campo
# =====================================================================

# Qué artefacto acredita cada binding. Congelar la ADJUDICACIÓN y no sólo
# "evidence no vacío" es la diferencia entre trazabilidad y decoración: bajo la
# raíz única de V1 los cinco bindings apuntaban al artefacto del piloto
# universitario, que no contiene DAPRE ni su NIT 899999083. El campo estaba
# presente, con la forma correcta, y la afirmación era falsa.
EXPECTED_EVIDENCE = {
    DAPRE_CANONICAL: "artifacts/audits/source_name_projection_v1.json",
    "UNIATLANTICO": "artifacts/audits/public_university_pilot_r2fa.json",
    "UNIVERSIDAD DE LA AMAZONIA": "artifacts/audits/public_university_pilot_r2fa.json",
    "UNIVERSIDAD DE CÓRDOBA": "artifacts/audits/public_university_pilot_r2fa.json",
    "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA": (
        "artifacts/audits/public_university_pilot_r2fa.json"
    ),
}


@pytest.mark.parametrize("canonical,artifact", sorted(EXPECTED_EVIDENCE.items()))
def test_each_binding_names_the_artifact_that_accredits_it(canonical, artifact):
    """La procedencia es por binding y la adjudicación está congelada."""
    binding = load_registry().binding_for(canonical)
    assert binding is not None, canonical
    assert binding.projection_evidence.evidence == artifact


def test_dapre_provenance_is_not_the_university_pilot():
    """El defecto R2FA.1, fijado como regresión.

    El artefacto del piloto no menciona DAPRE ni su NIT: adjudicárselo sería
    afirmar una evidencia que no existe. Este test cae si alguien vuelve a
    colapsar los cinco bindings en una sola procedencia raíz.
    """
    dapre = load_registry().binding_for(DAPRE_CANONICAL)
    assert dapre.projection_evidence.evidence == "artifacts/audits/source_name_projection_v1.json"
    assert dapre.projection_evidence.evidence != "artifacts/audits/public_university_pilot_r2fa.json"


def test_no_binding_lacks_provenance():
    registry = load_registry()
    bindings = registry.bindings()
    # Sin este piso, un sidecar vacío pasaría el bucle sin probar nada.
    assert {binding.canonical_name for binding in bindings} >= set(EXPECTED_EVIDENCE)
    for binding in bindings:
        assert binding.projection_evidence.evidence.strip(), binding.canonical_name


def test_provenance_is_not_query_semantics():
    """`evidence` es trazabilidad: no toca el WHERE por ninguna vía."""
    soql = SoQLBuilder().build_count(PROCESOS, {"entidad_resolved": DAPRE_CANONICAL})
    assert soql == "SELECT count(*) WHERE entidad = 'DAPRE'"
    assert "artifacts" not in soql


def test_duplicate_canonical_is_rejected():
    payload = _sidecar()
    payload["buyer_units"].append(dict(payload["buyer_units"][0], canonical_name="x"))
    with pytest.raises(BuyerSourceNameError, match="canonical duplicado"):
        parse_registry(payload)


def test_missing_sidecar_yields_empty_registry_not_an_error(tmp_path):
    """Ausente es seguro (nadie se proyecta); malformado no lo es."""
    registry = load_registry(tmp_path / "no-existe.json")
    assert len(registry) == 0
    assert registry.project(DAPRE_CANONICAL, PROCESOS) is None
