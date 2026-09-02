"""SOURCE_NAME_PROJECTION_V1 — ADR-073.

La identidad canónica y el string que cada dataset guarda son autoridades
distintas. Estos tests fijan las tres reglas y, sobre todo, impiden que la
comparación se relaje: bajo `LIKE` la grafía de un dataset alcanza la del otro
por substring y el defecto vuelve sin que ningún test caiga.
"""

from __future__ import annotations

import json

import pytest

from app.core.buyer_source_names import (
    BuyerSourceNameError,
    BuyerSourceNameRegistry,
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
    assert soql == expected


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
    assert builder.build_count(CONTRATOS, {"entidad_resolved": DAPRE_CANONICAL}) == (
        "SELECT count(*) WHERE nombre_entidad = 'DAPRE'"
    )


def test_dapre_projection_reaches_analytics_builders(builder):
    """build_count no es el único consumidor: analytics comparte el WHERE."""
    for method in ("build_top_entities", "build_value_stats", "build_top_modalities"):
        soql = getattr(builder, method)(PROCESOS, {"entidad_resolved": DAPRE_CANONICAL})
        assert "entidad = 'DAPRE'" in soql, method


def test_sidecar_binds_only_dapre_in_v1():
    """V1 liga una sola identidad. Soacha/Yopal/FFDS/UAESP siguen abiertos."""
    registry = load_registry()
    assert len(registry) == 1
    assert registry.project(DAPRE_CANONICAL, PROCESOS) == ("DAPRE",)
    assert registry.project("MUNICIPIO DE SOACHA.", PROCESOS) is None


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
    registry = BuyerSourceNameRegistry.__new__(BuyerSourceNameRegistry)
    registry = parse_registry({"bindings": [{
        "canonical_name": "SOLO PROCESOS",
        "identity_key": {"type": "NIT", "value": "1"},
        "sources": {PROCESOS: ["SOLO P"]},
    }]})
    assert registry.project("SOLO PROCESOS", PROCESOS) == ("SOLO P",)
    assert registry.project("SOLO PROCESOS", CONTRATOS) is None


# =====================================================================
# VALIDACIÓN DEL SIDECAR — fail fast, nunca silently malformed
# =====================================================================

def _binding(**overrides):
    record = {
        "canonical_name": "X",
        "identity_key": {"type": "NIT", "value": "1"},
        "sources": {PROCESOS: ["NAME"]},
    }
    record.update(overrides)
    return {"bindings": [record]}


@pytest.mark.parametrize("payload,reason", [
    (_binding(canonical_name=""), "canonical vacío"),
    (_binding(canonical_name=None), "canonical ausente"),
    (_binding(identity_key={}), "identity_key vacía"),
    (_binding(identity_key={"type": "NIT"}), "identity_key sin valor"),
    (_binding(sources={}), "sources vacío"),
    (_binding(sources={"no-existe": ["NAME"]}), "dataset desconocido"),
    (_binding(sources={PROCESOS: []}), "lista vacía"),
    (_binding(sources={PROCESOS: [""]}), "nombre vacío"),
    (_binding(sources={PROCESOS: ["NAME", "name"]}), "duplicado normalizado"),
    (_binding(sources={PROCESOS: "NAME"}), "tipo inválido"),
    ({"bindings": "no-es-lista"}, "bindings no es lista"),
    ({}, "sin bindings"),
])
def test_malformed_sidecar_fails_loudly(payload, reason):
    with pytest.raises(BuyerSourceNameError):
        parse_registry(payload)


def test_duplicate_identity_key_is_rejected():
    record = {
        "canonical_name": "A",
        "identity_key": {"type": "NIT", "value": "1"},
        "sources": {PROCESOS: ["A"]},
    }
    other = dict(record, canonical_name="B")
    with pytest.raises(BuyerSourceNameError):
        parse_registry({"bindings": [record, other]})


def test_missing_sidecar_yields_empty_registry_not_an_error(tmp_path):
    """Ausente es seguro (nadie se proyecta); malformado no lo es."""
    registry = load_registry(tmp_path / "no-existe.json")
    assert len(registry) == 0
    assert registry.project(DAPRE_CANONICAL, PROCESOS) is None
