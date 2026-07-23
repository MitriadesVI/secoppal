"""Loader fail-closed, matriz semántica y normalización del lexicón M2-R1.1."""
from __future__ import annotations

import copy
from pathlib import Path
from types import MappingProxyType

import pytest

import app.core.modality_lexicon as modality_lexicon_module
from app.core.modality_lexicon import (
    LexiconValidationError,
    ModalityLexicon,
    build_lexicon,
    load_lexicon,
    normalize_phrase,
    normalize_with_map,
)
from app.core.modality_models import load_catalog


@pytest.fixture(scope="module")
def catalog():
    return load_catalog()


def _valid_data() -> dict:
    return {
        "version": 1,
        "rules": [
            {
                "rule_id": "lic_family",
                "phrases": ["licitación pública", "licitaciones públicas"],
                "target_kind": "retrieval_family",
                "target_id": "licitacion_publica",
                "match_strength": "exact_legal",
                "confidence": "high",
                "explicit_intent": False,
                "assumption_code": None,
                "notes": "",
            },
            {
                "rule_id": "amp",
                "phrases": ["acuerdo marco"],
                "target_kind": "retrieval_family",
                "target_id": "acuerdo_marco",
                "match_strength": "exact_legal",
                "confidence": "high",
                "explicit_intent": True,
                "assumption_code": None,
                "notes": "",
            },
            {
                "rule_id": "amb_subasta",
                "phrases": ["subasta"],
                "target_kind": "ambiguity",
                "candidate_target_ids": [
                    "seleccion_abreviada_subasta_inversa", "enajenacion_subasta",
                ],
                "ambiguity_code": "BARE_SUBASTA_MULTIPLE_LEGAL_MEANINGS",
                "match_strength": "contextual",
                "confidence": "low",
                "explicit_intent": False,
                "notes": "",
            },
            {
                "rule_id": "q_precalif",
                "phrases": ["con precalificación"],
                "target_kind": "qualifier",
                "allowed_retrieval_families": ["concurso_de_meritos"],
                "qualifier_type": "procedure_variant",
                "qualifier_value": "con_precalificacion",
                "attachment_direction": "after",
                "max_attachment_tokens": 6,
                "match_strength": "contextual",
                "confidence": "high",
                "explicit_intent": False,
                "notes": "",
            },
            {
                "rule_id": "qset_ofertas",
                "phrases": ["con o sin ofertas"],
                "target_kind": "qualifier_set",
                "allowed_retrieval_families": ["contratacion_directa"],
                "qualifier_type": "submission_mode",
                "qualifier_values": ["con_ofertas", "standard"],
                "attachment_direction": "after",
                "max_attachment_tokens": 6,
                "match_strength": "contextual",
                "confidence": "high",
                "explicit_intent": False,
                "notes": "",
            },
        ],
    }


def _build(data: dict, catalog) -> ModalityLexicon:
    return build_lexicon(data, catalog)


def _rule(data: dict, rule_id: str) -> dict:
    return next(rule for rule in data["rules"] if rule["rule_id"] == rule_id)


def test_valid_minimal_lexicon_builds(catalog) -> None:
    lexicon = _build(_valid_data(), catalog)
    assert lexicon.rule_count() == 5
    assert lexicon.phrase_count() == 6
    assert lexicon.composition_target(
        "concurso_de_meritos", "procedure_variant", "con_precalificacion"
    ) == "concurso_meritos_precalificacion"
    assert lexicon.composition_targets(
        "contratacion_directa", "submission_mode", ("standard", "con_ofertas")
    ) == ("contratacion_directa", "contratacion_directa_con_ofertas")


def test_default_lexicon_loads_and_is_consistent(catalog) -> None:
    lexicon = load_lexicon(catalog=catalog)
    retrieval_families = {procedure.retrieval_family for procedure in catalog.procedures}
    assert lexicon.rule_count() >= 25
    for rule in lexicon.rules:
        assert all(phrase == normalize_phrase(phrase) for phrase in rule.phrases)
        if rule.target_kind == "retrieval_family":
            assert rule.target_id in retrieval_families
            assert rule.target_id != "platform_test"
        elif rule.target_kind == "procedure":
            assert catalog.by_id(rule.target_id) is not None
        elif rule.target_kind == "qualifier":
            assert rule.target_id is None
            assert rule.qualifier_type in {"submission_mode", "procedure_variant"}
            assert rule.qualifier_value
            for family in rule.allowed_retrieval_families:
                target = lexicon.composition_target(
                    family, rule.qualifier_type, rule.qualifier_value
                )
                assert catalog.by_id(target) is not None
        elif rule.target_kind == "qualifier_set":
            assert rule.target_id is None and rule.qualifier_value is None
            assert len(rule.qualifier_values) >= 2
            for family in rule.allowed_retrieval_families:
                targets = lexicon.composition_targets(
                    family, rule.qualifier_type, rule.qualifier_values
                )
                assert len(targets) == len(rule.qualifier_values)
                assert all(catalog.by_id(target) is not None for target in targets)
        else:
            assert all(catalog.by_id(candidate) is not None for candidate in rule.candidate_target_ids)


def test_default_composition_matrix_is_exact(catalog) -> None:
    lexicon = load_lexicon(catalog=catalog)
    assert dict(lexicon.qualifier_targets) == {
        ("contratacion_directa", "submission_mode", "con_ofertas"): "contratacion_directa_con_ofertas",
        ("regimen_especial", "submission_mode", "con_ofertas"): "regimen_especial_con_ofertas",
        ("contratacion_directa", "submission_mode", "standard"): "contratacion_directa",
        ("regimen_especial", "submission_mode", "standard"): "regimen_especial",
        ("concurso_de_meritos", "procedure_variant", "con_precalificacion"): "concurso_meritos_precalificacion",
        ("concurso_de_meritos", "procedure_variant", "abierto"): "concurso_meritos_abierto",
    }


def test_default_qualifier_set_matrix_is_exact(catalog) -> None:
    lexicon = load_lexicon(catalog=catalog)
    assert dict(lexicon.qualifier_set_targets) == {
        (
            "contratacion_directa", "submission_mode", ("con_ofertas", "standard")
        ): ("contratacion_directa", "contratacion_directa_con_ofertas"),
        (
            "regimen_especial", "submission_mode", ("con_ofertas", "standard")
        ): ("regimen_especial", "regimen_especial_con_ofertas"),
    }


def test_negative_prequalification_qualifier_is_explicit_and_canonical(catalog) -> None:
    lexicon = load_lexicon(catalog=catalog)
    rule = next(rule for rule in lexicon.rules if rule.rule_id == "qualifier_sin_precalificacion")
    assert rule.qualifier_type == "procedure_variant"
    assert rule.qualifier_value == "abierto"
    assert rule.allowed_retrieval_families == ("concurso_de_meritos",)
    assert lexicon.composition_target(
        "concurso_de_meritos", "procedure_variant", "abierto"
    ) == "concurso_meritos_abierto"


def test_phrase_index_and_composition_index_are_immutable(catalog) -> None:
    lexicon = load_lexicon(catalog=catalog)
    assert isinstance(lexicon.phrase_index, MappingProxyType)
    assert isinstance(lexicon.qualifier_targets, MappingProxyType)
    assert isinstance(lexicon.qualifier_set_targets, MappingProxyType)
    with pytest.raises(TypeError):
        lexicon.phrase_index["nuevo"] = lexicon.rules[0]
    with pytest.raises(TypeError):
        lexicon.qualifier_targets[("x", "y", "z")] = "id"
    with pytest.raises(TypeError):
        lexicon.qualifier_set_targets[("x", "y", ("a", "b"))] = ("id",)


def test_phrase_index_is_bijective(catalog) -> None:
    lexicon = load_lexicon(catalog=catalog)
    assert len(lexicon.phrase_index) == sum(len(rule.phrases) for rule in lexicon.rules)


# ── Mutation tests generales ────────────────────────────────────────────────


def test_mut_duplicate_rule_id(catalog) -> None:
    data = _valid_data()
    data["rules"][1]["rule_id"] = "lic_family"
    with pytest.raises(LexiconValidationError, match="rule_id duplicado"):
        _build(data, catalog)


def test_mut_duplicate_alias_across_rules(catalog) -> None:
    data = _valid_data()
    data["rules"][1]["phrases"] = ["licitación pública"]
    with pytest.raises(LexiconValidationError, match="alias conflictivo"):
        _build(data, catalog)


def test_mut_duplicate_phrase_after_normalization(catalog) -> None:
    data = _valid_data()
    data["rules"][0]["phrases"] = ["licitación pública", "LICITACIÓN  PÚBLICA"]
    with pytest.raises(LexiconValidationError, match="duplicada tras normalización"):
        _build(data, catalog)


@pytest.mark.parametrize(
    "field,value,pattern",
    [
        ("match_strength", "inventada", "match_strength desconocido"),
        ("confidence", "altisima", "confidence desconocida"),
        ("target_kind", "modalidad", "target_kind desconocido"),
    ],
)
def test_mut_unknown_vocabulary(catalog, field, value, pattern) -> None:
    data = _valid_data()
    data["rules"][0][field] = value
    with pytest.raises(LexiconValidationError, match=pattern):
        _build(data, catalog)


def test_mut_explicit_intent_not_bool(catalog) -> None:
    data = _valid_data()
    data["rules"][0]["explicit_intent"] = "false"
    with pytest.raises(LexiconValidationError, match="booleano real"):
        _build(data, catalog)


def test_mut_nonexistent_family_target(catalog) -> None:
    data = _valid_data()
    data["rules"][0]["target_id"] = "familia_fantasma"
    with pytest.raises(LexiconValidationError, match="retrieval_family inexistente"):
        _build(data, catalog)


def test_mut_nonexistent_procedure_target(catalog) -> None:
    data = _valid_data()
    data["rules"][0].update({
        "target_kind": "procedure",
        "target_id": "procedimiento_fantasma",
    })
    with pytest.raises(LexiconValidationError, match="procedure_id inexistente"):
        _build(data, catalog)


def test_mut_platform_test_family_exposed(catalog) -> None:
    data = _valid_data()
    data["rules"][0]["target_id"] = "platform_test"
    with pytest.raises(LexiconValidationError, match="no es buscable"):
        _build(data, catalog)


def test_mut_explicit_only_family_without_intent(catalog) -> None:
    data = _valid_data()
    _rule(data, "amp")["explicit_intent"] = False
    with pytest.raises(LexiconValidationError, match="explicit_intent=true"):
        _build(data, catalog)


def test_mut_explicit_only_procedure_without_intent(catalog) -> None:
    data = _valid_data()
    data["rules"][0].update({
        "target_kind": "procedure",
        "target_id": "enajenacion_subasta",
    })
    with pytest.raises(LexiconValidationError, match="explicit_intent=true"):
        _build(data, catalog)


# ── Matriz estricta por target_kind ─────────────────────────────────────────

@pytest.mark.parametrize(
    "rule_id,field,value",
    [
        ("q_precalif", "target_id", "concurso_meritos_precalificacion"),
        ("q_precalif", "assumption_code", "X"),
        ("amb_subasta", "target_id", "enajenacion_subasta"),
        ("amb_subasta", "assumption_code", "X"),
        ("lic_family", "qualifier_type", "submission_mode"),
        ("lic_family", "candidate_target_ids", ["licitacion_publica"]),
    ],
)
def test_mut_incompatible_fields_rejected(catalog, rule_id, field, value) -> None:
    data = _valid_data()
    _rule(data, rule_id)[field] = value
    with pytest.raises(LexiconValidationError, match="campos incompatibles"):
        _build(data, catalog)


def test_mut_ambiguity_without_candidates(catalog) -> None:
    data = _valid_data()
    _rule(data, "amb_subasta").pop("candidate_target_ids")
    with pytest.raises(LexiconValidationError, match="candidate_target_ids"):
        _build(data, catalog)


def test_mut_ambiguity_candidate_nonexistent(catalog) -> None:
    data = _valid_data()
    _rule(data, "amb_subasta")["candidate_target_ids"] = ["fantasma"]
    with pytest.raises(LexiconValidationError, match="inexistente"):
        _build(data, catalog)


def test_mut_ambiguity_candidate_not_searchable(catalog) -> None:
    data = _valid_data()
    _rule(data, "amb_subasta")["candidate_target_ids"] = ["subasta_de_prueba"]
    with pytest.raises(LexiconValidationError, match="no buscable"):
        _build(data, catalog)


@pytest.mark.parametrize(
    "field",
    [
        "qualifier_type", "qualifier_value", "allowed_retrieval_families",
        "attachment_direction", "max_attachment_tokens",
    ],
)
def test_mut_qualifier_missing_required_field(catalog, field) -> None:
    data = _valid_data()
    _rule(data, "q_precalif").pop(field)
    with pytest.raises(LexiconValidationError, match=field):
        _build(data, catalog)


@pytest.mark.parametrize("value", [0, 13, True, "6", None])
def test_mut_qualifier_bad_max_attachment_tokens(catalog, value) -> None:
    data = _valid_data()
    _rule(data, "q_precalif")["max_attachment_tokens"] = value
    with pytest.raises(LexiconValidationError, match="max_attachment_tokens"):
        _build(data, catalog)


def test_mut_qualifier_unknown_type(catalog) -> None:
    data = _valid_data()
    _rule(data, "q_precalif")["qualifier_type"] = "otro"
    with pytest.raises(LexiconValidationError, match="qualifier_type desconocido"):
        _build(data, catalog)


def test_mut_qualifier_unknown_direction(catalog) -> None:
    data = _valid_data()
    _rule(data, "q_precalif")["attachment_direction"] = "cerca"
    with pytest.raises(LexiconValidationError, match="attachment_direction"):
        _build(data, catalog)


def test_mut_qualifier_allowed_family_nonexistent(catalog) -> None:
    data = _valid_data()
    _rule(data, "q_precalif")["allowed_retrieval_families"] = ["fantasma"]
    with pytest.raises(LexiconValidationError, match="allowed_retrieval_families inexistente"):
        _build(data, catalog)


def test_mut_qualifier_composition_zero_results(catalog) -> None:
    data = _valid_data()
    _rule(data, "q_precalif")["qualifier_value"] = "no_existe"
    with pytest.raises(LexiconValidationError, match="exactamente un procedimiento"):
        _build(data, catalog)


# ── Qualifier sets fail-closed ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "field",
    [
        "qualifier_type", "qualifier_values", "allowed_retrieval_families",
        "attachment_direction", "max_attachment_tokens",
    ],
)
def test_mut_qualifier_set_missing_required_field(catalog, field) -> None:
    data = _valid_data()
    _rule(data, "qset_ofertas").pop(field)
    with pytest.raises(LexiconValidationError, match=field):
        _build(data, catalog)


def test_mut_qualifier_set_values_not_list(catalog) -> None:
    data = _valid_data()
    _rule(data, "qset_ofertas")["qualifier_values"] = "standard"
    with pytest.raises(LexiconValidationError, match="debe ser una lista"):
        _build(data, catalog)


def test_mut_qualifier_set_requires_two_values(catalog) -> None:
    data = _valid_data()
    _rule(data, "qset_ofertas")["qualifier_values"] = ["standard"]
    with pytest.raises(LexiconValidationError, match="al menos dos"):
        _build(data, catalog)


def test_mut_qualifier_set_rejects_duplicate_value(catalog) -> None:
    data = _valid_data()
    _rule(data, "qset_ofertas")["qualifier_values"] = ["standard", "standard"]
    with pytest.raises(LexiconValidationError, match="duplicado"):
        _build(data, catalog)


def test_mut_qualifier_set_combination_missing(catalog) -> None:
    data = _valid_data()
    _rule(data, "qset_ofertas")["qualifier_values"] = ["standard", "fantasma"]
    with pytest.raises(LexiconValidationError, match="exactamente un procedimiento"):
        _build(data, catalog)


def test_mut_qualifier_set_same_procedure_rejected(
    catalog, monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = _valid_data()
    original = modality_lexicon_module._qualifier_target

    def same_target(catalog_arg, family, qualifier_type, qualifier_value, ctx):
        if family == "contratacion_directa" and qualifier_type == "submission_mode":
            return "contratacion_directa"
        return original(catalog_arg, family, qualifier_type, qualifier_value, ctx)

    monkeypatch.setattr(modality_lexicon_module, "_qualifier_target", same_target)
    with pytest.raises(LexiconValidationError, match="procedimientos distintos"):
        _build(data, catalog)


def test_mut_qualifier_set_family_nonexistent(catalog) -> None:
    data = _valid_data()
    _rule(data, "qset_ofertas")["allowed_retrieval_families"] = ["fantasma"]
    with pytest.raises(LexiconValidationError, match="inexistente"):
        _build(data, catalog)


@pytest.mark.parametrize(
    "field,value",
    [
        ("target_id", "contratacion_directa"),
        ("candidate_target_ids", ["contratacion_directa"]),
        ("ambiguity_code", "X"),
        ("assumption_code", "X"),
        ("qualifier_value", "standard"),
    ],
)
def test_mut_qualifier_set_incompatible_fields(catalog, field, value) -> None:
    data = _valid_data()
    _rule(data, "qset_ofertas")[field] = value
    with pytest.raises(LexiconValidationError, match="campos incompatibles"):
        _build(data, catalog)


@pytest.mark.parametrize("value", [True, 0, -1, 13])
def test_mut_qualifier_set_bad_attachment_limit(catalog, value) -> None:
    data = _valid_data()
    _rule(data, "qset_ofertas")["max_attachment_tokens"] = value
    with pytest.raises(LexiconValidationError, match="max_attachment_tokens"):
        _build(data, catalog)


def test_mut_qualifier_set_bad_direction(catalog) -> None:
    data = _valid_data()
    _rule(data, "qset_ofertas")["attachment_direction"] = "cerca"
    with pytest.raises(LexiconValidationError, match="attachment_direction"):
        _build(data, catalog)


# ── Contextos y YAML estricto ───────────────────────────────────────────────


def test_mut_context_empty_after_normalization(catalog) -> None:
    data = _valid_data()
    _rule(data, "amb_subasta")["forbidden_context"] = ["\u0301"]
    with pytest.raises(LexiconValidationError, match="vacío tras normalización"):
        _build(data, catalog)


def test_mut_context_duplicate_after_normalization(catalog) -> None:
    data = _valid_data()
    _rule(data, "amb_subasta")["forbidden_context"] = ["privada", "PRIVADA"]
    with pytest.raises(LexiconValidationError, match="duplicado normalizado"):
        _build(data, catalog)


def test_mut_context_required_and_forbidden_overlap(catalog) -> None:
    data = _valid_data()
    rule = _rule(data, "amb_subasta")
    rule["required_context"] = ["arte"]
    rule["forbidden_context"] = ["ARTE"]
    with pytest.raises(LexiconValidationError, match="simultáneamente"):
        _build(data, catalog)


def test_mut_duplicate_yaml_key(catalog, tmp_path: Path) -> None:
    path = tmp_path / "duplicate.yaml"
    path.write_text(
        "version: 1\nrules:\n  - rule_id: r1\n    rule_id: r2\n",
        encoding="utf-8",
    )
    with pytest.raises(LexiconValidationError, match="clave YAML duplicada"):
        load_lexicon(path, catalog=catalog)




def test_mut_duplicate_root_yaml_key(catalog, tmp_path: Path) -> None:
    path = tmp_path / "duplicate_root.yaml"
    path.write_text(
        "version: 1\nversion: 2\nrules: []\n",
        encoding="utf-8",
    )
    with pytest.raises(LexiconValidationError, match="clave YAML duplicada"):
        load_lexicon(path, catalog=catalog)


def test_mut_invalid_yaml_root(catalog, tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("- uno\n- dos\n", encoding="utf-8")
    with pytest.raises(LexiconValidationError, match="mapping"):
        load_lexicon(path, catalog=catalog)


# ── Normalización y offsets ─────────────────────────────────────────────────

@pytest.mark.parametrize(
    "text,expected",
    [
        ("Licitación Pública", "licitacion publica"),
        ("SELECCIÓN-abreviada", "seleccion abreviada"),
        ("min.  cuantía", "min cuantia"),
        ("  régimen   especial  ", "regimen especial"),
        ("¿subasta?", "subasta"),
        ("acuerdo/marco", "acuerdo marco"),
        ("mínima\u00a0cuantía", "minima cuantia"),
        ("licitacio\u0301n pu\u0301blica", "licitacion publica"),
    ],
)
def test_normalize_phrase(text: str, expected: str) -> None:
    assert normalize_phrase(text) == expected


def test_normalize_map_roundtrip_unicode() -> None:
    text = "¿Hay SELECCIÓN\u00a0abreviada?"
    norm, pos = normalize_with_map(text)
    assert norm == "hay seleccion abreviada"
    start = norm.index("seleccion")
    end = len(norm)
    assert text[pos[start]:pos[end - 1] + 1] == "SELECCIÓN\u00a0abreviada"
