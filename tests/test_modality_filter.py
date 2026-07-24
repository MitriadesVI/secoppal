"""Tests del value object ejecutable ModalityFilter (M3B-1)."""
from __future__ import annotations

import json

import pytest

from app.core.modality_filter import ModalityFilter

_INCLUDE_KEY = "modalidad_include_procedure_ids"
_EXCLUDE_KEY = "modalidad_exclude_procedure_ids"


def test_empty_filter_is_empty_and_projects_nothing():
    f = ModalityFilter()
    assert f.is_empty()
    assert f.to_params() == {}


def test_include_only_projection():
    f = ModalityFilter(include_procedure_ids=("licitacion_publica",))
    assert not f.is_empty()
    assert f.to_params() == {_INCLUDE_KEY: ["licitacion_publica"]}
    assert _EXCLUDE_KEY not in f.to_params()


def test_exclude_only_projection():
    f = ModalityFilter(exclude_procedure_ids=("contratacion_directa",))
    assert not f.is_empty()
    assert f.to_params() == {_EXCLUDE_KEY: ["contratacion_directa"]}
    assert _INCLUDE_KEY not in f.to_params()


def test_include_and_exclude_projection():
    f = ModalityFilter(
        include_procedure_ids=("minima_cuantia",),
        exclude_procedure_ids=("contratacion_directa",),
    )
    assert f.to_params() == {
        _INCLUDE_KEY: ["minima_cuantia"],
        _EXCLUDE_KEY: ["contratacion_directa"],
    }


def test_deterministic_order_regardless_of_input_order():
    f = ModalityFilter(include_procedure_ids=("b", "a", "c"))
    assert f.include_procedure_ids == ("a", "b", "c")


def test_duplicate_ids_are_deduplicated():
    f = ModalityFilter(include_procedure_ids=("a", "a", "b", "b"))
    assert f.include_procedure_ids == ("a", "b")


def test_overlap_include_exclude_raises():
    with pytest.raises(ValueError):
        ModalityFilter(
            include_procedure_ids=("a", "b"),
            exclude_procedure_ids=("b", "c"),
        )


def test_bool_is_not_accepted_as_id():
    with pytest.raises(ValueError):
        ModalityFilter(include_procedure_ids=(True,))


def test_non_string_id_rejected():
    with pytest.raises(ValueError):
        ModalityFilter(include_procedure_ids=(1,))


def test_empty_or_whitespace_id_rejected():
    with pytest.raises(ValueError):
        ModalityFilter(include_procedure_ids=("",))
    with pytest.raises(ValueError):
        ModalityFilter(include_procedure_ids=("   ",))


def test_string_value_rejected_as_collection():
    # Un string es iterable de chars; debe rechazarse explícitamente.
    with pytest.raises(ValueError):
        ModalityFilter(include_procedure_ids="licitacion_publica")  # type: ignore[arg-type]


@pytest.mark.parametrize("include,exclude", [
    ((), ()),
    (("a",), ()),
    ((), ("z",)),
    (("a", "b"), ("y", "z")),
])
def test_params_roundtrip(include, exclude):
    original = ModalityFilter(include_procedure_ids=include, exclude_procedure_ids=exclude)
    restored = ModalityFilter.from_params(original.to_params())
    assert restored == original


def test_from_params_missing_keys_is_empty():
    assert ModalityFilter.from_params({}).is_empty()
    assert ModalityFilter.from_params({"otro": 1}).is_empty()


def test_from_params_non_list_value_rejected():
    with pytest.raises(ValueError):
        ModalityFilter.from_params({_INCLUDE_KEY: "licitacion_publica"})
    with pytest.raises(ValueError):
        ModalityFilter.from_params({_EXCLUDE_KEY: ("tuple", "no", "lista")})


@pytest.mark.parametrize("params", [None, [], "params", b"params", True])
def test_from_params_requires_real_dict(params):
    with pytest.raises(ValueError, match="params debe ser dict"):
        ModalityFilter.from_params(params)  # type: ignore[arg-type]


def test_from_params_rejects_dict_used_as_collection():
    with pytest.raises(ValueError, match="debe ser una lista"):
        ModalityFilter.from_params({_INCLUDE_KEY: {"licitacion_publica": True}})


@pytest.mark.parametrize("bad_id", [
    " licitacion_publica", "licitacion_publica ", " licitacion_publica ",
])
def test_external_spaces_are_rejected_not_normalized(bad_id):
    with pytest.raises(ValueError, match="espacios externos"):
        ModalityFilter.from_params({_INCLUDE_KEY: [bad_id]})


@pytest.mark.parametrize("bad_id", [b"id", True, 1, None, {}])
def test_from_params_rejects_non_string_ids(bad_id):
    with pytest.raises(ValueError):
        ModalityFilter.from_params({_EXCLUDE_KEY: [bad_id]})


def test_no_legacy_scalar_modalidad_key_emitted():
    f = ModalityFilter(include_procedure_ids=("licitacion_publica",))
    assert "modalidad" not in f.to_params()


def test_projection_is_json_serializable():
    f = ModalityFilter(
        include_procedure_ids=("a", "b"),
        exclude_procedure_ids=("z",),
    )
    blob = json.dumps(f.to_params())
    assert json.loads(blob) == f.to_params()


def test_deterministic_equality_and_hash():
    a = ModalityFilter(include_procedure_ids=("b", "a"))
    b = ModalityFilter(include_procedure_ids=("a", "b"))
    assert a == b
    assert hash(a) == hash(b)
    assert len({a, b}) == 1
