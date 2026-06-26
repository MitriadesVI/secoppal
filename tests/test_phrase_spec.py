"""Contrato de datos de PhraseSpec (Commit 2): inmutable, serializable, genérico."""
from __future__ import annotations

import dataclasses

import pytest

from app.core.phrase_spec import PhraseSpec

SPEC = PhraseSpec(
    canonical="manual de contratacion",
    ordered_anchors=("manual", "contratacion"),
    variants=("manuales de contratacion",),
    max_gap_tokens=2,
)


def test_defaults():
    minimal = PhraseSpec("aire acondicionado", ("aire", "acondicionado"))
    assert minimal.variants == ()
    assert minimal.max_gap_tokens == 2


def test_frozen_is_immutable():
    with pytest.raises(dataclasses.FrozenInstanceError):
        SPEC.canonical = "otro"  # type: ignore[misc]


def test_hashable_and_value_equality():
    twin = PhraseSpec(
        canonical="manual de contratacion",
        ordered_anchors=("manual", "contratacion"),
        variants=("manuales de contratacion",),
    )
    assert SPEC == twin
    assert {SPEC, twin} == {SPEC}  # hashable + igualdad por valor


def test_serializable_round_trip():
    data = dataclasses.asdict(SPEC)
    assert PhraseSpec(**data) == SPEC
    # las colecciones son tuplas (inmutables), no listas
    assert isinstance(SPEC.ordered_anchors, tuple)
    assert isinstance(SPEC.variants, tuple)


def test_is_pure_data_no_behaviour():
    # el contrato no debe arrastrar lógica semántica (regex, soql, detección)
    public_methods = [m for m in dir(SPEC) if not m.startswith("_") and callable(getattr(SPEC, m))]
    assert public_methods == []
