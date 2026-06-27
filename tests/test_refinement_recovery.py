"""Etapa 4 — recuperación de refinamientos vacíos (delta + UX)."""
from __future__ import annotations

from app.core.refinement_recovery import (
    build_recovery_message,
    describe_delta,
    refinement_delta,
)


def test_delta_detects_added_filter():
    prev = {"objeto": ["mantenimiento", "ascensores"], "dataset": "procesos"}
    curr = {"objeto": ["mantenimiento", "ascensores"], "dataset": "procesos",
            "valor_min": 100000000, "valor_max": 150000000}
    delta = refinement_delta(prev, curr)
    assert delta == {"valor_min": 100000000, "valor_max": 150000000}


def test_delta_detects_changed_scope():
    prev = {"objeto": ["x"], "ciudad": "Bogota"}
    curr = {"objeto": ["x"], "ciudad": "Medellin"}
    assert refinement_delta(prev, curr) == {"ciudad": "Medellin"}


def test_delta_ignores_empty_values():
    prev = {"objeto": ["x"]}
    curr = {"objeto": ["x"], "valor_min": None, "ciudad": ""}
    assert refinement_delta(prev, curr) == {}


def test_describe_delta_human_labels():
    assert describe_delta({"valor_min": 1, "valor_max": 2}) == "valor mínimo y valor máximo"
    assert describe_delta({"ciudad": "Medellin"}) == "ciudad"


def test_recovery_message_names_the_filter_and_offers_undo():
    msg = build_recovery_message({"ciudad": "Medellin"})
    assert "ciudad" in msg
    assert "Deshacer" in msg
    assert "anterior sí tenía" in msg
