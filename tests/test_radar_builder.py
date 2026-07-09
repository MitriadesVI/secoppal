"""Tests de app/core/radar/builder.py — sanitización y guardado compartido."""
from __future__ import annotations

import json

import pytest

from app.core.radar.builder import (
    has_scope_or_topic,
    sanitize_for_save,
    save_radar_from_params,
)
from app.core.radar.store import RadarExistsError, RadarStore


@pytest.fixture
def store(tmp_path):
    return RadarStore(tmp_path / "builder.db")


def test_sanitize_drops_ecosystem_and_resolution():
    params = {
        "objeto": ["cultura"],
        "departamento_resolved": "Cesar",
        "departamento_resolution": {"method": "scan_exact"},
        "entidad_ecosystem": ["GOBERNACIÓN DEL CESAR"],
        "valor_min": 500,
    }
    clean = sanitize_for_save(params)
    assert "entidad_ecosystem" not in clean
    assert "departamento_resolution" not in clean
    assert clean["departamento_resolved"] == "Cesar"
    assert clean["valor_min"] == 500


def test_sanitize_drops_cursor_keys():
    params = {"objeto": ["x"], "offset": 50, "_display_offset": 25, "_shown_total": 9}
    clean = sanitize_for_save(params)
    assert "offset" not in clean
    assert "_display_offset" not in clean
    assert "_shown_total" not in clean


def test_sanitize_drops_ordering_signal_for_radar_window():
    params = {
        "objeto": ["x"],
        "departamento_resolved": "Cesar",
        "estado": "Abierto",
        "valor_min": 100,
        "dataset": "procesos",
        "ordering_signal": "valor_desc",
    }
    clean = sanitize_for_save(params)
    assert "ordering_signal" not in clean
    assert clean["objeto"] == ["x"]
    assert clean["departamento_resolved"] == "Cesar"
    assert clean["estado"] == "Abierto"
    assert clean["valor_min"] == 100
    assert clean["dataset"] == "procesos"


def test_has_scope_or_topic():
    assert has_scope_or_topic({"objeto": ["x"]})
    assert has_scope_or_topic({"departamento_resolved": "Cesar"})
    assert not has_scope_or_topic({"valor_min": 100})
    assert not has_scope_or_topic({})


def test_save_builds_clean_frame(store):
    params = {
        "objeto": ["cultura"],
        "departamento_resolved": "Cesar",
        "entidad_ecosystem": ["X"],
        "dataset": "contratos",
        "valor_min": 500000000,
        "ordering_signal": "valor_desc",
    }
    rid = save_radar_from_params(store, owner="u1", name="Cultura Cesar", params=params)
    radar = store.get_radar(rid)
    blob = json.dumps(radar.frame_dict(), ensure_ascii=False)
    assert "entidad_ecosystem" not in blob
    assert radar.frame_dict()["scope"]["departamento_resolved"] == "Cesar"
    assert radar.frame_dict()["topic"] == ["cultura"]
    assert radar.frame_dict()["modifiers"]["valor_min"] == 500000000
    assert "ordering_signal" not in radar.frame_dict()["modifiers"]
    assert radar.dataset == "contratos"


def test_save_duplicate_raises(store):
    params = {"departamento_resolved": "Cesar"}
    save_radar_from_params(store, owner="u1", name="Dup", params=params)
    with pytest.raises(RadarExistsError):
        save_radar_from_params(store, owner="u1", name="Dup", params=params)
