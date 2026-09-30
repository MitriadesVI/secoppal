"""FOUNDATION-PRESENTATION: results/groups carry source rows, rows carry publishable cards.

The display projection never mutates the source, never calls SECOP, never
dedupes, reorders or cuts, and reads no clock of its own: one reference
instant per turn.
"""

from __future__ import annotations

import re
from copy import deepcopy
from datetime import datetime

import pytest

import app.core.formatter as formatter_module
from app.core.contract_universe import compatible
from app.core.foundation_presentation import DUAL_EXPORT_UNSUPPORTED, export_fields
from tests.test_foundation_basic_fallback import _down, _harness
from tests.test_foundation_runtime import DATASET, NOW, RAW, ROWS, accepted
from tests.product_contract.harness import ProductHarness


class _NoClock(datetime):
    @classmethod
    def now(cls, tz=None):
        raise AssertionError("the display projection read its own clock")


def _group(universe):
    return {"universe": universe, "dataset": "", "rows": [], "total": 0, "total_quality": "exact",
            "state": "ok", "technical_reason": None, "fallback_attempted": False}


def test_foundation_turn_publishes_cards_over_untouched_source_rows_with_the_turn_instant(tmp_path, monkeypatch):
    source_rows = {DATASET: [dict(ROWS[DATASET][0], id_del_proceso=f"P{i}") for i in range(3)]}
    source_rows[DATASET].append(dict(source_rows[DATASET][0]))  # an exact duplicate stays published
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=deepcopy(source_rows),
                             reference_instant=NOW, today=NOW.date(),
                             foundation_producer=lambda raw: accepted(raw),
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    monkeypatch.setattr(formatter_module, "datetime", _NoClock)
    turn = harness.say(RAW)
    result = turn.result
    source = harness.transport.snapshot()[0]["result"]
    calls = len(turn.transport_calls)
    assert result["results"] == result["groups"][0]["rows"] == source
    assert [card["id_del_proceso"] for card in result["rows"]] == result["groups"][0]["row_ids"]
    assert len(result["rows"]) == len(source) and all(card["titulo"] == "aseo" for card in result["rows"])
    assert not any(f"Referencia: {row['id_del_proceso']}" in result["response"] for row in source)
    assert "1. aseo" in result["response"]  # a text channel carries the cards themselves
    assert len(turn.transport_calls) == calls  # presentation made no source call of its own


def test_basic_fallback_publishes_cards_and_keeps_source_rows(tmp_path, monkeypatch):
    row = {"id_del_proceso": "CO1.REQ.1", "referencia_del_proceso": "LP-1", "nombre_del_procedimiento": "Servicio de aseo",
           "entidad": "MUNICIPIO DE SOLEDAD", "precio_base": "3500000", "estado_de_apertura_del_proceso": "Abierto",
           "estado_del_procedimiento": "Publicado", "fecha_de_publicacion_del": "2026-09-01T00:00:00.000"}
    harness = _harness(tmp_path, monkeypatch, _down(TimeoutError()), rows={"p6dx-8zbt": [row], "jbjy-vk9h": []})
    monkeypatch.setattr(formatter_module, "datetime", _NoClock)  # processes need the turn instant
    result = harness.say("procesos de aseo").result
    assert result["route_reason"] == "foundation_basic_fallback"
    assert [r["id_del_proceso"] for r in result["results"]] == [c["id_del_proceso"] for c in result["rows"]]
    card = result["rows"][0]
    assert card["titulo"] == "Servicio de aseo" and card["entidad"] == "MUNICIPIO DE SOLEDAD"
    assert card["valor"] == "$3,500,000" and "precio_base" not in card
    assert "Referencia: CO1" not in result["response"]


_EXACT_SOURCE = {
    "jbjy-vk9h": {"id_contrato": "CO1.PCCNTR.9", "objeto_del_contrato": "aseo", "nombre_entidad": "MUNICIPIO DE SOLEDAD",
                  "valor_del_contrato": "9007199254740993", "urlproceso": None, "estado_contrato": "En ejecución",
                  "fecha_de_firma": "2026-08-01"},
    "p6dx-8zbt": {"id_del_proceso": "CO1.REQ.7", "nombre_del_procedimiento": "aseo", "entidad": "MUNICIPIO DE SOLEDAD",
                  "precio_base": "1500000000", "urlproceso": None, "estado_del_procedimiento": "Publicado",
                  "fecha_de_publicacion_del": "2026-09-01T00:00:00.000"},
}


@pytest.mark.parametrize("raw,dataset", [("contratos de aseo", "jbjy-vk9h"), ("procesos de aseo", "p6dx-8zbt")])
def test_basic_fallback_results_are_the_transport_rows_untouched(tmp_path, monkeypatch, raw, dataset):
    # Z1: the legacy normalization turned exact strings into floats and None into synthesized URLs.
    rows = {"p6dx-8zbt": [], "jbjy-vk9h": [], dataset: [deepcopy(_EXACT_SOURCE[dataset])]}
    harness = _harness(tmp_path, monkeypatch, _down(TimeoutError()), rows=rows)
    turn = harness.say(raw, channel="streamlit")
    source = [row for call in turn.transport_calls if call["operation"] == "query" for row in call["result"]]
    assert turn.result["route_reason"] == "foundation_basic_fallback"
    assert turn.result["results"] == source and len(source) == 1
    published = turn.result["results"][0]
    for key, value in _EXACT_SOURCE[dataset].items():  # same value AND same type, no float, no URL
        assert published[key] == value and type(published[key]) is type(value), key
    assert len(turn.result["rows"]) == 1 and turn.result["rows"][0]["valor"] != ""


@pytest.mark.parametrize("channel,announced", [("telegram", 25), ("whatsapp", 25), ("streamlit", 30)])
def test_basic_fallback_announces_the_cards_it_actually_shows(tmp_path, monkeypatch, channel, announced):
    # Z2: a text channel cut to the formatter limit while the head announced every card.
    rows = {"jbjy-vk9h": [], "p6dx-8zbt": [dict(_EXACT_SOURCE["p6dx-8zbt"], id_del_proceso=f"CO1.REQ.{i}",
                                                 nombre_del_procedimiento=f"aseo {i}") for i in range(30)]}
    harness = _harness(tmp_path, monkeypatch, _down(TimeoutError()), rows=rows)
    harness.flow.formatter.max_results = 25
    result = harness.say("procesos de aseo", channel=channel).result
    assert result["route_reason"] == "foundation_basic_fallback" and len(result["rows"]) == 30
    assert f"te muestro {announced}." in result["response"]
    in_message = re.findall(r"^\d+\. ", result["response"], re.M)
    assert len(in_message) == (0 if channel == "streamlit" else announced)


@pytest.mark.parametrize("universes,expected", [
    (["contracts"], "stamp"),
    (["processes"], "none"),
    (["processes", "contracts"], "blocked"),
])
def test_export_is_stamped_for_contracts_and_explicitly_blocked_for_dual(universes, expected):
    fields = export_fields([_group(universe) for universe in universes])
    if expected == "stamp":
        assert set(fields) == {"export_params"}
        assert compatible({**fields["export_params"], "dataset": "jbjy-vk9h"})
    elif expected == "blocked":
        assert fields == {"export_notice": DUAL_EXPORT_UNSUPPORTED}
    else:
        assert fields == {}
