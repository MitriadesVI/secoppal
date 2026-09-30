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
from hypothesis import given, strategies as st

import app.core.formatter as formatter_module
from app.core.contract_universe import compatible
from app.core.entity_resolver import EntityResolver
from app.core.foundation_admission import admit_proposal
from app.core.foundation_presentation import DUAL_EXPORT_UNSUPPORTED, export_fields, response_text, shown_card_groups
from app.core.foundation_producers import decode_proposal
from app.core.foundation_projection import SOURCE_SELECT_FIELDS
from app.core.soql_builder import SoQLBuilder
from tests.test_foundation_basic_fallback import _down, _harness
from tests.test_foundation_runtime import DATASET, NOW, RAW, ROWS, accepted
from tests.product_contract.harness import ProductHarness
from tests.product_contract.frozen_source import InjectedFailure


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


def _publication_admission(raw):
    requests, conditions = [], []
    subjects = sorted((("processes", "procesos"), ("contracts", "contratos")), key=lambda item: raw.index(item[1]))
    for index, (subject, noun) in enumerate(subjects):
        rid, start, topic = f"r{index}", raw.index(noun), raw.index("aseo")
        requests.append({"request_id": rid, "scope_id": "shared", "operation": "SEARCH", "subject": subject,
                         "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                         "universe_evidence": {"start": start, "end": start + len(noun), "quote": noun},
                         "condition_expression": {"operator": "ATOM", "condition_id": rid + ".c"}})
        conditions.append({"condition_id": rid + ".c", "request_ids": [rid], "dimension": "TOPIC",
                           "operator": "contains", "polarity": "POSITIVE",
                           "evidence": [{"start": topic, "end": topic + 4, "quote": "aseo"}]})
    body = {"requests": requests, "conditions": conditions,
            "relations": [{"kind": "SHARES_SCOPE", "members": [r["request_id"] for r in requests]}]}
    return admit_proposal(decode_proposal(body, raw, arm="C"), raw,
                          entity_resolver=EntityResolver("app/data/aliases_db.json"))


def _publication_rows(sizes):
    return {dataset: [{**dict.fromkeys({*SoQLBuilder.SPECS[dataset].select_fields,
                                      *SOURCE_SELECT_FIELDS[dataset]}), **_EXACT_SOURCE[dataset],
                      id_key: f"{prefix}{size - i:03d}", title_key: f"aseo {prefix}{i}"}
                      for i in range(size)]
            for (dataset, id_key, title_key, prefix), size in zip((
                ("p6dx-8zbt", "id_del_proceso", "nombre_del_procedimiento", "P"),
                ("jbjy-vk9h", "id_contrato", "objeto_del_contrato", "C")), sizes)}


@pytest.mark.parametrize("channel", ["telegram", "whatsapp"])
def test_dual_public_response_has_one_global_text_budget(tmp_path, monkeypatch, channel):
    rows = _publication_rows((30, 30))
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=deepcopy(rows), reference_instant=NOW,
                             today=NOW.date(), foundation_producer=_publication_admission,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    harness.flow.formatter.max_results = 25
    result = harness.say("procesos y contratos de aseo", channel=channel).result
    numbered = re.findall(r"^\d+\. (aseo [PC]\d+)$", result["response"], re.M)
    assert len(numbered) == 25
    assert numbered == [f"aseo P{i}" for i in range(13)] + [f"aseo C{i}" for i in range(12)]
    assert "Procesos: encontré 30. Te muestro los 13 más recientes." in result["response"]
    assert "Contratos: encontré 30. Te muestro los 12 más recientes." in result["response"]


@pytest.mark.parametrize("channel", ["telegram", "whatsapp", "streamlit"])
@pytest.mark.parametrize("sizes,reverse,source_error,expected", [
    ((30, 30), False, False, (13, 12)), ((2, 30), False, False, (2, 23)),
    ((0, 30), False, False, (0, 25)), ((10, 10), False, False, (10, 10)),
    ((30, 30), True, False, (13, 12)), ((30, 30), False, True, (0, 25)),
])
def test_public_workflow_budget_keeps_all_evidence(tmp_path, monkeypatch, channel, sizes, reverse,
                                                 source_error, expected):
    rows = _publication_rows(sizes)
    original = deepcopy(rows)
    schemas = {dataset: dict.fromkeys({*SoQLBuilder.SPECS[dataset].select_fields,
                                      *SOURCE_SELECT_FIELDS[dataset]}, "TEXT")
               for dataset in rows}
    failures = [InjectedFailure(lambda: ValueError("source unavailable"), dataset="p6dx-8zbt",
                                operation="query")] if source_error else []
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows, schemas=schemas, failures=failures,
                             reference_instant=NOW, today=NOW.date(), foundation_producer=_publication_admission,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    harness.flow.formatter.max_results = 25
    raw = "contratos y procesos de aseo" if reverse else "procesos y contratos de aseo"
    turn = harness.say(raw, channel=channel)
    result, groups = turn.result, turn.result["groups"]
    assert rows == original
    assert [group["universe"] for group in groups] == (["contracts", "processes"] if reverse
                                                      else ["processes", "contracts"])
    sources = {call["dataset"]: call["result"] for call in turn.transport_calls
               if call["operation"] == "query" and "result" in call}
    assert result["results"] == [row for group in groups for row in group["rows"]]
    assert len(result["rows"]) == sum(len(group["rows"]) for group in groups)
    for group in groups:
        assert group["rows"] == sources.get(group["dataset"], [])
    numbered = re.findall(r"^\d+\. (aseo [PC]\d+)$", result["response"], re.M)
    if channel == "streamlit":
        assert numbered == []
    else:
        prefixes = ("C", "P") if reverse else ("P", "C")
        assert numbered == [f"aseo {prefix}{i}" for prefix, count in zip(prefixes, expected) for i in range(count)]
    if source_error:
        assert groups[0]["state"] == "source_error"
        assert "ninguna fila cumple" not in result["response"]


@pytest.mark.parametrize("limit", [0, 1])
@pytest.mark.parametrize("fallback", [False, True])
def test_budget_exhaustion_does_not_claim_nonempty_source_is_empty(limit, fallback):
    cards = [[{"titulo": f"aseo {i}", "entidad": "E", "valor": "$1"}] for i in range(2)]
    groups = [dict(_group(universe), rows=[{"id": i}], total=10, total_quality="lower_bound",
                   fallback_attempted=fallback) for i, universe in enumerate(("processes", "contracts"))]
    response = response_text(groups, cards, channel="telegram", limit=limit)
    assert "ninguna fila cumple" not in response
    assert "Contratos: encontré al menos 10. Te muestro 0" in response
    if fallback:
        assert response.count("muestra sin orden garantizado") == 2
        assert "más recientes" not in response
    else:
        assert "Te muestro 0 por el límite de tarjetas del mensaje." in response
    assert len(re.findall(r"^\d+\. ", response, re.M)) == limit


@given(capacities=st.lists(st.integers(0, 40), max_size=8), limit=st.integers(0, 350),
       channel=st.sampled_from(["telegram", "whatsapp", "streamlit"]))
def test_global_budget_conserves_prefixes_fairness_and_source(capacities, limit, channel):
    cards = [[{"group": group, "index": index} for index in range(size)]
             for group, size in enumerate(capacities)]
    original = deepcopy(cards)
    selected = shown_card_groups(cards, channel=channel, limit=limit)
    counts = [len(group) for group in selected]
    assert cards == original
    assert len(selected) == len(cards)
    for source, shown in zip(cards, selected):
        assert shown == source[:len(shown)]
        assert all(a is b for a, b in zip(source, shown))
    if channel == "streamlit":
        assert selected == cards
        return
    assert sum(counts) == min(limit, sum(capacities))
    if limit >= sum(size > 0 for size in capacities):
        assert all(count > 0 for count, capacity in zip(counts, capacities) if capacity)
    for index, count in enumerate(counts):
        if count < capacities[index]:
            assert all(other <= count + 1 for other in counts)
            for later in range(index + 1, len(counts)):
                assert counts[later] <= count


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
