"""ADR-102 §10: host paths that used to drop meaning silently now represent it or ask.

Each fix has a focal case, a property, and a harness run proving that a blocked
proposal makes no source call.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_resolver import EntityResolver
from app.core.foundation_admission import admit_proposal
from app.core.foundation_evidence import material_phrase
from app.core.foundation_producers import decode_proposal
from app.core.foundation_projection import project_accepted
from app.core.interpretation_contract import AcceptanceStatus, RawSpan
from app.utils.money import normalize_text

NOW = datetime.fromisoformat("2026-09-26T12:00:00-05:00")
RESOLVER = EntityResolver("app/data/aliases_db.json")


def span(raw, text, occurrence=1):
    at = -1
    for _ in range(occurrence):
        at = raw.index(text, at + 1)
    return {"start": at, "end": at + len(text), "quote": text}


def proposal(raw, conditions, *, subject="contracts", noun="contratos", kind="records"):
    """Arm-C wire payload: one request citing `noun`, conditions as (dim, op, [spans], polarity)."""
    ids = [f"c{at}" for at in range(len(conditions))]
    children = [{"operator": "ATOM", "condition_id": cid} for cid in ids]
    expression = children[0] if len(children) == 1 else {"operator": "AND", "children": children}
    payload = {
        "requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": subject,
                      "universe_kind": kind, "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                      "universe_evidence": span(raw, noun), "condition_expression": expression}],
        "conditions": [{"condition_id": cid, "request_ids": ["r1"], "dimension": dim, "operator": op,
                        "polarity": polarity, "evidence": spans}
                       for cid, (dim, op, spans, polarity) in zip(ids, conditions)],
    }
    return decode_proposal(payload, raw, arm="C")


def admit(raw, conditions, **kwargs):
    return admit_proposal(proposal(raw, conditions, **kwargs), raw, entity_resolver=RESOLVER)


def harness_run(tmp_path, monkeypatch, raw, produced):
    from tests.product_contract.harness import ProductHarness

    from app.core.foundation_projection import SOURCE_SELECT_FIELDS
    from app.core.soql_builder import SoQLBuilder

    # One all-NULL row per dataset only declares the columns; no filter matches it.
    rows = {dataset: [dict.fromkeys({*SoQLBuilder.SPECS[dataset].select_fields, *SOURCE_SELECT_FIELDS[dataset]})]
            for dataset in (SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET)}
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=rows,
                             reference_instant=NOW, today=NOW.date(), foundation_producer=lambda _: produced,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    return harness.say(raw), harness


# ── 10.1 G1/T4: several citations of one TOPIC ────────────────────────────────

def test_a_phrase_cited_in_contiguous_pieces_is_the_whole_phrase():
    raw = "contratos de primera infancia"
    accepted = admit(raw, [("TOPIC", "contains", [span(raw, "primera"), span(raw, "infancia")], "POSITIVE")])
    assert accepted.status is AcceptanceStatus.READY
    (topic,) = [r for r in accepted.resolutions if r.target_id == "c0"]
    assert topic.resolved_ids == ("primera infancia",)


def test_a_repeated_citation_is_one_topic():
    raw = "contratos de aseo, aseo urbano no"
    accepted = admit(raw, [("TOPIC", "contains", [span(raw, "aseo"), span(raw, "aseo", 2)], "POSITIVE")])
    assert accepted.status is AcceptanceStatus.READY
    assert [r.resolved_ids for r in accepted.resolutions if r.target_id == "c0"] == [("aseo",)]


@pytest.mark.parametrize("raw,first,second", [
    ("contratos de aseo y de vigilancia", "aseo", "vigilancia"),
    ("contratos de vigilancia o aseo", "vigilancia", "aseo"),
])
def test_two_separate_topics_in_one_condition_ask_instead_of_keeping_the_first(raw, first, second):
    accepted = admit(raw, [("TOPIC", "contains", [span(raw, first), span(raw, second)], "POSITIVE")])
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    assert accepted.reason == "MULTI_EVIDENCE_TOPIC_UNSUPPORTED"


def test_two_separate_buyers_in_one_condition_ask():
    raw = "contratos de la alcaldía de Soledad y de la gobernación del Atlántico"
    accepted = admit(raw, [("BUYER", "is", [span(raw, "alcaldía de Soledad"),
                                           span(raw, "gobernación del Atlántico")], "POSITIVE")])
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    assert accepted.reason == "MULTI_EVIDENCE_BUYER_UNSUPPORTED"


def test_a_blocked_multi_topic_makes_no_source_call(tmp_path, monkeypatch):
    raw = "contratos de aseo y de vigilancia"
    produced = proposal(raw, [("TOPIC", "contains", [span(raw, "aseo"), span(raw, "vigilancia")], "POSITIVE")])
    turn, harness = harness_run(tmp_path, monkeypatch, raw, produced)
    assert turn.result["foundation_terminal"] == "NEEDS_CLARIFICATION"
    assert turn.result["foundation_internal_reason"] == "MULTI_EVIDENCE_TOPIC_UNSUPPORTED"
    assert harness.transport.snapshot() == []


WORDS = ["aseo", "vigilancia", "obra", "primera", "infancia", "de", "y", "o"]


@settings(max_examples=300, deadline=None)
@given(st.lists(st.sampled_from(WORDS), min_size=1, max_size=8),
       st.lists(st.tuples(st.integers(0, 7), st.integers(1, 3)), min_size=0, max_size=5))
def test_material_phrase_never_silently_drops_a_citation(words, picks):
    raw = " ".join(words)
    bounds, at = [], 0
    for word in words:
        bounds.append((at, at + len(word)))
        at += len(word) + 1
    spans = []
    for first, width in picks:
        if first < len(words):
            last = min(len(words), first + width) - 1
            start, end = bounds[first][0], bounds[last][1]
            spans.append(RawSpan(start, end, raw[start:end]))
    phrase, reason = material_phrase(spans, raw, "TOPIC")
    if not spans:  # cardinality 0: nothing to read, never an invented phrase
        assert phrase is None and reason == "NO_MATERIAL_EVIDENCE:TOPIC"
        return
    assert (phrase is None) != (reason is None)
    if phrase is None:
        return
    folded = normalize_text(phrase)
    # Every citation is inside the phrase the host executes (none is dropped).
    assert all(normalize_text(item.quote) in folded for item in spans)
    distinct = {normalize_text(item.quote) for item in spans}
    if len(distinct) > 1:  # cardinality N: only one contiguous raw phrase is a faithful reading
        start = min(item.start for item in spans)
        assert raw[start:start + len(phrase)] == phrase
        # ...and nothing the user did not cite is added: every non-space char is inside a citation.
        assert all(char.isspace() or any(item.start <= start + at < item.end for item in spans)
                   for at, char in enumerate(phrase))


# ── 10.2 G2: the request's universe occurrence never becomes topic ────────────

INFANCIA = "contratos de primera infancia de la alcaldía de barranquilla"
BARRANQUILLA = "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"


def _query_soql(accepted):
    outcome = project_accepted(accepted, reference_instant=NOW)
    return next(m.soql for m in outcome.manifest if m.operation == "query")


@pytest.mark.parametrize("topic_spans,authority", [
    ([span(INFANCIA, "primera infancia")], "host-dimension"),
    ([span(INFANCIA, "contratos"), span(INFANCIA, "primera infancia")], "host-dimension:universe-span-omitted"),
])
def test_first_infancy_topic_never_carries_the_universe_word(topic_spans, authority):
    accepted = admit(INFANCIA, [("TOPIC", "contains", topic_spans, "POSITIVE"),
                                ("BUYER", "is", [span(INFANCIA, "alcaldía de barranquilla")], "POSITIVE")])
    assert accepted.status is AcceptanceStatus.READY, accepted.reason
    resolved = {r.target_id: r for r in accepted.resolutions}
    assert resolved["c0"].resolved_ids == ("primera infancia",) and resolved["c0"].authority == authority
    assert resolved["c1"].resolved_ids == (BARRANQUILLA,)
    assert resolved["r1"].resolved_ids == ("jbjy-vk9h",)
    soql = _query_soql(accepted)
    assert "%primera infancia%" in soql.lower()
    assert "%contratos%" not in soql.lower()
    assert BARRANQUILLA in soql


@pytest.mark.parametrize("topic", ["contratos de primera infancia", "contratos de primera"])
def test_a_topic_span_that_swallows_the_universe_occurrence_asks(topic):
    accepted = admit(INFANCIA, [("TOPIC", "contains", [span(INFANCIA, topic)], "POSITIVE")])
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    assert accepted.reason == "TOPIC_OVERLAPS_REQUEST_UNIVERSE"


def test_a_topic_that_is_only_the_universe_occurrence_asks():
    raw = "contratos de la alcaldía de barranquilla"
    accepted = admit(raw, [("TOPIC", "contains", [span(raw, "contratos")], "POSITIVE")])
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    assert accepted.reason == "TOPIC_ONLY_REQUEST_UNIVERSE"


def test_another_occurrence_of_the_universe_word_is_real_topic_content():
    raw = "contratos para implementar un sistema de gestión de contratos laborales"
    topic = "sistema de gestión de contratos laborales"
    accepted = admit(raw, [("TOPIC", "contains", [span(raw, topic)], "POSITIVE")])
    assert accepted.status is AcceptanceStatus.READY
    assert [r.resolved_ids for r in accepted.resolutions if r.target_id == "c0"] == [
        ("sistema de gestion de contratos laborales",)]
    soql = _query_soql(accepted)
    assert "%sistema de gestion de contratos laborales%" in soql.lower()
    assert "'%contratos%'" not in soql.lower()


def test_a_swallowing_topic_makes_no_source_call(tmp_path, monkeypatch):
    produced = proposal(INFANCIA, [("TOPIC", "contains", [span(INFANCIA, "contratos de primera infancia")],
                                    "POSITIVE")])
    turn, harness = harness_run(tmp_path, monkeypatch, INFANCIA, produced)
    assert turn.result["foundation_internal_reason"] == "TOPIC_OVERLAPS_REQUEST_UNIVERSE"
    assert harness.transport.snapshot() == []


# ── 10.3 State disjunction ─────────────────────────────────────────────────────

def _state_resolution(accepted, cid="c0"):
    return [r.resolved_ids for r in accepted.resolutions if r.target_id == cid]


@pytest.mark.parametrize("raw,words,operator", [
    ("procesos publicados o abiertos de aseo", ["publicados o abiertos"], "is"),       # literal operator
    ("procesos publicados o abiertos de aseo", ["publicados", "abiertos"], "is"),
    ("procesos publicados y abiertos de aseo", ["publicados", "abiertos"], "family"),   # «y» is not the funnel
    ("procesos publicados y abiertos de aseo", ["publicados y abiertos"], "family"),
    ("procesos publicados, abiertos de aseo", ["publicados", "abiertos"], "family"),
    ("procesos publicados o cerrados de aseo", ["publicados", "cerrados"], "is"),
])
def test_two_states_in_one_condition_are_asked_never_collapsed(raw, words, operator):
    accepted = admit(raw, [("STATE", operator, [span(raw, w) for w in words], "POSITIVE"),
                           ("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")],
                     subject="processes", noun="procesos")
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    assert accepted.reason == "STATE_COMPOSITION_UNSUPPORTED"


@pytest.mark.parametrize("words", [["publicados o abiertos"], ["publicados", "abiertos"]])
@pytest.mark.parametrize("raw", ["procesos publicados o abiertos de aseo", "procesos abiertos o publicados de aseo"])
def test_the_accredited_funnel_under_a_bare_o_is_kept(raw, words):
    if words == ["publicados o abiertos"] and "abiertos o publicados" in raw:
        words = ["abiertos o publicados"]
    accepted = admit(raw, [("STATE", "family", [span(raw, w) for w in words], "POSITIVE"),
                           ("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")],
                     subject="processes", noun="procesos")
    assert accepted.status is AcceptanceStatus.READY, accepted.reason
    assert _state_resolution(accepted) == [("oferta_abierta",)]
    soql = _query_soql(accepted)
    assert "estado_de_apertura_del_proceso = 'Abierto'" in soql


def test_a_single_literal_state_is_unchanged():
    raw = "procesos publicados de aseo"
    accepted = admit(raw, [("STATE", "is", [span(raw, "publicados")], "POSITIVE"),
                           ("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")],
                     subject="processes", noun="procesos")
    assert accepted.status is AcceptanceStatus.READY and _state_resolution(accepted) == [("Publicado",)]


def test_a_negated_state_is_unsupported_not_silently_positive(tmp_path, monkeypatch):
    raw = "procesos no publicados de aseo"
    produced = proposal(raw, [("STATE", "is", [span(raw, "publicados")], "NEGATIVE"),
                              ("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")],
                        subject="processes", noun="procesos")
    turn, harness = harness_run(tmp_path, monkeypatch, raw, produced)
    assert turn.result["foundation_terminal"] == "UNSUPPORTED"
    assert harness.transport.snapshot() == []


@pytest.mark.parametrize("operator", ["OR", "NOT"])
def test_state_under_or_or_not_is_unsupported_without_calls(tmp_path, monkeypatch, operator):
    raw = "procesos publicados o de aseo"
    payload_conditions = [("STATE", "is", [span(raw, "publicados")], "POSITIVE"),
                          ("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")]
    base = proposal(raw, payload_conditions, subject="processes", noun="procesos")
    from dataclasses import replace
    from app.core.interpretation_contract import ConditionExpression, LogicOperator
    atoms = tuple(ConditionExpression(LogicOperator.ATOM, c) for c in ("c0", "c1"))
    expression = (ConditionExpression(LogicOperator.OR, None, atoms) if operator == "OR" else
                  ConditionExpression(LogicOperator.AND, None, (
                      ConditionExpression(LogicOperator.NOT, None, (atoms[0],)), atoms[1])))
    produced = replace(base, requests=(replace(base.requests[0], condition_expression=expression),))
    turn, harness = harness_run(tmp_path, monkeypatch, raw, produced)
    assert turn.result["foundation_terminal"] == "UNSUPPORTED"
    assert harness.transport.snapshot() == []


_FORMS = {name: [stem + end for end in ("o", "a", "os", "as")]
          for name, stem in (("Publicado", "publicad"), ("abierto", "abiert"),
                             ("Terminado", "terminad"), ("cerrado", "cerrad"))}


def test_every_two_state_composition_of_the_lexicon_is_asked_or_is_the_funnel():
    """Exhaustive over the finite lexicon: forms x ordered pairs x connectors x operators x span layout."""
    checked = 0
    for left_name, left_forms in _FORMS.items():
        for right_name, right_forms in _FORMS.items():
            if left_name == right_name:
                continue
            for left in left_forms:
                for right in right_forms:
                    for connector in (" o ", " y ", ", ", " u "):
                        raw = f"procesos {left}{connector}{right} de aseo"
                        layouts = ([span(raw, f"{left}{connector}{right}")], [span(raw, left), span(raw, right)])
                        for operator in ("is", "family"):
                            for layout in layouts:
                                accepted = admit(raw, [("STATE", operator, layout, "POSITIVE"),
                                                       ("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE")],
                                                 subject="processes", noun="procesos")
                                funnel = ({left_name, right_name} == {"Publicado", "abierto"}
                                          and operator == "family" and connector == " o ")
                                if funnel:
                                    assert accepted.status is AcceptanceStatus.READY, raw
                                    assert _state_resolution(accepted) == [("oferta_abierta",)], raw
                                else:
                                    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION, (raw, operator)
                                    assert accepted.reason == "STATE_COMPOSITION_UNSUPPORTED", (raw, operator)
                                checked += 1
    assert checked == 4 * 3 * 4 * 4 * 4 * 2 * 2


# ── 10.4 VALUE operator vs its own evidence ────────────────────────────────────

def _value(comparative, operator, amount="300 millones"):
    raw = f"contratos de aseo de {comparative} {amount}"
    return admit(raw, [("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE"),
                       ("VALUE", operator, [span(raw, f"{comparative} {amount}")], "POSITIVE")])


# Written by hand from the Spanish meaning, not from the implementation's table.
@pytest.mark.parametrize("comparative,operator,reason", [
    ("más de", "greater_than", None), ("mayor que", "greater_than", None), ("superior a", "greater_than", None),
    ("menos de", "less_than", None), ("menor que", "less_than", None), ("inferior a", "less_than", None),
    ("más de", "less_than", "VALUE_OPERATOR_EVIDENCE_MISMATCH"),
    ("menos de", "greater_than", "VALUE_OPERATOR_EVIDENCE_MISMATCH"),
    ("al menos", "less_than", "VALUE_OPERATOR_EVIDENCE_MISMATCH"),
    ("a lo sumo", "greater_than", "VALUE_OPERATOR_EVIDENCE_MISMATCH"),
    ("no más de", "greater_than", "VALUE_OPERATOR_EVIDENCE_MISMATCH"),
    ("no menos de", "less_than", "VALUE_OPERATOR_EVIDENCE_MISMATCH"),
    ("al menos", "greater_than", "VALUE_OPERATOR_BOUNDARY_MISMATCH"),
    ("hasta", "less_than", "VALUE_OPERATOR_BOUNDARY_MISMATCH"),
    ("exactamente", "greater_than", "VALUE_OPERATOR_EVIDENCE_MISMATCH"),
    ("valor de", "greater_than", "VALUE_OPERATOR_UNGROUNDED"),
])
def test_the_value_operator_follows_its_own_comparator(comparative, operator, reason):
    accepted = _value(comparative, operator)
    if reason is None:
        assert accepted.status is AcceptanceStatus.READY, accepted.reason
    else:
        assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION and accepted.reason == reason


@pytest.mark.parametrize("text", ["entre 100 y 300 millones", "más de 100 y menos de 300 millones"])
def test_two_bounds_in_one_value_condition_are_asked(text):
    raw = f"contratos de aseo {text}"
    accepted = admit(raw, [("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE"),
                           ("VALUE", "greater_than", [span(raw, text)], "POSITIVE")])
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    assert accepted.reason in {"VALUE_OPERATOR_EVIDENCE_MISMATCH", "VALUE_OPERATOR_AMBIGUOUS"}


# FROZEN literal copy of the 44 (phrase, family) tuples of ``foundation_value_direction._MARKERS`` at main
# (4ec99bc1). Wave1 replaced that lexicon by ``money_comparators.HEADS``; main's protection must not shrink
# with it, so both are walked: these 44 (same assertion as main) and the new HEADS.
MAIN_4EC99BC1_MARKERS = (
    ("no menos de", "GTE"),
    ("no menos que", "GTE"),
    ("mayor o igual", "GTE"),
    ("igual o mayor", "GTE"),
    ("al menos", "GTE"),
    ("como minimo", "GTE"),
    ("por lo menos", "GTE"),
    ("desde", "GTE"),
    ("a partir de", "GTE"),
    ("no mas de", "LTE"),
    ("no mas que", "LTE"),
    ("menor o igual", "LTE"),
    ("igual o menor", "LTE"),
    ("a lo sumo", "LTE"),
    ("como maximo", "LTE"),
    ("hasta", "LTE"),
    ("mas de", "GT"),
    ("mas que", "GT"),
    ("mayor de", "GT"),
    ("mayor que", "GT"),
    ("mayor a", "GT"),
    ("mayores de", "GT"),
    ("mayores que", "GT"),
    ("mayores a", "GT"),
    ("superior a", "GT"),
    ("superiores a", "GT"),
    ("por encima de", "GT"),
    ("supere", "GT"),
    ("superen", "GT"),
    ("exceda", "GT"),
    ("excedan", "GT"),
    ("menos de", "LT"),
    ("menos que", "LT"),
    ("menor de", "LT"),
    ("menor que", "LT"),
    ("menor a", "LT"),
    ("menores de", "LT"),
    ("menores que", "LT"),
    ("menores a", "LT"),
    ("inferior a", "LT"),
    ("inferiores a", "LT"),
    ("por debajo de", "LT"),
    ("exactamente", "EQ"),
    ("igual a", "EQ"),
)


def _ready_only_in_its_own_direction(phrase, family):
    from app.core.money_comparators import GT, LT

    for operator in ("greater_than", "less_than"):
        accepted = _value(phrase, operator)
        ready = (operator, family) in {("greater_than", GT), ("less_than", LT)}
        assert (accepted.status is AcceptanceStatus.READY) is ready, (phrase, family, operator, accepted.reason)


def test_main_literal_markers_are_frozen_at_44():
    assert len(MAIN_4EC99BC1_MARKERS) == 44 and len(set(MAIN_4EC99BC1_MARKERS)) == 44


def test_every_comparator_of_main_is_ready_only_in_its_own_direction():
    for phrase, family in MAIN_4EC99BC1_MARKERS:
        _ready_only_in_its_own_direction(phrase, family)


def test_every_comparator_of_the_closed_lexicon_is_ready_only_in_its_own_direction():
    from app.core.money_comparators import HEADS

    for head in HEADS:
        phrase = f"{head.phrase} de" if head.needs_link else head.phrase
        _ready_only_in_its_own_direction(phrase, head.family)


def test_an_inverted_value_makes_no_source_call(tmp_path, monkeypatch):
    raw = "contratos de aseo de más de 300 millones"
    produced = proposal(raw, [("TOPIC", "contains", [span(raw, "aseo")], "POSITIVE"),
                              ("VALUE", "less_than", [span(raw, "más de 300 millones")], "POSITIVE")])
    turn, harness = harness_run(tmp_path, monkeypatch, raw, produced)
    assert turn.result["foundation_internal_reason"] == "VALUE_OPERATOR_EVIDENCE_MISMATCH"
    assert turn.result["foundation_terminal"] == "NEEDS_CLARIFICATION"
    assert harness.transport.snapshot() == []
