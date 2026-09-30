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

    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset={"p6dx-8zbt": [], "jbjy-vk9h": []},
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
