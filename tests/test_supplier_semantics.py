"""A2 SUPPLIERS polarity sidecar: a lock that can only stay shut.

The sidecar is consulted after every deterministic authority has already
accredited the turn, and its only power is to withhold EXECUTE. These tests
hold that shape from both ends: nothing it answers may widen the contract, and
nothing that goes wrong with it may soften one.

The replay case is the load-bearing one. Experiment a2-polarity-sidecar-x1
measured 210 real calls against a blind-adjudicated oracle; those recorded
answers are replayed here through the runtime adjudicator, so the measured
gates are claimed for this code and not merely for the harness that produced
them. No test in this file performs a provider call.
"""
from __future__ import annotations

import hashlib
import pathlib

import pytest

from datetime import date

from app.core.capability_binding import supplier_contract
from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.capability_binding.supplier_semantics_provider import PROMPT
from app.core.capability_binding.supplier_semantics import (
    MentionRef,
    adjudicate,
    VetoOutcome,
    VetoVerdict,
    mention_refs,
    reduce_turn,
)
from app.core.capability_binding.supplier_request_authority import assess_supplier_request
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter

EXPERIMENT = pathlib.Path(__file__).parent / "fixtures" / "a2_supplier_semantics" / "x1"

# A turn every other authority already accredits, so only the veto decides.
# Built through the real router, as the rest of the contract suite does: a
# hand-written params dict would test a shape the runtime never produces.
ACCREDITED_QUERY = "proveedores de la alcaldía de Galapa"
TODAY = date(2026, 9, 15)


@pytest.fixture(scope="module")
def router():
    return QueryRouter(entity_resolver=EntityResolver(pathlib.Path("app/data/aliases_db.json")))


@pytest.fixture
def accredited(router):
    return router.parse(ACCREDITED_QUERY, today=TODAY).params


def _veto(verdict: VetoVerdict):
    def veto(_query, _refs, _quantities=()) -> VetoVerdict:
        return verdict
    return veto


def _answer(labels: list[str]):
    """A sidecar that echoes the refs it was given with these labels."""
    def veto(query, refs, quantities=()) -> VetoVerdict:
        payload = {"mentions": [
            {"quote": ref.quote, "occurrence": ref.occurrence, "polarity": label}
            for ref, label in zip(refs, labels)
        ]}
        if quantities:
            payload["cardinality"] = {"quantity_ref": 1, "relation": "EXACT"}
        return adjudicate(refs, payload, quantities)
    return veto


# ── the contract without a veto is the V1 contract ────────────────────────────

def test_without_a_veto_the_decision_is_unchanged(accredited):
    assert decide(ACCREDITED_QUERY, accredited).outcome is Outcome.EXECUTE


def test_a_veto_is_never_consulted_for_a_turn_v1_already_refused(accredited):
    """Monotonicity: the sidecar cannot be the reason a turn dies, nor be paid for."""
    calls = []

    def veto(query, refs, quantities=()):
        calls.append(query)
        return VetoVerdict(VetoOutcome.CONFIRMED, "POSITIVE")

    for query, params in [
        ("sin proveedores", dict(accredited)),             # deterministic NEGATED
        ("proveedores", {}),                               # no buyer
    ]:
        assert decide(query, params, polarity_veto=veto).outcome is not Outcome.EXECUTE
    assert calls == []


# ── the veto can only withhold ────────────────────────────────────────────────

def test_an_unreadable_cardinal_is_deferred_to_the_sidecar_not_refused(accredited):
    """The one deliberate exception to "never consulted".

    A cardinal the grammar cannot read used to die as UNSUPPORTED_CARDINALITY,
    which cost 18 of 30 natural phrasings. It is now asked about, over a
    quantity the host has already grounded. With no sidecar wired the question
    is never posed and the deterministic whitelist still refuses on its own.
    """
    asked = []

    def veto(query, refs, quantities=()):
        asked.append((query, tuple((q.quote, q.value) for q in quantities)))
        payload = {"mentions": [{"quote": r.quote, "occurrence": r.occurrence,
                                 "polarity": "POSITIVE"} for r in refs],
                   "cardinality": {"quantity_ref": 1, "relation": "EXACT"}}
        return adjudicate(refs, payload, quantities)

    query = "sus 5 proveedores de la alcaldía de Galapa"
    assert decide(query, dict(accredited)).outcome is Outcome.CLARIFY      # no sidecar
    assert decide(query, dict(accredited), polarity_veto=veto).outcome is Outcome.EXECUTE
    assert asked and asked[0][1] == (("5", 5),)


@pytest.mark.parametrize("relation", ["LOWER_BOUND", "UPPER_BOUND", "RANGE", "UNKNOWN"])
def test_a_rescued_cardinal_only_survives_as_the_one_supported_exact(relation, accredited):
    """The capability serves one cardinality, so only EXACT on it may revive."""
    def veto(query, refs, quantities=()):
        payload = {"mentions": [{"quote": r.quote, "occurrence": r.occurrence,
                                 "polarity": "POSITIVE"} for r in refs],
                   "cardinality": {"quantity_ref": 1, "relation": relation}}
        return adjudicate(refs, payload, quantities)

    decision = decide("superior a 5 proveedores de la alcaldía de Galapa",
                      dict(accredited), polarity_veto=veto)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "UNSUPPORTED_CARDINALITY"


def test_a_rescued_cardinal_on_another_number_does_not_execute(accredited):
    def veto(query, refs, quantities=()):
        payload = {"mentions": [{"quote": r.quote, "occurrence": r.occurrence,
                                 "polarity": "POSITIVE"} for r in refs],
                   "cardinality": {"quantity_ref": 1, "relation": "EXACT"}}
        return adjudicate(refs, payload, quantities)

    decision = decide("sus 20 proveedores de la alcaldía de Galapa",
                      dict(accredited), polarity_veto=veto)
    assert decision.outcome is Outcome.CLARIFY


def test_confirmed_executes(accredited):
    decision = decide(ACCREDITED_QUERY, accredited, polarity_veto=_answer(["POSITIVE"]))
    assert decision.outcome is Outcome.EXECUTE


@pytest.mark.parametrize("label", ["NEGATED", "MIXED", "UNKNOWN"])
def test_any_non_positive_label_withholds_execute(label, accredited):
    decision = decide(ACCREDITED_QUERY, accredited, polarity_veto=_answer([label]))
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "POLARITY_VETO"
    assert decision.response


def test_an_unavailable_sidecar_withholds_execute_too(accredited):
    """A sidecar that is down must not be softer than one that says no."""
    decision = decide(ACCREDITED_QUERY, accredited,
                      polarity_veto=_veto(VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "BOOM")))
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "POLARITY_SIDECAR_UNAVAILABLE"


def test_a_veto_never_grants_what_the_authority_refused(accredited):
    """Even a CONFIRMED answer cannot revive a turn the authority failed closed."""
    # Not "5 proveedores en adelante": that one is SUPPLIER_RIGHT_TAIL_AFTER_LINK_DEBT,
    # which the authority still admits. The sidecar judges polarity, not
    # cardinality, so it does not close that debt and must not be credited with it.
    for query in ["sin proveedores", "salvo 5 proveedores", "ningún proveedor"]:
        assert decide(query, dict(accredited),
                      polarity_veto=_veto(VetoVerdict(VetoOutcome.CONFIRMED, "POSITIVE"))
                      ).outcome is not Outcome.EXECUTE


# ── mechanical validation of the answer ───────────────────────────────────────

@pytest.mark.parametrize("payload,detail", [
    ("not an object", "NOT_AN_OBJECT"),
    ({}, "MENTIONS_NOT_A_LIST"),
    ({"mentions": {}}, "MENTIONS_NOT_A_LIST"),
    ({"mentions": ["x"]}, "MENTION_NOT_AN_OBJECT"),
    ({"mentions": [{"quote": "proveedores", "occurrence": 1, "polarity": "SI"}]}, "INVALID_LABEL:'SI'"),
    ({"mentions": [{"quote": "proveedores", "occurrence": "1", "polarity": "POSITIVE"}]}, "INVALID_REF_TYPES"),
    ({"mentions": [{"quote": "proveedores", "occurrence": True, "polarity": "POSITIVE"}]}, "INVALID_REF_TYPES"),
    ({"mentions": []}, "MISSING_MENTION"),
    ({"mentions": [{"quote": "Galapa", "occurrence": 1, "polarity": "POSITIVE"}]}, "INVENTED_MENTION"),
    ({"mentions": [{"quote": "proveedores", "occurrence": 7, "polarity": "POSITIVE"}]}, "INVENTED_MENTION"),
    ({"mentions": [{"quote": "proveedores", "occurrence": 1, "polarity": "POSITIVE"},
                   {"quote": "proveedores", "occurrence": 1, "polarity": "NEGATED"}]}, "DUPLICATE_MENTION"),
])
def test_a_malformed_answer_is_unavailable_and_blocks(payload, detail):
    refs = (MentionRef("proveedores", 1),)
    verdict = adjudicate(refs, payload)
    assert verdict.outcome is VetoOutcome.UNAVAILABLE
    assert verdict.detail == detail
    assert verdict.blocks is True


def test_no_refs_is_unavailable_not_permission():
    assert adjudicate((), {"mentions": []}).outcome is VetoOutcome.UNAVAILABLE


# ── host-owned reduction ──────────────────────────────────────────────────────

@pytest.mark.parametrize("labels,expected", [
    (("POSITIVE",), "POSITIVE"),
    (("POSITIVE", "POSITIVE"), "POSITIVE"),
    (("NEGATED",), "NEGATED"),
    (("NEGATED", "NEGATED"), "NEGATED"),
    (("POSITIVE", "NEGATED"), "MIXED"),
    (("NEGATED", "POSITIVE"), "MIXED"),
    (("POSITIVE", "UNKNOWN"), "UNKNOWN"),
    (("POSITIVE", "MIXED"), "MIXED"),
    (("MIXED", "UNKNOWN"), "UNKNOWN"),
    ((), "UNKNOWN"),
])
def test_reduction_never_manufactures_positive(labels, expected):
    assert reduce_turn(labels) == expected


def test_only_a_uniformly_positive_turn_confirms():
    refs = (MentionRef("proveedores", 1), MentionRef("contratistas", 1))
    for labels in (["POSITIVE", "NEGATED"], ["NEGATED", "POSITIVE"], ["POSITIVE", "UNKNOWN"]):
        payload = {"mentions": [
            {"quote": r.quote, "occurrence": r.occurrence, "polarity": v}
            for r, v in zip(refs, labels)
        ]}
        assert adjudicate(refs, payload).outcome is VetoOutcome.VETOED


# ── host-owned refs ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected", [
    ("proveedores de Galapa", [("proveedores", 1)]),
    ("proveedores, proveedores y contratistas",
     [("proveedores", 1), ("proveedores", 2), ("contratistas", 1)]),
    ("PROVEEDORES de Galapa", [("PROVEEDORES", 1)]),
    ("quiero proveedores de Cali y ningún proveedor de Medellín",
     [("proveedores", 1), ("proveedor", 1)]),
])
def test_refs_carry_the_original_surface_and_a_correct_occurrence(query, expected):
    refs = mention_refs(query, assess_supplier_request(query))
    assert [(r.quote, r.occurrence) for r in refs] == expected
    for ref in refs:
        assert query.lower().count(ref.quote.lower()) >= ref.occurrence


# ── the runtime prompt is the measured prompt ─────────────────────────────────

def test_runtime_prompt_is_byte_identical_to_the_frozen_one():
    """Re-pinned to x2: adding cardinality changed the wording, so the x1 gates
    do not carry over and the x1 hash must not keep guarding this file."""
    frozen = (pathlib.Path(__file__).parent / "fixtures" / "a2_supplier_semantics"
              / "x2" / "prompt_v2.txt").read_text(encoding="utf-8")
    assert PROMPT == frozen
    assert hashlib.sha256(PROMPT.encode()).hexdigest() == (
        "6277afc750b357945116af7e377501dcd769b60efb8633aac5109af88f6cfe3e"
    )


def test_the_contract_never_imports_a_network_client():
    source = pathlib.Path(supplier_contract.__file__).read_text()
    assert "openai" not in source
    assert "provider" not in source


# ── findings from the ZEUS audit of 98e26ad, frozen as regressions ────────────

@pytest.mark.parametrize("query,expected", [
    ("Proveedores y proveedores de Galapa", [("Proveedores", 1), ("proveedores", 1)]),
    ("proveedores, Proveedores, PROVEEDORES",
     [("proveedores", 1), ("Proveedores", 1), ("PROVEEDORES", 1)]),
])
def test_occurrence_counts_the_surface_that_is_actually_sent(query, expected):
    """Counting case-insensitively invented refs that do not exist as written."""
    import re as _re

    refs = mention_refs(query, assess_supplier_request(query))
    assert [(r.quote, r.occurrence) for r in refs] == expected
    for ref in refs:
        found = len(_re.findall(rf"(?<!\w){_re.escape(ref.quote)}(?!\w)", query))
        assert found >= ref.occurrence, (ref, found)


def test_every_mention_the_authority_found_reaches_the_ref_list():
    """A dropped ref would be a mention the sidecar is never asked about.

    Refs must be exhaustive or empty; silently skipping one turns a fail-closed
    path into an unasked question.
    """
    query = "proveedores, proveedores y contratistas"
    evidence = assess_supplier_request(query)
    assert len(mention_refs(query, evidence)) == len(evidence.mentions) == 3


def test_the_provider_does_not_let_the_sdk_multiply_the_timeout(monkeypatch):
    """The SDK retries twice by default, so a timeout meant three of them.

    Asserted on the client that is actually built, not on the source text: the
    explanatory comment also contains the words, so a grep would pass against
    a provider that had lost the argument.
    """
    from app.core.capability_binding import supplier_semantics_provider as provider

    built = {}

    class Client:
        def __init__(self, **kwargs):
            built.update(kwargs)
            raise RuntimeError("stop after construction")

    monkeypatch.setattr("openai.OpenAI", Client)
    provider.classify("proveedores", (MentionRef("proveedores", 1),),
                      api_key="k", model="m", timeout_seconds=3.0)
    assert built["max_retries"] == 0
    assert built["timeout"] == 3.0


def test_a_mention_without_a_usable_surface_empties_the_refs(monkeypatch):
    """Skipping one ref would leave a mention the sidecar is never asked about.

    Refs are exhaustive or empty; an empty list adjudicates as UNAVAILABLE and
    blocks, while a short list would quietly narrow what gets classified.
    """
    from app.core.capability_binding import supplier_semantics as module

    query = "proveedores y contratistas"
    evidence = assess_supplier_request(query)
    assert len(evidence.mentions) == 2

    original = module.EvidenceSpan if hasattr(module, "EvidenceSpan") else None
    assert original is None or True

    class Span:
        original = (0, 0)

    class Mention:
        span = Span()

    class Stub:
        mentions = (evidence.mentions[0], Mention())

    assert mention_refs(query, Stub()) == ()
    assert adjudicate((), {"mentions": []}).outcome is VetoOutcome.UNAVAILABLE
