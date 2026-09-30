"""The frozen B v4.2 prompt states the exact wire shape the decoder admits (ADR-102)."""

from __future__ import annotations

import hashlib
import re

import pytest

from app.core import foundation_prompts
from app.core.foundation_producers import (
    PRODUCER_VERSION, LinguisticProducer, ProviderReply, decode_proposal, host_candidates,
)
from app.core.foundation_prompts import PRODUCER_ARM, PROMPT_SHA256, PROMPT_VERSION, SYSTEM_PROMPT, system_prompt
from app.core.interpretation_contract import AcceptanceStatus, ContractError, InterpretationProposal


RAW = "Busca contratos de vigilancia"  # Synthetic: not a development or Gold case.
TOPIC = RAW.index("vigilancia")
NOUN = RAW.index("contratos")


def _span(start, end):
    return {"start": start, "end": end, "quote": RAW[start:end]}


def _ids():
    return {(item["start"], item["end"]): item["candidate_id"] for item in host_candidates(RAW)}


def documented(arm):
    """A payload built only from what the arm's prompt documents."""
    if arm == "C":
        request_evidence = {"evidence": [_span(0, len(RAW))], "universe_evidence": _span(NOUN, NOUN + 9)}
        condition_evidence = {"evidence": [_span(TOPIC, len(RAW))]}
    else:
        ids = _ids()
        request_evidence = {"candidate_ids": [ids[(0, len(RAW))]],
                            "universe_evidence_candidate_ids": [ids[(NOUN, NOUN + 9)]]}
        condition_evidence = {"candidate_ids": [ids[(TOPIC, len(RAW))]]}
    # The universe claim is part of every request (ADR-101: no default).
    kind = {"universe_kind": "records"}
    return {"requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "contracts",
                          **kind, **request_evidence,
                          "condition_expression": {"operator": "ATOM", "condition_id": "c1"}}],
            "conditions": [{"condition_id": "c1", "request_ids": ["r1"], "dimension": "TOPIC",
                            "operator": "contains", "polarity": "POSITIVE", "role": "FILTER",
                            **condition_evidence}],
            "relations": [], "omissions": []}


@pytest.mark.parametrize("arm", ["B", "C"])
def test_documented_shape_decodes(arm):
    candidates = host_candidates(RAW) if arm == "B" else ()
    proposal = decode_proposal(documented(arm), RAW, arm=arm, candidates=candidates)
    assert isinstance(proposal, InterpretationProposal)
    assert proposal.producer_version == PROMPT_VERSION == PRODUCER_VERSION


def test_producer_sends_the_frozen_b_prompt_once():
    seen = []

    def provider(system, payload):
        seen.append((system, payload["arm"], payload["host_candidates"]))
        return ProviderReply(documented("B"), model="reported-model")

    producer = LinguisticProducer(provider)
    assert isinstance(producer.produce(RAW), InterpretationProposal)
    assert seen == [(SYSTEM_PROMPT, "B", host_candidates(RAW))]
    receipt = producer.receipt.to_dict()
    assert receipt["provider_calls"] == 1 and receipt["provider_model_reported"] == "reported-model"
    assert receipt["prompt_sha256"] == PROMPT_SHA256 and receipt["producer_version"] == "4.2"
    assert receipt["provider_error"] is None


def test_the_prompt_is_byte_frozen():
    """ADR-102: the product prompt is B v4.2, SHA-256 a5867d40...; a new byte is a new producer."""
    assert PROMPT_SHA256 == "a5867d40cb0027c1ebd32714b61efc9d97bfc82730afaee1e9d58007214340f4"
    assert hashlib.sha256(system_prompt().encode("utf-8")).hexdigest() == PROMPT_SHA256
    assert system_prompt() is SYSTEM_PROMPT
    assert PROMPT_VERSION == PRODUCER_VERSION == "4.2" and PRODUCER_ARM == "B"


def test_no_other_version_or_arm_is_reachable():
    public = {name for name in dir(foundation_prompts) if not name.startswith("_")}
    assert not {"prompts", "PROMPT_VERSIONS", "SYSTEM_PROMPTS"} & public
    assert not hasattr(foundation_prompts, "_EVIDENCE_C")
    assert not hasattr(foundation_prompts, "_CONVENTIONS_V4_1")
    with pytest.raises(TypeError):
        system_prompt("C")  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        LinguisticProducer("C", lambda system, prompt: {})  # type: ignore[call-arg]


def test_provider_failure_is_provider_error_and_malformed_answer_is_invalid():
    def down(system, payload):
        raise TimeoutError("provider timed out")

    failed = LinguisticProducer(down)
    outcome = failed.produce(RAW)
    assert outcome.status is AcceptanceStatus.PROVIDER_ERROR and outcome.reason == "PROVIDER_ERROR:TimeoutError"
    assert failed.receipt.provider_error == "TimeoutError" and failed.receipt.provider_calls == 1
    for answer in ("not json", ["a", "list"], {"requests": "x"}, None, 42):
        producer = LinguisticProducer(lambda system, payload, answer=answer: answer)
        rejected = producer.produce(RAW)
        assert rejected.status is AcceptanceStatus.INVALID_PROPOSAL, answer
        assert producer.receipt.provider_error is None


def test_prompt_names_the_b_evidence_keys_only():
    quoted = set(re.findall(r'"([a-z_]+)"', SYSTEM_PROMPT))
    shared = {"requests", "conditions", "relations", "omissions", "request_id", "scope_id", "operation",
              "subject", "condition_expression", "condition_id", "children", "request_ids", "dimension",
              "operator", "polarity", "role", "unsupported_label", "kind", "members", "state", "universe_kind"}
    assert shared <= quoted
    assert {"candidate_ids", "universe_evidence_candidate_ids", "source_evidence_candidate_ids"} <= quoted
    assert ('"evidence", "universe_evidence" and "source_evidence" are invalid in B'
            in " ".join(SYSTEM_PROMPT.split()))
    assert "RETRIEVAL_INTENT" in SYSTEM_PROMPT  # only to forbid it
    assert "Never emit dimension RETRIEVAL_INTENT" in " ".join(SYSTEM_PROMPT.split())


@pytest.mark.parametrize(("arm", "mutate", "reason"), [
    # The shapes observed in every rejected response of the first real pass.
    ("C", lambda p: p["requests"][0].update(evidence=p["requests"][0]["evidence"][0]), "MISSING_RAW_EVIDENCE"),
    ("C", lambda p: p["conditions"][0].update(evidence=p["conditions"][0]["evidence"][0]), "MISSING_RAW_EVIDENCE"),
    ("B", lambda p: p["requests"][0].update(evidence=[_span(0, len(RAW))]), "INVALID_LINGUISTIC_RECORD_KEYS"),
    ("B", lambda p: p.update(type="interpretation"), "INVALID_LINGUISTIC_RECORD_KEYS"),
    # The shapes the second real pass (prompt v2) produced once the keys were right.
    ("C", lambda p: p.update(relations=[{"kind": "SHARES_SCOPE", "members": ["r1"]}]), "INVALID_RELATION_ARITY"),
    ("B", lambda p: p["requests"][0].update(condition_expression={
        "operator": "AND", "children": [{"operator": "ATOM", "condition_id": "c1"}]}), "INVALID_GROUP_ARITY"),
    # A miscounted offset with a right quote: arm C grounds evidence on the model's counting.
    ("C", lambda p: p["conditions"][0].update(evidence=[{"start": TOPIC, "end": len(RAW) + 2,
                                                          "quote": "vigilancia"}]), "UNBOUND_RAW_SPAN"),
])
def test_rejected_pass_deviations_stay_rejected(arm, mutate, reason):
    payload = documented(arm)
    mutate(payload)
    candidates = host_candidates(RAW) if arm == "B" else ()
    with pytest.raises(ContractError, match=reason):
        decode_proposal(payload, RAW, arm=arm, candidates=candidates)
    if arm == "B":
        rejected = LinguisticProducer(lambda system, prompt: payload).produce(RAW)
        assert rejected.status is AcceptanceStatus.INVALID_PROPOSAL and rejected.reason == reason


# Prompt 4.2: an opportunity travels once, as "universe_kind"; the decoder writes the condition.
OPP = "hay licitaciones de fumigación para postularme"  # Invented: noun and intention coexist.


def _opportunity_payload(arm, universe_quote, kind="opportunities"):
    ids = {item["quote"]: item["candidate_id"] for item in host_candidates(OPP)}
    def span(text):
        return {"start": OPP.index(text), "end": OPP.index(text) + len(text), "quote": text}
    if arm == "B":
        request_evidence = {"candidate_ids": [ids[OPP]], "universe_evidence_candidate_ids": [ids[universe_quote]]}
        topic = {"candidate_ids": [ids["fumigación"]]}
    else:
        request_evidence = {"evidence": [span(OPP)], "universe_evidence": span(universe_quote)}
        topic = {"evidence": [span("fumigación")]}
    return {"requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "processes",
                          "universe_kind": kind, **request_evidence,
                          "condition_expression": {"operator": "ATOM", "condition_id": "t"}}],
            "conditions": [{"condition_id": "t", "request_ids": ["r1"], "dimension": "TOPIC",
                            "operator": "contains", "polarity": "POSITIVE", "role": "FILTER", **topic}],
            "relations": [], "omissions": []}


@pytest.mark.parametrize("arm", ["B", "C"])
@pytest.mark.parametrize("universe_quote", ["licitaciones", "para postularme"])
def test_universe_kind_writes_one_opportunity_that_cannot_disagree(arm, universe_quote):
    candidates = host_candidates(OPP) if arm == "B" else ()
    proposal = decode_proposal(_opportunity_payload(arm, universe_quote), OPP, arm=arm, candidates=candidates)
    (request,) = proposal.requests
    (universe,) = [c for c in proposal.conditions if c.role.value == "REQUESTED_UNIVERSE"]
    assert universe.dimension.value == "RETRIEVAL_INTENT" and universe.operator == "is"
    assert universe.polarity.value == "POSITIVE" and universe.request_ids == ("r1",)
    assert universe.evidence == (request.universe_evidence,) and request.universe_evidence.quote == universe_quote
    assert request.condition_expression.operator.value == "AND"
    assert set(request.condition_expression.atoms()) == {"t", universe.condition_id}


@pytest.mark.parametrize("arm", ["B", "C"])
def test_universe_kind_records_adds_nothing_and_bad_shapes_are_rejected(arm):
    candidates = host_candidates(OPP) if arm == "B" else ()
    plain = decode_proposal(_opportunity_payload(arm, "licitaciones", "records"), OPP, arm=arm,
                            candidates=candidates)
    assert [c.condition_id for c in plain.conditions] == ["t"]
    with pytest.raises(ContractError, match="INVALID_UNIVERSE_KIND"):
        decode_proposal(_opportunity_payload(arm, "licitaciones", "opportunity"), OPP, arm=arm,
                        candidates=candidates)
    # An omitted claim is not a "records" claim (ADR-101).
    omitted = _opportunity_payload(arm, "licitaciones")
    del omitted["requests"][0]["universe_kind"]
    with pytest.raises(ContractError, match="MISSING_UNIVERSE_KIND"):
        decode_proposal(omitted, OPP, arm=arm, candidates=candidates)
    duplicated = decode_proposal(_opportunity_payload(arm, "licitaciones"), OPP, arm=arm, candidates=candidates)
    explicit = _opportunity_payload(arm, "licitaciones", "records")
    universe = [c for c in duplicated.conditions if c.role.value == "REQUESTED_UNIVERSE"][0]
    explicit["conditions"].append({"condition_id": universe.condition_id, "request_ids": ["r1"],
                                   "dimension": "RETRIEVAL_INTENT", "operator": "is", "polarity": "POSITIVE",
                                   "role": "REQUESTED_UNIVERSE",
                                   **({"candidate_ids": explicit["requests"][0]["universe_evidence_candidate_ids"]}
                                      if arm == "B" else {"evidence": [explicit["requests"][0]["universe_evidence"]]})})
    # The pre-4.2 duplicated form is never taken back.
    with pytest.raises(ContractError, match="RETRIEVAL_INTENT_REPLACED_BY_UNIVERSE_KIND"):
        decode_proposal(explicit, OPP, arm=arm, candidates=candidates)
