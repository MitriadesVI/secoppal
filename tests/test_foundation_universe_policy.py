"""ADR-101/102: the host decides the universe of a cited word (H7); nothing is ever added.

Phrases are invented: none is a development or held-out case. The closed
vocabularies are enumerated from their canonical sources, not listed by hand.
"""

from __future__ import annotations

import hashlib
from datetime import datetime

import pytest

from app.core.foundation_admission import state_value
from app.core.foundation_lift_v1 import lift_level
from app.core.foundation_operations import Subject
from app.core.foundation_runtime import host_universe
from app.core.foundation_universe_policy import (
    CALL_HEADS, OPPORTUNITY_HEADS, PARTICIPATION_VERBS, TENDER_HEADS, apply_universe_policy,
)
from app.core.intent_vocabulary import BIDDER_PARTICIPATION_VERBS
from app.core.interpretation_contract import (
    ConditionExpression, ConditionRole, Effect, InterpretationProposal, LogicOperator, Polarity,
    ProposedCondition, ProposedRelation, ProposedRequest, RawSpan, RelationKind,
)
from app.core.semantic_dimensions import SemanticDimension
from app.utils.money import normalize_text
from tests.foundation_ir import search_jobs


def _span(raw, text, after=0):
    start = raw.index(text, after)
    return RawSpan(start, start + len(text), text)


def proposal(raw, universe, conditions=(), *, subject="processes", opportunity=False, requests=None):
    """One request (or the given ones) with its conditions; ``opportunity`` is the producer's claim."""
    built, atoms = [], []
    for index, (dimension, operator, quote) in enumerate(conditions):
        cid = f"c{index}"
        built.append(ProposedCondition(cid, ("r1",), SemanticDimension(dimension), operator, Polarity.POSITIVE,
                                       quote, (_span(raw, quote),), level=lift_level(SemanticDimension(dimension))))
        atoms.append(cid)
    universe_span = _span(raw, universe)
    if opportunity:
        built.append(ProposedCondition("u", ("r1",), SemanticDimension.RETRIEVAL_INTENT, "is", Polarity.POSITIVE,
                                       universe, (universe_span,), role=ConditionRole.REQUESTED_UNIVERSE))
        atoms.append("u")
    expression = (None if not atoms else ConditionExpression(LogicOperator.ATOM, atoms[0]) if len(atoms) == 1
                  else ConditionExpression(LogicOperator.AND, None,
                                           tuple(ConditionExpression(LogicOperator.ATOM, a) for a in atoms)))
    request = ProposedRequest("r1", "s1", Subject(subject), (RawSpan(0, len(raw), raw),),
                              universe_claim=universe, universe_evidence=universe_span,
                              condition_expression=expression)
    requests = requests or (request,)
    return InterpretationProposal(hashlib.sha256(raw.encode()).hexdigest(), "foundation-B", "4.2",
                                  requests, tuple(built), jobs=search_jobs(requests))


def decision(outcome):
    (item,) = outcome.decisions
    return item


def opportunity_condition(outcome, request_id="r1"):
    return [c for c in outcome.proposal.conditions
            if c.role is ConditionRole.REQUESTED_UNIVERSE and request_id in c.request_ids]


# ── O6: colloquial «licitación» ──────────────────────────────────────────────

def test_o6a_bare_tender_on_processes_is_an_opportunity_whatever_the_producer_said():
    raw = "una licitación de fumigación en el cesar"
    outcome = apply_universe_policy(proposal(raw, "licitación", [("TOPIC", "contains", "fumigación"),
                                                                  ("GEOGRAPHY", "is", "cesar")]))
    item = decision(outcome)
    assert (item.producer_kind, item.effective_kind, item.authority, item.rules, item.effect) == (
        "records", "opportunities", "HOST_CLOSED_POLICY", ("O6A",), Effect.NARROWING)
    (added,) = opportunity_condition(outcome)
    request = outcome.proposal.requests[0]
    # The added condition cites the producer's own universe evidence: it cannot disagree with it.
    assert added.evidence == (request.universe_evidence,) and added.claim == request.universe_claim
    assert set(request.condition_expression.atoms()) == {"c0", "c1", added.condition_id}


def test_o6b_a_year_or_a_resolved_state_makes_it_historical_and_drops_the_producer_opportunity():
    raw = "licitaciones de fumigación en 2021"
    outcome = apply_universe_policy(proposal(raw, "licitaciones", [("TOPIC", "contains", "fumigación"),
                                                                    ("TIME", "during_year", "2021")],
                                             opportunity=True))
    item = decision(outcome)
    assert (item.producer_kind, item.effective_kind, item.rules, item.effect) == (
        "opportunities", "records", ("O6B",), Effect.WIDENING)
    assert not opportunity_condition(outcome)
    assert set(outcome.proposal.requests[0].condition_expression.atoms()) == {"c0", "c1"}
    stated = "licitaciones abiertas de fumigación"
    assert decision(apply_universe_policy(proposal(stated, "licitaciones", [
        ("STATE", "family", "abiertas"), ("TOPIC", "contains", "fumigación")]))).rules == ("O6B",)


def test_an_invented_state_the_host_does_not_resolve_does_not_flip_o6():
    raw = "licitaciones de fumigación que están"
    invented = proposal(raw, "licitaciones", [("TOPIC", "contains", "fumigación"), ("STATE", "family", "están")])
    assert state_value(invented.conditions[1]) is None
    assert decision(apply_universe_policy(invented)).rules == ("O6A",)


def test_a_named_modality_or_public_tender_is_not_colloquial():
    raw = "licitaciones públicas de fumigación"
    assert decision(apply_universe_policy(proposal(raw, "licitaciones públicas", [
        ("TOPIC", "contains", "fumigación")]))).authority == "PRODUCER"
    assert decision(apply_universe_policy(proposal(raw, "licitaciones", [
        ("MODALITY", "family", "públicas"), ("TOPIC", "contains", "fumigación")]))).authority == "PRODUCER"


def test_o6c_a_dual_scope_is_historical_in_both_requests():
    raw = "licitaciones y contratos de fumigación"
    span_l, span_c, span_t = _span(raw, "licitaciones"), _span(raw, "contratos"), _span(raw, "fumigación")
    requests = (ProposedRequest("r1", "s1", Subject.PROCESSES, (RawSpan(0, len(raw), raw),),
                                universe_claim="licitaciones", universe_evidence=span_l,
                                condition_expression=ConditionExpression(LogicOperator.AND, None, (
                                    ConditionExpression(LogicOperator.ATOM, "u"),
                                    ConditionExpression(LogicOperator.ATOM, "t1")))),
                ProposedRequest("r2", "s1", Subject.CONTRACTS, (RawSpan(0, len(raw), raw),),
                                universe_claim="contratos", universe_evidence=span_c,
                                condition_expression=ConditionExpression(LogicOperator.ATOM, "t2")))
    conditions = (ProposedCondition("u", ("r1",), SemanticDimension.RETRIEVAL_INTENT, "is", Polarity.POSITIVE,
                                    "licitaciones", (span_l,), role=ConditionRole.REQUESTED_UNIVERSE),
                  ProposedCondition("t1", ("r1",), SemanticDimension.TOPIC, "contains", Polarity.POSITIVE,
                                    "fumigación", (span_t,)),
                  ProposedCondition("t2", ("r2",), SemanticDimension.TOPIC, "contains", Polarity.POSITIVE,
                                    "fumigación", (span_t,)))
    dual = InterpretationProposal(hashlib.sha256(raw.encode()).hexdigest(), "foundation-B", "4.2", requests,
                                  conditions, (ProposedRelation(RelationKind.SHARES_SCOPE, ("r1", "r2")),),
                                  jobs=search_jobs(requests))
    outcome = apply_universe_policy(dual)
    first, second = outcome.decisions
    assert (first.rules, first.effective_kind) == (("O6C",), "records")
    assert (second.authority, second.effective_kind) == ("PRODUCER", "records")
    assert not opportunity_condition(outcome)


# ── Heads and verbs: the closed domain, enumerated from its sources ─────────

@pytest.mark.parametrize("word,rule", [*((w, "O3") for w in sorted(OPPORTUNITY_HEADS)),
                                       *((w, "O7") for w in sorted(CALL_HEADS))])
def test_every_opportunity_and_call_head_decides_opportunity_on_processes(word, rule):
    raw = f"hay {word} de fumigación"
    item = decision(apply_universe_policy(proposal(raw, word, [("TOPIC", "contains", "fumigación")])))
    assert (item.effective_kind, item.rules) == ("opportunities", (rule,))


def test_participation_verbs_are_the_canonical_family_not_a_copy():
    assert PARTICIPATION_VERBS == {normalize_text(verb) for verb in BIDDER_PARTICIPATION_VERBS}


@pytest.mark.parametrize("verb", BIDDER_PARTICIPATION_VERBS)
def test_every_participation_verb_decides_opportunity(verb):
    raw = f"procesos de fumigación para {verb}"
    item = decision(apply_universe_policy(proposal(raw, f"para {verb}", [("TOPIC", "contains", "fumigación")])))
    assert (item.effective_kind, item.rules) == ("opportunities", ("O3-INTENT",))


@pytest.mark.parametrize("word", sorted(OPPORTUNITY_HEADS | CALL_HEADS | TENDER_HEADS | PARTICIPATION_VERBS))
def test_no_closed_word_makes_a_contracts_request_an_opportunity(word):
    raw = f"contratos {word} de fumigación"
    item = decision(apply_universe_policy(proposal(raw, f"contratos {word}", [("TOPIC", "contains", "fumigación")],
                                                   subject="contracts")))
    assert item.effective_kind == "records"


@pytest.mark.parametrize("word", sorted(OPPORTUNITY_HEADS | CALL_HEADS | TENDER_HEADS | PARTICIPATION_VERBS))
def test_a_closed_word_that_is_topic_content_decides_nothing(word):
    # The policy reads the cited universe, never the whole turn: a topic mentioning the word stays a topic.
    raw = f"procesos cuyo tema sea {word} cultural"
    outcome = apply_universe_policy(proposal(raw, "procesos", [("TOPIC", "contains", f"{word} cultural")]))
    assert (decision(outcome).authority, decision(outcome).effective_kind) == ("PRODUCER", "records")


@pytest.mark.parametrize("word", sorted(OPPORTUNITY_HEADS | CALL_HEADS | TENDER_HEADS | PARTICIPATION_VERBS))
def test_a_closed_word_outside_every_citation_decides_nothing(word):
    # Uncited material is the guards' business (G4); the policy never rereads the turn.
    raw = f"procesos de aseo como la {word} pasada"
    outcome = apply_universe_policy(proposal(raw, "procesos", [("TOPIC", "contains", "aseo")]))
    assert (decision(outcome).authority, decision(outcome).effective_kind) == ("PRODUCER", "records")


def test_a_universe_word_also_cited_by_a_topic_is_that_topic():
    raw = "convocatorias de artistas"
    outcome = apply_universe_policy(proposal(raw, "convocatorias", [("TOPIC", "contains", "convocatorias de artistas")]))
    assert decision(outcome).authority == "PRODUCER"


def test_outside_the_closed_domain_the_producer_claim_stands():
    raw = "soy proveedor de fumigación"
    for claimed in (True, False):
        item = decision(apply_universe_policy(proposal(raw, "soy proveedor", [("TOPIC", "contains", "fumigación")],
                                                       opportunity=claimed)))
        assert item.authority == "PRODUCER" and item.effect is Effect.EQUIVALENT
        assert item.effective_kind == ("opportunities" if claimed else "records")


def test_two_closed_rules_that_disagree_ask_for_clarification():
    raw = "licitaciones para presentarme de 2021"
    outcome = apply_universe_policy(proposal(raw, "licitaciones para presentarme",
                                             [("TIME", "during_year", "2021")]))
    assert outcome.conflict == "HOST_UNIVERSE_RULE_CONFLICT:r1:O3-INTENT+O6B"
    assert outcome.proposal.requests[0].condition_expression.atoms() == ("c0",)


# ── The historical tender complement is not part of the product (ADR-102) ───

TENDER = "las licitaciones de la gobernación del meta en 2022"


def tender(extra=()):
    return proposal(TENDER, "licitaciones", [("BUYER", "is", "gobernación del meta"),
                                             ("TIME", "during_year", "2022"), *extra])


def test_a_historical_tender_is_a_universe_decision_and_nothing_is_added():
    outcome = host_universe(tender())
    (request,) = outcome.proposal.requests
    assert request.subject == "processes" and not outcome.proposal.relations
    assert {c.dimension for c in outcome.proposal.conditions} == {SemanticDimension.BUYER, SemanticDimension.TIME}
    trace = outcome.to_dict()
    assert "complements" not in trace and not hasattr(outcome, "complements")
    assert trace["decisions"][0]["rules"] == ["O6B"]


@pytest.mark.parametrize("build", [
    lambda: tender(),
    lambda: proposal("una licitación de fumigación", "licitación", [("TOPIC", "contains", "fumigación")]),
    lambda: proposal("licitaciones de fumigación en 2021", "licitaciones",
                     [("TOPIC", "contains", "fumigación"), ("TIME", "during_year", "2021")], opportunity=True),
    lambda: proposal("soy proveedor de fumigación", "soy proveedor", [("TOPIC", "contains", "fumigación")]),
])
def test_the_host_policy_is_idempotent(build):
    once = host_universe(build())
    assert host_universe(once.proposal).proposal == once.proposal


# ── Runtime: behind its flag, traced, and the widening is disclosed ─────────

RUNTIME_RAW = "licitaciones de la alcaldía de barranquilla en 2024"
NOW = datetime.fromisoformat("2026-09-26T12:00:00-05:00")


def _source_rows():
    from app.core.foundation_projection import SOURCE_SELECT_FIELDS
    from app.core.soql_builder import SoQLBuilder

    process = dict.fromkeys(SOURCE_SELECT_FIELDS[SoQLBuilder.PROCESOS_DATASET])
    contract = dict.fromkeys(SOURCE_SELECT_FIELDS[SoQLBuilder.CONTRATOS_DATASET])
    process.update(id_del_proceso="P1", entidad="ALCALDIA DE BARRANQUILLA", fecha_de_publicacion_del="2024-03-01")
    contract.update(id_contrato="K1", nombre_entidad="ALCALDIA DE BARRANQUILLA", fecha_de_firma="2024-04-01",
                    estado_contrato="En ejecución")
    return {SoQLBuilder.PROCESOS_DATASET: [process], SoQLBuilder.CONTRATOS_DATASET: [contract]}


def test_runtime_always_applies_h7_and_never_adds_the_contracts_view(tmp_path, monkeypatch):
    from tests.product_contract.harness import ProductHarness

    produced = proposal(RUNTIME_RAW, "licitaciones", [("BUYER", "is", "alcaldía de barranquilla"),
                                                      ("TIME", "during_year", "2024")])
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=_source_rows(), reference_instant=NOW,
                             today=NOW.date(), foundation_producer=lambda raw: produced,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    result = harness.say(RUNTIME_RAW).result
    datasets = [call["dataset"] for call in harness.transport.snapshot()]
    assert result["foundation_mode"] == "foundation_v3"
    assert result["foundation_universe"]["policy"] == "H7@1"
    assert result["foundation_universe"]["decisions"][0]["rules"] == ["O6B"]
    assert "complements" not in result["foundation_universe"]
    assert set(datasets) == {"p6dx-8zbt"}
    assert "Contratos:" not in result["response"]


def test_runtime_turns_a_rule_conflict_into_a_clarification_without_calls(tmp_path, monkeypatch):
    from tests.product_contract.harness import ProductHarness

    raw = "licitaciones para presentarme de 2021"
    produced = proposal(raw, "licitaciones para presentarme", [("TIME", "during_year", "2021")])
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=_source_rows(), reference_instant=NOW,
                             today=NOW.date(), foundation_producer=lambda _: produced,
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    result = harness.say(raw).result
    assert result["foundation_mode"] == "foundation_v3"
    assert result["needs_clarification"] and not harness.transport.snapshot()
    assert result["foundation_universe"]["conflict"].startswith("HOST_UNIVERSE_RULE_CONFLICT")
