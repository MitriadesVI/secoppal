"""A2 Driver Remediation V1 — SUPPLIERS input contract.

Parameters come from the real deterministic router; the contract decides only
whether SUPPLIERS may execute them.
"""
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from hypothesis import given, settings, strategies as st

from app.core.capability_binding import residual, supplier_contract
from app.core.capability_binding.registry import SUPPLIERS, capability_for
from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.capability_binding.supplier_request_authority import assess_supplier_request
from app.core.capability_binding.supplier_request_types import Authorization, Polarity
from app.core.entity_resolver import EntityResolver
from app.core.natural_alpha.models import GroundedJob, Observation
from app.core.query_router import QueryRouter

TODAY = date(2026, 9, 15)


@pytest.fixture(scope="module")
def router():
    return QueryRouter(entity_resolver=EntityResolver(Path("app/data/aliases_db.json")))


def _decide(router, text):
    return decide(text, router.parse(text, today=TODAY).params)


EXECUTE = [
    "cuales son los 5 proveedores con mas contratos en la alcaldia de barranquilla en 2025?",
    "proveedor con mayor valor de contrato en el 2026 en la alcaldia de barranquilla",
    "top proveedores de la gobernación del atlántico",
    "los 5 proveedores de la alcaldía de Barranquilla",
    "top 5 proveedores de la alcaldía de Barranquilla",
    "proveedores de la alcaldía de Barranquilla entre 2024 y 2026",
    "contratos de proveedores de la alcaldía de Soledad",
    "los cinco principales proveedores de la alcaldía de Galapa",
]


@pytest.mark.parametrize("text", EXECUTE)
def test_accredited_supplier_questions_execute(router, text):
    decision = _decide(router, text)
    assert decision.outcome is Outcome.EXECUTE
    # Accounted terms are another dimension, never topic sent to the query.
    assert not any(key in decision.admitted_params for key in ("objeto", "objeto_or", "objeto_phrases"))


GUARD_TOPICS = [
    ("proveedores de obras viales de la alcaldía de Galapa", "obras"),
    ("principales proveedores de mantenimiento de la alcaldía de Barranquilla", "mantenimiento"),
    ("proveedores de alimentación de la alcaldía de Cartagena en 2025", "alimentacion"),
    ("proveedores de software de la alcaldía de Soledad", "software"),
]


@pytest.mark.parametrize("text,term", GUARD_TOPICS)
def test_a_real_topic_blocks_instead_of_widening_the_universe(router, text, term):
    decision = _decide(router, text)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "UNACCREDITED_RESIDUAL"
    assert term in decision.detail


@pytest.mark.parametrize("text", [
    "proveedores de educación de la Secretaría de Educación de Cali",
    "proveedores de educación de educación de la Secretaría de Educación de Cali",
])
def test_topic_requires_its_own_accredited_occurrence(router, text):
    decision = _decide(router, text)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "UNACCREDITED_RESIDUAL"
    assert "educacion" in decision.detail


def test_every_or_topic_alternative_requires_causal_accreditation(router):
    text = "proveedores de la Secretaría de Educación de Cali"
    params = router.parse(text, today=TODAY).params
    params["objeto"] = [["obras viales", "educacion"]]
    decision = decide(text, params)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "UNACCREDITED_RESIDUAL"
    assert "obras viales" in decision.detail


@settings(max_examples=20, deadline=None)
@given(extra=st.text(alphabet="bcdfghjklmnpqrstvwxz", min_size=4, max_size=10))
def test_any_unaccredited_or_alternative_blocks_the_group(router, extra):
    text = "proveedores de la Secretaría de Educación de Cali"
    params = router.parse(text, today=TODAY).params
    params["objeto"] = [[extra, "educacion"]]
    decision = decide(text, params)
    assert decision.outcome is Outcome.CLARIFY
    assert extra in decision.detail


@pytest.mark.parametrize("text,reason", [
    ("dame los 10 proveedores de la alcaldía de Soledad", "UNSUPPORTED_CARDINALITY"),
    ("top 20 proveedores de la alcaldía de Galapa", "UNSUPPORTED_CARDINALITY"),
    ("proveedores de la alcaldía de Galapa en contratación directa", "UNSUPPORTED_DIMENSION"),
    ("proveedores de la alcaldía de Soledad con contratos de más de 500 millones", "UNSUPPORTED_DIMENSION"),
    ("proveedores activos de la alcaldía de Soledad", "UNSUPPORTED_DIMENSION"),
    ("quiénes son sus proveedores?", "BUYER_REQUIRED"),  # C4A.3: "sus"/"son" ya no son material
    ("los proveedores con mas contratos?", "BUYER_REQUIRED"),
    ("proveedores de la alcaldía de San Juan", "BUYER_NOT_ACCREDITED"),
])
def test_unsupported_or_unaccredited_input_clarifies(router, text, reason):
    decision = _decide(router, text)
    assert (decision.outcome, decision.reason) == (Outcome.CLARIFY, reason)
    assert decision.response


def test_a2_still_rejects_fuzzy_buyer_after_global_parity_restore(router):
    text = "proveedores de la alcaldía de San Juan"
    decision = _decide(router, text)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "BUYER_NOT_ACCREDITED"
    assert decision.detail == ("Municipio de san juan",)


def test_missing_supplier_witness_is_not_bound(router):
    text = "analiza la alcaldía de Barranquilla"
    decision = _decide(router, text)
    assert (decision.outcome, decision.reason) == (
        Outcome.NOT_BOUND, "INTENT_WITNESS_MISSING",
    )


@pytest.mark.parametrize("text,polarity", [
    ("sin proveedores", Polarity.NEGATED),
    ("no sus proveedores", Polarity.NEGATED),
    ("proveedores no", Polarity.UNKNOWN),
])
def test_non_positive_supplier_witness_clarifies(router, text, polarity):
    query = f"{text} de la alcaldía de Barranquilla"
    assert assess_supplier_request(query).polarity is polarity
    decision = _decide(router, query)
    assert (decision.outcome, decision.reason) == (
        Outcome.CLARIFY, "INTENT_WITNESS_NOT_POSITIVE",
    )


def test_actor_subject_precedes_supplier_polarity_and_cardinality_guards(router):
    text = "5 proveedores en adelante del proveedor NIT 900711074"
    params = router.parse(text, today=TODAY).params
    assert params["actor_scope"]["base_number"] == "900711074"
    decision = decide(text, params)
    assert (decision.outcome, decision.reason) == (Outcome.NOT_BOUND, "ACTOR_SUBJECT")


@settings(max_examples=24, deadline=None)
@given(
    marker=st.sampled_from(["sin", "excepto"]),
    determiner=st.sampled_from(["", "el ", "la ", "los ", "las ", "su ", "sus "]),
    noun=st.sampled_from(["proveedor", "proveedores", "contratista", "contratistas"]),
)
def test_negated_supplier_morphology_never_becomes_a_positive_witness(
    marker, determiner, noun,
):
    text = f"{marker} {determiner}{noun}"
    evidence = assess_supplier_request(text)
    assert evidence.polarity is Polarity.NEGATED
    assert evidence.authorization is Authorization.NOT_ACCREDITED


@pytest.mark.parametrize("text", [
    "proveedores de la alcaldía de Barranquilla en Galapa",
    "proveedores de la alcaldía de Barranquilla en el departamento del Atlántico",
])
def test_buyer_plus_independent_geography_clarifies_without_projection(router, text):
    decision = _decide(router, text)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "GEOGRAPHY_WITH_BUYER"
    assert decision.detail
    assert decision.response


@pytest.mark.parametrize("text,reason", [
    ("cómo contrata el proveedor Fundacaribe", "ACTOR_SUBJECT"),
    ("contratos del proveedor fundacaribe en 2026", "ACTOR_SUBJECT"),
    ("con quién contrata más la alcaldía de Cartagena?", "INTENT_WITNESS_MISSING"),
    ("proveedores en Antioquia en 2025", "UNIVERSE_WITHOUT_BUYER"),
])
def test_turns_that_are_not_this_capability_are_not_bound(router, text, reason):
    assert (_decide(router, text).outcome, _decide(router, text).reason) == (Outcome.NOT_BOUND, reason)


@pytest.mark.parametrize("text", [
    "más de 5 proveedores de la alcaldía de Barranquilla",
    "menos de 5 proveedores de la alcaldía de Barranquilla",
    "entre 5 y 10 proveedores de la alcaldía de Barranquilla",
    "5 proveedores o más de la alcaldía de Barranquilla",
    "5 proveedores como mínimo de la alcaldía de Barranquilla",
    "al menos, 5 proveedores de la alcaldía de Barranquilla",
])
def test_non_exact_cardinality_never_executes(router, text):
    decision = _decide(router, text)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "UNSUPPORTED_CARDINALITY"
    assert decision.response


@settings(max_examples=20, deadline=None)
@given(requested=st.integers(min_value=1, max_value=20).filter(lambda value: value != 5))
def test_any_exact_supplier_limit_other_than_five_is_unsupported(router, requested):
    text = f"{requested} proveedores de la alcaldía de Barranquilla"
    assert assess_supplier_request(text).authorization is Authorization.NOT_ACCREDITED
    assert _decide(router, text).outcome is Outcome.CLARIFY


def test_external_unaccredited_number_cannot_execute(router):
    accredited = "proveedores de la alcaldía de Galapa"
    params = router.parse(accredited, today=TODAY).params
    decision = decide("77 codigo proveedores de la alcaldía de Galapa", params)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "UNACCREDITED_RESIDUAL"
    assert "77" in decision.detail


_TOKENS = st.text(alphabet="bcdfghjklmnpqrstvwxz", min_size=4, max_size=10)


@settings(max_examples=60, deadline=None)
@given(extra=_TOKENS, key=st.sampled_from(["objeto", "objeto_or", "objeto_phrases"]))
def test_any_unexplained_topic_param_prevents_execution(router, extra, key):
    """INV: an EXECUTE decision never silently drops a topic-like parameter."""
    text = "proveedores de la alcaldía de Galapa en 2025"
    params = router.parse(text, today=TODAY).params
    assert decide(text, params).outcome is Outcome.EXECUTE
    mutated = {**params, key: [*(params.get(key) or []), extra]}
    decision = decide(text, mutated)
    assert decision.outcome is Outcome.CLARIFY
    assert extra in decision.detail


def test_contract_assesses_once_and_passes_identical_evidence_to_residual(
    router, monkeypatch,
):
    text = "5 proveedores de la alcaldía de Galapa"
    params = router.parse(text, today=TODAY).params
    real_assess = supplier_contract.assess_supplier_request
    real_account = residual.account
    seen = []

    def recording_assess(query):
        evidence = real_assess(query)
        seen.append(evidence)
        return evidence

    def recording_account(query, parsed, *, supplier_evidence, today=None):
        assert supplier_evidence is seen[0]
        return real_account(
            query,
            parsed,
            supplier_evidence=supplier_evidence,
            today=today,
        )

    monkeypatch.setattr(supplier_contract, "assess_supplier_request", recording_assess)
    monkeypatch.setattr(supplier_contract, "account", recording_account)
    assert supplier_contract.decide(text, params).outcome is Outcome.EXECUTE
    assert len(seen) == 1


def _observation(action="ANALYZE", aspect="SUPPLIERS", mode="alpha", jobs=1, status="MATCHED"):
    job = GroundedJob(action, aspect, 0, 5)
    return Observation(mode, status, (job,) * jobs)


@pytest.mark.parametrize("observation,enabled,expected", [
    (_observation(), True, SUPPLIERS),
    (_observation(), False, None),
    (_observation(mode="shadow"), True, None),
    (_observation(jobs=2, status="MULTI_JOB_BLOCKED"), True, None),
    (_observation(aspect="TOP_ENTITIES"), True, None),
    (_observation(aspect="MODALITIES"), True, None),
    (None, True, None),
])
def test_registry_binds_only_alpha_single_suppliers(observation, enabled, expected):
    settings_ = SimpleNamespace(secoppal_a2_suppliers_binding_enabled=enabled)
    assert capability_for(observation, settings_) == expected
