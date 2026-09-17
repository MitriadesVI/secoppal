"""C1 contracts: credential grounding, declarative registry, and the neutral path.

Nothing here executes a capability: the credential accredits the job and the spec
declares what may be received. The no-credential path keeps every decision where
it is today.
"""
from dataclasses import FrozenInstanceError

import pytest
from hypothesis import given, strategies as st

from app.core.accreditation import (
    CONTRACTS_UNIVERSE,
    SUPPLIERS,
    CapabilityCredential,
    CapabilitySpec,
    Cardinality,
    Dimension,
    Owner,
    capability_spec,
    declared_capabilities,
    from_grounded_job,
    spec_for_credential,
)
from app.core.capability_binding.registry import SUPPLIERS as A2_SUPPLIERS
from app.core.entity_profile.aspects.suppliers import SUPPLIER_LIMIT
from app.core.natural_alpha.models import GroundedJob

RAW = "con quien ha contratado la alcaldia de galapa en 2025"


def credential(**overrides):
    fields = dict(capability=SUPPLIERS, action="ANALYZE", aspect="SUPPLIERS",
                  evidence_interval=(0, 10))
    fields.update(overrides)
    return CapabilityCredential(**fields)


def test_suppliers_spec_is_the_declared_contract():
    spec = capability_spec(SUPPLIERS)
    assert spec is not None
    assert (spec.capability, spec.focus, spec.owner) == (
        SUPPLIERS, "suppliers", Owner.ENTITY_PROFILE)
    assert (spec.action, spec.aspect) == ("ANALYZE", "SUPPLIERS")
    assert spec.universe == CONTRACTS_UNIVERSE
    assert spec.required == frozenset({Dimension.BUYER})
    assert spec.optional == frozenset({Dimension.TIME, Dimension.RANKING})
    assert spec.supported_cardinality == frozenset({Cardinality.NONE, Cardinality.EXACT_5})
    assert spec.unsupported == frozenset({
        Dimension.MONEY, Dimension.MODALITY, Dimension.TOPIC, Dimension.GEOGRAPHY,
    })
    assert declared_capabilities() == (SUPPLIERS,)
    # Same capability id as the A2 binding: if the two registries ever disagree,
    # one of them is lying about the same capability.
    assert SUPPLIERS == A2_SUPPLIERS


def test_declared_cardinality_matches_the_view_limit():
    spec = capability_spec(SUPPLIERS)
    assert spec is not None
    exact = {cardinal.value for cardinal in spec.supported_cardinality
             if cardinal is not Cardinality.NONE}
    assert exact == {SUPPLIER_LIMIT}
    assert Cardinality.NONE.value is None


def test_unknown_capability_and_missing_credential_are_neutral():
    assert capability_spec("entity_profile.modalities") is None
    assert capability_spec(None) is None
    assert capability_spec(123) is None
    assert spec_for_credential(None) is None


def test_credential_only_admits_the_declared_pair():
    assert spec_for_credential(credential()) is capability_spec(SUPPLIERS)
    assert spec_for_credential(credential(aspect="CONTRACTS")) is None
    assert spec_for_credential(credential(action="SEARCH")) is None
    assert spec_for_credential(credential(capability="entity_profile.modalities")) is None


@pytest.mark.parametrize("bad", [
    {"capability": ""}, {"capability": None}, {"capability": 7},
    {"action": ""}, {"aspect": "  "},
    {"source": "LEGACY"},
    {"evidence_interval": (3, 3)}, {"evidence_interval": (4, 3)},
    {"evidence_interval": (-1, 3)}, {"evidence_interval": (0,)},
    {"evidence_interval": [0, 3]}, {"evidence_interval": (True, 3)},
    {"evidence_interval": (0, True)},
])
def test_credential_refuses_malformed_values(bad):
    with pytest.raises(ValueError):
        credential(**bad)


def test_grounded_job_credential_keeps_the_interval_of_the_same_raw():
    start = RAW.index("alcaldia")
    job = GroundedJob("ANALYZE", "SUPPLIERS", start, start + len("alcaldia"))
    built = from_grounded_job(RAW, job, capability=SUPPLIERS)
    assert built.evidence_interval == (start, start + len("alcaldia"))
    assert built.source == "NATURAL_ALPHA"
    assert built.covers(RAW)


@pytest.mark.parametrize("shape", ["beyond", "negative", "empty", "bool"])
def test_ungrounded_interval_is_refused_never_clipped(shape):
    jobs = {
        "beyond": GroundedJob("ANALYZE", "SUPPLIERS", 0, len(RAW) + 5),
        "negative": GroundedJob("ANALYZE", "SUPPLIERS", -3, 4),
        "empty": GroundedJob("ANALYZE", "SUPPLIERS", 5, 5),
        "bool": GroundedJob("ANALYZE", "SUPPLIERS", True, 4),
    }
    with pytest.raises(ValueError, match="UNGROUNDED_CREDENTIAL_INTERVAL"):
        from_grounded_job(RAW, jobs[shape], capability=SUPPLIERS)


def test_credential_is_frozen_and_hashable():
    one = credential()
    with pytest.raises(FrozenInstanceError):
        one.aspect = "CONTRACTS"
    assert len({one, credential()}) == 1


@given(st.text(min_size=1), st.integers(min_value=0, max_value=40))
def test_interval_inside_the_same_raw_is_always_grounded(raw, offset):
    end = min(offset + 1, len(raw))
    job = GroundedJob("ANALYZE", "SUPPLIERS", end - 1, end)
    assert from_grounded_job(raw, job, capability=SUPPLIERS).covers(raw)


@given(st.text(min_size=1), st.integers(min_value=1, max_value=10))
def test_interval_beyond_the_raw_is_always_refused(raw, extra):
    job = GroundedJob("ANALYZE", "SUPPLIERS", 0, len(raw) + extra)
    with pytest.raises(ValueError, match="UNGROUNDED_CREDENTIAL_INTERVAL"):
        from_grounded_job(raw, job, capability=SUPPLIERS)


def _spec_fields(**overrides):
    fields = dict(
        capability="x.y", owner=Owner.ENTITY_PROFILE, focus="f", action="ANALYZE",
        aspect="SUPPLIERS", universe="contracts",
        required=frozenset({Dimension.BUYER}), optional=frozenset({Dimension.TIME}),
        supported_cardinality=frozenset({Cardinality.NONE}),
        unsupported=frozenset({Dimension.TOPIC}),
    )
    fields.update(overrides)
    return fields


@pytest.mark.parametrize("overrides", [
    {"required": frozenset({Dimension.BUYER}), "optional": frozenset({Dimension.BUYER})},
    {"unsupported": frozenset({Dimension.BUYER})},
    {"unsupported": frozenset({Dimension.TIME})},
    {"supported_cardinality": frozenset()},
    {"focus": ""}, {"universe": " "}, {"capability": None},
    {"owner": "ENTITY_PROFILE"},
    {"required": frozenset({"BUYER"})},
    {"supported_cardinality": frozenset({5})},
])
def test_spec_refuses_incoherent_declarations(overrides):
    with pytest.raises(ValueError):
        CapabilitySpec(**_spec_fields(**overrides))
