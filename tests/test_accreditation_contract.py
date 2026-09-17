"""Accreditation contracts: credential identity, declarative registry, neutral path.

R2 binds the credential to the exact raw text (stable sha256 fingerprint). R3
makes the capability spec total over ``Dimension``: every dimension belongs to
exactly one of required / optional / guarded / unsupported. Nothing here executes
a capability: the no-credential path keeps every decision where it is today.
"""
import hashlib
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
    raw_fingerprint,
    spec_for_credential,
)
from app.core.capability_binding.registry import SUPPLIERS as A2_SUPPLIERS
from app.core.entity_profile.aspects.suppliers import SUPPLIER_LIMIT
from app.core.natural_alpha.models import GroundedJob

RAW = "con quien ha contratado la alcaldia de galapa en 2025"


def credential(**overrides):
    fields = dict(capability=SUPPLIERS, action="ANALYZE", aspect="SUPPLIERS",
                  evidence_interval=(0, 10), raw_fingerprint=raw_fingerprint(RAW))
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
    assert spec.guarded == frozenset({
        Dimension.ACTOR, Dimension.SOURCE, Dimension.POLARITY, Dimension.CARDINALITY,
    })
    assert spec.supported_cardinality == frozenset({Cardinality.NONE, Cardinality.EXACT_5})
    assert spec.unsupported == frozenset({
        Dimension.MONEY, Dimension.MODALITY, Dimension.TOPIC, Dimension.GEOGRAPHY,
    })
    assert declared_capabilities() == (SUPPLIERS,)
    # Same capability id as the A2 binding: if the two registries ever disagree,
    # one of them is lying about the same capability.
    assert SUPPLIERS == A2_SUPPLIERS


def test_every_dimension_belongs_to_exactly_one_category():
    spec = capability_spec(SUPPLIERS)
    assert spec is not None
    categories = (spec.required, spec.optional, spec.guarded, spec.unsupported)
    assert frozenset().union(*categories) == frozenset(Dimension)
    assert sum(len(category) for category in categories) == len(Dimension)


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
    {"raw_fingerprint": ""}, {"raw_fingerprint": "0" * 63}, {"raw_fingerprint": "0" * 65},
    {"raw_fingerprint": "A" * 64}, {"raw_fingerprint": None}, {"raw_fingerprint": 64},
    {"evidence_interval": (3, 3)}, {"evidence_interval": (4, 3)},
    {"evidence_interval": (-1, 3)}, {"evidence_interval": (0,)},
    {"evidence_interval": [0, 3]}, {"evidence_interval": (True, 3)},
    {"evidence_interval": (0, True)},
])
def test_credential_refuses_malformed_values(bad):
    with pytest.raises(ValueError):
        credential(**bad)


def test_fingerprint_is_the_stable_sha256_of_the_raw():
    assert raw_fingerprint(RAW) == hashlib.sha256(RAW.encode("utf-8")).hexdigest()
    assert credential().raw_fingerprint == raw_fingerprint(RAW)
    for bad in ("", None, 7):
        with pytest.raises(ValueError, match="INVALID_RAW_TEXT"):
            raw_fingerprint(bad)


def test_credential_identity_is_the_exact_raw_text():
    start = RAW.index("alcaldia")
    job = GroundedJob("ANALYZE", "SUPPLIERS", start, start + len("alcaldia"))
    built = from_grounded_job(RAW, job, capability=SUPPLIERS)
    assert built.evidence_interval == (start, start + len("alcaldia"))
    assert built.source == "NATURAL_ALPHA"
    assert built.corresponds_to(RAW)
    # Same length, one character apart: the span still fits, the raw does not.
    same_length = RAW[:-1] + ("X" if RAW[-1] != "X" else "Y")
    assert len(same_length) == len(RAW)
    assert built.corresponds_to(same_length) is False
    # Longer while still containing the span: no longer the same raw.
    assert built.corresponds_to(RAW + " y algo mas") is False
    assert built.corresponds_to(RAW + " ") is False
    # A stale credential from another turn never matches.
    assert built.corresponds_to("muestrame los contratos de galapa") is False
    assert built.corresponds_to("") is False
    assert built.corresponds_to(None) is False


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
    built = from_grounded_job(raw, job, capability=SUPPLIERS)
    assert built.corresponds_to(raw)
    assert not built.corresponds_to(raw + "x")


@given(st.text(min_size=1), st.integers(min_value=1, max_value=10))
def test_interval_beyond_the_raw_is_always_refused(raw, extra):
    job = GroundedJob("ANALYZE", "SUPPLIERS", 0, len(raw) + extra)
    with pytest.raises(ValueError, match="UNGROUNDED_CREDENTIAL_INTERVAL"):
        from_grounded_job(raw, job, capability=SUPPLIERS)


def _spec_fields(**overrides):
    fields = dict(
        capability="x.y", owner=Owner.ENTITY_PROFILE, focus="f", action="ANALYZE",
        aspect="SUPPLIERS", universe="contracts",
        required=frozenset({Dimension.BUYER}),
        optional=frozenset({Dimension.TIME}),
        guarded=frozenset({Dimension.ACTOR, Dimension.SOURCE, Dimension.POLARITY}),
        supported_cardinality=frozenset({Cardinality.NONE}),
        unsupported=frozenset({Dimension.RANKING, Dimension.CARDINALITY, Dimension.MONEY,
                               Dimension.MODALITY, Dimension.GEOGRAPHY, Dimension.TOPIC}),
    )
    fields.update(overrides)
    return fields


@pytest.mark.parametrize("overrides", [
    {"required": frozenset({Dimension.BUYER}), "optional": frozenset({Dimension.BUYER})},
    {"guarded": frozenset({Dimension.BUYER, Dimension.ACTOR, Dimension.SOURCE,
                           Dimension.POLARITY})},
    {"unsupported": frozenset({Dimension.POLARITY})},
    {"guarded": "ACTOR"},
    {"guarded": frozenset({"ACTOR"})},
    {"guarded": frozenset({Dimension.ACTOR, Dimension.SOURCE})},  # POLARITY vanishes
    {"supported_cardinality": frozenset()},
    {"focus": ""}, {"universe": " "}, {"capability": None},
    {"owner": "ENTITY_PROFILE"},
    {"required": frozenset({"BUYER"})},
    {"supported_cardinality": frozenset({5})},
])
def test_spec_refuses_incoherent_declarations(overrides):
    with pytest.raises(ValueError):
        CapabilitySpec(**_spec_fields(**overrides))


def test_incomplete_spec_is_refused_as_not_total():
    with pytest.raises(ValueError, match="SPEC_NOT_TOTAL"):
        CapabilitySpec(**_spec_fields(guarded=frozenset({Dimension.ACTOR, Dimension.SOURCE})))


def test_overlapping_spec_categories_are_refused():
    with pytest.raises(ValueError, match="SPEC_DIMENSION_OVERLAP"):
        CapabilitySpec(**_spec_fields(guarded=frozenset({
            Dimension.BUYER, Dimension.ACTOR, Dimension.SOURCE, Dimension.POLARITY})))


def test_a_category_may_be_empty_as_long_as_the_spec_stays_total():
    spec = CapabilitySpec(**_spec_fields(
        optional=frozenset(),
        guarded=frozenset({Dimension.ACTOR, Dimension.SOURCE, Dimension.POLARITY,
                           Dimension.TIME})))
    assert spec.optional == frozenset()
    assert spec.guarded == frozenset({Dimension.ACTOR, Dimension.SOURCE,
                                      Dimension.POLARITY, Dimension.TIME})
