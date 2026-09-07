"""Invariants behind the V2.1 remediation.

The V2 properties passed while three P1 defects were live, because each
property tested the representation the fix produced rather than the contract
it promised. These test the contract.
"""
from hypothesis import assume, given, settings, strategies as st

from app.core.actor_identity import IdentityKey
from app.core.actor_identity_coherence import actor_identity_split_brain
from app.core.actor_resolution import (
    METHOD_EXPLICIT_DOCUMENT,
    apply_actor_identity,
    document_variants,
)


RULE = settings(max_examples=80, derandomize=True, deadline=None)
DOCUMENT = st.text(alphabet="123456789", min_size=6, max_size=10)
KIND = st.sampled_from(["CC", "NIT"])


def _framed(kind, number):
    return apply_actor_identity(
        {"dataset": "contratos"},
        IdentityKey(document_type=kind, base_number=number),
        method=METHOD_EXPLICIT_DOCUMENT,
    )


# ── The guard cannot depend on how contratista was spelled ────────────────

@RULE
@given(KIND, DOCUMENT, DOCUMENT)
def test_the_verdict_is_the_same_for_every_container(kind, number, stray):
    """str, list, tuple and set of the same documents get the same answer."""
    identity = IdentityKey(document_type=kind, base_number=number)
    accepted = document_variants(identity)
    assume(stray not in set(accepted))
    frame = _framed(kind, number)

    for value in accepted:
        verdicts = {
            actor_identity_split_brain({**frame, "contratista": shape})
            for shape in (value, [value], (value,), {value})
        }
        assert verdicts == {False}, (value, verdicts)

    verdicts = {
        actor_identity_split_brain({**frame, "contratista": shape})
        for shape in (stray, [stray], (stray,), {stray}, [*accepted, stray])
    }
    assert verdicts == {True}, verdicts


@RULE
@given(KIND, DOCUMENT)
def test_a_string_is_one_document_not_a_sequence_of_characters(kind, number):
    """Iterating a str would compare single digits and never block anything."""
    frame = _framed(kind, number)
    frame["contratista"] = document_variants(IdentityKey(kind, number))[0]
    assert not actor_identity_split_brain(frame)
    frame["contratista"] = frame["contratista"][:-1] + "0" * 3
    assert actor_identity_split_brain(frame)


@RULE
@given(KIND, DOCUMENT, st.text(alphabet="abcdefg ", min_size=3, max_size=12))
def test_a_textual_contratista_beside_a_typed_scope_always_blocks(kind, number, name):
    """No current path writes this; a name LIKE beside a typed identity is the
    actor-merging filter INV-NAME-NO-GUESS forbids, so it fails closed."""
    assume(name.strip())
    frame = _framed(kind, number)
    frame["contratista"] = name
    assert actor_identity_split_brain(frame)


@RULE
@given(KIND, DOCUMENT)
def test_an_absent_or_empty_projection_is_never_a_split_brain(kind, number):
    frame = _framed(kind, number)
    for empty in (None, "", "   ", [], (), set()):
        assert not actor_identity_split_brain({**frame, "contratista": empty})


# ── Ownership belongs to the occurrence, not to the word ──────────────────

from app.core.actor_arbitration import scrub_strong_actor_leaks  # noqa: E402
from app.core.actor_name import normalize_actor_name  # noqa: E402
from app.data.colombia_geography import REGION_ALIASES, ZONES_BY_NAME  # noqa: E402

NAME_WORD = (
    st.text(alphabet="abcdefghijlmnopqrstuvxyz", min_size=3, max_size=8)
    .map(normalize_actor_name)
    .filter(lambda word: bool(word) and word.isalpha())
)
REGION_ALIAS = st.sampled_from(sorted(REGION_ALIASES))


def _region_frame(alias):
    zone = REGION_ALIASES[alias]
    return {
        "region": zone,
        "departamentos_in": list(ZONES_BY_NAME.get(zone, [])),
        "region_resolution": {
            "value": zone, "method": "scan_exact",
            "kind": "territorial_region", "matched_alias": alias,
        },
    }


@RULE
@given(REGION_ALIAS, st.lists(NAME_WORD, min_size=1, max_size=3))
def test_an_alias_used_only_inside_the_span_is_span_material(alias, extra):
    actor = normalize_actor_name(" ".join([*extra, alias]))
    query = f"contratos del proveedor {actor}"
    clean = scrub_strong_actor_leaks(_region_frame(alias), actor, raw_query=query)
    assert "region" not in clean
    assert "departamentos_in" not in clean
    assert "region_resolution" not in clean


@RULE
@given(REGION_ALIAS, st.lists(NAME_WORD, min_size=1, max_size=3))
def test_the_same_alias_used_twice_keeps_its_outside_occurrence(alias, extra):
    """The surname is one occurrence; the territory asked for is another."""
    actor = normalize_actor_name(" ".join([*extra, alias]))
    query = f"contratos del proveedor {actor} en {alias}"
    clean = scrub_strong_actor_leaks(_region_frame(alias), actor, raw_query=query)
    assert clean.get("region") == REGION_ALIASES[alias]
    assert clean.get("departamentos_in")
    assert clean.get("region_resolution")


@RULE
@given(REGION_ALIAS, st.lists(NAME_WORD, min_size=1, max_size=4))
def test_an_alias_absent_from_the_span_always_survives(alias, words):
    actor = normalize_actor_name(" ".join(words))
    assume(actor and not set(alias.split()) & set(actor.split()))
    query = f"contratos del proveedor {actor} en {alias}"
    clean = scrub_strong_actor_leaks(_region_frame(alias), actor, raw_query=query)
    assert clean.get("region") == REGION_ALIASES[alias]


# ── The asserted-name door never spends a span another authority owns ─────

from app.core.asserted_actor_name import detect_bare_asserted_actor_name  # noqa: E402
from app.data.morphological_variants import MORPHOLOGICAL_VARIANTS  # noqa: E402

from app.core.actor_arbitration import has_strong_topic_evidence  # noqa: E402

# Only terms the topic authority itself recognizes inside a phrase. Hypothesis
# found the boundary: that authority indexes phrase tokens of four characters
# or more, so a three-letter term like "sat" is not recognized as topic
# evidence when it appears beside other words. That is the topic vocabulary's
# own policy, not this door's — asserting otherwise here would test the wrong
# module. Recorded so the limit is visible rather than silently excluded.
TOPIC_TERM = st.sampled_from(sorted(
    term for term in {
        normalize_actor_name(v) for v in MORPHOLOGICAL_VARIANTS if normalize_actor_name(v)
    }
    if has_strong_topic_evidence(f"{term} referencia xyz", {})
))
ACRONYM = st.sampled_from(["xyz", "lmp", "zxc", "brt", "kgb"])


@RULE
@given(TOPIC_TERM, ACRONYM, DOCUMENT)
def test_a_topic_is_never_consumed_however_name_like_the_rest_looks(topic, acronym, number):
    """The acronym branch of the shape evidence must not outrank the topic."""
    query = f"busca {topic} referencia {acronym} cc {number}"
    assert detect_bare_asserted_actor_name(query, query.rindex(number)) is None


@RULE
@given(TOPIC_TERM, DOCUMENT)
def test_a_bare_topic_beside_a_document_is_never_a_name(topic, number):
    query = f"busca {topic} cc {number}"
    assert detect_bare_asserted_actor_name(query, query.rindex(number)) is None


@RULE
@given(st.lists(NAME_WORD, min_size=2, max_size=4), DOCUMENT)
def test_a_buyer_claim_always_vetoes_the_name_door(words, number):
    """Whatever the caller's buyer authority claims is not this door's to spend."""
    query = f"busca {' '.join(words)} cc {number}"
    start = query.rindex(number)
    assert detect_bare_asserted_actor_name(
        query, start, claims_buyer=lambda span: True,
    ) is None
    without = detect_bare_asserted_actor_name(query, start, claims_buyer=lambda span: False)
    with_none = detect_bare_asserted_actor_name(query, start)
    assert (without is None) == (with_none is None)
