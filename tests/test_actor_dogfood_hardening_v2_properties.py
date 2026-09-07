"""Combinatorial invariants behind the dogfood hardening fixes.

The example tests freeze the surfaces that were observed. These freeze the
*rules* those surfaces are instances of, so a future change cannot satisfy the
examples while breaking the property they stand for.
"""
from hypothesis import given, settings, strategies as st

from app.core.actor_arbitration import scrub_strong_actor_leaks
from app.core.actor_identity import IdentityKey
from app.core.actor_identity_coherence import actor_identity_split_brain
from app.core.actor_name import normalize_actor_name
from app.core.actor_resolution import (
    METHOD_EXPLICIT_DOCUMENT,
    METHOD_USER_SELECTED_CANDIDATE,
    apply_actor_identity,
    document_variants,
)
from app.core.asserted_actor_name import detect_bare_asserted_actor_name
from app.core.entity_types import (
    name_tokens,
    strip_conversational_terminal_punctuation,
)
from app.data.colombia_geography import REGION_ALIASES, ZONES_BY_NAME


RULE = settings(max_examples=60, derandomize=True, deadline=None)

NAME_WORD = (
    st.text(alphabet="abcdefghijlmnopqrstuvxyzáéíóúñ", min_size=2, max_size=9)
    .map(normalize_actor_name)
    .filter(lambda word: bool(word) and word.isalpha())
)
DOCUMENT = st.text(alphabet="123456789", min_size=6, max_size=10)
REGION_ALIAS = st.sampled_from(sorted(REGION_ALIASES))
PUNCTUATION = st.text(alphabet="?!¿¡,;:.", min_size=1, max_size=4)


# ── Span ownership ────────────────────────────────────────────────────────

@RULE
@given(REGION_ALIAS, st.lists(NAME_WORD, min_size=1, max_size=3))
def test_region_alias_inside_the_span_never_survives_the_scrub(alias, extra):
    """A filter whose evidence lies inside the accredited span is span material."""
    zone = REGION_ALIASES[alias]
    actor_name = normalize_actor_name(" ".join([*extra, alias]))
    params = {
        "region": zone,
        "departamentos_in": list(ZONES_BY_NAME.get(zone, [])),
        "region_resolution": {
            "value": zone, "method": "scan_exact",
            "kind": "territorial_region", "matched_alias": alias,
        },
    }
    clean = scrub_strong_actor_leaks(params, actor_name)
    assert "region" not in clean
    assert "departamentos_in" not in clean
    assert "region_resolution" not in clean


@RULE
@given(REGION_ALIAS, st.lists(NAME_WORD, min_size=1, max_size=4))
def test_region_named_outside_the_span_always_survives(alias, words):
    """Geography the actor name does not contain is an independent filter."""
    zone = REGION_ALIASES[alias]
    actor_name = normalize_actor_name(" ".join(words))
    alias_tokens = set(alias.split())
    if alias_tokens & set(actor_name.split()) or not actor_name:
        return
    params = {
        "region": zone,
        "departamentos_in": list(ZONES_BY_NAME.get(zone, [])),
        "region_resolution": {
            "value": zone, "method": "scan_exact",
            "kind": "territorial_region", "matched_alias": alias,
        },
    }
    clean = scrub_strong_actor_leaks(params, actor_name)
    assert clean.get("region") == zone
    assert clean.get("departamentos_in") == params["departamentos_in"]


# ── Single identity ───────────────────────────────────────────────────────

@RULE
@given(
    st.sampled_from(["CC", "NIT"]),
    DOCUMENT,
    st.sampled_from([METHOD_EXPLICIT_DOCUMENT, METHOD_USER_SELECTED_CANDIDATE]),
)
def test_a_projected_identity_is_never_read_as_a_split_brain(kind, number, method):
    """The guard cannot fire on a frame the projection itself produced."""
    identity = IdentityKey(document_type=kind, base_number=number)
    frame = apply_actor_identity({}, identity, method=method)
    assert actor_identity_split_brain(frame) is False


@RULE
@given(st.sampled_from(["CC", "NIT"]), DOCUMENT, DOCUMENT)
def test_a_foreign_document_in_contratista_is_always_caught(kind, scope_number, stray):
    identity = IdentityKey(document_type=kind, base_number=scope_number)
    if stray in set(document_variants(identity)):
        return
    frame = apply_actor_identity({}, identity, method=METHOD_EXPLICIT_DOCUMENT)
    frame["contratista"] = [*frame["contratista"], stray]
    assert actor_identity_split_brain(frame) is True


@RULE
@given(st.sampled_from(["CC", "NIT"]), DOCUMENT)
def test_a_narrower_projection_is_not_a_divergence(kind, number):
    """The AP2 door emits only the forms the user declared; that is not a split."""
    identity = IdentityKey(document_type=kind, base_number=number)
    frame = apply_actor_identity({}, identity, method=METHOD_EXPLICIT_DOCUMENT)
    frame["contratista"] = [number]
    assert actor_identity_split_brain(frame) is False


# ── Asserted name beside a document ───────────────────────────────────────

@RULE
@given(st.lists(NAME_WORD, min_size=1, max_size=6), DOCUMENT)
def test_an_asserted_name_span_never_carries_digits(words, number):
    query = f"busca {' '.join(words)} cc {number}"
    reference = detect_bare_asserted_actor_name(query, query.index(number))
    if reference is None:
        return
    for span in reference.consumed_spans:
        assert not any(char.isdigit() for char in span)
        assert span in query


@RULE
@given(st.lists(NAME_WORD, min_size=1, max_size=4), st.integers(1, 9999), DOCUMENT)
def test_a_number_in_the_prefix_always_closes_the_name_door(words, amount, number):
    """An organization head noun passes the shape door; a digit must still stop it."""
    query = f"fundacion {' '.join(words)} {amount} millones con nit {number}"
    assert detect_bare_asserted_actor_name(query, query.rindex(number)) is None


@RULE
@given(st.lists(NAME_WORD, min_size=1, max_size=6), DOCUMENT)
def test_an_untyped_number_never_opens_the_name_door(words, number):
    """Without a declared document type there is no document, hence no context."""
    query = f"busca {' '.join(words)} {number}"
    assert detect_bare_asserted_actor_name(query, query.index(number)) is None


# ── Buyer punctuation ─────────────────────────────────────────────────────

@RULE
@given(st.lists(NAME_WORD, min_size=1, max_size=4), PUNCTUATION)
def test_terminal_punctuation_never_changes_the_name_tokens(words, marks):
    name = " ".join(words)
    assert name_tokens(strip_conversational_terminal_punctuation(name + marks)) == \
        name_tokens(name)


@RULE
@given(st.lists(NAME_WORD, min_size=1, max_size=4), PUNCTUATION)
def test_stripping_terminal_punctuation_is_idempotent(words, marks):
    once = strip_conversational_terminal_punctuation(" ".join(words) + marks)
    assert strip_conversational_terminal_punctuation(once) == once


@RULE
@given(st.lists(st.sampled_from("abcdefgilmnprstuv"), min_size=2, max_size=5),
       st.text(alphabet="?!¿¡,;:", max_size=3))
def test_a_dotted_acronym_keeps_its_closing_period(letters, trailing):
    """E.S.E. / S.A.S. — the period after a lone letter is name material."""
    acronym = "".join(f"{letter}." for letter in letters)
    assert strip_conversational_terminal_punctuation(
        f"hospital {acronym}{trailing}"
    ) == f"hospital {acronym}"
