"""Real combinatorial authority invariants with deterministic offline examples."""
import socket
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest.mock import patch

from hypothesis import given, settings, strategies as st

from app.core.actor_name import normalize_actor_name
from tests.actor_prefix_fixtures import EXACT, NAME, resolver_for


RULE = settings(max_examples=35, derandomize=True, deadline=None)
WORDS = st.text(alphabet="abcxyz012éñ李", min_size=1, max_size=10).map(normalize_actor_name).filter(bool)


def _resolve(rows, query=NAME):
    with TemporaryDirectory() as directory, patch.object(socket.socket, "connect", side_effect=AssertionError("network")), patch("app.data.actor_exact_prefix.perf_counter", return_value=0.0):
        return resolver_for(Path(directory) / "actors.sqlite", rows).resolve(query)


@RULE
@given(st.lists(WORDS, min_size=1, max_size=15, unique=True))
def test_same_identity_extensions_never_create_ambiguity(suffixes):
    result = _resolve([EXACT] + [("CC", EXACT[1], NAME + " " + s) for s in suffixes])
    assert result.status == "RESOLVED"


@RULE
@given(WORDS, st.booleans())
def test_other_cc_exact_extension_never_resolves_even_if_enumeration_fails(suffix, fail):
    from app.data import actor_exact_prefix as prefix
    with patch.object(prefix, "ENUMERATION_BUDGET_MS", 0 if fail else prefix.ENUMERATION_BUDGET_MS):
        result = _resolve([EXACT, ("CC", "70000002", NAME + " " + suffix)])
    assert result.status == "AMBIGUOUS"
    assert result.candidates_has_more is (None if fail else False)
    assert result.enumeration_incomplete is fail


@RULE
@given(WORDS)
def test_fuzzy_only_aliases_do_not_change_exact_authority(suffix):
    result = _resolve([EXACT, ("CC", "70000002", "ana maria lopes " + suffix)])
    assert result.status == "RESOLVED"


@RULE
@given(st.permutations(range(8)))
def test_insertion_permutations_preserve_representative_alias_and_candidate_order(order):
    rows = [EXACT] + [("CC", str(70000002 + n % 3), NAME + f" extension {n}") for n in range(7)]
    baseline = _resolve(rows)
    assert _resolve([rows[n] for n in order]) == baseline


@RULE
@given(st.sampled_from(["Ána", "ANA", "ＡＮＡ", "a\u0301na"]), st.sampled_from([" ", "-", "\t", "…", "\u00a0"]), st.sampled_from(["López", "LOPEZ", "lo\u0301pez"]))
def test_n2_equivalences_preserve_policy(first, separator, last):
    query = separator.join([first, "María", last])
    assert normalize_actor_name(query) == NAME
    assert _resolve([EXACT, ("CC", "70000002", NAME + " perez")], query).status == "AMBIGUOUS"


@RULE
@given(st.integers(min_value=1, max_value=24), st.booleans())
def test_has_more_requires_an_extra_distinct_identity_and_failure_does_not_prove_it(count, fail):
    from app.data import actor_exact_prefix as prefix
    rows = [EXACT] + [("CC", str(73000000 + n), NAME + f" extension {n:02d}") for n in range(count)]
    with patch.object(prefix, "ENUMERATION_BUDGET_MS", 0 if fail else prefix.ENUMERATION_BUDGET_MS):
        result = _resolve(rows)
    if result.candidates_has_more:
        assert count > len(result.candidates) - 1
    if fail:
        assert result.candidates_has_more is None


@RULE
@given(WORDS)
def test_existence_inconclusive_never_opens_fuzzy(suffix):
    from app.data import actor_exact_prefix as prefix
    with patch.object(prefix, "EXISTENCE_BUDGET_MS", 0), TemporaryDirectory() as directory:
        resolver = resolver_for(Path(directory) / "actors.sqlite", [EXACT, ("CC", "70000002", NAME + " " + suffix)])
        with patch.object(resolver, "_fuzzy_candidates", side_effect=AssertionError("fuzzy")):
            assert resolver.resolve(NAME).status == "INCONCLUSIVE"
            assert resolver.resolve_exact(NAME).status == "INCONCLUSIVE"


@RULE
@given(st.sampled_from(["explicit_document", "user_selected_candidate"]), st.integers(min_value=10000, max_value=99999999))
def test_explicit_identity_authority_never_reopens_challenger(method, number):
    from burr.core import State
    from app.core.actor_identity import IdentityKey
    from app.core.actor_resolution import apply_actor_identity
    from app.core.workflow.actions.actor_resolution import resolve_actor_name
    class Forbidden:
        def resolve(self, *args):
            raise AssertionError("prefix reopened")
        resolve_exact = resolve
    params = apply_actor_identity({}, IdentityKey("CC", str(number)), method=method)
    result = resolve_actor_name(State({"parsed_params": params, "raw_query": NAME}), Forbidden())
    assert result["parsed_params"] == params
