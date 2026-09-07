"""Bounded two-token candidate ranking — candidates only, never authority.

Frozen after the audit in ``docs/audits/ACTOR_DOGFOOD_HARDENING_V2.md``. The
policy widens *ordering*, never retrieval and never authority: a fuzzy result
is still ``CANDIDATES``, and only an exact alias or an explicit user selection
can identify anybody.
"""
import socket

import pytest

from app.core.actor_name import ActorResolutionStatus
from app.data.actor_alias_resolver import _anchored_name_distance
from tests.actor_prefix_fixtures import resolver_for


TARGET = ("CC", "70000101", "marcela quintero valbuena")
NEIGHBOURS = [
    ("CC", "70000102", "marcela rodriguez perez"),
    ("CC", "70000103", "andrea quintero solano"),
    ("CC", "70000104", "camila quintero mesa"),
    ("CC", "70000105", "marcela quintero mesa"),
]


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("external I/O forbidden")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)


@pytest.fixture
def resolver(tmp_path):
    return resolver_for(tmp_path / "actors.sqlite", [TARGET, *NEIGHBOURS])


def _ranks(result):
    return {candidate.identity.base_number: candidate.rank for candidate in result.candidates}


# ── The pure ordering function ────────────────────────────────────────────

ALIAS = "marcela quintero valbuena"


@pytest.mark.parametrize("query,expected", [
    ("marcela quintero valbuena", 0),      # exact coverage
    ("marcela quintero valbuenz", 1),      # one imperfect token
    ("marcelz quintero valbuenz", 2),      # two imperfect tokens, one edit each
    ("marcelzz quintero valbuenz", 3),     # two edits in one of the two
    ("marcelz quinterz valbuenz", 3),      # three imperfect tokens is not a typo
    ("marcelz valbuenz", 3),               # two tokens cannot keep an anchor
    ("marcelz quinterz", 3),               # no literal anchor left
    ("marcela quintero zzzzzzzzz", 3),     # unrelated token
])
def test_bounded_distance_admits_only_two_small_typos(query, expected):
    tokens = tuple(token for token in query.split() if len(token) >= 3)
    assert _anchored_name_distance(tokens, ALIAS) == expected


def test_one_alias_word_cannot_absorb_both_typos():
    """Otherwise a single word manufactures coverage for two query tokens."""
    assert _anchored_name_distance(("marcelz", "marcelb", "quintero"), ALIAS) == 3


def test_short_tokens_are_never_matched_approximately():
    assert _anchored_name_distance(("juan", "quintero", "lopz"), ALIAS) == 3


# ── End to end: ordering changes, authority does not ──────────────────────

def test_two_token_typo_surfaces_the_target_as_a_candidate(resolver):
    result = resolver.resolve("marcelz quintero valbuenz")
    assert result.status == ActorResolutionStatus.CANDIDATES
    assert _ranks(result).get(TARGET[1]) == 1


def test_a_fuzzy_hit_is_never_authoritative(resolver):
    result = resolver.resolve("marcelz quintero valbuenz")
    assert result.status not in {
        ActorResolutionStatus.RESOLVED, ActorResolutionStatus.AMBIGUOUS,
    }
    assert result.identity is None


def test_exact_authority_is_untouched_by_the_ranking_change(resolver):
    result = resolver.resolve(TARGET[2])
    assert result.status == ActorResolutionStatus.RESOLVED
    assert result.identity is not None
    assert result.identity.base_number == TARGET[1]


def test_one_typo_ranking_is_unchanged(resolver):
    result = resolver.resolve("marcela quintero valbuenz")
    assert result.status == ActorResolutionStatus.CANDIDATES
    assert _ranks(result).get(TARGET[1]) == 1


def test_exact_probe_never_opens_fuzzy_retrieval(resolver):
    result = resolver.resolve_exact("marcelz quintero valbuenz")
    assert result.status == ActorResolutionStatus.UNRESOLVED
    assert not result.candidates
