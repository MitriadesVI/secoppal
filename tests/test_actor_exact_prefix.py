"""Exact structural CC ambiguity: synthetic, offline, red-first authority gates."""
import socket
import sqlite3
from dataclasses import replace

import pytest

from app.core.actor_identity import IdentityKey
from app.core.actor_name import ActorResolutionResult, normalize_actor_name
from tests.actor_prefix_fixtures import CHALLENGERS, EXACT, NAME, resolver_for


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("network forbidden in deterministic prefix tests")
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)


def test_short_exact_requires_selection_with_exact_first_and_parity(tmp_path, monkeypatch):
    resolver = resolver_for(tmp_path / "actors.sqlite", [EXACT, *CHALLENGERS])
    calls = []
    monkeypatch.setattr(resolver, "_fuzzy_candidates", lambda *args: calls.append(args))
    result = resolver.resolve_exact(NAME)
    assert result.status == "AMBIGUOUS"
    assert result.method == "exact_prefix_ambiguous"
    assert result.identity is None
    assert [c.matched_alias for c in result.candidates] == [NAME, NAME + " diaz", NAME + " perez"]
    assert [c.rank for c in result.candidates] == [1, 2, 3]
    assert result.candidates[0].observed_name == NAME.upper()
    assert result.candidates_has_more is False
    assert result.enumeration_incomplete is False
    assert resolver.resolve(NAME) == result
    assert calls == []


@pytest.mark.parametrize("rows,method", [
    ([EXACT, ("CC", EXACT[1], NAME + " perez")], "exact_unique_alias"),
    ([EXACT, ("CC", "70000002", NAME)], "exact_ambiguous_alias"),
    ([EXACT, ("NIT", "900111222", NAME)], "exact_ambiguous_alias"),
    ([("NIT", "900111222", NAME), ("NIT", "900222333", NAME + " perez")], "exact_unique_alias"),
    ([EXACT, ("NIT", "900111222", NAME + " perez")], "exact_unique_alias"),
    ([EXACT, ("CC", "70000002", "ana maria lopes perez")], "exact_unique_alias"),
    ([EXACT, ("CC", "70000002", NAME + "ana")], "exact_unique_alias"),
])
def test_authorities_unchanged_outside_other_cc_exact_extension(tmp_path, rows, method):
    result = resolver_for(tmp_path / "actors.sqlite", rows).resolve(NAME)
    assert result.method == method


def test_no_alias_cap_before_identity_exclusion_or_dedup(tmp_path):
    rows = [EXACT]
    rows += [("CC", EXACT[1], NAME + f" a {n:04d}") for n in range(130)]
    rows += [("CC", "70000002", NAME + f" b {n:04d}") for n in range(130)]
    rows += [("CC", "70000003", NAME + " z ultimo")]
    result = resolver_for(tmp_path / "actors.sqlite", rows).resolve(NAME)
    assert result.status == "AMBIGUOUS"
    assert [c.identity.base_number for c in result.candidates] == ["70000001", "70000002", "70000003"]
    assert result.candidates[1].matched_alias == NAME + " b 0000"
    assert result.candidates_has_more is False


@pytest.mark.parametrize("count", [1, 9, 10, 14])
def test_cap_applies_to_identities_and_proves_extra_identity(tmp_path, count):
    rows = [EXACT] + [("CC", str(71000000 + n), NAME + f" extension {n:02d}") for n in range(count)]
    result = resolver_for(tmp_path / "actors.sqlite", rows).resolve(NAME)
    assert result.status == "AMBIGUOUS"
    assert len(result.candidates) == 1 + min(count, 9)
    assert len({c.identity for c in result.candidates}) == len(result.candidates)
    assert result.candidates_has_more is (count >= 10)
    assert result.enumeration_incomplete is False


@pytest.mark.parametrize("phase", ["existence", "enumeration"])
@pytest.mark.parametrize("failure", ["budget", "sqlite", "invalid_evidence"])
def test_incomplete_checks_preserve_only_accredited_authority(tmp_path, monkeypatch, phase, failure):
    from app.data import actor_exact_prefix as prefix
    resolver = resolver_for(tmp_path / "actors.sqlite", [EXACT, *CHALLENGERS])
    if failure == "budget":
        monkeypatch.setattr(prefix, phase.upper() + "_BUDGET_MS", 0)
    else:
        target = "find_prefix_challenger" if phase == "existence" else "enumerate_prefix_challengers"
        error = sqlite3.OperationalError("synthetic read failure") if failure == "sqlite" else ValueError("invalid row")
        def fail(*args, **kwargs):
            raise error
        monkeypatch.setattr(prefix, target, fail)
    calls = []
    monkeypatch.setattr(resolver, "_fuzzy_candidates", lambda *args: calls.append(args))
    result = resolver.resolve(NAME)
    assert resolver.resolve_exact(NAME) == result
    if phase == "existence":
        assert result.status == "INCONCLUSIVE"
        assert result.method == "exact_prefix_check_inconclusive"
        assert result.reason
        assert result.identity is None and result.candidates == ()
    else:
        assert result.status == "AMBIGUOUS"
        assert result.method == "exact_prefix_ambiguous"
        assert [c.matched_alias for c in result.candidates] == [NAME, NAME + " diaz"]
        assert result.candidates_has_more is None
        assert result.enumeration_incomplete is True
    assert calls == []


def test_inconclusive_model_invariants():
    valid = ActorResolutionResult("INCONCLUSIVE", NAME, method="exact_prefix_check_inconclusive", reason="budget")
    for updates in ({"identity": IdentityKey("CC", EXACT[1])}, {"reason": None}, {"method": "no_match"}):
        with pytest.raises(ValueError):
            replace(valid, **updates)


def test_prefix_result_metadata_invariants(tmp_path):
    result = resolver_for(tmp_path / "actors.sqlite", [EXACT, *CHALLENGERS]).resolve(NAME)
    with pytest.raises(ValueError):
        replace(result, enumeration_incomplete=True)
    with pytest.raises(ValueError):
        replace(result, candidates_has_more=None)
    with pytest.raises(ValueError):
        replace(result, candidates=result.candidates[:1])
    with pytest.raises(ValueError):
        replace(result, candidates=tuple(reversed(result.candidates)))
    with pytest.raises(ValueError):
        ActorResolutionResult("INCONCLUSIVE", NAME, candidates=result.candidates,
                              method="exact_prefix_check_inconclusive", reason="budget")


@pytest.mark.parametrize("query", ["ana", "ana maria", NAME, NAME + " perez"])
def test_all_token_lengths_covered(tmp_path, query):
    result = resolver_for(tmp_path / "actors.sqlite", [("CC", "70000001", query), ("CC", "70000002", query + " extra")]).resolve(query)
    assert result.status == "AMBIGUOUS"


@pytest.mark.parametrize("query", ["ÁNA María—LÓPEZ", " ANA  MARIA\tLOPEZ ", "ＡＮＡ ＭＡＲＩＡ ＬＯＰＥＺ", "Ana\u0301 Mari\u0301a Lo\u0301pez"])
def test_existing_n2_equivalences(tmp_path, query):
    assert normalize_actor_name(query) == NAME
    assert resolver_for(tmp_path / "actors.sqlite", [EXACT, *CHALLENGERS]).resolve(query).status == "AMBIGUOUS"


def test_sql_plan_uses_actual_binary_nominal_range(tmp_path):
    from app.data.actor_exact_prefix import ENUMERATION_SQL, EXISTENCE_SQL, prefix_bounds
    resolver = resolver_for(tmp_path / "actors.sqlite", [EXACT, *CHALLENGERS])
    with resolver._connect_readonly() as connection:
        assert connection.execute("PRAGMA encoding").fetchone()[0] == "UTF-8"
        index = connection.execute("PRAGMA index_xinfo(sqlite_autoindex_actor_aliases_1)").fetchall()
        assert [r[4] for r in index if r[5]] == ["BINARY", "BINARY", "BINARY"]
        for sql in (EXISTENCE_SQL, ENUMERATION_SQL):
            plan = " ".join(r[3] for r in connection.execute("EXPLAIN QUERY PLAN " + sql, (*prefix_bounds(NAME), EXACT[1])))
            assert "SEARCH actor_aliases" in plan
            assert "normalized_alias>? AND normalized_alias<?" in plan
            assert "SCAN" not in plan  # any partial ORDER BY sort is included in benchmark


def test_successor_and_sql_boundary_unicode(tmp_path):
    from app.data.actor_exact_prefix import lexicographic_successor, prefix_bounds
    assert lexicographic_successor("a\U0010ffff") == "b"
    assert lexicographic_successor("\ud7ff") == "\ue000"
    for value in ("", "\U0010ffff"):
        with pytest.raises(ValueError):
            lexicographic_successor(value)
    for query in ("a", "李 明", "𐐨", "é á", "１２３"):
        q = normalize_actor_name(query)
        values = [q, q + " a", q + " 李", q + "a", q + " z z"]
        low, high = prefix_bounds(q)
        with sqlite3.connect(":memory:") as connection:
            selected = [v for v in values if connection.execute("SELECT ? >= ? AND ? < ?", (v, low, v, high)).fetchone()[0]]
        assert selected == [v for v in values if v.startswith(q + " ")]
