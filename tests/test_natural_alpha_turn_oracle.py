"""External oracle: N eligible run_query turns == N classified SQLite traces.

Expected classes are hand-written literals per scripted turn. Actual classes
are read with raw SQL from the feedback database; neither side calls the host
classifier. The same script runs with Alpha off to prove legacy parity.
"""
from collections import Counter
import logging
import sqlite3

import pytest

from app.config import get_settings
from app.core.natural_alpha.dogfood import report_dogfood
from app.core.natural_alpha.provider import ProviderResult
from app.core.radar.store import RadarStore
from tests.test_entity_profile_workflow import WorkflowClient
from tests.test_natural_alpha_contract import IDENTITY, payload
from tests.test_natural_alpha_runtime import MULTI, MULTI_RAW, RAW, make_flow, run  # noqa: F401

PROFILE_RAW = "analiza la alcaldía de Baranoa en 2026"
PARITY_FIELDS = ("response", "route_reason", "parsed_params", "resolved_params", "query_plan", "soql_query",
                 "dataset_id", "results", "total_count", "needs_clarification", "profile_action_phrase")


def _steps(flow):
    """(name, expected class, frame flag, uses profile client, action)."""
    def say(raw, chat, **kwargs):
        return lambda: run(flow, raw, chat_id=chat, **kwargs)

    def select_actor():
        return flow.select_actor_candidate(1, "stale-token", channel="streamlit", chat_id="legacy",
                                           user_identity=IDENTITY)

    def profile_action():
        return flow.select_entity_profile_action("unknown-action", {"opaque": "reference"}, channel="streamlit",
                                                 chat_id="profile-pre", user_identity=IDENTITY)

    return [
        ("single_alpha", "REACHED_ALPHA", False, False, say(RAW, "legacy")),
        ("pagination", "FAST_PATH_BYPASS", False, False, say("muestrame mas", "legacy")),
        ("radar_save", "FAST_PATH_BYPASS", False, False, say("guardar esta búsqueda como Radar Mantenimiento", "legacy")),
        ("radar_list", "FAST_PATH_BYPASS", False, False, say("mis radares", "legacy")),
        ("radar_run", "FAST_PATH_BYPASS", False, False, say("ejecutar radar Mantenimiento", "legacy")),
        ("direct_lookup", "FAST_PATH_BYPASS", False, False, say("CO1.BDOS.123456", "legacy")),
        ("actor_structured_selection", "CONTROL_BYPASS", False, False, select_actor),
        ("reset", "CONTROL_BYPASS", False, False, say("/reset", "legacy")),
        ("profile_post_seam", "REACHED_ALPHA", False, True, say(PROFILE_RAW, "profile-post")),
        ("profile_pre_seam", "PROFILE_BYPASS", True, True, say(PROFILE_RAW, "profile-pre")),
        ("profile_action", "PROFILE_BYPASS", True, True, profile_action),
        ("clarify_pre_seam", "CONTROL_BYPASS", True, False, say("muestrame mas", "clarify")),
        ("multi_alpha", "REACHED_ALPHA", False, False, say(MULTI_RAW, "multi")),
    ]


def _rows(db):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            """SELECT json_extract(trace_json, '$.natural_alpha_coverage_class'),
                      json_extract(trace_json, '$.natural_alpha_coverage_turn_id'),
                      json_extract(trace_json, '$.natural_alpha_excluded_by_route'), route_reason
               FROM traces ORDER BY rowid""").fetchall()


def _play(flow, monkeypatch):
    flow.radar_store = RadarStore(flow.feedback.db_path.parent / "radar.db")
    sql_source, profile_source = flow.secop_client, WorkflowClient()
    observe = flow.natural_alpha_provider.observe
    turns = []
    for name, expected, frame, profile, action in _steps(flow):
        monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", str(frame).lower())
        get_settings.cache_clear()
        flow.secop_client = profile_source if profile else sql_source
        observe.return_value = ProviderResult(MULTI if name == "multi_alpha" else payload(), provider_calls=1)
        rows_before, provider_before, calls_before = len(_rows(flow.feedback.db_path)), observe.call_count, len(
            flow.secop_client.calls)
        result = action()
        turns.append({
            "name": name, "expected": expected, "result": result,
            "rows": _rows(flow.feedback.db_path)[rows_before:],
            "provider_calls": observe.call_count - provider_before,
            "secop_calls": list(flow.secop_client.calls[calls_before:]),
        })
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "false")
    get_settings.cache_clear()
    return turns


def _effects(flow):
    # The "multi" chat is excluded: skipping its legacy turn is the authorized
    # Alpha effect, covered by test_natural_alpha_runtime.
    history = {chat: [(turn.user_query, turn.response, turn.parsed_params) for turn in flow.conv_store.get_history(chat)]
               for chat in ("legacy", "profile-post", "profile-pre", "clarify")}
    envelopes = {}
    for chat in ("profile-pre", "clarify"):
        read = flow.conv_store.load_envelope(chat)
        envelopes[chat] = (read.usable, read.revision, (read.envelope.continuation or {}).get("owner")
                           if read.usable else None)
    radars = [(radar.name, [run_.status for run_ in flow.radar_store.get_runs(radar.id)])
              for radar in flow.radar_store.list_radars("legacy")]
    return history, envelopes, radars


def test_every_eligible_turn_persists_exactly_one_hand_labelled_class(make_flow, monkeypatch, caplog):
    caplog.set_level(logging.ERROR)
    baseline, candidate = make_flow("off"), make_flow("alpha")
    legacy, alpha = _play(baseline, monkeypatch), _play(candidate, monkeypatch)

    expected = Counter(turn["expected"] for turn in alpha)
    actual, turn_ids = Counter(), set()
    for turn in alpha:
        classified = [row for row in turn["rows"] if row[0] is not None]
        assert len(classified) == 1, (turn["name"], turn["rows"])  # no missing, no duplicate
        classification, turn_id, excluded_route, route = classified[0]
        assert classification == turn["expected"], (turn["name"], classified[0])
        assert {row[1] for row in turn["rows"]} == {turn_id}  # secondary traces keep the same turn
        assert type(turn_id) is str and turn_id not in turn_ids
        turn_ids.add(turn_id)
        assert excluded_route == (None if classification == "REACHED_ALPHA" else route)
        assert turn["provider_calls"] == (1 if classification == "REACHED_ALPHA" else 0), turn["name"]
        assert not any(key.startswith("natural_alpha_coverage") for key in turn["result"]), turn["name"]
        actual[classification] += 1
    assert sum(actual.values()) == len(alpha) == len(turn_ids)
    assert actual == expected and "UNACCREDITED" not in actual
    assert all(row[0] is None and row[1] is None for turn in legacy for row in turn["rows"])
    assert not [record for record in caplog.records if "natural_alpha_coverage" in record.getMessage()]

    report = report_dogfood(candidate.feedback.db_path, email=IDENTITY["email"], since="2000-01-01T00:00:00+00:00")
    assert report["unaccredited_turns_window"] == 0
    assert report["traffic_turns_window"] == len(alpha)
    for name in ("REACHED_ALPHA", "PROFILE_BYPASS", "CONTROL_BYPASS", "FAST_PATH_BYPASS", "OTHER_BYPASS"):
        assert report[name.lower()] == expected[name], name
    assert report["secondary_traces_window"] == sum(len(turn["rows"]) - 1 for turn in alpha)

    for before, after in zip(legacy, alpha):
        if before["name"] == "multi_alpha":
            continue  # The one authorized Alpha effect: stepwise clarification.
        for field in PARITY_FIELDS:
            assert after["result"].get(field) == before["result"].get(field), (before["name"], field)
        # Profile jobs run concurrently: compare the multiset of source calls.
        assert Counter(after["secop_calls"]) == Counter(before["secop_calls"]), before["name"]
        # Coverage adds one trace only to a turn that legacy did not trace.
        assert len(after["rows"]) == max(len(before["rows"]), 1), before["name"]
    assert _effects(candidate) == _effects(baseline)
