"""Executed SQLite reporting and pure partition properties for coverage."""
from collections import Counter
import json
import sqlite3

from hypothesis import given, strategies as st
import pytest

from app.core.natural_alpha.coverage_report import (
    classify_coverage, coverage_summary, is_reached_alpha, LANGUAGE_CLASSES, REACHED_STATUSES,
)
from app.core.natural_alpha.dogfood import report_dogfood
from tests.test_natural_alpha_dogfood import EMAIL, SINCE, _database, _row


def _bypass(trace_id, route, **kwargs):
    return _row(trace_id, route_reason=route, natural_alpha_mode=None,
                natural_alpha_eligible=None, natural_alpha_status=None, **kwargs)


def test_sqlite_checkpoint_fifty_reached_not_fifty_traffic_and_control_is_separate(tmp_path):
    db = _database(tmp_path, [
        _bypass("control", "reset"), _bypass("fast", "radar_list"),
        _bypass("profile", "entity_profile_overview"), _bypass("other", "unmapped_host_route"),
        *[_row(f"alpha-{i:02}", i + 1) for i in range(55)],
        _bypass("decoy-email", "reset", user_id="other"),
        _bypass("decoy-channel", "reset", channel="telegram"),
        _bypass("decoy-before", "reset", timestamp="2026-09-12T23:00:00+00:00"),
    ])
    with sqlite3.connect(db) as conn:
        conn.execute("ALTER TABLE traces ADD COLUMN trace_json TEXT DEFAULT '{}'")
        for trace_id, classification, route in (("control", "CONTROL_BYPASS", "reset"),
                                                ("fast", "FAST_PATH_BYPASS", "radar_list"),
                                                ("other", "OTHER_BYPASS", "unmapped_host_route")):
            conn.execute("UPDATE traces SET trace_json=? WHERE trace_id=?", (json.dumps({
                "natural_alpha_coverage_class": classification,
                "natural_alpha_excluded_by_route": route,
                "user_query": "PRIVATE MUST NOT BE SELECTED",
            }), trace_id))
        conn.execute("UPDATE traces SET trace_json=? WHERE trace_id='profile'", (json.dumps({
            "natural_alpha_coverage_class": "PROFILE_BYPASS",
            "natural_alpha_excluded_by_route": "entity_profile_overview",
            "timings_ms": {"entity_profile_planned_jobs": 4.0, "entity_profile_calls_total": 2.0},
        }),))
    before = db.read_bytes()
    report = report_dogfood(db, email=EMAIL, since=SINCE)
    assert report["checkpoint_ready"]
    assert report["window_end"]["trace_id"] == "alpha-49"
    assert report["reached_alpha"] == report["reached_seam"] == 50
    assert report["fast_path_bypass"] == report["profile_bypass"] == report["other_bypass"] == report["control_bypass"] == 1
    assert report["traffic_turns_window"] == report["eligible_turns_window"] == 54
    assert report["natural_work_turns_window"] == 53
    assert report["coverage_gap"] == 3 and report["coverage_rate"] == 50 / 53
    assert report["control_by_route"] == {"reset": 1}
    assert report["excluded_by_route"] == {"reset": 1, "radar_list": 1, "entity_profile_overview": 1, "unmapped_host_route": 1}
    assert report["language_coverage_classes"] == list(LANGUAGE_CLASSES)
    assert db.read_bytes() == before
    assert "PRIVATE" not in json.dumps(report) and EMAIL not in json.dumps(report)


def test_profile_requires_positive_preseam_witness_and_real_alpha_wins():
    row = _bypass("profile", "entity_profile_overview")
    assert classify_coverage(row) == "UNACCREDITED"
    row.update(coverage_class="PROFILE_BYPASS", coverage_excluded_route=row["route_reason"])
    assert classify_coverage(row) == "UNACCREDITED"
    row.update(profile_planned_jobs=4.0, profile_attempted_calls=2.0)
    assert classify_coverage(row) == "PROFILE_BYPASS"
    row.update(natural_alpha_mode="alpha", natural_alpha_eligible=1, natural_alpha_status="MATCHED")
    assert classify_coverage(row) == "REACHED_ALPHA"


@pytest.mark.parametrize("route", ["direct_lookup_id", "direct_lookup_url", "pagination_more", "refinement", "suggestion"])
def test_existing_trace_without_receipt_is_not_a_language_opportunity(route):
    report = coverage_summary([_bypass("old", route)])
    assert report["natural_work_turns_window"] == report["fast_path_bypass"] == 0
    assert report["coverage_rate"] is None


@pytest.mark.parametrize("status", ["FOREIGN", "", "matched", "MATCHED ", None, True, 1, [], {}])
def test_unknown_or_malformed_status_cannot_reach_alpha(status):
    assert not is_reached_alpha(_row("foreign", natural_alpha_status=status))


@pytest.mark.parametrize("status", ["SKIPPED", "MATCHED", "MULTI_JOB_BLOCKED", "INVALID", "PROVIDER_ERROR"])
def test_only_emitted_nominal_statuses_are_positive_controls(status):
    class ForeignStatus(str):
        pass

    assert is_reached_alpha(_row("valid", natural_alpha_status=status))
    assert not is_reached_alpha(_row("subtype", natural_alpha_status=ForeignStatus(status)))


@given(st.text().filter(lambda status: status not in REACHED_STATUSES))
def test_no_other_string_can_complete_a_checkpoint(status):
    row = _row("foreign", natural_alpha_status=status)
    assert not is_reached_alpha(row)
    assert coverage_summary([row])["natural_work_turns_window"] == 0


@pytest.mark.parametrize("mode", [None, "off", "shadow", "alpha"])
def test_route_and_profile_timings_do_not_prove_dogfood_eligibility(mode):
    row = _bypass("history", "entity_profile_overview")
    row.update(natural_alpha_mode=mode, profile_planned_jobs=4.0, profile_attempted_calls=2.0)
    summary = coverage_summary([row])
    assert summary["profile_bypass"] == summary["natural_work_turns_window"] == 0
    assert summary["unaccredited_by_route"] == {"entity_profile_overview": 1}


@pytest.mark.parametrize("field,value", [("natural_alpha_mode", "shadow"), ("natural_alpha_mode", "off"),
                                         ("natural_alpha_eligible", 0), ("natural_alpha_status", "FOREIGN")])
def test_contradictory_alpha_fields_do_not_borrow_a_bypass_receipt(field, value):
    row = _bypass("contradiction", "direct_lookup_id")
    row.update(coverage_class="FAST_PATH_BYPASS", coverage_excluded_route=row["route_reason"])
    row[field] = value
    assert coverage_summary([row])["natural_work_turns_window"] == 0


def test_foreign_status_cannot_complete_the_sqlite_checkpoint(tmp_path):
    db = _database(tmp_path, [*[_row(f"alpha-{i:02}", i) for i in range(49)],
                              _row("foreign", 49, natural_alpha_status="FOREIGN")])
    report = report_dogfood(db, email=EMAIL, since=SINCE)
    assert report["reached_seam"] == 49
    assert not report["checkpoint_ready"]
    assert report["natural_work_turns_window"] == 49


def test_fake_reached_metadata_and_unknown_route_do_not_create_checkpoint_volume():
    row = _bypass("fake", "unknown")
    row.update(coverage_class="REACHED_ALPHA", coverage_excluded_route="unknown")
    assert classify_coverage(row) == "UNACCREDITED"
    row.update(coverage_class="CONTROL_BYPASS", coverage_excluded_route="reset")
    assert classify_coverage(row) == "UNACCREDITED"


@given(st.lists(st.sampled_from((*LANGUAGE_CLASSES, "CONTROL_BYPASS")), max_size=80))
def test_coverage_partition_never_counts_controls_in_language_opportunities(classes):
    rows = []
    for classification in classes:
        row = _row("a") if classification == "REACHED_ALPHA" else _bypass("b", "unknown")
        if classification == "PROFILE_BYPASS":
            row.update(route_reason="entity_profile_overview", profile_planned_jobs=0.0, profile_attempted_calls=0.0,
                       coverage_class=classification, coverage_excluded_route="entity_profile_overview")
        elif classification != "REACHED_ALPHA":
            row.update(coverage_class=classification, coverage_excluded_route="unknown")
        rows.append(row)
    report = coverage_summary(rows)
    counts = Counter(classes)
    denominator = len(classes) - counts["CONTROL_BYPASS"]
    assert report["traffic_turns_window"] == len(classes)
    assert report["natural_work_turns_window"] == denominator
    assert report["coverage_gap"] + report["reached_alpha"] == denominator
    assert report["coverage_rate"] == (counts["REACHED_ALPHA"] / denominator if denominator else None)
