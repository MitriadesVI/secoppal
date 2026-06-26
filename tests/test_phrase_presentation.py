"""Commit 6C-UX: header determinístico honesto + trace completo."""
from __future__ import annotations

from app.core.phrase_presentation import build_phrase_header, build_phrase_trace

COUNTS = {"direct": 170, "probable": 12, "weak": 2, "none": 16}  # validated=184, total=200


def test_header_pool_not_full_uses_validated_not_total():
    counts = {"direct": 90, "probable": 6, "weak": 0, "none": 13}  # validated 96
    h = build_phrase_header(counts, candidate_count=109, candidate_pool_full=False,
                            secop_total_count=500, displayed_count=25)
    assert "96 coincidencias pertinentes" in h
    assert "25 mejor clasificadas" in h
    assert "500" not in h  # nunca presenta el total de SECOP como pertinentes


def test_header_pool_full_uses_first_n_language():
    h = build_phrase_header(COUNTS, candidate_count=200, candidate_pool_full=True,
                            secop_total_count=200, displayed_count=25)
    assert "184 coincidencias entre los primeros 200" in h
    assert "en total" not in h  # no afirma que no hay más después del límite


def test_header_pool_full_and_secop_reports_more():
    h = build_phrase_header(COUNTS, candidate_count=200, candidate_pool_full=True,
                            secop_total_count=1240, displayed_count=25)
    assert "1,240 candidatos textuales" in h
    assert "primeros 200" in h and "validé 184" in h


def test_trace_invariants():
    t = build_phrase_trace(COUNTS, candidate_count=200, candidate_pool_full=True,
                           secop_total_count=1240, total_count_approximate=False, displayed_count=25)
    assert t["validated_count"] == t["direct_count"] + t["probable_count"] + t["weak_count"]
    assert t["candidate_count"] == t["validated_count"] + t["unvalidated_count"]
    assert t["displayed_count"] <= t["validated_count"]
    assert t["ranking_strategy"] == "phrase_tier_then_bm25"
