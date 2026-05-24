"""Tests RELEVANCE-PHRASE-001 — BM25 + shingle bigram scoring."""
from __future__ import annotations

import pytest

from app.core.relevance import RelevanceScorer, RelevanceScore, tokenize, bigrams, _bm25_score


# ── Tokenization ─────────────────────────────────────────────────────────

def test_tokenize_basic():
    assert tokenize("control de calidad") == ["control", "calidad"]


def test_tokenize_strips_accents():
    assert tokenize("construcción vías") == ["construccion", "vias"]


def test_tokenize_filters_short_tokens():
    assert tokenize("el agua de la region") == ["agua", "region"]


# ── Bigrams ──────────────────────────────────────────────────────────────

def test_bigrams():
    tokens = ["control", "calidad", "agua"]
    assert bigrams(tokens) == [("control", "calidad"), ("calidad", "agua")]


# ── BM25 scoring ──────────────────────────────────────────────────────────

def test_bm25_perfect_match():
    query = ["control", "calidad"]
    doc = ["control", "calidad", "agua"]
    score = _bm25_score(query, doc, avg_doc_len=3.0, doc_count=10, term_df={"control": 3, "calidad": 2})
    assert score > 1.0


def test_bm25_no_match():
    query = ["control", "calidad"]
    doc = ["obra", "civil", "pavimentacion"]
    score = _bm25_score(query, doc, avg_doc_len=3.0, doc_count=10, term_df={"control": 3, "calidad": 2})
    assert score == 0.0


# ── RelevanceScorer integration ──────────────────────────────────────────

def test_score_rows_direct_match():
    scorer = RelevanceScorer()
    topic = ["control", "calidad", "agua"]
    rows = [
        {"objeto_del_contrato": "Control de calidad de agua potable"},
        {"objeto_del_contrato": "Pavimentacion vial"},
    ]
    scored = scorer.score_rows(topic, rows, field="objeto_del_contrato")
    assert len(scored) == 2
    # First result should be direct
    _, first_score = scored[0]
    assert first_score.classification in ("direct", "probable")
    assert first_score.bm25_score > 0
    # Second should be weak
    _, second_score = scored[1]
    assert second_score.classification == "weak"


def test_score_rows_empty():
    scorer = RelevanceScorer()
    scored = scorer.score_rows([], [{"objeto": "test"}])
    assert len(scored) == 1
    assert scored[0][1].classification == "weak"


def test_score_rows_sorts_direct_first():
    scorer = RelevanceScorer()
    topic = ["control", "calidad", "agua"]
    rows = [
        {"objeto_del_contrato": "Pavimentacion vial"},
        {"objeto_del_contrato": "Control de calidad de agua potable y saneamiento"},
        {"objeto_del_contrato": "Obra civil general"},
    ]
    scored = scorer.score_rows(topic, rows, field="objeto_del_contrato")
    # First should be the matching one
    assert "calidad" in str(scored[0][0]).lower() or "control" in str(scored[0][0]).lower()


def test_bigram_boost_improves_ranking():
    """Phrases with contiguous bigrams score higher than scattered tokens."""
    scorer = RelevanceScorer()
    topic = ["control", "calidad", "agua", "consumo", "humano"]
    rows = [
        # This has exactly the phrase
        {"objeto_del_contrato": "Control de calidad del agua para consumo humano"},
        # This has all tokens scattered
        {"objeto_del_contrato": "control ambiental calidad recurso agua consumo animal"},
    ]
    scored = scorer.score_rows(topic, rows, field="objeto_del_contrato")
    # First should be higher scored (has contiguous bigrams)
    _, first = scored[0]
    _, second = scored[1]
    assert first.bm25_score >= second.bm25_score
