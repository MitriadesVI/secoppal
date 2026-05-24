"""Relevance scoring layer v1 — BM25 + shingle bigrams + morphological synonyms.

Sprint 3 del plan maestro (SECOPPAL_ROBUSTECIMIENTO_SISTEMICO.md §5.4):
- score_row(topic, row) → {class: direct|probable|weak, score, reasons}
- Insertar nodo Burr score_relevance después de execute_query
- Validación: corpus relevance ≥85%

BM25 implementation is self-contained (no external deps). Shingle bigram boost
penalizes results that match individual tokens but miss contiguous phrases.
"""
from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

# BM25 constants — standard defaults
K1 = 1.2   # term frequency saturation
B = 0.75   # document length normalization


def _strip_accents(text: str) -> str:
    """Remove Spanish accents for token matching."""
    replacements = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n",
        "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ñ": "N",
    }
    for accented, plain in replacements.items():
        text = text.replace(accented, plain)
    return text


def tokenize(text: str) -> list[str]:
    """Tokenize Spanish text: lowercase, strip accents, split on non-alpha."""
    text = _strip_accents(text.lower())
    # Keep only letters and spaces
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return [t for t in text.split() if len(t) >= 3]


def bigrams(tokens: list[str]) -> list[tuple[str, str]]:
    """Extract contiguous bigram pairs."""
    return [(tokens[i], tokens[i + 1]) for i in range(len(tokens) - 1)]


def _bm25_score(
    query_terms: list[str],
    doc_terms: list[str],
    avg_doc_len: float,
    doc_count: int,
    term_df: dict[str, int],
) -> float:
    """Compute BM25 score for a single document against query terms."""
    doc_len = len(doc_terms)
    if doc_len == 0:
        return 0.0

    tf = Counter(doc_terms)
    score = 0.0

    for term in query_terms:
        if term not in term_df:
            continue
        df = term_df[term]
        # IDF: inverse document frequency
        idf = math.log((doc_count - df + 0.5) / (df + 0.5) + 1.0)
        # TF with saturation
        f = tf.get(term, 0)
        numerator = f * (K1 + 1)
        denominator = f + K1 * (1 - B + B * (doc_len / avg_doc_len))
        score += idf * (numerator / denominator)

    return score


@dataclass
class RelevanceScore:
    """Output of score_row: class + score + diagnostic reasons."""
    classification: str = "weak"   # direct | probable | weak
    bm25_score: float = 0.0
    bigram_matches: int = 0
    reasons: list[str] = field(default_factory=list)


class RelevanceScorer:
    """BM25 + shingle bigram scorer with morphological synonym awareness.

    Usage:
        scorer = RelevanceScorer()
        scored = scorer.score_rows(["control", "calidad", "agua"], rows, field="objeto_del_contrato")
        # scored is list of (row, RelevanceScore) sorted by classification then bm25_score
    """

    # Morphological synonyms from the variant dictionary — populated at runtime
    # to avoid circular imports. Key: root → set of surface forms.
    _synonyms: dict[str, set[str]] = {}

    def __init__(self):
        self._load_synonyms()

    @staticmethod
    def _load_synonyms():
        """Lazy-load morphological variant dictionary."""
        if RelevanceScorer._synonyms:
            return
        try:
            from app.data.morphological_variants import ROOT_TO_VARIANTS
            for root, variants in ROOT_TO_VARIANTS.items():
                RelevanceScorer._synonyms[root] = set(variants)
        except ImportError:
            pass  # no morphological variants available — skip synonym expansion

    def _expand_synonyms(self, terms: list[str]) -> list[str]:
        """Expand query terms using morphological variants."""
        expanded = list(terms)
        for term in terms:
            for root, variants in self._synonyms.items():
                if term in variants or term == root:
                    expanded.extend(variants)
        return list(set(expanded))

    def score_rows(
        self,
        topic_terms: list[str],
        rows: list[dict],
        field: str = "objeto_del_contrato",
    ) -> list[tuple[dict, RelevanceScore]]:
        """Score and classify rows against topic terms. Returns sorted by relevance."""
        if not rows or not topic_terms:
            return [(r, RelevanceScore()) for r in rows]

        # Expand topic with morphological synonyms
        expanded_terms = self._expand_synonyms(topic_terms)
        query_tokens = []
        for t in expanded_terms:
            query_tokens.extend(tokenize(t))

        # Extract query bigrams for shingle boost
        query_bigrams = set(bigrams(tokenize(" ".join(topic_terms))))

        # Build corpus stats for BM25
        docs: list[list[str]] = []
        for row in rows:
            text = str(row.get(field, ""))
            docs.append(tokenize(text))

        doc_count = len(docs)
        if doc_count == 0:
            return [(r, RelevanceScore()) for r in rows]

        # Average doc length
        avg_doc_len = sum(len(d) for d in docs) / doc_count

        # Document frequency per term
        term_df: dict[str, int] = {}
        for doc in docs:
            for term in set(doc):
                term_df[term] = term_df.get(term, 0) + 1

        # Score each doc
        results: list[tuple[dict, RelevanceScore]] = []
        for i, row in enumerate(rows):
            doc_tokens = docs[i]
            bm25 = _bm25_score(query_tokens, doc_tokens, avg_doc_len, doc_count, term_df)

            # Bigram boost: count matching contiguous bigrams
            doc_bigrams = set(bigrams(doc_tokens))
            bigram_hits = len(query_bigrams & doc_bigrams) if query_bigrams else 0

            # Classification thresholds
            reasons: list[str] = []
            if bm25 >= 3.0 and bigram_hits >= 2:
                classification = "direct"
                reasons.append(f"{bigram_hits} matching phrases")
            elif bm25 >= 1.5 or bigram_hits >= 1:
                classification = "probable"
                if bigram_hits >= 1:
                    reasons.append(f"{bigram_hits} matching phrase(s)")
                else:
                    reasons.append("partial token match")
            else:
                classification = "weak"
                if bm25 > 0:
                    reasons.append("low token overlap")

            results.append((row, RelevanceScore(
                classification=classification,
                bm25_score=round(bm25, 2),
                bigram_matches=bigram_hits,
                reasons=reasons,
            )))

        # Sort: direct first, then probable, then weak. Within class: by bm25 descending.
        class_order = {"direct": 0, "probable": 1, "weak": 2}
        results.sort(key=lambda x: (class_order[x[1].classification], -x[1].bm25_score))

        return results
