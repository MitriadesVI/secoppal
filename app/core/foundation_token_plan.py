"""Whole-token Foundation topics: one bounded candidate call, then the local predicate.

The SECOP endpoint offers no regex or Unicode word boundary in SoQL (probed
2026-09-28 on classic and SODA3: regexp_like, regex_match, regexp_replace,
regex_replace, ``~`` and SIMILAR TO are rejected). The approved meaning is kept
by asking the source for a folded-substring superset under the same WHERE,
ORDER BY and universe, and applying the approved Unicode token predicate before
the total and the page. Filtering keeps the source order, so nothing is re-sorted
locally and a truncated candidate page still yields an exact ordered prefix.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.core.foundation_text_predicates import fold_text, unicode_tokens, whole_token_truth


# Closed catalog: each entry is an approved meaning (N7, DA2 2026-09-28), keyed by
# the host-normalized evidence. A new word needs its own approval, not a rule.
APPROVED_TOKEN_SETS = {
    "obra": ("OBRA", "OBRAS"),
    "obras": ("OBRA", "OBRAS"),
}
# One source page: the protocol admits a single primary query per request.
CANDIDATE_CAP = 1000
PAGE_LIMIT = 50


@dataclass(frozen=True, slots=True)
class TokenFilter:
    condition_id: str
    columns: tuple[str, ...]
    tokens: tuple[str, ...]
    candidate_cap: int = CANDIDATE_CAP
    page_limit: int = PAGE_LIMIT

    def receipt(self) -> dict:
        return {"condition_id": self.condition_id, "columns": list(self.columns),
                "tokens": list(self.tokens), "candidate_cap": self.candidate_cap,
                "page_limit": self.page_limit}


def approved_tokens(value: str) -> tuple[str, ...]:
    tokens = APPROVED_TOKEN_SETS.get(value)
    if tokens is None:
        raise ValueError("TOKEN_SET_NOT_ACCREDITED")
    return tokens


def superset_term(tokens: tuple[str, ...]) -> str:
    """The one folded substring contained in every approved token."""
    base = min(tokens, key=len)
    if (any(unicode_tokens(token) != (fold_text(token),) for token in tokens)
            or any(fold_text(base) not in fold_text(token) for token in tokens)):
        raise ValueError("TOKEN_SUPERSET_NOT_SINGLE_TERM")
    return base


def select_members(rows: list[dict], spec: TokenFilter) -> tuple[list[dict], bool]:
    """Keep TRUE rows in source order; complete only if the candidate page did not fill."""
    if len(rows) > spec.candidate_cap:
        raise ValueError("TOKEN_CANDIDATES_EXCEED_CAP")
    members = [row for row in rows
               if whole_token_truth(tuple(row.get(column) for column in spec.columns),
                                    spec.tokens) is True]
    return members, len(rows) < spec.candidate_cap
