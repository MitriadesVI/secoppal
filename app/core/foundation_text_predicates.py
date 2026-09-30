"""Finite Foundation text predicates; these do not certify source tokenization.

The local Unicode predicate states the accepted lexical contract. SoQL LIKE
does not offer Unicode letter/number boundaries, so it cannot materialize it.
"""

from __future__ import annotations

import re
import unicodedata


_COLUMN = re.compile(r"[a-z][a-z0-9_]*\Z")


def fold_text(value: str) -> str:
    """Uppercase and remove accent marks without discarding Unicode letters."""
    if type(value) is not str:
        raise ValueError("INVALID_TEXT_VALUE")
    return "".join(character for character in unicodedata.normalize("NFD", value.upper())
                   if not unicodedata.combining(character))


def unicode_tokens(value: str) -> tuple[str, ...]:
    """Maximal Unicode letter/number runs after the declared folding."""
    folded = fold_text(value)
    separated = "".join(character if unicodedata.category(character)[0] in {"L", "N"}
                        else " " for character in folded)
    return tuple(separated.split())


def whole_token_truth(values: tuple[str | None, ...], tokens: tuple[str, ...]) -> bool | None:
    """Three-valued OR across columns; NULL never proves a token absent."""
    if (type(values) is not tuple or not values or type(tokens) is not tuple or not tokens
            or any(type(token) is not str or unicode_tokens(token) != (fold_text(token),)
                   for token in tokens)):
        raise ValueError("INVALID_TOKEN_PREDICATE")
    wanted = {fold_text(token) for token in tokens}
    unknown = False
    matched = False
    for value in values:
        if value is None:
            unknown = True
        else:
            matched |= bool(wanted.intersection(unicode_tokens(value)))
    return True if matched else None if unknown else False


def folded_contains_clause(fields: tuple[str, ...], term: str) -> str:
    """Exact folded substring on accredited columns, with SQL NULL semantics."""
    if (type(fields) is not tuple or not fields
            or any(type(field) is not str or _COLUMN.fullmatch(field) is None
                   for field in fields)):
        raise ValueError("INVALID_TEXT_COLUMNS")
    if type(term) is not str or not term.strip():
        raise ValueError("INVALID_TOPIC_LITERAL")
    # No source escape convention has been accredited for literal LIKE wildcards.
    if "%" in term or "_" in term:
        raise ValueError("UNACCREDITED_LIKE_LITERAL_ESCAPE")
    escaped = term.replace("'", "''")
    clauses = tuple(f"unaccent(UPPER({field})) LIKE unaccent(UPPER('%{escaped}%'))"
                    for field in fields)
    return f"({' OR '.join(clauses)})"
