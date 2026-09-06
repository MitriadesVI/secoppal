"""SoQL projection of Actor Identity's closed sentinel vocabulary.

Only source eligibility: typed document validation stays in normalize_identity.
UPPER/TRIM preserve internal spacing and accents; both accredited spellings of
SIN INFORMACION are supplied by the authority. No name or digit-shape fallback.
"""
from __future__ import annotations

import re

from app.core.actor_identity import actor_document_sentinels


def exclude_actor_document_sentinels(document_column: str) -> str:
    """Exclude sentinels before grouping; accept only a physical column name.

    SoQL TRIM removes outer spaces. Other whitespace rejected by Python's
    strip remains a parser defense; this is not full identity validation SQL.
    """
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", document_column):
        raise ValueError("Expected a physical document column")
    literals = ", ".join(
        "'" + sentinel.replace("'", "''") + "'"
        for sentinel in actor_document_sentinels()
    )
    return f"upper(trim({document_column})) NOT IN ({literals})"
