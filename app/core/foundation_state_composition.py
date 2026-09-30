"""States named inside one STATE condition (ADR-102 §10.3).

The host resolved a STATE from the first state word it recognized, so
«publicados o abiertos» proposed with operator "is" executed only «Publicado».
A condition that names two states is a composition. The contract represents
exactly one: the open funnel absorbing its own literal under a bare «o»
(ADR-100, prompt rule S3), proposed as one "family" condition. Any other
composition («y», a comma, another pair, the literal operator) is asked, never
collapsed to one of its states.
"""

from __future__ import annotations

import re

from app.utils.money import normalize_text

_STATE_WORDS = (
    ("Publicado", re.compile(r"\bpublicad[oa]s?\b")),
    ("abierto", re.compile(r"\babiert[oa]s?\b")),
    ("Terminado", re.compile(r"\bterminad[oa]s?\b")),
    ("cerrado", re.compile(r"\bcerrad[oa]s?\b")),
)
_FUNNEL_UNDER_BARE_O = re.compile(r"(?:publicad[oa]s?\s+o\s+abiert[oa]s?|abiert[oa]s?\s+o\s+publicad[oa]s?)")
STATE_COMPOSITION_UNSUPPORTED = "STATE_COMPOSITION_UNSUPPORTED"


def named_states(folded: str) -> tuple[str, ...]:
    return tuple(name for name, pattern in _STATE_WORDS if pattern.search(folded))


def composition_reason(condition, raw: str) -> str | None:
    """None when the condition names at most one state or the accredited funnel shape."""
    named = named_states(normalize_text(" ".join(span.quote for span in condition.evidence)))
    if len(named) <= 1:
        return None
    covered = normalize_text(raw[min(s.start for s in condition.evidence):max(s.end for s in condition.evidence)])
    if (set(named) == {"Publicado", "abierto"} and condition.operator == "family"
            and _FUNNEL_UNDER_BARE_O.fullmatch(covered.strip())):
        return None
    return STATE_COMPOSITION_UNSUPPORTED
