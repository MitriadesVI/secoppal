"""SUPPLIERS ranking criteria: the two orderings the capability already computes.

Entity Profile suppliers always returns two rankings, by number of contracts and
by contracted value. "Los proveedores con más contratos" names the first of them;
the word ``contratos`` there is the ranking metric, not a source choice and not
a topic. No other authority owns that reading (the source authority does not
accredit it, Control Language does not consume it), so the capability's own
contract does, and only for these two metrics.
"""
from __future__ import annotations

import re

_LEAD = r"(?:(?:con|de|por)\s+)?(?:(?:el|la|los|las)\s+)?"
_COUNT = re.compile(
    rf"\b{_LEAD}(?:mas|mayor(?:es)?\s+(?:numero|cantidad)\s+de)\s+contratos?\b",
)
_VALUE = re.compile(
    rf"\b{_LEAD}(?:mas|mayor(?:es)?)\s+valor(?:es)?"
    r"(?:\s+(?:de|del|en)\s+(?:(?:los|el)\s+)?contratos?)?\b",
)


def ranking_criterion_spans(normalized: str) -> tuple[tuple[int, int], ...]:
    """Spans naming a count or value supplier ranking, in folded lower-case text."""
    spans = [match.span() for pattern in (_COUNT, _VALUE) for match in pattern.finditer(normalized)]
    return tuple(sorted(set(spans)))
