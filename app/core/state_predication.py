"""The copula that predicates a typed contract state belongs to that state.

"¿Cuántos contratos de Galapa figuran actualmente suspendidos?" — once the
state authority owns ``suspendidos``, the verb that reports it (``figuran``)
has no other reading: it is not a topic, it is how the source's reported state
is being asked about.  Left behind it became ``objeto = [figuran]`` and a
``LIKE '%figuran%'`` on the contract object.  Until ANALYTIC_CONTROL_UPSTREAM_V1
the only thing absorbing it was a word list inside Contract Activity.

Span-based: the verb is consumed only when it is immediately followed (through
the adverbs of currentness "actualmente"/"hoy" or "como") by the state span the
authority accredited.  ``figuran`` anywhere else stays content.
"""
from __future__ import annotations

import re

# Reporting copulas of a state predicate.  ``estar`` is already structural
# (``app.data.stopwords``); these are the forms the corpus shows.
_PREDICATE = re.compile(
    r"\b(?:figuran|figura)(?:\s+(?:actualmente|hoy|como))*\s+$",
    re.IGNORECASE,
)


def state_predicate(text: str, state_span: str) -> tuple[int, int] | None:
    """Offsets of the copula (plus adverbs) right before ``state_span``, if any.

    Offsets, not a surface: the same verb may appear earlier in the turn with
    a topical reading ("contratos que figuran en el plan … figuran suspendidos").
    """
    match = re.search(rf"\b{re.escape(state_span)}\b", text, re.IGNORECASE)
    if match is None:
        return None
    predicate = _PREDICATE.search(text[: match.start()])
    if predicate is None:
        return None
    return predicate.start(), predicate.start() + len(predicate.group(0).rstrip())


__all__ = ["state_predicate"]
