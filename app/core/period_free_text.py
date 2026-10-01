"""The text the actor/buyer grammar reads, without the period the temporal authority consumed.

One responsibility: a period is not part of any actor or buyer mention. «con la alcaldía de X los últimos
2 años» names the buyer «la alcaldía de X»; read whole, the grammar captured «… los últimos 2 años» as the
buyer, a string that cannot be found again in the residual text (the period is masked there) and left the
buyer's own words behind to become the TOPIC («objeto LIKE '%alcaldia%'»). A period that opens the sentence
(«en los últimos 6 meses contratos de … con el ICBF») also made its leading «en» read as a territory switch.

The span is the temporal authority's; this module only masks it (same length, offsets unchanged), and the
«en» that opens the sentence right before it.
"""
from __future__ import annotations

import re

from app.core.query_text_consumption import mask_consumed_spans

# The one connective whose leftover changes how the sentence is read: a leading «en» turns the actor
# grammar's contextual-territory pattern on («en … contratos de … con el ICBF» = «move to <territory>»).
# Other introducers («durante», «desde»…) stay: when they trail a buyer they are consumed with its span,
# and masking them here would leave them behind in the residual text as topic words.
_LEADING_EN = re.compile(r"^\s*en\s*$", re.IGNORECASE)


def without_period(text: str, span: tuple[int, int] | None) -> str:
    """``text`` with ``span`` (and the connective right before it) masked; unchanged when there is none."""
    if span is None:
        return text
    start, end = span
    spans = [(start, end)]
    if _LEADING_EN.match(text[:start]):
        spans.append((0, start))
    return mask_consumed_spans(text, spans)


def aligned_pattern(span: str) -> str:
    """A regex that finds ``span`` again in a residual text whose separators differ from the grammar's.

    The actor grammar reads a normalised copy of the sentence (punctuation folded); the residual text
    keeps it («… sur occidente ese. en» vs «… sur occidente ese en»). An exact search then misses the
    buyer's own span and its words survive as topic. Tokens must appear in order, separated by anything
    that is not a word character.
    """
    tokens = re.findall(r"\w+", span)
    return r"\b" + r"[\W_]+".join(re.escape(token) for token in tokens) + r"\b"
