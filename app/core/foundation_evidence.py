"""The one raw phrase a grounded condition names, read by occurrence (ADR-102).

The host used to read a TOPIC from its first span only, so a condition citing
«A» and «B» executed «A» and dropped «B» without saying so (G1/T4). Here every
span is accounted for: a repeated citation is one phrase, a phrase cited in
contiguous pieces is that phrase, and anything else has no declared meaning in
the contract, so the host asks instead of choosing a part.
"""

from __future__ import annotations

from app.utils.money import normalize_text

MULTI_EVIDENCE = "MULTI_EVIDENCE_{dimension}_UNSUPPORTED"


def material_phrase(spans, raw: str, dimension: str) -> tuple[str | None, str | None]:
    """(phrase, None) when the spans name one raw phrase; (None, reason) otherwise."""
    ordered = sorted({(span.start, span.end) for span in spans})
    if not ordered:
        return None, f"NO_MATERIAL_EVIDENCE:{dimension}"
    if len({normalize_text(raw[start:end]) for start, end in ordered}) == 1:
        start, end = ordered[0]
        return raw[start:end], None
    reach = ordered[0][1]
    for start, end in ordered[1:]:
        if raw[reach:start].strip():  # a word between two citations: not one phrase
            return None, MULTI_EVIDENCE.format(dimension=dimension)
        reach = max(reach, end)
    return raw[ordered[0][0]:reach], None
