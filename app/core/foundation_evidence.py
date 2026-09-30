"""The one raw phrase a grounded condition names, read by occurrence (ADR-102).

The host used to read a TOPIC from its first span only, so a condition citing
«A» and «B» executed «A» and dropped «B» without saying so (G1/T4). Here every
span is accounted for: a repeated citation is one phrase, a phrase cited in
contiguous pieces is that phrase, and anything else has no declared meaning in
the contract, so the host asks instead of choosing a part.

A TOPIC span that is the very occurrence the request cites as its universe
(«contratos» in «contratos de primera infancia») is the request's word, not a
topic (G2). The rule is by occurrence, never by word: another «contratos» in the
same text («gestión de contratos laborales») stays topic content.
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


UNIVERSE_SPAN_OMITTED = "universe-span-omitted"


def topic_spans(condition, requests) -> tuple[tuple, str | None, bool]:
    """(material spans, reason, omitted) after removing the request's own universe occurrence."""
    universe = {(r.universe_evidence.start, r.universe_evidence.end) for r in requests
                if r.request_id in condition.request_ids and r.universe_evidence is not None}
    material, omitted = [], False
    for item in condition.evidence:
        if (item.start, item.end) in universe:
            omitted = True  # the universe claim owns this exact occurrence
            continue
        if any(item.start < end and start < item.end for start, end in universe):
            return (), "TOPIC_OVERLAPS_REQUEST_UNIVERSE", omitted
        material.append(item)
    if not material:
        return (), "TOPIC_ONLY_REQUEST_UNIVERSE", omitted
    return tuple(material), None, omitted
