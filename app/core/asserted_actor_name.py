"""The bare name typed beside an explicit document is actor context, not topic.

``detect_asserted_actor_name_before_document`` already claims a name when the
user marks the role ("busca **proveedor** WILSON HERRERA — CC …") or uses an
unambiguous verbal construction.  Every other surface of the same speech act
escaped it:

    busca WILSON HERRERA — CC 98636661
    WILSON HERRERA — CC 98636661
    ahora WILSON HERRERA — CC 98636661

There the name survived into ``objeto``, so the query demanded that the
*contract object* mention the contractor — the AP0 double error, on a query
whose identity was never in doubt: the user had typed the document.

This module is the third door, and it is deliberately narrow.  A number is not
an invitation to read whatever precedes it as a name:

* the document must be **typed** — the user wrote NIT/CC/cédula.  An untyped
  digit run keeps belonging to the amount and date grammars;
* what precedes it, minus conversational framing, must be **all** of the
  remaining text.  A leftover topic, dataset word, year or amount means the
  turn said something else too, and that something else is not a name;
* the name shape is judged by the same semantic evidence the bare-actor probe
  uses (``has_entity_like_name_evidence``), never by typography.

Authority is unchanged: the document identifies the actor.  This only decides
that the accompanying words were spent naming that actor, so no other
extractor may spend them again.
"""
from __future__ import annotations

import re
from collections.abc import Callable

from app.core.actor_arbitration import (
    BareActorProbe,
    has_entity_like_name_evidence,
    has_strong_topic_evidence,
)
from app.core.actor_intent import (
    REASON_NAME_NEEDS_RESOLUTION,
    ActorReference,
    strip_document_type_marker,
)
from app.core.actor_name import normalize_actor_name

__all__ = [
    "REASON_NAME_BESIDE_DOCUMENT",
    "detect_bare_asserted_actor_name",
]


REASON_NAME_BESIDE_DOCUMENT = "ACTOR_NAME_BESIDE_DOCUMENT"

# Framing that opens a request without naming anybody.  The determiner is only
# stripped as part of a framing verb: on its own it may be the head of a real
# registered name ("LA PREVISORA S.A. COMPAÑÍA DE SEGUROS").
_LEADING_FRAMING_RE = re.compile(
    r"^(?:(?:y|ok|listo|bueno|oye)\s+)*"
    r"(?:ahora\s*[:,]?\s+)?"
    r"(?:(?:busca(?:me|te)?|mira(?:me)?|muestra(?:me)?|dime|dame|averigua|"
    r"consulta|revisa|quiero|necesito)"
    r"(?:\s+(?:informacion|informacion\s+de|datos|datos\s+de))?"
    # Only the personal-object marker is framing. An article is name material:
    # LA PREVISORA S.A. is registered with it, and stripping it handed the
    # buyer authority a name it could not recognize.
    r"(?:\s+(?:a|al))?"
    r"\s+)?",
    re.IGNORECASE,
)
_EDGE_PUNCTUATION = " \t,.;:¿?¡!\"'()[]-–—"
_DIGIT_RE = re.compile(r"\d")


def detect_bare_asserted_actor_name(
    normalized_query: str,
    document_start: int,
    *,
    claims_buyer: Callable[[str], bool] | None = None,
) -> ActorReference | None:
    """Return the contextual name a typed document was introduced with.

    ``claims_buyer`` is the caller's own buyer authority, asked whether the
    span is already a recognized purchasing entity.  It is injected rather
    than imported so this module keeps no gazetteer of its own.
    """
    actor_context = strip_document_type_marker(normalized_query[:document_start])
    if actor_context is None:
        return None

    framing = _LEADING_FRAMING_RE.match(actor_context)
    candidate = actor_context[framing.end():] if framing else actor_context
    span = candidate.strip(_EDGE_PUNCTUATION)
    if not span or _DIGIT_RE.search(span):
        # A year, an amount or a second identifier in the same span means the
        # turn carried more than a name, and this door does not adjudicate it.
        return None

    name = normalize_actor_name(span)
    if not name:
        return None
    probe = BareActorProbe(
        actor_name=name,
        unbounded_name=name,
        surface_name=span,
        name_only=True,
    )
    if not has_entity_like_name_evidence(probe):
        return None
    # Name *shape* is not permission to consume. ``has_entity_like_name_evidence``
    # only ever offered shape evidence, and its acronym branch returns True on
    # a consonant cluster before any thematic control runs — which is how
    # "mantenimiento referencia XYZ" became a name and a real topic filter
    # disappeared.
    #
    # A span that an independent authority already reads as something else is
    # not this door's to spend. Those authorities answer here, in the order
    # they own the question: the topic vocabulary, and the caller's buyer
    # resolver. Dates, amounts and numeric references are already excluded by
    # the digit rule above, and a dataset word is refused by the name-shape
    # evidence itself.
    if has_strong_topic_evidence(name, {}):
        return None
    if claims_buyer is not None and claims_buyer(span):
        return None
    return ActorReference(
        resolution_method="name_unresolved",
        identity=None,
        observed_name=name,
        consumed_spans=(span,),
        reason_codes=(REASON_NAME_BESIDE_DOCUMENT, REASON_NAME_NEEDS_RESOLUTION),
    )
