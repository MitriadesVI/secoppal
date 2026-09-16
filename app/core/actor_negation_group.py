"""Actor atoms coordinated under a single negative marker.

Un marcador negativo gobierna en español una **colección**, no un
constituyente: "sin el proveedor NIT A ni el proveedor NIT B" excluye a los
dos.  La capa de binding leía sólo el primer documento acreditado bajo el
marcador, así que todo actor posterior de la misma colección quedaba fuera del
alcance negado y llegaba intacto al extractor positivo.  El usuario pedía
excluir a B y recibía exactamente B: el complemento de lo pedido, que es
indistinguible de una respuesta correcta.

Este módulo posee esa gramática y nada más.  **No resuelve identidad**: las
coordenadas de documento llegan ya acreditadas por la autoridad numérica del
router, y de esta costura no sale ningún valor ni tipo de documento.

La coordinación se declara aquí, local a ACTOR, a propósito.  Ensanchar el
vocabulario gramatical compartido —meter ``ni`` en las palabras de enlace
generales— dejaría que OBJECT, STATE o MONEY cruzaran un coordinador en
silencio, que es un defecto de la misma familia que este módulo cierra.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Pattern

_ACTOR_DOCUMENT_TYPE_PATTERN = (
    r"(?:nit|c[eé]dulas?|c\.c\.|cc|documento(?:\s+de\s+identidad)?)"
)
_ACTOR_DOCUMENT_PREFIX_RE = re.compile(
    r"(?P<mention>\b(?:(?:de|del|al|el|la|los|las)\s+)?(?:"
    r"(?:proveedor|contratista|adjudicatario)(?:\s+adjudicado)?"
    rf"(?:\s*(?:[,:(\-]\s*)?(?:(?:identificado\s+)?con|"
    rf"n(?:umero|ro)?\.?)?\s*{_ACTOR_DOCUMENT_TYPE_PATTERN})?"
    rf"|{_ACTOR_DOCUMENT_TYPE_PATTERN}"
    r")\s*)$",
    re.IGNORECASE,
)
_ACTOR_DOCUMENT_VALUE_RE = re.compile(
    r"(?:\d{1,3}(?:[.\- ]\d{3}){1,3}(?:[.\- ]\d)?|\d{5,11})"
)
# Coordinadores negativos y copulativos. Mismo conjunto cerrado que ya usan
# `modality_composition` y `modality_scope`: es morfología de enlace, no
# catálogo semántico, y no crece con el uso.
_ACTOR_COORDINATORS: tuple[str, ...] = ("ni", "y", "o")
_ACTOR_COORDINATION_RE = re.compile(
    r"^[\s,()\-]*(?:(?:" + "|".join(_ACTOR_COORDINATORS) + r")\b[\s,()\-]*)*$",
    re.IGNORECASE,
)
# Una mención introducida por preposición ("del proveedor C") abre un
# constituyente POSITIVO nuevo y nunca continúa la colección negada. Es la
# única frontera que separa "sin A ni B" de "sin A ni B; del C".
_ACTOR_MENTION_PREPOSITION_RE = re.compile(r"^\s*(?:de|del|al)\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class ActorAtom:
    """Un actor acreditado y la mención estructural que lo introduce."""

    mention_start: int
    document_start: int
    document_end: int


@dataclass(frozen=True, slots=True)
class ActorNegationGroup:
    """Los actores que un marcador gobierna, en orden de aparición.

    ``governed`` distingue las dos lecturas posibles del hueco entre marcador
    y primera mención: si es puramente gramatical el marcador gobierna a los
    actores; si hay un constituyente libre de por medio ("sin alimentación del
    proveedor NIT A") el marcador gobierna ese constituyente y debe detenerse
    antes del actor, que sigue siendo positivo.
    """

    atoms: tuple[ActorAtom, ...]
    governed: bool

    @property
    def start(self) -> int:
        return self.atoms[0].mention_start

    @property
    def end(self) -> int:
        return self.atoms[-1].document_end


def validate_document_spans(
    text: str,
    spans: Iterable[tuple[int, int]],
) -> tuple[tuple[int, int], ...]:
    """Keep only exact document-shaped spans in these text coordinates.

    Callers still own numeric/identity authority.  This is a defensive seam:
    stale, partial, foreign, or out-of-bounds coordinates cannot turn an
    arbitrary word into an ACTOR request when the binding layer is called
    directly.
    """
    valid: set[tuple[int, int]] = set()
    for span in spans:
        if (
            not isinstance(span, tuple)
            or len(span) != 2
            or any(isinstance(value, bool) or not isinstance(value, int) for value in span)
        ):
            continue
        start, end = span
        if not 0 <= start < end <= len(text):
            continue
        if start and (text[start - 1].isdigit() or text[start - 1] in ".-"):
            continue
        if end < len(text) and (text[end].isdigit() or text[end] in ".-"):
            continue
        value = text[start:end]
        if _ACTOR_DOCUMENT_VALUE_RE.fullmatch(value) is None:
            continue
        digits = re.sub(r"\D", "", value)
        if 5 <= len(digits) <= 11:
            valid.add(span)
    return tuple(sorted(valid))


def _mention_start(text: str, floor: int, document_start: int) -> int:
    """Expandir a la izquierda sobre la mención estructural, si la hay."""
    match = _ACTOR_DOCUMENT_PREFIX_RE.search(text[floor:document_start])
    if match is None:
        return document_start
    return floor + match.start("mention")


def _continues_collection(gap: str, mention: str) -> bool:
    """¿El siguiente actor pertenece a la colección ya negada?"""
    if _ACTOR_MENTION_PREPOSITION_RE.match(mention) is not None:
        return False
    return _ACTOR_COORDINATION_RE.fullmatch(gap) is not None


def locate_group(
    text: str,
    *,
    marker_end: int,
    limit: int,
    document_spans: tuple[tuple[int, int], ...],
    grammatical_gap: Pattern[str],
) -> ActorNegationGroup | None:
    """Locate the accredited actors a negative marker governs.

    ``grammatical_gap`` lo aporta la capa de binding: el vocabulario de enlace
    general es suyo, no de este módulo.  La coordinación entre actores, en
    cambio, es exclusiva de ACTOR y se decide aquí.
    """
    candidates = sorted(
        span for span in document_spans
        if marker_end <= span[0] < span[1] <= limit
    )
    if not candidates:
        return None

    first_start, first_end = candidates[0]
    first = ActorAtom(
        _mention_start(text, marker_end, first_start), first_start, first_end,
    )
    if grammatical_gap.fullmatch(text[marker_end:first.mention_start]) is None:
        return ActorNegationGroup(atoms=(first,), governed=False)

    atoms = [first]
    for document_start, document_end in candidates[1:]:
        previous_end = atoms[-1].document_end
        mention_start = _mention_start(text, previous_end, document_start)
        if not _continues_collection(
            text[previous_end:mention_start], text[mention_start:document_start],
        ):
            break
        atoms.append(ActorAtom(mention_start, document_start, document_end))
    return ActorNegationGroup(atoms=tuple(atoms), governed=True)
