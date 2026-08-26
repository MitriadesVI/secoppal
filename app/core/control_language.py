"""Control language authority — the grammar of commands, not of content.

A query mixes two languages.  **Content language** names what the user is
looking for (``obra``, ``mantenimiento``, ``Cali``): it belongs in the WHERE
clause.  **Control language** tells the system how to present or navigate that
content (``ordenados por mayor valor``, ``los más antiguos primero``) and what
kind of question is being asked (``qué está contratando``, ``procesos que
cierran``): it must never reach a thematic ``LIKE``.

Before this module the two were separated only by accident: the ordering regex
consumed the *criterion* (``mayor valor``) but not the *command* around it, so
``contratos de obra en Cali ordenados por mayor valor`` shipped
``LIKE '%ordenados%'`` to SECOP and returned zero rows on a query the source
could answer.

Contract
--------
``interpret_control_language`` receives the positive residual text (never the
raw query — see ``RAW_QUERY_PRESERVED``) and returns a :class:`ControlRequest`
carrying the recognized ordering plus the residual with every consumed control
span removed.

INV-CONTROL-LANGUAGE-NOT-CONTENT
    A span recognized as control never survives into ``residual_text``, so it
    cannot become ``objeto``, an actor, a geography or an ecosystem trigger.

INV-CONTROL-SPAN-DOES-NOT-CLASSIFY-CONTENT
    Recognition is span-based and contextual, never a global stopword list.
    ``ordenamiento territorial`` and ``sistemas de control`` keep every token:
    the command vocabulary is only stripped around a criterion the module
    actually matched in the same turn.

Ordering signals are the canonical ``ordering_signal`` values consumed by
``soql_builder`` and gated by ``order_semantics``; this module adds the two
ascending directions the system could not express before.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

VALOR_DESC = "valor_desc"
VALOR_ASC = "valor_asc"
FECHA_DESC = "fecha_desc"
FECHA_ASC = "fecha_asc"

ORDERING_SIGNALS = frozenset({VALOR_DESC, VALOR_ASC, FECHA_DESC, FECHA_ASC})

_ASC_SIGNALS = frozenset({VALOR_ASC, FECHA_ASC})


@dataclass(frozen=True)
class ControlRequest:
    """What the control language of one turn asked for."""

    ordering_signal: str = ""
    top_n: int | None = None
    consumed_spans: tuple[str, ...] = ()
    residual_text: str = ""
    control_verb_spans: tuple[str, ...] = field(default=())

    @property
    def has_ordering(self) -> bool:
        return bool(self.ordering_signal)

    @property
    def is_ascending(self) -> bool:
        return self.ordering_signal in _ASC_SIGNALS


def _rx(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE | re.VERBOSE)


# ── Tier 1 — explicit direction pairs ────────────────────────────────────────
# "de mayor a menor" names both ends, so it must be read before any one-sided
# pattern: "de menor a mayor VALOR" contains the substring "mayor valor" and a
# single-sided reading inverts the user's intent (NORMALIZATION_ERASES_USER_
# INTENT, HCA-R2).
_VALUE_NOUN = r"(?:valor(?:es)?|cuant[ií]a|monto|precio)"
# "menor cuantía" is a procurement MODALITY ("selección abreviada de menor
# cuantía", "mínima cuantía"), never a sort key.  The ascending vocabulary
# therefore excludes "cuantía"; the descending one keeps it because "mayor
# cuantía" names no modality and was already read as an ordering criterion.
_VALUE_NOUN_ASC = r"(?:valor(?:es)?|monto|precio)"
_OLD = r"(?:antigu[oa]s?|viej[oa]s?)"
_NEW = r"(?:recientes?|nuev[oa]s?)"

_TIER1: tuple[tuple[re.Pattern[str], str], ...] = (
    (_rx(rf"\bde\s+mayor\s+a\s+menor(?:\s+{_VALUE_NOUN})?\b"), VALOR_DESC),
    (_rx(rf"\bde\s+menor\s+a\s+mayor(?:\s+{_VALUE_NOUN_ASC})?\b"), VALOR_ASC),
    (_rx(r"\bdel?\s+m[áa]s\s+car[oa]s?\s+al?\s+m[áa]s\s+barat[oa]s?\b"), VALOR_DESC),
    (_rx(r"\bdel?\s+m[áa]s\s+barat[oa]s?\s+al?\s+m[áa]s\s+car[oa]s?\b"), VALOR_ASC),
    (_rx(rf"\bdel?\s+m[áa]s\s+{_NEW}\s+al?\s+m[áa]s\s+{_OLD}\b"), FECHA_DESC),
    (_rx(rf"\bdel?\s+m[áa]s\s+{_OLD}\s+al?\s+m[áa]s\s+{_NEW}\b"), FECHA_ASC),
    (_rx(rf"\b{_VALUE_NOUN}\s+descendente\b"), VALOR_DESC),
    (_rx(rf"\b{_VALUE_NOUN_ASC}\s+ascendente\b"), VALOR_ASC),
    (_rx(r"\bfecha\s+descendente\b"), FECHA_DESC),
    (_rx(r"\bfecha\s+ascendente\b"), FECHA_ASC),
)

# ── Tier 2 — one-sided criteria, ascending first ────────────────────────────
# Ascending is scanned before descending because the descending vocabulary is
# the historical default and its patterns are broader.
_TIER2: tuple[tuple[re.Pattern[str], str], ...] = (
    # value ascending
    (_rx(rf"\bmenor(?:es)?\s+{_VALUE_NOUN_ASC}\b"), VALOR_ASC),
    (_rx(r"\bmenos\s+valor\b"), VALOR_ASC),
    (_rx(r"\bm[áa]s\s+(?:barat[oa]s?|econ[óo]mic[oa]s?)\b"), VALOR_ASC),
    # date ascending
    (_rx(rf"\bm[áa]s\s+{_OLD}\b"), FECHA_ASC),
    (_rx(rf"\b{_OLD}\s+primer[oa]s?\b"), FECHA_ASC),
    (_rx(rf"\bprimero\s+(?:los|las)\s+(?:m[áa]s\s+)?{_OLD}\b"), FECHA_ASC),
    # value descending — the pre-R6 vocabulary, preserved verbatim
    (_rx(rf"\bmayor(?:es)?\s+{_VALUE_NOUN}\b"), VALOR_DESC),
    (_rx(r"\blos\s+de\s+mayor\s+valor\b"), VALOR_DESC),
    (_rx(r"\blos\s+m[áa]s\s+grandes\s+por\s+valor\b"), VALOR_DESC),
    (_rx(r"\bm[áa]s\s+valor\b"), VALOR_DESC),
    (_rx(r"\bm[áa]s\s+(?:car[oa]s?|costos[oa]s?)\b"), VALOR_DESC),
    (_rx(r"\bm[áa]s\s+(?:grande|grandes|importante|importantes|alt[oa]s?)\b"), VALOR_DESC),
    # A bare "por valor" is an ordering criterion only when no monetary
    # complement follows: "por valor de 500 millones" is a MONEY filter and
    # must not be read as a sort key (MONEY_ORDER_CONFUSION).
    (
        _rx(
            r"""\bpor\s+(?:el\s+)?(?:valor|monto)\b
                (?!\s*(?:de\b|del\b|superior|inferior|mayor|menor|entre|igual|
                         m[aá]s\b|menos\b|desde|hasta|arriba|encima|\d))"""
        ),
        VALOR_DESC,
    ),
    # date descending — the pre-R6 vocabulary, preserved verbatim
    (_rx(r"\bm[áa]s\s+recientes?\b"), FECHA_DESC),
    # "los últimos contratos" is recency; "los últimos 30 días" is a temporal
    # window and belongs to the time authority, not to ORDER BY.
    (
        _rx(
            r"""\b[uú]ltimos?\b
                (?!\s*(?:\d|d[ií]as?|semanas?|mes(?:es)?|a[nñ]os?|trimestres?|
                          quincenas?|horas?|minutos?))"""
        ),
        FECHA_DESC,
    ),
    (_rx(r"\b(?:reciente|recientes)\b"), FECHA_DESC),
    (_rx(r"\b(?:nuevo|nuevos|nueva|nuevas|actual|actuales)\b"), FECHA_DESC),
    (_rx(r"\bpor\s+fecha\b(?!\s*(?:de\s+(?:firma|publicacion|cierre))?\s*\d)"), FECHA_DESC),
)

# Imperative and participle forms of "ordenar". This morphology can only be a
# command: the NOUNS of the same family ("orden", "órdenes", "ordenamiento")
# match none of these alternatives, so "ordenamiento territorial" and "gestión
# de órdenes de compra" keep every token. Because no reading makes them a
# contract object, they are consumed even when the user gave no criterion —
# an incomplete command still is not a topic.
_ORDER_IMPERATIVE_RE = _rx(
    r"""\b(?:
        orden[ae]nos | orden[ae]melos | orden[ae]los | orden[ae]las |
        orden[ae]me | ordenar[ml]?[eo]s? | ordenar |
        ordenad[oa]s? | ordenand[oa] |
        ordena | ordene
    )\b"""
)

# Wider command vocabulary, recognized ONLY next to a matched criterion because
# these words do have nominal readings ("clasificados").
ORDER_VERB_RE = _rx(
    r"""\b(?:
        orden[ae]nos | orden[ae]melos | orden[ae]los | orden[ae]las |
        orden[ae]me | ordenar[ml]?[eo]s? | ordenar |
        ordenad[oa]s? | ordenand[oa] |
        ordena | ordene |
        clasific[ae]d[oa]s? | clasifica
    )\b"""
)

# A dangling ordinal at the very end of the turn ("muéstrame primero") is a
# command about position. Anywhere else it modifies the noun that follows and
# is content — "primeros auxilios" must survive untouched.
_TRAILING_ORDINAL_RE = _rx(r"\b(?:primero|primeros)\b\s*[.?!¡¿]*\s*$")

# Tokens that may sit between the command verb and the criterion.
_LEFT_GLUE = frozenset({
    "por", "de", "del", "en", "segun", "orden", "primero", "primeros",
})
_LEFT_FILLER = frozenset({
    "los", "las", "el", "la", "me", "nos", "lo", "solo", "solamente",
    "contratos", "contrato", "procesos", "proceso", "resultados",
    "ahora", "luego", "entonces", "todos", "todas", "primero", "primeros",
})
_LEFT_ANCHOR = frozenset({"primero", "primeros"})
_RIGHT_TRAILER = frozenset({"primero", "primeros", "primera", "primeras"})

# Deliberately narrow: a bare "primeros N" is usually temporal ("los primeros
# 3 meses"), so the cardinality is only read from an explicit result phrase.
# ── Control verbs — the question's mode, not its subject ────────────────────
# "qué está contratando la alcaldía" asks about procurement *intent*; the verb
# selects the reading, the noun after it is still content ("está contratando
# mantenimiento" keeps `mantenimiento`).  Likewise "procesos que cierran hoy"
# speaks about a deadline, and R6 only guarantees that the verb is not a
# contractual topic — the deadline semantics themselves stay as they are.
CONTROL_VERB_RE = _rx(
    r"""\b(?:que\s+)?(?:est[aá]n?|estamos|estoy|estuv(?:o|ieron))\s+contratando\b
        |\bcontratando\b
        |\b(?:piensa|piensan|planea|planean|proyecta|proyectan|pretende|pretenden
            |espera|esperan|quiere|quieren|necesita|necesitan|va|van|ir[áa]n?)
          \s+(?:a\s+)?contratar\b
        |\b(?:que\s+)?(?:se\s+)?(?:cierran|cierra|cierren|cerrar[áa]n?|cerraran)\b"""
)

_TOP_N_RE = _rx(
    r"""\btop\s+(\d{1,3})\b
        |\blos\s+(?:primeros|primeras)\s+(\d{1,3})\s+(?:contratos|procesos|resultados)\b"""
)


def _token_spans(text: str) -> list[tuple[int, int, str]]:
    return [(m.start(), m.end(), m.group(0).lower()) for m in re.finditer(r"\S+", text)]


def _expand_left(text: str, start: int) -> tuple[int, bool]:
    """Widen a criterion span left over the command that introduced it.

    Two passes.  The first absorbs pure glue ("por", "de") — always safe.  The
    second walks further over filler nouns/articles but only *commits* when it
    reaches an actual order verb or a "primero" anchor, so a criterion with no
    command around it never swallows neighbouring content.

    Returns the new start and whether an explicit command word was found.
    """
    tokens = [t for t in _token_spans(text) if t[1] <= start]
    idx = len(tokens) - 1
    while idx >= 0 and tokens[idx][2] in _LEFT_GLUE:
        start = tokens[idx][0]
        idx -= 1
    best = start
    commanded = False
    while idx >= 0:
        tok_start, _, tok = tokens[idx]
        if ORDER_VERB_RE.fullmatch(tok):
            best = tok_start
            commanded = True
        elif tok in _LEFT_FILLER or tok in _LEFT_GLUE:
            if tok in _LEFT_ANCHOR:
                best = tok_start
                commanded = True
        else:
            break
        idx -= 1
    return best, commanded


def _expand_right(text: str, end: int) -> int:
    for tok_start, tok_end, tok in _token_spans(text):
        if tok_start < end:
            continue
        if tok.strip(".,;:") in _RIGHT_TRAILER:
            end = tok_end
            continue
        break
    return end


def _first_match(text: str, table) -> tuple[int, int, str] | None:
    best: tuple[int, int, str] | None = None
    for pattern, signal in table:
        m = pattern.search(text)
        if m and (best is None or m.start() < best[0]):
            best = (m.start(), m.end(), signal)
    return best


@dataclass(frozen=True)
class ControlSpan:
    """One located control span, in offsets over the text it was found in."""

    start: int
    end: int
    signal: str
    command_led: bool


def locate_control_span(text: str) -> ControlSpan | None:
    """Locate the earliest ordering span in ``text`` without consuming it.

    ``command_led`` is True only when the span carries an explicit command word
    — an imperative of "ordenar", a "primero" anchor, or a two-ended direction
    ("de mayor a menor"). A bare adjective ("nuevas", "recientes") is not
    command-led: inside a negated object it is still content.
    """
    tier1 = _first_match(text, _TIER1)
    hit = tier1 or _first_match(text, _TIER2)
    if hit is None:
        return None
    start, end, signal = hit
    start, commanded = _expand_left(text, start)
    end = _expand_right(text, end)
    return ControlSpan(start, end, signal, bool(tier1) or commanded)


def command_boundary(text: str) -> int | None:
    """Offset where a command starts, for callers delimiting a content span.

    A command is never part of what the user is describing, so a span that would
    otherwise run to the end of the sentence must stop here.
    """
    span = locate_control_span(text)
    if span is None or not span.command_led:
        return None
    return span.start


def detect_ordering(text: str) -> str:
    """Ordering signal expressed by ``text``, or ``""``. Read-only helper."""
    return interpret_control_language(text).ordering_signal


def interpret_control_language(text: str) -> ControlRequest:
    """Split control language off a positive residual.

    The input is already the residual the parser is about to turn into topic
    terms; the output residual is what remains once every control span is
    removed.  The caller keeps the raw query untouched.
    """
    residual = text or ""
    consumed: list[str] = []

    verb_spans = tuple(m.group(0).strip() for m in CONTROL_VERB_RE.finditer(residual))
    if verb_spans:
        residual = CONTROL_VERB_RE.sub(" ", residual)

    top_n: int | None = None
    top_match = _TOP_N_RE.search(residual)
    if top_match:
        top_n = int(top_match.group(1) or top_match.group(2))
        consumed.append(top_match.group(0))
        residual = residual[: top_match.start()] + " " + residual[top_match.end():]

    span = locate_control_span(residual)
    signal = ""
    if span is not None:
        signal = span.signal
        consumed.append(residual[span.start:span.end].strip())
        residual = residual[:span.start] + " " + residual[span.end:]
        # A command verb detached from its criterion ("ordena contratos de
        # mantenimiento por mayor valor") is still a command, never a topic.
        for leftover in ORDER_VERB_RE.finditer(residual):
            consumed.append(leftover.group(0))
        residual = ORDER_VERB_RE.sub(" ", residual)
        residual = re.sub(r"\b(?:primero|primeros)\b", " ", residual, flags=re.IGNORECASE)

    # CONTROL_VERB_CANNOT_BECOME_TOPIC, criterion or not: "ordénalos" alone is
    # an incomplete command, so no direction is inferred, but it still may not
    # become an objeto term.
    for leftover in _ORDER_IMPERATIVE_RE.finditer(residual):
        consumed.append(leftover.group(0))
    residual = _ORDER_IMPERATIVE_RE.sub(" ", residual)
    trailing = _TRAILING_ORDINAL_RE.search(residual)
    if trailing:
        consumed.append(trailing.group(0).strip())
        residual = residual[: trailing.start()]

    residual = re.sub(r"\s+", " ", residual).strip()
    return ControlRequest(
        ordering_signal=signal,
        top_n=top_n,
        consumed_spans=tuple(s for s in (*verb_spans, *consumed) if s),
        residual_text=residual,
        control_verb_spans=verb_spans,
    )


# Words that may accompany a pure control turn without making it a search.
# Deliberately small: anything outside this set means the turn also carries
# content and must not be treated as an ordering-only command.
_CONTROL_TURN_FILLER = frozenset({
    "ahora", "entonces", "luego", "por", "favor", "me", "nos", "mi",
    "los", "las", "el", "la", "lo", "solo", "solamente", "todo", "todos",
    "dejame", "dejamelos", "dejalos", "ponlos", "pon", "pongamelos",
    "muestrame", "muestra", "dame", "quiero", "ver", "veamos", "listame",
    "contratos", "contrato", "procesos", "proceso", "resultados",
    "de", "del", "y", "que", "en", "primero", "primeros",
})


def is_pure_control_turn(text: str) -> bool:
    """True when the turn asks only for a different ordering of the same set.

    Used by the follow-up layer to keep a control-only turn from being read as
    a new topic. It requires a real ordering criterion — a bare imperative with
    no criterion is not enough — and nothing but filler left over.
    """
    request = interpret_control_language(text or "")
    if not request.has_ordering:
        return False
    leftovers = [t for t in re.split(r"[^0-9a-záéíóúüñ]+", request.residual_text.lower()) if t]
    return all(token in _CONTROL_TURN_FILLER for token in leftovers)
