"""Política de detección de consultas sobre un ACTOR contractual — AP2-B.

Qué decide este módulo, y qué NO decide
---------------------------------------
Decide **si el usuario está preguntando por un proveedor/contratista**, y
—cuando hay un documento— construye la referencia tipada de ese actor
reusando ``actor_identity``. No decide quién es un nombre: eso es
resolución de identidad y pertenece a AP3.

Esa separación es el eje del módulo::

    detección de intención de actor  ≠  resolución de identidad de actor

Detectar que "contratos del proveedor FUNDACARIBE" pregunta por un actor
es barato y seguro. Afirmar que FUNDACARIBE *es* el NIT 900.123.456 sin
consultar un índice de actores es inventar una atribución sobre un
tercero. Por eso el camino de nombre termina en
``resolution_method="name_unresolved"``: la consulta queda clasificada,
la identidad queda declarada como pendiente, y el perfil no se ejecuta.

Precision-first, y por qué
--------------------------
El patrón genérico ``contratos de X`` es ambiguo por construcción: X es
tanto un actor ("contratos de FUNDACARIBE") como un tema ("contratos de
alimentación"). Sin un índice de actores no hay forma honesta de
separarlos, así que **este módulo no lo intenta**. Solo clasifica como
actor cuando el texto trae evidencia lingüística fuerte —un marcador de
rol (``proveedor``/``contratista``/``la empresa``) o una construcción
verbal inequívoca (``qué ha contratado X``)—.

El costo de equivocarse es asimétrico y eso fija la política: un tema mal
clasificado como actor rompe una búsqueda que hoy funciona, mientras que
un actor no clasificado simplemente sigue el camino de siempre. De ahí
INV-ACTOR-PRECISION: una señal débil de actor nunca secuestra una
consulta temática.

Rol explícito
-------------
La referencia lleva ``role="contractor"`` en vez de un booleano
``actor=True``. Hoy solo existe ese rol, pero un proponente que no ganó,
un representante legal y un integrante de una unión temporal son roles
distintos sobre el mismo actor, y un booleano no podría distinguirlos
después sin reescribir a todos los consumidores.

Frontera
--------
Puro: sin red, sin base de datos, sin reloj, sin ``Settings``. Depende
solo de ``actor_identity`` (que a su vez no depende de nada de ``app``).
No importa ``query_router``: es el router quien llama aquí, y la
dependencia inversa crearía un ciclo.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable, Literal, Mapping

from app.core.actor_identity import IdentityMatch, normalize_identity

__all__ = [
    "REASON_DOCUMENT_TYPE_AMBIGUOUS",
    "REASON_NAME_NEEDS_RESOLUTION",
    "REASON_NAME_REJECTED_BUYER_MARKER",
    "REASON_ROLE_MARKER",
    "REASON_VERB_MARKER",
    "ActorReference",
    "ActorRole",
    "ActorResolution",
    "detect_asserted_actor_name_before_document",
    "detect_actor_name",
    "document_type_hint",
    "recognized_actor_modifier_tails",
    "reference_from_document",
    "strip_document_type_marker",
    "strip_recognized_actor_modifier_tail",
    "strong_actor_modifier_suffix",
    "strong_actor_name_prefixes",
]

ActorRole = Literal["contractor"]

# Cómo se llegó a esta referencia. NO es "cuán seguros estamos del
# número": eso vive en ``IdentityMatch.confidence``.
ActorResolution = Literal["explicit_document", "name_unresolved"]

REASON_ROLE_MARKER = "ACTOR_ROLE_MARKER"
REASON_VERB_MARKER = "ACTOR_VERB_MARKER"
REASON_NAME_NEEDS_RESOLUTION = "ACTOR_NAME_NEEDS_RESOLUTION"
REASON_NAME_REJECTED_BUYER_MARKER = "ACTOR_NAME_REJECTED_BUYER_MARKER"
REASON_DOCUMENT_TYPE_AMBIGUOUS = "ACTOR_DOCUMENT_TYPE_AMBIGUOUS"


# ---------------------------------------------------------------------------
# Léxico.
# ---------------------------------------------------------------------------

# Declaran el TIPO documental. Son las únicas señales admitidas: la
# longitud del número jamás decide (INV-NO-LENGTH, congelado en AP1).
_NIT_MARKER_RE = re.compile(r"\bnit\b", re.IGNORECASE)
_CEDULA_MARKER_RE = re.compile(
    r"(?:\bc[eé]dulas?\b|(?<!\w)c\.c\.?(?!\w)|\bc\s+c\b|\bcc\b|"
    r"\bpersona\s+natural\b|\bdocumento\s+de\s+identidad\b)",
    re.IGNORECASE,
)
_DOCUMENT_MARKER_SUFFIX_RE = re.compile(
    r"\s+(?:con\s+)?(?:(?:el|la)\s+)?(?:nit|c[eé]dulas?|c\.c\.|cc|"
    r"c\s+c|documento(?:\s+de\s+identidad)?)\s*$",
    re.IGNORECASE,
)

# Marcadores de ROL: la palabra dice que lo que sigue es quien contrata.
# "persona" y "empresa" solo cuentan con determinante ("la empresa X"),
# porque sueltas aparecen en objetos contractuales ("dotación de personal").
_ROLE_MARKERS = (
    "proveedor adjudicado", "proveedor", "contratista", "adjudicatario",
)
_DETERMINED_ROLE_MARKERS = (
    "empresa", "firma", "persona", "sociedad", "corporacion", "fundacion",
)

# Construcciones VERBALES inequívocas: el sujeto de la acción de contratar
# es el actor. Cubren la lista de D.1 del encargo.
_VERB_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bqu[eé]\s+contratos?\s+(?:tiene|ha\s+tenido|posee)\b", re.IGNORECASE),
    re.compile(r"\bqu[eé]\s+ha\s+(?:hecho|contratado|celebrado|suscrito|ejecutado)\b", re.IGNORECASE),
    re.compile(r"\bhistorial\s+contractual\s+(?:de|del)\b", re.IGNORECASE),
    re.compile(r"\bcontrataci[oó]n\s+d[eu][el]?\s+l[ao]s?\s+(?:empresa|persona|firma)\b", re.IGNORECASE),
    re.compile(r"\bcontratos?\s+adjudicados?\s+a\b", re.IGNORECASE),
    re.compile(r"\badjudicados?\s+a\b", re.IGNORECASE),
    re.compile(r"\bcontratos?\s+celebrados?\s+con\b", re.IGNORECASE),
    re.compile(r"\bcontratado\s+por\b", re.IGNORECASE),
    re.compile(r"\blo\s+contratado\s+por\b", re.IGNORECASE),
)

# Un nombre que empieza por artículo o preposición no es un nombre: es la
# continuación de un sintagma ("proveedor de alimentos", "contratista de
# la obra"). Rechazarlos es lo que impide que un tema entre por la puerta
# del actor.
_LEADING_STOPWORDS = frozenset({
    "de", "del", "la", "el", "los", "las", "un", "una", "unos", "unas",
    "en", "para", "por", "con", "que", "al", "a", "y", "o", "sobre",
})

_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")

_DATE_MODIFIER_RE = re.compile(
    r"\b(?:(?:en|durante|desde|hasta|entre|de)\s+(?:el\s+)?(?:19|20)\d{2}"
    r"(?:\s+(?:a|y|hasta)\s+(?:19|20)\d{2})?|este\s+a[nñ]o)\b",
    re.IGNORECASE,
)
_ORDER_MODIFIER_RE = re.compile(
    r"\b(?:por\s+)?(?:"
    r"mayor(?:es)?\s+(?:valor|cuantia)|menor(?:es)?\s+(?:valor|cuantia)|"
    r"mas\s+(?:caros?|costosos?|grandes?|recientes?)|"
    r"ordenar\s+por\s+(?:valor|monto|fecha)|"
    r"de\s+mayor\s+a\s+menor|ultimos?|recientes?"
    r")\b",
    re.IGNORECASE,
)
_AMOUNT_MODIFIER_RE = re.compile(
    r"\b(?:por|de|desde|hasta|entre|mayor(?:es)?\s+(?:a|de)|"
    r"menor(?:es)?\s+(?:a|de)|mas\s+de|menos\s+de)\s+"
    r"\$?\s*\d[\d.,]*(?:\s+(?:cop|pesos?|millones?|mil|smmlv|smlmv|smlv))?\b",
    re.IGNORECASE,
)
_BOUNDARY_PREPOSITION_RE = re.compile(
    r"\b(?:en|por|desde|hasta|entre|durante|con|para|sobre|de|del)\s*$",
    re.IGNORECASE,
)
_BOUNDARY_PUNCTUATION_RE = re.compile(r"[^\w\s]", re.UNICODE)


# ---------------------------------------------------------------------------
# ActorReference — la referencia transitoria de un actor.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ActorReference:
    """A quién se refiere la consulta, y por qué camino se supo.

    Deliberadamente **transitoria**: vive lo que dura el turno. No hay
    ``actor_id`` persistente, ni tabla de actores, ni UUID — eso exigiría
    decidir hoy la identidad de millones de actores con la evidencia de
    una sola fuente.

    ``identity`` es el ``IdentityMatch`` de AP1 cuando hubo un documento,
    y ``None`` cuando solo hubo un nombre. Esa asimetría es el contenido
    de la clase: representa el estado *"vi este nombre, todavía no sé de
    quién es"*, que es el más frecuente y el que un modelo con solo llave
    no puede expresar.
    """

    resolution_method: ActorResolution
    identity: IdentityMatch | None = None
    observed_name: str | None = None
    role: ActorRole = "contractor"
    consumed_spans: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()

    @property
    def requires_resolution(self) -> bool:
        """True si falta saber a qué documento corresponde este actor."""
        return self.identity is None or not self.identity.is_resolved

    @property
    def document_variants(self) -> tuple[str, ...]:
        """Formas del documento tal como la fuente puede haberlas publicado.

        El contrato de fuente (AP2-A) encontró las tres conviviendo en
        ``documento_proveedor``: el base solo (90% de los NIT), el base con
        DV pegado, y el base con DV separado por guion. Un filtro de
        igualdad contra una sola forma pierde las otras dos en silencio.
        """
        if self.identity is None or self.identity.base_number is None:
            return ()
        base = self.identity.base_number
        if not self.identity.check_digit:
            return (base,)
        return (base, f"{base}{self.identity.check_digit}", f"{base}-{self.identity.check_digit}")

    def to_params(self) -> dict:
        """Representación **plana y serializable** para ``params``/``QueryFrame``.

        Un dataclass dentro del frame se rompería al serializarse a JSON en
        Radar y en el store de conversación. Lo que viaja es este dict de
        escalares; el tipo rico se reconstruye donde haga falta.
        """
        payload: dict[str, object] = {
            "role": self.role,
            "resolution_method": self.resolution_method,
            "requires_resolution": self.requires_resolution,
        }
        if self.observed_name:
            payload["observed_name"] = self.observed_name
        if self.identity is not None:
            payload.update({
                "document_type": self.identity.document_type,
                "base_number": self.identity.base_number,
                "check_digit": self.identity.check_digit,
                "raw_value": self.identity.raw_value,
                "identity_confidence": self.identity.confidence,
                "identity_reason_codes": list(self.identity.reason_codes),
            })
        if self.reason_codes:
            payload["reason_codes"] = list(self.reason_codes)
        return payload


# ---------------------------------------------------------------------------
# Camino 1 — documento explícito.
# ---------------------------------------------------------------------------


def document_type_hint(normalized_query: str) -> str | None:
    """Tipo documental DECLARADO por el texto, o ``None`` si no lo declara.

    Solo el léxico decide: ``nit`` → NIT, ``cédula``/``c.c.`` → CC. Si la
    consulta trae ambos marcadores el tipo queda sin declarar, porque
    elegir uno sería inventar la mitad de la evidencia.

    Nótese lo que NO declara tipo: la palabra ``persona`` suelta. Una
    persona jurídica también es una persona, así que leerla como cédula
    atribuiría la actividad de una empresa a un ciudadano.
    """
    has_nit = bool(_NIT_MARKER_RE.search(normalized_query))
    has_cedula = bool(_CEDULA_MARKER_RE.search(normalized_query))
    if has_nit and has_cedula:
        return None
    if has_nit:
        return "NIT"
    if has_cedula:
        return "CC"
    return None


def reference_from_document(
    raw_document: str, normalized_query: str, *, source: str | None = None,
) -> ActorReference | None:
    """Construye la referencia de un actor a partir de un documento crudo.

    Un documento explícito **es** evidencia de que se pregunta por un
    actor: nadie escribe un NIT para hablar de un tema. Por eso este
    camino no exige marcador de rol — a diferencia del camino de nombre,
    donde el número no está y la ambigüedad es total.

    Devuelve ``None`` cuando el crudo no produce una identidad utilizable
    (centinela, fuera de rango, sin dígitos): fallar declarando "no sé" es
    preferible a fabricar un actor.
    """
    identity = normalize_identity(
        raw_document, document_type=document_type_hint(normalized_query), source=source,
    )
    if not identity.is_resolved:
        return None

    reasons: list[str] = []
    if _NIT_MARKER_RE.search(normalized_query) and _CEDULA_MARKER_RE.search(normalized_query):
        reasons.append(REASON_DOCUMENT_TYPE_AMBIGUOUS)

    return ActorReference(
        resolution_method="explicit_document",
        identity=identity,
        consumed_spans=_document_context_spans(normalized_query),
        reason_codes=tuple(reasons),
    )


def _document_context_spans(normalized_query: str) -> tuple[str, ...]:
    """Palabras que acompañan al documento y no deben caer en ``objeto``.

    ``"contratos de la persona con cédula 79482763"`` dejaba ``persona`` y
    ``cedula`` como términos de búsqueda textual sobre el objeto
    contractual. No son el tema del contrato: son la forma de nombrar al
    actor.
    """
    spans: list[str] = []
    for pattern in (_NIT_MARKER_RE, _CEDULA_MARKER_RE):
        spans.extend(match.group(0) for match in pattern.finditer(normalized_query))
    for marker in _ROLE_MARKERS + _DETERMINED_ROLE_MARKERS:
        for match in re.finditer(rf"\b{re.escape(marker)}\b", normalized_query, re.IGNORECASE):
            spans.append(match.group(0))
    for pattern in _VERB_PATTERNS:
        spans.extend(match.group(0) for match in pattern.finditer(normalized_query))
    # Más largo primero: quitar "persona" antes que "contratación de la
    # persona" dejaría el verbo huérfano convertido en término de búsqueda.
    return tuple(sorted(dict.fromkeys(spans), key=len, reverse=True))


# ---------------------------------------------------------------------------
# Camino 2 — nombre con contexto fuerte, SIN resolver.
# ---------------------------------------------------------------------------


def _boundary_normalize(value: object) -> str:
    decomposed = unicodedata.normalize("NFKD", str(value or "").casefold())
    without_accents = "".join(
        character for character in decomposed
        if not unicodedata.combining(character)
    )
    without_punctuation = _BOUNDARY_PUNCTUATION_RE.sub(" ", without_accents)
    return " ".join(without_punctuation.split())


def _append_span(
    spans: list[tuple[int, int]],
    query: str,
    start: int,
    end: int,
) -> None:
    """Record a modifier span, including its immediately preceding preposition."""
    prefix = query[:start]
    preposition = _BOUNDARY_PREPOSITION_RE.search(prefix)
    if preposition is not None:
        start = preposition.start()
    spans.append((start, end))


def recognized_actor_modifier_tails(
    normalized_query: str,
    params: Mapping[str, object],
    *,
    extra_consumed_spans: Iterable[str] = (),
) -> tuple[str, ...]:
    """Return suffixes independently recognized as supported query modifiers.

    A preposition is never evidence by itself.  It becomes a name boundary
    only when its following segment was independently classified as date,
    ordering, geography, amount, modality/state or another consumed modifier.
    Consecutive modifiers are combined into one tail, e.g. ``en 2025 por mayor
    valor``.
    """
    query = _boundary_normalize(normalized_query)
    spans: list[tuple[int, int]] = []

    if params.get("fecha_desde") or params.get("fecha_hasta"):
        spans.extend(match.span() for match in _DATE_MODIFIER_RE.finditer(query))
    if params.get("ordering_signal"):
        spans.extend(match.span() for match in _ORDER_MODIFIER_RE.finditer(query))
    if params.get("valor_min") is not None or params.get("valor_max") is not None:
        for match in _AMOUNT_MODIFIER_RE.finditer(query):
            _append_span(spans, query, match.start(), match.end())

    geography_values: list[object] = []
    for key in (
        "ciudad", "ciudad_resolved", "departamento", "departamento_resolved", "region",
    ):
        if params.get(key):
            geography_values.append(params[key])
    for value in geography_values:
        geography = _boundary_normalize(value)
        if not geography:
            continue
        pattern = re.compile(
            rf"\b(?:en|de|para)\s+(?:(?:el|la)\s+)?{re.escape(geography)}\b",
            re.IGNORECASE,
        )
        spans.extend(match.span() for match in pattern.finditer(query))

    for raw_span in extra_consumed_spans:
        consumed = _boundary_normalize(raw_span)
        if not consumed:
            continue
        for match in re.finditer(rf"\b{re.escape(consumed)}\b", query):
            _append_span(spans, query, match.start(), match.end())

    if not spans:
        return ()

    # A valid tail is a suffix fully covered by one or more recognized spans;
    # whitespace between adjacent modifiers is harmless, substantive text is not.
    tails: set[str] = set()
    query_end = len(query)
    for candidate_start in sorted({start for start, _ in spans}):
        covered = [False] * (query_end - candidate_start)
        for start, end in spans:
            if start < candidate_start:
                continue
            for index in range(start - candidate_start, min(end, query_end) - candidate_start):
                covered[index] = True
        remainder = query[candidate_start:]
        if all(
            is_covered or character.isspace()
            for character, is_covered in zip(remainder, covered)
        ):
            tails.add(remainder.strip())
    return tuple(sorted(tails, key=len, reverse=True))


def strip_recognized_actor_modifier_tail(
    candidate: str,
    recognized_modifier_tails: Iterable[str],
) -> str:
    """Cut only a suffix backed by independent modifier recognition."""
    normalized = _boundary_normalize(candidate)
    for tail in sorted(
        {_boundary_normalize(value) for value in recognized_modifier_tails if value},
        key=len,
        reverse=True,
    ):
        if normalized.endswith(f" {tail}"):
            return normalized[: -(len(tail) + 1)].strip()
    return normalized


def _clean_candidate_name(
    raw: str,
    recognized_modifier_tails: Iterable[str] = (),
    *,
    max_words: int | None = None,
) -> str | None:
    """Recorta el candidato y lo rechaza si no puede ser un nombre.

    Rechaza lo que empieza por artículo o preposición, porque eso indica
    que se capturó la continuación de un sintagma ("proveedor **de**
    alimentos") y no un nombre propio. También rechaza números y años:
    esos entran por el camino del documento o por el de fechas.
    """
    candidate = strip_recognized_actor_modifier_tail(
        raw.strip(" ,.;:¿?¡!\"'()"),
        recognized_modifier_tails,
    )
    if not candidate:
        return None
    words = candidate.split()
    if not words or words[0].lower() in _LEADING_STOPWORDS:
        return None
    if _YEAR_RE.fullmatch(candidate) or candidate.replace(".", "").isdigit():
        return None
    if len(candidate) > 512:
        return None
    # Los marcadores fuertes admiten razones sociales largas; el resolver aún
    # debe aportar autoridad exacta o limitarse a sugerencias.
    if max_words is not None and len(words) > max_words:
        return None
    return candidate


def _name_after_role_marker(
    normalized_query: str,
    recognized_modifier_tails: Iterable[str] = (),
    *,
    max_words: int | None = None,
) -> tuple[str, str] | None:
    """``(nombre, span_consumido)`` tras un marcador de rol, o ``None``."""
    for marker in _ROLE_MARKERS:
        pattern = re.compile(rf"\b{re.escape(marker)}\s+(.+)$", re.IGNORECASE)
        match = pattern.search(normalized_query)
        if match:
            name = _clean_candidate_name(
                match.group(1), recognized_modifier_tails, max_words=max_words,
            )
            if name:
                return name, f"{match.group(0)[:match.start(1) - match.start(0)]}{name}"
    for marker in _DETERMINED_ROLE_MARKERS:
        # Con determinante obligatorio: "la empresa X", nunca "empresa" suelta.
        pattern = re.compile(
            rf"\b(?:la|el|los|las|una|un|esta|este|estas|estos)\s+"
            rf"{re.escape(marker)}\s+(.+)$", re.IGNORECASE,
        )
        match = pattern.search(normalized_query)
        if match:
            name = _clean_candidate_name(
                match.group(1), recognized_modifier_tails, max_words=max_words,
            )
            if name:
                return name, f"{match.group(0)[:match.start(1) - match.start(0)]}{name}"
    return None


def _name_after_verb_marker(
    normalized_query: str,
    recognized_modifier_tails: Iterable[str] = (),
    *,
    max_words: int | None = None,
) -> tuple[str, str] | None:
    for pattern in _VERB_PATTERNS:
        match = pattern.search(normalized_query)
        if match is None:
            continue
        tail = normalized_query[match.end():]
        name = _clean_candidate_name(
            tail, recognized_modifier_tails, max_words=max_words,
        )
        if name:
            return name, f"{match.group(0)}{tail[:len(tail) - len(tail.lstrip())]}{name}"
    return None


def strip_document_type_marker(prefix: str) -> str | None:
    """The text before a document, minus the words that declared its type.

    ``None`` when no type marker precedes the number.  An untyped digit run is
    not a document the user declared, so whatever precedes it is not actor
    context either — that reading belongs to the amount and date grammars.
    """
    stripped = _DOCUMENT_MARKER_SUFFIX_RE.sub("", prefix).strip()
    if not stripped or stripped == prefix.strip():
        return None
    return stripped


def detect_asserted_actor_name_before_document(
    normalized_query: str,
    document_start: int,
) -> ActorReference | None:
    """Return a strong user-supplied name immediately before a document.

    The authoritative identity remains the document.  This helper only claims
    and consumes the contextual name span early enough that buyer, geography,
    and topic extractors cannot reinterpret its fragments as independent
    contract filters.  The document provides a hard right boundary, so this
    path intentionally has no historical eight-word limit.
    """
    actor_context = strip_document_type_marker(normalized_query[:document_start])
    if actor_context is None:
        return None

    name: str | None = None
    span: str | None = None
    marker_reason = REASON_ROLE_MARKER
    # Preserve the exact surface span for consumption.  The general name
    # detector returns an N2-like boundary string, which intentionally drops
    # punctuation; using that string to scrub ``S.A.S.`` or ``&`` would fail.
    for marker in _ROLE_MARKERS:
        match = re.search(
            rf"\b{re.escape(marker)}\s+(.+)$", actor_context, re.IGNORECASE,
        )
        if match is not None:
            name = _clean_candidate_name(match.group(1), max_words=None)
            if name:
                span = match.group(0)
                break
    if name is None:
        for marker in _DETERMINED_ROLE_MARKERS:
            match = re.search(
                rf"\b(?:la|el|los|las|una|un|esta|este|estas|estos)\s+"
                rf"{re.escape(marker)}\s+(.+)$",
                actor_context,
                re.IGNORECASE,
            )
            if match is not None:
                name = _clean_candidate_name(match.group(1), max_words=None)
                if name:
                    span = match.group(0)
                    break
    if name is None:
        marker_reason = REASON_VERB_MARKER
        for pattern in _VERB_PATTERNS:
            match = pattern.search(actor_context)
            if match is None:
                continue
            name = _clean_candidate_name(
                actor_context[match.end():], max_words=None,
            )
            if name:
                span = actor_context[match.start():]
                break
    if name is None or span is None:
        return None

    return ActorReference(
        resolution_method="name_unresolved",
        identity=None,
        observed_name=name,
        consumed_spans=(span,),
        reason_codes=(marker_reason, REASON_NAME_NEEDS_RESOLUTION),
    )


def detect_actor_name(
    normalized_query: str,
    *,
    recognized_modifier_tails: Iterable[str] = (),
) -> ActorReference | None:
    """Detecta un actor nombrado, **sin resolver su identidad**.

    Exige contexto lingüístico fuerte. ``"contratos de FUNDACARIBE"`` no
    califica a propósito: la interpretación del nombre desnudo necesita
    consultar un índice de actores y pertenece a AP3.

    Devuelve una referencia con ``identity=None`` y
    ``requires_resolution=True``. Quien la reciba debe declarar que falta
    el documento — jamás degradar el nombre a búsqueda textual sobre el
    objeto contractual, que es el doble error que AP0 documentó: se
    pierden los contratos del actor cuyo objeto no lo nombra, y se le
    atribuyen los de terceros que sí lo mencionan.
    """
    found = _name_after_role_marker(normalized_query, recognized_modifier_tails)
    marker_reason = REASON_ROLE_MARKER
    if found is None:
        found = _name_after_verb_marker(normalized_query, recognized_modifier_tails)
        marker_reason = REASON_VERB_MARKER
    if found is None:
        return None

    name, span = found
    return ActorReference(
        resolution_method="name_unresolved",
        identity=None,
        observed_name=name,
        consumed_spans=(span,),
        reason_codes=(marker_reason, REASON_NAME_NEEDS_RESOLUTION),
    )


def _strong_actor_tail(normalized_query: str) -> str | None:
    query = _boundary_normalize(normalized_query)
    tail: str | None = None
    for marker in _ROLE_MARKERS:
        match = re.search(rf"\b{re.escape(marker)}\s+(.+)$", query, re.IGNORECASE)
        if match is not None:
            tail = match.group(1)
            break
    if tail is None:
        for marker in _DETERMINED_ROLE_MARKERS:
            match = re.search(
                rf"\b(?:la|el|los|las|una|un)\s+{re.escape(marker)}\s+(.+)$",
                query,
                re.IGNORECASE,
            )
            if match is not None:
                tail = match.group(1)
                break
    if tail is None:
        for pattern in _VERB_PATTERNS:
            match = pattern.search(query)
            if match is not None:
                tail = query[match.end():].strip()
                break
    if not tail:
        return None
    return tail


def strong_actor_name_prefixes(
    normalized_query: str,
    *,
    minimum_name: str,
) -> tuple[str, ...]:
    """Longest-first boundary candidates from independently strong syntax.

    This function does not decide that any prefix is an actor.  The caller may
    grant boundary authority only when the exact alias index confirms one and
    any remainder is independently parsed as a supported modifier; fuzzy
    retrieval must never choose a prefix or consume that remainder.
    """
    tail = _strong_actor_tail(normalized_query)
    if tail is None:
        return ()

    minimum = _boundary_normalize(minimum_name)
    if not minimum or not (tail == minimum or tail.startswith(f"{minimum} ")):
        return ()
    words = tail.split()
    return tuple(
        " ".join(words[:size])
        for size in range(len(words), 0, -1)
    )


def strong_actor_modifier_suffix(
    normalized_query: str,
    actor_name: str,
) -> str | None:
    """Text after an exact actor prefix, or ``None`` if it is not a prefix."""
    tail = _strong_actor_tail(normalized_query)
    name = _boundary_normalize(actor_name)
    if tail is None or not name:
        return None
    if tail == name:
        return ""
    if tail.startswith(f"{name} "):
        return tail[len(name) + 1:]
    return None
