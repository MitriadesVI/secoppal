"""A qué HECHO ancla su periodo cada fuente que la persona nombró.

El sistema tiene UNA autoridad de rango (``temporal_semantics`` →
``fecha_desde``/``fecha_hasta``) y UNA de estado actual (``estado_families``).
Ninguna dice a qué hecho se refiere el rango, y sin eso dos preguntas distintas
producen los mismos parámetros.

V1 resolvió eso con **un escalar y una precedencia fija**, y la revisión
independiente demostró que no alcanza:

    "procesos publicados y contratos firmados en agosto"
    "contratos firmados y procesos publicados en agosto"

Ambos órdenes daban `event=CONTRACT_SIGNATURE` y perdían la publicación —la
firma ganaba por prioridad, no por posición—, así que no era un problema de
orden textual sino de que un escalar no puede representar dos hechos.

    "contratos terminados en agosto y actualmente activos"

`_CURRENT_MARKER` se buscaba en toda la frase: el `actualmente` de la SEGUNDA
cláusula desactivaba el `terminados` de la PRIMERA, y la consulta se ejecutaba
por fecha de firma sin decir jamás que se había pedido terminación.

Aquí un evento es una **reclamación por ocurrencia**: qué hecho, en qué tramo,
sobre qué fuente, y gobernada o no por un marcador de actualidad **de su propia
cláusula**. Un hecho sin fecha productiva acreditada bloquea la petición
completa aunque otra cláusula sí sea representable: contestar sólo la mitad
representable es contestar otra pregunta.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re

from app.core.entity_profile.clauses import Clause, split_clauses
from app.core.estado_families import detect_estado_family
from app.core.query_text_consumption import mask_consumed_spans
from app.utils.money import normalize_text


class TemporalEvent(str, Enum):
    NONE = "NONE"
    PROCESS_PUBLICATION = "PROCESS_PUBLICATION"
    CONTRACT_SIGNATURE = "CONTRACT_SIGNATURE"


_PUBLICATION = re.compile(
    r"\b(?:publicad[oa]s?|public[oó]|publicaron|publicar)\b"
)
_SIGNATURE = re.compile(
    r"\b(?:firmad[oa]s?|firm[oó]|firmaron"
    r"|celebrad[oa]s?|celebr[oó]|celebraron"
    r"|suscrit[oa]s?|suscribi[oó]|suscribieron)\b"
)

# Hechos del ciclo de vida que la persona nombra y SECOP II no publica como
# fecha propia acreditada en estos dos datasets. No se traducen a firma ni a
# publicación: son preguntas distintas, y contestarlas con otra fecha es
# inventar. ADR-056/O1A dejaron establecido que sólo hay fecha productiva para
# publicación (procesos) y firma (contratos).
_UNSUPPORTED_EVENTS = re.compile(
    r"\b(?:terminad[oa]s?|termin[oó]|terminaron"
    r"|finalizad[oa]s?|finaliz[oó]|finalizaron"
    r"|liquidad[oa]s?|liquid[oó]|liquidaron"
    r"|cerrad[oa]s?|cerr[oó]|cerraron"
    r"|iniciad[oa]s?|inici[oó]|iniciaron"
    r"|adjudicad[oa]s?|adjudic[oó]|adjudicaron"
    r"|cedid[oa]s?|prorrogad[oa]s?|suspendid[oa]s?)\b"
)

# "actualmente terminados" no pide una fecha de terminación: pide el estado de
# hoy. El adverbio desactiva la lectura de evento SOBRE SU PROPIA CLÁUSULA.
_CURRENT_MARKER = re.compile(
    r"\b(?:actualmente|hoy|ahora|todavia|aun|vigentes?|en\s+curso|siguen?)\b"
)

_SUPPORTED = (
    (_PUBLICATION, TemporalEvent.PROCESS_PUBLICATION),
    (_SIGNATURE, TemporalEvent.CONTRACT_SIGNATURE),
)


@dataclass(frozen=True, slots=True)
class TemporalEventClaim:
    """Un hecho nombrado, con el tramo que lo nombra."""

    event: TemporalEvent
    span: tuple[int, int]
    term: str

    def to_dict(self) -> dict:
        return {"event": self.event.value, "span": list(self.span), "term": self.term}


@dataclass(frozen=True, slots=True)
class TemporalResolution:
    """Todo lo temporal que la petición nombró, sin colapsar nada."""

    claims: tuple[TemporalEventClaim, ...] = ()
    unsupported_terms: tuple[str, ...] = ()
    current_state_spans: tuple[tuple[int, int], ...] = ()

    @property
    def is_unsupported(self) -> bool:
        return bool(self.unsupported_terms)

    @property
    def unsupported_term(self) -> str | None:
        return self.unsupported_terms[0] if self.unsupported_terms else None

    @property
    def events(self) -> frozenset[TemporalEvent]:
        return frozenset(claim.event for claim in self.claims)

    def event_for(self, event: TemporalEvent) -> TemporalEventClaim | None:
        for claim in self.claims:
            if claim.event is event:
                return claim
        return None

    @property
    def claimed_spans(self) -> tuple[tuple[int, int], ...]:
        return tuple(claim.span for claim in self.claims)


def _has_time_range(params: dict) -> bool:
    return bool(params.get("fecha_desde") or params.get("fecha_hasta"))


def _governed_by_current_marker(clause: Clause, span: tuple[int, int]) -> bool:
    """¿Un marcador de actualidad de ESTA cláusula gobierna esta ocurrencia?"""
    del span  # el gobierno es de cláusula: basta con que el marcador viva aquí.
    return bool(_CURRENT_MARKER.search(clause.text))


def resolve_temporal_events(user_query: str, params: dict) -> TemporalResolution:
    """Reclamaciones temporales por ocurrencia; un evento exige un periodo."""
    if not _has_time_range(params):
        return TemporalResolution()

    text = normalize_text(user_query)
    clauses = split_clauses(text)
    claims: list[TemporalEventClaim] = []
    unsupported: list[str] = []
    current_spans: list[tuple[int, int]] = []

    for clause in clauses:
        governed = bool(_CURRENT_MARKER.search(clause.text))
        offset = clause.start
        for pattern, event in _SUPPORTED:
            for match in pattern.finditer(clause.text):
                span = (offset + match.start(), offset + match.end())
                if governed:
                    # "actualmente publicados": el tramo describe el estado de
                    # hoy, no un hecho fechado.
                    current_spans.append(span)
                    continue
                if event not in {claim.event for claim in claims}:
                    claims.append(TemporalEventClaim(event, span, match.group(0)))
        for match in _UNSUPPORTED_EVENTS.finditer(clause.text):
            if governed:
                current_spans.append(
                    (offset + match.start(), offset + match.end()),
                )
                continue
            unsupported.append(match.group(0))

    return TemporalResolution(
        claims=tuple(claims),
        unsupported_terms=tuple(dict.fromkeys(unsupported)),
        current_state_spans=tuple(current_spans),
    )


def current_state_survives_events(
    user_query: str, resolution: TemporalResolution, dataset: str,
) -> bool:
    """¿Queda evidencia de estado ACTUAL fuera de los tramos reclamados?

    Relee la autoridad de estado sobre el texto con los tramos de evento
    enmascarados. Si todavía resuelve una familia, la persona nombró el estado
    con otra palabra —"publicados en agosto y todavía abiertos"— y ese filtro
    es suyo, no un eco del evento.

    Un tramo gobernado por un marcador de actualidad NUNCA se enmascara: es
    precisamente la evidencia de estado que debe sobrevivir.
    """
    if not resolution.claims:
        return True
    text = normalize_text(user_query)
    masked = mask_consumed_spans(text, list(resolution.claimed_spans))
    return detect_estado_family(masked, dataset) is not None


def is_consumed_temporal_term(term: str) -> bool:
    """¿Este término ya lo reclamó una dimensión de tiempo, evento o vigencia?

    ``INV-STRUCTURED-DIMENSION-NOT-TOPIC``: un tramo consumido como dimensión
    estructurada no puede además filtrarse como tema. El vocabulario es
    exactamente el que este módulo usa para decidir; por eso los sustantivos
    homónimos ("firma", "firma digital", "publicación de avisos") quedan fuera
    por construcción.
    """
    folded = normalize_text(term)
    if not folded:
        return False
    return any(
        pattern.fullmatch(folded)
        for pattern in (
            _PUBLICATION, _SIGNATURE, _UNSUPPORTED_EVENTS, _CURRENT_MARKER,
        )
    )
