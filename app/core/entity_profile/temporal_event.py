"""Evento temporal: a qué hecho ancla el periodo que el usuario pidió.

Hoy el sistema tiene UNA autoridad de rango (``temporal_semantics`` →
``fecha_desde``/``fecha_hasta``) y UNA de estado actual (``estado_families``),
y ninguna que diga a qué HECHO se refiere el rango. Sin ella, dos preguntas
distintas producen los mismos parámetros. Medido sobre `main` 7868664:

    "analiza contratos firmados en agosto y actualmente terminados de X"
    "analiza contratos terminados en agosto de X"

    -> fecha_desde=2026-08-01, fecha_hasta=2026-08-31,
       estado_contrato=['terminado']   (idénticos)

La primera tiene respuesta —firmados en agosto, hoy terminados—; la segunda
pregunta por una fecha de terminación que SECOP no publica de forma acreditada,
y hoy se contesta como si preguntara por la de firma. Eso es peor que no
contestar: es contestar otra cosa en silencio.

Este módulo no reabre ninguna de las dos autoridades existentes. Decide una
cosa nueva y sólo una: **si el periodo del usuario está anclado a un evento
nombrado, y si ese evento tiene fecha productiva**.

Reclamar un tramo como evento tiene una consecuencia, por
``INV-STRUCTURED-DIMENSION-NOT-TOPIC`` extendida al estado: el mismo tramo no
puede además filtrar el estado ACTUAL. "Publicados en agosto" pregunta por lo
que se publicó en agosto, no por lo que sigue publicado hoy. Si el texto trae
OTRA evidencia de estado, ésa sobrevive: la comprobación se hace releyendo
``estado_families`` sobre el texto con el tramo del evento enmascarado.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re

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

# Hechos del ciclo de vida que el usuario nombra y SECOP II no publica como
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
# hoy. El adverbio desactiva la lectura de evento sobre ese participio.
_CURRENT_MARKER = re.compile(
    r"\b(?:actualmente|hoy|ahora|todavia|aun|vigentes?|en\s+curso|siguen?)\b"
)


@dataclass(frozen=True, slots=True)
class TemporalEventResolution:
    event: TemporalEvent
    unsupported_term: str | None = None
    claimed_span: tuple[int, int] | None = None

    @property
    def is_unsupported(self) -> bool:
        return self.unsupported_term is not None


def _has_time_range(params: dict) -> bool:
    return bool(params.get("fecha_desde") or params.get("fecha_hasta"))


def resolve_temporal_event(user_query: str, params: dict) -> TemporalEventResolution:
    """Un evento sólo existe cuando hay un periodo que anclar."""
    if not _has_time_range(params):
        return TemporalEventResolution(TemporalEvent.NONE)

    text = normalize_text(user_query)
    signature = _SIGNATURE.search(text)
    publication = _PUBLICATION.search(text)
    if signature:
        return TemporalEventResolution(
            TemporalEvent.CONTRACT_SIGNATURE, claimed_span=signature.span(),
        )
    if publication:
        return TemporalEventResolution(
            TemporalEvent.PROCESS_PUBLICATION, claimed_span=publication.span(),
        )

    unsupported = _UNSUPPORTED_EVENTS.search(text)
    if unsupported and not _CURRENT_MARKER.search(text):
        return TemporalEventResolution(
            TemporalEvent.NONE, unsupported_term=unsupported.group(0),
        )
    return TemporalEventResolution(TemporalEvent.NONE)


def current_state_survives_event(
    user_query: str, resolution: TemporalEventResolution, dataset: str,
) -> bool:
    """¿Queda evidencia de estado ACTUAL fuera del tramo que reclamó el evento?

    Relee la autoridad de estado sobre el texto con ese tramo enmascarado. Si
    todavía resuelve una familia, el usuario nombró el estado con otra palabra
    —"publicados en agosto y todavía abiertos"— y ese filtro es suyo, no un eco
    del evento.
    """
    if resolution.claimed_span is None:
        return True
    text = normalize_text(user_query)
    masked = mask_consumed_spans(text, [resolution.claimed_span])
    return detect_estado_family(masked, dataset) is not None


def is_consumed_temporal_term(term: str) -> bool:
    """¿Este término ya lo reclamó una dimensión de tiempo, evento o vigencia?

    ``INV-STRUCTURED-DIMENSION-NOT-TOPIC``: un tramo consumido como dimensión
    estructurada no puede además filtrarse como tema. Sin esta comprobación
    "analiza procesos publicados en agosto y todavía abiertos" consultaba
    ``LIKE '%todavia%'`` sobre el objeto del proceso, y "contratos finalizados
    en 2026" —que además se responde con una aclaración— guardaba
    ``finalizados`` como tema de la conversación.

    El vocabulario es exactamente el que este módulo ya usa para decidir; no se
    inventa uno nuevo, y por eso los sustantivos homónimos ("firma", "firma
    digital", "publicación de avisos") quedan fuera por construcción.
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
