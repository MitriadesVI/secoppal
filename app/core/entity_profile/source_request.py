"""Qué FUENTE nombró el usuario, que no es lo mismo que qué dataset ejecutar.

``params["dataset"]`` responde otra pregunta: cuál es el dataset escalar que la
ruta tabular debe ejecutar, y lo decide ``QueryRouter._select_dataset`` con una
sopa de señales sin orden. Esa autoridad se conserva intacta y sigue siendo la
única para su pregunta.

Ésta responde la del producto: **qué pidió la persona**. Son distintas de forma
demostrable. Medido sobre `main` 7868664:

    "analiza procesos de alcaldia de baranoa con proveedores"
      -> dataset = contratos, dataset_explicit = True

La palabra `proveedores` es señal de contratos y gana por estar en la lista, no
por posición ni por jerarquía; la palabra explícita del usuario —`procesos`— ya
se perdió, y `dataset_explicit` afirma que fue explícita. Por eso
``dataset_explicit`` es señal auxiliar y nunca autoridad de fuente.

La ausencia de una palabra de fuente NO significa procesos: significa que el
usuario no eligió, y una radiografía institucional sin fuente elegida cubre las
dos.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from app.core.query_text_consumption import mask_consumed_spans
from app.utils.money import normalize_text


class RequestedSources(str, Enum):
    PROCESSES = "PROCESSES"
    CONTRACTS = "CONTRACTS"
    BOTH = "BOTH"


class SourceProvenance(str, Enum):
    """De dónde salió la fuente: sólo EXPLICIT es palabra del usuario."""

    EXPLICIT = "EXPLICIT"
    INHERITED = "INHERITED"
    DEFAULT = "DEFAULT"


@dataclass(frozen=True, slots=True)
class SourceRequest:
    sources: RequestedSources
    provenance: SourceProvenance

    @property
    def wants_processes(self) -> bool:
        return self.sources in (RequestedSources.PROCESSES, RequestedSources.BOTH)

    @property
    def wants_contracts(self) -> bool:
        return self.sources in (RequestedSources.CONTRACTS, RequestedSources.BOTH)


def _buyer_surfaces(params: dict) -> tuple[str, ...]:
    """Las formas del comprador que pueden aparecer literalmente en el texto.

    ``entidad_resolution`` sólo se lee si acredita el comprador: su ``metadata``
    conserva la superficie que el usuario escribió, que es la única que puede
    coincidir con el texto cuando la canónica difiere.
    """
    surfaces = [params.get("entidad_resolved"), params.get("entidad")]
    resolution = params.get("entidad_resolution")
    if isinstance(resolution, dict):
        metadata = resolution.get("metadata")
        if isinstance(metadata, dict):
            surfaces.append(metadata.get("rewritten_from"))
    return tuple(
        surface for surface in surfaces
        if isinstance(surface, str) and surface.strip()
    )


def _buyer_free_text(user_query: str, params: dict) -> str:
    """El texto sin el nombre del comprador.

    Un comprador puede llamarse "Agencia Nacional de Contratación Pública" o
    "Departamento Administrativo de Procesos"; esa aparición de la palabra no es
    una elección de fuente del usuario. Se enmascara por cadena y no por offset
    porque el riesgo aquí es asimétrico: enmascarar de más pierde una elección
    de fuente y cae al default BOTH, que es conservador y no oculta nada;
    enmascarar de menos inventaría una elección que el usuario no hizo.
    """
    text = normalize_text(user_query)
    spans: list[tuple[int, int]] = []
    for surface in _buyer_surfaces(params):
        needle = normalize_text(surface)
        start = text.find(needle)
        if start >= 0:
            spans.append((start, start + len(needle)))
    return mask_consumed_spans(text, spans) if spans else text


def _request_sources(text: str) -> tuple[bool, bool]:
    """Las ocurrencias que la autoridad compartida acredita como fuente.

    F3: buscar la palabra en toda la frase hacía que el TEMA eligiera fuente.

        "contratos de gestion de procesos industriales"  -> BOTH (mal)
        "procesos de administracion de contratos"        -> BOTH (mal)
        "contratos de publicacion de convocatorias"      -> BOTH (mal)

    En las tres, la segunda aparición está dentro de un complemento del tema y
    no es una elección de fuente. Una coordinación REAL —"procesos y contratos",
    "procesos publicados y contratos firmados"— sí pone una palabra de fuente al
    frente de cada cláusula, y por eso el núcleo de cada una es exactamente la
    señal que hay que leer. Es estructura posicional, no un catálogo nuevo ni
    inferencia probabilística.
    """
    # Local import avoids the package ``__init__`` cycle while keeping this
    # adapter on the shared occurrence authority.
    from app.core.source_occurrence import source_request_occurrences

    occurrences = source_request_occurrences(text)
    return (
        any(occurrence.is_process for occurrence in occurrences),
        any(not occurrence.is_process for occurrence in occurrences),
    )


def resolve_requested_sources(user_query: str, params: dict) -> SourceRequest:
    """Lee la fuente de las palabras del usuario; sólo si calla mira el frame."""
    text = _buyer_free_text(user_query, params)
    asked_processes, asked_contracts = _request_sources(text)
    if asked_processes and asked_contracts:
        return SourceRequest(RequestedSources.BOTH, SourceProvenance.EXPLICIT)
    if asked_processes:
        return SourceRequest(RequestedSources.PROCESSES, SourceProvenance.EXPLICIT)
    if asked_contracts:
        return SourceRequest(RequestedSources.CONTRACTS, SourceProvenance.EXPLICIT)

    scope = params.get("dataset_scope")
    if isinstance(scope, (list, tuple)) and len(set(scope)) >= 2:
        return SourceRequest(RequestedSources.BOTH, SourceProvenance.INHERITED)

    inherited = params.get("_entity_profile_sources")
    if isinstance(inherited, str):
        try:
            return SourceRequest(
                RequestedSources(inherited), SourceProvenance.INHERITED,
            )
        except ValueError:
            pass
    return SourceRequest(RequestedSources.BOTH, SourceProvenance.DEFAULT)
