"""secop_access.py — cómo llegar a la fuente oficial desde lo que se observó.

Una sola pregunta, una sola respuesta: dado lo que SECOP entregó de un proceso
(su ``urlproceso`` y su referencia), ¿qué vía a la fuente oficial se le puede
ofrecer a una persona? Nunca "¿está publicado?", nunca "¿puede ofertar?": esas
son del Truth Layer (``truth_layer/visibility_resolver.py``,
``opportunity_stage.py``) y responderlas aquí sería crear un segundo árbitro más
silencioso sobre el ciclo de vida.

Existe porque una oportunidad que el Radar detecta NUNCA debe dejar al usuario
sin camino a SECOP. Antes había dos salidas y ninguna era esa: o un enlace
directo, o una nota que además AFIRMABA una causa ("proceso pre-publicación")
que los datos no sostienen. Este módulo produce siempre una tercera cosa —la
búsqueda pública oficial— y deja la afirmación de etapa a quien tiene evidencia.

Tres reglas, y las tres son fail-closed:

1. **El enlace directo se observa, no se construye.** Nunca se arma una URL a
   partir de ``id_del_proceso`` ni de la referencia. Un identificador no es un
   enlace: la medición del 2026-08-31 sobre los 9.096.877 procesos de
   ``p6dx-8zbt`` (ver ``docs/audits/2026-08-31_CO1_REQ_PUBLIC_LINK_SPIKE.md``)
   encontró CERO filas cuya URL pública use el propio id del proceso como
   ``noticeUID``. Fabricarla produciría un enlace roto con apariencia de bueno.

2. **Un enlace directo tiene que ser de SECOP y tiene que ser una vista del
   proceso.** Se exige http(s) y host de SECOP —``javascript:`` y un host ajeno
   no pueden llegar a un ``link_button``—, y se rechaza la pantalla de
   autenticación y la de búsqueda: ambas viven en el host correcto y no muestran
   NINGÚN proceso. Presentarlas como "Abrir en SECOP" prometería una ficha que
   la página no tiene.

3. **Sin enlace directo siempre queda la búsqueda pública.** ``search_url`` no
   se inventó: es la URL que el propio SECOP publica en ``urlproceso`` para los
   contratos sin vista pública (4.130 filas de ``jbjy-vk9h`` al 2026-08-31). La
   referencia observada viaja aparte para que la vista la muestre y la persona
   pueda pegarla en el buscador; si el registro no la trae, la búsqueda se
   ofrece igual, sin fabricar un texto de búsqueda.

Lo que este módulo NO hace, a propósito: no consulta la red (``Guardar`` sigue
siendo local, R2), no lee el reloj, no clasifica etapas y no reescribe la URL
observada. Recibe hechos y devuelve la vía; el canal decide cómo pintarla.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

__all__ = [
    "SECOP_PUBLIC_SEARCH_URL",
    "SECOP_PUBLIC_HOSTS",
    "AccessKind",
    "SecopAccess",
    "observed_direct_url",
    "resolve_secop_access",
]

# Buscador público de SECOP II. No es una ruta deducida: es exactamente el valor
# que SECOP entrega en `urlproceso` cuando un registro no tiene vista pública
# propia. `Page=login` es parte del literal oficial y NO indica que la página
# exija sesión: es el buscador abierto de procesos y contratos.
SECOP_PUBLIC_SEARCH_URL = (
    "https://community.secop.gov.co/Public/Tendering/ContractNoticeManagement/"
    "Index?currentLanguage=es-CO&Page=login&Country=CO&SkinName=CCE"
)

# Hosts que pueden sostener un enlace directo de confianza. Es la misma lista
# que `direct_lookup` usa para leer un `noticeUID` de una URL pegada por el
# usuario: si un host no es fiable para LEER un id de él, tampoco lo es para
# MANDAR a alguien allí. Medido al 2026-08-31: cero filas de p6dx-8zbt y cero de
# jbjy-vk9h traen un `urlproceso` fuera de community.secop.gov.co, así que la
# lista no rechaza nada observado.
SECOP_PUBLIC_HOSTS = frozenset({"community.secop.gov.co", "www.secop.gov.co"})

_SUPPORTED_SCHEMES = frozenset({"http", "https"})

AccessKind = Literal["direct", "search_with_reference", "search"]


@dataclass(frozen=True, slots=True)
class SecopAccess:
    """La vía a SECOP que se le puede ofrecer a esta oportunidad.

    ``kind`` es lo único que la vista necesita ramificar:

    * ``direct``               — SECOP entregó una vista pública del proceso.
    * ``search_with_reference``— no hay vista propia, pero sí una referencia
      observada con la que buscar.
    * ``search``               — no hay ninguna de las dos; queda la entrada
      pública genérica, que es más que un mensaje de error.

    ``search_url`` está SIEMPRE poblada, incluso en ``direct``: es la misma
    entrada oficial, y tenerla disponible evita que cada superficie decida por
    su cuenta qué hacer cuando el enlace directo falla.
    """

    kind: AccessKind
    direct_url: str | None
    search_url: str
    reference: str | None

    @property
    def has_direct_link(self) -> bool:
        return self.direct_url is not None


def _text(value: object) -> str | None:
    """Texto no vacío, o ``None``. "" y "   " son el mismo hecho: no hay dato."""
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _unwrap(value: object) -> object:
    """Socrata devuelve las columnas de tipo ``url`` unas veces como escalar y
    otras como ``{"url": ...}``. Desenvolver esa forma es leer el mismo dato."""
    return value.get("url") if isinstance(value, Mapping) else value


def _is_process_view(url: str) -> bool:
    """¿Esta URL de SECOP muestra un proceso, o solo la puerta de entrada?

    Dos páginas reales viven en el host bueno y no muestran ningún proceso: la
    de autenticación (``/STS/Users/Login/Index``, 94.054 filas de p6dx-8zbt) y
    el buscador (``...?Page=login``, 4.130 filas de jbjy-vk9h). Se descartan por
    lo que son, no por una lista blanca de rutas de detalle: las rutas de ficha
    son varias (``OpportunityDetail``, ``NoticeDetail``, …) y enumerarlas haría
    que una ficha nueva de SECOP dejara de tener enlace.
    """
    partes = urlsplit(url)
    host = partes.hostname or ""
    if host.casefold().startswith("login."):
        return False
    if "/login" in partes.path.casefold():
        return False
    return "page=login" not in partes.query.casefold()


def observed_direct_url(value: object) -> str | None:
    """La vista pública que SECOP entregó, o ``None``. Nunca una construida.

    Un texto que ``urlsplit`` no puede leer (un puerto inválido, un IPv6 sin
    cerrar) es exactamente el caso que esta función existe para atrapar, así que
    se trata como ausencia y no se deja escapar la excepción: una URL heredada
    malformada no puede tumbar el render de una tarjeta.
    """
    url = _text(_unwrap(value))
    if url is None:
        return None
    try:
        partes = urlsplit(url)
        host = (partes.hostname or "").casefold()
        _ = partes.port  # se evalúa a propósito: un puerto ilegible lanza
    except ValueError:
        return None
    if partes.scheme.casefold() not in _SUPPORTED_SCHEMES:
        return None
    if host not in SECOP_PUBLIC_HOSTS:
        return None
    return url if _is_process_view(url) else None


def resolve_secop_access(
    observed_url: object = None, reference: object = None,
) -> SecopAccess:
    """La vía a SECOP de una oportunidad, a partir de lo que se observó de ella.

    Ambos argumentos son crudos a propósito (el escalar de Socrata, el dict, el
    ``None``): quien llama no tiene que saber en qué forma vino el dato, y así
    ninguna superficie inventa su propia limpieza.
    """
    directa = observed_direct_url(observed_url)
    referencia = _text(reference)
    if directa is not None:
        kind: AccessKind = "direct"
    elif referencia is not None:
        kind = "search_with_reference"
    else:
        kind = "search"
    return SecopAccess(
        kind=kind,
        direct_url=directa,
        search_url=SECOP_PUBLIC_SEARCH_URL,
        reference=referencia,
    )
