"""Errores tipados del transporte SODA3.

No es un framework de errores: es lo mínimo para que un fallo HTTP **no**
llegue al workflow disfrazado de lista vacía ni de "error genérico". El
workflow ya distingue un timeout del resto (``_is_timeout_exception`` recorre
la cadena ``__cause__``/``__context__``), y de esa distinción depende que el
fallback H4 se arme o no:

- un **timeout** es recuperable → abre el ``except`` que arma H4;
- un **400 de SoQL**, un **429** o un **401** no lo son → camino honesto sin
  fallback, que es lo que ya hace ``execution.py`` para el clásico.

Por eso ``Soda3TimeoutError`` **es** un ``TimeoutError`` de verdad, y ninguna
otra categoría menciona la palabra «timeout» en su mensaje: la detección del
workflow es por tipo y por texto, y confundir un 429 con un timeout dispararía
un fallback que no corresponde.

Este módulo NO cambia la política de retry: ``SecopClient.max_retries=1``
sigue significando un intento total.
"""
from __future__ import annotations


class TransportErrorCategory:
    """Categorías del contrato (INV-SODA-05 del plan de deuda técnica)."""

    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    BAD_QUERY = "bad_query"
    AUTHENTICATION = "authentication"
    NOT_FOUND = "not_found"
    TRANSIENT_SERVER = "transient_server"
    PERMANENT_SERVER = "permanent_server"
    NETWORK = "network"
    DECODE = "decode"
    CONTRACT_VIOLATION = "contract_violation"


class TransportError(Exception):
    """Fallo del transporte con categoría estable.

    La causa original se preserva siempre por ``raise ... from exc``; el
    mensaje no incluye el SoQL ni el payload (ni el App Token).
    """

    #: Sobrescrito por las subclases.
    category = TransportErrorCategory.NETWORK

    def __init__(
        self,
        message: str,
        *,
        dataset_id: str | None = None,
        http_status: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.dataset_id = dataset_id
        self.http_status = http_status
        self.retryable = retryable


class Soda3TimeoutError(TransportError, TimeoutError):
    """El presupuesto de la llamada se agotó.

    Hereda de ``TimeoutError`` a propósito: es lo que hace que el workflow lo
    reconozca como recuperable a través de la cadena de excepciones, igual que
    reconoce hoy el timeout del cliente clásico.
    """

    category = TransportErrorCategory.TIMEOUT


class Soda3NetworkError(TransportError):
    category = TransportErrorCategory.NETWORK


class Soda3RateLimitError(TransportError):
    category = TransportErrorCategory.RATE_LIMIT


class Soda3BadQueryError(TransportError):
    category = TransportErrorCategory.BAD_QUERY


class Soda3AuthenticationError(TransportError):
    category = TransportErrorCategory.AUTHENTICATION


class Soda3NotFoundError(TransportError):
    category = TransportErrorCategory.NOT_FOUND


class Soda3TransientServerError(TransportError):
    category = TransportErrorCategory.TRANSIENT_SERVER


class Soda3PermanentServerError(TransportError):
    category = TransportErrorCategory.PERMANENT_SERVER


class Soda3DecodeError(TransportError):
    category = TransportErrorCategory.DECODE


class Soda3ContractViolationError(TransportError):
    category = TransportErrorCategory.CONTRACT_VIOLATION


def classify_http_status(status: int) -> type[TransportError]:
    """Mapea un status HTTP a su categoría. Ningún 4xx/5xx es un timeout."""
    if status == 400:
        return Soda3BadQueryError
    if status in (401, 403):
        return Soda3AuthenticationError
    if status == 404:
        return Soda3NotFoundError
    if status == 429:
        return Soda3RateLimitError
    if status in (500, 502, 503, 504):
        return Soda3TransientServerError
    if status >= 500:
        return Soda3PermanentServerError
    return Soda3PermanentServerError
