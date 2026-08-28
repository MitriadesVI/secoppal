"""Transporte clásico — el comportamiento sodapy existente, encapsulado.

Este adapter **no cambia nada**. Envuelve la única llamada que
``SecopClient`` hacía en línea:

    self.client.get(dataset_id, query=soql)

y la expone bajo el contrato de ``SocrataTransport``. El timeout sigue
horneado en el objeto ``Socrata`` (lo acota ``SecopClient.derived_for_timeout``
creando un cliente derivado por llamada, thread-safe); el
``ordering_specifier`` es un eje que la API clásica no tiene. Ambos se aceptan
en la firma para que la costura sea uniforme, y se ignoran aquí de forma
explícita y documentada — no silenciosa.
"""
from __future__ import annotations

from typing import Any

from app.core.socrata_transport.base import (
    TRANSPORT_CLASSIC,
    Soda3OrderingSpecifier,
)


class ClassicSocrataTransport:
    """Adapter sobre un cliente sodapy-like (``.get(dataset_id, query=...)``).

    Envoltura sin estado propio: ``SecopClient`` la construye alrededor de
    ``self.client`` en cada acceso, de modo que reasignar ese atributo —cosa
    que hacen los harnesses de auditoría para cortar la red— sigue surtiendo
    efecto inmediato.
    """

    name = TRANSPORT_CLASSIC

    def __init__(self, client: Any) -> None:
        self.raw_client = client

    # ── Las tres formas remotas ──────────────────────────────────────────
    # Idénticas entre sí en el transporte clásico: la diferencia entre
    # query/count/aggregate es semántica y vive en SecopClient. Se mantienen
    # separadas porque el contrato es el mismo para ambos transportes, y en
    # SODA3 cada una declara su propia política de ordenamiento.

    def query(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float | None = None,
        ordering_specifier: Soda3OrderingSpecifier | None = None,
    ) -> list[dict]:
        return self._get(dataset_id, soql)

    def count(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float | None = None,
        ordering_specifier: Soda3OrderingSpecifier | None = None,
    ) -> list[dict]:
        return self._get(dataset_id, soql)

    def aggregate(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float | None = None,
        ordering_specifier: Soda3OrderingSpecifier | None = None,
    ) -> list[dict]:
        return self._get(dataset_id, soql)

    def _get(self, dataset_id: str, soql: str) -> list[dict]:
        """La llamada sodapy exacta que SecopClient hacía antes de la costura.

        Devuelve lo que devuelve el cliente, sin envolver en ``list()`` ni
        copiar: ``SecopClient`` conserva el trato que ya le daba a cada forma
        (``list(...)`` en ``aggregate``, ``len(rows)`` en ``count``).
        """
        return self.raw_client.get(dataset_id, query=soql)
