"""Contrato de transporte Socrata — la costura entre ``SecopClient`` y la red.

Un transporte hace **una** cosa: emitir el SoQL que recibe y devolver las filas
crudas que el backend responde. No construye SoQL, no lo reescribe, no lo
normaliza y no interpreta el resultado: la normalización de filas SECOP sigue
siendo autoridad exclusiva de ``SecopClient._normalize_row`` y la lectura del
entero de un ``count`` sigue siendo de ``SecopClient.count``.

Dos implementaciones conviven:

    SecopClient
        ↓
    SocrataTransport
        ├── classic  (sodapy — GET /resource/{dataset}.json)
        └── soda3    (POST /api/v3/views/{dataset}/query.json)

El default visible es ``classic``. SODA3 es un candidato apagado.
"""
from __future__ import annotations

from enum import Enum
from typing import Protocol


TRANSPORT_CLASSIC = "classic"
TRANSPORT_SODA3 = "soda3"


class Soda3OrderingSpecifier(str, Enum):
    """Política de ordenamiento declarada al backend SODA3.

    ``TOTAL`` es la política que el discovery midió y adjudicó para **toda**
    operación normal (``query`` ordenada, ``count`` y ``aggregate``).

    ``NO_PRESERVE`` es la única excepción adjudicada
    (``docs/audits/2026-08-27_SODA3_QUERY_FINAL_ADJUDICATION.md`` §7): el
    fallback H4, la ruta donde el runtime retira **deliberadamente** el
    ``ORDER BY`` para abaratar la consulta. Exigirle un orden total a esa
    consulta contradice el propósito de la ruta, y el microprobe causal lo
    midió: ``total`` 0/5 contra ``no_preserve`` 5/5 sobre el mismo SoQL.

    La señal viaja **desde la ruta de ejecución**. Queda prohibido inferirla
    del texto del SoQL: la ausencia de ``ORDER BY`` también aparece de forma
    legítima en ``count``, ``aggregate``, direct lookup y otras operaciones que
    no son H4.
    """

    TOTAL = "total"
    NO_PRESERVE = "no_preserve"


#: Política por defecto de toda operación que no declare otra cosa.
DEFAULT_ORDERING_SPECIFIER = Soda3OrderingSpecifier.TOTAL


class SocrataTransport(Protocol):
    """Interfaz mínima: las tres formas remotas que producción usa hoy.

    ``/export`` **no** forma parte de la interfaz V1: el discovery no lo probó
    y el veredicto ``GO_INTEGRATION`` no lo autoriza.

    Las tres devuelven ``list[dict]`` **cruda**. ``count`` no devuelve un
    entero a propósito: interpretar la fila agregada es semántica, y la
    semántica no vive en el transporte.
    """

    #: Nombre estable del transporte (``classic`` / ``soda3``).
    name: str

    def query(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float,
        ordering_specifier: Soda3OrderingSpecifier,
    ) -> list[dict]:
        ...

    def count(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float,
        ordering_specifier: Soda3OrderingSpecifier,
    ) -> list[dict]:
        ...

    def aggregate(
        self,
        dataset_id: str,
        soql: str,
        *,
        timeout_seconds: float,
        ordering_specifier: Soda3OrderingSpecifier,
    ) -> list[dict]:
        ...
