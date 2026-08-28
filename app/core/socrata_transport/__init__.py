"""Transporte Socrata reversible: clásico (default) y SODA3 (candidato apagado)."""
from __future__ import annotations

from app.core.socrata_transport.base import (
    DEFAULT_ORDERING_SPECIFIER,
    TRANSPORT_CLASSIC,
    TRANSPORT_SODA3,
    Soda3OrderingSpecifier,
    SocrataTransport,
)
from app.core.socrata_transport.classic import ClassicSocrataTransport

__all__ = [
    "DEFAULT_ORDERING_SPECIFIER",
    "TRANSPORT_CLASSIC",
    "TRANSPORT_SODA3",
    "ClassicSocrataTransport",
    "Soda3OrderingSpecifier",
    "SocrataTransport",
]
