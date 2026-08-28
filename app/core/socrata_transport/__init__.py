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
from app.core.socrata_transport.factory import (
    DEFAULT_TRANSPORT_KIND,
    TRANSPORT_ENV_VAR,
    TransportKind,
    build_transport,
    parse_transport_kind,
    transport_kind_from_settings,
)

__all__ = [
    "DEFAULT_ORDERING_SPECIFIER",
    "DEFAULT_TRANSPORT_KIND",
    "TRANSPORT_ENV_VAR",
    "TRANSPORT_CLASSIC",
    "TRANSPORT_SODA3",
    "ClassicSocrataTransport",
    "Soda3OrderingSpecifier",
    "SocrataTransport",
    "TransportKind",
    "build_transport",
    "parse_transport_kind",
    "transport_kind_from_settings",
]
