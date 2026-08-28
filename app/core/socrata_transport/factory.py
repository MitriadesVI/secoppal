"""Selección del transporte — fail closed, default SODA3.

Una configuración inválida no selecciona ningún transporte por accidente:
revienta al construir el workflow con un mensaje que dice qué valor llegó y
cuáles son los válidos. El clásico queda disponible como rollback explícito.
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from app.core.socrata_transport.base import (
    TRANSPORT_CLASSIC,
    TRANSPORT_SODA3,
    SocrataTransport,
)
from app.core.socrata_transport.classic import ClassicSocrataTransport


class TransportKind(str, Enum):
    """Transportes seleccionables en V1.

    NO existe un modo shadow ni un modo con fallback automático SODA3→clásico:
    eso exigiría una política observada y reason codes propios, y este bloque
    no los tiene. Son dos transportes seleccionables, no una cascada.
    """

    CLASSIC = TRANSPORT_CLASSIC
    SODA3 = TRANSPORT_SODA3


#: Ausencia de configuración → SODA3; ``classic`` queda como rollback explícito.
DEFAULT_TRANSPORT_KIND = TransportKind.SODA3

#: Variable de entorno que gobierna la selección.
TRANSPORT_ENV_VAR = "SECOPPAL_SOCRATA_TRANSPORT"


def parse_transport_kind(value: Any) -> TransportKind:
    """Normaliza el valor configurado. Valor desconocido → error explícito."""
    if isinstance(value, TransportKind):
        return value
    text = str(value or "").strip().lower()
    try:
        return TransportKind(text)
    except ValueError:
        valid = ", ".join(kind.value for kind in TransportKind)
        raise ValueError(
            f"{TRANSPORT_ENV_VAR}={value!r} is not a valid Socrata transport. "
            f"Valid values: {valid}."
        ) from None


def transport_kind_from_settings(settings: Any) -> TransportKind:
    """Lee el flag desde ``Settings``. Ausente → SODA3."""
    return parse_transport_kind(
        getattr(settings, "secoppal_socrata_transport", DEFAULT_TRANSPORT_KIND.value)
    )


def build_transport(
    kind: TransportKind | str,
    *,
    domain: str,
    app_token: str | None,
    timeout_seconds: int,
    classic_client: Any | None = None,
) -> SocrataTransport:
    """Construye el transporte pedido.

    ``timeout_seconds`` solo se usa para el clásico, donde el timeout está
    horneado en el objeto ``Socrata``. SODA3 lo recibe por llamada.
    """
    resolved = parse_transport_kind(kind)
    if resolved is TransportKind.SODA3:
        from app.core.socrata_transport.soda3 import Soda3HttpTransport

        return Soda3HttpTransport(domain=domain, app_token=app_token)

    if classic_client is None:
        from sodapy import Socrata

        classic_client = Socrata(domain, app_token=app_token, timeout=timeout_seconds)
    return ClassicSocrataTransport(classic_client)
