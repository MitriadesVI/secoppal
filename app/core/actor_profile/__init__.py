"""Perfil contractual de un actor (AP2).

Superficie pública del paquete. La huella se sirve EN VIVO desde SECOP II
contratos: el warehouse local no contiene contratos ni ningún campo de
proveedor, así que esperarlo sería esperar otro proyecto.

Fuera de alcance por decisión, no por falta de tiempo: RUP, RUES, SIRI,
responsabilidad fiscal, uniones temporales, representante legal, índice de
nombres y radar por actor. Ninguna de esas fuentes tiene contrato
verificado, y en este dominio equivocarse significa afirmar que un
ciudadano tiene antecedentes que no tiene.
"""
from __future__ import annotations

from app.core.actor_profile.assembler import build_actor_profile
from app.core.actor_profile.models import (
    ActorProfileResult,
    AspectStatus,
    ObservedName,
)

__all__ = [
    "ActorProfileResult",
    "AspectStatus",
    "ObservedName",
    "build_actor_profile",
]
