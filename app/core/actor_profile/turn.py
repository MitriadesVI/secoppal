"""Punto de entrada del perfil en un turno — espejo de ``analytics/registry``.

Decide si ESTE turno es una consulta de huella contractual y, si lo es,
devuelve la respuesta ya construida. Devuelve ``None`` cuando no aplica,
para que el flujo tabular siga su camino sin enterarse.

Precedencia frente a la capa analítica
--------------------------------------
El perfil corre DESPUÉS de los analyzers, a propósito. "Cuánto suma lo
contratado por el NIT X" es una pregunta concreta con respuesta concreta;
contestarla con el perfil entero sería responder de más. El perfil atiende
la pregunta abierta —"qué ha contratado X"—, que es la que hoy no tiene
respuesta correcta.

Qué NO llega hasta aquí
-----------------------
Las consultas de actor sobre un dataset que no lo soporta, y los nombres
sin resolver: P17 las bloqueó antes con una aclaración explícita. Este
módulo asume lo que esa política ya garantizó y no lo revalida por su
cuenta — dos guardas que pueden divergir son peores que una.
"""
from __future__ import annotations

import logging
from typing import Any

from app.config import actor_profile_enabled
from app.core.actor_profile.assembler import build_actor_profile
from app.core.actor_profile.render import render_actor_profile
from app.core.actor_scope_policy import dataset_supports_actor

logger = logging.getLogger(__name__)

__all__ = ["is_actor_profile_turn", "maybe_handle_actor_profile"]

ROUTE_REASON = "actor_profile"


def is_actor_profile_turn(params: dict) -> bool:
    """¿Este turno pide la huella contractual de un actor identificado?

    Exige identidad por DOCUMENTO. Un nombre sin resolver nunca ejecuta un
    perfil: atribuirle contratos a partir de un parecido textual es
    precisamente la falsa atribución que AP2 existe para cerrar.
    """
    if not actor_profile_enabled():
        return False
    if params.get("actor_resolution") != "explicit_document":
        return False
    if not params.get("contratista"):
        return False
    if _asks_for_a_listing(params):
        return False
    return dataset_supports_actor(params.get("dataset"))


_TOPIC_KEYS = ("objeto", "objeto_or", "objeto_phrases")


def _asks_for_a_listing(params: dict) -> bool:
    """¿El turno pide ver contratos, en vez de preguntar por el actor?

    Dos señales, y las dos dicen lo mismo: el actor es un FILTRO, no el
    sujeto de la pregunta.

    Un TEMA — "contratos de alimentación del NIT X" pide esa lista, y
    contestarla con la huella completa respondería de más.

    Un ORDEN explícito — "los de mayor valor" pide ver los contratos
    grandes de ese actor. El perfil muestra agregados y una muestra
    reciente, así que devolverlo aquí no respondería nada.

    En ambos casos el camino tabular sirve la consulta —ahora con el filtro
    exacto de documento que AP2-B corrigió— y el perfil se reserva para la
    pregunta abierta, que es la que hoy no tiene respuesta correcta.
    """
    if params.get("ordering_signal"):
        return True
    return any(params.get(key) not in (None, "", [], {}, ()) for key in _TOPIC_KEYS)


def maybe_handle_actor_profile(
    params: dict,
    dataset_id: str,
    secop_client: Any,
    soql_builder: Any,
) -> dict | None:
    """Construye y renderiza el perfil, o ``None`` si el turno no es uno.

    Nunca propaga una excepción: si el ensamblado falla por completo, el
    turno vuelve al camino tabular, que ya sabe filtrar por documento. Es
    preferible una respuesta más pobre que ninguna.
    """
    if not is_actor_profile_turn(params):
        return None

    try:
        profile = build_actor_profile(params, dataset_id, secop_client, soql_builder)
    except Exception as exc:  # pragma: no cover - el ensamblador ya absorbe por aspecto
        logger.warning("actor profile assembly failed, falling back to search: %s", exc)
        return None

    return {
        "response": render_actor_profile(profile),
        "route_reason": ROUTE_REASON,
        "dataset_id": dataset_id,
        "total_count": profile.contract_count or 0,
        "partial": profile.partial,
        "profile": profile,
    }
