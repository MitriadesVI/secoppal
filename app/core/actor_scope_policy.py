"""Capacidad de dataset para consultas de actor — INV-NO-SILENT-DROP.

El defecto que este módulo cierra
---------------------------------
``soql_builder._build_where`` aplica el filtro de contratista así::

    if params.get("contratista") and spec.contractor:

En el dataset de procesos ``spec.contractor is None``, así que la condición
es falsa y **el filtro simplemente no se añade**. La consulta se ejecuta con
lo que quedó. AP0 lo verificó emitiendo el SoQL: ``"qué ha celebrado la
persona 1140835135 en 2025"`` producía una búsqueda de la palabra
``persona`` en 2025, y esa lista se presentaba como el historial de esa
persona.

Por qué el degradado silencioso es la peor de las opciones: no falla, no
avisa, y devuelve una respuesta con la forma correcta. Es indistinguible de
un acierto para quien pregunta, y lo que afirma es falso sobre un tercero
identificable.

La regla
--------
Cuando la consulta trae alcance de actor y el dataset objetivo no puede
proyectarlo, **no se ejecuta**: se pide la aclaración que corresponde. El
actor nunca se descarta para seguir adelante con los filtros restantes.

Antes de llegar aquí el router ya resolvió el caso barato: si el usuario no
pidió procesos explícitamente, un documento de actor promueve la consulta a
contratos, que sí publica quién contrata. Este módulo gobierna lo que queda
—el usuario pidió procesos a sabiendas— y ahí su intención manda: se le dice
que esa combinación no se puede servir, en vez de responderle otra cosa.

Capacidad, no lista
-------------------
Qué dataset soporta actor se pregunta al ``DatasetSpec``, no a una lista
escrita a mano. Si mañana la fuente de procesos publicara el documento del
proveedor, la política se entera sola; una lista habría que acordarse de
actualizarla.

Puro y determinista: sin red, sin reloj, sin ``Settings``.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.actor_identity import IdentityKey
from app.core.actor_name import (
    ActorResolutionStatus,
    format_actor_identity_for_user,
)
from app.core.actor_resolution import (
    METHOD_NAME_UNRESOLVED,
    actor_resolution_method,
    actor_resolution_status,
    is_authoritative_actor_resolution,
)
from app.core.soql_builder import SoQLBuilder

__all__ = [
    "REASON_DATASET_UNSUPPORTED",
    "REASON_INDEX_UNAVAILABLE",
    "REASON_INTENT_CONFLICT",
    "REASON_NAME_AMBIGUOUS",
    "REASON_NAME_CANDIDATES",
    "REASON_NAME_UNRESOLVED",
    "ActorScopeDecision",
    "dataset_supports_actor",
    "evaluate_actor_scope",
    "has_actor_scope",
]

REASON_DATASET_UNSUPPORTED = "actor_dataset_unsupported"
REASON_NAME_UNRESOLVED = "actor_name_requires_resolution"
REASON_NAME_AMBIGUOUS = "actor_name_ambiguous"
REASON_NAME_CANDIDATES = "actor_name_candidates"
REASON_INDEX_UNAVAILABLE = "actor_index_unavailable"
REASON_INTENT_CONFLICT = "actor_or_topic_ambiguous"

_DATASET_LABELS = {"procesos": "procesos", "contratos": "contratos"}


@dataclass(frozen=True)
class ActorScopeDecision:
    """Resultado de ``evaluate_actor_scope``. Nunca muta los params."""

    should_execute: bool
    missing_slots: tuple[str, ...] = ()
    risk_flags: tuple[str, ...] = ()
    clarification_question: str | None = None
    reason: str = ""


def has_actor_scope(params: dict) -> bool:
    """¿La consulta está acotada a un actor?

    Cuenta tanto el documento resuelto como el nombre pendiente de
    resolver: en ambos casos el usuario preguntó por alguien concreto, y
    ejecutar sin ese alcance respondería otra pregunta.
    """
    return bool(
        actor_resolution_method(params)
        or actor_resolution_status(params)
        or params.get("actor_scope")
        or params.get("actor_pending")
        or params.get("actor_intent_conflict")
        or _ambiguous_relation_buyer(params)
    )


def dataset_supports_actor(dataset: str | None) -> bool:
    """¿Ese dataset publica el documento del proveedor?

    Se responde desde el ``DatasetSpec``, que es lo que el builder usa de
    verdad para decidir si añade el filtro.
    """
    spec = SoQLBuilder.SPECS.get(SoQLBuilder.dataset_id_for(dataset))
    return bool(spec and spec.contractor_document)


def evaluate_actor_scope(params: dict) -> ActorScopeDecision:
    """Decide si una consulta con alcance de actor puede ejecutarse.

    Devuelve ``should_execute=True`` —sin más— cuando no hay actor: esta
    política no opina sobre el resto de las consultas.
    """
    if not has_actor_scope(params):
        return ActorScopeDecision(should_execute=True)

    if actor_resolution_status(params) == ActorResolutionStatus.INCONCLUSIVE:
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("actor_document",),
            risk_flags=("actor_prefix_check_inconclusive",),
            clarification_question=(
                "Encontré una coincidencia exacta, pero no pude comprobar si hay "
                "otras cédulas con nombres que la prolongan. Escribe un nombre "
                "más completo o comparte la cédula para continuar."
            ),
            reason="actor_prefix_check_inconclusive",
        )

    conflict = params.get("actor_intent_conflict")
    if isinstance(conflict, dict):
        name = str(conflict.get("actor_name") or "ese término").upper()
        if conflict.get("kind") == "entity_like_no_exact":
            return ActorScopeDecision(
                should_execute=False,
                missing_slots=("actor_or_topic", "actor_document"),
                risk_flags=("unresolved_entity_like_name",),
                clarification_question=(
                    f"¿Te refieres a {name} como tema del contrato o al nombre "
                    "de un proveedor? No encontré una coincidencia exacta y no "
                    "voy a convertirlo en una búsqueda temática distinta. Si es "
                    "el proveedor, compárteme su NIT o cédula."
                ),
                reason=REASON_INTENT_CONFLICT,
            )
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("actor_or_topic",),
            risk_flags=("actor_topic_conflict",),
            clarification_question=(
                f"¿Te refieres a contratos sobre {name} como tema, o al proveedor "
                f"observado como ‘{name}’? No voy a elegir una interpretación "
                "silenciosamente."
            ),
            reason=REASON_INTENT_CONFLICT,
        )

    status = actor_resolution_status(params)
    if status in {ActorResolutionStatus.AMBIGUOUS, ActorResolutionStatus.CANDIDATES}:
        name = _observed_name(params)
        choices = _candidate_choices(params)
        if status == ActorResolutionStatus.AMBIGUOUS:
            intro = f"El nombre {name} corresponde a más de un actor observado en SECOP."
            if actor_resolution_method(params) == "exact_prefix_ambiguous":
                intro = (
                    f"Encontré una coincidencia exacta para {name}, pero también nombres "
                    "observados más largos que comienzan exactamente por ese nombre "
                    "y corresponden a otras cédulas."
                )
            reason = REASON_NAME_AMBIGUOUS
        else:
            intro = f"No encontré una coincidencia exacta para {name}; sí nombres similares."
            reason = REASON_NAME_CANDIDATES
        question = intro
        if choices:
            question += "\n\n" + "\n".join(choices)
            question += "\n\nResponde con el número correcto o comparte el NIT/cédula."
        else:
            question += " Necesito el NIT o la cédula para continuar."
        pending = params.get("actor_pending") or {}
        if pending.get("candidates_has_more") is True:
            question += "\n\nHay más opciones compatibles; escribe un nombre más completo o comparte la cédula."
        elif pending.get("candidates_has_more") is None and pending.get("enumeration_incomplete"):
            question += "\n\nNo pude completar la lista de opciones; escribe un nombre más completo o comparte la cédula."
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("actor_selection",),
            risk_flags=("actor_requires_selection",),
            clarification_question=question,
            reason=reason,
        )

    if status == ActorResolutionStatus.INDEX_UNAVAILABLE:
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("actor_document",),
            risk_flags=("actor_index_unavailable",),
            clarification_question=(
                "No tengo disponible el índice local para distinguir ese nombre. "
                "Compárteme el NIT o la cédula y continúo sin adivinar."
            ),
            reason=REASON_INDEX_UNAVAILABLE,
        )

    if (
        status == ActorResolutionStatus.UNRESOLVED
        or actor_resolution_method(params) == METHOD_NAME_UNRESOLVED
    ):
        name = _observed_name(params)
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("actor_document",),
            risk_flags=("actor_requires_resolution",),
            clarification_question=(
                f"Para darte la huella contractual de {name} necesito su NIT o "
                "su cédula. Con el nombre solo no puedo distinguirlo de otros "
                "proveedores de nombre parecido, y atribuirle contratos ajenos "
                "sería peor que no responder."
                if name else
                "Necesito el NIT o la cédula del proveedor para consultar su "
                "actividad contractual."
            ),
            reason=REASON_NAME_UNRESOLVED,
        )

    buyer_resolution = _ambiguous_relation_buyer(params)
    if buyer_resolution is not None:
        hint = buyer_resolution.get("clarification_hint")
        mention = str(
            buyer_resolution.get("metadata", {}).get("buyer_mention")
            if isinstance(buyer_resolution.get("metadata"), dict)
            else "la entidad"
        )
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("buyer_entity",),
            risk_flags=("buyer_relation_ambiguous",),
            clarification_question=(
                str(hint) if hint else
                f"¿A cuál {mention} te refieres? Indica la ciudad o el "
                "departamento; no voy a escoger una entidad al azar."
            ),
            reason="actor_relation_buyer_ambiguous",
        )

    if not is_authoritative_actor_resolution(params):
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("authoritative_actor",),
            risk_flags=("actor_not_authoritative",),
            clarification_question=(
                "Necesito confirmar el NIT, la cédula o una de las opciones antes "
                "de atribuir contratos a ese actor."
            ),
            reason=REASON_NAME_UNRESOLVED,
        )

    dataset = params.get("dataset")
    if not dataset_supports_actor(dataset):
        label = _DATASET_LABELS.get(str(dataset), str(dataset))
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("actor_supported_dataset",),
            risk_flags=("actor_dataset_unsupported",),
            clarification_question=(
                f"El conjunto de {label} no publica quién contrata, así que ahí "
                "no puedo filtrar por ese NIT o cédula. ¿Busco su actividad en "
                "los contratos?"
            ),
            reason=REASON_DATASET_UNSUPPORTED,
        )

    return ActorScopeDecision(should_execute=True)


def _ambiguous_relation_buyer(params: dict) -> dict | None:
    resolution = params.get("entidad_resolution")
    if not isinstance(resolution, dict):
        return None
    if resolution.get("method") != "actor_relation_buyer_ambiguous":
        return None
    return resolution


def _observed_name(params: dict) -> str:
    scope = params.get("actor_scope")
    if isinstance(scope, dict):
        name = scope.get("observed_name")
        if isinstance(name, str) and name.strip():
            return name.strip().upper()
    pending = params.get("actor_pending")
    if isinstance(pending, dict):
        name = pending.get("actor_name")
        if isinstance(name, str) and name.strip():
            return name.strip().upper()
    return "INDICADO"


def _candidate_choices(params: dict) -> list[str]:
    pending = params.get("actor_pending")
    if not isinstance(pending, dict):
        return []
    candidates = pending.get("candidates")
    if not isinstance(candidates, list):
        return []
    choices: list[str] = []
    for candidate in candidates[:10]:
        if not isinstance(candidate, dict):
            continue
        try:
            identity = IdentityKey(
                str(candidate["document_type"]),  # type: ignore[arg-type]
                str(candidate["base_number"]),
            )
        except (KeyError, TypeError, ValueError):
            continue
        rank = candidate.get("rank") or len(choices) + 1
        observed = str(candidate.get("observed_name") or "Nombre observado").upper()
        choices.append(
            f"{rank}. {observed} — {format_actor_identity_for_user(identity)}"
        )
    return choices
