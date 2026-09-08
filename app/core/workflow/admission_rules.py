"""Clasificar UN turno en UNA ruta, leyendo el contrato publicado.

Hoy la decisión está repartida: `_dispatch` prueba handlers en orden y el
primero que devuelve algo gana. Eso funciona mientras cada handler sepa
reconocer lo suyo mirando hacia atrás — y ahí está el defecto: «más» lo reclama
la paginación por la forma de la frase, no por ser dueña del resultado visible.

Aquí la pregunta se invierte. Primero se mira QUIÉN publicó el resultado que la
persona está viendo; después, qué clase de entrada es. Sólo se reclaman las
entradas que **no significan nada sin un referente**: paginación, un ordinal
suelto, un «sí», un chip de refinamiento. Todo lo demás se delega a la cadena
existente, que conserva sus autoridades, sus permisos y su validación.

Este módulo es PURO: no consulta fuentes, no toca disco y no llama handlers.
Devuelve una sola decisión, y el conjunto de reglas es total y disjunto —lo
comprueba `tests/test_entity_profile_frame_b_admission.py`.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
import unicodedata

from app.core.conversation_store import is_pagination_phrase, is_reset_command
from app.core.conversation_envelope import EnvelopeStatus
from app.core.entity_profile.turn import is_bare_affirmative
from app.core.workflow.continuation import ContinuationContract, ContinuationOwner


# La referencia que la interfaz adjunta a un refinamiento estructurado. Sin
# ella, un chip es un diccionario de params sin dueño acreditado.
CONTINUATION_REF_KEY = "_continuation_ref"

_ORDINAL = re.compile(
    r"^(?:la\s+)?(?:opcion\s+)?(?:\d{1,2}|primera|segunda|tercera)$",
)

# `is_pagination_phrase` exige una frase completa («muéstrame más») y deja fuera
# el «más» a secas, que en la ruta tabular cae al parser sin daño. Sobre un
# perfil sí importa: es la entrada del adversarial R7a, y contestarla con el
# fallback tabular —«necesito al menos un filtro», más tres chips de búsqueda—
# es exactamente el rescate que el diseño prohíbe. La admisión la reconoce como
# lo que es: una continuación que no significa nada sin su referente.
_BARE_MORE = re.compile(r"^(?:y\s+)?(?:mas|otros|otras|siguiente)$")


class AdmissionRoute(str, Enum):
    PROFILE = "PROFILE"
    DELEGATED = "DELEGATED"
    CLARIFY = "CLARIFY"


class InputShape(str, Enum):
    """Qué clase de entrada es, antes de saber quién es su dueño."""

    RESET = "RESET"
    SEED_REFINEMENT = "SEED_REFINEMENT"
    PAGINATION = "PAGINATION"
    ORDINAL = "ORDINAL"
    AFFIRMATIVE = "AFFIRMATIVE"
    TEXT = "TEXT"


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    route: AdmissionRoute
    reason: str
    shape: InputShape
    owner: ContinuationOwner
    response: str = ""

    @property
    def clarifies(self) -> bool:
        return self.route is AdmissionRoute.CLARIFY


_PROFILE_MORE = (
    "Este análisis institucional no es una lista paginable. Dime en qué quieres "
    "profundizar: sus proveedores, sus montos, sus modalidades, su evolución o "
    "su actividad reciente."
)
_NO_REFERENT = (
    "No tengo un resultado anterior al que darle continuidad. Dime qué quieres "
    "buscar y lo consulto."
)
_LOST_CONTEXT = (
    "No pude recuperar el contexto de esta conversación, así que no voy a "
    "continuar un resultado que no puedo acreditar. Repíteme la consulta y la "
    "hago de nuevo."
)
_PROFILE_HAS_NO_OPTIONS = (
    "Este análisis no ofreció opciones numeradas. Dime qué quieres ver de esta "
    "entidad y lo preparo."
)
_SEED_OVER_PROFILE = (
    "Ese filtro venía de una búsqueda en tabla y este resultado es un análisis "
    "institucional; aplicarlo cambiaría el universo sin que lo pidieras. Dime "
    "qué quieres ajustar del análisis y lo hago sobre él."
)
_STALE_SEED = (
    "Ese filtro apunta a un resultado que ya no está en pantalla. Vuelve a "
    "aplicarlo sobre el resultado actual."
)


def input_shape(
    user_query: str, *, seed_resolved_params: dict | None = None,
) -> InputShape:
    """Una sola forma por entrada; el orden aquí es el contrato."""
    if is_reset_command(user_query):
        return InputShape.RESET
    if seed_resolved_params:
        return InputShape.SEED_REFINEMENT
    folded = _fold(user_query).strip()
    if is_pagination_phrase(user_query) or _BARE_MORE.fullmatch(folded):
        return InputShape.PAGINATION
    if _ORDINAL.fullmatch(folded):
        return InputShape.ORDINAL
    if is_bare_affirmative(user_query):
        return InputShape.AFFIRMATIVE
    return InputShape.TEXT


def classify(
    *,
    user_query: str,
    seed_resolved_params: dict | None,
    envelope_status: EnvelopeStatus,
    contract: ContinuationContract | None,
    conversation_revision: int,
    profile_owns_text: bool,
    profile_has_options: bool,
) -> AdmissionDecision:
    """La decisión única del turno.

    `profile_owns_text` lo aporta el perfil sobre su propio Frame confirmado;
    la admisión no vuelve a parsear el texto para adivinarlo.
    """
    shape = input_shape(user_query, seed_resolved_params=seed_resolved_params)
    owner = contract.owner if contract else ContinuationOwner.NONE

    # Un reset explícito conserva su semántica y su handler; invalidar la
    # continuidad es consecuencia suya, no una decisión de la admisión.
    if shape is InputShape.RESET:
        return AdmissionDecision(
            AdmissionRoute.DELEGATED, "explicit_reset", shape, owner,
        )

    # Una cola corrupta NO habilita continuar con lo que sea: las entradas que
    # sólo significan algo respecto de un referente se detienen aquí.
    if envelope_status is EnvelopeStatus.CORRUPT and shape in _NEEDS_REFERENT:
        return AdmissionDecision(
            AdmissionRoute.CLARIFY, "unreadable_continuation", shape, owner,
            _LOST_CONTEXT,
        )

    if shape is InputShape.SEED_REFINEMENT:
        return _seed(seed_resolved_params, contract, conversation_revision, owner)

    if shape is InputShape.PAGINATION:
        if owner in (ContinuationOwner.ENTITY_PROFILE,):
            return AdmissionDecision(
                AdmissionRoute.CLARIFY, "profile_is_not_paginable", shape, owner,
                _PROFILE_MORE,
            )
        if owner is ContinuationOwner.NONE and envelope_status is EnvelopeStatus.OK:
            return AdmissionDecision(
                AdmissionRoute.CLARIFY, "no_referent_to_continue", shape, owner,
                _NO_REFERENT,
            )
        # Tabular, Actor, Radar y las conversaciones sin sobre publicado
        # conservan su ruta: el handler valida cursor, huella y vigencia.
        return AdmissionDecision(
            AdmissionRoute.DELEGATED, "delegated_pagination", shape, owner,
        )

    if shape in (InputShape.ORDINAL, InputShape.AFFIRMATIVE):
        if owner is ContinuationOwner.NONE and envelope_status is EnvelopeStatus.OK:
            return AdmissionDecision(AdmissionRoute.CLARIFY, "no_current_selection", shape, owner,
                                     _NO_REFERENT)
        if owner is ContinuationOwner.ENTITY_PROFILE:
            if profile_has_options:
                return AdmissionDecision(
                    AdmissionRoute.PROFILE, "profile_pending_selection", shape, owner,
                )
            return AdmissionDecision(
                AdmissionRoute.CLARIFY, "profile_has_no_options", shape, owner,
                _PROFILE_HAS_NO_OPTIONS,
            )
        # Actor y sugerencias validan token, versión y TTL en su propio handler.
        return AdmissionDecision(
            AdmissionRoute.DELEGATED, "delegated_selection", shape, owner,
        )

    if profile_owns_text:
        return AdmissionDecision(
            AdmissionRoute.PROFILE, "profile_turn", shape, owner,
        )
    return AdmissionDecision(AdmissionRoute.DELEGATED, "new_query", shape, owner)


_NEEDS_REFERENT = frozenset({
    InputShape.SEED_REFINEMENT, InputShape.PAGINATION, InputShape.ORDINAL,
    InputShape.AFFIRMATIVE,
})


def _seed(
    seed: dict | None, contract, conversation_revision: int, owner,
) -> AdmissionDecision:
    shape = InputShape.SEED_REFINEMENT
    if owner is ContinuationOwner.ENTITY_PROFILE:
        # Un seed tabular no adquiere autoridad sobre un Frame de perfil por el
        # hecho de traer params.
        return AdmissionDecision(
            AdmissionRoute.CLARIFY, "seed_over_profile", shape, owner,
            _SEED_OVER_PROFILE,
        )
    reference = (seed or {}).get(CONTINUATION_REF_KEY)
    if owner is not ContinuationOwner.TABULAR or not _matches(reference, contract, conversation_revision):
        return AdmissionDecision(
            AdmissionRoute.CLARIFY, "stale_seed_reference", shape, owner,
            _STALE_SEED,
        )
    return AdmissionDecision(
        AdmissionRoute.DELEGATED, "delegated_refinement", shape, owner,
    )


def _matches(reference: object, contract, conversation_revision: int) -> bool:
    if not isinstance(reference, dict) or contract is None:
        return False
    return (
        reference.get("result_id") == contract.result_id
        and reference.get("conversation_revision") == conversation_revision
    )


def _fold(text: str) -> str:
    stripped = re.sub(r"[¿?¡!.,;:]+", " ", text)
    decomposed = unicodedata.normalize("NFD", stripped.casefold())
    folded = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(folded.split())


__all__ = [
    "CONTINUATION_REF_KEY",
    "AdmissionDecision",
    "AdmissionRoute",
    "InputShape",
    "classify",
    "input_shape",
]
