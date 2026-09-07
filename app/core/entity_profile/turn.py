"""Small deterministic detector and safe parameter projection for profile turns."""
from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata

from app.core.entity_profile.execution_plan import (
    EntityProfileExecutionPlan,
    build_execution_plan,
)
from app.core.entity_profile.models import EntityProfileIdentity, EntityProfileScope
from app.core.entity_profile.followup import PROFILE_FOCUS_PARAM, is_supplier_followup
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.semantic_request import (
    EntityProfileSemanticRequest,
    build_semantic_request,
    inherit_request,
    request_context,
)
from app.core.entity_profile.source_request import SourceProvenance
from app.core.municipal_territory import get_municipal_territories


PROFILE_ACTIVE_PARAM = "_entity_profile_active"
PROFILE_REQUEST_PARAM = "_entity_profile_request"
PROFILE_REQUEST_PARAMS = (PROFILE_REQUEST_PARAM, "_entity_profile_sources")

_OVERVIEW = re.compile(
    r"\b(?:analiza(?:me)?|como\s+(?:ha\s+)?contrata(?:do)?|que\s+sabes\s+de|"
    r"perfil\s+d(?:e|el)|radiografia\s+d(?:e|el))\b"
)
_FOCUS = (
    ("suppliers", re.compile(r"\b(?:proveedores?|contratistas?)\b")),
    ("modalities", re.compile(r"\bmodalidad(?:es)?\b")),
    ("values", re.compile(r"\b(?:valores?|montos?|cuanto\s+(?:contrato|contratado))\b")),
    ("recent", re.compile(r"\b(?:recientes?|ultimos?|actividad\s+reciente)\b")),
    ("temporal", re.compile(r"\b(?:evolucion|por\s+anos?|compar(?:a|ar)\s+anos?)\b")),
)
_MONTHS = (
    "enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre"
    "|octubre|noviembre|diciembre"
)
_TIME_FOLLOWUP = re.compile(
    r"\b(?:este\s+ano|ano\s+pasado|en\s+20\d{2}|entre\s+20\d{2}"
    rf"|en\s+(?:{_MONTHS}))\b"
)
# Un cambio de FUENTE dentro de un perfil activo. Gramática completa y no
# subcadena, como el resto de follow-ups del perfil: "solo contratos" es un
# turno de perfil, pero "contratos de obra en cali" es una búsqueda nueva.
_SOURCE_NOUN = r"(?:procesos?|contratos?|licitaciones?|convocatorias?)"
_SOURCE_FOLLOWUP = re.compile(
    r"(?:(?:y|ahora|solo|solamente|unicamente)\s+)?"
    r"(?:(?:muestrame|muestra|dame|analiza(?:me)?)\s+)?"
    rf"(?:(?:los|las)\s+)?{_SOURCE_NOUN}"
    rf"(?:\s+y\s+(?:(?:los|las)\s+)?{_SOURCE_NOUN})?"
)
_MODALITY_FOLLOWUP = re.compile(r"\b(?:solo|solamente)\b.*\bcontratacion\b")
_PROFILE_NOISE = frozenset({
    "analiza", "analizame", "perfil", "radiografia", "sabes", "contrata",
    "contratado", "entidad", "principales", "proveedores", "proveedor",
    "contratistas", "contratista", "modalidades", "modalidad", "valores",
    "valor", "montos", "monto", "recientes", "reciente", "ultimos", "ultimo",
    "evolucion",
})
# El clúster territorial se purga entero o no se purga. ``region_resolution``
# es la procedencia de ``region``/``departamentos_in``: dejarla atrás produce
# metadata que afirma un territorio que el frame ya no filtra. No la consume el
# builder —no es un filtro—, pero sí la leen el arbitraje de span de actor y las
# auditorías, y ahí una evidencia huérfana miente.
_GEOGRAPHY_PARAM_KEYS = frozenset({
    "ciudad", "ciudad_resolved", "departamento", "departamento_resolved",
    "departamento_resolution", "departamentos_in", "region", "region_resolution",
    "municipal_territory",
})


@dataclass(frozen=True, slots=True)
class EntityProfileTurn:
    detected: bool
    focus: str | None
    params: dict
    scope: EntityProfileScope | None
    identity: EntityProfileIdentity | None
    clarification: str | None = None
    request: EntityProfileSemanticRequest | None = None
    plan: EntityProfileExecutionPlan | None = None


def _fold(text: str) -> str:
    decomposed = unicodedata.normalize("NFD", text.casefold())
    return "".join(char for char in decomposed if not unicodedata.combining(char))


def _focus(text: str) -> str | None:
    for focus, pattern in _FOCUS:
        if pattern.search(text):
            return focus
    return None


def detect_entity_profile_turn(user_query: str, params: dict) -> tuple[bool, str | None]:
    normalized = _fold(user_query)
    if is_supplier_followup(user_query, params):
        return True, "suppliers"
    focus = _focus(normalized)
    if _OVERVIEW.search(normalized):
        return True, focus or "overview"
    if not params.get(PROFILE_ACTIVE_PARAM):
        return False, None
    if focus:
        return True, focus
    if _TIME_FOLLOWUP.search(normalized) or _MODALITY_FOLLOWUP.search(normalized):
        return True, "overview"
    if _SOURCE_FOLLOWUP.fullmatch(_cleaned(normalized)):
        return True, "overview"
    return False, None


def _cleaned(normalized: str) -> str:
    """Texto sin puntuación ni cortesía, para gramáticas de coincidencia total."""
    folded = re.sub(r"[¿?¡!.,;:]+", " ", normalized)
    folded = re.sub(r"\b(?:por favor|porfa|gracias)\b", " ", folded)
    return " ".join(folded.split())


def _clean_topic_value(value: object) -> object:
    if isinstance(value, list):
        cleaned = [_clean_topic_value(item) for item in value]
        return [item for item in cleaned if item not in (None, "", [], ())]
    if not isinstance(value, str):
        return value
    return "" if _fold(value) in _PROFILE_NOISE else value


def profile_query_params(params: dict) -> dict:
    """Keep validated filters, remove only profile language and geography."""
    clean = {
        key: value
        for key, value in params.items()
        if key not in _GEOGRAPHY_PARAM_KEYS
        and key not in {PROFILE_ACTIVE_PARAM, *PROFILE_REQUEST_PARAMS}
    }
    for key in ("objeto", "objeto_or", "objeto_phrases"):
        if key not in clean:
            continue
        value = _clean_topic_value(clean[key])
        if value in (None, "", [], ()):
            clean.pop(key, None)
        else:
            clean[key] = value
    clean.pop("offset", None)
    clean.pop("limit", None)
    return clean


def _territory_only_name(user_query: str) -> bool:
    """A naked municipal name stays Territory, even if a legacy alias owns it."""
    text = _fold(user_query)
    spans = get_municipal_territories().name_spans(text)
    if len(spans) != 1:
        return False
    start, end = spans[0]
    residual = f"{text[:start]} {text[end:]}"
    residual = _OVERVIEW.sub(" ", residual)
    residual = re.sub(r"\b(?:en\s+)?20\d{2}\b", " ", residual)
    residual = re.sub(r"\b(?:la|el|de|del|un|una|por|favor)\b", " ", residual)
    return not re.sub(r"[^a-z0-9]+", "", residual)


def _territory_clarification(user_query: str) -> str:
    registry = get_municipal_territories()
    folded = _fold(user_query)
    spans = registry.name_spans(folded)
    if len(spans) == 1:
        start, end = spans[0]
        candidates = registry.lookup(folded[start:end])
        if len(candidates) == 1:
            name = candidates[0].municipality.title()
            return (
                f"¿Quieres analizar la Alcaldía de {name} o el territorio "
                f"institucional de {name}?"
            )
    return (
        "Necesito una entidad compradora reconocida. Indica, por ejemplo, "
        "‘la Alcaldía de Baranoa’; un territorio por sí solo no selecciona una entidad."
    )


def classify_entity_profile_turn(user_query: str, params: dict) -> EntityProfileTurn:
    detected, focus = detect_entity_profile_turn(user_query, params)
    if not detected:
        clean = dict(params)
        for marker in (PROFILE_ACTIVE_PARAM, PROFILE_FOCUS_PARAM, *PROFILE_REQUEST_PARAMS):
            clean.pop(marker, None)
        return EntityProfileTurn(False, None, clean, None, None)

    clean = profile_query_params(params)
    resolution = resolve_profile_scope(clean)
    territory_only = _territory_only_name(user_query)
    has_geographic_scope = any(params.get(key) for key in _GEOGRAPHY_PARAM_KEYS)
    if (
        resolution.scope is None
        and resolution.reason in {
            "BUYER_SCOPE_REQUIRED", "PRODUCTIVE_BUYER_RESOLUTION_REQUIRED",
        }
        and not territory_only
        and not has_geographic_scope
    ):
        return EntityProfileTurn(False, None, clean, None, None)
    if (
        resolution.scope is None
        or resolution.identity is None
        or (
            resolution.scope.buyer_group_id is None
            and territory_only
        )
    ):
        return EntityProfileTurn(
            True,
            focus,
            clean,
            None,
            None,
            resolution.reason
            if resolution.reason
            and resolution.reason not in {
                "BUYER_SCOPE_REQUIRED", "PRODUCTIVE_BUYER_RESOLUTION_REQUIRED",
            }
            else _territory_clarification(user_query),
        )
    request = _composed_request(user_query, clean, resolution.scope, focus, params)
    plan = build_execution_plan(request, clean)
    clean[PROFILE_ACTIVE_PARAM] = True
    clean[PROFILE_FOCUS_PARAM] = focus
    clean.update(request_context(request))
    if plan.clarification:
        # Con una aclaración pendiente la petición anterior queda intacta: no se
        # muta fuente, tema ni evento hasta que la persona confirme.
        return EntityProfileTurn(
            True, focus, clean, resolution.scope, resolution.identity,
            plan.clarification, request, plan,
        )
    return EntityProfileTurn(
        True,
        focus,
        clean,
        resolution.scope,
        resolution.identity,
        None,
        request,
        plan,
    )


def _composed_request(
    user_query: str,
    clean: dict,
    scope: EntityProfileScope,
    focus: str,
    original_params: dict,
) -> EntityProfileSemanticRequest:
    """Compone el turno sobre la petición anterior sin ensanchar dimensiones."""
    current = build_semantic_request(
        user_query=user_query, params=clean, scope=scope, focus=focus,
    )
    previous = EntityProfileSemanticRequest.from_dict(
        original_params.get(PROFILE_REQUEST_PARAM),
    )
    if previous is None:
        return current
    return inherit_request(
        previous,
        current,
        source_is_explicit=current.sources_provenance is SourceProvenance.EXPLICIT,
    )


def is_explicit_entity_profile_search(user_query: str, params: dict) -> bool:
    """A named institutional analysis starts its own conversational universe."""
    if not _OVERVIEW.search(_fold(user_query)):
        return False
    turn = classify_entity_profile_turn(user_query, params)
    return turn.detected and (turn.scope is not None or bool(turn.clarification))
