"""Adaptador de wiring para operaciones conversacionales de modalidad (M3C-C).

Mantiene ``parsing.py`` como coordinador delgado y evita otro monolito. Aquí
vive TODO lo que el motor puro (``modality_followup.py``, M3C-B congelado) no
hace: serialización JSON-native de la instrucción efímera para Burr state,
fuente cacheada de ``known_procedure_ids``, masking de spans de control, mapa de
mensajes de aclaración (por los códigos REALES que emite el motor) y el bloque
``modality`` del mutation trace.

Autoridad: M2 resuelve la modalidad; el motor puro aplica la mutación; este
módulo solo transporta y coordina. No importa ``FollowupEngine`` ni ``parsing``
(sin ciclos); no llama a M2 ni a HTTP; no lee YAML por turno.
"""
from __future__ import annotations

from functools import lru_cache

from app.core.modality_filter import ModalityFilter
from app.core.modality_followup import (
    ModalityFollowupInstruction,
    ModalityFollowupOperation,
    ModalityMutationDecision,
    apply_modality_followup_operation,
    classify_modality_followup_control,
)
from app.core.modality_followup import _normalize as _normalize_text
from app.core.modality_resolution_models import ResolutionStatus
from app.core.query_text_consumption import mask_consumed_spans

_TOPIC_KEYS = ("objeto", "objeto_or", "objeto_phrases")

INCLUDE_KEY = "modalidad_include_procedure_ids"
EXCLUDE_KEY = "modalidad_exclude_procedure_ids"
MODALITY_KEYS: tuple[str, str] = (INCLUDE_KEY, EXCLUDE_KEY)

# Intents genéricos que NO admiten promoción a REPLACE implícito (enmienda #1):
# una búsqueda nueva explícita o una paginación con modalidad no se convierten
# en follow-up de modalidad por el mero hecho de traer un target M2.
_IMPLICIT_REPLACE_BLOCKED_INTENTS = frozenset({"new_search", "pagination_more"})

_EMPTY_FILTER = ModalityFilter()


# ── Fuente única de IDs conocidos (enmienda #4) ────────────────────────────

@lru_cache(maxsize=1)
def known_procedure_ids() -> frozenset[str]:
    """Universo de ``procedure_ids`` del catálogo de modalidad, cacheado una vez.

    No se lee YAML por turno ni se infieren IDs desde params, aliases o raws: la
    autoridad es el mismo catálogo declarativo que usa M2.
    """
    from app.core.modality_models import load_catalog

    return frozenset(p.procedure_id for p in load_catalog().procedures)


# ── Serialización estricta para Burr state (enmienda #3) ───────────────────

_INSTR_FIELDS = (
    "status", "operation", "control_code", "construction_id",
    "source_text", "control_spans", "target_required", "explanation",
)


def instruction_to_state(instruction: ModalityFollowupInstruction) -> dict:
    """Proyecta la instrucción a un dict JSON-native (enum → str, spans → listas)."""
    return {
        "status": instruction.status,
        "operation": instruction.operation.value if instruction.operation else None,
        "control_code": instruction.control_code,
        "construction_id": instruction.construction_id,
        "source_text": instruction.source_text,
        "control_spans": [[int(s), int(e)] for s, e in instruction.control_spans],
        "target_required": bool(instruction.target_required),
        "explanation": instruction.explanation,
    }


def _exact_str(value: object, field: str) -> str:
    """Exige un ``str`` EXACTO (no subtipos como str-Enums foráneos)."""
    if type(value) is not str:
        raise ValueError(f"{field} debe ser str exacto, no {type(value).__name__}")
    return value


def _optional_exact_str(value: object, field: str) -> str | None:
    """``None`` o un ``str`` EXACTO; cualquier otro tipo se rechaza."""
    if value is None:
        return None
    if type(value) is not str:
        raise ValueError(f"{field} debe ser None o str exacto, no {type(value).__name__}")
    return value


def instruction_from_state(payload: object) -> ModalityFollowupInstruction:
    """Reconstruye la instrucción validando de forma hostil el payload.

    Deserialización ESTRICTA (§2): sin coerciones ``int(...)``/``bool(...)``. Cada
    campo se valida por tipo exacto (``type(x) is …``) para que un payload de Burr
    state corrupto o adversarial no se "arregle" silenciosamente:

    * ``status``/``source_text``/``explanation`` → str exacto.
    * ``operation`` → ``None`` o str exacto (rechaza enums foráneos aunque sean
      subtipos de str); luego se valida como valor conocido del enum propio.
    * ``control_code``/``construction_id`` → ``None`` o str exacto.
    * ``target_required`` → ``bool`` exacto (rechaza ``0``/``1`` y ``"false"``).
    * ``control_spans`` → lista de listas ``[int, int]`` con índices ``int``
      exactos (rechaza ``True``/``False``, floats, strings numéricos, ``Decimal``
      y mappings dentro del span).

    La consistencia semántica final (status↔operation↔spans) la impone el
    ``__post_init__`` de la dataclass congelada.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"payload de instrucción debe ser dict, no {type(payload).__name__}")
    keys = set(payload)
    missing = set(_INSTR_FIELDS) - keys
    if missing:
        raise ValueError(f"payload de instrucción incompleto: faltan {sorted(missing)}")
    extra = keys - set(_INSTR_FIELDS)
    if extra:
        raise ValueError(f"payload de instrucción con campos inesperados: {sorted(extra)}")

    status = _exact_str(payload["status"], "status")
    control_code = _optional_exact_str(payload["control_code"], "control_code")
    construction_id = _optional_exact_str(payload["construction_id"], "construction_id")
    source_text = _exact_str(payload["source_text"], "source_text")
    explanation = _exact_str(payload["explanation"], "explanation")

    op_raw = payload["operation"]
    if op_raw is None:
        operation = None
    else:
        if type(op_raw) is not str:  # str exacto: descarta str-Enums foráneos
            raise ValueError(f"operation debe ser None o str exacto, no {type(op_raw).__name__}")
        operation = ModalityFollowupOperation(op_raw)  # valida que sea un valor conocido

    target_required = payload["target_required"]
    if type(target_required) is not bool:  # bool exacto: 0/1 y "false" no cuentan
        raise ValueError(
            f"target_required debe ser bool exacto, no {type(target_required).__name__}"
        )

    spans_raw = payload["control_spans"]
    if not isinstance(spans_raw, list):
        raise ValueError(f"control_spans debe ser lista, no {type(spans_raw).__name__}")
    spans: list[tuple[int, int]] = []
    for pair in spans_raw:
        # El span debe ser una lista de exactamente dos elementos (no tupla persistida,
        # no mapping, no set): la proyección JSON-native emite listas.
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError(f"span debe ser lista de longitud 2: {pair!r}")
        start, end = pair
        # type(x) is int rechaza bool (True/False), float, str numérico y Decimal.
        if type(start) is not int or type(end) is not int:
            raise ValueError(f"índices de span deben ser int exactos: {pair!r}")
        spans.append((start, end))

    return ModalityFollowupInstruction(
        status=status,
        operation=operation,
        control_code=control_code,
        construction_id=construction_id,
        source_text=source_text,
        control_spans=tuple(spans),
        target_required=target_required,
        explanation=explanation,
    )


# ── Clasificación transportable (enmienda #1) ──────────────────────────────

def classify_control(
    user_query: str,
    interpretation,
    *,
    allow_implicit_replace: bool,
) -> ModalityFollowupInstruction:
    """Clasifica el lenguaje de control del turno usando la evidencia de M2.

    ``current_filter``/``resolution_status`` provienen del ``ModalityInterpretation``
    del turno actual (executable_filter). Si M2 no corrió, se asume NO_MATCH sin
    filtro. ``allow_implicit_replace`` se deja en ``False`` en ``parse_query`` y
    solo se activa en ``apply_context`` cuando el intent genérico ya es conocido.
    """
    return classify_modality_followup_control(
        user_query,
        current_filter=current_filter(interpretation),
        resolution_status=resolution_status(interpretation),
        target_spans=target_spans(interpretation),
        allow_implicit_replace=allow_implicit_replace,
    )


def current_filter(interpretation) -> ModalityFilter:
    """Filtro ejecutable del turno actual (vacío si M2 no corrió)."""
    if interpretation is None:
        return _EMPTY_FILTER
    return interpretation.executable_filter


def resolution_status(interpretation) -> ResolutionStatus:
    if interpretation is None:
        return ResolutionStatus.NO_MATCH
    return interpretation.resolution_status


def target_spans(interpretation) -> tuple[tuple[int, int], ...]:
    """Spans de modalidad consumidos por M2 (vacío si M2 no corrió). R3′: evidencia
    de POSICIÓN que decide si un verbo de control gobierna una modalidad."""
    if interpretation is None:
        return ()
    return interpretation.consumed_spans


def is_eligible_for_implicit_replace(
    instruction: ModalityFollowupInstruction,
    interpretation,
    intent: str,
) -> bool:
    """True si un turno sin control explícito debe promoverse a REPLACE implícito.

    Requiere: instrucción en ``none``, filtro M2 ejecutable no vacío y un intent
    genérico que NO sea búsqueda nueva ni paginación (enmienda #1).
    """
    if instruction.status != "none":
        return False
    if intent in _IMPLICIT_REPLACE_BLOCKED_INTENTS:
        return False
    if resolution_status(interpretation) not in (
        ResolutionStatus.RESOLVED, ResolutionStatus.RESOLVED_WITH_ASSUMPTION
    ):
        return False
    return not current_filter(interpretation).is_empty()


# ── Masking de spans de control (enmienda #5) ──────────────────────────────

def mask_control_spans(user_query: str, instruction: ModalityFollowupInstruction) -> str:
    """Enmascara SOLO los spans de control sobre la MISMA cadena que se clasificó.

    Los offsets se calcularon sobre ``user_query`` (el clasificador normaliza
    preservando longitud), así que se enmascara exactamente ese string antes de
    entregarlo al follow-up genérico. Evita que el motor genérico reclame verbos
    como ``quita``/``ahora`` como su propio control. No se tocan spans jurídicos.
    """
    if not instruction.control_spans:
        return user_query
    return mask_consumed_spans(user_query, instruction.control_spans)


def _control_words(user_query: str, control_spans) -> set[str]:
    """Palabras de control derivadas de los spans del motor (sin léxico propio)."""
    words: set[str] = set()
    for start, end in control_spans:
        for token in _normalize_text(user_query[start:end]).split():
            if token:
                words.add(token)
    return words


def _scrub_topic_list(value: list, words: set[str]) -> list:
    """Filtra una lista de topic respetando la unidad real del campo.

    Forma real del contrato (verificada contra ``QueryRouter._extract_object_terms``):
    ``objeto`` es ``list[str | list[str]]`` —un nivel de anidamiento, el grupo OR de
    "X o Y"—; ``objeto_or`` y ``objeto_phrases`` son listas planas de ``str``. Por eso
    la limpieza es recursiva UN nivel: un término de control dentro de un grupo OR
    también es control. El grupo que queda vacío se retira y el que queda con un solo
    término se colapsa a escalar, que es exactamente como el parser lo representa.
    """
    cleaned: list = []
    for item in value:
        if isinstance(item, list):
            group = [
                sub for sub in item
                if isinstance(sub, list) or _normalize_text(str(sub)) not in words
            ]
            if not group:
                continue  # el grupo solo contenía control: desaparece
            cleaned.append(group if len(group) > 1 else group[0])
            continue
        if _normalize_text(str(item)) not in words:
            cleaned.append(item)
    return cleaned


def scrub_control_topic(params: dict, user_query: str, control_spans) -> dict:
    """Retira del topic parseado los tokens que son verbos de control.

    El parser extrae ``objeto`` antes de conocer la operación de modalidad, así
    que ``quita``/``además``/``ahora`` pueden quedar como topic. Se limpian usando
    exactamente las palabras cubiertas por los spans de control (§8: los controles
    nunca entran en objeto/topic/fingerprint) —no hay léxico propio de controles—.
    No se tocan otros campos ni se muta ``params`` ni sus listas.
    """
    if not control_spans:
        return params
    words = _control_words(user_query, control_spans)
    if not words:
        return params
    result = dict(params)
    for key in _TOPIC_KEYS:
        value = result.get(key)
        if not isinstance(value, list):
            continue
        filtered = _scrub_topic_list(value, words)
        if filtered:
            result[key] = filtered
        else:
            result.pop(key, None)
    return result


# ── Aplicación del motor (autoridad única de modalidad) ────────────────────

def apply_engine(
    previous_filter: ModalityFilter,
    interpretation,
    instruction: ModalityFollowupInstruction,
) -> ModalityMutationDecision:
    """Aplica el motor puro con la evidencia canónica del turno."""
    return apply_modality_followup_operation(
        previous_filter,
        current_filter(interpretation),
        instruction,
        resolution_status(interpretation),
        known_procedure_ids(),
    )


def filter_from_params(params: dict) -> ModalityFilter:
    """Reconstruye el ``ModalityFilter`` canónico desde un dict de params.

    Reconstrucción ESTRICTA (§3): la representación de cada clave de modalidad debe
    ser ``list`` o ``tuple`` (las tuplas se normalizan a lista para
    ``ModalityFilter.from_params``); una clave ausente ⇒ vacío. TODO lo demás se
    rechaza EXPRESAMENTE por la FORMA del contenedor —``str``, ``set``,
    ``frozenset``, generador, ``dict``, ``bool``, ``int`` y ``None`` cuando la clave
    está presente— sin apoyarse en ``list(value)`` como validador universal (que
    aceptaría strings char-a-char o consumiría generadores) ni en la validación de
    IDs desconocidos aguas abajo. No muta ``params`` ni sus listas.
    """
    payload: dict[str, list] = {}
    for key in MODALITY_KEYS:
        if key not in params:
            continue  # clave ausente ⇒ ese lado del filtro queda vacío
        value = params[key]
        # bool es subtipo de int pero NO de (list, tuple); str/set/frozenset/dict/
        # generator/None tampoco lo son: todos caen aquí con un error explícito.
        if not isinstance(value, (list, tuple)):
            raise ValueError(
                f"{key} debe ser list o tuple (recibido {type(value).__name__})"
            )
        payload[key] = list(value)  # copia nueva: normaliza tuple→list, no muta el original
    return ModalityFilter.from_params(payload)


def strip_modality(params: dict) -> dict:
    """Copia ``params`` sin las claves de modalidad (para reescribir el filtro final)."""
    return {k: v for k, v in params.items() if k not in MODALITY_KEYS}


def mark_modality_delegated(trace: dict) -> None:
    """Reetiqueta el trace genérico: la modalidad se delega, no se acepta/rechaza.

    El merge genérico ve las claves de modalidad (las necesita para clasificar
    refine_filter vs new_search), pero su resultado de modalidad se descarta y lo
    reemplaza el motor puro. El trace debe reflejar delegación, no aceptación
    (§7.6): se retiran de accepted/rejected/cleared y se listan como delegadas.
    """
    for field in ("accepted_fields", "rejected_fields", "transition_cleared_fields"):
        current = trace.get(field)
        if isinstance(current, list):
            trace[field] = [k for k in current if k not in MODALITY_KEYS]
    reasons = trace.get("rejection_reasons")
    if isinstance(reasons, dict):
        trace["rejection_reasons"] = {
            k: v for k, v in reasons.items() if k not in MODALITY_KEYS
        }
    trace["delegated_fields"] = list(MODALITY_KEYS)


def write_final_filter(params: dict, final_filter: ModalityFilter) -> dict:
    """Devuelve ``params`` con la modalidad reemplazada por el filtro final.

    Retira ambas claves y reescribe solo las no vacías: un filtro vacío deja el
    turno sin modalidad (CLEAR / remoción total).
    """
    result = strip_modality(params)
    result.update(final_filter.to_params())
    return result


# ── Mensajes de aclaración (fuente única; claves = códigos REALES del motor) ─

_CLARIFICATION_MESSAGES: dict[str, str] = {
    "target_missing": "¿Qué modalidad quieres modificar?",
    "ambiguous_clear_or_null_value": (
        "¿Quieres quitar el filtro de modalidad o buscar registros sin ese dato?"
    ),
    "control_conflict": (
        "Veo dos instrucciones distintas sobre la modalidad. "
        "¿Quieres reemplazarla, agregarla, excluirla o quitarla?"
    ),
    "partial_target": (
        "Entendí una parte de la modalidad, pero necesito que aclares la otra "
        "antes de cambiar el filtro."
    ),
    "ambiguous_target": "Esa modalidad puede significar varias opciones. ¿Cuál quieres usar?",
    "conflicting_target": "La modalidad indicada combina opciones incompatibles. ¿Puedes reformularla?",
    "non_searchable_target": (
        "Esa opción no está disponible como filtro. ¿Quieres indicar otra modalidad?"
    ),
    "dataset_inapplicable_target": (
        "Esa modalidad no está disponible en el conjunto de datos elegido. "
        "¿Quieres cambiar el conjunto o la modalidad?"
    ),
    "mixed_target_polarity": (
        "Esa modalidad mezcla inclusión y exclusión a la vez. "
        "¿Quieres incluirla o excluirla?"
    ),
    "unknown_procedure_id": (
        "No pude verificar esa modalidad. ¿Puedes indicarla de otra forma?"
    ),
}

_DEFAULT_CLARIFICATION = "¿Qué modalidad quieres modificar?"

# Códigos de aclaración con mensaje propio (los que el motor puede emitir).
KNOWN_CLARIFICATION_CODES = frozenset(_CLARIFICATION_MESSAGES)


def message_for(clarification_code: str | None) -> str:
    """Mensaje de usuario para un código de aclaración. Nunca expone IDs/enums."""
    if clarification_code is None:
        return _DEFAULT_CLARIFICATION
    return _CLARIFICATION_MESSAGES.get(clarification_code, _DEFAULT_CLARIFICATION)
