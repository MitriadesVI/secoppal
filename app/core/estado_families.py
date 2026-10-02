"""
Estado Families — semantic normalization layer for SECOPPAL.

Maps natural-language estado tokens (publicados, abiertos, vigentes, etc.)
to SECOP-accurate field/value combinations grouped by lifecycle family.

Values verified against SECOP via discover_secop.py on 2026-05-12.

Discrepancies from specification (documented):
  - "En convocatoria": NOT in SECOP real data. Mapped to "Publicado" (closest).
  - "Presentación de oferta": NOT in SECOP. Excluded.
  - "Revocado": NOT in SECOP. Excluded.
  - "Celebrado": NOT in SECOP contratos. Closest is "Cerrado" (final state).
  - "Liquidado": NOT in SECOP contratos. Closest is "Cerrado" / "terminado".
    NOTE: contratos use "terminado" (lowercase, exact SECOP value).
"""
from dataclasses import dataclass
import re
import unicodedata

from app.data.estados import (
    ESTADO_APERTURA_SYNONYMS,
    ESTADO_PROCEDIMIENTO_SYNONYMS,
    VALID_ESTADOS_APERTURA,
    VALID_ESTADOS_PROCEDIMIENTO,
)

ESTADO_FAMILIES = {
    "oferta_abierta": {
        "procesos": {
            # Funnel pre-cierre completo: desde Borrador hasta Publicado.
            # estado_del_procedimiento es SNAPSHOT del momento, no histórico.
            # Borrador (42K), En aprobación (3K), Aprobado (7K) son fases
            # legítimas pre-publicación. Publicado (2.5M) recibe ofertas.
            # Abierto (31K) es valor residual de versiones antiguas.
            # Excluidos: Seleccionado (ya con ganador), Evaluación (plazo
            # cerrado), Cancelado, Suspendido.
            # DOGFOOD-2026-05-25: commit 12188a6 fue demasiado agresivo
            # al reducir a solo Publicado. Corregido con lectura correcta
            # del modelo de funnel.
            "estado_del_procedimiento": [
                "Publicado",
                "Borrador",
                "En aprobación",
                "Aprobado",
                "Abierto",
            ],
        },
        "contratos": {
            # "oferta_abierta" is a process intent, not a contract state
            "estado_contrato": [],
        },
    },
    "en_evaluacion": {
        "procesos": {
            "estado_de_apertura_del_proceso": [],  # aperture is still Abierto or Cerrado
            "estado_del_procedimiento": [
                "Evaluación",
                "Seleccionado",
                "Aprobado",
                "En aprobación",
            ],
        },
        "contratos": {
            "estado_contrato": [
                "Borrador",
                "enviado Proveedor",
                "En aprobación",
                "Aprobado",
            ],
        },
    },
    # ── contrato_activo: contrato vivo en ejecución ──
    # En SECOP, "Cerrado" NO significa "firmado". Cerrado indica cierre del
    # expediente/gestión contractual. "En ejecución", "Modificado" y "Prorrogado"
    # son contratos activos/vigentes. "Firmado" solo fuerza dataset='contratos'
    # sin aplicar filtro de estado (no hay campo "firmado" en SECOP).
    #
    # NOTA (Sprint 5): "Seleccionado" en estado_del_procedimiento aparece tanto
    # en en_evaluacion como en contrato_activo. Es intencional: un proceso en
    # "Seleccionado" puede ser (a) evaluación pre-adjudicación o (b) contrato
    # activo ya adjudicado — depende de contexto adicional.
    "contrato_activo": {
        "procesos": {
            "estado_de_apertura_del_proceso": ["Cerrado"],
            "estado_del_procedimiento": ["Seleccionado"],
        },
        "contratos": {
            "estado_contrato": [
                "En ejecución",
                "Modificado",
                "Prorrogado",
            ],
        },
    },
    "contrato_cerrado": {
        "contratos": {
            "estado_contrato": ["Cerrado"],
        },
    },
    "contrato_terminado": {
        "contratos": {
            "estado_contrato": ["terminado"],
        },
    },
    "contrato_cedido": {
        "contratos": {
            "estado_contrato": ["cedido"],
        },
    },
    "contrato_suspendido": {"contratos": {"estado_contrato": ["Suspendido"]}},
    "no_procede": {
        "procesos": {
            "estado_de_apertura_del_proceso": [],  # not necessarily closed
            "estado_del_procedimiento": [
                "Cancelado",
                "Suspendido",
            ],
        },
        "contratos": {
            # Suspendido belongs to contrato_suspendido: under Contract
            # Universe V1.1 a suspended contract is CELEBRATED, while a
            # cancelled record is outside that universe. One family cannot
            # hold both sides of that line.
            "estado_contrato": [
                "Cancelado",
            ],
        },
    },
}

# Map natural language tokens → family name
# These are consumed from the scrubbed text so they don't end up in 'objeto'.
NATURAL_TO_FAMILY = {
    # ── oferta_abierta ──
    "publicado": "oferta_abierta",
    "publicados": "oferta_abierta",
    "publicadas": "oferta_abierta",
    "abierto": "oferta_abierta",
    "abiertos": "oferta_abierta",
    "abiertas": "oferta_abierta",
    "convocatoria": "oferta_abierta",
    "convocatorias": "oferta_abierta",
    # CORPUS-R005: "licitaciones de construccion convocadas" — participio de
    # "convocar" tan fuerte como "convocatoria" para indicar proceso abierto.
    "convocado": "oferta_abierta",
    "convocados": "oferta_abierta",
    "convocada": "oferta_abierta",
    "convocadas": "oferta_abierta",
    "borrador": "oferta_abierta",
    "borradores": "oferta_abierta",
    # ── multi-word tokens ──
    "en convocatoria": "oferta_abierta",
    "en borrador": "oferta_abierta",
    # Follow-up de control semántico: describe disponibilidad, no un objeto.
    "todavia reciben ofertas": "oferta_abierta",
    "reciben ofertas": "oferta_abierta",
    # ── contrato_activo ──
    # Nota: "firmado/firmados" NO están aquí. "Firmado" no tiene estado_contrato
    # en SECOP; solo fuerza dataset='contratos' vía intent_vocabulary.py.
    "ejecucion": "contrato_activo",
    "ejecutando": "contrato_activo",
    "en ejecucion": "contrato_activo",
    # ── contrato_cerrado ──
    "cerrado": "contrato_cerrado",
    "cerrados": "contrato_cerrado",
    # ── contrato_terminado ──
    "terminado": "contrato_terminado",
    "terminados": "contrato_terminado",
    # ── contrato_cedido ──
    "cedido": "contrato_cedido",
    "cedidos": "contrato_cedido",
    # ── en_evaluacion ──
    "adjudicado": "en_evaluacion",
    "adjudicados": "en_evaluacion",
    "evaluacion": "en_evaluacion",
    "en evaluacion": "en_evaluacion",
    "seleccionado": "en_evaluacion",
    "aprobado": "en_evaluacion",
    # ── no_procede ──
    # "desierto", "suspendido" and "revocado" are dataset-aware and live in
    # DATASET_AWARE_FAMILY below; a single mapping cannot answer for both
    # lifecycles.
    "cancelado": "no_procede",
    "cancelados": "no_procede",
    "cancelada": "no_procede",
}


# ── Vocabulario de VIGENCIA: la familia depende del dataset ──────────────
#
# INV-LIVENESS-STATE-IS-DATASET-AWARE (HCA fresh, FRESH-STATE-002).
#
# "vigente", "activo" y "disponible" no nombran ningún estado de SECOP: dicen
# que la cosa está viva AHORA. Qué significa "vivo" depende de qué se mira.
# Un proceso vivo todavía recibe ofertas (``oferta_abierta``); un contrato vivo
# ya se firmó y se está ejecutando (``contrato_activo``). Son la MISMA petición
# proyectada sobre dos ciclos de vida distintos.
#
# Antes esto se escribía como dos entradas del mismo literal en
# ``NATURAL_TO_FAMILY``. Python conservaba la última en silencio, así que
# "oportunidades vigentes" —una consulta sobre procesos— se proyectaba como
# ``estado_de_apertura_del_proceso='Cerrado'`` + ``estado_del_procedimiento=
# 'Seleccionado'``: literalmente el complemento de lo pedido, con la etiqueta
# ``estado_family='oferta_abierta'`` encima. El canario vivo lo cazó como
# violación de contrato semántico con cero violaciones de filtro duro.
#
# La familia se declara completa (todas las flexiones del mismo lema) para que
# el comportamiento no dependa de qué forma escribió el usuario.
LIVENESS_TOKENS: tuple[str, ...] = (
    "vigente", "vigentes",
    "activo", "activos", "activa", "activas",
    "disponible", "disponibles",
)

# Suspension is the same word on two axes: a process that stopped advancing
# does not proceed, while a suspended CONTRACT was signed and is celebrated.
# Projecting one onto the other put Suspendido in two contractual families at
# once — the duplicate test_catalog_no_unexpected_duplicates reported.
SUSPENSION_TOKENS: tuple[str, ...] = (
    "suspendido", "suspendidos", "suspendida", "suspendidas",
)

# Outcomes only a PROCESS can have. A contract is never declared void for lack
# of bidders, and revocation belongs to the award, not to the signed contract.
# Absent a contractual reading, these project nothing rather than inventing a
# state: a missing dataset key resolves to no family at all.
PROCESS_ONLY_OUTCOME_TOKENS: tuple[str, ...] = (
    "desierto", "desierta", "desiertos", "desiertas", "revocado",
)

DATASET_AWARE_FAMILY: dict[str, dict[str, str]] = {
    **{token: {"procesos": "oferta_abierta", "contratos": "contrato_activo"}
       for token in LIVENESS_TOKENS},
    **{token: {"procesos": "no_procede", "contratos": "contrato_suspendido"}
       for token in SUSPENSION_TOKENS},
    **{token: {"procesos": "no_procede"} for token in PROCESS_ONLY_OUTCOME_TOKENS},
}

LIVENESS_FAMILY_BY_DATASET: dict[str, str] = {
    "procesos": "oferta_abierta",
    "contratos": "contrato_activo",
}
_LIVENESS_FAMILIES = frozenset(LIVENESS_FAMILY_BY_DATASET.values())


def rebase_family_to_dataset(family: str, dataset: str) -> str:
    """Relee una familia de vigencia en el ciclo de vida de ``dataset``.

    Solo tiene sentido cuando la familia declarada NO proyecta nada sobre el
    dataset heredado: "solo los activos" sobre una conversación de contratos se
    parsea sin contexto como ``oferta_abierta``, que en contratos no existe.
    Conservarla dejaría filtros de procesos colgando de una consulta de
    contratos; traducirla devuelve lo que el usuario pidió.
    """
    if family not in _LIVENESS_FAMILIES:
        return family
    return LIVENESS_FAMILY_BY_DATASET.get(dataset, family)


# ── Estado LÉXICO EXACTO vs FAMILIA SEMÁNTICA ────────────────────────────
#
# INV-EXACT-STATE-DOES-NOT-WIDEN-TO-FAMILY (HCA-STATE-005).
#
# NATURAL_TO_FAMILY resuelve vocabulario que DESCRIBE una familia: "vigentes",
# "disponibles", "convocatorias" no nombran ningún estado concreto de SECOP, así
# que expandirlos al funnel completo es la lectura correcta.
#
# Estos tokens son distintos: nombran LITERALMENTE un valor del dataset. Quien
# escribe "procesos publicados" pidió ``Publicado``; devolverle borradores y
# procesos en aprobación no es ser generoso, es contestar otra pregunta.
#
# El catálogo es deliberadamente corto. Solo entra un token que cumpla las tres
# condiciones:
#   1. es el nombre literal de un valor real del dataset,
#   2. su familia tiene MÁS de un valor para ese dataset (si no, exacto y
#      familia coinciden y no hay nada que decidir),
#   3. no es a la vez vocabulario que describe la familia entera.
#
# Por (3) quedan FUERA "abiertos" y "vigentes" — son el lenguaje natural de la
# oferta abierta, no una petición de ``Abierto``, que además es un valor
# residual de versiones antiguas. Y por (3) queda fuera "en ejecución": ADR-003
# fija que un contrato activo es En ejecución + Modificado + Prorrogado, así que
# ese token pide la familia, no un literal.
LEXICAL_EXACT_STATE: dict[str, str] = {
    "publicado": "Publicado",
    "publicados": "Publicado",
    "publicadas": "Publicado",
    "borrador": "Borrador",
    "borradores": "Borrador",
    "en borrador": "Borrador",
}


# ── Literales de PROCESO nombrados explícitamente ────────────────────────
#
# INV-PROCESS-STATE-NO-SILENT-LOSS (PROCESS_STATE_REQUEST_INTEGRITY_V1).
#
# Misma regla que LEXICAL_EXACT_STATE, aplicada al ciclo de vida de procesos:
# "procesos cancelados" pide ``Cancelado``, no Cancelado + Suspendido;
# "procesos seleccionados" pide ``Seleccionado``, no el funnel de evaluación.
# La familia conserva su etiqueta —el follow-up la usa para releer la petición
# en otro dataset— pero su proyección se estrecha al literal nombrado.
#
# El catálogo no es una lista nueva de alias: se deriva del léxico que ya
# declara esos valores (``app/data/estados.py``), tomando sólo la forma que ES
# el nombre del literal. "adjudicado" o "vigente" también llevan a un valor,
# pero no lo nombran. ``Abierto`` queda fuera por la regla (3) de arriba:
# "abiertos" es el lenguaje natural de la oferta abierta, no un literal.
# Las flexiones de género y número se leen por lema, no por enumeración.
_FAMILY_VOCABULARY_LITERALS = frozenset({"Abierto"})


def _fold(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value.lower())
    return "".join(char for char in decomposed if not unicodedata.combining(char))


def _names_literal(alias: str, literal: str) -> bool:
    return alias in (_fold(literal), "en " + _fold(literal))


PROCESS_EXACT_STATE: dict[str, str] = {
    alias: literal
    for alias, literal in {
        **ESTADO_APERTURA_SYNONYMS, **ESTADO_PROCEDIMIENTO_SYNONYMS,
    }.items()
    if _names_literal(alias, literal) and literal not in _FAMILY_VOCABULARY_LITERALS
}


def _inflection_lemmas(token: str) -> tuple[str, ...]:
    """El token y su forma masculina singular: cancelad{a,os,as} -> cancelado."""
    lemmas = [token]
    if token.endswith(("os", "as")):
        lemmas.append(token[:-2] + "o")
    elif token.endswith("a"):
        lemmas.append(token[:-1] + "o")
    if token.endswith("es"):
        lemmas.append(token[:-2])  # borradores -> borrador
    return tuple(lemmas)


def process_state_literal(token: str) -> str | None:
    """El literal de ``procesos`` que ``token`` nombra, o None."""
    for lemma in _inflection_lemmas(token.lower().strip()):
        literal = PROCESS_EXACT_STATE.get(lemma)
        if literal is not None:
            return literal
    return None


def process_state_field(literal: str) -> str:
    """Campo de ``procesos`` que transporta ``literal``, según el léxico."""
    if literal in VALID_ESTADOS_APERTURA and literal not in VALID_ESTADOS_PROCEDIMIENTO:
        return "estado_de_apertura_del_proceso"
    return "estado_del_procedimiento"


def locate_state_literal(literal: str, dataset: str) -> tuple[str, str] | None:
    """Devuelve (familia, campo) donde ``literal`` vive para ``dataset``.

    Deriva la ubicación de ESTADO_FAMILIES en vez de duplicarla: un literal que
    no exista en el catálogo no puede resolverse como estado exacto.
    """
    for family_name, family in ESTADO_FAMILIES.items():
        for field, values in family.get(dataset, {}).items():
            if literal in values:
                return family_name, field
    return None


def collect_estado_values(dataset: str | None = None) -> tuple[str, ...]:
    """Return SECOP state literals declared in ESTADO_FAMILIES.

    Values are exact dataset literals; do not normalize capitalization.
    """
    values: list[str] = []
    for family in ESTADO_FAMILIES.values():
        datasets = [dataset] if dataset else list(family.keys())
        for dataset_name in datasets:
            for field_values in family.get(dataset_name, {}).values():
                values.extend(field_values)
    return tuple(dict.fromkeys(values))


PROCESS_STATE_VALUES = collect_estado_values("procesos")
CONTRACT_STATE_VALUES = collect_estado_values("contratos")
LLM_ESTADO_VALUES = tuple(dict.fromkeys([*PROCESS_STATE_VALUES, *CONTRACT_STATE_VALUES]))


def resolve_estado(natural_token: str, dataset: str) -> dict | None:
    """Resolve a natural-language estado token to family + filters.

    Detects both single-word tokens ('publicados') and multi-word spans
    ('en convocatoria') by checking the combined token context.

    Args:
        natural_token: A token or short span from the user query.
        dataset: 'procesos' or 'contratos'.

    Returns:
        dict with 'family', 'filters' (field→list[value]), 'consumed_span', or None.
    """
    token_lower = natural_token.lower().strip()

    # Precedencia léxica: si el token NOMBRA un estado del dataset, ese estado
    # es la respuesta — la familia no lo ensancha.
    literal = LEXICAL_EXACT_STATE.get(token_lower)
    if literal is not None:
        located = locate_state_literal(literal, dataset)
        if located is not None:
            exact_family, field = located
            return {
                "family": exact_family,
                "filters": {field: [literal]},
                "consumed_span": natural_token,
                "kind": "exact",
            }

    # Vigencia: misma petición, proyección distinta según el ciclo de vida.
    # Sobre procesos, cada flexión de un literal nombra el mismo estado; el
    # vocabulario contractual conserva su propia superficie (Contract Universe).
    lemmas = _inflection_lemmas(token_lower) if dataset == "procesos" else (token_lower,)
    family_name = None
    for lemma in lemmas:
        dataset_aware = DATASET_AWARE_FAMILY.get(lemma)
        family_name = (
            dataset_aware.get(dataset)
            if dataset_aware is not None
            else NATURAL_TO_FAMILY.get(lemma)
        )
        if family_name is not None:
            break
    if family_name is None:
        return None

    family = ESTADO_FAMILIES[family_name]
    dataset_config = family.get(dataset, {})
    if not dataset_config:
        return None

    # Build filters: only include fields that have values for this dataset
    filters: dict[str, list[str]] = {}
    for field, values in dataset_config.items():
        if values:  # empty list means not applicable
            filters[field] = list(values)

    if not filters:
        return None

    # Un literal de proceso nombrado no se ensancha a su familia.
    literal = process_state_literal(token_lower) if dataset == "procesos" else None
    for field, values in filters.items():
        if literal is not None and literal in values:
            return {
                "family": family_name,
                "filters": {field: [literal]},
                "consumed_span": natural_token,
                "kind": "exact",
            }

    return {
        "family": family_name,
        "filters": filters,
        "consumed_span": natural_token,
        "kind": "family",
    }


_TOKEN_PUNCTUATION = "¿?¡!.,;:\"'()[]{}"

# Vocabulario que lee ``detect_estado_family``. Es también el que enumera
# ``read_process_state_mentions``: una sola superficie para el productor y para
# el guard.
FAMILY_TOKENS = frozenset({*NATURAL_TO_FAMILY, *LEXICAL_EXACT_STATE, *DATASET_AWARE_FAMILY})


# ── Todas las MENCIONES de estado de proceso, no sólo la primera ──────────
#
# INV-PROCESS-STATE-NO-SILENT-LOSS (PROCESS_STATE_REQUEST_INTEGRITY_V1.1).
#
# ``detect_estado_family`` responde una pregunta —el primer estado— y el
# extractor legacy consume el segundo sin aplicarlo. Un guard que sólo sabía
# enumerar LITERALES veía "cancelados y adjudicados" como un único estado:
# "adjudicados" es vocabulario de familia, el productor lo reconoce y el guard
# no. El resultado dependía del orden de las palabras.
#
# La lectura de cada mención es la que le dan los productores de procesos, en
# su mismo orden: ``resolve_estado`` (literal exacto o familia) y, si no
# resuelve, el léxico legacy de ``_extract_state``.
_LEGACY_PROCESS_SYNONYMS: dict[str, str] = {
    **ESTADO_PROCEDIMIENTO_SYNONYMS, **ESTADO_APERTURA_SYNONYMS,
}
_MENTION_TOKENS = frozenset({*FAMILY_TOKENS, *_LEGACY_PROCESS_SYNONYMS})


@dataclass(frozen=True, slots=True)
class ProcessStateMention:
    """Una mención de estado sobre ``procesos``: literal exacto o familia."""

    span: tuple[int, int]
    surface: str
    literal: str | None
    field: str | None
    family: str | None

    @property
    def reading(self) -> str:
        return self.literal or f"familia:{self.family}"


def _process_reading(token: str) -> tuple[str | None, str | None, str | None]:
    """(literal, campo, familia) que los productores de procesos dan a ``token``."""
    resolved = resolve_estado(token, "procesos")
    if resolved is not None:
        if resolved["kind"] == "exact":
            ((field, values),) = resolved["filters"].items()
            return values[0], field, resolved["family"]
        return None, None, resolved["family"]
    legacy = _LEGACY_PROCESS_SYNONYMS.get(token)
    if legacy is not None:
        return legacy, process_state_field(legacy), None
    return None, None, None


def read_process_state_mentions(text: str) -> tuple[ProcessStateMention, ...]:
    """Cada mención de estado de proceso en ``text``, en orden textual.

    Las formas de varias palabras ganan su span antes que las sueltas, igual
    que en ``detect_estado_family``; las flexiones se leen por lema dentro de
    ``resolve_estado``.
    """
    lowered = text.lower()
    taken: list[tuple[int, int]] = []
    mentions: list[ProcessStateMention] = []

    def claim(start: int, end: int) -> None:
        if any(a < end and start < b for a, b in taken):
            return
        surface = text[start:end]
        literal, field, family = _process_reading(surface.lower())
        if literal is None and family is None:
            return
        taken.append((start, end))
        mentions.append(ProcessStateMention((start, end), surface, literal, field, family))

    for token in sorted((t for t in _MENTION_TOKENS if " " in t), key=len, reverse=True):
        for match in re.finditer(rf"\b{re.escape(token)}\b", lowered):
            claim(*match.span())
    for match in re.finditer(r"\b\w+\b", lowered):
        claim(*match.span())
    return tuple(sorted(mentions, key=lambda mention: mention.span))


def detect_estado_family(normalized_query: str, dataset: str) -> dict | None:
    """Scan a query for estado-family tokens and return the best match.

    Checks multi-word tokens first (e.g., 'en ejecucion'), then single tokens.
    Returns the first match found. Multiple matches are not allowed — a query
    can only belong to one estado family.

    This is designed to run BEFORE the existing estado extractor so the
    consumed span can be scrubbed from the query before object extraction.
    """
    # Check multi-word tokens first (longest match wins)
    for multi_token in sorted(FAMILY_TOKENS, key=len, reverse=True):
        if " " not in multi_token:
            continue
        if multi_token.lower() in normalized_query.lower():
            return resolve_estado(multi_token, dataset)

    # Then single-word tokens by position in query (first occurrence).
    # Punctuation glued to a word is not part of it: "suspendidos?" at the end
    # of a question named the state and was read as the topic "suspendidos".
    tokens = [token.strip(_TOKEN_PUNCTUATION) for token in normalized_query.split()]
    for token in tokens:
        result = resolve_estado(token, dataset)
        if result:
            return result

    return None
