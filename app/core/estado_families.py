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
            # "oferta_abierta" doesn't apply to contracts — contracts mean it's signed
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
    "no_procede": {
        "procesos": {
            "estado_de_apertura_del_proceso": [],  # not necessarily closed
            "estado_del_procedimiento": [
                "Cancelado",
                "Suspendido",
            ],
        },
        "contratos": {
            "estado_contrato": [
                "Cancelado",
                "Suspendido",
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
    "desierto": "no_procede",
    "desierta": "no_procede",
    "cancelado": "no_procede",
    "cancelados": "no_procede",
    "cancelada": "no_procede",
    "suspendido": "no_procede",
    "suspendidos": "no_procede",
    "suspendida": "no_procede",
    "revocado": "no_procede",
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

DATASET_AWARE_FAMILY: dict[str, dict[str, str]] = {
    token: {"procesos": "oferta_abierta", "contratos": "contrato_activo"}
    for token in LIVENESS_TOKENS
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
    dataset_aware = DATASET_AWARE_FAMILY.get(token_lower)
    family_name = (
        dataset_aware.get(dataset)
        if dataset_aware is not None
        else NATURAL_TO_FAMILY.get(token_lower)
    )
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

    return {
        "family": family_name,
        "filters": filters,
        "consumed_span": natural_token,
        "kind": "family",
    }


def detect_estado_family(normalized_query: str, dataset: str) -> dict | None:
    """Scan a query for estado-family tokens and return the best match.

    Checks multi-word tokens first (e.g., 'en ejecucion'), then single tokens.
    Returns the first match found. Multiple matches are not allowed — a query
    can only belong to one estado family.

    This is designed to run BEFORE the existing estado extractor so the
    consumed span can be scrubbed from the query before object extraction.
    """
    # Check multi-word tokens first (longest match wins)
    known_tokens = {*NATURAL_TO_FAMILY, *LEXICAL_EXACT_STATE, *DATASET_AWARE_FAMILY}
    for multi_token in sorted(known_tokens, key=len, reverse=True):
        if " " not in multi_token:
            continue
        if multi_token.lower() in normalized_query.lower():
            return resolve_estado(multi_token, dataset)

    # Then single-word tokens by position in query (first occurrence)
    tokens = normalized_query.split()
    for token in tokens:
        result = resolve_estado(token, dataset)
        if result:
            return result

    return None
