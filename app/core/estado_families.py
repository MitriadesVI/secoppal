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
            "estado_de_apertura_del_proceso": ["Abierto"],
            "estado_del_procedimiento": [
                "Publicado",
                "Borrador",
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
            ],
        },
        "contratos": {
            "estado_contrato": [
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
    "borrador": "oferta_abierta",
    "borradores": "oferta_abierta",
    "vigente": "oferta_abierta",
    "vigentes": "oferta_abierta",
    "disponible": "oferta_abierta",
    "disponibles": "oferta_abierta",
    # ── multi-word tokens ──
    "en convocatoria": "oferta_abierta",
    "en borrador": "oferta_abierta",
    # ── contrato_activo ──
    # Nota: "firmado/firmados" NO están aquí. "Firmado" no tiene estado_contrato
    # en SECOP; solo fuerza dataset='contratos' vía intent_vocabulary.py.
    "ejecucion": "contrato_activo",
    "ejecutando": "contrato_activo",
    "en ejecucion": "contrato_activo",
    "activo": "contrato_activo",
    "activos": "contrato_activo",
    "vigente": "contrato_activo",       # en dataset procesos
    "vigentes": "contrato_activo",
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
    family_name = NATURAL_TO_FAMILY.get(token_lower)
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
    for multi_token in sorted(NATURAL_TO_FAMILY, key=len, reverse=True):
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
