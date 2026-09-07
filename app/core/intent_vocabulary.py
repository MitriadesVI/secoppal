"""
Intent Vocabulary — bidder intent classifier for SECOPPAL.

Detects high-level intent phrases in natural language queries and
maps them to parameter configurations. Runs BEFORE other extraction
so the consumed span can be scrubbed from the query.

Deterministic regex-based. No LLM.
"""

import re

COMMON_TOKENS = {
    # Originales (Sprint 2A)
    "servicios", "servicio", "general", "generales",
    "mantenimiento", "suministro", "suministros",
    "prestacion", "prestación", "apoyo", "gestion", "gestión",
    "contratacion", "contratación", "adquisicion", "adquisición",
    # Sprint 4 — MULTI-AND-OVER-RESTRICTIVE-001 (degradar AND→OR sobre tokens genéricos)
    "integral", "integrales",
    "soporte", "soportes", "apoyos",
    "obra", "obras", "publica", "publico", "pública", "público",
    "profesional", "profesionales",
    "asistencia", "asistencias", "tecnica", "técnica", "tecnicos", "técnicos",
    "proyecto", "proyectos", "actividad", "actividades",
    "nacional", "nacionales", "regional", "regionales",
    "suministro", "suministrar", "compra", "compras",
    "adecuacion", "adecuación", "mejoramiento", "reparacion", "reparación",
    "construccion", "construcción", "rehabilitacion", "rehabilitación",
}

# ── Acción de compra: quién la ejerce, no qué se compra ─────────────────
#
# Sprint 5 — PROCUREMENT-ACTION-001, revisado por HCA fresh.
#
# La lista original mezclaba dos cosas que se comportan distinto:
#
#   NOMINALIZACIONES ("contratación", "adquisición", "compras") sí pueden ser
#   el tema de una consulta: "oportunidades de contratación" pregunta por eso.
#   Por eso solo se descartan cuando la consulta trae además sustantivos
#   reales — la regla conservadora que ya existía.
#
#   FORMAS CONJUGADAS ("compró", "adquirió", "celebró", "suscribió") no pueden
#   serlo NUNCA. Un verbo conjugado no es un objeto contractual en ninguna
#   lectura del español; es el predicado cuyo sujeto es el comprador.
#
# Tratarlas igual producía dos fallos opuestos según qué más hubiera en la
# frase: "qué compró la alcaldía para salud" perdía el verbo por suerte (había
# un sustantivo), y "qué celebró la alcaldía en 2025" lo conservaba como tema y
# consultaba `LIKE '%celebro%'`. El criterio ahora es morfológico, no de
# catálogo: si está conjugado, no es tema.
#
# Los lemas se declaran completos. Deliberadamente FUERA: "pagar"/"pago" e
# "invertir"/"inversión", cuyas formas nominales son objetos contractuales
# frecuentes y cuya ambigüedad no compensa; y "contrató", que al normalizar
# pierde la tilde y colisiona con el sustantivo "contrato".
PROCUREMENT_ACTION_NOUNS: frozenset[str] = frozenset({
    "adquisicion", "adquisición", "adquisiciones",
    "compra", "compras", "comprar",
    "contratar", "contratacion", "contratación", "contrataciones",
    "contractual", "contractuales",
    "adquirir", "celebrar", "suscribir", "gastar",
})

PROCUREMENT_ACTION_VERB_FORMS: frozenset[str] = frozenset({
    # comprar
    "compro", "compró", "compraron", "compraba", "compraban",
    # adquirir
    "adquirio", "adquirió", "adquirieron", "adquiere", "adquieren",
    # celebrar  (DOGFOOD-G: "qué contratos celebraron")
    "celebro", "celebró", "celebraron", "celebra", "celebran",
    # suscribir
    "suscribio", "suscribió", "suscribieron", "suscribe", "suscriben",
    # gastar
    "gasto", "gastó", "gastaron", "gasta", "gastan",
    # firmar  (P1 Baranoa: "qué contratos firmó X en agosto")
    #
    # La familia estaba incompleta: sus hermanos semánticos —celebrar,
    # suscribir— ya estaban aquí y ``firmar`` no, así que "qué contratos firmó
    # la alcaldía" consultaba ``LIKE '%firmo%'`` sobre el objeto contractual y
    # devolvía cero por el filtro, no por la fuente.
    #
    # Deliberadamente FUERA "firma" y "firmadas/firmados": la primera colisiona
    # con el sustantivo ("firma digital", "firma electrónica", "la firma
    # contratista") por la misma razón documentada para "contrató", y los
    # participios ya los reclama INTENT_PATTERNS como señal de dataset.
    "firmo", "firmó", "firmaron", "firman", "firmaba", "firmaban",
})

# Compatibilidad: los consumidores que solo preguntan "¿es acción de compra?".
ACTION_VERBS: frozenset[str] = PROCUREMENT_ACTION_NOUNS | PROCUREMENT_ACTION_VERB_FORMS

# Una acción de compra en PASADO describe una compra ya ejecutada, y eso vive
# en contratos, no en procesos. Se comprueba con frontera de palabra: la tupla
# de señales de dataset usa `in`, y "compro" es subcadena de "comprobante".
PAST_PROCUREMENT_ACTION_RE = re.compile(
    r"\b(?:compr[oó]|compraron"
    r"|adquiri[oó]|adquirieron"
    r"|celebr[oó]|celebraron"
    r"|suscribi[oó]|suscribieron"
    r"|gast[oó]|gastaron)\b",
    re.IGNORECASE,
)

# ── Familia de intención de participación ────────────────────────────────
#
# "necesito participar" activaba la familia de oferta abierta y "necesito
# licitar" no. Es la misma intención —quiero presentarme a un proceso— con otro
# verbo, y la diferencia venía de que una alternancia del patrón enumeraba unos
# verbos y otra alternancia enumeraba otros.
#
# La familia se declara UNA vez y todas las ramas se construyen desde ella, así
# que ninguna puede volver a quedarse corta respecto de las demás.
BIDDER_PARTICIPATION_VERBS: tuple[str, ...] = (
    "presentarme", "presentarse", "presentar",
    "participar",
    "ofertar",
    "licitar",
    "postularme", "postularse", "postular",
    "aplicarme", "aplicar",
    "concursar",
)
_BIDDER_VERBS = "|".join(
    sorted((re.escape(verb) for verb in BIDDER_PARTICIPATION_VERBS), key=len, reverse=True)
)

# Each entry: (compiled_regex, intent_config_dict)
# intent_config can include:
#   estado_family: str — force a specific estado family
#   force_dataset: str — override dataset selection
#   filter_active_only: bool — add fecha_cierre >= today (future work)
#   consumed_span: str — override auto-detected span
INTENT_PATTERNS: list[tuple[re.Pattern, dict]] = [
    # ── "para presentarme" / "donde me pueda presentar" / "para poder presentarme" ──
    # CORPUS-003B: added quiero/necesito + verb, puedo variant of pueda
    (
        re.compile(
            rf"(?:para\s+(?:poder\s+)?(?:{_BIDDER_VERBS}))"
            rf"|(?:donde\s+(?:me\s+)?pued[ao]\s+(?:{_BIDDER_VERBS}))"
            rf"|(?:(?:quiero|necesito|quisiera|deseo)\s+(?:{_BIDDER_VERBS}))"
            rf"|(?:(?:me\s+)?puedo\s+(?:{_BIDDER_VERBS}))",
            re.IGNORECASE,
        ),
        {
            "estado_family": "oferta_abierta",
            "filter_active_only": True,
        },
    ),
    # ── "que ya se firmaron" / "contratos firmados" ──
    # Force dataset=contratos, NO aplicar estado_contrato (no hay campo "firmado" en SECOP)
    (
        re.compile(
            r"(?:que\s+ya\s+se\s+firmaron)"
            r"|(?:contratos?\s+(?:firmados?|celebrados?))"
            r"|(?:que\s+est[ée]n\s+firmados?)",
            re.IGNORECASE,
        ),
        {
            "force_dataset": "contratos",
        },
    ),
    # ── "contratos en ejecucion" / "ejecucion de contratos" ──
    (
        re.compile(
            r"(?:contratos?\s+en\s+ejecucion)"
            r"|(?:ejecucion\s+de\s+contratos?)",
            re.IGNORECASE,
        ),
        {
            "estado_family": "contrato_activo",
            "force_dataset": "contratos",
        },
    ),
    # ── "contratos liquidados" / "liquidados" ──
    (
        re.compile(r"contratos?\s+liquidados?", re.IGNORECASE),
        {
            "force_dataset": "contratos",
        },
    ),
    # ── "que se cayeron" / "que no procedieron" ──
    (
        re.compile(
            r"(?:que\s+se\s+(?:cayeron|cay[oó]|cay[oó]\s+el\s+proceso))"
            r"|(?:que\s+no\s+procedieron)"
            r"|(?:procesos?\s+(?:desiertos?|fallidos?))",
            re.IGNORECASE,
        ),
        {
            "estado_family": "no_procede",
        },
    ),
    # ── "oportunidades de X" → filler detection ──
    (
        re.compile(
            r"\boportunidades?\s+de\b",
            re.IGNORECASE,
        ),
        {
            "scrub_only": True,  # just remove the span, no filter config
        },
    ),
    # ── "que tiene X para presentarme" ──
    (
        re.compile(
            r"que\s+tiene\s+.+?\s+para\s+presentar(?:me)?",
            re.IGNORECASE,
        ),
        {
            "estado_family": "oferta_abierta",
            "filter_active_only": True,
        },
    ),
    # ── Commercial bidder intent (BIDDER-INTENT-003) ──
    # "quiero vender", "vendo", "ofrezco", "soy proveedor de"
    (
        re.compile(
            r"(?:quiero\s+)?(?:vender|vendemos|vendo)"
            r"|(?:ofrezco|ofrecer|ofrecemos|ofrecen)"
            r"|(?:comercializo|comercializar)"
            r"|(?:soy\s+proveedor(?:es)?\s+de)"
            r"|(?:tengo\s+.*\s+para\s+vender)",
            re.IGNORECASE,
        ),
        {
            "estado_family": "oferta_abierta",
            "force_dataset": "procesos",
        },
    ),
    # ── "que procesos en convocatoria o publicados tiene X" ──
    (
        re.compile(
            r"que\s+procesos\s+en\s+(?:convocatoria|publicados?|abiertos?|vigentes?)",
            re.IGNORECASE,
        ),
        {
            "estado_family": "oferta_abierta",
        },
    ),
    # ── "hay abiertas" / "hay abiertos" ──
    (
        re.compile(
            r"\bhay\s+(?:abiert[ao]s?|disponibles?|vigentes?|publicad[ao]s?)\b",
            re.IGNORECASE,
        ),
        {
            "estado_family": "oferta_abierta",
        },
    ),
]


def extract_intent(normalized_query: str) -> tuple[dict, str]:
    """Extract high-level intent from query.

    Returns (intent_config, consumed_span).
    intent_config is a dict with optional keys: estado_family, force_dataset,
    filter_active_only, scrub_only.
    consumed_span is the matched text to remove.

    Only the FIRST match is returned (one intent per query).
    """
    for pattern, config in INTENT_PATTERNS:
        match = pattern.search(normalized_query)
        if match:
            span = match.group(0).strip()
            return (dict(config), span)

    return ({}, "")
