"""
Intent Vocabulary — bidder intent classifier for SECOPPAL.

Detects high-level intent phrases in natural language queries and
maps them to parameter configurations. Runs BEFORE other extraction
so the consumed span can be scrubbed from the query.

Deterministic regex-based. No LLM.
"""

import re

# Each entry: (compiled_regex, intent_config_dict)
# intent_config can include:
#   estado_family: str — force a specific estado family
#   force_dataset: str — override dataset selection
#   filter_active_only: bool — add fecha_cierre >= today (future work)
#   consumed_span: str — override auto-detected span
INTENT_PATTERNS: list[tuple[re.Pattern, dict]] = [
    # ── "para presentarme" / "donde me pueda presentar" / "para ofertar" ──
    (
        re.compile(
            r"(?:para\s+(?:presentar(?:me)?|participar|ofertar|postular(?:me)?|aplicar(?:me)?))"
            r"|(?:donde\s+(?:me\s+)?pueda\s+(?:presentar|participar|ofertar|postular|aplicar))",
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
