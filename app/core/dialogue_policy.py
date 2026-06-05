"""Dialogue Policy — stateless classifier for SECOPPAL conversational signals.

Emits DialogueSignal instances that QueryPlan (B2) consumes to decide
whether to execute, modify params, or ask for clarification.

Pure function: same input → same output. No I/O, no SECOP, no DB.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date as Date, timedelta
from typing import Literal

# ── Signal kinds ─────────────────────────────────────────────────────────────

DialogueSignalKind = Literal[
    "command_incomplete",       # "filtra por departamento" sin valor
    "implicit_opportunity",     # "algún proceso para X" / "alguna oferta del Y"
    "discourse_filler",         # "hola, estoy interesado", quejas, verbos
    "temporal_month",           # "en mayo", "este mes", "el mes pasado"
    "temporal_range",           # "hoy", "ayer", "esta semana", "últimos N días"
    "number_phrase_scrub",      # "supere", "exceda" cuando ya hay valor numérico
    "city_mention_as_topic",    # token geográfico atrapado en objeto
    "non_query_followup",       # queja/comentario tras query previa
]


@dataclass
class DialogueSignal:
    kind: DialogueSignalKind
    payload: dict = field(default_factory=dict)
    confidence: Literal["high", "medium", "low"] = "high"
    source_tokens: list[str] = field(default_factory=list)


# ── Lexical catalogs ─────────────────────────────────────────────────────────

# R1: imperativos de filtrado sin valor
_CMD_PREFIX = r"^(filtra|filtrar|filtre|ordena|ordenar|mu[eé]strame|ord[eé]name)"

# R1: slots detectables (departamento, ciudad, etc.)
_SLOT_WORDS = {
    "departamento": "departamento",
    "departamento": "departamento",
    "ciudad": "ciudad",
    "entidad": "entidad",
    "valor": "valor",
    "modalidad": "modalidad",
    "fecha": "fecha",
}

_R1_SLOT_PATTERN = re.compile(
    r"^(?:filtra|filtrar|filtre)\s+por\s+(departamento|ciudad|entidad|valor|modalidad|fecha)$",
    re.IGNORECASE,
)
_R1_ORDER_PATTERN = re.compile(
    r"(?:ordena|ordenar|mu[eé]strame)\s+por\s*$",
    re.IGNORECASE,
)

# R2: implicit_opportunity triggers
_R2_PATTERNS = [
    re.compile(r"\balg[uú]n\s+proceso\b", re.IGNORECASE),
    re.compile(r"\balguna\s+oferta\b", re.IGNORECASE),
    re.compile(r"\bhay\s+alg[uú]n\b", re.IGNORECASE),
    re.compile(r"\bd[oó]nde\s+(?:me\s+)?(?:puedo|podr[ií]a)\s+presentar\b", re.IGNORECASE),
    re.compile(r"\bproceso[s]?\s+para\b", re.IGNORECASE),
    re.compile(r"\bd[oó]nde\s+(?:me\s+)?(?:puedo|podr[ií]a)\s+ofertar\b", re.IGNORECASE),
]

# R3: discourse_filler tokens — NOT contractual object
_COURTESY_TOKENS = frozenset({
    "hola", "buenas", "buenos", "gracias",
    "porfa", "porfavor", "favor",
})

_FILLER_PREFIXES = frozenset({
    "estoy", "estamos", "actualmente", "quisiera", "quisieramos",
    "me gustaría", "nos gustaría",
})

_QUEJA_TOKENS = frozenset({
    "nada", "terrible", "incorrecto", "mal", "malo", "fracaso",
    "no funcionó", "no funciono", "está mal", "esta mal",
    "no sirve", "no sirvió", "no sirvio",
})

_VAGUE_QUANTIFIERS = frozenset({
    "algun", "algún", "alguna", "algo de",
})

_INTERROGATIVE_VERBS = frozenset({
    "fueron", "fue", "son", "están", "estan",
    "tienes", "tiene", "tendrás", "tendrán", "tendrias", "tendría",
    "hay", "habrá", "haya",
    "puedes", "puede", "podrías", "podria", "pueden",
    "sabes", "sabe", "conoces", "conoce",
    "encuentro", "encuentra", "encuentran",
})

# R4: temporal month patterns
_MONTH_MAP: dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

_R4_MONTH_PATTERN = re.compile(
    r"\ben\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\b",
    re.IGNORECASE,
)
_R4_ESTE_MES = re.compile(r"\beste\s+mes\b", re.IGNORECASE)
_R4_MES_PASADO = re.compile(r"\bel\s+mes\s+pasado\b", re.IGNORECASE)
_R4_HACE_MESES = re.compile(r"\bhace\s+(\d+)\s+meses\b", re.IGNORECASE)

# R4b: temporal week/day patterns
_R4_HOY = re.compile(r"\bhoy\b", re.IGNORECASE)
_R4_AYER = re.compile(r"\bayer\b", re.IGNORECASE)
_R4_ESTA_SEMANA = re.compile(r"\besta\s+semana\b", re.IGNORECASE)
_R4_SEMANA_PASADA = re.compile(r"\b(?:la\s+)?semana\s+pasada\b", re.IGNORECASE)
_R4_ULTIMOS_DIAS = re.compile(r"\b(?:ultimos|últimos)\s+(\d+)\s+(?:dias|días)\b", re.IGNORECASE)

# R5: number phrase scrub catalog
SCRUB_CATALOG = frozenset({
    "supere", "exceda", "supera", "alcance", "rebase", "llegue",
    "baje", "suba", "cueste", "valga",
    "mayores", "menores", "mayor", "menor", "sea", "sean",
    "en", "desde", "hasta", "durante",
    # Filler articles that appear with number phrases
    "los", "las", "el", "la", "un", "una", "unos", "unas",
})

# R6: SECOP contract nouns for implicit_opportunity cross-check
_SECOP_NOUNS = frozenset({
    "proceso", "procesos", "contrato", "contratos",
    "oferta", "ofertas", "licitacion", "licitación",
    "licitaciones", "convocatoria", "convocatorias",
    "mantenimiento", "suministro", "obra", "obras",
    "consultoria", "consultoría", "asesoria", "asesoría",
    "prestacion", "prestación", "servicio", "servicios",
    "compra", "compras", "adquisicion", "adquisición",
})


# ── Classifier ───────────────────────────────────────────────────────────────

def classify(
    raw_query: str,
    params: dict,
    context: dict | None = None,
) -> list[DialogueSignal]:
    """Emit 0..N dialogue signals from the raw query and parsed params.

    Stateless. No SECOP calls. No mutation of inputs.

    Args:
        raw_query: original user text (unaltered).
        params: parsed params dict from query_router.
        context: optional dict with keys:
            - prev_params: dict | None
            - prev_intent_type: str | None
            - turn_index: int
            - today: date | None (defaults to date.today())
    """
    ctx = context or {}
    today = ctx.get("today") or Date.today()
    prev_params = ctx.get("prev_params")
    turn_index = ctx.get("turn_index", 0)

    query_lower = raw_query.strip().lower()
    signals: list[DialogueSignal] = []

    # ── R1: command_incomplete ──
    _detect_command_incomplete(query_lower, signals)

    # ── R2: implicit_opportunity ──
    _detect_implicit_opportunity(query_lower, signals)

    # ── R3: discourse_filler + non_query_followup ──
    _detect_discourse_filler(query_lower, prev_params, turn_index, signals)

    # ── R4: temporal_month ──
    _detect_temporal_month(query_lower, today, signals)

    # ── R4b: temporal_range (week/day) ──
    _detect_temporal_range(query_lower, today, signals)

    # ── R5: number_phrase_scrub ──
    _detect_number_phrase_scrub(params, signals)

    # ── R6: city_mention_as_topic ──
    _detect_city_mention_as_topic(query_lower, params, signals)

    return signals


# ── Rule detectors ── (private, in evaluation order) ─────────────────────────

def _detect_command_incomplete(query_lower: str, signals: list[DialogueSignal]) -> None:
    m = _R1_SLOT_PATTERN.search(query_lower)
    if m:
        signals.append(DialogueSignal(
            kind="command_incomplete",
            payload={"slot": m.group(1)},
            source_tokens=[m.group(0)],
        ))
        return

    if _R1_ORDER_PATTERN.search(query_lower):
        tokens = query_lower.split()
        signals.append(DialogueSignal(
            kind="command_incomplete",
            payload={"slot": "criterio"},
            source_tokens=tokens,
        ))


def _detect_implicit_opportunity(query_lower: str, signals: list[DialogueSignal]) -> None:
    # Historical markers suppress implicit_opportunity
    if re.search(r"\b(año|ano)\s+pasado\b", query_lower):
        return
    if re.search(r"\b(?:el\s+)?(?:año|ano)\s+(?:anterior|202[0-5])\b", query_lower):
        return
    if re.search(r"\b(?:del|en\s+el)\s+202[0-5]\b", query_lower):
        return

    for pat in _R2_PATTERNS:
        m = pat.search(query_lower)
        if m:
            signals.append(DialogueSignal(
                kind="implicit_opportunity",
                payload={"trigger_phrase": m.group(0).strip()},
                source_tokens=[m.group(0)],
            ))
            return


def _detect_discourse_filler(
    query_lower: str,
    prev_params: dict | None,
    turn_index: int,
    signals: list[DialogueSignal],
) -> None:
    import re as _re
    tokens = _re.split(r"[\s,;.!¿?¡]+", query_lower)
    tokens = [t for t in tokens if t]  # remove empty strings
    tokens_to_scrub: list[str] = []

    for t in tokens:
        if t in _COURTESY_TOKENS:
            tokens_to_scrub.append(t)
        elif t in _VAGUE_QUANTIFIERS:
            tokens_to_scrub.append(t)
        elif t in _INTERROGATIVE_VERBS:
            tokens_to_scrub.append(t)

    # Check filler prefixes (multi-word)
    for prefix in _FILLER_PREFIXES:
        if query_lower.startswith(prefix):
            for w in prefix.split():
                if w not in tokens_to_scrub:
                    tokens_to_scrub.append(w)

    # "por favor" → scrub both tokens even though individually innocent
    if re.search(r"\bpor\s+favor\b", query_lower):
        for w in ("por", "favor"):
            if w in tokens and w not in tokens_to_scrub:
                tokens_to_scrub.append(w)

    # "estoy interesado en" → scrub "estoy", "interesado"
    if re.search(r"\bestoy\s+interesad[oa]\b", query_lower):
        for w in ("estoy", "interesado", "interesada", "en"):
            if w in tokens and w not in tokens_to_scrub:
                tokens_to_scrub.append(w)

    if tokens_to_scrub:
        signals.append(DialogueSignal(
            kind="discourse_filler",
            payload={"tokens_to_scrub": tokens_to_scrub},
            source_tokens=tokens_to_scrub,
        ))

    # R3b — non_query_followup: queja tras query previa
    if prev_params and turn_index >= 1:
        complaint_hits = [t for t in tokens if t in _QUEJA_TOKENS]
        # "no funcionó" / "no sirvió" → multi-word
        for phrase in ("no funcionó", "no funciono", "no sirve", "no sirvió", "no sirvio"):
            if phrase in query_lower:
                complaint_hits.append(phrase)

        if complaint_hits and not _has_secop_nouns(query_lower):
            signals.append(DialogueSignal(
                kind="non_query_followup",
                payload={"reason": "complaint_after_query", "hits": complaint_hits},
                source_tokens=complaint_hits,
            ))


def _detect_temporal_month(
    query_lower: str,
    today: Date,
    signals: list[DialogueSignal],
) -> None:
    m = _R4_MONTH_PATTERN.search(query_lower)
    if m:
        month_name = m.group(1).lower()
        month_num = _MONTH_MAP[month_name]
        signals.append(DialogueSignal(
            kind="temporal_month",
            payload={"month": month_num, "year": today.year, "phrase": m.group(0)},
            source_tokens=[m.group(0)],
        ))
        return

    if _R4_ESTE_MES.search(query_lower):
        signals.append(DialogueSignal(
            kind="temporal_month",
            payload={"month": today.month, "year": today.year, "phrase": "este mes"},
        ))
        return

    if _R4_MES_PASADO.search(query_lower):
        # Previous month
        if today.month == 1:
            month, year = 12, today.year - 1
        else:
            month, year = today.month - 1, today.year
        signals.append(DialogueSignal(
            kind="temporal_month",
            payload={"month": month, "year": year, "phrase": "el mes pasado"},
        ))
        return

    m = _R4_HACE_MESES.search(query_lower)
    if m:
        n = int(m.group(1))
        total_months = today.month - n
        if total_months <= 0:
            year = today.year - 1
            month = 12 + total_months
        else:
            year = today.year
            month = total_months
        signals.append(DialogueSignal(
            kind="temporal_month",
            payload={"month": month, "year": year, "phrase": m.group(0)},
        ))


def _detect_temporal_range(
    query_lower: str,
    today: Date,
    signals: list[DialogueSignal],
) -> None:
    """Emit kind='temporal_range' for week/day relative expressions.

    Semantics:
      - "hoy"                       → desde = hasta = today
      - "ayer"                      → desde = hasta = today - 1
      - "esta semana"               → desde = lunes ISO, hasta = today
      - "la semana pasada" / "semana pasada" → desde = lunes previo, hasta = domingo previo
      - "últimos N días"            → desde = today - N, hasta = today
    """
    if _R4_HOY.search(query_lower):
        iso = today.isoformat()
        signals.append(DialogueSignal(
            kind="temporal_range",
            payload={"desde": iso, "hasta": iso, "phrase": "hoy"},
        ))
        return

    if _R4_AYER.search(query_lower):
        yesterday = today - timedelta(days=1)
        iso = yesterday.isoformat()
        signals.append(DialogueSignal(
            kind="temporal_range",
            payload={"desde": iso, "hasta": iso, "phrase": "ayer"},
        ))
        return

    if _R4_ESTA_SEMANA.search(query_lower):
        # ISO weekday: 1=Monday, 7=Sunday
        weekday = today.isocalendar().weekday
        monday = today - timedelta(days=weekday - 1)
        signals.append(DialogueSignal(
            kind="temporal_range",
            payload={
                "desde": monday.isoformat(),
                "hasta": today.isoformat(),
                "phrase": "esta semana",
            },
        ))
        return

    if _R4_SEMANA_PASADA.search(query_lower):
        weekday = today.isocalendar().weekday
        this_monday = today - timedelta(days=weekday - 1)
        last_monday = this_monday - timedelta(days=7)
        last_sunday = last_monday + timedelta(days=6)
        signals.append(DialogueSignal(
            kind="temporal_range",
            payload={
                "desde": last_monday.isoformat(),
                "hasta": last_sunday.isoformat(),
                "phrase": "semana pasada",
            },
        ))
        return

    m = _R4_ULTIMOS_DIAS.search(query_lower)
    if m:
        n = int(m.group(1))
        if 1 <= n <= 365:
            desde = today - timedelta(days=n)
            signals.append(DialogueSignal(
                kind="temporal_range",
                payload={
                    "desde": desde.isoformat(),
                    "hasta": today.isoformat(),
                    "phrase": m.group(0),
                },
            ))


def _detect_number_phrase_scrub(
    params: dict,
    signals: list[DialogueSignal],
) -> None:
    has_value = bool(
        params.get("valor_max") or params.get("valor_min")
    )
    if not has_value:
        return

    objeto: list[str] = params.get("objeto", [])
    if not objeto:
        return

    tokens_to_scrub = [t for t in objeto if not isinstance(t, list) and t.lower() in SCRUB_CATALOG]
    if tokens_to_scrub:
        signals.append(DialogueSignal(
            kind="number_phrase_scrub",
            payload={"tokens_to_scrub": tokens_to_scrub},
            source_tokens=tokens_to_scrub,
        ))


def _detect_city_mention_as_topic(
    query_lower: str,
    params: dict,
    signals: list[DialogueSignal],
) -> None:
    # Only emit if ciudad/resolved department are NOT already set
    if params.get("ciudad") or params.get("departamento_resolved"):
        return

    objeto: list[str] = params.get("objeto", [])
    if not objeto:
        return

    from app.core.entity_resolver import geo_mention_resolution

    for token in objeto:
        if isinstance(token, list):
            continue
        gm = geo_mention_resolution(token)
        if gm.kind == "city":
            signals.append(DialogueSignal(
                kind="city_mention_as_topic",
                payload={"city": gm.canonical, "token_in_objeto": token},
                source_tokens=[token],
            ))
            return  # only first city match


def _has_secop_nouns(query_lower: str) -> bool:
    """True if the query contains SECOP contract-related nouns."""
    return any(noun in query_lower for noun in _SECOP_NOUNS)
