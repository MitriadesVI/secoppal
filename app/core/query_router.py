from __future__ import annotations

import calendar
import re
from pathlib import Path
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from app.data.departamentos import DEPARTAMENTOS
from app.data.estados import (
    ESTADO_APERTURA_SYNONYMS,
    ESTADO_CONTRATO_SYNONYMS,
    ESTADO_PROCEDIMIENTO_SYNONYMS,
    ESTADO_SYNONYMS,
)
from app.config import actor_profile_enabled
from app.core.actor_conversation import (
    ActorConversationInstruction,
    ActorConversationOperation,
    interpret_actor_conversation,
)
from app.core.actor_identity import has_plausible_actor_document_digits
from app.core.actor_intent import (
    detect_asserted_actor_name_before_document,
    detect_actor_name,
    recognized_actor_modifier_tails,
    reference_from_document,
)
from app.core.contract_invariants import contains_signed_token
from app.core.contract_type_resolver import resolve_contract_types
from app.core.money_resolution import CARDINAL_WORD_PATTERN
from app.core.negation_binding import (
    NegationDimension,
    NegationRequest,
    NegationVerdict,
    attach_scope_anchors,
    bind_negations,
)
from app.core.modality_parser_integration import resolve_parser_modality
from app.core.query_text_consumption import mask_consumed_spans
from app.utils.money import (
    SMMLV_UNIT_PATTERN,
    is_smmlv_expression,
    money_to_cop,
    normalize_text,
    smmlv_to_cop,
)
from app.data.smmlv import current_colombian_year, smmlv_cop
from app.utils.spell_correction import correct_query

import logging

logger = logging.getLogger(__name__)

# Import bigram registry from morphological variants dict (created in 1.2.c).
# If the module doesn't exist yet, KNOWN_BIGRAMS is empty — no bigram detection.
try:
    from app.data.morphological_variants import KNOWN_BIGRAMS, VARIANT_TO_ROOT
except ImportError as exc:
    logger.warning(
        "Could not import morphological_variants; bigram detection and variant expansion disabled: %s",
        exc,
    )
    KNOWN_BIGRAMS: frozenset[tuple[str, ...]] = frozenset()
    VARIANT_TO_ROOT: dict[str, str] = {}

if TYPE_CHECKING:
    from app.core.contract_type_resolver import ContractTypeInterpretation
    from app.core.entity_resolver import EntityResolver
    from app.core.modality_frame_adapter import ModalityInterpretation

DATASET_PROCESOS = "procesos"
DATASET_CONTRATOS = "contratos"

# Dual-dataset: el usuario pide procesos Y contratos en el mismo turno.
# Conjunción explícita ("contratos o procesos", "procesos y contratos") o un
# marcador de cobertura total ("ambos", "todo lo relacionado con").
_DUAL_DATASET_PATTERNS = (
    r"\bcontratos?\s+(?:o|y|e)\s+procesos?\b",
    r"\bprocesos?\s+(?:o|y|e)\s+contratos?\b",
    r"\bambos\b",
    r"\btodo lo relacionado\b",
)

_ORDERING_VALOR_RE = re.compile(
    r"""
    \b(?:mayor|mayores)\s+valor\b
    |\b(?:mayor|mayores)\s+cuant(?:ia|ía)\b
    |\blos\s+de\s+mayor\s+valor\b
    |\blos\s+m[áa]s\s+grandes\s+por\s+valor\b
    |\bm[áa]s\s+valor\b
    |\bm[áa]s\s+(?:caro|caros|cara|caras|costoso|costosos|costosa|costosas)\b
    |\bm[áa]s\s+(?:grande|grandes|importante|importantes|alto|altos|alta|altas)\b
    |\bordenar\s+por\s+valor\b
    |\bordenar\s+por\s+monto\b
    |\bde\s+mayor\s+a\s+menor\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

_ORDERING_FECHA_RE = re.compile(
    r"""
    \bm[áa]s\s+recientes?\b
    |\b[uú]ltimos?\b
    |\b(?:reciente|recientes)\b
    |\bordenar\s+por\s+fecha\b
    |\b(?:nuevo|nuevos|nueva|nuevas|actual|actuales)\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# NIT colombiano: 8-10 dígitos base, separadores opcionales (punto/guion/espacio),
# DV opcional al final. Lookahead negativo para no confundir con montos.
# Formatos: 800091140 / 800.091.140 / 800.091.140-4 / 8000911404
_NIT_SHAPE_RE = re.compile(
    rf"""
    (?<!\d)                       # no precedido de dígito
    (\d{{1,3}}                      # bloque inicial 1-3 dígitos
    (?:[.\- ]\d{{3}}){{1,3}}          # uno a tres grupos de 3 dígitos con separador
    (?:[.\- ]\d)?                 # DV opcional con separador
    |\d+)                             # o secuencia continua; el rango se valida
    (?!\d)                        # no seguido de dígito
    (?!\s*(?:millones?|millon|mil\s+millones|billones?|palos?|barras?|pesos|{SMMLV_UNIT_PATTERN}))  # no es monto
    """,
    re.IGNORECASE | re.VERBOSE,
)

_MONEY_UNIT_AFTER_NUMBER_RE = re.compile(
    rf"^\s*(?:cop\b|pesos?\b|millones?\b|millon\b|mil\s+millones\b|"
    rf"billones?\b|palos?\b|barras?\b|{SMMLV_UNIT_PATTERN})",
    re.IGNORECASE,
)
_MONEY_COMPARATOR_BEFORE_NUMBER_RE = re.compile(
    r"(?:\$\s*|\bcop\s+|\b(?:mas|menos)\s+de\s+|"
    r"\b(?:mayor(?:es)?|menor(?:es)?|superior(?:es)?|inferior(?:es)?)\s+(?:a|de|que)\s+|"
    r"\b(?:desde|hasta|por)\s+)\s*$",
    re.IGNORECASE,
)
_IDENTIFIER_BEFORE_NUMBER_RE = re.compile(
    r"\b(?:nit|c[eé]dulas?|c\.c\.|cc|documento(?:\s+de\s+identidad)?|"
    r"contratista|proveedor)\s*(?:n(?:umero|ro)?\.?\s*)?$",
    re.IGNORECASE,
)


def _number_role(text: str, match: re.Match[str]) -> str:
    """Clasifica una cifra con forma de NIT usando evidencia semántica local.

    Una forma de 8-11 dígitos es identificador por defecto, preservando las
    búsquedas históricas por NIT desnudo. Evidencia monetaria explícita (moneda,
    unidad o comparador) desplaza ese default; una etiqueta de identificador
    explícita tiene precedencia. Así, la forma por sí sola no se convierte en
    una segunda autoridad monetaria.
    """
    left = text[max(0, match.start() - 48):match.start()]
    right = text[match.end():match.end() + 32]
    if _IDENTIFIER_BEFORE_NUMBER_RE.search(left):
        return "identifier"
    if not has_plausible_actor_document_digits(match.group(0)):
        return "other"
    # Short documents overlap ordinary quantities too heavily to authorize an
    # actor by shape alone.  They need the explicit local evidence above.
    if len(re.sub(r"[^0-9]", "", match.group(0))) < 8:
        return "other"
    if _MONEY_UNIT_AFTER_NUMBER_RE.search(right):
        return "money"
    if _MONEY_COMPARATOR_BEFORE_NUMBER_RE.search(left):
        return "money"
    return "identifier"

STOPWORDS = {
    # Conversational verbs
    "muestrame", "muestreme", "muestra", "mostrar", "mostra",
    "busca", "buscar", "buscame", "buscate", "buscando", "busco",
    "mirame", "averigua", "revisa", "cambia",
    "dame", "dime", "necesito", "quiero", "quisiera",
    "encuentra", "encontrar", "encontrame", "encontra",
    "lista", "listar", "listame",
    "consulta", "consultar", "consultame",
    "traeme", "traer", "trae",
    "decime", "digame",
    # Questions
    "cuales", "cuantos", "cuantas", "cual",
    "hay", "tiene", "tienen", "esta", "estan", "este", "esten",
    "sea", "sean",
    "encuentre", "encuentren",
    "donde", "como", "cuando",
    # Greetings (including variants with extra letters)
    "hola", "holaa", "holaaa", "buenas", "buenass", "bueno", "buenos", "ey", "oye", "ok", "okay",
    # Courtesy
    "gracias", "favor", "podrias", "puedes", "puede", "porfa",
    # Bidder intent noise
    "poder", "pueda", "puedo", "puedan",
    # Commercial bidder verbs (BIDDER-INTENT-003)
    "vender", "vendo", "vendemos", "venderle", "venderles",
    "ofrezco", "ofrecer", "ofrecemos", "ofrecen",
    # Bidder opportunity signals (not contractual topics — RATED-016)
    "oferta", "ofertas",
    # Institutional labels (not contractual topics — RATED-041)
    "oficial",
    # OR-expansion noise (not contractual topics — RATED-067)
    "similares", "similar",
    "comercializo", "comercializar", "comercializamos",
    "proveedor", "proveedores",
    # States (extracted separately)
    "abierta", "abiertas", "abierto", "abiertos",
    "adjudicado", "adjudicados",
    "celebrado", "celebrados",
    "firmado", "firmados", "firmada", "firmadas",
    "liquidado", "liquidados",
    "cerrado", "cerrados",
    # Search types
    "contrato", "contratos", "convocatoria", "convocatorias",
    "licitacion", "licitaciones", "concurso",
    "proceso", "procesos",
    "publica", "publico",
    # Articles & prepositions
    "de", "del", "en", "el", "la", "las", "los",
    "por", "para", "que", "se", "un", "una", "con", "excepto", "sin",
    "pero", "sino", "ni", "salvo",
    # Comparators
    "mayor", "mayores", "menor", "menores",
    "superior", "superiores", "supere", "superen",
    "exceda", "excedan", "rebase", "rebasen",
    "mas", "menos",
    "no", "maximo", "máximo", "minimo", "mínimo",
    # Money units
    "pesos", "millones", "millon", "smmlv", "smlmv", "smlv",
    "salario", "salarios", "minimo", "minimos",
    # Filler
    "tipo", "tema", "temas", "asunto", "asuntos", "materia", "sector",
    "area", "área", "linea", "línea", "relacionado", "relacionados",
    "vigente", "vigentes", "sobre", "todos", "todas", "todo", "toda",
    # Temporal adverbs (not contractual objects)
    "actualmente", "ahora", "hoy", "recientemente", "momento",
    # Temporal week/day (not contractual objects)
    "semana", "dia", "dias", "día", "días", "ayer",
    "mes", "meses",
    # Interrogative verbs ("qué ha hecho/hizo/gastó X") — no son objetos
    "hecho", "hizo", "hace", "hacen", "haciendo", "gastado", "tenido",
    # Indefinite pronouns
    "algun", "alguna", "algunos", "algunas", "alguno", "algunos", "ningun", "ninguna",
    "otro", "otra", "otros", "otras",
    # Demonstrative pronouns / anaphoric refinement words.
    # Aparecen en follow-ups tipo "muestrame solo aquellos que sean mas de X".
    # Sin scrub, contaminan 'objeto' y borran el topic heredado.
    "solo", "solos", "sola", "solas", "solamente", "unicamente", "únicamente",
    # Follow-up refinement verbs — "filtra por X", "aplicá filtro", "ponelo entre X y Y"
    "filtra", "filtrar", "filtrame", "filtrarme", "filtrado", "filtrados",
    "aplica", "aplicar", "aplicale", "aplicame",
    "pon", "pone", "poner", "ponelo", "ponle", "poneme",
    "deja", "dejar", "dejame",
    "limita", "limitar", "limitame", "limitalo",
    "acota", "acotar", "acotame", "acotalo",
    "restringe", "restringir", "restrinjame",
    "aquel", "aquella", "aquellos", "aquellas",
    "eso", "esos", "esa", "esas",
    "esto", "estos", "estas",  # nota: "esta"/"estan"/"este"/"esten" ya están arriba
    "mismo", "misma", "mismos", "mismas",
    # Value/size adjectives (ordering instructions, not objects)
    "caro", "caros", "cara", "caras",
    "costoso", "costosos", "costosa", "costosas",
    "grande", "grandes", "importante", "importantes",
    "barato", "baratos", "barata", "baratas",
    "reciente", "recientes", "ultimo", "ultimos", "ultima", "ultimas",
    "nuevo", "nuevos", "nueva", "nuevas",
    "mejor", "mejores", "peor", "peores",
    "alto", "altos", "alta", "altas",
    "bajo", "bajos", "baja", "bajas",
    "valor", "cuantia", "cuantía",
    # Months (not contractual objects)
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
    # More temporal
    "semestre", "trimestre", "bimestre", "periodo", "anual", "mensual",
    "ano", "año",
    "pasado", "pasada", "anterior", "siguiente", "proximo",
    # Semantic-layer scrubbed terms (should already be consumed, safety net)
    "departamento", "departamentos",
    "oportunidades", "oportunidad",
    "publicados", "publicadas",
    "presentar", "presentarme", "presentarse",
    "licitar",
    "postular", "postularme", "postularse",
    "aplicar", "aplicarme", "aplicarse",
    "participar", "ofertar", "competir", "concursar",
    "pueda", "puedas", "puedan", "puedo",
    "tenga", "tengan", "tiene", "tienen",  # "que tiene la alcaldia..."
    "provincia",  # sometimes used as synonym for departamento
    # Conversational filler confirmed by feedback audit (1.2.a)
    "sido", "han", "entre",  # rango "entre X y Y"
    "convocadas", "convocado", "convocados",
    "disponibles", "disponible",
    "ver", "mirar", "revisar",
    "conocer", "saber", "identificar",
    "existe", "existen",
    "ofrecen", "ofrece",
    # CORPUS-AN003: verbos/cuantificadores analíticos. No son objeto
    # contractual; son señal de pregunta agregada ("cuánto suma/n", "cuántos
    # contratos hay", "total contratado"). El detector analítico vive en
    # analytics.py; aquí solo se previene que contaminen 'objeto'.
    "cuanto", "cuanta",
    "suma", "suman", "sumaron", "sumo", "sumó",
    "total", "totales",
    # Sprint 5 — VALUE-POLARITY residual scrub:
    # "por debajo de X" → polarity already sets valor_max, but "debajo"
    # leaks into objeto when "por" is consumed as stopword earlier.
    "debajo",
    # PARSER-POLARITY-MONETARY-002: post-number polarity tokens
    "adelante", "arriba", "abajo",
}

MONTH_MAP = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

# ── Cities / municipalities ──────────────────────────────────────────────────
_COMMON_CITIES = frozenset({
    "barranquilla", "soledad", "cartagena", "bogota", "bogotá",
    "medellin", "medellín", "cali", "santa marta", "sincelejo",
    "monteria", "montería", "valledupar", "bucaramanga", "cucuta", "cúcuta",
    "ibague", "ibagué", "pereira", "manizales", "armenia",
    "neiva", "popayan", "popayán", "pasto", "tunja",
    "villavicencio", "yopal", "quibdo", "quibdó", "leticia",
    "riohacha", "san andres", "san andrés", "mocoa",
    "inirida", "inírida", "mitú", "puerto carreño", "arauca",
})

# Mapa de ciudad a entidad preferida (cuando no hay otra entidad explícita)
_CITY_TO_ENTITY: dict[str, str] = {
    "barranquilla": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
}

# Regex para detectar "en/de/para ciudad" y "ciudad" sola
_CITY_PATTERN_RE = re.compile(
    r"\b(?:en\s+|de\s+|para\s+)?(barranquilla|soledad|cartagena|bogot[áa]"
    r"|medell[íi]n|cali|santa\s+marta|sincelejo|monter[íi]a|valledupar"
    r"|bucaramanga|c[uú]cuta|ibagu[ée]|pereira|manizales|armenia"
    r"|neiva|popay[áa]n|pasto|tunja"
    r"|villavicencio|yopal|quibd[óo]|leticia"
    r"|riohacha|san\s+andr[eé]s|mocoa"
    r"|in[íi]rida|mit[uú]|puerto\s+carreño|arauca"
    r"|chiriguana|chiriguaná)\b",
    re.IGNORECASE,
)

_ENTITY_SIGNAL_WORDS = {
    "secretaria", "ministerio", "instituto", "corporacion", "fundacion",
    "agencia", "unidad", "autoridad", "comision",
    "superintendencia", "direccion", "servicio", "empresa",
}

_OR_ONLY_PHRASES: dict[tuple[str, ...], str] = {
    ("primera", "infancia"): "primera_infancia",
}

_PREPOSITION_PLACE_RE = re.compile(
    r"\ben\s+([a-z]{4,}(?:\s+[a-z]{4,})*)",
    re.IGNORECASE,
)

# ── Protección de años dentro de entidades jurídicas (BUG-001) ─────────────
_ENTITY_NUMBER_RE = re.compile(
    r"\b(?:fundaci[oó]n|fundacion|corporaci[oó]n|corporacion|"
    r"asociaci[oó]n|asociacion|empresa|uni[oó]n\s+temporal|"
    r"union\s+temporal|consorcio|cooperativa)\s+(20\d{2})\b",
    re.IGNORECASE,
)


def _protected_entity_year_spans(text: str) -> list[tuple[int, int]]:
    """Retorna spans de años que hacen parte de nombres jurídicos, no fechas."""
    return [m.span(1) for m in _ENTITY_NUMBER_RE.finditer(text)]


def _inside_spans(start: int, end: int, spans: list[tuple[int, int]]) -> bool:
    return any(start >= s and end <= e for s, e in spans)


@dataclass(slots=True)
class ParsedQuery:
    params: dict
    needs_llm: bool
    route_reason: str
    dataset_explicit: bool = False
    consumed_spans: list[str] | None = None
    modality_interpretation: ModalityInterpretation | None = None
    contract_type_interpretation: ContractTypeInterpretation | None = None
    negation_requests: tuple[NegationRequest, ...] = ()
    positive_text: str = ""


class QueryRouter:
    """
    Deterministic parser for SECOP queries.

    Uses GAZETTEER-FIRST entity detection: scans text for known entity/department
    aliases before any regex extraction. This eliminates the ambiguity problem
    where "gobernación de santander" gets split between entity and department.
    """

    _DEFAULT_ALIAS_DB = Path(__file__).parent.parent / "data" / "aliases_db.json"

    def __init__(self, entity_resolver: EntityResolver | None = None):
        if entity_resolver is None and self._DEFAULT_ALIAS_DB.exists():
            from app.core.entity_resolver import EntityResolver as _ER
            entity_resolver = _ER(self._DEFAULT_ALIAS_DB)
        self.entity_resolver = entity_resolver

    def parse(self, user_query: str) -> ParsedQuery:
        normalized = normalize_text(user_query)
        normalized = correct_query(normalized)
        params: dict[str, object] = {}
        actor_conversation = ActorConversationInstruction()
        actor_relation_spans: tuple[str, ...] = ()

        # ── 0. Semantic normalization layer (runs BEFORE existing extractors) ──
        #    This prevents tokens like "publicados", "departamento", "alcaldía"
        #    from leaking into 'objeto' and poisoning LIKE clauses.
        consumed_spans: list[str] = []  # track consumed text for debugging
        actor_modifier_spans: list[str] = []

        # ── 0a. INTENT extraction (bidder intent → config overrides) ─────
        from app.core.intent_vocabulary import extract_intent
        intent_config, intent_span = extract_intent(normalized)
        if intent_config:
            if intent_config.get("force_dataset"):
                params["dataset"] = intent_config["force_dataset"]
            if intent_config.get("estado_family"):
                params["estado_family"] = intent_config["estado_family"]
            if intent_config.get("scrub_only") or "estado_family" in intent_config:
                consumed_spans.append(intent_span)
                if intent_config.get("estado_family"):
                    actor_modifier_spans.append(intent_span)
            # OPP-003 G1: si el intent del bidder activa la familia oferta_abierta
            # (ej. "para presentarme", "donde me pueda presentar"), también
            # marcar intent_type=opportunity_search para que el resto del
            # pipeline (suggester, soql ordering, follow-up merge) lo respete.
            if intent_config.get("estado_family") == "oferta_abierta":
                params["intent_type"] = "opportunity_search"
                params.setdefault("dataset", "procesos")

        # OPP-002: opportunity_search trigger (minimal, deterministic)
        opportunity_keywords = (
            "oportunidad",
            "oportunidades",
            "convocatoria",
            "convocatorias",
            "licitacion abierta",
            "licitaciones abiertas",
            "licitacion abierto",
            "licitaciones abiertos",
            "licitacion publica abierta",
            "licitacion publica abierto",
            "licitaciones publicas abiertas",
            "licitaciones publicas abiertos",
            "procesos abiertos",
        )
        if any(kw in normalized for kw in opportunity_keywords):
            params["intent_type"] = "opportunity_search"
            params.setdefault("dataset", "procesos")
            params.setdefault("estado_family", "oferta_abierta")
            # OPP-003: filtro de estado debe usar estado_del_procedimiento
            # (el campo correcto con 9 valores). estado_de_apertura_del_proceso
            # solo tiene 2 valores y su semántica solapa peligrosamente — ver
            # fix Pamplonita SAMC-PNITA-002-2026 (2026-05-17).
            params.setdefault("estado_del_procedimiento", ["Publicado", "Borrador", "Abierto"])

        # ── 1. Dataset selection (keyword-based) ────────────────────────
        #    Only set if not already determined by intent
        params.setdefault("dataset", self._select_dataset(normalized))
        params["dataset_explicit"] = False  # default

        # Detect if user explicitly mentioned a dataset keyword
        _DATASET_KEYWORDS = frozenset({
            "contrato", "contratos", "proceso", "procesos",
            "licitacion", "licitaciones", "convocatoria", "convocatorias",
            "oportunidades", "oportunidad",
            "firmado", "firmados", "firmada", "firmadas",
            "contratista", "proveedor", "historico",
            "liquidado", "liquidados",
            "cedido", "cedidos",
            "terminado", "terminados",
        })
        dataset_explicit = any(kw in normalized for kw in _DATASET_KEYWORDS)
        if dataset_explicit:
            params["dataset_explicit"] = True

        # ── 1.b Dataset SCOPE: "procesos o contratos" / "ambos" → los DOS ───
        # El usuario pide AMBOS conjuntos. `dataset` sigue ESCALAR (primario =
        # procesos, el universo más amplio) por compatibilidad; el ejecutor (Commit 2)
        # corre cada hijo con su propio {"dataset": ...} y agrupa. dataset_scope NO
        # debe llegar al SoQLBuilder: es señal de orquestación, no de filtro.
        if any(re.search(p, normalized) for p in _DUAL_DATASET_PATTERNS):
            params["dataset_scope"] = ["procesos", "contratos"]
            params["dataset"] = "procesos"
            params["dataset_explicit"] = True

        # ── 1.c Modalidad M2: única autoridad lingüística ────────────────
        # Los offsets se calculan y aplican sobre el mismo texto normalizado
        # y corregido. Se anticipan únicamente los cambios determinísticos de
        # dataset que QueryPlan aplicará después.
        effective_dataset = str(params["dataset"])
        if params.get("intent_type") == "opportunity_search":
            effective_dataset = DATASET_PROCESOS
        elif contains_signed_token(normalized):
            effective_dataset = DATASET_CONTRATOS
        dataset_scope = params.get("dataset_scope")
        modality_result = resolve_parser_modality(
            normalized,
            dataset=effective_dataset,
            dataset_scope=(
                dataset_scope
                if isinstance(dataset_scope, (list, tuple))
                else None
            ),
        )
        if modality_result.executable_params:
            params.update(modality_result.executable_params)

        # ── 1.c.1 Dimensional negation binding ─────────────────────────
        # Bind polarity before any positive extractor can claim the governed
        # value.  Modality spans are protected aliases: an internal ``sin``
        # ("contratación directa sin ofertas") never becomes an operator.
        # ADR-063 remains the monetary authority.  Its complete consumed spans
        # are protected before the shared word ``menos``/``no`` is considered
        # dimensional negation, including verbal comparators such as
        # "que no supere" and "al menos".
        preliminary_dates = self._extract_dates(normalized)
        _, preliminary_amount_texts = self._extract_amounts(
            normalized, preliminary_dates,
        )
        preliminary_amount_spans = tuple({
            match.span()
            for amount_text in preliminary_amount_texts
            for match in re.finditer(re.escape(amount_text), normalized)
        })
        negation_binding = bind_negations(
            normalized,
            city_pattern=_CITY_PATTERN_RE,
            modality_spans=modality_result.interpretation.consumed_spans,
            protected_spans=preliminary_amount_spans,
        )
        positive_text = negation_binding.masked_text
        scrubbed = mask_consumed_spans(
            modality_result.masked_text,
            negation_binding.masked_spans,
        )

        # AP3-F projects roles only from positive residual text.  A provider or
        # buyer governed by negation remains a typed NegationRequest and cannot
        # become positive actor/entity evidence.
        if actor_profile_enabled():
            actor_conversation = interpret_actor_conversation(positive_text)
            actor_conversation, actor_relation_spans = (
                self._seed_actor_conversation(actor_conversation, params)
            )
        for relation_span in actor_relation_spans:
            scrubbed = re.sub(
                re.escape(relation_span), " ", scrubbed, count=1,
                flags=re.IGNORECASE,
            )
        scrubbed = re.sub(r"\s+", " ", scrubbed).strip()
        modality_spans = [
            normalized[start:end]
            for start, end in modality_result.interpretation.consumed_spans
        ]
        consumed_spans.extend(modality_spans)
        actor_modifier_spans.extend(modality_spans)

        # El intent se detectó antes para seleccionar dataset, pero se consume
        # después del masking por offsets de M2.
        if (
            intent_config
            and intent_span
            and (intent_config.get("scrub_only") or "estado_family" in intent_config)
        ):
            scrubbed = scrubbed.replace(intent_span, " ", 1)

        # ── 1.d Contractual type/nature (independent from modality) ─────
        # A recognized contractual class owns its span before the catch-all
        # thematic extractor runs.  Projection is source-backed and dataset-
        # aware; this layer only emits canonical class IDs.
        contract_type_result = resolve_contract_types(scrubbed)
        if contract_type_result.include_ids:
            params["contract_type_include"] = list(
                contract_type_result.include_ids
            )
            consumed_spans.extend(contract_type_result.consumed_spans)
            actor_modifier_spans.extend(contract_type_result.consumed_spans)
        scrubbed = contract_type_result.residual_text

        # ── 1.5. NIT detection (shape-based, before year/amount stripping) ──
        # Detecta NITs colombianos por forma antes de cualquier otra extracción
        # para que no sean confundidos con años ni montos.
        scrubbed = self._consume_actor_document(positive_text, scrubbed, params)

        # ── 2. Dates (regex — reliable for numbers) ─────────────────────
        date_params = self._extract_dates(positive_text)
        params.update(date_params)

        # ── 3. Strip year tokens so they don't contaminate scans ────────
        # Lookahead negativo: no capturar años seguidos de unidades de dinero
        # (ej: "2000 millones" no es el año 2000)
        _year_re = re.compile(
            rf"\b(20\d{{2}})\b(?!\s*(?:millones?|millon|mil\s+millones|billones?|palos?|barras?|pesos|{SMMLV_UNIT_PATTERN}))",
            re.IGNORECASE,
        )
        for m in re.finditer(_year_re, scrubbed):
            scrubbed = re.sub(rf"\b{re.escape(m.group(0))}\b", " ", scrubbed)
        # Also extract bare years as dates.  A structured result from
        # _extract_dates() owns the endpoint contract: in particular,
        # ``desde 2025`` intentionally has no fecha_hasta.  The generic bare-
        # year fallback may create a closed year only when the date extractor
        # did not recognize any temporal structure.
        protected_year_spans = _protected_entity_year_spans(positive_text)
        if not date_params:
            for m in re.finditer(_year_re, positive_text):
                if _inside_spans(m.start(1), m.end(1), protected_year_spans):
                    continue
                year = m.group(1)
                params.setdefault("fecha_desde", f"{year}-01-01")
                params.setdefault("fecha_hasta", f"{year}-12-31")
        scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 4. Ordering signals ─────────────────────────────────────────
        if _ORDERING_VALOR_RE.search(scrubbed):
            params["ordering_signal"] = "valor_desc"
            scrubbed = _ORDERING_VALOR_RE.sub(" ", scrubbed)
            scrubbed = re.sub(r"\s+", " ", scrubbed).strip()
        elif _ORDERING_FECHA_RE.search(scrubbed):
            params["ordering_signal"] = "fecha_desc"
            scrubbed = _ORDERING_FECHA_RE.sub(" ", scrubbed)
            scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 4.5. Temporal anaphora: "ese año", "ese periodo" ──────────────────
        # Scrub BEFORE entity rewrite para que "ESE" no capture "ese año"
        _TEMPORAL_ANAPHORA_RE = re.compile(
            r"\bese\s+(?:año|ano|periodo|período|rango|mismo\s+año|año\s+fiscal)\b",
            re.IGNORECASE,
        )
        if _TEMPORAL_ANAPHORA_RE.search(scrubbed):
            params["_temporal_anaphora"] = True
            scrubbed = _TEMPORAL_ANAPHORA_RE.sub(" ", scrubbed)
            scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 4.55. TYPED GEO: "departamento de X" → TERRITORIO, no comprador (Etapa 7) ──
        #    "departamento de X" = el TERRITORIO (todas las entidades del depto). El
        #    gazetteer de entidades lista frases territoriales ("departamento del
        #    atlantico") como ALIAS del comprador "DEPARTAMENTO DEL ATLANTICO**" → colapsa
        #    territorio y comprador. Interceptamos el patrón ANTES del scan de entidades y
        #    lo enrutamos a departamento_resolved (kind=territorial_department). En cambio
        #    "Gobernación de X" NO trae el keyword "departamento" → cae al scan de entidades
        #    y resuelve al comprador real (kind=buyer_entity). Así INV-7: ambos generan
        #    WHERE distintos (departamento vs entidad).
        if "departamento_resolved" not in params:
            from app.core.entity_resolver import geo_mention_resolution
            _km = re.search(
                r"\b(?:departamento|depto\.?)\s+de(?:l)?\s+(.+)", scrubbed, re.IGNORECASE,
            )
            if _km:
                _rest = _km.group(1).split()
                for _n in (3, 2, 1):
                    _cand = " ".join(_rest[:_n])
                    _geo = geo_mention_resolution(_cand)
                    if _geo.kind == "department" and _geo.canonical:
                        params["departamento"] = _cand
                        params["departamento_resolved"] = _geo.canonical
                        params["departamento_resolution"] = {
                            "value": _geo.canonical,
                            "method": "territorial_pattern",
                            "kind": "territorial_department",
                            "confidence": "high",
                            "like_value": None,
                            "metadata": {},
                        }
                        _consumed = scrubbed[_km.start():_km.start(1) + len(_cand)]
                        consumed_spans.append(_consumed)
                        actor_modifier_spans.append(_consumed)
                        scrubbed = scrubbed.replace(_consumed, " ", 1)
                        scrubbed = re.sub(r"\s+", " ", scrubbed).strip()
                        break

        # ── 4.6. ENTITY REWRITE: try canonical variants for entity patterns ──
        #    "alcaldía de X" → "Municipio de X" / "Alcaldía de X"
        #    Runs BEFORE gazetteer scan so variants feed into alias matching.
        from app.core.entity_types import rewrite_entity
        entity_candidates = rewrite_entity(scrubbed)
        rewritten_entity = None
        unresolved_rewrite = None
        if entity_candidates and self.entity_resolver:
            for canonical, consumed, rtype in entity_candidates:
                match = self.entity_resolver.resolve_entidad(canonical)
                if match and match.value and match.method != "fallback_like":
                    rewritten_entity = (canonical, consumed, rtype, match)
                    break
                if match and match.clarification_needed and unresolved_rewrite is None:
                    unresolved_rewrite = (canonical, consumed, rtype, match)
            if rewritten_entity:
                canonical, consumed, rtype, match = rewritten_entity
                params["entidad"] = canonical
                params["entidad_resolved"] = match.value
                params["entidad_resolution"] = {
                    "value": match.value,
                    "method": f"rewrite_{rtype}",
                    "kind": "buyer_entity",
                    "confidence": "high",
                    "like_value": None,
                    "metadata": {"rewritten_from": consumed},
                }
                consumed_spans.append(consumed)
                scrubbed = scrubbed.replace(consumed, " ", 1)
                scrubbed = re.sub(r"\s+", " ", scrubbed).strip()
            elif unresolved_rewrite:
                canonical, consumed, rtype, match = unresolved_rewrite
                params["entidad"] = canonical
                params["entidad_resolution"] = {
                    **match.to_dict(),
                    "method": f"rewrite_{rtype}_unresolved",
                    "metadata": {**match.metadata, "rewritten_from": consumed},
                }
                consumed_spans.append(consumed)
                scrubbed = scrubbed.replace(consumed, " ", 1)
                scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 5. GAZETTEER SCAN: Entity (longest alias match) ─────────────
        #    This is the core change: detect entities by substring match
        #    against 10K+ aliases, NOT by regex pattern matching.
        #    Skip if entity was already resolved by rewrite (step 4.5).
        if self.entity_resolver and "entidad_resolved" not in params:
            entity_scan = self.entity_resolver.scan_entity(scrubbed)
            if entity_scan:
                params["entidad"] = entity_scan.matched_alias
                params["entidad_resolved"] = entity_scan.official_name
                params["entidad_resolution"] = {
                    "value": entity_scan.official_name,
                    "method": entity_scan.method,
                    "kind": "buyer_entity",
                    "confidence": "high",
                    "like_value": None,
                    "metadata": {},
                }
                scrubbed = entity_scan.remaining_text

        # ── 6. GAZETTEER SCAN: Region/zone (BEFORE department) ──────────
        #    Runs before the department scan so a multi-word zone whose head
        #    is a department name ("tolima grande") is captured WHOLE before
        #    the department scan can steal "tolima". Sets departamentos_in for
        #    the builder to emit `departamento IN (...)`.
        if self.entity_resolver:
            region_scan = self.entity_resolver.scan_region(scrubbed)
            if region_scan:
                from app.data.colombia_geography import ZONES_BY_NAME
                zone = region_scan.official_name
                depts = ZONES_BY_NAME.get(zone, [])
                if depts:
                    params["region"] = zone
                    params["departamentos_in"] = depts
                    scrubbed = region_scan.remaining_text

        # ── 6.2 GAZETTEER SCAN: Department (on remaining text) ──────────
        #    Runs AFTER entity scan so "de santander" can't be stolen.
        #    A specific department wins over a broader zone: if one resolves,
        #    drop any zone captured in step 6.
        if self.entity_resolver:
            dept_scan = self.entity_resolver.scan_departamento(scrubbed)
            if dept_scan:
                params["departamento"] = dept_scan.matched_alias
                params["departamento_resolved"] = dept_scan.official_name
                params["departamento_resolution"] = {
                    "value": dept_scan.official_name,
                    "method": dept_scan.method,
                    "kind": "territorial_department",
                    "confidence": "high",
                    "like_value": None,
                    "metadata": {},
                }
                scrubbed = dept_scan.remaining_text
                params.pop("region", None)
                params.pop("departamentos_in", None)

        # ── 6.5 MULTI-REGIONAL ENTITY RESOLUTION (ENTITY-MULTI-REGIONAL-001) ──
        # Si la entidad resuelta es multi-regional (SENA, ICBF, etc.) y hay
        # departamento, resolver a la regional específica.
        if "entidad_resolved" in params and "departamento_resolved" in params:
            try:
                from app.data.regional_entities import resolve_regional
                original_entity = str(params["entidad_resolved"])
                regional, canonical_short = resolve_regional(
                    original_entity,
                    str(params["departamento_resolved"]),
                )
                if regional:
                    # Guardar el nombre canónico corto como like_value para SoQL
                    # (ej: "SENA" en vez de "SENA REGIONAL BOLÍVAR" que tiene tildes y no existe en SECOP)
                    params["entidad_like"] = canonical_short
                    params["entidad_resolved"] = regional
                    if "entidad_resolution" in params and isinstance(params["entidad_resolution"], dict):
                        params["entidad_resolution"]["value"] = regional
                        params["entidad_resolution"]["method"] = "multi_regional"
            except ImportError:
                pass  # regional_entities module not available — skip silently

        # ── 7. ESTADO FAMILY: semantic normalization (runs BEFORE old extractor) ──
        #    Detects natural-language tokens like "publicados", "abiertos",
        #    "vigentes" and maps them to SECOP-accurate estado filters.
        #    The matched token is scrubbed so it doesn't leak into 'objeto'.
        from app.core.estado_families import detect_estado_family
        estado_family_result = detect_estado_family(scrubbed, str(params["dataset"]))
        if estado_family_result:
            for field, values in estado_family_result["filters"].items():
                params[field] = values
            params["estado_family"] = estado_family_result["family"]
            # Backward compat: set legacy estado/estado_field for old tests
            if "estado_de_apertura_del_proceso" in estado_family_result["filters"]:
                params["estado"] = estado_family_result["filters"]["estado_de_apertura_del_proceso"][0]
                params["estado_field"] = "estado_de_apertura_del_proceso"
            elif "estado_del_procedimiento" in estado_family_result["filters"]:
                params["estado"] = estado_family_result["filters"]["estado_del_procedimiento"][0]
                params["estado_field"] = "estado_del_procedimiento"
            elif "estado_contrato" in estado_family_result["filters"]:
                params["estado"] = estado_family_result["filters"]["estado_contrato"][0]
                params["estado_field"] = "estado_contrato"
            # Scrub the consumed span from text
            span = estado_family_result["consumed_span"]
            consumed_spans.append(span)
            actor_modifier_spans.append(span)
            scrubbed = re.sub(r"\b" + re.escape(span) + r"\b", " ", scrubbed, flags=re.IGNORECASE)
            secondary_state = self._extract_state(
                scrubbed,
                str(params["dataset"]),
            )
            if secondary_state and secondary_state.get("source_text"):
                secondary_source = str(secondary_state["source_text"])
                consumed_spans.append(secondary_source)
                actor_modifier_spans.append(secondary_source)
                scrubbed = re.sub(
                    rf"\b{re.escape(secondary_source)}\b",
                    " ",
                    scrubbed,
                    flags=re.IGNORECASE,
                )
            # CORPUS-F006/OC001: si la apertura del proceso quedó establecida
            # vía estado_family=oferta_abierta sobre dataset=procesos, esto es
            # equivalente a opportunity_search. Sincronizamos intent_type para
            # que el suggester, soql ordering y followup_merge lo respeten igual
            # que con las keywords explícitas ("oportunidad"/"convocatoria").
            if (estado_family_result["family"] == "oferta_abierta"
                    and params.get("dataset") == "procesos"):
                params.setdefault("intent_type", "opportunity_search")
                params.setdefault(
                    "estado_del_procedimiento",
                    ["Publicado", "Borrador", "Abierto"],
                )
        else:
            # ── 7b. OLD State extractor (dataset-aware keyword match) ────────
            state_scan_text = mask_consumed_spans(
                modality_result.masked_text,
                negation_binding.masked_spans,
            )
            primary_state = True
            while state_result := self._extract_state(
                state_scan_text,
                str(params["dataset"]),
            ):
                if primary_state:
                    params["estado"] = state_result["value"]
                    if state_result.get("field"):
                        params["estado_field"] = state_result["field"]
                source_text = state_result.get("source_text")
                if not source_text:
                    break
                consumed_spans.append(source_text)
                actor_modifier_spans.append(source_text)
                state_pattern = rf"\b{re.escape(source_text)}\b"
                scrubbed = re.sub(
                    state_pattern,
                    " ",
                    scrubbed,
                    flags=re.IGNORECASE,
                )
                state_scan_text = re.sub(
                    state_pattern,
                    " ",
                    state_scan_text,
                    flags=re.IGNORECASE,
                )
                primary_state = False

        # ── 9. Amounts ──────────────────────────────────────────────────
        # Amounts run over the progressively scrubbed text. Identifier-shaped
        # spans accepted by step 1.5 are therefore unavailable to the monetary
        # scanner, while currency-shaped spans rejected as NIT remain visible.
        amount_params, amount_spans = self._extract_amounts(scrubbed, date_params)
        params.update(amount_params)
        # Scrub consumed amount spans so residual tokens ("adelante", "menos")
        # don't leak into objeto.
        for span in amount_spans:
            scrubbed = scrubbed.replace(span, " ", 1)
        scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 10. Contractor ──────────────────────────────────────────────
        # Solo corre si el NIT no fue ya detectado por forma en paso 1.5
        if "contratista" not in params and "actor_resolution" not in params:
            if actor_profile_enabled():
                modifier_tails = recognized_actor_modifier_tails(
                    positive_text,
                    params,
                    extra_consumed_spans=(*actor_modifier_spans, *amount_spans),
                )
                scrubbed = self._detect_named_actor(
                    positive_text,
                    scrubbed,
                    params,
                    recognized_modifier_tails=modifier_tails,
                )
            else:
                contractor = self._extract_contractor(positive_text, str(params["dataset"]))
                if contractor:
                    params["contratista"] = contractor
                    scrubbed = scrubbed.replace(contractor, " ")

        # Command framing is consumed only after this turn has independently
        # established actor intent.  Thus "busca mantenimiento" keeps its
        # thematic semantics, while "búscate al proveedor X" cannot leak the
        # framing verb into ``objeto`` or a replayed candidate frame.
        if params.get("actor_resolution"):
            for framing_span in actor_conversation.framing_spans:
                scrubbed = re.sub(
                    re.escape(framing_span), " ", scrubbed, count=1,
                    flags=re.IGNORECASE,
                )
            scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 10.5. City detection (before object terms) ────────────────────
        city_match = _CITY_PATTERN_RE.search(scrubbed)
        if city_match:
            city_raw = city_match.group(0).strip().lower()
            # Extract just the city name (remove "en ", "de ", "para ")
            for prefix in ("en ", "de ", "para "):
                if city_raw.startswith(prefix):
                    city_raw = city_raw[len(prefix):]
                    break
            city_clean = city_raw.strip()
            params["ciudad"] = city_clean.capitalize()
            # If no entidad_resolved yet, use city→entity mapping for common cities
            if "entidad_resolved" not in params:
                entity_for_city = _CITY_TO_ENTITY.get(city_clean)
                if entity_for_city:
                    params["entidad"] = city_clean.title()
                    params["entidad_resolved"] = entity_for_city
                    params["entidad_resolution"] = {
                        "value": entity_for_city,
                        "method": "city_to_entity",
                        "confidence": "medium",
                        "like_value": None,
                    }
            # Scrub city from scrubbed so it doesn't enter objeto
            scrubbed = _CITY_PATTERN_RE.sub(" ", scrubbed)
            scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 11. Phrase detection BEFORE token scrub (objeto_phrases) ────
        #    Preserva colocaciones de dominio ("manual de contratación") como
        #    unidad antes de que "de" se descarte y la frase se pierda. Consume
        #    el span para que sus tokens NO recreen el objeto roto (no duplicar).
        # Preserve the pre-R4 executable OBJECT forms exactly.  New linguistic
        # forms remain typed/deferred until R4-S2; none can leak into objeto.
        legacy_excluded: list[str | list[str]] = []
        for request in negation_binding.requests:
            if (
                request.dimension is NegationDimension.OBJECT
                and request.verdict is NegationVerdict.EXECUTE
            ):
                legacy_excluded.extend(self._extract_object_terms(request.value))
        if legacy_excluded:
            seen_excluded: set[str] = set()
            unique_excluded: list[str | list[str]] = []
            for value in legacy_excluded:
                key = "|".join(value) if isinstance(value, list) else value
                if key not in seen_excluded:
                    seen_excluded.add(key)
                    unique_excluded.append(value)
            params["objeto_exclude"] = unique_excluded

        from app.core.phrase_catalog import detect_phrases
        objeto_phrases, objeto_text = detect_phrases(scrubbed)
        if objeto_phrases:
            params["objeto_phrases"] = objeto_phrases

        # ── 12. Object terms (whatever remains after all extractions) ───
        object_terms = self._extract_object_terms(objeto_text)
        if object_terms:
            params["objeto"] = object_terms

        # ── 12. LLM decision ───────────────────────────────────────────
        extracted_keys = {k for k, v in params.items() if v not in (None, [], "", {})}
        needs_llm = self._needs_llm(positive_text, params)
        route_reason = "heuristic_only" if not needs_llm else "heuristic_plus_llm"

        if extracted_keys == {"dataset"}:
            needs_llm = True
            route_reason = "insufficient_signals"

        if extracted_keys - {"dataset"} == {"ordering_signal"}:
            needs_llm = True
            route_reason = "ordering_without_object"

        negation_requests = attach_scope_anchors(
            negation_binding.requests, params,
        )
        return ParsedQuery(
            params=params,
            needs_llm=needs_llm,
            route_reason=route_reason,
            dataset_explicit=dataset_explicit,
            consumed_spans=consumed_spans,
            modality_interpretation=modality_result.interpretation,
            contract_type_interpretation=contract_type_result,
            negation_requests=negation_requests,
            positive_text=positive_text,
        )

    # ─── Helper methods (only for non-entity extractions) ───────────────

    def _select_dataset(self, normalized_query: str) -> str:
        contract_signals = (
            "contrato", "contratos",
            "firmado", "firmados", "firmada", "firmadas",
            "contratista", "proveedor", "historico",
            "liquidado", "liquidados",
            "en ejecucion", "ejecutando",
            "cedido", "cedidos",
            "terminado", "terminados",
        )
        if any(s in normalized_query for s in contract_signals):
            if re.search(r"\babiert[ao]s?\b", normalized_query):
                return DATASET_PROCESOS
            return DATASET_CONTRATOS
        return DATASET_PROCESOS

    def _extract_state(self, normalized_query: str, dataset: str) -> dict | None:
        if dataset == DATASET_CONTRATOS:
            for alias, official in sorted(ESTADO_CONTRATO_SYNONYMS.items(), key=lambda x: len(x[0]), reverse=True):
                if re.search(rf"\b{re.escape(alias)}\b", normalized_query):
                    return {
                        "value": official,
                        "field": "estado_contrato",
                        "source_text": alias,
                    }
        else:
            for alias, official in sorted(ESTADO_APERTURA_SYNONYMS.items(), key=lambda x: len(x[0]), reverse=True):
                if re.search(rf"\b{re.escape(alias)}\b", normalized_query):
                    return {
                        "value": official,
                        "field": "estado_de_apertura_del_proceso",
                        "source_text": alias,
                    }
            for alias, official in sorted(ESTADO_PROCEDIMIENTO_SYNONYMS.items(), key=lambda x: len(x[0]), reverse=True):
                if re.search(rf"\b{re.escape(alias)}\b", normalized_query):
                    return {
                        "value": official,
                        "field": "estado_del_procedimiento",
                        "source_text": alias,
                    }
        return None

    def _extract_dates(self, normalized_query: str) -> dict[str, str]:
        params: dict[str, str] = {}
        iso_dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", normalized_query)
        if len(iso_dates) >= 1:
            params["fecha_desde"] = iso_dates[0]
        if len(iso_dates) >= 2:
            params["fecha_hasta"] = iso_dates[1]

        # ── Month + year patterns ─────────────────────────────────────────
        # Matches: "abril de 2026", "del mes de enero de 2025", "en marzo 2024"
        _month_names = "|".join(MONTH_MAP.keys())
        month_year_re = re.compile(
            rf"(?:del\s+mes\s+de\s+|en\s+)?({_month_names})\s+(?:de\s+)?(20\d{{2}})\b",
            re.IGNORECASE,
        )
        month_match = month_year_re.search(normalized_query)
        if month_match:
            month_num = MONTH_MAP[month_match.group(1).lower()]
            year_num = int(month_match.group(2))
            last_day = calendar.monthrange(year_num, month_num)[1]
            params.setdefault("fecha_desde", f"{year_num}-{month_num:02d}-01")
            params.setdefault("fecha_hasta", f"{year_num}-{month_num:02d}-{last_day:02d}")

        year_range_re = re.compile(
            r"""
            \bentre\s+(?:el\s+)?(20\d{2})\s+y\s+(?:el\s+)?(20\d{2})\b
            |\bde\s+(20\d{2})\s+a\s+(20\d{2})\b
            |\bdesde\s+(20\d{2})\s+hasta\s+(20\d{2})\b
            |(?<!\d)(20\d{2})\s*(?:-|a|y)\s*(20\d{2})(?!\d)
            """,
            re.IGNORECASE | re.VERBOSE,
        )
        year_range_match = year_range_re.search(normalized_query)
        if year_range_match:
            years = [int(year) for year in year_range_match.groups() if year]
            year1, year2 = sorted(years[:2])
            params["fecha_desde"] = f"{year1}-01-01"
            params["fecha_hasta"] = f"{year2}-12-31"

        year_match = re.search(r"\ben\s+(20\d{2})\b", normalized_query)
        if year_match:
            year = year_match.group(1)
            params.setdefault("fecha_desde", f"{year}-01-01")
            params.setdefault("fecha_hasta", f"{year}-12-31")

        since_match = re.search(r"\bdesde\s+(20\d{2})\b", normalized_query)
        if since_match:
            params["fecha_desde"] = f"{since_match.group(1)}-01-01"

        until_match = re.search(r"\bhasta\s+(20\d{2})\b", normalized_query)
        if until_match:
            params["fecha_hasta"] = f"{until_match.group(1)}-12-31"

        if re.search(r"\beste\s+a[nñ]o\b", normalized_query, re.IGNORECASE):
            year = current_colombian_year()
            params.setdefault("fecha_desde", f"{year}-01-01")
            params.setdefault("fecha_hasta", f"{year}-12-31")

        return params

    def _extract_amounts(self, normalized_query: str, date_params: dict[str, str]) -> tuple[dict[str, object], list[str]]:
        """Extrae montos monetarios con polaridad (min/max/rango).

        Retorna (params, consumed_spans) donde consumed_spans son los
        spans de texto detectados y consumidos, para scrub posterior.
        """
        params: dict[str, object] = {}
        consumed_spans: list[str] = []
        digit_amount = r"(?:\$\s*|cop\s+)?\d+(?:[.,]\d+)*"
        verbal_amount = rf"{CARDINAL_WORD_PATTERN}(?:\s+{CARDINAL_WORD_PATTERN})*"
        amount_atom = rf"(?:{digit_amount}|{verbal_amount})"
        unit_pattern = (
            rf"(?:billon(?:es)?|mil\s+millones?|millones?|palos?|barras?|"
            rf"{SMMLV_UNIT_PATTERN}|mil)"
        )
        smmlv_requested = is_smmlv_expression(normalized_query)
        smmlv_year: int | None = None
        smmlv_year_assumed = False
        smmlv_sources: dict[int, tuple[str, str]] = {}

        if smmlv_requested:
            desde = date_params.get("fecha_desde")
            hasta = date_params.get("fecha_hasta")
            if desde or hasta:
                if not desde or not hasta or desde[:4] != hasta[:4]:
                    years = sorted({value[:4] for value in (desde, hasta) if value})
                    params["money_metadata"] = {
                        "unit": "SMMLV",
                        "status": "unsupported_multi_year",
                        "year_range": years,
                    }
                    params["money_clarification"] = (
                        "Los SMMLV cambian cada año. Indica un solo año para aplicar "
                        "un umbral correcto; no convertiré un rango multianual con una sola tarifa."
                    )
                    return params, consumed_spans
                smmlv_year = int(desde[:4])
            else:
                smmlv_year = current_colombian_year()
                smmlv_year_assumed = True

            annual_value = smmlv_cop(smmlv_year)
            if annual_value is None:
                params["money_metadata"] = {
                    "unit": "SMMLV",
                    "status": "unsupported_year",
                    "reference_year": smmlv_year,
                }
                params["money_clarification"] = (
                    f"No tengo un valor oficial de SMMLV catalogado para {smmlv_year}. "
                    "Indica uno de los años soportados: 2024, 2025 o 2026."
                )
                return params, consumed_spans

        def _to_cop(amount_text: str) -> int | None:
            if is_smmlv_expression(amount_text):
                if smmlv_year is None:
                    return None
                resolved = smmlv_to_cop(amount_text, smmlv_year)
                if resolved is None:
                    return None
                cop, source_amount = resolved
                smmlv_sources[cop] = (str(source_amount), amount_text.strip())
                return cop
            return money_to_cop(amount_text)
        year_tokens: set[str] = set()
        for val in date_params.values():
            year_tokens.add(val[:4])
        _year_money_re = re.compile(
            rf"\b(20\d{{2}})\b(?!\s*(?:millones?|millon|mil\s+millones|billones?|palos?|barras?|pesos|{SMMLV_UNIT_PATTERN}))",
            re.IGNORECASE,
        )
        for m in re.finditer(_year_money_re, normalized_query):
            year_tokens.add(m.group(1))

        # Detect "entre X y Y" range BEFORE individual amount parsing so the
        # two numbers don't each get mis-classified by the generic loop below.
        # ── Shared unit-inheritance helpers (used by range + compound block) ──
        _compound_unit_re = re.compile(
            rf"mil\s+millones?|billon(?:es)?|millones?|palos?|barras?|{SMMLV_UNIT_PATTERN}",
            re.IGNORECASE,
        )
        _atomic_unit_re = re.compile(
            rf"billon(?:es)?|millones?|palos?|barras?|{SMMLV_UNIT_PATTERN}",
            re.IGNORECASE,
        )
        _has_digit_re = re.compile(r"\d")

        def _has_monetary_unit(amount_text: str) -> bool:
            if _compound_unit_re.search(amount_text):
                return True
            return bool(re.search(r"\d(?:[\d.,]*\d)?\s+mil\s*$", amount_text))

        def _inherit_unit(donor_text: str) -> str | None:
            if _has_digit_re.search(donor_text):
                m = _compound_unit_re.search(donor_text)
            else:
                m = _atomic_unit_re.search(donor_text)
            return m.group(0) if m else None

        _range_re = re.compile(
            rf"(?:entre|de|desde)\s+({amount_atom}\s*{unit_pattern}?(?:\s+de\s+pesos)?)(?:\s*[-–—]\s*|\s+y\s+|\s+a\s+|\s+hasta\s+)({amount_atom}\s*{unit_pattern}?(?:\s+de\s+pesos)?)",
            re.IGNORECASE,
        )
        range_match = _range_re.search(normalized_query)
        if range_match:
            lo_text = range_match.group(1)
            hi_text = range_match.group(2)

            # DOGFOOD-D: cuando un lado del rango no trae unidad explícita,
            # hereda la unidad del otro lado con esta regla:
            #   - Si el otro lado tiene NÚMERO + unidad compuesta ("2 mil millones"),
            #     hereda la unidad COMPUESTA ("mil millones") → "1" → 1 mil millones.
            #   - Si el otro lado es solo "mil millones" (sin número antes),
            #     hereda solo la unidad ATÓMICA ("millones") → "500" → 500 millones.
            lo_has_unit = _has_monetary_unit(lo_text)
            hi_has_unit = _has_monetary_unit(hi_text)

            if not lo_has_unit and hi_has_unit:
                u = _inherit_unit(hi_text)
                if u:
                    lo_text = lo_text + " " + u
            if not hi_has_unit and lo_has_unit:
                u = _inherit_unit(lo_text)
                if u:
                    hi_text = hi_text + " " + u

            amount_lo = _to_cop(lo_text)
            amount_hi = _to_cop(hi_text)

            if amount_lo and amount_hi and amount_lo >= 1_000_000 and amount_hi >= 1_000_000:
                params["valor_min"] = min(amount_lo, amount_hi)
                params["valor_max"] = max(amount_lo, amount_hi)
                # Remove matched span so generic loop below doesn't re-parse it
                span = normalized_query[range_match.start():range_match.end()]
                consumed_spans.append(span)
                normalized_query = normalized_query[: range_match.start()] + " " + normalized_query[range_match.end() :]

        # Detect "N1-N2 unidad" pattern WITHOUT "entre"/"de"/"desde" prefix.
        # e.g., "contratos 1000-3000 millones" where the hyphen acts as range
        # separator and the unit follows both numbers.
        # Only fires when the prefixed _range_re above did NOT match.
        if not range_match:
            _range_hyphen_re = re.compile(
                rf"({digit_amount})\s*[-–—]\s*({digit_amount})\s+"
                rf"({unit_pattern})",
                re.IGNORECASE,
            )
            h_match = _range_hyphen_re.search(normalized_query)
            if h_match:
                unit = h_match.group(3)
                lo_text = h_match.group(1) + " " + unit
                hi_text = h_match.group(2) + " " + unit

                amount_lo = _to_cop(lo_text)
                amount_hi = _to_cop(hi_text)

                if amount_lo and amount_hi and amount_lo >= 1_000_000 and amount_hi >= 1_000_000:
                    params["valor_min"] = min(amount_lo, amount_hi)
                    params["valor_max"] = max(amount_lo, amount_hi)
                    span = normalized_query[h_match.start():h_match.end()]
                    consumed_spans.append(span)
                    normalized_query = normalized_query[: h_match.start()] + " " + normalized_query[h_match.end() :]
                range_match = h_match  # prevent polarity patterns from re-parsing

        # Detect compound polarity ranges: "más de X y menos de Y"
        # e.g., "más de 350 millones y menos de 700" → valor_min + valor_max
        _compound_range_re = re.compile(
            r"(?:m[aá]s\s+de|mayor(?:es)?\s+(?:a|de|que)|por\s+encima\s+de|al\s+menos|superior(?:es)?\s+(?:a|de|que))\s+"
            rf"({amount_atom}\s*{unit_pattern}?(?:\s+de\s+pesos)?)"
            r"\s+(?:y|e)\s+"
            r"(?:menos\s+de|menor(?:es)?\s+(?:a|de|que)|por\s+debajo\s+de|a\s+lo\s+sumo|inferior(?:es)?\s+(?:a|de|que))\s+"
            rf"({amount_atom}\s*{unit_pattern}?(?:\s+de\s+pesos)?)",
            re.IGNORECASE,
        )
        cr_match = _compound_range_re.search(normalized_query)
        if cr_match:
            lo_text = cr_match.group(1)
            hi_text = cr_match.group(2)

            # Inherit monetary unit from one side to the other
            lo_has_unit = _has_monetary_unit(lo_text)
            hi_has_unit = _has_monetary_unit(hi_text)

            if not lo_has_unit and hi_has_unit:
                u = _inherit_unit(hi_text)
                if u:
                    lo_text = lo_text + " " + u
            if not hi_has_unit and lo_has_unit:
                u = _inherit_unit(lo_text)
                if u:
                    hi_text = hi_text + " " + u

            amount_lo = _to_cop(lo_text)
            amount_hi = _to_cop(hi_text)

            if amount_lo and amount_hi and amount_lo >= 1_000_000 and amount_hi >= 1_000_000:
                # Unlike a symmetric ``entre X y Y`` range, these operands
                # have explicit semantic roles.  Preserve them so P8 can see
                # a contradictory lower bound instead of silently swapping it.
                params["valor_min"] = amount_lo
                params["valor_max"] = amount_hi
                span = normalized_query[cr_match.start():cr_match.end()]
                consumed_spans.append(span)
                normalized_query = normalized_query[: cr_match.start()] + " " + normalized_query[cr_match.end() :]

        # Detect "X o más" / "X o mas" → valor_min only (before generic loop)
        _o_mas_re = re.compile(
            rf"({amount_atom}\s*{unit_pattern}?(?:\s+de\s+pesos)?)\s+o\s+m[aá]s",
            re.IGNORECASE,
        )
        om = _o_mas_re.search(normalized_query)
        if om:
            amount = _to_cop(om.group(1))
            if amount and amount >= 1_000_000:
                params["valor_min"] = amount
                # Remove matched span
                span = normalized_query[om.start():om.end()]
                consumed_spans.append(span)
                normalized_query = normalized_query[: om.start()] + " " + normalized_query[om.end() :]

        amount_text = (
            rf"({amount_atom}\s*"
            rf"{unit_pattern}?"
            r"(?:\s+de\s+pesos)?)"
        )
        polarity_patterns = (
            (
                "valor_max",
                re.compile(
                    rf"""
                    \b(?:
                        no\s+(?:sea|sean)?\s*(?:mayor(?:es)?|superior(?:es)?)\s+(?:a|de|que)
                        |que\s+no\s+(?:supere|superen|exceda|excedan|rebase|rebasen)(?:\s+los?)?
                        |no\s+(?:supere|superen|exceda|excedan|rebase|rebasen)(?:\s+los?)?
                        |no\s+(?:sea|sean)?\s*(?:por\s+)?m[aá]s\s+de
                    )\s+{amount_text}
                    """,
                    re.IGNORECASE | re.VERBOSE,
                ),
            ),
            (
                "valor_min",
                re.compile(
                    rf"""
                    \b(?:
                        no\s+(?:sea|sean)?\s*menor(?:es)?\s+(?:a|de|que)
                        |no\s+(?:sea|sean)?\s*(?:por\s+)?menos\s+de
                        |que\s+no\s+baje(?:n)?\s+de
                        |no\s+baje(?:n)?\s+de
                    )\s+{amount_text}
                    """,
                    re.IGNORECASE | re.VERBOSE,
                ),
            ),
            (
                "valor_max",
                re.compile(
                    rf"""
                    \b(?:
                        menor(?:es)?\s+(?:o\s+igual\s+)?(?:a|de|que)
                        |menos\s+de
                        |inferior(?:es)?\s+(?:a|de|que)
                        |por\s+debajo\s+de
                        |hasta
                        |m[aá]ximo
                        |a\s+lo\s+sumo
                    )\s+{amount_text}
                    """,
                    re.IGNORECASE | re.VERBOSE,
                ),
            ),
            (
                "valor_min",
                re.compile(
                    rf"""
                    \b(?:
                        mayor(?:es)?\s+(?:o\s+igual\s+)?(?:a|de|que)
                        |m[aá]s\s+de
                        |superior(?:es)?\s+(?:a|de|que)
                        |supere(?:n)?(?:\s+los?)?
                        |exceda(?:n)?(?:\s+los?)?
                        |por\s+encima\s+de
                        |al\s+menos
                        |m[ií]nim[oa]
                        |desde
                        |a\s+partir\s+de
                    )\s+{amount_text}
                    """,
                    re.IGNORECASE | re.VERBOSE,
                ),
            ),
        )
        for field, pattern in polarity_patterns:
            for match in list(pattern.finditer(normalized_query)):
                amount = _to_cop(match.group(1))
                if amount is None or amount < 1_000_000:
                    continue
                params[field] = amount
                span = normalized_query[match.start():match.end()]
                consumed_spans.append(span)
                normalized_query = (
                    normalized_query[: match.start()]
                    + " "
                    + normalized_query[match.end() :]
                )

        # PARSER-POLARITY-MONETARY-002: post-number polarity patterns.
        # Detects "X o menos", "X en adelante", etc. AFTER pre-number polarity
        # but BEFORE the generic number scanner, so the number is consumed
        # and doesn't trigger false ±20% range construction.
        _less_than_post_re = re.compile(
            rf"{amount_text}\s+(?:o\s+menos|o\s+menores|y\s+menos|y\s+menores|para\s+abajo|para\s+menos)",
            re.IGNORECASE,
        )
        for match in list(_less_than_post_re.finditer(normalized_query)):
            amount = _to_cop(match.group(1))
            if amount and amount >= 1_000_000:
                params["valor_max"] = amount
                span = normalized_query[match.start():match.end()]
                consumed_spans.append(span)
                normalized_query = (
                    normalized_query[: match.start()]
                    + " "
                    + normalized_query[match.end() :]
                )

        _more_than_post_re = re.compile(
            rf"{amount_text}\s+(?:en\s+adelante|o\s+mas|o\s+más|o\s+mayores|y\s+mas|y\s+más|y\s+mayores|o\s+superiores|para\s+arriba|para\s+mas|para\s+más)",
            re.IGNORECASE,
        )
        for match in list(_more_than_post_re.finditer(normalized_query)):
            amount = _to_cop(match.group(1))
            if amount and amount >= 1_000_000:
                params["valor_min"] = amount
                span = normalized_query[match.start():match.end()]
                consumed_spans.append(span)
                normalized_query = (
                    normalized_query[: match.start()]
                    + " "
                    + normalized_query[match.end() :]
                )

        matches = list(re.finditer(
            rf"(?:(?:mas|mayor(?:es)?|superior(?:es)?|superen?)\s+(?:de|a)|(?:menos|menor(?:es)?)\s+(?:de|a)|hasta|por|de|desde)?\s*({amount_atom})\s*{unit_pattern}?(?:\s+de\s+pesos)?",
            normalized_query,
        ))

        for match in matches:
            raw = match.group(0).strip()
            number_part = match.group(1)
            if number_part in year_tokens:
                continue
            amount = _to_cop(raw)
            if amount is None or amount < 1_000_000:
                continue

            if re.search(r"(?:mas|mayor(?:es)?|superior(?:es)?|superen?)\s+(?:de|a)|desde", raw):
                params["valor_min"] = amount
            elif re.search(r"(?:menos|menor(?:es)?)\s+(?:de|a)|hasta", raw):
                params["valor_max"] = amount
            elif re.search(r"\bpor\b", raw):
                # PARSER-POLARITY-MONETARY-002: only build ±20% range if NO
                # explicit valor_min/valor_max was already set by a post-pattern.
                if "valor_min" not in params and "valor_max" not in params:
                    params.setdefault("valor_min", int(amount * 0.8))
                    params.setdefault("valor_max", int(amount * 1.2))
            else:
                params.setdefault("valor_max", amount)

        if smmlv_sources and smmlv_year is not None:
            source_values: dict[str, str] = {}
            source_texts: dict[str, str] = {}
            for field in ("valor_min", "valor_max"):
                value = params.get(field)
                if isinstance(value, int) and value in smmlv_sources:
                    source_values[field], source_texts[field] = smmlv_sources[value]
            params["money_metadata"] = {
                "unit": "SMMLV",
                "reference_year": smmlv_year,
                "reference_year_assumed": smmlv_year_assumed,
                "year_authority": (
                    "current_bogota_year" if smmlv_year_assumed else "query_date_scope"
                ),
                "annual_value_cop": smmlv_cop(smmlv_year),
                "source_values": source_values,
                "source_texts": source_texts,
                "representation_kind": "indexed_unit",
            }

        return params, consumed_spans

    # ── Actor conversation roles (AP3-F) ───────────────────────────────

    def _seed_actor_conversation(
        self,
        instruction: ActorConversationInstruction,
        params: dict[str, object],
    ) -> tuple[ActorConversationInstruction, tuple[str, ...]]:
        """Seed role mentions without granting actor/buyer identity authority."""
        relation_spans: tuple[str, ...] = ()
        buyer_accepted = False
        if instruction.buyer_mention:
            buyer_has_explicit_role = bool(re.match(
                r"^(?:la\s+|el\s+)?(?:alcaldia|gobernacion|secretaria|"
                r"municipio|departamento|distrito|entidad)\b",
                instruction.buyer_mention,
                re.IGNORECASE,
            ))
            # An explicit actor-role span owns its complete mention.  ``CON``
            # inside "proveedor FUNDACIÓN CON AMOR" is not a buyer boundary;
            # only a structurally explicit buyer phrase may override it.
            role_name_requires_full_boundary = (
                instruction.construction.startswith("role_")
                and not buyer_has_explicit_role
            )
            if not role_name_requires_full_boundary:
                buyer_accepted = self._project_actor_relation_buyer(
                    instruction.buyer_mention, params,
                )
            if buyer_accepted and instruction.relation_buyer_span:
                relation_spans = (instruction.relation_buyer_span,)
            elif role_name_requires_full_boundary:
                return ActorConversationInstruction(
                    framing_spans=instruction.framing_spans,
                ), ()
            elif instruction.fallback_actor_mention:
                # ``CON`` may be part of a legal name (FUNDACIÓN CON AMOR).
                # It becomes a relation boundary only with independent buyer
                # evidence; otherwise restore the complete actor mention.
                instruction = replace(
                    instruction,
                    actor_mention=instruction.fallback_actor_mention,
                    actor_evidence=instruction.fallback_actor_evidence,
                    buyer_mention=None,
                    relation_buyer_span=None,
                )
            elif instruction.operation == ActorConversationOperation.SET_BUYER:
                return ActorConversationInstruction(
                    framing_spans=instruction.framing_spans,
                ), ()

        actor = instruction.actor_mention
        evidence = instruction.actor_evidence
        if actor and evidence not in {None, "topic"}:
            params["actor_conversation_operation"] = instruction.operation
            params["actor_conversation_construction"] = instruction.construction
            if evidence == "probe":
                # Exact-only directory evidence may promote this mention later.
                # Fuzzy retrieval is forbidden on this door.
                params["actor_conversation_probe"] = {
                    "actor_name": actor,
                    "construction": instruction.construction,
                }
            else:
                params["actor_resolution"] = "name_unresolved"
                params["actor_scope"] = {
                    "role": "contractor",
                    "resolution_method": "name_unresolved",
                    "requires_resolution": True,
                    "observed_name": actor,
                    "reason_codes": [
                        "ACTOR_CONVERSATION_OPERATION",
                        "ACTOR_NAME_NEEDS_RESOLUTION",
                    ],
                }
        elif buyer_accepted:
            params["actor_conversation_operation"] = (
                ActorConversationOperation.SET_BUYER
            )
            params["actor_conversation_construction"] = instruction.construction
        elif instruction.operation == ActorConversationOperation.SET_TERRITORY:
            params["actor_conversation_operation"] = instruction.operation
            params["actor_conversation_construction"] = instruction.construction

        return instruction, relation_spans

    def _project_actor_relation_buyer(
        self,
        buyer_mention: str,
        params: dict[str, object],
    ) -> bool:
        """Use the existing buyer resolver under relation-role semantics.

        Only deterministic exact evidence is accepted here.  In particular,
        the generic word ``gobernación`` is represented as a clarification,
        never sent to the resolver's global fuzzy tier where it could guess a
        specific department.
        """
        mention = normalize_text(buyer_mention).strip(" ,.;:¿?¡!")
        if not mention:
            return False
        generic = mention in {
            "alcaldia", "gobernacion", "secretaria", "municipio",
            "departamento", "distrito", "entidad",
        }
        if generic:
            params["entidad"] = mention
            params["entidad_resolution"] = {
                "value": None,
                "method": "actor_relation_buyer_ambiguous",
                "kind": "buyer_entity",
                "confidence": "none",
                "like_value": None,
                "metadata": {"buyer_mention": mention},
                "clarification_needed": True,
                "clarification_hint": (
                    f"¿A cuál {mention} te refieres? Indica la ciudad o el "
                    "departamento; no voy a escoger una entidad al azar."
                ),
            }
            return True

        if self.entity_resolver is not None:
            scanned = self.entity_resolver.scan_entity(mention)
            if scanned is not None:
                params["entidad"] = mention
                params["entidad_resolved"] = scanned.official_name
                params["entidad_resolution"] = {
                    "value": scanned.official_name,
                    "method": "actor_relation_scan_exact",
                    "kind": "buyer_entity",
                    "confidence": "high",
                    "like_value": None,
                    "metadata": {"buyer_mention": mention},
                }
                return True

            resolved = self.entity_resolver.resolve_entidad(mention)
            if (
                resolved.value
                and resolved.method == "exact"
                and resolved.confidence == "high"
            ):
                params["entidad"] = mention
                params["entidad_resolved"] = resolved.value
                params["entidad_resolution"] = {
                    **resolved.to_dict(),
                    "method": "actor_relation_exact",
                    "kind": "buyer_entity",
                    "metadata": {
                        **resolved.metadata,
                        "buyer_mention": mention,
                    },
                }
                return True

        # Reuse the router's established city→buyer projection.  This is a
        # buyer relation because syntax said CON, not because a city alias by
        # itself has buyer authority.
        city_buyer = _CITY_TO_ENTITY.get(mention)
        if city_buyer:
            params["entidad"] = mention.title()
            params["entidad_resolved"] = city_buyer
            params["entidad_resolution"] = {
                "value": city_buyer,
                "method": "actor_relation_city_to_entity",
                "kind": "buyer_entity",
                "confidence": "high",
                "like_value": None,
                "metadata": {"buyer_mention": mention},
            }
            return True
        return False

    # ── Actor (AP2) ─────────────────────────────────────────────────────
    # El documento y el nombre del actor se consumen aquí y NO en el
    # extractor de objeto: un actor que se filtra a `objeto` produce el
    # doble error que AP0 documentó — se pierden sus contratos cuando el
    # objeto no lo nombra, y se le atribuyen los de terceros que sí.

    def _consume_actor_document(
        self, normalized: str, scrubbed: str, params: dict,
    ) -> str:
        """Extrae el documento del actor y devuelve el texto ya sin él.

        Corre antes que fechas y montos porque un identificador leído como
        año o como cifra deja de ser un actor y se vuelve un filtro
        equivocado. Devuelve ``scrubbed`` para que el llamador no tenga que
        saber qué palabras se consumieron.
        """
        match = self._find_identifier_span(normalized)
        if match is None:
            return scrubbed

        if not actor_profile_enabled():
            # Camino previo a AP2, intacto.
            nit_variants = self._extract_nit(normalized)
            if not nit_variants:
                return scrubbed
            params["contratista"] = nit_variants
            return self._scrub_identifier_spans(scrubbed)

        reference = reference_from_document(match.group(0).strip(), normalized)
        if reference is None:
            # El crudo no produjo identidad utilizable (centinela, fuera de
            # rango). No se fabrica un actor: se deja el texto como estaba.
            return scrubbed

        variants = list(reference.document_variants)
        if not variants:
            return scrubbed

        asserted_actor = detect_asserted_actor_name_before_document(
            normalized, match.start(),
        )
        params["contratista"] = variants
        params.pop("actor_conversation_probe", None)
        params["actor_resolution"] = reference.resolution_method
        actor_scope = reference.to_params()
        if asserted_actor is not None and asserted_actor.observed_name:
            # Contextual evidence only.  The document remains the sole
            # execution authority and the resolver never adds a name filter.
            actor_scope["asserted_name"] = asserted_actor.observed_name
        params["actor_scope"] = actor_scope

        # Promoción de dataset, que NO es lo mismo que descartar el filtro.
        # "historial contractual del NIT X" y "contratación de la persona con
        # cédula Y" no contienen la palabra "contratos", así que la selección
        # por palabra clave las manda a procesos — el único dataset que no
        # publica quién contrata. Con un documento de actor sobre la mesa y
        # sin que el usuario haya pedido procesos, la fuente que puede
        # responder es contratos, y se elige esa. Cuando el usuario SÍ pidió
        # procesos su intención manda: ahí la política de capacidad bloquea
        # con una aclaración explícita en vez de tirar el actor
        # (INV-NO-SILENT-DROP).
        if not params.get("dataset_explicit"):
            params["dataset"] = DATASET_CONTRATOS

        scrubbed = self._scrub_identifier_spans(scrubbed)
        # Consume the atomic role/name span before its shorter role marker;
        # otherwise removing "proveedor" first would make the full span
        # impossible to match and leave the asserted name behind as a topic.
        if asserted_actor is not None:
            for span in asserted_actor.consumed_spans:
                scrubbed = scrubbed.replace(span, " ")
        for span in reference.consumed_spans:
            scrubbed = re.sub(
                rf"\b{re.escape(span)}\b", " ", scrubbed, flags=re.IGNORECASE,
            )
        return scrubbed

    @staticmethod
    def _scrub_identifier_spans(scrubbed: str) -> str:
        """Quita del texto los spans aceptados como identificador.

        Solo esos: una consulta puede traer NIT y monto a la vez, y el monto
        debe seguir visible para el paso 9.
        """
        scrubbed = _NIT_SHAPE_RE.sub(
            lambda candidate: (
                " " if _number_role(scrubbed, candidate) == "identifier"
                else candidate.group(0)
            ),
            scrubbed,
        )
        return re.sub(r"\bnit\b", " ", scrubbed, flags=re.IGNORECASE)

    def _detect_named_actor(
        self,
        normalized: str,
        scrubbed: str,
        params: dict,
        *,
        recognized_modifier_tails: tuple[str, ...] = (),
    ) -> str:
        """Clasifica un actor NOMBRADO sin resolver quién es (INV-NAME-NO-GUESS).

        Marca la consulta como dirigida a un actor y deja constancia de que
        falta el documento. No escribe ``contratista``: hacerlo dispararía un
        ``LIKE '%nombre%'`` sobre ``proveedor_adjudicado`` que fusiona actores
        distintos —"FUNDACARIBE" coincide con "FUNDACARIBE SAS" y con
        "CORPORACIÓN FUNDACARIBE DEL NORTE"— y presentaría la suma como la
        huella de uno solo.
        """
        reference = detect_actor_name(
            normalized,
            recognized_modifier_tails=recognized_modifier_tails,
        )
        if reference is None:
            return scrubbed

        params["actor_resolution"] = reference.resolution_method
        params.setdefault(
            "actor_conversation_operation",
            ActorConversationOperation.SET_ACTOR,
        )
        params.setdefault(
            "actor_conversation_construction", "strong_actor_syntax",
        )
        params["actor_scope"] = reference.to_params()
        for span in reference.consumed_spans:
            scrubbed = scrubbed.replace(span, " ")
        return scrubbed

    @staticmethod
    def _find_identifier_span(normalized_query: str) -> "re.Match[str] | None":
        """Primer span con forma de identificador que ADR-059 no lee como monto.

        Extraído de ``_extract_nit`` para que la ruta de actor obtenga el
        crudo VERBATIM —con sus puntos y su guion— sin repetir el arbitraje
        número-vs-monto ni desincronizarse de él. AP1 necesita el crudo,
        no los dígitos ya limpios: es el separador lo que distingue un DV
        declarado por la fuente de una hipótesis del módulo 11.
        """
        return next(
            (
                candidate
                for candidate in _NIT_SHAPE_RE.finditer(normalized_query)
                if _number_role(normalized_query, candidate) == "identifier"
            ),
            None,
        )

    def _extract_nit(self, normalized_query: str) -> list[str] | None:
        """Detecta NITs colombianos por forma y retorna variantes base + con DV.

        Retorna lista con nit_base y nit_completo (si hay DV) para que el
        SoQL builder pueda hacer OR sobre documento_proveedor.
        Retorna None si no hay NIT en la query.

        Camino LEGACY: sigue siendo la ruta con ``ACTOR_PROFILE`` apagado.
        Con el flag encendido gobierna ``actor_intent.reference_from_document``,
        que separa el DV validándolo en vez de exigir un separador.
        """
        match = self._find_identifier_span(normalized_query)
        if match is None:
            return None

        raw = match.group(0).strip()
        # Extraer solo dígitos
        digits = re.sub(r"[^0-9]", "", raw)

        # Verificar longitud: 8-11 dígitos totales (8-10 base + 1 DV opcional)
        if not (8 <= len(digits) <= 11):
            return None

        # El DV es el último dígito si el raw tenía separador antes del último dígito
        # O si el total de dígitos es 10-11 (base 9-10 + DV)
        # Heurística: si el número formateado tiene un guion/punto antes de 1 dígito final → DV
        has_dv_separator = bool(re.search(r"[.\- ]\d$", raw.rstrip()))
        if has_dv_separator:
            nit_base = digits[:-1]
            nit_completo = digits
            return [nit_base, nit_completo]
        else:
            # Sin separador de DV — retornamos el número tal cual
            return [digits]

    def _extract_contractor(self, normalized_query: str, dataset: str) -> str | None:
        if dataset != DATASET_CONTRATOS:
            return None
        pattern = re.compile(
            r"(?:contratista|proveedor)(?:\s+adjudicado)?\s+(.+?)(?=\s+(?:en|por|desde|hasta|mayor|menor|mas|menos)\b|$)"
        )
        match = pattern.search(normalized_query)
        return match.group(1).strip(" ,.") if match else None

    def _extract_object_terms(self, scrubbed_query: str) -> list[str | list[str]]:
        cleaned = scrubbed_query
        for alias in ESTADO_SYNONYMS:
            cleaned = re.sub(rf"\b{re.escape(alias)}\b", " ", cleaned)
        cleaned = re.sub(r"\b(?:\d+(?:[.,]\d+)?)\b", " ", cleaned)
        cleaned = re.sub(
            rf"\b(?:millon(?:es)?|mil|billon(?:es)?|palos?|barras?|pesos|{SMMLV_UNIT_PATTERN})\b",
            " ",
            cleaned,
        )
        # Strip punctuation that can stick to tokens (e.g., "actualmente?")
        cleaned = re.sub(r"[?¿!¡.,;:\"'(){}[\]]", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        # Detect registered multiword phrases BEFORE individual tokenization.
        # KNOWN_BIGRAMS contains all multi-word keys from MORPHOLOGICAL_VARIANTS
        # (bigrams, trigrams, etc.). Greedy: longest match wins.
        words = cleaned.split()
        tokens: list[str] = []
        i = 0
        # Pre-sort phrases by length descending for greedy longest-match
        _sorted_phrases = sorted(KNOWN_BIGRAMS, key=len, reverse=True)
        while i < len(words):
            matched = False
            for phrase_tuple, root in _OR_ONLY_PHRASES.items():
                n = len(phrase_tuple)
                if tuple(words[i : i + n]) == phrase_tuple and (
                    (i > 0 and words[i - 1] == "o") or (i + n < len(words) and words[i + n] == "o")
                ):
                    tokens.append(root)
                    i += n
                    matched = True
                    break
            if matched:
                continue
            for phrase_tuple in _sorted_phrases:
                n = len(phrase_tuple)
                if tuple(words[i : i + n]) == phrase_tuple:
                    surface = " ".join(phrase_tuple)
                    tokens.append(VARIANT_TO_ROOT.get(surface, surface))
                    i += n
                    matched = True
                    break
            if not matched:
                token = words[i]
                if token == "o":
                    tokens.append(token)
                elif token not in STOPWORDS and token not in DEPARTAMENTOS and (
                    len(token) >= 4 or token in VARIANT_TO_ROOT
                ):
                    tokens.append(VARIANT_TO_ROOT.get(token, token))
                i += 1

        # ── PROCUREMENT-ACTION-001: scrub verbos contractuales ──
        # Si hay tokens sustantivos (no verbos), eliminar los verbos de acción.
        # Los verbos solos (sin sustantivos) se mantienen para no romper queries
        # como "oportunidades de contratacion".
        from app.core.intent_vocabulary import ACTION_VERBS
        non_verb_tokens = [t for t in tokens if t != "o" and t not in ACTION_VERBS]
        if non_verb_tokens:
            tokens = [t for t in tokens if t not in ACTION_VERBS]

        grouped_tokens: list[str | list[str]] = []
        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token == "o":
                i += 1
                continue
            if i + 2 < len(tokens) and tokens[i + 1] == "o" and tokens[i + 2] != "o":
                group = [token]
                i += 2
                while i < len(tokens):
                    group.append(tokens[i])
                    if i + 2 < len(tokens) and tokens[i + 1] == "o" and tokens[i + 2] != "o":
                        i += 2
                    else:
                        i += 1
                        break
                group_seen: set[str] = set()
                unique_group: list[str] = []
                for item in group:
                    if item not in group_seen:
                        unique_group.append(item)
                        group_seen.add(item)
                grouped_tokens.append(unique_group if len(unique_group) > 1 else unique_group[0])
                continue
            grouped_tokens.append(token)
            i += 1

        seen: set[str] = set()
        unique: list[str | list[str]] = []
        for t in grouped_tokens:
            key = "|".join(t) if isinstance(t, list) else t
            if key not in seen:
                unique.append(t)
                seen.add(key)
        return unique[:6]

    def _needs_llm(self, normalized_query: str, params: dict[str, object]) -> bool:
        """Detect when heuristic likely misclassified tokens.

        Triggers:
        1. "en [place]" where place is NOT a resolved department → likely a city
        2. Entity-signal words in objeto (secretaria, ministerio, etc.)
        """
        objeto = params.get("objeto", [])
        if not objeto:
            return False
        objeto_terms: list[str] = []
        for item in objeto:
            if isinstance(item, list):
                objeto_terms.extend(str(sub_item) for sub_item in item)
            else:
                objeto_terms.append(str(item))

        # Signal 1: "en [place]" not resolved as department
        if not params.get("departamento_resolved"):
            match = _PREPOSITION_PLACE_RE.search(normalized_query)
            if match:
                place_tokens = set(match.group(1).split())
                objeto_set = set(objeto_terms)
                if place_tokens & objeto_set:
                    return True

        # Signal 2: Entity-like words in objeto
        if set(objeto_terms) & _ENTITY_SIGNAL_WORDS:
            return True

        return False
