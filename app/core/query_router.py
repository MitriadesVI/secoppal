from __future__ import annotations

import re
from pathlib import Path
from dataclasses import dataclass, replace
from datetime import date as Date
from typing import TYPE_CHECKING

from app.data.departamentos import DEPARTAMENTOS
from app.data.stopwords import STOPWORDS
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
    buyer_contract_request_spans,
    recognized_actor_modifier_tails,
    reference_from_document,
)
from app.core.asserted_actor_name import detect_bare_asserted_actor_name
from app.core.contract_invariants import contains_signed_token
from app.core.temporal_semantics import (
    TemporalResolution,
    civil_today,
    resolve_temporal_scope,
)
from app.core.contract_type_resolver import resolve_contract_types
from app.core.analytic_control import (
    AnalyticControlSpan,
    control_spans as analytic_control_spans,
    locate_analytic_controls,
)
from app.core.capability_binding.ranking_criterion import ranking_criterion_spans
from app.core.control_language import interpret_control_language
from app.core.material_residual import (
    MaterialEvidence,
    OccurrenceOwnership,
    material_evidence,
)
from app.core.semantic_turn_evidence import SemanticTurnEvidence
from app.core.explicit_content import (
    adjudicate_buyer_ownership,
    explicit_content_occurrences,
    strong_content_spans,
)
from app.core.state_predication import state_predicate
from app.core.money_polarity import (
    MoneyRelation,
    has_money_relation_before,
    resolve_money_polarity,
)
from app.core.estado_projection import legacy_estado_scalar, project_estado_family
from app.core.generic_buyer import (
    build_incomplete_buyer_resolution,
    detect_incomplete_buyer,
    is_territory_fragment_of_buyer,
)
from app.core.money_admission import (
    RANGE_PARTIAL_CLARIFICATION,
    RangeVerdict,
    admit_range,
    has_explicit_monetary_evidence,
    is_admissible_amount,
    protected_money_spans,
)
from app.core.intent_vocabulary import PAST_PROCUREMENT_ACTION_RE
from app.core.negation_binding import (
    NegationDimension,
    NegationRequest,
    NegationVerdict,
    attach_scope_anchors,
    bind_negations,
)
from app.core.negation_policy import apply_negation_policy
from app.core.modality_parser_integration import resolve_parser_modality
from app.core import period_free_text
from app.core.query_text_consumption import (
    compact_whitespace,
    mask_consumed_spans,
    replace_first_literal,
    replace_regex,
    replace_text_span,
)
from app.core.source_occurrence import (
    is_source_word,
    mask_source_requests,
    source_request_occurrences,
)
from app.utils.money import (
    SMMLV_UNIT_PATTERN,
    is_smmlv_expression,
    money_to_cop,
    MONETARY_ATOMIC_SCALE_UNIT_PATTERN,
    MONETARY_SCALE_UNIT_PATTERN,
    normalize_text,
    smmlv_to_cop,
)
from app.data.smmlv import current_colombian_year, smmlv_cop
from app.core.text_lineage import (
    Proof,
    TextLineageDraft,
    TextLineageView,
    normalize_query_with_lineage,
)
from app.utils.spell_correction import correct_query, correct_query_with_receipts

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

# Opportunity keywords that are also source words (source_occurrence): they
# seed an opportunity search only through an accredited process occurrence.
_OPPORTUNITY_SOURCE_WORDS = frozenset({"convocatoria", "convocatorias"})


def opportunity_head_occurrences(occurrences) -> tuple:
    """The occurrences of "convocatoria(s)" accredited as a PROCESS request.

    Ownership is per occurrence (span), never per surface: in "convocatorias
    de obra y servicios de publicación de convocatorias" only the first one is
    a head. ``occurrences`` comes from ``source_request_occurrences``.
    """
    return tuple(
        occurrence for occurrence in occurrences
        if occurrence.is_process and occurrence.surface in _OPPORTUNITY_SOURCE_WORDS
    )

# Dual-dataset: el usuario pide procesos Y contratos en el mismo turno.
# Conjunción explícita ("contratos o procesos", "procesos y contratos") o un
# marcador de cobertura total ("ambos", "todo lo relacionado con").
_DUAL_DATASET_PATTERNS = (
    r"\bambos\b",
    r"\btodo lo relacionado\b",
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
    if re.search(r"(?:\$\s*|\bcop\s+)$", left) or has_money_relation_before(left):
        return "money"
    return "identifier"

# El lenguaje funcional vive en app/data/stopwords.py: tiene dos consumidores
# (este extractor y el corrector ortográfico) y una sola autoridad.

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
    text_lineage: TextLineageView | None = None
    # One parse-owned receipt beside params, never inside executable params.
    semantic_evidence: SemanticTurnEvidence | None = None

    @property
    def analytic_controls(self) -> tuple[AnalyticControlSpan, ...]:
        return self.semantic_evidence.analytic_controls if self.semantic_evidence else ()

    @property
    def material_residual(self) -> tuple[str, ...]:
        return self.semantic_evidence.material_residual if self.semantic_evidence else ()

    @property
    def material_evidence(self) -> MaterialEvidence | None:
        return self.semantic_evidence.material_evidence if self.semantic_evidence else None


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

    def parse(
        self,
        user_query: str,
        *,
        today: Date | None = None,
        text_lineage_request_id: str | None = None,
    ) -> ParsedQuery:
        clock = civil_today(today)
        if not isinstance(user_query, str):
            # Preserve the canonical normalizer's public exception in opt-in
            # mode instead of letting observability become a new input oracle.
            normalize_text(user_query)
        lineage = (
            TextLineageDraft(user_query, request_id=text_lineage_request_id)
            if text_lineage_request_id is not None
            else None
        )
        normalized = (
            normalize_query_with_lineage(user_query, lineage)
            if lineage is not None
            else normalize_text(user_query)
        )
        from app.core.municipal_territory import get_municipal_territories
        municipal_geography = get_municipal_territories()
        correction_protected_spans = (
            *municipal_geography.name_spans(normalized),
            *protected_money_spans(normalized),
        )
        if lineage is None:
            normalized = correct_query(
                normalized,
                protected_spans=correction_protected_spans,
            )
        else:
            normalized = correct_query_with_receipts(
                normalized,
                protected_spans=correction_protected_spans,
                receipt_sink=lambda output, receipts: lineage.apply_partition(
                    output,
                    receipts,
                    stage="spell_correction",
                    operation="CORRECT",
                ),
            ).text
        source_occurrences = source_request_occurrences(normalized)
        from app.core.contract_universe_intent import request, apply_request, state_authorized
        universe_request = request(user_query, protected_surfaces=tuple(
            user_query[a:b] for a, b in self.entity_resolver.proper_names.spans_in(user_query)
        ) if self.entity_resolver else ())
        params: dict[str, object] = {}
        actor_conversation = ActorConversationInstruction()
        actor_relation_spans: tuple[str, ...] = ()

        # ── 0. Semantic normalization layer (runs BEFORE existing extractors) ──
        #    This prevents tokens like "publicados", "departamento", "alcaldía"
        #    from leaking into 'objeto' and poisoning LIKE clauses.
        consumed_spans: list[str] = []  # track consumed text for debugging
        actor_modifier_spans: list[str] = []
        # Spans de texto CRUDO que sembraron una familia de estado antes de que
        # la negación estuviera resuelta. Si la negación se los come, la
        # siembra deja de valer (ver la revocación tras el binding).
        state_seed_spans: list[str] = []

        # ── 0a. INTENT extraction (bidder intent → config overrides) ─────
        from app.core.intent_vocabulary import extract_intent
        intent_config, intent_span = extract_intent(normalized)
        if intent_config:
            if intent_config.get("force_dataset"):
                params["dataset"] = intent_config["force_dataset"]
            if intent_config.get("estado_family"):
                params["estado_family"] = intent_config["estado_family"]
                state_seed_spans.append(intent_span)
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
        # OPPORTUNITY_HEAD_OCCURRENCE_AUTHORITY_V1: "convocatoria(s)" is a
        # source word, so presence proves nothing ("contratos de publicación
        # de convocatorias" asks for contratos). Only an occurrence that
        # source_request_occurrences accredits as a PROCESS request may seed
        # the opportunity search; a topic occurrence of the same word may not.
        opportunity_hits = [
            kw for kw in opportunity_keywords
            if kw not in _OPPORTUNITY_SOURCE_WORDS and kw in normalized
        ]
        opportunity_hits.extend(dict.fromkeys(
            occurrence.surface for occurrence in opportunity_head_occurrences(source_occurrences)
        ))
        # "procesos abiertos" is a state phrase as much as an opportunity
        # keyword: the intent it sets belongs to the process-state decision
        # (step 7), which reads the same word wherever it stands in the text.
        # "oportunidades" or a bidder intent is an independent authority.
        from app.core.estado_families import read_process_state_mentions
        intent_from_state_words = (
            bool(opportunity_hits)
            and params.get("intent_type") != "opportunity_search"
            and all(
                any(m.family == "oferta_abierta" and not m.literal
                    for m in read_process_state_mentions(kw))
                for kw in opportunity_hits
            )
        )
        if opportunity_hits:
            state_seed_spans.extend(opportunity_hits)
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
        source_kinds = {occurrence.is_process for occurrence in source_occurrences}
        if len(source_kinds) == 2 or any(
            re.search(pattern, normalized) for pattern in _DUAL_DATASET_PATTERNS
        ):
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
        preliminary_dates = self._extract_dates(normalized, today=clock)
        _, preliminary_amount_texts = self._extract_amounts(
            normalized, preliminary_dates, reference_year=clock.year,
        )
        preliminary_amount_spans = tuple({
            match.span()
            for amount_text in preliminary_amount_texts
            for match in re.finditer(re.escape(amount_text), normalized)
        })
        actor_document_matches = self._find_identifier_spans(normalized)
        # ── 1.c.0 Analytic control (count / group by) ──────────────────
        # Located before negation so an instruction about the results can
        # neither be excluded as content nor be swallowed by a preceding
        # ``sin``.  Spans owned by other authorities — the SUPPLIERS ranking
        # metric, proper names, municipal names, resolved modality aliases —
        # are never claimed.
        analytic_controls = locate_analytic_controls(
            normalized,
            protected_spans=(
                *ranking_criterion_spans(normalized),
                *(occurrence.span for occurrence in explicit_content_occurrences(normalized)),
                *(
                    self.entity_resolver.proper_names.spans_in(normalized)
                    if self.entity_resolver else ()
                ),
            ),
            modality_spans=modality_result.interpretation.consumed_spans,
            candidate_spans=municipal_geography.name_spans(normalized),
        )
        analytic_spans = analytic_control_spans(analytic_controls)
        negation_binding = bind_negations(
            normalized,
            city_pattern=_CITY_PATTERN_RE,
            modality_spans=modality_result.interpretation.consumed_spans,
            protected_spans=preliminary_amount_spans,
            actor_document_spans=tuple(
                match.span() for match in actor_document_matches
            ),
            control_spans=analytic_spans,
        )
        positive_text = mask_consumed_spans(
            negation_binding.masked_text, analytic_spans,
        )
        # Unsupported negation must not erase a monetary request before its
        # authority runs. Recognized money negations are already protected.
        if any(
            normalized[start:end] != positive_text[start:end]
            and has_explicit_monetary_evidence(normalized[start:end])
            for start, end in preliminary_amount_spans
        ):
            params["money_clarification"] = (
                "No pude interpretar la relación monetaria bajo esa negación. "
                "¿Quieres un mínimo, un máximo, un rango o un valor aproximado?"
            )
        # ── 1.c.2 Revocación de siembras pre-negación ──────────────────
        # INV-NO-SILENT-SEMANTIC-FALLBACK. El atajo de oportunidades y el
        # vocabulario de intención escanean el texto CRUDO para poder elegir
        # dataset antes de que exista un binding. Eso los deja leyendo tokens
        # que el usuario negó: "procesos que no estén en convocatoria" sembraba
        # la familia de oferta abierta —el universo exactamente contrario— sin
        # que ninguna capa posterior pudiera notarlo. La negación se resuelve
        # antes que cualquier polaridad positiva; si el span que sembró el
        # estado ya no está en el texto positivo, la siembra se cae entera.
        if state_seed_spans and any(
            seed and seed not in positive_text for seed in state_seed_spans
        ):
            for key in (
                "estado_family", "estado_del_procedimiento",
                "estado_de_apertura_del_proceso", "estado_contrato",
                "intent_type", "opportunity_mode", "filter_active_only",
            ):
                params.pop(key, None)
        scrubbed = mask_consumed_spans(
            normalized,
            modality_result.interpretation.consumed_spans,
            lineage=lineage,
            stage="modality",
        )
        if scrubbed != modality_result.masked_text:
            raise RuntimeError("lineage modality mutation diverged from parser output")
        scrubbed = mask_consumed_spans(
            scrubbed,
            negation_binding.masked_spans,
            lineage=lineage,
            stage="negation",
        )
        scrubbed = mask_consumed_spans(
            scrubbed,
            analytic_spans,
            lineage=lineage,
            stage="analytic_control",
        )
        consumed_spans.extend(control.evidence_text for control in analytic_controls)
        actor_modifier_spans.extend(
            control.evidence_text for control in analytic_controls
        )
        if universe_request and universe_request.span != (0, 0):
            control = normalize_text(user_query[slice(*universe_request.span)])
            scrubbed = replace_first_literal(
                scrubbed,
                control,
                " ",
                lineage=lineage,
                stage="contract_universe",
                operation="MASK",
                proof=Proof.EXACT,
                placeholder=True,
            )
        temporal_resolution = resolve_temporal_scope(
            positive_text,
            today=clock,
            protected_spans=tuple(_protected_entity_year_spans(positive_text)),
        )
        # Each mention on its own: the span of a window next to another period ENVELOPES what is written
        # between them (a buyer), and masking it erased the buyer from the residual text.
        period_spans = period_free_text.consumed(positive_text, temporal_resolution.mentions)
        period_mentions = period_free_text.mention_texts(positive_text, period_spans)
        if temporal_resolution.span is not None:
            scrubbed = mask_consumed_spans(
                scrubbed,
                period_spans,
                lineage=lineage,
                stage="temporal",
            )
            consumed_spans.extend(
                period_mentions if len(period_spans) > 1
                else (temporal_resolution.phrase,)
            )

        # AP3-F projects roles only from positive residual text.  A provider or
        # buyer governed by negation remains a typed NegationRequest and cannot
        # become positive actor/entity evidence.
        if actor_profile_enabled():
            actor_conversation = period_free_text.read_buyer(
                positive_text,
                period_spans,
                period_mentions,
                interpret_actor_conversation,
                lambda mention: self._project_actor_relation_buyer(mention, {}),
                self._claimed_by_declared_group,
            )
            actor_conversation, actor_relation_spans = (
                self._seed_actor_conversation(actor_conversation, params)
            )
        for relation_span in actor_relation_spans:
            scrubbed = replace_regex(
                scrubbed,
                period_free_text.aligned_pattern(
                    period_free_text.without_mentions(relation_span, period_mentions)
                    or relation_span
                ),
                " ",
                count=1,
                flags=re.IGNORECASE,
                lineage=lineage,
                stage="actor_relation",
                operation="MASK",
                proof=Proof.EXACT,
            )
        scrubbed = compact_whitespace(
            scrubbed,
            lineage=lineage,
            stage="pre_contract_type_whitespace",
        )
        modality_spans = [
            normalized[start:end]
            for start, end in modality_result.interpretation.consumed_spans
        ]
        consumed_spans.extend(modality_spans)
        actor_modifier_spans.extend(modality_spans)

        # ── 1.d Contractual type/nature (independent from modality) ─────
        # A recognized contractual class owns its span before the catch-all
        # thematic extractor runs.  Projection is source-backed and dataset-
        # aware; this layer only emits canonical class IDs.
        contract_type_result = resolve_contract_types(
            scrubbed,
            lineage=lineage,
            emit_receipts=lineage is not None,
        )
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
        scrubbed = self._consume_actor_document(
            positive_text,
            scrubbed,
            params,
            lineage=lineage,
        )

        # ── 2. Dates (regex — reliable for numbers) ─────────────────────
        date_params = self._temporal_params(temporal_resolution)
        params.update(date_params)

        # ── 3. Strip year tokens so they don't contaminate scans ────────
        # Lookahead negativo: no capturar años seguidos de unidades de dinero
        # (ej: "2000 millones" no es el año 2000)
        _year_re = re.compile(
            rf"\b(20\d{{2}})\b(?!\s*(?:millones?|millon|mil\s+millones|billones?|palos?|barras?|pesos|{SMMLV_UNIT_PATTERN}))",
            re.IGNORECASE,
        )
        for m in re.finditer(_year_re, scrubbed):
            scrubbed = replace_regex(
                scrubbed,
                rf"\b{re.escape(m.group(0))}\b",
                " ",
                lineage=lineage,
                stage="year_stripping",
                operation="MASK",
                proof=Proof.EXACT,
            )
        scrubbed = compact_whitespace(
            scrubbed,
            lineage=lineage,
            stage="pre_control_whitespace",
        )

        # ── 4. Control language (ordering + command/intent verbs) ───────
        # CONTROL vs CONTENT: a command about how to present or navigate the
        # results is not part of what the user is looking for. Consuming only
        # the criterion ("mayor valor") and leaving the command around it
        # ("ordenados por") shipped LIKE '%ordenados%' to SECOP and returned
        # zero rows. The control authority owns the whole span.
        text_lineage = lineage.seal(scrubbed) if lineage is not None else None
        protected_names = (
            self.entity_resolver.proper_names.spans_in(scrubbed)
            if self.entity_resolver else []
        )
        control = interpret_control_language(
            scrubbed, protected_spans=tuple(protected_names + municipal_geography.name_spans(scrubbed)),
        )
        if control.ordering_signal:
            params["ordering_signal"] = control.ordering_signal
        if control.consumed_spans:
            consumed_spans.extend(control.consumed_spans)
            actor_modifier_spans.extend(control.consumed_spans)
        scrubbed = control.residual_text

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
        # Explicit municipal geography precedes legacy buyer aliases and the
        # department scan, so `en Los Patios` cannot accredit a mayoralty.
        protected_names = (
            self.entity_resolver.proper_names.spans_in(scrubbed)
            if self.entity_resolver else []
        )
        geo_params, scrubbed, geo_spans = municipal_geography.consume(
            scrubbed, protected_spans=protected_names,
        )
        params.update(geo_params)
        consumed_spans.extend(geo_spans)
        actor_modifier_spans.extend(geo_spans)

        if "departamento_resolved" not in params:
            from app.core.entity_resolver import department_mention_resolution
            _km = re.search(
                r"\b(?:departamento|depto\.?)\s+de(?:l)?\s+(.+)", scrubbed, re.IGNORECASE,
            )
            if _km:
                _rest = _km.group(1).split()
                for _n in (3, 2, 1):
                    _cand = " ".join(_rest[:_n])
                    _geo = department_mention_resolution(_cand)
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

        # Declared groups consume their own phrase before institutional rewrite.
        # A remaining explicit BuyerUnit is resolved normally and wins below.
        from app.core.buyer_groups import BuyerGroupError, consume_group_mention
        try:
            group_scope, scrubbed = consume_group_mention(scrubbed, self.entity_resolver)
            params.update(group_scope)
        except BuyerGroupError as exc:
            params["buyer_group_error"] = str(exc)

        # ── 4.6. ENTITY REWRITE: try canonical variants for entity patterns ──
        #    "alcaldía de X" → "Municipio de X" / "Alcaldía de X"
        #    Runs BEFORE gazetteer scan so variants feed into alias matching.
        from app.core.entity_resolver import is_department_root_name
        from app.core.entity_types import rewrite_entity
        from app.core.compound_identity_guard import identity_guards
        entity_candidates = rewrite_entity(scrubbed)
        municipal_guards = identity_guards(entity_candidates, municipal_geography)
        accredited_name_spans = (
            self.entity_resolver.proper_names.spans_in(scrubbed)
            if self.entity_resolver else []
        )
        rewritten_entity = None
        unresolved_rewrite = None
        buyer_ownership: OccurrenceOwnership | None = None
        if entity_candidates and self.entity_resolver and not params.get("entidad_resolved"):
            # Los candidatos llegan agrupados por span, del más largo al más
            # corto. Dentro de UN span compiten formas canónicas rivales
            # ("Empresa Social del Estado X" vs "ESE X"): ahí un exacto le gana
            # a un fuzzy aunque llegue después, porque el orden del replacer es
            # una preferencia de redacción, no evidencia. Sin esto "ESE Hospital
            # San Rafael de Fusagasugá" —que existe— perdía contra el fuzzy(95)
            # del hospital de Facatativá.
            pending_fuzzy = None
            terminal = False
            for candidate in entity_candidates:
                canonical = candidate.canonical
                consumed = candidate.consumed_span
                rtype = candidate.rewrite_type
                # A trailing-token fallback cannot split an accredited name
                # starting here. Otherwise a newly added shorter buyer steals
                # a longer existing institution before its own rewrite runs.
                span = candidate.occurrence_span
                if span is None or scrubbed[slice(*span)] != consumed:
                    continue
                start = span[0]
                if any(a == start and b > start + len(consumed) for a, b in accredited_name_spans):
                    continue
                identity = municipal_guards.get(rtype)
                if identity and len(consumed) <= len(identity.protected_span):
                    # Consume the entire protected identity if no full exact
                    # buyer wins; neither shrink nor later scan may reopen it.
                    protected_span = (span[0], span[0] + len(identity.protected_span))
                    if scrubbed[slice(*protected_span)] != identity.protected_span:
                        continue
                    unresolved_rewrite = (identity.protected_span, identity.protected_span,
                                          rtype, identity.unresolved(), protected_span)
                    if consumed != identity.protected_span:
                        continue
                if pending_fuzzy and pending_fuzzy[1] != consumed:
                    rewritten_entity = pending_fuzzy
                    break
                match = self.entity_resolver.resolve_entidad(canonical)
                if identity and consumed == identity.protected_span:
                    if not identity.accepts(canonical, match, self.entity_resolver, municipal_geography):
                        continue
                if match and match.value and match.method != "fallback_like":
                    # Un hospital no se llama "DEPARTAMENTO DE SANTANDER". Al
                    # capturar el nombre completo, el fuzzy sin puerta de las
                    # familias sectoriales empezaba a contestar una pregunta por
                    # un hospital con el comprador raíz del territorio: error de
                    # categoría, peor que no responder.
                    if candidate.rejects_territorial_root and is_department_root_name(
                        str(match.value)
                    ):
                        continue
                    # COMPOUND-BUYER-CAPTURE (R2A.1). El span salió de acortar
                    # un nombre articulado con ``de|del``: es un FRAGMENTO. Un
                    # fragmento se acredita por alias exacto, nunca por parecido
                    # — si no, "Villa" (de "Villa de Leyva") resuelve a
                    # VILLAVICENCIO con confianza media y nadie se entera.
                    if match.method.startswith("fuzzy"):
                        if candidate.requires_deterministic:
                            continue
                        if pending_fuzzy is None:
                            pending_fuzzy = (canonical, consumed, rtype, match, span)
                        continue
                    rewritten_entity = (canonical, consumed, rtype, match, span)
                    break
                if match and match.clarification_needed and unresolved_rewrite is None:
                    unresolved_rewrite = (canonical, consumed, rtype, match, span)
                # A complete, valid department name with no unique root owner
                # is a terminal fail-closed result.  Trying shorter rewrite
                # candidates would turn "San Andrés" into "San" and reopen
                # lexical fuzzy against an unrelated buyer (Santander).
                if match and match.metadata.get("canonical_department"):
                    terminal = True
                    break
            if not rewritten_entity and pending_fuzzy and not terminal:
                rewritten_entity = pending_fuzzy
            if not rewritten_entity and unresolved_rewrite:
                _, consumed, rtype, _, span = unresolved_rewrite
                identity = municipal_guards.get(rtype)
                if identity and consumed == identity.protected_span:
                    district = identity.district_alias(self.entity_resolver, municipal_geography)
                    if district:
                        rewritten_entity = (district[0], consumed, rtype, district[1], span)
            if rewritten_entity and adjudicate_buyer_ownership(
                scrubbed, rewritten_entity[4],
            ) is None:
                rewritten_entity = None
            if rewritten_entity:
                canonical, consumed, rtype, match, span = rewritten_entity
                buyer_ownership = adjudicate_buyer_ownership(scrubbed, span)
                assert buyer_ownership is not None and buyer_ownership.valid()
                params["entidad"] = canonical
                params["entidad_resolved"] = match.value
                params["entidad_resolution"] = {
                    "value": match.value,
                    "method": f"rewrite_{rtype}",
                    "kind": "buyer_entity",
                    # Keep the historical global rewrite contract.  Capability-
                    # specific admission owns any stricter fuzzy policy; the
                    # router must not demote every fuzzy buyer in SECOPPAL.
                    "confidence": "high",
                    "like_value": None,
                    "metadata": {
                        **match.metadata,
                        "rewritten_from": consumed,
                        # El method de arriba nombra la GRAMÁTICA que disparó
                        # ("rewrite_gobernación"), no cómo se acreditó la
                        # identidad. Sin esto, un fuzzy interno queda invisible
                        # y cualquier auditoría declara ROOT_FUZZY=0 mirando el
                        # dato en vez de la ejecución (R2A.1 §5).
                        "resolution_method": match.method,
                    },
                }
                consumed_spans.append(consumed)
                scrubbed = mask_consumed_spans(scrubbed, (span,))
                scrubbed = re.sub(r"\s+", " ", scrubbed).strip()
            elif unresolved_rewrite:
                canonical, consumed, rtype, match, span = unresolved_rewrite
                params["entidad"] = canonical
                params["entidad_resolution"] = {
                    **match.to_dict(),
                    "method": f"rewrite_{rtype}_unresolved",
                    "metadata": {
                        **match.metadata,
                        "rewritten_from": consumed,
                        "resolution_method": match.method,
                    },
                }
                consumed_spans.append(consumed)
                scrubbed = mask_consumed_spans(scrubbed, (span,))
                scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 5. GAZETTEER SCAN: Entity (longest alias match) ─────────────
        #    This is the core change: detect entities by substring match
        #    against 10K+ aliases, NOT by regex pattern matching.
        #    Skip if entity was already resolved by rewrite (step 4.5).
        if self.entity_resolver and "entidad_resolved" not in params:
            scan_text = mask_consumed_spans(scrubbed, strong_content_spans(scrubbed))
            entity_scan = self.entity_resolver.scan_entity(scan_text)
            if entity_scan:
                if (
                    entity_scan.occurrence_span is None
                    or scrubbed[slice(*entity_scan.occurrence_span)] != entity_scan.matched_alias
                ):
                    raise RuntimeError("entity scan occurrence does not match its source")
                buyer_ownership = adjudicate_buyer_ownership(
                    scrubbed, entity_scan.occurrence_span,
                )
                assert buyer_ownership is not None and buyer_ownership.valid()
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
                scrubbed = mask_consumed_spans(scrubbed, (entity_scan.occurrence_span,))
                scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

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
                    # ACTOR-GEO-SPAN-OWNERSHIP-001. The canonical zone name is
                    # not the word the user typed: "llanos" becomes
                    # "Orinoquia". Without the alias, a later owner —the actor
                    # span, a negation, an audit— can only compare the
                    # canonical value against the text and concludes the
                    # filter came from nowhere. Every other geography key
                    # already carries its provenance; region did not.
                    params["region_resolution"] = {
                        "value": zone,
                        "method": region_scan.method,
                        "kind": "territorial_region",
                        "matched_alias": region_scan.matched_alias,
                    }
                    scrubbed = region_scan.remaining_text

        # ── 6.2 GAZETTEER SCAN: Department (on remaining text) ──────────
        #    Runs AFTER entity scan so "de santander" can't be stolen.
        #    A specific department wins over a broader zone: if one resolves,
        #    drop any zone captured in step 6.
        if self.entity_resolver:
            dept_scan = self.entity_resolver.scan_departamento(scrubbed)
            # INV-NO-SILENT-SEMANTIC-FALLBACK (ADR-069). Si el comprador ya
            # declaró su jurisdicción, un departamento escaneado que sea trozo
            # de ESE mismo nombre es la cola de la frase consumida, no un
            # filtro: "gobernación del Valle del Cauca" dejaba "del Cauca" y el
            # WHERE terminaba en el departamento del Cauca.
            if dept_scan:
                _buyer_meta = (params.get("entidad_resolution") or {}).get("metadata") or {}
                _buyer_department = _buyer_meta.get("canonical_department")
                if _buyer_department and is_territory_fragment_of_buyer(
                    dept_scan.official_name, str(_buyer_department)
                ):
                    scrubbed = dept_scan.remaining_text
                    dept_scan = None
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
                params.pop("region_resolution", None)

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
        from app.core.process_state_request import (
            OPPORTUNITY_HEAD_SURFACES, apply_process_state_decision, decide_process_states,
        )
        # INV-PROCESS-STATE-NO-SILENT-LOSS: read every named process state
        # before the single-answer producers below choose one of them. A dual
        # scope is still scalar procesos here (1.b); only the contract
        # universe moves it to contratos, at the end of the parse.
        # An opportunity head the opportunity authority already read
        # ("convocatorias", like "oportunidades") is a source noun, not a
        # second state: it is dropped by occurrence. "en convocatoria" is its
        # own multiword mention and stays a state reading.
        process_state_text = scrubbed
        process_states = tuple(
            mention for mention in read_process_state_mentions(process_state_text)
            if mention.surface.lower() not in OPPORTUNITY_HEAD_SURFACES
            or mention.surface.lower() not in opportunity_hits
        ) if params["dataset"] == DATASET_PROCESOS else ()
        seeded_families = {str(params["estado_family"])} if params.get("estado_family") else set()
        # The intent an independent authority set before any state word was
        # read; the process-state decision may add the funnel's intent, never
        # remove this one.
        intent_before_states = None if intent_from_state_words else params.get("intent_type")
        estado_family_result = detect_estado_family(scrubbed, str(params["dataset"]))
        if estado_family_result and not state_authorized(
            params['dataset'], estado_family_result['filters'].get('estado_contrato', ()), universe_request,
        ):
            estado_family_result = None
        if estado_family_result:
            for field, values in estado_family_result["filters"].items():
                params[field] = values
            params["estado_family"] = estado_family_result["family"]
            # Backward compat: set legacy estado/estado_field for old tests
            scalar = legacy_estado_scalar(estado_family_result["filters"])
            if scalar is not None:
                params["estado"], params["estado_field"] = scalar
            # Scrub the consumed span from text
            span = estado_family_result["consumed_span"]
            predicate = state_predicate(scrubbed, span)
            if predicate:
                # "figuran actualmente suspendidos": the copula reports the
                # state it precedes and has no topical reading.  Removed by
                # offset before the state scrub shifts the text.
                start, end = predicate
                consumed_spans.append(scrubbed[start:end])
                actor_modifier_spans.append(scrubbed[start:end])
                scrubbed = scrubbed[:start] + " " + scrubbed[end:]
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
                (*negation_binding.masked_spans, *analytic_spans),
            )
            primary_state = True
            while state_result := self._extract_state(
                state_scan_text,
                str(params["dataset"]),
            ):
                if not state_authorized(params['dataset'], state_result['value'], universe_request):
                    state_scan_text = re.sub(r"\b" + re.escape(state_result['source_text']) + r"\b",
                                             " ", state_scan_text, flags=re.I)
                    continue
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

        if process_states:
            decision = decide_process_states(process_states, seeded_families, process_state_text)
            # R3/R1: the decision is the only writer of what the mentions
            # mean. The producer above answered with the FIRST state; every
            # state key it derived by position is replaced, whole, so word
            # order cannot reach the SoQL.
            apply_process_state_decision(params, decision)
            if decision.funnel and not decision.clarification:
                params["intent_type"] = "opportunity_search"
            elif intent_before_states is None:
                params.pop("intent_type", None)
            else:
                params["intent_type"] = intent_before_states
            # Every named state belongs to the state authority, applied or
            # clarified; none of them is topic.
            for mention in process_states:
                consumed_spans.append(mention.surface)
                actor_modifier_spans.append(mention.surface)
                scrubbed = re.sub(
                    rf"\b{re.escape(mention.surface)}\b", " ", scrubbed,
                    count=1, flags=re.IGNORECASE,
                )

        # ── 9. Amounts ──────────────────────────────────────────────────
        # Amounts run over the progressively scrubbed text. Identifier-shaped
        # spans accepted by step 1.5 are therefore unavailable to the monetary
        # scanner, while currency-shaped spans rejected as NIT remain visible.
        amount_params, amount_spans = self._extract_amounts(
            scrubbed, date_params, reference_year=clock.year,
        )
        params.update(amount_params)
        if params.get("money_clarification"):
            params.pop("valor_min", None)
            params.pop("valor_max", None)
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

        # Same rule for the intent marker, and for the same reason. The intent
        # was read early to select the dataset, but its span is not always its
        # own: an accredited Actor mention owns the WHOLE construction that
        # names it — "que ha contratado ASILO DE SAN ANTONIO" is one span, not
        # a marker plus a leftover. Consuming the marker first left the actor
        # doors with a span they could no longer match, and the name survived
        # into ``objeto`` and into the buyer gazetteer. The semantic effect
        # (force_dataset) was applied at detection time and is unaffected.
        if (
            intent_config
            and intent_span
            and (intent_config.get("scrub_only") or "estado_family" in intent_config)
        ):
            scrubbed = scrubbed.replace(intent_span, " ", 1)
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

        # ── 10.7. Comprador institucional incompleto ──────────────────────
        # INV-INCOMPLETE-BUYER-CANNOT-BECOME-TOPIC. Si nadie logró resolver una
        # entidad y el texto solo menciona la CLASE ("de la alcaldía"), el token
        # no puede seguir viaje hasta ``objeto``: allí se convertiría en
        # LIKE '%alcaldia%' sobre el objeto del contrato — otra pregunta — y de
        # paso haría pasar por tema lo que es un comprador a medio nombrar.
        # Se rutea a ``entidad`` sin resolverla: la resolución dirá que falta
        # precisión y el turno se bloquea sin tocar la fuente.
        if not any(
            params.get(key)
            for key in ("entidad", "entidad_resolved", "entidad_like", "buyer_group")
        ):
            incomplete_buyer = detect_incomplete_buyer(normalized)
            if incomplete_buyer:
                params["entidad"] = incomplete_buyer
                params["entidad_resolution"] = build_incomplete_buyer_resolution(
                    incomplete_buyer,
                )
                consumed_spans.append(incomplete_buyer)
                scrubbed = re.sub(
                    rf"\b{re.escape(incomplete_buyer)}\b", " ", scrubbed,
                    flags=re.IGNORECASE,
                )
                scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 11. Phrase detection BEFORE token scrub (objeto_phrases) ────
        #    Preserva colocaciones de dominio ("manual de contratación") como
        #    unidad antes de que "de" se descarte y la frase se pierda. Consume
        #    el span para que sus tokens NO recreen el objeto roto (no duplicar).
        # R4-S2 resolves every safe OBJECT request into the one existing runtime
        # authority. NegationRequest remains ephemeral and no governed token can
        # re-enter the positive topic extractors below.
        object_excluded: list[str | list[str]] = []
        for request in negation_binding.requests:
            if (
                request.dimension is NegationDimension.OBJECT
                and request.verdict is NegationVerdict.EXECUTE
            ):
                object_excluded.extend(self._extract_object_terms(request.value))
        if object_excluded:
            seen_excluded: set[str] = set()
            unique_excluded: list[str | list[str]] = []
            for value in object_excluded:
                key = "|".join(value) if isinstance(value, list) else value
                if key not in seen_excluded:
                    seen_excluded.add(key)
                    unique_excluded.append(value)
            params["objeto_exclude"] = unique_excluded

        # Un turno que es ÍNTEGRAMENTE una orden de navegación o de reinicio no
        # tiene tema contractual. Reconocerlo aquí impide que sus palabras
        # lleguen al extractor temático y viajen como `LIKE '%pagina%'` o
        # `LIKE '%filtros%'`. Qué hacer con la orden sigue siendo autoridad de
        # la capa conversacional; esto solo evita que sea contenido.
        from app.core.followup_control_tokens import is_pure_navigation_turn
        if is_pure_navigation_turn(normalized):
            scrubbed = ""

        from app.core.phrase_catalog import detect_phrases
        source_owned_text = mask_source_requests(scrubbed)
        objeto_phrases, objeto_text = detect_phrases(source_owned_text)
        if objeto_phrases:
            params["objeto_phrases"] = objeto_phrases
        # Lossless twin of the extractor below: ``objeto`` may come back empty
        # for "contratos de gas" (short token) and must not be read as "no
        # subject" by any consumer.
        occurrence_material = material_evidence(objeto_text)
        residual_material = tuple(dict.fromkeys((
            *objeto_phrases,
            *(occurrence.surface.lower() for occurrence in occurrence_material.occurrences),
        )))
        if buyer_ownership is not None:
            occurrence_material = replace(occurrence_material, ownership=buyer_ownership)
        # A governed literal remains evidence even if the legacy object
        # extractor projects only some of its words.
        content_surfaces = (
            (
                buyer_ownership.snapshot[start:end]
                for start, end in buyer_ownership.content_spans
            )
            if buyer_ownership is not None
            else (
                occurrence.surface for occurrence in explicit_content_occurrences(normalized)
            )
        )
        residual_material = tuple(dict.fromkeys((
            *residual_material,
            *content_surfaces,
        )))

        # ── 12. Object terms (whatever remains after all extractions) ───
        object_terms = self._extract_object_terms(objeto_text)
        if object_terms:
            params["objeto"] = object_terms

        if params.get("entidad_resolved") or params.get("entidad_like"):
            params.pop("buyer_group", None)

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

        if params.get("money_clarification"):
            needs_llm = False
            route_reason = "heuristic_only"

        negation_policy = apply_negation_policy(negation_binding.requests, params)
        params = negation_policy.params
        # INV-STATE-FAMILY-PROJECTS-COMPLETE: una familia declarada por
        # cualquier ruta (detector de estado o intent) se materializa entera
        # antes de salir del parser. Rellena; nunca pisa un estado ya acotado.
        params = apply_request(project_estado_family(params), universe_request)
        negation_requests = attach_scope_anchors(
            negation_policy.requests, params,
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
            text_lineage=text_lineage,
            semantic_evidence=SemanticTurnEvidence.from_parse(
                user_query, analytic_controls, residual_material, occurrence_material,
            ),
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
        # Una acción de compra en PASADO describe una compra ya ejecutada, y una
        # compra ejecutada es un contrato: "qué compró la alcaldía en 2024" no
        # pregunta por procesos en curso. No pisa una mención explícita del otro
        # dataset, que sigue mandando.
        if PAST_PROCUREMENT_ACTION_RE.search(normalized_query) and not re.search(
            r"\bprocesos?\b|\blicitaci(?:on|ones)\b|\bconvocatorias?\b",
            normalized_query,
        ):
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

    @staticmethod
    def _temporal_params(resolution: TemporalResolution) -> dict[str, object]:
        params: dict[str, object] = {}
        if resolution.fecha_desde:
            params["fecha_desde"] = resolution.fecha_desde
        if resolution.fecha_hasta:
            params["fecha_hasta"] = resolution.fecha_hasta
        if resolution.matched and not resolution.should_clarify:
            # Señal efímera para que el follow-up reemplace ambos extremos de
            # forma atómica, incluso cuando el nuevo scope es abierto.
            params["_temporal_replace_scope"] = True
        if resolution.should_clarify:
            params["_temporal_error"] = resolution.error_code
            params["_temporal_clarification"] = resolution.clarification
        return params

    def _extract_dates(
        self,
        normalized_query: str,
        *,
        today: Date | None = None,
    ) -> dict[str, object]:
        """Compatibilidad: proyecta la autoridad temporal de dialogue_policy."""
        resolution = resolve_temporal_scope(
            normalized_query,
            today=today,
            protected_spans=tuple(_protected_entity_year_spans(normalized_query)),
        )
        return self._temporal_params(resolution)

    def _extract_amounts(
        self,
        normalized_query: str,
        date_params: dict[str, object],
        *,
        reference_year: int | None = None,
    ) -> tuple[dict[str, object], list[str]]:
        """Extrae montos monetarios con polaridad (min/max/rango).

        Retorna (params, consumed_spans) donde consumed_spans son los
        spans de texto detectados y consumidos, para scrub posterior.
        """
        params: dict[str, object] = {}
        consumed_spans: list[str] = []
        smmlv_requested = is_smmlv_expression(normalized_query)
        smmlv_year: int | None = None
        smmlv_year_assumed = False
        smmlv_sources: dict[int, tuple[str, str]] = {}

        if smmlv_requested:
            desde = date_params.get("fecha_desde")
            hasta = date_params.get("fecha_hasta")
            if desde or hasta:
                if (
                    not isinstance(desde, str)
                    or not isinstance(hasta, str)
                    or desde[:4] != hasta[:4]
                ):
                    years = sorted({
                        value[:4]
                        for value in (desde, hasta)
                        if isinstance(value, str)
                    })
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
                smmlv_year = reference_year or current_colombian_year()
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
        _compound_unit_re = re.compile(
            rf"{MONETARY_SCALE_UNIT_PATTERN}|{SMMLV_UNIT_PATTERN}",
            re.IGNORECASE,
        )
        _atomic_unit_re = re.compile(
            rf"{MONETARY_ATOMIC_SCALE_UNIT_PATTERN}|{SMMLV_UNIT_PATTERN}",
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

        for decision in resolve_money_polarity(normalized_query):
            operands = list(decision.amount_spans)
            if len(operands) == 2:
                lo_text, hi_text = operands
                lo_has_unit = _has_monetary_unit(lo_text)
                hi_has_unit = _has_monetary_unit(hi_text)
                if not lo_has_unit and hi_has_unit and (unit := _inherit_unit(hi_text)):
                    lo_text += " " + unit
                if not hi_has_unit and lo_has_unit and (unit := _inherit_unit(lo_text)):
                    hi_text += " " + unit
                operands = [lo_text, hi_text]
            values = [_to_cop(operand) for operand in operands]
            explicit = any(has_explicit_monetary_evidence(operand) for operand in operands)
            if decision.relation is MoneyRelation.RANGE:
                low, high = values
                verdict = admit_range(low, operands[0], high, operands[1])
                if verdict is RangeVerdict.ABSENT and not explicit:
                    continue
                consumed_spans.append(decision.source_span)
                if verdict is RangeVerdict.EXECUTE:
                    params["valor_min"], params["valor_max"] = (
                        (low, high) if decision.ordered else (min(low, high), max(low, high))
                    )
                else:
                    params["money_clarification"] = RANGE_PARTIAL_CLARIFICATION.format(
                        span=decision.source_span,
                    )
                continue
            admissible = any(is_admissible_amount(value, operand)
                             for value, operand in zip(values, operands))
            if not admissible and not explicit:
                continue
            consumed_spans.append(decision.source_span)
            if decision.relation is MoneyRelation.UNKNOWN or not all(
                is_admissible_amount(value, operand) for value, operand in zip(values, operands)
            ):
                params["money_clarification"] = (
                    f'No pude determinar el filtro monetario de "{decision.source_span}". '
                    '¿Quieres un mínimo, un máximo, un rango o un valor aproximado? '
                    'Por ejemplo: "más de 200 millones" o "entre 100 y 200 millones".'
                )
            elif decision.relation is MoneyRelation.LOWER_BOUND:
                params["valor_min"] = values[0]
            elif decision.relation is MoneyRelation.UPPER_BOUND:
                params["valor_max"] = values[0]
            elif decision.relation is MoneyRelation.APPROXIMATE:
                # Preserve the authorized legacy approximation exactly.
                if "valor_min" not in params and "valor_max" not in params:
                    params["valor_min"] = int(values[0] * 0.8)
                    params["valor_max"] = int(values[0] * 1.2)

        # No partial executable constraint survives an unresolved operand.
        if params.get("money_clarification"):
            params.pop("valor_min", None)
            params.pop("valor_max", None)

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

    def _claimed_by_declared_group(self, mention: str) -> bool:
        """Does a declared buyer group (ICBF, Bogotá…) own this mention? It then keeps the group path."""
        from app.core.buyer_groups import BuyerGroupError, consume_group_mention
        try:
            scope, _ = consume_group_mention(mention, self.entity_resolver)
        except BuyerGroupError:
            return True
        return bool(scope.get("buyer_group"))

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
        self,
        normalized: str,
        scrubbed: str,
        params: dict,
        *,
        lineage: TextLineageDraft | None = None,
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
            return self._scrub_identifier_spans(scrubbed, lineage=lineage)

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
        if asserted_actor is None:
            # ACTOR-ASSERTED-NAME-DOCUMENT-LEAK-001. The role marker is one
            # surface of the speech act, not the act itself: "busca NOMBRE —
            # CC X" says exactly what "busca proveedor NOMBRE — CC X" says.
            # Without this door the name survived as contract object.
            asserted_actor = detect_bare_asserted_actor_name(
                normalized, match.start(), claims_buyer=self._span_claims_buyer,
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

        scrubbed = self._scrub_identifier_spans(scrubbed, lineage=lineage)
        # Consume the atomic role/name span before its shorter role marker;
        # otherwise removing "proveedor" first would make the full span
        # impossible to match and leave the asserted name behind as a topic.
        if asserted_actor is not None:
            for span in asserted_actor.consumed_spans:
                scrubbed = replace_regex(
                    scrubbed,
                    re.escape(span),
                    " ",
                    lineage=lineage,
                    stage="actor_asserted_name",
                    operation="MASK",
                    proof=Proof.EXACT,
                )
        for span in reference.consumed_spans:
            scrubbed = replace_regex(
                scrubbed,
                rf"\b{re.escape(span)}\b",
                " ",
                flags=re.IGNORECASE,
                lineage=lineage,
                stage="actor_document_context",
                operation="MASK",
                proof=Proof.EXACT,
            )
        return scrubbed

    def _span_claims_buyer(self, span: str) -> bool:
        """Whether the buyer authority already recognizes this span.

        Asked by the asserted-name door before it consumes a span beside a
        typed document. Without it, ``busca LA PREVISORA S.A. — NIT X`` lost a
        buyer this parser resolves on its own: a recognized dimension deleted
        in silence, which is exactly what the actor fixes exist to prevent.

        A resemblance counts as a claim here, deliberately. Excluding fuzzy
        matches would have been the sharper rule — ``universidad solano``
        resolving fuzzy(85) to UNIVERSIDAD NACIONAL DE COLOMBIA is a poor
        answer — but this door is not the place to overturn it: the buyer
        authority already answers that way on its own, and silently deleting
        what it recognized is the failure mode being remediated. Whether a
        fuzzy buyer should accredit anything at all is its own question, for
        the buyer authority to settle.

        The cost is bounded: only a span the rewrite grammar or the gazetteer
        recognizes can be vetoed, and a personal name produces neither.

        The acceptance rule mirrors step 4.6 exactly, ``fallback_like``
        excluded, so the door asks the buyer authority the same question that
        authority asks itself. No span in the corpus actually reaches that
        exclusion — every rewrite candidate here either matches or returns
        nothing — so it is stated for agreement with 4.6, not because a test
        exercises it.
        """
        from app.core.entity_types import rewrite_entity

        if not self.entity_resolver or not span.strip():
            return False
        if self.entity_resolver.scan_entity(span) is not None:
            return True
        return any(
            (match := self.entity_resolver.resolve_entidad(candidate.canonical))
            and match.value
            and match.method != "fallback_like"
            for candidate in rewrite_entity(span)
        )

    @staticmethod
    def _scrub_identifier_spans(
        scrubbed: str,
        *,
        lineage: TextLineageDraft | None = None,
    ) -> str:
        """Quita del texto los spans aceptados como identificador.

        Solo esos: una consulta puede traer NIT y monto a la vez, y el monto
        debe seguir visible para el paso 9.
        """
        identifier_spans = [
            candidate.span()
            for candidate in _NIT_SHAPE_RE.finditer(scrubbed)
            if _number_role(scrubbed, candidate) == "identifier"
        ]
        for span in reversed(identifier_spans):
            scrubbed = replace_text_span(
                scrubbed,
                span,
                " ",
                lineage=lineage,
                stage="actor_document",
                operation="MASK",
                proof=Proof.EXACT,
            )
        return replace_regex(
            scrubbed,
            r"\bnit\b",
            " ",
            flags=re.IGNORECASE,
            lineage=lineage,
            stage="actor_document_marker",
            operation="MASK",
            proof=Proof.EXACT,
        )

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
        buyer_spans = buyer_contract_request_spans(
            normalized, params, recognized_modifier_tails,
        )
        if buyer_spans:
            if not params.get("dataset_explicit"):
                params["dataset"] = "contratos"
            for span in buyer_spans:
                scrubbed = scrubbed.replace(span, " ", 1)
            return scrubbed

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
    def _find_identifier_spans(normalized_query: str) -> tuple[re.Match[str], ...]:
        """All numeric spans accredited as identifiers in source coordinates."""
        return tuple(
            candidate
            for candidate in _NIT_SHAPE_RE.finditer(normalized_query)
            if _number_role(normalized_query, candidate) == "identifier"
        )

    @staticmethod
    def _find_identifier_span(normalized_query: str) -> "re.Match[str] | None":
        """Primer span con forma de identificador que ADR-059 no lee como monto.

        Extraído de ``_extract_nit`` para que la ruta de actor obtenga el
        crudo VERBATIM —con sus puntos y su guion— sin repetir el arbitraje
        número-vs-monto ni desincronizarse de él. AP1 necesita el crudo,
        no los dígitos ya limpios: es el separador lo que distingue un DV
        declarado por la fuente de una hipótesis del módulo 11.
        """
        return next(iter(QueryRouter._find_identifier_spans(normalized_query)), None)

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
                elif (token not in STOPWORDS or is_source_word(token)) and (
                    token not in DEPARTAMENTOS and
                    len(token) >= 4 or token in VARIANT_TO_ROOT
                ):
                    tokens.append(VARIANT_TO_ROOT.get(token, token))
                i += 1

        # ── PROCUREMENT-ACTION-001: scrub verbos contractuales ──
        # Si hay tokens sustantivos (no verbos), eliminar los verbos de acción.
        # Los verbos solos (sin sustantivos) se mantienen para no romper queries
        # como "oportunidades de contratacion".
        from app.core.intent_vocabulary import (
            PROCUREMENT_ACTION_NOUNS,
            PROCUREMENT_ACTION_VERB_FORMS,
        )
        # Un verbo conjugado no es un objeto contractual en ninguna lectura del
        # español: es el predicado cuyo sujeto es el comprador. Se descarta
        # siempre, haya o no sustantivos alrededor — "qué celebró la alcaldía en
        # 2025" consultaba LIKE '%celebro%' solo porque no quedaba ningún otro
        # token que activara la regla condicional.
        tokens = [t for t in tokens if t not in PROCUREMENT_ACTION_VERB_FORMS]
        # Las nominalizaciones SÍ pueden ser el tema ("oportunidades de
        # contratación"), así que solo ceden ante sustantivos reales.
        non_verb_tokens = [
            t for t in tokens if t != "o" and t not in PROCUREMENT_ACTION_NOUNS
        ]
        if non_verb_tokens:
            tokens = [t for t in tokens if t not in PROCUREMENT_ACTION_NOUNS]

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
