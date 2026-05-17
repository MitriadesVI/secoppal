from __future__ import annotations

import calendar
import re
from pathlib import Path
from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.data.departamentos import DEPARTAMENTOS
from app.data.estados import (
    ESTADO_APERTURA_SYNONYMS,
    ESTADO_CONTRATO_SYNONYMS,
    ESTADO_PROCEDIMIENTO_SYNONYMS,
    ESTADO_SYNONYMS,
)
from app.utils.money import money_to_cop, normalize_text
from app.utils.spell_correction import correct_query

# Import bigram registry from morphological variants dict (created in 1.2.c).
# If the module doesn't exist yet, KNOWN_BIGRAMS is empty — no bigram detection.
try:
    from app.data.morphological_variants import KNOWN_BIGRAMS, VARIANT_TO_ROOT
except ImportError:
    KNOWN_BIGRAMS: frozenset[tuple[str, ...]] = frozenset()
    VARIANT_TO_ROOT: dict[str, str] = {}

if TYPE_CHECKING:
    from app.core.entity_resolver import EntityResolver

DATASET_PROCESOS = "procesos"
DATASET_CONTRATOS = "contratos"

# Values are EXACT strings from SECOP — verified via discover_secop.py
MODALIDAD_KEYWORDS = {
    "minima cuantia": "Mínima cuantía",
    "contratacion directa": "Contratación directa",
    "licitacion publica": "Licitación pública",
    "concurso de meritos": "Concurso de méritos abierto",
    "seleccion abreviada": "Selección Abreviada de Menor Cuantía",
    "menor cuantia": "Selección Abreviada de Menor Cuantía",
    "subasta inversa": "Selección abreviada subasta inversa",
    "regimen especial": "Contratación régimen especial",
}


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
    r"""
    (?<!\d)                       # no precedido de dígito
    (\d{1,3}                      # bloque inicial 1-3 dígitos
    (?:[.\- ]\d{3}){1,3}          # uno a tres grupos de 3 dígitos con separador
    (?:[.\- ]\d)?                 # DV opcional con separador
    |\d{8,11})                    # o secuencia continua de 8-11 dígitos
    (?!\d)                        # no seguido de dígito
    (?!\s*(?:millones?|millon|mil\s+millones|billones?|palos?|pesos))  # no es monto
    """,
    re.IGNORECASE | re.VERBOSE,
)

STOPWORDS = {
    # Conversational verbs
    "muestrame", "muestreme", "muestra", "mostrar", "mostra",
    "busca", "buscar", "buscame", "buscando", "busco",
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
    # States (extracted separately)
    "abierta", "abiertas", "abierto", "abiertos",
    "adjudicado", "adjudicados",
    "celebrado", "celebrados",
    "firmado", "firmados",
    "liquidado", "liquidados",
    "cerrado", "cerrados",
    # Search types
    "contrato", "contratos", "convocatoria", "convocatorias",
    "licitacion", "licitaciones", "concurso",
    "proceso", "procesos",
    "publica", "publico",
    # Articles & prepositions
    "de", "del", "en", "el", "la", "las", "los",
    "por", "para", "que", "se", "un", "una", "con",
    # Comparators
    "mayor", "mayores", "menor", "menores",
    "superior", "superiores", "superen",
    "mas", "menos",
    # Money units
    "pesos", "millones", "millon",
    # Filler
    "tipo", "tema", "temas", "asunto", "asuntos", "materia", "sector",
    "area", "área", "linea", "línea", "relacionado", "relacionados",
    "vigente", "vigentes", "sobre", "todos", "todas", "todo", "toda",
    # Temporal adverbs (not contractual objects)
    "actualmente", "ahora", "hoy", "recientemente", "momento",
    # Indefinite pronouns
    "algun", "alguna", "algunos", "algunas", "ningun", "ninguna",
    "otro", "otra", "otros", "otras",
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
    "pasado", "anterior", "siguiente", "proximo",
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
        scrubbed = normalized

        # ── 0. Semantic normalization layer (runs BEFORE existing extractors) ──
        #    This prevents tokens like "publicados", "departamento", "alcaldía"
        #    from leaking into 'objeto' and poisoning LIKE clauses.
        consumed_spans: list[str] = []  # track consumed text for debugging

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
                scrubbed = scrubbed.replace(intent_span, " ", 1)

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

        # ── 1.5. NIT detection (shape-based, before year/amount stripping) ──
        # Detecta NITs colombianos por forma antes de cualquier otra extracción
        # para que no sean confundidos con años ni montos.
        nit_variants = self._extract_nit(normalized)
        if nit_variants:
            params["contratista"] = nit_variants
            # Scrub: remover el NIT del texto para que no contamine el objeto
            # Eliminar cualquier secuencia que luzca como NIT (con puntos, guiones, espacios)
            scrubbed = _NIT_SHAPE_RE.sub(" ", scrubbed)
            # También remover la palabra "nit" si quedó suelta
            scrubbed = re.sub(r"\bnit\b", " ", scrubbed, flags=re.IGNORECASE)

        # ── 2. Dates (regex — reliable for numbers) ─────────────────────
        date_params = self._extract_dates(normalized)
        params.update(date_params)

        # ── 3. Strip year tokens so they don't contaminate scans ────────
        # Lookahead negativo: no capturar años seguidos de unidades de dinero
        # (ej: "2000 millones" no es el año 2000)
        _year_re = re.compile(
            r"\b(20\d{2})\b(?!\s*(?:millones?|millon|mil\s+millones|billones?|palos?|pesos))",
            re.IGNORECASE,
        )
        for m in re.finditer(_year_re, scrubbed):
            scrubbed = re.sub(rf"\b{re.escape(m.group(0))}\b", " ", scrubbed)
        # Also extract bare years as dates
        protected_year_spans = _protected_entity_year_spans(normalized)
        for m in re.finditer(_year_re, normalized):
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

        # ── 4.6. ENTITY REWRITE: try canonical variants for entity patterns ──
        #    "alcaldía de X" → "Municipio de X" / "Alcaldía de X"
        #    Runs BEFORE gazetteer scan so variants feed into alias matching.
        from app.core.entity_types import rewrite_entity
        entity_candidates = rewrite_entity(scrubbed)
        rewritten_entity = None
        if entity_candidates and self.entity_resolver:
            for canonical, consumed, rtype in entity_candidates:
                match = self.entity_resolver.resolve_entidad(canonical)
                if match and match.method != "fallback_like":
                    rewritten_entity = (canonical, consumed, rtype, match)
                    break
            if rewritten_entity:
                canonical, consumed, rtype, match = rewritten_entity
                params["entidad"] = canonical
                params["entidad_resolved"] = match.value
                params["entidad_resolution"] = {
                    "value": match.value,
                    "method": f"rewrite_{rtype}",
                    "confidence": "high",
                    "like_value": None,
                    "metadata": {"rewritten_from": consumed},
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
                    "confidence": "high",
                    "like_value": None,
                    "metadata": {},
                }
                scrubbed = entity_scan.remaining_text

        # ── 6. GAZETTEER SCAN: Department (on remaining text) ───────────
        #    Runs AFTER entity scan so "de santander" can't be stolen
        if self.entity_resolver:
            dept_scan = self.entity_resolver.scan_departamento(scrubbed)
            if dept_scan:
                params["departamento"] = dept_scan.matched_alias
                params["departamento_resolved"] = dept_scan.official_name
                params["departamento_resolution"] = {
                    "value": dept_scan.official_name,
                    "method": dept_scan.method,
                    "confidence": "high",
                    "like_value": None,
                    "metadata": {},
                }
                scrubbed = dept_scan.remaining_text

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
            scrubbed = re.sub(r"\b" + re.escape(span) + r"\b", " ", scrubbed, flags=re.IGNORECASE)
        else:
            # ── 7b. OLD State extractor (dataset-aware keyword match) ────────
            state_result = self._extract_state(normalized, str(params["dataset"]))
            if state_result:
                params["estado"] = state_result["value"]
                if state_result.get("field"):
                    params["estado_field"] = state_result["field"]

        # ── 8. Modality ─────────────────────────────────────────────────
        modality = self._extract_modality(normalized)
        if modality:
            params["modalidad"] = modality

        # ── 9. Amounts ──────────────────────────────────────────────────
        amount_params = self._extract_amounts(normalized, date_params)
        params.update(amount_params)

        # ── 10. Contractor ──────────────────────────────────────────────
        # Solo corre si el NIT no fue ya detectado por forma en paso 1.5
        if "contratista" not in params:
            contractor = self._extract_contractor(normalized, str(params["dataset"]))
            if contractor:
                params["contratista"] = contractor
                scrubbed = scrubbed.replace(contractor, " ")

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

        # ── 11. Object terms (whatever remains after all extractions) ───
        object_terms = self._extract_object_terms(scrubbed)
        if object_terms:
            params["objeto"] = object_terms

        # ── 12. LLM decision ───────────────────────────────────────────
        extracted_keys = {k for k, v in params.items() if v not in (None, [], "", {})}
        needs_llm = self._needs_llm(normalized, params)
        route_reason = "heuristic_only" if not needs_llm else "heuristic_plus_llm"

        if extracted_keys == {"dataset"}:
            needs_llm = True
            route_reason = "insufficient_signals"

        if extracted_keys - {"dataset"} == {"ordering_signal"}:
            needs_llm = True
            route_reason = "ordering_without_object"

        return ParsedQuery(params=params, needs_llm=needs_llm, route_reason=route_reason,
                          dataset_explicit=dataset_explicit,
                          consumed_spans=consumed_spans)

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
                    return {"value": official, "field": "estado_contrato"}
        else:
            for alias, official in sorted(ESTADO_APERTURA_SYNONYMS.items(), key=lambda x: len(x[0]), reverse=True):
                if re.search(rf"\b{re.escape(alias)}\b", normalized_query):
                    return {"value": official, "field": "estado_de_apertura_del_proceso"}
            for alias, official in sorted(ESTADO_PROCEDIMIENTO_SYNONYMS.items(), key=lambda x: len(x[0]), reverse=True):
                if re.search(rf"\b{re.escape(alias)}\b", normalized_query):
                    return {"value": official, "field": "estado_del_procedimiento"}
        return None

    def _extract_modality(self, normalized_query: str) -> str | None:
        for alias, official in sorted(MODALIDAD_KEYWORDS.items(), key=lambda x: len(x[0]), reverse=True):
            if alias in normalized_query:
                return official
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

        return params

    def _extract_amounts(self, normalized_query: str, date_params: dict[str, str]) -> dict[str, int]:
        params: dict[str, int] = {}
        year_tokens: set[str] = set()
        for val in date_params.values():
            year_tokens.add(val[:4])
        _year_money_re = re.compile(
            r"\b(20\d{2})\b(?!\s*(?:millones?|millon|mil\s+millones|billones?|palos?|pesos))",
            re.IGNORECASE,
        )
        for m in re.finditer(_year_money_re, normalized_query):
            year_tokens.add(m.group(1))

        # Detect "entre X y Y" range BEFORE individual amount parsing so the
        # two numbers don't each get mis-classified by the generic loop below.
        _range_re = re.compile(
            r"entre\s+((?:\d+(?:[.,]\d+)?|un|mil)\s*(?:billon(?:es)?|mil\s+millones|millones?|palos?|mil)?(?:\s+de\s+pesos)?)\s+y\s+((?:\d+(?:[.,]\d+)?|un|mil)\s*(?:billon(?:es)?|mil\s+millones|millones?|palos?|mil)?(?:\s+de\s+pesos)?)",
            re.IGNORECASE,
        )
        range_match = _range_re.search(normalized_query)
        if range_match:
            full_range_text = range_match.group(0)
            lo_text = range_match.group(1)
            hi_text = range_match.group(2)
            amount_lo = money_to_cop(lo_text)
            amount_hi = money_to_cop(hi_text)
            # If one side lacks a unit, inherit the unit from the full range text.
            # e.g. "entre 100 y 500 millones" — lo_text="100", hi_text="500 millones"
            if amount_lo is None or amount_lo < 1_000_000:
                # Try to infer multiplier from hi side or full text
                _mult_re = re.compile(r"billon(?:es)?|mil\s+millones|millones?|palos?", re.IGNORECASE)
                mult_match = _mult_re.search(hi_text) or _mult_re.search(full_range_text)
                if mult_match:
                    amount_lo = money_to_cop(lo_text + " " + mult_match.group(0))
            if amount_hi is None or amount_hi < 1_000_000:
                _mult_re = re.compile(r"billon(?:es)?|mil\s+millones|millones?|palos?", re.IGNORECASE)
                mult_match = _mult_re.search(lo_text) or _mult_re.search(full_range_text)
                if mult_match:
                    amount_hi = money_to_cop(hi_text + " " + mult_match.group(0))
            if amount_lo and amount_hi and amount_lo >= 1_000_000 and amount_hi >= 1_000_000:
                params["valor_min"] = min(amount_lo, amount_hi)
                params["valor_max"] = max(amount_lo, amount_hi)
                # Remove matched span so generic loop below doesn't re-parse it
                normalized_query = normalized_query[: range_match.start()] + " " + normalized_query[range_match.end() :]

        matches = list(re.finditer(
            r"(?:(?:mas|mayor(?:es)?|superior(?:es)?|superen?)\s+(?:de|a)|(?:menos|menor(?:es)?)\s+(?:de|a)|hasta|por|de)?\s*((?:\d+(?:[.,]\d+)?)|mil|un)\s*(?:billon(?:es)?|mil millones|millones?|palos?|mil)?(?:\s+de\s+pesos)?",
            normalized_query,
        ))

        for match in matches:
            raw = match.group(0).strip()
            number_part = match.group(1)
            if number_part in year_tokens:
                continue
            amount = money_to_cop(raw)
            if amount is None or amount < 1_000_000:
                continue

            if re.search(r"(?:mas|mayor(?:es)?|superior(?:es)?|superen?)\s+(?:de|a)", raw):
                params["valor_min"] = amount
            elif re.search(r"(?:menos|menor(?:es)?)\s+(?:de|a)|hasta", raw):
                params["valor_max"] = amount
            elif re.search(r"\bpor\b", raw):
                params.setdefault("valor_min", int(amount * 0.8))
                params.setdefault("valor_max", int(amount * 1.2))
            else:
                params.setdefault("valor_max", amount)

        return params

    def _extract_nit(self, normalized_query: str) -> list[str] | None:
        """Detecta NITs colombianos por forma y retorna variantes base + con DV.

        Retorna lista con nit_base y nit_completo (si hay DV) para que el
        SoQL builder pueda hacer OR sobre documento_proveedor.
        Retorna None si no hay NIT en la query.
        """
        match = _NIT_SHAPE_RE.search(normalized_query)
        if not match:
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
        for alias in MODALIDAD_KEYWORDS:
            cleaned = cleaned.replace(alias, " ")
        cleaned = re.sub(r"\b(?:\d+(?:[.,]\d+)?)\b", " ", cleaned)
        cleaned = re.sub(r"\b(?:millon(?:es)?|mil|billon(?:es)?|palos?|pesos)\b", " ", cleaned)
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
