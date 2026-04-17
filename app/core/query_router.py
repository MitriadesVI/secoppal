from __future__ import annotations

import calendar
import re
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

_ORDERING_SIGNAL_RE = re.compile(
    r"\bmas\s+(?:caro|caros|cara|caras"
    r"|costoso|costosos|costosa|costosas"
    r"|grande|grandes|importante|importantes"
    r"|reciente|recientes|nuevo|nuevos|nueva|nuevas"
    r"|alto|altos|alta|altas"
    r"|mejor|mejores)\b"
    r"|\bmenos\s+(?:barato|baratos|barata|baratas"
    r"|bajo|bajos|baja|bajas"
    r"|peor|peores)\b",
    re.IGNORECASE,
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
    "hay", "tiene", "tienen", "esta", "estan",
    "donde", "como", "cuando",
    # Courtesy
    "hola", "gracias", "favor", "podrias", "puedes", "puede", "porfa",
    # States (extracted separately)
    "abierta", "abiertas", "abierto", "abiertos",
    "adjudicado", "adjudicados",
    "celebrado", "celebrados",
    "firmado", "firmados",
    "liquidado", "liquidados",
    "cerrado", "cerrados",
    # Search types
    "contrato", "contratos", "convocatoria", "convocatorias",
    "licitacion", "licitaciones",
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
    "tipo", "vigente", "vigentes", "sobre", "todos", "todas", "todo", "toda",
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
    # Months (not contractual objects)
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
    # More temporal
    "semestre", "trimestre", "bimestre", "periodo", "anual", "mensual",
    "pasado", "anterior", "siguiente", "proximo",
}

MONTH_MAP = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}

# ── LLM trigger heuristics ──────────────────────────────────────────────────
_ENTITY_SIGNAL_WORDS = {
    "secretaria", "ministerio", "instituto", "corporacion", "fundacion",
    "agencia", "unidad", "autoridad", "comision",
    "superintendencia", "direccion", "servicio", "empresa",
}

_PREPOSITION_PLACE_RE = re.compile(
    r"\ben\s+([a-z]{4,}(?:\s+[a-z]{4,})*)",
    re.IGNORECASE,
)


@dataclass(slots=True)
class ParsedQuery:
    params: dict
    needs_llm: bool
    route_reason: str


class QueryRouter:
    """
    Deterministic parser for SECOP queries.

    Uses GAZETTEER-FIRST entity detection: scans text for known entity/department
    aliases before any regex extraction. This eliminates the ambiguity problem
    where "gobernación de santander" gets split between entity and department.
    """

    def __init__(self, entity_resolver: EntityResolver | None = None):
        self.entity_resolver = entity_resolver

    def parse(self, user_query: str) -> ParsedQuery:
        normalized = normalize_text(user_query)
        normalized = correct_query(normalized)
        params: dict[str, object] = {}
        scrubbed = normalized

        # ── 1. Dataset selection (keyword-based) ────────────────────────
        params["dataset"] = self._select_dataset(normalized)

        # ── 2. Dates (regex — reliable for numbers) ─────────────────────
        date_params = self._extract_dates(normalized)
        params.update(date_params)

        # ── 3. Strip year tokens so they don't contaminate scans ────────
        for m in re.finditer(r"\b(20\d{2})\b", scrubbed):
            scrubbed = re.sub(rf"\b{m.group(0)}\b", " ", scrubbed)
        # Also extract bare years as dates
        for m in re.finditer(r"\b(20\d{2})\b", normalized):
            year = m.group(1)
            params.setdefault("fecha_desde", f"{year}-01-01")
            params.setdefault("fecha_hasta", f"{year}-12-31")
        scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 4. Ordering signals ("más caros", etc.) ─────────────────────
        if _ORDERING_SIGNAL_RE.search(scrubbed):
            params["ordering_signal"] = "valor_desc"
            scrubbed = _ORDERING_SIGNAL_RE.sub(" ", scrubbed)
            scrubbed = re.sub(r"\s+", " ", scrubbed).strip()

        # ── 5. GAZETTEER SCAN: Entity (longest alias match) ─────────────
        #    This is the core change: detect entities by substring match
        #    against 10K+ aliases, NOT by regex pattern matching.
        if self.entity_resolver:
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

        # ── 7. State (dataset-aware keyword match) ──────────────────────
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
        contractor = self._extract_contractor(normalized, str(params["dataset"]))
        if contractor:
            params["contratista"] = contractor
            scrubbed = scrubbed.replace(contractor, " ")

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

        return ParsedQuery(params=params, needs_llm=needs_llm, route_reason=route_reason)

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
        for m in re.finditer(r"\b(20\d{2})\b", normalized_query):
            year_tokens.add(m.group(1))

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

    def _extract_contractor(self, normalized_query: str, dataset: str) -> str | None:
        if dataset != DATASET_CONTRATOS:
            return None
        pattern = re.compile(
            r"(?:contratista|proveedor)(?:\s+adjudicado)?\s+(.+?)(?=\s+(?:en|por|desde|hasta|mayor|menor|mas|menos)\b|$)"
        )
        match = pattern.search(normalized_query)
        return match.group(1).strip(" ,.") if match else None

    def _extract_object_terms(self, scrubbed_query: str) -> list[str]:
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

        tokens: list[str] = []
        for token in cleaned.split():
            if token in STOPWORDS or len(token) < 4:
                continue
            if token in DEPARTAMENTOS:
                continue
            tokens.append(token)

        seen: set[str] = set()
        unique: list[str] = []
        for t in tokens:
            if t not in seen:
                unique.append(t)
                seen.add(t)
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

        # Signal 1: "en [place]" not resolved as department
        if not params.get("departamento_resolved"):
            match = _PREPOSITION_PLACE_RE.search(normalized_query)
            if match:
                place_tokens = set(match.group(1).split())
                objeto_set = set(objeto)
                if place_tokens & objeto_set:
                    return True

        # Signal 2: Entity-like words in objeto
        if set(objeto) & _ENTITY_SIGNAL_WORDS:
            return True

        return False
