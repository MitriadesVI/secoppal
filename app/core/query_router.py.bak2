from __future__ import annotations

import re
from dataclasses import dataclass

from app.data.departamentos import DEPARTAMENTOS
from app.data.estados import (
    ESTADO_APERTURA_SYNONYMS,
    ESTADO_CONTRATO_SYNONYMS,
    ESTADO_PROCEDIMIENTO_SYNONYMS,
    ESTADO_SYNONYMS,
)
from app.utils.money import money_to_cop, normalize_text

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

ENTITY_HINTS = (
    "gobernacion",
    "alcaldia",
    "sena",
    "icbf",
    "ministerio",
    "universidad",
    "hospital",
    "instituto",
    "agencia",
    "empresa",
    "secretaria",
    "departamento de",
    "municipio de",
    "policia",
    "ejercito",
    "armada",
    "fuerza aerea",
    "contraloria",
    "procuraduria",
    "fiscalia",
    "dane",
    "dian",
    "invias",
    "idu",
    "eaab",
)

# Detecta frases de ordenamiento como "más caros", "más costosos", "más grandes", etc.
# Estas frases son instrucciones de ordenamiento, NO objetos de búsqueda.
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
    # Verbos conversacionales — el usuario habla con el bot, no busca estos términos
    "muestrame", "muestreme", "muestra", "mostrar", "mostra",
    "busca", "buscar", "buscame", "buscando", "busco",
    "dame", "dime", "necesito", "quiero", "quisiera",
    "encuentra", "encontrar", "encontrame", "encontra",
    "lista", "listar", "listame",
    "consulta", "consultar", "consultame",
    "traeme", "traer", "trae",
    "dime", "decime", "digame",
    # Preguntas
    "cuales", "cuantos", "cuantas", "cual",
    "hay", "tiene", "tienen", "esta", "estan",
    "donde", "como", "cuando",
    # Cortesía
    "hola", "gracias", "favor", "podrias", "puedes", "puede", "porfa",
    # Estados (ya se extraen aparte)
    "abierta", "abiertas", "abierto", "abiertos",
    "adjudicado", "adjudicados",
    "celebrado", "celebrados",
    "firmado", "firmados",
    "liquidado", "liquidados",
    "cerrado", "cerrados",
    # Tipo de búsqueda
    "contrato", "contratos", "convocatoria", "convocatorias",
    "licitacion", "licitaciones",
    "proceso", "procesos",
    "publica", "publico",
    # Artículos y preposiciones
    "de", "del", "en", "el", "la", "las", "los",
    "por", "para", "que", "se", "un", "una", "con",
    # Comparadores (ya se extraen en amounts)
    "mayor", "mayores", "menor", "menores",
    "superior", "superiores", "superen",
    "mas", "menos",
    # Unidades monetarias
    "pesos", "millones", "millon",
    # Filler
    "tipo", "vigente", "vigentes", "sobre", "todos", "todas", "todo", "toda",
    # Adjetivos de valor/tamaño — son instrucciones de ordenamiento, no objetos
    "caro", "caros", "cara", "caras",
    "costoso", "costosos", "costosa", "costosas",
    "grande", "grandes", "importante", "importantes",
    "barato", "baratos", "barata", "baratas",
    "reciente", "recientes", "ultimo", "ultimos", "ultima", "ultimas",
    "nuevo", "nuevos", "nueva", "nuevas",
    "mejor", "mejores", "peor", "peores",
    "alto", "altos", "alta", "altas",
    "bajo", "bajos", "baja", "bajas",
}


@dataclass(slots=True)
class ParsedQuery:
    params: dict
    needs_llm: bool
    route_reason: str


class QueryRouter:
    """Cheap deterministic parser for common SECOP queries."""

    def parse(self, user_query: str) -> ParsedQuery:
        normalized = normalize_text(user_query)
        params: dict[str, object] = {}
        scrubbed = normalized

        # 1. Dataset
        params["dataset"] = self._select_dataset(normalized)

        # 2. Dates BEFORE amounts
        date_params = self._extract_dates(normalized)
        params.update(date_params)

        # 2b. Strip year tokens from scrubbed text so they don't contaminate entity
        for m in re.finditer(r"\b20\d{2}\b", scrubbed):
            scrubbed = re.sub(rf"\b{m.group(0)}\b", " ", scrubbed)

        # 3. Department BEFORE entity
        department = self._extract_department(normalized)
        if department:
            params["departamento"] = department
            for alias in sorted(DEPARTAMENTOS, key=len, reverse=True):
                if re.search(rf"\b{re.escape(alias)}\b", scrubbed):
                    scrubbed = re.sub(rf"\b{re.escape(alias)}\b", " ", scrubbed, count=1)
                    break

        # 4. Entity on scrubbed text
        entity = self._extract_entity(scrubbed)
        if entity:
            params["entidad"] = entity
            scrubbed = scrubbed.replace(entity, " ")

        # 5. State — dataset-aware
        state_result = self._extract_state(normalized, str(params["dataset"]))
        if state_result:
            params["estado"] = state_result["value"]
            # Track which state field to use in SoQL
            if state_result.get("field"):
                params["estado_field"] = state_result["field"]

        # 6. Modality
        modality = self._extract_modality(normalized)
        if modality:
            params["modalidad"] = modality

        # 7. Amounts
        amount_params = self._extract_amounts(normalized, date_params)
        params.update(amount_params)

        # 8. Contractor
        contractor = self._extract_contractor(normalized, str(params["dataset"]))
        if contractor:
            params["contratista"] = contractor
            scrubbed = scrubbed.replace(contractor, " ")

        # 8b. Ordering signal ("más caros", "más costosos", etc.)
        if _ORDERING_SIGNAL_RE.search(normalized):
            params["ordering_signal"] = "valor_desc"
            # Strip ordering phrase from scrubbed so it doesn't bleed into object terms
            scrubbed = _ORDERING_SIGNAL_RE.sub(" ", scrubbed)

        # 9. Object terms
        object_terms = self._extract_object_terms(scrubbed)
        if object_terms:
            params["objeto"] = object_terms

        # 10. LLM decision
        extracted_keys = {key for key, value in params.items() if value not in (None, [], "", {})}
        needs_llm = self._needs_llm(normalized, params)
        route_reason = "heuristic_only" if not needs_llm else "heuristic_plus_llm"

        if extracted_keys == {"dataset"}:
            needs_llm = True
            route_reason = "insufficient_signals"

        # Ordering signal alone (no real object) → LLM must interpret intent
        if extracted_keys - {"dataset"} == {"ordering_signal"}:
            needs_llm = True
            route_reason = "ordering_without_object"

        return ParsedQuery(params=params, needs_llm=needs_llm, route_reason=route_reason)

    def _select_dataset(self, normalized_query: str) -> str:
        contract_signals = (
            "contrato", "contratos",
            "firmado", "firmados", "firmada", "firmadas",
            "contratista", "proveedor",
            "historico",
            "liquidado", "liquidados",
            "en ejecucion", "ejecutando",
            "cedido", "cedidos",
            "terminado", "terminados",
        )
        # Special case: "contratos abiertos" → user probably means PROCESOS
        if any(s in normalized_query for s in contract_signals):
            # But if they also say "abierto/abierta", redirect to procesos
            if re.search(r"\babiert[ao]s?\b", normalized_query):
                return DATASET_PROCESOS
            return DATASET_CONTRATOS
        return DATASET_PROCESOS

    def _extract_department(self, normalized_query: str) -> str | None:
        for alias in sorted(DEPARTAMENTOS, key=len, reverse=True):
            pattern = rf"\b{re.escape(alias)}\b"
            if re.search(pattern, normalized_query):
                return self._clean_department_alias(alias)
        return None

    def _extract_entity(self, normalized_query: str) -> str | None:
        pattern = re.compile(
            r"(?:de las|de los|de la|del|de)\s+(.+?)(?=\s+(?:en|por|para|abiert|vigent|cerrad|adjudic|firmad|celebrad|liquidado|terminado|desde|hasta|mayor|menor|mas|menos|\d{4})\b|$)"
        )
        for candidate in pattern.findall(normalized_query):
            candidate = candidate.strip(" ,.")
            candidate = re.sub(r"^(?:la|el|los|las)\s+", "", candidate)
            if any(hint in candidate for hint in ENTITY_HINTS):
                if candidate not in DEPARTAMENTOS:
                    return candidate
        return None

    def _extract_state(self, normalized_query: str, dataset: str) -> dict | None:
        """
        Extract state with awareness of which dataset and which field to use.
        Returns {"value": "...", "field": "..."} or None.
        """
        if dataset == DATASET_CONTRATOS:
            for alias, official in sorted(ESTADO_CONTRATO_SYNONYMS.items(), key=lambda x: len(x[0]), reverse=True):
                if re.search(rf"\b{re.escape(alias)}\b", normalized_query):
                    return {"value": official, "field": "estado_contrato"}
        else:
            # For procesos, check estado_de_apertura first (simpler, more common)
            for alias, official in sorted(ESTADO_APERTURA_SYNONYMS.items(), key=lambda x: len(x[0]), reverse=True):
                if re.search(rf"\b{re.escape(alias)}\b", normalized_query):
                    return {"value": official, "field": "estado_de_apertura_del_proceso"}

            # Then check estado_del_procedimiento for more specific states
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

        matches = list(
            re.finditer(
                r"(?:(?:mas|mayor(?:es)?|superior(?:es)?|superen?)\s+(?:de|a)|(?:menos|menor(?:es)?)\s+(?:de|a)|hasta|por|de)?\s*((?:\d+(?:[.,]\d+)?)|mil|un)\s*(?:billon(?:es)?|mil millones|millones?|palos?|mil)?(?:\s+de\s+pesos)?",
                normalized_query,
            )
        )

        for match in matches:
            raw = match.group(0).strip()
            number_part = match.group(1)
            if number_part in year_tokens:
                continue

            amount = money_to_cop(raw)
            if amount is None:
                continue
            if amount < 1_000_000:
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
        if not match:
            return None
        return match.group(1).strip(" ,.")

    def _extract_object_terms(self, scrubbed_query: str) -> list[str]:
        cleaned = scrubbed_query
        for alias in ESTADO_SYNONYMS:
            cleaned = re.sub(rf"\b{re.escape(alias)}\b", " ", cleaned)
        for alias in MODALIDAD_KEYWORDS:
            cleaned = cleaned.replace(alias, " ")
        cleaned = re.sub(r"\b(?:\d+(?:[.,]\d+)?)\b", " ", cleaned)
        cleaned = re.sub(r"\b(?:millon(?:es)?|mil|billon(?:es)?|palos?|pesos)\b", " ", cleaned)
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
        advanced_signals = (
            "top", "comparar", "resumen", "quien", "quienes",
            "estadistica", "ranking", "mas reciente", "ultimos",
        )
        if any(signal in normalized_query for signal in advanced_signals):
            return True
        if params.get("entidad") and params.get("objeto"):
            return False
        if params.get("departamento") and params.get("objeto"):
            return False
        if params.get("valor_min") or params.get("valor_max") or params.get("estado"):
            return False
        if params.get("entidad"):
            return False
        return not bool(params.get("objeto"))

    @staticmethod
    def _clean_department_alias(alias: str) -> str:
        cleaned = re.sub(r"^(?:en|del)\s+", "", alias)
        cleaned = re.sub(r"^(?:depto|departamento)\s+del?\s+", "", cleaned)
        cleaned = re.sub(r"^(?:departamento)\s+de\s+", "", cleaned)
        return cleaned.strip()
