"""Query parsing utilities for SECOP natural language search."""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class QueryParams:
    """Structured representation of the user's search intent."""

    entity: str
    filters: Dict[str, Any] = field(default_factory=dict)
    metrics: List[str] = field(default_factory=list)
    raw_query: str = ""
    limit: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.entity or not self.entity.strip():
            raise ValueError("entity must be a non-empty string")
        if self.limit is not None and self.limit <= 0:
            raise ValueError("limit must be a positive integer when provided")

    def as_dict(self) -> Dict[str, Any]:
        return {
            "entity": self.entity,
            "filters": self.filters,
            "metrics": self.metrics,
            "raw_query": self.raw_query,
            "limit": self.limit,
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict())


@dataclass
class GeminiResponse:
    """Internal container for responses returned by the Gemini client."""

    entity: Optional[str]
    filters: Dict[str, Any]
    metrics: List[str]
    limit: Optional[int] = None


class QueryParser:
    """Translate free-form questions into :class:`QueryParams` objects.

    The parser expects a Gemini-compatible client implementing a ``generate``
    method that accepts a prompt and returns a JSON-serialisable string with the
    following shape::

        {"entity": "contracts", "filters": {"year": 2023}, "metrics": []}

    When the model response is invalid or unavailable, a deterministic fallback
    heuristic keeps the API usable for unit tests and offline development.
    """

    def __init__(self, llm_client: Optional[Any] = None) -> None:
        self._llm_client = llm_client

    # Public API ------------------------------------------------------------
    def parse(self, query: str) -> QueryParams:
        """Parse ``query`` into a :class:`QueryParams` instance."""

        logger.debug("Parsing query: %s", query)
        response = None
        if self._llm_client is not None:
            prompt = self._build_prompt(query)
            try:
                logger.debug("Invoking Gemini client with prompt: %s", prompt)
                raw_response = self._llm_client.generate(prompt)
                response = self._coerce_response(raw_response)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Gemini parsing failed; using heuristic fallback")

        if response is None:
            response = self._heuristic_parse(query)

        params = QueryParams(
            entity=response.entity or "contracts",
            filters=response.filters,
            metrics=response.metrics,
            raw_query=query,
            limit=response.limit,
        )
        logger.debug("Parsed query params: %s", params.to_json())
        return params

    # Gemini helpers -------------------------------------------------------
    def _build_prompt(self, query: str) -> str:
        instructions = (
            "You are a query understanding system for the Colombian SECOP open "
            "data portal. Convert the user request into JSON with keys 'entity', "
            "'filters', 'metrics', and optionally 'limit' (a positive integer). "
            "Filters must be a JSON object with simple key/value pairs or lists."
        )
        example = json.dumps(
            {
                "entity": "contracts",
                "filters": {"buyer": "Bogotá", "year": 2023},
                "metrics": ["total_amount"],
                "limit": 20,
            }
        )
        return f"{instructions}\nInput: {query}\nRespond with JSON like: {example}"

    def _coerce_response(self, raw_response: Any) -> Optional[GeminiResponse]:
        if raw_response is None:
            return None
        if isinstance(raw_response, str):
            try:
                payload = json.loads(raw_response)
            except json.JSONDecodeError:
                logger.debug("Model response is not JSON: %s", raw_response)
                return None
        elif isinstance(raw_response, dict):
            payload = raw_response
        else:
            logger.debug("Unsupported response type: %s", type(raw_response))
            return None

        entity = payload.get("entity")
        filters = payload.get("filters") or {}
        metrics = payload.get("metrics") or []
        limit_value = payload.get("limit")

        limit: Optional[int] = None
        if isinstance(limit_value, int) and limit_value > 0:
            limit = limit_value
        elif isinstance(limit_value, str) and limit_value.isdigit():
            parsed_limit = int(limit_value)
            if parsed_limit > 0:
                limit = parsed_limit

        if not isinstance(filters, dict) or not isinstance(metrics, Iterable):
            logger.debug("Invalid response payload structure: %s", payload)
            return None

        return GeminiResponse(
            entity=entity,
            filters=dict(filters),
            metrics=list(metrics),
            limit=limit,
        )

    # Fallback heuristics --------------------------------------------------
    def _heuristic_parse(self, query: str) -> GeminiResponse:
        lowered = query.lower()
        entity = "contracts"
        if "proveedor" in lowered or "supplier" in lowered:
            entity = "suppliers"
        elif "entidad" in lowered or "agency" in lowered:
            entity = "agencies"

        filters: Dict[str, Any] = {}
        limit: Optional[int] = None

        year_match = re.search(r"(19|20)\d{2}", lowered)
        if year_match:
            filters["year"] = int(year_match.group())

        buyer_match = re.search(r"entidad\s+([\w\sáéíóúñ.&-]+)", query, re.IGNORECASE)
        if buyer_match:
            filters["buyer"] = buyer_match.group(1).strip()

        supplier_match = re.search(r"proveedor(?:a)?\s+([\w\sáéíóúñ.&-]+)", query, re.IGNORECASE)
        if supplier_match:
            supplier = supplier_match.group(1).strip()
            supplier = re.sub(r"(?:\b(?:19|20)\d{2})$", "", supplier).strip(" ,.-")
            if supplier:
                filters["supplier"] = supplier

        amount_match = re.search(r"mayor(?: a)?\s+\$?(\d+[\d.,]*)", lowered)
        if amount_match:
            amount = amount_match.group(1).replace(".", "").replace(",", "")
            try:
                filters["min_amount"] = float(amount)
            except ValueError:
                pass

        metrics: List[str] = []
        if "cuánto" in lowered or "total" in lowered or "sum" in lowered:
            metrics.append("total_amount")

        limit_match = re.search(r"(?:top|primer[oa]s?)\s+(\d+)", lowered)
        if limit_match:
            try:
                parsed_limit = int(limit_match.group(1))
                if parsed_limit > 0:
                    limit = parsed_limit
            except ValueError:
                pass

        logger.debug(
            "Heuristic parser result -> entity: %s filters: %s metrics: %s",
            entity,
            filters,
            metrics,
        )
        return GeminiResponse(entity=entity, filters=filters, metrics=metrics, limit=limit)


__all__ = ["QueryParams", "QueryParser"]
