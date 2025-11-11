"""Service layer orchestrating the SECOP search workflow."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from ..core.entity_resolver import EntityResolver, ResolvedEntity
from ..core.formatter import ResultFormatter
from ..core.query_parser import QueryParser, QueryParams
from ..core.secop_client import SecopClient
from ..core.soql_builder import SoQLBuilder

logger = logging.getLogger(__name__)


class SearchService:
    """High level service combining parsing, resolution and querying."""

    def __init__(
        self,
        parser: QueryParser,
        entity_resolver: EntityResolver,
        soql_builder: SoQLBuilder,
        secop_client: SecopClient,
        formatter: Optional[ResultFormatter] = None,
    ) -> None:
        self._parser = parser
        self._resolver = entity_resolver
        self._builder = soql_builder
        self._client = secop_client
        self._formatter = formatter or ResultFormatter()

    def search(self, query: str, channel: str = "web") -> Dict[str, Any]:
        logger.info("Processing SECOP search query", extra={"query": query, "channel": channel})

        params = self._parser.parse(query)
        logger.debug("Parser produced params: %s", params.to_json())

        resolved_entity = self._resolve_entity(params)
        soql_payload = self._builder.build(params, resolved_entity)
        logger.debug("SoQL payload: %s", soql_payload)

        results = self._client.query(soql_payload["dataset"], soql_payload["params"])
        logger.debug("Received %s SECOP rows", len(results) if hasattr(results, "__len__") else "unknown")

        if channel == "whatsapp":
            formatted = self._formatter.format_for_whatsapp(results, params)
        else:
            formatted = self._formatter.format_for_web(results, params)

        return {"data": formatted, "raw": results}

    # ------------------------------------------------------------------
    def _resolve_entity(self, params: QueryParams) -> Optional[ResolvedEntity]:
        mention = params.entity
        resolved = self._resolver.resolve(mention)
        if resolved is None:
            logger.debug("Entity resolver returned None; using mention '%s'", mention)
        return resolved


__all__ = ["SearchService"]
