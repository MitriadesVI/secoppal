"""FastAPI application entrypoint for the SECOP assistant backend."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from .core.entity_resolver import EntityResolver
from .core.formatter import ResultFormatter
from .core.query_parser import QueryParser
from .core.secop_client import SecopClient
from .core.soql_builder import SoQLBuilder
from .services.search_service import SearchService

logger = logging.getLogger(__name__)

_service: Optional[SearchService] = None


class SearchRequest(BaseModel):
    query: str
    channel: str = "web"


def configure_service(service: SearchService) -> None:
    global _service
    _service = service


def get_search_service() -> SearchService:
    if _service is None:
        raise RuntimeError("Search service has not been configured")
    return _service


def create_default_service() -> SearchService:
    parser = QueryParser()
    resolver = EntityResolver()
    builder = SoQLBuilder()
    # A dummy Socrata client is required for local testing. We inject a simple
    # object exposing the ``get`` method that raises a helpful error.
    class _UnconfiguredClient:
        def get(self, dataset: str, **params: Any) -> Any:  # pragma: no cover - guard rail
            raise RuntimeError(
                "SecopClient has not been configured with a real Socrata client. "
                "Use configure_service() to inject a fully initialised instance."
            )

    secop = SecopClient(domain="datos.gov.co", socrata_client=_UnconfiguredClient())
    formatter = ResultFormatter()
    return SearchService(parser, resolver, builder, secop, formatter)


def create_app(service: Optional[SearchService] = None) -> FastAPI:
    app = FastAPI(title="SECOP Assistant")

    if service is None:
        service = create_default_service()
    configure_service(service)

    from .api.whatsapp import router as whatsapp_router  # Local import to avoid cycles

    @app.post("/search")
    def search_endpoint(
        payload: SearchRequest,
        svc: SearchService = Depends(get_search_service),
    ) -> Dict[str, Any]:
        if not payload.query:
            raise HTTPException(status_code=400, detail="Query cannot be empty")
        result = svc.search(payload.query, channel=payload.channel)
        return result

    app.include_router(whatsapp_router)

    return app


app = create_app()


__all__ = [
    "app",
    "create_app",
    "configure_service",
    "get_search_service",
]
