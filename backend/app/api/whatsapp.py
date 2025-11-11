"""WhatsApp webhook integration for SECOP search."""
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request

from ..services.search_service import SearchService
from ..main import get_search_service

router = APIRouter(prefix="/whatsapp", tags=["whatsapp"])


@router.post("/webhook")
async def whatsapp_webhook(
    request: Request,
    service: SearchService = Depends(get_search_service),
) -> Dict[str, Any]:
    payload = await request.json()
    query = payload.get("Body") or payload.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Missing query text")

    result = service.search(query, channel="whatsapp")
    return {"reply": result["data"]}


__all__ = ["router"]
