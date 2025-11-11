"""Formatting helpers for rendering SECOP responses."""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List

from .query_parser import QueryParams


class ResultFormatter:
    """Produce WhatsApp and web friendly representations of search results."""

    def format_for_whatsapp(self, records: Iterable[Dict[str, Any]], params: QueryParams) -> str:
        records = list(records)
        if not records:
            return "No se encontraron resultados para tu búsqueda."  # Spanish friendly

        lines: List[str] = ["Resultados para tu consulta:"]
        for idx, record in enumerate(records[:5], start=1):
            summary = self._summarise_record(record)
            lines.append(f"{idx}. {summary}")

        if len(records) > 5:
            lines.append(f"Y {len(records) - 5} resultados más…")
        return "\n".join(lines)

    def format_for_web(self, records: Iterable[Dict[str, Any]], params: QueryParams) -> Dict[str, Any]:
        data = list(records)
        return {
            "query": params.as_dict(),
            "count": len(data),
            "results": data,
        }

    def _summarise_record(self, record: Dict[str, Any]) -> str:
        pieces = []
        for key in ("buyer", "supplier", "amount", "status"):
            if key in record and record[key]:
                pieces.append(f"{key.capitalize()}: {record[key]}")
        if not pieces:
            pieces.append(json.dumps(record, ensure_ascii=False))
        return " | ".join(pieces)


__all__ = ["ResultFormatter"]
