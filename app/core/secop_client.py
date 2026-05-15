from __future__ import annotations

import time
from typing import Any

try:
    from sodapy import Socrata
except ImportError:  # pragma: no cover - guarded for environments without deps
    Socrata = None

from app.utils.logging import get_logger

logger = get_logger(__name__)


class SecopClient:
    """Thin wrapper around Socrata with retries and SECOP quirks handling."""

    def __init__(
        self,
        domain: str,
        app_token: str | None,
        timeout: int = 30,
        client: Any | None = None,
        max_retries: int = 3,
    ) -> None:
        if client is None and Socrata is None:
            raise RuntimeError("sodapy is required to instantiate SecopClient without a custom client.")

        self.client = client or Socrata(domain, app_token=app_token, timeout=timeout)
        self.max_retries = max_retries

    def aggregate(self, dataset_id: str, soql: str, timeout: int | None = None) -> list[dict]:
        """Execute a single aggregation query. No retries — best-effort for observer."""
        client = self.client
        if timeout is not None:
            # Patch timeout on the underlying client for this call if possible
            orig = getattr(client, "timeout", None)
            try:
                client.timeout = timeout
                rows = client.get(dataset_id, query=soql)
            finally:
                if orig is not None:
                    client.timeout = orig
        else:
            rows = client.get(dataset_id, query=soql)
        return list(rows)

    def count(self, dataset_id: str, soql_count_query: str) -> int:
        """Execute a SELECT count(*) query and return the integer result."""
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                rows = self.client.get(dataset_id, query=soql_count_query)
                if rows:
                    raw = list(rows[0].values())[0]
                    return int(raw)
                return 0
            except Exception as exc:  # pragma: no cover - network/runtime path
                last_error = exc
                logger.warning("SECOP count failed on attempt %s/%s: %s", attempt + 1, self.max_retries, exc)
                if attempt < self.max_retries - 1:
                    time.sleep(2**attempt)

        raise RuntimeError(f"Unable to count SECOP dataset {dataset_id}") from last_error

    def query(self, dataset_id: str, soql_query: str) -> list[dict]:
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                logger.info("Executing SECOP query against %s", dataset_id)
                rows = self.client.get(dataset_id, query=soql_query)
                return [self._normalize_row(dataset_id, row) for row in rows]
            except Exception as exc:  # pragma: no cover - network/runtime path
                last_error = exc
                logger.warning("SECOP query failed on attempt %s/%s: %s", attempt + 1, self.max_retries, exc)
                if attempt < self.max_retries - 1:
                    time.sleep(2**attempt)

        raise RuntimeError(f"Unable to query SECOP dataset {dataset_id}") from last_error

    def _normalize_row(self, dataset_id: str, row: dict) -> dict:
        normalized = dict(row)
        url_value = normalized.get("urlproceso", "")
        if isinstance(url_value, dict):
            url_value = url_value.get("url", "")
        url_value = str(url_value or "")

        # Fix broken URLs for BOTH datasets
        if not url_value or "Login" in url_value:
            if dataset_id == "p6dx-8zbt":
                reference = normalized.get("referencia_del_proceso", "")
            else:
                reference = normalized.get("referencia_del_contrato", "")

            if reference:
                url_value = (
                    "https://community.secop.gov.co/Public/Tendering/"
                    f"OpportunityDetail/Index?noticeUID={reference}"
                )

        normalized["urlproceso"] = url_value

        # Normalize monetary values to float for consistent handling
        for money_field in ("precio_base", "valor_del_contrato", "valor_contrato_con_adiciones"):
            if money_field in normalized:
                normalized[money_field] = self._safe_money(normalized[money_field])

        return normalized

    @staticmethod
    def _safe_money(value: object) -> float | None:
        """Robustly convert SECOP monetary values (strings, floats, nulls)."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            cleaned = value.replace(",", "").replace("$", "").replace(" ", "").strip()
            if not cleaned:
                return None
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None

