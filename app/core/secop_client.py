from __future__ import annotations

import time
from typing import Any

try:
    from sodapy import Socrata
except ImportError:  # pragma: no cover - guarded for environments without deps
    Socrata = None

from app.core.instrumentation import http_span, record_http
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
        max_retries: int = 1,
    ) -> None:
        if client is None and Socrata is None:
            raise RuntimeError("sodapy is required to instantiate SecopClient without a custom client.")

        self.domain = domain
        self.app_token = app_token
        self.timeout = timeout
        self.client = client or Socrata(domain, app_token=app_token, timeout=timeout)
        self.max_retries = max_retries

    def with_timeout(self, timeout: int) -> "SecopClient":
        """Return a new SecopClient with a different timeout, sharing nothing mutable."""
        if Socrata is None:
            raise RuntimeError("sodapy is required for with_timeout.")
        return SecopClient(
            domain=self.domain,
            app_token=self.app_token,
            timeout=timeout,
            max_retries=self.max_retries,
        )

    def aggregate(self, dataset_id: str, soql: str) -> list[dict]:
        """Execute a single aggregation query. No retries — best-effort for observer."""
        with http_span("aggregate", dataset_id, soql) as span:
            rows = list(self.client.get(dataset_id, query=soql))
            span["rows_received"] = len(rows)
        return rows

    def count(self, dataset_id: str, soql_count_query: str) -> int:
        """Execute a SELECT count(*) query and return the integer result."""
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                with http_span("count", dataset_id, soql_count_query, attempt=attempt + 1) as span:
                    rows = self.client.get(dataset_id, query=soql_count_query)
                    span["rows_received"] = len(rows)
                if rows:
                    raw = list(rows[0].values())[0]
                    return int(raw)
                return 0
            except Exception as exc:  # pragma: no cover - network/runtime path
                last_error = exc
                logger.warning("SECOP count failed on attempt %s/%s: %s", attempt + 1, self.max_retries, exc)
                if attempt < self.max_retries - 1:
                    delay = 2**attempt
                    record_http({
                        "operation": "retry_delay", "dataset": dataset_id,
                        "attempt": attempt + 1, "duration_ms": float(delay * 1000),
                        "status": "completed", "rows_received": 0,
                    })
                    time.sleep(delay)

        raise RuntimeError(f"Unable to count SECOP dataset {dataset_id}") from last_error

    def query(self, dataset_id: str, soql_query: str) -> list[dict]:
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                logger.info("Executing SECOP query against %s", dataset_id)
                with http_span("query", dataset_id, soql_query, attempt=attempt + 1) as span:
                    rows = self.client.get(dataset_id, query=soql_query)
                    span["rows_received"] = len(rows)
                return [self._normalize_row(dataset_id, row) for row in rows]
            except Exception as exc:  # pragma: no cover - network/runtime path
                last_error = exc
                logger.warning("SECOP query failed on attempt %s/%s: %s", attempt + 1, self.max_retries, exc)
                if attempt < self.max_retries - 1:
                    delay = 2**attempt
                    record_http({
                        "operation": "retry_delay", "dataset": dataset_id,
                        "attempt": attempt + 1, "duration_ms": float(delay * 1000),
                        "status": "completed", "rows_received": 0,
                    })
                    time.sleep(delay)

        raise RuntimeError(f"Unable to query SECOP dataset {dataset_id}") from last_error

    def _normalize_row(self, dataset_id: str, row: dict) -> dict:
        normalized = dict(row)
        url_value = normalized.get("urlproceso", "")
        if isinstance(url_value, dict):
            url_value = url_value.get("url", "")
        url_value = str(url_value or "")

        # URL repair hierarchy (FIX-URL-FREEFORM-001 + DOGFOOD-URL-005):
        # 1. Use original URL if valid (not empty, not "Login")
        # 2. Build from id_del_proceso / id_contrato (CO1.NTC = público; CO1.REQ = pre-publicación, no link)
        # 3. Fallback to url-encoded referencia
        # 4. Empty string (UI muestra "Vista pública no disponible")
        if not url_value or "Login" in url_value:
            if dataset_id == "p6dx-8zbt":
                process_id = normalized.get("id_del_proceso", "")
                reference = normalized.get("referencia_del_proceso", "")
            else:
                process_id = normalized.get("id_contrato", "")
                reference = normalized.get("referencia_del_contrato", "")

            url_value = ""
            pid = str(process_id or "").strip()
            # CO1.REQ.X son requisiciones pre-publicación; SECOP devuelve
            # "argument out of range" si las consultamos vía OpportunityDetail.
            # Mejor no mostrar link que mostrar uno roto.
            if pid and not pid.upper().startswith("CO1.REQ."):
                from urllib.parse import quote
                url_value = (
                    "https://community.secop.gov.co/Public/Tendering/"
                    f"OpportunityDetail/Index?noticeUID={quote(pid)}"
                )
            elif reference and not pid.upper().startswith("CO1.REQ."):
                from urllib.parse import quote
                ref_str = str(reference).strip()
                url_value = (
                    "https://community.secop.gov.co/Public/Tendering/"
                    f"OpportunityDetail/Index?noticeUID={quote(ref_str)}"
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

