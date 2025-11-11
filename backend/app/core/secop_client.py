"""Client utilities for interacting with the SECOP Socrata API."""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

try:  # pragma: no cover - imported lazily to keep tests light
    from sodapy import Socrata
except Exception:  # pragma: no cover
    Socrata = None  # type: ignore


class SecopClient:
    """Thin wrapper around ``sodapy.Socrata`` with caching and retries."""

    def __init__(
        self,
        domain: str,
        app_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        redis_client: Optional[Any] = None,
        socrata_client: Optional[Any] = None,
        max_retries: int = 3,
        retry_backoff: float = 0.5,
    ) -> None:
        if socrata_client is not None:
            self._client = socrata_client
        elif Socrata is not None:
            self._client = Socrata(domain, app_token, username, password, timeout=30)
        else:  # pragma: no cover - fallback when sodapy not installed
            raise RuntimeError("sodapy is required when no custom client is supplied")

        self._redis = redis_client
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff

    def query(self, dataset: str, params: Optional[Dict[str, Any]] = None) -> Any:
        params = params or {}
        cache_key = self._cache_key(dataset, params)

        if self._redis is not None:
            cached = self._redis.get(cache_key)
            if cached is not None:
                logger.debug("Cache hit for %s", cache_key)
                return json.loads(cached)

        attempt = 0
        while True:
            try:
                logger.debug("Querying SECOP dataset %s with params %s", dataset, params)
                result = self._client.get(dataset, **params)
                break
            except Exception as exc:  # pragma: no cover - network failure path
                attempt += 1
                if attempt > self._max_retries:
                    logger.exception("SECOP query failed after %s attempts", attempt)
                    raise
                sleep_for = self._retry_backoff * attempt
                logger.warning("SECOP query failed (%s); retrying in %.2fs", exc, sleep_for)
                time.sleep(sleep_for)

        if self._redis is not None:
            self._redis.setex(cache_key, 300, json.dumps(result))

        return result

    @staticmethod
    def _cache_key(dataset: str, params: Dict[str, Any]) -> str:
        serialised_params = json.dumps(params, sort_keys=True)
        return f"secop:{dataset}:{serialised_params}"


__all__ = ["SecopClient"]
