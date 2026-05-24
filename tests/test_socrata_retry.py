"""Tests para retry/backoff en SecopClient (B5).

SecopClient.max_retries default is 1 (one attempt, no internal retry).
Retry/degradation logic lives in the orchestrator, not here.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.core.secop_client import SecopClient


def test_count_single_attempt_no_retry():
    """With max_retries=1, only one attempt is made; no internal retry."""
    client = MagicMock()
    client.get.side_effect = TimeoutError("read timeout")
    secop = SecopClient("test.gov", "token", client=client)

    with pytest.raises(RuntimeError):
        secop.count("ds", "SELECT count(*)")
    assert client.get.call_count == 1


def test_count_succeeds_on_first_attempt():
    """If the first attempt succeeds, no retry needed."""
    client = MagicMock()
    client.get.return_value = [{"count": "42"}]
    secop = SecopClient("test.gov", "token", client=client)
    assert secop.count("ds", "SELECT count(*)") == 42
    assert client.get.call_count == 1


def test_query_single_attempt_no_retry():
    """With max_retries=1, query attempts once and raises on failure."""
    client = MagicMock()
    client.get.side_effect = TimeoutError("read timeout")
    secop = SecopClient("test.gov", "token", client=client)

    with pytest.raises(RuntimeError):
        secop.query("ds", "SELECT *")
    assert client.get.call_count == 1


def test_query_succeeds_on_first_attempt():
    """If the first query attempt succeeds, return normalized rows."""
    client = MagicMock()
    client.get.return_value = [{"id": "1", "urlproceso": "https://test"}]
    secop = SecopClient("test.gov", "token", client=client)
    result = secop.query("ds", "SELECT *")
    assert len(result) == 1
    assert result[0]["id"] == "1"
    assert client.get.call_count == 1
