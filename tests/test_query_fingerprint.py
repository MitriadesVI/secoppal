"""Etapa 4 — huella estable de consulta (sha256, no hash())."""
from __future__ import annotations

from app.core.query_fingerprint import query_fingerprint


def test_same_query_same_fingerprint():
    a = {"objeto": ["aire", "acondicionado"], "ciudad": "Medellin", "dataset": "procesos"}
    b = {"dataset": "procesos", "ciudad": "Medellin", "objeto": ["acondicionado", "aire"]}
    assert query_fingerprint(a) == query_fingerprint(b)  # orden de claves/listas no importa


def test_transient_keys_do_not_change_fingerprint():
    base = {"objeto": ["x"], "dataset": "procesos"}
    paged = {**base, "offset": 25, "limit": 50, "_display_offset": 25}
    assert query_fingerprint(base) == query_fingerprint(paged)


def test_different_filter_changes_fingerprint():
    a = {"objeto": ["x"], "dataset": "procesos"}
    b = {"objeto": ["x"], "dataset": "procesos", "valor_min": 100}
    assert query_fingerprint(a) != query_fingerprint(b)


def test_extra_versions_change_fingerprint():
    base = {"objeto": ["x"], "dataset": "procesos"}
    assert query_fingerprint(base, extra={"ranking_version": "v2"}) != query_fingerprint(base)


def test_fingerprint_is_hex_sha256():
    fp = query_fingerprint({"objeto": ["x"]})
    assert len(fp) == 64 and all(c in "0123456789abcdef" for c in fp)
