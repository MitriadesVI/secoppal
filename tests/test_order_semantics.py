"""H4 — gate semántico del ORDER BY (presentacional vs semántico)."""
from __future__ import annotations

from app.core.order_semantics import (
    ORDER_EXACT,
    ORDER_SAMPLE,
    ORDER_UNAVAILABLE,
    order_is_presentational,
    order_is_semantic,
    sort_page_presentational,
)


# ── gate ─────────────────────────────────────────────────────────────────────

def test_default_order_is_presentational():
    # Sin ordering_signal: el ORDER BY es el default del sistema → retirable.
    assert order_is_presentational({"objeto_phrases": ["aire"]}) is True
    assert order_is_semantic({"objeto_phrases": ["aire"]}) is False


def test_valor_desc_is_semantic():
    # "los más caros" / "mayor valor" → valor_desc → NO retirable.
    p = {"ordering_signal": "valor_desc"}
    assert order_is_semantic(p) is True
    assert order_is_presentational(p) is False


def test_fecha_desc_is_semantic():
    # "los últimos" / "más recientes" → fecha_desc → NO retirable.
    p = {"ordering_signal": "fecha_desc"}
    assert order_is_semantic(p) is True
    assert order_is_presentational(p) is False


def test_empty_params_are_presentational():
    assert order_is_presentational({}) is True
    assert order_is_semantic(None) is False


# ── garantías de orden (constantes estables para el formateo) ────────────────

def test_order_guarantee_constants_distinct():
    assert len({ORDER_EXACT, ORDER_SAMPLE, ORDER_UNAVAILABLE}) == 3


# ── sort de página (best-effort, nunca lanza) ────────────────────────────────

def test_sort_page_by_date_then_value_desc():
    rows = [
        {"f": "2024-01-01", "v": "100"},
        {"f": "2026-05-01", "v": "5"},
        {"f": "2026-05-01", "v": "9"},
    ]
    out = sort_page_presentational(rows, "f", "v")
    assert out[0] == {"f": "2026-05-01", "v": "9"}
    assert out[1] == {"f": "2026-05-01", "v": "5"}
    assert out[2] == {"f": "2024-01-01", "v": "100"}


def test_sort_page_tolerates_missing_and_unparseable():
    rows = [{"f": "2026-01-01"}, {"f": None, "v": "x"}, {"f": "2025-01-01", "v": "3"}]
    out = sort_page_presentational(rows, "f", "v")
    assert out[0]["f"] == "2026-01-01"        # no lanza; ausentes al fondo
    assert len(out) == 3


def test_sort_page_empty():
    assert sort_page_presentational([], "f", "v") == []
    assert sort_page_presentational(None, "f", "v") == []
