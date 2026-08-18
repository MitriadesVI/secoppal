from __future__ import annotations

import re
import unicodedata
from decimal import Decimal
from typing import Any

from app.core.money_resolution import resolve_amount


def safe_money(value: Any) -> float:
    """Converts SECOP monetary fields into floats safely."""
    if value is None or value == "":
        return 0.0
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return 0.0

    text = text.replace("$", "").replace(",", "").strip()
    return float(text)


def normalize_text(text: str) -> str:
    """Lowercases and removes diacritics to simplify matching."""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char)).lower().strip()


def money_to_cop(raw_text: str) -> int | None:
    """Parses colloquial Colombian money expressions into COP integers.

    Contrato público inalterado: ``str -> int | None``. La cantidad la resuelve
    ``app.core.money_resolution`` (puntuación colombiana, Decimal); las unidades
    y su escala siguen siendo autoridad de esta función.
    """
    text = normalize_text(raw_text)
    # Las fracciones verbales están deliberadamente fuera de la gramática M4.
    # Rechazarlas evita interpretar parcialmente "un millón y medio" como 1M.
    if re.search(r"\by\s+medi[oa]s?\b", text):
        return None
    # "mil millones" es una unidad léxica compuesta: el cardinal que la
    # precede multiplica 10^9. En cambio "mil quinientos millones" contiene el
    # cardinal 1500 y la unidad simple millón.
    compound_scale = re.search(r"\bmil\s+millones\b", text)
    if compound_scale:
        prefix = text[:compound_scale.start()]
        amount = resolve_amount(prefix) if prefix.strip() else Decimal(1)
    else:
        amount = resolve_amount(text)
    if amount is None:
        return None

    multiplier = 1

    if "billon" in text:
        multiplier = 1_000_000_000_000
    elif "mil millones" in text:
        multiplier = 1_000_000_000
    elif "millon" in text:
        multiplier = 1_000_000
    elif "palo" in text:
        multiplier = 1_000_000
    elif "barra" in text:
        multiplier = 1_000_000
    elif "mil" in text:
        multiplier = 1_000

    return int(amount * multiplier)


def format_cop(value: Any) -> str:
    amount = safe_money(value)
    if amount <= 0:
        return "Sin valor"
    return f"${amount:,.0f}"


def format_money(value: Any) -> str:
    """Formato legible para cifras grandes — convención Colombia.

    Uso: narración conversacional (chat/whatsapp/telegram) donde la
    compacidad importa más que el centavo exacto.

    Para vistas que requieren precisión exacta (tarjetas, detalle),
    usar format_cop, que devuelve el valor completo con separadores.

    billón  = 10^12  → $X.XXB
    mil millones = 10^9  → $X.XXMM
    millón  = 10^6   → $XM
    """
    amount = safe_money(value)
    if amount <= 0:
        return "Sin valor"
    if amount >= 1_000_000_000_000:
        return f"${amount / 1_000_000_000_000:.2f}B"
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.2f}MM"
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.0f}M"
    return f"${amount:,.0f}"
