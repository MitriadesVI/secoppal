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
    amount = resolve_amount(text)
    if amount is None:
        # Compatibilidad legacy: expresiones sin dígito cuya cantidad va
        # implícita en la palabra. Es el mínimo necesario para que "un billón" y
        # "mil millones" sigan valiendo lo de siempre. M4 sustituirá esta rama
        # por un tokenizador de numerales verbales; hasta entonces "dos mil
        # millones" seguirá cayendo aquí y valiendo 10^9 (bug H2 conocido,
        # congelado en tests/test_money_v2_invariants.py).
        if re.search(r"\bun\b", text):
            amount = Decimal(1)
        elif re.search(r"\bmil\s+millones\b", text):
            amount = Decimal(1)   # "mil millones" = 1 000 000 000, no 1000 × 1e9
        elif re.search(r"\bmil\b", text):
            amount = Decimal(1000)
        else:
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
