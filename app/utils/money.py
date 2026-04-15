from __future__ import annotations

import re
import unicodedata
from typing import Any


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
    """Parses colloquial Colombian money expressions into COP integers."""
    text = normalize_text(raw_text)
    match = re.search(r"(\d+(?:[.,]\d+)?)", text)
    if match:
        amount = float(match.group(1).replace(",", "."))
    elif re.search(r"\bun\b", text):
        amount = 1.0
    elif re.search(r"\bmil\b", text):
        amount = 1000.0
    else:
        return None

    multiplier = 1

    if "billon" in text:
        multiplier = 1_000_000_000
    elif "mil millones" in text:
        multiplier = 1_000_000_000
    elif "millon" in text:
        multiplier = 1_000_000
    elif "palo" in text:
        multiplier = 1_000_000
    elif "mil" in text:
        multiplier = 1_000

    return int(amount * multiplier)


def format_cop(value: Any) -> str:
    amount = safe_money(value)
    if amount <= 0:
        return "Sin valor"
    return f"${amount:,.0f}"
