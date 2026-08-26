from __future__ import annotations

import re
import unicodedata
from decimal import Decimal
from typing import Any

from app.core.money_resolution import find_numeric_literal, resolve_amount
from app.data.smmlv import smmlv_cop


SMMLV_UNIT_PATTERN = r"(?:smmlv|smlmv|smlv|salarios?\s+minimos?)"
_SMMLV_UNIT_RE = re.compile(rf"\b{SMMLV_UNIT_PATTERN}\b", re.IGNORECASE)

# ── Gramática canónica de unidades monetarias ────────────────────────────
# Autoridad única de RECONOCIMIENTO, alineada por construcción con las escalas
# que money_to_cop sabe aplicar más abajo. Vive aquí, pegada a la primitiva de
# conversión, porque una unidad reconocible que la conversión no escale produce
# un monto silenciosamente equivocado — que es como "1 millon" acabó valiendo
# 1.000: el catálogo del router escribía ``millones?``, que no acepta el
# singular, y la alternativa ``mil`` se comía el prefijo de "millon".
#
# El orden de las alternativas es significativo: la escala compuesta primero,
# "mil" al final, para que la coincidencia más larga gane.
MONETARY_ATOMIC_SCALE_UNIT_PATTERN = (
    r"(?:bill[oó]n(?:es)?|mill[oó]n(?:es)?|palos?|barras?)"
)
MONETARY_SCALE_UNIT_PATTERN = (
    rf"(?:mil\s+mill[oó]n(?:es)?|{MONETARY_ATOMIC_SCALE_UNIT_PATTERN})"
)
CURRENCY_MARKER_PATTERN = r"(?:\$|\bcop\b|\bpesos?\b)"
# El lookahead va DENTRO del grupo: los consumidores escriben
# ``{MONETARY_UNIT_PATTERN}?`` para hacer la unidad opcional, y si la frontera
# quedara fuera, el ``?`` haría opcional el lookahead en vez de la unidad.
MONETARY_UNIT_PATTERN = (
    rf"(?:(?:{MONETARY_SCALE_UNIT_PATTERN}|{SMMLV_UNIT_PATTERN}|mil)(?![a-z]))"
)


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


def is_smmlv_expression(text: str) -> bool:
    """True si ``text`` contiene un alias soportado de salario mínimo."""
    return bool(_SMMLV_UNIT_RE.search(normalize_text(text)))


def smmlv_to_cop(raw_text: str, year: int) -> tuple[int, Decimal] | None:
    """Convierte una cantidad SMMLV con año explícito a COP y cantidad fuente."""
    text = normalize_text(raw_text)
    if not _SMMLV_UNIT_RE.search(text):
        return None
    annual_value = smmlv_cop(year)
    if annual_value is None:
        return None
    amount_text = _SMMLV_UNIT_RE.sub(" ", text)
    amount = resolve_amount(amount_text)
    if amount is None or amount <= 0:
        return None
    return int(amount * annual_value), amount


def money_to_cop(raw_text: str) -> int | None:
    """Parses colloquial Colombian money expressions into COP integers.

    Contrato público inalterado: ``str -> int | None``. La cantidad la resuelve
    ``app.core.money_resolution`` (puntuación colombiana, Decimal); las unidades
    y su escala siguen siendo autoridad de esta función.
    """
    text = normalize_text(raw_text)
    # SMMLV no tiene valor atemporal. El router usa smmlv_to_cop con un año y
    # conserva metadata; este contrato escalar se niega a ocultar esa decisión.
    if _SMMLV_UNIT_RE.search(text):
        return None
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
        # En un cardinal verbal, ``resolve_amount`` ya incorpora la escala de
        # ``mil`` ("dos mil" -> 2000). Solo los literales numéricos usan
        # ``mil`` como unidad separada ("500 mil" -> 500 * 1000).
        multiplier = 1_000 if find_numeric_literal(text) is not None else 1

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
