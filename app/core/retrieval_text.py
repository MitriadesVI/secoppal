"""Normalización pura de texto para retrieval y validación de frases.

`normalize_retrieval_text()` colapsa la suciedad del texto de SECOP (saltos de
línea, puntuación, tildes, mayúsculas, espacios repetidos) a una forma uniforme
sobre la que aplicar matching de frase tolerante.

Función PURA: sin estado, sin red, sin dependencias de dominio. Reutilizable por
frases, ranking y polisemia. NO genera SoQL ni conoce "manual de contratación".

Invariantes (cubiertas por tests/test_retrieval_text.py):
  - None / "" -> ""
  - idempotente: normalize(normalize(t)) == normalize(t)
  - convierte separadores (puntuación, \\n, \\t, guiones, barras) en UN espacio;
    NO elimina alfanuméricos — números y letras se preservan, no se trituran.
"""
from __future__ import annotations

import re
import unicodedata

_SEPARATORS = re.compile(r"[^a-z0-9]+")


def normalize_retrieval_text(text: str | None) -> str:
    """Devuelve `text` en minúsculas, sin tildes y con separadores colapsados.

    >>> normalize_retrieval_text("MANUAL DE\\nCONTRATACIÓN")
    'manual de contratacion'
    >>> normalize_retrieval_text("Manual;  interno de contratación")
    'manual interno de contratacion'
    """
    if not text:
        return ""
    decomposed = unicodedata.normalize("NFKD", text)
    no_accents = "".join(c for c in decomposed if not unicodedata.combining(c))
    return _SEPARATORS.sub(" ", no_accents.lower()).strip()
