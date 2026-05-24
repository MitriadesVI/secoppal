"""Text normalization utilities for SECOPPAL.

Includes accent stripping for matching SECOP data with tildes against user
queries without tildes, and vice versa.
"""
from __future__ import annotations

import re
import unicodedata


_ACCENT_REPLACEMENTS: dict[str, str] = {
    "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u", "ñ": "n",
    "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U", "Ñ": "N",
}


def strip_accents(text: str) -> str:
    """Remove tildes and accents from a string, preserving uppercase/lowercase.

    Used to normalize query-side values before injecting into SoQL WHERE clauses,
    so that a user typing 'CONSULTORÍA' matches data with 'CONSULTORIA' and
    vice versa.
    """
    result = text
    for accented, plain in _ACCENT_REPLACEMENTS.items():
        result = result.replace(accented, plain)
    return result
