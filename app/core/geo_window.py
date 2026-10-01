"""Context window for the geography authority: it can only RETIRE authority.

The window is ``left``/``right`` folded tokens (at most ``WINDOW_TOKENS`` each)
around the quote. It never decides a level, a department or a municipality: a
RESOLVED value may be withdrawn (CLARIFY) when the neighbours show the quote is
the owner of the records (``del municipio de X``, ``alcaldía de X``), that the
typed frame contradicts the resolved level, or that the next words name a
different department. It cannot turn CLARIFY/BLOCK into RESOLVED.
"""
from __future__ import annotations

import re

WINDOW_TOKENS = 4

_OWNER_CUE = re.compile(
    r'\b(?:(?:alcaldia|gobernacion|alcalde|gobernador|concejo|asamblea|personeria|contraloria)'
    r'(?:\s+(?:municipal|departamental|distrital))?\s+(?:de|del)'
    r'|(?:del|de\s+la|al)\s+(?:municipio|distrito|departamento|ciudad)\s+de)$')
_FRAME_BEFORE = re.compile(r'\b(municipio|ciudad|distrito|departamento|depto|region|zona)\s+(?:de|del)(?:\s+(?:la|el))?$')
_FRAME_LEVEL = {'municipio': 'MUNICIPALITY', 'ciudad': 'MUNICIPALITY', 'distrito': 'MUNICIPALITY',
                'departamento': 'DEPARTMENT', 'depto': 'DEPARTMENT', 'region': 'REGION', 'zona': 'REGION'}


def clip(tokens: tuple[str, ...] | list[str] | None, side: str) -> tuple[str, ...]:
    seq = tuple(tokens or ())
    return seq[-WINDOW_TOKENS:] if side == 'left' else seq[:WINDOW_TOKENS]


def owner_cue(left: tuple[str, ...], quote_tokens: tuple[str, ...]) -> bool:
    """True when the words before the place name make it the OWNER (buyer).

    The cue may end in the left window or inside a typed frame at the start of
    the quote (left ``del`` + quote ``municipio de Soledad``).
    """
    return any(_OWNER_CUE.search(' '.join(left + quote_tokens[:k])) for k in range(0, 4))


def frame_level_before(left: tuple[str, ...]) -> str | None:
    """Level typed by the words immediately before the quote (``municipio de``)."""
    found = _FRAME_BEFORE.search(' '.join(left))
    return _FRAME_LEVEL[found.group(1)] if found else None


def department_after(right: tuple[str, ...], department_of) -> str | None:
    """Department named right after the quote (``... , Tolima`` / ``departamento de Tolima``)."""
    tokens = [t for t in right]
    while tokens and tokens[0] in {'en', 'de', 'del', 'departamento', 'depto'}:
        tokens.pop(0)
    for width in range(min(len(tokens), WINDOW_TOKENS), 0, -1):
        found = department_of(' '.join(tokens[:width]))
        if found:
            return found
    return None
