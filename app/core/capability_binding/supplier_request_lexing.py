"""Typed lexer for the deliberately small SUPPLIERS request grammar."""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import re


MAX_CARDINAL_DIGITS = 4


class TokenKind(str, Enum):
    WORD = "WORD"
    SUPPLIER_NOUN = "SUPPLIER_NOUN"
    NUMBER_WORD = "NUMBER_WORD"
    BOUNDED_INTEGER = "BOUNDED_INTEGER"
    OVERSIZE_INTEGER = "OVERSIZE_INTEGER"
    QUANTITY_PLACEHOLDER = "QUANTITY_PLACEHOLDER"
    NON_DECIMAL_DIGIT = "NON_DECIMAL_DIGIT"
    COMPARATOR_SYMBOL = "COMPARATOR_SYMBOL"
    SOFT_PUNCT = "SOFT_PUNCT"
    HARD_BREAK = "HARD_BREAK"


@dataclass(frozen=True, slots=True)
class Token:
    kind: TokenKind
    text: str
    start: int
    end: int


_TOKEN = re.compile(
    r">=|<=|!=|==|[><=+\-≥≤≠≈~]|[a-z]+|\d+|[^\w\s]|\n",
    re.ASCII,
)
_SUPPLIER_NOUNS = frozenset({
    "proveedor", "proveedores", "contratista", "contratistas",
})
_NUMBER_VALUES = {
    "dos": 2,
    "tres": 3,
    "cuatro": 4,
    "cinco": 5,
    "seis": 6,
    "siete": 7,
    "ocho": 8,
    "nueve": 9,
    "diez": 10,
    "quince": 15,
    "veinte": 20,
    "treinta": 30,
    "cincuenta": 50,
    "cien": 100,
}
_UNRESOLVED_NUMBER_WORDS = frozenset({
    "uno", "once", "doce", "trece", "catorce", "dieciseis",
    "diecisiete", "dieciocho", "diecinueve", "cuarenta", "sesenta",
    "setenta", "ochenta", "noventa", "ciento", "mil", "millon",
    "millones",
    # La familia veinti- y las centenas no tienen producción: sin esta marca
    # caían a WORD y el número desaparecía sin residuo — un pedido fuera de
    # rango servido con el default en silencio.
    "veintiun", "veintiuna", "veintiuno", "veintidos", "veintitres",
    "veinticuatro", "veinticinco", "veintiseis", "veintisiete",
    "veintiocho", "veintinueve",
    "doscientos", "doscientas", "trescientos", "trescientas",
    "cuatrocientos", "cuatrocientas", "quinientos", "quinientas",
    "seiscientos", "seiscientas", "setecientos", "setecientas",
    "ochocientos", "ochocientas", "novecientos", "novecientas",
})
_COMPARATORS = frozenset({
    ">=", "<=", "!=", "==", ">", "<", "=", "+", "-", "≥", "≤",
    "≠", "≈", "~",
})
_SOFT = frozenset({",", ":", "(", ")", "[", "]"})
_HARD = frozenset({".", "!", "\n"})
_PLACEHOLDERS = frozenset({"x", "n", "#", "*", "?"})
_RANK_MODIFIERS = frozenset({"principales", "primeros", "mayores", "grandes"})


def number_word_value(token: Token) -> int | None:
    """Resolve only the finite NUMBER_WORD grammar."""
    if token.kind is not TokenKind.NUMBER_WORD:
        return None
    return _NUMBER_VALUES.get(token.text)


def _initial_kind(text: str) -> TokenKind:
    if text in _COMPARATORS:
        return TokenKind.COMPARATOR_SYMBOL
    if text.isdecimal():
        return (
            TokenKind.BOUNDED_INTEGER
            if len(text) <= MAX_CARDINAL_DIGITS
            else TokenKind.OVERSIZE_INTEGER
        )
    if text.isdigit():
        # Ethiopic five satisfies isdigit() but not int(); it is a quantity the
        # grammar cannot resolve, not punctuation and not a bounded integer.
        return TokenKind.NON_DECIMAL_DIGIT
    if text in _SUPPLIER_NOUNS:
        return TokenKind.SUPPLIER_NOUN
    if text in _NUMBER_VALUES or text in _UNRESOLVED_NUMBER_WORDS:
        return TokenKind.NUMBER_WORD
    if text.isalpha():
        return TokenKind.WORD
    if text in _SOFT:
        return TokenKind.SOFT_PUNCT
    if text in _HARD:
        return TokenKind.HARD_BREAK
    return TokenKind.HARD_BREAK


def _is_quantity_slot(tokens: list[Token], index: int) -> bool:
    """A placeholder is typed only directly before rank* + supplier noun."""
    cursor = index + 1
    while cursor < len(tokens) and tokens[cursor].kind is TokenKind.SOFT_PUNCT:
        cursor += 1
    while cursor < len(tokens) and tokens[cursor].text in _RANK_MODIFIERS:
        cursor += 1
        while cursor < len(tokens) and tokens[cursor].kind is TokenKind.SOFT_PUNCT:
            cursor += 1
    return cursor < len(tokens) and tokens[cursor].kind is TokenKind.SUPPLIER_NOUN


def tokenize(normalized: str) -> tuple[Token, ...]:
    """Tokenize once with longest-match symbolic comparators and exact spans."""
    tokens = [
        Token(_initial_kind(match.group()), match.group(), match.start(), match.end())
        for match in _TOKEN.finditer(normalized)
    ]
    for index, token in enumerate(tokens):
        if token.text in _PLACEHOLDERS and _is_quantity_slot(tokens, index):
            tokens[index] = replace(token, kind=TokenKind.QUANTITY_PLACEHOLDER)
    return tuple(tokens)


def hard_segments(tokens: tuple[Token, ...]) -> tuple[tuple[Token, ...], ...]:
    segments: list[tuple[Token, ...]] = []
    current: list[Token] = []
    for token in tokens:
        if token.kind is TokenKind.HARD_BREAK:
            if current:
                segments.append(tuple(current))
                current = []
        else:
            current.append(token)
    if current:
        segments.append(tuple(current))
    return tuple(segments)
