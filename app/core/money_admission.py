"""Política de admisión monetaria — qué monto puede convertirse en filtro.

Money V2 / HCA-P1. Responsabilidad única: decidir si una cifra ya convertida a
COP debe aceptarse como ``valor_min``/``valor_max``, o si es ruido numérico que
el parser tiene que ignorar. NO convierte, NO conoce polaridad, NO conoce
datasets y NO formatea.

    money_resolution  →  app/utils/money.py::money_to_cop  →  money_admission

INV-EXPLICIT-MONEY-IS-NOT-DROPPED-BY-MAGNITUDE
----------------------------------------------
Cuando el usuario expresa un comparador, una cantidad y una **unidad monetaria
explícita**, el monto es una señal inequívoca y el router no puede descartarlo
por ser pequeño. "más de 100 mil pesos" pide exactamente 100.000 COP.

Antes de HCA-P1 el router aplicaba un piso duro de 1.000.000 COP en los ocho
sitios donde acepta una cifra. El piso existe por una razón real —evita que
"100 procesos", "2025" o un NIT se conviertan en dinero— pero estaba aplicado a
la cifra en vez de a la *evidencia*, así que también mataba montos declarados.
Aquí el piso se conserva **solo para números desnudos**.

INV-NO-SILENT-EXPLICIT-MONEY-DROP
---------------------------------
El complemento: si hay evidencia monetaria explícita y aun así la cifra no puede
resolverse, el sistema debe pedir precisión en vez de ejecutar sin filtro de
valor. Ejecutar en silencio devuelve un universo que el usuario no pidió y que
no tiene forma de detectar.

Deliberadamente NO cuenta como evidencia "<dígitos> mil" sin marcador de moneda:
"100 mil" a secas es tan compatible con una cantidad de ítems como con un monto,
y este módulo prefiere no producir filtro a producir uno equivocado.

Puro, determinista, sin estado, sin I/O.
"""
from __future__ import annotations

import re
from enum import Enum

from app.core.money_polarity import MoneyRelation, resolve_money_polarity
from app.utils.money import (
    CURRENCY_MARKER_PATTERN,
    MONETARY_SCALE_UNIT_PATTERN,
    SMMLV_UNIT_PATTERN,
    normalize_text,
    money_to_cop,
)

__all__ = [
    "MIN_BARE_AMOUNT_COP",
    "RANGE_PARTIAL_CLARIFICATION",
    "RangeVerdict",
    "admit_range",
    "has_explicit_monetary_evidence",
    "is_admissible_amount",
    "shared_currency_marker",
    "protected_money_spans",
]

# Piso anti-ruido histórico. Sigue vigente, pero solo para cifras SIN unidad
# monetaria: es una defensa contra números que nunca fueron dinero.
MIN_BARE_AMOUNT_COP = 1_000_000

_EXPLICIT_EVIDENCE_RE = re.compile(
    rf"{CURRENCY_MARKER_PATTERN}|{MONETARY_SCALE_UNIT_PATTERN}|{SMMLV_UNIT_PATTERN}",
    re.IGNORECASE,
)


def has_explicit_monetary_evidence(span: str) -> bool:
    """True si ``span`` declara la unidad: marcador de moneda o escala ≥ millón."""
    return bool(_EXPLICIT_EVIDENCE_RE.search(normalize_text(span or "")))


def is_admissible_amount(amount: int | None, span: str) -> bool:
    """True si ``amount`` puede aplicarse como filtro de valor.

    Un monto con unidad monetaria explícita se admite en cualquier magnitud
    positiva; uno desnudo debe superar el piso anti-ruido.
    """
    if amount is None or amount <= 0:
        return False
    if has_explicit_monetary_evidence(span):
        return True
    return amount >= MIN_BARE_AMOUNT_COP


def protected_money_spans(text: str) -> tuple[tuple[int, int], ...]:
    """Protect lexical money relations only after monetary admission.

    "menos 2025" is a time exclusion, not an economic upper bound. Explicit
    units also protect unconvertible money so it can reach clarification.
    Lexical families remain exclusively owned by money_polarity.
    """
    return tuple(
        (decision.start, decision.end)
        for decision in resolve_money_polarity(text)
        if (decision.comparator_span or decision.relation is MoneyRelation.RANGE)
        and any(
            has_explicit_monetary_evidence(operand)
            or is_admissible_amount(money_to_cop(operand), operand)
            for operand in decision.amount_spans
        )
    )


# ── Rangos explícitos ────────────────────────────────────────────────────
#
# INV-MONEY-RANGE-ATOMIC
# ----------------------
# Un rango con dos extremos declarados se aplica ENTERO o no se aplica. Nunca
# se degrada a una cota unilateral.
#
# El caso que obligó a escribirlo: "entre 120 mil y 350 mil pesos". El bloque
# de rango es todo-o-nada, así que al rechazar el extremo bajo descartaba el
# rango completo… y un bucle genérico posterior volvía a leer el texto y fijaba
# solo ``valor_max``. El usuario pedía un intervalo y recibía un techo sin
# piso, sin ninguna señal de que la mitad de su petición se había perdido. Un
# techo sin piso no es "menos preciso": es otra pregunta, y devuelve resultados
# perfectamente plausibles por debajo de lo que el usuario quería ver.

_CURRENCY_MARKER_RE = re.compile(CURRENCY_MARKER_PATTERN, re.IGNORECASE)


class RangeVerdict(str, Enum):
    """Qué puede hacerse con un rango de dos extremos ya convertidos."""

    EXECUTE = "EXECUTE"   # los dos extremos son admisibles
    BLOCK = "BLOCK"       # exactamente uno lo es → pedir precisión, no ejecutar
    ABSENT = "ABSENT"     # ninguno lo es → el rango no era monetario


def shared_currency_marker(*spans: str) -> str:
    """Marcador de moneda que gobierna TODOS los operandos del rango.

    El español lo escribe una sola vez, al final: "entre 120 mil y 350 mil
    **pesos**". Leerlo como propiedad exclusiva del operando que lo lleva
    literalmente deja al otro desnudo, bajo el piso anti-ruido, y convierte un
    intervalo en una cota. La coordinación distribuye la unidad; el parser
    también debe hacerlo.
    """
    for span in spans:
        match = _CURRENCY_MARKER_RE.search(normalize_text(span or ""))
        if match:
            return match.group(0)
    return ""


def admit_range(
    low_amount: int | None,
    low_span: str,
    high_amount: int | None,
    high_span: str,
) -> RangeVerdict:
    """Decide el destino de un rango explícito, sin aplicarlo a medias."""
    marker = shared_currency_marker(low_span, high_span)
    low_ok = is_admissible_amount(low_amount, f"{low_span} {marker}")
    high_ok = is_admissible_amount(high_amount, f"{high_span} {marker}")
    if low_ok and high_ok:
        return RangeVerdict.EXECUTE
    if low_ok or high_ok:
        return RangeVerdict.BLOCK
    return RangeVerdict.ABSENT


RANGE_PARTIAL_CLARIFICATION = (
    "Entendí que quieres un rango de valor en \"{span}\", pero solo pude "
    "interpretar uno de los dos extremos. Dime los dos con su unidad "
    "—por ejemplo \"entre 120 mil pesos y 350 mil pesos\"— y lo aplico completo."
)
