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

from app.utils.money import (
    CURRENCY_MARKER_PATTERN,
    MONETARY_SCALE_UNIT_PATTERN,
    SMMLV_UNIT_PATTERN,
    normalize_text,
)

__all__ = [
    "MIN_BARE_AMOUNT_COP",
    "has_explicit_monetary_evidence",
    "is_admissible_amount",
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
