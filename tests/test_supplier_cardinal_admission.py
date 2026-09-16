"""Positive admission of the cardinal span's immediate context.

Supplier Request Authority V1 admitted ``EXACT`` whenever it failed to recognize
an operator, so an unmodelled comparator ("superior a 5 proveedores") degraded
to the one exact value the capability can serve.  Completeness cannot be proved
by enumerating every bad operator in Spanish; it is proved by requiring an
accredited boundary on each side of the cardinal span.  An opener missing from
the whitelist costs coverage (the turn clarifies); an operator missing from a
blacklist cost authority (the turn executed the wrong question).
"""
from __future__ import annotations

import pytest

from app.core.capability_binding.supplier_request_authority import assess_supplier_request
from app.core.capability_binding.supplier_request_types import (
    Authorization,
    CardinalityRelation,
    CardinalityResidueReason,
    Polarity,
)


def _evidence(text: str):
    return assess_supplier_request(text)


# ZEUS reported the first four; the rest are neighbours of the same class found
# while reproducing them.  Every one of them executed as EXACT(5) before.
@pytest.mark.parametrize("text", [
    "diferente de 5 proveedores",
    "mayor de 5 proveedores",
    "superior a 5 proveedores",
    "seis o cinco proveedores",
    "por encima de 5 proveedores",
    "cuando mucho 5 proveedores",
    "distinto de 5 proveedores",
    "alrededor de 5 proveedores",
    "casi 5 proveedores",
    "salvo 5 proveedores",
    "aparte de los 5 proveedores",
    "un total distinto de 5 proveedores",
    "no mas de 5 proveedores",
])
def test_unmodelled_left_operator_never_degrades_to_exact(text):
    evidence = _evidence(text)
    assert evidence.authorization is Authorization.NOT_ACCREDITED
    assert evidence.cardinality.relation is not CardinalityRelation.EXACT
    assert evidence.cardinality.values == ()


@pytest.mark.parametrize("text", [
    "5 proveedores aproximadamente",
    "5 proveedores mas o menos",
    "5 proveedores al menos",
    "5 proveedores minimo",
    pytest.param("5 proveedores en adelante", marks=pytest.mark.xfail(
        strict=True,
        reason="SUPPLIER_RIGHT_TAIL_AFTER_LINK_DEBT: a quantity locution behind "
               "an accredited link is not adjacent to the cardinal span, so the "
               "pure authority cannot reach it; closing it belongs to the "
               "contract's dimension accounting, not to another word list.",
    )),
    "5 proveedores arriba",
    "top 5 proveedores aproximadamente",
    "los 5 principales proveedores mas o menos",
])
def test_unmodelled_right_continuation_never_degrades_to_exact(text):
    evidence = _evidence(text)
    assert evidence.authorization is Authorization.NOT_ACCREDITED
    assert evidence.cardinality.relation is not CardinalityRelation.EXACT


@pytest.mark.parametrize("text", ["unos 5 proveedores", "unas 5 contratistas"])
def test_approximating_determiners_are_not_transparent(text):
    """``unos 5`` is "about 5"; it must not ride the determiner skip."""
    evidence = _evidence(text)
    assert evidence.authorization is Authorization.NOT_ACCREDITED
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN


def test_unaccredited_context_is_named_as_its_own_reason():
    evidence = _evidence("superior a 5 proveedores")
    assert evidence.cardinality.residue_reason is (
        CardinalityResidueReason.UNACCREDITED_CONTEXT
    )
    assert evidence.cardinality.residue_spans
    assert evidence.complete is False


# The complement: what the gate must keep executing.  These are the closed
# positive forms plus the request openers observed in the dogfood corpus.
@pytest.mark.parametrize("text", [
    "5 proveedores",
    "top 5 proveedores",
    "exactamente 5 proveedores",
    "los 5 principales proveedores",
    "los 5 proveedores",
    "cuales son los 5 proveedores con mas contratos en la alcaldia de barranquilla",
    "muestrame los 5 principales proveedores",
    "dame los 5 proveedores de la alcaldia de cali",
    "quiero los 5 proveedores de barranquilla",
    "necesito 5 proveedores del idu",
    "busca los 5 proveedores de la gobernacion",
    "lista los 5 proveedores",
    "ver los 5 proveedores de la gobernacion",
    "en 2025 quiero ver 5 proveedores",
    "5 proveedores para el ano 2025",
    "5 proveedores con contratos de 500 millones",
    "5 proveedores en secop ii",
])
def test_accredited_context_still_executes(text):
    evidence = _evidence(text)
    assert evidence.authorization is Authorization.EXECUTABLE
    assert evidence.cardinality.relation is CardinalityRelation.EXACT
    assert evidence.cardinality.values == (5,)
    assert evidence.polarity is Polarity.POSITIVE


@pytest.mark.parametrize("text", [
    "proveedores",
    "los proveedores de la alcaldia de galapa",
    "proveedores con mas contratos en 2025",
    "sus proveedores",
])
def test_gate_does_not_reach_uncounted_mentions(text):
    """No quantifier means no cardinal span, so the gate must stay out."""
    evidence = _evidence(text)
    assert evidence.cardinality.relation is CardinalityRelation.NONE
    assert evidence.authorization is Authorization.EXECUTABLE


@pytest.mark.parametrize("text", ["፭ proveedores", "፭፮ proveedores"])
def test_non_decimal_digits_fail_closed_instead_of_raising(text):
    """``isdigit()`` does not imply ``int()``: Ethiopic digits satisfy one only."""
    evidence = _evidence(text)
    assert evidence.authorization is Authorization.NOT_ACCREDITED
    assert evidence.cardinality.relation is CardinalityRelation.UNKNOWN
    assert evidence.cardinality.residue_reason is (
        CardinalityResidueReason.UNKNOWN_QUANTITY
    )


@pytest.mark.parametrize("digits", ["٥", "५"])
def test_decimal_digits_of_other_scripts_still_resolve(digits):
    """Arabic-Indic and Devanagari five are decimal, so they keep their value."""
    evidence = _evidence(f"{digits} proveedores")
    assert evidence.cardinality.relation is CardinalityRelation.EXACT
    assert evidence.cardinality.values == (5,)
