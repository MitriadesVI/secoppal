"""Typed fiscal attestations and source observations for ADR-073 Schema 3.

Neither record identifies a BuyerUnit. In particular, a directory NOT_FOUND
observation is not negative fiscal evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Literal


OBSERVATION_OUTCOMES = frozenset({
    "FOUND_UNIQUE", "FOUND_MULTI", "NOT_FOUND", "PARTIAL",
    "INCONCLUSIVE", "NO_OBSERVED_NIT",
})


@dataclass(frozen=True, slots=True)
class CheckDigitEvidence:
    declared: str | None
    computed: int | None
    split_validated: bool


@dataclass(frozen=True, slots=True)
class FiscalAccreditation:
    scheme: str
    base_number: str
    raw_forms: tuple[str, ...]
    check_digit: CheckDigitEvidence
    shared: bool
    source: str
    observed_at: date
    evidence: str
    kind: Literal["ACCREDITATION"] = "ACCREDITATION"


@dataclass(frozen=True, slots=True)
class SourceObservation:
    scheme: str
    base_number: str
    raw_forms: tuple[str, ...]
    check_digit: CheckDigitEvidence
    shared: bool
    source: str
    observed_at: date
    evidence: str
    outcome: str
    outcome_note: str | None
    kind: Literal["SOURCE_OBSERVATION"] = "SOURCE_OBSERVATION"


FiscalEvidence = FiscalAccreditation | SourceObservation


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} debe ser un string no vacío")
    return value


def _date(value: Any, field: str) -> date:
    text = _text(value, field)
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        raise ValueError(f"{field} debe tener formato YYYY-MM-DD")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} no es una fecha válida") from exc


def parse_fiscal_evidence(raw: Any, canonical: str) -> tuple[FiscalEvidence, ...]:
    """Parse a heterogeneous JSON list into disjoint, explicit record types."""
    if not isinstance(raw, list):
        raise ValueError(f"{canonical}: fiscal_evidence debe ser una lista (puede estar vacía)")
    parsed: list[FiscalEvidence] = []
    for index, item in enumerate(raw):
        field = f"{canonical}/fiscal_evidence[{index}]"
        if not isinstance(item, dict):
            raise ValueError(f"{field} debe ser un objeto")
        kind = _text(item.get("kind"), f"{field}/kind")
        scheme = _text(item.get("scheme"), f"{field}/scheme")
        base = _text(item.get("base_number"), f"{field}/base_number")
        if not base.isdigit():
            raise ValueError(f"{field}/base_number debe ser numérico")
        forms = item.get("raw_forms")
        if not isinstance(forms, list) or not forms:
            raise ValueError(f"{field}/raw_forms debe ser una lista no vacía")
        forms = tuple(_text(form, f"{field}/raw_forms") for form in forms)
        check = item.get("check_digit")
        if not isinstance(check, dict):
            raise ValueError(f"{field}/check_digit debe ser un objeto")
        declared = check.get("declared")
        computed = check.get("computed")
        validated = check.get("split_validated")
        if declared is not None and (not isinstance(declared, str) or not declared.isdigit()):
            raise ValueError(f"{field}/check_digit.declared inválido")
        if computed is not None and (isinstance(computed, bool) or not isinstance(computed, int)):
            raise ValueError(f"{field}/check_digit.computed inválido")
        if not isinstance(validated, bool):
            raise ValueError(f"{field}/check_digit.split_validated debe ser booleano")
        shared = item.get("shared")
        if not isinstance(shared, bool):
            raise ValueError(f"{field}/shared debe ser booleano")
        common = dict(
            scheme=scheme, base_number=base, raw_forms=forms,
            check_digit=CheckDigitEvidence(declared, computed, validated),
            shared=shared, source=_text(item.get("source"), f"{field}/source"),
            observed_at=_date(item.get("observed_at"), f"{field}/observed_at"),
            evidence=_text(item.get("evidence"), f"{field}/evidence"),
        )
        if kind == "ACCREDITATION":
            if "outcome" in item or "outcome_note" in item:
                raise ValueError(f"{field}: una acreditación no lleva outcome")
            parsed.append(FiscalAccreditation(**common))
        elif kind == "SOURCE_OBSERVATION":
            outcome = _text(item.get("outcome"), f"{field}/outcome")
            if outcome not in OBSERVATION_OUTCOMES:
                raise ValueError(f"{field}/outcome desconocido: {outcome}")
            note = item.get("outcome_note")
            if note is not None:
                note = _text(note, f"{field}/outcome_note")
            parsed.append(SourceObservation(**common, outcome=outcome, outcome_note=note))
        else:
            raise ValueError(f"{field}/kind desconocido: {kind}")
    return tuple(parsed)
