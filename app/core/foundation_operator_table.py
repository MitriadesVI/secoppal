"""Closed operator vocabulary of the IR v2, per dimension (C0 §5.1.3).

A condition's operator says what the user's words claim about the value (``is``, ``greater_than``,
``during_month`` ...). The table is data: the contract rejects a pair that is not here, so an operator can
never be invented for a dimension that does not know it. ``UNSUPPORTED`` keeps a free operator and its label;
that is how a producer names material it cannot map. ``RETRIEVAL_INTENT`` is written by the host only.

Where the design says «reserved» (``family`` for ``CONTRACT_TYPE_OR_NATURE``, D3) the table keeps what the host
already executes, so the v4.2 wire loses nothing when it is lifted (INV-C0-2).
"""

from __future__ import annotations

from types import MappingProxyType

from app.core.semantic_dimensions import SemanticDimension as D

OPERATORS: MappingProxyType = MappingProxyType({
    D.TOPIC: frozenset({"contains", "whole_token_folded"}),
    D.BUYER: frozenset({"is"}),
    D.ACTOR: frozenset({"is"}),
    D.GEOGRAPHY: frozenset({"is"}),
    D.TIME: frozenset({"during_year", "during_month", "between_dates", "relative_period"}),
    D.VALUE: frozenset({"greater_than", "greater_or_equal", "less_than", "less_or_equal"}),
    D.STATE: frozenset({"is", "family"}),
    D.MODALITY: frozenset({"is", "family"}),
    D.CONTRACT_TYPE_OR_NATURE: frozenset({"is", "family"}),
    D.ORDER: frozenset({"value_desc", "value_asc", "date_desc", "date_asc"}),
    D.CARDINALITY: frozenset({"top_n"}),
    D.RETRIEVAL_INTENT: frozenset({"is"}),
    # The v4.2 and v5 prompts never offer it; a producer cannot propose it.
    D.CONTROL_LANGUAGE: frozenset(),
})


def operator_allowed(dimension: D, operator: str) -> bool:
    """True when the dimension's closed table names this operator. UNSUPPORTED is not table-bound."""
    if dimension is D.UNSUPPORTED:
        return True
    return operator in OPERATORS.get(dimension, frozenset())
