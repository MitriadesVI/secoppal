"""Immutable presentation of applied filters, separate from actor identity."""
from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Literal


@dataclass(frozen=True, slots=True)
class ActorProfileAppliedScope:
    buyer_label: str | None = None
    buyer_scope_kind: Literal['exact', 'group', 'approximate'] | None = None
    buyer_match_labels: tuple[str, ...] = ()
    geography_labels: tuple[str, ...] = ()
    date_from: str | None = None
    date_to: str | None = None
    value_min: int | None = None
    value_max: int | None = None
    state_labels: tuple[str, ...] = ()
    modality_labels: tuple[str, ...] = ()
    excluded_modality_labels: tuple[str, ...] = ()
    contract_type_labels: tuple[str, ...] = ()
    topic_labels: tuple[str, ...] = ()
    excluded_topic_labels: tuple[str, ...] = ()

    def is_empty(self) -> bool:
        # Zero is a real monetary bound.
        return all(getattr(self, field.name) in (None, (), '') for field in fields(self))
