"""Public query-plan value type, separate from execution policy."""
from __future__ import annotations

from dataclasses import dataclass, field

from app.core.dialogue_policy import DialogueSignal


@dataclass
class QueryPlan:
    """Result of compute_query_plan. Dict-serializable via dataclasses.asdict."""

    should_execute: bool
    params: dict  # modified params (deep copy, never mutates input)
    missing_slots: list[str] = field(default_factory=list)
    risk_flags: list[str] = field(default_factory=list)
    clarification_question: str | None = None
    reason: str = ""  # "command_incomplete", "valid_query", ...
    signals: list[DialogueSignal] = field(default_factory=list)

