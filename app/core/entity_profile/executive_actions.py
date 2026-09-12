"""Server-owned catalogue for published Frame Entity Profile actions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET


PROFILE_ACTION_CATALOG_VERSION = 1


@dataclass(frozen=True, slots=True)
class ExecutiveAction:
    action_id: str
    label: str
    phrase: str
    required_sources: frozenset[str]
    any_source: bool = False

    def available_for(self, sources: frozenset[str]) -> bool:
        return bool(sources) if self.any_source else self.required_sources <= sources

    def presentation(self) -> dict[str, str]:
        return {"id": self.action_id, "label": self.label}


_ACTIONS = (
    ExecutiveAction(
        "suppliers", "Proveedores", "Muéstrame sus principales proveedores",
        frozenset({CONTRACT_DATASET}),
    ),
    ExecutiveAction(
        "modalities", "Modalidades", "Muéstrame sus modalidades",
        frozenset({PROCESS_DATASET}),
    ),
    ExecutiveAction(
        "temporal", "Evolución", "Muéstrame su evolución",
        frozenset(), any_source=True,
    ),
    ExecutiveAction(
        "recent_contracts", "Contratos recientes",
        "Muéstrame sus contratos recientes", frozenset({CONTRACT_DATASET}),
    ),
)
_BY_ID = {action.action_id: action for action in _ACTIONS}


def action_for_id(action_id: object) -> ExecutiveAction | None:
    return _BY_ID.get(action_id) if type(action_id) is str else None


def actions_for_sources(sources: Iterable[str]) -> tuple[ExecutiveAction, ...]:
    accredited = frozenset(source for source in sources if type(source) is str)
    return tuple(action for action in _ACTIONS if action.available_for(accredited))


def project_actions(sources: Iterable[str]) -> list[dict[str, str]]:
    return [action.presentation() for action in actions_for_sources(sources)]


def presented_action_ids(raw: object) -> tuple[str, ...]:
    """Validate a complete display list; malformed controls disappear."""
    if type(raw) is not list:
        return ()
    ids: list[str] = []
    for item in raw:
        if type(item) is not dict or set(item) != {"id", "label"}:
            return ()
        action = action_for_id(item.get("id"))
        if action is None or item.get("label") != action.label or action.action_id in ids:
            return ()
        ids.append(action.action_id)
    return tuple(ids)


__all__ = [
    "ExecutiveAction",
    "PROFILE_ACTION_CATALOG_VERSION",
    "action_for_id",
    "actions_for_sources",
    "presented_action_ids",
    "project_actions",
]
