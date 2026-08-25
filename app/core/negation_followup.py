"""Bounded follow-up mutations for persisted dimensional exclusions.

OBJECT exclusions use the existing ``objeto_exclude`` authority.  Department
subtraction is materialized as ``departamentos_in``; reinclusion is safe only
while the originating region is still present as provenance.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Literal

from app.core.object_negation_policy import object_negation_overlap
from app.data.colombia_geography import ZONES_BY_NAME


ADD_EXCLUSION = "ADD_EXCLUSION"
REMOVE_EXCLUSION = "REMOVE_EXCLUSION"
REPLACE_EXCLUSION = "REPLACE_EXCLUSION"
REINCLUDE = "REINCLUDE"

_Operation = Literal[
    "ADD_EXCLUSION",
    "REMOVE_EXCLUSION",
    "REPLACE_EXCLUSION",
    "REINCLUDE",
]


@dataclass(frozen=True, slots=True)
class ExclusionMutation:
    operation: _Operation
    requested: tuple[str, ...]
    resulting: tuple[str, ...]
    should_apply: bool = True
    clarification: str | None = None
    reason: str = "mutation_applied"

    def trace(self, previous: list[object]) -> dict[str, object]:
        return {
            "operation": self.operation,
            "previous_exclusions": list(previous),
            "requested_exclusions": list(self.requested),
            "resulting_exclusions": list(self.resulting),
            "reason": self.reason,
            "no_op": tuple(previous) == self.resulting,
        }


@dataclass(frozen=True, slots=True)
class DepartmentReincludeMutation:
    requested_department: str
    resulting_departments: tuple[str, ...]
    should_apply: bool
    clarification: str | None = None
    reason: str = "department_reinclude_applied"

    def trace(self, previous: list[str], region: object) -> dict[str, object]:
        return {
            "operation": REINCLUDE,
            "dimension": "DEPARTMENT",
            "region_provenance": region,
            "previous_departments": list(previous),
            "requested_department": self.requested_department,
            "resulting_departments": list(self.resulting_departments),
            "reason": self.reason,
            "no_op": tuple(previous) == self.resulting_departments,
        }


_CLEAR_SINGLE_RE = re.compile(
    r"^(?:ahora\s+)?(?:quita|elimina|borra|retira)\s+la\s+exclusion$"
)
_CLEAR_ALL_RE = re.compile(
    r"^(?:ahora\s+)?(?:quita|elimina|borra|retira)\s+"
    r"(?:(?:esas|estas|las|todas\s+las)\s+)?exclusiones$"
)
_AMBIGUOUS_REFERENT_RE = re.compile(
    r"^(?:ahora\s+)?(?:quita|elimina|borra|retira)\s+(?:eso|esto)$"
)
_REMOVE_TARGET_RES = (
    re.compile(
        r"^(?:ahora\s+)?(?P<verb>quita|elimina|borra|retira|saca)\s+"
        r"(?:(?P<explicit>la\s+exclusion\s+de|la\s+exclusion|"
        r"exclusion\s+de|lo\s+de)\s+)?(?P<target>.+)$"
    ),
    re.compile(
        r"^(?P<explicit>ya\s+no\s+excluyas|no\s+excluyas|"
        r"deja\s+de\s+excluir)\s+(?P<target>.+)$"
    ),
)
_REINCLUDE_TARGET_RE = re.compile(
    r"^(?:(?:ahora\s+)?(?:incluye|permite)|vuelve\s+a\s+incluir)\s+"
    r"(?P<target>.+)$"
)
_REPLACE_RE = re.compile(r"^(?:mejor\s+)(?:sin|excepto|salvo|excluyendo)\b")
_SINO_RE = re.compile(r"\bsino(?:\s+tambien)?\b")
_AMBIGUOUS_EXCEPT_RE = re.compile(
    r"^(?:excepto|salvo)\s+.+,\s*mejor\s+.+$"
)
_DEPARTMENT_REINCLUDE_RE = re.compile(
    r"^(?:(?:ahora\s+)?(?:incluye|permite)|vuelve\s+a\s+incluir)\s+.+$"
)


def _plain(text: str) -> str:
    folded = unicodedata.normalize("NFKD", text.casefold())
    folded = "".join(char for char in folded if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", folded).strip(" \t\r\n¿?¡!.,;:")


def requests_object_exclusion_clear(text: str) -> bool:
    """Return whether the surface requests removal of a negative topic slot."""
    norm = _plain(text)
    return bool(
        _CLEAR_SINGLE_RE.fullmatch(norm)
        or _CLEAR_ALL_RE.fullmatch(norm)
        or _AMBIGUOUS_REFERENT_RE.fullmatch(norm)
        or _REINCLUDE_TARGET_RE.fullmatch(norm)
        or any(pattern.fullmatch(norm) for pattern in _REMOVE_TARGET_RES)
    )


def is_object_exclusion_replace(text: str) -> bool:
    norm = _plain(text)
    return bool(_REPLACE_RE.search(norm) or _SINO_RE.search(norm))


def _matches(target: str, exclusion: object) -> bool:
    if not isinstance(exclusion, str) or not exclusion:
        return False
    return bool(object_negation_overlap({
        "objeto": [target],
        "objeto_exclude": [exclusion],
    }))


def _matching_exclusions(target: str, previous: list[object]) -> tuple[str, ...]:
    return tuple(
        exclusion
        for exclusion in previous
        if isinstance(exclusion, str) and _matches(target, exclusion)
    )


def _blocked_object(
    operation: _Operation,
    previous: list[object],
    *,
    requested: tuple[str, ...] = (),
    reason: str,
    clarification: str,
) -> ExclusionMutation:
    return ExclusionMutation(
        operation=operation,
        requested=requested,
        resulting=tuple(str(value) for value in previous),
        should_apply=False,
        clarification=clarification,
        reason=reason,
    )


def classify_object_exclusion_mutation(
    text: str,
    current_params: dict,
    previous_exclusions: list[object],
) -> ExclusionMutation | None:
    """Classify only exclusion-specific follow-ups; ordinary topic edits fall through."""
    norm = _plain(text)
    previous = list(previous_exclusions)

    if _AMBIGUOUS_REFERENT_RE.fullmatch(norm) and previous:
        return _blocked_object(
            REMOVE_EXCLUSION,
            previous,
            reason="ambiguous_exclusion_referent",
            clarification=(
                "No sé si quieres quitar el tema, otro filtro o una exclusión. "
                "Indica qué exclusión quieres retirar."
            ),
        )

    if _CLEAR_ALL_RE.fullmatch(norm):
        if not previous:
            return _blocked_object(
                REMOVE_EXCLUSION,
                previous,
                reason="exclusion_not_present",
                clarification="La búsqueda actual no tenía exclusiones para retirar.",
            )
        requested = tuple(str(value) for value in previous)
        return ExclusionMutation(REMOVE_EXCLUSION, requested, ())

    if _CLEAR_SINGLE_RE.fullmatch(norm):
        if not previous:
            return _blocked_object(
                REMOVE_EXCLUSION,
                previous,
                reason="exclusion_not_present",
                clarification="La búsqueda actual no tenía esa exclusión.",
            )
        if len(previous) != 1:
            return _blocked_object(
                REMOVE_EXCLUSION,
                previous,
                reason="ambiguous_exclusion_referent",
                clarification=(
                    "Hay varias exclusiones activas. Indica cuál quieres retirar "
                    "o di ‘quita esas exclusiones’."
                ),
            )
        requested = (str(previous[0]),)
        return ExclusionMutation(REMOVE_EXCLUSION, requested, ())

    for pattern in _REMOVE_TARGET_RES:
        match = pattern.fullmatch(norm)
        if match is None:
            continue
        target = match.group("target").strip()
        matches = _matching_exclusions(target, previous)
        if matches:
            resulting = tuple(value for value in previous if value not in matches)
            return ExclusionMutation(REMOVE_EXCLUSION, matches, resulting)
        if match.groupdict().get("explicit"):
            return _blocked_object(
                REMOVE_EXCLUSION,
                previous,
                requested=(target,),
                reason="exclusion_not_present",
                clarification=(
                    f"La búsqueda actual no tenía la exclusión ‘{target}’. "
                    "No cambié los filtros."
                ),
            )
        return None

    reinclude = _REINCLUDE_TARGET_RE.fullmatch(norm)
    if reinclude is not None:
        target = reinclude.group("target").strip()
        matches = _matching_exclusions(target, previous)
        if matches:
            resulting = tuple(value for value in previous if value not in matches)
            return ExclusionMutation(REINCLUDE, matches, resulting)
        return None

    requested = tuple(
        str(value) for value in current_params.get("objeto_exclude") or []
    )
    if not requested:
        return None
    if _AMBIGUOUS_EXCEPT_RE.fullmatch(norm):
        return _blocked_object(
            REPLACE_EXCLUSION,
            previous,
            requested=requested,
            reason="ambiguous_exclusion_replacement",
            clarification=(
                "No es claro si ‘mejor’ reemplaza el tema o la exclusión. "
                "Indica el tema a buscar y la exclusión por separado."
            ),
        )
    if is_object_exclusion_replace(text):
        return ExclusionMutation(REPLACE_EXCLUSION, requested, requested)
    resulting = tuple(dict.fromkeys([*previous, *requested]))
    return ExclusionMutation(ADD_EXCLUSION, requested, resulting)


def classify_department_reinclude(
    text: str,
    current_params: dict,
    previous_params: dict,
) -> DepartmentReincludeMutation | None:
    """Safely restore a department only from preserved region provenance."""
    if not _DEPARTMENT_REINCLUDE_RE.fullmatch(_plain(text)):
        return None
    previous = previous_params.get("departamentos_in")
    target = current_params.get("departamento_resolved")
    if not isinstance(previous, list) or not previous or not isinstance(target, str):
        return None
    if target in previous:
        return DepartmentReincludeMutation(
            requested_department=target,
            resulting_departments=tuple(previous),
            should_apply=False,
            clarification=f"{target} ya estaba incluido; no cambié el alcance.",
            reason="department_already_included",
        )

    region = previous_params.get("region")
    full_region = ZONES_BY_NAME.get(str(region), []) if region else []
    if (
        not full_region
        or target not in full_region
        or not set(previous).issubset(set(full_region))
    ):
        return DepartmentReincludeMutation(
            requested_department=target,
            resulting_departments=tuple(previous),
            should_apply=False,
            clarification=(
                "No conservo suficiente procedencia para reconstruir ese conjunto "
                "territorial con seguridad. Repite la región y los departamentos "
                "que quieres incluir."
            ),
            reason="department_reinclude_missing_provenance",
        )

    wanted = set(previous) | {target}
    resulting = tuple(department for department in full_region if department in wanted)
    return DepartmentReincludeMutation(
        requested_department=target,
        resulting_departments=resulting,
        should_apply=True,
    )
