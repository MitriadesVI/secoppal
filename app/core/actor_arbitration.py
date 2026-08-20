"""Pure arbitration helpers for AP3-C's two actor-name doors."""
from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.actor_name import normalize_actor_name
from app.data.morphological_variants import MORPHOLOGICAL_VARIANTS

__all__ = [
    "BareActorProbe",
    "detect_bare_actor_probe",
    "has_strong_topic_evidence",
    "scrub_strong_actor_leaks",
]


@dataclass(frozen=True, slots=True)
class BareActorProbe:
    actor_name: str


_BARE_ACTOR_RE = re.compile(r"\bcontratos?\s+de\s+(.+)$", re.IGNORECASE)
_TAIL_MODIFIER_RE = re.compile(
    r"\s+(?:en|por|desde|hasta|entre|durante|mayor(?:es)?|menor(?:es)?|"
    r"mas|menos|con|sobre)\b.*$",
    re.IGNORECASE,
)
_REJECTED_LEADS = frozenset({"el", "la", "los", "las", "un", "una", "del", "de"})

_KNOWN_TOPIC_TERMS = frozenset(
    normalize_actor_name(value)
    for value in (*MORPHOLOGICAL_VARIANTS.keys(), *MORPHOLOGICAL_VARIANTS.values())
    if value
)
_KNOWN_TOPIC_TOKENS = frozenset(
    term for term in _KNOWN_TOPIC_TERMS if " " not in term and len(term) >= 4
)


def detect_bare_actor_probe(raw_query: str, params: dict) -> BareActorProbe | None:
    """Extract X only from bare ``contratos de X`` syntax.

    Strong role syntax is excluded by the caller before reaching this helper.
    Temporal, monetary and ordering tails are not part of the actor mention.
    """
    if params.get("dataset") != "contratos":
        return None
    normalized = normalize_actor_name(raw_query)
    match = _BARE_ACTOR_RE.search(normalized)
    if match is None:
        return None
    candidate = _TAIL_MODIFIER_RE.sub("", match.group(1)).strip()
    words = candidate.split()
    if not words or len(words) > 8 or words[0] in _REJECTED_LEADS:
        return None
    if candidate.isdigit():
        return None
    return BareActorProbe(actor_name=candidate)


def has_strong_topic_evidence(actor_name: str, params: dict) -> bool:
    """Whether baseline semantics independently recognize X as a known topic."""
    normalized = normalize_actor_name(actor_name)
    if normalized in _KNOWN_TOPIC_TERMS:
        return True
    if set(normalized.split()) & _KNOWN_TOPIC_TOKENS:
        return True

    topic = params.get("objeto")
    values: list[str] = []
    if isinstance(topic, list):
        for value in topic:
            if isinstance(value, list):
                values.extend(str(item) for item in value)
            else:
                values.append(str(value))
    return any(normalize_actor_name(value) in _KNOWN_TOPIC_TERMS for value in values)


def scrub_strong_actor_leaks(params: dict, actor_name: str) -> dict:
    """Remove actor-name fragments consumed too late by baseline extractors.

    QueryRouter detects geography before the AP2 name marker and may therefore
    read ``CARIBE`` in ``proveedor FUNDACIÓN CARIBE`` as a region, leaving
    ``fundación`` as topic.  This projection uses the already-detected mention
    boundary: only values contained in the actor name are removed, so an
    explicit tail such as ``proveedor X en Bolívar`` remains intact.
    """
    clean = dict(params)
    name = normalize_actor_name(actor_name)
    name_tokens = set(name.split())

    topic = clean.get("objeto")
    if isinstance(topic, list):
        kept = []
        for value in topic:
            if isinstance(value, list):
                nested = [
                    item for item in value
                    if not _value_is_part_of_name(item, name, name_tokens)
                ]
                if nested:
                    kept.append(nested if len(nested) > 1 else nested[0])
            elif not _value_is_part_of_name(value, name, name_tokens):
                kept.append(value)
        if kept:
            clean["objeto"] = kept
        else:
            clean.pop("objeto", None)

    if _value_is_part_of_name(clean.get("region"), name, name_tokens):
        clean.pop("region", None)
        clean.pop("departamentos_in", None)
    for key, companions in {
        "ciudad": ("ciudad_resolved",),
        "departamento": ("departamento_resolved", "departamento_resolution"),
        "departamento_resolved": ("departamento", "departamento_resolution"),
    }.items():
        if _value_is_part_of_name(clean.get(key), name, name_tokens):
            clean.pop(key, None)
            for companion in companions:
                clean.pop(companion, None)
    return clean


def _value_is_part_of_name(value: object, name: str, name_tokens: set[str]) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    normalized = normalize_actor_name(value).replace("_", " ")
    if not normalized:
        return False
    return normalized in name or set(normalized.split()) <= name_tokens
