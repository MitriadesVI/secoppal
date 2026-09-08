"""Mention-local buyer authority, independent of the supplier directory."""
from collections.abc import Mapping

from app.core.actor_name import normalize_actor_name


def _without_article(value: str) -> str:
    words = normalize_actor_name(value).split()
    if words and words[0] in {"el", "la", "los", "las"}:
        words = words[1:]
    return " ".join(words)


def buyer_owns_complete_mention(mention: str, params: Mapping) -> bool:
    """Only the full, confirmed buyer surface can defeat an optional Actor.

    Rewrites retain both the consumed surface and the identity method. A
    gazetteer scan retains its matched alias in entidad. Neither a fuzzy hit,
    city promotion, nor a buyer from a different mention grants this authority.
    Callers must keep explicit contractor roles on their existing path.
    """
    resolution = params.get("entidad_resolution")
    if not isinstance(resolution, Mapping):
        return False
    if (
        resolution.get("kind") != "buyer_entity"
        or resolution.get("confidence") != "high"
        or not resolution.get("value")
        or resolution["value"] != params.get("entidad_resolved")
        or resolution.get("clarification_needed")
    ):
        return False
    method = resolution.get("method", "")
    metadata = resolution.get("metadata") or {}
    if not isinstance(metadata, Mapping):
        return False
    if str(method).startswith("rewrite_"):
        if metadata.get("resolution_method") not in {"exact", "root_alias_evidence"}:
            return False
        surface = metadata.get("rewritten_from")
    elif method in {"exact", "scan_exact"}:
        surface = params.get("entidad")
    else:
        return False
    return bool(
        isinstance(surface, str)
        and _without_article(mention)
        and _without_article(mention) == _without_article(surface)
    )
