"""Bounded lookup-time equivalence for punctuated Colombian legal forms.

The canonical N2 index is intentionally unchanged.  These helpers only
generate deterministic *tail* alternatives for the AP3-F challenger and, if
the challenger proves safe, the exact resolver.  No mid-name token rewriting
or legal-form removal is allowed.
"""
from __future__ import annotations

from app.core.actor_name import normalize_actor_name

__all__ = ["LEGAL_FORM_EQUIVALENCES", "legal_form_variants"]


LEGAL_FORM_EQUIVALENCES: tuple[tuple[str, str], ...] = (
    ("s a s", "sas"),
    ("e s e", "ese"),
    ("s a", "sa"),
)


def legal_form_variants(value: str | None) -> tuple[str, ...]:
    """Return deterministic N2 alternatives for a final legal-form token."""
    normalized = normalize_actor_name(value)
    if not normalized:
        return ()
    variants: set[str] = set()
    for spaced, compact in LEGAL_FORM_EQUIVALENCES:
        if normalized == spaced:
            variants.add(compact)
        elif normalized.endswith(f" {spaced}"):
            variants.add(f"{normalized[:-(len(spaced) + 1)]} {compact}")
        if normalized == compact:
            variants.add(spaced)
        elif normalized.endswith(f" {compact}"):
            variants.add(f"{normalized[:-(len(compact) + 1)]} {spaced}")
    variants.discard(normalized)
    return tuple(sorted(variants))
