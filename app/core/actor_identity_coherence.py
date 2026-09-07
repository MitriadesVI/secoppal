"""INV-ACTOR-SINGLE-IDENTITY — one executable frame, one actor identity.

The frame carries the actor twice on purpose, and the two copies answer
different questions:

* ``actor_scope`` says **who** the actor is — a typed ``IdentityKey`` plus the
  provenance that made it authoritative;
* ``contratista`` says **how that identity is filtered** in SECOP — the
  published document forms of the very same person.

They are a projection of one fact, so they cannot disagree. When they do, the
query still executes and still returns rows: it filters by one person while
every consumer that reads the scope — the profile header, the follow-up frame,
the trace — names the other. Nothing in the answer looks wrong.

This module is the seat belt, not the fix. A divergence means an upstream
merge wrote half an identity, and that merge is what has to be corrected; the
gate exists so the incorrect frame never reaches the source in the meantime.

Pure: no clock, no store, no retrieval. It only re-derives the projection from
the same authority that produced it (``actor_resolution.document_variants``)
and compares.
"""
from __future__ import annotations

from app.core.actor_identity import IdentityKey
from app.core.actor_resolution import document_variants

__all__ = [
    "ACTOR_IDENTITY_SPLIT_BRAIN",
    "ACTOR_IDENTITY_SPLIT_BRAIN_QUESTION",
    "actor_identity_split_brain",
]


ACTOR_IDENTITY_SPLIT_BRAIN = "ACTOR_IDENTITY_SPLIT_BRAIN"
ACTOR_IDENTITY_SPLIT_BRAIN_QUESTION = (
    "Tengo dos identidades distintas para el contratista en esta consulta y "
    "no puedo elegir por ti. Repite la búsqueda indicando un solo documento."
)

_TYPED_DOCUMENTS = frozenset({"CC", "NIT"})


def actor_identity_split_brain(params: dict) -> bool:
    """True when ``contratista`` projects an identity ``actor_scope`` denies.

    Only a *typed* scope can be checked: a name-unresolved scope has no
    document to project, and a legacy frame without a scope has nothing to
    contradict.  A ``contratista`` narrower than the full variant set is not a
    divergence — the AP2 door emits only the forms the user actually declared.
    """
    scope = params.get("actor_scope")
    if not isinstance(scope, dict):
        return False
    document_type = scope.get("document_type")
    base_number = scope.get("base_number")
    if document_type not in _TYPED_DOCUMENTS or not base_number:
        return False
    projected = params.get("contratista")
    if not isinstance(projected, (list, tuple)) or not projected:
        return False
    try:
        identity = IdentityKey(
            document_type=str(document_type),  # type: ignore[arg-type]
            base_number=str(base_number),
        )
    except (TypeError, ValueError):
        return False
    accepted = set(document_variants(identity))
    return any(str(value) not in accepted for value in projected)
