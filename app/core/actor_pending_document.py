"""Decide whether a documentary reply *selects* from a pending actor list.

A pending candidate list can be answered in two ways, and they are not the
same speech act:

* ``"2"`` names a **position** — it carries no identity, so the pending list
  is the only authority that can turn it into an actor;
* ``"CC 70000003"`` names an **identity** — it is authoritative on its own,
  and it may or may not belong to the list that is on the table.

Confusing the second with a new search is what this module exists to prevent.
When the document identifies exactly one candidate of the list, the user is
answering the question that was asked, and the frame that produced the list
(year, buyer, value, topic) is still what they want filtered.  When it
identifies nobody, the pending list has nothing to say about it and the frame
must not follow.

Purity is the point: no clock, no store, no retrieval.  The caller supplies
the already-parsed reply and the pending payload; this module only compares
canonical ``IdentityKey`` values.  Names, ranks, similarity scores and raw
document strings are deliberately never consulted — an actor is selected by
who they are, not by how their row was ordered or spelled.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.actor_identity import IdentityKey
from app.core.actor_resolution import (
    METHOD_EXPLICIT_DOCUMENT,
    actor_resolution_method,
)
from app.core.contract_universe import ContractUniverse, POLICY_VERSION

__all__ = [
    "DOCUMENT_PENDING_INCOMPATIBLE",
    "DOCUMENT_PENDING_MATCH",
    "DOCUMENT_PENDING_NO_DOCUMENT",
    "DOCUMENT_PENDING_NO_MATCH",
    "DocumentPendingSelection",
    "explicit_document_identity",
    "match_pending_document",
]


DOCUMENT_PENDING_MATCH = "MATCH"
DOCUMENT_PENDING_NO_MATCH = "NO_MATCH"
DOCUMENT_PENDING_NO_DOCUMENT = "NO_DOCUMENT"
DOCUMENT_PENDING_INCOMPATIBLE = "INCOMPATIBLE"


# Everything an explicit-document reply legitimately writes into a frame.  The
# document promotes the dataset to ``contratos`` on its own, so those two keys
# belong here too. Apart from validated default universe metadata, another
# populated key means the reply asked for something else as well — a different
# year, a different dataset — and a selection would have to silently discard it.
_DOCUMENT_REPLY_KEYS = frozenset({
    "dataset",
    "dataset_explicit",
    "contratista",
    "actor_resolution",
    "actor_resolution_method",
    "actor_resolution_status",
    "actor_scope",
})


@dataclass(frozen=True, slots=True)
class DocumentPendingSelection:
    """What a documentary reply turned out to be, and whom it named."""

    outcome: str
    rank: int | None = None
    identity: IdentityKey | None = None
    observed_name: str | None = None


def _carries_content(value: object) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, (str, bytes, list, tuple, dict, set, frozenset)):
        return bool(value)
    return True


def _is_default_universe_metadata(key: str, value: object) -> bool:
    """Recognize only the policy's derived defaults, never source authority.

    Explicit universe requests also set dataset_explicit in apply_request;
    that guard and the content check for states/errors remain mandatory.
    Unknown versions and nondefault universes therefore fail closed.
    """
    return (
        key == "contract_universe" and value == ContractUniverse.CELEBRATED.value
    ) or (key == "contract_universe_version" and value == POLICY_VERSION)


def explicit_document_identity(params: dict) -> IdentityKey | None:
    """The identity a *document-only* reply established, or ``None``.

    The document grammar is not re-implemented here: ``params`` already went
    through the router, so the typed identity comes from the same authority
    that serves ``"contratos del NIT X"``.  This function only answers whether
    the reply was *nothing but* that document.
    """
    if not isinstance(params, dict):
        return None
    if params.get("dataset_explicit"):
        # The user asked for a specific dataset.  That is an intent of its own
        # and a pending replay would overwrite it with the original frame's.
        return None
    if actor_resolution_method(params) != METHOD_EXPLICIT_DOCUMENT:
        return None
    if any(
        _carries_content(value)
        for key, value in params.items()
        if key not in _DOCUMENT_REPLY_KEYS and not _is_default_universe_metadata(key, value)
    ):
        return None
    scope = params.get("actor_scope")
    if not isinstance(scope, dict):
        return None
    try:
        return IdentityKey(
            document_type=str(scope.get("document_type")),  # type: ignore[arg-type]
            base_number=str(scope.get("base_number")),
        )
    except (TypeError, ValueError):
        return None


def match_pending_document(params: dict, candidates: object) -> DocumentPendingSelection:
    """Match a documentary reply against a pending list by canonical identity.

    ``INCOMPATIBLE`` is reserved for a corrupt pending that offers the same
    ``IdentityKey`` under two ranks.  Picking the first would be choosing for
    the user among options the system itself cannot tell apart.
    """
    identity = explicit_document_identity(params)
    if identity is None:
        return DocumentPendingSelection(DOCUMENT_PENDING_NO_DOCUMENT)

    matches: list[dict] = []
    for candidate in candidates if isinstance(candidates, list) else []:
        if not isinstance(candidate, dict):
            continue
        rank = candidate.get("rank")
        if not isinstance(rank, int) or isinstance(rank, bool) or rank < 1:
            continue
        try:
            key = IdentityKey(
                document_type=str(candidate["document_type"]),  # type: ignore[arg-type]
                base_number=str(candidate["base_number"]),
            )
        except (KeyError, TypeError, ValueError):
            continue
        if key == identity:
            matches.append(candidate)

    if not matches:
        return DocumentPendingSelection(DOCUMENT_PENDING_NO_MATCH, identity=identity)
    if len(matches) > 1:
        return DocumentPendingSelection(DOCUMENT_PENDING_INCOMPATIBLE, identity=identity)

    selected = matches[0]
    observed_name = selected.get("observed_name")
    return DocumentPendingSelection(
        DOCUMENT_PENDING_MATCH,
        rank=int(selected["rank"]),
        identity=identity,
        observed_name=str(observed_name) if observed_name else None,
    )
