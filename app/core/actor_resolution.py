"""Contrato puro de resolución de actor entre routing, plan y ejecución — AP3-C.

El índice responde *qué ocurrió* (``actor_resolution_status``) y la
procedencia responde *por qué una identidad es autoritativa*
(``actor_resolution_method``).  Mantener ambas dimensiones separadas evita
elevar una sugerencia léxica a identidad exacta durante un follow-up.

Este módulo no abre SQLite, no consulta red y no decide intención.  Proyecta
resultados ya obtenidos y ofrece el único predicado de autoridad que consumen
P17, Actor Profile y los futuros consumidores.
"""
from __future__ import annotations

from typing import Any

from app.core.actor_identity import IdentityKey, compute_check_digit
from app.core.actor_name import (
    ActorResolutionCandidate,
    ActorResolutionResult,
    ActorResolutionStatus,
)

__all__ = [
    "ACTOR_PENDING_VERSION",
    "AUTHORITATIVE_ACTOR_METHODS",
    "METHOD_EXACT_UNIQUE_ALIAS",
    "METHOD_EXPLICIT_DOCUMENT",
    "METHOD_NAME_UNRESOLVED",
    "METHOD_USER_SELECTED_CANDIDATE",
    "actor_resolution_method",
    "actor_resolution_status",
    "apply_actor_identity",
    "candidate_from_payload",
    "candidate_to_payload",
    "is_authoritative_actor_resolution",
    "project_actor_resolution",
]


METHOD_EXPLICIT_DOCUMENT = "explicit_document"
METHOD_EXACT_UNIQUE_ALIAS = "exact_unique_alias"
METHOD_USER_SELECTED_CANDIDATE = "user_selected_candidate"
METHOD_NAME_UNRESOLVED = "name_unresolved"

AUTHORITATIVE_ACTOR_METHODS = frozenset({
    METHOD_EXPLICIT_DOCUMENT,
    METHOD_EXACT_UNIQUE_ALIAS,
    METHOD_USER_SELECTED_CANDIDATE,
})

ACTOR_PENDING_VERSION = 1


def actor_resolution_method(params: dict) -> str | None:
    """Return the canonical method, failing closed on a divergent legacy mirror."""
    canonical = params.get("actor_resolution_method")
    legacy = params.get("actor_resolution")
    if canonical and legacy and canonical != legacy:
        return None
    value = canonical or legacy
    return str(value) if value else None


def actor_resolution_status(params: dict) -> str | None:
    value = params.get("actor_resolution_status")
    return str(value) if value else None


def is_authoritative_actor_resolution(params: dict) -> bool:
    """Single authority gate for actor identity consumers.

    Legacy AP2 frames did not carry a separate status; they remain valid when
    their method is ``explicit_document``.  New frames must explicitly be
    ``RESOLVED``.  Any intent conflict or pending selection fails closed.
    """
    method = actor_resolution_method(params)
    status = actor_resolution_status(params)
    if params.get("actor_intent_conflict") or params.get("actor_pending"):
        return False
    if method not in AUTHORITATIVE_ACTOR_METHODS:
        return False
    if status not in (None, ActorResolutionStatus.RESOLVED):
        return False
    scope = params.get("actor_scope")
    if not isinstance(scope, dict):
        # Backward compatibility for persisted AP2 frames/tests written before
        # actor_scope carried the typed key.  New AP3-C writers always include
        # canonical method/status + scope, so this narrow exception cannot
        # authorize a newly-created fuzzy/name resolution.
        return bool(
            method == METHOD_EXPLICIT_DOCUMENT
            and not params.get("actor_resolution_method")
            and status is None
            and params.get("contratista")
        )
    document_type = scope.get("document_type")
    base_number = scope.get("base_number")
    return bool(base_number) and (
        document_type in {"NIT", "CC"}
        or (method == METHOD_EXPLICIT_DOCUMENT and document_type == "UNKNOWN")
    )


def _document_variants(identity: IdentityKey) -> list[str]:
    if identity.document_type == "CC":
        return [identity.base_number]
    if identity.document_type != "NIT":
        return []
    check_digit = compute_check_digit(identity.base_number)
    if check_digit is None:
        return [identity.base_number]
    dv = str(check_digit)
    return [identity.base_number, f"{identity.base_number}{dv}", f"{identity.base_number}-{dv}"]


def apply_actor_identity(
    params: dict,
    identity: IdentityKey,
    *,
    method: str,
    observed_name: str | None = None,
) -> dict:
    """Return a new frame with an authoritative actor scope and provenance."""
    if method not in AUTHORITATIVE_ACTOR_METHODS:
        raise ValueError(f"non-authoritative actor method: {method!r}")
    variants = _document_variants(identity)
    if not variants:
        raise ValueError("only typed NIT/CC identities can become actor scope")

    projected = dict(params)
    projected.pop("actor_pending", None)
    projected.pop("actor_intent_conflict", None)
    projected["actor_resolution_status"] = ActorResolutionStatus.RESOLVED
    projected["actor_resolution_method"] = method
    # Compatibility mirror for AP2 consumers.  It is method-only and every
    # writer in AP3-C updates it atomically with the canonical field.
    projected["actor_resolution"] = method
    projected["contratista"] = variants
    scope: dict[str, Any] = {
        "role": "contractor",
        "resolution_method": method,
        "requires_resolution": False,
        "document_type": identity.document_type,
        "base_number": identity.base_number,
    }
    if observed_name:
        scope["observed_name"] = observed_name
    projected["actor_scope"] = scope
    return projected


def candidate_to_payload(candidate: ActorResolutionCandidate) -> dict:
    """JSON-safe internal payload; masking is applied only when rendered."""
    return {
        "document_type": candidate.identity.document_type,
        "base_number": candidate.identity.base_number,
        "matched_alias": candidate.matched_alias,
        "observed_name": candidate.observed_name,
        "rank": candidate.rank,
        "occurrences": candidate.occurrences,
    }


def candidate_from_payload(payload: dict) -> tuple[IdentityKey, str | None]:
    identity = IdentityKey(
        document_type=str(payload["document_type"]),  # type: ignore[arg-type]
        base_number=str(payload["base_number"]),
    )
    name = payload.get("observed_name")
    return identity, str(name) if name else None


def project_actor_resolution(
    params: dict,
    result: ActorResolutionResult,
    *,
    actor_name: str,
    original_query: str,
    origin: str,
) -> dict:
    """Project a resolver result without confusing candidates with authority."""
    if result.status == ActorResolutionStatus.RESOLVED:
        assert result.identity is not None
        projected = apply_actor_identity(
            params,
            result.identity,
            method=METHOD_EXACT_UNIQUE_ALIAS,
            observed_name=actor_name,
        )
        if origin == "bare_exact_probe":
            # The baseline parser necessarily read bare X as a topic.  Once an
            # exact unique identity wins arbitration, X is actor mention, not
            # contract object; dates, amounts and ordering stay untouched.
            projected.pop("objeto", None)
            projected.pop("objeto_phrases", None)
        return projected

    projected = dict(params)
    projected["actor_resolution_status"] = result.status
    projected["actor_resolution_method"] = result.method or METHOD_NAME_UNRESOLVED
    projected["actor_resolution"] = result.method or METHOD_NAME_UNRESOLVED

    if result.status in {ActorResolutionStatus.AMBIGUOUS, ActorResolutionStatus.CANDIDATES}:
        original_params = dict(params)
        original_params.pop("actor_pending", None)
        projected["actor_pending"] = {
            "version": ACTOR_PENDING_VERSION,
            "origin": origin,
            "original_query": original_query,
            "original_params": original_params,
            "actor_name": actor_name,
            "resolution_status": result.status,
            "candidates": [candidate_to_payload(candidate) for candidate in result.candidates],
        }
    else:
        projected.pop("actor_pending", None)
    return projected
