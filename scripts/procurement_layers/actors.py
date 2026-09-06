"""Adapt an existing Actor Profile identity reading without linking supplier accounts."""
from app.core.actor_identity import IdentityMatch
from scripts.audits.relationship_foundation.identity_spine import actor_id
from scripts.procurement_layers.model import Node


def add_actor(graph, match, evidence):
    """Caller supplies the original source receipt; unknown types stay unknown."""
    if not isinstance(match, IdentityMatch):
        raise ValueError("Actor requires an existing IdentityMatch")
    if not match.source or match.source != evidence.source:
        raise ValueError("Actor reading and evidence must identify the same source")
    if match.key is None:
        graph.issue("ACTOR_IDENTITY_UNAVAILABLE", {"source": match.source,
                                                  "reason_codes": list(match.reason_codes)})
        return None
    identifier = actor_id(match.key)
    return graph.add_node(Node("ACTOR", identifier, f"{match.key.document_type} {match.key.base_number}"),
                          evidence, {"raw_value": match.raw_value, "check_digit": match.check_digit,
                                     "confidence": match.confidence, "reason_codes": list(match.reason_codes),
                                     "legal_certification": False})
