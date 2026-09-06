"""Admission to name resolution is distinct from authority to execute SECOP."""
from app.core.actor_conversation import ActorConversationOperation
from app.core.actor_resolution import METHOD_NAME_UNRESOLVED, actor_resolution_method


def can_resolve_new_actor(params: dict, intent: str) -> bool:
    """Only a complete strong SET_ACTOR may cross the pre-resolution guard."""
    scope = params.get("actor_scope")
    return bool(
        intent in {"new_search", "set_actor"}
        and params.get("actor_conversation_operation") == ActorConversationOperation.SET_ACTOR
        and params.get("actor_conversation_construction") == "strong_actor_syntax"
        and actor_resolution_method(params) == METHOD_NAME_UNRESOLVED
        and isinstance(scope, dict)
        and scope.get("role") == "contractor"
        and scope.get("requires_resolution") is True
        and scope.get("resolution_method") == METHOD_NAME_UNRESOLVED
        and isinstance(scope.get("observed_name"), str)
        and scope["observed_name"].strip()
    )
