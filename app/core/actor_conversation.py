"""Deterministic conversational algebra for Actor Profile V1.

This module recognizes *operations and roles*, never identities.  An actor
mention still goes through ``ActorNameResolver`` and a buyer mention still goes
through ``EntityResolver``.  Keeping this layer pure makes the same policy
available to Streamlit, Telegram, and every other workflow entrypoint.

Precedence is enforced by callers in this order:

1. pending candidate selection;
2. explicit ``SET_ACTOR``/``REPLACE_ACTOR``;
3. contextual ``SET_BUYER``/``SET_TERRITORY``;
4. the generic router.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from app.core.actor_name import normalize_actor_name
from app.data.morphological_variants import MORPHOLOGICAL_VARIANTS

__all__ = [
    "ActorConversationInstruction",
    "ActorConversationOperation",
    "actor_likeness",
    "interpret_actor_conversation",
    "parse_actor_candidate_selection",
    "purge_actor_state",
]


class ActorConversationOperation:
    SET_ACTOR = "SET_ACTOR"
    REPLACE_ACTOR = "REPLACE_ACTOR"
    SELECT_ACTOR_CANDIDATE = "SELECT_ACTOR_CANDIDATE"
    SET_BUYER = "SET_BUYER"
    CLEAR_BUYER = "CLEAR_BUYER"
    SET_TERRITORY = "SET_TERRITORY"
    CLEAR_TERRITORY = "CLEAR_TERRITORY"
    SET_TIME = "SET_TIME"
    SET_VALUE = "SET_VALUE"
    SET_MODALITY = "SET_MODALITY"
    SET_ORDERING = "SET_ORDERING"
    CLEAR_FILTER = "CLEAR_FILTER"
    RESET = "RESET"


ActorEvidence = Literal["strong", "independent", "probe", "topic"]


@dataclass(frozen=True, slots=True)
class ActorConversationInstruction:
    operation: str | None = None
    actor_mention: str | None = None
    buyer_mention: str | None = None
    territory_mention: str | None = None
    actor_evidence: ActorEvidence | None = None
    relation_buyer_span: str | None = None
    fallback_actor_mention: str | None = None
    fallback_actor_evidence: ActorEvidence | None = None
    framing_spans: tuple[str, ...] = ()
    construction: str = ""


_KNOWN_TOPICS = frozenset({
    normalize_actor_name(value)
    for value in (
        *MORPHOLOGICAL_VARIANTS.keys(),
        *MORPHOLOGICAL_VARIANTS.values(),
        "alimentacion", "mantenimiento", "software", "vigilancia",
        "nutricion", "adulto mayor", "transporte", "obras", "aseo",
        "salud", "ingenieria",
    )
    if value
})
_KNOWN_TOPIC_TOKENS = frozenset(
    token
    for topic in _KNOWN_TOPICS
    for token in topic.split()
    if len(token) >= 4
)

_LEGAL_FORM_RE = re.compile(
    r"(?:^|\s)(?:s\s*a\s*s|sas|s\s*a|sa|e\s*s\s*e|ese|ltda|limitada|"
    r"s\s*c\s*a|sca|e\s*s\s*p|esp)$",
    re.IGNORECASE,
)
_ORGANIZATION_MARKERS = frozenset({
    "agencia", "asociacion", "camara", "cooperativa", "corporacion",
    "empresa", "fundacion", "grupo", "hospital", "instituto", "ips",
    "sociedad", "universidad", "consorcio", "ese", "union", "ut",
})
_CONNECTORS = frozenset({
    "a", "al", "con", "de", "del", "el", "en", "la", "las", "los",
    "para", "por", "y",
})


def actor_likeness(mention: str, *, strong_syntax: bool = False) -> ActorEvidence:
    """Classify actor intent without consulting actor retrieval.

    ``probe`` means that only an exact directory lookup may establish actor
    evidence.  Fuzzy results are deliberately excluded from this decision.
    """
    normalized = normalize_actor_name(mention)
    if not normalized:
        return "topic"
    if normalized in _KNOWN_TOPICS:
        return "topic"
    tokens = normalized.split()
    if _LEGAL_FORM_RE.search(normalized):
        return "independent"
    if tokens and tokens[0] in _ORGANIZATION_MARKERS:
        return "independent"
    if strong_syntax:
        return "strong"

    substantive = [token for token in tokens if token not in _CONNECTORS]
    distinctive = [
        token for token in substantive
        if token not in _KNOWN_TOPIC_TOKENS
    ]
    if len(substantive) >= 2 and distinctive:
        return "independent"
    return "probe"


_SELECTION_NUMBER_RE = re.compile(
    r"^(?:(?:el|la)\s+|opci[oó]n\s+)?(?P<rank>\d+)$",
    re.IGNORECASE,
)
_ORDINAL_RANKS = {
    "primero": 1, "primera": 1,
    "segundo": 2, "segunda": 2,
    "tercero": 3, "tercera": 3,
    "cuarto": 4, "cuarta": 4,
    "quinto": 5, "quinta": 5,
    "sexto": 6, "sexta": 6,
    "septimo": 7, "septima": 7,
    "octavo": 8, "octava": 8,
    "noveno": 9, "novena": 9,
    "decimo": 10, "decima": 10,
}


def parse_actor_candidate_selection(text: str) -> int | None:
    """Return a natural V1 candidate rank, or ``None`` for another intent."""
    normalized = normalize_actor_name(text)
    number = _SELECTION_NUMBER_RE.fullmatch(normalized)
    if number is not None:
        return int(number.group("rank"))
    normalized = re.sub(r"^(?:el|la)\s+", "", normalized)
    return _ORDINAL_RANKS.get(normalized)


_RELATION_PATTERNS: tuple[tuple[str, re.Pattern[str], bool], ...] = (
    (
        "contracts_of_x_with_y",
        re.compile(
            r"\bcontratos?\s+(?:de|del)\s+(?P<actor>.+?)\s+con\s+"
            r"(?P<buyer>.+)$",
            re.IGNORECASE,
        ),
        False,
    ),
    (
        "contracts_held_by_x_with_y",
        re.compile(
            r"\bqu[eé]\s+contratos?\s+(?:tiene|ha\s+tenido|posee)\s+"
            r"(?P<actor>.+?)\s+con\s+(?P<buyer>.+)$",
            re.IGNORECASE,
        ),
        True,
    ),
    (
        "what_x_did_with_y",
        re.compile(
            r"\bqu[eé]\s+ha\s+(?:hecho|contratado|celebrado|suscrito|"
            r"ejecutado)\s+(?P<actor>.+?)\s+con\s+(?P<buyer>.+)$",
            re.IGNORECASE,
        ),
        True,
    ),
    (
        "history_of_x_with_y",
        re.compile(
            r"\bhistorial(?:\s+contractual)?\s+(?:de|del)\s+"
            r"(?P<actor>.+?)\s+con\s+(?P<buyer>.+)$",
            re.IGNORECASE,
        ),
        False,
    ),
    (
        "lo_de_x_with_y",
        re.compile(
            r"\blo\s+de\s+(?P<actor>.+?)\s+con\s+(?P<buyer>.+)$",
            re.IGNORECASE,
        ),
        False,
    ),
)

_LEADING_FRAMING_RE = re.compile(
    r"^(?P<span>(?:busca(?:me|te)?|mira(?:me)?|muestra(?:me)?|dime|"
    r"averigua|consulta|revisa)"
    r"(?:\s+(?:a|al))?"
    r"(?:\s+informaci[oó]n\s+de)?"
    r"(?:\s+(?:este|esta|el|la))?)\b",
    re.IGNORECASE,
)
_REPLACE_RE = re.compile(
    r"^(?:ahora\s+)?cambia(?:me)?\s+(?:al?|por)\s+(?P<actor>.+)$",
    re.IGNORECASE,
)
_SEARCH_BARE_RE = re.compile(
    r"^(?:busca(?:me|te)?|mira(?:me)?|consulta|revisa)\s+"
    r"(?P<actor>.+)$",
    re.IGNORECASE,
)
_NON_ACTOR_SEARCH_LEADS = frozenset({
    "compra", "compras", "contratacion", "contrato", "contratos",
    "licitacion", "oportunidad", "oportunidades", "proceso", "procesos",
})
_BARE_CONTRACTS_RE = re.compile(
    r"\bcontratos?\s+(?:de|del)\s+(?P<actor>.+)$",
    re.IGNORECASE,
)
_CONTEXT_BUYER_RE = re.compile(
    r"^(?:ahora\s+)?con\s+(?P<buyer>.+)$",
    re.IGNORECASE,
)
_CONTEXT_TERRITORY_RE = re.compile(
    r"^(?:ahora\s+)?en\s+(?P<territory>.+)$",
    re.IGNORECASE,
)
_ACTOR_ROLE_RE = re.compile(
    r"\b(?:proveedor(?:\s+adjudicado)?|contratista|adjudicatario)\b",
    re.IGNORECASE,
)
_SUPPORTED_ACTOR_TAIL_RE = re.compile(
    r"\s+(?:en\s+(?:el\s+)?(?:19|20)\d{2}|este\s+a[nñ]o|"
    r"por\s+mayor\s+valor|por\s+menor\s+valor)\s*$",
    re.IGNORECASE,
)


def _clean_mention(value: str) -> str:
    return normalize_actor_name(value.strip(" ,.;:¿?¡!\"'()"))


def _clean_relation_actor(value: str) -> tuple[str, bool]:
    cleaned = _clean_mention(value)
    role = re.match(
        r"^(?:el\s+|la\s+)?(?:proveedor(?:\s+adjudicado)?|contratista|"
        r"adjudicatario)\s+",
        cleaned,
        re.IGNORECASE,
    )
    if role is None:
        return cleaned, False
    return cleaned[role.end():].strip(), True


def _strip_supported_actor_tail(value: str) -> str:
    candidate = value.strip(" ,.;:¿?¡!\"'()")
    while True:
        shortened = _SUPPORTED_ACTOR_TAIL_RE.sub("", candidate).strip()
        if shortened == candidate:
            return _clean_mention(candidate)
        candidate = shortened


def _framing_spans(query: str) -> tuple[str, ...]:
    match = _LEADING_FRAMING_RE.search(query)
    return (match.group("span"),) if match is not None else ()


def interpret_actor_conversation(query: str) -> ActorConversationInstruction:
    """Project natural language into a small operation/role instruction."""
    normalized = normalize_actor_name(query)
    framing = _framing_spans(normalized)

    contextual_buyer = _CONTEXT_BUYER_RE.fullmatch(normalized)
    if contextual_buyer is not None:
        buyer = _clean_mention(contextual_buyer.group("buyer"))
        return ActorConversationInstruction(
            operation=ActorConversationOperation.SET_BUYER,
            buyer_mention=buyer,
            relation_buyer_span=contextual_buyer.group(0),
            framing_spans=framing,
            construction="contextual_con",
        )

    contextual_territory = _CONTEXT_TERRITORY_RE.fullmatch(normalized)
    if contextual_territory is not None:
        territory = _clean_mention(contextual_territory.group("territory"))
        if re.fullmatch(r"(?:19|20)\d{2}", territory):
            # ``en 2025`` is temporal grammar.  CON/EN relation precedence
            # applies only when EN introduces a place, never by preposition
            # alone.
            return ActorConversationInstruction(framing_spans=framing)
        return ActorConversationInstruction(
            operation=ActorConversationOperation.SET_TERRITORY,
            territory_mention=territory,
            framing_spans=framing,
            construction="contextual_en",
        )

    replacement = _REPLACE_RE.fullmatch(normalized)
    if replacement is not None:
        actor = _strip_supported_actor_tail(replacement.group("actor"))
        if actor.split(maxsplit=1)[0] in _NON_ACTOR_SEARCH_LEADS:
            # ``cambia a procesos/contratos`` is a dataset mutation, not an
            # actor called PROCESOS.  The generic follow-up policy owns it.
            return ActorConversationInstruction(framing_spans=framing)
        return ActorConversationInstruction(
            operation=ActorConversationOperation.REPLACE_ACTOR,
            actor_mention=actor,
            actor_evidence=actor_likeness(actor, strong_syntax=True),
            framing_spans=framing,
            construction="replace_actor",
        )

    for construction, pattern, strong in _RELATION_PATTERNS:
        relation = pattern.search(normalized)
        if relation is None:
            continue
        actor, actor_had_role = _clean_relation_actor(relation.group("actor"))
        buyer = _clean_mention(relation.group("buyer"))
        whole_tail = _BARE_CONTRACTS_RE.search(normalized)
        fallback = _strip_supported_actor_tail(
            whole_tail.group("actor") if whole_tail is not None else actor
        )
        fallback, _ = _clean_relation_actor(fallback)
        return ActorConversationInstruction(
            operation=ActorConversationOperation.SET_ACTOR,
            actor_mention=actor,
            buyer_mention=buyer,
            actor_evidence=actor_likeness(actor, strong_syntax=strong),
            relation_buyer_span=f"con {relation.group('buyer')}",
            fallback_actor_mention=fallback,
            fallback_actor_evidence=actor_likeness(fallback),
            framing_spans=framing,
            construction=(f"role_{construction}" if actor_had_role else construction),
        )

    bare_search = _SEARCH_BARE_RE.fullmatch(normalized)
    if bare_search is not None and not _ACTOR_ROLE_RE.search(normalized):
        actor = _strip_supported_actor_tail(bare_search.group("actor"))
        if actor.split(maxsplit=1)[0] in _NON_ACTOR_SEARCH_LEADS:
            return ActorConversationInstruction(framing_spans=framing)
        evidence = actor_likeness(actor)
        if evidence != "topic":
            return ActorConversationInstruction(
                operation=ActorConversationOperation.SET_ACTOR,
                actor_mention=actor,
                actor_evidence=evidence,
                framing_spans=framing,
                construction="natural_search_actor",
            )

    return ActorConversationInstruction(framing_spans=framing)


_ACTOR_STATE_KEYS = frozenset({
    "contratista", "actor_scope", "actor_pending", "actor_intent_conflict",
    "actor_resolution", "actor_resolution_method", "actor_resolution_status",
    "actor_selection_rank", "actor_conversation_probe",
    "actor_conversation_operation", "actor_conversation_construction",
})


def purge_actor_state(params: dict) -> dict:
    """Return a frame with every old actor identity/provenance field removed."""
    return {key: value for key, value in params.items() if key not in _ACTOR_STATE_KEYS}
