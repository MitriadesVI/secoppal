"""Pure arbitration helpers for AP3-C's two actor-name doors."""
from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.actor_intent import (
    recognized_actor_modifier_tails,
    strip_recognized_actor_modifier_tail,
)
from app.core.actor_name import normalize_actor_name
from app.core.control_language import is_pure_control_turn
from app.core.followup_control_tokens import classify_control_intent, requested_clear_fields
from app.core.intent_vocabulary import COMMON_TOKENS
from app.data.morphological_variants import MORPHOLOGICAL_VARIANTS

__all__ = [
    "BareActorProbe",
    "detect_bare_actor_probe",
    "has_entity_like_name_evidence",
    "has_strong_topic_evidence",
    "has_strong_search_intent",
    "scrub_strong_actor_leaks",
]


@dataclass(frozen=True, slots=True)
class BareActorProbe:
    actor_name: str
    unbounded_name: str
    surface_name: str
    name_only: bool = False


_BARE_ACTOR_RE = re.compile(r"\bcontratos?\s+de\s+(.+)$", re.IGNORECASE)
_SEARCH_DATASET_RE = re.compile(r"\b(?:contratos?|procesos?|convenios?)\b")
_REJECTED_LEADS = frozenset({"del", "de"})
_ACTOR_SYNTAX_TOPIC_TERMS = frozenset({
    "adjudicatario", "contratista", "proveedor",
    "historial", "contractual", "buscate", "buscame", "mirame",
    "muestrame", "averigua", "consulta", "revisa", "informacion",
    "contratado",
})
_LEGAL_FORM_SUFFIXES = (
    "sas", "s a s", "sa", "s a", "ltda", "limitada", "esp", "e s p",
    "sca", "s c a", "fundacion", "corporacion",
)
_INTERNAL_LEGAL_FORM_RE = re.compile(r"\b(?:sas|s a s|sa|s a|ltda|limitada)\b")
_ORGANIZATION_NOUNS = frozenset({
    "agencia", "asilo", "asociacion", "camara", "cooperativa",
    "corporacion", "empresa", "fundacion", "grupo", "hogar", "hospital",
    "instituto", "ips", "junta", "sanatorio", "sociedad", "universidad",
    "consorcio", "ese", "union", "ut",
})
_NAME_CONNECTORS = frozenset({
    "a", "al", "con", "de", "del", "el", "en", "la", "las", "los",
    "para", "por", "y",
})

_KNOWN_TOPIC_TERMS = frozenset(
    normalize_actor_name(value)
    for value in (*MORPHOLOGICAL_VARIANTS.keys(), *MORPHOLOGICAL_VARIANTS.values())
    if value
)
_KNOWN_TOPIC_TOKENS = frozenset(
    token for term in _KNOWN_TOPIC_TERMS for token in term.split() if len(token) >= 4
) | frozenset(normalize_actor_name(token) for token in COMMON_TOKENS)

_SEARCH_FILTER_KEYS = (
    "ciudad", "departamento_resolved", "departamentos_in", "region",
    "entidad", "entidad_resolved", "fecha_desde", "fecha_hasta",
    "valor_min", "valor_max", "modalidad_include_procedure_ids",
    "contract_type_include", "estado", "estado_family", "ordering_signal",
)


def has_strong_search_intent(raw_query: str, params: dict) -> bool:
    """Dataset language + independently parsed content/filter, never casing.

    This guards optional actor heuristics, not exact aliases or explicit roles.
    Pure R6 control turns keep their existing owner.
    """
    control = classify_control_intent(raw_query, params)
    if is_pure_control_turn(raw_query) or control in {
        "pagination_more", "change_order", "change_dataset", "remove_filter", "refine_filter",
    }:
        return False
    return bool(
        _SEARCH_DATASET_RE.search(normalize_actor_name(raw_query))
        and (params.get("objeto") or any(params.get(k) for k in _SEARCH_FILTER_KEYS))
    )


def detect_bare_actor_probe(raw_query: str, params: dict) -> BareActorProbe | None:
    """Extract X from ``contratos de X`` or a genuinely bare name.

    Strong role syntax is excluded by the caller before reaching this helper.
    Temporal, monetary and ordering tails are not part of the actor mention.
    """
    normalized = normalize_actor_name(raw_query)
    match = _BARE_ACTOR_RE.search(normalized)
    if match is not None:
        if params.get("dataset") != "contratos":
            return None
        unbounded = match.group(1).strip()
    else:
        # A bare-name probe must not reinterpret a complete search, a control,
        # or a recognized buyer. Unknown single words are exact-only probes.
        if (
            params.get("dataset_explicit")
            or _SEARCH_DATASET_RE.search(normalized)
            or any(params.get(k) for k in _SEARCH_FILTER_KEYS)
            or classify_control_intent(raw_query, params)
            or requested_clear_fields(raw_query)
            or is_pure_control_turn(raw_query)
        ):
            return None
        unbounded = normalized
    candidate = strip_recognized_actor_modifier_tail(
        unbounded,
        recognized_actor_modifier_tails(normalized, params),
    ).strip()
    words = candidate.split()
    if not words or words[0] in _REJECTED_LEADS:
        return None
    if candidate.isdigit() or len(candidate) < 3:
        return None
    if words[0] in _ACTOR_SYNTAX_TOPIC_TERMS or len(candidate) > 512:
        return None
    surface_match = _BARE_ACTOR_RE.search(raw_query)
    surface = surface_match.group(1).strip() if surface_match else unbounded
    probe = BareActorProbe(
        actor_name=candidate,
        unbounded_name=unbounded,
        surface_name=surface,
        name_only=match is None,
    )
    if match is None and has_strong_topic_evidence(candidate, params):
        if not has_entity_like_name_evidence(probe):
            return None
    return probe


def has_entity_like_name_evidence(probe: BareActorProbe) -> bool:
    """Conservative cues that bare X may be a person or organization name."""
    # Shape evidence belongs to the actor candidate, not to a recognized
    # modifier tail.  Otherwise ``mantenimiento ... 1000 SMMLV`` looks like an
    # acronym-bearing organization merely because its money unit is uppercase.
    normalized = normalize_actor_name(probe.actor_name)
    tokens = normalized.split()
    # A complete catalogued topic phrase is stronger than a generic noun that
    # happens to appear at its head (for example, a thematic phrase beginning
    # with "grupo").
    if normalized in _KNOWN_TOPIC_TERMS:
        return False
    if any(
        normalized.endswith(f" {suffix}")
        for suffix in _LEGAL_FORM_SUFFIXES
    ):
        return True
    # Legal form may be followed by the registered descriptive name, e.g.
    # LA PREVISORA S.A. COMPAÑÍA DE SEGUROS. It is semantic evidence, not case.
    if _INTERNAL_LEGAL_FORM_RE.search(normalized) and len(tokens) >= 3:
        return True
    if len(tokens) >= 2 and tokens[0] in _ORGANIZATION_NOUNS:
        return True

    # Acronym-like distinctive material is independent actor-shape evidence
    # even when the user typed it in lowercase.  Requiring two consonants and
    # no vowels keeps ordinary short topic words out while covering forms such
    # as ``LMP INGIENERIA`` and ``XYZ MANTENIMIENTO``.  The accompanying topic
    # token never authorizes identity; it only prevents the acronym from being
    # silently discarded.
    if len(tokens) >= 2 and any(
        2 <= len(token) <= 5
        and token.isalpha()
        and not set(token) & set("aeiou")
        for token in tokens
    ):
        return True

    # INV-ACTOR-EVIDENCE-MUST-BE-SEMANTIC. Name-like material is independent of
    # typography, and domain words or query language defeat this weak door.
    substantive = [word for word in tokens if word not in _NAME_CONNECTORS]
    return bool(
        probe.name_only
        and 2 <= len(substantive) <= 6
        and all(word.isalpha() and len(word) >= 2 for word in substantive)
        and not set(tokens) & (_KNOWN_TOPIC_TOKENS | _ACTOR_SYNTAX_TOPIC_TERMS)
        and not set(tokens) & {"en", "por", "para", "con", "desde", "hasta", "entre"}
        and not _SEARCH_DATASET_RE.search(normalized)
    )


def _canonical_geography_terms() -> frozenset[str]:
    """Topónimos que el propio parser trata como geografía canónica.

    Se derivan de las mismas fuentes que el router usa —el diccionario de
    departamentos y la lista de ciudades— para que la política no pueda
    divergir de lo que el sistema ya considera un lugar.
    """
    from app.core.query_router import _COMMON_CITIES
    from app.data.departamentos import DEPARTAMENTOS

    terms = {normalize_actor_name(city) for city in _COMMON_CITIES}
    for alias, canonical in DEPARTAMENTOS.items():
        terms.add(normalize_actor_name(alias))
        terms.add(normalize_actor_name(canonical))
    return frozenset(term for term in terms if term)


def has_canonical_geography_evidence(actor_name: str, params: dict) -> bool:
    """INV-BARE-GEOGRAPHY-PRECEDENCE.

    ``contratos de Bogotá`` es una consulta territorial. Que el directorio
    contenga a alguien cuyo nombre observado en SECOP sea literalmente
    "Bogota" —hay dos cédulas así— no puede cambiar eso: el routing de una
    geografía canónica dejaría de ser determinista y pasaría a depender de
    qué personas aparezcan en el directorio del día.

    Solo aplica a la mención DESNUDA. Con sintaxis explícita de rol
    —"proveedor Bogotá", "qué ha contratado Bogotá"— el usuario dijo que
    pregunta por un actor, y ahí la resolución sigue permitida.
    """
    normalized = normalize_actor_name(actor_name)
    if not normalized:
        return False
    return normalized in _canonical_geography_terms()


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


def scrub_strong_actor_leaks(
    params: dict,
    actor_name: str,
    *,
    raw_query: str = "",
) -> dict:
    """Remove actor-name fragments consumed too late by baseline extractors.

    QueryRouter detects geography before the AP2 name marker and may therefore
    read ``CARIBE`` in ``proveedor FUNDACIÓN CARIBE`` as a region, leaving
    ``fundación`` as topic.  This projection uses the already-detected mention
    boundary: only values contained in the actor name are removed, so an
    explicit tail such as ``proveedor X en Bolívar`` remains intact.

    Ownership belongs to the **occurrence**, not to the word.  Asking whether
    the matched alias appears lexically inside the actor name cannot tell two
    appearances of the same text apart, and ``proveedor Ana Llanos en llanos``
    names the surname once and the territory once.  When ``raw_query`` is
    supplied, an alias appearing more often in the turn than inside the actor
    name has at least one occurrence outside the accredited span, and that
    occurrence is an independent filter the user asked for.
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
                    if not _value_is_actor_syntax_or_name(item, name, name_tokens)
                ]
                if nested:
                    kept.append(nested if len(nested) > 1 else nested[0])
            elif not _value_is_actor_syntax_or_name(value, name, name_tokens):
                kept.append(value)
        if kept:
            clean["objeto"] = kept
        else:
            clean.pop("objeto", None)

    # INV-ACTOR-SPAN-OWNERSHIP. A region is the only geography whose stored
    # value is not the words that produced it — "llanos" is stored as
    # "Orinoquia" — so comparing the value against the name never matched and
    # a surname inside the accredited span kept filtering four departments.
    # What decides ownership is the matched alias: the text the scan consumed,
    # and *which occurrence* of it the actor span actually covers.
    region_evidence = clean.get("region_resolution")
    region_alias = (
        region_evidence.get("matched_alias")
        if isinstance(region_evidence, dict) else None
    )
    owned_by_span = _value_is_part_of_name(
        clean.get("region"), name, name_tokens,
    ) or _value_is_part_of_name(region_alias, name, name_tokens)
    if owned_by_span and _occurs_outside_name(region_alias, name, raw_query):
        owned_by_span = False
    if owned_by_span:
        clean.pop("region", None)
        clean.pop("departamentos_in", None)
        clean.pop("region_resolution", None)
    for key, companions in {
        "ciudad": ("ciudad_resolved",),
        "departamento": ("departamento_resolved", "departamento_resolution"),
        "departamento_resolved": ("departamento", "departamento_resolution"),
        "entidad": ("entidad_resolved", "entidad_resolution", "entidad_like"),
        "entidad_resolved": ("entidad", "entidad_resolution", "entidad_like"),
    }.items():
        if _value_is_part_of_name(clean.get(key), name, name_tokens):
            clean.pop(key, None)
            for companion in companions:
                clean.pop(companion, None)
    return clean


def _occurrences(needle: str, haystack: str) -> int:
    if not needle or not haystack:
        return 0
    return len(re.findall(rf"(?<!\w){re.escape(needle)}(?!\w)", haystack))


def _occurs_outside_name(alias: object, name: str, raw_query: str) -> bool:
    """Whether the turn used ``alias`` somewhere the actor span does not cover.

    Counting is the cheapest faithful test: the accredited span is a substring
    of the turn, so any surplus appearance lies outside it.  Without the turn
    text the question cannot be asked and the caller keeps its lexical answer.
    """
    if not raw_query or not isinstance(alias, str):
        return False
    needle = normalize_actor_name(alias)
    if not needle:
        return False
    return _occurrences(needle, normalize_actor_name(raw_query)) > _occurrences(needle, name)


def _value_is_part_of_name(value: object, name: str, name_tokens: set[str]) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    normalized = normalize_actor_name(value).replace("_", " ")
    if not normalized:
        return False
    return normalized in name or set(normalized.split()) <= name_tokens


def _value_is_actor_syntax_or_name(
    value: object,
    name: str,
    name_tokens: set[str],
) -> bool:
    if not isinstance(value, str):
        return _value_is_part_of_name(value, name, name_tokens)
    normalized = normalize_actor_name(value).replace("_", " ")
    return (
        normalized in _ACTOR_SYNTAX_TOPIC_TERMS
        or _value_is_part_of_name(value, name, name_tokens)
    )
