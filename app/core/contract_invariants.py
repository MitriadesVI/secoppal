"""Legal and semantic invariants for SECOP contracting queries.

Single source of truth for rules such as: signed/celebrated selects the
contracts dataset, but is not a valid SECOP state value.

Pure module: no I/O, no SECOP, no DB. Same inputs -> same outputs.
"""

from __future__ import annotations

from copy import deepcopy
import re
import unicodedata

from app.core._followup_constants import SCOPE_TOPIC_KEYS


FIRMADO_TOKENS: frozenset[str] = frozenset({
    "firmado",
    "firmados",
    "firmada",
    "firmadas",
    "celebrado",
    "celebrados",
    "celebrada",
    "celebradas",
    "suscrito",
    "suscritos",
    "suscrita",
    "suscritas",
})

INVALID_CONTRACT_STATES: frozenset[str] = frozenset({"firmado", "celebrado"})

NON_OPPORTUNITY_STATES: frozenset[str] = frozenset({
    "cerrado",
    "cerrados",
    "terminado",
    "terminados",
    "liquidado",
    "liquidados",
    "adjudicado",
    "adjudicados",
})

BIDDER_PHRASES: frozenset[str] = frozenset({
    "quiero presentarme",
    "para presentarme",
    "para poder presentarme",
    "puedo ofertar",
    "quiero ofertar",
    "quiero participar",
    "donde pueda ofertar",
    "donde pueda presentarme",
})


def _normalize(text: str) -> str:
    """Strip accents and lowercase."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower()


def contains_signed_token(raw_query: str) -> bool:
    """Return True when raw_query mentions signed/celebrated/suscribed tokens."""
    normalized = _normalize(raw_query)
    return any(
        re.search(rf"\b{re.escape(token)}\b", normalized)
        for token in FIRMADO_TOKENS
    )


def _canonical_invalid_state(value: object) -> str:
    normalized = _normalize(str(value)).strip()
    if normalized.startswith("firmad"):
        return "firmado"
    if normalized.startswith("celebrad"):
        return "celebrado"
    return normalized


def _is_invalid_contract_state(value: object) -> bool:
    return _canonical_invalid_state(value) in INVALID_CONTRACT_STATES


def _is_non_opportunity_state(value: object) -> bool:
    return _normalize(str(value)).strip() in NON_OPPORTUNITY_STATES


def _purge_values(params: dict, keys: tuple[str, ...], predicate) -> tuple[dict, bool]:
    cleaned = deepcopy(params)
    changed = False

    for key in keys:
        value = cleaned.get(key)
        if value in (None, "", [], {}):
            continue

        if isinstance(value, list):
            kept = [item for item in value if not predicate(item)]
            if len(kept) != len(value):
                changed = True
                if kept:
                    cleaned[key] = kept
                else:
                    cleaned.pop(key, None)
        elif predicate(value):
            changed = True
            cleaned.pop(key, None)

    if (
        not cleaned.get("estado_contrato")
        and not cleaned.get("estado")
        and cleaned.get("estado_field") == "estado_contrato"
    ):
        cleaned.pop("estado_field", None)

    return cleaned, changed


def purge_invalid_contract_states(params: dict) -> tuple[dict, list[str]]:
    """Remove invalid contract-state literals while preserving real SECOP states."""
    cleaned, changed = _purge_values(
        params,
        ("estado_contrato", "estado"),
        _is_invalid_contract_state,
    )
    return cleaned, ["invalid_estado_purged"] if changed else []


def apply_signed_dataset_invariant(raw_query: str, params: dict) -> tuple[dict, list[str]]:
    """Signed/celebrated mentions select contracts and never create state filters."""
    cleaned = deepcopy(params)
    flags: list[str] = []

    if contains_signed_token(raw_query) and cleaned.get("dataset") != "contratos":
        cleaned["dataset"] = "contratos"
        flags.append("firmado_dataset_forced")

    cleaned, purge_flags = purge_invalid_contract_states(cleaned)
    flags.extend(purge_flags)
    return cleaned, flags


def apply_opportunity_invariant(raw_query: str, params: dict) -> tuple[dict, list[str]]:
    """Bidder/opportunity intent searches open processes, idempotently."""
    normalized = _normalize(raw_query)
    has_bidder_phrase = any(phrase in normalized for phrase in BIDDER_PHRASES)
    already_opportunity = (
        params.get("intent_type") == "opportunity_search"
        or params.get("estado_family") == "oferta_abierta"
    )
    if not has_bidder_phrase and not already_opportunity:
        return params, []

    cleaned = deepcopy(params)
    before = deepcopy(cleaned)
    cleaned["dataset"] = "procesos"
    cleaned["intent_type"] = "opportunity_search"
    cleaned["estado_family"] = "oferta_abierta"
    cleaned, _ = _purge_values(
        cleaned,
        ("estado_contrato", "estado"),
        _is_non_opportunity_state,
    )

    return cleaned, ["opportunity_enforced"] if cleaned != before else []


def enforce_oferta_abierta_not_contracts(params: dict) -> tuple[dict, list[str]]:
    """Resolve oferta_abierta + contracts by selecting process opportunity search."""
    if params.get("estado_family") != "oferta_abierta" or params.get("dataset") != "contratos":
        return params, []

    cleaned = deepcopy(params)
    cleaned["dataset"] = "procesos"
    cleaned["intent_type"] = "opportunity_search"
    return cleaned, ["oferta_abierta_not_contracts"]


def has_substantial_filter(params: dict) -> bool:
    """Return True when params include canonical scope/topic filters."""
    for key in SCOPE_TOPIC_KEYS:
        value = params.get(key)
        if value in (None, "", [], {}, False):
            continue
        return True
    return False
