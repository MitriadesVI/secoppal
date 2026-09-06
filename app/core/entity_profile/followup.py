"""Bounded supplier questions within an already accredited institutional profile."""
import re

from app.core.entity_profile.scope import resolve_profile_scope
from app.utils.money import normalize_text


PROFILE_FOCUS_PARAM = "_entity_profile_focus"
_PREFIX = r"(?:(?:y|ahora)\s+)?"
_SUPPLIER_QUESTION = re.compile(
    _PREFIX
    + r"(?:(?:cuales|quienes)\s+son\s+|(?:muestrame|muestra|dame)\s+)?"
    r"(?:(?:sus|los)\s+)?(?:(?:mayores|principales)\s+)?"
    r"(?:proveedores|contratistas)"
    r"(?:\s+por\s+(?:valor|numero\s+de\s+contratos))?"
)
_WHO_CONTRACTS = re.compile(
    _PREFIX + r"quien(?:es)?\s+(?:tiene(?:n)?\s+mas\s+contratos|contrata(?:n)?\s+mas)"
    r"(?:\s+con\s+(?:esa|esta|la)\s+entidad)?"
)
_BY_VALUE = re.compile(_PREFIX + r"por\s+valor")


def is_supplier_followup(text: str, params: dict) -> bool:
    """Full grammatical constructions, never a substring or an identity lookup."""
    if not params.get("_entity_profile_active"):
        return False
    folded = re.sub(r"[¿?¡!.,;:]+", " ", normalize_text(text))
    folded = re.sub(r"\b(?:por favor|porfa|gracias)\b", " ", folded)
    folded = " ".join(folded.split())
    matched = bool(
        _SUPPLIER_QUESTION.fullmatch(folded)
        or _WHO_CONTRACTS.fullmatch(folded)
        or (params.get(PROFILE_FOCUS_PARAM) == "suppliers" and _BY_VALUE.fullmatch(folded))
    )
    return matched and resolve_profile_scope(params).scope is not None
