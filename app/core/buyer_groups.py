"""Declared, versioned selections of accredited BuyerUnits (ADR-074).

Only registry references enter execution. Request-supplied names or membership
never establish authority. Fiscal and result identities are deliberately absent.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import re

from app.core.buyer_source_names import project_buyer_names
from app.core.icbf_resolver import resolve_icbf_reference
from app.utils.spell_correction import correct_query
from app.utils.money import normalize_text

DATA = Path(__file__).resolve().parents[1] / "data"
MAX_GROUP_SOURCE_NAMES = 40


class BuyerGroupError(ValueError):
    """Unknown, stale, conflicting or unprojectable group: never a subset."""


@dataclass(frozen=True)
class BuyerGroup:
    group_id: str
    version: str
    label: str
    aliases: tuple[str, ...]
    members: tuple[str, ...]

    def reference(self) -> dict:
        return {"id": self.group_id, "version": self.version}

    def names_for(self, dataset_id: str) -> tuple[str, ...]:
        names: list[str] = []
        for member in self.members:
            physical = project_buyer_names(member, dataset_id)
            if not physical:
                raise BuyerGroupError(f"Sin proyección acreditada: {member}/{dataset_id}")
            names.extend(physical)
        if len(set(names)) != len(names):
            raise BuyerGroupError("Dos miembros reclaman un mismo nombre físico")
        if not names or len(names) > MAX_GROUP_SOURCE_NAMES:
            raise BuyerGroupError("El conjunto supera la capacidad acreditada de 40 nombres")
        return tuple(names)


def parse_groups(payload: dict, known_units: set[str]) -> dict[str, BuyerGroup]:
    if payload.get("schema_version") != 1:
        raise BuyerGroupError("Schema de grupos no soportado")
    groups: dict[str, BuyerGroup] = {}
    aliases: set[str] = set()
    for record in payload.get("groups", []):
        for key in ("id", "version", "label"):
            if not isinstance(record.get(key), str) or not record[key].strip():
                raise BuyerGroupError(f"Falta {key} en el grupo")
        members = tuple(m.get("canonical_name", "") for m in record.get("members", []))
        if not members or len(members) != len(set(members)) or set(members) - known_units:
            raise BuyerGroupError("Membresía duplicada, vacía o no acreditada")
        if any(not m.get("evidence") or not m.get("membership_source") for m in record["members"]):
            raise BuyerGroupError("Cada miembro requiere procedencia de membresía")
        terms = tuple(correct_query(normalize_text(a)) for a in record.get("aliases", []))
        if not terms or any(not a or a in aliases for a in terms) or len(set(terms)) != len(terms):
            raise BuyerGroupError("Alias de grupo vacío o compartido")
        aliases.update(terms)
        key = record["id"]
        if key in groups:
            raise BuyerGroupError("Grupo duplicado")
        groups[key] = BuyerGroup(key, record["version"], record["label"], terms, members)
    return groups


@lru_cache(maxsize=1)
def get_groups() -> dict[str, BuyerGroup]:
    records = json.loads((DATA / "aliases_db.json").read_text())
    icbf = json.loads((DATA / "icbf_regionals.json").read_text())
    known = {r["official_name"] for r in [*records, icbf["sede_nacional"], *icbf["regionales"]]}
    return parse_groups(json.loads((DATA / "buyer_groups.json").read_text()), known)


def group_for_params(params: dict) -> BuyerGroup | None:
    ref = params.get("buyer_group")
    if not ref:
        return None
    if params.get("entidad_resolved") or params.get("entidad_like"):
        return None
    if not isinstance(ref, dict) or set(ref) != {"id", "version"}:
        raise BuyerGroupError("La consulta debe referenciar un grupo, no definir miembros")
    group = get_groups().get(ref["id"])
    if not group or group.version != ref["version"]:
        raise BuyerGroupError("Grupo desconocido o versión de membresía vencida; vuelve a consultarlo")
    if any(params.get(k) for k in ("entidad_in_candidates", "entidad_contains_all", "entidad_ecosystem")):
        raise BuyerGroupError("El grupo declarado no puede mezclarse con candidatos de otra resolución")
    return group


def consume_group_mention(text: str, entity_resolver=None) -> tuple[dict, str]:
    """Consume only declared phrases; bare geography cannot select a group."""
    text = text.strip()
    normalized = normalize_text(text)
    matches = []
    icbf = resolve_icbf_reference(text)
    for group in get_groups().values():
        if group.group_id == "icbf_national" and re.search(r"\bsede\s+(?:nacional|central)\b", normalized):
            continue
        if group.group_id == "icbf_national" and (not icbf or icbf.resolution_type != "icbf_national"):
            continue
        for alias in sorted(group.aliases, key=len, reverse=True):
            match = re.search(r"(?<!\w)" + re.escape(alias) + r"(?!\w)", normalized)
            if match:
                matches.append((group, match))
                break
    if len(matches) > 1:
        raise BuyerGroupError("Hay más de un grupo institucional; precisa cuál deseas consultar")
    if not matches:
        return {}, text
    group, match = matches[0]
    if entity_resolver:
        explicit = entity_resolver.scan_entity(text, declared_group=group)
        if explicit:
            alias = normalize_text(explicit.matched_alias)
            alias = re.sub(r"^(?:(?:de|del|la|el|los|las)\s+)+", "", alias)
            if alias not in group.aliases and any(term in alias for term in group.aliases):
                return {
                    "entidad": explicit.matched_alias,
                    "entidad_resolved": explicit.official_name,
                    "entidad_resolution": {
                        "value": explicit.official_name, "method": explicit.method,
                        "kind": "buyer_entity", "confidence": "high", "like_value": None,
                        "metadata": {"declared_parent_group": group.group_id},
                    },
                }, explicit.remaining_text
    # normalize_text preserves character positions for these accent-only aliases.
    residual = text[:match.start()] + " " + text[match.end():]
    return {"buyer_group": group.reference()}, residual


def group_declaration(params: dict) -> str:
    group = group_for_params(params)
    if not group:
        return ""
    return (
        f"Alcance declarado: {group.label} ({len(group.members)} unidades compradoras; "
        f"versión {group.version}). Miembros: " + "; ".join(group.members) + ".\n"
        "Los conteos de registros se expresan en filas físicas; los resultados únicos conservan su identificador de proceso o contrato."
    )
