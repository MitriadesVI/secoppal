#!/usr/bin/env python3
"""GAZETTEER-ALIAS-OWNERSHIP-R1C — auditoría determinista de propiedad de aliases.

Read-only. No modifica ``aliases_db.json``.

Tesis
-----
Un alias no significa *"esta frase podría tener alguna relación con esta
entidad"*. Significa *"cuando el usuario usa esta frase como comprador, esta
entidad es una interpretación institucionalmente válida"*. La cobertura no
justifica un ownership incorrecto: es preferible ``FAIL_CLOSED`` a responder
que la Gobernación de Cundinamarca es un hospital de Sopó.

Clasificaciones
---------------
``EXACT_MULTI_OWNER``           un alias normalizado reclamado por >1 entidad.
``GENERIC_STRUCTURAL_ALIAS``    alias sin ningún token identitario ni territorial.
``PARENT_TO_CHILD_HIJACK``      una subentidad reclama la frase del comprador raíz.
``CROSS_ENTITY_HIJACK``         una institución reclama el nombre de otra.
``TERRITORY_AS_ENTITY_HIJACK``  una entidad no territorial reclama un territorio desnudo.
``POSSIBLE_SAME_LOGICAL_IDENTITY``  grafías físicas rivales de una misma identidad.
``MANUAL_REVIEW``               sospecha sin evidencia suficiente para actuar.

La normalización es la MISMA que usa ``EntityResolver`` (``normalize_text``),
para que lo que aquí se llama "colisión" sea exactamente lo que el runtime ve.

Uso:
    python scripts/audits/audit_gazetteer_alias_ownership.py \
        [--aliases-db app/data/aliases_db.json] \
        [--out artifacts/audits/gazetteer_alias_ownership_r1c.json]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.entity_resolver import GAZETTEER_CIUDADES  # noqa: E402
from app.data.departamentos import DEPARTAMENTOS  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

DEFAULT_DB = ROOT / "app" / "data" / "aliases_db.json"
DEFAULT_OUT = ROOT / "artifacts" / "audits" / "gazetteer_alias_ownership_r1c.json"

# ---------------------------------------------------------------------------
# Léxicos
# ---------------------------------------------------------------------------

STOPWORDS = {"de", "del", "la", "el", "los", "las", "y", "en", "e", "al", "a"}
_ARTICLES = {"el", "la", "los", "las"}

# Cabezas inequívocas por eje institucional.
DEPARTMENTAL_HEADS = {"gobernacion"}
MUNICIPAL_HEADS = {"alcaldia", "municipio"}
# "gobierno" y "departamento" son polisémicos: se resuelven por contexto en
# ``alias_axes()``. "departamento de estadística" NO es un territorio.
AMBIGUOUS_HEADS = {"gobierno", "departamento"}

NATIONAL_HEADS = {
    "ministerio", "superintendencia", "fiscalia", "procuraduria",
    "contraloria", "defensoria", "registraduria", "presidencia",
}

# Marcadores que hacen de una entidad una SUBENTIDAD cualquiera sea su prefijo:
# "GOBERNACION DEL VALLE DEL CAUCA - DADI" empieza por "gobernacion" y sin
# embargo no es el comprador raíz del Valle.
SUBENTITY_MARKERS = {
    "secretaria": "secretaria",
    "hospital": "salud",
    "ese": "salud",
    "ips": "salud",
    "clinica": "salud",
    "unidad": "unidad",
    "instituto": "instituto",
    "direccion": "direccion",
    "fondo": "fondo",
    "empresa": "empresa",
    "oficina": "oficina",
    "corporacion": "corporacion",
    "universidad": "universidad",
    "personeria": "personeria",
    "concejo": "concejo",
    "asamblea": "asamblea",
}

# Formas jurídicas y sufijos de registro: "EMPRESA SOCIAL DEL ESTADO",
# "UNIDAD ADMINISTRATIVA ESPECIAL", "MUNICIPIO DE PEREIRA- OFICIAL". Describen
# el ropaje legal del registro SECOP, no la identidad de la unidad.
LEGAL_FORM_TOKENS = {
    "social", "estado", "especial", "prestadora", "mayor", "oficial",
    "colombiano", "colombia", "nacion", "republica",
}

# Tokens estructurales: describen la forma del Estado, no una identidad.
STRUCTURAL_TOKENS = (
    DEPARTMENTAL_HEADS
    | MUNICIPAL_HEADS
    | AMBIGUOUS_HEADS
    | {
        "departamental", "municipal", "distrital", "distrito", "nacional",
        "administrativo", "administrativa", "administracion", "general",
        "central", "publico", "publica", "entidad", "ente", "territorial",
        "local", "seccional", "regional", "sede", "capital",
    }
)

# Tokens que nombran una forma de gobierno: su presencia convierte un alias
# genérico en una frase de comprador, capturable por cualquier territorio.
GOVERNMENT_HEAD_TOKENS = (
    DEPARTMENTAL_HEADS | MUNICIPAL_HEADS | AMBIGUOUS_HEADS | {"capital"}
)

# Un token que no identifica: no distingue una subentidad de su raíz.
NON_IDENTITY_TOKENS = STRUCTURAL_TOKENS | set(SUBENTITY_MARKERS) | LEGAL_FORM_TOKENS

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
_DECOR_RE = re.compile(r"[.\*/\-–—\s]+$")
_SEPARATOR_RE = re.compile(r"\s*[-–—]\s*")


def tokens_of(text: str) -> list[str]:
    return [t for t in _PUNCT_RE.sub(" ", text).split() if t]


def content_tokens(text: str) -> list[str]:
    return [t for t in tokens_of(text) if t not in STOPWORDS]


# Vocabulario geográfico. Un token geográfico NUNCA distingue una subentidad
# de su raíz: es justamente lo que ambas comparten.
GEO_TOKENS: set[str] = set()
for _key in list(DEPARTAMENTOS) + list(GAZETTEER_CIUDADES):
    GEO_TOKENS.update(content_tokens(normalize_text(_key)))

BARE_TERRITORIES: dict[str, tuple[str, str]] = {}
for _key, _canon in DEPARTAMENTOS.items():
    BARE_TERRITORIES.setdefault(normalize_text(_key), ("departamento", _canon))
for _key, _canon in GAZETTEER_CIUDADES.items():
    BARE_TERRITORIES.setdefault(normalize_text(_key), ("ciudad", _canon))


def _department_of(tokens: list[str]) -> str | None:
    """Departamento nombrado por una cola de tokens.

    Se prueban las dos formas porque ``DEPARTAMENTOS`` indexa la grafía con
    preposiciones internas: "valle del cauca" está, "valle cauca" no.
    """
    stripped = list(tokens)
    while stripped and stripped[0] in STOPWORDS:
        stripped.pop(0)
    candidates = (
        tokens,                                       # "del valle del cauca"
        stripped,                                     # "valle del cauca"
        [t for t in tokens if t not in STOPWORDS],    # "valle cauca"
    )
    for phrase in (" ".join(c) for c in candidates):
        found = DEPARTAMENTOS.get(normalize_text(phrase).strip())
        if found:
            return found
    return None


# ---------------------------------------------------------------------------
# Modelo de entidad
# ---------------------------------------------------------------------------


class Entity:
    """Un registro del gazetteer con su identidad institucional derivada."""

    def __init__(self, record: dict) -> None:
        self.official: str = record["official_name"]
        self.norm: str = normalize_text(self.official)
        self.departamento: str | None = record.get("departamento")
        self.nit: str | None = record.get("nit")
        self.total_procesos: int = record.get("total_procesos") or 0
        self.total_contratos: int = record.get("total_contratos") or 0
        self.source_datasets: list[str] = record.get("source_datasets") or []
        self.aliases: list[str] = record.get("aliases", [])
        self.tokens: set[str] = set(content_tokens(self.norm))

        self.subentity_kind: str | None = next(
            (kind for marker, kind in SUBENTITY_MARKERS.items() if marker in self.tokens),
            None,
        )
        self.axis: str = self._axis()
        self.identity_key: str = _DECOR_RE.sub("", self.norm).strip()

        # Prefijo antes del primer separador: la familia a la que pertenece
        # una subentidad con nombre compuesto ("GOBERNACION DEL VALLE … - DADI").
        parts = _SEPARATOR_RE.split(self.norm)
        self.family_prefix: str | None = parts[0].strip() if len(parts) > 1 else None
        self.is_root_pure: bool = self.subentity_kind is None and self.family_prefix is None

    def _axis(self) -> str:
        """Eje institucional del dueño: departamental, municipal, o su tipo.

        La cabeza de gobierno no siempre va al inicio: SECOP registra
        "CUNDINAMARCA-ALCALDIA MUNICIPIO MOSQUERA", que es una alcaldía.
        """
        if self.subentity_kind:
            return self.subentity_kind
        toks = content_tokens(self.norm)
        head = next(iter(toks), "")
        if head == "departamento":
            raw = tokens_of(self.norm)
            return "departamental" if _department_of(raw[1:]) else "otro"
        if head in NATIONAL_HEADS:
            return head
        for tok in toks:
            if tok in DEPARTMENTAL_HEADS or tok == "gobierno":
                return "departamental"
            if tok in MUNICIPAL_HEADS or tok == "distrito":
                return "municipal"
        return "otro"

    def is_bare_parent_phrase(
        self, alias_tokens: set[str], toponyms: frozenset[str]
    ) -> bool:
        """¿El alias es la frase DESNUDA del comprador raíz de esta entidad?

        Lo es cuando cada token que lo compone describe la forma de gobierno,
        una forma jurídica o un lugar — y ninguno nombra a esta unidad. Basta
        un token propio ("… **dadi**", "… **inder**", "alcaldía de Bogotá
        **movilidad**") para que el alias deje de ser la frase del padre y pase
        a nombrar legítimamente a la subentidad.
        """
        permitted = STRUCTURAL_TOKENS | LEGAL_FORM_TOKENS | GEO_TOKENS | toponyms
        if not all(t in permitted or len(t) <= 2 for t in alias_tokens):
            return False
        return not (alias_tokens & self.identity_tokens(toponyms))

    def identity_tokens(self, toponyms: frozenset[str]) -> set[str]:
        """Tokens que identifican a ESTA unidad y no a su comprador raíz.

        Se excluyen los estructurales y las formas jurídicas (describen la
        forma del Estado, no una identidad) y los topónimos (el territorio es
        justamente lo que la unidad comparte con su raíz).
        """
        return {
            t
            for t in self.tokens
            if t not in NON_IDENTITY_TOKENS
            and t not in GEO_TOKENS
            and t not in toponyms
            and len(t) > 2
        }

    def alias_names_this_unit(self, alias_tokens: set[str]) -> bool:
        """¿El alias aporta alguna prueba de que se refiere a ESTA unidad?

        Vale como prueba nombrar su identidad ("… secretaría de educación") o
        su tipo institucional ("municipio de Malambo **hospital**"). Sin
        ninguna de las dos, el alias es la frase desnuda del comprador raíz.
        """
        if self.identity_tokens & alias_tokens:
            return True
        markers = {m for m in SUBENTITY_MARKERS if m in self.tokens}
        return bool(markers & alias_tokens)


# ---------------------------------------------------------------------------
# Lectura del alias
# ---------------------------------------------------------------------------

ROOT_AXES = ("departamental", "municipal")


class AliasReading:
    """Lo que un alias declara por sí mismo, sin mirar a su dueño."""

    __slots__ = ("axes", "territory", "unambiguous_departmental")

    def __init__(
        self,
        axes: frozenset[str],
        territory: str | None = None,
        unambiguous_departmental: bool = False,
    ) -> None:
        self.axes = axes
        self.territory = territory
        # Sólo una cabeza inequívocamente departamental autoriza a juzgar
        # territorio cruzado: "Caldas" es a la vez departamento y municipio de
        # Antioquia, y "alcaldía de Caldas" nombra al municipio, no al
        # departamento.
        self.unambiguous_departmental = unambiguous_departmental


def alias_axes(alias_norm: str) -> AliasReading:
    """Ejes institucionales que el alias declara, y el departamento que nombra.

    Un eje vacío significa que el alias no declara un comprador raíz: no se
    juzga su ownership territorial.
    """
    raw = tokens_of(alias_norm)
    # La cabeza es el primer token de contenido; la cola conserva las
    # preposiciones internas ("valle **del** cauca").
    head_pos = next((i for i, t in enumerate(raw) if t not in STOPWORDS), None)
    if head_pos is None:
        return AliasReading(frozenset())
    head = raw[head_pos]
    raw_tail = raw[head_pos + 1:]
    tail = [t for t in raw_tail if t not in STOPWORDS]
    tail_dept = _department_of(raw_tail) if raw_tail else None

    if head in DEPARTMENTAL_HEADS:
        return AliasReading(frozenset({"departamental"}), tail_dept, True)
    if head in MUNICIPAL_HEADS:
        return AliasReading(frozenset({"municipal"}), tail_dept)
    if head == "departamento":
        # Territorial sólo si nombra un departamento real; "departamento
        # administrativo nacional de estadística" no lo es.
        if tail_dept:
            return AliasReading(frozenset({"departamental"}), tail_dept, True)
        return AliasReading(frozenset())
    if head == "gobierno":
        if tail and tail[0] == "departamental":
            after = raw_tail[raw_tail.index("departamental") + 1:]
            return AliasReading(
                frozenset({"departamental"}), _department_of(after), True
            )
        if tail and tail[0] in ("municipal", "distrital"):
            return AliasReading(frozenset({"municipal"}))
        # "gobierno de X" es ambiguo aunque X sea un departamento: hay
        # municipios homónimos. Compatible con ambos ejes, sin autoridad
        # para declarar territorio cruzado.
        return AliasReading(frozenset(ROOT_AXES), tail_dept)
    if head in NATIONAL_HEADS:
        return AliasReading(frozenset({head}), tail_dept)
    return AliasReading(frozenset())


# ---------------------------------------------------------------------------
# Clasificación de un par (alias, dueño)
# ---------------------------------------------------------------------------


class Audit:
    def __init__(self, entities: list[Entity]) -> None:
        self.entities = entities
        self.owners: dict[str, list[Entity]] = defaultdict(list)
        for ent in entities:
            for alias in {normalize_text(a) for a in ent.aliases}:
                if alias:
                    self.owners[alias].append(ent)
        self.family_size: dict[str, int] = defaultdict(int)
        for ent in entities:
            if ent.family_prefix:
                self.family_size[ent.family_prefix] += 1
        self.toponyms: frozenset[str] = self._derive_toponyms()

    def _derive_toponyms(self) -> frozenset[str]:
        """Vocabulario de lugares derivado del propio gazetteer.

        Toda frase "municipio de X" / "alcaldía de X" / "gobernación de X"
        —venga de un nombre oficial o de un alias— declara que X es un lugar.
        Es lo que permite saber que "sopó" es un municipio y "movilidad" no,
        sin una lista de municipios que el repositorio no tiene.
        """
        heads = DEPARTMENTAL_HEADS | MUNICIPAL_HEADS
        found: set[str] = set()
        phrases = [normalize_text(e.official) for e in self.entities]
        phrases += [normalize_text(a) for e in self.entities for a in e.aliases]
        for phrase in phrases:
            toks = content_tokens(phrase)
            for i, tok in enumerate(toks):
                if tok not in heads:
                    continue
                # Sólo el token que sigue inmediatamente a la cabeza: en
                # "alcaldía de Bogotá movilidad" el lugar es "bogotá", no
                # "movilidad".
                tail = [t for t in toks[i + 1:] if t != "municipal"]
                if not tail:
                    continue
                candidate = tail[0]
                if candidate not in NON_IDENTITY_TOKENS and len(candidate) > 2:
                    found.add(candidate)
        return frozenset(found)

    # -- ayudas -----------------------------------------------------------

    def root_for_territory(self, territory: str | None) -> Entity | None:
        """Comprador raíz de un departamento, si existe en el gazetteer."""
        if not territory:
            return None
        terr_tokens = set(content_tokens(normalize_text(territory)))
        best: Entity | None = None
        for ent in self.entities:
            if ent.axis != "departamental" or not ent.is_root_pure:
                continue
            if not terr_tokens.issubset(ent.tokens):
                continue
            if best is None or ent.total_procesos > best.total_procesos:
                best = ent
        return best

    def entity_matching(
        self, alias_tokens: set[str], head: str, exclude: Entity
    ) -> Entity | None:
        """Entidad cuyo nombre oficial contiene la cabeza y todos los tokens del alias."""
        candidates = [
            e
            for e in self.entities
            if e is not exclude and head in e.tokens and alias_tokens.issubset(e.tokens)
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda e: e.total_procesos + e.total_contratos, reverse=True)
        return candidates[0]

    # -- clasificador -----------------------------------------------------

    def classify(self, alias_norm: str, owner: Entity) -> dict | None:
        alias_tokens = set(content_tokens(alias_norm))
        if not alias_tokens:
            return None
        reading = alias_axes(alias_norm)

        return (
            self._generic(alias_norm, alias_tokens)
            or self._territory(alias_norm, owner, reading.axes)
            or self._cross_territory(alias_norm, owner, reading)
            or self._cross_national(alias_norm, alias_tokens, owner, reading.axes)
            or self._parent_child(alias_norm, alias_tokens, owner, reading)
        )

    # B ------------------------------------------------------------------
    def _generic(self, alias_norm: str, alias_tokens: set[str]) -> dict | None:
        if not all(t in STRUCTURAL_TOKENS for t in alias_tokens):
            return None
        # Un adjetivo institucional CON artículo ("la distrital", "la
        # nacional") es el apodo real de una universidad: genérico y peligroso,
        # pero retirarlo borra una identidad de uso corriente. Sin artículo no
        # es el apodo, y sí captura cualquier frase que lleve la palabra
        # ("gobierno distrital de Cali" → Universidad Distrital).
        has_article = bool(set(tokens_of(alias_norm)) & _ARTICLES)
        if has_article and not (alias_tokens & GOVERNMENT_HEAD_TOKENS):
            return {
                "classification": "GENERIC_STRUCTURAL_ALIAS",
                "action": "MANUAL_REVIEW",
                "confidence": "low",
                "owner_expected": None,
                "evidence": (
                    f"'{alias_norm}' es puramente estructural ({sorted(alias_tokens)}) "
                    f"pero no nombra una forma de gobierno; puede ser el apodo real "
                    f"de la entidad. Retirarlo exige juicio humano."
                ),
            }
        return {
            "classification": "GENERIC_STRUCTURAL_ALIAS",
            "action": "REMOVE_WRONG_ALIAS",
            "confidence": "high",
            "owner_expected": None,
            "evidence": (
                f"'{alias_norm}' está compuesto sólo por tokens estructurales "
                f"({sorted(alias_tokens)}) y nombra una forma de gobierno sin "
                f"territorio: captura la frase de cualquier otro territorio."
            ),
        }

    # E ------------------------------------------------------------------
    def _territory(self, alias_norm: str, owner: Entity, axes: frozenset[str]) -> dict | None:
        if alias_norm not in BARE_TERRITORIES or axes:
            return None
        kind, canon = BARE_TERRITORIES[alias_norm]
        if owner.axis in ROOT_AXES and owner.is_root_pure:
            return None
        return {
            "classification": "TERRITORY_AS_ENTITY_HIJACK",
            "action": "REMOVE_WRONG_ALIAS",
            "confidence": "high",
            "owner_expected": None,
            "evidence": (
                f"'{alias_norm}' es un territorio desnudo ({kind}: {canon}). "
                f"El dueño '{owner.official}' es de tipo '{owner.axis}': no es "
                f"su gobierno, así que la frase pertenece a la autoridad geográfica."
            ),
        }

    # D.1 ----------------------------------------------------------------
    def _cross_territory(
        self, alias_norm: str, owner: Entity, reading: "AliasReading"
    ) -> dict | None:
        territory = reading.territory
        if not territory or not reading.unambiguous_departmental:
            return None
        if not owner.departamento or owner.departamento == "No Definido":
            return None
        if territory == owner.departamento:
            return None
        expected = self.root_for_territory(territory)
        return {
            "classification": "CROSS_ENTITY_HIJACK",
            "action": "REMOVE_WRONG_ALIAS",
            "confidence": "high",
            "owner_expected": expected.official if expected else None,
            "evidence": (
                f"'{alias_norm}' nombra el gobierno de {territory}; el dueño "
                f"'{owner.official}' pertenece a {owner.departamento}. "
                f"Territorios distintos."
            ),
        }

    # D.2 ----------------------------------------------------------------
    def _cross_national(
        self, alias_norm: str, alias_tokens: set[str], owner: Entity, axes: frozenset[str]
    ) -> dict | None:
        head = next((a for a in axes if a in NATIONAL_HEADS), None)
        if head is None or head in owner.tokens:
            return None
        if len(alias_tokens) < 2:
            # Cabeza institucional desnuda ("contraloría"): genérica, no un
            # nombre que pertenezca a otra entidad en particular.
            return {
                "classification": "MANUAL_REVIEW",
                "action": "MANUAL_REVIEW",
                "confidence": "low",
                "owner_expected": None,
                "evidence": (
                    f"'{alias_norm}' es una cabeza institucional desnuda: genérica, "
                    f"sin identidad que la califique."
                ),
            }
        expected = self.entity_matching(alias_tokens, head, exclude=owner)
        if expected is None:
            return {
                "classification": "MANUAL_REVIEW",
                "action": "MANUAL_REVIEW",
                "confidence": "low",
                "owner_expected": None,
                "evidence": (
                    f"'{alias_norm}' declara '{head}', ausente del nombre del dueño "
                    f"'{owner.official}', y no hay entidad candidata en el gazetteer."
                ),
            }
        if self._same_identity(owner, expected):
            return {
                "classification": "POSSIBLE_SAME_LOGICAL_IDENTITY",
                "action": "SAME_IDENTITY_REVIEW",
                "confidence": "medium",
                "owner_expected": expected.official,
                "evidence": (
                    f"'{alias_norm}' apunta a '{expected.official}', que comparte "
                    f"identidad lógica con el dueño '{owner.official}'."
                ),
            }
        return {
            "classification": "CROSS_ENTITY_HIJACK",
            "action": "REMOVE_WRONG_ALIAS",
            "confidence": "high",
            "owner_expected": expected.official,
            "evidence": (
                f"'{alias_norm}' declara '{head}', ausente del nombre del dueño "
                f"'{owner.official}'. La entidad '{expected.official}' existe en el "
                f"gazetteer y contiene todos los tokens del alias."
            ),
        }

    # C ------------------------------------------------------------------
    def _parent_child(
        self,
        alias_norm: str,
        alias_tokens: set[str],
        owner: Entity,
        reading: "AliasReading",
    ) -> dict | None:
        territory = reading.territory
        root_axes = reading.axes & set(ROOT_AXES)
        if not root_axes:
            return None
        expected = self.root_for_territory(territory)
        expected_name = expected.official if expected and expected is not owner else None

        if not owner.is_bare_parent_phrase(alias_tokens, self.toponyms):
            return None

        # Localidades de Bogotá: la alcaldía local y su fondo de desarrollo
        # local son el mismo comprador en SECOP. Retirarle al fondo la frase
        # de "su" alcaldía no corrige un ownership ajeno — exige juicio humano.
        if "local" in owner.tokens:
            return {
                "classification": "MANUAL_REVIEW",
                "action": "MANUAL_REVIEW",
                "confidence": "low",
                "owner_expected": None,
                "evidence": (
                    f"'{alias_norm}' es la frase de un comprador raíz, pero "
                    f"'{owner.official}' es una entidad de escala LOCAL: en SECOP "
                    f"la alcaldía local y su fondo de desarrollo son el mismo "
                    f"comprador. No hay ownership ajeno demostrado."
                ),
            }

        # (a) El alias declara un tipo institucional que el dueño no es.
        if owner.axis not in root_axes:
            if owner.subentity_kind:
                return {
                    "classification": "PARENT_TO_CHILD_HIJACK",
                    "action": "REMOVE_WRONG_ALIAS",
                    "confidence": "high",
                    "owner_expected": expected_name,
                    "evidence": (
                        f"'{alias_norm}' es la frase natural de un comprador "
                        f"{'/'.join(sorted(root_axes))}; el dueño "
                        f"'{owner.official}' es una subentidad de tipo "
                        f"'{owner.subentity_kind}'."
                    ),
                }
            if expected_name:
                return {
                    "classification": "PARENT_TO_CHILD_HIJACK",
                    "action": "REMOVE_WRONG_ALIAS",
                    "confidence": "high",
                    "owner_expected": expected_name,
                    "evidence": (
                        f"'{alias_norm}' es la frase natural de un comprador "
                        f"{'/'.join(sorted(root_axes))} y '{expected_name}' existe "
                        f"en el gazetteer como ese comprador. El dueño actual "
                        f"'{owner.official}' no lo es."
                    ),
                }
            # Sin raíz alternativa el resultado será FAIL_CLOSED, y eso es lo
            # correcto: una entidad que no es un gobierno de ningún tipo no
            # puede poseer la frase desnuda de un gobierno. Es la misma regla
            # que TERRITORY_AS_ENTITY_HIJACK, un nivel más arriba.
            return {
                "classification": "PARENT_TO_CHILD_HIJACK",
                "action": "REMOVE_WRONG_ALIAS",
                "confidence": "high",
                "owner_expected": None,
                "evidence": (
                    f"'{alias_norm}' es la frase desnuda de un comprador "
                    f"{'/'.join(sorted(root_axes))}; el dueño '{owner.official}' "
                    f"no es un gobierno (eje='{owner.axis}'). No hay raíz "
                    f"alternativa en el gazetteer: el resultado correcto es "
                    f"FAIL_CLOSED, no este dueño."
                ),
            }

        # (b) El dueño es del eje correcto pero NO es el comprador raíz.
        if owner.is_root_pure:
            return None
        discs = owner.identity_tokens(self.toponyms)
        if not discs:
            return None

        rival_owners = [o.official for o in self.owners[alias_norm] if o is not owner]
        contested = bool(rival_owners) and not any(
            o.is_root_pure for o in self.owners[alias_norm]
        )
        in_family = bool(owner.family_prefix) and self.family_size[owner.family_prefix] >= 2

        if not (contested or in_family):
            return {
                "classification": "MANUAL_REVIEW",
                "action": "MANUAL_REVIEW",
                "confidence": "low",
                "owner_expected": expected_name,
                "evidence": (
                    f"'{alias_norm}' no contiene los tokens que distinguen a "
                    f"'{owner.official}' ({sorted(discs)}), pero es su único "
                    f"reclamante y no tiene hermanos con el mismo prefijo."
                ),
            }

        reason = (
            f"comparte el prefijo '{owner.family_prefix}' con "
            f"{self.family_size.get(owner.family_prefix, 0)} registros"
            if in_family
            else f"la frase está disputada con {rival_owners} y ninguno es la raíz"
        )
        return {
            "classification": "PARENT_TO_CHILD_HIJACK",
            "action": "REMOVE_WRONG_ALIAS",
            "confidence": "high",
            "owner_expected": expected_name,
            "evidence": (
                f"'{alias_norm}' no contiene ninguno de los tokens que distinguen "
                f"a '{owner.official}' de su comprador raíz ({sorted(discs)}); "
                f"{reason}. La consulta al padre termina en la subentidad."
            ),
        }

    @staticmethod
    def _same_identity(a: Entity, b: Entity) -> bool:
        if a.nit and b.nit and a.nit == b.nit:
            return True
        if a.identity_key == b.identity_key:
            return True
        # Siglas oficiales de SECOP: "MINJUSTICIA" es una contracción de
        # "MINISTERIO DE JUSTICIA Y DEL DERECHO", no una entidad ajena.
        return _is_contraction(a, b) or _is_contraction(b, a)


def _is_contraction(short: Entity, long: Entity) -> bool:
    """¿``short`` es la sigla-contracción del nombre de ``long``?

    "minjusticia" se descompone en prefijos consecutivos de los tokens de
    contenido de "ministerio de justicia y del derecho".
    """
    compact = "".join(content_tokens(short.norm))
    if not compact or len(compact) > 24:
        return False
    pos = 0
    for token in content_tokens(long.norm):
        if pos >= len(compact):
            break
        matched = 0
        while (
            pos + matched < len(compact)
            and matched < len(token)
            and compact[pos + matched] == token[matched]
        ):
            matched += 1
        if matched >= 3:
            pos += matched
    return pos == len(compact)


# ---------------------------------------------------------------------------
# Informe
# ---------------------------------------------------------------------------


def build_report(db_path: Path) -> dict:
    records = json.loads(db_path.read_text(encoding="utf-8"))
    entities = [Entity(r) for r in records]
    audit = Audit(entities)

    findings: list[dict] = []
    for alias_norm in sorted(audit.owners):
        for owner in audit.owners[alias_norm]:
            verdict = audit.classify(alias_norm, owner)
            if verdict is None:
                continue
            findings.append(
                {
                    "alias": alias_norm,
                    "owner_actual": owner.official,
                    "owner_departamento": owner.departamento,
                    "owner_axis": owner.axis,
                    "owner_total_procesos": owner.total_procesos,
                    "owner_total_contratos": owner.total_contratos,
                    "owner_source_datasets": owner.source_datasets,
                    **verdict,
                }
            )

    # A. EXACT_MULTI_OWNER — el runtime sólo puede honrar a uno.
    multi_owner = []
    for alias_norm, owner_list in sorted(audit.owners.items()):
        names = [o.official for o in owner_list]
        if len(set(names)) < 2:
            continue
        multi_owner.append(
            {
                "alias": alias_norm,
                "owners": names,
                # EntityResolver._load_aliases sobrescribe: gana el ÚLTIMO del JSON.
                "exact_winner": names[-1],
                # _scan_index ordena por longitud desc con sort estable: el PRIMERO.
                "scan_winner": names[0],
                "same_logical_identity": len({o.identity_key for o in owner_list}) == 1,
            }
        )

    # F. POSSIBLE_SAME_LOGICAL_IDENTITY — grafías físicas rivales.
    by_identity: dict[str, list[Entity]] = defaultdict(list)
    for ent in entities:
        by_identity[ent.identity_key].append(ent)

    identity_groups = []
    for key, group in sorted(by_identity.items()):
        if len(group) < 2:
            continue
        shared = set.intersection(
            *({normalize_text(a) for a in e.aliases} for e in group)
        )
        identity_groups.append(
            {
                "identity_key": key,
                "action": "SAME_IDENTITY_REVIEW",
                "physical_names": [
                    {
                        "official_name": e.official,
                        "departamento": e.departamento,
                        "nit": e.nit,
                        "total_procesos": e.total_procesos,
                        "total_contratos": e.total_contratos,
                        "source_datasets": e.source_datasets,
                        "alias_count": len(e.aliases),
                    }
                    for e in group
                ],
                "shared_aliases": sorted(shared),
            }
        )

    counts: dict[str, int] = defaultdict(int)
    for f in findings:
        counts[f["classification"]] += 1

    removals = [f for f in findings if f["action"] == "REMOVE_WRONG_ALIAS"]
    affected = sorted({f["owner_actual"] for f in removals})

    return {
        "audit": "GAZETTEER_ALIAS_OWNERSHIP_R1C",
        "aliases_db": str(db_path.relative_to(ROOT)),
        "totals": {
            "entities": len(entities),
            "total_aliases": sum(len(e.aliases) for e in entities),
            "unique_normalized_aliases": len(audit.owners),
            "exact_multi_owner_groups": len(multi_owner),
            "possible_same_logical_identity_groups": len(identity_groups),
            "proposed_removals": len(removals),
            "entities_affected_by_removals": len(affected),
            **{k.lower(): v for k, v in sorted(counts.items())},
        },
        "exact_multi_owner": multi_owner,
        "possible_same_logical_identity": identity_groups,
        "findings": findings,
        "proposed_removals": [
            {"alias": f["alias"], "owner": f["owner_actual"], "classification": f["classification"]}
            for f in removals
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aliases-db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    report = build_report(args.aliases_db)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    print(json.dumps(report["totals"], ensure_ascii=False, indent=2))
    print(f"\n→ {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
