#!/usr/bin/env python3
"""ADR-073 GAZETTEER-WIDE SOURCE BINDING AUDIT — Schema-3-aware (read-only).

`audit_gazetteer_source_bindings_v1.py` quedó congelado como
``GAZETTEER_WIDE_V1_SCHEMA3_COMPATIBLE = NO``: su candidato exigía un
`identity_key` (``BUYER_UNIT:<canónico>`` o ``NIT:<número>``) que Schema 3
eliminó, y por eso 497 BuyerUnits cerraron ``NO_STABLE_IDENTITY`` sólo por no
tener llave, no por evidencia en contra.

Este auditor NO repite la medición física de V1: la CONSUME. V1 midió, para las
1.134 identidades, el universo que alcanza hoy el fallback productivo
(``SoQLBuilder._accent_fold_like``) agrupado por (forma física, NIT) en ambos
datasets, con `records.sha256` como índice de integridad. Lo incompatible con
Schema 3 era la adjudicación de identidad, no la observación de la fuente. Aquí
se readjudica con las autoridades de Schema 3, cada una por separado:

    BuyerUnit            -> canonical_name (llave operacional; NO es un NIT)
    FiscalAccreditation  -> sidecar schema 3, kind=ACCREDITATION
    SourceObservation    -> sidecar schema 3 + attestations del Directorio CCE
    SourceProjection     -> sidecar schema 3 (vigentes) / universo LIKE V1 (candidatos)
    BuyerGroups          -> app/data/buyer_groups.json (declarados, nunca inferidos)
    Ecosistemas          -> registros `kind=ecosystem` de V1 (SENA/ICBF)

Por qué el universo exacto se DERIVA sin red: una fila con ``campo = 'F'`` tiene
exactamente la grafía F; si F apareció en el universo LIKE, toda fila con esa
grafía satisface el LIKE (el LIKE plegado de acentos es una función de la
grafía). Con el universo LIKE COMPLETO (sin tope de 200 grupos), el universo
exacto de F es la suma de sus grupos. Proponer como binding TODAS las formas que
el LIKE alcanza deja ``exact == LIKE`` por construcción; la seguridad se
desplaza entonces a la pregunta que sí importa: ¿todas esas formas son ESTA
BuyerUnit y sólo ella?

Reglas duras (ADR-073 / ADR-074):

    BuyerUnit != NIT · mismo NIT != misma BuyerUnit · NIT compartido != BuyerGroup
    Directory NOT_FOUND != identidad refutada
    Directory ADMISSIBLE != READY_TO_PROMOTE
    similitud != autoridad · ausencia != cero · timeout != cero

El NIT compartido es una DIMENSIÓN que se reporta, nunca una regla de rechazo.

Sobre la corroboración del Directorio (no es circular, pero hay que decir por
qué): el Directorio se consultó con el NIT que la FUENTE publica junto a la
forma; que el NIT coincida es, por tanto, de construcción. Lo que aporta es otra
cosa: que ese NIT tiene EXACTAMENTE UNA cuenta oficial SECOP II y que su nombre
es token-igual al canónico (`ADMISSIBILITY_FISCAL_STRICT_V0`). Es evidencia
independiente sobre el NOMBRE del titular del NIT, no sobre el NIT.

Salidas (``--output``, por defecto ``artifacts/audits/gazetteer_source_bindings_schema3``):

    records.jsonl          1 línea por identidad con las 13 respuestas
    candidates.jsonl       SOURCE_BINDING_CANDIDATE con borrador schema 3 (NO promovido)
    review.jsonl           todo lo demás que no es binding vigente ni ecosistema
    inputs/directory_attestations_projection.jsonl
                           proyección mínima de las attestations (reproducible sin
                           el artefacto de 5 MB, con su sha256 de origen)
    summary.json · manifest.json

``--live-recheck`` añade ``live_recheck.jsonl``: re-observa en la fuente viva la
proyección exacta de cada candidato, detrás de una guarda de salud de fuente. Un
dataset degradado (p. ej. ``jbjy-vk9h`` republicado con 1.000 filas el
2026-09-23) se declara ``SOURCE_DEGRADED`` y no se mide: su cero no es un cero.

Uso:

    .venv/bin/python scripts/audits/audit_gazetteer_source_bindings_schema3.py
    .venv/bin/python scripts/audits/audit_gazetteer_source_bindings_schema3.py --live-recheck
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import subprocess
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.buyer_source_names import (  # noqa: E402
    BuyerSourceBinding,
    load_registry,
)
from app.utils.money import normalize_text  # noqa: E402

AUDIT_ID = "ADR073_GAZETTEER_WIDE_SOURCE_BINDING_AUDIT_SCHEMA3"
V1_DIR = ROOT / "artifacts" / "audits" / "gazetteer_source_bindings_v1"
DEFAULT_OUTPUT = ROOT / "artifacts" / "audits" / "gazetteer_source_bindings_schema3"
DEFAULT_ATTESTATIONS = (
    ROOT / "artifacts" / "audits" / "adr073_directory_identity_attestations_v1"
    / "directory_identity_attestations.jsonl"
)
SIDECAR = ROOT / "app" / "data" / "buyer_source_names.json"
AUTHORITY_FILES = (
    "app/data/aliases_db.json",
    "app/data/buyer_source_names.json",
    "app/data/buyer_groups.json",
    "app/data/icbf_regionals.json",
    "app/data/sena_units.json",
)
DATASETS: dict[str, str] = {"p6dx-8zbt": "entidad", "jbjy-vk9h": "nombre_entidad"}
DATASET_LABEL = {"p6dx-8zbt": "procesos", "jbjy-vk9h": "contratos"}

# Piso de salud por dataset. No es una estimación del tamaño: es el umbral bajo
# el cual un conteo total deja de ser creíble como "la fuente" (jbjy-vk9h tiene
# millones de filas; el 2026-09-23 amaneció con 1.000).
SOURCE_HEALTH_FLOOR: dict[str, int] = {"p6dx-8zbt": 5_000_000, "jbjy-vk9h": 1_000_000}

# Un dataset con menos del 1 % de las filas del otro no se proyecta en el
# borrador: la grafía canónica casi seguro NO es cómo esa BuyerUnit publica ahí
# (Universidad de Antioquia: 44.658 procesos / 34 contratos). Vincularla dejaría
# el comportamiento igual pero DECLARARÍA como proyección algo que no lo es; se
# retiene (fallback LIKE intacto) hasta descubrir la forma real por NIT. Mismo
# umbral de reporte que V1 §7.
COVERAGE_IMBALANCE_RATIO = 0.01

# Catálogos institucionales cuya declaración V1 ya aceptó como identidad
# acreditada de BuyerUnit (no de NIT).
DECLARED_CATALOGS = frozenset({"app/data/sena_units.json", "app/data/icbf_regionals.json"})

# ── Veredictos ──────────────────────────────────────────────────────────────
# Orden de evaluación = orden de la tupla: el PRIMER gate que falla da la clase,
# y todos los fallos quedan en `reasons` para que ninguno se esconda detrás de otro.
BOUND = "BOUND_VERIFIED"
BOUND_REVIEW = "BOUND_REVIEW"
ECOSYSTEM = "INSTITUTIONAL_ECOSYSTEM"
CANDIDATE = "SOURCE_BINDING_CANDIDATE"
REVIEW_CLASSES: tuple[str, ...] = (
    "INCOMPLETE_OBSERVATION",
    "NO_SOURCE_PRESENCE",
    "CANONICAL_COLLISION",
    "MULTI_NIT_REVIEW",
    "NO_OBSERVED_NIT",
    "MULTI_FORM_REVIEW",
    "NAME_EXTENSION_REVIEW",
    "CROSS_UNIT_FORM_COLLISION",
    "DECLARED_GROUP_MEMBER_REVIEW",
    "FISCAL_CONFLICT_REVIEW",
    "STRUCTURAL_SAFE_UNCORROBORATED",
)
VERDICTS: tuple[str, ...] = (BOUND, BOUND_REVIEW, ECOSYSTEM, CANDIDATE, *REVIEW_CLASSES)


# ── Utilidades ──────────────────────────────────────────────────────────────


def tokens_of(value: str | None) -> tuple[str, ...]:
    """Misma función de tokens que V1 (`[^\\W_]+` sobre `normalize_text`)."""
    import re

    return tuple(re.findall(r"[^\W_]+", normalize_text(value or ""), re.UNICODE))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def digits(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


CLAIMING_RELATIONS = frozenset({"IDENTICAL_BYTES", "EQUAL_TOKENS"})


def name_relation(canonical: str, form: str) -> str:
    if form == canonical:
        return "IDENTICAL_BYTES"
    ct, ft = tokens_of(canonical), tokens_of(form)
    if ct == ft:
        return "EQUAL_TOKENS"
    if ct and _contains_run(ft, ct):
        return "EXTENDS_CANONICAL"
    return "OTHER"


def _contains_run(haystack: tuple[str, ...], needle: tuple[str, ...]) -> bool:
    n = len(needle)
    return any(haystack[i:i + n] == needle for i in range(len(haystack) - n + 1))


def git(*args: str) -> str:
    try:
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True,
                              check=False).stdout.strip()
    except OSError:
        return ""


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
                    encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=False) + "\n")
            count += 1
    return count


# ── Entradas ────────────────────────────────────────────────────────────────


class InputIntegrityError(RuntimeError):
    """Una entrada no coincide con su índice de integridad. No se adjudica nada."""


def load_v1_records(v1_dir: Path = V1_DIR) -> list[dict]:
    """Registros V1 verificados contra `records.sha256` ANTES de leerlos.

    Un registro alterado o ausente aborta: readjudicar sobre evidencia que ya
    no es la medida certificaría la alteración.
    """
    index = v1_dir / "records.sha256"
    if not index.exists():
        raise InputIntegrityError(f"falta el índice de integridad {index}")
    expected: dict[str, str] = {}
    for line in index.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        digest, rel = line.split(maxsplit=1)
        expected[rel.strip()] = digest
    on_disk = {f"records/{p.name}" for p in (v1_dir / "records").glob("*.json")}
    if on_disk != set(expected):
        raise InputIntegrityError(
            f"records/ no coincide con records.sha256: "
            f"{len(on_disk - set(expected))} sin índice, {len(set(expected) - on_disk)} ausentes"
        )
    records = []
    for rel, digest in sorted(expected.items()):
        path = v1_dir / rel
        if sha256_file(path) != digest:
            raise InputIntegrityError(f"{rel}: sha256 distinto del índice")
        records.append(json.loads(path.read_text(encoding="utf-8")))
    return records


def project_attestation(row: dict) -> dict:
    """Los campos del Directorio que esta auditoría lee. Nada más."""
    fiscal = row.get("fiscal_identity_evidence") or {}
    admissible = row.get("source_binding_admissible") or {}
    name_match = row.get("directory_name_match") or {}
    matched_nits = sorted({
        digits(m.get("directory_nit")) for m in fiscal.get("matched") or []
        if digits(m.get("directory_nit"))
    })
    return {
        "canonical_name": row["canonical_name"],
        "canonical_hash": row.get("canonical_hash"),
        "observed_nits": [digits(n) for n in row.get("observed_nits") or []],
        "fiscal_status": fiscal.get("status"),
        "matched_directory_nits": matched_nits,
        "name_match": name_match.get("status"),
        "admissibility": admissible.get("status"),
        "shared_nit": (row.get("shared_nit") or {}).get("status"),
        "directory_account_count": (row.get("directory_account_count") or {}).get("value"),
    }


def load_attestations(path: Path) -> tuple[dict[str, dict], str]:
    """Attestations del Directorio por canónico normalizado + sha256 de origen."""
    by_key: dict[str, dict] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if "fiscal_identity_evidence" in row:
                row = project_attestation(row)
            key = normalize_text(row["canonical_name"])
            if key in by_key:
                raise InputIntegrityError(f"attestation duplicada: {row['canonical_name']!r}")
            by_key[key] = row
    return by_key, sha256_file(path)


def load_declared_groups(path: Path = ROOT / "app" / "data" / "buyer_groups.json") -> dict[str, list[str]]:
    """canónico normalizado -> ids de grupos DECLARADOS que lo listan."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    groups = payload.get("groups", payload) if isinstance(payload, dict) else payload
    membership: dict[str, list[str]] = defaultdict(list)
    items = groups.items() if isinstance(groups, dict) else ((g.get("id"), g) for g in groups)
    for group_id, group in items:
        for member in (group or {}).get("members") or []:
            name = member if isinstance(member, str) else member.get("canonical_name")
            if name:
                membership[normalize_text(name)].append(str(group_id))
    return membership


# ── Modelo de una identidad ─────────────────────────────────────────────────


@dataclass
class Unit:
    canonical: str
    kind: str
    v1_status: str
    v1_identity: dict | None
    declared_groups: list[str]
    datasets: dict[str, dict]  # ds -> {status, complete, universe:[{name,nit,rows}]}
    binding: BuyerSourceBinding | None
    attestation: dict | None
    v1_record_hash: str
    v1_binding_verdict: dict = field(default_factory=dict)
    v1_bound: dict[str, dict] = field(default_factory=dict)


def unit_from_v1(raw: dict, registry, attestations: dict[str, dict],
                 group_membership: dict[str, list[str]]) -> Unit:
    record = raw["record"]
    measurement = raw.get("measurement") or {}
    canonical = record["canonical_name"]
    key = normalize_text(canonical)
    datasets: dict[str, dict] = {}
    bound: dict[str, dict] = {}
    for ds in DATASETS:
        summary = (record.get("datasets") or {}).get(ds) or {}
        measured = (measurement.get("datasets") or {}).get(ds) or {}
        datasets[ds] = {
            "status": summary.get("like_status"),
            "complete": measured.get("like_universe_complete"),
            "universe": list(measured.get("like_universe") or []),
        }
        bound[ds] = {
            "status": summary.get("bound_status"),
            "rows": summary.get("bound_rows"),
            "universe": list(summary.get("bound_universe") or []),
            "forms_not_reached_by_like": summary.get("forms_not_reached_by_like"),
        }
    declared = sorted(set(record.get("declared_groups") or []) | set(group_membership.get(key, [])))
    return Unit(
        canonical=canonical,
        kind=record.get("kind") or "buyer_unit",
        v1_status=record.get("status"),
        v1_identity=record.get("identity"),
        declared_groups=[str(g) if not isinstance(g, dict) else str(g.get("id")) for g in declared],
        datasets=datasets,
        binding=registry.binding_for(canonical) if record.get("kind") != "ecosystem" else None,
        attestation=attestations.get(key),
        v1_record_hash=record.get("canonical_hash"),
        v1_binding_verdict=raw.get("binding_verdict") or {},
        v1_bound=bound,
    )


# ── Índices de colisión entre BuyerUnits ────────────────────────────────────


@dataclass
class CollisionIndex:
    canonical_owner: dict[str, list[str]]          # canónico normalizado -> canónicos
    bound_form_owner: dict[str, set[str]]          # forma normalizada -> canónicos con binding
    proposed_form_owner: dict[str, set[str]]       # forma normalizada -> canónicos sin binding cuyo NOMBRE es esa forma
    like_reach: dict[str, set[str]]                # forma normalizada -> canónicos sin binding cuyo LIKE la alcanza


def proposed_forms(unit: Unit) -> dict[str, list[str]]:
    """Formas que el fallback LIKE alcanza hoy, por dataset con filas."""
    out: dict[str, list[str]] = {}
    for ds, data in unit.datasets.items():
        names = sorted({g.get("name") for g in data["universe"] if g.get("name") and int(g.get("rows") or 0) > 0})
        if names:
            out[ds] = names
    return out


def build_collision_index(units: list[Unit], registry) -> CollisionIndex:
    canonical_owner: dict[str, list[str]] = defaultdict(list)
    for unit in units:
        if unit.kind == "buyer_unit":
            canonical_owner[normalize_text(unit.canonical)].append(unit.canonical)
    bound_form_owner: dict[str, set[str]] = defaultdict(set)
    for binding in registry.bindings():
        for names in binding.source_names.values():
            for name in names:
                bound_form_owner[normalize_text(name)].add(binding.canonical_name)
    # Sólo RECLAMA una forma la unidad cuyo nombre ES esa forma (bytes o tokens
    # iguales). Que el LIKE de otra unidad la alcance por substring —`ANI`
    # alcanza `ARTESANIAS DE COLOMBIA S.A`— es un defecto de ESA otra unidad, no
    # una segunda identidad para la forma; se reporta aparte (`reached_by_like_of`).
    proposed_form_owner: dict[str, set[str]] = defaultdict(set)
    like_reach: dict[str, set[str]] = defaultdict(set)
    for unit in units:
        if unit.kind != "buyer_unit" or unit.binding is not None:
            continue
        for names in proposed_forms(unit).values():
            for name in names:
                like_reach[normalize_text(name)].add(unit.canonical)
                if name_relation(unit.canonical, name) in CLAIMING_RELATIONS:
                    proposed_form_owner[normalize_text(name)].add(unit.canonical)
    return CollisionIndex(canonical_owner, bound_form_owner, proposed_form_owner, like_reach)


# ── Adjudicación ────────────────────────────────────────────────────────────


def fiscal_view(unit: Unit) -> dict:
    """Pregunta 2 y 3: evidencia fiscal positiva y lo que observó el Directorio."""
    accreditations, observations = [], []
    if unit.binding is not None:
        for item in unit.binding.fiscal_attestations:
            accreditations.append({"scheme": item.scheme, "base_number": item.base_number,
                                   "source": item.source, "observed_at": item.observed_at.isoformat()})
        for item in unit.binding.source_observations:
            observations.append({"scheme": item.scheme, "base_number": item.base_number,
                                 "source": item.source, "outcome": item.outcome})
    catalog = None
    ident = unit.v1_identity or {}
    if ident.get("accredited") and ident.get("provenance") in DECLARED_CATALOGS:
        catalog = {"type": ident.get("type"), "provenance": ident.get("provenance")}
    att = unit.attestation
    return {
        "accreditations": accreditations,
        "sidecar_source_observations": observations,
        "declared_catalog": catalog,
        "directory": None if att is None else {
            "fiscal_status": att.get("fiscal_status"),
            "admissibility": att.get("admissibility"),
            "name_match": att.get("name_match"),
            "observed_nits": att.get("observed_nits"),
            "matched_directory_nits": att.get("matched_directory_nits"),
            "shared_nit": att.get("shared_nit"),
            "account_count": att.get("directory_account_count"),
        },
    }


def dataset_view(unit: Unit) -> dict:
    """Preguntas 4-8 y 10 por dataset."""
    out = {}
    for ds, data in unit.datasets.items():
        universe = data["universe"]
        like_rows = sum(int(g.get("rows") or 0) for g in universe)
        forms = sorted({g.get("name") for g in universe if g.get("name")})
        nits = sorted({digits(g.get("nit")) or "∅" for g in universe})
        entry: dict[str, Any] = {
            "label": DATASET_LABEL[ds],
            "like_status": data["status"],
            "like_universe_complete": data["complete"],
            "physical_forms": [
                {"name": g.get("name"), "nit": digits(g.get("nit")) or None, "rows": int(g.get("rows") or 0)}
                for g in universe
            ],
            "forms": forms,
            "nits": nits,
            "fallback_like_rows": like_rows if data["status"] == "OK" else None,
        }
        if unit.binding is not None:
            declared = list(unit.binding.names_for(ds) or [])
            bound = unit.v1_bound.get(ds) or {}
            entry.update({
                "projection": "BOUND",
                "bound_source_names": declared,
                "exact_rows": bound.get("rows"),
                "exact_status": bound.get("status"),
                "exact_nits": sorted({digits(g.get("nit")) or "∅" for g in bound.get("universe") or []}),
                "forms_lost_vs_identity": bound.get("forms_not_reached_by_like"),
            })
        else:
            proposed = proposed_forms(unit).get(ds, [])
            exact_rows = sum(int(g.get("rows") or 0) for g in universe if g.get("name") in proposed)
            complete = data["status"] == "OK" and data["complete"] is True
            entry.update({
                "projection": "PROPOSED" if proposed else "NONE",
                "proposed_source_names": proposed,
                "exact_rows_derived": exact_rows if complete else None,
                "equivalent_to_fallback": (exact_rows == like_rows) if complete else None,
                # Formas del NIT que el LIKE no alcanza: V1 sólo las midió con NIT
                # acreditado. Sin esa medición, la pérdida frente a la IDENTIDAD no
                # se conoce; la pérdida frente al FALLBACK es cero por construcción.
                "forms_lost_vs_fallback": [] if complete else None,
                "forms_lost_vs_identity": "NOT_MEASURED",
            })
        out[ds] = entry
    return out


def adjudicate(unit: Unit, index: CollisionIndex) -> dict:
    """Una identidad -> veredicto + todas las razones, sin cortocircuitar la evidencia."""
    fiscal = fiscal_view(unit)
    datasets = dataset_view(unit)
    reasons: list[str] = []
    failed: list[str] = []

    def fail(cls: str, reason: str) -> None:
        failed.append(cls)
        reasons.append(f"{cls}: {reason}")

    base = {
        "canonical_name": unit.canonical,
        "canonical_hash": unit.v1_record_hash,
        "kind": unit.kind,
        "v1_status": unit.v1_status,
        "declared_groups": unit.declared_groups,
        "ecosystem": unit.kind == "ecosystem",
        "fiscal": fiscal,
        "datasets": datasets,
    }

    if unit.kind == "ecosystem":
        return {**base, "verdict": ECOSYSTEM, "reasons": ["composición institucional, no BuyerUnit"],
                "candidate": None}

    if unit.binding is not None:
        verdict = unit.v1_binding_verdict or {}
        drift = verdict.get("missing_names") or verdict.get("foreign_nits")
        ok = unit.v1_status == "ALREADY_BOUND_VERIFIED" and not drift
        if not ok:
            reasons.append(f"V1 status {unit.v1_status}; drift={bool(drift)}")
        directory = fiscal["directory"] or {}
        accredited_bases = {a["base_number"] for a in fiscal["accreditations"]}
        if accredited_bases and directory.get("admissibility") == "ADMISSIBLE" and not (
            accredited_bases & set(directory.get("observed_nits") or [])
        ):
            reasons.append("nota: NIT acreditado distinto del NIT observado por el Directorio "
                           "(se reporta; ninguna fuente invalida a otra)")
        return {**base, "verdict": BOUND if ok else BOUND_REVIEW,
                "reasons": reasons or ["binding vigente re-observado en V1 sin drift"],
                "candidate": None}

    # ── Sin binding: ¿puede proponerse uno sin cambiar semántica? ──
    for ds, data in unit.datasets.items():
        if data["status"] != "OK" or data["complete"] is not True:
            fail("INCOMPLETE_OBSERVATION",
                 f"{ds}: like_status={data['status']} complete={data['complete']} — "
                 "sin universo completo no hay equivalencia que afirmar")

    forms_by_ds = proposed_forms(unit)
    if not forms_by_ds and "INCOMPLETE_OBSERVATION" not in failed:
        fail("NO_SOURCE_PRESENCE", "el fallback no alcanza filas en ningún dataset")

    key = normalize_text(unit.canonical)
    if len(index.canonical_owner.get(key, [])) > 1:
        fail("CANONICAL_COLLISION", f"canónicos que normalizan igual: {index.canonical_owner[key]}")

    observed_nits = sorted({
        digits(g.get("nit")) or "∅"
        for data in unit.datasets.values() for g in data["universe"] if int(g.get("rows") or 0) > 0
    })
    real_nits = [n for n in observed_nits if n != "∅"]
    if len(observed_nits) > 1:
        fail("MULTI_NIT_REVIEW",
             f"el universo alcanza {len(observed_nits)} NIT distintos {observed_nits[:6]} — "
             "una proyección exacta acreditaría la mezcla")
    elif observed_nits == ["∅"]:
        fail("NO_OBSERVED_NIT", "la fuente no publica NIT para estas filas")

    all_forms = sorted({f for names in forms_by_ds.values() for f in names})
    token_groups = {tokens_of(f) for f in all_forms}
    for ds, names in forms_by_ds.items():
        if len(names) > 1:
            fail("MULTI_FORM_REVIEW", f"{ds}: {len(names)} formas físicas {names[:4]}")

    relations = {f: name_relation(unit.canonical, f) for f in all_forms}
    extended = [f for f, rel in relations.items() if rel not in CLAIMING_RELATIONS]
    if extended:
        fail("NAME_EXTENSION_REVIEW",
             f"formas con tokens distintos del canónico {extended[:3]} — el LIKE las alcanza por "
             "substring; que sean ESTA BuyerUnit no está acreditado")

    like_reached_by: dict[str, list[str]] = {}
    for form in all_forms:
        nform = normalize_text(form)
        others = sorted(
            (index.bound_form_owner.get(nform, set()) | index.proposed_form_owner.get(nform, set()))
            - {unit.canonical}
        )
        other_canonicals = [c for c in index.canonical_owner.get(nform, []) if c != unit.canonical]
        if others or other_canonicals:
            fail("CROSS_UNIT_FORM_COLLISION",
                 f"{form!r} también es forma/canónico de {sorted(set(others) | set(other_canonicals))[:4]}")
        reached_by = sorted(index.like_reach.get(nform, set()) - {unit.canonical})
        if reached_by:
            like_reached_by[form] = reached_by

    if unit.declared_groups:
        fail("DECLARED_GROUP_MEMBER_REVIEW",
             f"miembro declarado de {unit.declared_groups}; el binding de unidad no se adjudica en lote")

    directory = fiscal["directory"] or {}
    corroboration = None
    if directory.get("admissibility") == "ADMISSIBLE":
        dir_nits = set(directory.get("observed_nits") or [])
        if real_nits and dir_nits == set(real_nits):
            corroboration = "SECOP_DIRECTORY_ADMISSIBLE"
        else:
            fail("FISCAL_CONFLICT_REVIEW",
                 f"Directorio ADMISSIBLE sobre {sorted(dir_nits)} pero la fuente publica {real_nits}")
    if corroboration is None and fiscal["declared_catalog"]:
        corroboration = "DECLARED_CATALOG"
    if corroboration is None and "FISCAL_CONFLICT_REVIEW" not in failed:
        fail("STRUCTURAL_SAFE_UNCORROBORATED",
             f"sin corroboración independiente (Directorio: admissibility="
             f"{directory.get('admissibility')}, status={directory.get('fiscal_status')}); "
             "NOT_FOUND/REVIEW no refuta la identidad, sólo no la corrobora")

    if failed:
        verdict = next(cls for cls in REVIEW_CLASSES if cls in failed)
        return {**base, "verdict": verdict, "reasons": reasons, "failed_gates": sorted(set(failed)),
                "corroboration": corroboration, "candidate": None,
                "structural": {"forms": all_forms, "token_groups": len(token_groups),
                               "observed_nits": observed_nits, "reached_by_like_of": like_reached_by}}

    rows_by_ds = {ds: datasets[ds]["exact_rows_derived"] or 0 for ds in DATASETS}
    withheld: dict[str, str] = {}
    top = max(rows_by_ds.values())
    for ds in forms_by_ds:
        if top and rows_by_ds[ds] < COVERAGE_IMBALANCE_RATIO * top:
            withheld[ds] = (f"IMBALANCED_COVERAGE: {rows_by_ds[ds]} filas vs {top} en el otro dataset; "
                            "la forma real de esta BuyerUnit ahí no está descubierta (requiere búsqueda por NIT)")
    projected = {ds: names for ds, names in forms_by_ds.items() if ds not in withheld}
    candidate = {
        "canonical_name": unit.canonical,
        "source_names": projected,
        "observed_rows": {ds: rows_by_ds[ds] for ds in projected},
        "withheld_datasets": withheld,
        "observed_at": "2026-09-18",
        "observed_nit": real_nits[0],
        "corroboration": corroboration,
        "name_relation": {f: relations[f] for f in all_forms},
        # Otras unidades SIN binding cuyo fallback LIKE hoy también alcanza esta
        # forma. No bloquea: el binding de ESTA unidad no cambia el LIKE de ellas.
        "reached_by_like_of": like_reached_by,
        "datasets_without_presence": sorted(set(DATASETS) - set(forms_by_ds)),
        "evidence_record": f"artifacts/audits/gazetteer_source_bindings_v1/records/{unit.v1_record_hash}.json",
        "promotion_status": "NOT_PROMOTED",
    }
    return {**base, "verdict": CANDIDATE,
            "reasons": ["una forma por dataset, un NIT, tokens = canónico, sin colisión, "
                        f"exact == fallback, corroborado por {corroboration}"],
            "corroboration": corroboration, "candidate": candidate}


# ── Re-observación viva (opcional) ──────────────────────────────────────────


def _load_v1_module():
    spec = importlib.util.spec_from_file_location(
        "audit_gazetteer_source_bindings_v1", ROOT / "scripts" / "audits" / "audit_gazetteer_source_bindings_v1.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module  # los @dataclass del módulo resuelven su namespace aquí
    spec.loader.exec_module(module)
    return module


def source_health(source, v1) -> dict[str, dict]:
    health = {}
    for ds in DATASETS:
        result = source.query(ds, "SELECT count(*) AS cnt")
        total = v1.first_scalar(result.rows) if result.status == "OK" else None
        healthy = total is not None and total >= SOURCE_HEALTH_FLOOR[ds]
        health[ds] = {"status": result.status, "count": total, "floor": SOURCE_HEALTH_FLOOR[ds],
                      "healthy": healthy}
    return health


def live_recheck(candidates: list[dict], workers: int, timeout: float) -> tuple[list[dict], dict]:
    """Proyección exacta viva de cada candidato, sólo en datasets sanos.

    Mide `campo = 'F'` agrupado por (forma, NIT) con la primitiva del runtime.
    PASS exige: filas > 0 y un único NIT igual al observado en V1. El conteo
    puede crecer (la fuente acumula); una caída > 10 % se marca DRIFT.
    """
    v1 = _load_v1_module()
    settings = v1.read_only_settings()
    source = v1.Source(settings, transport="soda3", timeout_seconds=timeout, retries=2)
    health = source_health(source, v1)

    def one(candidate: dict) -> dict:
        out = {"canonical_name": candidate["canonical_name"], "datasets": {}}
        for ds, names in candidate["source_names"].items():
            if not health[ds]["healthy"]:
                out["datasets"][ds] = {"status": "SOURCE_DEGRADED", "health": health[ds]}
                continue
            where = v1.exact_clause(DATASETS[ds], names)
            result = source.query(ds, v1.grouped_universe_soql(DATASETS[ds], where))
            if result.status != "OK":
                out["datasets"][ds] = {"status": result.status, "error": result.error}
                continue
            groups = v1.parse_grouped(result.rows, DATASETS[ds])
            rows_now = sum(g["rows"] for g in groups)
            nits_now = sorted({digits(g["nit"]) or "∅" for g in groups})
            rows_then = candidate["observed_rows"][ds]
            if rows_now == 0:
                status = "FORM_ABSENT"
            elif nits_now != [candidate["observed_nit"]]:
                status = "NIT_DRIFT"
            elif rows_now < 0.9 * rows_then:
                status = "ROW_DRIFT"
            else:
                status = "PASS"
            out["datasets"][ds] = {"status": status, "rows_v1": rows_then, "rows_now": rows_now,
                                   "nits_now": nits_now, "where": where}
        statuses = {d["status"] for d in out["datasets"].values()}
        measured = statuses - {"SOURCE_DEGRADED"}
        out["verdict"] = ("PASS" if measured == {"PASS"} else
                          "NOT_MEASURED" if not measured else "FAIL")
        return out

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(one, candidates))
    return results, {"health": health, "telemetry": dict(source.telemetry),
                     "measured_at": datetime.now(timezone.utc).isoformat()}


# ── Resumen ────────────────────────────────────────────────────────────────


def build_summary(records: list[dict], live: list[dict] | None) -> dict:
    verdicts = Counter(r["verdict"] for r in records)
    buyer_units = [r for r in records if r["kind"] == "buyer_unit"]
    unbound = [r for r in buyer_units if r["verdict"] not in {BOUND, BOUND_REVIEW}]
    admissible_unbound = [r for r in unbound
                          if ((r["fiscal"]["directory"] or {}).get("admissibility") == "ADMISSIBLE")]
    gate_hits = Counter(g for r in unbound for g in r.get("failed_gates", []))
    directory_x_verdict = Counter(
        f"{(r['fiscal']['directory'] or {}).get('admissibility')}→{r['verdict']}" for r in buyer_units
    )
    shared = Counter(
        f"{(r['fiscal']['directory'] or {}).get('shared_nit')}→{r['verdict']}" for r in unbound
    )
    candidates = [r for r in records if r["verdict"] == CANDIDATE]
    summary = {
        "audit": AUDIT_ID,
        "total_identities": len(records),
        "buyer_units": len(buyer_units),
        "ecosystems": verdicts.get(ECOSYSTEM, 0),
        "verdicts": {v: verdicts.get(v, 0) for v in VERDICTS},
        "bound": {"verified": verdicts.get(BOUND, 0), "review": verdicts.get(BOUND_REVIEW, 0)},
        "unbound_buyer_units": len(unbound),
        "directory_admissible_unbound": len(admissible_unbound),
        "directory_admissible_unbound_to_candidate": sum(
            1 for r in admissible_unbound if r["verdict"] == CANDIDATE),
        "candidates": len(candidates),
        "candidates_by_corroboration": dict(Counter(r["corroboration"] for r in candidates)),
        "candidate_rows": {
            ds: sum((r["candidate"]["observed_rows"].get(ds) or 0) for r in candidates) for ds in DATASETS
        },
        "candidates_single_dataset": sum(1 for r in candidates if len(r["candidate"]["source_names"]) == 1),
        "candidates_absent_in_dataset": dict(Counter(
            ds for r in candidates for ds in r["candidate"]["datasets_without_presence"])),
        "candidates_withheld_dataset": dict(Counter(
            ds for r in candidates for ds in r["candidate"]["withheld_datasets"])),
        "candidates_low_volume_lt_50_rows": sorted(
            r["canonical_name"] for r in candidates
            if sum(r["candidate"]["observed_rows"].values()) < 50),
        "failed_gate_hits_unbound": dict(sorted(gate_hits.items())),
        "directory_admissibility_x_verdict": dict(sorted(directory_x_verdict.items())),
        "shared_nit_x_verdict_unbound": dict(sorted(shared.items())),
        "promotion": "NONE — este artefacto no modifica product data",
        "answers_limits": {
            "forms_lost_vs_identity_unbound": "NOT_MEASURED: V1 sólo buscó formas por NIT con NIT "
            "acreditado; para candidatos la pérdida frente al fallback productivo es 0 por construcción",
            "observation_date": "2026-09-18 (lote V1); jbjy-vk9h no se re-observa mientras siga degradado",
        },
    }
    if live is not None:
        summary["live_recheck"] = dict(Counter(r["verdict"] for r in live))
        summary["live_recheck_by_dataset"] = dict(Counter(
            f"{ds}:{d['status']}" for r in live for ds, d in r["datasets"].items()))
    return summary


# ── Orquestación ────────────────────────────────────────────────────────────


def run(args: argparse.Namespace) -> int:
    out: Path = args.output
    raw = load_v1_records(args.v1_dir)
    attestations, att_sha = load_attestations(args.attestations)
    registry = load_registry(SIDECAR)
    groups = load_declared_groups()
    units = [unit_from_v1(r, registry, attestations, groups) for r in raw]

    # Invariantes de inventario: el lote V1 debe cubrir exactamente el censo de hoy.
    buyer_units = [u for u in units if u.kind == "buyer_unit"]
    bound_keys = {normalize_text(b.canonical_name) for b in registry.bindings()}
    unit_keys = {normalize_text(u.canonical) for u in buyer_units}
    problems = []
    if bound_keys - unit_keys:
        problems.append(f"bindings sin registro V1: {sorted(bound_keys - unit_keys)[:5]}")
    missing_att = [u.canonical for u in buyer_units if u.attestation is None]
    if missing_att:
        problems.append(f"BuyerUnits sin attestation del Directorio: {missing_att[:5]} ({len(missing_att)})")
    if problems:
        for p in problems:
            print(f"INPUT: {p}", file=sys.stderr)
        return 2

    index = build_collision_index(units, registry)
    records = sorted((adjudicate(u, index) for u in units), key=lambda r: normalize_text(r["canonical_name"]))
    candidates = [r["candidate"] for r in records if r["verdict"] == CANDIDATE]

    live = None
    live_meta = None
    if args.live_recheck:
        live, live_meta = live_recheck(candidates, args.workers, args.timeout)
        write_jsonl(out / "live_recheck.jsonl", live)
        write_json(out / "live_recheck_meta.json", live_meta)
    elif (out / "live_recheck.jsonl").exists() and not args.discard_live:
        # Una corrida offline posterior conserva la medición viva y SU procedencia;
        # sin el meta, el resumen citaría un PASS cuya fuente y fecha nadie sabría.
        live = [json.loads(line) for line in (out / "live_recheck.jsonl").read_text(encoding="utf-8").splitlines() if line]
        live_meta = json.loads((out / "live_recheck_meta.json").read_text(encoding="utf-8"))
        live_names = {r["canonical_name"] for r in live}
        if live_names != {c["canonical_name"] for c in candidates}:
            print("INPUT: live_recheck.jsonl no cubre exactamente los candidatos actuales; "
                  "re-ejecutar con --live-recheck o --discard-live", file=sys.stderr)
            return 3

    write_jsonl(out / "records.jsonl", records)
    write_jsonl(out / "candidates.jsonl", candidates)
    write_jsonl(out / "review.jsonl", (r for r in records if r["verdict"] in REVIEW_CLASSES or r["verdict"] == BOUND_REVIEW))
    write_jsonl(out / "inputs" / "directory_attestations_projection.jsonl",
                sorted(attestations.values(), key=lambda a: normalize_text(a["canonical_name"])))
    summary = build_summary(records, live)
    write_json(out / "summary.json", summary)

    manifest = {
        "audit": AUDIT_ID,
        "head": git("rev-parse", "HEAD"),
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "script": "scripts/audits/audit_gazetteer_source_bindings_schema3.py",
        "script_sha256": sha256_file(Path(__file__)),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "network": ("live recheck en esta invocación" if args.live_recheck else
                    "none en esta invocación (readjudicación offline del lote V1"
                    + ("; live_recheck previo incorporado)" if live_meta else ")")),
        "inputs": {
            "v1_records_sha256_index": sha256_file(args.v1_dir / "records.sha256"),
            "v1_records_verified": len(raw),
            "v1_batch": json.loads((args.v1_dir / "manifest.json").read_text(encoding="utf-8")).get("batch", {}).get("head"),
            "directory_attestations": {"path": str(args.attestations.relative_to(ROOT)) if args.attestations.is_relative_to(ROOT) else str(args.attestations),
                                       "sha256": att_sha, "rows": len(attestations)},
            "authority_sha256": {p: sha256_file(ROOT / p) for p in AUTHORITY_FILES},
        },
        "live_recheck": live_meta,
    }
    write_json(out / "manifest.json", manifest)
    print(json.dumps({k: summary[k] for k in ("verdicts", "candidates", "candidates_by_corroboration",
                                              "directory_admissible_unbound",
                                              "directory_admissible_unbound_to_candidate")},
                     ensure_ascii=False, indent=1))
    if live is not None:
        print(json.dumps(summary.get("live_recheck"), ensure_ascii=False))
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--v1-dir", type=Path, default=V1_DIR)
    parser.add_argument("--attestations", type=Path, default=DEFAULT_ATTESTATIONS)
    parser.add_argument("--live-recheck", action="store_true")
    parser.add_argument("--discard-live", action="store_true",
                        help="no incorporar un live_recheck.jsonl previo al resumen")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args(argv)
    if not args.attestations.exists():
        fallback = args.output / "inputs" / "directory_attestations_projection.jsonl"
        if fallback.exists():
            args.attestations = fallback
    return args


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
