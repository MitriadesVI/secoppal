#!/usr/bin/env python3
"""ADR-073 GAZETTEER-WIDE SOURCE BINDING AUDIT V1 — read-only, resumible.

Recorre TODAS las identidades relevantes del gazetteer de compradores y deja
por escrito, para cada una, cómo aparece físicamente en `p6dx-8zbt` (procesos) y
`jbjy-vk9h` (contratos). No decide producto: MIDE y CLASIFICA.

Cuatro capas que este script nunca mezcla (ADR-073 / ADR-074):

    alias humano          -> input del resolver, no se toca aquí
    identidad canónica    -> clave estable ACREDITADA por una autoridad declarada
    nombre físico de fuente -> string que el dataset publica, byte-for-byte
    Buyer Group           -> composición DECLARADA de BuyerUnits distintas

Reglas de autoridad (no negociables en esta corrida):

1. `identity_key` sólo sale de una autoridad declarada: el sidecar
   `buyer_source_names.json`, o un catálogo institucional existente
   (`icbf_regionals.json`, `sena_units.json`). Un NIT observado en la fuente es
   EVIDENCIA, no acreditación: no habilita discovery ni binding por sí mismo.
2. Sin identidad acreditada -> `NO_STABLE_IDENTITY` (o `AMBIGUOUS_IDENTITY` si
   la evidencia física apunta a más de una identidad). No se inventa.
3. `SAME_NIT != SAME_BUYER_UNIT`: varias formas físicas bajo una identidad no se
   suman jamás en un binding; se declaran y se revisan.
4. Un timeout NUNCA se lee como equivalencia. `same_universe` es `None` cuando
   alguna de las dos mediciones no llegó.
5. El candidato de binding son EXACTAMENTE las formas físicas que el fallback
   LIKE del producto ya alcanza, y sólo si no alcanza identidades ajenas. Las
   formas reales que el LIKE no alcanza quedan declaradas y FUERA del candidato:
   incorporarlas ampliaría el universo en silencio, y eso es una decisión de
   producto (Buyer Group), no de proyección de nombres.

Los predicados no se reconstruyen a mano: el universo LIKE actual sale de
`SoQLBuilder._accent_fold_like(...)` y el candidato exacto de
`SoQLBuilder._exact_entity_clause(...)`, que son las mismas primitivas que
emiten producción. Reimplementarlas mediría una consulta que el producto nunca
manda.

Lectura de los datos: por defecto `soda3` (`/api/v3/views/{dataset}/query.json`),
que es además el transporte del runtime (ADR-070), con `--transport classic`
disponible. La latencia de Socrata resultó MUY variable y las sondas se
contradicen entre ventanas —el mismo `GROUP BY` con `unaccent(UPPER()) LIKE`
medido entre 4 y 126 segundos—, pero la medición que decide es la consulta que
este auditor más emite, la igualdad exacta sobre `jbjy-vk9h`: 63,9 s en classic
contra 2,6 s en soda3, y un LIKE que expiraba a 120 s en classic contra 50,7 s
en soda3. Lo que sí está probado, y es lo que importa para la validez del
resultado, es que **ambos transportes devuelven el MISMO universo** para estos
predicados: fingerprints idénticos en 4/4 casos
(`artifacts/audits/smoke_adr073_v1/transport_parity.json`). Presupuesto 60 s por
llamada, hasta 2 reintentos, workers=2; lo que no responda se clasifica
`INCONCLUSIVE` y se reprocesa con `--recheck-existing`, que es la única salida
honesta para un timeout.

Uso:

    .venv/bin/python scripts/audits/audit_gazetteer_source_bindings_v1.py \
        --resume --workers 2

Opciones: --resume, --workers, --limit, --start-after, --only,
--recheck-existing, --output, --transport, --timeout, --retries, --summary-only,
--env-file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.core.buyer_groups import get_groups  # noqa: E402
from app.core.buyer_source_names import BuyerSourceNameRegistry, load_registry  # noqa: E402
from app.core.soql_builder import SoQLBuilder  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

# ── Autoridades y datasets ──────────────────────────────────────────────────

PROCESSES = SoQLBuilder.PROCESOS_DATASET
CONTRACTS = SoQLBuilder.CONTRATOS_DATASET
DATASETS: tuple[tuple[str, str], ...] = ((PROCESSES, "entidad"), (CONTRACTS, "nombre_entidad"))
NIT_COLUMN = "nit_entidad"

AUTHORITY_FILES: dict[str, str] = {
    "aliases_db": "app/data/aliases_db.json",
    "buyer_source_names": "app/data/buyer_source_names.json",
    "buyer_groups": "app/data/buyer_groups.json",
    "icbf_regionals": "app/data/icbf_regionals.json",
    "sena_units": "app/data/sena_units.json",
}

DEFAULT_OUT = ROOT / "artifacts/audits/gazetteer_source_bindings_v1"

PROGRESS_FILE = "progress.jsonl"
SUPERSESSIONS_FILE = "progress_supersessions.jsonl"
RECORDS_SUBDIR = "records"
RECORDS_HASH_FILE = "records.sha256"

# Campos que DECIDEN el resultado de una identidad. Dos intentos del mismo
# canónico que difieran en alguno son contradictorios: elegir uno por orden de
# línea (last-write-wins) cambiaría la clasificación sin dejar rastro.
SUPERSESSION_FIELDS: tuple[str, ...] = (
    "status", "candidate", "already_bound", "binding", "identity",
)

# Procedencia del LOTE que el manifiesto hereda y NO reescribe: cambios de
# workers/transporte/script entre intentos son parte de la historia del lote.
# `retries` NO entra aquí: en el manifiesto viejo era la política por llamada, y
# en `batch` es lo OBSERVADO (`retry_budget` guarda la política).
BATCH_ADOPTED_KEYS: tuple[str, ...] = (
    "head", "branch", "script_sha256", "authority_sha256", "runtime_files_sha256",
    "transport", "workers", "timeout_seconds", "options",
)

# Claves que una versión anterior del manifiesto escribió en el nivel raíz
# describiendo el LOTE. Se ABSORBEN en `batch` una sola vez (ninguna se queda
# como telemetría recompuesta a mano) y, si el manifiesto ya tiene `batch`, se
# arrastran sin reescribirse.
LEGACY_BATCH_KEYS: tuple[str, ...] = (
    "batch_totals", "source_telemetry", "source_telemetry_reconstructed",
    "source_telemetry_note", "started_at", "finished_at", "elapsed_seconds",
    "processed_in_this_run", "processed_total_in_batch", "skipped_resume",
    "provenance_preserved_from_previous_manifest",
)

# Orden determinista de la clasificación. Cambiarlo cambia el resultado, así que
# cada regla vive en `classify()` con su razón explícita en `status_reasons`.
CLASSES: tuple[str, ...] = (
    "ALREADY_BOUND_VERIFIED",
    "BINDING_DRIFT_REVIEW",
    "EXACT_BINDABLE",
    "SOURCE_NAME_SPLIT",
    "MULTI_NAME_REVIEW",
    "BUYER_GROUP_MEMBER",
    "BUYER_GROUP_CANDIDATE",
    "INSTITUTIONAL_ECOSYSTEM",
    "NO_STABLE_IDENTITY",
    "AMBIGUOUS_IDENTITY",
    "FOREIGN_IDENTITY_COLLISION",
    "NOT_EQUIVALENT",
    "INCONCLUSIVE",
    "ERROR",
)

# Un universo agrupado que devuelve el tope pedido está TRUNCADO: el conteo
# observado deja de ser el universo y la equivalencia no se puede afirmar.
GROUP_LIMIT = 200
MAX_GROUP_SOURCE_NAMES = 40  # autoridad: app/core/buyer_groups.py

_WORD_RE = re.compile(r"[^\W_]+", re.UNICODE)


def tokens_of(value: str) -> tuple[str, ...]:
    """Tokens normalizados. Dos formas con los mismos tokens son la MISMA grafía
    (puntuación, acentos y mayúsculas no hacen dos nombres físicos)."""
    return tuple(_WORD_RE.findall(normalize_text(value or "")))


def digits_of(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_hash(name: str) -> str:
    return hashlib.sha1(normalize_text(name).encode("utf-8")).hexdigest()[:16]


def append_jsonl(path: Path, payload: dict, lock: threading.Lock) -> None:
    """Checkpoint atómico: una línea completa por registro, bajo lock."""
    line = json.dumps(payload, ensure_ascii=False) + "\n"
    with lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Bitácora de intentos, supersession y evidencia ──────────────────────────
#
# `progress.jsonl` es append-only. Reprocesar un canónico AGREGA un intento; no
# reemplaza al anterior en silencio. Cada intento se identifica
# (`record_id` = `<canonical_hash>:<attempt>`) y, cuando reemplaza a otro, lo
# declara él mismo (`supersedes_record_id` + `supersession_reason`) o lo declara
# el ledger explícito `progress_supersessions.jsonl` —para los intentos que midió
# una versión anterior del auditor, que no escribía esos campos—. Un duplicado
# sin declaración es un ERROR del reporte: el resumen NUNCA elige por orden de
# línea. La telemetría del lote se DERIVA de esta bitácora (no se recompone a
# mano) y distingue todos los intentos de los registros activos.


def record_digest(line: str) -> str:
    """sha256 del ASIENTO tal como quedó en `progress.jsonl` (la línea cruda)."""
    return hashlib.sha256(line.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ProgressEntry:
    line_number: int  # 1-based sobre progress.jsonl
    record_id: str
    canonical_hash: str
    attempt: int
    digest: str
    record: dict

    @property
    def declared_supersession(self) -> str | None:
        value = self.record.get("supersedes_record_id")
        return str(value) if value else None

    @property
    def started_at(self) -> str | None:
        value = self.record.get("started_at")
        return str(value) if value else None

    def brief(self) -> dict:
        return {
            "record_id": self.record_id, "line": self.line_number, "attempt": self.attempt,
            "record_sha256": self.digest, "status": self.record.get("status"),
            "started_at": self.started_at,
        }


def build_progress_index(path: Path) -> list[ProgressEntry]:
    """Un intento por línea. `record_id` es declarado o derivado del orden."""
    if not path.exists():
        return []
    entries: list[ProgressEntry] = []
    per_canonical: Counter = Counter()
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        record = json.loads(line)
        canonical = record["canonical_hash"]
        per_canonical[canonical] += 1
        attempt = int(record.get("attempt") or per_canonical[canonical])
        entries.append(ProgressEntry(
            line_number=number,
            record_id=str(record.get("record_id") or f"{canonical}:{attempt}"),
            canonical_hash=canonical,
            attempt=attempt,
            digest=record_digest(line),
            record=record,
        ))
    return entries


def read_supersessions(path: Path) -> tuple[dict[str, dict], list[str]]:
    """Declaraciones explícitas de reemplazo, indexadas por el id del SUCESOR.

    Sólo aplica a intentos que no pudieron autodeclararse. Cada declaración se
    valida después contra la bitácora real: canónico existente, sucesor posterior
    y reemplazo que corresponde al intento inmediatamente anterior.
    """
    declared: dict[str, dict] = {}
    problems: list[str] = []
    for row in read_jsonl(path):
        successor = row.get("successor_record_id")
        superseded = row.get("superseded_record_id")
        if not successor or not superseded:
            problems.append(f"{path.name}: declaración sin sucesor o sin reemplazado: "
                            f"{json.dumps(row, ensure_ascii=False)[:200]}")
            continue
        if str(successor) in declared:
            problems.append(f"{path.name}: {successor} declarado más de una vez")
            continue
        declared[str(successor)] = row
    return declared, problems


def divergence(previous: dict, current: dict) -> list[str]:
    """Campos que DECIDEN el resultado y cambian entre dos intentos."""
    return sorted(field for field in SUPERSESSION_FIELDS if previous.get(field) != current.get(field))


def _is_later(successor: dict, predecessor: dict) -> bool:
    """El sucesor tiene que ser posterior en el tiempo, no sólo en el archivo."""
    after, before = successor.get("started_at"), predecessor.get("started_at")
    if after and before:
        return str(after) > str(before)
    return True  # sin ventana declarada no se puede afirmar lo contrario


@dataclass
class Reconciliation:
    entries: list[ProgressEntry]
    attempts: dict[str, list[ProgressEntry]]     # canonical_hash -> intentos en orden
    active: dict[str, ProgressEntry]             # canonical_hash -> intento vigente
    supersessions: list[dict]                    # reemplazos declarados y verificados
    undeclared: list[dict]                       # repeticiones sin declaración → ERROR
    contradictory: list[dict]                    # sin declarar y con resultado divergente
    problems: list[str]

    @property
    def progress_lines(self) -> int:
        return len(self.entries)

    @property
    def canonical_unique(self) -> int:
        return len(self.attempts)

    @property
    def active_records(self) -> int:
        return len(self.active)

    @property
    def ok(self) -> bool:
        return not (self.problems or self.undeclared)

    def brief(self) -> dict:
        return {
            "progress_lines": self.progress_lines,
            "canonical_unique": self.canonical_unique,
            "superseded_records": len(self.supersessions),
            "active_records": self.active_records,
            "undeclared_duplicates": len(self.undeclared),
            "contradictory_records": len(self.contradictory),
            "problems": list(self.problems),
        }


def reconcile_progress(entries: list[ProgressEntry], declared: dict[str, dict]) -> Reconciliation:
    """Selección DETERMINISTA del intento vigente. Nada de last-write-wins.

    Un canónico con repeticiones sin declarar queda sin resultado final
    (`active` no lo contiene) y el reporte falla: ante dos mediciones
    contradictorias el auditor no elige, declara.
    """
    problems: list[str] = []
    attempts: dict[str, list[ProgressEntry]] = defaultdict(list)
    for entry in entries:
        attempts[entry.canonical_hash].append(entry)
    for group in attempts.values():
        group.sort(key=lambda item: item.line_number)

    supersessions: list[dict] = []
    superseded_ids: set[str] = set()
    used_declarations: set[str] = set()
    for canonical, group in attempts.items():
        for previous, current in zip(group, group[1:]):
            self_declared = current.declared_supersession
            row = declared.get(current.record_id)
            if self_declared is None and row is None:
                continue  # sin declarar: lo reporta el barrido de abajo
            used_declarations.add(current.record_id)
            if self_declared is not None and row is not None \
                    and str(row.get("superseded_record_id")) != self_declared:
                problems.append(
                    f"{SUPERSESSIONS_FILE}: {current.record_id} declara reemplazar {self_declared} "
                    f"y el ledger declara {row.get('superseded_record_id')}: no coinciden")
                continue
            if self_declared is not None:
                if self_declared != previous.record_id:
                    problems.append(
                        f"línea {current.line_number} ({current.record_id}) declara reemplazar "
                        f"{self_declared}, que no es el intento anterior {previous.record_id}")
                    continue
                source = "record"
                reason = str(current.record.get("supersession_reason") or "no declarada")
            else:
                if row is None:  # inalcanzable: la guarda de arriba ya lo descartó
                    continue
                if str(row.get("superseded_record_id")) != previous.record_id:
                    problems.append(
                        f"{SUPERSESSIONS_FILE}: {current.record_id} declara reemplazar "
                        f"{row.get('superseded_record_id')}, que no es el intento anterior "
                        f"{previous.record_id}")
                    continue
                source = "ledger"
                reason = str(row.get("reason") or "no declarada")
            if previous.record_id in superseded_ids:
                problems.append(f"línea {previous.line_number} ({previous.record_id}) "
                                f"reemplazada más de una vez")
                continue
            if not _is_later(current.record, previous.record):
                problems.append(
                    f"línea {current.line_number} ({current.record_id}) declara reemplazar a "
                    f"{previous.record_id}, pero no es posterior en el tiempo")
                continue
            superseded_ids.add(previous.record_id)
            supersessions.append({
                "canonical_hash": canonical,
                "canonical_name": previous.record.get("canonical_name"),
                "superseded": previous.brief(),
                "successor": current.brief(),
                "declared_by": source,
                "reason": reason,
                "changed_fields": divergence(previous.record, current.record),
            })

    for successor in sorted(set(declared) - used_declarations):
        problems.append(f"{SUPERSESSIONS_FILE}: declara un sucesor {successor} que no es "
                        f"un intento de esta bitácora")

    undeclared: list[dict] = []
    contradictory: list[dict] = []
    undetermined: set[str] = set()
    for canonical, group in attempts.items():
        for entry in group[:-1]:
            if entry.record_id in superseded_ids:
                continue
            changed = divergence(entry.record, group[-1].record)
            undeclared.append(dict(entry.brief(), canonical_hash=canonical,
                                   canonical_name=entry.record.get("canonical_name"),
                                   attempts_in_file=len(group),
                                   final_record_id=group[-1].record_id))
            if changed:
                contradictory.append({
                    "canonical_hash": canonical,
                    "canonical_name": entry.record.get("canonical_name"),
                    "record_id": entry.record_id,
                    "line": entry.line_number,
                    "final_record_id": group[-1].record_id,
                    "final_line": group[-1].line_number,
                    "divergent_fields": changed,
                })
            undetermined.add(canonical)

    active = {canonical: group[-1] for canonical, group in attempts.items()
              if canonical not in undetermined}
    return Reconciliation(entries=entries, attempts=attempts, active=active,
                          supersessions=supersessions, undeclared=undeclared,
                          contradictory=contradictory, problems=problems)


def measurement_window(entries: Iterable[ProgressEntry]) -> dict:
    """Ventana de MEDICIÓN del lote, derivada de la bitácora (no de un log externo)."""
    started = [entry.started_at for entry in entries if entry.started_at]
    finished = [str(entry.record.get("finished_at")) for entry in entries
                if entry.record.get("finished_at")]
    window: dict[str, Any] = {"started_at": None, "finished_at": None, "elapsed_seconds": None}
    if not started or not finished:
        return window
    window["started_at"] = min(started)
    window["finished_at"] = max(finished)
    try:
        delta = datetime.fromisoformat(window["finished_at"]) - datetime.fromisoformat(window["started_at"])
        window["elapsed_seconds"] = round(delta.total_seconds(), 1)
    except ValueError:
        window["elapsed_seconds"] = None
    return window


def _sum_network(entries: Sequence[ProgressEntry]) -> dict:
    calls = timeouts = retries = 0
    latency = 0.0
    for entry in entries:
        network = entry.record.get("network") or {}
        calls += int(network.get("calls") or 0)
        timeouts += int(network.get("timeouts") or 0)
        retries += int(network.get("retries") or 0)
        latency += float(network.get("latency_ms") or 0.0)
    return {
        "records": len(entries),
        "source_calls": calls,
        "timeouts": timeouts,
        "retries": retries,
        "latency_ms_total": round(latency, 1),
    }


def batch_telemetry(reconciliation: Reconciliation) -> dict:
    """Telemetría DERIVADA de la bitácora, nunca recompuesta a mano.

    El lote tuvo reprocesos: sus llamadas son reales aunque el intento ya no sea
    el vigente. Por eso `all_attempts` y `active_records` son distintos, y la
    diferencia se declara en vez de esconderse.
    """
    superseded_ids = {item["superseded"]["record_id"] for item in reconciliation.supersessions}
    superseded = [entry for entry in reconciliation.entries if entry.record_id in superseded_ids]
    all_attempts = _sum_network(reconciliation.entries)
    active_records = _sum_network(list(reconciliation.active.values()))
    return {
        "all_attempts": all_attempts,
        "active_records": active_records,
        "superseded_attempts": _sum_network(superseded),
        "measurement_window": measurement_window(reconciliation.entries),
        "derived_from": PROGRESS_FILE,
        "note": ("Suma del bloque `network` de cada asiento de progress.jsonl. "
                 "`all_attempts` incluye los intentos reemplazados; `active_records` sólo "
                 "los vigentes. La diferencia son las llamadas de los reprocesos."),
    }


def records_relative_path(canonical_hash: str) -> str:
    return f"{RECORDS_SUBDIR}/{canonical_hash}.json"


def parse_records_hash_file(path: Path) -> tuple[dict[str, str], list[str]]:
    declared: dict[str, str] = {}
    problems: list[str] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        digest, separator, relative = line.partition("  ")
        if not separator or len(digest) != 64:
            problems.append(f"{path.name}:{number}: la línea no tiene la forma `<sha256>  <path>`")
            continue
        if relative in declared:
            problems.append(f"{path.name}:{number}: {relative} aparece más de una vez")
            continue
        declared[relative] = digest
    return declared, problems


def write_records_hash_file(out: Path, relative_paths: Sequence[str]) -> int:
    """`records.sha256` determinista: una entrada por registro ACTIVO, path + sha256."""
    lines = [f"{sha256_file(out / relative)}  {relative}\n" for relative in relative_paths]
    (out / RECORDS_HASH_FILE).write_text("".join(sorted(lines)), encoding="utf-8")
    return len(lines)


def verify_evidence(out: Path, expected: Sequence[str]) -> dict:
    """Integridad por registro, ANTES de construir el resumen.

    Un raw record alterado, ilegible o sin entrada en `records.sha256` no pasa en
    silencio: el resumen lo publica y la corrida termina con código ≠ 0. Si el
    archivo de hashes no existe todavía se crea (primer contacto), pero si existe
    NO se reescribe: regenerarlo sobre evidencia alterada lavaría la alteración.
    """
    hash_path = out / RECORDS_HASH_FILE
    declared: dict[str, str] = {}
    problems: list[str] = []
    created = False
    if hash_path.exists():
        declared, problems = parse_records_hash_file(hash_path)
    elif expected:
        write_records_hash_file(out, expected)
        declared, problems = parse_records_hash_file(hash_path)
        created = True
    report = {
        "hash_file": RECORDS_HASH_FILE,
        "hash_file_created_by_this_run": created,
        "declared_entries": len(declared),
        "expected": len(expected),
        "checked": 0,
        "missing": [],
        "unparsable": [],
        "mismatched": [],
        "undeclared": [],
        "problems": problems,
    }
    for relative in expected:
        path = out / relative
        if not path.exists():
            report["missing"].append(relative)
            continue
        try:
            json.loads(path.read_text(encoding="utf-8"))
        except ValueError as exc:
            report["unparsable"].append({"path": relative, "error": f"{type(exc).__name__}: {exc}"})
            continue
        digest = sha256_file(path)
        if relative not in declared:
            report["undeclared"].append(relative)
            continue
        if declared[relative] != digest:
            report["mismatched"].append({"path": relative, "declared": declared[relative],
                                         "observed": digest})
            continue
        report["checked"] += 1
    report["ok"] = not (report["missing"] or report["unparsable"] or report["mismatched"]
                        or report["undeclared"] or report["problems"])
    return report


def report_problems(reconciliation: Reconciliation, integrity: dict) -> list[str]:
    """Todo lo que impide leer el resumen como un resultado cerrado.

    Cada fallo concreto de integridad se enumera: un `ok: false` sin motivo
    obligaría a abrir el archivo de hashes a mano para entender qué se rompió.
    """
    problems: list[str] = [
        *reconciliation.problems,
        *[f"progress.jsonl:{item['line']}: repetición sin declarar del canónico "
          f"{item['canonical_name']!r} ({item['canonical_hash']})"
          for item in reconciliation.undeclared],
        *[f"{RECORDS_HASH_FILE}: {problem}" for problem in integrity.get("problems", [])],
    ]
    problems += [f"evidencia ausente: {path}" for path in integrity.get("missing", [])]
    problems += [f"evidencia ilegible: {item['path']} ({item['error']})"
                 for item in integrity.get("unparsable", [])]
    problems += [f"evidencia alterada: {item['path']} (declarado {item['declared']}, "
                 f"observado {item['observed']})" for item in integrity.get("mismatched", [])]
    problems += [f"evidencia sin entrada en {RECORDS_HASH_FILE}: {path}"
                 for path in integrity.get("undeclared", [])]
    return problems


def invocation_mode(args: argparse.Namespace, will_measure: bool) -> str:
    """`measure` (lote completo), `recheck` (micro-reproceso) o `rebuild` (reportes).

    Sólo un pase completo puede reescribir la procedencia del lote.
    """
    if args.summary_only or not will_measure:
        return "rebuild"
    if args.only or args.start_after or args.limit or args.recheck_existing:
        return "recheck"
    return "measure"


def adopt_batch_provenance(previous: dict, reconciliation: Reconciliation,
                           args: argparse.Namespace) -> dict:
    """Bloque `batch`: procedencia del LOTE original, inmutable.

    Adopta del manifiesto anterior lo que el script ya declaró en su momento
    (HEAD, rama, sha256 del script y de las autoridades, transporte, workers,
    presupuesto) y DERIVA de `progress.jsonl` la ventana y los conteos. Las claves
    heredadas que describían el lote en el nivel raíz se absorben aquí: ninguna
    queda como telemetría recompuesta a mano.

    `retry_budget` (política por llamada) y `retries` (reintentos OBSERVADOS) son
    cosas distintas y aquí se llaman distinto: el manifiesto anterior usaba
    `retries` para la política y `batch_totals.retries` para lo observado.
    """
    telemetry = batch_telemetry(reconciliation)
    window = telemetry["measurement_window"]
    adopted = [key for key in BATCH_ADOPTED_KEYS if key in previous]
    batch: dict[str, Any] = {key: previous[key] for key in adopted}
    batch.update({
        "started_at": window["started_at"],
        "finished_at": window["finished_at"],
        "elapsed_seconds": window["elapsed_seconds"],
        "source_calls": telemetry["all_attempts"]["source_calls"],
        "retries": telemetry["all_attempts"]["retries"],
        "records": telemetry["all_attempts"]["records"],
        "absorbed_legacy_keys": sorted(key for key in LEGACY_BATCH_KEYS if key in previous),
        "adopted_from_previous_manifest": adopted,
        "derived_from": PROGRESS_FILE,
        "note": ("Procedencia del lote ORIGINAL: ni --summary-only, ni --only, ni una "
                 "reconstrucción de reportes la reescriben. La ventana y los conteos salen de "
                 "progress.jsonl, no de un log externo. `head` es el HEAD sobre el que se MIDIÓ; "
                 "el HEAD de la última invocación vive en el nivel raíz del manifiesto. "
                 "`retry_budget` es la política por llamada; `retries` son los reintentos "
                 "OBSERVADOS en la bitácora."),
    })
    batch.setdefault("retry_budget", previous.get("retries", args.retries))
    batch.setdefault("transport", args.transport)
    batch.setdefault("workers", args.workers)
    batch.setdefault("timeout_seconds", args.timeout)
    return batch


# ── Lectura de la fuente ────────────────────────────────────────────────────


@dataclass
class CallResult:
    dataset: str
    soql: str
    status: str  # OK | TIMEOUT | ERROR
    rows: list[dict] = field(default_factory=list)
    latency_ms: float = 0.0
    attempts: int = 1
    error: str | None = None


class Source:
    """Acceso SoQL de sólo lectura, con presupuesto por llamada y reintentos.

    Nunca lanza por una llamada fallida: el fallo ES una medición y se registra
    (`TIMEOUT`/`ERROR`) para que la identidad se clasifique `INCONCLUSIVE` en vez
    de tumbar el batch. Lo que sí prohíbe es degradar un fallo a lista vacía:
    eso convertiría "no respondió" en "no alcanza nada".
    """

    def __init__(
        self,
        settings: Settings,
        *,
        transport: str = "soda3",
        timeout_seconds: float = 60.0,
        retries: int = 2,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.transport_name = transport
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self._sleep = sleep
        self._domain = settings.datos_gov_domain
        self._token = settings.secop_app_token or ""
        self._soda3 = None
        if transport == "soda3":
            from app.core.socrata_transport.base import Soda3OrderingSpecifier
            from app.core.socrata_transport.soda3 import Soda3HttpTransport

            self._ordering = Soda3OrderingSpecifier.TOTAL
            self._soda3 = Soda3HttpTransport(domain=self._domain, app_token=self._token or None)
        self._lock = threading.Lock()
        self.telemetry: dict[str, int] = {
            "calls": 0, "ok": 0, "timeouts": 0, "errors": 0, "retries": 0,
        }
        self._per_canonical = threading.local()

    # -- transporte ---------------------------------------------------------

    def _post(self, dataset: str, soql: str) -> list[dict]:
        if self._soda3 is not None:
            return self._soda3.query(
                dataset, soql, timeout_seconds=self.timeout_seconds,
                ordering_specifier=self._ordering,
            )
        query = urllib.parse.urlencode({"$query": soql})
        url = f"https://{self._domain}/resource/{dataset}.json?{query}"
        request = urllib.request.Request(
            url, headers={"X-App-Token": self._token, "Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.load(response)
        if not isinstance(payload, list):
            raise ValueError("la fuente devolvió un payload que no es una lista")
        return payload

    @staticmethod
    def _is_timeout(exc: BaseException) -> bool:
        for error in (exc, exc.__cause__, exc.__context__):
            if isinstance(error, (TimeoutError, urllib.error.URLError)):
                return True
            if "timeout" in type(error).__name__.lower():
                return True
        return False

    def query(self, dataset: str, soql: str) -> CallResult:
        started = time.perf_counter()
        last: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                rows = self._post(dataset, soql)
                latency = (time.perf_counter() - started) * 1000
                with self._lock:
                    self.telemetry["calls"] += 1
                    self.telemetry["ok"] += 1
                self._note(latency, attempt)
                return CallResult(dataset, soql, "OK", rows, latency, attempt)
            except Exception as exc:  # noqa: BLE001 — el fallo se registra, no se traga
                last = exc
                if attempt < self.retries:
                    with self._lock:
                        self.telemetry["retries"] += 1
                    self._sleep(min(4.0, 1.0 * attempt))
        latency = (time.perf_counter() - started) * 1000
        status = "TIMEOUT" if last is not None and self._is_timeout(last) else "ERROR"
        with self._lock:
            self.telemetry["calls"] += 1
            self.telemetry["timeouts" if status == "TIMEOUT" else "errors"] += 1
        self._note(latency, self.retries)
        return CallResult(dataset, soql, status, [], latency, self.retries,
                          f"{type(last).__name__}: {last}")

    # -- contabilidad por identidad ----------------------------------------

    def reset_canonical(self) -> None:
        self._per_canonical.calls = 0
        self._per_canonical.timeouts = 0
        self._per_canonical.retries = 0
        self._per_canonical.latency_ms = 0.0

    def _note(self, latency_ms: float, attempts: int) -> None:
        self._per_canonical.calls = getattr(self._per_canonical, "calls", 0) + 1
        self._per_canonical.latency_ms = getattr(self._per_canonical, "latency_ms", 0.0) + latency_ms
        self._per_canonical.retries = getattr(self._per_canonical, "retries", 0) + max(0, attempts - 1)

    def note_status(self, status: str) -> None:
        if status != "OK":
            self._per_canonical.timeouts = getattr(self._per_canonical, "timeouts", 0) + 1

    def canonical_network(self) -> dict:
        return {
            "calls": getattr(self._per_canonical, "calls", 0),
            "timeouts": getattr(self._per_canonical, "timeouts", 0),
            "retries": getattr(self._per_canonical, "retries", 0),
            "latency_ms": round(getattr(self._per_canonical, "latency_ms", 0.0), 1),
        }


# ── SoQL: sólo primitivas del builder ──────────────────────────────────────


def like_clause(field: str, value: str) -> str:
    """El fallback productivo, byte-for-byte: `_accent_fold_like` + `_escape`."""
    return SoQLBuilder._accent_fold_like(field, SoQLBuilder._escape(str(value)))


def exact_clause(field: str, names: tuple[str, ...] | list[str]) -> str:
    return SoQLBuilder._exact_entity_clause(field, tuple(names))


def grouped_universe_soql(field: str, where: str, limit: int = GROUP_LIMIT) -> str:
    """Grupos (forma física, NIT). SIN `ORDER BY cnt DESC` a propósito.

    Medido: ordenar el agregado encarece la consulta ~30% en `jbjy-vk9h`
    (68.5s vs 100.3s sobre el mismo predicado) y el orden no lo lee nadie — el
    universo se compara por conjuntos, no por ranking. Si el resultado llega al
    tope, `like_universe_complete=False` lo declara truncado, que es la parte
    del contrato que sí importa.
    """
    return (
        f"SELECT {field}, {NIT_COLUMN}, count(*) AS cnt WHERE {where} "
        f"GROUP BY {field}, {NIT_COLUMN} LIMIT {limit}"
    )


def grouped_forms_soql(field: str, where: str, limit: int = GROUP_LIMIT) -> str:
    return (
        f"SELECT {field}, count(*) AS cnt WHERE {where} "
        f"GROUP BY {field} ORDER BY cnt DESC LIMIT {limit}"
    )


def count_soql(where: str) -> str:
    return f"SELECT count(*) AS cnt WHERE {where}"


# ── Autoridades ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Identity:
    type: str
    value: str
    provenance: str

    @property
    def discovery_capable(self) -> bool:
        """Sólo un NIT acreditado habilita discovery por identidad fiscal."""
        return self.type == "NIT"

    def key(self) -> str:
        return f"{self.type}:{self.value}"

    def as_dict(self) -> dict:
        return {
            "type": self.type, "value": self.value, "provenance": self.provenance,
            "accredited": True, "discovery_capable": self.discovery_capable,
        }


@dataclass
class Authorities:
    aliases: list[dict]
    registry: BuyerSourceNameRegistry
    groups: dict
    icbf: dict
    sena: dict
    buyer_groups_payload: dict


def load_authorities(root: Path = ROOT) -> Authorities:
    load = lambda name: json.loads((root / AUTHORITY_FILES[name]).read_text(encoding="utf-8"))  # noqa: E731
    return Authorities(
        aliases=load("aliases_db"),
        registry=load_registry(root / AUTHORITY_FILES["buyer_source_names"]),
        groups=get_groups(),
        icbf=load("icbf_regionals"),
        sena=load("sena_units"),
        buyer_groups_payload=load("buyer_groups"),
    )


# ── Inventario ──────────────────────────────────────────────────────────────


@dataclass
class Canonical:
    canonical_name: str
    kind: str  # buyer_unit | ecosystem
    origin: str
    identity: Identity | None = None
    origins: list[str] = field(default_factory=list)
    aliases_count: int = 0
    declared_groups: list[str] = field(default_factory=list)
    ecosystem: dict | None = None
    binding: dict | None = None
    # Proyección declarada de un ecosistema (no es una identidad).
    ecosystem_projection: dict | None = None
    labels: list[str] = field(default_factory=list)

    @property
    def already_bound(self) -> bool:
        return self.binding is not None

    @property
    def hash(self) -> str:
        return canonical_hash(self.canonical_name)


def _binding_payload(binding) -> dict:
    return {
        "identity_key": binding.identity_key,
        "sources": {dataset: list(names) for dataset, names in binding.sources.items()},
        "observed_at": binding.observed_at.isoformat(),
        "observed_rows": dict(binding.observed_rows),
        "evidence": binding.evidence,
    }


def build_inventory(authorities: Authorities) -> list[Canonical]:
    """Universo determinista: unión de las autoridades declaradas, sin inventar.

    `aliases_db` es la fuente principal. Los catálogos institucionales y el
    sidecar sólo AGREGAN identidades ya declaradas por ellos; nunca derivan una
    identidad nueva de un nombre observado.
    """
    by_key: dict[str, Canonical] = {}

    def ensure(name: str, origin: str, kind: str = "buyer_unit") -> Canonical:
        key = normalize_text(name)
        record = by_key.get(key)
        if record is None:
            record = Canonical(canonical_name=name, kind=kind, origin=origin)
            by_key[key] = record
        if origin not in record.origins:
            record.origins.append(origin)
        return record

    for entry in authorities.aliases:
        record = ensure(entry["official_name"], "aliases_db")
        record.aliases_count = max(record.aliases_count, len(entry.get("aliases", ())))

    # Sidecar: identidad ACREDITADA (binding vigente) + los canónicos que sólo
    # existen ahí (ICBF sin ficha en el gazetteer).
    for binding in authorities.registry.bindings():
        record = ensure(binding.canonical_name, "buyer_source_names")
        record.binding = _binding_payload(binding)
        type_, _, value = binding.identity_key.partition(":")
        record.identity = Identity(type_, value, AUTHORITY_FILES["buyer_source_names"])
        record.origins.append("sidecar_identity")

    # Catálogo ICBF: declara el nombre oficial de cada unidad.
    icbf_catalog = [authorities.icbf["sede_nacional"], *authorities.icbf["regionales"]]
    for entry in icbf_catalog:
        record = ensure(entry["official_name"], "icbf_regionals")
        record.labels.append(entry.get("departamento") or "sede nacional")
        if record.identity is None:
            record.identity = Identity("BUYER_UNIT", entry["official_name"], AUTHORITY_FILES["icbf_regionals"])
        record.ecosystem = {"family": "ICBF", "role": "declared_unit",
                            "catalog": AUTHORITY_FILES["icbf_regionals"],
                            "department": entry.get("departamento")}

    # Catálogo SENA: unidades nacionales declaradas + ecosistemas (padre,
    # regionales, macro-regiones). El padre NO es una BuyerUnit y no se aplana.
    sena_national = authorities.sena.get("national", {})
    for name in sena_national.get("official_names", ()):
        record = ensure(name, "sena_units")
        if record.identity is None:
            record.identity = Identity("BUYER_UNIT", name, AUTHORITY_FILES["sena_units"])
        record.ecosystem = {"family": "SENA", "role": "declared_unit",
                            "catalog": AUTHORITY_FILES["sena_units"]}

    ecosystems: list[Canonical] = []
    national_label = sena_national.get("label", "SENA Nacional")
    parent = Canonical(
        canonical_name="ECOSYSTEM:SENA:national",
        kind="ecosystem", origin="sena_units", labels=[national_label],
        ecosystem={"family": "SENA", "role": "national_parent",
                   "declared_official_names": list(sena_national.get("official_names", ()))},
        ecosystem_projection={"mode": "official_names",
                              "official_names": list(sena_national.get("official_names", ()))},
    )
    ecosystems.append(parent)
    for group in authorities.sena.get("regional_groups", ()):
        ecosystems.append(Canonical(
            canonical_name=f"ECOSYSTEM:SENA:regional:{group['department']}",
            kind="ecosystem", origin="sena_units", labels=[group.get("label", group["department"])],
            ecosystem={"family": "SENA", "role": "regional_ecosystem",
                       "department": group["department"]},
            ecosystem_projection={"mode": "contains_all",
                                  "contains_all": list(group.get("member_name_contains_all", ()))},
        ))
    for region in (authorities.sena.get("macro_regions") or {}):
        ecosystems.append(Canonical(
            canonical_name=f"ECOSYSTEM:SENA:macro:{region}",
            kind="ecosystem", origin="sena_units", labels=[region],
            ecosystem={"family": "SENA", "role": "macro_region", "region": region,
                       "departments": list(authorities.sena["macro_regions"][region])},
            # Una macro-región es la composición de sus ecosistemas regionales:
            # el token "SENA" solo mediria todo SENA y diria otra cosa. No se mide.
            ecosystem_projection={"mode": "derived", "derived": True,
                                  "contains_all": ["SENA"], "macro_region": region},
        ))
    ecosystems.append(Canonical(
        canonical_name="ECOSYSTEM:ICBF:national",
        kind="ecosystem", origin="icbf_regionals",
        labels=["I.C.B.F. Nacional (sede + 32 regionales)"],
        ecosystem={"family": "ICBF", "role": "national_parent",
                   "declared_group": "icbf_national" if "icbf_national" in authorities.groups else None,
                   "declared_official_names": [entry["official_name"] for entry in icbf_catalog]},
        ecosystem_projection={"mode": "official_names",
                              "official_names": [entry["official_name"] for entry in icbf_catalog]},
    ))

    # Membresía declarada de Buyer Groups: dato del registro, no inferencia.
    for group_id, group in authorities.groups.items():
        for member in group.members:
            record = by_key.get(normalize_text(member))
            if record is None:  # miembro declarado sin ficha propia: se registra tal cual
                record = ensure(member, "buyer_groups")
            if group_id not in record.declared_groups:
                record.declared_groups.append(group_id)

    inventory = sorted(by_key.values(), key=lambda item: normalize_text(item.canonical_name))
    for record in inventory:
        record.declared_groups.sort()
    return inventory + ecosystems


# ── Medición ────────────────────────────────────────────────────────────────


def parse_grouped(rows: list[dict], field: str) -> list[dict]:
    """Formas físicas de una fila agrupada. Nunca completa lo que no vino."""
    parsed = []
    for row in rows:
        parsed.append({
            "name": row.get(field),
            "nit": row.get(NIT_COLUMN),
            "rows": int(row.get("cnt", 0)),
        })
    return parsed


def parse_forms(rows: list[dict], field: str) -> list[dict]:
    return [{"name": row.get(field), "rows": int(row.get("cnt", 0))} for row in rows]


def first_scalar(rows: list[dict]) -> int | None:
    if not rows:
        return None
    for key in ("cnt", "count"):
        if key in rows[0]:
            return int(rows[0][key])
    values = list(rows[0].values())
    return int(values[0]) if values and str(values[0]).isdigit() else None


def measure_canonical(
    source: Source,
    canonical: Canonical,
    *,
    phase_b: bool,
    limit: int = GROUP_LIMIT,
) -> dict:
    """Phase A (siempre) + Phase B (sólo cuando el caso puede ser bindable)."""
    source.reset_canonical()
    result: dict[str, Any] = {
        "datasets": {},
        "errors": [],
        "phase_b": phase_b,
    }
    nit = canonical.identity.value if (canonical.identity and canonical.identity.discovery_capable) else None
    bound_names = (canonical.binding or {}).get("sources", {})
    projection = canonical.ecosystem_projection or {}
    projection_mode = projection.get("mode")
    projection_terms = (projection.get("official_names") if projection_mode == "official_names"
                        else projection.get("contains_all")) or []
    # Un ecosistema no se mide por su nombre sintético: se mide su proyección
    # DECLARADA (contains_all / official_names), que es lo que el runtime emite.
    if canonical.kind == "ecosystem" and projection.get("derived"):
        return {"datasets": {}, "errors": [], "phase_b": False,
                "projection_mode": "derived", "projection_terms": projection_terms}

    for dataset, entity_field in DATASETS:
        entry: dict[str, Any] = {"dataset": dataset, "entity_field": entity_field}
        # C — universo LIKE actual (el camino productivo de hoy). En un
        # ecosistema, la proyección declarada ocupa el lugar del nombre canónico.
        # En un binding vigente NO se mide: producción ya no pasa por LIKE para
        # esa identidad, y el universo que importa es el suyo (sección 17).
        skip_like = canonical.already_bound
        if skip_like:
            entry["like_status"] = "SKIPPED_ALREADY_BOUND"
            entry["like_universe"] = []
            entry["like_universe_complete"] = None
            entry["like_rows_grouped"] = None
            entry["like_where"] = None
        else:
            if canonical.kind == "ecosystem" and projection:
                if projection_mode == "official_names":
                    where = exact_clause(entity_field, tuple(projection_terms))
                else:
                    where = "(" + " AND ".join(like_clause(entity_field, term) for term in projection_terms) + ")"
                entry["projection_mode"] = projection_mode
            else:
                where = like_clause(entity_field, canonical.canonical_name)
            entry["like_where"] = where
            call = source.query(dataset, grouped_universe_soql(entity_field, where, limit))
            source.note_status(call.status)
            entry["like_status"] = call.status
            universe = parse_grouped(call.rows, entity_field) if call.status == "OK" else []
            entry["like_universe"] = universe
            entry["like_universe_complete"] = call.status == "OK" and len(call.rows) < limit
            entry["like_rows_grouped"] = sum(item["rows"] for item in universe)
            if call.status != "OK":
                result["errors"].append(f"like_universe/{dataset}: {call.error}")

        # B — formas físicas de la identidad acreditada (sólo si es un NIT).
        if nit:
            forms_call = source.query(
                dataset,
                grouped_forms_soql(entity_field, f"{NIT_COLUMN}='{SoQLBuilder._escape(nit)}'", limit),
            )
            source.note_status(forms_call.status)
            entry["identity_forms_status"] = forms_call.status
            entry["identity_forms"] = parse_forms(forms_call.rows, entity_field) if forms_call.status == "OK" else []
            entry["identity_forms_complete"] = forms_call.status == "OK" and len(forms_call.rows) < limit
            if forms_call.status != "OK":
                result["errors"].append(f"identity_forms/{dataset}: {forms_call.error}")

        # 17 — binding vigente: re-observar la fuente sin modificarlo.
        names = tuple(bound_names.get(dataset, ()))
        if names:
            bound_call = source.query(
                dataset, grouped_universe_soql(entity_field, exact_clause(entity_field, names), limit),
            )
            source.note_status(bound_call.status)
            entry["bound_status"] = bound_call.status
            entry["bound_universe"] = parse_grouped(bound_call.rows, entity_field) if bound_call.status == "OK" else []
            entry["bound_rows"] = sum(item["rows"] for item in entry["bound_universe"])
            if bound_call.status != "OK":
                result["errors"].append(f"bound_universe/{dataset}: {bound_call.error}")

        result["datasets"][dataset] = entry

    # F/G/H — candidato conservador + equivalencia (Phase B).
    if phase_b:
        candidate_names: dict[str, list[str]] = {}
        for dataset, entry in result["datasets"].items():
            reached = [item["name"] for item in entry.get("like_universe", [])
                       if entry.get("like_status") == "OK" and item["name"]]
            candidate_names[dataset] = reached
        result["candidate_names"] = candidate_names
        for dataset, entry in result["datasets"].items():
            names = candidate_names.get(dataset, [])
            if entry.get("like_status") != "OK" or not names:
                entry["equivalence"] = {
                    "candidate_names": names,
                    "like_count": None, "exact_count": None, "same_universe": None,
                    "note": "sin candidato medible" if entry.get("like_status") == "OK"
                            else "universo LIKE no medido",
                }
                continue
            like_call = source.query(dataset, count_soql(entry["like_where"]))
            source.note_status(like_call.status)
            exact_where = exact_clause(entry["entity_field"], names)
            exact_call = source.query(dataset, count_soql(exact_where))
            source.note_status(exact_call.status)
            like_count = first_scalar(like_call.rows) if like_call.status == "OK" else None
            exact_count = first_scalar(exact_call.rows) if exact_call.status == "OK" else None
            same = None if like_count is None or exact_count is None else like_count == exact_count
            entry["equivalence"] = {
                "exact_where": exact_where,
                "candidate_names": names,
                "like_count": like_count,
                "exact_count": exact_count,
                # Un None (no respondió) NO acredita equivalencia.
                "same_universe": same,
                "like_status": like_call.status,
                "exact_status": exact_call.status,
            }
            if same is None:
                result["errors"].append(f"equivalence/{dataset}: medición incompleta")
    return result


# ── Clasificación ───────────────────────────────────────────────────────────


def dataset_summary(entry: dict) -> dict:
    universe = entry.get("like_universe") or []
    nits = sorted({digits_of(item.get("nit")) for item in universe if digits_of(item.get("nit"))})
    forms = sorted({item["name"] for item in universe if item.get("name")})
    identity_names = sorted({item["name"] for item in entry.get("identity_forms", []) if item.get("name")})
    reached = set(forms) if entry.get("like_status") == "OK" else None
    return {
        "like_status": entry.get("like_status"),
        "like_where": entry.get("like_where"),
        "forms_reached": forms,
        "forms_reached_count": len(forms),
        "nits_reached": nits,
        "like_rows": entry.get("like_rows_grouped"),
        "like_universe_complete": entry.get("like_universe_complete"),
        "identity_forms": entry.get("identity_forms", []),
        "identity_forms_status": entry.get("identity_forms_status"),
        # Formas REALES de la identidad acreditada que el LIKE no alcanza: se
        # declaran y no entran al candidato (regla F), pero son señal de
        # composición para revisión humana.
        "forms_not_reached_by_like": (
            sorted(set(identity_names) - reached) if reached is not None else None
        ),
        "bound_universe": entry.get("bound_universe", []),
        "bound_status": entry.get("bound_status"),
        "bound_rows": entry.get("bound_rows"),
        "equivalence": entry.get("equivalence"),
    }


def token_distinct_forms(forms: list[str]) -> dict[tuple[str, ...], list[str]]:
    """Agrupa formas por su secuencia de tokens: la MISMA grafía no es multi-nombre."""
    groups: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for form in forms:
        groups[tokens_of(form)].append(form)
    return {key: sorted(value) for key, value in groups.items() if key}


def composition_shape(forms: list[str], canonical: str) -> bool:
    """¿La evidencia tiene forma de RAÍZ + UNIDADES hija?

    Regla mecánica y declarada: una forma es hija cuando sus tokens CONTIENEN
    estrictamente los del canónico (p. ej. `MUNICIPIO DE SOACHA` y
    `MUNICIPIO DE SOACHA - SECRETARIA X`). Eso no prueba composición —no existe
    autoridad declarada aquí— pero sí la vuelve una pregunta de Buyer Group en
    vez de una simple grafía distinta.
    """
    root = tokens_of(canonical)
    if not root:
        return False
    children = 0
    for form in forms:
        form_tokens = tokens_of(form)
        if len(form_tokens) > len(root) and form_tokens[: len(root)] == root:
            children += 1
    return children >= 1 and len(token_distinct_forms(forms)) >= 2


def evaluate_binding(canonical: Canonical, measurement: dict) -> dict:
    """Verificación de un binding vigente (sección 17). No lo modifica."""
    binding = canonical.binding or {}
    declared_names = binding.get("sources", {})
    verdict: dict[str, Any] = {"status": "NOT_BOUND", "missing_names": {}, "foreign_nits": {}}
    if not binding:
        return verdict
    expected_nit = None
    if canonical.identity and canonical.identity.type == "NIT":
        expected_nit = digits_of(canonical.identity.value)
    inconclusive: list[str] = []
    drift = False
    for dataset, entry in measurement["datasets"].items():
        names = declared_names.get(dataset, ())
        if not names:
            continue
        if entry.get("bound_status") != "OK":
            inconclusive.append(dataset)
            continue
        observed = {item["name"] for item in entry["bound_universe"] if item["rows"] > 0}
        missing = sorted(set(names) - observed)
        if missing:
            drift = True
            verdict["missing_names"][dataset] = missing
        # Identidad ajena SÓLO se puede afirmar contra un NIT acreditado.
        # Una identidad BUYER_UNIT (p. ej. municipal_m0) no tiene NIT propio:
        # los NIT observados se registran como información, no como drift.
        observed_nits = sorted({digits_of(item["nit"]) for item in entry["bound_universe"]
                                if digits_of(item["nit"])})
        verdict.setdefault("observed_nits", {})[dataset] = observed_nits
        if expected_nit is not None:
            foreign = [nit for nit in observed_nits if nit != expected_nit]
            if foreign:
                drift = True
                verdict["foreign_nits"][dataset] = foreign
        verdict.setdefault("names_now", {})[dataset] = {
            "declared": list(names),
            "present": sorted(observed),
            "rows": entry.get("bound_rows"),
        }
    verdict["inconclusive_datasets"] = inconclusive
    if inconclusive:
        verdict["status"] = "UNVERIFIED_THIS_RUN"
    elif drift:
        verdict["status"] = "DRIFT"
    else:
        verdict["status"] = "VERIFIED"
    return verdict


def classify(canonical: Canonical, measurement: dict, binding_verdict: dict) -> dict:
    """Una clase principal por identidad, con la razón explícita que la sostiene."""
    reasons: list[str] = []
    datasets = measurement["datasets"]

    def dataset_flags() -> tuple[list[str], list[str], dict[str, list[str]]]:
        nits: set[str] = set()
        incomplete: list[str] = []
        forms_by_dataset: dict[str, list[str]] = {}
        for dataset, entry in datasets.items():
            summary = dataset_summary(entry)
            nits.update(summary["nits_reached"])
            forms_by_dataset[dataset] = summary["forms_reached"]
            if summary["like_status"] != "OK":
                incomplete.append(dataset)
        return sorted(nits), incomplete, forms_by_dataset

    observed_nits, incomplete_like, forms_by_dataset = dataset_flags()
    all_forms = sorted({form for forms in forms_by_dataset.values() for form in forms})
    token_groups = token_distinct_forms(all_forms)
    multi_forms = len(token_groups) >= 2
    composition = multi_forms and composition_shape(all_forms, canonical.canonical_name)

    # 1 — Ecosistema institucional: composición, no BuyerUnit. No se aplana.
    if canonical.kind == "ecosystem":
        return {"status": "INSTITUTIONAL_ECOSYSTEM", "reasons": ["composición institucional declarada"]}

    # 2 — Fallo del auditor.
    if measurement.get("fatal_error"):
        return {"status": "ERROR", "reasons": [str(measurement["fatal_error"])]}

    # 3 — Binding vigente: se verifica, no se modifica.
    if canonical.already_bound:
        if binding_verdict["status"] == "UNVERIFIED_THIS_RUN":
            return {"status": "INCONCLUSIVE",
                    "reasons": ["binding no re-observado: " + ", ".join(binding_verdict["inconclusive_datasets"])]}
        if binding_verdict["status"] == "DRIFT":
            reasons.append("drift observado en el binding vigente")
            if binding_verdict["missing_names"]:
                reasons.append("nombres declarados sin filas: " + json.dumps(binding_verdict["missing_names"], ensure_ascii=False))
            if binding_verdict["foreign_nits"]:
                reasons.append("identidad ajena alcanzada: " + json.dumps(binding_verdict["foreign_nits"], ensure_ascii=False))
            return {"status": "BINDING_DRIFT_REVIEW", "reasons": reasons}
        return {"status": "ALREADY_BOUND_VERIFIED", "reasons": ["binding re-observado sin drift"]}

    # 4 — Sin identidad acreditada: se declara, no se inventa. Pero la evidencia
    # física no se descarta: varias formas bajo un NIT observado son exactamente
    # el caso `SAME_NIT != SAME_BUYER_UNIT` y se marcan como tales (sección 16).
    if canonical.identity is None:
        if len(observed_nits) >= 2:
            return {"status": "AMBIGUOUS_IDENTITY",
                    "reasons": [f"evidencia física apunta a {len(observed_nits)} identidades: "
                                + ", ".join(observed_nits)]}
        if multi_forms:
            detail = ("identidad fiscal sin acreditar; el universo alcanza "
                      f"{len(token_groups)} formas token-distintas")
            if composition:
                return {"status": "BUYER_GROUP_CANDIDATE",
                        "reasons": [detail + " con forma de raíz + unidades hija"]}
            return {"status": "MULTI_NAME_REVIEW", "reasons": [detail]}
        detail = "sin NIT publicado" if not observed_nits else f"NIT observado no acreditado: {observed_nits[0]}"
        return {"status": "NO_STABLE_IDENTITY", "reasons": [detail]}

    expected_nit = digits_of(canonical.identity.value) if canonical.identity.type == "NIT" else None
    foreign = sorted(nit for nit in observed_nits if expected_nit is None or nit != expected_nit)

    # 5 — El LIKE actual alcanza identidades/NIT ajenos.
    if expected_nit is not None and foreign:
        return {"status": "FOREIGN_IDENTITY_COLLISION",
                "reasons": [f"LIKE alcanza NIT ajenos: {', '.join(foreign)}"]}

    # 6/7 — Varias formas físicas entran al UNIVERSO del LIKE: lo que el
    # producto alcanza hoy. Las formas que la identidad publica y el LIKE NO
    # alcanza se declaran aparte (`forms_not_reached_by_like`) y quedan FUERA
    # del candidato — regla F del encargo —; su existencia es señal de
    # composición para revisión, nunca autoridad para sumar unidades.
    if multi_forms:
        if composition:
            return {"status": "BUYER_GROUP_CANDIDATE",
                    "reasons": ["el universo alcanzado tiene forma de raíz + unidades hija, "
                                "sin autoridad declarada"]}
        return {"status": "MULTI_NAME_REVIEW",
                "reasons": ["más de una forma física token-distinta en el universo"]}

    # 8 — Medición incompleta: nunca se lee como equivalencia.
    if incomplete_like:
        return {"status": "INCONCLUSIVE", "reasons": ["universo LIKE no medido: " + ", ".join(incomplete_like)]}

    # 8b — Identidad acreditada que la fuente no publica en NINGÚN dataset.
    # No es un timeout ni un fallo: es un hecho medido (0 filas), y sin universo
    # no hay nada que proyectar. Se dice con esas palabras en vez de dejarlo
    # como «comparación no terminada», que sugeriría una duda que no existe.
    if not all_forms and not any(entry.get("identity_forms") for entry in datasets.values()):
        return {"status": "INCONCLUSIVE",
                "reasons": ["el nombre declarado no se publica en la fuente (0 filas en ambos "
                            "datasets): no hay universo que proyectar"]}

    # 9 — El LIKE es ciego a las formas reales de la identidad acreditada.
    if expected_nit is not None:
        for dataset, entry in datasets.items():
            if not entry.get("identity_forms"):
                continue
            reached = set(forms_by_dataset.get(dataset, []))
            if not reached:
                return {"status": "NOT_EQUIVALENT",
                        "reasons": [f"LIKE no alcanza ninguna forma de la identidad en {dataset}: "
                                    "un binding ampliaría el universo"]}

    # 10 — Equivalencia de universos.
    same_universe = [entry.get("equivalence", {}).get("same_universe")
                     for entry in datasets.values()
                     if entry.get("equivalence", {}).get("candidate_names")]
    if any(value is None for value in same_universe) or not same_universe:
        return {"status": "INCONCLUSIVE", "reasons": ["comparación de universos no completada"]}
    if any(value is False for value in same_universe):
        return {"status": "NOT_EQUIVALENT", "reasons": ["LIKE y candidato exacto no producen el mismo universo"]}

    # 11 — Unidad individual de un grupo declarado: el binding se audita aquí,
    # pero la composición vive en el grupo, no en el miembro.
    if canonical.declared_groups:
        return {"status": "BUYER_GROUP_MEMBER",
                "reasons": ["miembro declarado de " + ", ".join(canonical.declared_groups)]}

    # 12/13 — Proyección 1:1 acreditable.
    if len(token_distinct_forms(all_forms)) != 1:
        return {"status": "MULTI_NAME_REVIEW", "reasons": ["formas token-distintas por dataset"]}
    per_dataset = {dataset: sorted(set(forms)) for dataset, forms in forms_by_dataset.items()}
    if len({json.dumps(forms) for forms in per_dataset.values()}) > 1:
        return {"status": "SOURCE_NAME_SPLIT",
                "reasons": ["misma identidad, formas físicas distintas por dataset"]}
    canonical_tokens = tokens_of(canonical.canonical_name)
    if all(tokens_of(form) != canonical_tokens for forms in per_dataset.values() for form in forms):
        return {"status": "SOURCE_NAME_SPLIT",
                "reasons": ["el nombre canónico no es el nombre físico publicado"]}
    return {"status": "EXACT_BINDABLE", "reasons": ["proyección exacta con el mismo universo"]}


def build_candidate(canonical: Canonical, measurement: dict, status: str) -> dict | None:
    """Candidato ADR-073 conservador, o `None` si no se puede demostrar.

    Nunca contiene una forma que el LIKE no alcance (ampliaría el universo) ni
    una forma con filas bajo una identidad ajena.

    Una unidad individual declarada por un catálogo institucional (`role ==
    "declared_unit"`: las unidades nacionales del SENA, las regionales del ICBF)
    SÍ puede llevar binding: es una BuyerUnit concreta, no la composición. Lo que
    no puede llevar binding es el PADRE ni un ecosistema regional, porque su
    semántica es «varias unidades», y para eso está el Buyer Group.
    """
    if status not in ("EXACT_BINDABLE", "SOURCE_NAME_SPLIT"):
        return None
    if canonical.identity is None or canonical.kind == "ecosystem":
        return None
    role = (canonical.ecosystem or {}).get("role")
    if role not in (None, "declared_unit"):
        return None
    expected_nit = digits_of(canonical.identity.value) if canonical.identity.type == "NIT" else None
    sources: dict[str, list[str]] = {}
    rows: dict[str, int] = {}
    for dataset, entry in measurement["datasets"].items():
        equivalence = entry.get("equivalence") or {}
        names = list(equivalence.get("candidate_names") or [])
        if not names or equivalence.get("same_universe") is not True:
            return None
        for name in names:
            nits = {digits_of(item.get("nit")) for item in entry.get("like_universe", [])
                    if item.get("name") == name and digits_of(item.get("nit"))}
            if expected_nit is not None and nits - {expected_nit}:
                return None
            if expected_nit is None and len(nits) > 1:
                return None
        sources[dataset] = names
        rows[dataset] = sum(item["rows"] for item in entry.get("like_universe", [])
                            if item.get("name") in names)
    if not sources:
        return None
    return {
        "canonical_name": canonical.canonical_name,
        "identity_key": {"type": canonical.identity.type, "value": canonical.identity.value},
        "identity_provenance": canonical.identity.provenance,
        "observed_at": datetime.now(timezone.utc).date().isoformat(),
        "sources": sources,
        "observed_rows": rows,
        "classification": status,
        "semantics_preserved": True,
        "foreign_identities_reached": [],
        "declared_groups": list(canonical.declared_groups),
        "evidence_record": f"records/{canonical.hash}.json",
        "promotion_status": "CANDIDATE_NOT_PROMOTED",
        "note": ("Candidato producido por auditoría read-only. NO es un binding "
                 "productivo: no se ha escrito en app/data/buyer_source_names.json "
                 "y requiere revisión humana independiente antes de cualquier promoción."),
    }


# ── Auditor ─────────────────────────────────────────────────────────────────


def evidence_flags(canonical: Canonical, measurement: dict) -> dict:
    """Banderas de evidencia que el informe necesita y la clase no agota.

    `source_name_split_observed` responde la pregunta del encargo para un
    binding vigente: ¿el nombre canónico es el nombre físico publicado?
    `composition_signal`/`identity_publishes_unreached_forms` marcan que la
    identidad acreditada publica más de una unidad — evidencia para revisión,
    nunca autoridad para sumar.
    """
    datasets = measurement.get("datasets", {})
    canonical_tokens = tokens_of(canonical.canonical_name)
    flags = {
        "like_universe_skipped": bool(canonical.already_bound),
        "source_name_split_observed": False,
        "identity_publishes_unreached_forms": False,
        "composition_signal": False,
        "like_universe_truncated": False,
    }
    bound_names = (canonical.binding or {}).get("sources", {}) if canonical.binding else {}
    if bound_names:
        names_by_dataset = {dataset: list(names) for dataset, names in bound_names.items()}
        if any(tokens_of(name) != canonical_tokens
               for names in names_by_dataset.values() for name in names):
            flags["source_name_split_observed"] = True
        if len({tuple(sorted(names)) for names in names_by_dataset.values()}) > 1:
            flags["source_name_split_observed"] = True
    for entry in datasets.values():
        summary = dataset_summary(entry)
        unreached = summary.get("forms_not_reached_by_like") or []
        if unreached:
            flags["identity_publishes_unreached_forms"] = True
        identity_names = [item["name"] for item in entry.get("identity_forms", []) if item.get("name")]
        if len(token_distinct_forms(identity_names)) >= 2:
            flags["composition_signal"] = True
        if summary.get("like_universe_complete") is False:
            flags["like_universe_truncated"] = True
    return flags


class Auditor:
    def __init__(
        self,
        args: argparse.Namespace,
        settings: Settings,
        source: "Source | None" = None,
        prior: dict[str, list[ProgressEntry]] | None = None,
    ) -> None:
        self.args = args
        self.out = Path(args.output)
        self.source = source if source is not None else Source(
            settings, transport=args.transport, timeout_seconds=args.timeout,
            retries=args.retries,
        )
        self.lock = threading.Lock()
        self.counter = 0
        self.total = 0
        # Intentos anteriores por canónico: un reproceso AGREGA un intento y
        # declara a cuál reemplaza. Sin esta declaración el registro no entra.
        self.prior = prior or {}
        self.supersession_reason = "recheck_existing" if args.recheck_existing else "duplicate_run"

    def process(self, canonical: Canonical) -> dict:
        started = now_iso()
        prior_attempts = self.prior.get(canonical.hash, [])
        attempt = len(prior_attempts) + 1
        predecessor = prior_attempts[-1] if prior_attempts else None
        measurement: dict[str, Any] = {"datasets": {}, "errors": []}
        error: str | None = None
        try:
            measurement = measure_canonical(
                self.source, canonical, phase_b=needs_equivalence(canonical),
            )
        except Exception as exc:  # noqa: BLE001 — un fallo por identidad no tumba el batch
            error = f"{type(exc).__name__}: {exc}"
            measurement.setdefault("datasets", {})
            measurement.setdefault("errors", []).append(error)
            measurement["fatal_error"] = error

        binding_verdict = evaluate_binding(canonical, measurement)
        if error:
            outcome = {"status": "ERROR", "reasons": [error]}
        else:
            try:
                outcome = classify(canonical, measurement, binding_verdict)
            except Exception as exc:  # noqa: BLE001 — clasificar no puede tumbar el batch
                outcome = {"status": "ERROR", "reasons": [f"classify: {type(exc).__name__}: {exc}"]}

        candidate = None
        if not error:
            try:
                candidate = build_candidate(canonical, measurement, outcome["status"])
            except Exception as exc:  # noqa: BLE001
                measurement["errors"].append(f"candidate: {type(exc).__name__}: {exc}")

        record = {
            "canonical_name": canonical.canonical_name,
            "canonical_hash": canonical.hash,
            # Identidad del INTENTO, no del canónico: dos intentos del mismo
            # canónico son dos asientos distintos y auditables por separado.
            "record_id": f"{canonical.hash}:{attempt}",
            "attempt": attempt,
            "supersedes_record_id": predecessor.record_id if predecessor else None,
            "supersedes_record_sha256": predecessor.digest if predecessor else None,
            "supersession_reason": self.supersession_reason if predecessor else None,
            "kind": canonical.kind,
            "labels": list(canonical.labels),
            "origin": canonical.origins or [canonical.origin],
            "aliases_count": canonical.aliases_count,
            "started_at": started,
            "finished_at": now_iso(),
            "status": outcome["status"],
            "status_reasons": outcome["reasons"],
            "identity": canonical.identity.as_dict() if canonical.identity else None,
            "already_bound": canonical.already_bound,
            "binding": canonical.binding,
            "binding_status": binding_verdict["status"],
            "declared_groups": list(canonical.declared_groups),
            "ecosystem": canonical.ecosystem,
            # Procedencia del transporte EN EL REGISTRO: un lote puede
            # reanudarse con otro transporte (así se resolvió el primer intento),
            # y que dos registros del mismo progress.jsonl se midieran distinto
            # no puede quedar invisible en el manifiesto.
            "transport": self.source.transport_name,
            "evidence_flags": evidence_flags(canonical, measurement),
            "datasets": {dataset: dataset_summary(entry) for dataset, entry in measurement["datasets"].items()},
            "proposed_binding": candidate["sources"] if candidate else None,
            "semantics_preserved": candidate["semantics_preserved"] if candidate else None,
            "candidate": candidate,
            "network": self.source.canonical_network(),
            "errors": measurement.get("errors", []),
            "error": error,
        }
        with self.lock:
            self.counter += 1
            position = self.counter
        append_jsonl(self.out / "progress.jsonl", record, self.lock)
        write_json(self.out / "records" / f"{canonical.hash}.json",
                   {"record": record, "measurement": measurement,
                    "binding_verdict": binding_verdict})
        print(f"[{position}/{self.total}] {record['status']:26s} {canonical.canonical_name[:70]}",
              flush=True)
        return record


def needs_equivalence(canonical: Canonical) -> bool:
    """Phase B sólo donde el caso puede ser bindable (sección 19).

    No se gastan conteos caros en ecosistemas padre, identidades ausentes ni en
    bindings ya vigentes, que se auditan con su propio universo exacto.
    """
    if canonical.kind == "ecosystem":
        return False
    if canonical.already_bound:
        return False
    if canonical.identity is None:
        return False
    return True


# ── Reportes ────────────────────────────────────────────────────────────────

CLASS_FILES = {
    "ALREADY_BOUND_VERIFIED": "already_bound_verified.jsonl",
    "BINDING_DRIFT_REVIEW": "binding_drift_review.jsonl",
    "SOURCE_NAME_SPLIT": "source_name_split.jsonl",
    "MULTI_NAME_REVIEW": "multi_name_review.jsonl",
    "BUYER_GROUP_MEMBER": "buyer_group_members.jsonl",
    "BUYER_GROUP_CANDIDATE": "buyer_group_candidates.jsonl",
    "INSTITUTIONAL_ECOSYSTEM": "institutional_ecosystems.jsonl",
    "NO_STABLE_IDENTITY": "no_stable_identity.jsonl",
    "AMBIGUOUS_IDENTITY": "ambiguous_identity.jsonl",
    "NOT_EQUIVALENT": "not_equivalent.jsonl",
    "FOREIGN_IDENTITY_COLLISION": "foreign_identity_collision.jsonl",
    "INCONCLUSIVE": "inconclusive.jsonl",
    "ERROR": "errors.jsonl",
}


def duplicated_names(by_member: dict[str, list[str]]) -> list[dict]:
    """El MISMO nombre físico reclamado por DOS miembros distintos.

    Es exactamente la condición que `buyer_groups.BuyerGroup.names_for` rechaza en
    producción ("Dos miembros reclaman un mismo nombre físico"). Sumar y contar
    sin mirar esto la escondería: `extend()` deja los dos nombres en la lista y el
    total sube como si todo estuviera bien.
    """
    owners: dict[str, set[str]] = defaultdict(set)
    for member, names in by_member.items():
        for name in names:
            owners[name].add(member)
    return [{"name": name, "members": sorted(members)}
            for name, members in sorted(owners.items()) if len(members) > 1]


def group_readiness(records: list[dict], groups: dict) -> dict:
    by_name = {normalize_text(record["canonical_name"]): record for record in records}
    report = {}
    for group_id, group in groups.items():
        members = list(group.members)
        bound, ready, blocked, inconclusive = [], [], [], []
        names_today: dict[str, list[str]] = {dataset: [] for dataset, _ in DATASETS}
        names_after: dict[str, list[str]] = {dataset: [] for dataset, _ in DATASETS}
        today_by_member: dict[str, dict[str, list[str]]] = {
            dataset: defaultdict(list) for dataset, _ in DATASETS}
        after_by_member: dict[str, dict[str, list[str]]] = {
            dataset: defaultdict(list) for dataset, _ in DATASETS}
        for member in members:
            record = by_name.get(normalize_text(member))
            if record is None:
                blocked.append({"canonical_name": member, "reason": "sin evidencia en esta corrida"})
                continue
            binding = record.get("binding") or {}
            if record["already_bound"]:
                samples = (binding.get("sources") or {})
                for dataset in names_today:
                    names_today[dataset].extend(samples.get(dataset, []))
                    names_after[dataset].extend(samples.get(dataset, []))
                    today_by_member[dataset][member].extend(samples.get(dataset, []))
                    after_by_member[dataset][member].extend(samples.get(dataset, []))
                if record["status"] == "ALREADY_BOUND_VERIFIED":
                    bound.append(member)
                elif record["status"] in ("INCONCLUSIVE", "ERROR"):
                    inconclusive.append({"canonical_name": member, "reason": record["status"]})
                else:
                    inconclusive.append({"canonical_name": member, "reason": record["status"]})
                continue
            candidate = record.get("candidate")
            if candidate and record["status"] in ("BUYER_GROUP_MEMBER", "EXACT_BINDABLE", "SOURCE_NAME_SPLIT"):
                ready.append(member)
                for dataset in names_after:
                    produced = (candidate.get("sources") or {}).get(dataset, [])
                    names_after[dataset].extend(produced)
                    after_by_member[dataset][member].extend(produced)
            else:
                blocked.append({"canonical_name": member, "reason": record["status"]})
        # Colisión de nombre físico: dos miembros distintos no pueden proyectar el
        # mismo string, hoy ni tras adjudicar los candidatos.
        duplicated_today = {dataset: duplicated_names(today_by_member[dataset])
                            for dataset, _ in DATASETS}
        duplicated_after = {dataset: duplicated_names(after_by_member[dataset])
                            for dataset, _ in DATASETS}
        duplicated = any(entries for entries in duplicated_today.values()) \
            or any(entries for entries in duplicated_after.values())
        over_today = any(len(value) > MAX_GROUP_SOURCE_NAMES for value in names_today.values())
        over_after = any(len(value) > MAX_GROUP_SOURCE_NAMES for value in names_after.values())
        # Precedencia: un grupo que NO CABE —hoy o tras adjudicar los candidatos—
        # o cuyos miembros se pisan el nombre físico está BLOCKED, aunque todos
        # sus miembros ya estén bindeados o con candidato listo. "Listo tras
        # revisión" no puede ganarle a "no cabe".
        if blocked or inconclusive:
            # Un miembro sin verificar hoy no autoriza a declarar el grupo listo.
            status = "BLOCKED" if not bound and not ready else "PARTIAL"
        elif duplicated or over_today or over_after:
            status = "BLOCKED"
        elif ready:
            status = "READY_AFTER_REVIEW"
        else:
            status = "READY_TODAY"
        report[group_id] = {
            "group_id": group_id,
            "version": group.version,
            "label": group.label,
            "members_total": len(members),
            "members_already_bound": len(bound),
            "members_candidate_ready": len(ready),
            "members_blocked": len(blocked),
            "members_inconclusive": len(inconclusive),
            "missing_bindings": [item["canonical_name"] for item in blocked],
            "inconclusive_members": [item["canonical_name"] for item in inconclusive],
            "projected_names_processes": len(names_today[PROCESSES]),
            "projected_names_contracts": len(names_today[CONTRACTS]),
            "projected_names_after_review_processes": len(names_after[PROCESSES]),
            "projected_names_after_review_contracts": len(names_after[CONTRACTS]),
            "would_exceed_max_group_names": over_today,
            "would_exceed_after_review": over_after,
            "duplicated_source_names": {"today": duplicated_today, "after_review": duplicated_after},
            "max_group_source_names": MAX_GROUP_SOURCE_NAMES,
            "status": status,
            "note": ("Los candidatos NO son bindings productivos: READY_AFTER_REVIEW significa "
                     "que el grupo quedaría completo si se adjudican y promueven individualmente. "
                     "BLOCKED cubre también «no cabe» o «dos miembros reclaman el mismo nombre "
                     "físico», que son condiciones de la autoridad de Buyer Groups."),
        }
    return report


def sena_coverage(records: list[dict], authorities: Authorities) -> dict:
    ecosystems = [record for record in records if record.get("kind") == "ecosystem"
                  and (record.get("ecosystem") or {}).get("family") == "SENA"]
    units = [record for record in records
             if (record.get("ecosystem") or {}).get("family") == "SENA"
             and record.get("kind") == "buyer_unit"]
    return {
        "family": "SENA",
        "authority": AUTHORITY_FILES["sena_units"],
        "note": ("SENA no es un mapa 1:1: el padre y los ecosistemas regionales son "
                 "COMPOSICIONES declaradas (contains_all / official_names), no BuyerUnits. "
                 "Este reporte no propone bindings ni crea Buyer Groups."),
        "ecosystems_total": len(ecosystems),
        "ecosystems": [{"canonical_name": record["canonical_name"],
                        "role": (record.get("ecosystem") or {}).get("role"),
                        "label": (record.get("labels") or [None])[0],
                        "status": record["status"],
                        "datasets": record["datasets"]} for record in ecosystems],
        "units_total": len(units),
        "units_with_stable_identity": sum(1 for record in units if record["identity"]),
        "units_already_bound": sum(1 for record in units if record["already_bound"]),
        "units_candidate_ready": sum(1 for record in units if record.get("candidate")),
        "units_review": sum(1 for record in units if record["status"] in
                            ("MULTI_NAME_REVIEW", "BUYER_GROUP_CANDIDATE", "AMBIGUOUS_IDENTITY")),
        "units_inconclusive": sum(1 for record in units if record["status"] == "INCONCLUSIVE"),
        "regional_coverage": [{"department": group["department"],
                               "label": group["label"],
                               "contains_all": group.get("member_name_contains_all", [])}
                              for group in authorities.sena.get("regional_groups", ())],
        "dataset_coverage": {dataset: sum(1 for record in units
                                          if (record["datasets"].get(dataset) or {}).get("like_status") == "OK")
                             for dataset, _ in DATASETS},
    }


def build_summary(records: list[dict], authorities: Authorities, *,
                  reconciliation: Reconciliation | None = None,
                  integrity: dict | None = None) -> dict:
    classes = Counter(record["status"] for record in records)
    candidates = [record["candidate"] for record in records if record.get("candidate")]
    bound_records = [record for record in records if record["already_bound"]]
    verified = [record for record in bound_records if record["status"] == "ALREADY_BOUND_VERIFIED"]
    # Qué se comprobó REALMENTE en cada binding vigente. Una liveness de nombre en
    # ambos datasets se midió en los 593; un NIT ajeno sólo se puede afirmar donde
    # hay NIT acreditado, y eso son 18. Llamar "identity-safe verified" a los 575
    # restantes sería inflar la evidencia.
    def identity_type(record: dict) -> str | None:
        return ((record.get("identity") or {}).get("type"))

    nit_checked = [record for record in verified if identity_type(record) == "NIT"]
    structural_only = [record for record in verified if identity_type(record) != "NIT"]
    groups = group_readiness(records, authorities.groups)

    def rows_of(record: dict) -> int:
        """Filas observadas por identidad. En un binding vigente el universo LIKE
        no se mide (producción no pasa por ahí): manda el universo de su binding.

        Las claves son las del registro de `progress.jsonl` (`like_rows`,
        `bound_rows`), no las del measurement interno: leer la clave equivocada
        dejaba todos los top-lists en cero sin que nada fallara.
        """
        total = 0
        for dataset, _ in DATASETS:
            entry = record["datasets"].get(dataset) or {}
            value = entry.get("like_rows")
            if value is None:
                value = entry.get("bound_rows")
            if value is None:
                value = sum(item.get("rows", 0) for item in entry.get("identity_forms") or [])
            total += value or 0
        return total

    unbound = [record for record in records
               if not record["already_bound"] and record["kind"] == "buyer_unit"]
    unbound.sort(key=rows_of, reverse=True)
    no_identity = [record for record in records if record["status"] == "NO_STABLE_IDENTITY"]
    no_identity.sort(key=rows_of, reverse=True)

    def distinct_nits(record: dict) -> set[str]:
        """NITs acreditados por la evidencia, UNIDOS entre datasets.

        Sumar por dataset duplicaba el mismo NIT observado en procesos y en
        contratos y dejaba el worklist de acreditación en una fracción de su
        tamaño real: el NIT es uno, no uno por dataset.
        """
        return {nit for dataset, _ in DATASETS
                for nit in ((record["datasets"].get(dataset) or {}).get("nits_reached") or [])}

    single_nit = [record for record in no_identity if len(distinct_nits(record)) == 1]
    splits = [record for record in records if record["status"] == "SOURCE_NAME_SPLIT"]
    splits.sort(key=rows_of, reverse=True)
    inconclusive = [record for record in records if record["status"] == "INCONCLUSIVE"]
    inconclusive.sort(key=rows_of, reverse=True)
    collisions = [record for record in records
                  if record["status"] in ("FOREIGN_IDENTITY_COLLISION", "AMBIGUOUS_IDENTITY")]
    groups_missing = sorted(groups.values(), key=lambda item: item["members_blocked"], reverse=True)
    unreached_forms = [record for record in records
                       if any((record["datasets"].get(dataset) or {}).get("forms_not_reached_by_like")
                              for dataset, _ in DATASETS)]
    unreached_forms.sort(key=rows_of, reverse=True)
    split_evidenced = [record for record in records
                       if (record.get("evidence_flags") or {}).get("source_name_split_observed")]
    split_evidenced.sort(key=rows_of, reverse=True)
    composition = [record for record in records
                   if (record.get("evidence_flags") or {}).get("composition_signal")]
    composition.sort(key=rows_of, reverse=True)

    def dataset_rows(record: dict) -> dict:
        return {dataset: (record["datasets"].get(dataset) or {}).get("like_rows")
                for dataset, _ in DATASETS}

    def unbalanced(record: dict) -> bool:
        """Un dataset alcanza <1% de lo que alcanza el otro.

        Es un umbral de REPORTE, no una regla semántica: sirve para que la
        revisión matutina vea de una mirada dónde el nombre canónico no viaja
        igual entre procesos y contratos (p. ej. UNAL: 112.154 / 14).

        Un dataset en CERO es cobertura desbalanceada, no ausencia de dato: con
        `if value` el cero desaparecía del vector y el caso quedaba invisible
        (p. ej. EMPRESAS PUBLICAS DE MEDELLIN E.S.P.: 26.067 procesos / 0
        contratos). La comparación es `value is not None`, que distingue 0 filas
        medidas de una medición que no existe.
        """
        if record["kind"] != "buyer_unit" or record["already_bound"]:
            return False
        values = [value for value in dataset_rows(record).values() if value is not None]
        if len(values) < 2:
            return False
        return max(values) > 0 and min(values) / max(values) < 0.01

    unbalanced_records = [record for record in records if unbalanced(record)]
    unbalanced_records.sort(key=rows_of, reverse=True)

    def brief(record: dict) -> dict:
        flags = record.get("evidence_flags") or {}
        return {
            "canonical_name": record["canonical_name"],
            "status": record["status"],
            "identity": record["identity"],
            "rows": rows_of(record),
            "already_bound": record["already_bound"],
            "source_name_split_observed": bool(flags.get("source_name_split_observed")),
            "composition_signal": bool(flags.get("composition_signal")),
        }

    report = {
        "total": len(records),
        "classes": {name: classes.get(name, 0) for name in CLASSES},
        "bindings": {
            "existing_bindings_total": len(bound_records),
            "existing_bindings_verified": len(verified),
            "existing_drift": classes.get("BINDING_DRIFT_REVIEW", 0),
            # Lo que se midió en cada verificación, sin aplanarlo en una sola
            # cifra: liveness de nombre en los 593, chequeo de identidad ajena
            # —que exige NIT acreditado— en 18.
            "source_name_liveness_verified": len(verified),
            "foreign_identity_check": {
                "NIT_authoritative_checked": len(nit_checked),
                "BUYER_UNIT_structural_only": len(structural_only),
                "note": ("`NIT_authoritative_checked` re-observó identidad fiscal ajena contra un "
                         "NIT acreditado. `BUYER_UNIT_structural_only` verificó presencia y filas de "
                         "las formas declaradas, pero su identidad es BUYER_UNIT (no tiene NIT "
                         "propio) así que no acredita nada sobre identidad fiscal ajena."),
            },
            "candidate_ready": len(candidates),
            "candidate_requires_review": classes.get("MULTI_NAME_REVIEW", 0)
            + classes.get("BUYER_GROUP_CANDIDATE", 0),
        },
        "groups": {
            "total": len(groups),
            "ready_today": sum(1 for item in groups.values() if item["status"] == "READY_TODAY"),
            "ready_after_review": sum(1 for item in groups.values() if item["status"] == "READY_AFTER_REVIEW"),
            "partial": sum(1 for item in groups.values() if item["status"] == "PARTIAL"),
            "blocked": sum(1 for item in groups.values() if item["status"] == "BLOCKED"),
        },
        "ecosystems": {
            "institutional": classes.get("INSTITUTIONAL_ECOSYSTEM", 0),
            # Unidades declaradas por el catálogo, no entradas de composición: el
            # padre `ECOSYSTEM:ICBF:national` no es una unidad, y contarlo daba 34
            # cuando el catálogo declara 33.
            "icbf_units": sum(1 for record in records
                              if (record.get("ecosystem") or {}).get("family") == "ICBF"
                              and record["kind"] == "buyer_unit"),
            "sena_units": sum(1 for record in records if (record.get("ecosystem") or {}).get("family") == "SENA"
                              and record["kind"] == "buyer_unit"),
        },
        "top_lists": {
            "largest_unbound_buyers_by_rows": [brief(record) for record in unbound[:20]],
            "largest_source_name_splits_by_rows": [brief(record) for record in splits[:20]],
            "largest_inconclusive_by_rows": [brief(record) for record in inconclusive[:20]],
            "foreign_collision_cases": [brief(record) for record in collisions[:50]],
            "groups_with_most_missing_bindings": [
                {"group_id": item["group_id"], "status": item["status"],
                 "members_blocked": item["members_blocked"],
                 "missing_bindings": item["missing_bindings"][:20]}
                for item in groups_missing[:10]
            ],
            "no_stable_identity_with_single_observed_nit": {
                "count": len(single_nit),
                "top": [brief(record) for record in single_nit[:20]],
            },
            # Formas reales de una identidad acreditada que el LIKE NO alcanza.
            # No entran a ningún candidato (regla F) y son la señal que un
            # lote futuro tendría que resolver como Buyer Group si corresponde.
            "identities_with_unreached_source_forms": {
                "count": len(unreached_forms),
                "top": [brief(record) for record in unreached_forms[:20]],
            },
            # El canónico NO es el nombre físico publicado. Incluye bindings
            # VIGENTES (DAPRE, Soacha, Yopal…): es evidencia del fenómeno, no
            # una reclasificación de esos bindings.
            "source_name_split_evidenced": {
                "count": len(split_evidenced),
                "top": [brief(record) for record in split_evidenced[:20]],
            },
            # La identidad acreditada publica más de una unidad token-distinta:
            # pregunta de Buyer Group, sin autoridad declarada todavía.
            "identities_with_composition_signal": {
                "count": len(composition),
                "top": [brief(record) for record in composition[:20]],
            },
            # Umbral de REPORTE: un dataset alcanza <1% del otro. No clasifica
            # nada; marca dónde revisar la cobertura del nombre canónico.
            "unbalanced_dataset_coverage": {
                "count": len(unbalanced_records),
                "top": [dict(brief(record), rows_by_dataset=dataset_rows(record))
                        for record in unbalanced_records[:20]],
            },
        },
        "group_readiness": groups,
    }
    if reconciliation is not None:
        telemetry = batch_telemetry(reconciliation)
        report["progress"] = dict(reconciliation.brief(), supersessions=reconciliation.supersessions,
                                  ledger=SUPERSESSIONS_FILE)
        # Espejo en el nivel raíz: una repetición sin declarar tiene que verse sin
        # abrir `progress`, porque invalida el resultado del lote entero.
        report["undeclared_duplicates"] = reconciliation.undeclared
        report["contradictory_records"] = reconciliation.contradictory
        report["telemetry"] = telemetry
        report["evidence_integrity"] = integrity if integrity is not None else {"ok": None}
        report["report_status"] = {
            "ok": reconciliation.ok and (integrity is None or bool(integrity.get("ok"))),
            "progress_ok": reconciliation.ok,
            "evidence_integrity_ok": None if integrity is None else bool(integrity.get("ok")),
            "problems": report_problems(reconciliation, integrity or {}),
        }
    return report


def write_outputs(out: Path, records: list[dict], authorities: Authorities,
                  manifest: dict, lock: threading.Lock | None = None, *,
                  reconciliation: Reconciliation | None = None,
                  integrity: dict | None = None) -> dict:
    lock = lock or threading.Lock()
    summary = build_summary(records, authorities, reconciliation=reconciliation, integrity=integrity)
    # El conjunto que se reporta es el LOTE: la selección de esta invocación no lo
    # reduce ni lo redefine (ver `run`).
    summary["transport"] = manifest.get("transport") or (manifest.get("batch") or {}).get("transport")
    summary["generated_at"] = now_iso()
    write_json(out / "summary.json", summary)
    for status, filename in CLASS_FILES.items():
        path = out / filename
        path.write_text(
            "".join(json.dumps(record, ensure_ascii=False) + "\n"
                    for record in records if record["status"] == status),
            encoding="utf-8",
        )
    candidates = [record["candidate"] for record in records if record.get("candidate")]
    (out / "bindings_ready.jsonl").write_text(
        "".join(json.dumps(candidate, ensure_ascii=False) + "\n" for candidate in candidates),
        encoding="utf-8",
    )
    write_json(out / "buyer_group_readiness.json", summary["group_readiness"])
    write_json(out / "sena_ecosystem_coverage.json", sena_coverage(records, authorities))
    return summary


# ── CLI ─────────────────────────────────────────────────────────────────────


def read_only_settings() -> Settings:
    """`Settings` con la persistencia desviada a un temporal.

    Fuera de pytest, construir `Settings()` a secas apunta a `data/`. Este
    auditor no escribe feedback, pero el invariante no admite excepciones por
    buena conducta: la ruta real nunca debe poder escribirse desde aquí.
    """
    scratch = Path(tempfile.mkdtemp(prefix="adr073-gazetteer-audit-"))
    overrides = {
        "secop_feedback_path": scratch / "feedback.jsonl",
        "secop_feedback_db_path": scratch / "feedback.db",
    }
    return Settings(**overrides)


def git(*args: str) -> str:
    try:
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True,
                              check=False).stdout.strip()
    except OSError:
        return ""


def build_manifest(args: argparse.Namespace, inventory: list[Canonical], settings: Settings,
                   previous_manifest: dict, reconciliation: Reconciliation, *, mode: str,
                   refresh_batch: bool) -> dict:
    """Manifiesto con tres bloques que NO se pisan entre sí.

    `batch` — procedencia del LOTE original (inmutable ante `--summary-only`,
    `--only` y reconstrucciones de reportes; sólo un pase COMPLETO la actualiza,
    porque un lote resumible crece).
    `last_invocation` — qué hizo ESTA invocación (cambia en cada corrida).
    `head`/`branch` — el HEAD de esta invocación; `batch.head` es el HEAD sobre el
    que se MIDIÓ, que es otra cosa y antes quedaba como un HEAD «viejo» sin
    explicación.
    """
    if refresh_batch:
        batch = adopt_batch_provenance(previous_manifest, reconciliation, args)
        absorbed = list(batch.get("absorbed_legacy_keys") or [])
    else:
        batch = dict(previous_manifest["batch"])
        absorbed = []
    runtime_files = {
        "app/core/soql_builder.py": sha256_file(ROOT / "app/core/soql_builder.py"),
        "app/core/buyer_source_names.py": sha256_file(ROOT / "app/core/buyer_source_names.py"),
        "app/core/buyer_groups.py": sha256_file(ROOT / "app/core/buyer_groups.py"),
        "app/core/entity_resolver.py": sha256_file(ROOT / "app/core/entity_resolver.py"),
    }
    manifest: dict[str, Any] = {
        "audit": "ADR073_GAZETTEER_WIDE_SOURCE_BINDING_AUDIT_V1",
        "head": git("rev-parse", "HEAD"),
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "script": "scripts/audits/audit_gazetteer_source_bindings_v1.py",
        "datasets": {dataset: field for dataset, field in DATASETS},
        "nit_column": NIT_COLUMN,
        "domain": settings.datos_gov_domain,
        "group_limit": GROUP_LIMIT,
        "total_canonical_identities": len(inventory),
        "identities_with_accredited_key": sum(1 for item in inventory if item.identity),
        "ecosystem_entries": sum(1 for item in inventory if item.kind == "ecosystem"),
        "already_bound": sum(1 for item in inventory if item.already_bound),
        "batch": batch,
        "last_invocation": {
            "mode": mode,
            "started_at": now_iso(),
            "git_status_short": git("status", "--short")[:4000],
            "script_sha256": sha256_file(Path(__file__)),
            "authority_sha256": {name: sha256_file(ROOT / path) for name, path in AUTHORITY_FILES.items()},
            "runtime_files_sha256": runtime_files,
            "transport": args.transport,
            "workers": args.workers,
            "timeout_seconds": args.timeout,
            "retries": args.retries,
            "options": {"limit": args.limit, "only": args.only, "start_after": args.start_after,
                        "resume": args.resume, "recheck_existing": args.recheck_existing,
                        "summary_only": args.summary_only},
        },
        "progress": reconciliation.brief(),
        "report_rebuilt_at": now_iso(),
    }
    if mode != "measure":
        # Un micro-reproceso no borra lo que el lote ya declaró: se arrastra tal
        # cual, salvo lo que esta misma corrida absorbió en `batch`.
        carried = []
        for key in LEGACY_BATCH_KEYS:
            if key in absorbed or key not in previous_manifest:
                continue
            manifest[key] = previous_manifest[key]
            carried.append(key)
        if carried:
            manifest["last_invocation"]["carried_batch_keys"] = carried
    return manifest


def select_inventory(inventory: list[Canonical], args: argparse.Namespace) -> list[Canonical]:
    selected = list(inventory)
    if args.only:
        wanted = {normalize_text(name) for name in args.only}
        selected = [item for item in selected if normalize_text(item.canonical_name) in wanted
                    or item.canonical_name in args.only]
    if args.start_after:
        pivot = normalize_text(args.start_after)
        selected = [item for item in selected if normalize_text(item.canonical_name) > pivot]
    if args.limit:
        selected = selected[: args.limit]
    return selected


def source_from_settings(settings: Settings, args: argparse.Namespace) -> Source:
    """Construye la fuente real. Los tests inyectan la suya por `source_factory`."""
    return Source(settings, transport=args.transport, timeout_seconds=args.timeout,
                  retries=args.retries)


def run(args: argparse.Namespace, source_factory: Callable[[Settings, argparse.Namespace], Source] | None = None) -> int:
    started = time.time()
    started_at = now_iso()
    authorities = load_authorities()
    inventory = build_inventory(authorities)
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "manifest.json"
    previous_manifest: dict = {}
    if manifest_path.exists():
        try:
            previous_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except ValueError:
            previous_manifest = {}
    progress_path = out / PROGRESS_FILE
    ledger_path = out / SUPERSESSIONS_FILE

    # La bitácora manda: el intento vigente de cada canónico sale de sus propias
    # declaraciones, no del orden en que se escribieron las líneas.
    entries = build_progress_index(progress_path)
    declared, ledger_problems = read_supersessions(ledger_path)
    reconciliation = reconcile_progress(entries, declared)
    reconciliation.problems.extend(ledger_problems)

    done: dict[str, dict] = {canonical: entry.record
                             for canonical, entry in reconciliation.active.items()}
    if args.recheck_existing:
        done = {}

    selected = select_inventory(inventory, args)
    pending = [item for item in selected if args.recheck_existing or item.hash not in done]
    skipped = len(selected) - len(pending)
    will_measure = bool(pending) and not args.summary_only
    mode = invocation_mode(args, will_measure)
    # Un pase COMPLETO (o el primer manifiesto con bloques) es el único que
    # (re)escribe la procedencia del lote. Un micro-reproceso o un rebuild la
    # reciben tal cual quedó.
    refresh_batch = mode == "measure" or not isinstance(previous_manifest.get("batch"), dict)

    manifest = build_manifest(args, inventory, read_only_settings(),
                              previous_manifest, reconciliation, mode=mode,
                              refresh_batch=refresh_batch)
    manifest["last_invocation"].update({
        "started_at": started_at, "selected": len(selected), "pending": len(pending),
        "skipped_resume": skipped,
    })
    write_json(manifest_path, manifest)

    print(f"inventario: {len(inventory)} · seleccionados: {len(selected)} · "
          f"pendientes: {len(pending)} · saltados por --resume: {skipped} · "
          f"bitácora: {reconciliation.progress_lines} líneas / "
          f"{reconciliation.canonical_unique} canónicos / "
          f"{len(reconciliation.supersessions)} reemplazos declarados / "
          f"{len(reconciliation.undeclared)} repeticiones sin declarar", flush=True)

    # La evidencia se verifica ANTES de construir el resumen: si un raw record no
    # cuadra con `records.sha256`, el resumen no puede nacer diciendo que todo está
    # bien. Y `records.sha256` no se regenera sobre evidencia alterada.
    integrity = verify_evidence(
        out, [records_relative_path(canonical) for canonical in sorted(reconciliation.active)])

    invocation_telemetry = None
    if will_measure:
        settings = read_only_settings()
        source = (source_factory or source_from_settings)(settings, args)
        auditor = Auditor(args, settings, source=source, prior=reconciliation.attempts)
        auditor.total = len(pending)
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [pool.submit(auditor.process, item) for item in pending]
            for future in as_completed(futures):
                future.result()
        invocation_telemetry = dict(source.telemetry)
        # Se relee la bitácora: los intentos nuevos acaban de declarar a quién
        # reemplazan, y el archivo de hashes tiene que cubrir los registros nuevos.
        entries = build_progress_index(progress_path)
        reconciliation = reconcile_progress(entries, declared)
        reconciliation.problems.extend(ledger_problems)
        expected = sorted(records_relative_path(canonical) for canonical in reconciliation.active)
        if integrity["ok"]:
            write_records_hash_file(out, expected)
            integrity = verify_evidence(out, expected)

    # El resumen describe el LOTE, no la selección de esta invocación: un
    # `--only` de reproceso sobre el directorio canónico reconstruía el summary
    # con dos o tres registros y pisaba el del lote completo. Los registros que
    # se reportan son los intentos VIGENTES de progress.jsonl, y `uncovered` es lo
    # que falta del INVENTARIO, no de lo seleccionado.
    fresh_all = {canonical: entry.record for canonical, entry in reconciliation.active.items()}
    records = [fresh_all[item.hash] for item in inventory if item.hash in fresh_all]
    problems = report_problems(reconciliation, integrity)
    # Manifiesto FINAL, ya con la bitácora medida: el bloque `batch` se deriva del
    # resultado completo, no del estado previo a medir.
    manifest = build_manifest(args, inventory, read_only_settings(),
                              previous_manifest, reconciliation, mode=mode,
                              refresh_batch=refresh_batch)
    manifest["last_invocation"].update({
        "started_at": started_at,
        "finished_at": now_iso(),
        "elapsed_seconds": round(time.time() - started, 1),
        "selected": len(selected), "pending": len(pending), "skipped_resume": skipped,
        "processed": len(pending) if will_measure else 0,
        "batch_records": len(records),
    })
    if invocation_telemetry is not None:
        manifest["last_invocation"]["source_telemetry"] = invocation_telemetry
    manifest["progress"] = reconciliation.brief()
    manifest["report_status"] = {
        "ok": reconciliation.ok and integrity["ok"],
        "progress_ok": reconciliation.ok,
        "evidence_integrity_ok": integrity["ok"],
        "problems": problems,
    }
    manifest["report_rebuilt_at"] = now_iso()
    write_json(manifest_path, manifest)
    summary = write_outputs(out, records, authorities, manifest,
                            reconciliation=reconciliation, integrity=integrity)
    uncovered = [item.canonical_name for item in inventory if item.hash not in fresh_all]
    print(json.dumps({
        "total": summary["total"],
        "classes": summary["classes"],
        "groups": summary["groups"],
        "bindings": summary["bindings"],
        "progress": summary.get("progress"),
        "uncovered_from_inventory": len(uncovered),
        "elapsed_seconds": manifest["last_invocation"]["elapsed_seconds"],
    }, ensure_ascii=False, indent=2), flush=True)
    if problems:
        print("REPORTE INCOMPLETO — el resumen queda marcado como no concluyente:", flush=True)
        for problem in problems:
            print(f"  · {problem}", flush=True)
    print(f"escrito {out}", flush=True)
    # Código ≠ 0: un reporte que no se puede leer como resultado cerrado no puede
    # salir con la misma señal que uno que sí.
    return 0 if not problems else 2


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--resume", action="store_true",
                        help="saltar identidades ya cerradas en progress.jsonl")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit", type=int, default=0, help="máximo de identidades a procesar")
    parser.add_argument("--start-after", dest="start_after", default=None)
    parser.add_argument("--only", action="append", default=None,
                        help="procesar sólo estos canónicos (repetible)")
    parser.add_argument("--recheck-existing", action="store_true",
                        help="reprocesar identidades ya cerradas")
    parser.add_argument("--output", default=str(DEFAULT_OUT))
    parser.add_argument("--transport", choices=("soda3", "classic"), default="soda3")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--summary-only", action="store_true",
                        help="reconstruir reportes desde progress.jsonl sin tocar la red")
    parser.add_argument("--env-file", default=None,
                        help=".env a cargar (por defecto el del checkout principal)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env_file = Path(args.env_file) if args.env_file else Path(ROOT) / ".env"
    if env_file.exists():
        try:
            from dotenv import load_dotenv

            load_dotenv(env_file)
        except ImportError:  # sin dotenv se opera con el entorno tal cual
            pass
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
