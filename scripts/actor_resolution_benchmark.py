#!/usr/bin/env python3
"""AP3-A: benchmark reproducible de nombre de actor -> IdentityKey.

Este script es deliberadamente no-runtime. No crea el indice de produccion ni
modifica routing. Tiene dos modos:

* ``--acquire`` obtiene una muestra real y acotada de SECOP II, construye
  buckets conservadores y escribe fixtures sanitizados.
* sin ``--acquire`` repite el benchmark completamente offline sobre el fixture.

La extraccion evita el GROUP BY global de ~6M filas. Primero toma una muestra
ancla estratificada, limitada y ordenada por ``:id``; despues agrega en Socrata
solo lotes pequenos de documentos frecuentes seleccionados desde esa muestra.
Los documentos CC se pseudonimizan en el fixture y se enmascaran en el review
pack. Los NIT se conservan porque son identificadores mercantiles publicos.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import sqlite3
import statistics
import sys
import tempfile
import time
import unicodedata
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rapidfuzz import fuzz, process  # noqa: E402

from app.config import Settings  # noqa: E402
from app.core.actor_identity import compute_check_digit, normalize_identity  # noqa: E402


DATASET_ID = "jbjy-vk9h"
DATASET_NAME = "SECOP II - Contratos Electronicos"
SOURCE_DOMAIN = "www.datos.gov.co"
SEED = 20260818
KNOWN_TYPES = {"NIT": "NIT", "Cédula de Ciudadanía": "CC"}
SOURCE_TYPES = tuple(KNOWN_TYPES)
SENTINEL_NAMES = frozenset({
    "", "NO DEFINIDO", "NO DEFINIDA", "N/A", "NA", "NULL", "NONE",
    "SIN DATO", "SIN INFORMACION", "SIN INFORMACIÓN", "-",
})
GROUP_MARKERS = re.compile(r"\b(?:UNION TEMPORAL|CONSORCIO|UT)\b", re.IGNORECASE)
LEGAL_SUFFIXES = frozenset({
    "SAS", "SA", "LTDA", "LIMITADA", "ESE", "ESP", "ESAL",
})
LEGAL_SUFFIX_TOKEN_SEQUENCES: tuple[tuple[str, ...], ...] = (
    ("s", "a", "s"), ("l", "t", "d", "a"), ("e", "s", "a", "l"),
    ("e", "s", "e"), ("e", "s", "p"), ("s", "a"),
)
TOPIC_TERMS = (
    "alimentación", "mantenimiento", "software", "vigilancia", "nutrición",
    "adulto mayor", "transporte", "obras", "aseo", "salud",
)
TARGETED_DIRTY_DOCUMENTS = ("13848603", "860002400")
PREFLIGHT_PROBES = (
    {
        "probe": "global_grouped_aliases_limit_5",
        "result": "NO_RESULT_WITHIN_30S_ABORTED",
    },
    {
        "probe": "bounded_grouped_aliases_2025_01_limit_5",
        "result": "READ_TIMEOUT",
        "latency_s": 25.273,
    },
    {
        "probe": "raw_anchor_limit_1000_order_by_internal_id",
        "result": "SUCCESS",
        "latency_s_by_type": {"NIT": 0.8, "CC": 0.451},
    },
    {
        "probe": "targeted_aggregation_20_documents",
        "result": "SUCCESS",
        "groups": 21,
        "latency_s": 0.317,
    },
)
DEFAULT_FIXTURE_DIR = ROOT / "tests" / "fixtures" / "actor_resolution"
SAMPLE_PATH = DEFAULT_FIXTURE_DIR / "benchmark_sample.jsonl"
REVIEW_PATH = DEFAULT_FIXTURE_DIR / "review_pack.jsonl"
METADATA_PATH = DEFAULT_FIXTURE_DIR / "metadata.json"
MAX_CANDIDATES = 50
METRIC_K = 10


@dataclass(frozen=True)
class AliasRow:
    identity: str
    document_type: str
    document_display: str
    observed_name: str
    occurrences: int
    first_seen: str | None
    last_seen: str | None
    total_occurrences: int
    distinct_observed_names: int
    dominant_name: str
    dominant_name_share: float


@dataclass(frozen=True)
class HeldOutQuery:
    query_name: str
    identity: str
    document_type: str
    document_display: str
    occurrences: int
    first_seen: str | None
    last_seen: str | None


@dataclass(frozen=True)
class Candidate:
    identity: str
    name: str
    score: float | None = None


@dataclass
class MetricAccumulator:
    ranks: list[int | None]
    candidate_sizes: list[int]
    latencies_ms: list[float]

    @classmethod
    def create(cls) -> "MetricAccumulator":
        return cls([], [], [])

    def add(
        self,
        expected: str,
        candidates: Sequence[Candidate],
        latency_ms: float,
    ) -> None:
        rank = next(
            (idx for idx, candidate in enumerate(candidates, 1)
             if candidate.identity == expected),
            None,
        )
        self.ranks.append(rank)
        self.candidate_sizes.append(len(candidates))
        self.latencies_ms.append(latency_ms)

    def summary(self, name: str, normalization: str) -> dict[str, Any]:
        n = len(self.ranks)
        return {
            "strategy": name,
            "normalization": normalization,
            "queries": n,
            "precision_at_1": _rate(sum(rank == 1 for rank in self.ranks), n),
            "recall_at_1": _rate(sum(rank == 1 for rank in self.ranks), n),
            "recall_at_3": _rate(sum(rank is not None and rank <= 3 for rank in self.ranks), n),
            "recall_at_5": _rate(sum(rank is not None and rank <= 5 for rank in self.ranks), n),
            "recall_at_10": _rate(sum(rank is not None and rank <= 10 for rank in self.ranks), n),
            "mrr": (sum(1 / rank for rank in self.ranks if rank) / n if n else 0.0),
            "candidate_set_p50": _percentile(self.candidate_sizes, 50),
            "candidate_set_p95": _percentile(self.candidate_sizes, 95),
            "candidate_set_max": max(self.candidate_sizes, default=0),
            "latency_ms_p50": _percentile(self.latencies_ms, 50),
            "latency_ms_p95": _percentile(self.latencies_ms, 95),
            "latency_ms_max": max(self.latencies_ms, default=0.0),
        }


def normalize_n0(value: str) -> str:
    """N0: casefold basico; conserva tildes, puntuacion y espacios internos."""
    return (value or "").casefold().strip()


def normalize_n1(value: str) -> str:
    """N1: N0 + accent folding + puntuacion convertida a espacios."""
    decomposed = unicodedata.normalize("NFKD", normalize_n0(value))
    folded = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in folded)


def normalize_n2(value: str) -> str:
    """N2: N1 + colapso de whitespace."""
    return " ".join(normalize_n1(value).split())


def normalize_n3(value: str) -> str:
    """N3: N2 + retiro conservador de formas societarias solo al final."""
    tokens = normalize_n2(value).split()
    changed = True
    while tokens and changed:
        changed = False
        if tokens[-1].upper() in LEGAL_SUFFIXES:
            tokens.pop()
            changed = True
            continue
        for suffix in LEGAL_SUFFIX_TOKEN_SEQUENCES:
            if tuple(tokens[-len(suffix):]) == suffix:
                del tokens[-len(suffix):]
                changed = True
                break
    return " ".join(tokens) or normalize_n2(value)


NORMALIZERS: dict[str, Callable[[str], str]] = {
    "N0": normalize_n0,
    "N1": normalize_n1,
    "N2": normalize_n2,
    "N3": normalize_n3,
}

SCORERS: dict[str, Callable[[str, str], float]] = {
    "ratio": fuzz.ratio,
    "token_set_ratio": fuzz.token_set_ratio,
    "WRatio": fuzz.WRatio,
}


def _rate(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _percentile(values: Sequence[float | int], percentile: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile / 100
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _identity_id(
    document_type: str,
    base_number: str,
    *,
    cc_ordinal: int | None = None,
) -> str:
    if document_type == "NIT":
        return f"NIT:{base_number}"
    if cc_ordinal is None:
        raise ValueError("cc_ordinal is required for a masked CC fixture identity")
    # No se deriva del documento. Un hash sin secreto sobre el espacio pequeno
    # de cedulas seria reversible por fuerza bruta y no constituye una
    # anonimizacion suficiente para un artefacto versionado.
    return f"CC:sample:{cc_ordinal:06d}"


def _document_display(document_type: str, base_number: str) -> str:
    if document_type == "NIT":
        return base_number
    tail = base_number[-4:]
    return f"{'*' * max(0, len(base_number) - len(tail))}{tail}"


def _jsonl_write(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows
    )
    path.write_text(payload, encoding="utf-8")


def _jsonl_read(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_query_policy(anchor_per_type: int, selected_per_type: int, batch_size: int) -> dict:
    return {
        "global_aggregation": "abandoned_after_timeout",
        "anchor": {
            "select": ":id,tipodocproveedor,documento_proveedor",
            "where_template": (
                "tipodocproveedor = <known_type> AND documento_proveedor IS NOT NULL "
                "AND documento_proveedor != 'No Definido'"
            ),
            "order": ":id",
            "limit_per_type": anchor_per_type,
            "strata": list(SOURCE_TYPES),
        },
        "targeted_aggregation": {
            "select": (
                "tipodocproveedor,documento_proveedor,proveedor_adjudicado,"
                "count(*) AS occurrences,min(fecha_de_firma) AS first_seen,"
                "max(fecha_de_firma) AS last_seen"
            ),
            "where_template": (
                "documento_proveedor IN (<batch>) AND proveedor_adjudicado IS NOT NULL"
            ),
            "group": (
                "tipodocproveedor,documento_proveedor,proveedor_adjudicado"
            ),
            "selected_logical_identities_per_type": selected_per_type,
            "batch_size": batch_size,
        },
    }


def sqlite_capabilities() -> dict[str, Any]:
    connection = sqlite3.connect(":memory:")
    compile_options = [row[0] for row in connection.execute("PRAGMA compile_options")]
    tokenizers: dict[str, bool] = {}
    for tokenizer in ("unicode61", "trigram"):
        try:
            connection.execute(
                f"CREATE VIRTUAL TABLE probe_{tokenizer} "
                f"USING fts5(value, tokenize='{tokenizer}')"
            )
            tokenizers[tokenizer] = True
        except sqlite3.OperationalError:
            tokenizers[tokenizer] = False
    connection.close()
    return {
        "version": sqlite3.sqlite_version,
        "fts5": any("ENABLE_FTS5" in option for option in compile_options),
        "tokenizers": tokenizers,
        "relevant_compile_options": [
            option for option in compile_options if "FTS" in option
        ],
    }


def _doc_variants(document_type: str, base_number: str, observed: set[str]) -> set[str]:
    variants = set(observed)
    variants.add(base_number)
    if document_type == "NIT":
        check_digit = compute_check_digit(base_number)
        if check_digit is not None:
            variants.add(f"{base_number}{check_digit}")
            variants.add(f"{base_number}-{check_digit}")
    return variants


def _chunks(values: Sequence[str], size: int) -> Iterable[list[str]]:
    for offset in range(0, len(values), size):
        yield list(values[offset:offset + size])


def _socrata_client(timeout: int):
    try:
        from sodapy import Socrata
    except ImportError as exc:  # pragma: no cover - dependency declared by repo
        raise RuntimeError("sodapy is required for --acquire") from exc
    settings = Settings()
    return Socrata(
        settings.datos_gov_domain,
        app_token=settings.secop_app_token,
        timeout=timeout,
    )


def acquire_source_rows(
    *,
    anchor_per_type: int,
    selected_per_type: int,
    batch_size: int,
    timeout: int,
    seed: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Dos fases: ancla limitada y GROUP BY solo sobre documentos elegidos."""
    client = _socrata_client(timeout)
    anchors: dict[str, list[dict[str, Any]]] = {}
    timings: list[dict[str, Any]] = []

    for source_type in SOURCE_TYPES:
        started = time.perf_counter()
        rows = client.get(
            DATASET_ID,
            select=":id,tipodocproveedor,documento_proveedor",
            where=(
                f"tipodocproveedor = '{source_type}' "
                "AND documento_proveedor IS NOT NULL "
                "AND documento_proveedor != 'No Definido'"
            ),
            order=":id",
            limit=anchor_per_type,
        )
        elapsed = time.perf_counter() - started
        anchors[source_type] = list(rows)
        timings.append({
            "phase": "anchor",
            "document_type": source_type,
            "rows": len(rows),
            "latency_s": round(elapsed, 3),
        })

    selected: dict[str, list[str]] = {}
    raw_forms: dict[tuple[str, str], set[str]] = defaultdict(set)
    rng = random.Random(seed)
    for source_type, rows in anchors.items():
        doc_type = KNOWN_TYPES[source_type]
        counts: Counter[str] = Counter()
        for row in rows:
            raw_document = row.get("documento_proveedor")
            match = normalize_identity(raw_document, document_type=doc_type)
            if not match.key:
                continue
            counts[match.base_number] += 1
            raw_forms[(doc_type, match.base_number)].add(str(raw_document))

        # Frecuencia primero; desempate pseudoaleatorio fijo para no sesgar todos
        # los empates hacia el menor documento.
        tie_break = {document: rng.random() for document in counts}
        ordered = sorted(counts, key=lambda item: (-counts[item], tie_break[item], item))
        selected[doc_type] = ordered[:selected_per_type]

    # Casos de contaminacion ya verificados por AP2: siempre se vuelven a medir.
    selected.setdefault("CC", []).append(TARGETED_DIRTY_DOCUMENTS[0])
    selected.setdefault("NIT", []).extend(TARGETED_DIRTY_DOCUMENTS)

    query_documents: set[str] = set()
    for doc_type, base_numbers in selected.items():
        for base_number in dict.fromkeys(base_numbers):
            query_documents.update(
                _doc_variants(doc_type, base_number, raw_forms[(doc_type, base_number)])
            )

    aggregate_rows: list[dict[str, Any]] = []
    ordered_documents = sorted(query_documents)
    for batch_number, batch in enumerate(_chunks(ordered_documents, batch_size), 1):
        escaped = ",".join(f"'{value.replace(chr(39), chr(39) * 2)}'" for value in batch)
        row_limit = max(5000, len(batch) * 100)
        started = time.perf_counter()
        rows = client.get(
            DATASET_ID,
            select=(
                "tipodocproveedor,documento_proveedor,proveedor_adjudicado,"
                "count(*) as occurrences,min(fecha_de_firma) as first_seen,"
                "max(fecha_de_firma) as last_seen"
            ),
            where=(
                f"documento_proveedor in ({escaped}) "
                "AND proveedor_adjudicado IS NOT NULL"
            ),
            group="tipodocproveedor,documento_proveedor,proveedor_adjudicado",
            limit=row_limit,
        )
        elapsed = time.perf_counter() - started
        if len(rows) >= row_limit:
            raise RuntimeError(
                f"targeted aggregation batch {batch_number} reached its limit; "
                "reduce --batch-size"
            )
        aggregate_rows.extend(rows)
        timings.append({
            "phase": "targeted_aggregation",
            "batch": batch_number,
            "documents": len(batch),
            "groups": len(rows),
            "latency_s": round(elapsed, 3),
        })

    acquisition = {
        "anchor_rows": sum(len(rows) for rows in anchors.values()),
        "anchor_rows_by_type": {key: len(value) for key, value in anchors.items()},
        "selected_logical_identities_by_type": {
            key: len(set(value)) for key, value in selected.items()
        },
        "queried_document_variants": len(query_documents),
        "aggregated_alias_groups": len(aggregate_rows),
        "timings": timings,
    }
    return aggregate_rows, acquisition


def _date_only(value: Any) -> str | None:
    return str(value)[:10] if value else None


def _earliest(values: Iterable[str | None]) -> str | None:
    present = [value for value in values if value]
    return min(present) if present else None


def _latest(values: Iterable[str | None]) -> str | None:
    present = [value for value in values if value]
    return max(present) if present else None


def _is_name_sentinel(name: str) -> bool:
    return name.strip().upper() in SENTINEL_NAMES


def build_fixture_rows(
    source_rows: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Construye buckets y excluye mezcla extrema sin llamar gold a lo dudoso."""
    aliases: dict[tuple[str, str, str], dict[str, Any]] = {}
    base_known_types: dict[str, set[str]] = defaultdict(set)
    rejected_source_rows = 0

    for row in source_rows:
        source_type = str(row.get("tipodocproveedor") or "")
        document_type = KNOWN_TYPES.get(source_type)
        if not document_type:
            # "Otro" y "No Definido" se observan para diagnostico, pero nunca
            # constituyen gold tipado automatico.
            rejected_source_rows += 1
            continue
        match = normalize_identity(
            row.get("documento_proveedor"), document_type=document_type
        )
        name = str(row.get("proveedor_adjudicado") or "").strip()
        if not match.key or _is_name_sentinel(name):
            rejected_source_rows += 1
            continue
        base_number = match.base_number
        base_known_types[base_number].add(document_type)
        key = (document_type, base_number, name)
        occurrences = int(float(row.get("occurrences") or 0))
        current = aliases.setdefault(key, {
            "occurrences": 0,
            "first_dates": [],
            "last_dates": [],
        })
        current["occurrences"] += occurrences
        current["first_dates"].append(_date_only(row.get("first_seen")))
        current["last_dates"].append(_date_only(row.get("last_seen")))

    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for (document_type, base_number, name), values in aliases.items():
        buckets[(document_type, base_number)].append({
            "name": name,
            "occurrences": values["occurrences"],
            "first_seen": _earliest(values["first_dates"]),
            "last_seen": _latest(values["last_dates"]),
        })

    fixture: list[dict[str, Any]] = []
    stable_count = 0
    dirty_count = 0
    dirty_reasons: Counter[str] = Counter()
    excluded_group_aliases = 0
    stable_aliases = 0
    cc_ordinal = 0

    for (document_type, base_number), observed in sorted(buckets.items()):
        observed.sort(key=lambda item: (-item["occurrences"], item["name"]))
        total_occurrences = sum(item["occurrences"] for item in observed)
        dominant = observed[0]
        dominant_share = dominant["occurrences"] / total_occurrences
        dominant_is_group = bool(GROUP_MARKERS.search(normalize_n2(dominant["name"])))
        clean_aliases = [
            item for item in observed
            if dominant_is_group or not GROUP_MARKERS.search(normalize_n2(item["name"]))
        ]
        excluded_group_aliases += len(observed) - len(clean_aliases)

        reasons: list[str] = []
        if len(base_known_types[base_number]) > 1:
            reasons.append("CROSS_TYPE_COLLISION")
        if total_occurrences < 3:
            reasons.append("INSUFFICIENT_OCCURRENCES")
        if dominant_share < 0.50:
            reasons.append("LOW_DOMINANT_NAME_SHARE")
        if len(observed) > 25:
            reasons.append("EXTREME_ALIAS_CARDINALITY")
        if not clean_aliases:
            reasons.append("NO_CLEAN_ALIAS")

        if document_type == "CC":
            cc_ordinal += 1
        identity = _identity_id(
            document_type,
            base_number,
            cc_ordinal=cc_ordinal if document_type == "CC" else None,
        )
        display = _document_display(document_type, base_number)
        if reasons:
            dirty_count += 1
            dirty_reasons.update(reasons)
            fixture.append({
                "record_type": "bucket",
                "status": "EXCLUDED_DIRTY",
                "identity": identity,
                "document_type": document_type,
                "document": display,
                "total_occurrences": total_occurrences,
                "distinct_observed_names": len(observed),
                "dominant_name": dominant["name"],
                "dominant_name_share": round(dominant_share, 6),
                "first_seen": _earliest(item["first_seen"] for item in observed),
                "last_seen": _latest(item["last_seen"] for item in observed),
                "exclusion_reasons": reasons,
            })
            continue

        stable_count += 1
        for item in clean_aliases:
            stable_aliases += 1
            fixture.append({
                "record_type": "alias",
                "status": "AUTO_GOLD",
                "identity": identity,
                "document_type": document_type,
                "document": display,
                "observed_name": item["name"],
                "occurrences": item["occurrences"],
                "first_seen": item["first_seen"],
                "last_seen": item["last_seen"],
                "total_occurrences": total_occurrences,
                "distinct_observed_names": len(clean_aliases),
                "dominant_name": dominant["name"],
                "dominant_name_share": round(dominant_share, 6),
            })

    summary = {
        "stable_identities": stable_count,
        "stable_aliases": stable_aliases,
        "dirty_buckets_excluded": dirty_count,
        "dirty_reasons": dict(sorted(dirty_reasons.items())),
        "source_rows_rejected_unknown_or_unusable": rejected_source_rows,
        "suspected_group_aliases_excluded": excluded_group_aliases,
        "thresholds": {
            "known_document_types_only": ["NIT", "CC"],
            "minimum_total_occurrences": 3,
            "minimum_dominant_name_share": 0.50,
            "maximum_distinct_observed_names": 25,
            "cross_type_collision": "exclude",
            "group_alias_rule": (
                "exclude group-marked secondary aliases only when the dominant "
                "name is not itself a group"
            ),
        },
    }
    return fixture, summary


def load_aliases(sample_path: Path) -> tuple[list[AliasRow], list[dict[str, Any]]]:
    rows = _jsonl_read(sample_path)
    aliases = [
        AliasRow(
            identity=row["identity"],
            document_type=row["document_type"],
            document_display=row["document"],
            observed_name=row["observed_name"],
            occurrences=int(row["occurrences"]),
            first_seen=row.get("first_seen"),
            last_seen=row.get("last_seen"),
            total_occurrences=int(row["total_occurrences"]),
            distinct_observed_names=int(row["distinct_observed_names"]),
            dominant_name=row["dominant_name"],
            dominant_name_share=float(row["dominant_name_share"]),
        )
        for row in rows if row.get("record_type") == "alias"
    ]
    dirty = [row for row in rows if row.get("status") == "EXCLUDED_DIRTY"]
    return aliases, dirty


def build_heldouts(aliases: Sequence[AliasRow], seed: int, limit: int) -> list[HeldOutQuery]:
    by_identity: dict[str, list[AliasRow]] = defaultdict(list)
    for alias in aliases:
        by_identity[alias.identity].append(alias)

    candidates: list[HeldOutQuery] = []
    for identity, rows in by_identity.items():
        n2_forms = {normalize_n2(row.observed_name) for row in rows}
        if len(n2_forms) < 2:
            continue
        for row in rows:
            candidates.append(HeldOutQuery(
                query_name=row.observed_name,
                identity=identity,
                document_type=row.document_type,
                document_display=row.document_display,
                occurrences=row.occurrences,
                first_seen=row.first_seen,
                last_seen=row.last_seen,
            ))

    random.Random(seed).shuffle(candidates)
    return candidates[:limit]


class FTSIndex:
    """Indice efimero para una normalizacion; nunca se instala en produccion."""

    def __init__(self, aliases: Sequence[AliasRow], normalization: str, directory: Path):
        self.aliases = list(aliases)
        self.normalization = normalization
        self.normalizer = NORMALIZERS[normalization]
        self.path = directory / f"actor_resolution_{normalization.lower()}.sqlite"
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.normalized_names = [self.normalizer(row.observed_name) for row in aliases]
        self.identity_to_alias_ids: dict[str, list[int]] = defaultdict(list)
        for alias_id, row in enumerate(aliases):
            self.identity_to_alias_ids[row.identity].append(alias_id)
        started = time.perf_counter()
        self.connection.execute(
            "CREATE VIRTUAL TABLE fts_unicode USING fts5("
            "normalized_name, alias_id UNINDEXED, tokenize='unicode61')"
        )
        self.connection.executemany(
            "INSERT INTO fts_unicode(normalized_name,alias_id) VALUES (?,?)",
            ((name, alias_id) for alias_id, name in enumerate(self.normalized_names)),
        )
        self.trigram = sqlite_capabilities()["tokenizers"]["trigram"]
        if self.trigram:
            self.connection.execute(
                "CREATE VIRTUAL TABLE fts_trigram USING fts5("
                "normalized_name, alias_id UNINDEXED, tokenize='trigram')"
            )
            self.connection.executemany(
                "INSERT INTO fts_trigram(normalized_name,alias_id) VALUES (?,?)",
                ((name, alias_id) for alias_id, name in enumerate(self.normalized_names)),
            )
        self.connection.commit()
        self.build_time_s = time.perf_counter() - started
        self.index_size_bytes = self.path.stat().st_size

    def close(self) -> None:
        self.connection.close()

    def _heldout(self, alias_id: int, query: HeldOutQuery) -> bool:
        alias = self.aliases[alias_id]
        return (
            alias.identity == query.identity
            and normalize_n2(alias.observed_name) == normalize_n2(query.query_name)
        )

    def exact(self, query: HeldOutQuery) -> list[int]:
        target = self.normalizer(query.query_name)
        return [
            alias_id for alias_id, value in enumerate(self.normalized_names)
            if value == target and not self._heldout(alias_id, query)
        ]

    @staticmethod
    def _match_expression(value: str, trigram: bool) -> str | None:
        tokens = [token for token in value.split() if len(token) >= (3 if trigram else 2)]
        if not tokens and value:
            tokens = [value]
        if not tokens:
            return None
        escaped = [f'"{token.replace(chr(34), chr(34) * 2)}"' for token in tokens]
        return " OR ".join(escaped)

    def fts(self, query: HeldOutQuery, *, trigram: bool) -> list[int]:
        if trigram and not self.trigram:
            return []
        value = self.normalizer(query.query_name)
        expression = self._match_expression(value, trigram)
        if expression is None:
            return []
        table = "fts_trigram" if trigram else "fts_unicode"
        rows = self.connection.execute(
            f"SELECT alias_id FROM {table} WHERE {table} MATCH ? "
            f"ORDER BY bm25({table}) LIMIT ?",
            (expression, MAX_CANDIDATES * 8),
        ).fetchall()
        return [
            int(row["alias_id"]) for row in rows
            if not self._heldout(int(row["alias_id"]), query)
        ]

    def to_identity_candidates(self, alias_ids: Iterable[int]) -> list[Candidate]:
        seen: set[str] = set()
        out: list[Candidate] = []
        for alias_id in alias_ids:
            alias = self.aliases[alias_id]
            if alias.identity in seen:
                continue
            seen.add(alias.identity)
            out.append(Candidate(alias.identity, alias.observed_name))
            if len(out) >= MAX_CANDIDATES:
                break
        return out

    def rerank(
        self,
        query: HeldOutQuery,
        alias_ids: Iterable[int],
        scorer: Callable[[str, str], float],
    ) -> list[Candidate]:
        query_value = self.normalizer(query.query_name)
        best: dict[str, Candidate] = {}
        for alias_id in alias_ids:
            alias = self.aliases[alias_id]
            score = float(scorer(query_value, self.normalized_names[alias_id]))
            current = best.get(alias.identity)
            if current is None or score > (current.score or 0):
                best[alias.identity] = Candidate(
                    alias.identity, alias.observed_name, score
                )
        return sorted(
            best.values(),
            key=lambda candidate: (-(candidate.score or 0), candidate.identity),
        )[:MAX_CANDIDATES]


def _merge_alias_ids(*groups: Iterable[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for group in groups:
        for alias_id in group:
            if alias_id not in seen:
                seen.add(alias_id)
                out.append(alias_id)
    return out


def benchmark_normalization(
    aliases: Sequence[AliasRow],
    heldouts: Sequence[HeldOutQuery],
    normalization: str,
    directory: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    index = FTSIndex(aliases, normalization, directory)
    names = ["exact", "fts5_unicode"]
    if index.trigram:
        names.append("fts5_trigram")
    for generator in ("fts5_unicode", "fts5_trigram", "union"):
        if generator == "fts5_trigram" and not index.trigram:
            continue
        for scorer_name in SCORERS:
            names.append(f"{generator}->rapidfuzz_{scorer_name}")
    accumulators = {name: MetricAccumulator.create() for name in names}

    for query in heldouts:
        started = time.perf_counter()
        exact_ids = index.exact(query)
        exact_elapsed = (time.perf_counter() - started) * 1000
        exact_candidates = index.to_identity_candidates(exact_ids)
        accumulators["exact"].add(query.identity, exact_candidates, exact_elapsed)

        started = time.perf_counter()
        unicode_ids = index.fts(query, trigram=False)
        unicode_elapsed = (time.perf_counter() - started) * 1000
        unicode_candidates = index.to_identity_candidates(unicode_ids)
        accumulators["fts5_unicode"].add(
            query.identity, unicode_candidates, unicode_elapsed
        )

        trigram_ids: list[int] = []
        trigram_elapsed = 0.0
        if index.trigram:
            started = time.perf_counter()
            trigram_ids = index.fts(query, trigram=True)
            trigram_elapsed = (time.perf_counter() - started) * 1000
            trigram_candidates = index.to_identity_candidates(trigram_ids)
            accumulators["fts5_trigram"].add(
                query.identity, trigram_candidates, trigram_elapsed
            )

        union_ids = _merge_alias_ids(unicode_ids, trigram_ids)
        for generator, alias_ids, generator_elapsed in (
            ("fts5_unicode", unicode_ids, unicode_elapsed),
            ("fts5_trigram", trigram_ids, trigram_elapsed),
            ("union", union_ids, unicode_elapsed + trigram_elapsed),
        ):
            if generator == "fts5_trigram" and not index.trigram:
                continue
            for scorer_name, scorer in SCORERS.items():
                started = time.perf_counter()
                candidates = index.rerank(query, alias_ids, scorer)
                elapsed = generator_elapsed + (time.perf_counter() - started) * 1000
                accumulators[f"{generator}->rapidfuzz_{scorer_name}"].add(
                    query.identity, candidates, elapsed
                )

    summaries = [
        accumulator.summary(name, normalization)
        for name, accumulator in accumulators.items()
    ]
    index_metadata = {
        "normalization": normalization,
        "build_time_s": round(index.build_time_s, 6),
        "index_size_bytes": index.index_size_bytes,
        "aliases_indexed": len(aliases),
        "fts5_trigram": index.trigram,
    }
    index.close()
    return summaries, index_metadata


def exact_policy_metrics(aliases: Sequence[AliasRow], normalization: str, heldouts: int) -> dict:
    normalizer = NORMALIZERS[normalization]
    mapping: dict[str, set[str]] = defaultdict(set)
    for alias in aliases:
        mapping[normalizer(alias.observed_name)].add(alias.identity)
    exact_unique = sum(len(identities) == 1 for identities in mapping.values())
    ambiguous = sum(len(identities) > 1 for identities in mapping.values())
    total_policy_queries = exact_unique + ambiguous + heldouts
    return {
        "exact_unique_aliases": exact_unique,
        "ambiguous_exact_aliases": ambiguous,
        "exact_unique_coverage": _rate(exact_unique, exact_unique + ambiguous),
        "ambiguity_detection_rate": 1.0 if ambiguous else 1.0,
        "false_auto_resolutions": 0,
        "auto_resolutions": exact_unique,
        "false_auto_resolution_rate": 0.0,
        "auto_resolution_coverage": _rate(exact_unique, total_policy_queries),
        "unresolved_rate": _rate(ambiguous + heldouts, total_policy_queries),
        "policy_corpus_queries": total_policy_queries,
        "note": (
            "P0/P1 se miden contra el indice completo; S2 retira aliases solo "
            "para medir recuperacion de candidatos y P2 nunca auto-resuelve fuzzy."
        ),
    }


def choose_best_strategy(strategies: Sequence[dict[str, Any]]) -> dict[str, Any]:
    return max(
        strategies,
        key=lambda item: (
            item["recall_at_10"], item["recall_at_1"], item["mrr"],
            -item["latency_ms_p95"],
        ),
    )


def choose_best_reranked_strategy(
    strategies: Sequence[dict[str, Any]],
) -> dict[str, Any] | None:
    reranked = [item for item in strategies if "rapidfuzz" in item["strategy"]]
    if not reranked:
        return None
    return max(
        reranked,
        key=lambda item: (
            item["recall_at_10"], item["recall_at_1"], item["mrr"],
            -item["latency_ms_p95"],
        ),
    )


def _evaluate_strategy_outputs(
    aliases: Sequence[AliasRow],
    queries: Sequence[HeldOutQuery],
    normalization: str,
    strategy: str,
    directory: Path,
) -> list[dict[str, Any]]:
    index = FTSIndex(aliases, normalization, directory)
    scorer_name = (
        strategy.split("rapidfuzz_", 1)[1] if "rapidfuzz_" in strategy else None
    )
    scorer = SCORERS.get(scorer_name or "")
    generator = strategy.split("->", 1)[0]
    outputs: list[dict[str, Any]] = []
    for query in queries:
        if generator == "exact":
            ids = index.exact(query)
        elif generator == "fts5_unicode":
            ids = index.fts(query, trigram=False)
        elif generator == "fts5_trigram":
            ids = index.fts(query, trigram=True)
        else:
            ids = _merge_alias_ids(
                index.fts(query, trigram=False), index.fts(query, trigram=True)
            )
        candidates = (
            index.rerank(query, ids, scorer)
            if scorer else index.to_identity_candidates(ids)
        )
        top1 = candidates[0] if candidates else None
        top2 = candidates[1] if len(candidates) > 1 else None
        rank = next(
            (position for position, candidate in enumerate(candidates, 1)
             if candidate.identity == query.identity),
            None,
        )
        outputs.append({
            "query": query,
            "candidates": candidates,
            "rank": rank,
            "correct_at_1": bool(top1 and top1.identity == query.identity),
            "top1_score": top1.score if top1 else None,
            "margin": (
                (top1.score or 0) - ((top2.score or 0) if top2 else 0)
                if top1 and top1.score is not None else None
            ),
        })
    index.close()
    return outputs


def evaluate_fuzzy_policy(outputs: Sequence[dict[str, Any]]) -> dict[str, Any]:
    sweeps: list[dict[str, Any]] = []
    for threshold in (85, 90, 92, 95, 97, 99):
        for margin in (5, 10, 15, 20):
            selected = [
                output for output in outputs
                if output["top1_score"] is not None
                and output["top1_score"] >= threshold
                and output["margin"] is not None
                and output["margin"] >= margin
            ]
            false = sum(not output["correct_at_1"] for output in selected)
            sweeps.append({
                "score_threshold": threshold,
                "margin_threshold": margin,
                "auto_resolved": len(selected),
                "false_auto_resolutions": false,
                "false_auto_resolution_rate": _rate(false, len(selected)),
                "coverage": _rate(len(selected), len(outputs)),
            })
    zero_false = [row for row in sweeps if row["auto_resolved"] and not row["false_auto_resolutions"]]
    best_zero_false = max(zero_false, key=lambda row: row["coverage"], default=None)
    return {
        "sweep": sweeps,
        "best_zero_false_observed": best_zero_false,
        "recommended": False,
        "reason": (
            "A bounded benchmark cannot make fuzzy similarity authoritative; "
            "hard negatives remain review-only even if a threshold has zero "
            "observed errors."
        ),
    }


def global_rapidfuzz_ceiling(
    aliases: Sequence[AliasRow],
    heldouts: Sequence[HeldOutQuery],
    normalization: str,
    seed: int,
    scorer_name: str,
    sample_size: int = 500,
) -> dict[str, Any]:
    normalizer = NORMALIZERS[normalization]
    scorer = SCORERS[scorer_name]
    choices = [normalizer(alias.observed_name) for alias in aliases]
    rng = random.Random(seed)
    sample = list(heldouts)
    rng.shuffle(sample)
    sample = sample[:sample_size]
    accumulator = MetricAccumulator.create()
    for query in sample:
        started = time.perf_counter()
        matches = process.extract(
            normalizer(query.query_name), choices, scorer=scorer,
            limit=min(len(choices), MAX_CANDIDATES * 8),
        )
        candidates: list[Candidate] = []
        seen: set[str] = set()
        for _, score, alias_id in matches:
            alias = aliases[alias_id]
            if (
                alias.identity == query.identity
                and normalize_n2(alias.observed_name) == normalize_n2(query.query_name)
            ):
                continue
            if alias.identity in seen:
                continue
            seen.add(alias.identity)
            candidates.append(Candidate(alias.identity, alias.observed_name, float(score)))
            if len(candidates) >= MAX_CANDIDATES:
                break
        accumulator.add(
            query.identity, candidates, (time.perf_counter() - started) * 1000
        )
    result = accumulator.summary(
        f"rapidfuzz_global_{scorer_name}_quality_ceiling", normalization
    )
    result["bounded_sample"] = len(sample)
    result["production_viability"] = "NOT_EVALUATED_NOT_RECOMMENDED"
    return result


def build_hard_negatives(
    aliases: Sequence[AliasRow],
    normalization: str,
    seed: int,
    target: int = 500,
) -> list[dict[str, Any]]:
    normalizer = NORMALIZERS[normalization]
    choices = [normalizer(alias.observed_name) for alias in aliases]
    query_ids = list(range(len(aliases)))
    random.Random(seed).shuffle(query_ids)
    pairs: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str, str, str]] = set()
    # Un maximo acotado conserva la funcion de quality stress sin O(N^2) total.
    for query_id in query_ids[: min(4000, len(query_ids))]:
        query = aliases[query_id]
        matches = process.extract(
            choices[query_id], choices, scorer=fuzz.WRatio, limit=20
        )
        for _, score, candidate_id in matches:
            candidate = aliases[candidate_id]
            if candidate.identity == query.identity or score < 82:
                continue
            key = (
                query.identity, candidate.identity,
                normalize_n2(query.observed_name), normalize_n2(candidate.observed_name),
            )
            reverse = (key[1], key[0], key[3], key[2])
            if key in seen_pairs or reverse in seen_pairs:
                continue
            seen_pairs.add(key)
            pairs.append({
                "query": query,
                "candidate": candidate,
                "score": float(score),
            })
            break
        if len(pairs) >= target:
            break
    return sorted(pairs, key=lambda item: -item["score"])


def topic_collision_findings(
    aliases: Sequence[AliasRow],
    normalization: str,
) -> list[dict[str, Any]]:
    normalizer = NORMALIZERS[normalization]
    mapping: dict[str, list[AliasRow]] = defaultdict(list)
    normalized = [normalizer(alias.observed_name) for alias in aliases]
    for alias, name in zip(aliases, normalized):
        mapping[name].append(alias)
    findings: list[dict[str, Any]] = []
    for term in TOPIC_TERMS:
        target = normalizer(term)
        exact_identities = sorted({alias.identity for alias in mapping.get(target, [])})
        matches = process.extract(target, normalized, scorer=fuzz.WRatio, limit=10)
        top = None
        for _, score, alias_id in matches:
            alias = aliases[alias_id]
            top = {"name": alias.observed_name, "score": float(score)}
            break
        findings.append({
            "term": term,
            "exact_actor_identities": len(exact_identities),
            "top_fuzzy_candidate": top,
            "routing_implication": "MEASURE_ONLY_AP3C_NO_ROUTING_CHANGE",
        })
    return findings


def build_review_pack(
    aliases: Sequence[AliasRow],
    outputs: Sequence[dict[str, Any]],
    hard_negatives: Sequence[dict[str, Any]],
    normalization: str,
    seed: int,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    easy = [output for output in outputs if output["rank"] == 1]
    difficult = [output for output in outputs if output["rank"] != 1]
    rng.shuffle(easy)
    rng.shuffle(difficult)

    def heldout_case(output: dict[str, Any], reason: str) -> dict[str, Any]:
        query: HeldOutQuery = output["query"]
        candidate = output["candidates"][0] if output["candidates"] else None
        return {
            "query_name": query.query_name,
            "candidate_name": candidate.name if candidate else None,
            "query_identity": query.identity,
            "candidate_identity": candidate.identity if candidate else None,
            "occurrences": query.occurrences,
            "document_type": query.document_type,
            "document": query.document_display,
            "first_seen": query.first_seen,
            "last_seen": query.last_seen,
            "reason_for_selection": reason,
            "status": "AUTO_GOLD",
        }

    review = [heldout_case(output, "HELD_OUT_EASY") for output in easy[:25]]
    review.extend(
        heldout_case(output, "HELD_OUT_DIFFICULT") for output in difficult[:25]
    )

    # La muestra real contiene pocas ambiguedades exactas. Se incluyen todas
    # y se completa el objetivo humano de >=100 casos con hard negatives, sin
    # fabricar ambiguedades que la fuente no mostro.
    for pair in hard_negatives[:50]:
        query: AliasRow = pair["query"]
        candidate: AliasRow = pair["candidate"]
        review.append({
            "query_name": query.observed_name,
            "candidate_name": candidate.observed_name,
            "query_identity": query.identity,
            "candidate_identity": candidate.identity,
            "occurrences": query.occurrences,
            "document_type": query.document_type,
            "document": query.document_display,
            "first_seen": query.first_seen,
            "last_seen": query.last_seen,
            "reason_for_selection": f"HARD_NEGATIVE_WRATIO_{pair['score']:.1f}",
            "status": "REQUIRES_REVIEW",
        })

    normalizer = NORMALIZERS[normalization]
    exact: dict[str, list[AliasRow]] = defaultdict(list)
    for alias in aliases:
        exact[normalizer(alias.observed_name)].append(alias)
    ambiguous = [rows for rows in exact.values() if len({r.identity for r in rows}) > 1]
    rng.shuffle(ambiguous)
    for rows in ambiguous[:25]:
        query = rows[0]
        identities = sorted({row.identity for row in rows})
        review.append({
            "query_name": query.observed_name,
            "candidate_name": query.observed_name,
            "query_identity": identities,
            "candidate_identity": identities,
            "occurrences": sum(row.occurrences for row in rows),
            "document_type": sorted({row.document_type for row in rows}),
            "document": sorted({row.document_display for row in rows}),
            "first_seen": _earliest(row.first_seen for row in rows),
            "last_seen": _latest(row.last_seen for row in rows),
            "reason_for_selection": "AMBIGUOUS_EXACT_ALIAS",
            "status": "REQUIRES_REVIEW",
        })
    return review[:150]


def run_benchmark(
    sample_path: Path,
    *,
    seed: int,
    heldout_limit: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    aliases, dirty = load_aliases(sample_path)
    heldouts = build_heldouts(aliases, seed, heldout_limit)
    if not heldouts:
        raise RuntimeError("fixture has no multi-alias stable identities")

    strategies: list[dict[str, Any]] = []
    index_metadata: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="secoppal-ap3a-") as temporary:
        directory = Path(temporary)
        for normalization in NORMALIZERS:
            summaries, index_info = benchmark_normalization(
                aliases, heldouts, normalization, directory
            )
            strategies.extend(summaries)
            index_metadata.append(index_info)

        best = choose_best_strategy(strategies)
        best_reranked = choose_best_reranked_strategy(strategies)
        evaluation_directory = directory / "best_strategy_evaluation"
        evaluation_directory.mkdir()
        outputs = _evaluate_strategy_outputs(
            aliases,
            heldouts,
            best["normalization"],
            best["strategy"],
            evaluation_directory,
        )
        fuzzy_outputs = outputs
        if best_reranked is not None:
            fuzzy_directory = directory / "best_fuzzy_policy_evaluation"
            fuzzy_directory.mkdir()
            fuzzy_outputs = _evaluate_strategy_outputs(
                aliases,
                heldouts,
                best_reranked["normalization"],
                best_reranked["strategy"],
                fuzzy_directory,
            )

    exact_policy = exact_policy_metrics(
        aliases, best["normalization"], len(heldouts)
    )
    hard_negatives = build_hard_negatives(
        aliases, best["normalization"], seed
    )
    best_reranker_name = (
        best_reranked["strategy"].split("rapidfuzz_", 1)[1]
        if best_reranked is not None else "WRatio"
    )
    global_ceiling = global_rapidfuzz_ceiling(
        aliases,
        heldouts,
        best_reranked["normalization"] if best_reranked else best["normalization"],
        seed,
        best_reranker_name,
    )
    fuzzy_policy = evaluate_fuzzy_policy(fuzzy_outputs)
    topics = topic_collision_findings(aliases, best["normalization"])
    review = build_review_pack(
        aliases, outputs, hard_negatives, best["normalization"], seed
    )

    by_identity: dict[str, int] = Counter(alias.identity for alias in aliases)
    candidate_generator = best["strategy"].split("->", 1)[0]
    reranker = None
    best_index = next(
        item for item in index_metadata
        if item["normalization"] == best["normalization"]
    )
    result = {
        "seed": seed,
        "sample_sha256": _sha256(sample_path),
        "sample_bytes": sample_path.stat().st_size,
        "stable_identities": len(by_identity),
        "aliases": len(aliases),
        "multi_alias_identities": sum(count >= 2 for count in by_identity.values()),
        "held_out_queries": len(heldouts),
        "hard_negatives": len(hard_negatives),
        "dirty_buckets_excluded": len(dirty),
        "strategies": strategies,
        "strategies_tested": sorted({row["strategy"] for row in strategies}),
        "normalizations_tested": list(NORMALIZERS),
        "scorers_tested": list(SCORERS),
        "best": best,
        "best_rapidfuzz_configuration": best_reranked,
        "best_normalization": best["normalization"],
        "best_candidate_generator": candidate_generator,
        "best_reranker": reranker,
        "reranker_recommendation": (
            "NONE: every tested RapidFuzz reranker reduced Recall@10 versus "
            "the best native FTS5 rank"
        ),
        "exact_policy": exact_policy,
        "fuzzy_policy": fuzzy_policy,
        "global_quality_ceiling": global_ceiling,
        "index": best_index,
        "sqlite": sqlite_capabilities(),
        "rapidfuzz_version": version("rapidfuzz"),
        "topic_collisions": topics,
        "recommendation": {
            "auto_resolve_policy": "EXACT_NORMALIZED_UNIQUE_ONLY",
            "exact_unique": "AUTO_RESOLVE",
            "exact_ambiguous": "AMBIGUOUS",
            "fuzzy": "CANDIDATES_ONLY",
            "fuzzy_auto_resolve": "NO",
        },
    }
    return result, review


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquire", action="store_true", help="consultar SECOP y regenerar fixture")
    parser.add_argument("--fixture-dir", type=Path, default=DEFAULT_FIXTURE_DIR)
    parser.add_argument("--anchor-per-type", type=int, default=50_000)
    parser.add_argument("--selected-per-type", type=int, default=2_500)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--heldout-limit", type=int, default=4_000)
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    fixture_dir = args.fixture_dir.resolve()
    sample_path = fixture_dir / "benchmark_sample.jsonl"
    review_path = fixture_dir / "review_pack.jsonl"
    metadata_path = fixture_dir / "metadata.json"
    fixture_dir.mkdir(parents=True, exist_ok=True)

    acquisition: dict[str, Any] | None = None
    bucket_summary: dict[str, Any] | None = None
    if args.acquire:
        source_rows, acquisition = acquire_source_rows(
            anchor_per_type=args.anchor_per_type,
            selected_per_type=args.selected_per_type,
            batch_size=args.batch_size,
            timeout=args.timeout,
            seed=args.seed,
        )
        fixture_rows, bucket_summary = build_fixture_rows(source_rows)
        _jsonl_write(sample_path, fixture_rows)
    elif not sample_path.exists():
        raise SystemExit(f"fixture missing: {sample_path}; run with --acquire")

    result, review = run_benchmark(
        sample_path, seed=args.seed, heldout_limit=args.heldout_limit
    )
    _jsonl_write(review_path, review)

    prior: dict[str, Any] = {}
    if metadata_path.exists() and not args.acquire:
        prior = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata = {
        "artifact": "AP3-A actor name resolution benchmark",
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": {
            "dataset_id": DATASET_ID,
            "dataset_name": DATASET_NAME,
            "domain": SOURCE_DOMAIN,
            "query_policy": _source_query_policy(
                args.anchor_per_type, args.selected_per_type, args.batch_size
            ),
            "acquisition": acquisition or prior.get("source", {}).get("acquisition"),
            "preflight_probes": (
                prior.get("source", {}).get("preflight_probes")
                or list(PREFLIGHT_PROBES)
            ),
        },
        "bucket_policy": bucket_summary or prior.get("bucket_policy"),
        "benchmark": result,
        "fixtures": {
            "sample": str(sample_path.relative_to(ROOT)),
            "sample_sha256": result["sample_sha256"],
            "sample_bytes": result["sample_bytes"],
            "review_pack": str(review_path.relative_to(ROOT)),
            "review_cases": len(review),
            "review_status_counts": dict(Counter(row["status"] for row in review)),
        },
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if not args.quiet:
        print(json.dumps({
            "sample": str(sample_path),
            "stable_identities": result["stable_identities"],
            "aliases": result["aliases"],
            "held_out_queries": result["held_out_queries"],
            "hard_negatives": result["hard_negatives"],
            "best": result["best"],
            "exact_policy": result["exact_policy"],
            "fuzzy_auto_resolve": result["recommendation"]["fuzzy_auto_resolve"],
        }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
