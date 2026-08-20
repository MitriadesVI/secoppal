"""Índice SQLite local y reproducible de aliases de actores — AP3-B.

El builder acepta filas ya agregadas por SECOP, aplica la política conservadora
de AP3-A y construye un artefacto nuevo antes de reemplazar atómicamente el
vigente. Los buckets sucios quedan solo en la tabla de auditoría: nunca entran
en ``actor_aliases`` ni en FTS, por lo que no pueden convertirse por accidente
en evidencia de auto-resolución exacta.

Clasificaciones de build
------------------------
``INDEXED_STABLE``
    Alias de un bucket estable cuya forma N2 pertenece a una sola IdentityKey.
``INDEXED_AMBIGUOUS``
    Alias de bucket estable cuya forma N2 pertenece a varias IdentityKeys. Se
    indexa para detectar ambigüedad, jamás para elegir una por frecuencia.
``EXCLUDED_DIRTY``
    Registro desconocido/inutilizable, bucket contaminado o alias secundario de
    agrupación. Se conserva como auditoría local y no es consultable por resolver.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from app.core.actor_identity import normalize_identity
from app.core.actor_name import (
    ACTOR_NAME_NORMALIZATION_VERSION,
    normalize_actor_name,
)

__all__ = [
    "ACTOR_ALIAS_INDEX_SCHEMA_VERSION",
    "ACTOR_ALIAS_INDEX_TOKENIZER",
    "ActorAliasBuildPolicy",
    "ActorAliasIndexMetadata",
    "ActorAliasIndexReport",
    "BuildDisposition",
    "build_actor_alias_index",
    "validate_actor_alias_index",
]


ACTOR_ALIAS_INDEX_SCHEMA_VERSION = "1"
ACTOR_ALIAS_INDEX_TOKENIZER = "trigram"
ACTOR_ALIAS_INDEX_APPLICATION_ID = 0x53415033  # "SAP3"

_SOURCE_TYPE_TO_IDENTITY_TYPE = {
    "NIT": "NIT",
    "Cédula de Ciudadanía": "CC",
    # Útil para fuentes agregadas/fixtures que ya tradujeron el vocabulario.
    "CC": "CC",
}
_NAME_SENTINELS = frozenset({
    "", "no definido", "no definida", "n a", "na", "null", "none",
    "sin dato", "sin informacion",
})
_GROUP_MARKERS = re.compile(r"\b(?:union temporal|consorcio|ut)\b")


class BuildDisposition:
    INDEXED_STABLE = "INDEXED_STABLE"
    INDEXED_AMBIGUOUS = "INDEXED_AMBIGUOUS"
    EXCLUDED_DIRTY = "EXCLUDED_DIRTY"


@dataclass(frozen=True, slots=True)
class ActorAliasBuildPolicy:
    """Umbrales de AP3-A que separan evidencia estable de buckets sucios."""

    # AP3-E: la baja frecuencia NO es suciedad. Con umbral 3 quedaban fuera el
    # 43,9% de los proveedores del país —casi todos legítimos, con uno o dos
    # contratos— mientras la cobertura de ACTIVIDAD seguía en 89,8%, cifra que
    # camuflaba el sesgo hacia proveedores grandes. Que un actor sea infrecuente
    # no dice nada sobre la calidad de su evidencia; las reglas que sí la miden
    # (colisión de tipo, colapso DV sin puente, cuota del nombre dominante,
    # cardinalidad extrema, alias de grupo secundarios) siguen intactas y una
    # sola observación con cualquiera de esos problemas sigue siendo dirty.
    minimum_total_occurrences: int = 1
    minimum_dominant_name_share: float = 0.50
    maximum_distinct_observed_names: int = 25
    exclude_cross_document_type_collisions: bool = True
    exclude_secondary_group_aliases: bool = True
    exclude_unbridged_document_form_collapses: bool = True

    def __post_init__(self) -> None:
        if self.minimum_total_occurrences < 1:
            raise ValueError("minimum_total_occurrences must be positive")
        if not 0 < self.minimum_dominant_name_share <= 1:
            raise ValueError("minimum_dominant_name_share must be in (0, 1]")
        if self.maximum_distinct_observed_names < 1:
            raise ValueError("maximum_distinct_observed_names must be positive")
        if not self.exclude_cross_document_type_collisions:
            raise ValueError("cross-document-type collision exclusion is mandatory")
        if not self.exclude_secondary_group_aliases:
            raise ValueError("secondary group alias exclusion is mandatory")
        if not self.exclude_unbridged_document_form_collapses:
            raise ValueError("unbridged document-form collapse exclusion is mandatory")


@dataclass(frozen=True, slots=True)
class ActorAliasIndexMetadata:
    schema_version: str
    normalization_version: str
    built_at: str
    source_dataset: str
    source_max_date: str | None
    source_row_count: int
    identity_count: int
    alias_count: int
    ambiguous_alias_count: int
    excluded_dirty_count: int
    build_parameters: dict[str, Any]
    source_quality: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ActorAliasIndexReport:
    target: Path
    metadata: ActorAliasIndexMetadata
    disposition_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "target": str(self.target),
            "metadata": asdict(self.metadata),
            "disposition_counts": dict(self.disposition_counts),
        }


@dataclass(slots=True)
class _RawAlias:
    document_type: str
    base_number: str
    raw_document: str
    raw_alias: str
    normalized_alias: str
    occurrences: int
    first_seen: str | None
    last_seen: str | None
    source: str


@dataclass(frozen=True, slots=True)
class _AuditRecord:
    document_type: str
    base_number: str | None
    raw_document: str
    raw_alias: str
    normalized_alias: str
    occurrences: int
    first_seen: str | None
    last_seen: str | None
    source: str
    disposition: str
    reason_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _IndexedAlias:
    normalized_alias: str
    raw_alias: str
    raw_aliases: tuple[str, ...]
    document_type: str
    base_number: str
    occurrences: int
    first_seen: str | None
    last_seen: str | None
    source: str
    disposition: str


def _date_only(value: Any) -> str | None:
    return str(value)[:10] if value else None


def _minimum(values: Iterable[str | None]) -> str | None:
    present = [value for value in values if value]
    return min(present) if present else None


def _maximum(values: Iterable[str | None]) -> str | None:
    present = [value for value in values if value]
    return max(present) if present else None


def _occurrences(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _built_at(value: datetime | str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("built_at datetime must be timezone-aware")
        return value.isoformat(timespec="seconds")
    if not value.strip():
        raise ValueError("built_at string cannot be empty")
    return value


def _source_value(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def _rejected_record(
    row: Mapping[str, Any],
    *,
    source: str,
    reason: str,
    document_type: str = "UNKNOWN",
    base_number: str | None = None,
) -> _AuditRecord:
    raw_document = str(
        _source_value(row, "documento_proveedor", "raw_document", "document") or ""
    )
    raw_alias = str(
        _source_value(row, "proveedor_adjudicado", "observed_name", "raw_alias") or ""
    ).strip()
    return _AuditRecord(
        document_type=document_type,
        base_number=base_number,
        raw_document=raw_document,
        raw_alias=raw_alias,
        normalized_alias=normalize_actor_name(raw_alias),
        occurrences=max(0, _occurrences(row.get("occurrences"))),
        first_seen=_date_only(row.get("first_seen")),
        last_seen=_date_only(row.get("last_seen")),
        source=source,
        disposition=BuildDisposition.EXCLUDED_DIRTY,
        reason_codes=(reason,),
    )


def _prepare_rows(
    source_rows: Sequence[Mapping[str, Any]],
    *,
    source_dataset: str,
) -> tuple[list[_RawAlias], list[_AuditRecord], Counter[str]]:
    merged: dict[tuple[str, str, str, str], _RawAlias] = {}
    rejected: list[_AuditRecord] = []
    quality: Counter[str] = Counter()

    for row in source_rows:
        source = str(row.get("source") or source_dataset)
        source_type = str(
            _source_value(row, "tipodocproveedor", "document_type") or ""
        ).strip()
        document_type = _SOURCE_TYPE_TO_IDENTITY_TYPE.get(source_type)
        if document_type is None:
            quality["unknown_document_type"] += 1
            rejected.append(_rejected_record(
                row, source=source, reason="UNKNOWN_DOCUMENT_TYPE",
            ))
            continue

        raw_document_value = _source_value(
            row, "documento_proveedor", "raw_document", "document"
        )
        match = normalize_identity(raw_document_value, document_type=document_type)
        if match.key is None or match.key.document_type not in {"NIT", "CC"}:
            quality["unusable_document"] += 1
            rejected.append(_rejected_record(
                row, source=source, reason="UNUSABLE_DOCUMENT",
                document_type=document_type,
            ))
            continue

        raw_alias = str(
            _source_value(row, "proveedor_adjudicado", "observed_name", "raw_alias")
            or ""
        ).strip()
        normalized_alias = normalize_actor_name(raw_alias)
        if normalized_alias in _NAME_SENTINELS:
            quality["empty_or_sentinel_name"] += 1
            rejected.append(_rejected_record(
                row, source=source, reason="EMPTY_OR_SENTINEL_NAME",
                document_type=document_type, base_number=match.key.base_number,
            ))
            continue

        occurrences = _occurrences(row.get("occurrences"))
        if occurrences <= 0:
            quality["non_positive_occurrences"] += 1
            rejected.append(_rejected_record(
                row, source=source, reason="NON_POSITIVE_OCCURRENCES",
                document_type=document_type, base_number=match.key.base_number,
            ))
            continue

        raw_document = str(raw_document_value or "")
        # Keep source document forms separate through bucket classification.
        # AP1 can legitimately collapse base/base+DV to one IdentityKey, but
        # AP3-B.1 still needs to audit whether those forms share any name
        # evidence before granting exact-alias authority.
        key = (document_type, match.key.base_number, raw_document, raw_alias)
        current = merged.get(key)
        if current is None:
            merged[key] = _RawAlias(
                document_type=document_type,
                base_number=match.key.base_number,
                raw_document=raw_document,
                raw_alias=raw_alias,
                normalized_alias=normalized_alias,
                occurrences=occurrences,
                first_seen=_date_only(row.get("first_seen")),
                last_seen=_date_only(row.get("last_seen")),
                source=source,
            )
        else:
            current.occurrences += occurrences
            current.first_seen = _minimum((current.first_seen, _date_only(row.get("first_seen"))))
            current.last_seen = _maximum((current.last_seen, _date_only(row.get("last_seen"))))
            current.raw_document = min(current.raw_document, raw_document)
            current.source = min(current.source, source)

    quality["accepted_aggregated_rows"] = len(merged)
    quality["rejected_aggregated_rows"] = len(rejected)
    return list(merged.values()), rejected, quality


def _classify_rows(
    source_rows: Sequence[Mapping[str, Any]],
    *,
    source_dataset: str,
    policy: ActorAliasBuildPolicy,
) -> tuple[list[_IndexedAlias], list[_AuditRecord], dict[str, Any]]:
    prepared, audit, quality_counts = _prepare_rows(
        source_rows, source_dataset=source_dataset,
    )
    buckets: dict[tuple[str, str], list[_RawAlias]] = defaultdict(list)
    types_by_base: dict[str, set[str]] = defaultdict(set)
    for alias in prepared:
        buckets[(alias.document_type, alias.base_number)].append(alias)
        types_by_base[alias.base_number].add(alias.document_type)

    stable_rows: list[_RawAlias] = []
    dirty_buckets = 0
    unbridged_document_form_collapses = 0
    excluded_group_aliases = 0
    dirty_reasons: Counter[str] = Counter()
    for (document_type, base_number), aliases in sorted(buckets.items()):
        aliases.sort(key=lambda item: (-item.occurrences, item.raw_alias.casefold()))
        total_occurrences = sum(item.occurrences for item in aliases)
        dominant = aliases[0]
        dominant_share = dominant.occurrences / total_occurrences
        dominant_is_group = bool(_GROUP_MARKERS.search(dominant.normalized_alias))
        clean_aliases = [
            item for item in aliases
            if (
                not policy.exclude_secondary_group_aliases
                or dominant_is_group
                or not _GROUP_MARKERS.search(item.normalized_alias)
            )
        ]
        secondary_groups = [item for item in aliases if item not in clean_aliases]

        reasons: list[str] = []
        if (
            policy.exclude_cross_document_type_collisions
            and len(types_by_base[base_number]) > 1
        ):
            reasons.append("CROSS_TYPE_COLLISION")
        if total_occurrences < policy.minimum_total_occurrences:
            reasons.append("INSUFFICIENT_OCCURRENCES")
        if dominant_share < policy.minimum_dominant_name_share:
            reasons.append("LOW_DOMINANT_NAME_SHARE")
        if len(aliases) > policy.maximum_distinct_observed_names:
            reasons.append("EXTREME_ALIAS_CARDINALITY")
        if not clean_aliases:
            reasons.append("NO_CLEAN_ALIAS")
        if (
            policy.exclude_unbridged_document_form_collapses
            and document_type == "NIT"
            and _has_unbridged_document_form_collapse(aliases)
        ):
            reasons.append("UNBRIDGED_DOCUMENT_FORM_COLLAPSE")
            unbridged_document_form_collapses += 1

        if reasons:
            dirty_buckets += 1
            dirty_reasons.update(reasons)
            for item in aliases:
                audit.append(_audit_from_raw(
                    item, BuildDisposition.EXCLUDED_DIRTY, tuple(reasons),
                ))
            continue

        stable_rows.extend(clean_aliases)
        for item in secondary_groups:
            excluded_group_aliases += 1
            dirty_reasons["SECONDARY_GROUP_ALIAS"] += 1
            audit.append(_audit_from_raw(
                item,
                BuildDisposition.EXCLUDED_DIRTY,
                ("SECONDARY_GROUP_ALIAS",),
            ))

    by_identity_alias: dict[tuple[str, str, str], list[_RawAlias]] = defaultdict(list)
    identities_by_alias: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in stable_rows:
        key = (row.document_type, row.base_number, row.normalized_alias)
        by_identity_alias[key].append(row)
        identities_by_alias[row.normalized_alias].add((row.document_type, row.base_number))

    indexed: list[_IndexedAlias] = []
    for (document_type, base_number, normalized_alias), rows in sorted(
        by_identity_alias.items(), key=lambda item: item[0]
    ):
        rows.sort(key=lambda item: (-item.occurrences, item.raw_alias.casefold()))
        disposition = (
            BuildDisposition.INDEXED_AMBIGUOUS
            if len(identities_by_alias[normalized_alias]) > 1
            else BuildDisposition.INDEXED_STABLE
        )
        representative = rows[0]
        raw_aliases = tuple(dict.fromkeys(item.raw_alias for item in rows))
        indexed.append(_IndexedAlias(
            normalized_alias=normalized_alias,
            raw_alias=representative.raw_alias,
            raw_aliases=raw_aliases,
            document_type=document_type,
            base_number=base_number,
            occurrences=sum(item.occurrences for item in rows),
            first_seen=_minimum(item.first_seen for item in rows),
            last_seen=_maximum(item.last_seen for item in rows),
            source=representative.source,
            disposition=disposition,
        ))
        for row in rows:
            audit.append(_audit_from_raw(row, disposition, ()))

    disposition_counts = Counter(record.disposition for record in audit)
    source_quality = {
        **dict(sorted(quality_counts.items())),
        "dirty_buckets_excluded": dirty_buckets,
        "dirty_reason_counts": dict(sorted(dirty_reasons.items())),
        "secondary_group_aliases_excluded": excluded_group_aliases,
        "unbridged_document_form_collapse_buckets_excluded": (
            unbridged_document_form_collapses
        ),
        "known_document_types_only": ["NIT", "CC"],
        "disposition_counts": dict(sorted(disposition_counts.items())),
    }
    return indexed, audit, source_quality


def _has_unbridged_document_form_collapse(aliases: Sequence[_RawAlias]) -> bool:
    """Detect distinct bare NIT forms collapsed by DV inference without a name bridge.

    ``base`` and ``base+valid_DV`` are normally two publications of one NIT.
    They are accepted when at least one N2 alias appears under both forms.  If
    the forms carry disjoint name sets, the local index cannot distinguish a
    legitimate representation variant from two source identifiers collapsed
    by arithmetic inference, so the entire bucket is excluded from authority.
    """
    aliases_by_form: dict[str, set[str]] = defaultdict(set)
    for alias in aliases:
        digits = re.sub(r"\D", "", alias.raw_document)
        if digits:
            aliases_by_form[digits].add(alias.normalized_alias)
    if len(aliases_by_form) <= 1:
        return False
    forms = list(aliases_by_form.values())
    return not any(
        left & right
        for index, left in enumerate(forms)
        for right in forms[index + 1:]
    )


def _audit_from_raw(
    row: _RawAlias,
    disposition: str,
    reasons: tuple[str, ...],
) -> _AuditRecord:
    return _AuditRecord(
        document_type=row.document_type,
        base_number=row.base_number,
        raw_document=row.raw_document,
        raw_alias=row.raw_alias,
        normalized_alias=row.normalized_alias,
        occurrences=row.occurrences,
        first_seen=row.first_seen,
        last_seen=row.last_seen,
        source=row.source,
        disposition=disposition,
        reason_codes=reasons,
    )


_SCHEMA_SQL = f"""
PRAGMA application_id = {ACTOR_ALIAS_INDEX_APPLICATION_ID};
PRAGMA user_version = 1;

CREATE TABLE actor_aliases (
    alias_id INTEGER PRIMARY KEY,
    normalized_alias TEXT NOT NULL,
    raw_alias TEXT NOT NULL,
    raw_aliases_json TEXT NOT NULL,
    document_type TEXT NOT NULL CHECK (document_type IN ('NIT', 'CC')),
    base_number TEXT NOT NULL,
    occurrences INTEGER NOT NULL CHECK (occurrences > 0),
    first_seen TEXT,
    last_seen TEXT,
    source TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (
        disposition IN ('INDEXED_STABLE', 'INDEXED_AMBIGUOUS')
    ),
    UNIQUE (normalized_alias, document_type, base_number)
);
CREATE INDEX actor_aliases_exact_idx ON actor_aliases(normalized_alias);
CREATE INDEX actor_aliases_identity_idx
    ON actor_aliases(document_type, base_number);

CREATE VIRTUAL TABLE actor_aliases_fts USING fts5(
    normalized_alias,
    content='actor_aliases',
    content_rowid='alias_id',
    tokenize='{ACTOR_ALIAS_INDEX_TOKENIZER}'
);

CREATE TABLE actor_alias_build_audit (
    audit_id INTEGER PRIMARY KEY,
    document_type TEXT NOT NULL,
    base_number TEXT,
    raw_document TEXT NOT NULL,
    raw_alias TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    occurrences INTEGER NOT NULL CHECK (occurrences >= 0),
    first_seen TEXT,
    last_seen TEXT,
    source TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (
        disposition IN ('INDEXED_STABLE', 'INDEXED_AMBIGUOUS', 'EXCLUDED_DIRTY')
    ),
    reason_codes_json TEXT NOT NULL
);
CREATE INDEX actor_alias_build_audit_disposition_idx
    ON actor_alias_build_audit(disposition);

CREATE TABLE actor_index_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version TEXT NOT NULL,
    normalization_version TEXT NOT NULL,
    built_at TEXT NOT NULL,
    source_dataset TEXT NOT NULL,
    source_max_date TEXT,
    source_row_count INTEGER NOT NULL CHECK (source_row_count >= 0),
    identity_count INTEGER NOT NULL CHECK (identity_count >= 0),
    alias_count INTEGER NOT NULL CHECK (alias_count >= 0),
    ambiguous_alias_count INTEGER NOT NULL CHECK (ambiguous_alias_count >= 0),
    excluded_dirty_count INTEGER NOT NULL CHECK (excluded_dirty_count >= 0),
    build_parameters_json TEXT NOT NULL,
    source_quality_json TEXT NOT NULL
);
"""


def _write_index(
    path: Path,
    *,
    aliases: Sequence[_IndexedAlias],
    audit: Sequence[_AuditRecord],
    metadata: ActorAliasIndexMetadata,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(_SCHEMA_SQL)
        connection.executemany(
            "INSERT INTO actor_aliases("
            "alias_id,normalized_alias,raw_alias,raw_aliases_json,document_type,"
            "base_number,occurrences,first_seen,last_seen,source,disposition"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                (
                    alias_id,
                    row.normalized_alias,
                    row.raw_alias,
                    json.dumps(row.raw_aliases, ensure_ascii=False),
                    row.document_type,
                    row.base_number,
                    row.occurrences,
                    row.first_seen,
                    row.last_seen,
                    row.source,
                    row.disposition,
                )
                for alias_id, row in enumerate(aliases, 1)
            ),
        )
        connection.execute(
            "INSERT INTO actor_aliases_fts(actor_aliases_fts) VALUES ('rebuild')"
        )
        connection.executemany(
            "INSERT INTO actor_alias_build_audit("
            "document_type,base_number,raw_document,raw_alias,normalized_alias,"
            "occurrences,first_seen,last_seen,source,disposition,reason_codes_json"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                (
                    row.document_type,
                    row.base_number,
                    row.raw_document,
                    row.raw_alias,
                    row.normalized_alias,
                    row.occurrences,
                    row.first_seen,
                    row.last_seen,
                    row.source,
                    row.disposition,
                    json.dumps(row.reason_codes),
                )
                for row in audit
            ),
        )
        connection.execute(
            "INSERT INTO actor_index_metadata VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                metadata.schema_version,
                metadata.normalization_version,
                metadata.built_at,
                metadata.source_dataset,
                metadata.source_max_date,
                metadata.source_row_count,
                metadata.identity_count,
                metadata.alias_count,
                metadata.ambiguous_alias_count,
                metadata.excluded_dirty_count,
                json.dumps(metadata.build_parameters, ensure_ascii=False, sort_keys=True),
                json.dumps(metadata.source_quality, ensure_ascii=False, sort_keys=True),
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _read_metadata(connection: sqlite3.Connection) -> ActorAliasIndexMetadata:
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        "SELECT * FROM actor_index_metadata WHERE singleton = 1"
    ).fetchone()
    if row is None:
        raise ValueError("actor index metadata is missing")
    return ActorAliasIndexMetadata(
        schema_version=str(row["schema_version"]),
        normalization_version=str(row["normalization_version"]),
        built_at=str(row["built_at"]),
        source_dataset=str(row["source_dataset"]),
        source_max_date=row["source_max_date"],
        source_row_count=int(row["source_row_count"]),
        identity_count=int(row["identity_count"]),
        alias_count=int(row["alias_count"]),
        ambiguous_alias_count=int(row["ambiguous_alias_count"]),
        excluded_dirty_count=int(row["excluded_dirty_count"]),
        build_parameters=json.loads(row["build_parameters_json"]),
        source_quality=json.loads(row["source_quality_json"]),
    )


def _integrity_check(
    path: Path,
    *,
    expected_schema_version: str = ACTOR_ALIAS_INDEX_SCHEMA_VERSION,
    expected_normalization_version: str = ACTOR_NAME_NORMALIZATION_VERSION,
) -> ActorAliasIndexMetadata:
    connection = sqlite3.connect(path)
    try:
        result = connection.execute("PRAGMA integrity_check").fetchone()
        if result is None or result[0] != "ok":
            raise ValueError("actor index failed SQLite integrity_check")
        metadata = _read_metadata(connection)
        if metadata.schema_version != expected_schema_version:
            raise ValueError("actor index schema version is incompatible")
        if metadata.normalization_version != expected_normalization_version:
            raise ValueError("actor index normalization version is incompatible")
        alias_count = int(connection.execute(
            "SELECT count(*) FROM actor_aliases"
        ).fetchone()[0])
        fts_count = int(connection.execute(
            "SELECT count(*) FROM actor_aliases_fts"
        ).fetchone()[0])
        if alias_count != metadata.alias_count or fts_count != alias_count:
            raise ValueError("actor index row counts do not match metadata")
        ambiguous = int(connection.execute(
            "SELECT count(*) FROM ("
            "SELECT normalized_alias FROM actor_aliases "
            "GROUP BY normalized_alias HAVING count(*) > 1)"
        ).fetchone()[0])
        if ambiguous != metadata.ambiguous_alias_count:
            raise ValueError("actor index ambiguity count does not match metadata")
        return metadata
    finally:
        connection.close()


def validate_actor_alias_index(
    path: str | Path,
    *,
    expected_schema_version: str = ACTOR_ALIAS_INDEX_SCHEMA_VERSION,
    expected_normalization_version: str = ACTOR_NAME_NORMALIZATION_VERSION,
) -> ActorAliasIndexMetadata:
    """Valida un índice existente sin crearlo ni modificarlo."""
    target = Path(path)
    if not target.is_file():
        raise FileNotFoundError(target)
    return _integrity_check(
        target,
        expected_schema_version=expected_schema_version,
        expected_normalization_version=expected_normalization_version,
    )


def build_actor_alias_index(
    source_rows: Iterable[Mapping[str, Any]],
    target: str | Path,
    *,
    source_dataset: str,
    built_at: datetime | str | None = None,
    build_parameters: Mapping[str, Any] | None = None,
    policy: ActorAliasBuildPolicy | None = None,
) -> ActorAliasIndexReport:
    """Construye ``target`` vía archivo temporal, checks y ``os.replace``.

    ``source_rows`` debe contener grupos server-side, no contratos crudos: tipo,
    documento, nombre observado, ocurrencias y opcionalmente primera/última fecha.
    Un fallo antes del replace deja intacto el último índice válido.
    """
    target_path = Path(target)
    rows = list(source_rows)
    active_policy = policy or ActorAliasBuildPolicy()
    aliases, audit, source_quality = _classify_rows(
        rows,
        source_dataset=source_dataset,
        policy=active_policy,
    )
    identity_count = len({(row.document_type, row.base_number) for row in aliases})
    ambiguous_alias_count = len({
        row.normalized_alias
        for row in aliases
        if row.disposition == BuildDisposition.INDEXED_AMBIGUOUS
    })
    excluded_dirty_count = sum(
        row.disposition == BuildDisposition.EXCLUDED_DIRTY for row in audit
    )
    source_max_date = _maximum(
        _date_only(row.get("last_seen")) for row in rows
    )
    parameters = {
        "build_policy": asdict(active_policy),
        **dict(build_parameters or {}),
    }
    metadata = ActorAliasIndexMetadata(
        schema_version=ACTOR_ALIAS_INDEX_SCHEMA_VERSION,
        normalization_version=ACTOR_NAME_NORMALIZATION_VERSION,
        built_at=_built_at(built_at),
        source_dataset=source_dataset,
        source_max_date=source_max_date,
        source_row_count=len(rows),
        identity_count=identity_count,
        alias_count=len(aliases),
        ambiguous_alias_count=ambiguous_alias_count,
        excluded_dirty_count=excluded_dirty_count,
        build_parameters=parameters,
        source_quality=source_quality,
    )

    target_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f"{target_path.name}.",
        suffix=".tmp",
        dir=target_path.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        _write_index(temporary, aliases=aliases, audit=audit, metadata=metadata)
        _integrity_check(temporary)
        os.replace(temporary, target_path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    disposition_counts = Counter(row.disposition for row in audit)
    return ActorAliasIndexReport(
        target=target_path,
        metadata=metadata,
        disposition_counts=dict(sorted(disposition_counts.items())),
    )
