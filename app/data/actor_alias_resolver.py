"""Resolver local precision-first sobre el índice SQLite de AP3-B.

Orden de autoridad:

1. alias N2 exacto asociado a una sola IdentityKey -> ``RESOLVED``;
2. alias N2 exacto asociado a varias IdentityKeys -> ``AMBIGUOUS``;
3. sin exacto, FTS5 trigram/BM25 -> ``CANDIDATES`` solamente.

No importa RapidFuzz, clientes HTTP, LLM ni routing. La falta, corrupción o
incompatibilidad del artefacto produce ``INDEX_UNAVAILABLE`` sin fallback.
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from app.core.actor_identity import IdentityKey
from app.core.actor_name import (
    ACTOR_NAME_NORMALIZATION_VERSION,
    ActorResolutionCandidate,
    ActorResolutionResult,
    ActorResolutionStatus,
    normalize_actor_name,
)
from app.data.actor_alias_index import (
    ACTOR_ALIAS_INDEX_APPLICATION_ID,
    ACTOR_ALIAS_INDEX_SCHEMA_VERSION,
)

__all__ = [
    "ACTOR_RESOLUTION_TOP_K",
    "ActorNameResolver",
]


ACTOR_RESOLUTION_TOP_K = 10
_REQUIRED_TABLES = frozenset({
    "actor_aliases",
    "actor_aliases_fts",
    "actor_index_metadata",
})


class _IncompatibleIndexError(ValueError):
    pass


class ActorNameResolver:
    """Consulta un artefacto local en solo lectura; no conserva conexiones."""

    def __init__(self, index_path: str | Path):
        self.index_path = Path(index_path)

    @classmethod
    def from_settings(cls, settings: Any | None = None) -> "ActorNameResolver":
        """Construye el resolver desde la ruta configurable del proyecto."""
        if settings is None:
            from app.config import get_settings

            settings = get_settings()
        return cls(settings.secop_actor_alias_index_path)

    def resolve(self, query: str | None) -> ActorResolutionResult:
        normalized_query = normalize_actor_name(query)
        if not normalized_query:
            return ActorResolutionResult(
                status=ActorResolutionStatus.UNRESOLVED,
                normalized_query=normalized_query,
                method="empty_query",
            )
        if not self.index_path.is_file():
            return self._unavailable(normalized_query, "INDEX_MISSING")

        connection: sqlite3.Connection | None = None
        try:
            connection = self._connect_readonly()
            self._verify_compatibility(connection)
            exact_rows = connection.execute(
                "SELECT normalized_alias,raw_alias,raw_aliases_json,document_type,"
                "base_number,occurrences,first_seen,last_seen "
                "FROM actor_aliases WHERE normalized_alias = ? "
                "ORDER BY occurrences DESC,document_type,base_number,alias_id",
                (normalized_query,),
            ).fetchall()
            exact = self._collapse_exact(exact_rows)
            if len(exact) == 1:
                return ActorResolutionResult(
                    status=ActorResolutionStatus.RESOLVED,
                    normalized_query=normalized_query,
                    identity=exact[0].identity,
                    method="exact_unique_alias",
                )
            if len(exact) > 1:
                return ActorResolutionResult(
                    status=ActorResolutionStatus.AMBIGUOUS,
                    normalized_query=normalized_query,
                    candidates=exact,
                    method="exact_ambiguous_alias",
                )

            expression = self._fts_expression(normalized_query)
            if expression is None:
                return ActorResolutionResult(
                    status=ActorResolutionStatus.UNRESOLVED,
                    normalized_query=normalized_query,
                    method="no_exact_or_trigram_query",
                )
            candidates = self._fuzzy_candidates(connection, expression)
            if not candidates:
                return ActorResolutionResult(
                    status=ActorResolutionStatus.UNRESOLVED,
                    normalized_query=normalized_query,
                    method="no_match",
                )
            return ActorResolutionResult(
                status=ActorResolutionStatus.CANDIDATES,
                normalized_query=normalized_query,
                candidates=candidates,
                method="fts5_trigram_bm25",
            )
        except _IncompatibleIndexError:
            return self._unavailable(normalized_query, "INDEX_INCOMPATIBLE")
        except (OSError, ValueError, sqlite3.DatabaseError):
            return self._unavailable(normalized_query, "INDEX_CORRUPT_OR_UNREADABLE")
        finally:
            if connection is not None:
                connection.close()

    def _connect_readonly(self) -> sqlite3.Connection:
        uri = f"{self.index_path.resolve().as_uri()}?mode=ro&immutable=1"
        connection = sqlite3.connect(uri, uri=True, timeout=1, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection

    @staticmethod
    def _verify_compatibility(connection: sqlite3.Connection) -> None:
        application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
        if application_id != ACTOR_ALIAS_INDEX_APPLICATION_ID:
            raise _IncompatibleIndexError("unexpected application_id")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
            )
        }
        if not _REQUIRED_TABLES <= tables:
            raise _IncompatibleIndexError("required actor index tables are missing")
        metadata = connection.execute(
            "SELECT schema_version,normalization_version "
            "FROM actor_index_metadata WHERE singleton = 1"
        ).fetchone()
        if metadata is None:
            raise _IncompatibleIndexError("actor index metadata is missing")
        if str(metadata["schema_version"]) != ACTOR_ALIAS_INDEX_SCHEMA_VERSION:
            raise _IncompatibleIndexError("schema version mismatch")
        if str(metadata["normalization_version"]) != ACTOR_NAME_NORMALIZATION_VERSION:
            raise _IncompatibleIndexError("normalization version mismatch")

    @classmethod
    def _collapse_exact(
        cls,
        rows: Iterable[sqlite3.Row],
    ) -> tuple[ActorResolutionCandidate, ...]:
        grouped: dict[IdentityKey, list[sqlite3.Row]] = defaultdict(list)
        for row in rows:
            grouped[cls._identity(row)].append(row)
        ordered = sorted(
            grouped.items(),
            key=lambda item: (
                -sum(int(row["occurrences"]) for row in item[1]),
                item[0].document_type,
                item[0].base_number,
            ),
        )
        return tuple(
            cls._candidate(identity, evidence, rank=rank)
            for rank, (identity, evidence) in enumerate(ordered, 1)
        )

    @classmethod
    def _fuzzy_candidates(
        cls,
        connection: sqlite3.Connection,
        expression: str,
    ) -> tuple[ActorResolutionCandidate, ...]:
        # MATERIALIZED fija el score BM25 por alias; ROW_NUMBER conserva el
        # mejor alias de cada IdentityKey antes del LIMIT. Top 10 significa así
        # diez identidades distintas incluso si una tiene muchos aliases.
        rows = connection.execute(
            "WITH matches AS MATERIALIZED ("
            " SELECT a.alias_id,a.normalized_alias,a.raw_alias,a.raw_aliases_json,"
            " a.document_type,a.base_number,a.occurrences,a.first_seen,a.last_seen,"
            " bm25(actor_aliases_fts) AS retrieval_score"
            " FROM actor_aliases_fts"
            " JOIN actor_aliases a ON a.alias_id = actor_aliases_fts.rowid"
            " WHERE actor_aliases_fts MATCH ?"
            "), ranked AS ("
            " SELECT *,row_number() OVER ("
            "  PARTITION BY document_type,base_number"
            "  ORDER BY retrieval_score,alias_id"
            " ) AS identity_alias_rank FROM matches"
            ")"
            " SELECT * FROM ranked WHERE identity_alias_rank = 1"
            " ORDER BY retrieval_score,alias_id LIMIT ?",
            (expression, ACTOR_RESOLUTION_TOP_K),
        ).fetchall()
        return tuple(
            cls._candidate(
                cls._identity(row),
                [row],
                rank=rank,
                retrieval_score=float(row["retrieval_score"]),
            )
            for rank, row in enumerate(rows, 1)
        )

    @staticmethod
    def _fts_expression(normalized_query: str) -> str | None:
        tokens = [token for token in normalized_query.split() if len(token) >= 3]
        if not tokens:
            return None
        return " OR ".join(
            f'"{token.replace(chr(34), chr(34) * 2)}"' for token in tokens
        )

    @staticmethod
    def _identity(row: sqlite3.Row) -> IdentityKey:
        return IdentityKey(
            document_type=str(row["document_type"]),  # type: ignore[arg-type]
            base_number=str(row["base_number"]),
        )

    @staticmethod
    def _candidate(
        identity: IdentityKey,
        rows: list[sqlite3.Row],
        *,
        rank: int,
        retrieval_score: float | None = None,
    ) -> ActorResolutionCandidate:
        best = rows[0]
        aliases: list[str] = []
        for row in rows:
            raw_aliases = json.loads(str(row["raw_aliases_json"]))
            if not isinstance(raw_aliases, list):
                raise ValueError("raw_aliases_json must be a list")
            aliases.extend(str(value) for value in raw_aliases)
        return ActorResolutionCandidate(
            identity=identity,
            matched_alias=str(best["normalized_alias"]),
            observed_name=str(best["raw_alias"]),
            occurrences=sum(int(row["occurrences"]) for row in rows),
            first_seen=min(
                (str(row["first_seen"]) for row in rows if row["first_seen"]),
                default=None,
            ),
            last_seen=max(
                (str(row["last_seen"]) for row in rows if row["last_seen"]),
                default=None,
            ),
            rank=rank,
            retrieval_score=retrieval_score,
            alias_evidence=tuple(dict.fromkeys(aliases)),
        )

    @staticmethod
    def _unavailable(normalized_query: str, reason: str) -> ActorResolutionResult:
        return ActorResolutionResult(
            status=ActorResolutionStatus.INDEX_UNAVAILABLE,
            normalized_query=normalized_query,
            method="local_index",
            reason=reason,
        )
