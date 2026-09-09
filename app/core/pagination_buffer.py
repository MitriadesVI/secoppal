"""Modelo persistible del buffer canonico de paginacion (E8B.2)."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.query_fingerprint import query_fingerprint

DEFAULT_TTL_SECONDS = 15 * 60
MAX_CANDIDATE_ROWS = 600
MAX_VALIDATED_ROWS = 600
PAGINATION_CONTRACT_VERSION = "e8b.2-2-contract-universe-1"


def pagination_contract_version(params: dict) -> str:
    return (PAGINATION_CONTRACT_VERSION if params.get("dataset") in ("contratos", "jbjy-vk9h")
            else "e8b.2-1")


def pagination_fingerprint(
    params: dict, *, contract_version: str | None = None,
) -> str:
    return query_fingerprint(
        params, extra={"pagination_contract": contract_version or pagination_contract_version(params)},
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def row_identity(row: dict, dataset_id: str) -> str:
    fields = (
        ("id_del_proceso", "referencia_del_proceso")
        if dataset_id == "p6dx-8zbt"
        else ("id_contrato", "referencia_del_contrato")
    )
    for field in fields:
        value = row.get(field)
        if value:
            return str(value)
    return ""


def _merge_unique(
    current: list[dict], incoming: list[dict], dataset_id: str, limit: int,
) -> list[dict]:
    out = list(current)
    seen = {identity for row in out if (identity := row_identity(row, dataset_id))}
    for row in incoming:
        identity = row_identity(row, dataset_id)
        if identity and identity in seen:
            continue
        if len(out) >= limit:
            break
        out.append(row)
        if identity:
            seen.add(identity)
    return out


@dataclass(frozen=True)
class PaginationBuffer:
    query_fingerprint: str
    group: str
    dataset_id: str
    query_params: dict
    candidate_buffer: list[dict]
    validated_ranked_buffer: list[dict]
    display_cursor: int
    source_cursor: int
    source_candidate_total: int
    validated_total_known: int
    created_at_utc: str
    expires_at_utc: str
    source_total_approximate: bool = False

    @classmethod
    def create(
        cls,
        *,
        query_fingerprint: str,
        group: str,
        dataset_id: str,
        query_params: dict,
        candidate_buffer: list[dict],
        validated_ranked_buffer: list[dict],
        display_cursor: int,
        source_cursor: int,
        source_candidate_total: int,
        now: datetime | None = None,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        source_total_approximate: bool = False,
    ) -> "PaginationBuffer":
        created = _as_utc(now or _utc_now())
        candidates = _merge_unique([], candidate_buffer, dataset_id, MAX_CANDIDATE_ROWS)
        validated = _merge_unique([], validated_ranked_buffer, dataset_id, MAX_VALIDATED_ROWS)
        cursor = min(max(0, int(display_cursor)), len(validated))
        return cls(
            query_fingerprint=str(query_fingerprint),
            group=str(group),
            dataset_id=str(dataset_id),
            query_params=dict(query_params),
            candidate_buffer=candidates,
            validated_ranked_buffer=validated,
            display_cursor=cursor,
            source_cursor=max(0, int(source_cursor)),
            source_candidate_total=max(0, int(source_candidate_total)),
            validated_total_known=len(validated),
            created_at_utc=created.isoformat(),
            expires_at_utc=(created + timedelta(seconds=max(1, ttl_seconds))).isoformat(),
            source_total_approximate=bool(source_total_approximate),
        )

    @property
    def displayed_so_far(self) -> int:
        return self.display_cursor

    @property
    def source_exhausted(self) -> bool:
        return (
            not self.source_total_approximate
            and self.source_cursor >= self.source_candidate_total
        )

    def is_expired(self, now: datetime | None = None) -> bool:
        current = _as_utc(now or _utc_now())
        try:
            expires = _as_utc(datetime.fromisoformat(self.expires_at_utc))
        except (TypeError, ValueError):
            return True
        return current >= expires

    def take(self, count: int) -> tuple[list[dict], "PaginationBuffer"]:
        size = max(0, int(count))
        end = min(self.display_cursor + size, len(self.validated_ranked_buffer))
        page = self.validated_ranked_buffer[self.display_cursor:end]
        return page, replace(self, display_cursor=end)

    def extend(
        self,
        *,
        candidate_rows: list[dict],
        validated_rows: list[dict],
        next_source_cursor: int,
        source_candidate_total: int,
        source_total_approximate: bool | None = None,
    ) -> "PaginationBuffer":
        candidates = _merge_unique(
            self.candidate_buffer, candidate_rows, self.dataset_id, MAX_CANDIDATE_ROWS,
        )
        validated = _merge_unique(
            self.validated_ranked_buffer, validated_rows, self.dataset_id, MAX_VALIDATED_ROWS,
        )
        return replace(
            self,
            candidate_buffer=candidates,
            validated_ranked_buffer=validated,
            source_cursor=max(self.source_cursor, int(next_source_cursor)),
            source_candidate_total=max(0, int(source_candidate_total)),
            validated_total_known=len(validated),
            source_total_approximate=(
                self.source_total_approximate
                if source_total_approximate is None
                else bool(source_total_approximate)
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_fingerprint": self.query_fingerprint,
            "group": self.group,
            "dataset_id": self.dataset_id,
            "query_params": self.query_params,
            "candidate_buffer": self.candidate_buffer,
            "validated_ranked_buffer": self.validated_ranked_buffer,
            "display_cursor": self.display_cursor,
            "source_cursor": self.source_cursor,
            "source_candidate_total": self.source_candidate_total,
            "validated_total_known": self.validated_total_known,
            "created_at_utc": self.created_at_utc,
            "expires_at_utc": self.expires_at_utc,
            "source_total_approximate": self.source_total_approximate,
        }

    @classmethod
    def from_dict(cls, value: dict) -> "PaginationBuffer":
        return cls(
            query_fingerprint=str(value["query_fingerprint"]),
            group=str(value["group"]),
            dataset_id=str(value["dataset_id"]),
            query_params=dict(value.get("query_params") or {}),
            candidate_buffer=list(value.get("candidate_buffer") or []),
            validated_ranked_buffer=list(value.get("validated_ranked_buffer") or []),
            display_cursor=int(value.get("display_cursor", 0) or 0),
            source_cursor=int(value.get("source_cursor", 0) or 0),
            source_candidate_total=int(value.get("source_candidate_total", 0) or 0),
            validated_total_known=int(value.get("validated_total_known", 0) or 0),
            created_at_utc=str(value["created_at_utc"]),
            expires_at_utc=str(value["expires_at_utc"]),
            source_total_approximate=bool(value.get("source_total_approximate", False)),
        )
