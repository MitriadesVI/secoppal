"""Adquisición acotada de grupos de alias desde SECOP II — AP3-B.

La agregación global del dataset contractual agotó tiempo en AP3-A. Esta capa
repite la alternativa validada: una muestra ancla limitada y ordenada por
``:id``, seguida de agregaciones server-side solo para lotes pequeños de
documentos seleccionados. Nunca descarga el universo de contratos crudos.
"""
from __future__ import annotations

import random
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from app.core.actor_identity import compute_check_digit, normalize_identity
from app.core.soql_builder import SoQLBuilder

__all__ = [
    "ACTOR_ALIAS_SOURCE_SEED",
    "ActorAliasSourceSnapshot",
    "acquire_actor_alias_source",
]


ACTOR_ALIAS_SOURCE_SEED = 20260818
_SOURCE_TYPES = ("NIT", "Cédula de Ciudadanía")
_SOURCE_TO_DOCUMENT_TYPE = {"NIT": "NIT", "Cédula de Ciudadanía": "CC"}
_TARGETED_DIRTY_DOCUMENTS = ("13848603", "860002400")
_MAX_ANCHOR_PER_TYPE = 100_000
_MAX_SELECTED_PER_TYPE = 10_000
_MAX_BATCH_SIZE = 100
_MAX_GROUPS_PER_BATCH = 10_000


@dataclass(frozen=True, slots=True)
class ActorAliasSourceSnapshot:
    rows: tuple[dict[str, Any], ...]
    dataset_id: str
    acquisition: dict[str, Any]

    @property
    def build_parameters(self) -> dict[str, Any]:
        return {
            "acquisition": self.acquisition,
            "dataset_id": self.dataset_id,
        }


def _chunks(values: Sequence[str], size: int) -> Iterable[list[str]]:
    for offset in range(0, len(values), size):
        yield list(values[offset:offset + size])


def _document_variants(
    document_type: str,
    base_number: str,
    observed: set[str],
) -> set[str]:
    variants = set(observed)
    variants.add(base_number)
    if document_type == "NIT":
        check_digit = compute_check_digit(base_number)
        if check_digit is not None:
            variants.add(f"{base_number}{check_digit}")
            variants.add(f"{base_number}-{check_digit}")
    return variants


def _validate_bounds(
    *,
    anchor_per_type: int,
    selected_per_type: int,
    batch_size: int,
) -> None:
    for name, value, maximum in (
        ("anchor_per_type", anchor_per_type, _MAX_ANCHOR_PER_TYPE),
        ("selected_per_type", selected_per_type, _MAX_SELECTED_PER_TYPE),
        ("batch_size", batch_size, _MAX_BATCH_SIZE),
    ):
        if value < 1 or value > maximum:
            raise ValueError(f"{name} must be between 1 and {maximum}")


def acquire_actor_alias_source(
    client: Any,
    *,
    anchor_per_type: int = 50_000,
    selected_per_type: int = 2_500,
    batch_size: int = 50,
    seed: int = ACTOR_ALIAS_SOURCE_SEED,
) -> ActorAliasSourceSnapshot:
    """Obtiene grupos ``(tipo, documento, nombre)`` con límites duros.

    ``client`` es un transporte Socrata-compatible con ``get``. Inyectarlo hace
    que la política de adquisición pueda probarse sin red.
    """
    _validate_bounds(
        anchor_per_type=anchor_per_type,
        selected_per_type=selected_per_type,
        batch_size=batch_size,
    )
    spec = SoQLBuilder.SPECS[SoQLBuilder.CONTRATOS_DATASET]
    if not all((
        spec.contractor,
        spec.contractor_document,
        spec.contractor_document_type,
        spec.date,
    )):
        raise RuntimeError("contract DatasetSpec lacks actor alias source fields")

    anchors: dict[str, list[dict[str, Any]]] = {}
    for source_type in _SOURCE_TYPES:
        rows = client.get(
            spec.dataset_id,
            select=f":id,{spec.contractor_document_type},{spec.contractor_document}",
            where=(
                f"{spec.contractor_document_type} = '{source_type}' "
                f"AND {spec.contractor_document} IS NOT NULL "
                f"AND {spec.contractor_document} != 'No Definido'"
            ),
            order=":id",
            limit=anchor_per_type,
        )
        anchors[source_type] = list(rows)

    selected: dict[str, list[str]] = {}
    raw_forms: dict[tuple[str, str], set[str]] = defaultdict(set)
    rng = random.Random(seed)
    for source_type, rows in anchors.items():
        document_type = _SOURCE_TO_DOCUMENT_TYPE[source_type]
        counts: Counter[str] = Counter()
        for row in rows:
            raw_document = row.get(spec.contractor_document)
            match = normalize_identity(raw_document, document_type=document_type)
            if match.key is None:
                continue
            counts[match.key.base_number] += 1
            raw_forms[(document_type, match.key.base_number)].add(str(raw_document))
        tie_break = {document: rng.random() for document in counts}
        ordered = sorted(
            counts,
            key=lambda document: (-counts[document], tie_break[document], document),
        )
        selected[document_type] = ordered[:selected_per_type]

    # Los dos documentos contaminados observados por AP2/AP3-A son canarios de
    # seguridad: vuelven a medirse aunque no entren en la muestra ancla.
    selected.setdefault("CC", []).append(_TARGETED_DIRTY_DOCUMENTS[0])
    selected.setdefault("NIT", []).extend(_TARGETED_DIRTY_DOCUMENTS)

    query_documents: set[str] = set()
    selected_identity_counts: dict[str, int] = {}
    for document_type, base_numbers in selected.items():
        unique_base_numbers = tuple(dict.fromkeys(base_numbers))
        selected_identity_counts[document_type] = len(unique_base_numbers)
        for base_number in unique_base_numbers:
            query_documents.update(_document_variants(
                document_type,
                base_number,
                raw_forms[(document_type, base_number)],
            ))

    aggregate_rows: list[dict[str, Any]] = []
    batches = 0
    for batch in _chunks(sorted(query_documents), batch_size):
        batches += 1
        escaped = ",".join(
            f"'{value.replace(chr(39), chr(39) * 2)}'" for value in batch
        )
        row_limit = min(_MAX_GROUPS_PER_BATCH, max(5_000, len(batch) * 100))
        rows = list(client.get(
            spec.dataset_id,
            select=(
                f"{spec.contractor_document_type},{spec.contractor_document},"
                f"{spec.contractor},count(*) AS occurrences,"
                f"min({spec.date}) AS first_seen,max({spec.date}) AS last_seen"
            ),
            where=(
                f"{spec.contractor_document} IN ({escaped}) "
                f"AND {spec.contractor} IS NOT NULL"
            ),
            group=(
                f"{spec.contractor_document_type},{spec.contractor_document},"
                f"{spec.contractor}"
            ),
            limit=row_limit,
        ))
        if len(rows) >= row_limit:
            raise RuntimeError(
                f"targeted aggregate batch {batches} reached its {row_limit} row cap"
            )
        aggregate_rows.extend(rows)

    acquisition = {
        "policy": "BOUNDED_ANCHOR_THEN_TARGETED_SERVER_AGGREGATION",
        "seed": seed,
        "anchor_per_type": anchor_per_type,
        "selected_per_type": selected_per_type,
        "batch_size": batch_size,
        "anchor_rows_by_source_type": {
            source_type: len(rows) for source_type, rows in anchors.items()
        },
        "selected_logical_identities_by_type": selected_identity_counts,
        "queried_document_variants": len(query_documents),
        "targeted_batches": batches,
        "aggregated_alias_groups": len(aggregate_rows),
        "global_group_scan": False,
        "raw_contract_download": False,
        "limits": {
            "maximum_anchor_per_type": _MAX_ANCHOR_PER_TYPE,
            "maximum_selected_per_type": _MAX_SELECTED_PER_TYPE,
            "maximum_batch_size": _MAX_BATCH_SIZE,
            "maximum_groups_per_batch": _MAX_GROUPS_PER_BATCH,
        },
    }
    return ActorAliasSourceSnapshot(
        rows=tuple(aggregate_rows),
        dataset_id=spec.dataset_id,
        acquisition=acquisition,
    )
