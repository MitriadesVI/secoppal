"""Adquisición acotada de grupos de alias desde SECOP II — AP3-B.

La agregación global del dataset contractual agotó tiempo en AP3-A. Esta capa
repite la alternativa validada: una muestra ancla limitada y ordenada por
``:id``, seguida de agregaciones server-side solo para lotes pequeños de
documentos seleccionados. Nunca descarga el universo de contratos crudos.
"""
from __future__ import annotations

import random
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from app.core.actor_identity import compute_check_digit, normalize_identity
from app.core.soql_builder import SoQLBuilder

__all__ = [
    "ACTOR_ALIAS_SOURCE_SEED",
    "ACTOR_DIRECTORY_PAGE_SIZE",
    "ActorAliasSourceSnapshot",
    "DirectoryTraversalError",
    "acquire_actor_alias_directory",
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

# Traversal completo (AP3-E). El servidor devuelve páginas de este tamaño en
# 13–33 s; con ~1,3 M de grupos son unas treinta llamadas.
ACTOR_DIRECTORY_PAGE_SIZE = 50_000
_MAX_DIRECTORY_PAGES = 400

# Un recorrido completo dura unos diez minutos contra el servicio real, y en
# esa ventana un corte transitorio es esperable: el primer build de AP3-E murió
# en la página 25 —tras 1,2 M de grupos ya recorridos— por un read timeout.
# Reintentar la página es mucho más barato que rehacer el recorrido, y no es
# maquinaria de resume: el progreso vive en memoria y el reintento es local.
_DIRECTORY_PAGE_ATTEMPTS = 4
_DIRECTORY_RETRY_BACKOFF_SECONDS = 5.0


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


class DirectoryTraversalError(RuntimeError):
    """El recorrido no pudo demostrar que vio la fuente entera.

    Se lanza en vez de devolver un snapshot incompleto: un directorio al que
    le faltan actores en silencio es peor que no tener directorio, porque su
    ausencia se lee como "este proveedor no existe".
    """


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


# ---------------------------------------------------------------------------
# Recorrido completo — AP3-E.
# ---------------------------------------------------------------------------

_DIRECTORY_WHERE = (
    "{document} IS NOT NULL AND {document} != 'No Definido' AND {name} IS NOT NULL"
)


def _directory_group_key(
    row: Mapping[str, Any] | dict[str, Any],
    spec: Any,
) -> tuple[str, str, str]:
    return (
        str(row.get(spec.contractor_document_type) or ""),
        str(row.get(spec.contractor_document) or ""),
        str(row.get(spec.contractor) or ""),
    )


def acquire_actor_alias_directory(
    client: Any,
    *,
    page_size: int = ACTOR_DIRECTORY_PAGE_SIZE,
    max_pages: int = _MAX_DIRECTORY_PAGES,
    progress: Any = None,
    sleep: Any = time.sleep,
) -> ActorAliasSourceSnapshot:
    """Recorre TODOS los grupos ``(tipo, documento, nombre)`` de la fuente.

    Frente a :func:`acquire_actor_alias_source` —que muestrea un ancla y luego
    consulta los documentos más frecuentes— aquí no hay selección: el
    agrupamiento ocurre en el servidor y se pagina hasta agotarlo. Esa
    diferencia es la que separa un directorio de una muestra, y con ella
    desaparece el sesgo hacia los proveedores grandes.

    Completitud demostrada, no supuesta
    -----------------------------------
    Contar duplicados no detecta huecos: un grupo omitido no aparece en ningún
    sitio. La verificación real es una conciliación aritmética — la suma de
    ``occurrences`` de todos los grupos recibidos debe igualar el ``count(*)``
    que la propia fuente reporta bajo el mismo predicado. Si falta un grupo o
    se contó dos veces, la suma no cuadra y el recorrido falla.

    El orden es total y determinista (documento, tipo, nombre), así que
    ``OFFSET`` recorre una secuencia estable. Se registra la primera y última
    clave de cada página para poder auditar el solapamiento después.

    No descarga contratos crudos: cada página ya viene agregada.
    """
    if page_size < 1 or page_size > 50_000:
        raise ValueError("page_size must be between 1 and 50000")
    spec = SoQLBuilder.SPECS[SoQLBuilder.CONTRATOS_DATASET]
    if not all((
        spec.contractor,
        spec.contractor_document,
        spec.contractor_document_type,
        spec.date,
    )):
        raise RuntimeError("contract DatasetSpec lacks actor alias source fields")

    where = _DIRECTORY_WHERE.format(
        document=spec.contractor_document, name=spec.contractor,
    )
    select = (
        f"{spec.contractor_document_type},{spec.contractor_document},"
        f"{spec.contractor},count(*) AS occurrences,"
        f"min({spec.date}) AS first_seen,max({spec.date}) AS last_seen"
    )
    group = (
        f"{spec.contractor_document_type},{spec.contractor_document},{spec.contractor}"
    )
    order = f"{spec.contractor_document},{spec.contractor_document_type},{spec.contractor}"

    expected_rows = _expected_contract_rows(client, spec, where)

    rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    duplicate_keys = 0
    page_boundaries: list[dict[str, str]] = []
    pages = 0
    offset = 0

    while True:
        if pages >= max_pages:
            raise DirectoryTraversalError(
                f"traversal exceeded {max_pages} pages; refusing a partial directory"
            )
        page = _fetch_directory_page(
            client,
            spec.dataset_id,
            sleep=sleep,
            select=select,
            where=where,
            group=group,
            order=order,
            limit=page_size,
            offset=offset,
        )
        pages += 1
        if not page:
            break
        for row in page:
            key = _directory_group_key(row, spec)
            if key in seen_keys:
                duplicate_keys += 1
                continue
            seen_keys.add(key)
            rows.append(dict(row))
        page_boundaries.append({
            "page": str(pages),
            "first_document": _directory_group_key(page[0], spec)[1],
            "last_document": _directory_group_key(page[-1], spec)[1],
            "rows": str(len(page)),
        })
        if progress is not None:
            progress(pages, len(rows), duplicate_keys)
        if len(page) < page_size:
            break
        offset += page_size

    observed_rows = sum(int(row.get("occurrences") or 0) for row in rows)
    reconciled = expected_rows is not None and observed_rows == expected_rows
    if expected_rows is not None and not reconciled:
        raise DirectoryTraversalError(
            "traversal did not reconcile with the source: "
            f"{observed_rows} contract rows across groups vs {expected_rows} reported "
            "by the source. A gap or a double count would silently remove actors."
        )

    acquisition = {
        "policy": "FULL_SERVER_SIDE_GROUP_TRAVERSAL",
        "page_size": page_size,
        "pages": pages,
        "groups_received": len(rows) + duplicate_keys,
        "unique_group_keys": len(rows),
        "duplicate_group_keys": duplicate_keys,
        "observed_contract_rows": observed_rows,
        "expected_contract_rows": expected_rows,
        "reconciled": reconciled,
        "order_by": order,
        "page_boundaries": page_boundaries,
        "global_group_scan": True,
        "raw_contract_download": False,
        "complete": True,
    }
    return ActorAliasSourceSnapshot(
        rows=tuple(rows),
        dataset_id=spec.dataset_id,
        acquisition=acquisition,
    )


def _fetch_directory_page(
    client: Any,
    dataset_id: str,
    *,
    sleep: Any = time.sleep,
    **query: Any,
) -> list[dict[str, Any]]:
    """Una página del recorrido, con reintentos ante fallo transitorio.

    Se reintenta la MISMA página con el mismo ``offset``: el orden del
    servidor es total y determinista, así que repetir la petición devuelve
    exactamente el mismo tramo. Agotados los intentos se propaga el error —
    seguir sin esa página produciría un directorio con un hueco, que es
    justo lo que la conciliación existe para impedir.
    """
    last_error: Exception | None = None
    for attempt in range(_DIRECTORY_PAGE_ATTEMPTS):
        try:
            return list(client.get(dataset_id, **query))
        except Exception as exc:  # transporte: timeout, corte, 5xx
            last_error = exc
            if attempt < _DIRECTORY_PAGE_ATTEMPTS - 1:
                sleep(_DIRECTORY_RETRY_BACKOFF_SECONDS * (2 ** attempt))
    raise DirectoryTraversalError(
        f"page at offset {query.get('offset')} failed after "
        f"{_DIRECTORY_PAGE_ATTEMPTS} attempts: {last_error}"
    ) from last_error


def _expected_contract_rows(client: Any, spec: Any, where: str) -> int | None:
    """``count(*)`` de la fuente bajo el mismo predicado, o ``None``.

    Es el denominador de la conciliación. Devuelve ``None`` —en vez de
    lanzar— cuando la fuente no puede darlo: sin él el recorrido sigue,
    pero el snapshot queda marcado ``reconciled=False`` y quien lo consuma
    sabe que la completitud no está demostrada.
    """
    try:
        rows = list(client.get(
            spec.dataset_id, select="count(*) AS n", where=where,
        ))
    except Exception:  # pragma: no cover - la fuente decide si lo soporta
        return None
    if not rows:
        return None
    try:
        return int(str(list(rows[0].values())[0]))
    except (TypeError, ValueError):  # pragma: no cover
        return None
