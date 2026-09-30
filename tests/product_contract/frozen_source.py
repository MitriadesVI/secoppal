"""Cable SECOP congelado para un ``SecopClient`` real (plan P3/P4, F2.5).

Sólo se sustituye el transporte. ``SecopClient`` conserva reintentos,
instrumentación, normalización de filas y derivación por timeout: ``_respawn``
propaga el transporte explícito, de modo que ``with_timeout`` y
``derived_for_timeout`` no pueden abrir un Socrata real.

El SoQL emitido se **evalúa** en SQLite sobre filas versionadas; no hay
respuestas enlatadas por ruta. Un SoQL que SQLite no sabe evaluar falla ruidoso.
"""
from __future__ import annotations

import json
import re
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder
from app.core.source_call_receipts import active_source_call_metadata
from tests.buyer_actor_sql_dialect import unaccent
from tests.contract_universe_sql_dates import register_dates

_NUMERIC_COLUMN = re.compile(r"^(valor_|precio_)")
_CLAUSE = re.compile(r"\b(WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET)\b", re.IGNORECASE)
_STRING = re.compile(r"'(?:[^']|'')*'")
_CAST = re.compile(r"::(number|text|floating_timestamp|double)\b", re.IGNORECASE)
_OPPORTUNITY_CASE = re.compile(
    r"case\(fecha_de_recepcion_de IS NULL, fecha_de_publicacion_del >= ('[^']+'), "
    r"true, fecha_de_recepcion_de >= ('[^']+')\)", re.IGNORECASE,
)


class FrozenSourceError(AssertionError):
    """El SoQL emitido no pudo evaluarse contra la fuente congelada."""


@dataclass(frozen=True)
class InjectedFailure:
    """Falla determinista de la fuente (timeout, indisponibilidad) para PC-FAIL-*."""

    exception: Callable[[], BaseException]
    operation: str | None = None
    dataset: str | None = None
    times: int | None = None
    soql_contains: str | None = None

    def __post_init__(self):
        if self.times is not None and (type(self.times) is not int or self.times <= 0):
            raise ValueError("INVALID_FAILURE_MULTIPLICITY")

    def matches(self, operation: str, dataset: str) -> bool:
        return self.operation in (None, operation) and self.dataset in (None, dataset)


def table_name(dataset: str) -> str:
    return "ds_" + re.sub(r"[^a-z0-9]", "_", dataset.lower())


def soql_to_sql(soql: str, table: str) -> str:
    """Inserta ``FROM`` antes de la primera cláusula de nivel superior fuera de literales."""
    masked = _STRING.sub(lambda m: "'" + "_" * (len(m.group()) - 2) + "'", soql)
    match = _CLAUSE.search(masked)
    position = match.start() if match else len(soql)
    sql = f"{soql[:position].rstrip()} FROM {table} {soql[position:]}".strip()
    # Socrata's four-argument case(predicate, value, true, fallback) is not
    # SQLite syntax. Translate only the exact O1B predicate; retain raw SoQL
    # in the call ledger so assertions still inspect what runtime emitted.
    sql = _OPPORTUNITY_CASE.sub(
        lambda match: (
            "(CASE WHEN fecha_de_recepcion_de IS NULL THEN "
            f"fecha_de_publicacion_del >= {match.group(1)} ELSE "
            f"fecha_de_recepcion_de >= {match.group(2)} END)"
        ), sql,
    )
    # Casts belong to syntax, never to the bytes of a quoted literal.
    pieces = re.split(r"('(?:[^']|'')*')", sql)
    return "".join(piece if at % 2 else _CAST.sub("", piece)
                   for at, piece in enumerate(pieces))


def load_rows_jsonl(path: Path) -> dict[str, list[dict]]:
    """Cada línea declara ``_dataset``; el resto son columnas SECOP tal como llegan."""
    grouped: dict[str, list[dict]] = {}
    for number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        record = json.loads(line)
        dataset = record.pop("_dataset", None)
        if not dataset:
            raise ValueError(f"{path}:{number}: missing _dataset")
        grouped.setdefault(dataset, []).append({k: v for k, v in record.items() if not k.startswith("_")})
    return grouped


class FrozenTransport:
    """Implementa la interfaz de transporte que ``SecopClient`` invoca."""

    name = "frozen"

    def __init__(self, rows_by_dataset: Mapping[str, Sequence[Mapping]],
                 *, failures: Sequence[InjectedFailure] = (),
                 schemas: Mapping[str, Mapping[str, str]] | None = None) -> None:
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.create_function("unaccent", 1, unaccent)
        register_dates(self._conn)
        # El observer consulta desde varios hilos; una conexión, un candado.
        self._lock = threading.RLock()
        self._tables: set[str] = set()
        self.failures = list(failures)
        self._failure_uses: dict[int, int] = {}
        self.calls: list[dict] = []
        self._schemas = deepcopy(schemas)
        for dataset, rows in rows_by_dataset.items():
            self._load(dataset, rows)

    def _load(self, dataset: str, rows: Sequence[Mapping]) -> None:
        if self._schemas is not None:
            schema = self._schemas.get(dataset)
            if not schema or any(kind not in {"TEXT", "NUMERIC"} for kind in schema.values()):
                raise FrozenSourceError("frozen source schema missing or invalid")
            columns = list(schema)
            if (any(not re.fullmatch(r"[a-z_][a-z0-9_]*", column) for column in columns)
                    or any(not set(row).issubset(schema) for row in rows)):
                raise FrozenSourceError("frozen source row outside sealed schema")
            declared = ", ".join(f'"{column}" {schema[column]}' for column in columns)
        else:
            spec = SoQLBuilder.SPECS.get(dataset)
            columns = list(dict.fromkeys([*(spec.select_fields if spec else ()), *(k for r in rows for k in r)]))
            declared = ", ".join(f'"{c}" {"NUMERIC" if _NUMERIC_COLUMN.match(c) else "TEXT"}' for c in columns)
        table = table_name(dataset)
        self._conn.execute(f"CREATE TABLE {table} ({declared})")
        marks = ", ".join("?" for _ in columns)
        self._conn.executemany(f"INSERT INTO {table} VALUES ({marks})",
                               [[row.get(c) for c in columns] for row in rows])
        self._tables.add(table)

    @property
    def errors(self) -> list[dict]:
        return [call for call in self.calls if "error" in call]

    def snapshot(self, start: int = 0) -> list[dict]:
        """Detach full source results and attempts from later runtime mutation."""
        with self._lock:
            return deepcopy(self.calls[start:])

    def _execute(self, operation: str, dataset: str, soql: str, *,
                 timeout_seconds=None, ordering_specifier=None) -> list[dict]:
        call = {"operation": operation, "dataset": dataset, "soql": soql,
                "timeout_seconds": timeout_seconds,
                "ordering_specifier": getattr(ordering_specifier, "value", ordering_specifier),
                **active_source_call_metadata()}
        with self._lock:
            call["attempt"] = 1 + sum(previous.get("call_id") == call.get("call_id")
                                      for previous in self.calls if call.get("call_id") is not None)
            self.calls.append(call)
            for at, failure in enumerate(self.failures):
                if (failure.matches(operation, dataset)
                        and (failure.soql_contains is None or failure.soql_contains in soql)
                        and (failure.times is None or self._failure_uses.get(at, 0) < failure.times)):
                    self._failure_uses[at] = self._failure_uses.get(at, 0) + 1
                    exc = failure.exception()
                    call["injected"] = type(exc).__name__
                    call["status"] = "error"
                    call["error_type"] = type(exc).__name__
                    raise exc
            table = table_name(dataset)
            if table not in self._tables:
                call["error"] = f"dataset {dataset!r} is not frozen in this scenario"
                call["status"] = "error"
                call["error_type"] = "FrozenSourceError"
                raise FrozenSourceError(call["error"])
            try:
                cursor = self._conn.execute(soql_to_sql(soql, table))
            except sqlite3.Error as exc:
                call["error"] = repr(exc)
                call["status"] = "error"
                call["error_type"] = "FrozenSourceError"
                raise FrozenSourceError(f"frozen source could not evaluate emitted SoQL ({exc}): {soql}") from exc
            # Socrata entrega texto; el runtime nunca ve enteros de SQLite.
            rows = [{k: (None if v is None else str(v)) for k, v in dict(r).items()} for r in cursor.fetchall()]
            call["rows"] = len(rows)
            call["result"] = deepcopy(rows)
            call["status"] = "success"
            return deepcopy(rows)

    def query(self, dataset_id, soql, *, timeout_seconds=None, ordering_specifier=None):
        return self._execute("query", dataset_id, soql, timeout_seconds=timeout_seconds,
                             ordering_specifier=ordering_specifier)

    def count(self, dataset_id, soql, *, timeout_seconds=None, ordering_specifier=None):
        return self._execute("count", dataset_id, soql, timeout_seconds=timeout_seconds,
                             ordering_specifier=ordering_specifier)

    def aggregate(self, dataset_id, soql, *, timeout_seconds=None, ordering_specifier=None):
        return self._execute("aggregate", dataset_id, soql, timeout_seconds=timeout_seconds,
                             ordering_specifier=ordering_specifier)


def frozen_client(transport: FrozenTransport, *, timeout: int = 15) -> SecopClient:
    """``SecopClient`` productivo cuyo único cable es la fuente congelada."""
    return SecopClient(domain="frozen.invalid", app_token=None, timeout=timeout, transport=transport)
