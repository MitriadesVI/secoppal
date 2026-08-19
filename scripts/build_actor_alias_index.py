#!/usr/bin/env python3
"""Construye el índice local de aliases de actores de AP3-B.

Ejemplos::

    # Producción: adquisición SECOP acotada + replace atómico
    .venv/bin/python scripts/build_actor_alias_index.py --acquire

    # Offline: grupos server-side previamente capturados como JSONL
    .venv/bin/python scripts/build_actor_alias_index.py --source-jsonl groups.jsonl

El JSONL offline usa los campos SECOP ``tipodocproveedor``,
``documento_proveedor``, ``proveedor_adjudicado``, ``occurrences``,
``first_seen`` y ``last_seen``. No debe contener contratos crudos.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.core.soql_builder import SoQLBuilder  # noqa: E402
from app.data.actor_alias_index import build_actor_alias_index  # noqa: E402
from app.data.actor_alias_source import (  # noqa: E402
    ACTOR_ALIAS_SOURCE_SEED,
    acquire_actor_alias_source,
)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number}: expected a JSON object")
        rows.append(value)
    return rows


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--acquire",
        action="store_true",
        help="consultar SECOP con la política ancla+agregación acotada",
    )
    source.add_argument(
        "--source-jsonl",
        type=Path,
        help="leer grupos ya agregados; nunca contratos crudos",
    )
    parser.add_argument("--target", type=Path)
    parser.add_argument(
        "--source-dataset",
        default=SoQLBuilder.CONTRATOS_DATASET,
        help="dataset que produjo --source-jsonl",
    )
    parser.add_argument("--anchor-per-type", type=int, default=50_000)
    parser.add_argument("--selected-per-type", type=int, default=2_500)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--seed", type=int, default=ACTOR_ALIAS_SOURCE_SEED)
    parser.add_argument(
        "--built-at",
        help="timestamp explícito para builds reproducibles; por defecto UTC actual",
    )
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    settings = Settings()
    target = args.target or settings.secop_actor_alias_index_path

    if args.acquire:
        try:
            from sodapy import Socrata
        except ImportError as exc:  # pragma: no cover - dependencia declarada
            raise RuntimeError("sodapy is required for --acquire") from exc
        client = Socrata(
            settings.datos_gov_domain,
            app_token=settings.secop_app_token,
            timeout=max(45, settings.secop_timeout_seconds),
        )
        try:
            snapshot = acquire_actor_alias_source(
                client,
                anchor_per_type=args.anchor_per_type,
                selected_per_type=args.selected_per_type,
                batch_size=args.batch_size,
                seed=args.seed,
            )
        finally:
            client.close()
        rows = snapshot.rows
        source_dataset = snapshot.dataset_id
        build_parameters = snapshot.build_parameters
    else:
        source_path = args.source_jsonl.resolve()
        rows = _read_jsonl(source_path)
        source_dataset = args.source_dataset
        build_parameters = {
            "input": "PREAGGREGATED_JSONL",
            "source_file": source_path.name,
        }

    report = build_actor_alias_index(
        rows,
        target,
        source_dataset=source_dataset,
        built_at=args.built_at,
        build_parameters=build_parameters,
    )
    if not args.quiet:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
