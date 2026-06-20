#!/usr/bin/env python3
"""run_radar_scheduler.py — entrypoint del scheduler de radares.

Lo dispara un timer de systemd (ver ``deploy/secoppal-radar.{service,timer}``).
Construye el workflow headless (``get_workflow`` es ``lru_cache``, no Streamlit),
corre los radares activos que toquen según su ``frequency`` y loguea un resumen.

Uso:
    python -m scripts.run_radar_scheduler            # corre los que toquen
    python -m scripts.run_radar_scheduler --force    # corre todos los activos
    python -m scripts.run_radar_scheduler --dry-run  # lista sin ejecutar

Exit code ≠ 0 solo si el scheduler mismo falla; un radar individual con error no
rompe la pasada (queda en ``radars.last_error_*``).
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone

from app.core.radar.scheduler import is_due, run_due_radars
from app.service import get_workflow

logger = logging.getLogger("secoppal.radar_scheduler")


def _dry_run(store) -> int:
    now = datetime.now(timezone.utc)
    radars = store.list_all_enabled_radars()
    due = [r for r in radars if is_due(r, now)]
    print(f"Radares activos: {len(radars)} · que correrían ahora: {len(due)}")
    for r in radars:
        marca = "▶ DUE " if r in due else "·     "
        print(f"  {marca} {r.name!r}  freq={r.frequency}  "
              f"last_run={r.last_run_at or 'nunca'}  owner={r.owner}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scheduler de radares SECOPPAL")
    parser.add_argument("--force", action="store_true",
                        help="corre todos los radares activos, ignorando su frecuencia")
    parser.add_argument("--dry-run", action="store_true",
                        help="lista qué radares correrían, sin ejecutar")
    args = parser.parse_args(argv)

    workflow = get_workflow()
    store = getattr(workflow, "radar_store", None)
    if store is None:
        logger.error("radar_store no disponible en el workflow")
        return 1

    if args.dry_run:
        return _dry_run(store)

    summary = run_due_radars(workflow.deps, store, force=args.force)
    logger.info("scheduler terminado: %s", summary.as_line())
    print(summary.as_line())
    return 0


if __name__ == "__main__":
    sys.exit(main())
