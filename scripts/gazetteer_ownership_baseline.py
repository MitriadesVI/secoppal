#!/usr/bin/env python3
"""Gate de propiedad múltiple de aliases del gazetteer.

Un alias normalizado reclamado por más de una entidad es deuda: el runtime
sólo puede honrar a UNA (``EntityResolver._load_aliases`` sobrescribe y
``_scan_index`` desempata por longitud). Quién responde no lo decide nadie.

R1C dejó esa deuda contada — 217 grupos antes de la limpieza, 173 después — y
``gazetteer_manager`` la vigilaba con un tope **cardinal**::

    if len(multi_owner) > EXACT_MULTI_OWNER_BASELINE: ...

Ese tope no distingue conjuntos. Con baseline ``{A, B, C}`` un lote que retira
``C`` e introduce ``D`` deja el cardinal intacto (3 → 3) y pasa la puerta
mientras crea una colisión nueva. R2A congela el **conjunto** normalizado de
claves aceptado tras R1C/R1D y exige::

    NEW_MULTI_OWNER_KEYS = current_keys - baseline_keys == ∅

La deuda histórica puede desaparecer (el tope baja, nunca sube); lo que no
puede es sustituirse por deuda nueva.
"""
from __future__ import annotations

import json
from pathlib import Path

_HERE = Path(__file__).resolve().parent
DEFAULT_BASELINE_PATH = _HERE / "gazetteer_multi_owner_baseline.json"


def multi_owner_groups(records: list[dict]) -> dict[str, list[str]]:
    """Alias normalizado → dueños, para los aliases con más de un dueño.

    La normalización es la MISMA que usa ``EntityResolver``, para que lo que
    aquí se llama colisión sea exactamente lo que el runtime ve.
    """
    from app.utils.money import normalize_text

    owners: dict[str, set[str]] = {}
    for record in records:
        official = record.get("official_name", "")
        for alias in record.get("aliases", []):
            key = normalize_text(alias)
            if key:
                owners.setdefault(key, set()).add(official)
    return {key: sorted(names) for key, names in owners.items() if len(names) > 1}


def load_baseline(path: Path | None = None) -> set[str]:
    """Conjunto congelado de claves con dueño múltiple aceptado."""
    baseline_path = path or DEFAULT_BASELINE_PATH
    if not baseline_path.exists():
        raise FileNotFoundError(
            f"Baseline de ownership múltiple ausente: {baseline_path}. "
            "Regenerar con: python scripts/gazetteer_ownership_baseline.py --write"
        )
    with baseline_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return set(payload["keys"])


def new_multi_owner_keys(
    records: list[dict], baseline_keys: set[str]
) -> list[str]:
    """Claves con dueño múltiple que el baseline no acepta."""
    return sorted(set(multi_owner_groups(records)) - baseline_keys)


def _write_baseline(aliases_db: Path, out: Path) -> int:
    with aliases_db.open("r", encoding="utf-8") as handle:
        records = json.load(handle)
    groups = multi_owner_groups(records)
    payload = {
        "source": str(aliases_db.name),
        "note": (
            "Conjunto congelado de aliases normalizados con dueño múltiple "
            "aceptado tras R1C/R1D. Puede encogerse (deuda saldada); no puede "
            "recibir claves nuevas."
        ),
        "count": len(groups),
        "keys": sorted(groups),
    }
    with out.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return len(groups)


if __name__ == "__main__":
    import argparse
    import sys

    _ROOT = _HERE.parent
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aliases-db", type=Path, default=_ROOT / "app" / "data" / "aliases_db.json")
    parser.add_argument("--out", type=Path, default=DEFAULT_BASELINE_PATH)
    parser.add_argument("--write", action="store_true", help="Regenerar el baseline congelado")
    args = parser.parse_args()

    if args.write:
        total = _write_baseline(args.aliases_db, args.out)
        print(f"baseline escrito: {args.out} ({total} claves)")
    else:
        with args.aliases_db.open("r", encoding="utf-8") as handle:
            current = multi_owner_groups(json.load(handle))
        baseline = load_baseline(args.out)
        added = sorted(set(current) - baseline)
        removed = sorted(baseline - set(current))
        print(f"actual={len(current)} baseline={len(baseline)} nuevas={len(added)} saldadas={len(removed)}")
        for key in added:
            print(f"  NUEVA: {key!r} -> {current[key]}")
