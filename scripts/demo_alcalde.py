#!/usr/bin/env python3
"""Demo A — Perfil Alcalde: seguimiento de contratación en su municipio."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.demo_base import run_demo

TURNS = [
    "procesos abiertos en Barranquilla",
    "solo los de infraestructura vial",
    "ordena por mayor valor",
    "muéstrame más",
]

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=None)
    args = parser.parse_args()
    kwargs = {"chat_id": "demo_alcalde_baq"}
    if args.base_url:
        kwargs["base_url"] = args.base_url
    run_demo(turns=TURNS, **kwargs)
