#!/usr/bin/env python3
"""Demo C — Perfil Veedor: vigilancia de concentración en contratos."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.demo_base import run_demo

TURNS = [
    "contratos del INVIAS",
    "comparar entre años",
    "cuánto suman en total",
    "solo los de 2025",
]

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=None)
    args = parser.parse_args()
    kwargs = {"chat_id": "demo_veedor_invias"}
    if args.base_url:
        kwargs["base_url"] = args.base_url
    run_demo(turns=TURNS, **kwargs)
