#!/usr/bin/env python3
"""Demo B — Perfil Proponente: busca oportunidades de negocio."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.demo_base import run_demo

TURNS = [
    "oportunidades de obra en Atlántico",
    "filtrar por más de 500 millones",
    "ahora solo licitaciones públicas",
    "muestra los de mayor valor",
]

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default=None)
    args = parser.parse_args()
    kwargs = {"chat_id": "demo_proponente_atl"}
    if args.base_url:
        kwargs["base_url"] = args.base_url
    run_demo(turns=TURNS, **kwargs)
