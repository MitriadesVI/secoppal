"""Base para scripts de demo multi-turno de SECOPPAL.

Usa la API local /chat (por defecto http://localhost:8000/chat).
Puede apuntar a producción via BASE_URL env var.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import httpx

DEFAULT_BASE_URL = os.environ.get("SECOPPAL_BASE_URL", "http://localhost:8000")


def run_demo(chat_id: str, turns: list[str], base_url: str = DEFAULT_BASE_URL) -> None:
    """Ejecuta una secuencia de turnos contra /chat y muestra resultados."""
    url = f"{base_url}/chat"
    print(f"\n{'=' * 60}")
    print(f"  DEMO SECOPPAL — chat_id: {chat_id}")
    print(f"  API: {url}")
    print(f"{'=' * 60}\n")

    for i, query in enumerate(turns, 1):
        print(f"\n--- Turno {i} ---")
        print(f"Usuario: {query}")
        try:
            resp = httpx.post(
                url,
                json={"query": query, "channel": "demo", "chat_id": chat_id},
                timeout=120.0,
            )
            resp.raise_for_status()
            data = resp.json()
            print(f"\nAsistente:\n{data.get('response', '(sin respuesta)')}")
            trace = data.get("trace_id", "?")
            count = len(data.get("results", []))
            print(f"\n[trace={trace} | results={count}]")
        except httpx.ConnectError:
            print("ERROR: No se pudo conectar al servidor.")
            print(f"Asegurate de que FastAPI esté corriendo: uvicorn app.main:app --reload")
            sys.exit(1)
        except httpx.HTTPStatusError as e:
            print(f"ERROR HTTP {e.response.status_code}: {e.response.text}")
            sys.exit(1)
        time.sleep(0.5)

    print(f"\n{'=' * 60}")
    print("  DEMO finalizado")
    print(f"{'=' * 60}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Demo multi-turno SECOPPAL")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base URL de la API")
    parser.add_argument("--chat-id", default="demo_default", help="chat_id para mantener contexto")
    args = parser.parse_args()

    # Cada script específico define su propia lista de turns antes de llamar run_demo
    print("Usa demo_alcalde.py, demo_proponente.py o demo_veedor.py")


if __name__ == "__main__":
    main()
