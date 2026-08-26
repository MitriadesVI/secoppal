"""Sonda de autopsia: corre queries reales por el workflow aislado.

Uso:
    .venv/bin/python scripts/audits/probe_query.py "consulta" ["turno 2" ...]
    .venv/bin/python scripts/audits/probe_query.py --router "consulta"

No escribe en los stores reales: reutiliza el aislamiento del runner HCA.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

os.environ["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
os.environ.setdefault(
    "SECOP_ACTOR_ALIAS_INDEX_PATH", str(ROOT / "data" / "actor_alias_index.sqlite"))
os.environ.pop("DEEPSEEK_API_KEY", None)

from scripts.audits.audit_isolation import isolated_runtime_stores  # noqa: E402


def _rows(n: int = 5) -> list[dict]:
    return [{
        "nombre_del_procedimiento": f"Objeto sintetico {i}",
        "objeto_del_contrato": f"Objeto sintetico {i}",
        "descripcion_del_proceso": f"Descripcion {i}",
        "descripci_n_del_procedimiento": f"Descripcion {i}",
        "entidad": "ENTIDAD SINTETICA", "nombre_entidad": "ENTIDAD SINTETICA",
        "departamento": "Atlantico", "departamento_entidad": "Atlantico",
        "ciudad_entidad": "Barranquilla", "ciudad": "Barranquilla",
        "precio_base": str(100_000_000 * (i + 1)),
        "valor_del_contrato": str(100_000_000 * (i + 1)),
        "estado_de_apertura_del_proceso": "Abierto", "estado_contrato": "Activo",
        "fecha_de_publicacion_del": "2026-02-15T00:00:00.000",
        "fecha_de_firma": "2026-02-15T00:00:00.000",
        "modalidad_de_contratacion": "Licitacion Publica",
        "proveedor_adjudicado": "PROVEEDOR SINTETICO SAS",
        "documento_proveedor": "900123456",
        "referencia_del_contrato": f"REF-{i}", "referencia_del_proceso": f"PROC-{i}",
        "id_del_proceso": f"ID-{i}", "id_contrato": f"CO-{i}",
        "urlproceso": {"url": "https://example.com"},
    } for i in range(n)]


def main() -> None:
    args = [a for a in sys.argv[1:]]
    router_only = "--router" in args
    if router_only:
        args.remove("--router")
    chat = f"probe-{abs(hash(tuple(args))) % 10**8}"

    with isolated_runtime_stores() as stores:
        from app.config import Settings, get_settings
        get_settings.cache_clear()
        from app.core.conversation_store import ConversationStore
        from app.core.orchestrator import SecopalWorkflow

        if router_only:
            from app.core.query_router import QueryRouter
            r = QueryRouter()
            for q in args:
                parsed = r.parse(q)
                p = parsed if isinstance(parsed, dict) else getattr(
                    parsed, "params", None) or vars(parsed)
                print("=" * 70)
                print("Q:", q)
                print(json.dumps(
                    {k: v for k, v in p.items() if v not in (None, [], {}, "", False)},
                    ensure_ascii=False, indent=1, default=str))
            return

        wf = SecopalWorkflow(Settings(**stores.settings_overrides, deepseek_api_key=None))
        wf.secop_client.query = lambda *a, **k: _rows(5)      # type: ignore
        wf.secop_client.count = lambda *a, **k: 5             # type: ignore
        wf.secop_client.aggregate = lambda *a, **k: []        # type: ignore
        try:
            wf.narrator.narrate_with_grounding = lambda **k: None  # type: ignore
        except Exception:
            pass
        wf.conv_store = ConversationStore(base_path=stores.conversation_dir)

        for i, q in enumerate(args, 1):
            res = wf.run_query(q, channel="telegram", chat_id=chat)
            p = res.get("parsed_params") or {}
            print("=" * 70)
            print(f"T{i}: {q}")
            print("route:", res.get("route_reason"), "| clar:",
                  res.get("needs_clarification"), "| followup:", res.get("followup"))
            print("PARAMS:", json.dumps(
                {k: v for k, v in p.items() if v not in (None, [], {}, "", False)},
                ensure_ascii=False, default=str))
            print("SOQL:", res.get("soql_query"))
            print("RESP:", (res.get("response") or "")[:400].replace("\n", " "))


if __name__ == "__main__":
    main()
