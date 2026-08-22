"""HCA-1 exploratory probe — dump real parse/frame/SoQL for sample utterances.

Read-only against runtime: SECOP mocked, narrator stubbed, all persistence
redirected to a temp dir. Usage:

    .venv/bin/python scripts/audits/hca1_probe.py < probes.txt

Input: one utterance per line. A line "---" resets the conversation.
Lines starting with "## " set a new chat label (new conversation).
Output: JSONL to stdout with the full per-turn evidence.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

TMP = Path(tempfile.mkdtemp(prefix="hca1_probe_"))
os.environ["SECOP_FEEDBACK_PATH"] = str(TMP / "feedback.jsonl")
os.environ["SECOP_FEEDBACK_DB_PATH"] = str(TMP / "secoppal.db")
os.environ["CONVERSATION_STORE_DIR"] = str(TMP / "conversations")
os.environ["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
# El worktree de auditoría no versiona data/: el índice de producción vive en
# el checkout principal. Solo lectura (mode=ro&immutable en el resolver).
_MAIN_INDEX = Path("/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite")
os.environ.setdefault("SECOP_ACTOR_ALIAS_INDEX_PATH", str(_MAIN_INDEX))
os.environ.pop("DEEPSEEK_API_KEY", None)

from app.config import Settings, get_settings  # noqa: E402

get_settings.cache_clear()

from app.core.conversation_store import ConversationStore  # noqa: E402
from app.core.orchestrator import SecopalWorkflow  # noqa: E402


def _mk_rows(dataset_id: str, n: int = 5) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append({
            "nombre_del_procedimiento": f"Objeto sintetico {i}",
            "objeto_del_contrato": f"Objeto sintetico {i}",
            "descripcion_del_proceso": f"Descripcion {i}",
            "descripci_n_del_procedimiento": f"Descripcion {i}",
            "entidad": "ENTIDAD SINTETICA",
            "nombre_entidad": "ENTIDAD SINTETICA",
            "departamento": "Atlantico",
            "departamento_entidad": "Atlantico",
            "ciudad_entidad": "Barranquilla",
            "ciudad": "Barranquilla",
            "precio_base": str(100_000_000 * (i + 1)),
            "valor_del_contrato": str(100_000_000 * (i + 1)),
            "estado_de_apertura_del_proceso": "Abierto",
            "estado_contrato": "Activo",
            "fecha_de_publicacion_del": "2026-02-15T00:00:00.000",
            "fecha_de_firma": "2026-02-15T00:00:00.000",
            "modalidad_de_contratacion": "Licitacion Publica",
            "proveedor_adjudicado": "PROVEEDOR SINTETICO SAS",
            "referencia_del_contrato": f"REF-{i}",
            "referencia_del_proceso": f"PROC-{i}",
            "id_del_proceso": f"ID-{i}",
            "urlproceso": {"url": "https://example.com"},
        })
    return rows


def main() -> None:
    wf = SecopalWorkflow(Settings())
    captured: list[dict] = []

    def _query(dataset_id, soql_query, **kw):
        captured.append({"kind": "query", "dataset": dataset_id, "soql": soql_query})
        return _mk_rows(dataset_id)

    def _count(dataset_id, soql_query, **kw):
        captured.append({"kind": "count", "dataset": dataset_id, "soql": soql_query})
        return 5

    wf.secop_client.query = _query  # type: ignore[method-assign]
    wf.secop_client.count = _count  # type: ignore[method-assign]
    try:
        wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore
    except Exception:
        pass
    wf.conv_store = ConversationStore(base_path=TMP / "conv")

    chat_n = 0
    chat_id = "probe-0"
    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        if line.strip() == "---" or line.startswith("## "):
            chat_n += 1
            chat_id = f"probe-{chat_n}"
            if line.startswith("## "):
                print(json.dumps({"chat": chat_id, "label": line[3:]}, ensure_ascii=False))
            continue
        captured.clear()
        try:
            result = wf.run_query(line, channel="telegram", chat_id=chat_id)
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({
                "chat": chat_id, "q": line, "error": f"{type(exc).__name__}: {exc}",
            }, ensure_ascii=False))
            continue
        out = {
            "chat": chat_id,
            "q": line,
            "route_reason": result.get("route_reason"),
            "needs_llm": result.get("needs_llm"),
            "needs_clarification": result.get("needs_clarification"),
            "followup": result.get("followup"),
            "dataset": (result.get("parsed_params") or {}).get("dataset"),
            "params": result.get("parsed_params"),
            "soql": result.get("soql_query"),
            "secop_calls": list(captured),
            "response_head": (result.get("response") or "")[:400],
        }
        print(json.dumps(out, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
