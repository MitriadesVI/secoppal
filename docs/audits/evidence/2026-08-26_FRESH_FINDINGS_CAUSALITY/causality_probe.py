"""Sonda causal de los hallazgos fresh: mecanismo, no síntomas.

Audit-only. No modifica runtime. Ejecuta tres bloques:

  A. VOCABULARY_GAP   — muestra que el comportamiento cambia entre formas
                        superficiales del MISMO concepto, según estén o no
                        enumeradas en una lista mantenida a mano.
  B. PREDICTIVE       — consultas que el lote fresh NUNCA probó, derivadas del
                        mecanismo. Si el mecanismo es real, deben fallar.
  C. POLARITY         — end-to-end con stores aislados: la negación de estado
                        sobre vocabulario SÍ catalogado no se ignora, se
                        INVIERTE (consulta el universo excluido).

Uso:
    .venv/bin/python docs/audits/evidence/2026-08-26_FRESH_FINDINGS_CAUSALITY/causality_probe.py \
        [--json <salida>]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

os.environ["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
os.environ.setdefault(
    "SECOP_ACTOR_ALIAS_INDEX_PATH", str(ROOT / "data" / "actor_alias_index.sqlite")
)
os.environ.pop("DEEPSEEK_API_KEY", None)

FIELDS = (
    "objeto", "objeto_exclude", "valor_min", "valor_max",
    "estado_family", "estado_del_procedimiento", "estado_contrato", "dataset",
)

# ── A. Pares mínimos: misma intención, distinta forma superficial ──────────
MINIMAL_PAIRS = [
    ("ACTION_VERB_INFLECTION", "intent_vocabulary.ACTION_VERBS", [
        "que compra la alcaldia de pasto para salud durante 2025",
        "que compro la alcaldia de pasto para salud durante 2025",
        "que adquirio la alcaldia de pasto para salud durante 2025",
    ]),
    ("CONTROL_PAGINATION", "query_router.STOPWORDS", [
        "muestrame la siguiente pagina",
        "dame mas resultados",
    ]),
    ("CONTROL_RESET", "query_router.STOPWORDS", [
        "limpia todos los filtros y empecemos de cero",
        "borra los filtros",
        "reinicia la busqueda",
    ]),
    ("BIDDER_INTENT_VERB", "intent_vocabulary.INTENT_PATTERNS", [
        "necesito participar suministro de uniformes en cordoba",
        "quiero ofertar suministro de uniformes en cordoba",
        "necesito licitar suministro de uniformes en cordoba",
    ]),
    ("MONEY_SCALE_UNIT", "money_admission.has_explicit_monetary_evidence", [
        "contratos de aseo entre 120 millones y 350 millones en 2024",
        "contratos de aseo entre 120 mil y 350 mil pesos en 2024",
        "contratos de aseo entre 2 mil y 5 mil millones",
    ]),
]

# ── B. Predicciones: consultas ausentes del lote fresh ─────────────────────
PREDICTIONS = [
    ("NEGATED_STATE_NOT_IN_ESTADO_SYNONYMS", [
        "procesos que no esten adjudicados en caldas",
        "contratos que no esten liquidados en 2025",
    ]),
    ("MIL_SCALE_RANGE_LOSES_LOWER_BOUND", [
        "contratos de obra entre 40 mil y 80 mil pesos",
        "contratos de obra entre 700 mil y 900 mil pesos en 2025",
    ]),
    ("UNCATALOGUED_VERB_INFLECTION_LEAKS", [
        "que celebro la alcaldia de pasto en 2025",
        "que gasto la alcaldia de pasto en 2025",
        "que suscribio la alcaldia de pasto en 2025",
    ]),
    ("CONTROL_PHRASING_LEAKS", [
        "continua con los siguientes",
        "quitalo todo y empezamos otra vez",
    ]),
]

# ── C. Inversión de polaridad (end-to-end) ────────────────────────────────
POLARITY = [
    "procesos de obra en Caldas que no esten publicados",
    "procesos de obra en Caldas que no esten abiertos",
    "contratos de aseo que no esten en ejecucion",
    "procesos de obra en Caldas que no esten adjudicados",
]


def _kept(params: dict) -> dict:
    return {k: params.get(k) for k in FIELDS if params.get(k) not in (None, [], "", {})}


def run_parser_blocks() -> dict:
    from app.core.query_router import QueryRouter

    router = QueryRouter()
    pairs = []
    for group, authority, queries in MINIMAL_PAIRS:
        pairs.append({
            "group": group,
            "vocabulary_authority": authority,
            "observations": [
                {"q": q, "frame": _kept(router.parse(q).params)} for q in queries
            ],
        })
    predictions = [
        {
            "prediction": name,
            "observations": [
                {"q": q, "frame": _kept(router.parse(q).params)} for q in queries
            ],
        }
        for name, queries in PREDICTIONS
    ]
    return {"minimal_pairs": pairs, "predictions": predictions}


def run_polarity_block() -> list[dict]:
    """End-to-end con toda persistencia redirigida al sandbox de auditoría."""
    from scripts.audits.audit_isolation import isolated_runtime_stores

    out = []
    with isolated_runtime_stores() as stores:
        from app.config import Settings, get_settings

        get_settings.cache_clear()
        from app.core.conversation_store import ConversationStore
        from app.core.orchestrator import SecopalWorkflow

        wf = SecopalWorkflow(Settings(**stores.settings_overrides, deepseek_api_key=None))
        calls: list[str] = []
        wf.secop_client.query = lambda dataset_id, soql_query, **kw: calls.append(soql_query) or []
        wf.secop_client.count = lambda *a, **k: 0
        wf.secop_client.aggregate = lambda *a, **k: []
        try:
            wf.narrator.narrate_with_grounding = lambda **kw: None
        except Exception:  # noqa: BLE001
            pass
        wf.conv_store = ConversationStore(base_path=stores.conversation_dir)

        for i, q in enumerate(POLARITY):
            calls.clear()
            res = wf.run_query(q, channel="telegram", chat_id=f"causality-polarity-{i}")
            soql = res.get("soql_query") or ""
            where = soql[soql.find("WHERE"):] if "WHERE" in soql else ""
            params = res.get("parsed_params") or {}
            state = {
                k: params.get(k) for k in
                ("estado_family", "estado_del_procedimiento", "estado_contrato", "objeto_exclude")
                if params.get(k) not in (None, [], "", {})
            }
            out.append({
                "q": q,
                "needs_clarification": bool(res.get("needs_clarification")),
                "source_calls": len(calls),
                "state_projection": state,
                "where": where[:400],
                "verdict": (
                    "INVERTED" if state.get("estado_family") else
                    "DROPPED" if not state else "OTHER"
                ),
            })
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    report = run_parser_blocks()
    report["polarity_end_to_end"] = run_polarity_block()

    text = json.dumps(report, ensure_ascii=False, indent=1)
    if args.json:
        Path(args.json).write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
