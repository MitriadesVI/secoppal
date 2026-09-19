#!/usr/bin/env python
"""SUPPLIERS request-flex probe: run a corpus through the REAL workflow, no network.

One entrypoint for the three measurements of the front:

* baseline (BASE_SHA):  the V1 shapes the change must preserve;
* candidate:            the V1.1 shapes it must produce;
* dogfood:              20-30 standalone turns under the product route.

Everything is scripted at the boundaries the mission allows (SECOP source, the
two LLM providers); nothing inside the capability is mocked: admission,
authority, capability binding, execution planning and presentation run for real.

Persistencia redirigida (AGENTS.md): every Settings() points at a scratch dir,
never at data/. No app process is started and no live instance is touched.

Usage:
    .venv/bin/python scripts/audits/suppliers_flex_probe.py \
        --corpus docs/audits/suppliers_request_flex_v1/baseline_corpus.json \
        --modes frame_on,frame_off \
        --out docs/audits/suppliers_request_flex_v1/baseline.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from datetime import date
from unittest.mock import Mock

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

# Import after sys.path so the worktree's own app package is the one loaded.
from app.core.natural_alpha.identity import issue_streamlit_identity  # noqa: E402
from app.core.natural_alpha.provider import ProviderResult  # noqa: E402

TODAY = date(2026, 9, 18)

# The allowlisted identity Natural Alpha's eligibility requires; the same shape
# the runtime tests issue (sealed receipt, never a bare mapping).
IDENTITY = issue_streamlit_identity(
    {"email": " ALPHA@EXAMPLE.TEST ", "is_anonymous": False, "channel": "streamlit"},
    {"email": "alpha@example.test"}, auth_enabled=True,
)


def _payload(quote: str) -> str:
    return json.dumps({"jobs": [{
        "action": "ANALYZE", "aspect": "SUPPLIERS",
        "evidence_quote": quote, "occurrence": 1,
    }]}, ensure_ascii=False)


OTHER = json.dumps({"jobs": [{"action": "OTHER", "aspect": "OTHER",
                              "evidence_quote": "x", "occurrence": 1}]})


class ProbeSource:
    """Every call recorded; supplier aggregates answered with scripted rows."""

    timeout = 15

    def __init__(self, rows):
        self.calls: list[tuple[str, str]] = []
        self.rows = list(rows)

    def with_timeout(self, timeout):
        return self

    derived_for_timeout = with_timeout

    def aggregate(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return list(self.rows) if "GROUP BY" in soql else []

    query = aggregate

    def count(self, dataset, soql, **kwargs):
        self.calls.append((dataset, soql))
        return 0


class ApprovingVerifier:
    """B's contract, scripted: every unit JOB_LANGUAGE. Records the units it saw."""

    def __init__(self, labels: dict[str, str] | None = None):
        self.labels = labels or {}
        self.calls: list[list[str]] = []

    def observe(self, call_key, request):
        body = json.loads(
            json.loads(request["input"])["text"].split("\n\nENTRADA\n", 1)[1],
        )
        units = [line.split(":", 1) for line in body["residual_units"]]
        self.calls.append([text.strip().strip('"') for _, text in units])
        return ProviderResult(payload=json.dumps({"labels": [
            {"id": unit_id, "label": self.labels.get(text.strip().strip('"'), "JOB_LANGUAGE")}
            for unit_id, text in units
        ]}), provider_calls=1)


def default_rows(count: int = 7) -> list[dict]:
    """Synthetic suppliers; el 2º y 3º empatan a 10 contratos (testigo de corte)."""
    counts = [12, 10, 10, 8, 6, 4, 1]
    rows = []
    for index in range(min(count, len(counts))):
        document = str(900100000 + index)
        rows.append({
            "documento_proveedor": document,
            "tipodocproveedor": "NIT",
            "supplier_name": f"PROVEEDOR {index + 1}",
            "contract_count": counts[index],
            "total_contract_value": float(counts[index] * 1_000_000),
        })
    return rows


def _settings(root: Path):
    from app.config import Settings

    return Settings(
        _env_file=None,
        deepseek_api_key=None,
        secoppal_natural_alpha_mode="alpha",
        secoppal_natural_alpha_email="alpha@example.test",
        secoppal_a2_suppliers_binding_enabled=True,
        secop_feedback_path=root / "traces.jsonl",
        secop_feedback_db_path=root / "feedback.db",
    )


def run_case(case: dict, mode: str, root: Path) -> dict:
    from app.config import get_settings
    from app.core.conversation_store import ConversationStore
    from app.core.orchestrator import SecopalWorkflow

    os.environ["SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED"] = (
        "true" if mode.startswith("frame_on") else "false"
    )
    os.environ["SECOPPAL_A2_SUPPLIERS_BINDING_ENABLED"] = "true"
    get_settings.cache_clear()
    try:
        import burr.telemetry
        burr.telemetry.g_telemetry_enabled = False
    except Exception:  # pragma: no cover - telemetry is best-effort
        pass

    root.mkdir(parents=True, exist_ok=True)
    flow = SecopalWorkflow(_settings(root))
    flow.conv_store = ConversationStore(root / "conversations")
    flow.narrator.narrate_with_grounding = lambda **kwargs: None
    flow.secop_client = ProbeSource(case.get("rows") or default_rows())
    observation = (
        ProviderResult(OTHER, provider_calls=1)
        if mode.endswith("detected")
        else ProviderResult(_payload(case.get("quote") or "proveedores"), provider_calls=1)
    )
    flow.natural_alpha_provider = Mock(observe=Mock(return_value=observation))
    flow.residual_verifier_provider = ApprovingVerifier(case.get("labels"))

    chat_id = f"probe-{case['id']}-{mode}"
    try:
        result = flow.run_query(
            case["query"], channel="streamlit", chat_id=chat_id,
            user_identity=IDENTITY, today=TODAY,
        )
    except Exception as exc:  # a crash is an outcome, recorded as one
        return {
            "case_id": case["id"], "query": case["query"], "mode": mode,
            "outcome": "CRASH", "error": f"{type(exc).__name__}: {exc}",
        }
    view = result.get("entity_profile_view") or {}
    suppliers_view = view.get("suppliers") if isinstance(view, dict) else None
    return {
        "case_id": case["id"],
        "query": case["query"],
        "mode": mode,
        "outcome": "ANSWER" if not result.get("needs_clarification") else "CLARIFY",
        "route_reason": result.get("route_reason", ""),
        "needs_clarification": bool(result.get("needs_clarification")),
        "response": result.get("response", ""),
        "view_focus": view.get("focus") if isinstance(view, dict) else None,
        "view_suppliers": suppliers_view,
        "accredited_params": {
            key: value for key, value in (result.get("accredited_params") or {}).items()
            if key in ("entidad", "fecha_desde", "fecha_hasta", "ordering_signal",
                       "contract_universe", "top_n")
        },
        "authority_status": result.get("authority_status", ""),
        "authority_reason": result.get("authority_reason", ""),
        "credential_capability": result.get("credential_capability", ""),
        "residual_labels": result.get("residual_labels", {}),
        "calls": [
            {"dataset": dataset, "soql": soql}
            for dataset, soql in flow.secop_client.calls
        ],
        "profile_job_receipts": result.get("profile_job_receipts", []),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--modes", default="frame_on",
                        help="comma list: frame_on, frame_on_detected, frame_off")
    parser.add_argument("--out", required=True)
    parser.add_argument("--only", default="", help="comma list of case ids")
    args = parser.parse_args()

    corpus = json.loads(Path(args.corpus).read_text(encoding="utf-8"))
    cases = corpus["cases"]
    if args.only:
        wanted = set(args.only.split(","))
        cases = [case for case in cases if case["id"] in wanted]
    modes = [mode.strip() for mode in args.modes.split(",") if mode.strip()]

    scratch = Path(tempfile.mkdtemp(prefix="suppliers-flex-probe-"))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        for case in cases:
            for mode in modes:
                record = run_case(case, mode, scratch / f"{case['id']}-{mode}")
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                print(f"{case['id']:>4} {mode:<18} {record['outcome']:<8} "
                      f"{record['route_reason']:<45} "
                      f"calls={len(record.get('calls', []))}")
                for call in record.get("calls", []):
                    compact = " ".join(call["soql"].split())
                    print(f"      → {call['dataset']} {compact[:150]}")
    print(f"\nwrote {out}")
    print(f"scratch: {scratch}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
