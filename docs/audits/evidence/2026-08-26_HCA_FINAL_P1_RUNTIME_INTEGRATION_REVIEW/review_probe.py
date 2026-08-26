"""Read-only integration evidence; executes repository runtime with isolated stores."""
from __future__ import annotations

import ast
import dataclasses
import json
import os
from pathlib import Path
import re
import sys

ROOT = Path.cwd()
sys.path.insert(0, str(ROOT))
os.environ["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
os.environ["SECOP_ACTOR_ALIAS_INDEX_PATH"] = "/Users/rodrigoortiz/Documents/secoppal/data/actor_alias_index.sqlite"
os.environ.pop("DEEPSEEK_API_KEY", None)

from scripts.audits.audit_isolation import isolated_runtime_stores
from scripts.audits.run_conversation_holistic_audit import _mk_rows, _case_clock
import yaml


def dump(value):
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def where(sql):
    match = re.search(r"\bWHERE\b(.*?)(?:\bORDER BY\b|\bLIMIT\b|$)", sql, re.S)
    return re.sub(r"\s+", " ", match.group(1)).strip() if match else ""


def main():
    out = Path(sys.argv[1])
    corpus = yaml.safe_load((ROOT / "tests/fixtures/audits/conversation_holistic_v1.yaml").read_bytes())
    extra = {
        "MONEY-ACCENT": ["contratos de papeleria por mas de 1 millón"],
        "MONEY-2M": ["contratos de papeleria por mas de 2 millones"],
        "MONEY-500M": ["contratos de papeleria por mas de 500 millones"],
        "MONEY-HALF": ["contratos de papeleria por más de un millón y medio de pesos"],
        "MONEY-HALF-EXACT": ["más de un millón y medio de pesos"],
        "MONEY-YEAR": ["contratos de papeleria en 2025"],
        "MONEY-NIT": ["contratos del proveedor NIT 900739049"],
        "MONEY-COUNT": ["100 procesos de papeleria"],
        "MONEY-BARE": ["contratos de papeleria por mas de 100"],
        "MONEY-BARE-EXACT": ["100"],
        "MONEY-REFERENCE": ["contratos referencia CO1.PCCNTR.1234567"],
        "STATE-OPPORTUNITY": ["oportunidades de aseo en cesar"],
        "STATE-DESIERTOS": ["procesos desiertos de aseo en cesar"],
        "BUYER-COMPLETE": ["contratos de la alcaldia de medellin"],
        "BUYER-LOCAL": ["contratos de la alcaldia local de teusaquillo"],
        "BUYER-TOPIC": ["contratos de aseo de la alcaldia de medellin"],
        "BUYER-ACTOR-PREEXISTING": ["contratos del proveedor LMP Ingeniería con la alcaldía"],
        "BUYER-FONDO-PREEXISTING": ["contratos del fondo nacional de vivienda"],
        "FOLLOWUP-ICBF": ["contratos del ICBF en Antioquia", "ordenalos por mayor valor"],
        "FOLLOWUP-SENA": ["contratos del SENA en Antioquia", "los mas antiguos primero"],
    }
    cases = corpus["cases"] + [{"id": key, "turns": [{"q": q} for q in qs]} for key, qs in extra.items()]
    evidence = {"root": str(ROOT), "turns": [], "modality": [], "state_properties": []}
    with isolated_runtime_stores(prefix="hca_integration_review_") as stores:
        from app.config import Settings, get_settings
        get_settings.cache_clear()
        from app.core.orchestrator import SecopalWorkflow
        from app.core.query_router import QueryRouter
        from app.core.soql_builder import SoQLBuilder
        from app.core.estado_families import ESTADO_FAMILIES, resolve_estado
        import app.core.estado_families as state_module
        from app.core.query_fingerprint import query_fingerprint

        wf = SecopalWorkflow(Settings(**stores.settings_overrides, deepseek_api_key=None))
        calls = []
        def query(dataset_id, soql_query, **kw):
            calls.append({"kind": "query", "dataset": dataset_id, "soql": soql_query, "where": where(soql_query)})
            return _mk_rows(5)
        def count(dataset_id, soql_query, **kw):
            calls.append({"kind": "count", "dataset": dataset_id, "soql": soql_query, "where": where(soql_query)})
            return 5
        def aggregate(*args, **kw):
            calls.append({"kind": "aggregate", "args": args, "kwargs": kw})
            return []
        wf.secop_client.query = query
        wf.secop_client.count = count
        wf.secop_client.aggregate = aggregate
        wf.narrator.narrate_with_grounding = lambda **kw: None
        effective = {"feedback": str(wf.feedback.path), "db": str(wf.feedback.db_path),
                     "conversations": str(wf.conv_store.base_path), "radar": str(wf.radar_store.db_path),
                     "telemetry": str(wf.settings.modality_followup_telemetry_path)}
        assert all(stores.contains(p) for p in effective.values()), effective
        evidence["isolation"] = {"sandbox": str(stores.root), "stores": effective, "outside": []}
        for case in cases:
            chat = "hca-integration-review-" + case["id"]
            for turn_number, turn in enumerate(case["turns"], 1):
                calls.clear()
                result = wf.run_query(turn["q"], channel="telegram", chat_id=chat, today=_case_clock(case))
                history = wf.conv_store.get_history(chat, last_n=1)
                evidence["turns"].append(dump({"id": case["id"], "turn": turn_number, "q": turn["q"],
                    "params": result.get("parsed_params"), "needs_clarification": result.get("needs_clarification"),
                    "route_reason": result.get("route_reason"), "followup": result.get("followup"),
                    "source_calls": calls, "persisted": history[-1].parsed_params if history else None}))

        tree = ast.parse((ROOT / "tests/test_modality_pipeline_integration.py").read_text())
        golden = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == "_NO_MODALITY_GOLDEN" for t in node.targets))
        modality_queries = list(golden) + [
            "procesos de licitacion publica en cali en 2026", "contratos de aseo sin contratacion directa",
            "contratos en ejecucion de aseo por licitacion publica", "contratos por seleccion abreviada",
        ]
        router = QueryRouter()
        builder = SoQLBuilder()
        for q in modality_queries:
            parsed = router.parse(q)
            try:
                sql = builder.build(builder.dataset_id_for(parsed.params.get("dataset")), parsed.params)
            except Exception as exc:
                sql = f"BLOCKED:{type(exc).__name__}:{exc}"
            interpretation = parsed.modality_interpretation
            evidence["modality"].append(dump({"q": q, "params": parsed.params,
                "interpretation": dataclasses.asdict(interpretation), "needs_llm": parsed.needs_llm,
                "route_reason": parsed.route_reason, "soql": sql,
                "fingerprint": query_fingerprint(parsed.params)}))

        evidence["estado_families"] = ESTADO_FAMILIES
        try:
            from app.core.estado_projection import project_estado_family
        except ImportError:
            project_estado_family = None
        if project_estado_family:
            for family, datasets in ESTADO_FAMILIES.items():
                for dataset, fields in datasets.items():
                    expected = {field: values for field, values in fields.items() if values}
                    result = project_estado_family({"dataset": dataset, "estado_family": family})
                    actual = {field: result[field] for field in fields if field in result}
                    assert actual == expected, (family, dataset, actual, expected)
                    evidence["state_properties"].append({"family": family, "dataset": dataset, "pass": True})
            for token, literal in state_module.LEXICAL_EXACT_STATE.items():
                resolved = resolve_estado(token, "procesos")
                p = {"dataset": "procesos", "estado_family": resolved["family"], **resolved["filters"]}
                assert project_estado_family(dict(p)) == p, token
        evidence["query_plan_loc"] = sum(1 for line in (ROOT / "app/core/query_plan.py").read_text().splitlines()
                                        if line.strip() and not line.strip().startswith("#"))
    out.write_text(json.dumps(evidence, ensure_ascii=False, indent=1, default=str))
    print(json.dumps({"turns": len(evidence["turns"]), "modality_queries": len(evidence["modality"]),
                      "state_properties": len(evidence["state_properties"]), "out": str(out)}))


if __name__ == "__main__":
    main()
