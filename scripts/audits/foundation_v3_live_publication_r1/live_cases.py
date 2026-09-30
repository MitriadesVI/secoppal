"""Casos productivos A-E en vivo sobre el candidato; persistencia redirigida a un tmp.

Uso: PYTHONPATH=. .venv/bin/python scripts/audits/foundation_v3_live_publication_r1/live_cases.py <salida.json>
Requiere SECOPPAL_FOUNDATION_MODE=foundation_v3 en el entorno o .env, red y proveedor.
"""
import json, re, subprocess, sys, tempfile
from pathlib import Path
from app.config import Settings
from app.core.foundation_producers import LinguisticProducer
from app.core.foundation_trace_view import project
from app.core.orchestrator import SecopalWorkflow
from app.utils.money import format_cop, safe_money

out = Path(sys.argv[1])
tmp = Path(tempfile.mkdtemp())
s = Settings()
assert s.secoppal_foundation_mode == "foundation_v3"
s.secop_feedback_path, s.secop_feedback_db_path = tmp / "f.jsonl", tmp / "s.db"
CARD = ("titulo", "entidad", "valor", "fecha", "fecha_label", "url", "estado", "estado_op_label")

def run(q, channel="streamlit", producer=None):
    wf = SecopalWorkflow(s)
    if producer is not None:
        wf.foundation_producer = producer
    return wf.run_query(q, channel=channel)

def summary(case, q, r, checks):
    soql = [rc["soql"] for rc in r.get("source_call_receipts") or []]
    return {"case": case, "query": q, "route_reason": r.get("route_reason"),
            "terminal": r.get("foundation_terminal"), "total_count": r.get("total_count"),
            "cards": len(r["rows"]), "results": len(r["results"]), "response": r["response"],
            "first_cards": [{k: c.get(k) for k in CARD} for c in r["rows"][:3]],
            "export": {k: r.get(k) for k in ("export_params", "export_notice") if r.get(k)},
            "soql": soql, "trace_view_keys": sorted((project(r) or {}).keys()),
            "checks": checks, "pass": all(checks.values())}

def full(c):
    return bool(c["titulo"]) and c["titulo"] != "Sin nombre" and bool(c["entidad"]) and bool(c["fecha"] or c["fecha_label"])

def no_dump(r):
    return not re.search(r"Referencia: CO1\.", r["response"])

cases = []
q = "contratos de primera infancia de la alcaldía de barranquilla"; r = run(q)
soql = " ".join(rc["soql"] for rc in r["source_call_receipts"])
cases.append(summary("A", q, r, {
    "cards_complete": bool(r["rows"]) and all(full(c) and c["valor"] != "Sin valor" for c in r["rows"]),
    "topic_in_soql": "%primera infancia%" in soql,
    "celebrated_universe_in_soql": "upper(unaccent(trim(estado_contrato))) = 'APROBADO'" in soql,
    "export_stamped": (r.get("export_params") or {}).get("contract_universe") == "celebrated",
    "no_id_dump": no_dump(r),
    "results_raw_cards_aligned": [x["id_contrato"] for x in r["results"]] == [c["id_contrato"] for c in r["rows"]]}))

q = "qué licitaciones de más de 1000 millones están para presentarme en atlántico"; r = run(q)
cases.append(summary("B", q, r, {
    "value_visible": bool(r["rows"]) and all(c["valor"].startswith("$") for c in r["rows"]),
    "all_rows_over_1000M": all(safe_money(x.get("precio_base")) > 1_000_000_000 for x in r["results"]),
    "state_and_date_visible": all((c.get("estado_op_label") or c.get("estado")) and c["fecha"] for c in r["rows"]),
    "no_id_dump": no_dump(r)}))

q = "licitaciones de la alcaldía de barranquilla en 2025"; r = run(q)
cases.append(summary("C", q, r, {
    "cards_complete": bool(r["rows"]) and all(full(c) for c in r["rows"]),
    "value_matches_source": all(c["valor"] == format_cop(x.get("precio_base")) for x, c in zip(r["results"], r["rows"])),
    "no_id_dump": no_dump(r)}))

q = "procesos y contratos de alimentación escolar en bolívar durante 2026"; r = run(q)
by_ds = {}
for c in r["rows"]:
    by_ds.setdefault(c["dataset_id"], []).append(c)
cases.append(summary("D", q, r, {
    "two_groups": len(r["groups"]) == 2,
    "both_datasets_carded": set(by_ds) == {"p6dx-8zbt", "jbjy-vk9h"} and all(full(c) for c in r["rows"]),
    "both_in_text": "Procesos:" in r["response"] and "Contratos:" in r["response"],
    "dual_export_explicitly_blocked": bool(r.get("export_notice")) and not r.get("export_params"),
    "no_id_dump": no_dump(r)}))
rt = run(q, channel="telegram")
cases.append(summary("D-telegram", q, rt, {"cards_in_message": "🏛️" in rt["response"], "no_id_dump": no_dump(rt)}))

def down(system, payload):
    raise TimeoutError("forced provider outage for case E")
q = "contratos de primera infancia de la alcaldía de barranquilla"; r = run(q, producer=LinguisticProducer(down))
cases.append(summary("E", q, r, {
    "labeled_basic_fallback": r.get("route_reason") == "foundation_basic_fallback" and r["response"].startswith("Búsqueda básica — modo degradado"),
    "terminal_provider_error": r.get("foundation_terminal") == "PROVIDER_ERROR",
    "cards_complete": bool(r["rows"]) and all(full(c) for c in r["rows"]),
    "raw_results_preserved": len(r["results"]) == len(r["rows"]) and all("objeto_del_contrato" in x for x in r["results"]),
    "no_id_dump": no_dump(r)}))
out.write_text(json.dumps({"head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                           "cases": cases}, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
for c in cases:
    print(c["case"], "PASS" if c["pass"] else "FAIL", {k: v for k, v in c["checks"].items() if not v}, c["total_count"], c["cards"])
