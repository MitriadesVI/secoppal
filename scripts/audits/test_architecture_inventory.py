"""TEST_ARCHITECTURE_V1 — Fase 0/1: baseline e inventario estático de la suite.

Ninguna etapa ejecuta tests:

* ``collect``: ``pytest --collect-only`` en proceso con un plugin que vuelca por
  nodeid markers, fixtures, parametrización y la evaluación estática de
  ``skip``/``skipif``/``xfail``.
* ``build``: escaneo AST de ``tests/test_*.py``, clasificación heurística por
  nodeid, familias, candidatos de duplicación y (opcional) duraciones de un
  JUnit histórico.
* ``render``: ``docs/testing/TEST_INVENTORY_V1.md`` y ``baseline.json``.

La clasificación es heurística, no autoridad. Cada nodeid lleva ``evidence`` y
``confidence`` para que un revisor pueda refutarla; nada aquí decide retiros.

Uso::

    python scripts/audits/test_architecture_inventory.py collect
    python scripts/audits/test_architecture_inventory.py build \
        --junit docs/audits/contract_universe_v1_1_final_closeout/global.xml \
        --junit-head ded6ae4c09d3085b82c15c1945efcab5d446b73a
    python scripts/audits/test_architecture_inventory.py render
"""
from __future__ import annotations

import argparse
import ast
import gzip
import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "artifacts" / "audits" / "test_architecture_v1"
COLLECTION = OUT_DIR / "collection.json.gz"
INVENTORY = OUT_DIR / "inventory.json"
ITEMS = OUT_DIR / "inventory_items.jsonl.gz"
BASELINE = OUT_DIR / "baseline.json"
INVENTORY_MD = ROOT / "docs" / "testing" / "TEST_INVENTORY_V1.md"

AUTOUSE = {"request", "monkeypatch", "tmp_path", "isolate_runtime_persistence",
           "actor_profile_flag_on", "isolate_actor_alias_index"}


# ---------------------------------------------------------------- collect ---


class _Collector:
    def __init__(self) -> None:
        self.items: list[dict] = []
        self.collect_reports: list[dict] = []

    def pytest_collectreport(self, report):
        if report.outcome != "passed":
            self.collect_reports.append({"nodeid": report.nodeid, "outcome": report.outcome,
                                         "longrepr": str(report.longrepr)[:2000]})

    def pytest_collection_finish(self, session):
        # API privada de pytest 9: evalúa skipif/xfail igual que en ejecución.
        from _pytest.skipping import evaluate_skip_marks, evaluate_xfail_marks

        for item in session.items:
            try:
                skip = evaluate_skip_marks(item)
                skip_reason, skip_error = (skip.reason if skip else None), None
            except Exception as exc:
                skip_reason, skip_error = None, repr(exc)[:300]
            try:
                xfail = evaluate_xfail_marks(item)
                xfail_info = ({"reason": xfail.reason, "strict": xfail.strict, "run": xfail.run}
                              if xfail else None)
            except Exception as exc:
                xfail_info = {"error": repr(exc)[:300]}
            callspec = getattr(item, "callspec", None)
            self.items.append({
                "nodeid": item.nodeid,
                "path": item.nodeid.split("::", 1)[0],
                "function": getattr(item, "originalname", item.name),
                "cls": item.cls.__name__ if getattr(item, "cls", None) else None,
                "markers": sorted({m.name for m in item.iter_markers()}),
                "fixtures": sorted(getattr(item, "fixturenames", ())),
                "param_id": callspec.id if callspec else None,
                "static_skip": skip_reason,
                "static_skip_error": skip_error,
                "static_xfail": xfail_info,
            })


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def _write_gz(path: Path, text: str) -> None:
    # mtime=0: el mismo inventario produce los mismos bytes (diff revisable).
    with open(path, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
        gz.write(text.encode("utf-8"))


def _read_gz(path: Path) -> str:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return fh.read()


def cmd_collect(_args) -> int:
    import pytest

    collector = _Collector()
    code = pytest.main(["--collect-only", "-q", "-p", "no:cacheprovider", "tests"], plugins=[collector])
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _write_gz(COLLECTION, json.dumps({
        "head": _git("rev-parse", "HEAD"),
        "worktree_dirty": bool(_git("status", "--porcelain")),
        "pytest_exit_code": int(code),
        "python": sys.version.split()[0],
        "pytest": pytest.__version__,
        "items": collector.items,
        "collect_reports": collector.collect_reports,
    }, ensure_ascii=False))
    print(f"items={len(collector.items)} collect_reports={len(collector.collect_reports)} exit={int(code)}")
    return int(code)


# ------------------------------------------------------------------- scan ---

NETWORK_MODULES = {"requests", "httpx", "urllib.request", "socket", "aiohttp", "urllib3"}
WORKFLOW_CALLS = {"run_query", "run_turn", "handle_message", "say"}
WORKFLOW_FIXTURES = re.compile(r"^(workflow|flow|wf|chat|client|app_client|pipeline|harness)(_|$)")
ARTIFACT_RE = re.compile(r"artifacts/audits/[\w.-]+")
CORPUS_RE = re.compile(r"(?:tests/)?fixtures/[\w./-]+\.(?:ya?ml|jsonl?|py)")
QUERY_RE = re.compile(r"^[a-záéíóúñü¿?][\wáéíóúñü¿?,.$ -]{10,160}$", re.I)
INV_RE = re.compile(r"INV-[A-Z0-9][A-Z0-9-]+")

STRONG_HISTORY = ("independent", "remediation", "audit", "adjudicat", "final_review", "final_delta",
                  "counterexamples", "mutation_oracles")
WEAK_HISTORY = ("dogfood", "pilot", "sprint", "bloque", "canary", "characterization", "benchmark",
                "performance", "freeze", "frozen", "regressions")
VERSION_RE = re.compile(r"(?:^|_)(v\d+(?:_\d+)*[a-z]?\d*|r\d+[a-z]*\d*[a-z]?|m\d+[a-z]*\d*|f\d+(?:_\d+)*|p0\d|h\d)(?=_|$)")
ARCH_NAME = ("module_size", "import_boundary", "import_guard", "workflow_architecture", "index_isolation",
             "runner_isolation", "feedback_isolation", "ui_boundary", "catalog_parity",
             "corpus_structure", "invariants_lock")

FAMILIES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("product_contract", ("product_contract_v1", "product_harness_safety", "invariant_registry")),
    ("architecture_meta", ("module_size", "workflow_architecture", "audit_runner_isolation", "stress_corpus_structure", "invariants_lock", "invariants_characterization", "llm_integration_coverage")),
    ("truth_layer", ("truth_layer", "process_operational_state", "streamlit_operational_state", "published_temporal_value", "firmado_signed", "suspended_contract")),
    ("contract_universe", ("contract_universe", "cu_v11", "cu_f21", "contract_invariants")),
    ("entity_profile_frame", ("entity_profile_frame", "v12a", "v12b", "frame_leak")),
    ("entity_profile", ("entity_profile", "entity_ecosystem", "procurement_")),
    ("buyer_actor_arbitration", ("buyer_actor", "beta_entity_actor", "beta_conversation")),
    ("actor", ("actor_",)),
    ("buyer_entity_resolution", ("buyer_", "entity_resolver", "entity_candidates", "entity_multi", "entity_trailing", "qualified_buyer", "compound_", "generic_buyer", "root_buyer", "declared_buyer", "institutional_", "fuzzy_identity", "scan_alias", "sena_", "icbf_", "car_core", "control_national", "national_core", "governorate", "public_university", "d6_department", "secop1_entity", "source_name_projection", "accredited_name")),
    ("geo_territory", ("municipal", "turbaco", "gazetteer", "territorial", "regiones", "city_detection", "geo_", "plan_query_city")),
    ("money_value", ("money", "value_", "format_money")),
    ("modality", ("modality", "search_sh1_modality")),
    ("negation_control", ("negation", "control_language", "order_semantics", "ordering_", "state_polarity", "safe_negation")),
    ("time_state_opportunity", ("time_semantics", "temporal_", "deadline", "opportunity", "state_", "estado_", "reorder_active")),
    ("followup_conversation", ("followup", "e2e_", "dialogue", "conversation", "anaphora", "refinement", "newsearch", "suggest", "contextual_suggestions", "slash_commands", "llm_handler", "dogfood_regressions", "search_sh1", "hca_fresh")),
    ("query_frame_parsing", ("query_frame", "query_plan", "plan_query", "query_router", "query_text", "spell", "accent", "morpho", "polysemic", "objeto_phrases", "phrase_", "bidder_intent", "contract_type", "normalization_contract", "multi_and", "llm_expansion", "linguistic_family", "semantic_sanity", "degrade_query", "query_fingerprint", "contractual_dimension", "p15_", "sip049", "bloque1", "guard_where")),
    ("soql_source_execution", ("soql", "socrata", "soda3", "secop_", "safe_soql", "h4_fallback", "parallel_secop", "timeout", "count_", "candidate_limit", "direct_lookup", "url_", "secop1_procesos", "outcome", "observer", "instrumentation", "observability", "recall_audit")),
    ("ranking_results_presentation", ("rank", "relevance", "result_", "pagination", "formatter", "narrator", "response_policy", "export_xlsx", "retrieval_text", "orchestrator", "analytic", "dual_dataset", "workflow_", "demo_", "rated_eval", "torture", "pilot_real", "sprint", "feedback_", "conversation_snapshot", "conversation_store")),
    ("notify", ("notify",)),
    ("radar", ("radar", "workspace_store")),
    ("channels_auth", ("telegram", "openwa", "auth", "web_session")),
)


def family_of(stem: str) -> str:
    name = stem.removeprefix("test_")
    for family, patterns in FAMILIES:
        if any(p in name for p in patterns):
            return family
    return "other"


def _dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_dotted(node.value)}.{node.attr}"
    if isinstance(node, ast.Call):
        return _dotted(node.func)
    return ""


def _is_arch_probe(segment: str, calls: set[str]) -> bool:
    if calls & {"ast.parse", "inspect.getsource", "inspect.getsourcefile", "inspect.getsourcelines"}:
        return True
    return bool(re.search(r"\.(read_text|rglob|glob)\(", segment)) and bool(
        re.search(r"""['"]app(?:/|['"])|/\s*['"]app['"]|app/[\w/]+\.py""", segment))


def scan_file(path: Path) -> dict:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    imports: set[str] = set()
    identifiers: set[str] = set()
    queries: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
        elif isinstance(node, ast.Attribute):
            identifiers.add(node.attr)
        elif isinstance(node, ast.Name):
            identifiers.add(node.id)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            text = " ".join(node.value.split()).lower()
            if " " in text and QUERY_RE.match(text) and not text.startswith(("select ", "where ")):
                queries.add(text)

    # Helpers y fixtures del módulo que llegan al workflow real (cierre transitivo):
    # muchos tests llaman ``_turn(...)`` o reciben ``chat`` en vez de ``run_query``.
    defs = {n.name: n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    def_calls = {name: {_dotted(c).split(".")[-1] for c in ast.walk(n) if isinstance(c, ast.Call)}
                 for name, n in defs.items()}
    reaches = {name for name, calls in def_calls.items()
               if calls & (WORKFLOW_CALLS | {"SecopalWorkflow"})}
    changed = True
    while changed:
        grown = {name for name, calls in def_calls.items() if calls & reaches} | {
            name for name, n in defs.items() if {a.arg for a in n.args.args} & reaches}
        changed = not grown <= reaches
        reaches |= grown

    functions: dict[str, dict] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or not node.name.startswith("test"):
            continue
        segment = ast.get_source_segment(source, node) or ""
        calls = {_dotted(n) for n in ast.walk(node) if isinstance(n, ast.Call)}
        args = [a.arg for a in node.args.args]
        doc = ast.get_docstring(node) or ""
        functions.setdefault(node.name, {
            "hypothesis": any(_dotted(d).endswith("given") for d in node.decorator_list),
            "arch_probe": _is_arch_probe(segment, calls),
            "workflow_call": any(c.split(".")[-1] in WORKFLOW_CALLS for c in calls) or node.name in reaches,
            "workflow_fixture": any(WORKFLOW_FIXTURES.match(a) or (a in reaches and a in defs) for a in args),
            "runtime_skip": any(c in {"pytest.skip", "pytest.xfail"} for c in calls),
            "docstring": doc.strip().splitlines()[0][:160] if doc.strip() else "",
            "lineno": node.lineno,
        })

    module_doc = ast.get_docstring(tree) or ""
    app_imports = sorted(i for i in imports if i == "app" or i.startswith("app."))
    loads_app_by_path = "importlib" in " ".join(imports) and bool(re.search(r"['\"]app[/'\"]", source))
    return {
        "lines": source.count("\n") + 1,
        "module_doc_head": module_doc.strip().splitlines()[0][:200] if module_doc.strip() else "",
        "imports_app": app_imports,
        "loads_app_by_path": loads_app_by_path,
        "reads_app_data": bool(re.search(r"app/data/|['\"]app['\"]\s*/\s*['\"]data", source)),
        "imports_tests_helpers": sorted(i for i in imports if i.startswith("tests")),
        "imports_scripts": sorted(i for i in imports if i.startswith("scripts")),
        "hypothesis": any(i.startswith("hypothesis") for i in imports),
        "network_imports": sorted(i for i in imports if i in NETWORK_MODULES),
        "blocks_network": "socket.create_connection" in source or "External I/O forbidden" in source
                          or "external I/O forbidden" in source,
        "uses_workflow": "SecopalWorkflow" in identifiers or "TestClient" in identifiers,
        "corpora": sorted(set(CORPUS_RE.findall(source))),
        "artifacts": sorted(set(ARTIFACT_RE.findall(source))),
        "adr": sorted(set(re.findall(r"ADR-\d{3}", source))),
        "invariants": sorted(set(INV_RE.findall(source))),
        "severity_tokens": sorted(set(re.findall(r"\bP[01]\b", source))),
        "mentions_mutation": bool(re.search(r"mutaci|mutant|mutation", source, re.I)),
        "live_opt_in": bool(re.search(r"production actor index|BUYER_ACTOR_LOCAL_INDEX|Recall vivo|data/actor_alias_index", source)),
        "functions": functions,
        "_identifiers": identifiers,
        "_queries": queries,
    }


# --------------------------------------------------------------- classify ---


def classify(stem: str, scan: dict, fn: dict | None) -> tuple[str, str, list[str]]:
    """Devuelve (clase, confianza, evidencia) para un nodeid."""
    name = stem.removeprefix("test_")
    strong = [t for t in STRONG_HISTORY if t in name]
    weak = [t for t in WEAK_HISTORY if t in name]
    version = VERSION_RE.findall(name)
    fn = fn or {}

    if name == "product_contract_v1":
        return "PRODUCT", "high", ["suite:READY_product_contract"]
    if name in {"product_harness_safety", "invariant_registry"}:
        return "ARCHITECTURE", "high", ["suite:harness_or_registry_gate"]

    if strong:
        return "AUDIT_HISTORICAL", "high", [f"name:{t}" for t in strong] + [f"version:{v}" for v in version]
    if any(t in name for t in ARCH_NAME):
        return "ARCHITECTURE", "high", [f"name:{t}" for t in ARCH_NAME if t in name]
    if weak:
        return "AUDIT_HISTORICAL", "medium", [f"name:{t}" for t in weak] + [f"version:{v}" for v in version]
    if fn.get("hypothesis"):
        return "PROPERTY", "high", ["fn:@given"]
    if fn.get("arch_probe"):
        return "ARCHITECTURE", "medium", ["fn:reads_app_source"]
    if scan["imports_scripts"] and any(i.startswith("scripts.audits") for i in scan["imports_scripts"]):
        return "AUDIT_HISTORICAL", "medium", ["imports:scripts.audits"] + [f"version:{v}" for v in version]
    if version and scan["artifacts"]:
        return "AUDIT_HISTORICAL", "medium", [f"version:{v}" for v in version] + ["reads:artifacts/audits"]
    if fn.get("workflow_call") or fn.get("workflow_fixture") or name.endswith("_e2e") or name.startswith("e2e_"):
        ev = ["fn:workflow_call" if fn.get("workflow_call") else "fn:workflow_fixture_or_e2e_name"]
        return "PRODUCT", "medium" if scan["uses_workflow"] else "low", ev
    if scan["artifacts"] and not scan["imports_app"]:
        return "AUDIT_HISTORICAL", "low", ["reads:artifacts/audits", "no_app_import"]
    if scan["imports_app"] or scan["loads_app_by_path"] or scan["reads_app_data"]:
        ev = ["imports:app" if scan["imports_app"] else "loads_app_by_path_or_app_data"]
        return "DOMAIN_INVARIANT", "low" if version else "medium", ev + [f"version:{v}" for v in version]
    if scan["corpora"]:
        return "ARCHITECTURE", "low", ["corpus_or_fixture_structure_only"]
    return "UNKNOWN", "none", ["no_signal"]


# -------------------------------------------------------------- durations ---


def load_junit(path: Path) -> dict[str, float]:
    durations: dict[str, float] = {}
    for tc in ET.parse(path).getroot().iter("testcase"):
        parts = tc.get("classname", "").split(".")
        if len(parts) < 2:
            continue
        nodeid = f"{parts[0]}/{parts[1]}.py" + "".join(f"::{p}" for p in parts[2:]) + f"::{tc.get('name')}"
        durations[nodeid] = float(tc.get("time", 0.0))
    return durations


# ------------------------------------------------------------ duplication ---


def _jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def duplication(scans: dict[str, dict], files: dict[str, dict]) -> dict:
    pairs = []
    for a, b in combinations(sorted(scans), 2):
        sa, sb = scans[a], scans[b]
        shared_app = set(sa["imports_app"]) & set(sb["imports_app"])
        if not shared_app:
            continue
        q = _jaccard(sa["_queries"], sb["_queries"])
        ident = _jaccard(sa["_identifiers"], sb["_identifiers"])
        app = _jaccard(set(sa["imports_app"]), set(sb["imports_app"]))
        score = round(0.45 * q + 0.35 * ident + 0.20 * app, 3)
        if score >= 0.35:
            pairs.append({
                "a": a, "b": b, "score": score, "query_jaccard": round(q, 3),
                "identifier_jaccard": round(ident, 3), "app_import_jaccard": round(app, 3),
                "shared_queries": len(sa["_queries"] & sb["_queries"]),
                "same_family": files[a]["family"] == files[b]["family"],
                "classes": [files[a]["primary_class"], files[b]["primary_class"]],
                "workflow": [files[a]["uses_workflow"], files[b]["uses_workflow"]],
            })
    # Desempates explícitos: el orden de iteración de sets de str cambia con PYTHONHASHSEED.
    pairs.sort(key=lambda p: (-p["score"], p["a"], p["b"]))

    query_files: dict[str, set[str]] = defaultdict(set)
    for path, scan in scans.items():
        for text in scan["_queries"]:
            query_files[text].add(path)
    clusters = sorted(
        ({"query": q, "files": len(fs), "families": sorted({files[f]["family"] for f in fs}),
          "sample_files": sorted(fs)[:8]} for q, fs in query_files.items() if len(fs) >= 4),
        key=lambda c: (-c["files"], c["query"]),
    )

    fn_files: dict[str, set[str]] = defaultdict(set)
    for path, scan in scans.items():
        for fn in scan["functions"]:
            fn_files[fn].add(path)
    same_names = sorted(
        ({"function": fn, "files": sorted(fs)} for fn, fs in fn_files.items() if len(fs) >= 2),
        key=lambda d: (-len(d["files"]), d["function"]),
    )
    return {"pairs": pairs[:80], "query_clusters": clusters[:60], "same_function_names": same_names[:60]}


# ------------------------------------------------------------------ build ---


def cmd_build(args) -> int:
    collection = json.loads(_read_gz(COLLECTION))
    if collection["pytest_exit_code"] != 0 or collection["collect_reports"]:
        raise ValueError("inventory cannot build from an incomplete pytest collection")
    durations = load_junit(ROOT / args.junit) if args.junit else {}
    # rglob: desde F2.5 hay suites en subdirectorios (tests/product_contract, tests/contracts).
    scans = {p.relative_to(ROOT).as_posix(): scan_file(p) for p in sorted((ROOT / "tests").rglob("test_*.py"))}

    by_path: dict[str, list[dict]] = defaultdict(list)
    items_out = []
    for item in collection["items"]:
        scan = scans.get(item["path"])
        stem = Path(item["path"]).stem
        fn = scan["functions"].get(item["function"]) if scan else None
        cls, confidence, evidence = classify(stem, scan, fn) if scan else ("UNKNOWN", "none", ["outside_tests_root"])
        out = {
            **item,
            "domain": family_of(stem),
            "estimated_class": cls,
            "confidence": confidence,
            "class_evidence": evidence,
            "hypothesis": bool(fn and fn["hypothesis"]),
            "imports_app": scan["imports_app"] if scan else [],
            "uses_workflow": bool(fn and (fn["workflow_call"] or fn["workflow_fixture"])),
            "network_dependency": "live_opt_in" if scan and scan["live_opt_in"] else (
                "blocked" if scan and scan["blocks_network"] else "unknown_or_mocked"),
            "duration_hist_s": durations.get(item["nodeid"]),
        }
        items_out.append(out)
        by_path[item["path"]].append(out)

    files: dict[str, dict] = {}
    for rel, scan in scans.items():
        items = by_path.get(rel, [])
        mix = Counter(it["estimated_class"] for it in items)
        files[rel] = {
            "family": family_of(Path(rel).stem),
            "primary_class": mix.most_common(1)[0][0] if mix else "UNCOLLECTED",
            "class_mix": dict(mix),
            "confidence_mix": dict(Counter(it["confidence"] for it in items)),
            "nodeids": len(items),
            "parametrized_nodeids": sum(1 for it in items if it["param_id"]),
            "test_functions": len(scan["functions"]),
            "workflow_nodeids": sum(1 for it in items if it["uses_workflow"]),
            "hypothesis_functions": sum(1 for f in scan["functions"].values() if f["hypothesis"]),
            "static_skips": sum(1 for it in items if it["static_skip"]),
            "static_xfails": sum(1 for it in items if it["static_xfail"] and "error" not in it["static_xfail"]),
            "runtime_skip_functions": sorted(n for n, f in scan["functions"].items() if f["runtime_skip"]),
            "duration_hist_s": round(sum(it["duration_hist_s"] or 0 for it in items), 2),
            "fixtures_top": [f for f, _ in Counter(f for it in items for f in it["fixtures"]
                                                   if f not in AUTOUSE).most_common(6)],
            "distinct_queries": len(scan["_queries"]),
            **{k: v for k, v in scan.items() if not k.startswith("_") and k != "functions"},
        }

    dup = duplication(scans, files)
    inv_files: dict[str, list[str]] = defaultdict(list)
    for rel, f in files.items():
        for inv in f["invariants"]:
            inv_files[inv].append(rel)

    summary = {
        "head": collection["head"],
        "worktree_dirty_at_collection": collection.get("worktree_dirty", False),
        "python": collection["python"],
        "pytest": collection.get("pytest"),
        "total_test_files": len(files),
        "total_nodeids": len(items_out),
        "collect_errors": [r for r in collection["collect_reports"] if r["outcome"] == "failed"],
        "static_skips": sum(1 for it in items_out if it["static_skip"]),
        "static_xfails": sum(1 for it in items_out if it["static_xfail"] and "error" not in it["static_xfail"]),
        "static_xfails_strict": sum(1 for it in items_out if it["static_xfail"] and it["static_xfail"].get("strict")),
        "live_opt_in_nodeids": sum(1 for it in items_out if it["network_dependency"] == "live_opt_in"),
        "class_by_nodeid": dict(Counter(it["estimated_class"] for it in items_out).most_common()),
        "confidence_by_nodeid": dict(Counter(it["confidence"] for it in items_out).most_common()),
        "class_by_file": dict(Counter(f["primary_class"] for f in files.values()).most_common()),
        "family_by_nodeid": dict(Counter(it["domain"] for it in items_out).most_common()),
        "family_by_file": dict(Counter(f["family"] for f in files.values()).most_common()),
        "markers": dict(Counter(m for it in items_out for m in it["markers"]).most_common()),
        "junit": {"path": args.junit, "head": args.junit_head,
                  "matched_nodeids": sum(1 for it in items_out if it["duration_hist_s"] is not None)},
        "invariant_mentions_in_tests": {k: len(v) for k, v in sorted(inv_files.items(), key=lambda x: -len(x[1]))},
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    INVENTORY.write_text(json.dumps({"summary": summary, "files": files, "duplication": dup,
                                     "invariant_files": inv_files, "items_path": ITEMS.name},
                                    ensure_ascii=False, indent=1, sort_keys=True))
    _write_gz(ITEMS, "".join(json.dumps(it, ensure_ascii=False, sort_keys=True) + "\n" for it in items_out))
    print(json.dumps(summary, ensure_ascii=False, indent=1)[:5000])
    return 0


# ----------------------------------------------------------------- render ---


def _makefile_targets() -> list[str]:
    text = (ROOT / "Makefile").read_text()
    return re.findall(r"^([a-zA-Z][\w-]*):", text, re.M)


def cmd_render(_args) -> int:
    inv = json.loads(INVENTORY.read_text())
    s, files, dup = inv["summary"], inv["files"], inv["duplication"]
    status = _git("status", "--short", "--untracked-files=no")
    baseline = {
        "baseline_head": s["head"],
        "collection_kind": "working_tree",
        "worktree_dirty_at_collection": s["worktree_dirty_at_collection"],
        "branch": _git("rev-parse", "--abbrev-ref", "HEAD"),
        "tracked_status_clean": status == "",
        "python": s["python"], "pytest": s["pytest"],
        "total_test_files": s["total_test_files"], "total_nodeids": s["total_nodeids"],
        "collect_errors": len(s["collect_errors"]),
        "static_skips": s["static_skips"], "static_xfails": s["static_xfails"],
        "static_xfails_strict": s["static_xfails_strict"],
        "live_opt_in_nodeids": s["live_opt_in_nodeids"],
        "markers": s["markers"],
        "makefile_targets": _makefile_targets(),
        "ci": {"workflow": ".github/workflows/ci.yml", "python": "3.12", "command": "pytest tests/ -q --tb=short"},
        "full_suite_duration": {"measured_on_baseline_head": False,
                                "historical": s["junit"]},
    }
    BASELINE.write_text(json.dumps(baseline, ensure_ascii=False, indent=1))

    lines = [
        "# TEST_INVENTORY_V1 — inventario automático",
        "",
        "> Generado por `scripts/audits/test_architecture_inventory.py render`. **Heurística, no autoridad.**",
        f"> `BASELINE_HEAD = {s['head'][:7]}` · {s['total_test_files']} archivos · {s['total_nodeids']} nodeids.",
        "> La adjudicación humana/agente vive en `TEST_ARCHITECTURE_V1.md`; nada aquí autoriza retiros.",
        "",
        "## Clases por nodeid",
        "",
        "| Clase | nodeids | archivos (clase primaria) |",
        "|---|---:|---:|",
    ]
    for cls, n in s["class_by_nodeid"].items():
        lines.append(f"| {cls} | {n} | {s['class_by_file'].get(cls, 0)} |")
    lines += ["", "Confianza: " + ", ".join(f"{k}={v}" for k, v in s["confidence_by_nodeid"].items()), "",
              "## Familias", "", "| Familia | archivos | nodeids | workflow nodeids | tiempo hist. (s) |",
              "|---|---:|---:|---:|---:|"]
    fam = defaultdict(lambda: [0, 0, 0, 0.0])
    for f in files.values():
        row = fam[f["family"]]
        row[0] += 1
        row[1] += f["nodeids"]
        row[2] += f["workflow_nodeids"]
        row[3] += f["duration_hist_s"]
    for name, (nf, nn, nw, t) in sorted(fam.items(), key=lambda x: -x[1][1]):
        lines.append(f"| {name} | {nf} | {nn} | {nw} | {t:.1f} |")

    lines += ["", "## Top candidatos de duplicación (score estructural, sin adjudicar)", "",
              "| score | A | B | queries compartidas | clases | workflow A/B |", "|---:|---|---|---:|---|---|"]
    for p in dup["pairs"][:40]:
        lines.append(f"| {p['score']} | `{p['a'][6:]}` | `{p['b'][6:]}` | {p['shared_queries']} | "
                     f"{'/'.join(p['classes'])} | {'/'.join('sí' if w else 'no' for w in p['workflow'])} |")

    lines += ["", "## Consultas literales repetidas en ≥4 archivos", "", "| archivos | consulta | familias |",
              "|---:|---|---|"]
    for c in dup["query_clusters"][:30]:
        lines.append(f"| {c['files']} | `{c['query']}` | {', '.join(c['families'])} |")

    lines += ["", "## Por archivo", "",
              "| archivo | familia | clase primaria | mezcla | nodeids | param | hyp fn | wf | skip/xfail | t hist (s) | evidencia |",
              "|---|---|---|---|---:|---:|---:|---:|---|---:|---|"]
    for rel, f in sorted(files.items(), key=lambda x: (x[1]["family"], x[0])):
        mix = ", ".join(f"{k[:4]}={v}" for k, v in f["class_mix"].items())
        lines.append(
            f"| `{rel[6:]}` | {f['family']} | {f['primary_class']} | {mix} | {f['nodeids']} | "
            f"{f['parametrized_nodeids']} | {f['hypothesis_functions']} | {f['workflow_nodeids']} | "
            f"{f['static_skips']}/{f['static_xfails']}{'+rt' if f['runtime_skip_functions'] else ''} | "
            f"{f['duration_hist_s']:.1f} | {f['module_doc_head'][:80].replace('|', '/')} |")
    INVENTORY_MD.parent.mkdir(parents=True, exist_ok=True)
    INVENTORY_MD.write_text("\n".join(lines) + "\n")
    print(f"wrote {BASELINE.relative_to(ROOT)} and {INVENTORY_MD.relative_to(ROOT)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("collect").set_defaults(fn=cmd_collect)
    build = sub.add_parser("build")
    build.add_argument("--junit", help="JUnit XML histórico (ruta relativa al repo) para duraciones")
    build.add_argument("--junit-head", help="HEAD sobre el que se generó el JUnit")
    build.set_defaults(fn=cmd_build)
    sub.add_parser("render").set_defaults(fn=cmd_render)
    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
