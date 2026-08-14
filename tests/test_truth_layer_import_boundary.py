"""Import-safety and architectural boundary gates for dormant Truth Layer M1."""
from __future__ import annotations

import ast
import json
from pathlib import Path
import statistics
import subprocess
import sys


FORBIDDEN_TOKENS = (
    "soql",
    "modality",
    "workflow",
    "socrata",
    "warehouse",
    "streamlit",
    "feedback",
    "generic_enrichment",
)


def _cold_import_probe() -> dict:
    code = r'''import json, sys, time
started = time.perf_counter()
import app.core.truth_layer.models
elapsed_ms = (time.perf_counter() - started) * 1000
forbidden = sorted(
    name for name in sys.modules
    if any(token in name.casefold() for token in (
        "soql", "modality", "workflow", "socrata", "warehouse",
        "streamlit", "feedback", "generic_enrichment",
    ))
)
print(json.dumps({"elapsed_ms": elapsed_ms, "forbidden": forbidden}))
'''
    completed = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def test_models_cold_import_has_zero_forbidden_transitive_imports():
    probe = _cold_import_probe()
    assert probe["forbidden"] == []


def test_cold_import_median_is_below_observational_ceiling():
    samples = [_cold_import_probe()["elapsed_ms"] for _ in range(5)]
    assert statistics.median(samples) < 100, samples


def test_foundation_modules_only_import_stdlib_or_truth_layer_modules():
    root = Path("app/core/truth_layer")
    violations: list[tuple[str, str]] = []
    for path in sorted(root.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            for module in modules:
                if any(token in module.casefold() for token in FORBIDDEN_TOKENS):
                    violations.append((str(path), module))
    assert violations == []


def test_experimental_runtime_files_are_absent_from_product_package():
    root = Path("app/core/truth_layer")
    forbidden_files = {
        "runtime_binding.py",
        "shadow_adapter.py",
        "opportunity_composition.py",
        "opportunity_policy.py",
    }
    assert forbidden_files.isdisjoint(path.name for path in root.glob("*.py"))
