"""INV-AUDIT-RUNNER-NO-PRODUCTION-WRITES.

El runner de auditoría HCA es offline y de solo lectura: correrlo no puede
tocar los stores reales del proyecto. La prueba central no mockea las rutas
—eso escondería justo lo que hay que medir— sino que ejecuta el runner en un
subproceso con un ``data/`` centinela como directorio de trabajo y comprueba
que quedó intacto.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.audits.audit_isolation import (
    WRITABLE_ENV_STORES,
    isolated_runtime_stores,
)

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "audits" / "run_conversation_holistic_audit.py"

MINI_CORPUS = """
cases:
  - id: ISO_SMOKE_1
    category: isolation
    turns:
      - q: "contratos de aseo en Barranquilla en 2026"
        expect: execute
"""


# ── Contrato del sandbox ────────────────────────────────────────────────────

def test_isolation_redirects_and_cleans_up():
    with isolated_runtime_stores() as stores:
        root = stores.root
        assert root.is_dir()
        for name in WRITABLE_ENV_STORES:
            assert stores.contains(os.environ[name]), name
        for value in stores.settings_overrides.values():
            assert stores.contains(value)
        assert stores.contains(stores.conversation_dir)
    assert not root.exists()


def test_isolation_cleans_up_on_exception():
    captured: list[Path] = []
    with pytest.raises(RuntimeError):
        with isolated_runtime_stores() as stores:
            captured.append(stores.root)
            raise RuntimeError("boom")
    assert captured and not captured[0].exists()


def test_isolation_restores_previous_environment(monkeypatch):
    monkeypatch.setenv("SECOP_FEEDBACK_PATH", "/previo/feedback.jsonl")
    monkeypatch.delenv("MODALITY_FOLLOWUP_TELEMETRY_PATH", raising=False)
    with isolated_runtime_stores():
        assert os.environ["SECOP_FEEDBACK_PATH"] != "/previo/feedback.jsonl"
        assert "MODALITY_FOLLOWUP_TELEMETRY_PATH" in os.environ
    assert os.environ["SECOP_FEEDBACK_PATH"] == "/previo/feedback.jsonl"
    assert "MODALITY_FOLLOWUP_TELEMETRY_PATH" not in os.environ


def test_isolation_rejects_paths_outside_sandbox():
    with isolated_runtime_stores() as stores:
        assert not stores.contains(ROOT / "data" / "feedback.jsonl")
        assert not stores.contains(None)


# ── Corrida real contra un data/ centinela ──────────────────────────────────

def _sentinel_run(tmp_path: Path) -> tuple[subprocess.CompletedProcess, Path, Path]:
    """Ejecuta el runner con cwd=tmp_path y un ``data/`` centinela poblado.

    El cwd importa: los defaults de persistencia de ``Settings`` son rutas
    RELATIVAS (``data/feedback.jsonl``). Con el cwd en un temporal, cualquier
    escritura no aislada aterriza en el centinela y es visible, sin poner en
    riesgo el ``data/`` del checkout.
    """
    sentinel = tmp_path / "data"
    (sentinel / "conversations").mkdir(parents=True)
    (sentinel / "feedback.jsonl").write_text("SENTINEL\n", encoding="utf-8")
    out_dir = tmp_path / "audit_out"

    corpus = tmp_path / "mini_corpus.yaml"
    corpus.write_text(MINI_CORPUS, encoding="utf-8")

    env = dict(os.environ)
    # Sin esto el subproceso heredaría el aislamiento de la propia suite y la
    # prueba mediría la fixture, no el runner.
    for name in WRITABLE_ENV_STORES:
        env.pop(name, None)
    env.pop("PYTEST_CURRENT_TEST", None)
    # Recursos de solo lectura: rutas absolutas porque el cwd ya no es el repo.
    env["SECOP_ALIAS_DB_PATH"] = str(ROOT / "app" / "data" / "aliases_db.json")
    env["SECOP_ACTOR_ALIAS_INDEX_PATH"] = str(tmp_path / "no_such_index.sqlite")
    env["PYTHONPATH"] = str(ROOT)

    proc = subprocess.run(
        [sys.executable, str(RUNNER), "--corpus", str(corpus), "--out", str(out_dir)],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=600,
    )
    return proc, sentinel, out_dir


def test_runner_writes_nothing_into_repo_data(tmp_path):
    proc, sentinel, out_dir = _sentinel_run(tmp_path)
    assert proc.returncode == 0, proc.stderr[-3000:]

    # La corrida tiene que haber ejecutado de verdad; si no, la prueba de
    # no-escritura sería vacua.
    verdicts = json.loads((out_dir / "verdicts.json").read_text(encoding="utf-8"))
    assert verdicts["cases"] == 1
    assert verdicts["error"] == 0, verdicts["verdicts"]

    assert (sentinel / "feedback.jsonl").read_text(encoding="utf-8") == "SENTINEL\n"
    assert list((sentinel / "conversations").iterdir()) == []
    assert sorted(p.name for p in sentinel.iterdir()) == [
        "conversations", "feedback.jsonl",
    ]
    # Ninguna escritura fuera de data/ tampoco (salvo la salida pedida).
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "audit_out", "data", "mini_corpus.yaml",
    ]


def test_runner_reports_every_writable_store_inside_sandbox(tmp_path):
    proc, _, out_dir = _sentinel_run(tmp_path)
    assert proc.returncode == 0, proc.stderr[-3000:]

    isolation = json.loads((out_dir / "isolation.json").read_text(encoding="utf-8"))
    assert isolation["outside_sandbox"] == []
    sandbox = Path(isolation["stores"]["sandbox_root"])
    for name in ("feedback_jsonl", "feedback_db", "conversation_dir",
                 "radar_db", "modality_telemetry"):
        assert Path(isolation["stores"][name]).is_relative_to(sandbox), name
    # El sandbox de stores se borra al terminar; la salida sobrevive.
    assert not sandbox.exists()
    assert (out_dir / "results.jsonl").exists()
