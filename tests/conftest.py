from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def isolate_runtime_persistence(monkeypatch, tmp_path):
    """Toda persistencia de tests vive en tmp_path, nunca en rutas del proyecto."""
    project_sidecars = set((ROOT / "data" / "conversations").glob("*.pagination.json"))
    monkeypatch.setenv("SECOP_FEEDBACK_PATH", str(tmp_path / "feedback.jsonl"))
    monkeypatch.setenv("SECOP_FEEDBACK_DB_PATH", str(tmp_path / "secoppal.db"))
    monkeypatch.setenv("CONVERSATION_STORE_DIR", str(tmp_path / "conversations"))
    yield
    assert set((ROOT / "data" / "conversations").glob("*.pagination.json")) == project_sidecars
