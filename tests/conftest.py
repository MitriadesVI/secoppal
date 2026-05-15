from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def isolate_feedback_jsonl(monkeypatch, tmp_path):
    """Route workflow feedback traces to per-test temp files, never data/feedback.jsonl."""
    monkeypatch.setenv("SECOP_FEEDBACK_PATH", str(tmp_path / "feedback.jsonl"))
