"""ADR-102 boundaries of the promoted Foundation runtime.

These are static and import-level guarantees: one profile, one switch, no
historical tender complement, no silent universe default.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.config import Settings

APP = Path(__file__).resolve().parents[1] / "app"
SOURCES = {path: path.read_text(encoding="utf-8") for path in APP.rglob("*.py")}


def _settings(**overrides):
    return Settings(deepseek_api_key=None, secop_app_token=None, _env_file=None, **overrides)


def test_the_historical_tender_complement_is_not_importable():
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("app.core.foundation_tender_complement")


@pytest.mark.parametrize("symbol", ["foundation_tender_complement", "with_tender_complement",
                                    "HISTORICAL_TENDER_COMPLEMENT", "RequestComplement"])
def test_no_runtime_module_names_the_complement(symbol):
    assert [str(path) for path, text in SOURCES.items() if symbol in text] == []


def test_universe_kind_has_no_default_anywhere_in_the_runtime():
    pattern = re.compile(r"""(?:pop|get|setdefault)\(\s*["']universe_kind["']\s*,""")
    assert [str(path) for path, text in SOURCES.items() if pattern.search(text)] == []


def test_foundation_is_off_by_default_and_has_one_profile():
    fields = Settings.model_fields
    assert fields["secoppal_foundation_mode"].default == "legacy"
    assert "secoppal_foundation_universe_policy" not in fields
    assert "secoppal_foundation_producer" not in fields
    assert _settings().secoppal_foundation_mode == "legacy"
    assert _settings(secoppal_foundation_mode="foundation_v3").secoppal_foundation_mode == "foundation_v3"
    for partial in ("foundation_v3_h7_off", "h7", "B", "on", "true"):
        with pytest.raises(ValidationError):
            _settings(secoppal_foundation_mode=partial)


def test_the_runtime_has_no_switch_that_skips_h7():
    runtime = (APP / "core" / "foundation_runtime.py").read_text(encoding="utf-8")
    assert "universe_policy\", \"off\"" not in runtime and "== \"h7\"" not in runtime
    assert "host_universe(" in runtime
