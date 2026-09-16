"""A focused supplier view never answers with a bare header, and never blames the source unfairly."""
from app.core.entity_profile.models import SourceCall
from app.core.entity_profile.sections import _warnings


def _call(call_id, status):
    return SourceCall(call_id, "suppliers", "jbjy-vk9h", "aggregate", status, 0, 0.0, "", attempted=False)


def test_budget_exhaustion_is_not_reported_as_source_silence():
    calls = {"supplier_count": _call("supplier_count", "BUDGET_EXHAUSTED"),
             "supplier_value": _call("supplier_value", "BUDGET_EXHAUSTED")}
    (warning,) = _warnings("los proveedores", ("supplier_count", "supplier_value"), calls)
    assert "no respondió" not in warning
    assert "no es una ausencia" in warning


def test_a_real_timeout_still_names_the_source():
    calls = {"supplier_count": _call("supplier_count", "TIMEOUT")}
    (warning,) = _warnings("los proveedores", ("supplier_count",), calls)
    assert "la fuente no respondió" in warning
