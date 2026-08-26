"""G8 discipline — LoC caps for architectural modules."""

from pathlib import Path


def _count_loc(path: Path) -> int:
    return sum(
        1 for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    )


def test_query_plan_under_300_loc():
    # NEXT_TOUCH_RULE (integration review HCA-R5): el módulo quedó en 299/300.
    # Cualquier nueva modificación funcional de query_plan.py debe extraer
    # primero una responsabilidad suficiente para recuperar margen — como hizo
    # R5 con app/core/temporal_gate.py. El tope de 300 no se sube.
    loc = _count_loc(Path("app/core/query_plan.py"))
    assert loc < 300, f"query_plan.py grew to {loc} LoC — refactor before merging"


def test_dialogue_policy_under_400_loc():
    loc = _count_loc(Path("app/core/dialogue_policy.py"))
    assert loc < 400, f"dialogue_policy.py grew to {loc} LoC — refactor before merging"
