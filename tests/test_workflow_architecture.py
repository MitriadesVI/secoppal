"""Architectural lock-in for the workflow refactor.

These tests are the structural counterpart to the behavioral golden
tests. They protect the import topology established by Paso 5 so future
contributors don't accidentally re-couple ``workflow/`` back to
``orchestrator.py``.

Two invariants are enforced:

1. ``workflow/graph.py`` never imports from ``app.core.orchestrator``.
   The whole point of 5F was to break that edge.

2. No module under ``workflow/actions/`` imports orchestrator either.
   Actions are the leaves of the dependency tree — they must be
   importable without dragging in the workflow facade.

If you add a new file under ``workflow/`` and one of these tests starts
failing, the cycle is the bug, not the test.
"""
from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (_REPO_ROOT / rel).read_text()


def test_graph_does_not_import_from_orchestrator():
    """graph.py is the composition root for the Burr application — it must
    sit BELOW orchestrator in the dependency tree, not above it."""
    src = _read("app/core/workflow/graph.py")
    assert "from app.core.orchestrator" not in src, (
        "graph.py imports from orchestrator — recreates the cycle Paso 5F broke"
    )
    assert "import app.core.orchestrator" not in src, (
        "graph.py imports orchestrator module — same cycle, different syntax"
    )


def test_actions_do_not_import_orchestrator():
    """Action modules must be leaves: importable without touching the
    workflow facade. Pulling orchestrator into an action recreates the
    coupling Paso 5 deliberately removed."""
    actions_dir = _REPO_ROOT / "app" / "core" / "workflow" / "actions"
    offenders: list[str] = []
    for py in actions_dir.glob("*.py"):
        if py.name == "__init__.py":
            continue
        src = py.read_text()
        if "app.core.orchestrator" in src:
            offenders.append(py.name)
    assert not offenders, (
        f"These action modules import from orchestrator: {offenders}. "
        f"Actions must be leaves in the dependency tree."
    )


def test_handlers_do_not_import_orchestrator():
    """Handler modules must be leaves too: SecopalWorkflow.run_query
    depends on handlers, never the other way around. A handler that
    imports orchestrator (or even SecopalWorkflow) is a cycle waiting
    to happen."""
    handlers_dir = _REPO_ROOT / "app" / "core" / "workflow" / "handlers"
    offenders: list[str] = []
    for py in handlers_dir.glob("*.py"):
        if py.name == "__init__.py":
            continue
        src = py.read_text()
        if "app.core.orchestrator" in src:
            offenders.append(py.name)
    assert not offenders, (
        f"These handler modules import from orchestrator: {offenders}. "
        f"Handlers must be leaves in the dependency tree."
    )


def test_orchestrator_does_not_import_burr_actions():
    """After Paso 5 orchestrator.py contains no @action and no direct
    Burr imports — those moved to workflow/actions/ and workflow/graph.py.

    Reintroducing burr.core here means an action snuck back in.
    """
    src = _read("app/core/orchestrator.py")
    assert "@action" not in src, (
        "orchestrator.py contains @action — actions must live in workflow/actions/"
    )
    assert "from burr.core" not in src, (
        "orchestrator.py imports from burr.core — Burr internals belong to "
        "workflow/graph.py and workflow/actions/"
    )
