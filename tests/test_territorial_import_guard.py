"""Syntax and mutation regressions for the single territorial runtime guard."""
import ast

from hypothesis import given, settings, strategies as st
import pytest

from scripts import build_territorial_knowledge_v0 as builder
from scripts.territorial_import_guard import source_imports_territorial_knowledge as imports_knowledge


@pytest.mark.parametrize("source", [
    "import app.core.territorial_knowledge",
    "import app.core.territorial_knowledge as knowledge",
    "import os, app.core.territorial_knowledge",
    "from app.core import territorial_knowledge",
    "from app.core import (other, territorial_knowledge as knowledge)",
    "from app.core.territorial_knowledge import load",
    "from app.core.territorial_knowledge import *",
    "from app.core.territorial_knowledge import (\n    load,\n)",
    "from . import territorial_knowledge",
    "from .territorial_knowledge import load",
    "from ..core import territorial_knowledge",
    'importlib.import_module("app.core.territorial_knowledge")',
    'importlib.import_module(name="app.core.territorial_knowledge")',
    'import importlib as il\nil.import_module("app.core.territorial_knowledge")',
    'from importlib import import_module\nimport_module("app.core.territorial_knowledge")',
    'from importlib import import_module as load\nload("app.core.territorial_knowledge")',
    'importlib.import_module(".territorial_knowledge", "app.core")',
    'importlib.import_module(".territorial_knowledge", package="app.core")',
    '__import__("app.core.territorial_knowledge")',
    '__import__(name="app.core.territorial_knowledge")',
    'from builtins import __import__ as load\nload("app.core.territorial_knowledge")',
    "def lazy():\n    from app.core.territorial_knowledge import load",
    "if False:\n    import app.core.territorial_knowledge",
    "import app.core.territorial_knowledge.child",
])
def test_real_import_syntax_is_consumption(source):
    assert imports_knowledge(source)


@pytest.mark.parametrize("source", [
    '"app/core/territorial_knowledge.py:Institution.institution_id"',
    '"app/data/territorial_knowledge_v0.json:DIVIPOLA"',
    'AUTHORITY = "app.core.territorial_knowledge"',
    '"""Docstring: territorial_knowledge; import app.core.territorial_knowledge"""',
    "# territorial_knowledge: from app.core.territorial_knowledge import load",
    'PROVENANCE = "import app.core.territorial_knowledge"',
    'AUTHORITY = {"path": "app.core.territorial_knowledge", "sha": "c79f846"}',
    'def description():\n    """importlib.import_module("app.core.territorial_knowledge")"""',
    "import app.core.territorial_knowledge_helpers",
    "from other import territorial_knowledge",
    "from other.territorial_knowledge import load",
    'other.import_module("app.core.territorial_knowledge")',
    'importlib.import_module("app.core.territorial_knowledge_helpers")',
    '__import__("app.data.territorial_knowledge_v0")',
    'importlib.import_module(".territorial_knowledge", "other")',
    'importlib.import_module(".territorial_knowledge", package=unknown)',
    "importlib.import_module(variable_target)",
    "__import__(variable_target)",
    "from app.core import other",
    "",
])
def test_data_and_unrelated_imports_are_not_consumption(source):
    assert not imports_knowledge(source)


def test_relative_static_imports_use_the_consumers_package():
    source = "from . import territorial_knowledge"
    assert imports_knowledge(source, package="app.core")
    assert not imports_knowledge(source, package="app.unrelated")
    assert imports_knowledge("from .. import territorial_knowledge", package="app.core.child")


@pytest.mark.parametrize("source", [
    "import app.core.territorial_knowledge as",
    '"unterminated territorial_knowledge',
    "def invalid(:",
])
def test_invalid_python_never_silently_passes(source):
    with pytest.raises(SyntaxError):
        imports_knowledge(source)


@settings(max_examples=100, derandomize=True)
@given(value=st.text(max_size=300))
def test_arbitrary_serialized_provenance_cannot_become_an_import(value):
    provenance = repr("app.core.territorial_knowledge:" + value)
    assert not imports_knowledge(f"{provenance}\nAUTHORITY = {provenance}")


@settings(max_examples=100, derandomize=True)
@given(
    alias=st.from_regex(r"alias_[a-z]{1,16}", fullmatch=True),
    form=st.sampled_from([
        "import app.core.territorial_knowledge as {alias}",
        "from app.core import (\n    territorial_knowledge as {alias},\n)",
        "from app.core.territorial_knowledge import load as {alias}",
        'import importlib as {alias}\n{alias}.import_module("app.core.territorial_knowledge")',
        'from importlib import import_module as {alias}\n{alias}("app.core.territorial_knowledge")',
    ]),
    wrapper=st.sampled_from(["", "def lazy():", "if __name__ == '__main__':"]),
    data=st.text(max_size=100),
)
def test_import_detection_survives_aliases_nesting_formatting_and_data(alias, form, wrapper, data):
    statement = form.format(alias=alias)
    if wrapper:
        statement = wrapper + "\n" + "\n".join("    " + line for line in statement.splitlines())
    source = f"AUTHORITY = {data!r}\n{statement}\n"
    assert imports_knowledge(source)
    assert imports_knowledge(ast.unparse(ast.parse(source)))


@pytest.mark.parametrize("statement", [
    "import app.core.territorial_knowledge",
    'importlib.import_module("app.core.territorial_knowledge")',
    "from . import territorial_knowledge",
])
def test_procurement_is_not_allowlisted_and_builder_rejects_real_imports(tmp_path, monkeypatch, statement):
    core = tmp_path / "app/core"
    core.mkdir(parents=True)
    (tmp_path / "app/data").symlink_to(builder.ROOT / "app/data", target_is_directory=True)
    (tmp_path / "artifacts").symlink_to(builder.ROOT / "artifacts", target_is_directory=True)
    # The loader itself remains the only excluded module.
    (core / "territorial_knowledge.py").write_text(statement, encoding="utf-8")
    procurement = core / "procurement_relationships.py"
    provenance = 'AUTHORITY = "app/core/territorial_knowledge.py:Institution.institution_id"\n'
    procurement.write_text(provenance, encoding="utf-8")
    monkeypatch.setattr(builder, "ROOT", tmp_path)
    assert builder._runtime_imports() == []
    assert builder._build_payload()["gates"]["RUNTIME_IMPORTS"] == 0
    procurement.write_text(provenance + statement, encoding="utf-8")
    assert builder._runtime_imports() == ["app/core/procurement_relationships.py"]
    with pytest.raises(ValueError, match="territorial knowledge gates failed"):
        builder._build_payload()
