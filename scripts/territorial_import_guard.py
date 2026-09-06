"""Pure syntax guard for Territorial Knowledge's SHADOW_NO_RUNTIME boundary."""
from __future__ import annotations

import ast
from importlib.util import resolve_name

TARGET = "app.core.territorial_knowledge"


def _is_target(name: str) -> bool:
    return name == TARGET or name.startswith(TARGET + ".")


def _from_module(node: ast.ImportFrom, package: str) -> str:
    name = "." * node.level + (node.module or "")
    return resolve_name(name, package) if node.level else name


def _literal_argument(node: ast.Call, position: int, keyword: str) -> str | None:
    value = next((item.value for item in node.keywords if item.arg == keyword), None)
    if len(node.args) > position:
        value = node.args[position]
    return value.value if isinstance(value, ast.Constant) and isinstance(value.value, str) else None


def source_imports_territorial_knowledge(source: str, *, package: str = "app.core") -> bool:
    """Find static imports and explicit literal dynamic loads, never data mentions.

    ``package`` locates relative static imports (the scanner supplies the file's
    package). Importlib aliases are recognized conservatively across the AST;
    arbitrary expressions, rebinding and runtime control flow are not evaluated.
    Invalid Python or unresolvable static relative imports raise, never pass.
    """
    nodes = list(ast.walk(ast.parse(source)))
    importlib_names = {"importlib"}
    loaders = {"__import__"}
    import_module_names: set[str] = set()
    for node in nodes:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_target(alias.name):
                    return True
                if alias.name == "importlib":
                    importlib_names.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom):
            module = _from_module(node, package)
            if _is_target(module) or any(_is_target(f"{module}.{a.name}") for a in node.names):
                return True
            for alias in node.names:
                if module == "importlib" and alias.name == "import_module":
                    import_module_names.add(alias.asname or alias.name)
                if module == "builtins" and alias.name == "__import__":
                    loaders.add(alias.asname or alias.name)

    for node in nodes:
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        is_import_module = (
            isinstance(function, ast.Attribute)
            and isinstance(function.value, ast.Name)
            and function.value.id in importlib_names
            and function.attr == "import_module"
        ) or (isinstance(function, ast.Name) and function.id in import_module_names)
        is_builtin = isinstance(function, ast.Name) and function.id in loaders
        if not (is_import_module or is_builtin):
            continue
        target = _literal_argument(node, 0, "name")
        if target is None:
            continue
        if is_import_module and target.startswith("."):
            literal_package = _literal_argument(node, 1, "package")
            if literal_package is None:
                continue
            target = resolve_name(target, literal_package)
        if _is_target(target):
            return True
    return False
