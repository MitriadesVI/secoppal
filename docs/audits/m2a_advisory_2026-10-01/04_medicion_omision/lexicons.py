"""Load function-word lexicons from source files by AST (no import, no execution of the repos)."""
import ast, sys
from pathlib import Path

D = Path("/Users/rodrigoortiz/Documents")
SOURCES = [  # (label, file, variable)
    ("STOPWORDS", D / "secoppal-m1/app/data/stopwords.py", "STOPWORDS"),
    ("RESIDUAL_FUNCTION_WORDS", D / "secoppal-m1/app/core/accreditation/residual_language.py", "RESIDUAL_FUNCTION_WORDS"),
    ("time_coverage._GLUE", D / "secoppal-m1-c12/app/core/foundation_time_coverage.py", "_GLUE"),
    ("time_coverage._TRAILING_GLUE", D / "secoppal-m1-c12/app/core/foundation_time_coverage.py", "_TRAILING_GLUE"),
    ("buyer._FUNCTION_WORDS", D / "secoppal-m1-dbuyer/app/core/host_authorities/buyer.py", "_FUNCTION_WORDS"),
    ("legacy_shadow._FUNCTION_WORDS", D / "secoppal-m1/app/core/legacy_shadow.py", "_FUNCTION_WORDS"),
    ("fuzzy_identity_guard.FILLER_TOKENS", D / "secoppal-m1/app/core/fuzzy_identity_guard.py", "FILLER_TOKENS"),
    ("dialogue_policy._COURTESY_TOKENS", D / "secoppal-m1/app/core/dialogue_policy.py", "_COURTESY_TOKENS"),
    ("dialogue_policy._FILLER_PREFIXES", D / "secoppal-m1/app/core/dialogue_policy.py", "_FILLER_PREFIXES"),
    ("control_language._LEFT_GLUE", D / "secoppal-m1/app/core/control_language.py", "_LEFT_GLUE"),
    ("control_language._LEFT_FILLER", D / "secoppal-m1/app/core/control_language.py", "_LEFT_FILLER"),
    ("control_language._CONTROL_TURN_FILLER", D / "secoppal-m1/app/core/control_language.py", "_CONTROL_TURN_FILLER"),
    ("generic_buyer._ARTICLES", D / "secoppal-m1/app/core/generic_buyer.py", "_ARTICLES"),
    ("actor_intent._LEADING_STOPWORDS", D / "secoppal-m1/app/core/actor_intent.py", "_LEADING_STOPWORDS"),
    ("entity_profile.turn._PROFILE_NOISE", D / "secoppal-m1/app/core/entity_profile/turn.py", "_PROFILE_NOISE"),
    ("source_occurrence._HEAD_FILLERS", D / "secoppal-m1/app/core/source_occurrence.py", "_HEAD_FILLERS"),
    ("spell_correction.PROTECTED_FUNCTIONAL_VOCABULARY", D / "secoppal-m1/app/utils/spell_correction.py", "PROTECTED_FUNCTIONAL_VOCABULARY"),
    ("actor_arbitration._NAME_CONNECTORS", D / "secoppal-m1/app/core/actor_arbitration.py", "_NAME_CONNECTORS"),
    ("actor_conversation._SEARCH_LEAD_ARTICLES", D / "secoppal-m1/app/core/actor_conversation.py", "_SEARCH_LEAD_ARTICLES"),
]


class _Fail(Exception):
    pass


def _ev(node, env):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
        out = []
        for e in node.elts:
            if isinstance(e, ast.Starred):
                out.extend(_ev(e.value, env))
            else:
                out.append(_ev(e, env))
        return set(out) if isinstance(node, ast.Set) else out
    if isinstance(node, ast.Dict):
        return {_ev(k, env): None for k in node.keys}
    if isinstance(node, ast.Name):
        if node.id in env:
            return env[node.id]
        raise _Fail(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return set(_ev(node.left, env)) | set(_ev(node.right, env))
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"frozenset", "set", "tuple", "list"}:
        return set(_ev(node.args[0], env)) if node.args else set()
    raise _Fail(type(node).__name__)


def load(path, var):
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    env = {}
    for node in tree.body:
        targets = []
        if isinstance(node, ast.Assign):
            targets, value = [t for t in node.targets if isinstance(t, ast.Name)], node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
            targets, value = [node.target], node.value
        else:
            continue
        try:
            val = _ev(value, env)
        except _Fail:
            continue
        for t in targets:
            env[t.id] = val
    if var not in env:
        raise KeyError(f"{var} not evaluable in {path}")
    return {str(x) for x in env[var] if isinstance(x, str)}


if __name__ == "__main__":
    sys.path.insert(0, "/Users/rodrigoortiz/Documents/secoppal-m1")
    from app.utils.money import normalize_text
    for label, p, v in SOURCES:
        try:
            s = load(p, v)
            multi = sum(1 for x in s if " " in x)
            print(f"{label:55s} n={len(s):4d} multiword={multi}")
        except Exception as e:
            print(label, "FAIL", e)
