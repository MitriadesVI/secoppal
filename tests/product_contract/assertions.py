"""Oráculos estructurales del Product Contract (plan §8).

``evaluate`` devuelve la lista completa de violaciones en vez de detenerse en la
primera: un escenario rojo debe decir todo lo que rompió. Una clave de oráculo
desconocida es una violación (fallo cerrado), no una expectativa ignorada.
"""
from __future__ import annotations

import unicodedata
from collections.abc import Mapping, Sequence
from typing import Any

ORACLES = frozenset({
    "outcome", "outcome_reason", "route", "route_reason", "forbidden_routes",
    "needs_clarification", "params", "absent_params", "soql", "source_calls",
    "datasets_called", "results", "response", "followup", "lineage",
    "actor_unresolved",
})
NESTED_ORACLES = {
    "soql": {"contains", "not_contains"},
    "source_calls": {"exact", "min", "max"},
    "datasets_called": {"only", "includes"},
    "results": {"must_include_ids", "must_exclude_ids", "first_id", "max_displayed"},
    "response": {"contains", "not_contains"},
    "followup": {"preserves", "clears"},
    "lineage": {"raw_text", "min_events", "min_tombstones"},
}
ID_KEYS = ("id_contrato", "id_del_proceso", "referencia_del_contrato", "referencia_del_proceso")
_EMPTY = (None, "", [], {}, ())


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFD", str(value or ""))
    return "".join(c for c in text if not unicodedata.combining(c)).casefold()


def row_ids(result: Mapping) -> list[str]:
    """IDs visibles en orden, recorriendo grupos duales anidados."""
    found: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, Mapping):
            for key in ID_KEYS:
                if node.get(key) not in _EMPTY:
                    found.append(str(node[key]))
                    break
            for key in ("rows", "results", "groups", "children"):
                walk(node.get(key))
        elif isinstance(node, list):
            for child in node:
                walk(child)

    for key in ("results", "rows", "groups"):
        walk(result.get(key))
    return list(dict.fromkeys(found))


def evaluate(result: Mapping, source_calls: Sequence[Mapping], expect: Mapping,
             previous_params: Mapping | None = None, *,
             lineage_views: Sequence[Any] = ()) -> list[str]:
    violations = [f"unknown oracle {key!r}" for key in expect if key not in ORACLES]
    for key, allowed in NESTED_ORACLES.items():
        section = expect.get(key) or {}
        if isinstance(section, Mapping):
            violations.extend(f"unknown oracle {key}.{child}" for child in section if child not in allowed)
    params = result.get("resolved_params") or {}
    routes = {result.get("analytical_intent"), result.get("route_reason")} - {None, ""}

    def check(label: str, ok: bool, detail: str) -> None:
        if not ok:
            violations.append(f"{label}: {detail}")

    for key in ("outcome", "outcome_reason", "route_reason"):
        if key in expect:
            check(key, result.get(key) == expect[key], f"expected {expect[key]!r}, got {result.get(key)!r}")
    if "route" in expect:
        got = result.get("analytical_intent")
        check("route", got == expect["route"], f"expected analytical_intent {expect['route']!r}, got {got!r}")
    if "forbidden_routes" in expect:
        hit = routes & set(expect["forbidden_routes"])
        check("forbidden_routes", not hit, f"reached {sorted(hit)}")
    if "needs_clarification" in expect:
        got = bool(result.get("needs_clarification"))
        check("needs_clarification", got is bool(expect["needs_clarification"]), f"got {got}")
    if "actor_unresolved" in expect:
        scope = params.get("actor_scope") or {}
        pending = params.get("actor_pending") or {}
        wanted = bool(expect["actor_unresolved"])
        unresolved = (isinstance(scope, Mapping) and scope.get("requires_resolution") is True
                      and not scope.get("base_number") and not params.get("contratista")
                      and isinstance(pending, Mapping) and len(pending.get("candidates") or ()) >= 2)
        check("actor_unresolved", unresolved is wanted, f"got {unresolved}")

    for key, value in (expect.get("params") or {}).items():
        check(f"params.{key}", params.get(key) == value, f"expected {value!r}, got {params.get(key)!r}")
    for key in expect.get("absent_params") or ():
        check(f"absent_params.{key}", params.get(key) in _EMPTY, f"present as {params.get(key)!r}")

    soql = "\n".join(str(call.get("soql", "")) for call in source_calls)
    for fragment in (expect.get("soql") or {}).get("contains", ()):
        check("soql.contains", fragment in soql, repr(fragment))
    for fragment in (expect.get("soql") or {}).get("not_contains", ()):
        check("soql.not_contains", fragment not in soql, repr(fragment))

    calls = expect.get("source_calls") or {}
    count = len(source_calls)
    if "exact" in calls:
        check("source_calls.exact", count == calls["exact"], f"expected {calls['exact']}, got {count}")
    if "min" in calls:
        check("source_calls.min", count >= calls["min"], f"expected >= {calls['min']}, got {count}")
    if "max" in calls:
        check("source_calls.max", count <= calls["max"], f"expected <= {calls['max']}, got {count}")

    datasets = {str(call.get("dataset")) for call in source_calls}
    wanted = expect.get("datasets_called") or {}
    if "only" in wanted:
        check("datasets_called.only", datasets <= set(wanted["only"]), f"called {sorted(datasets)}")
    for dataset in wanted.get("includes", ()):
        check("datasets_called.includes", dataset in datasets, f"{dataset!r} not called")

    ids = row_ids(result)
    rows = expect.get("results") or {}
    for rid in rows.get("must_include_ids", ()):
        check("results.must_include_ids", rid in ids, repr(rid))
    for rid in rows.get("must_exclude_ids", ()):
        check("results.must_exclude_ids", rid not in ids, repr(rid))
    if "first_id" in rows:
        check("results.first_id", bool(ids) and ids[0] == rows["first_id"], f"got {ids[:1]}")
    if "max_displayed" in rows:
        check("results.max_displayed", len(ids) <= rows["max_displayed"], f"got {len(ids)}")

    text = normalize_text(result.get("response"))
    for fragment in (expect.get("response") or {}).get("contains", ()):
        check("response.contains", normalize_text(fragment) in text, repr(fragment))
    for fragment in (expect.get("response") or {}).get("not_contains", ()):
        check("response.not_contains", normalize_text(fragment) not in text, repr(fragment))

    followup = expect.get("followup") or {}
    if followup and previous_params is None:
        violations.append("followup: oracle declared on a turn without a previous turn")
    for key in followup.get("preserves", ()) if previous_params is not None else ():
        before, after = previous_params.get(key), params.get(key)
        check(f"followup.preserves.{key}", before not in _EMPTY and before == after, f"{before!r} -> {after!r}")
    for key in followup.get("clears", ()) if previous_params is not None else ():
        check(f"followup.clears.{key}", params.get(key) in _EMPTY, f"still {params.get(key)!r}")
    lineage = expect.get("lineage") or {}
    if lineage:
        check("lineage.present", bool(lineage_views), "no product trace observed")
        if "raw_text" in lineage:
            check("lineage.raw_text", any(v.raw_text == lineage["raw_text"] for v in lineage_views),
                  f"expected raw text {lineage['raw_text']!r}")
        if "min_events" in lineage:
            check("lineage.min_events", sum(len(v.events) for v in lineage_views) >= lineage["min_events"],
                  f"expected >= {lineage['min_events']} events")
        if "min_tombstones" in lineage:
            check("lineage.min_tombstones",
                  sum(len(v.tombstones) for v in lineage_views) >= lineage["min_tombstones"],
                  f"expected >= {lineage['min_tombstones']} tombstones")
    return violations
