"""M2A-8a0 oracle B: the invariants of a TRANSITION, from the CONTRACT (never from the Gold).

Two oracles answer two different questions, and neither imports the other nor any M2a module:

* the GOLD oracle (``m2a_oracle_audit.audit``): is the adjudicated MEANING right (disposition, code, semantic effective,
  effects)? The Gold decides meaning;
* this one, ``check_transition(base, patch, effective)``: did the reducer get there WITHOUT FALSIFYING IDENTITY OR
  PROVENANCE? The contract decides the mechanical invariants (what is carried, what is placed, what is refused, how a
  redirected universe is rebuilt); the human labelers never adjudicate mechanics.

``check_transition`` is a pure function of three dicts (``m2a-accepted/1`` base, ``m2a-patch/1`` patch, the effective
the reducer returned) and the standard library; it returns the list of violations (empty = the transition is lawful).

Interface for M2A-3 (its Hypothesis properties call it; the oracle never calls the reducer): feed it GENERATED bases and
patches, not only the Gold's. The Gold's bases carry empty ``policies``, ``receipt_rules`` and
``provenance.{source_amount, year, year_source, buyer_scope, inherited_from}``, so a reducer that rebuilds a carried
condition with those defaults is invisible on them; on a base whose conditions carry non-default values for every field
(and a dual anchor, and a ``view``), rebuilding, re-originating or re-resolving shows up as a violation. Pass the host's
own subject-to-dataset table as ``dataset_of`` to make the dataset check exact; without it the check is relational (the
base's dataset for the same subject, else one that is none of the base's other datasets).

``TRANSITION_RULES`` lists every rule with its contract clause, what it demands and the mutants of the tests that must
die (the manifest declares the table).
"""

from __future__ import annotations

import collections
from collections.abc import Callable

POLARIZED = frozenset({"TOPIC", "BUYER", "ACTOR", "GEOGRAPHY", "TIME", "STATE", "MODALITY", "CONTRACT_TYPE_OR_NATURE"})
EFFECTIVE_KEYS = frozenset({"schema", "requests", "conditions", "logic", "policies", "base_trace_id", "root_trace_id"})
REQUEST_KEYS = frozenset({"request_id", "scope_id", "subject", "universe_kind", "universe_origin", "view", "dataset"})
CONDITION_KEYS = frozenset({"condition_id", "request_ids", "slot", "dimension", "operator", "polarity", "level", "value",
                            "authority", "receipt_rules", "policies", "origin", "provenance"})
POLICY_KEYS = frozenset({"policy_id", "version", "condition_ids", "effect"})
SINGLE_SLOTS = ("VALUE/lower", "VALUE/upper", "ORDER", "CARDINALITY")

TRANSITION_RULES = (
    {"id": "T-STRUCT", "clause": "§3.2; M2A-1 types",
     "demands": "the effective has exactly the keys of m2a-accepted/1; unique request and condition ids; every reference "
                "to a request resolves; a dual anchor shares its scope; every policy is well formed and names conditions "
                "of the effective; at most one condition per VALUE side, ORDER and CARDINALITY; no CARDINALITY without ORDER",
     "killed_by": ["effective_extra_key", "duplicate_request_id", "duplicate_condition_id", "condition_unknown_request",
                   "dual_scope_differs", "dangling_policy", "malformed_policy", "two_bounds_same_side",
                   "cardinality_without_order", "request_ids_not_rewritten_after_reduction"]},
    {"id": "T-REFUSAL", "clause": "§6.1, §6.2, §6.4 H12, §6.7 (amendment R3)",
     "demands": "a patch the contract refuses produces NO effective: two SET on one slot, KEEP and CLEAR of one slot, two "
                "REPLACE_UNIVERSE, KEEP of an empty slot, a lone CLEAR of an empty slot, and (amendment R3) a CLEAR of "
                "ORDER that leaves a CARDINALITY standing",
     "killed_by": ["effective_for_slot_conflict", "effective_for_keep_on_empty_slot", "effective_for_clear_on_empty_slot",
                   "effective_for_clear_order_leaving_cardinality"]},
    {"id": "T-REQ-SET", "clause": "§5.2 (REPLACE_UNIVERSE), §14",
     "demands": "without REPLACE_UNIVERSE the effective has exactly the base's requests; with it, exactly one: the named "
                "subject's (a dual anchor keeps the named one and the other disappears) or the single one",
     "killed_by": ["request_added_without_op", "dual_keeps_both", "dual_keeps_the_other"]},
    {"id": "T-REQ-UNTOUCHED", "clause": "§6.3 (identity of what no op touches); user decision R3",
     "demands": "an untouched request is byte-identical to the base's in EVERY field: request_id, scope_id, subject, "
                "universe_kind, universe_origin, view, dataset",
     "killed_by": ["M3_reoriginate_untouched_request", "M4_change_scope_id_untouched", "M5_change_request_id_untouched",
                   "untouched_request_dataset_changed"]},
    {"id": "T-REQ-REDIRECT", "clause": "§5.2; user decision R3",
     "demands": "a request touched by REPLACE_UNIVERSE keeps request_id, scope_id and view: only subject, universe_kind, "
                "universe_origin and dataset change",
     "killed_by": ["redirect_changes_request_id", "redirect_changes_scope_id", "redirect_changes_view"]},
    {"id": "T-REQ-TARGET", "clause": "§5.2",
     "demands": "the redirected request has the op's subject and universe kind",
     "killed_by": ["redirect_keeps_old_kind", "redirect_keeps_old_subject"]},
    {"id": "T-REQ-ORIGIN", "clause": "§5.2 (el universo pasa a ser dicho)",
     "demands": "the redirected request's universe_origin is the origin of the CURRENT PatchOp",
     "killed_by": ["M2_stale_origin_after_redirect"]},
    {"id": "T-REQ-DATASET", "clause": "§3.2; §14",
     "demands": "the redirected request's dataset is the one that corresponds to its new subject",
     "killed_by": ["M1_stale_dataset_after_redirect"]},
    {"id": "T-COND-CARRIED", "clause": "§6.3 (F1), INV-M2A-02",
     "demands": "a carried condition is byte-identical to the base's (value, authority, policies, origin, provenance, "
                "condition_id); only request_ids may be rewritten, and only when a dual anchor is reduced",
     "killed_by": ["tamper_carried_origin", "tamper_carried_provenance", "tamper_carried_authority",
                   "tamper_carried_policies", "tamper_carried_receipt_rules", "rename_carried_condition_id",
                   "reset_carried_host_fields"]},
    {"id": "T-COND-PLACED", "clause": "§5.2 (SET pone c)",
     "demands": "a placed condition is the patch's, unchanged but for condition_id and request_ids (ids are the reducer's: "
                "a patch condition may collide with a base id)",
     "killed_by": ["tamper_placed_origin", "tamper_placed_provenance", "tamper_placed_authority",
                   "tamper_placed_policies", "tamper_placed_receipt_rules", "placed_value_changed"]},
    {"id": "T-COND-FABRICATED", "clause": "INV-M2A-02, INV-M2A-16",
     "demands": "every condition of the effective is carried from the base or placed from the patch: none is fabricated",
     "killed_by": ["fabricated_condition"]},
    {"id": "T-COND-WRITTEN-SLOT", "clause": "§5.2 (SET sustituye la ranura; CLEAR la quita)",
     "demands": "a base condition in a slot an op writes is not carried",
     "killed_by": ["carried_in_written_slot", "cleared_condition_kept", "clear_dimension_only_positive"]},
    {"id": "T-COND-REMOVAL", "clause": "§6.3, INV-M2A-16; §6.5 (A8.1, F16); §15.2",
     "demands": "a base condition absent from the effective has an op on its slot, or is a declared substitution checked "
                "on the typed values: A8.1 (a VALUE bound against the patch's new opposite bound that leaves the interval "
                "empty or degenerate) or F16 (the single positive of a slot against a negative SET of the same value)",
     "killed_by": ["removal_without_op", "f16_any_value", "set_order_drops_cardinality", "set_clears_whole_dimension",
                   "a81_without_collision"]},
    {"id": "T-COND-ATOMIC", "clause": "§6.1, INV-M2A-01",
     "demands": "every SET / SET_ORDER of the patch is placed exactly once: no op is applied partially or twice",
     "killed_by": ["op_not_applied", "op_applied_twice"]},
    {"id": "T-COND-REQUESTS", "clause": "§6.3; user decision R3 (P-1)",
     "demands": "a condition's request_ids are the base's when the request ids are unchanged; when a dual anchor is "
                "reduced they are rewritten to the effective's request ids; a placed condition applies to all of them",
     "killed_by": ["carried_request_ids_changed_without_reduction", "placed_applies_to_one_request"]},
    {"id": "T-COND-INTERVAL", "clause": "§6.7, §15.2 (A8.1), INV-M2A-18",
     "demands": "a VALUE interval that is empty or degenerate is never produced by a follow-up with the opposite bound "
                "carried (two bounds of the same patch are the user's request; two carried ones are the base's own)",
     "killed_by": ["no_a81"]},
    {"id": "T-EFF-LOGIC", "clause": "§4.2, §5",
     "demands": "a base without logic (the AND of all) yields an effective without logic: the closed ops create no group",
     "killed_by": ["logic_invented"]},
)


def _body(condition: dict, *drop: str) -> dict:
    return {k: v for k, v in condition.items() if k not in drop}


def _diff(a: dict, b: dict) -> list[str]:
    return sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))


def _covered(written: str, key: str) -> bool:
    """Whether a written slot (or a dimension-wide CLEAR) covers the slot ``key``."""
    return key == written or (written in POLARIZED and key.startswith(written + "/")) or (
        written == "VALUE" and key.startswith("VALUE/"))


def _item(value, name: str):
    return next((i.partition("=")[2] for i in value if i.startswith(name + "=")), None)


def _amount(condition: dict) -> int:
    return int(_item(condition["value"], "amount_cop"))


def _canonical(condition: dict) -> tuple:
    return (condition["dimension"], tuple(i for i in condition["value"] if not i.startswith("excluded=")))


def _structure(effective) -> tuple[list[str], list[str]]:
    """(fatal, soft): a malformed effective cannot be judged further; a slot held twice or a CARDINALITY without ORDER is
    a violation but the other checks still run (they say WHY the condition is there)."""
    if type(effective) is not dict or set(effective) != EFFECTIVE_KEYS:
        return ["the effective does not have exactly the keys of m2a-accepted/1"], []
    requests, conditions, policies = effective["requests"], effective["conditions"], effective["policies"]
    if effective["schema"] != "m2a-accepted/1":
        return ["unknown schema"], []
    if type(requests) is not list or not requests or type(conditions) is not list or type(policies) is not list:
        return ["requests, conditions and policies must be lists (requests non-empty)"], []
    if any(type(r) is not dict or set(r) != REQUEST_KEYS for r in requests):
        return ["a request does not have exactly the keys of AcceptedRequest"], []
    if any(type(c) is not dict or set(c) != CONDITION_KEYS for c in conditions):
        return ["a condition does not have exactly the keys of AcceptedCondition"], []
    out, soft = [], []
    rids, cids = [r["request_id"] for r in requests], [c["condition_id"] for c in conditions]
    if len(set(rids)) != len(rids):
        out.append("duplicate request ids")
    if len(set(cids)) != len(cids):
        out.append("duplicate condition ids")
    if len(requests) > 1 and len({r["scope_id"] for r in requests}) != 1:
        out.append("a dual anchor does not share its scope")
    for c in conditions:
        if type(c["request_ids"]) is not list or not c["request_ids"] or not set(c["request_ids"]) <= set(rids):
            out.append(f"condition {c['condition_id']} points to a request the effective does not have")
    for p in policies:
        if type(p) is not dict or set(p) != POLICY_KEYS or type(p["condition_ids"]) is not list or not set(
                p["condition_ids"]) <= set(cids):
            out.append("a policy is not well formed or names conditions the effective does not have")
    for slot in SINGLE_SLOTS:
        if sum(1 for c in conditions if c["slot"] == slot) > 1:
            soft.append(f"two conditions in the slot {slot}")
    if any(c["slot"] == "CARDINALITY" for c in conditions) and not any(c["slot"] == "ORDER" for c in conditions):
        soft.append("CARDINALITY without ORDER")
    return out, soft


def _refusal(base: dict, ops: list[dict]) -> str | None:
    """Why the contract refuses this patch (so that no effective may exist), or None."""
    sets = collections.Counter(op["slot"] for op in ops if op["op"] in ("SET", "SET_ORDER"))
    if any(n > 1 for n in sets.values()):
        return "DELTA_SLOT_CONFLICT: two SET on one slot"
    keeps = [op["slot"] for op in ops if op["op"] == "KEEP"]
    clears = [op["slot"] for op in ops if op["op"] == "CLEAR"]
    if any(_covered(c, k) or _covered(k, c) for c in clears for k in keeps):
        return "DELTA_SLOT_CONFLICT: KEEP and CLEAR of one slot"
    if sum(1 for op in ops if op["op"] == "REPLACE_UNIVERSE") > 1:
        return "DELTA_SLOT_CONFLICT: two REPLACE_UNIVERSE"
    slots = [c["slot"] for c in base["conditions"]]
    if any(k not in slots for k in keeps):
        return "KEEP_ON_EMPTY_SLOT"
    if len(ops) == 1 and clears and not any(_covered(clears[0], s) for s in slots):
        return "CLEAR_ON_EMPTY_SLOT (H12: INFORM, the anchor is intact)"
    written = [op["slot"] for op in ops if op["op"] in ("SET", "SET_ORDER", "CLEAR")]
    after = {s for s in slots if not any(_covered(w, s) for w in written)} | set(sets)
    if "CARDINALITY" in after and "ORDER" not in after:
        return "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER (§6.7; amendment R3): CLEAR(ORDER) with a CARDINALITY not withdrawn"
    return None


def _check_requests(base: dict, ops: list[dict], effective: dict, dataset_of, bad: Callable) -> None:
    by_id = {r["request_id"]: r for r in base["requests"]}
    mine = effective["requests"]
    redirects = [op for op in ops if op["op"] == "REPLACE_UNIVERSE"]
    if not redirects:
        if len(mine) != len(base["requests"]):
            bad("T-REQ-SET", "the effective's requests are not the base's (no REPLACE_UNIVERSE)")
            return
        for b in base["requests"]:
            same = next((r for r in mine if r["request_id"] == b["request_id"]), None) or next(
                (r for r in mine if r["subject"] == b["subject"]), None)
            if same is None:
                bad("T-REQ-SET", f"the base's request {b['request_id']} is not in the effective")
            elif same != b:
                bad("T-REQ-UNTOUCHED", f"request {b['request_id']} changed: {_diff(same, b)}")
        return
    op = redirects[0]
    target = op["universe"]
    named = [b for b in base["requests"] if b["subject"] == target["subject"]]
    touched = named[0] if named else base["requests"][0]
    if len(mine) != 1:
        bad("T-REQ-SET", "REPLACE_UNIVERSE must leave exactly one request")
        return
    if mine[0]["request_id"] != touched["request_id"]:
        if mine[0]["request_id"] in by_id:
            bad("T-REQ-SET", f"the request kept is not the named one ({touched['request_id']})")
            return
        bad("T-REQ-REDIRECT", "request_id of the redirected request changed")
    r = mine[0]
    for field in ("scope_id", "view"):
        if r[field] != touched[field]:
            bad("T-REQ-REDIRECT", f"{field} of the redirected request changed")
    if (r["subject"], r["universe_kind"]) != (target["subject"], target["universe_kind"]):
        bad("T-REQ-TARGET", "the redirected request is not the op's (subject, universe_kind)")
    if r["universe_origin"] != op["origin"]:
        bad("T-REQ-ORIGIN", "the redirected request's universe_origin is not the origin of the current PatchOp")
    if dataset_of is not None:
        if r["dataset"] != dataset_of(r["subject"]):
            bad("T-REQ-DATASET", "the dataset is not the one of the new subject")
    else:
        same = [b["dataset"] for b in base["requests"] if b["subject"] == r["subject"]]
        others = {b["dataset"] for b in base["requests"] if b["subject"] != r["subject"]}
        if (same and r["dataset"] != same[0]) or (not same and r["dataset"] in others | {touched["dataset"]}):
            bad("T-REQ-DATASET", "the dataset is not the one of the new subject (stale or another subject's)")


def _declared_substitution(b: dict, base: dict, patch_conds: list[dict], effective_conds: list[dict]) -> str | None:
    """Why a base condition may vanish with no op on its slot: A8.1 or F16 (§6.5), checked on the typed values."""
    if b["dimension"] == "VALUE":
        side = "VALUE/upper" if b["slot"] == "VALUE/lower" else "VALUE/lower"
        new = [p for p in patch_conds if p["slot"] == side]
        if not new:
            return None
        lo, hi = (_amount(new[0]), _amount(b)) if side == "VALUE/lower" else (_amount(b), _amount(new[0]))
        return "A8.1" if lo >= hi else None
    if b["dimension"] in POLARIZED and b["polarity"] == "POSITIVE":
        negative = b["slot"].replace("/POSITIVE", "/NEGATIVE")
        same = [p for p in patch_conds if p["slot"] == negative and _canonical(p) == _canonical(b)]
        single = sum(1 for x in base["conditions"] if x["slot"] == b["slot"]) == 1
        return "F16" if same and single else None
    return None


def _check_conditions(base: dict, ops: list[dict], effective: dict, bad: Callable) -> None:
    written = [op["slot"] for op in ops if op["op"] in ("SET", "SET_ORDER", "CLEAR")]
    patch_conds = [op["condition"] for op in ops if op["op"] in ("SET", "SET_ORDER") and op["condition"]]
    base_rids = sorted(r["request_id"] for r in base["requests"])
    eff_rids = sorted(r["request_id"] for r in effective["requests"])
    carried_ids: set[str] = set()
    unused = list(patch_conds)
    consumed: list[dict] = []
    for c in effective["conditions"]:
        hit_written = any(_covered(w, c["slot"]) for w in written)
        same_id = next((b for b in base["conditions"] if b["condition_id"] == c["condition_id"]), None)
        carried = same_id is not None and _body(c, "request_ids") == _body(same_id, "request_ids")
        placed = next((p for p in unused if _body(p, "condition_id", "request_ids") == _body(c, "condition_id", "request_ids")),
                      None)
        if hit_written:
            if placed is not None:
                unused.remove(placed)
                consumed.append(placed)
                if c["request_ids"] != eff_rids:
                    bad("T-COND-REQUESTS", f"placed condition {c['condition_id']} must apply to every request")
            elif any(_body(p, "condition_id", "request_ids") == _body(c, "condition_id", "request_ids") for p in consumed):
                bad("T-COND-ATOMIC", f"an op was applied twice ({c['slot']})")
            elif carried or any(_body(c, "condition_id", "request_ids") == _body(b, "condition_id", "request_ids")
                                for b in base["conditions"]):
                bad("T-COND-WRITTEN-SLOT", f"a base condition survives in the slot {c['slot']}, which an op writes")
            else:
                near = next((p for p in patch_conds if p["slot"] == c["slot"]), None)
                bad("T-COND-PLACED" if near is not None else "T-COND-FABRICATED",
                    f"condition {c['condition_id']} is not the patch's" + (f": {_diff(_body(c, 'condition_id', 'request_ids'), _body(near, 'condition_id', 'request_ids'))}" if near else ""))
            continue
        if carried:
            carried_ids.add(c["condition_id"])
            expected = same_id["request_ids"] if set(eff_rids) == set(base_rids) else eff_rids
            if c["request_ids"] != expected:
                bad("T-COND-REQUESTS", f"carried condition {c['condition_id']} has stale request_ids")
            continue
        if same_id is not None:
            bad("T-COND-CARRIED", f"carried condition {c['condition_id']} changed: "
                                  f"{_diff(_body(c, 'request_ids'), _body(same_id, 'request_ids'))}")
            carried_ids.add(c["condition_id"])
            continue
        twin = next((b for b in base["conditions"] if _body(b, "condition_id", "request_ids") == _body(c, "condition_id", "request_ids")), None)
        if twin is not None:
            bad("T-COND-CARRIED", f"carried condition {twin['condition_id']} changed its condition_id")
            carried_ids.add(twin["condition_id"])
        else:
            bad("T-COND-FABRICATED", f"condition {c['condition_id']} is neither carried from the base nor placed from the patch")
    for p in unused:
        bad("T-COND-ATOMIC", f"the patch's condition in {p['slot']} was not placed")
    for b in base["conditions"]:
        if b["condition_id"] in carried_ids or any(_covered(w, b["slot"]) for w in written):
            continue
        if _declared_substitution(b, base, patch_conds, effective["conditions"]) is None:
            bad("T-COND-REMOVAL", f"base condition {b['condition_id']} ({b['slot']}) vanished with no op on its slot "
                                  f"and no declared substitution")
    lower = [c for c in effective["conditions"] if c["slot"] == "VALUE/lower"]
    upper = [c for c in effective["conditions"] if c["slot"] == "VALUE/upper"]
    if lower and upper and _amount(lower[0]) >= _amount(upper[0]):
        def placed_here(c):
            return any(_body(c, "condition_id", "request_ids") == _body(p, "condition_id", "request_ids") for p in patch_conds)
        if placed_here(lower[0]) != placed_here(upper[0]):  # one bound is the follow-up's and the other is carried
            bad("T-COND-INTERVAL", "an empty or degenerate VALUE interval left by a carried opposite bound")


def check_transition(base: dict, patch: dict, effective: dict | None, *, dataset_of: Callable[[str], str] | None = None
                     ) -> list[dict]:
    """The violations of the contract's mechanical invariants by the transition ``base --patch--> effective`` (empty =
    lawful). ``effective`` is what the reducer returned for an EXECUTED delta; for a refused patch pass ``None``: a
    refused patch with an effective is a violation. ``dataset_of``: the host's subject-to-dataset table (exact check)."""
    out: list[dict] = []

    def bad(rule: str, detail: str) -> None:
        out.append({"rule": rule, "detail": detail})

    ops = patch["ops"]
    refusal = _refusal(base, ops)
    if effective is None:
        return out  # nothing was produced: whether it had to be is the Gold oracle's (disposition and code)
    if refusal is not None:
        bad("T-REFUSAL", f"the contract refuses this patch ({refusal}) and the reducer produced an effective")
        return out
    fatal, soft = _structure(effective)
    if fatal:
        return [{"rule": "T-STRUCT", "detail": p} for p in fatal]
    for problem in soft:
        bad("T-STRUCT", problem)
    _check_requests(base, ops, effective, dataset_of, bad)
    _check_conditions(base, ops, effective, bad)
    if base["logic"] is None and effective["logic"] is not None:
        bad("T-EFF-LOGIC", "the effective has a logic group the base did not and no closed op can create")
    return out
