"""M2A-8a0 oracle B: the invariants of a TRANSITION, from the CONTRACT (never from the Gold).

Two oracles answer two different questions, and neither imports the other nor any M2a module:

* the GOLD oracle (``m2a_oracle_audit.audit``): is the adjudicated MEANING right (disposition, code, semantic effective,
  effects)? The Gold decides meaning;
* this one, ``check_transition(base, patch, effective)``: did the reducer get there WITHOUT FALSIFYING IDENTITY OR
  PROVENANCE, and did it refuse what the contract refuses, for the contract's reason? The contract decides the
  mechanical invariants (what is carried, what is placed, what is refused and with which cause first, how a redirected
  universe is rebuilt); the human labelers never adjudicate mechanics.

``check_transition`` is a pure function of the dicts (``m2a-accepted/1`` base, ``m2a-patch/1`` patch, and either the
effective the reducer returned or the code it refused with) and the standard library; it returns the list of violations
(empty = the transition is lawful). ``refusal_causes(base, patch)`` and ``expected_refusal(base, patch)`` expose what the
contract demands of a refusal (§6 step 3b: the causes, with their closed precedence), also as pure functions.

Interface for M2A-3 (its Hypothesis properties call it; the oracle never calls the reducer): feed it GENERATED bases and
patches, not only the Gold's. The Gold's bases carry empty ``policies``, ``receipt_rules`` and
``provenance.{source_amount, year, year_source, buyer_scope, inherited_from}``, so a reducer that rebuilds a carried
condition with those defaults is invisible on them; on a base whose conditions carry non-default values for every field
(and a dual anchor, and a ``view``), rebuilding, re-originating or re-resolving shows up as a violation. Pass the host's
own subject-to-dataset table as ``dataset_of`` to make the dataset check exact; without it the check is relational (the
base's dataset for the same subject, else one that is none of the base's other datasets). When the reducer REFUSED, pass
``effective=None`` and ``refusal=<the code it refused with>`` (a ``DeltaRefusal`` code, or the A.3 code published for it:
``UNRESOLVED_VALUE:<DIM>``, ``VALUE_WITHOUT_COMPARATOR``); with both ``None`` there is nothing to judge.

What this function cannot decide from the dicts, declared (see ``LIMITED_GUARANTEES``): ``TARGET_NOT_ADDRESSABLE`` (it needs
the shape of the IR v2 ``logic`` expression: with a ``logic`` in the base that refusal is accepted unjudged), the refusals
that come from an AUTHORITY (``OP_NOT_ACCREDITED`` of an op whose authority did not resolve: a ``m2a-patch/1`` carries only
resolved conditions), a value CONTAINED in a positive (G-FU-09), and the host rules H1-H11, which need the conversational
context and are the Gold's to decide.

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
# §4.2 slot order (the rows of the slot table: each polarized dimension POSITIVE then NEGATIVE; VALUE lower then upper):
# the tie-break between causes of the same precedence (§6 step 3b). The test suite pins it against the contract's table.
SLOT_ORDER = tuple(f"{d}/{p}" for d in ("TOPIC", "BUYER", "ACTOR", "GEOGRAPHY", "TIME", "STATE", "MODALITY",
                                        "CONTRACT_TYPE_OR_NATURE") for p in ("POSITIVE", "NEGATIVE")) + (
    "VALUE/lower", "VALUE/upper", "ORDER", "CARDINALITY", "UNIVERSE")
# §6 step 3b: the closed precedence of refusal causes, highest first (pinned against the contract's table by the tests).
PRECEDENCE = ("TARGET_NOT_ADDRESSABLE", "DELTA_SLOT_CONFLICT", "KEEP_ON_EMPTY_SLOT", "OP_NOT_ACCREDITED",
              "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER")
OPNA = "OP_NOT_ACCREDITED"
H12 = "CLEAR_ON_EMPTY_SLOT"  # §6.4 H12: INFORM, no effective, the anchor intact (not a DeltaRefusal: it follows the causes)
WRITERS = ("SET", "SET_ORDER", "REPLACE_UNIVERSE")
PESOS_SCALE = frozenset({"PESOS", "MILES", "MILLONES", "MIL_MILLONES", "BILLONES"})  # §15.2: comparable among themselves

TRANSITION_RULES = (
    {"id": "T-STRUCT", "clause": "§3.2; M2A-1 types",
     "demands": "the effective has exactly the keys of m2a-accepted/1; unique request and condition ids; every reference "
                "to a request resolves; a dual anchor shares its scope; every policy is well formed and names conditions "
                "of the effective; at most one condition per VALUE side, ORDER and CARDINALITY; no CARDINALITY without ORDER",
     "killed_by": ["effective_extra_key", "duplicate_request_id", "duplicate_condition_id", "condition_unknown_request",
                   "dual_scope_differs", "dangling_policy", "malformed_policy", "two_bounds_same_side",
                   "cardinality_without_order", "request_ids_not_rewritten_after_reduction"]},
    {"id": "T-REFUSAL", "clause": "§6 steps 2, 3b and 5 (F16, P-3, A8.1), §6.7 (R3-1), §6.4 H12",
     "demands": "a patch with a refusal cause produces NO effective: two SET (or two SET_ORDER, or two REPLACE_UNIVERSE) on "
                "one slot; KEEP with CLEAR, or KEEP with SET/SET_ORDER/REPLACE_UNIVERSE, of one slot EVEN WHEN THE VALUES "
                "COINCIDE (R3b, U22); KEEP of an empty slot; a negative SET of a value that is one of several carried "
                "positives, or whose positive the same patch creates (F16 fail-closed, R3-4, P-3); a VALUE bound against a "
                "carried opposite bound of incomparable units (§15.2); a composed state that leaves a CARDINALITY standing "
                "without ORDER (R3-1, decided on the STATE, not on the op); and a lone CLEAR of an empty slot (H12)",
     "killed_by": ["effective_for_two_set_one_slot", "effective_for_keep_and_clear_one_slot",
                   "effective_for_keep_and_set_same_value", "effective_for_keep_and_set_order",
                   "effective_for_keep_and_replace_universe", "effective_for_two_replace_universe",
                   "effective_for_keep_on_empty_slot", "effective_for_f16_several_positives",
                   "effective_for_f16_same_patch_positive", "effective_for_a81_incomparable_units",
                   "effective_for_clear_order_leaving_cardinality", "effective_for_set_cardinality_without_order",
                   "effective_for_clear_on_empty_slot"]},
    {"id": "T-REFUSAL-CODE", "clause": "§6 step 3b (closed precedence of causes), §6 table (A.3 code per DeltaRefusal)",
     "demands": "a refused patch is refused with the code of the HIGHEST-precedence cause (TARGET_NOT_ADDRESSABLE > "
                "DELTA_SLOT_CONFLICT > KEEP_ON_EMPTY_SLOT > OP_NOT_ACCREDITED:<DIM> > "
                "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER), with ties decided by the §4.2 slot order and the dimension of "
                "the slot named in OP_NOT_ACCREDITED; a host-rule code never replaces a cause (causes precede §6 step 4)",
     "killed_by": ["refusal_conflict_reported_as_keep_on_empty", "refusal_keep_on_empty_reported_as_opna",
                   "refusal_opna_reported_as_cardinality", "refusal_conflict_reported_as_cardinality",
                   "refusal_precedence_reversed", "refusal_slot_order_ignored", "refusal_wrong_dimension",
                   "refusal_replaced_by_a_host_rule"]},
    {"id": "T-REFUSAL-UNJUSTIFIED", "clause": "§6 step 3b (a refusal needs a cause), §6.1",
     "demands": "a structural refusal (DELTA_SLOT_CONFLICT, KEEP_ON_EMPTY_SLOT, DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER, "
                "TARGET_NOT_ADDRESSABLE over a base without logic) over a patch with no such cause refuses a lawful "
                "transition",
     "killed_by": ["refuse_lawful_as_slot_conflict", "refuse_lawful_as_keep_on_empty",
                   "refuse_lawful_as_cardinality_without_order", "refuse_lawful_as_target_not_addressable"]},
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


def _cover(slot: str) -> tuple[str, ...]:
    """The slots an op's slot addresses: a dimension-wide slot (CLEAR(TOPIC), CLEAR(VALUE)) covers each of its slots."""
    return tuple(k for k in SLOT_ORDER if _covered(slot, k)) or (slot,)


def _rank(slot: str) -> int:
    return SLOT_ORDER.index(slot) if slot in SLOT_ORDER else len(SLOT_ORDER)


def _unit_class(condition: dict) -> str | None:
    unit = (condition.get("provenance") or {}).get("unit")
    return "SMMLV" if unit == "SMMLV" else ("PESOS" if unit in PESOS_SCALE else None)


def _dimension(slot: str) -> str:
    return slot.partition("/")[0]


def refusal_causes(base: dict, patch: dict) -> list[dict]:
    """The refusal causes the contract finds in ``patch`` over ``base`` (§6 step 3b), highest precedence first and, within
    one precedence, in §4.2 slot order. Each is ``{"code", "refusal", "slot"}``; ``code`` is the ``DeltaRefusal`` code
    (``OP_NOT_ACCREDITED:<DIM>`` with the dimension of the slot). The result does not depend on the order of the ops
    (§6 step 2). Only what the dicts show is decided (see the module docstring): ``TARGET_NOT_ADDRESSABLE`` is never found
    here, and an op whose authority did not resolve cannot be written in a ``m2a-patch/1``."""
    ops = patch["ops"]
    present = {c["slot"] for c in base["conditions"]} | {"UNIVERSE"}
    writers: dict[str, list] = {}
    keeps: dict[str, list] = {}
    clears: dict[str, list] = {}
    for op in ops:
        if op["op"] in WRITERS:
            writers.setdefault(op["slot"], []).append(op)
        elif op["op"] in ("KEEP", "CLEAR"):
            for key in _cover(op["slot"]):
                (keeps if op["op"] == "KEEP" else clears).setdefault(key, []).append(op)
    found: list[tuple[str, str]] = []  # (refusal, slot)
    for key in sorted(set(writers) | set(keeps), key=_rank):
        if len(writers.get(key, ())) >= 2 or (key in keeps and (key in writers or key in clears)):
            found.append(("DELTA_SLOT_CONFLICT", key))
    for op in ops:
        if op["op"] == "KEEP" and not any(k in present for k in _cover(op["slot"])):
            found.append(("KEEP_ON_EMPTY_SLOT", op["slot"]))

    def carried(key: str) -> bool:  # §6 step 3: standing in the base and touched by no op (KEEP touches nothing)
        return key in present and key not in writers and key not in clears

    for op in ops:
        if op["op"] != "SET" or op["condition"] is None:
            continue
        slot, condition = op["slot"], op["condition"]
        if slot.endswith("/NEGATIVE") and _dimension(slot) in POLARIZED:
            positive = slot.replace("/NEGATIVE", "/POSITIVE")
            same_patch = any(o["op"] == "SET" and o["slot"] == positive and o["condition"] is not None
                             and _canonical(o["condition"]) == _canonical(condition) for o in ops)
            standing = [c for c in base["conditions"] if c["slot"] == positive]
            several = (carried(positive) and len(standing) > 1
                       and any(_canonical(c) == _canonical(condition) for c in standing))
            if same_patch or several:  # F16 fail-closed (R3-4) and P-3: never inverted, never accepted
                found.append((OPNA, slot))
        elif slot in ("VALUE/lower", "VALUE/upper"):
            opposite = "VALUE/upper" if slot == "VALUE/lower" else "VALUE/lower"
            kept = [c for c in base["conditions"] if c["slot"] == opposite]
            if carried(opposite) and kept and None not in (_unit_class(condition), _unit_class(kept[0])) and (
                    _unit_class(condition) != _unit_class(kept[0])):  # §15.2: incomparable units are never compared
                found.append((OPNA, slot))
    after: dict[str, set] = {r["request_id"]: set() for r in base["requests"]}  # per request: slots left standing (R3-1)
    for c in base["conditions"]:
        if c["slot"] not in writers and c["slot"] not in clears:
            for rid in c["request_ids"]:
                after.setdefault(rid, set()).add(c["slot"])
    placed = {op["slot"] for op in ops if op["op"] in ("SET", "SET_ORDER")}
    for slots in after.values():
        slots |= placed
    if any("CARDINALITY" in slots and "ORDER" not in slots for slots in after.values()):
        found.append(("DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER", "ORDER"))
    found.sort(key=lambda f: (PRECEDENCE.index(f[0].partition(":")[0]), _rank(f[1])))
    return [{"code": f"{OPNA}:{_dimension(slot)}" if refusal == OPNA else refusal, "refusal": refusal, "slot": slot}
            for refusal, slot in found]


def _h12(base: dict, ops: list[dict]) -> bool:
    """§6.4 H12: a CLEAR of an empty slot as the only operation (INFORM, no effective, the anchor intact)."""
    slots = [c["slot"] for c in base["conditions"]]
    clears = [op["slot"] for op in ops if op["op"] == "CLEAR"]
    return len(ops) == 1 and bool(clears) and not any(_covered(clears[0], s) for s in slots)


def expected_refusal(base: dict, patch: dict) -> str | None:
    """The code the contract demands when it refuses ``patch`` over ``base``: the code of the highest-precedence cause
    (``OP_NOT_ACCREDITED:<DIM>``, ``DELTA_SLOT_CONFLICT``...), ``CLEAR_ON_EMPTY_SLOT`` for H12 once no cause exists, or
    ``None`` when the contract refuses nothing the dicts show (the reducer must produce an effective)."""
    causes = refusal_causes(base, patch)
    if causes:
        return causes[0]["code"]
    return H12 if _h12(base, patch["ops"]) else None


def _refusal(base: dict, ops: list[dict]) -> str | None:
    """Why the contract refuses this patch (so that no effective may exist), or None: the code, for messages."""
    return expected_refusal(base, {"ops": ops})


def _published(code: str) -> str:
    """The ``DeltaRefusal`` a code stands for: the A.3 codes of OP_NOT_ACCREDITED (§6 table) are mapped back to it."""
    head, _, tail = code.partition(":")
    if head == "UNRESOLVED_VALUE" and tail:
        return f"{OPNA}:{tail}"
    return f"{OPNA}:VALUE" if code == "VALUE_WITHOUT_COMPARATOR" else code


def _check_refusal(base: dict, patch: dict, refusal: str, bad: Callable) -> None:
    """The reducer refused with ``refusal``: it needs a cause, and the code must be the highest-precedence one."""
    code, causes = _published(refusal), refusal_causes(base, patch)
    if causes:
        if code != causes[0]["code"] and not (code == "TARGET_NOT_ADDRESSABLE" and base["logic"] is not None):
            bad("T-REFUSAL-CODE", f"the highest-precedence cause is {causes[0]['code']} (of {[c['code'] for c in causes]}) "
                                  f"and the reducer refused with {refusal}")
        return
    if code == H12 and _h12(base, patch["ops"]):
        return
    structural = {"DELTA_SLOT_CONFLICT", "KEEP_ON_EMPTY_SLOT", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER", H12}
    if code in structural or (code == "TARGET_NOT_ADDRESSABLE" and base["logic"] is None):
        bad("T-REFUSAL-UNJUSTIFIED", f"the reducer refused with {refusal} and the contract finds no such cause in the patch")


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
        # a patch whose negative SET matches one of SEVERAL carried positives (F16's fail-closed branch) never gets here: it is
        # refused first (``refusal_causes``), so what vanishes here is the single carried positive the declaration covers
        return "F16" if same else None
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


def check_transition(base: dict, patch: dict, effective: dict | None = None, *,
                     dataset_of: Callable[[str], str] | None = None, refusal: str | None = None) -> list[dict]:
    """The violations of the contract's mechanical invariants by the transition ``base --patch--> effective`` (empty =
    lawful). Pass the ``effective`` the reducer returned, or, when it REFUSED, ``refusal=<its code>`` (and no effective):
    the refusal needs a cause, and must carry the code of the highest-precedence one. An effective for a patch the contract
    refuses is a violation. With neither there is nothing to judge: whether something had to be produced is the Gold
    oracle's (disposition and code). ``dataset_of``: the host's subject-to-dataset table (exact check)."""
    if effective is not None and refusal is not None:
        raise ValueError("pass the effective the reducer returned or the code it refused with, not both")
    out: list[dict] = []

    def bad(rule: str, detail: str) -> None:
        out.append({"rule": rule, "detail": detail})

    ops = patch["ops"]
    if refusal is not None:
        _check_refusal(base, patch, refusal, bad)
        return out
    if effective is None:
        return out  # nothing was produced: whether it had to be is the Gold oracle's (disposition and code)
    expected = expected_refusal(base, patch)
    if expected is not None:
        bad("T-REFUSAL", f"the contract refuses this patch ({expected}) and the reducer produced an effective")
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
