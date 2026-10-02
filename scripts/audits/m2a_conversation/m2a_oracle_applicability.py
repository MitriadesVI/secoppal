"""M2A-8a0 oracle, part 3: applicability, generated mechanically from Gold fields (never classified by hand).

A case is measurable by M2A-3 (``APLICABLE_M2A3``) unless one of the rules below fires. A rule fires on a predicate over
the fields of BOTH labels of the Gold record (prior IR, ops, resulting IR, alternatives, input kind, turn kind, scope)
and names the owner whose rule or authority must exist before the case can be measured (contract §19, §6.4). All rules
that fire are recorded; the primary owner is the first by ``OWNER_ORDER``.

  M2A-4      pending protocol (§8.3): a pending prior, an ANSWER or CANCEL_PENDING op.
  M2A-6      event or protocol turn (§7.1 #3, §9).
  F          derived Product View / ``HostViewDecision`` (§14): the request is not representable yet.
  E-VALUE    SMMLV (§15.5), unit inherited from the previous value (§15.4).
  E-TIME     relative or inherited temporal arithmetic (§16).
  E-GEO      REGION (members and the closed list are E-GEO's, H7).
  E-BUYER/E-GEO  «con X» after an ACTOR anchor (H5): buyer or place needs both authorities, the host has no lexicon.
  D-BUYER    a new territory next to a buyer (H6): needs the buyer's ``buyer.territory`` provenance.
  E-STATE / E-MODALITY  a STATE/MODALITY set, or a universe change that carries one (H8): the literals per dataset
             are the authority's.
  E-<DIM>    a value op the authority of its dimension left unresolved (the labels mark it): the reducer would have to
             refuse with ``UNRESOLVED_VALUE:<DIM>`` and the authority is not the reducer's.
  <owner>    a user decision about a case (``APPLICABILITY_DECISIONS.json``), e.g. a capability pending downstream.

The dependency is decided by the STRUCTURE of the Gold record, never by a list of reason codes kept here: the reason is
validated against the sealed checker's vocabulary and carried as data, so a new code of the reducer's own (a new rule
that refuses with its own code) needs no change in this module.

Not a dependency: a unit shared inside a range (``SIBLING_IN_RANGE``) is the VALUE authority's reading in ``resolve``,
not inheritance between turns (§15.3): the patch already carries both bounds typed. Neither is a referent
(``refers_to_result_set``): it is continuation evidence (§7.2) and the reducer composes the delta.

A rule is deliberately broad where the host cannot decide without an authority fact: failing closed on a missing fact
would turn a Gold EXECUTE into a CLARIFY, so the case is declared red with its owner instead of being measured against
a reducer that has to guess. Nothing here is a lexicon: every predicate reads typed fields and closed labels.
"""

from __future__ import annotations


def irs(label: dict) -> list[dict]:
    out = [label["prior"]["ir"]] if label["prior"].get("ir") else []
    return out + ([label["resulting_ir"]] if label["resulting_ir"] else []) + list(label.get("alternatives") or [])


def conds(label: dict) -> list[dict]:
    return [c for ir in irs(label) for c in ir["conditions"]]


def value_ops(label: dict) -> list[dict]:
    return [o for o in label["ops"] if o["op"] in ("SET", "SET_ORDER", "ANSWER")]


def prior_dims(label: dict) -> set[str]:
    return {c["dimension"] for c in (label["prior"]["ir"] or {"conditions": []})["conditions"]}


def set_dims(label: dict) -> set[str]:
    return {o["slot"].partition(".")[0] for o in label["ops"] if o["op"] == "SET"}


def _any(rec, predicate) -> bool:
    return any(predicate(label) for label in (rec["label_A"], rec["label_B"]))


def _rule_pending(rec):
    return _any(rec, lambda L: L["prior"]["state"] == "PENDING"
                or any(o["op"] in ("ANSWER", "CANCEL_PENDING") for o in L["ops"]))


def _rule_protocol(rec):
    return rec["m2a_scope"] == "IN_PROTOCOL" or _any(
        rec, lambda L: L["input_kind"] == "EVENT" or L["turn_kind"] == "PROTOCOL" or L["m2a_scope"] == "IN_PROTOCOL")


def _rule_view(rec):
    return _any(rec, lambda L: any(ir["universe"] == "UNSPECIFIED" or ir["product_view"] is not None
                                   or ir["view_source"] == "DERIVED" for ir in irs(L)))


def _rule_smmlv(rec):
    return _any(rec, lambda L: any(c.get("unit") == "SMMLV" for c in conds(L)))


def _rule_unit(rec):
    return _any(rec, lambda L: any(o["inherited"].get("unit_source") == "PREVIOUS_ACCEPTED_VALUE" for o in L["ops"]))


def _rule_time(rec):
    clock = {"PREVIOUS_ACCEPTED_TIME", "RELATIVE_TO_CLOCK", "CLOCK_INFERRED"}
    return _any(rec, lambda L: any(c["dimension"] == "TIME" and c["operator"] == "relative_period" for c in conds(L))
                or any(o["inherited"].get("year_source") in clock or (
                    o["slot"] == "TIME" and o["depends_on_prior_value"] and o["op"] in ("SET", "ANSWER"))
                    for o in L["ops"]) or "RELATIVE_ANCHOR" in (L.get("gap_codes") or []))


def _rule_region(rec):
    return _any(rec, lambda L: any(c["dimension"] == "GEOGRAPHY" and c["level"] == "REGION" for c in conds(L))
                or any(o["slot"] == "GEOGRAPHY" and o["level"] == "REGION" for o in L["ops"]))


def _rule_h5(rec):  # «con X» after an ACTOR anchor: buyer or place (F12); the host has no lexicon to tell
    return _any(rec, lambda L: "ACTOR" in prior_dims(L) and bool(set_dims(L) & {"BUYER", "GEOGRAPHY"}))


def _rule_h6(rec):  # a new territory next to a buyer needs its buyer.territory provenance (F30)
    return _any(rec, lambda L: "GEOGRAPHY" in set_dims(L) and "BUYER" in (prior_dims(L) | set_dims(L)))


def _rule_h8(dimension):  # STATE / MODALITY must exist in the resulting universe (F27): the literals are the authority's
    return lambda rec: _any(rec, lambda L: dimension in set_dims(L) or (
        any(o["op"] == "SET_UNIVERSE" for o in L["ops"]) and dimension in (prior_dims(L) | set_dims(L))))


LANE_OF_DIMENSION = {  # the authority that must resolve a value of each dimension (E-* lanes of ADR-103)
    "TOPIC": "E-TOPIC", "BUYER": "E-BUYER", "ACTOR": "E-ACTOR", "GEOGRAPHY": "E-GEO", "TIME": "E-TIME",
    "VALUE": "E-VALUE", "STATE": "E-STATE", "MODALITY": "E-MODALITY", "CONTRACT_TYPE_OR_NATURE": "E-CONTRACT_TYPE",
    "ORDER": "E-ORDER", "CARDINALITY": "E-ORDER"}


def unresolved_owners(rec) -> tuple[str, ...]:
    """The lanes whose authority left a value op unresolved in either label."""
    dims = {o["slot"].partition(".")[0] for label in (rec["label_A"], rec["label_B"]) for o in value_ops(label)
            if o["unresolved"]}
    return tuple(sorted(LANE_OF_DIMENSION[d] for d in dims))


# (rule id, owners, contract reference, predicate over the Gold record): ALL that fire are recorded. ``owners`` may be
# a function of the record when the owner depends on what the record carries.
DEPENDENCY_RULES = (
    ("PENDING_PRIOR", ("M2A-4",), "§8.3", _rule_pending),
    ("EVENT_OR_PROTOCOL", ("M2A-6",), "§7.1 #3, §9", _rule_protocol),
    ("DERIVED_VIEW", ("F",), "§14", _rule_view),
    ("SMMLV", ("E-VALUE",), "§15.5, §19", _rule_smmlv),
    ("UNIT_INHERITED", ("E-VALUE",), "§15.4, §19", _rule_unit),
    ("TIME_RELATIVE_OR_INHERITED", ("E-TIME",), "§16, §19", _rule_time),
    ("REGION", ("E-GEO",), "H7, §19", _rule_region),
    ("COUNTERPART_AFTER_ACTOR", ("E-BUYER", "E-GEO"), "H5", _rule_h5),
    ("NEW_TERRITORY_NEXT_TO_BUYER", ("D-BUYER",), "H6, §19", _rule_h6),
    ("STATE_IN_UNIVERSE", ("E-STATE",), "H8", _rule_h8("STATE")),
    ("MODALITY_IN_UNIVERSE", ("E-MODALITY",), "H8", _rule_h8("MODALITY")),
    ("UNRESOLVED_AUTHORITY_VALUE", unresolved_owners, "§6.1", lambda rec: bool(unresolved_owners(rec))),
)
OWNER_ORDER = ("M2A-4", "M2A-5", "M2A-6", "F", "E-VALUE", "E-TIME", "E-BUYER", "D-BUYER", "E-GEO", "E-STATE",
               "E-MODALITY")
USER_DECISION = "USER_DECISION"


def dependencies(rec: dict, decisions: dict[str, dict] | None = None) -> tuple[list[str], list[str]]:
    """(rules that fired, owners by precedence) for a Gold record, from its fields and the user's decisions."""
    rules, owners = [], set()
    for rule_id, rule_owners, _ref, predicate in DEPENDENCY_RULES:
        if predicate(rec):
            rules.append(rule_id)
            owners |= set(rule_owners(rec) if callable(rule_owners) else rule_owners)
    decision = (decisions or {}).get(rec["gold_id"])
    if decision:
        rules.append(USER_DECISION)
        owners.add(decision["owner"])
    order = {o: i for i, o in enumerate(OWNER_ORDER)}
    return rules, sorted(owners, key=lambda o: (order.get(o, len(order)), o))
