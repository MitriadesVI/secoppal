"""M2A-8a0 oracle, part 3: applicability, generated mechanically from Gold fields (never classified by hand).

A case is measurable by M2A-3 (``APLICABLE_M2A3``) unless a rule below fires or typing fails. Every rule cites its
clause and says what it rests on:

* ``§19`` rests on a row of the dependency table of contract §19 (the case needs an input that another lane produces);
* ``PLAN`` rests on the step of the implementation plan that owns the behaviour (M2A-4 pending protocol, M2A-6 events);
* ``STRUCTURAL`` is a rule declared here: the host cannot decide the clause without an authority fact the oracle can
  neither type nor guess (the reason is written with the rule).

A rule fires on a predicate over the fields of BOTH labels of the Gold record and names the owner whose rule or
authority must exist before the case can be measured. All rules that fire are recorded; the primary owner is the first
by ``OWNER_ORDER``. The dependency is decided by the STRUCTURE of the record, never by a list of reason codes: the
reason is validated against the sealed checker's vocabulary and carried as data.

Deliberately NOT dependencies (they stay measurable, with the reason):

* a unit shared inside a range (``SIBLING_IN_RANGE``): the VALUE authority's reading in ``resolve``, not inheritance
  between turns (§15.3); the patch already carries both bounds typed;
* a referent (``refers_to_result_set``): continuation evidence (§7.2); the reducer composes the delta;
* the surface forms of VALUE (``B5RANGE``, ``COMPARATOR_FORM``, ``NEGATED_COMPARATOR``), which §19 row «formas
  «entre X y Y», «mayores de», «pasen de»» assigns to D-MONEY: §19 is about the AUTHORITY reading the TEXT; compose
  receives the operators already typed, and here they come from the Gold. They are measurable only if both labels agree
  on operator and amount (``surface_form_problem``); if they disagree the case is NO_MEDIBLE.
* a relative window (``relative_period``, year from the clock): not a dependency but NO_MEDIBLE
  (``TIME_OPERATOR:relative_period``): the window is the TIME authority's first-turn reading and the Gold's labels
  disagree on its end, so no typed value is determined.

Nothing here is a lexicon: every predicate reads typed fields and closed labels.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import NamedTuple

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audits.m2a_conversation.m2a_oracle_gold import OracleError  # noqa: E402


class Rule(NamedTuple):
    id: str
    owners: object  # a tuple of owners, or a function of the record when the owner depends on what it carries
    basis: str  # "§19" | "PLAN" | "STRUCTURAL"
    clause: str
    why: str
    predicate: object


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


def written_dims(label: dict) -> set[str]:
    """Dimensions an op writes (SET or CLEAR): what the patch does not write is carried."""
    return {o["slot"].partition(".")[0] for o in label["ops"] if o["op"] in ("SET", "SET_ORDER", "CLEAR")}


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
    inherited = {"PREVIOUS_ACCEPTED_TIME", "CLOCK_INFERRED"}  # the year is not the user's: it is inherited or inferred
    return _any(rec, lambda L: any(
        o["inherited"].get("year_source") in inherited or (
            o["slot"] == "TIME" and o["depends_on_prior_value"] and o["op"] in ("SET", "ANSWER"))
        for o in L["ops"]) or "RELATIVE_ANCHOR" in (L.get("gap_codes") or []))


def _rule_region(rec):
    return _any(rec, lambda L: any(c["dimension"] == "GEOGRAPHY" and c["level"] == "REGION" for c in conds(L))
                or any(o["slot"] == "GEOGRAPHY" and o["level"] == "REGION" for o in L["ops"]))


def _rule_h6(rec):
    """A new territory next to a CARRIED buyer (F30): a buyer the patch writes is not carried and H6 does not apply."""
    return _any(rec, lambda L: "GEOGRAPHY" in set_dims(L) and "BUYER" in prior_dims(L) and "BUYER" not in written_dims(L))


def _rule_h8(dimension):
    return lambda rec: _any(rec, lambda L: dimension in set_dims(L) or (
        any(o["op"] == "SET_UNIVERSE" for o in L["ops"]) and dimension in (prior_dims(L) | set_dims(L))))


LANE_OF_DIMENSION = {  # the authority that must resolve a value of each dimension (E-* lanes of ADR-103)
    "TOPIC": "E-TOPIC", "BUYER": "E-BUYER", "ACTOR": "E-ACTOR", "GEOGRAPHY": "E-GEO", "TIME": "E-TIME",
    "VALUE": "E-VALUE", "STATE": "E-STATE", "MODALITY": "E-MODALITY", "CONTRACT_TYPE_OR_NATURE": "E-CONTRACT_TYPE",
    "ORDER": "E-ORDER", "CARDINALITY": "E-ORDER"}


def unresolved_owners(rec) -> tuple[str, ...]:
    """The lanes whose authority left a value op unresolved in either label."""
    out = set()
    for label in (rec["label_A"], rec["label_B"]):
        for o in value_ops(label):
            if o["unresolved"]:
                dim = o["slot"].partition(".")[0]
                if dim not in LANE_OF_DIMENSION:
                    raise OracleError(f"no authority lane declared for the dimension {dim!r} "
                                      f"(unresolved op in {rec['gold_id']}): add it to LANE_OF_DIMENSION on purpose")
                out.add(LANE_OF_DIMENSION[dim])
    return tuple(sorted(out))


RULES = (
    Rule("PENDING_PRIOR", ("M2A-4",), "PLAN", "§8.3; plan M2A-4",
         "a pending prior, an ANSWER or CANCEL_PENDING op: the response protocol is M2A-4's", _rule_pending),
    Rule("EVENT_OR_PROTOCOL", ("M2A-6",), "PLAN", "§7.1 #3, §9; plan M2A-6",
         "events and pagination are the runtime protocol of M2A-6", _rule_protocol),
    Rule("DERIVED_VIEW", ("F",), "§19", "§19 row «vista derivada»; §14",
         "HostViewDecision does not exist yet: a request with a derived view is not representable", _rule_view),
    Rule("SMMLV", ("E-VALUE",), "§19", "§19 row 4 (SMMLV en follow-ups); §15.5",
         "the conversion by vigencia (D10) is E-VALUE's: the typed value does not exist without it", _rule_smmlv),
    Rule("UNIT_INHERITED", ("E-VALUE",), "§19", "§19 row «herencia de unidad/año»; §15.4",
         "unit.inherited comes from the separate input of the host (3b)", _rule_unit),
    Rule("YEAR_INHERITED_OR_INFERRED", ("E-TIME",), "§19", "§19 rows 3 (A5) and «herencia de unidad/año»; §16.1, §16.3",
         "year.inherited / year.inferred / time.relative_to_previous come from the separate input of the host (3b)",
         _rule_time),
    Rule("REGION", ("E-GEO",), "§19", "§19 G-FU-07; §6.4 H7",
         "the members of a region and the closed list are E-GEO's: a GeoValue REGION cannot be typed without them",
         _rule_region),
    Rule("NEW_TERRITORY_NEXT_TO_CARRIED_BUYER", ("D-BUYER",), "§19", "§19 row F30; §6.4 H6",
         "H6 needs the carried buyer's buyer.territory provenance", _rule_h6),
    Rule("STATE_IN_UNIVERSE", ("E-STATE",), "STRUCTURAL", "§6.4 H8; §14",
         "StateValue without literals per dataset cannot say whether the state exists in the resulting universe "
         "(a reducer that fails closed would turn a Gold EXECUTE into a CLARIFY)", _rule_h8("STATE")),
    Rule("MODALITY_IN_UNIVERSE", ("E-MODALITY",), "STRUCTURAL", "§6.4 H8; §14",
         "ModalityValue: same as STATE_IN_UNIVERSE", _rule_h8("MODALITY")),
    Rule("UNRESOLVED_AUTHORITY_VALUE", unresolved_owners, "STRUCTURAL", "§6.1",
         "a value the authority of its dimension left unresolved is refused with that authority's code: the patch never "
         "carries a typed condition for it", lambda rec: bool(unresolved_owners(rec))),
)
OWNER_ORDER = ("M2A-4", "M2A-5", "M2A-6", "F", "E-VALUE", "E-TIME", "E-BUYER", "D-BUYER", "E-GEO", "E-STATE",
               "E-MODALITY")
USER_DECISION = "USER_DECISION"


def surface_form_problem(rec: dict, surface_form_gaps) -> bool:
    """§19's surface forms are measurable only when both labels agree on the typed operation (operator, polarity and
    side); the amount agreement is the typing's (both labels' ops and results must type equal)."""
    A, B = rec["label_A"], rec["label_B"]
    if not (set(A.get("gap_codes") or []) | set(B.get("gap_codes") or [])) & set(surface_form_gaps):
        return False

    def signature(label):
        return sorted((o["op"], o["slot"], o["operator"], o["polarity"]) for o in label["ops"])
    return signature(A) != signature(B)


def dependencies(rec: dict, decisions: dict[str, dict] | None = None) -> tuple[list[str], list[str]]:
    """(rules that fired, owners by precedence) for a Gold record, from its fields and the user's decisions."""
    rules, owners = [], set()
    for rule in RULES:
        if rule.predicate(rec):
            rules.append(rule.id)
            owners |= set(rule.owners(rec) if callable(rule.owners) else rule.owners)
    decision = (decisions or {}).get(rec["gold_id"])
    if decision:
        rules.append(USER_DECISION)
        owners.add(decision["owner"])
    order = {o: i for i, o in enumerate(OWNER_ORDER)}
    return rules, sorted(owners, key=lambda o: (order.get(o, len(order)), o))


def rule_table() -> list[dict]:
    """The rules as data (they go into the manifest: the exclusions the oracle makes, each with its clause)."""
    return [{"id": r.id, "owners": list(r.owners) if not callable(r.owners) else "E-<DIMENSION OF THE UNRESOLVED OP>",
             "basis": r.basis, "clause": r.clause, "why": r.why} for r in RULES]
