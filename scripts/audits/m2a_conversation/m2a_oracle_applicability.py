"""M2A-8a0 oracle, part 3: applicability. ONE authority for dependencies (U19): contract section 19.

A case is measurable by M2A-3 (``APLICABLE_M2A3``) unless one of these fires, in this order:

1. its dependency in the sealed extraction of section 19 (``gold/CONTRACT_S19_DEPENDENCIES.json``, schema
   ``m2a-contract-s19/2``, read through ``m2a_oracle_gold.s19`` after the seal verifies it) has ``impacto_m2a3 =
   COMPOSICION``: the reducer needs an input that another lane produces and does not exist yet, so the case is
   ``DEPENDIENTE(<owner of §19>)``, a declared red. A dependency with ``impacto_m2a3 = LECTURA`` is the reading of the TEXT
   by an authority: compose receives the value already typed (here, by the Gold), so the case stays measurable and the
   dependency is only recorded;
2. one of the STRUCTURAL rules below, which exist only for what §19 does not cover, each with its clause: a pending prior
   (the response protocol is M2A-4's) and an event or a pagination turn (the runtime protocol is M2A-6's).

There is no other rule, no recorded user decision and no local override. In particular an op flagged ``unresolved`` is NOT
a dependency: the mark says that compose refuses it (F16's fail-closed branch, a KEEP of an empty slot, a slot conflict...)
or that the value could not be typed, and the second is a typing outcome (``NO_MEDIBLE(<motive>)``), never an owner. What
the Gold does not let the oracle type or determine without guessing is ``NO_MEDIBLE(<motive>)``, declared with its motive.

Nothing here is a lexicon: every predicate reads typed fields and closed labels.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import NamedTuple

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class Rule(NamedTuple):
    id: str
    owners: tuple[str, ...]
    clause: str
    why: str
    predicate: object


def _any(rec, predicate) -> bool:
    return any(predicate(label) for label in (rec["label_A"], rec["label_B"]))


def _rule_pending(rec):
    return _any(rec, lambda L: L["prior"]["state"] == "PENDING"
                or any(o["op"] in ("ANSWER", "CANCEL_PENDING") for o in L["ops"]))


def _rule_protocol(rec):
    return rec["m2a_scope"] == "IN_PROTOCOL" or _any(
        rec, lambda L: L["input_kind"] == "EVENT" or L["turn_kind"] == "PROTOCOL" or L["m2a_scope"] == "IN_PROTOCOL")


STRUCTURAL_RULES = (
    Rule("PENDING_PRIOR", ("M2A-4",), "§8.3; plan M2A-4",
         "a pending prior, an ANSWER or CANCEL_PENDING op: the response protocol is M2A-4's, and §19 does not cover it",
         _rule_pending),
    Rule("EVENT_OR_PROTOCOL", ("M2A-6",), "§7.1 #3, §9; plan M2A-6",
         "events and pagination are the runtime protocol of M2A-6, and §19 does not cover them", _rule_protocol),
)
STRUCTURAL_OWNERS = tuple(dict.fromkeys(o for r in STRUCTURAL_RULES for o in r.owners))


def blocking(rec: dict, s19: dict) -> tuple[list[str], list[str], dict | None]:
    """``(rules that block, owners by precedence, the §19 facts of the case)``.

    ``rules`` are the ids of the structural rules and the §19 dependencies with ``impacto_m2a3 = COMPOSICION`` that fire;
    ``owners`` are THEIR owners (those of a LECTURA dependency do not block), the structural owners first, then the order
    of the closed owner list of the extraction. The §19 facts (every dependency, impact and owner) are returned whether or
    not they block, so that a measurable case still says what it reads."""
    gid = rec["gold_id"]
    rows = {d["dep_id"]: d for d in s19["dependencies"]}
    deps = s19["case_dependencies"].get(gid, [])
    facts = None if not deps else {"dependencies": list(deps), "impacto_m2a3": s19["case_impacto_m2a3"][gid],
                                   "owners": list(s19["case_owners"][gid])}
    rules, owners = [r.id for r in STRUCTURAL_RULES if r.predicate(rec)], set()
    for rule in STRUCTURAL_RULES:
        if rule.id in rules:
            owners |= set(rule.owners)
    for dep in deps:
        if rows[dep]["impacto_m2a3"] == "COMPOSICION":
            rules.append(dep)
            owners |= set(rows[dep]["owners"])
    order = {o: i for i, o in enumerate((*STRUCTURAL_OWNERS, *s19["owners"]))}
    return rules, sorted(owners, key=lambda o: (order.get(o, len(order)), o)), facts


def rule_table(s19: dict) -> list[dict]:
    """The rules as data (they go into the manifest): the structural ones with their clause, and the §19 rows with the
    impact that decides whether they block."""
    rows = [{"id": r.id, "basis": "STRUCTURAL", "owners": list(r.owners), "clause": r.clause, "why": r.why}
            for r in STRUCTURAL_RULES]
    rows += [{"id": d["dep_id"], "basis": "§19", "owners": list(d["owners"]), "impacto_m2a3": d["impacto_m2a3"],
              "blocks": d["impacto_m2a3"] == "COMPOSICION", "cases": d["cases_count"], "needs": d["needs"],
              "clause": f"§19 {d['dep_id']}: {d['scope']}"} for d in s19["dependencies"]]
    return rows
