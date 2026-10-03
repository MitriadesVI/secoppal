"""M2A-7 against the sealed ``M2A_GOLD``: «eco mínimo del Gold ⊆ emitido ⊆ justificado» (plan, fila M2A-7).

The Gold's echo is read from the labels: what they store (``expected_echo``: the synthetic slice stores it; labeler B
fills it, A left it empty) UNION what Gold schema 2.1 §7 derives («sí y sólo sí»; the REAL slice stores no derived
field) plus the contract's two additions (§11, «Derivación exacta»: a relative to the previous referent also emits
RELATIVE_WINDOW_RESOLVED; every turn over an expired anchor that does not confirm it emits CONTEXT_EXPIRED).
``gold_echo`` is written HERE from the schema's table, over the labels' own fields, not from ``foundation_echo``, and it
is BOTH bounds: ``emitted`` must equal it, there is no list of allowed extras.

Each case is measured with the typed turn the M2A-3 oracle already gives it (``tests/test_m2a3_oracle_link``: the base, the
patch and the effective that ``delta_outcome`` composes), through ``echo_items`` (or ``echo_items_for_answer``). Five verdicts, and only five:

* GREEN: ``gold == emitted``;
* RED_S19: the case is a declared dependency of §19 (the sealed extraction ``CONTRACT_S19_DEPENDENCIES.json`` is the ONLY
  authority: U19/U23) and the echo is proved green once the dependency's owner supplies its input: a SYNTHETIC receipt
  (``topn.capped``: ``DEP-E-ORDER``; ``time.relative_to_previous``: ``DEP-TIME-RELATIVE``), or the case is untypable by the
  oracle because of a ``DEP-*`` row it names (``DEP-DERIVED-VIEW``: ``VIEW_*`` unrepresentable, ``DEP-MODALITY-UNIVERSE``…);
* MEASURED_AT_INTEGRATION: the oracle cannot type the turn and ITS OWN ``owners`` are the integration lanes (M2A-4 builds
  the pending, M2A-6 builds the patch of a button event): the measurement exists only where those lanes meet this one, and
  this lane cannot import them. It is not a defect of a sealed artifact. For the six cases that answer, or cancel, a
  pending WITHOUT an anchor (SF056, SF058, SF059, SF060, SF188, SF191) ``test_foundation_echo`` carries the equivalent
  scenario, built with the real reducer, and this file checks its echo against the label (M2A7-D22);
* FAIL_EXTERNAL: the echo cannot match and the cause is DEMONSTRATED in a sealed artifact outside this lane: the oracle
  cannot type the turn for a reason of its own (``NO_MEDIBLE``…), its typing contradicts the label (a ``year_source`` the
  Gold op does not state, or omitted), or the sealed checker's ``derive_effects`` contradicts the Gold text. It counts
  apart and never makes a case green;
* FAIL: a defect of this lane.

Partition discipline (as M2A-3's): pytest measures DEV only. TEST is measured ONCE, at freeze, with
``python -m tests.test_foundation_echo_gold TEST``. With the private REAL directory (``M2A_GOLD_REAL_DIR``, never in git)
the REAL slice is measured too; nothing of it ever leaves this module but ids and class names.
"""
from __future__ import annotations

import os
import sys
from dataclasses import replace
from functools import lru_cache
from pathlib import Path

import pytest

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
import app.core.foundation_echo as fe
from app.core.foundation_authority_values import decode_value
from app.core.semantic_dimensions import SemanticDimension as D
from scripts.audits.m2a_conversation import m2a_oracle_gold as gold
from scripts.audits.m2a_conversation import m2a_reducer_oracle as oracle
from tests.test_foundation_echo import CASES, GOLD_ANCHORED_PENDINGS, GOLD_FIRST_TURN_PENDINGS, make_record
from tests.test_m2a3_oracle_link import base_of, run

# §10.1: the plazo is 30 minutes since the previous turn; the sealed Gold (schema §5.3) and the sealed checker
# (``check_m2a_gold_contract.py``) say the anchor is expired when ``gap_seconds > 1800``. ONE threshold in this file
# (``m2a3_oracle_link.facts_of`` reads ``>=``: a follow-up for M2A-3's link, which this lane does not touch).
EXPIRY_SECONDS = 30 * 60


def expired(gap) -> bool:
    return gap is not None and gap > EXPIRY_SECONDS


GREEN, RED_S19, MEASURED_AT_INTEGRATION, FAIL_EXTERNAL, FAIL = (
    "GREEN", "RED_S19", "MEASURED_AT_INTEGRATION", "FAIL_EXTERNAL", "FAIL")
INTEGRATION_OWNERS = frozenset({"M2A-4", "M2A-6"})  # the lanes where a pending / an event patch is built
# What the owner of a §19 dependency will supply, as a SYNTHETIC input proving the echo maps it when present.
RECEIPT_OF = {"TOPN_CAPPED": fe.RULE_TOPN_CAPPED}
S19_OF = {"TOPN_CAPPED": "DEP-E-ORDER", "RELATIVE_WINDOW_RESOLVED": "DEP-TIME-RELATIVE",
          "VIEW_DERIVED": "DEP-DERIVED-VIEW", "VIEW_REDERIVED": "DEP-DERIVED-VIEW"}
YEAR_SOURCE_OF = {"RELATIVE_WINDOW_RESOLVED": fa.YearSource.RELATIVE_TO_CLOCK,
                  "YEAR_INHERITED": fa.YearSource.PREVIOUS_ACCEPTED_TIME,
                  "YEAR_CLOCK_INFERRED": fa.YearSource.CLOCK_INFERRED}


# ---- the Gold's echo, from Gold schema 2.1 §7 over the labels' own fields -----------------------------------------------
def _executed(label: dict) -> bool:
    return label["expected_disposition"] in ("EXECUTE", "NEW_SEARCH")


def _survivors(label: dict) -> bool:
    """CARRIED (§7): a delta or an answer, executed, with at least one condition, the order, the cardinality or the said
    universe of the previous turn that survives with no operation touching it (a KEEP touches nothing: §7 YEAR)."""
    prior_ir = label["prior"].get("ir")
    if prior_ir is None or label["turn_kind"] not in ("DELTA", "ANSWER_TO_CLARIFICATION") or not _executed(label):
        return False
    chk = gold.checker()
    ops = [o for o in label["ops"] if o["op"] != "KEEP"]
    touched = chk.touched(ops)
    universe_said = prior_ir["universe"] != "UNSPECIFIED" and not any(o["op"] == "SET_UNIVERSE" for o in ops)
    standing = [c for c in prior_ir["conditions"] if chk.slot_of(c) not in touched]
    order = prior_ir.get("order") is not None and "ORDER" not in touched
    cardinality = prior_ir.get("cardinality") is not None and "CARDINALITY" not in touched
    return universe_said or bool(standing) or order or cardinality


def gold_echo(label: dict) -> set[str]:
    """§7 «sí y sólo sí», plus the contract's additions: a stored echo is unioned (a label may know more than the table)."""
    out = set(label.get("expected_echo") or [])
    over_expired = expired(label["prior"].get("gap_seconds"))
    if label["expected_disposition"] == "CLARIFY" and label["disposition_reason"] == "CONTEXT_EXPIRED_CONFIRM":
        return {"CONTEXT_EXPIRED"} | out
    if not _executed(label):
        return out | ({"CONTEXT_EXPIRED"} if over_expired else set())  # R3d: every turn over an expired anchor not confirmed
    chk = gold.checker()
    prior_ir, res = label["prior"].get("ir"), label["resulting_ir"]
    effects = set(chk.derive_effects(label)) | set(label.get("effects") or [])
    if _survivors(label):
        out.add("CARRIED")
    for op in label["ops"]:
        inherited = op.get("inherited") or {}
        out |= {"PREVIOUS_ACCEPTED_VALUE": {"UNIT_INHERITED"}, "SIBLING_IN_RANGE": {"UNIT_SHARED_IN_RANGE"}}.get(
            inherited.get("unit_source"), set())
        out |= {"PREVIOUS_ACCEPTED_TIME": {"YEAR_INHERITED"}, "CLOCK_INFERRED": {"YEAR_CLOCK_INFERRED"},
                "RELATIVE_TO_CLOCK": {"RELATIVE_WINDOW_RESOLVED"}}.get(inherited.get("year_source"), set())
        if inherited.get("year_source") == "PREVIOUS_ACCEPTED_TIME" and "RELATIVE_ANCHOR" in label["gap_codes"]:
            out.add("RELATIVE_WINDOW_RESOLVED")  # §11 «Derivación exacta», first addition: a relative to the previous referent
    out |= effects & {"VALUE_RANGE_REPLACED", "POLARITY_INVERTED", "CONTEXT_RESET", "TOPIC_REPLACED", "UNIVERSE_CHANGED",
                      "TOPN_CARRIED", "TOPN_CAPPED", "ORDER_DIRECTION_DEFAULTED"}
    if res is not None:
        if res.get("order") and res["order"].get("direction_source") == "DEFAULTED":
            out.add("ORDER_DIRECTION_DEFAULTED")
        if res.get("cardinality") and res["cardinality"]["n_requested"] > res["cardinality"]["n"]:
            out.add("TOPN_CAPPED")
        if res.get("view_source") == "DERIVED":
            out.add("VIEW_DERIVED")
        if label["turn_kind"] != "NEW_SEARCH" and prior_ir is not None:
            positive = [c["value_hint"] for c in prior_ir["conditions"] if c["dimension"] == "TOPIC" and c["polarity"] == "POSITIVE"]
            mine = [o["value_hint"] for o in label["ops"] if o["op"] == "SET" and o["slot"] == "TOPIC" and o["polarity"] == "POSITIVE"]
            if mine and any(p not in mine for p in positive):
                out.add("TOPIC_REPLACED")
            if any(o["op"] == "SET_UNIVERSE" for o in label["ops"]) and res["universe"] != prior_ir["universe"]:
                out.add("UNIVERSE_CHANGED")
            if (prior_ir.get("cardinality") is not None and res.get("cardinality") == prior_ir["cardinality"]
                    and any(o["op"] == "SET_ORDER" for o in label["ops"])):
                out.add("TOPN_CARRIED")
    if "VIEW_REDERIVED" in effects:
        out |= {"VIEW_REDERIVED", "VIEW_DERIVED"}
    return out


def gold_labels(real_dir: str | None) -> dict[str, dict]:
    """``gold_id -> the labels' echo`` (union of both labelers: B fills it, A may have left it empty)."""
    out = {}
    for source, record in gold.gold_records(None if real_dir is None else Path(real_dir)):
        out[record["gold_id"]] = {"source": source, "echo": gold_echo(record["label_A"]) | gold_echo(record["label_B"]),
                                  "stored": set(record["label_A"].get("expected_echo") or [])
                                  | set(record["label_B"].get("expected_echo") or [])}
    return out


# ---- the typed turn of a case, and its echo ------------------------------------------------------------------------------
RESOLUTION = {"CLARIFY": fa.TurnResolution.CLARIFY, "REASK": fa.TurnResolution.REASK, "INFORM": fa.TurnResolution.INFORM}


@lru_cache(maxsize=None)
def cases(real_dir: str | None = None) -> tuple:
    return tuple(oracle.load_cases(None if real_dir is None else Path(real_dir)))


def fresh_effective(patch: fa.SemanticPatch) -> fa.AcceptedInterpretation:
    """A new search's effective, as ``from_acceptance`` (M2A-1b) will build it: the patch's conditions and its universe."""
    redirect = next(op for op in patch.ops if op.op is fa.PatchOpKind.REPLACE_UNIVERSE)
    target = redirect.universe
    request = fa.AcceptedRequest("r1", "s1", target.subject, target.universe_kind, redirect.origin, None,
                                 fd.SUBJECT_DATASET[target.subject])
    conditions = tuple(replace(op.condition, request_ids=("r1",)) for op in patch.ops if op.condition is not None)
    return fa.canonical_interpretation((request,), conditions, None, (), None, "t-root")


def _map_conditions(patch: fa.SemanticPatch, change) -> fa.SemanticPatch:
    ops = []
    for op in patch.ops:
        if op.condition is not None:
            new = change(op.condition)
            op = op if new is op.condition else replace(op, condition=new)
        ops.append(op)
    return fa.SemanticPatch(tuple(ops), patch.origin_kind)


def with_receipt(patch: fa.SemanticPatch, echo_class: str) -> fa.SemanticPatch:
    """The SYNTHETIC receipt the authority will emit (the oracle types ``receipt_rules`` as empty): the cap's, on the
    CARDINALITY condition of the patch whose figure exceeds what ran (the receipt belongs to the resolved condition, so the
    patch's operation carries it and ``compose`` passes it through)."""
    rule = RECEIPT_OF[echo_class]
    return _map_conditions(patch, lambda c: replace(c, receipt_rules=tuple(sorted({*c.receipt_rules, rule})))
                           if c.dimension is D.CARDINALITY and decode_value(c.value).capped else c)


def with_relative_receipt(patch: fa.SemanticPatch) -> fa.SemanticPatch:
    """The receipt ``time.relative_to_previous`` (§16.3, D-TIME) on the TIME conditions whose year is inherited."""
    return _map_conditions(patch, lambda c: replace(c, receipt_rules=tuple(sorted({*c.receipt_rules, fe.RULE_RELATIVE_TO_PREVIOUS})))
                           if c.dimension is D.TIME and c.provenance.year_source is fa.YearSource.PREVIOUS_ACCEPTED_TIME else c)


def with_year_source(patch: fa.SemanticPatch, echo_class: str) -> fa.SemanticPatch:
    """The year provenance the Gold op states, on the TIME conditions the oracle's typing left without one."""
    source = YEAR_SOURCE_OF[echo_class]
    return _map_conditions(patch, lambda c: replace(c, provenance=replace(c.provenance, year_source=source))
                           if c.dimension is D.TIME and c.provenance.year_source is None else c)


def with_stated_year(patch: fa.SemanticPatch) -> fa.SemanticPatch:
    """The typing a label that states no ``year_source`` implies: the year of a bound is the user's own (STATED)."""
    return _map_conditions(patch, lambda c: replace(c, provenance=replace(c.provenance, year_source=fa.YearSource.STATED))
                           if c.provenance.year is not None and c.provenance.year_source is not fa.YearSource.STATED else c)


def silent_emitted(label_gap, disposition: str) -> set[str]:
    record = make_record(RESOLUTION[disposition])
    return {item.echo_class.value for item in fe.echo_items(None, None, None, record, anchor_expired=expired(label_gap))}


def executed_emitted(case: dict, change=None) -> tuple[set[str] | None, str | None]:
    """``(classes emitted, why it could not run)``; ``change`` is a SYNTHETIC input an authority will provide."""
    actual, scope, problem = run(case["inputs"])
    if problem is not None or actual is None:
        return None, problem or "UNTYPED"
    if actual["disposition"] not in ("NEW_SEARCH", "EXECUTE"):
        return None, f"NOT_EXECUTED:{actual['disposition']}"
    patch = fa.SemanticPatch.from_dict(case["inputs"]["patch"])
    if change is not None:
        patch = change(patch)
    over_expired = expired(case["inputs"]["context"].get("gap_seconds"))
    base = base_of(case["inputs"]["base"])
    if actual["disposition"] == "NEW_SEARCH":
        effective, resolution, patch_arg, base_trace = fresh_effective(patch), fa.TurnResolution.NEW_SEARCH, None, None
    else:
        composed = fd.compose(base, patch, base_trace_id="t0")
        assert isinstance(composed, fd.Composed)
        effective, resolution, patch_arg, base_trace = composed.effective, fa.TurnResolution.DELTA, patch, "t0"
        if change is None:  # the effective the oracle froze is the one the reducer composes
            assert effective.to_dict() == actual["effective"]
    record = make_record(resolution, patch=patch_arg, effective=effective, base_trace_id=base_trace)
    items = fe.echo_items(base, effective, patch_arg, record, anchor_expired=over_expired)
    return {item.echo_class.value for item in items}, None


# ---- the measurement -------------------------------------------------------------------------------------------------------
def s19_dependencies(case: dict) -> tuple[str, ...]:
    """The §19 rows that list this case: the sealed extraction as the oracle read it (the one authority)."""
    return tuple(case["s19"]["dependencies"]) if case["s19"] is not None else ()


def untypable(case: dict, wanted: set[str], why: str) -> tuple[str, str]:
    """A case the oracle cannot type: RED_S19 when ITS OWN cause is a §19 row (a ``DEP-*`` rule that §19 lists for the case),
    FAIL_EXTERNAL otherwise (pendings, protocol events, ``NO_MEDIBLE``…: the sealed oracle's own statement)."""
    named = [rule for rule in case["rules"] if rule.startswith("DEP-") and rule in s19_dependencies(case)]
    owes = ("; owes " + ",".join(sorted(wanted))) if wanted else ""
    owners = set(case["owners"])
    if named:
        return RED_S19, f"§19 {', '.join(named)} (the oracle cannot type it: {case['applicability']}){owes}"
    if owners and owners <= INTEGRATION_OWNERS:
        return MEASURED_AT_INTEGRATION, (f"owners {sorted(owners)} rules={case['rules']}: built by an integration lane{owes}")
    return FAIL_EXTERNAL, f"the sealed oracle cannot type it: {case['applicability']} rules={case['rules']}{owes}"


def typed_year_sources(case: dict) -> set[fa.YearSource]:
    patch = fa.SemanticPatch.from_dict(case["inputs"]["patch"])
    return {op.condition.provenance.year_source for op in patch.ops
            if op.condition is not None and op.condition.provenance.year_source is not None}


def label_year_sources(record: dict) -> set[fa.YearSource]:
    return {fa.YearSource(op["inherited"]["year_source"]) for lab in ("label_A", "label_B") for op in record[lab]["ops"]
            if (op.get("inherited") or {}).get("year_source") is not None}


def checker_contradicts_the_text(case: dict) -> bool:
    """The sealed checker's ``derive_effects`` marks VALUE_RANGE_REPLACED for a whole-VALUE ``CLEAR`` that no ``SET`` of a
    VALUE bound follows; Gold schema §6 says «No se marca: CLEAR(VALUE) sin SET» and §11 «seguido de SET». The proof is
    the typed patch itself (a whole-VALUE CLEAR, no VALUE SET)."""
    patch = fa.SemanticPatch.from_dict(case["inputs"]["patch"])
    cleared = any(op.op is fa.PatchOpKind.CLEAR and op.slot.dimension is D.VALUE and op.slot.side is None for op in patch.ops)
    return cleared and not any(op.op is fa.PatchOpKind.SET and op.slot.dimension is D.VALUE for op in patch.ops)


def explain(case: dict, record: dict, wanted: set[str], got: set[str]) -> tuple[str, str]:
    """Verdict of a typed case whose echo is not exactly the Gold's: every difference must be explained by a §19 row (and
    proved green with the owner's synthetic input) or by a demonstrated cause in a sealed artifact; else FAIL."""
    missing, extra = wanted - got, got - wanted
    deps, external, fixes, notes = s19_dependencies(case), [], [], []
    for name in sorted(missing):
        if name in S19_OF and S19_OF[name] in deps and name in RECEIPT_OF:
            fixes.append(lambda patch, name=name: with_receipt(patch, name))
            notes.append(f"{name}: §19 {S19_OF[name]}")
        elif name == "RELATIVE_WINDOW_RESOLVED" and "DEP-TIME-RELATIVE" in deps:
            fixes.append(with_relative_receipt)
            notes.append(f"{name}: §19 DEP-TIME-RELATIVE")
        elif name in YEAR_SOURCE_OF and YEAR_SOURCE_OF[name] in label_year_sources(record) - typed_year_sources(case):
            external.append(f"{name}: the oracle's typing omits year_source {YEAR_SOURCE_OF[name].value} that the Gold op states")
        elif name == "VALUE_RANGE_REPLACED" and checker_contradicts_the_text(case):
            external.append("VALUE_RANGE_REPLACED: the sealed checker's derive_effects vs Gold §6 / §11 (CLEAR(VALUE) with no SET)")
        else:
            return FAIL, f"missing {name}"
    for name in sorted(extra):
        if name in YEAR_SOURCE_OF and YEAR_SOURCE_OF[name] in typed_year_sources(case) - label_year_sources(record):
            external.append(f"{name}: the oracle's typing carries year_source {YEAR_SOURCE_OF[name].value} that the Gold op does not state")
        else:
            return FAIL, f"emitted {name}, which the Gold does not owe"
    if external:
        return FAIL_EXTERNAL, "; ".join(external)
    def together(patch):
        for fix in fixes:
            patch = fix(patch)
        return patch
    with_it, _ = executed_emitted(case, together)
    if with_it is None or with_it != wanted:
        return FAIL, f"{'; '.join(notes)}: not green even with the owner's input"
    return RED_S19, "; ".join(notes) + " (green with the synthetic input)"


def measure(partition: str, real_dir: str | None = None) -> dict:
    """``rows[gold_id] = (verdict, detail)``; verdict in GREEN, RED_S19, FAIL_EXTERNAL, FAIL."""
    labels = gold_labels(real_dir)
    records = {r["gold_id"]: r for _, r in gold.gold_records(None if real_dir is None else Path(real_dir))}
    chosen = oracle.select_partition(list(cases(real_dir)), partition)
    rows = {}
    for case in chosen:
        gid, wanted = case["gold_id"], labels[case["gold_id"]]["echo"]
        label = records[gid]["label_B"]
        if case["disposition"] in RESOLUTION:
            got = silent_emitted(label["prior"].get("gap_seconds"), case["disposition"])
            rows[gid] = (GREEN, "") if got == wanted else (FAIL, f"emitted {sorted(got)} owed {sorted(wanted)}")
            continue
        if (label["turn_kind"], label["input_kind"]) == ("PROTOCOL", "TEXT"):  # a page: the same plan again
            got = {item.echo_class.value for item in fe.echo_items(
                None, None, None, make_record(fa.TurnResolution.PROTOCOL_PAGE, base_trace_id="t0"), anchor_expired=False)}
            rows[gid] = (GREEN, "page") if got == wanted else (FAIL, f"a page emitted {sorted(got)} owed {sorted(wanted)}")
            continue
        if case["disposition"] == "HANDOFF_LEGACY":  # no M2A_GOLD case has it (§1.1): a harness error, not a category
            raise AssertionError(f"{gid}: HANDOFF_LEGACY is not a disposition of M2A_GOLD (a legacy owner writes no record, §10.2)")
        got, why = executed_emitted(case) if case["inputs"] is not None else (None, case["applicability"])
        if got is None:
            rows[gid] = untypable(case, wanted, why)
        elif got == wanted:
            rows[gid] = (GREEN, "")
        else:
            rows[gid] = explain(case, records[gid], wanted, got)
    return rows


@lru_cache(maxsize=None)
def dev(real_dir: str | None = None) -> dict:
    return measure("DEV", real_dir)


def test_the_gold_echo_of_the_stored_labels_is_what_section_7_derives():
    """The derivation written here from Gold schema 2.1 §7 agrees with what the labelers stored (B fills it): every stored
    class is derived (no class a label lists without a §7 rule behind it)."""
    labels = gold_labels(None)
    for gid, row in labels.items():
        assert row["stored"] <= row["echo"], gid
    synthetic = {gid: row for gid, row in labels.items() if row["source"] == "SYNTHETIC"}
    assert len(synthetic) == 160
    differs = sorted(gid for gid, row in synthetic.items() if row["stored"] and row["echo"] != row["stored"])
    # the one deliberate difference: §11's first addition (RELATIVE_WINDOW_RESOLVED for a relative to the previous
    # referent) which the labels, derived from Gold §7 alone, do not store (SF037, SF042, SF174)
    assert differs == ["SF037", "SF042", "SF174"], differs
    for gid in differs:
        assert labels[gid]["echo"] - labels[gid]["stored"] == {"RELATIVE_WINDOW_RESOLVED"}


def test_no_gold_case_hands_off_to_legacy_and_the_priors_of_the_pendings_carry_no_provenance():
    """M2A7-D24: the frames of the Gold's pendings hold no provenance the Gold can state (its IR carries a provenance only as
    an order direction and a capped top-N): so announcing the frame's provenance, as the new search would, changes no sealed
    case. And the harness has no HANDOFF_LEGACY case to classify (a legacy owner writes no record)."""
    records = list(gold.gold_records(Path(REAL_DIR) if REAL_DIR else None))
    assert all(r["label_B"]["expected_disposition"] != "HANDOFF_LEGACY" for _, r in records)
    pendings = [r for _, r in records if r["label_B"]["prior"]["state"] == "PENDING"]
    assert len(pendings) >= 20
    for record in pendings:
        for label in ("label_A", "label_B"):
            ir = record[label]["prior"]["ir"]
            assert ir is not None
            assert ir.get("order") is None or ir["order"].get("direction_source") != "DEFAULTED", record["gold_id"]
            assert ir.get("cardinality") is None or ir["cardinality"]["n_requested"] <= ir["cardinality"]["n"], record["gold_id"]
            assert ir.get("view_source") != "DERIVED" or ir.get("universe") == "UNSPECIFIED", record["gold_id"]


def test_the_expiry_threshold_is_the_one_of_the_sealed_gold():
    assert expired(1800) is False and expired(1801) is True and expired(None) is False


def test_dev_no_case_fails_and_no_case_is_dropped():
    rows = dev()
    assert {gid for gid, row in rows.items() if row[0] == FAIL} == set()
    assert set(rows) == {case["gold_id"] for case in oracle.select_partition(list(cases()), "DEV")}
    assert all(verdict in (GREEN, RED_S19, MEASURED_AT_INTEGRATION, FAIL_EXTERNAL) for verdict, _ in rows.values())


def test_dev_every_red_is_a_section_19_row_and_every_external_failure_has_its_demonstrated_cause():
    sealed = {c["gold_id"]: c for c in cases()}
    for gid, (verdict, detail) in dev().items():
        if verdict == RED_S19:
            assert detail and s19_dependencies(sealed[gid]), (gid, detail)  # the sealed extraction lists it
        if verdict == MEASURED_AT_INTEGRATION:
            assert set(sealed[gid]["owners"]) <= INTEGRATION_OWNERS and sealed[gid]["owners"], gid  # the oracle's own owners
        if verdict == FAIL_EXTERNAL:
            owners = set(sealed[gid]["owners"])
            assert detail and not (owners and owners <= INTEGRATION_OWNERS), gid  # an integration lane's would be MEASURED_AT_INTEGRATION


def test_dev_the_greens_are_not_vacuous():
    rows = dev()
    labels = gold_labels(None)
    greens = [gid for gid, (verdict, _) in rows.items() if verdict == GREEN]
    assert len(greens) >= 40
    owed = {name for gid in greens for name in labels[gid]["echo"]}
    assert {"CARRIED", "CONTEXT_EXPIRED", "CONTEXT_RESET", "UNIT_INHERITED", "UNIT_SHARED_IN_RANGE", "YEAR_CLOCK_INFERRED",
            "ORDER_DIRECTION_DEFAULTED", "VALUE_RANGE_REPLACED", "TOPIC_REPLACED", "POLARITY_INVERTED", "UNIVERSE_CHANGED",
            "TOPN_CARRIED"} <= owed


def test_dev_relative_to_the_previous_referent_is_a_declared_red_of_section_19_proved_with_the_receipt():
    rows = dev()
    owed = {gid for gid, row in gold_labels(None).items() if "RELATIVE_WINDOW_RESOLVED" in row["echo"]}
    relative = {gid for gid in rows if "DEP-TIME-RELATIVE" in s19_dependencies({c["gold_id"]: c for c in cases()}[gid])}
    assert relative and relative <= owed
    for gid in relative:
        assert rows[gid][0] == RED_S19 and "DEP-TIME-RELATIVE" in rows[gid][1] and "green with the synthetic input" in rows[gid][1]


def test_the_six_pendings_without_an_anchor_have_their_scenario_and_it_gives_the_label_echo():
    """M2A7-D22: the Gold fixes ``CARRIED`` for the answers (and the cancelling delta) over a first-turn pending. Each case
    is modelled by a scenario built with the real reducer (a frame typed as P resolved it, an answer composed by
    ``compose``), whose echo is exactly the label's; and each is measured only at integration (not a sealed artifact's
    defect), never green or red-by-§19 here."""
    by_name = {case.name: case for case in CASES}
    labels = gold_labels(None)
    assert set(GOLD_FIRST_TURN_PENDINGS) == {"SF056", "SF058", "SF059", "SF060", "SF188", "SF191"}
    for gid, name in GOLD_FIRST_TURN_PENDINGS.items():
        case = by_name[name]
        assert case.args[0] is None and case.args[2] == ()  # no anchor, no frame operations
        assert {item[0] for item in case.run()} == labels[gid]["echo"] == {"CARRIED"}, gid
    for gid, name in GOLD_ANCHORED_PENDINGS.items():  # the pending WITH an anchor (D15) gives the label's echo as well
        assert {item[0] for item in by_name[name].run()} == labels[gid]["echo"] == {"CARRIED"}, gid
    sealed = {c["gold_id"]: c for c in cases()}
    for gid in GOLD_FIRST_TURN_PENDINGS:
        assert set(sealed[gid]["owners"]) <= INTEGRATION_OWNERS and "PENDING_PRIOR" in sealed[gid]["rules"], gid
    in_dev = [gid for gid in GOLD_FIRST_TURN_PENDINGS if sealed[gid]["partition"] == "DEV"]
    assert in_dev and all(dev()[gid][0] == MEASURED_AT_INTEGRATION for gid in in_dev)


def test_the_module_does_what_the_contract_says_where_a_sealed_artifact_does_not():
    """The three FAIL_EXTERNAL causes, each shown with the module on the typed state the CONTRACT describes."""
    by_name = {case.name: case for case in CASES}
    # (1) a TIME across several civil years that carries its year provenance: RELATIVE_WINDOW_RESOLVED (§11)
    window = by_name["relative_window_across_years"]
    assert "RELATIVE_WINDOW_RESOLVED" in {item[0] for item in window.run()}
    # (2) a whole-VALUE CLEAR that no SET follows is no VALUE_RANGE_REPLACED (Gold §6 «No se marca», §11 «seguido de SET»)
    clear_alone = by_name["value_range_clear_alone_is_not_a_replacement"]
    assert "VALUE_RANGE_REPLACED" not in {item[0] for item in clear_alone.run()}
    # (3) the year of a bound is the user's (STATED): no YEAR_INHERITED; inherited from the previous TIME: YEAR_INHERITED
    stated = next(c for c in cases() if c["gold_id"] == "SF167")
    assert executed_emitted(stated, with_stated_year)[0] == gold_labels(None)["SF167"]["echo"]
    assert "YEAR_INHERITED" in executed_emitted(stated)[0]


REAL_DIR = os.environ.get("M2A_GOLD_REAL_DIR")


@pytest.mark.skipif(REAL_DIR is None, reason="M2A_GOLD_REAL_DIR (private GOLD_CANDIDATE) not set")
def test_dev_with_the_real_slice():
    rows = measure("DEV", REAL_DIR)
    assert {gid for gid, row in rows.items() if row[0] == FAIL} == set()
    sources = {gid: row["source"] for gid, row in gold_labels(REAL_DIR).items()}
    assert sum(1 for gid, row in rows.items() if row[0] == GREEN and sources[gid] == "REAL") > 0


def denominators(rows: dict) -> dict:
    return {verdict: sum(1 for v, _ in rows.values() if v == verdict)
            for verdict in (GREEN, RED_S19, MEASURED_AT_INTEGRATION, FAIL_EXTERNAL, FAIL)}


def summary_of(rows: dict) -> str:
    counts = denominators(rows)
    lines = [f"cases {len(rows)}: " + ", ".join(f"{verdict} {n}" for verdict, n in counts.items())]
    for verdict in (RED_S19, MEASURED_AT_INTEGRATION, FAIL_EXTERNAL, FAIL):
        lines += [f"  {verdict} {gid}: {detail}" for gid, (v, detail) in sorted(rows.items()) if v == verdict]
    return "\n".join(lines)


if __name__ == "__main__":
    print(summary_of(measure(sys.argv[1] if len(sys.argv) > 1 else "DEV", os.environ.get("M2A_GOLD_REAL_DIR"))))
