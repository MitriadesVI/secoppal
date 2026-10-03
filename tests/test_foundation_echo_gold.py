"""M2A-7 against the sealed ``M2A_GOLD``: «eco mínimo del Gold ⊆ emitido ⊆ justificado» (plan, fila M2A-7).

The Gold's echo is read from the labels: what they store (``expected_echo``: the synthetic slice stores it; labeler B
fills it, A left it empty) UNION what Gold schema 2.1 §7 derives («sí y sólo sí»; the REAL slice stores no derived
field) plus the contract's two additions (§11, «Derivación exacta»). ``gold_echo`` is written HERE from the schema's
table, over the labels' own fields, not from ``foundation_echo``.

Each case is measured with the typed turn the M2A-3 oracle already gives it (``tests/test_m2a3_oracle_link``: the base, the
patch and the effective that ``delta_outcome`` composes), through ``echo_items``:

* GREEN: ``gold ⊆ emitted ⊆ gold`` (the emitted echo is exactly what §7 owes);
* RED, DECLARED (never dispensed): the case needs something another lane owes. Three kinds, each with its cause and each
  proved green the moment the missing input exists:
  - the turn is not typable yet (the oracle's applicability: a dependency of §19 or ``NO_MEDIBLE``), so the echo cannot
    run on its effective;
  - the class needs a receipt rule that no authority emits yet (``topn.capped``: E-ORDER, §19 ``DEP-E-ORDER``): the
    case is green with a SYNTHETIC receipt on the typed condition (the echo maps the receipt when it is present);
  - the class is unrepresentable (``VIEW_DERIVED`` / ``VIEW_REDERIVED``: ``HostViewDecision``, §19 ``DEP-DERIVED-VIEW``).

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
from tests.test_foundation_echo import make_record
from tests.test_foundation_echo_properties import fresh_problems, problems
from tests.test_m2a3_oracle_link import base_of, facts_of, run
from scripts.audits.m2a_conversation import m2a_oracle_gold as gold
from scripts.audits.m2a_conversation import m2a_reducer_oracle as oracle

EXPIRY_SECONDS = 30 * 60
RECEIPT_OF = {"TOPN_CAPPED": fe.RULE_TOPN_CAPPED}
# The declared causes, closed. A missing class outside this table is a failure, never a red.
UNREPRESENTABLE = {"VIEW_DERIVED": "§19 DEP-DERIVED-VIEW (HostViewDecision, wave F)",
                   "VIEW_REDERIVED": "§19 DEP-DERIVED-VIEW (HostViewDecision, wave F)"}
NEEDS_RECEIPT = {"TOPN_CAPPED": "§19 DEP-E-ORDER: receipt rule topn.capped (E-ORDER)"}
# FINDING (reported, not resolved here): ``check_m2a_gold_contract.derive_effects`` marks VALUE_RANGE_REPLACED for a
# whole-VALUE ``CLEAR`` that no ``SET`` follows (REAL R0202), while Gold schema 2.1 §6 says «No se marca: CLEAR(VALUE) sin
# SET» and contract §11 says «CLEAR(VALUE) explícito seguido de SET que no repone un lado». The echo follows the
# normative text; the case stays a declared red until the checker (HERA's) and the text agree.
CHECKER_DISAGREES = {"VALUE_RANGE_REPLACED": "sealed checker derive_effects vs Gold §6 / contract §11 text: "
                                             "CLEAR(VALUE) with no SET"}
# What the echo may emit beyond the labels (``emitted ⊆ justified``, not ``emitted ⊆ labels``): the contract justifies an
# echo by the typed provenance. The labels read ``inherited.year_source`` on TIME operations only (schema O6), while the
# type carries the vigencia of a SMMLV bound too (``Provenance.year_source``; §15.5 names YEAR_CLOCK_INFERRED for it and
# the oracle types the inherited year of SF167). The addition is allowed ONLY for these two classes and only when the
# reference of the properties (written from compose's own account) says it is owed.
JUSTIFIED_ADDITIONS = {"YEAR_INHERITED": "§15.5/§16.1: the vigencia of a SMMLV bound is inherited (typed provenance)",
                       "YEAR_CLOCK_INFERRED": "§15.5 OQ-1: the vigencia of a SMMLV bound is inferred from the clock"}


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
    gap = label["prior"].get("gap_seconds")
    expired = gap is not None and gap > EXPIRY_SECONDS
    if label["expected_disposition"] == "CLARIFY" and label["disposition_reason"] == "CONTEXT_EXPIRED_CONFIRM":
        return {"CONTEXT_EXPIRED"} | out
    if not _executed(label):
        return out | ({"CONTEXT_EXPIRED"} if expired else set())  # R3d: every turn over an expired anchor not confirmed
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


def with_receipt(patch: fa.SemanticPatch, echo_class: str) -> fa.SemanticPatch:
    """The SYNTHETIC receipt the authority will emit (the oracle types ``receipt_rules`` as empty): the cap's, on the
    CARDINALITY condition of the patch whose figure exceeds what ran (the receipt belongs to the resolved condition, so the
    patch's operation carries it and ``compose`` passes it through)."""
    rule = RECEIPT_OF[echo_class]
    ops = []
    for op in patch.ops:
        condition = op.condition
        if condition is not None and condition.dimension.value == "CARDINALITY" and decode_value(condition.value).capped:
            condition = replace(condition, receipt_rules=tuple(sorted({*condition.receipt_rules, rule})))
            op = replace(op, condition=condition)
        ops.append(op)
    return fa.SemanticPatch(tuple(ops), patch.origin_kind)


YEAR_SOURCE_OF = {"RELATIVE_WINDOW_RESOLVED": fa.YearSource.RELATIVE_TO_CLOCK,
                  "YEAR_INHERITED": fa.YearSource.PREVIOUS_ACCEPTED_TIME,
                  "YEAR_CLOCK_INFERRED": fa.YearSource.CLOCK_INFERRED}
# FINDING (reported, not resolved here): ``m2a_oracle_typing.tcond`` types a TIME's ``year_source`` only when the interval
# lies in ONE civil year, so a window across years («últimos 12 meses», SF029 in TEST) reaches the typed turn without the
# provenance the Gold op states (RELATIVE_TO_CLOCK). The echo cannot announce what the typed turn does not say; it
# announces it the moment the typed condition carries it (proved below). Declared red, with the proof.
UNTYPED_YEAR = "the oracle's typing omits year_source for a TIME across several civil years (m2a_oracle_typing.tcond)"


def with_year_source(patch: fa.SemanticPatch, echo_class: str) -> fa.SemanticPatch:
    """The year provenance the Gold op states, on the TIME conditions the typing left without one."""
    source, ops = YEAR_SOURCE_OF[echo_class], []
    for op in patch.ops:
        condition = op.condition
        if condition is not None and condition.dimension is D.TIME and condition.provenance.year_source is None:
            op = replace(op, condition=replace(condition, provenance=replace(condition.provenance, year_source=source)))
        ops.append(op)
    return fa.SemanticPatch(tuple(ops), patch.origin_kind)


def silent_emitted(label_gap, disposition: str) -> set[str]:
    expired = label_gap is not None and label_gap >= EXPIRY_SECONDS
    record = make_record(RESOLUTION[disposition])
    return {item.echo_class.value for item in fe.echo_items(None, None, None, record, anchor_expired=expired)}


def executed_emitted(case: dict, change=None) -> tuple[set[str] | None, str | None, list[str]]:
    """``(classes emitted, why it could not run, the reference's problems with the emitted echo: [] = justified)``."""
    actual, scope, problem = run(case["inputs"])
    if problem is not None or actual is None:
        return None, problem or "UNTYPED", []
    if actual["disposition"] not in ("NEW_SEARCH", "EXECUTE"):
        return None, f"NOT_EXECUTED:{actual['disposition']}", []
    patch = fa.SemanticPatch.from_dict(case["inputs"]["patch"])
    if change is not None:  # a SYNTHETIC input an authority will provide (a receipt, a provenance)
        patch = change(patch)
    expired = facts_of(case["inputs"]).anchor_expired
    base = base_of(case["inputs"]["base"])
    if actual["disposition"] == "NEW_SEARCH":
        effective, resolution, patch_arg, base_trace = fresh_effective(patch), fa.TurnResolution.NEW_SEARCH, None, None
        justified = fresh_problems(fe.echo_items, effective, base, expired)
    else:
        composed = fd.compose(base, patch, base_trace_id="t0")
        assert isinstance(composed, fd.Composed)
        effective, resolution, patch_arg, base_trace = composed.effective, fa.TurnResolution.DELTA, patch, "t0"
        if change is None:  # the effective the oracle froze is the one the reducer composes
            assert effective.to_dict() == actual["effective"]
        justified = problems(fe.echo_items, base, patch, composed)
    record = make_record(resolution, patch=patch_arg, effective=effective, base_trace_id=base_trace)
    items = fe.echo_items(base, effective, patch_arg, record, anchor_expired=expired)
    return {item.echo_class.value for item in items}, None, justified


# ---- the measurement -------------------------------------------------------------------------------------------------------
def measure(partition: str, real_dir: str | None = None) -> dict:
    """``rows[gold_id] = (verdict, detail)`` with verdict GREEN, RED (declared, with its cause) or FAIL (a defect)."""
    labels = gold_labels(real_dir)
    chosen = oracle.select_partition(list(cases(real_dir)), partition)
    rows = {}
    for case in chosen:
        gid, wanted = case["gold_id"], labels[case["gold_id"]]["echo"]
        if case["disposition"] in RESOLUTION:
            got = silent_emitted(_gaps(real_dir)[gid], case["disposition"])
            rows[gid] = ("GREEN", "") if got == wanted else ("FAIL", f"emitted {sorted(got)} owed {sorted(wanted)}")
            continue
        if _kinds(real_dir)[gid] == ("PROTOCOL", "TEXT"):  # a page: the same plan again, no typed composition to read
            got = {item.echo_class.value for item in fe.echo_items(
                None, None, None, make_record(fa.TurnResolution.PROTOCOL_PAGE, base_trace_id="t0"))}
            rows[gid] = ("GREEN", "page") if got == wanted else ("FAIL", f"a page emitted {sorted(got)} owed {sorted(wanted)}")
            continue
        if case["disposition"] == "HANDOFF_LEGACY":
            rows[gid] = ("RED", "HANDOFF_LEGACY: legacy owner, no record is written (§10.2)")
            continue
        got, why, unjustified = (executed_emitted(case) if case["inputs"] is not None
                                 else (None, case["applicability"], []))
        if got is None:
            rows[gid] = ("RED", f"not typable by the oracle: {why}" + ("; owes " + ",".join(sorted(wanted)) if wanted else ""))
            continue
        missing, extra = wanted - got, got - wanted
        if unjustified:
            rows[gid] = ("FAIL", f"emitted what its typed turn does not justify: {unjustified}")
        elif extra - set(JUSTIFIED_ADDITIONS):
            rows[gid] = ("FAIL", f"emitted what the Gold does not owe and the contract does not justify: {sorted(extra)}")
        elif not missing:
            rows[gid] = ("GREEN", "; ".join(f"+{c}: {JUSTIFIED_ADDITIONS[c]}" for c in sorted(extra)))
        elif all(m in UNREPRESENTABLE for m in missing):
            rows[gid] = ("RED", "; ".join(f"{m}: {UNREPRESENTABLE[m]}" for m in sorted(missing)))
        elif all(m in NEEDS_RECEIPT for m in missing):
            with_it, _, with_problems = executed_emitted(case, lambda patch: with_receipt(patch, sorted(missing)[0]))
            if with_it is not None and wanted <= with_it and not with_problems:
                rows[gid] = ("RED", "; ".join(f"{m}: {NEEDS_RECEIPT[m]} (green with the synthetic receipt)" for m in sorted(missing)))
            else:
                rows[gid] = ("FAIL", f"missing {sorted(missing)} even with the receipt")
        elif missing and missing <= set(YEAR_SOURCE_OF) and _proved_with_year_source(case, wanted, missing):
            rows[gid] = ("RED", f"{', '.join(sorted(missing))}: {UNTYPED_YEAR} (green with the year provenance)")
        elif missing == {"VALUE_RANGE_REPLACED"} and _clear_alone(case):
            rows[gid] = ("RED", f"VALUE_RANGE_REPLACED: {CHECKER_DISAGREES['VALUE_RANGE_REPLACED']}")
        else:
            rows[gid] = ("FAIL", f"missing {sorted(missing)}")
    return rows


def _proved_with_year_source(case: dict, wanted: set[str], missing: set[str]) -> bool:
    """With the year provenance on the typed TIME conditions the echo is what the Gold owes and what the typed turn justifies."""
    if len(missing) != 1:
        return False
    with_it, _, unjustified = executed_emitted(case, lambda patch: with_year_source(patch, next(iter(missing))))
    return with_it is not None and wanted <= with_it and not unjustified


def _clear_alone(case: dict) -> bool:
    """A whole-VALUE ``CLEAR`` and no ``SET`` of a VALUE bound in the typed patch."""
    patch = fa.SemanticPatch.from_dict(case["inputs"]["patch"])
    cleared = any(op.op is fa.PatchOpKind.CLEAR and op.slot.dimension is D.VALUE and op.slot.side is None
                  for op in patch.ops)
    set_value = any(op.op is fa.PatchOpKind.SET and op.slot.dimension is D.VALUE for op in patch.ops)
    return cleared and not set_value


@lru_cache(maxsize=None)
def _gaps(real_dir: str | None) -> dict:
    return {r["gold_id"]: r["label_B"]["prior"].get("gap_seconds")
            for _, r in gold.gold_records(None if real_dir is None else Path(real_dir))}


@lru_cache(maxsize=None)
def _kinds(real_dir: str | None) -> dict:
    return {r["gold_id"]: (r["label_B"]["turn_kind"], r["label_B"]["input_kind"])
            for _, r in gold.gold_records(None if real_dir is None else Path(real_dir))}


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
    assert all(row["echo"] == row["stored"] for row in synthetic.values() if row["stored"]), "the derivation drifts"


def test_dev_no_case_fails():
    fails = {gid: row for gid, row in dev().items() if row[0] == "FAIL"}
    assert fails == {}


def test_dev_every_red_has_its_declared_cause_and_no_case_is_dropped():
    rows = dev()
    chosen = oracle.select_partition(list(cases()), "DEV")
    assert set(rows) == {case["gold_id"] for case in chosen}
    assert all(verdict in ("GREEN", "RED") for verdict, _ in rows.values())
    assert all(detail for verdict, detail in rows.values() if verdict == "RED")


def test_dev_the_greens_are_not_vacuous():
    rows = dev()
    labels = gold_labels(None)
    greens = [gid for gid, (verdict, _) in rows.items() if verdict == "GREEN"]
    assert len(greens) >= 40
    assert any(labels[gid]["echo"] for gid in greens)  # a non-empty echo was measured, not only silences
    owed = {name for gid in greens for name in labels[gid]["echo"]}
    assert {"CARRIED", "CONTEXT_EXPIRED", "CONTEXT_RESET", "UNIT_INHERITED", "YEAR_INHERITED", "ORDER_DIRECTION_DEFAULTED",
            "VALUE_RANGE_REPLACED", "TOPIC_REPLACED", "POLARITY_INVERTED", "UNIVERSE_CHANGED"} <= owed


REAL_DIR = os.environ.get("M2A_GOLD_REAL_DIR")


@pytest.mark.skipif(REAL_DIR is None, reason="M2A_GOLD_REAL_DIR (private GOLD_CANDIDATE) not set")
def test_dev_with_the_real_slice():
    rows = measure("DEV", REAL_DIR)
    assert all(verdict in ("GREEN", "RED") for verdict, _ in rows.values()), {g: r for g, r in rows.items() if r[0] == "FAIL"}
    assert sum(1 for gid, r in rows.items() if r[0] == "GREEN" and labels_source(REAL_DIR)[gid] == "REAL") > 0


def labels_source(real_dir) -> dict:
    return {gid: row["source"] for gid, row in gold_labels(real_dir).items()}


def summary_of(rows: dict) -> str:
    greens = sorted(g for g, (v, _) in rows.items() if v == "GREEN")
    reds = sorted((g, d) for g, (v, d) in rows.items() if v == "RED")
    fails = sorted((g, d) for g, (v, d) in rows.items() if v == "FAIL")
    lines = [f"cases {len(rows)}: GREEN {len(greens)}, RED(declared) {len(reds)}, FAIL {len(fails)}"]
    lines += [f"  RED {g}: {d}" for g, d in reds] + [f"  FAIL {g}: {d}" for g, d in fails]
    return "\n".join(lines)


if __name__ == "__main__":
    print(summary_of(measure(sys.argv[1] if len(sys.argv) > 1 else "DEV", os.environ.get("M2A_GOLD_REAL_DIR"))))
