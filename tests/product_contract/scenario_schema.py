"""Esquema del corpus Product Contract V1 y de su cuarentena (F2.5).

Estados de un escenario:

* ``DESIGN`` — diseño revisable; puede llevar ``TBD_RUNTIME`` donde el valor
  canónico debe tomarse del runtime acreditado en F3. No se ejecuta.
* ``READY`` — ejecutable y parte del gate; ``TBD_RUNTIME`` prohibido.

Un defecto funcional conocido nunca es un escenario: vive en ``quarantine.yaml``
como ``OPEN_PRODUCT_GAP`` con reproducción RED y no cuenta como contrato verde.
La deuda del harness (``HARNESS_DEBT``) también vive ahí, separada de los bugs.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from tests.product_contract.assertions import ORACLES

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "product_contract_v1"
SCENARIOS = FIXTURE_DIR / "scenarios.yaml"
QUARANTINE = FIXTURE_DIR / "quarantine.yaml"

PLACEHOLDER = "TBD_RUNTIME"
SCENARIO_ID = re.compile(r"^PC-[A-Z]+-\d{3}$")
GAP_ID = re.compile(r"^OPG-[A-Z]+-\d{3}$")
DEBT_ID = re.compile(r"^HD-[A-Z]+-\d{3}$")
FAMILIES = {"search", "followup_scope", "authority_arbitration", "money", "modality_control",
            "opportunity_truth", "dual_dataset", "failure_source"}
SEVERITIES = {"critical", "high", "medium"}
SOURCE_KINDS = {"incident", "dogfood", "feedback", "audit", "synthetic"}
SCENARIO_KEYS = {"id", "title", "status", "severity", "family", "mandatory_item", "source",
                 "invariants", "fixtures", "today", "reference_instant", "frame", "turns", "mine_from", "open_questions",
                 "red_first", "mutation"}
TURN_KEYS = {"text", "chat_id", "expect", "run_kwargs"}


def load(path: Path) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def _contains_placeholder(node: Any) -> bool:
    if isinstance(node, str):
        return PLACEHOLDER in node
    if isinstance(node, Mapping):
        return any(_contains_placeholder(v) for v in node.values())
    if isinstance(node, list):
        return any(_contains_placeholder(v) for v in node)
    return False


def validate_scenarios(document: Mapping) -> list[str]:
    errors: list[str] = []
    scenarios = document.get("scenarios") or []
    seen: set[str] = set()
    for index, sc in enumerate(scenarios):
        sid = sc.get("id", f"#{index}")
        where = f"scenario {sid}"
        errors += [f"{where}: unknown key {k!r}" for k in sc if k not in SCENARIO_KEYS]
        if not SCENARIO_ID.match(str(sc.get("id", ""))):
            errors.append(f"{where}: invalid id")
        if sid in seen:
            errors.append(f"{where}: duplicated id")
        seen.add(sid)
        if sc.get("status") not in {"DESIGN", "READY"}:
            errors.append(f"{where}: status must be DESIGN or READY (known defects go to quarantine.yaml)")
        if sc.get("severity") not in SEVERITIES:
            errors.append(f"{where}: invalid severity")
        if sc.get("family") not in FAMILIES:
            errors.append(f"{where}: invalid family {sc.get('family')!r}")
        source = sc.get("source") or {}
        if source.get("kind") not in SOURCE_KINDS or not source.get("reference"):
            errors.append(f"{where}: source needs kind in {sorted(SOURCE_KINDS)} and a reference")
        if not sc.get("invariants"):
            errors.append(f"{where}: must name at least one registered invariant")
        turns = sc.get("turns") or []
        if not turns:
            errors.append(f"{where}: needs at least one turn")
        for number, turn in enumerate(turns, 1):
            errors += [f"{where} turn {number}: unknown key {k!r}" for k in turn if k not in TURN_KEYS]
            if not str(turn.get("text", "")).strip() and not turn.get("run_kwargs"):
                errors.append(f"{where} turn {number}: needs text or run_kwargs")
            expect = turn.get("expect") or {}
            if not expect:
                errors.append(f"{where} turn {number}: needs at least one oracle")
            errors += [f"{where} turn {number}: unknown oracle {k!r}" for k in expect if k not in ORACLES]
            if number == 1 and "followup" in expect:
                errors.append(f"{where} turn 1: followup oracle needs a previous turn")
        if sc.get("status") == "READY" and _contains_placeholder(sc):
            errors.append(f"{where}: READY scenarios cannot contain {PLACEHOLDER}")
        if sc.get("status") == "READY":
            mutation = sc.get("mutation") or {}
            if set(mutation) != {"law", "kind", "expected_failure"}:
                errors.append(f"{where}: READY needs LAW, MUTATION and EXPECTED_FAILURE")
            if mutation.get("law") not in sc.get("invariants", ()):
                errors.append(f"{where}: mutation law must be a cited invariant")
    return errors


def validate_quarantine(document: Mapping, scenario_ids: set[str]) -> list[str]:
    errors: list[str] = []
    for gap in document.get("open_product_gaps") or []:
        gid = gap.get("id")
        if not GAP_ID.match(str(gid)):
            errors.append(f"gap {gid}: invalid id")
        if gap.get("status") != "OPEN_PRODUCT_GAP":
            errors.append(f"gap {gid}: status must be OPEN_PRODUCT_GAP")
        for key in ("title", "observed", "expected", "reproduction", "invariants"):
            if not gap.get(key):
                errors.append(f"gap {gid}: missing {key}")
        if gid in scenario_ids or gap.get("promotes_to") in scenario_ids and gap.get("counted_green"):
            errors.append(f"gap {gid}: an open product gap cannot count as a green scenario")
        if gap.get("counted_green"):
            errors.append(f"gap {gid}: counted_green is forbidden while the gap is open")
    for debt in document.get("harness_debt") or []:
        did = debt.get("id")
        if not DEBT_ID.match(str(did)):
            errors.append(f"debt {did}: invalid id")
        if debt.get("status") != "HARNESS_DEBT":
            errors.append(f"debt {did}: status must be HARNESS_DEBT")
        if debt.get("decision") not in {"REPAIR_MOCKS", "MOVE_TO_AUDIT", "UNDECIDED"}:
            errors.append(f"debt {did}: decision must be REPAIR_MOCKS, MOVE_TO_AUDIT or UNDECIDED")
    return errors
