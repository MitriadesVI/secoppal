"""Generated, sealed extraction of contract §19: the ONE machine-readable source of dependencies and gaps (R3, U19).

Principle (user, 2026-10-02): one question -> one authority -> everyone else consumes. §19 of
``docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`` is the authority. This generator is the only producer of its
machine-readable form, ``docs/audits/m2a_conversation/gold/CONTRACT_S19_DEPENDENCIES.json``. The oracle, the checkers
and the lanes read that file and never keep a list or an override of their own. The seal binds this script, the
contract parser it uses and the output, and ``m2a_gold_seal.py verify`` checks that the output reproduces byte for
byte from the contract.

It fails (``SystemExit``) when:
  * §19 or one of its two tables cannot be parsed (``m2a_contract_text``);
  * an owner cell is not in the declared closed list ``OWNER_CELLS``, or names an owner outside ``OWNERS``;
  * a cited case does not exist in the sealed Gold (gaps: ``FOLLOWUP_FRONTIER``; dependencies: ``M2A_GOLD``);
  * a row declares a count that disagrees with the ids it cites, or an id is malformed or repeated.

  python scripts/audits/m2a_conversation/extract_contract_dependencies.py          # write
  python scripts/audits/m2a_conversation/extract_contract_dependencies.py --check  # exit 1 if the committed copy differs
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
GOLD_DIR = ROOT / "docs/audits/m2a_conversation/gold"
OUT = GOLD_DIR / "CONTRACT_S19_DEPENDENCIES.json"
INDEX = GOLD_DIR / "M2A_GOLD_AND_FRONTIER_INDEX.json"
SCHEMA = "m2a-contract-s19/1"

_SPEC = importlib.util.spec_from_file_location("m2a_contract_text", Path(__file__).with_name("m2a_contract_text.py"))
text_mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(text_mod)

SECTION = ("## 19. Frontera: M2b, M3 y E-*", "## 20. Gates")
GAP_HEADER = "| gap_id | Hueco | Casos (frontera) | Provisional en M2a | Dueño | Salida |"
DEP_HEADER = "| Id | Casos `M2A_GOLD` | Necesitan | Dueño |"

# Closed list of owners. A new owner is a decision recorded in §19 AND here, never a silent addition.
OWNERS = ("CONTRATO_M1", "D-BUYER", "D-MONEY", "D-TIME", "E-BUYER", "E-GEO", "E-ORDER", "E-TIME", "E-VALUE", "F",
          "FOUNDATION_O1B_EXPLICIT_TIME_PARITY", "M2A-1b", "M2A-5", "M2A-6", "M2b", "M3", "PROTOCOLO_M2A")
# Closed list of owner CELLS as §19 writes them -> normalized owners. A cell that is not here fails the extraction.
OWNER_CELLS = {
    "protocolo M2a (M2A-6)": ("M2A-6",),
    "protocolo / M2b": ("PROTOCOLO_M2A", "M2b"),
    "E-VALUE": ("E-VALUE",),
    "E-GEO / E-BUYER": ("E-GEO", "E-BUYER"),
    "E-TIME": ("E-TIME",),
    "M2b": ("M2b",),
    "M3": ("M3",),
    "señal: M2A-5 (gate); ejecución: M2b": ("M2A-5", "M2b"),
    "protocolo M2a": ("PROTOCOLO_M2A",),
    "contrato M1 / E-VALUE": ("CONTRATO_M1", "E-VALUE"),
    "ola F": ("F",),
    "D-TIME (E-TIME)": ("D-TIME", "E-TIME"),
    "D-MONEY (E-VALUE)": ("D-MONEY", "E-VALUE"),
    "D-MONEY, D-TIME": ("D-MONEY", "D-TIME"),
    "D-BUYER": ("D-BUYER",),
    "`FOUNDATION_O1B_EXPLICIT_TIME_PARITY` (+ M2A-1b)": ("FOUNDATION_O1B_EXPLICIT_TIME_PARITY", "M2A-1b"),
    "E-ORDER (núcleo, plan M2A-3c)": ("E-ORDER",),
}
_GAP_ID = re.compile(r"G-FU-\d{2}b?")
_DEP_ID = re.compile(r"`(DEP-[A-Z0-9-]+)`")


def _fail(message: str):
    raise SystemExit(f"§19 extraction failed: {message}")


def _owners(cell: str, row: str) -> list[str]:
    owners = OWNER_CELLS.get(cell)
    if owners is None:
        _fail(f"{row}: owner cell {cell!r} is not in the declared closed list")
    if not set(owners) <= set(OWNERS):
        _fail(f"{row}: owner outside OWNERS: {sorted(set(owners) - set(OWNERS))}")
    return list(owners)


def _cases(cell: str, row: str, universe: set[str], slice_name: str) -> tuple[int | None, list[str]]:
    count, cases = text_mod.leading_count(cell), text_mod.case_ids(cell)
    if len(set(cases)) != len(cases):
        _fail(f"{row}: repeated case id in {cases}")
    if cases and count is not None and count != len(cases):
        _fail(f"{row}: declares {count} cases but cites {len(cases)}")
    missing = sorted(set(cases) - universe)
    if missing:
        _fail(f"{row}: cited cases not in the sealed {slice_name}: {missing}")
    return count, cases


def _gold_ids() -> tuple[set[str], set[str]]:
    index = json.loads(INDEX.read_text(encoding="utf-8"))
    gold = {i for name in ("REAL", "SYNTHETIC") for i in index["slices"][name]["M2A_GOLD"]["ids"]}
    frontier = {r["gold_id"] for name in ("REAL", "SYNTHETIC")
                for r in index["slices"][name]["FOLLOWUP_FRONTIER"]["records"]}
    return gold, frontier


def extract() -> dict:
    contract = text_mod.read()
    try:
        body = text_mod.section(contract, *SECTION)
        gap_rows, dep_rows = text_mod.table(body, GAP_HEADER), text_mod.table(body, DEP_HEADER)
    except text_mod.ContractTextError as error:
        _fail(str(error))
    gold, frontier = _gold_ids()
    gaps = []
    for gap_id, topic, cases_cell, provisional, owner, exit_ in gap_rows:
        if not _GAP_ID.fullmatch(gap_id):
            _fail(f"malformed gap id {gap_id!r}")
        count, cases = _cases(cases_cell, gap_id, frontier, "FOLLOWUP_FRONTIER")
        gaps.append({"gap_id": gap_id, "topic": topic, "cases_count": count, "cases": cases,
                     "provisional": provisional, "owners": _owners(owner, gap_id), "exit": exit_})
    dependencies = []
    for id_cell, scope, needs, owner in dep_rows:
        match = _DEP_ID.fullmatch(id_cell)
        if not match:
            _fail(f"malformed dependency id {id_cell!r}")
        dep_id = match.group(1)
        count, cases = _cases(scope, dep_id, gold, "M2A_GOLD")
        dependencies.append({"dep_id": dep_id, "scope": scope, "cases_count": count, "cases": cases, "needs": needs,
                             "owners": _owners(owner, dep_id)})
    for kind, ids in (("gap", [g["gap_id"] for g in gaps]), ("dependency", [d["dep_id"] for d in dependencies])):
        if len(set(ids)) != len(ids):
            _fail(f"repeated {kind} id")
    case_dependencies: dict[str, list[str]] = {}
    for dep in dependencies:
        for case in dep["cases"]:
            case_dependencies.setdefault(case, []).append(dep["dep_id"])
    case_owners = {case: sorted({o for d in dependencies if d["dep_id"] in deps for o in d["owners"]})
                   for case, deps in case_dependencies.items()}
    return {
        "schema": SCHEMA,
        "source": {"contract": "docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md",
                   "contract_sha256": hashlib.sha256(contract.encode("utf-8")).hexdigest(), "section": "§19"},
        "owners": list(OWNERS),
        "gaps": gaps,
        "dependencies": dependencies,
        "case_dependencies": dict(sorted(case_dependencies.items())),
        "case_owners": dict(sorted(case_owners.items())),
    }


def render() -> str:
    return json.dumps(extract(), ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", help="compare with the committed copy instead of writing")
    a = ap.parse_args()
    body = render()
    if a.check:
        same = OUT.exists() and OUT.read_text(encoding="utf-8") == body
        print("§19 extraction", "reproduces byte for byte" if same else f"DIFFERS from {OUT.relative_to(ROOT)}")
        return 0 if same else 1
    OUT.write_text(body, encoding="utf-8")
    print("written", OUT.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
