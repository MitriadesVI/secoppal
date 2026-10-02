"""Generated, sealed extraction of contract §19: the ONE machine-readable source of dependencies and gaps (R3, U19/U23).

Principle (user, 2026-10-02): one question -> one authority -> everyone else consumes. §19 of
``docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`` is the authority. This generator is the only producer of its
machine-readable form, ``docs/audits/m2a_conversation/gold/CONTRACT_S19_DEPENDENCIES.json``. The oracle, the checkers
and the lanes read that file and never keep a list or an override of their own. The seal binds this script, the
contract parser it uses and the output; ``m2a_gold_seal.py verify`` checks that the output reproduces byte for byte.

It fails closed (``SystemExit``) when:
  * §19 or one of its two tables cannot be parsed (``m2a_contract_text``);
  * an id is not written exactly (``[SF075, R0301]``, sorted, no repeats; ``sf075`` or ``SF-075`` fail) or is unknown
    to the sealed index;
  * a declared count differs from the ids it lists (also with 0 and ``[]``);
  * an owner cell is not in the declared closed list ``OWNER_CELLS`` or names an owner outside ``OWNERS``;
  * an ``impacto_m2a3`` value is not ``LECTURA`` or ``COMPOSICION``;
  * it disagrees with the seal: a gap's ids are not exactly the sealed FOLLOWUP_FRONTIER records with that ``gap_id``;
    a dependency id is not in the sealed M2A_GOLD; a sealed record declares ``partial_alternatives`` for a dependency
    that is not a COMPOSICION row listing that case.

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
RECORDS = GOLD_DIR / "M2A_GOLD_synthetic.jsonl"
SCHEMA = "m2a-contract-s19/2"

_SPEC = importlib.util.spec_from_file_location("m2a_contract_text", Path(__file__).with_name("m2a_contract_text.py"))
text_mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(text_mod)

SECTION = ("## 19. Frontera: M2b, M3 y E-*", "## 20. Gates")
GAP_HEADER = "| gap_id | Hueco | Ids (frontera) | Conteo | Provisional en M2a | Dueño | Salida |"
DEP_HEADER = "| Id | Alcance | Ids | Conteo | impacto_m2a3 | Necesitan | Dueño |"
IMPACTS = ("LECTURA", "COMPOSICION")

# Closed list of owners. A new owner is a decision recorded in §19 AND here, never a silent addition.
OWNERS = ("CONTRATO_M1", "D-BUYER", "D-MONEY", "D-TIME", "E-BUYER", "E-GEO", "E-MODALITY", "E-ORDER", "E-STATE",
          "E-TIME", "E-VALUE", "F", "FOUNDATION_O1B_EXPLICIT_TIME_PARITY", "M2A-1b", "M2A-5", "M2A-6", "M2b", "M3",
          "PROTOCOLO_M2A")
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
    "E-GEO": ("E-GEO",),
    "E-STATE": ("E-STATE",),
    "E-MODALITY": ("E-MODALITY",),
    "`FOUNDATION_O1B_EXPLICIT_TIME_PARITY` (+ M2A-1b)": ("FOUNDATION_O1B_EXPLICIT_TIME_PARITY", "M2A-1b"),
    "E-ORDER": ("E-ORDER",),
    "M2A-1b": ("M2A-1b",),
}
_GAP_ID = re.compile(r"G-FU-\d{2}b?")
_DEP_ID = re.compile(r"`(DEP-[A-Z0-9-]+)`")
_CASE = re.compile(r"(?:SF|R)\d{3,4}")
_COUNT = re.compile(r"0|[1-9]\d*")


def _fail(message: str):
    raise SystemExit(f"§19 extraction failed: {message}")


def _ids(cell: str, row: str) -> list[str]:
    """``[]`` or ``[ID, ID, …]``: exact case, exact separators, sorted, no repeats."""
    if not (cell.startswith("[") and cell.endswith("]")):
        _fail(f"{row}: ids cell is not a bracketed list: {cell!r}")
    body = cell[1:-1]
    ids = [] if body == "" else body.split(", ")
    for case in ids:
        if not _CASE.fullmatch(case):
            _fail(f"{row}: malformed case id {case!r} (expected SF### / R#### exactly)")
    if len(set(ids)) != len(ids):
        _fail(f"{row}: repeated case id")
    if ids != sorted(ids):
        _fail(f"{row}: ids are not sorted")
    return ids


def _count(cell: str, ids: list[str], row: str) -> int:
    if not _COUNT.fullmatch(cell):
        _fail(f"{row}: count {cell!r} is not a non-negative integer")
    if int(cell) != len(ids):
        _fail(f"{row}: declares {cell} cases but lists {len(ids)}")
    return int(cell)


def _owners(cell: str, row: str) -> list[str]:
    owners = OWNER_CELLS.get(cell)
    if owners is None:
        _fail(f"{row}: owner cell {cell!r} is not in the declared closed list")
    if not set(owners) <= set(OWNERS):
        _fail(f"{row}: owner outside OWNERS: {sorted(set(owners) - set(OWNERS))}")
    return list(owners)


def _sealed(index: dict) -> tuple[set[str], dict[str, set[str]]]:
    gold = {i for name in ("REAL", "SYNTHETIC") for i in index["slices"][name]["M2A_GOLD"]["ids"]}
    by_gap: dict[str, set[str]] = {}
    for name in ("REAL", "SYNTHETIC"):
        for record in index["slices"][name]["FOLLOWUP_FRONTIER"]["records"]:
            for gap in record["gap_ids"]:
                by_gap.setdefault(gap, set()).add(record["gold_id"])
    return gold, by_gap


def extract(contract_text: str | None = None, index: dict | None = None, records: list[dict] | None = None) -> dict:
    contract = text_mod.read() if contract_text is None else contract_text
    index = json.loads(INDEX.read_text(encoding="utf-8")) if index is None else index
    if records is None:
        records = [json.loads(x) for x in RECORDS.read_text(encoding="utf-8").splitlines() if x.strip()]
    try:
        body = text_mod.section(contract, *SECTION)
        gap_rows, dep_rows = text_mod.table(body, GAP_HEADER), text_mod.table(body, DEP_HEADER)
    except text_mod.ContractTextError as error:
        _fail(str(error))
    gold, by_gap = _sealed(index)
    gaps = []
    for gap_id, topic, ids_cell, count_cell, provisional, owner, exit_ in gap_rows:
        if not _GAP_ID.fullmatch(gap_id):
            _fail(f"malformed gap id {gap_id!r}")
        ids = _ids(ids_cell, gap_id)
        if set(ids) != by_gap.get(gap_id, set()):
            _fail(f"{gap_id}: ids differ from the sealed FOLLOWUP_FRONTIER records with this gap_id: "
                  f"listed-not-sealed {sorted(set(ids) - by_gap.get(gap_id, set()))}, "
                  f"sealed-not-listed {sorted(by_gap.get(gap_id, set()) - set(ids))}")
        gaps.append({"gap_id": gap_id, "topic": topic, "cases": ids, "cases_count": _count(count_cell, ids, gap_id),
                     "provisional": provisional, "owners": _owners(owner, gap_id), "exit": exit_})
    missing_gaps = sorted(set(by_gap) - {g["gap_id"] for g in gaps})
    if missing_gaps:
        _fail(f"sealed FOLLOWUP_FRONTIER gap ids absent from §19: {missing_gaps}")
    dependencies = []
    for id_cell, scope, ids_cell, count_cell, impact, needs, owner in dep_rows:
        match = _DEP_ID.fullmatch(id_cell)
        if not match:
            _fail(f"malformed dependency id {id_cell!r}")
        dep_id = match.group(1)
        ids = _ids(ids_cell, dep_id)
        unknown = sorted(set(ids) - gold)
        if unknown:
            _fail(f"{dep_id}: cases not in the sealed M2A_GOLD: {unknown}")
        if impact not in IMPACTS:
            _fail(f"{dep_id}: impacto_m2a3 {impact!r} is not one of {IMPACTS}")
        dependencies.append({"dep_id": dep_id, "scope": scope, "cases": ids,
                             "cases_count": _count(count_cell, ids, dep_id), "impacto_m2a3": impact, "needs": needs,
                             "owners": _owners(owner, dep_id)})
    for kind, ids in (("gap", [g["gap_id"] for g in gaps]), ("dependency", [d["dep_id"] for d in dependencies])):
        if len(set(ids)) != len(ids):
            _fail(f"repeated {kind} id")
    rows = {d["dep_id"]: d for d in dependencies}
    for record in records:
        partial = record.get("partial_alternatives")
        if partial is None:
            continue
        row = rows.get(partial.get("dependency"))
        if row is None or row["impacto_m2a3"] != "COMPOSICION" or record["gold_id"] not in row["cases"]:
            _fail(f"{record['gold_id']}: sealed partial_alternatives names {partial.get('dependency')!r}, which is not "
                  f"a COMPOSICION row of §19 listing the case")
    case_dependencies: dict[str, list[str]] = {}
    for dep in dependencies:
        for case in dep["cases"]:
            case_dependencies.setdefault(case, []).append(dep["dep_id"])
    case_owners = {case: sorted({o for d in deps for o in rows[d]["owners"]}) for case, deps in case_dependencies.items()}
    case_impact = {case: ("COMPOSICION" if any(rows[d]["impacto_m2a3"] == "COMPOSICION" for d in deps) else "LECTURA")
                   for case, deps in case_dependencies.items()}
    return {
        "schema": SCHEMA,
        "source": {"contract": "docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md",
                   "contract_sha256": hashlib.sha256(contract.encode("utf-8")).hexdigest(), "section": "§19"},
        "owners": list(OWNERS),
        "impacts": list(IMPACTS),
        "gaps": gaps,
        "dependencies": dependencies,
        "case_dependencies": dict(sorted(case_dependencies.items())),
        "case_owners": dict(sorted(case_owners.items())),
        "case_impacto_m2a3": dict(sorted(case_impact.items())),
    }


def render(**kwargs) -> str:
    return json.dumps(extract(**kwargs), ensure_ascii=False, indent=1, sort_keys=True) + "\n"


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
