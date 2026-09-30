"""HERA public-turn oracle: fixed expected distributions, untouched material rows."""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import re
import sys

import legacy_oracles as legacy


CASES = [([30, 30], [13, 12]), ([2, 30], [2, 23]), ([0, 30], [0, 25]),
         ([10, 10], [10, 10]), ([30, 2], [23, 2]), ([30, 0], [25, 0])]


def observe(sizes, channel, *, error=False):
    import pytest

    raw = "procesos y contratos de aseo"
    data = {ds: legacy.rows(ds, size) for ds, size in zip((legacy.P, legacy.C), sizes)}
    for ds, items in data.items():
        field = "nombre_del_procedimiento" if ds == legacy.P else "objeto_del_contrato"
        for i, row in enumerate(items):
            row[field] = f"aseo {'process' if ds == legacy.P else 'contract'} {i:03}"
    with pytest.MonkeyPatch.context() as mp:
        flow, source = legacy.workflow(mp, data, subjects=("processes", "contracts"),
                                       errors=[ValueError("independent source_error")] if error else ())
        result = legacy.run_turn(flow, raw, channel)
        assert result["route_reason"] == "foundation_search", "public supported dual route"
        expected_source = data[legacy.C] if error else data[legacy.P] + data[legacy.C]
        assert legacy.exact(result["results"], expected_source), "results exact source"
        assert legacy.exact(source.data, source.before), "source immutable"
        assert len(result["rows"]) == len(expected_source), "all cards retained separately"
        assert legacy.exact([row for group in result["groups"] for row in group["rows"]], expected_source), "groups source exact"
        counts, titles, heads = [0, 0], [[], []], [None, None]
        group = None
        for line in result["response"].splitlines():
            if line.startswith("Procesos:"):
                group = 0
            elif line.startswith("Contratos:"):
                group = 1
            if group is not None:
                announced = re.search(r"[Tt]e muestro (?:los )?(\d+)", line)
                if announced:
                    heads[group] = int(announced[1])
                card = re.match(r"\d+\. (.*)", line)
                if card:
                    counts[group] += 1
                    titles[group].append(card[1])
        material = {key: result[key] for key in ("results", "rows", "groups")}
        return {"sizes": sizes, "channel": channel, "error": error, "counts": counts, "heads": heads,
                "titles": titles, "response": result["response"], "material": material,
                "typed_material": legacy.typed(material), "calls": source.calls}


def validate(case, expected):
    if case["channel"] == "streamlit":
        assert case["counts"] == [0, 0], "web cards render separately"
        shown = [len(group["rows"]) for group in case["material"]["groups"]]
    else:
        assert sum(case["counts"]) <= 25, "GLOBAL_TEXT_LIMIT"
        assert case["counts"] == expected, "FAIR_REDISTRIBUTED_COUNTS"
        shown = expected
        for i, titles in enumerate(case["titles"]):
            expected_titles = [f"aseo {'process' if i == 0 else 'contract'} {j:03}" for j in range(shown[i])]
            assert titles == expected_titles, "SOURCE_ORDER_PREFIX"
    for i, group in enumerate(case["material"]["groups"]):
        if group["rows"]:
            assert case["heads"][i] == shown[i], "HEADING_EXACT_ASSIGNED_COUNT"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--collect-only", action="store_true")
    args = parser.parse_args()
    sys.path.insert(0, str(args.root))
    os.chdir(args.root)
    observed = []
    for channel in ("telegram", "whatsapp", "streamlit"):
        for sizes, expected in CASES:
            case = observe(sizes, channel)
            observed.append(case)
            if not args.collect_only:
                validate(case, expected)
        case = observe([0, 30], channel, error=True)
        observed.append(case)
        if not args.collect_only:
            validate(case, [0, 25])
    args.out.write_text(json.dumps(observed, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"cases": len(observed), "properties_passed": not args.collect_only}))


if __name__ == "__main__":
    main()
