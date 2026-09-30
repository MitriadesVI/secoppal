"""M01–M19: fresh source overlays and explicit semantic assertion killers."""
from __future__ import annotations

import argparse
import ast
import json
import os
from pathlib import Path
import sys

import legacy_oracles as legacy
from budget_oracle import observe


def allocator_mutation(root, name):
    file = root / "app/core/foundation_presentation.py"
    source = file.read_text()
    node = next(node for node in ast.parse(source).body
                if isinstance(node, ast.FunctionDef) and node.name == "shown_card_groups")
    lines = source.splitlines(True)
    old = "".join(lines[node.lineno - 1:node.end_lineno])
    signature = "def shown_card_groups(cards_by_group, *, channel, limit):\n"
    if name == "M17":
        body = "    return cards_by_group if channel in _CARD_CHANNELS else [cards[:max(0, limit)] for cards in cards_by_group]\n"
    elif name == "M18":
        body = ("    if channel in _CARD_CHANNELS:\n        return cards_by_group\n"
                "    active = sum(bool(cards) for cards in cards_by_group)\n"
                "    if not active:\n        return [[] for cards in cards_by_group]\n"
                "    quota, extra = divmod(max(0, limit), active)\n"
                "    assigned = []\n"
                "    for cards in cards_by_group:\n"
                "        assigned.append(cards[:quota + int(extra > 0)] if cards else [])\n"
                "        if cards and extra > 0:\n            extra -= 1\n"
                "    return assigned\n")
    else:
        body = ("    if channel in _CARD_CHANNELS:\n        return cards_by_group\n"
                "    budget = max(0, limit)\n    assigned = []\n"
                "    for cards in cards_by_group:\n"
                "        assigned.append(cards[:budget])\n        budget -= len(assigned[-1])\n"
                "    return assigned\n")
    return "foundation_presentation", old, signature + body


def killer(name):
    if name not in ("M17", "M18", "M19"):
        legacy.killer(name)
        return
    sizes = [2, 30] if name == "M18" else [30, 30]
    observed = observe(sizes, "telegram")
    if name == "M17":
        assert sum(observed["counts"]) == 25, "M17_GLOBAL_RESPONSE_BUDGET"
    elif name == "M18":
        assert observed["counts"] == [2, 23], "M18_REDISTRIBUTE_UNUSED_QUOTA"
    else:
        assert observed["counts"] == [13, 12], "M19_FAIR_SHARE_AND_REPRESENTATION"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--control", action="store_true")
    args = parser.parse_args()
    sys.path.insert(0, str(args.root))
    os.chdir(args.root)
    if args.name in ("M17", "M18", "M19"):
        legacy.MUTATIONS[args.name] = allocator_mutation(args.root, args.name)
    if not args.control:
        sys.meta_path.insert(0, legacy.MutationLoader(args.root, args.name, str(args.out) + ".diff"))
    killer(args.name)
    args.out.write_text(json.dumps({"name": args.name, "control": args.control, "property": "passed"}))
    print(args.name, "control" if args.control else "mutant", "property passed")


if __name__ == "__main__":
    main()
