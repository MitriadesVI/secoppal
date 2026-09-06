"""Export six procurement layers from frozen evidence, with optional bounded navigation."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audits.relationship_foundation.identity_spine import COHORTS
from scripts.procurement_layers.build import build


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", choices=sorted(COHORTS), default="baranoa")
    parser.add_argument("--node", help="Exact typed node ID, never a name or inferred NIT")
    parser.add_argument("--hops", type=int, choices=range(4), default=1)
    parser.add_argument("--output", type=Path, help="New output file; existing files are preserved")
    args = parser.parse_args(argv)
    graph, scope = build(args.cohort)
    try:
        result = graph.neighborhood(args.node, hops=args.hops) if args.node else graph.export()
    except ValueError as exc:
        parser.error(str(exc))
    result["scope"] = scope
    rendered = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    if args.output:
        with args.output.open("x", encoding="utf-8") as output:
            output.write(rendered)
        print(json.dumps({"state": result["state"], "nodes": len(result["nodes"]),
                          "edges": len(result["edges"]), "output": str(args.output)}, sort_keys=True))
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
