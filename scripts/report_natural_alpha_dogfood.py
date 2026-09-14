"""Report scoped Alpha coverage from an explicit SQLite path without writes."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.natural_alpha.dogfood import DogfoodReportError, report_dogfood  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--email", required=True)
    parser.add_argument("--since", required=True, help="Inclusive ISO-8601 timestamp with timezone")
    parser.add_argument("--checkpoint", type=int, default=50, help="Number of reached-Alpha turns")
    parser.add_argument("--review-queue", action="store_true", help="Include trace IDs and host status; never query text")
    args = parser.parse_args(argv)
    try:
        report = report_dogfood(args.db, email=args.email, since=args.since,
                               checkpoint=args.checkpoint, review_queue=args.review_queue)
    except DogfoodReportError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
