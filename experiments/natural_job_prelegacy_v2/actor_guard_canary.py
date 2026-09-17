"""C4A.2 G — actor exact guard latency canary (real index, no semantics touched).

Measures the two calls the guard makes per credentialed turn separately, plus the
EXPLAIN QUERY PLAN of the aggregate that decides the window width. Reports cold
and warm percentiles. Does not modify the index or its schema.

Run: SECOP_ACTOR_ALIAS_INDEX_PATH=/path/to/actor_alias_index.sqlite \
     python experiments/natural_job_prelegacy_v2/actor_guard_canary.py [--warm 30]
"""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.data.actor_alias_resolver import ActorNameResolver  # noqa: E402

OUT = ROOT / "experiments" / "natural_job_prelegacy_v2" / "actor_guard_canary.json"
CANARY_TURNS = (
    "con quien ha contratado la alcaldia de galapa en 2025",
    "cuales son los proveedores con contratos mas grandes de alcaldia de barranquilla",
    "cuanto le ha vendido fundacaribe a galapa",
)


def _percentiles(values: list[float]) -> dict:
    ordered = sorted(values)
    def at(fraction: float) -> float:
        index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
        return round(ordered[index], 3)
    return {"n": len(ordered), "p50": at(0.50), "p95": at(0.95), "max": round(ordered[-1], 3),
            "min": round(ordered[0], 3), "mean": round(statistics.fmean(ordered), 3)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warm", type=int, default=30)
    args = parser.parse_args()

    scratch = pathlib.Path(tempfile.mkdtemp(prefix="c4a2-canary-"))
    settings = Settings(
        _env_file=None,
        secop_feedback_path=scratch / "traces.jsonl",
        secop_feedback_db_path=scratch / "feedback.db",
    )
    resolver = ActorNameResolver(settings.secop_actor_alias_index_path)

    cold_max = _timed(lambda: resolver._maximum_alias_tokens())
    warm_max = [_timed(lambda: resolver._maximum_alias_tokens()) for _ in range(args.warm)]
    cold_exact = [_timed(lambda turn=turn: resolver.exact_aliases_in(turn)) for turn in CANARY_TURNS[:1]]
    warm_exact = [
        _timed(lambda turn=turn: resolver.exact_aliases_in(turn))
        for _ in range(args.warm)
        for turn in CANARY_TURNS
    ]
    plan = _explain(resolver)
    document = {
        "index_path": str(settings.secop_actor_alias_index_path),
        "cold": {"maximum_alias_tokens_ms": cold_max, "exact_aliases_in_ms": cold_exact[0]},
        "warm": {
            "maximum_alias_tokens_ms": _percentiles(warm_max),
            "exact_aliases_in_ms": _percentiles(warm_exact),
        },
        "explain_query_plan": plan,
    }
    OUT.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps(document, indent=1) + "\n")


def _timed(call) -> float:
    started = time.perf_counter()
    call()
    return round((time.perf_counter() - started) * 1000, 3)


def _explain(resolver) -> list[str]:
    connection = resolver._connect_readonly()
    try:
        rows = connection.execute(
            "EXPLAIN QUERY PLAN SELECT MAX(LENGTH(normalized_alias) - "
            "LENGTH(REPLACE(normalized_alias, ' ', '')) + 1) FROM actor_aliases"
        ).fetchall()
    except Exception as exc:  # noqa: BLE001 -- the canary reports, never hides
        return [f"error: {type(exc).__name__}"]
    finally:
        connection.close()
    return [str(tuple(row)) for row in rows]


if __name__ == "__main__":
    main()
