#!/usr/bin/env python
"""ACTOR-FUZZY-MULTI-TYPO-RECALL-001 — audit, not a change.

The question: can we recover a 3+ token actor name when *two* of its tokens
carry a small typo, without degrading candidate quality or latency?

The audit is staged on purpose, because the two stages answer different
questions and the second is only meaningful if the first says so:

``pool``
    Does the target ever reach the candidate pool?  ``_anchored_name_distance``
    reorders ``ACTOR_CANDIDATE_POOL`` rows; a row that FTS never retrieved
    cannot be reordered into view by any distance function.  If the target is
    absent from the pool, this is a retrieval question, not a ranking one, and
    the assignment is explicit that retrieval is not to be widened on the
    strength of one case.

``baseline``
    Measures recall@1/3/10, MRR, in-pool rate and latency over a reproducible
    perturbation corpus drawn from the real index, split by perturbation
    family, so the one-typo path (which works) and the two-token path (which
    is the open question) are never averaged together.

Read-only throughout: the index is opened ``mode=ro&immutable=1`` by the
resolver itself and by this script.  Nothing here rebuilds or writes an index.

Documents are masked in every artifact: an identity is recorded as a salted
digest, never as a cédula or NIT.  Alias text is aggregated, never dumped.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.actor_name import ActorResolutionStatus, normalize_actor_name  # noqa: E402
from app.data.actor_alias_resolver import (  # noqa: E402
    ACTOR_CANDIDATE_POOL,
    ACTOR_RESOLUTION_TOP_K,
    ActorNameResolver,
)

MASK_SALT = "actor-dogfood-hardening-v2"

_POOL_SQL = (
    "WITH matches AS MATERIALIZED ("
    " SELECT a.alias_id,a.normalized_alias,a.document_type,a.base_number,"
    " bm25(actor_aliases_fts) AS retrieval_score"
    " FROM actor_aliases_fts JOIN actor_aliases a ON a.alias_id = actor_aliases_fts.rowid"
    " WHERE actor_aliases_fts MATCH ?"
    "), ranked AS ("
    " SELECT *,row_number() OVER (PARTITION BY document_type,base_number"
    "  ORDER BY retrieval_score,alias_id) AS identity_alias_rank FROM matches"
    ") SELECT * FROM ranked WHERE identity_alias_rank = 1"
    " ORDER BY retrieval_score,alias_id LIMIT ?"
)


def mask(document_type: str, base_number: str) -> str:
    material = f"{MASK_SALT}|{document_type}|{base_number}".encode()
    return hashlib.sha256(material).hexdigest()[:12]


def fts_expression(query: str) -> str | None:
    tokens = [token for token in normalize_actor_name(query).split() if len(token) >= 3]
    if not tokens:
        return None
    return " OR ".join(f'"{token}"' for token in tokens)


def connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro&immutable=1", uri=True,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


# ── Perturbations ─────────────────────────────────────────────────────────
# Deterministic: the position of every edit is a function of the token, so the
# same corpus is produced on every run, on any machine.

def _pivot(token: str) -> int:
    return 1 + (len(token) - 2) // 2


def _substitute(token: str) -> str:
    index = _pivot(token)
    replacement = "k" if token[index] != "k" else "w"
    return token[:index] + replacement + token[index + 1:]


def _transpose(token: str) -> str:
    index = _pivot(token)
    return token[:index - 1] + token[index] + token[index - 1] + token[index + 1:]


def _insert(token: str) -> str:
    index = _pivot(token)
    return token[:index] + token[index] + token[index:]


def _delete(token: str) -> str:
    index = _pivot(token)
    return token[:index] + token[index + 1:]


def _edit(words: list[str], positions: tuple[int, ...], operation) -> str:
    out = list(words)
    for position in positions:
        out[position] = operation(out[position])
    return " ".join(out)


COMMON_SURNAMES = ("rodriguez", "gonzalez", "martinez", "ramirez", "gutierrez")


def perturbations(alias: str, ordinal: int) -> dict[str, str]:
    words = alias.split()
    last = len(words) - 1
    return {
        "clean": alias,
        "one_typo": _edit(words, (0,), _substitute),
        "two_token_typo": _edit(words, (0, last), _substitute),
        "transposition": _edit(words, (0,), _transpose),
        "insertion": _edit(words, (0,), _insert),
        "deletion": _edit(words, (0,), _delete),
        "typo_plus_noise": _edit(words, (0,), _substitute) + ".",
        # Hard negative: the anchor token is kept, the rest names somebody
        # else. Success here is the target NOT appearing.
        "hard_negative": " ".join(
            [words[len(words) // 2], COMMON_SURNAMES[ordinal % len(COMMON_SURNAMES)]]
        ),
    }


NEGATIVE_FAMILIES = frozenset({"hard_negative"})


# ── Corpus ────────────────────────────────────────────────────────────────

@dataclass(frozen=True, slots=True)
class Target:
    alias: str
    document_type: str
    base_number: str

    @property
    def masked(self) -> str:
        return mask(self.document_type, self.base_number)


def sample_targets(connection: sqlite3.Connection, size: int) -> list[Target]:
    """Deterministic sample of unambiguous 3-token personal aliases.

    Only aliases whose identity is the *only* holder of that alias are used:
    with a duplicated alias, a miss would be ambiguity, not recall.
    """
    rows = connection.execute(
        "SELECT normalized_alias, document_type, base_number FROM actor_aliases"
        " WHERE disposition = 'INDEXED_STABLE' AND document_type = 'CC'"
        "  AND length(normalized_alias) - length(replace(normalized_alias,' ','')) = 2"
        "  AND length(normalized_alias) BETWEEN 18 AND 34"
        "  AND alias_id % 977 = 3"
        " ORDER BY alias_id LIMIT ?",
        (size * 3,),
    ).fetchall()
    targets: list[Target] = []
    for row in rows:
        alias = str(row["normalized_alias"])
        words = alias.split()
        if any(len(word) < 5 for word in words):
            continue
        holders = connection.execute(
            "SELECT COUNT(DISTINCT document_type || base_number) FROM actor_aliases"
            " WHERE normalized_alias = ?",
            (alias,),
        ).fetchone()[0]
        if holders != 1:
            continue
        targets.append(Target(alias, str(row["document_type"]), str(row["base_number"])))
        if len(targets) == size:
            break
    return targets


# ── Measurement ───────────────────────────────────────────────────────────

@dataclass
class FamilyReport:
    name: str
    ranks: list[int | None] = field(default_factory=list)
    in_pool: list[bool] = field(default_factory=list)
    latencies_ms: list[float] = field(default_factory=list)

    def summary(self) -> dict:
        found = [rank for rank in self.ranks if rank is not None]
        total = len(self.ranks) or 1
        return {
            "n": len(self.ranks),
            "recall_at_1": round(sum(1 for r in found if r <= 1) / total, 4),
            "recall_at_3": round(sum(1 for r in found if r <= 3) / total, 4),
            "recall_at_10": round(sum(1 for r in found if r <= ACTOR_RESOLUTION_TOP_K) / total, 4),
            "mrr": round(sum(1 / r for r in found) / total, 4),
            "in_pool_rate": round(sum(self.in_pool) / (len(self.in_pool) or 1), 4),
            "latency_p50_ms": round(statistics.median(self.latencies_ms), 1),
            "latency_p95_ms": round(
                statistics.quantiles(self.latencies_ms, n=20)[-1], 1,
            ) if len(self.latencies_ms) >= 20 else None,
        }


def target_rank(result, target: Target) -> int | None:
    if result.status == ActorResolutionStatus.RESOLVED and result.identity is not None:
        same = (
            result.identity.document_type == target.document_type
            and result.identity.base_number == target.base_number
        )
        return 1 if same else None
    for candidate in result.candidates:
        if (
            candidate.identity.document_type == target.document_type
            and candidate.identity.base_number == target.base_number
        ):
            return candidate.rank
    return None


def target_in_pool(connection: sqlite3.Connection, query: str, target: Target) -> bool:
    expression = fts_expression(query)
    if expression is None:
        return False
    rows = connection.execute(_POOL_SQL, (expression, ACTOR_CANDIDATE_POOL)).fetchall()
    return any(
        str(row["document_type"]) == target.document_type
        and str(row["base_number"]) == target.base_number
        for row in rows
    )


def run_baseline(index_path: Path, size: int) -> dict:
    connection = connect(index_path)
    resolver = ActorNameResolver(index_path)
    targets = sample_targets(connection, size)
    families: dict[str, FamilyReport] = {}
    negatives = {"contaminated": 0, "total": 0}

    for ordinal, target in enumerate(targets):
        for family, query in perturbations(target.alias, ordinal).items():
            start = time.perf_counter()
            result = resolver.resolve(query)
            elapsed = (time.perf_counter() - start) * 1000
            rank = target_rank(result, target)
            if family in NEGATIVE_FAMILIES:
                negatives["total"] += 1
                if rank is not None and rank <= 3:
                    negatives["contaminated"] += 1
                continue
            report = families.setdefault(family, FamilyReport(family))
            report.ranks.append(rank)
            report.latencies_ms.append(elapsed)
            report.in_pool.append(
                True if rank is not None else target_in_pool(connection, query, target)
            )
    connection.close()
    return {
        "corpus_size": len(targets),
        "families": {name: report.summary() for name, report in sorted(families.items())},
        "hard_negatives": {
            **negatives,
            "contamination_rate": round(
                negatives["contaminated"] / (negatives["total"] or 1), 4,
            ),
        },
    }


def run_pool_probe(index_path: Path, query: str, target_alias: str) -> dict:
    connection = connect(index_path)
    row = connection.execute(
        "SELECT document_type, base_number FROM actor_aliases WHERE normalized_alias = ?",
        (normalize_actor_name(target_alias),),
    ).fetchone()
    if row is None:
        connection.close()
        return {"query": query, "target_present_in_index": False}
    target = Target(target_alias, str(row["document_type"]), str(row["base_number"]))
    expression = fts_expression(query)
    pool = connection.execute(_POOL_SQL, (expression, ACTOR_CANDIDATE_POOL)).fetchall()
    everything = connection.execute(_POOL_SQL, (expression, 10_000_000)).fetchall()
    full_rank = next(
        (
            index for index, candidate in enumerate(everything, 1)
            if str(candidate["document_type"]) == target.document_type
            and str(candidate["base_number"]) == target.base_number
        ),
        None,
    )
    connection.close()
    return {
        "query": query,
        "target_masked_document": target.masked,
        "target_present_in_index": True,
        "fts_expression": expression,
        "retrieved_identities_total": len(everything),
        "candidate_pool_size": ACTOR_CANDIDATE_POOL,
        "target_in_candidate_pool": any(
            str(candidate["document_type"]) == target.document_type
            and str(candidate["base_number"]) == target.base_number
            for candidate in pool
        ),
        "target_bm25_rank_over_all_matches": full_rank,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--stage", choices=("pool", "baseline"), required=True)
    parser.add_argument("--size", type=int, default=120)
    parser.add_argument("--query", default="wikson herrera llanaos")
    parser.add_argument("--target-alias", default="wilson herrera llanos")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    if args.stage == "pool":
        payload = run_pool_probe(args.index, args.query, args.target_alias)
    else:
        payload = run_baseline(args.index, args.size)
    payload["stage"] = args.stage
    payload["index_sha_prefix"] = hashlib.sha256(
        args.index.read_bytes()[:1 << 20]
    ).hexdigest()[:16]
    rendered = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
    if args.out:
        args.out.write_text(rendered + "\n")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
