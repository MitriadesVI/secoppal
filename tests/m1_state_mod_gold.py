"""Oracle-IR access to the synthetic Gold for the STATE / MODALITY authorities (M1-D-STATE/MOD).

Only the three SYNTHETIC slices under ``docs/audits/m1_search_foundation/gold/`` are read here; the
REAL private slice never enters git (it is measured by gold_id only, by
``scripts/audits/m1_d_state_mod/gold_slice.py``).  The oracle IR gives, per condition, the dimension,
operator, polarity and quote; the context datasets come from the request universe (UNSPECIFIED = every
candidate dataset, the V2 rule of the contract).
"""
from __future__ import annotations

import json
from pathlib import Path

from app.core.foundation_authority_contract import AuthorityContext
from app.core.interpretation_contract import Polarity
from app.core.semantic_dimensions import SemanticDimension
from tests.foundation_authority_kit import REFERENCE_INSTANT, evidence_from_text

GOLD_DIR = Path(__file__).resolve().parents[1] / "docs" / "audits" / "m1_search_foundation" / "gold"
SYNTHETIC_FILES = ("minimal_pair.jsonl", "neighbor.jsonl", "adversarial.jsonl")
PROCESSES_ID, CONTRACTS_ID = "p6dx-8zbt", "jbjy-vk9h"
_DATASETS = {"PROCESSES": (PROCESSES_ID,), "CONTRACTS": (CONTRACTS_ID,),
             "UNSPECIFIED": (PROCESSES_ID, CONTRACTS_ID)}


def load_rows() -> list[dict]:
    rows = []
    for name in SYNTHETIC_FILES:
        rows += [json.loads(line) for line in (GOLD_DIR / name).read_text(encoding="utf-8").splitlines()]
    return rows


def conditions(dimension: str):
    """``(gold_id, text, universe, condition)`` for every oracle condition of ``dimension``."""
    for row in load_rows():
        for request in row["requests"]:
            for condition in request["conditions"]:
                if condition["dimension"] == dimension:
                    yield row["gold_id"], row["text"], request["universe"], condition


def evidence_and_context(text: str, universe: str, condition: dict, dimension: SemanticDimension):
    evidence = evidence_from_text(
        text, condition["quote"], dimension=dimension, operator=condition["operator"],
        polarity=Polarity(condition["polarity"]), condition_id=condition["id"])
    return evidence, AuthorityContext(REFERENCE_INSTANT, frozenset(_DATASETS[universe]))


def outcome(verdict) -> str:
    """A stable one-line disposition of a verdict (what the Gold test freezes)."""
    if verdict.value is not None:
        return f"RESOLVED:{getattr(verdict.value, 'canonical_class', None) or 'modality'}"
    return f"{verdict.decision.value}:{verdict.reason}:{verdict.detail}"
