"""STATE / MODALITY slice of the Gold with the oracle IR (M1-D-STATE/MOD).

Synthetic slices (in git) are reported with their text; the REAL private slice (never in git) is
reported ONLY by gold_id and counts.  Usage:
``PYTHONPATH=. python scripts/audits/m1_d_state_mod/gold_slice.py [--real PATH]``.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sys

from app.core.foundation_authority_contract import AuthorityContext
from app.core.host_authorities.modality import ModalityAuthority
from app.core.host_authorities.state import StateAuthority
from app.core.interpretation_contract import Polarity, RawSpan
from app.core.foundation_evidence_window import build_window
from app.core.semantic_dimensions import SemanticDimension
from tests.foundation_authority_kit import REFERENCE_INSTANT
from tests.foundation_authority_kit import evidence_from_text
from tests.m1_state_mod_gold import GOLD_DIR, SYNTHETIC_FILES, _DATASETS  # noqa: PLC2701

REAL_DEFAULT = os.path.expanduser("~/Documents/tools/secoppal-diagnostics/m1_search_gold/sealed_v1/gold_v1.jsonl")
# oracle value_hint -> canonical class (the Gold spells the classes the contract lacks as ``other:<x>``)
STATE_CLASS = {"other:published_or_open": "open", "other:borrador": "draft", "other:en_evaluacion": "in_evaluation",
               "other:seleccionado": "awarded", "other:in_competition": "open", "other:en_curso": "open",
               "other:celebrado": "signed", "other:liquidado": "liquidated", "other:aprobado": "approved"}
AUTHORITIES = {"STATE": StateAuthority(), "MODALITY": ModalityAuthority()}
MODALITY_IDS = {"licitacion_publica": ("licitacion_publica", "licitacion_publica_obra"),
                "contratacion_directa": ("contratacion_directa", "contratacion_directa_con_ofertas"),
                "minima_cuantia": ("minima_cuantia",)}


def _evidence(text: str, condition: dict, dimension: SemanticDimension):
    quote = condition["quote"]
    index = text.find(quote)
    if index < 0:
        match = re.search(re.escape(quote), text, re.IGNORECASE)
        if match is None:
            return None
        index = match.start()
    span = RawSpan(index, index + len(quote), text[index:index + len(quote)])
    return evidence_from_text(text, text[span.start:span.end], dimension=dimension,
                              operator=condition["operator"], polarity=Polarity(condition["polarity"]),
                              condition_id=condition["id"], occurrence=text[:index].count(text[span.start:span.end]))


def _outcome(verdict) -> str:
    if verdict.value is not None:
        return f"RESOLVED:{getattr(verdict.value, 'canonical_class', 'modality')}"
    return f"{verdict.decision.value}:{verdict.reason}:{verdict.detail}"


def measure(rows: list[dict], *, show_text: bool) -> dict:
    report = {name: collections.defaultdict(list) for name in AUTHORITIES}
    for row in rows:
        for request in row["requests"]:
            for condition in request["conditions"]:
                name = condition["dimension"]
                if name not in AUTHORITIES:
                    continue
                universe = request["universe"]
                if universe not in _DATASETS:
                    universe = "UNSPECIFIED"
                dimension = SemanticDimension[name]
                try:
                    evidence = _evidence(row["text"], condition, dimension)
                except Exception as error:  # a window the builder refuses is a finding, not a crash
                    report[name][f"WINDOW_REFUSED:{type(error).__name__}"].append(row["gold_id"])
                    continue
                if evidence is None:
                    report[name]["QUOTE_NOT_IN_TEXT"].append(row["gold_id"])
                    continue
                verdict = AUTHORITIES[name].resolve(evidence, AuthorityContext(REFERENCE_INSTANT,
                                                                               frozenset(_DATASETS[universe])))
                key = _outcome(verdict)
                hint = condition.get("value_hint")
                if verdict.value is not None and name == "STATE":
                    expected = STATE_CLASS.get(hint, hint)
                    if verdict.value.canonical_class != expected:
                        key = f"RESOLVED_CLASS_DIFFERS_FROM_HINT:{verdict.value.canonical_class}"
                tag = row["gold_id"] if not show_text else f"{row['gold_id']} «{condition['quote']}»"
                report[name][key].append(tag)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--real", nargs="?", const=REAL_DEFAULT, default=None)
    args = parser.parse_args()
    synthetic = []
    for name in SYNTHETIC_FILES:
        synthetic += [json.loads(line) for line in (GOLD_DIR / name).read_text(encoding="utf-8").splitlines()]
    sections = [("SYNTHETIC", synthetic, True)]
    if args.real:
        real = [json.loads(line) for line in open(args.real, encoding="utf-8")]
        sections.append(("REAL (private, gold_id only)", real, False))
    for title, rows, show_text in sections:
        print(f"== {title}: {len(rows)} records")
        for dimension, outcomes in measure(rows, show_text=show_text).items():
            print(f"-- {dimension}: {sum(len(v) for v in outcomes.values())} conditions")
            for key, ids in sorted(outcomes.items(), key=lambda item: -len(item[1])):
                shown = ", ".join(ids) if not key.startswith("RESOLVED:") or not show_text else f"{len(ids)} ids"
                print(f"   {len(ids):3d}  {key}  [{shown if len(ids) <= 40 else str(len(ids)) + ' ids'}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
