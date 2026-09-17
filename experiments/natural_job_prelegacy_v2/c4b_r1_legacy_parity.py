"""C4B-R1 — legacy parity probe: BASE (6b30737) vs the remediated candidate.

C4B had moved 28 functional forms into the GLOBAL closed class
(``app/data/stopwords.py``). That file feeds the legacy parser
(``QueryRouter.parse``), ``source_occurrence`` and the spell corrector, so the
move changed legacy decisions that C4B had no business touching. C4B-R1 puts them
back into a private class of the credentialed lane; this probe measures that the
legacy lane is byte-identical again.

Two snapshots and two gates:

  LEGACY_PARAM_DIFF_WITH_FLAGS_OFF       every corpus query parsed by QueryRouter
  SOURCE_OCCURRENCE_DIFF_WITH_FLAGS_OFF  the source-request probes (spans, roles, mask)

``--with-flags-off`` is the default and the only mode this probe is about: the
Frame and the A2 binding stay off, so what is measured is the legacy lane.

Run:
  python experiments/natural_job_prelegacy_v2/c4b_r1_legacy_parity.py --out /tmp/snap.json
  python experiments/natural_job_prelegacy_v2/c4b_r1_legacy_parity.py \
      --compare BASE.json CANDIDATE.json --out experiments/.../c4b_r1_legacy_parity.json
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import tempfile
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

TODAY = date(2026, 9, 17)

# 20 queries for the parser. The first seven are the corpus the audit named as
# the impact radius; the rest are legacy phrasings that exercise the same
# surface — interrogatives, clitics, prepositions, source words and the frozen
# modality observation of tests/test_modality_followup_pipeline.py.
QUERY_CORPUS = (
    "quien es el",
    "quienes son los",
    "le ha",
    "su",
    "es",
    "contra",
    "mediante",
    "contratación directa para quien usa plataforma",
    "quien es el contratista de la alcaldia de galapa",
    "quienes son los proveedores de la alcaldia de barranquilla",
    "le ha contratado a la alcaldia de galapa",
    "su proveedor de galapa",
    "a quien le ha comprado mas el municipio de galapa",
    "con quien ha contratado la alcaldia de galapa en 2025",
    "cuales son los proveedores con contratos mas grandes de alcaldia de barranquilla",
    "proveedores de galapa en 2025",
    "dame los 5 principales proveedores de galapa",
    "no me muestres con quien ha contratado Galapa",
    "contratos de educacion de la alcaldia de cali",
    "muestrame los contratos de galapa mediante licitacion publica",
)

# The source-occurrence probes the audit named, plus the inputs that actually
# discriminate C4B from base: a bare functional prefix ("su ", "le ", "mediante ")
# became request framing while the 28 forms lived in the global class, so a
# source word after it was accredited as a REQUEST where base accredited none.
SOURCE_PROBES = (
    "quien es el", "quienes son los", "le ha", "su", "es", "contra", "mediante",
    "su procesos", "le procesos", "les procesos", "lo procesos", "me procesos",
    "mi procesos", "mis procesos", "nos procesos", "te procesos", "tu contratos",
    "tus procesos", "al procesos", "ante procesos", "cabe procesos",
    "contra procesos", "desde procesos", "durante procesos", "hacia procesos",
    "hasta procesos", "tras procesos", "mediante procesos", "quien procesos",
    "quienes procesos", "quien es el contratos", "es el contratos", "su el procesos",
    "muestrame su procesos",
)

GATES = ("LEGACY_PARAM_DIFF_WITH_FLAGS_OFF", "SOURCE_OCCURRENCE_DIFF_WITH_FLAGS_OFF")


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def snapshot() -> dict:
    """The legacy lane with both flags off, over the corpus and the probes."""
    from app.config import Settings
    from app.core.entity_resolver import EntityResolver
    from app.core.query_router import QueryRouter
    from app.core.source_occurrence import mask_source_requests, source_request_occurrences

    scratch = pathlib.Path(tempfile.mkdtemp(prefix="c4b-r1-parity-"))
    settings = Settings(
        _env_file=None,
        secoppal_natural_alpha_mode="off",
        secoppal_a2_suppliers_binding_enabled=False,
        secoppal_entity_profile_frame_enabled=False,
        secop_feedback_path=scratch / "traces.jsonl",
        secop_feedback_db_path=scratch / "feedback.db",
    )
    router = QueryRouter(entity_resolver=EntityResolver(settings.secop_alias_db_path))
    params = {
        query: _canonical(router.parse(query, today=TODAY).params)
        for query in QUERY_CORPUS
    }
    occurrences = {
        probe: _canonical([
            {"span": list(occurrence.span), "surface": occurrence.surface,
             "is_process": occurrence.is_process,
             "clause_index": occurrence.clause_index,
             "role": occurrence.role.value}
            for occurrence in source_request_occurrences(probe)
        ])
        for probe in SOURCE_PROBES
    }
    masks = {probe: mask_source_requests(probe) for probe in SOURCE_PROBES}
    return {
        "flags": {"natural_alpha": "off", "a2_suppliers_binding": False,
                  "entity_profile_frame": False},
        "alias_db": str(settings.secop_alias_db_path),
        "params": params,
        "source_occurrences": occurrences,
        "source_masks": masks,
    }


def compare(base: dict, candidate: dict) -> dict:
    param_diff = {
        query: {"base": base["params"].get(query), "candidate": candidate["params"].get(query)}
        for query in QUERY_CORPUS
        if base["params"].get(query) != candidate["params"].get(query)
    }
    occurrence_diff = {}
    for probe in SOURCE_PROBES:
        before = (base["source_occurrences"].get(probe), base["source_masks"].get(probe))
        after = (candidate["source_occurrences"].get(probe), candidate["source_masks"].get(probe))
        if before != after:
            occurrence_diff[probe] = {
                "base": {"occurrences": before[0], "mask": before[1]},
                "candidate": {"occurrences": after[0], "mask": after[1]},
            }
    return {
        "gates": {
            "LEGACY_PARAM_DIFF_WITH_FLAGS_OFF": len(param_diff),
            "SOURCE_OCCURRENCE_DIFF_WITH_FLAGS_OFF": len(occurrence_diff),
        },
        "queries": len(QUERY_CORPUS),
        "source_probes": len(SOURCE_PROBES),
        "param_diff": param_diff,
        "source_occurrence_diff": occurrence_diff,
        "base_alias_db": base.get("alias_db"),
        "candidate_alias_db": candidate.get("alias_db"),
        "same_environment": base.get("alias_db") == candidate.get("alias_db"),
        "flags": candidate.get("flags"),
    }


def _write(path: pathlib.Path, document: dict) -> None:
    path.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    sys.stdout.write(json.dumps({"out": str(path)}, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="")
    parser.add_argument("--compare", nargs=2, metavar=("BASE", "CANDIDATE"), default=None)
    parser.add_argument("--control", default="",
                        help="optional pre-remediation snapshot: proves the probe has teeth")
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    if args.compare:
        base = json.loads(pathlib.Path(args.compare[0]).read_text(encoding="utf-8"))
        candidate = json.loads(pathlib.Path(args.compare[1]).read_text(encoding="utf-8"))
        document = {
            "checkpoint": "C4B-R1 legacy parity probe",
            "purpose": "the 28 forms moved back from the global closed class to the lane's "
                       "private one: the legacy lane must be identical to base",
            "base": args.compare[0],
            "candidate": args.compare[1],
            **compare(base, candidate),
            "snapshot_base": base,
            "snapshot_candidate": candidate,
        }
        document["gates_all_zero"] = all(document["gates"][name] == 0 for name in GATES)
        if args.control:
            control = json.loads(pathlib.Path(args.control).read_text(encoding="utf-8"))
            result = compare(base, control)
            document["negative_control"] = {
                "snapshot": args.control,
                "meaning": "the same probe run on C4B BEFORE this remediation: the diff it "
                           "reports is the impact radius the audit measured, so a zero in "
                           "the gate above is a measurement, not a blind spot",
                "gates": result["gates"],
                "param_diff_queries": sorted(result["param_diff"]),
                "source_occurrence_diff_probes": sorted(result["source_occurrence_diff"]),
            }
    else:
        document = {"checkpoint": "C4B-R1 legacy parity snapshot", "label": args.label,
                    **snapshot()}
    if args.out:
        _write(pathlib.Path(args.out), document)
    else:
        sys.stdout.write(json.dumps(
            {key: value for key, value in document.items() if not key.startswith("snapshot")},
            ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
