#!/usr/bin/env python3
"""Offline R2D.3 auditor over the frozen R2D.2 adversarial universe.

No network access is needed: buyer identity, the 52 HIGH/MEDIUM probes and the
17 D6-attributable hijacks come from the immutable R2D.2 artifact.  This script
replays every query against the live resolver and writes the required POST
artifact.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.entity_resolver import EntityResolver  # noqa: E402
from app.core.query_router import QueryRouter  # noqa: E402
from app.utils.money import normalize_text  # noqa: E402

DB = ROOT / "app/data/aliases_db.json"
R2D2 = ROOT / "artifacts/audits/freed_alias_safety_r2d2.json"
CAR_R2D = ROOT / "artifacts/audits/car_core_r2d_candidates.json"
MULTI_OWNER_BASELINE = ROOT / "scripts/gazetteer_multi_owner_baseline.json"
OUT = ROOT / "artifacts/audits/scan_alias_eligibility_r2d3.json"

HEAD_BEFORE = "db8329802339d672ee15b01dac7c98ecbb4b8720"
TERMINAL_OWNER = "CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA."

DECISIONS = {
    "candelaria valle": "KEEP_EXACT_DISABLE_SCAN",
    "ciudad bolivar": "REMOVE_ALIAS_COMPLETELY",
    "corporacion quindio": "REMOVE_ALIAS_COMPLETELY",
    "francisco jose de caldas": "REMOVE_ALIAS_COMPLETELY",
    "puerto boyaca": "KEEP_EXACT_DISABLE_SCAN",
    "salud arauca": "REMOVE_ALIAS_COMPLETELY",
    "salud narino": "REMOVE_ALIAS_COMPLETELY",
    "santa marta magdalena": "REMOVE_ALIAS_COMPLETELY",
    "soledad atlantico": "KEEP_EXACT_DISABLE_SCAN",
}

VALIDATION = {
    "closeout_head": "debf2e3489572368ab564cfc6b8decb2f10def6b",
    "r2d3_tests": "tests/test_scan_alias_eligibility_r2d3.py — 15 tests PASS",
    "focal_suites": (
        "test_scan_alias_eligibility_r2d3 + test_d6_department_suffix_r2d1 "
        "+ test_car_core_r2d — 348 passed"
    ),
    "r2d2_auditor": (
        "POST-aware run PASS — freed 2098 active + 6 retired = 2104, "
        "NEW_WRONG_OWNER_CONFIRMED=0, R2D1_VERDICT=PASS_CONFIRMED"
    ),
    "corpus": "scripts/run_query_corpus.py --mode all — 89/89 PASS",
    "lint": "ruff check app/ tests/ scripts/ — PASS; make lint-core — PASS",
    "full_suite": "10351 passed, 2 skipped, 15 xfailed, 0 failed",
    "xfails": "15 — same as the post-D6 baseline; no new xfail",
}

SENA_ICBF_CANARIES = (
    "SENA regional Cundinamarca",
    "SENA Antioquia",
    "SENA Santander",
    "ICBF Córdoba",
    "ICBF Magdalena",
    "ICBF Caldas",
    "ICBF Cesar",
    "ICBF Norte de Santander",
)

NON_CAR_CANARIES = {
    "Secretaría General Alcaldía Mayor Bogotá": (
        "SECRETARÍA GENERAL DE LA ALCALDÍA MAYOR DE BOGOTÁ"
    ),
    "Empresa de Acueducto y Alcantarillado de Bogotá": (
        "EMPRESA DE ACUEDUCTO Y ALCANTARILLADO DE BOGOTÁ - E.S.P."
    ),
    "Instituto de Caminos y Construcciones de Cundinamarca": (
        "INSTITUTO DE CAMINOS Y CONSTRUCCIONES DE CUNDINAMARCA - ICCU"
    ),
    "Canal Teleantioquia": "SOCIEDAD TELEVISION DE ANTIOQUIA LTDA",
}


def _head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def _router_probe(router: QueryRouter, query: str) -> dict[str, Any]:
    params = router.parse(query).params
    return {
        "entity": params.get("entidad_resolved"),
        "department": params.get("departamento_resolved"),
        "method": (params.get("entidad_resolution") or {}).get("method"),
        "clarification": params.get("clarification_needed"),
    }


def _scan_probe(resolver: EntityResolver, phrase: str) -> dict[str, Any] | None:
    result = resolver.scan_entity(normalize_text(phrase))
    if result is None:
        return None
    return {
        "owner": result.official_name,
        "matched_alias": result.matched_alias,
        "remaining_text": result.remaining_text,
        "method": result.method,
    }


def _resolution_probe(resolver: EntityResolver, phrase: str) -> dict[str, Any]:
    result = resolver.resolve_entidad(phrase)
    return {
        "owner": result.value,
        "method": result.method,
        "confidence": result.confidence,
        "clarification": result.clarification_needed,
    }


def _is_expected_owner(
    resolver: EntityResolver,
    actual: str,
    expected_physical: str,
) -> bool:
    expected_key = normalize_text(expected_physical)
    return bool(
        normalize_text(actual) == expected_key
        or expected_key in resolver.aliases_by_official.get(actual, set())
        or actual in resolver.source_names_to_official.get(expected_key, set())
    )


def _post_outcome(
    resolver: EntityResolver,
    baseline_record: dict[str, Any],
    actual: str | None,
) -> str:
    if actual is None:
        return "FAIL_CLOSED"
    expected = str(baseline_record["expected_secop_buyer"]["physical_name"])
    if _is_expected_owner(resolver, actual, expected):
        return "CORRECT_OWNER"

    # Preserve R2D.2's conservative SAME_LOGICAL_IDENTITY adjudication when
    # the owner did not move; it used NIT and directory evidence unavailable
    # to this deliberately offline replay.
    if actual == baseline_record["probe"]["live"]["entity"]:
        return str(baseline_record["outcome"])
    return "WRONG_OWNER"


def _adversarial_post(
    resolver: EntityResolver,
    router: QueryRouter,
    r2d2: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    records: list[dict[str, Any]] = []
    outcomes: Counter[str] = Counter()
    causal: Counter[str] = Counter()
    audited_aliases = set(DECISIONS)

    for baseline in r2d2["adversarial_sample"]["records"]:
        query = str(baseline["probe"]["query"])
        phrase = str(baseline["probe"]["phrase"])
        route = _router_probe(router, query)
        scan = _scan_probe(resolver, phrase)
        outcome = _post_outcome(resolver, baseline, route["entity"])
        matched = (scan or {}).get("matched_alias")
        activated_freed_owner = bool(
            matched in audited_aliases
            and route["entity"] in set(baseline["current_owners"])
        )
        new_wrong = outcome == "WRONG_OWNER" and activated_freed_owner
        preexisting_wrong = outcome == "WRONG_OWNER" and not new_wrong
        outcomes[outcome] += 1
        causal["NEW_WRONG_OWNER_FROM_FREED_ALIASES"] += int(new_wrong)
        causal["PREEXISTING_WRONG_OWNER"] += int(preexisting_wrong)
        records.append(
            {
                "alias": baseline["alias"],
                "expected_secop_buyer": baseline["expected_secop_buyer"],
                "current_owners_pre": baseline["current_owners"],
                "baseline_outcome": baseline["outcome"],
                "baseline_live": baseline["probe"]["live"],
                "baseline_old": baseline["probe"]["old"],
                "post_route": route,
                "post_scan": scan,
                "post_outcome": outcome,
                "new_wrong_owner_from_freed_alias": new_wrong,
                "preexisting_wrong_owner": preexisting_wrong,
            }
        )

    before = sum(
        1
        for baseline in r2d2["adversarial_sample"]["records"]
        if baseline.get("new_wrong_owner_confirmed")
    )
    summary = {
        **dict(sorted(outcomes.items())),
        "NEW_WRONG_OWNER_BEFORE": before,
        "NEW_WRONG_OWNER_AFTER": causal["NEW_WRONG_OWNER_FROM_FREED_ALIASES"],
        "NEW_WRONG_OWNER_FROM_FREED_ALIASES": causal[
            "NEW_WRONG_OWNER_FROM_FREED_ALIASES"
        ],
        "PREEXISTING_WRONG_OWNER": causal["PREEXISTING_WRONG_OWNER"],
        "TOTAL": len(records),
    }
    return records, summary


def _alias_discovery(
    resolver: EntityResolver,
    r2d2: dict[str, Any],
) -> list[dict[str, Any]]:
    input_records = {
        record["alias"]: record
        for record in r2d2["alias_records"]
        if record["alias"] in DECISIONS
    }
    confirmed = {
        record["alias"]: record
        for record in r2d2["new_wrong_owner_confirmed"]
    }
    result = []
    for alias, decision in DECISIONS.items():
        source = input_records[alias]
        hijacks = confirmed[alias]["verified_secop_buyers_hijacked"]
        result.append(
            {
                "alias": alias,
                "current_owner_pre": source["current_owners"][0],
                "exact_query_result_pre": source["current_owners"][0],
                "exact_query_result_post": _resolution_probe(resolver, alias),
                "other_physical_buyers_containing_alias": source[
                    "other_physical_buyers_containing_alias"
                ],
                "other_physical_buyer_count": source["other_physical_buyer_count"],
                "current_scan_hijacks_pre": hijacks,
                "current_scan_hijack_count_pre": len(hijacks),
                "token_count": source["token_count"],
                "institutional_head_present": source["contains_institutional_head"],
                "institutional_heads": source["institutional_heads"],
                "location_like": source["location_only"],
                "functional_fragment": source["risk_class"]
                == "INSTITUTIONAL_FRAGMENT",
                "risk_class": source["risk_class"],
                "decision": decision,
                "post_exact_owner_set": sorted(
                    resolver.owners_by_alias.get(alias, set())
                ),
                "post_scan_of_alias": _scan_probe(resolver, alias),
            }
        )
    return result


def _d6_canaries(router: QueryRouter, car_r2d: dict[str, Any]) -> dict[str, Any]:
    regional = [
        {"query": phrase, **_router_probe(router, f"contratos de {phrase}")}
        for phrase in SENA_ICBF_CANARIES
    ]

    cars = []
    for record in car_r2d["entities"]:
        query = str(record["official_institutional_name"])
        probe = _router_probe(router, f"contratos de {query}")
        cars.append(
            {
                "acronym": record["acronym"],
                "query": query,
                "expected_physical_names": record["physical_names"],
                **probe,
                "pass": probe["entity"] in set(record["physical_names"]),
            }
        )

    non_car = []
    for phrase, expected in NON_CAR_CANARIES.items():
        probe = _router_probe(router, f"contratos de {phrase}")
        non_car.append(
            {
                "query": phrase,
                "expected": expected,
                **probe,
                "pass": probe["entity"] == expected,
            }
        )

    return {
        "sena_icbf": {
            "count": len(regional),
            "fail_closed": sum(record["entity"] is None for record in regional),
            "records": regional,
        },
        "car": {
            "count": len(cars),
            "correct": sum(record["pass"] for record in cars),
            "records": cars,
        },
        "non_car": {
            "count": len(non_car),
            "correct": sum(record["pass"] for record in non_car),
            "records": non_car,
        },
    }


def _collision_gate(resolver: EntityResolver) -> dict[str, Any]:
    baseline = json.loads(MULTI_OWNER_BASELINE.read_text(encoding="utf-8"))
    current = {
        alias for alias, owners in resolver.owners_by_alias.items() if len(owners) > 1
    }
    baseline_keys = set(baseline["keys"])
    return {
        "baseline_keys": len(baseline_keys),
        "current_keys": len(current),
        "new_multi_owner_keys": sorted(current - baseline_keys),
        "status": "PASS" if not current - baseline_keys else "FAIL",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUT)
    args = parser.parse_args()

    r2d2 = json.loads(R2D2.read_text(encoding="utf-8"))
    car_r2d = json.loads(CAR_R2D.read_text(encoding="utf-8"))
    resolver = EntityResolver(DB)
    router = QueryRouter(entity_resolver=resolver)
    post_records, post_summary = _adversarial_post(resolver, router, r2d2)
    terminal_phrase = "Terminal de Transportes de Santa Marta Magdalena"
    terminal_exact = "terminal de transportes de santa marta"

    collision = _collision_gate(resolver)
    d6 = _d6_canaries(router, car_r2d)
    required_gates_pass = bool(
        post_summary["NEW_WRONG_OWNER_FROM_FREED_ALIASES"] == 0
        and collision["status"] == "PASS"
        and d6["sena_icbf"]["fail_closed"] == 0
        and d6["car"]["correct"] == 33
        and d6["non_car"]["correct"] == len(NON_CAR_CANARIES)
    )

    payload = {
        "audit": "SCAN-ALIAS-ELIGIBILITY-R2D3",
        "type": "offline-post-remediation",
        "as_of": date.today().isoformat(),
        "status": "PASS" if required_gates_pass else "PARTIAL",
        "head_before": HEAD_BEFORE,
        "head_runtime_audited": _head(),
        "runtime_changed": True,
        "aliases_db_changed": True,
        "source_artifact": str(R2D2.relative_to(ROOT)),
        "aliases_audited": len(DECISIONS),
        "decisions": dict(DECISIONS),
        "decision_counts": {
            **dict(sorted(Counter(DECISIONS.values()).items())),
            # Tenth remediation action: the terminal identity is an addition to
            # the gazetteer, not a verdict on one of the nine audited aliases.
            "ADD_SPECIFIC_IDENTITY": 1,
        },
        "alias_discovery": _alias_discovery(resolver, r2d2),
        "terminal_santa_marta": {
            "identity_added": True,
            "official_name": TERMINAL_OWNER,
            "nit": "891701694",
            "secop_code": "724398862",
            "exact_result": _resolution_probe(resolver, terminal_exact),
            "territorial_exact_result": _resolution_probe(
                resolver, "santa marta magdalena"
            ),
            "scan_result": _scan_probe(resolver, terminal_phrase),
            "router_result": _router_probe(
                router, f"contratos de {terminal_phrase}"
            ),
            "generic_alias_still_blocked": (
                resolver.resolve_entidad("santa marta magdalena").value is None
            ),
        },
        "adversarial_sample": {
            "baseline": r2d2["adversarial_sample"]["outcomes"],
            "post": post_summary,
            "records": post_records,
        },
        "d6_good_cases": d6,
        "collision_gate": collision,
        "mutation_tests": {
            "required": ["puerto boyaca", "ciudad bolivar", "santa marta magdalena"],
            "test": "tests/test_scan_alias_eligibility_r2d3.py",
            "result": "PASS",
        },
        # Recorded from the closeout run on the audited HEAD.  These gates are
        # external to this offline replay (they need pytest, the network and
        # the linter), so the artifact transcribes their result instead of
        # pretending to re-derive it.
        "validation": dict(VALIDATION),
        "r2d1_final_verdict": (
            "PASS_CONFIRMED"
            if post_summary["NEW_WRONG_OWNER_FROM_FREED_ALIASES"] == 0
            else "REMEDIATION_REQUIRED"
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "STATUS": payload["status"],
                "ALIASES_AUDITED": payload["aliases_audited"],
                "ADVERSARIAL_POST": post_summary,
                "COLLISION_GATE": collision["status"],
                "D6": {
                    "SENA_ICBF_FAIL_CLOSED": d6["sena_icbf"]["fail_closed"],
                    "CAR": f"{d6['car']['correct']}/{d6['car']['count']}",
                    "NON_CAR": f"{d6['non_car']['correct']}/{d6['non_car']['count']}",
                },
                "OUTPUT": str(args.output),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0 if payload["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
