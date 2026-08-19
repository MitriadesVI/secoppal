"""AP3-A — benchmark no-runtime de nombre de actor.

Congela la reproducibilidad, la sanitizacion de CC y las reglas que impiden
convertir fuente sucia o similitud fuzzy en autoridad de identidad.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.actor_resolution_benchmark import (
    AliasRow,
    build_fixture_rows,
    build_heldouts,
    exact_policy_metrics,
    normalize_n0,
    normalize_n1,
    normalize_n2,
    normalize_n3,
    sqlite_capabilities,
)

FIXTURE_DIR = Path("tests/fixtures/actor_resolution")
SAMPLE = FIXTURE_DIR / "benchmark_sample.jsonl"
REVIEW = FIXTURE_DIR / "review_pack.jsonl"
METADATA = FIXTURE_DIR / "metadata.json"


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _alias(identity: str, name: str) -> AliasRow:
    document_type = identity.split(":", 1)[0]
    return AliasRow(
        identity=identity,
        document_type=document_type,
        document_display="900000000" if document_type == "NIT" else "******1234",
        observed_name=name,
        occurrences=3,
        first_seen="2024-01-01",
        last_seen="2025-01-01",
        total_occurrences=6,
        distinct_observed_names=2,
        dominant_name=name,
        dominant_name_share=0.5,
    )


def test_normalizations_are_progressive_and_n3_only_removes_a_final_legal_form():
    raw = "  Fundación Ágil, S.A.S.  "
    assert normalize_n0(raw) == "fundación ágil, s.a.s."
    assert normalize_n1(raw) == "fundacion agil  s a s "
    assert normalize_n2(raw) == "fundacion agil s a s"
    assert normalize_n3(raw) == "fundacion agil"
    assert normalize_n3("UNIÓN TEMPORAL FUNDACIÓN CARIBE") == (
        "union temporal fundacion caribe"
    )
    assert normalize_n3("CONSORCIO SALUD LTDA PROYECTO") == (
        "consorcio salud ltda proyecto"
    )


def test_stable_bucket_rule_excludes_cross_type_and_secondary_group_names():
    source_rows = [
        {
            "tipodocproveedor": "NIT",
            "documento_proveedor": "900123456",
            "proveedor_adjudicado": "ACTOR EMPRESA",
            "occurrences": "3",
        },
        {
            "tipodocproveedor": "Cédula de Ciudadanía",
            "documento_proveedor": "900123456",
            "proveedor_adjudicado": "ACTOR PERSONA",
            "occurrences": "3",
        },
        {
            "tipodocproveedor": "NIT",
            "documento_proveedor": "860002400",
            "proveedor_adjudicado": "ACME COLOMBIA SAS",
            "occurrences": "10",
        },
        {
            "tipodocproveedor": "NIT",
            "documento_proveedor": "860002400",
            "proveedor_adjudicado": "ACME COLOMBIA",
            "occurrences": "2",
        },
        {
            "tipodocproveedor": "NIT",
            "documento_proveedor": "860002400",
            "proveedor_adjudicado": "UNION TEMPORAL ACME",
            "occurrences": "1",
        },
    ]

    fixture, summary = build_fixture_rows(source_rows)

    dirty = [row for row in fixture if row["status"] == "EXCLUDED_DIRTY"]
    stable = [row for row in fixture if row["record_type"] == "alias"]
    assert len(dirty) == 2
    assert all("CROSS_TYPE_COLLISION" in row["exclusion_reasons"] for row in dirty)
    assert {row["observed_name"] for row in stable} == {
        "ACME COLOMBIA SAS", "ACME COLOMBIA",
    }
    assert summary["suspected_group_aliases_excluded"] == 1


def test_leave_one_alias_out_requires_two_distinct_n2_forms():
    aliases = [
        _alias("NIT:1", "ACME S.A.S."),
        _alias("NIT:1", "ACME S A S"),
        _alias("NIT:2", "FUNDACION CARIBE"),
        _alias("NIT:2", "FUND CARIBE"),
    ]
    heldouts = build_heldouts(aliases, seed=7, limit=10)
    assert {query.identity for query in heldouts} == {"NIT:2"}
    assert len(heldouts) == 2


def test_exact_policy_detects_ambiguity_and_never_silently_chooses_one():
    aliases = [
        _alias("NIT:1", "SERVICIOS DEL CARIBE"),
        _alias("NIT:2", "SERVICIOS DEL CARIBE"),
        _alias("NIT:3", "FUNDACION UNICA"),
    ]
    metrics = exact_policy_metrics(aliases, "N2", heldouts=0)
    assert metrics["exact_unique_aliases"] == 1
    assert metrics["ambiguous_exact_aliases"] == 1
    assert metrics["ambiguity_detection_rate"] == 1.0
    assert metrics["false_auto_resolution_rate"] == 0.0


def test_local_sqlite_build_really_has_fts5_unicode_and_trigram():
    capabilities = sqlite_capabilities()
    assert capabilities["fts5"] is True
    assert capabilities["tokenizers"] == {"unicode61": True, "trigram": True}


def test_committed_fixture_masks_every_cc_document_and_keeps_type_separate():
    rows = _jsonl(SAMPLE)
    cc_rows = [row for row in rows if row["document_type"] == "CC"]
    assert cc_rows
    for row in cc_rows:
        assert row["identity"].startswith("CC:sample:")
        assert "*" in row["document"]
        assert row["document"].replace("*", "").isdigit()
        assert len(row["document"].replace("*", "")) <= 4


def test_committed_fixture_hash_and_safety_gates_match_metadata():
    metadata = json.loads(METADATA.read_text(encoding="utf-8"))
    digest = hashlib.sha256(SAMPLE.read_bytes()).hexdigest()
    benchmark = metadata["benchmark"]
    policy = benchmark["exact_policy"]

    assert metadata["fixtures"]["sample_sha256"] == digest
    assert benchmark["sample_sha256"] == digest
    assert benchmark["stable_identities"] >= 1_000
    assert policy["policy_corpus_queries"] >= 1_000
    assert benchmark["hard_negatives"] >= 250
    assert benchmark["sqlite"]["tokenizers"]["trigram"] is True
    assert policy["false_auto_resolution_rate"] == 0.0
    assert policy["ambiguity_detection_rate"] == 1.0
    assert benchmark["recommendation"]["fuzzy_auto_resolve"] == "NO"


def test_review_pack_is_human_sized_honest_and_complete():
    rows = _jsonl(REVIEW)
    required = {
        "query_name", "candidate_name", "query_identity", "candidate_identity",
        "occurrences", "document_type", "first_seen", "last_seen",
        "reason_for_selection", "status",
    }
    assert 100 <= len(rows) <= 150
    assert {row["status"] for row in rows} == {"AUTO_GOLD", "REQUIRES_REVIEW"}
    assert all(required <= set(row) for row in rows)
    for row in rows:
        if row["document_type"] == "CC":
            assert "*" in row["document"]
