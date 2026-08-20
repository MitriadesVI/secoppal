"""AP3-B/B1 — índice local reproducible y política de fuente estable."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from app.core.actor_name import (
    ACTOR_NAME_NORMALIZATION_VERSION,
    normalize_actor_name,
)
from app.data.actor_alias_index import (
    ACTOR_ALIAS_INDEX_SCHEMA_VERSION,
    BuildDisposition,
    build_actor_alias_index,
    validate_actor_alias_index,
)


def _row(
    document_type: str,
    document: str,
    name: str,
    occurrences: int = 5,
    *,
    first_seen: str | None = "2024-01-01",
    last_seen: str | None = "2025-01-01",
) -> dict:
    return {
        "tipodocproveedor": document_type,
        "documento_proveedor": document,
        "proveedor_adjudicado": name,
        "occurrences": str(occurrences),
        "first_seen": first_seen,
        "last_seen": last_seen,
    }


def _build(tmp_path: Path, rows: list[dict], name: str = "actors.sqlite") -> Path:
    path = tmp_path / name
    build_actor_alias_index(
        rows,
        path,
        source_dataset="fixture-contracts",
        built_at="2026-08-19T12:00:00+00:00",
        build_parameters={"fixture": "AP3-B"},
    )
    return path


def test_n2_normalization_is_versioned_and_frozen():
    assert ACTOR_NAME_NORMALIZATION_VERSION == "N2"
    cases = {
        "  Fundación Ágil, S.A.S.  ": "fundacion agil s a s",
        "FUNDACIÓN\tÁGIL\nSAS": "fundacion agil sas",
        "Unión Temporal Caribe": "union temporal caribe",
        "CONSORCIO SALUD LTDA": "consorcio salud ltda",
        "O'Connor & Hijos": "o connor hijos",
        "": "",
    }
    for raw, expected in cases.items():
        assert normalize_actor_name(raw) == expected
    assert normalize_actor_name(None) == ""


def test_schema_keeps_alias_to_identity_multiplicity_and_trigram_fts(tmp_path):
    path = _build(tmp_path, [
        _row("NIT", "900111222", "SERVICIOS DEL CARIBE", 80),
        _row("NIT", "900222333", "Servicios del Caribe", 3),
        _row("NIT", "900333444", "FUNDACIÓN ÁGIL S.A.S.", 7),
    ])

    metadata = validate_actor_alias_index(path)
    assert metadata.schema_version == ACTOR_ALIAS_INDEX_SCHEMA_VERSION
    assert metadata.normalization_version == "N2"
    assert metadata.alias_count == 3
    assert metadata.identity_count == 3
    assert metadata.ambiguous_alias_count == 1

    with sqlite3.connect(path) as connection:
        ambiguous = connection.execute(
            "SELECT document_type,base_number,disposition FROM actor_aliases "
            "WHERE normalized_alias = ? ORDER BY base_number",
            ("servicios del caribe",),
        ).fetchall()
        assert ambiguous == [
            ("NIT", "900111222", BuildDisposition.INDEXED_AMBIGUOUS),
            ("NIT", "900222333", BuildDisposition.INDEXED_AMBIGUOUS),
        ]
        fts_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'actor_aliases_fts'"
        ).fetchone()[0]
        assert "tokenize='trigram'" in fts_sql
        assert connection.execute(
            "SELECT count(*) FROM actor_aliases_fts "
            "WHERE actor_aliases_fts MATCH ?",
            ('"fundacion"',),
        ).fetchone()[0] == 1


def test_build_classifies_stable_ambiguous_and_dirty_records_exactly(tmp_path):
    rows = [
        _row("NIT", "900111222", "ACTOR ESTABLE", 5),
        _row("NIT", "900222333", "ALIAS COMPARTIDO", 30),
        _row("NIT", "900333444", "ALIAS COMPARTIDO", 3),
        _row("NIT", "13848603", "UNION TEMPORAL EMPRESA", 5),
        _row("Cédula de Ciudadanía", "13848603", "PERSONA OBSERVADA", 20),
        _row("Otro", "99999111", "TIPO DESCONOCIDO", 4),
        _row("NIT", "900444555", "NO DEFINIDO", 4),
        _row("NIT", "860002400", "ACME COLOMBIA SAS", 10),
        _row("NIT", "860002400", "UNION TEMPORAL ACME", 1),
    ]
    path = _build(tmp_path, rows)
    metadata = validate_actor_alias_index(path)

    assert metadata.identity_count == 4
    assert metadata.ambiguous_alias_count == 1
    assert metadata.excluded_dirty_count == 5
    quality = metadata.source_quality
    assert quality["dirty_buckets_excluded"] == 2
    assert quality["secondary_group_aliases_excluded"] == 1
    assert quality["unknown_document_type"] == 1
    assert quality["empty_or_sentinel_name"] == 1

    with sqlite3.connect(path) as connection:
        indexed = connection.execute(
            "SELECT normalized_alias,disposition FROM actor_aliases "
            "ORDER BY normalized_alias,base_number"
        ).fetchall()
        assert ("actor estable", BuildDisposition.INDEXED_STABLE) in indexed
        assert indexed.count((
            "alias compartido", BuildDisposition.INDEXED_AMBIGUOUS,
        )) == 2
        forbidden = {
            "union temporal empresa", "persona observada",
            "tipo desconocido", "no definido", "union temporal acme",
        }
        assert forbidden.isdisjoint({row[0] for row in indexed})

        audit = connection.execute(
            "SELECT normalized_alias,reason_codes_json "
            "FROM actor_alias_build_audit WHERE disposition = 'EXCLUDED_DIRTY'"
        ).fetchall()
        reasons = {name: set(json.loads(raw_reasons)) for name, raw_reasons in audit}
        assert "CROSS_TYPE_COLLISION" in reasons["persona observada"]
        assert reasons["union temporal acme"] == {"SECONDARY_GROUP_ALIAS"}
        assert reasons["tipo desconocido"] == {"UNKNOWN_DOCUMENT_TYPE"}


def test_dirty_bucket_cannot_enter_exact_or_fts_evidence(tmp_path):
    path = _build(tmp_path, [
        _row("NIT", "13848603", "COLLIDING ACTOR", 100),
        _row("Cédula de Ciudadanía", "13848603", "COLLIDING ACTOR", 1),
    ])
    metadata = validate_actor_alias_index(path)
    assert metadata.alias_count == 0
    assert metadata.identity_count == 0
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM actor_aliases WHERE normalized_alias = ?",
            ("colliding actor",),
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM actor_aliases_fts"
        ).fetchone()[0] == 0


def test_same_n2_alias_variants_collapse_only_within_one_typed_identity(tmp_path):
    path = _build(tmp_path, [
        _row("NIT", "900111222", "FUNDACIÓN ÁGIL", 3),
        _row("NIT", "900111222", "Fundacion Agil", 4),
    ])
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT normalized_alias,raw_alias,raw_aliases_json,occurrences "
            "FROM actor_aliases"
        ).fetchone()
    assert row[0] == "fundacion agil"
    assert row[1] == "Fundacion Agil"
    assert set(json.loads(row[2])) == {"FUNDACIÓN ÁGIL", "Fundacion Agil"}
    assert row[3] == 7


def test_base_and_attached_dv_need_shared_alias_evidence_to_remain_authoritative(
    tmp_path,
):
    # AP1 recognizes 8 as the valid DV of 86000773.  A shared N2 name bridges
    # the two source forms and documents that they are representation variants.
    path = _build(tmp_path, [
        _row("NIT", "86000773", "ACTOR PUENTE", 4),
        _row("NIT", "860007738", "Actor Puente", 5),
    ])
    metadata = validate_actor_alias_index(path)

    assert metadata.identity_count == 1
    assert metadata.alias_count == 1
    assert metadata.source_quality[
        "unbridged_document_form_collapse_buckets_excluded"
    ] == 0


def test_disjoint_names_on_base_and_inferred_dv_form_are_excluded(tmp_path):
    path = _build(tmp_path, [
        _row("NIT", "86000773", "ACTOR OCHO DIGITOS", 8),
        _row("NIT", "860007738", "ACTOR NUEVE DIGITOS", 7),
    ])
    metadata = validate_actor_alias_index(path)

    assert metadata.identity_count == 0
    assert metadata.alias_count == 0
    assert metadata.source_quality[
        "unbridged_document_form_collapse_buckets_excluded"
    ] == 1
    assert metadata.source_quality["dirty_reason_counts"][
        "UNBRIDGED_DOCUMENT_FORM_COLLAPSE"
    ] == 1
    with sqlite3.connect(path) as connection:
        reasons = connection.execute(
            "SELECT reason_codes_json FROM actor_alias_build_audit"
        ).fetchall()
    assert reasons
    assert all(
        "UNBRIDGED_DOCUMENT_FORM_COLLAPSE" in json.loads(raw)
        for (raw,) in reasons
    )


def test_failed_build_preserves_last_valid_index(tmp_path, monkeypatch):
    path = _build(tmp_path, [_row("NIT", "900111222", "VALID ACTOR")])
    before = path.read_bytes()

    def fail_integrity(*args, **kwargs):
        raise ValueError("simulated integrity failure")

    monkeypatch.setattr(
        "app.data.actor_alias_index._integrity_check", fail_integrity,
    )
    with pytest.raises(ValueError, match="simulated integrity failure"):
        build_actor_alias_index(
            [_row("NIT", "900222333", "REPLACEMENT ACTOR")],
            path,
            source_dataset="fixture-contracts",
            built_at="2026-08-19T12:01:00+00:00",
        )

    assert path.read_bytes() == before
    assert not list(tmp_path.glob("*.tmp"))


def test_logical_build_is_reproducible_for_fixed_input_and_timestamp(tmp_path):
    rows = [
        _row("NIT", "900222333", "BETA", 4),
        _row("NIT", "900111222", "ALFA", 5),
    ]
    first = _build(tmp_path, rows, "first.sqlite")
    second = _build(tmp_path, list(reversed(rows)), "second.sqlite")

    query = (
        "SELECT normalized_alias,raw_alias,document_type,base_number,occurrences,"
        "first_seen,last_seen,source,disposition FROM actor_aliases ORDER BY alias_id"
    )
    with sqlite3.connect(first) as one, sqlite3.connect(second) as two:
        assert one.execute(query).fetchall() == two.execute(query).fetchall()
        assert one.execute(
            "SELECT * FROM actor_index_metadata"
        ).fetchall() == two.execute(
            "SELECT * FROM actor_index_metadata"
        ).fetchall()


# ---------------------------------------------------------------------------
# AP3-E — política de frecuencia mínima.
# ---------------------------------------------------------------------------


def test_low_frequency_alone_is_not_dirty(tmp_path):
    """Un proveedor con un solo contrato pertenece al directorio.

    Con el umbral anterior (3) quedaban fuera el 43,9% de los proveedores del
    país, medido sobre la fuente. Ser infrecuente no dice nada sobre la calidad
    de la evidencia; excluirlos convertía el directorio en un censo de
    proveedores grandes.
    """
    from app.data.actor_alias_index import build_actor_alias_index

    rows = [{
        "tipodocproveedor": "NIT", "documento_proveedor": "900123456",
        "proveedor_adjudicado": "PROVEEDOR DE UN SOLO CONTRATO",
        "occurrences": 1, "first_seen": "2025-01-01", "last_seen": "2025-01-01",
    }]
    report = build_actor_alias_index(
        rows, tmp_path / "index.sqlite", source_dataset="jbjy-vk9h",
    )
    assert report.to_dict()["metadata"]["identity_count"] == 1


def test_one_occurrence_with_a_quality_problem_is_still_dirty(tmp_path):
    """Baja frecuencia deja de excluir; los defectos de evidencia no.

    El mismo número bajo dos tipos documentales sigue siendo una colisión de
    actores aunque cada lado tenga una sola observación.
    """
    from app.data.actor_alias_index import build_actor_alias_index

    rows = [
        {"tipodocproveedor": "NIT", "documento_proveedor": "13848603",
         "proveedor_adjudicado": "UNION TEMPORAL X", "occurrences": 1,
         "first_seen": "2025-01-01", "last_seen": "2025-01-01"},
        {"tipodocproveedor": "Cédula de Ciudadanía", "documento_proveedor": "13848603",
         "proveedor_adjudicado": "PERSONA NATURAL", "occurrences": 1,
         "first_seen": "2025-01-01", "last_seen": "2025-01-01"},
    ]
    report = build_actor_alias_index(
        rows, tmp_path / "index.sqlite", source_dataset="jbjy-vk9h",
    )
    payload = report.to_dict()
    assert payload["metadata"]["identity_count"] == 0
    assert "CROSS_TYPE_COLLISION" in json.dumps(payload)


def test_the_default_threshold_is_pinned(tmp_path):
    """Si alguien vuelve a subirlo, que sea una decisión, no un descuido."""
    from app.data.actor_alias_index import ActorAliasBuildPolicy

    assert ActorAliasBuildPolicy().minimum_total_occurrences == 1
