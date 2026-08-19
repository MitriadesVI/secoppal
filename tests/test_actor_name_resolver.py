"""AP3-B/B2 — resolver local precision-first y sus mutantes adversariales."""
from __future__ import annotations

import ast
import inspect
import socket
import sqlite3
from pathlib import Path

import pytest

from app.config import Settings
from app.core.actor_identity import IdentityKey, normalize_identity
from app.core.actor_name import ActorResolutionStatus
from app.data.actor_alias_index import build_actor_alias_index
from app.data.actor_alias_resolver import ACTOR_RESOLUTION_TOP_K, ActorNameResolver


def _row(
    document_type: str,
    document: str,
    name: str,
    occurrences: int = 5,
) -> dict:
    return {
        "tipodocproveedor": document_type,
        "documento_proveedor": document,
        "proveedor_adjudicado": name,
        "occurrences": occurrences,
        "first_seen": "2023-01-01",
        "last_seen": "2026-01-01",
    }


def _nit(document: str) -> IdentityKey:
    match = normalize_identity(document, document_type="NIT")
    assert match.key is not None
    return match.key


def _build(tmp_path: Path, rows: list[dict], name: str = "actors.sqlite") -> Path:
    path = tmp_path / name
    build_actor_alias_index(
        rows,
        path,
        source_dataset="resolver-fixture",
        built_at="2026-08-19T12:00:00+00:00",
    )
    return path


@pytest.fixture
def resolver(tmp_path: Path) -> ActorNameResolver:
    path = _build(tmp_path, [
        _row("NIT", "900111222", "Fundación Ágil, S.A.S.", 10),
        _row("NIT", "900222333", "SERVICIOS DEL CARIBE", 100),
        _row("NIT", "900333444", "Servicios del Caribe", 3),
        _row("NIT", "900444555", "FUNDACION PARA EL DESARROLLO DEL CARIBE", 9),
        _row("NIT", "900555666", "COMERCIAL DEL PACIFICO", 8),
        _row("NIT", "900666777", "SOFTWARE HOUSE SAS", 7),
    ])
    return ActorNameResolver(path)


@pytest.mark.parametrize(
    "query",
    [
        "fundación ágil, s.a.s.",
        "FUNDACION AGIL S A S",
        "Fundación Ágil — S.A.S.",
        "  FuNdAcIóN   ÁgIl, S.A.S.  ",
    ],
)
def test_exact_accent_punctuation_case_and_spacing_variants_resolve(
    resolver: ActorNameResolver,
    query: str,
):
    result = resolver.resolve(query)
    assert result.status == ActorResolutionStatus.RESOLVED
    assert result.identity == _nit("900111222")
    assert result.method == "exact_unique_alias"
    assert result.candidates == ()


def test_frequency_heavy_exact_ambiguity_never_selects_an_identity(
    resolver: ActorNameResolver,
):
    result = resolver.resolve("servicios del caribe")
    assert result.status == ActorResolutionStatus.AMBIGUOUS
    assert result.identity is None
    assert len(result.candidates) == 2
    assert result.candidates[0].occurrences == 100  # UI order only
    assert {candidate.identity for candidate in result.candidates} == {
        _nit("900222333"),
        _nit("900333444"),
    }


def test_zero_exact_returns_native_trigram_candidates_never_resolution(
    resolver: ActorNameResolver,
):
    result = resolver.resolve("fundacion para desarrollo caribe")
    assert result.status == ActorResolutionStatus.CANDIDATES
    assert result.identity is None
    assert result.method == "fts5_trigram_bm25"
    assert result.candidates[0].identity == _nit("900444555")
    assert result.candidates[0].retrieval_score is not None


def test_even_an_incorrect_or_topic_like_fuzzy_top_hit_is_only_a_candidate(
    resolver: ActorNameResolver,
):
    incorrect = resolver.resolve("comercial pacifica")
    topic = resolver.resolve("software")
    assert incorrect.status == ActorResolutionStatus.CANDIDATES
    assert incorrect.identity is None
    assert topic.status == ActorResolutionStatus.CANDIDATES
    assert topic.identity is None


def test_multiple_matching_aliases_collapse_before_top_ten(tmp_path):
    rows = [
        _row(
            "NIT",
            "900777888",
            f"ALFA SERVICIO ESPECIAL {number:02d}",
            100 if number == 0 else 3,
        )
        for number in range(12)
    ]
    rows.extend(
        _row(
            "NIT",
            f"901{number:06d}",
            f"BETA SERVICIO ESPECIAL {number:02d}",
            5,
        )
        for number in range(15)
    )
    resolver = ActorNameResolver(_build(tmp_path, rows))

    result = resolver.resolve("servicio especial")
    identities = [candidate.identity for candidate in result.candidates]
    assert result.status == ActorResolutionStatus.CANDIDATES
    assert len(identities) == ACTOR_RESOLUTION_TOP_K
    assert len(set(identities)) == ACTOR_RESOLUTION_TOP_K
    assert identities.count(_nit("900777888")) <= 1
    assert [candidate.rank for candidate in result.candidates] == list(range(1, 11))


def test_same_base_number_with_nit_and_cc_never_collapses(tmp_path):
    path = _build(tmp_path, [
        _row("NIT", "900888999", "ACTOR TIPO MIXTO", 7),
        _row("Cédula de Ciudadanía", "1140835135", "ACTOR TIPO MIXTO", 6),
    ])
    # Simula un índice compatible procedente de dos registros tipados que
    # comparten base. El builder normal excluye ese bucket sucio; el resolver,
    # si lo recibe como evidencia ambigua, debe preservar ambas dimensiones.
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE actor_aliases SET base_number = ? WHERE document_type = 'CC'",
            (_nit("900888999").base_number,),
        )
        connection.commit()

    result = ActorNameResolver(path).resolve("actor tipo mixto")
    assert result.status == ActorResolutionStatus.AMBIGUOUS
    assert len(result.candidates) == 2
    assert {(c.identity.document_type, c.identity.base_number) for c in result.candidates} == {
        ("NIT", _nit("900888999").base_number),
        ("CC", _nit("900888999").base_number),
    }


def test_dirty_cross_type_bucket_cannot_auto_resolve(tmp_path):
    path = _build(tmp_path, [
        _row("NIT", "13848603", "ZZZ DIRTY ONLY", 100),
        _row("Cédula de Ciudadanía", "13848603", "ZZZ DIRTY ONLY", 5),
    ])
    result = ActorNameResolver(path).resolve("zzz dirty only")
    assert result.status == ActorResolutionStatus.UNRESOLVED
    assert result.identity is None


def test_empty_or_too_short_query_is_unresolved_without_heuristics(
    resolver: ActorNameResolver,
):
    assert resolver.resolve("").status == ActorResolutionStatus.UNRESOLVED
    assert resolver.resolve(" — ").status == ActorResolutionStatus.UNRESOLVED
    assert resolver.resolve("sa").status == ActorResolutionStatus.UNRESOLVED


def test_missing_index_is_fail_safe_and_never_touches_network(tmp_path, monkeypatch):
    missing = tmp_path / "missing.sqlite"

    def network_forbidden(*args, **kwargs):
        raise AssertionError("network fallback attempted")

    monkeypatch.setattr(socket, "create_connection", network_forbidden)
    result = ActorNameResolver(missing).resolve("fundacion caribe")
    assert result.status == ActorResolutionStatus.INDEX_UNAVAILABLE
    assert result.reason == "INDEX_MISSING"
    assert not missing.exists()


def test_corrupt_index_is_unavailable(tmp_path):
    path = tmp_path / "corrupt.sqlite"
    path.write_bytes(b"this is not sqlite")
    result = ActorNameResolver(path).resolve("fundacion caribe")
    assert result.status == ActorResolutionStatus.INDEX_UNAVAILABLE
    assert result.reason == "INDEX_CORRUPT_OR_UNREADABLE"


@pytest.mark.parametrize(
    ("column", "wrong_value"),
    [("schema_version", "999"), ("normalization_version", "N3")],
)
def test_incompatible_index_version_is_rejected(tmp_path, column, wrong_value):
    path = _build(tmp_path, [_row("NIT", "900111222", "ACTOR ESTABLE")])
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"UPDATE actor_index_metadata SET {column} = ?",  # controlled test identifier
            (wrong_value,),
        )
        connection.commit()
    result = ActorNameResolver(path).resolve("actor estable")
    assert result.status == ActorResolutionStatus.INDEX_UNAVAILABLE
    assert result.reason == "INDEX_INCOMPATIBLE"


def test_resolver_reads_configured_path_and_never_writes_sidecars(tmp_path):
    path = _build(tmp_path, [_row("NIT", "900111222", "ACTOR ESTABLE")])
    before = path.read_bytes()
    settings = Settings(secop_actor_alias_index_path=path)
    resolver = ActorNameResolver.from_settings(settings)

    assert resolver.resolve("actor estable").status == ActorResolutionStatus.RESOLVED
    assert path.read_bytes() == before
    assert not path.with_name(path.name + "-wal").exists()
    assert not path.with_name(path.name + "-shm").exists()


def test_human_facing_repr_masks_full_cc_but_runtime_keeps_identity(tmp_path):
    document = "1140835135"
    path = _build(tmp_path, [
        _row("Cédula de Ciudadanía", document, "PERSONA OBSERVADA", 5),
    ])
    result = ActorNameResolver(path).resolve("persona observada")
    rendered = repr(result)
    assert result.status == ActorResolutionStatus.RESOLVED
    assert result.identity == IdentityKey("CC", document)
    assert document not in rendered
    assert "****5135" in rendered


def test_resolver_source_has_no_rapidfuzz_or_network_client_dependency():
    source = inspect.getsource(inspect.getmodule(ActorNameResolver))
    imported = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(ast.parse(source))
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in (
            node.names if isinstance(node, ast.Import) else [ast.alias(node.module or "")]
        )
    }
    assert imported.isdisjoint({"rapidfuzz", "sodapy", "httpx", "requests"})
