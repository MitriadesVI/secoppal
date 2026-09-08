"""R3d: one occurrence authority for postposed source and topic ownership."""
from __future__ import annotations

from datetime import date
import json

from hypothesis import given, settings, strategies as st
import pytest

from app.core.entity_profile.claim_adapters import collect_claims
from app.core.entity_profile.claims import ClaimDimension
from app.core.entity_profile.frame_values import SourceKind
from app.core.entity_profile.source_request import (
    RequestedSources,
    SourceProvenance,
    resolve_requested_sources,
)
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.source_occurrence import (
    SourceOccurrenceRole,
    mask_source_requests,
    source_request_occurrences,
)
from app.utils.money import normalize_text
from entity_profile_frame_harness import BUYER, observe


TODAY = date(2026, 9, 7)
PROPERTY_SETTINGS = settings(max_examples=36, derandomize=True, deadline=None)
DIRECT_MODALITIES = {
    "contratacion_directa",
    "contratacion_directa_con_ofertas",
}


def _source_claims(claims):
    return tuple(
        claim for claim in claims.claims
        if claim.dimension is ClaimDimension.SOURCE
    )


@PROPERTY_SETTINGS
@given(
    courtesy=st.sampled_from(("", "por favor, ", "¿Por favor, busca ")),
    modality=st.sampled_from(("contratación directa", "licitación pública")),
    topic=st.sampled_from(("alimentación", "mantenimiento", "software")),
    article=st.sampled_from(("", "el ", "los ")),
    source=st.sampled_from(("proceso", "procesos", "contrato", "contratos")),
    suffix=st.sampled_from(("", " en 2026", ", por favor")),
    gap=st.sampled_from((" ", "  ", "\t")),
)
def test_supported_postposed_framing_has_one_owner_and_one_consumption(
    courtesy,
    modality,
    topic,
    article,
    source,
    suffix,
    gap,
):
    query = (
        f"{courtesy}{modality}{gap}de{gap}{topic}{gap}en{gap}"
        f"{article}{source}{suffix}"
    )
    normalized = normalize_text(query)
    occurrences = source_request_occurrences(normalized)
    assert len(occurrences) == 1
    assert occurrences[0].role is SourceOccurrenceRole.POSTPOSED
    assert normalized[slice(*occurrences[0].span)] == source

    parsed = QueryRouter().parse(query, today=TODAY).params
    assert parsed.get("objeto") == [normalize_text(topic)]
    request = resolve_requested_sources(query, parsed)
    expected = (
        RequestedSources.PROCESSES
        if source.startswith("proceso")
        else RequestedSources.CONTRACTS
    )
    assert request.sources is expected
    assert request.provenance is SourceProvenance.EXPLICIT


@pytest.mark.parametrize(
    ("query", "interior"),
    [
        ("contratos de capacitacion en procesos industriales", "procesos industriales"),
        ("contratos de apoyo en procesos judiciales", "procesos judiciales"),
        ("contratos de software para gestion de procesos", "gestion de procesos"),
        (
            "contratos de asesoria en contratos de arrendamiento",
            "en contratos de arrendamiento",
        ),
        (
            "contratos de gestion de procesos y contratos",
            "gestion de procesos y contratos",
        ),
    ],
)
def test_interior_source_words_remain_available_to_topic_authorities(query, interior):
    normalized = normalize_text(query)
    occurrences = source_request_occurrences(normalized)
    assert len(occurrences) == 1
    assert occurrences[0].role is SourceOccurrenceRole.HEAD
    assert occurrences[0].surface == "contratos"
    assert interior in " ".join(mask_source_requests(normalized).split())


@PROPERTY_SETTINGS
@given(
    thematic_tail=st.sampled_from((
        "capacitacion en procesos industriales",
        "apoyo en procesos judiciales",
        "software para gestion de procesos",
        "asesoria en contratos de arrendamiento",
        "gestion de procesos y contratos",
    )),
    period=st.sampled_from(("", " en 2026")),
)
def test_orthogonal_period_never_promotes_an_interior_occurrence(
    thematic_tail,
    period,
):
    normalized = normalize_text(f"contratos de {thematic_tail}{period}")
    occurrences = source_request_occurrences(normalized)
    assert [(item.surface, item.role) for item in occurrences] == [
        ("contratos", SourceOccurrenceRole.HEAD),
    ]
    masked = " ".join(mask_source_requests(normalized).split())
    assert thematic_tail in masked


@pytest.mark.parametrize("operator", ("excepto", "sin", "excluyendo"))
def test_excluded_topic_source_word_never_becomes_a_positive_source(operator):
    query = f"contratos {operator} procesos industriales"
    occurrences = source_request_occurrences(normalize_text(query))
    assert [(item.surface, item.is_process) for item in occurrences] == [
        ("contratos", False),
    ]
    params = QueryRouter().parse(query, today=TODAY).params
    assert "dataset_scope" not in params
    request = resolve_requested_sources(query, params)
    assert request.sources is RequestedSources.CONTRACTS


@pytest.mark.parametrize(
    "query",
    [
        "contratos de gestion de procesos y contratos en 2026",
        "contratos de gestion de procesos y contratos de la alcaldia de Baranoa en 2026",
    ],
)
def test_year_and_buyer_do_not_reclassify_thematic_coordination(query):
    params = QueryRouter().parse(query, today=TODAY).params
    assert params["dataset"] == "contratos"
    assert "dataset_scope" not in params
    assert params["fecha_desde"] == "2026-01-01"
    assert params["fecha_hasta"] == "2026-12-31"
    assert params["objeto"] == ["gestion", "procesos", "contratos"]

    claims = collect_claims(query, params, turn_id="orthogonal", today=TODAY)
    sources = _source_claims(claims)
    assert len(sources) == 1
    assert sources[0].canonical_value is SourceKind.CONTRACTS
    assert query[slice(*sources[0].span)].lower() == "contratos"


def test_distinct_sources_with_owned_events_remain_a_supported_contrast():
    query = "procesos publicados en julio y contratos firmados en agosto"
    params = QueryRouter().parse(query, today=TODAY).params
    assert params["dataset_scope"] == ["procesos", "contratos"]
    occurrences = source_request_occurrences(normalize_text(query))
    assert {item.is_process for item in occurrences} == {True, False}
    assert all(item.role is SourceOccurrenceRole.HEAD for item in occurrences)


def test_accredited_buyer_name_cannot_supply_a_second_source(tmp_path):
    buyer = "unidad administrativa de contratos"
    aliases = tmp_path / "aliases.json"
    aliases.write_text(json.dumps([{
        "official_name": buyer.upper(),
        "aliases": [buyer],
        "source_datasets": ["procesos", "contratos"],
    }]), encoding="utf-8")
    query = f"alimentacion en procesos de {buyer} en 2026"
    params = QueryRouter(EntityResolver(aliases)).parse(query, today=TODAY).params
    assert params["entidad_resolved"] == buyer.upper()
    assert params["objeto"] == ["alimentacion"]
    assert "dataset_scope" not in params
    request = resolve_requested_sources(query, params)
    assert request.sources is RequestedSources.PROCESSES
    assert request.provenance is SourceProvenance.EXPLICIT


def test_seed_reaches_frame_plan_sql_and_visible_scope_without_source_like():
    query = (
        "contratacion directa de alimentacion en procesos "
        f"de la {BUYER} en 2026"
    )
    outcome = observe(query)
    assert outcome.ready, outcome.issues
    assert outcome.sources == frozenset({SourceKind.PROCESSES})
    assert outcome.params["dataset"] == "procesos"
    assert "dataset_scope" not in outcome.params
    assert outcome.params["objeto"] == ["alimentacion"]

    sources = _source_claims(outcome.claims)
    assert len(sources) == 1
    assert sources[0].canonical_value is SourceKind.PROCESSES
    assert query[slice(*sources[0].span)] == "procesos"

    predicates = outcome.predicates(SourceKind.PROCESSES)
    assert predicates.topic.fingerprint() == "TERM(alimentacion)"
    assert set(predicates.modality.include) == DIRECT_MODALITIES
    assert predicates.temporal.period.lower == "2026-01-01"
    assert predicates.temporal.period.upper == "2026-12-31"

    assert outcome.jobs
    assert all(job.source is SourceKind.PROCESSES for job in outcome.jobs)
    sql = "\n".join(outcome.sql).lower()
    assert "%alimentacion%" in sql
    assert "%procesos%" not in sql
    assert "modalidad_de_contratacion in" in sql
    assert "fecha_de_publicacion_del >= '2026-01-01'" in sql
    assert "fecha_de_publicacion_del <= '2026-12-31'" in sql

    scope = "\n".join(outcome.scope_lines).lower()
    assert "fuentes: procesos" in scope
    assert "tema: alimentacion" in scope
    assert "modalidad: contratación directa" in scope
    assert "2026, según fecha de publicación" in scope


def test_thematic_contracts_survive_frame_sql_and_scope_with_orthogonal_filters():
    query = (
        "contratos de gestion de procesos y contratos "
        f"de la {BUYER} en 2026"
    )
    outcome = observe(query)
    assert outcome.ready, outcome.issues
    assert outcome.sources == frozenset({SourceKind.CONTRACTS})
    assert outcome.params["objeto"] == ["gestion", "procesos", "contratos"]
    fingerprint = outcome.predicates(SourceKind.CONTRACTS).topic.fingerprint()
    assert fingerprint == "ALL(TERM(gestion),TERM(procesos),TERM(contratos))"
    sql = "\n".join(outcome.sql).lower()
    assert all(f"%{term}%" in sql for term in ("gestion", "procesos", "contratos"))
    scope = "\n".join(outcome.scope_lines).lower()
    assert "fuentes: contratos" in scope
    assert "tema: gestion y procesos y contratos" in scope
    assert "2026, según fecha de firma" in scope
