"""C4A: authorities over the raw turn, accredited params, residual and overreach.

Only the control plane is measured here: credentials are injected (oracle) and
the classifier is not run. The assembler never touches QueryRouter, the legacy
graph or a source — that is asserted in the credentialed-admission tests, where
the real counters live.
"""
from datetime import date

import pytest

from app.config import Settings
from app.core.accreditation import (
    SUPPLIERS,
    Dimension,
    capability_spec,
    credential_from_observation,
)
from app.core.accreditation.assembly import AssemblyStatus, assemble, material_tokens, residual_of
from app.core.accreditation.authorities import actor_authority, buyer_authority, ranking_authority
from app.core.entity_resolver import EntityResolver
from app.core.natural_alpha.models import GroundedJob, Observation

GALAPA = "con quien ha contratado la alcaldia de galapa en 2025"
CARTAGENA = "con quien ha contratado mas la alcaldia de cartagena en 2026"
BARRANQUILLA = "cuales son los proveedores con contratos mas grandes de alcaldia de barranquilla"
OBRAS = "proveedores de galapa en obras civiles"
NEGATION = "no me muestres con quien ha contratado Galapa"
ACTOR = "contratos del proveedor Fundacaribe"
TOPIC = "proveedores de educación de Secretaría de Educación de Cali"
TODAY = date(2026, 9, 17)


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    """Entity resolver plus a small actor index built for these tests.

    The repo's conftest isolates ``SECOP_ACTOR_ALIAS_INDEX_PATH`` from the
    machine's 662 MB artifact on purpose: a test that needs an index builds its
    own and passes it explicitly.
    """
    root = tmp_path_factory.mktemp("accreditation-c4a")
    settings = Settings(
        _env_file=None, secoppal_a2_suppliers_binding_enabled=True,
        secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
    )
    from app.data.actor_alias_index import build_actor_alias_index
    from app.data.actor_alias_resolver import ActorNameResolver

    index_path = root / "actors.sqlite"
    build_actor_alias_index(
        [
            {"tipodocproveedor": "NIT", "documento_proveedor": "900123456",
             "proveedor_adjudicado": "FUNDACARIBE", "occurrences": 12,
             "first_seen": "2023-01-01", "last_seen": "2026-01-01"},
            {"tipodocproveedor": "NIT", "documento_proveedor": "900654321",
             "proveedor_adjudicado": "CONSTRUCCIONES DEL CARIBE SAS", "occurrences": 30,
             "first_seen": "2023-01-01", "last_seen": "2026-01-01"},
        ],
        index_path, source_dataset="c4a1-fixture", built_at="2026-09-17T12:00:00+00:00",
    )
    return settings, EntityResolver(settings.secop_alias_db_path), ActorNameResolver(index_path)


def credential_for(raw, settings, *, evidence):
    """The oracle credential: the model witnessed ``evidence`` inside ``raw``."""
    start = raw.index(evidence)
    observation = Observation("alpha", "MATCHED",
                              (GroundedJob("ANALYZE", "SUPPLIERS", start,
                                           start + len(evidence)),), provider_calls=1)
    rebuilt = credential_from_observation(observation, raw, settings=settings)
    assert rebuilt is not None
    return rebuilt


def accredited(raw, world, *, evidence):
    settings, resolver, actor_resolver = world
    return assemble(raw, credential_for(raw, settings, evidence=evidence),
                    capability_spec(SUPPLIERS), entity_resolver=resolver, today=TODAY,
                    actor_resolver=actor_resolver)


# ── C4A.13: mandatory incidents (oracle: the credential witnesses the job phrase,
# never the whole turn — a correct credential is what the control plane measures) ──

def test_galapa_2025_assembles_buyer_and_time(world):
    assembly = accredited(GALAPA, world, evidence="con quien ha contratado")
    assert assembly.status is AssemblyStatus.READY, (assembly.issues, assembly.residual_material)
    # The gazetteer's own string is carried verbatim: "MUNICIPIO DE GALAPA." (the
    # trailing period is the alias DB's, not this lane's to repair).
    assert assembly.params["entidad_resolved"].startswith("MUNICIPIO DE GALAPA")
    assert assembly.params["fecha_desde"] == "2025-01-01"
    assert assembly.params["fecha_hasta"] == "2025-12-31"
    assert assembly.params["dataset"] == "contratos"
    assert assembly.residual == "" and assembly.residual_material == ()
    assert assembly.credential_overreach is False
    receipts = {receipt.dimension: receipt for receipt in assembly.authority_receipts}
    assert receipts["BUYER"].status == "OK" and receipts["BUYER"].detail == "buyer_exact"
    assert receipts["TIME"].status == "OK"


def test_cartagena_2026_assembles(world):
    assembly = accredited(CARTAGENA, world, evidence="con quien ha contratado")
    assert assembly.status is AssemblyStatus.READY, (assembly.issues, assembly.residual_material)
    assert "CARTAGENA" in assembly.params["entidad_resolved"]
    assert assembly.params["fecha_desde"] == "2026-01-01"
    assert assembly.params["fecha_hasta"] == "2026-12-31"
    assert assembly.residual_material == ()
    assert "ordering_signal" not in assembly.params  # bare "mas" is not an ordering


def test_barranquilla_ranking_comes_from_control_language(world):
    assembly = accredited(BARRANQUILLA, world, evidence="cuales son los proveedores")
    assert assembly.status is AssemblyStatus.READY, (assembly.issues, assembly.residual_material)
    assert "BARRANQUILLA" in assembly.params["entidad_resolved"]
    assert assembly.params["ordering_signal"] == "valor_desc"
    ranking = next(r for r in assembly.authority_receipts if r.dimension == "RANKING")
    assert ranking.authority == "control_language.interpret_control_language"
    assert assembly.residual_material == ()


@pytest.mark.parametrize("evidence", ["proveedores", OBRAS])
def test_material_residual_is_never_erased(world, evidence):
    """A topic filter stays visible whether or not the evidence overreaches."""
    assembly = accredited(OBRAS, world, evidence=evidence)
    assert assembly.status is AssemblyStatus.CLARIFY
    assert assembly.reason == "UNACCREDITED_RESIDUAL"
    assert "obras" in assembly.residual_material and "civiles" in assembly.residual_material
    assert assembly.evidence_consumes_residual is (evidence != OBRAS)
    assert assembly.credential_overreach is (evidence == OBRAS)


def test_negation_without_accreditable_scope_clarifies(world):
    assembly = accredited(NEGATION, world, evidence="con quien ha contratado")
    assert assembly.status is AssemblyStatus.CLARIFY
    assert assembly.reason == "negation_scope_unaccredited"


def test_named_actor_takes_the_turn_from_suppliers(world):
    assembly = accredited(ACTOR, world, evidence="contratos del proveedor")
    assert assembly.status is AssemblyStatus.OTHER_OWNER
    assert assembly.owner == "ACTOR"


def test_topic_filter_stays_visible(world):
    assembly = accredited(TOPIC, world, evidence="proveedores")
    assert assembly.status is AssemblyStatus.CLARIFY
    assert any("educaci" in token for token in assembly.residual_material)


# ── unit level: each authority answers only its dimension ─────────────────────

def test_buyer_authority_refuses_bare_territory_and_missing_buyer(world):
    _, resolver, _actor = world
    assert buyer_authority("proveedores en Antioquia", entity_resolver=resolver).status == "CLARIFY"
    assert buyer_authority("proveedores sin entidad", entity_resolver=resolver).status == "CLARIFY"
    exact = buyer_authority(GALAPA, entity_resolver=resolver)
    assert exact.status == "OK" and exact.spans[0].dimension == "BUYER"
    matched = resolver.scan_entity("con quien ha contratado la alcaldia de galapa en 2025")
    assert GALAPA[exact.spans[0].start:exact.spans[0].end] == matched.matched_alias


def test_ranking_authority_supports_value_descending_only():
    supported = ranking_authority(BARRANQUILLA)
    assert supported.status == "OK"
    assert dict(supported.params)["ordering_signal"] == "valor_desc"
    unsupported = ranking_authority("los proveedores de galapa de menor a mayor valor")
    assert unsupported.status == "CLARIFY"
    assert unsupported.detail == "unsupported_ordering"
    none = ranking_authority("proveedores de galapa")
    assert none.status == "OK" and dict(none.params) == {}


def test_actor_authority_stays_absent_without_a_strong_reference():
    assert actor_authority("proveedores de galapa").status == "ABSENT"
    assert actor_authority("que ha contratado la alcaldia de galapa").status == "ABSENT"
    assert actor_authority(ACTOR).status == "OTHER_OWNER"


def test_residual_and_materiality_use_the_general_closed_class():
    assert residual_of("proveedores de galapa", ()) == "proveedores de galapa"
    assert material_tokens("dame los de la en") == ()
    assert material_tokens("proveedores de educación") == ("educación",)
    assert material_tokens("en obras civiles") == ("obras", "civiles")


# ── C4A.1: spec coverage and the guards of the unsupported dimensions ─────────

def test_every_declared_dimension_has_an_authority():
    from app.core.accreditation.coverage import (
        AUTHORITY_BY_DIMENSION,
        declared_dimensions,
        uncovered_dimensions,
        unknown_authority_dimensions,
    )

    assert uncovered_dimensions(capability_spec(SUPPLIERS)) == frozenset()
    assert unknown_authority_dimensions() == frozenset()
    assert declared_dimensions(capability_spec(SUPPLIERS)) == frozenset(
        dimension.value for dimension in Dimension)
    assert set(AUTHORITY_BY_DIMENSION) == {dimension.value for dimension in Dimension}


def test_modality_guard_stops_the_turn(world):
    from app.core.accreditation.guards import modality_guard

    report = modality_guard("proveedores de licitación pública de galapa")
    assert report.status == "CLARIFY" and report.detail == "UNSUPPORTED_MODALITY"
    assembly = accredited("proveedores de licitación pública de galapa", world,
                          evidence="proveedores")
    assert assembly.status is AssemblyStatus.CLARIFY
    assert assembly.reason == "UNSUPPORTED_MODALITY"
    assert any(issue.dimension == "MODALITY" for issue in assembly.issues)


def test_money_guard_is_not_confused_with_years_or_counts(world):
    from app.core.accreditation.guards import money_guard

    assert money_guard("proveedores de Galapa por más de 500 millones").status == "CLARIFY"
    assert money_guard("proveedores de galapa en 2025").status == "ABSENT"
    assert money_guard("proveedores de galapa").status == "ABSENT"
    money = accredited("proveedores de Galapa por más de 500 millones", world,
                       evidence="proveedores")
    assert money.status is AssemblyStatus.CLARIFY and money.reason == "UNSUPPORTED_MONEY"
    year = accredited("proveedores de galapa en 2025", world, evidence="proveedores")
    assert year.status is AssemblyStatus.READY and year.params["fecha_desde"] == "2025-01-01"


def test_geography_guard_ignores_the_buyers_own_name(world):
    from app.core.accreditation.guards import geography_guard

    assert geography_guard("proveedores de la alcaldía de Barranquilla en Antioquia",
                           buyer_span=(16, 43)).status == "CLARIFY"
    independent = accredited("proveedores de la alcaldía de Barranquilla en Antioquia", world,
                             evidence="proveedores")
    assert independent.status is AssemblyStatus.CLARIFY
    assert independent.reason == "UNSUPPORTED_GEOGRAPHY"
    contained = accredited(GALAPA, world, evidence="con quien ha contratado")
    assert contained.status is AssemblyStatus.READY


def test_actor_exact_guard_contains_a_named_actor(world):
    from app.core.accreditation.guards import actor_exact_guard

    settings, resolver, actor_resolver = world
    found = actor_resolver.exact_aliases_in("cuanto le ha vendido fundacaribe a galapa")
    assert any("fundacaribe" in alias for alias, _, _ in found), (
        "the actor index did not recognise the exact alias; an unavailable index must "
        "not be mistaken for containment")
    assembly = accredited("cuanto le ha vendido fundacaribe a galapa", world,
                          evidence="le ha vendido")
    assert assembly.status is AssemblyStatus.CLARIFY
    assert assembly.reason in ("ACTOR_MENTION_PRESENT", "actor_subject")
    guard = actor_exact_guard("cuanto le ha vendido fundacaribe a galapa",
                              actor_resolver=actor_resolver, protected_spans=())
    assert guard.status == "CLARIFY"
    # A token the host already accredited to another authority is not an actor.
    protected = actor_exact_guard("cuanto le ha vendido fundacaribe a galapa",
                                  actor_resolver=actor_resolver,
                                  protected_spans=((16, 28),))
    assert protected.status == "ABSENT"
    # A turn that names nobody keeps its lane.
    clean = actor_exact_guard("proveedores de galapa", actor_resolver=actor_resolver)
    assert clean.status == "ABSENT"


@pytest.mark.parametrize("guard_name,raw,evidence,expected_reason", [
    ("modality_guard", "proveedores de licitación pública de galapa", "proveedores",
     "UNSUPPORTED_MODALITY"),
    ("money_guard", "proveedores de Galapa por más de 500 millones", "proveedores",
     "UNSUPPORTED_MONEY"),
    ("geography_guard", "proveedores de la alcaldía de Barranquilla en Antioquia",
     "proveedores", "UNSUPPORTED_GEOGRAPHY"),
    ("actor_exact_guard", "cuanto le ha vendido fundacaribe a galapa", "le ha vendido",
     "ACTOR_MENTION_PRESENT"),
])
def test_guard_mutation_lets_its_case_through(world, monkeypatch, guard_name, raw, evidence,
                                              expected_reason):
    """Mutation: with the guard disabled the turn can no longer stop for it."""
    from app.core.accreditation import assembly as assembly_module
    from app.core.accreditation.guards import GuardReport

    blocked = accredited(raw, world, evidence=evidence)
    assert blocked.reason == expected_reason, blocked.reason

    monkeypatch.setattr(assembly_module, guard_name,
                        lambda *args, **kwargs: GuardReport("disabled", "ABSENT"))
    mutated = accredited(raw, world, evidence=evidence)
    assert mutated.reason != expected_reason
