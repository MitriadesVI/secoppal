"""C4A: authorities over the raw turn, accredited params, residual and overreach.

Only the control plane is measured here: credentials are injected (oracle) and
the classifier is not run. The assembler never touches QueryRouter, the legacy
graph or a source — that is asserted in the credentialed-admission tests, where
the real counters live.
"""
from datetime import date

import pytest

from app.config import Settings
from app.core.accreditation import SUPPLIERS, capability_spec, credential_from_observation
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
    root = tmp_path_factory.mktemp("accreditation-c4a")
    settings = Settings(
        _env_file=None, secoppal_a2_suppliers_binding_enabled=True,
        secop_feedback_path=root / "traces.jsonl", secop_feedback_db_path=root / "feedback.db",
    )
    return settings, EntityResolver(settings.secop_alias_db_path)


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
    settings, resolver = world
    return assemble(raw, credential_for(raw, settings, evidence=evidence),
                    capability_spec(SUPPLIERS), entity_resolver=resolver, today=TODAY)


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
    _, resolver = world
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
