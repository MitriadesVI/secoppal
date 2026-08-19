"""AP2-D — el perfil que ve el usuario, y lo que se niega a decirle.

Dos mitades: el texto (qué se muestra, qué se enmascara, qué se declara)
y el turno completo por el workflow real, sin red.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from app.config import Settings
from app.core.actor_profile.models import ActorProfileResult, AspectStatus, ObservedName
from app.core.actor_profile.render import mask_document, render_actor_profile
from app.core.actor_profile.turn import ROUTE_REASON, is_actor_profile_turn
from app.core.orchestrator import SecopalWorkflow
from app.core.outcome import Outcome, OutcomeReason

CONTRACT = json.loads(
    Path("tests/fixtures/actor_sources/secop2_contratos_source_contract.json")
    .read_text(encoding="utf-8")
)
REFERENCE = CONTRACT["reference_actor"]
BOGOTA = ZoneInfo("America/Bogota")


def make_profile(**overrides) -> ActorProfileResult:
    base = dict(
        queried_at=datetime(2026, 8, 18, 14, 32, tzinfo=BOGOTA),
        identity={"document_type": "NIT", "base_number": "860002400",
                  "role": "contractor", "resolution_method": "explicit_document"},
        dataset_id="jbjy-vk9h",
        contract_count=REFERENCE["contract_count"],
        total_reported_value=REFERENCE["total_reported_value"],
        first_observed_date=REFERENCE["first_signature_date"],
        last_observed_date=REFERENCE["last_signature_date"],
        buyers=tuple((b["nombre_entidad"], b["cnt"]) for b in REFERENCE["top_buyers"]),
        years=(("2026", 511), ("2025", 844)),
        modalities=tuple(
            (m["modalidad_de_contratacion"], m["cnt"]) for m in REFERENCE["top_modalities"]
        ),
        territories=tuple((t["departamento"], t["cnt"]) for t in REFERENCE["top_territories"]),
        observed_names=tuple(
            ObservedName(n["name"], "860002400", "NIT", n["count"])
            for n in CONTRACT["observed_names_sample"]["names"]
        ),
        recent_contracts=tuple(CONTRACT["frozen_rows"]),
        aspects=(AspectStatus("core"),),
    )
    base.update(overrides)
    return ActorProfileResult(**base)


# ---------------------------------------------------------------------------
# INV-SCOPE-DECLARED — el límite es parte del producto.
# ---------------------------------------------------------------------------


def test_inv_scope_declared_the_profile_always_says_what_it_did_not_consult():
    """Sin esta línea, "4.230 contratos" se lee como "empresa verificada"."""
    text = render_actor_profile(make_profile())
    assert "SECOP II" in text
    for absent in ("RUES", "RUP", "antecedentes", "uniones temporales"):
        assert absent in text


def test_inv_cutoff_the_query_timestamp_is_visible_to_the_reader():
    text = render_actor_profile(make_profile())
    assert "18/08/2026 14:32" in text
    assert "hora de Colombia" in text


def test_inv_source_value_the_total_is_named_reported_never_real_or_executed():
    """El universo incluye borradores y cancelados, y el valor es el reportado."""
    text = render_actor_profile(make_profile())
    assert "valor contractual reportado" in text
    assert "valor real" not in text.lower()
    assert "ejecutado" not in text.lower()


def test_inv_no_verdict_the_profile_never_grades_the_actor():
    text = render_actor_profile(make_profile()).lower()
    for verdict in ("riesgoso", "confiable", "recomendado", "sospechoso", "irregular"):
        assert verdict not in text


def test_the_date_coverage_gap_is_declared_when_a_range_is_shown():
    text = render_actor_profile(make_profile())
    assert "fecha de firma" in text and "7%" in text


# ---------------------------------------------------------------------------
# INV-PII — el documento de una persona natural no se reproduce.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "document_type",
    ["CC", "Cédula de Ciudadanía", "Cédula de Extranjería", "Tarjeta de Identidad"],
)
def test_inv_pii_a_personal_document_is_masked(document_type):
    assert mask_document("1140835135", document_type) == "******5135"


def test_inv_pii_a_nit_is_shown_in_full():
    """Un NIT es un identificador mercantil público, no un dato personal."""
    assert mask_document("860002400", "NIT") == "860002400"


def test_inv_pii_the_rendered_profile_of_a_person_hides_the_full_number():
    profile = make_profile(
        identity={"document_type": "CC", "base_number": "1140835135"},
        observed_names=(ObservedName("PERSONA NATURAL", "1140835135",
                                     "Cédula de Ciudadanía", 12),),
    )
    text = render_actor_profile(profile)
    assert "1140835135" not in text
    assert "5135" in text


def test_inv_pii_no_legal_representative_data_reaches_the_text():
    """El dataset publica su nombre, domicilio y género; el perfil no los pide."""
    text = render_actor_profile(make_profile()).lower()
    for column in ("representante legal", "domicilio", "número de cuenta"):
        assert column not in text


# ---------------------------------------------------------------------------
# INV-PARTIAL — lo que no se pudo saber, se dice.
# ---------------------------------------------------------------------------


def test_inv_partial_a_failed_aspect_is_declared_never_shown_as_absence():
    profile = make_profile(
        territories=(),
        aspects=(AspectStatus("territories", Outcome.ERROR, OutcomeReason.FETCH_TIMEOUT),),
    )
    text = render_actor_profile(profile)
    assert "no pude consultar" in text.lower()
    assert "territorios" in text.lower()
    assert "no lo sé" in text.lower()


def test_inv_partial_a_complete_profile_carries_no_missing_data_warning():
    text = render_actor_profile(make_profile())
    assert "No pude consultar" not in text


def test_inv_partial_losing_the_core_metrics_still_renders_the_rest():
    profile = make_profile(
        contract_count=None,
        total_reported_value=None,
        aspects=(AspectStatus("core", Outcome.ERROR, OutcomeReason.QUERY_ERROR),),
    )
    text = render_actor_profile(profile)
    assert "No pude obtener el total de contratos" in text
    assert "Universidad del Atlántico" in text


# ---------------------------------------------------------------------------
# INV-OBSERVED-NAME e INV-ID-TYPE en pantalla.
# ---------------------------------------------------------------------------


def test_inv_observed_name_the_text_says_these_are_observed_not_registered():
    text = render_actor_profile(make_profile())
    assert "no una razón social registrada" in text


def test_inv_id_type_mixed_document_types_are_warned_about_in_the_text():
    profile = make_profile(
        observed_names=(
            ObservedName("PERSONA NATURAL", "13848603", "Cédula de Ciudadanía", 191),
            ObservedName("UNIÓN TEMPORAL MIGRACIÓN COLOMBIA 2021", "13848603", "NIT", 1),
        ),
    )
    text = render_actor_profile(profile)
    assert "más de un tipo de documento" in text
    assert "actores distintos" in text


def test_a_single_observed_name_does_not_get_a_names_section():
    profile = make_profile(observed_names=(ObservedName("UNICO SAS", "860002400", "NIT", 4230),))
    assert "Nombres con que aparece" not in render_actor_profile(profile)


def test_recent_contracts_carry_their_links():
    text = render_actor_profile(make_profile())
    assert "https://community.secop.gov.co" in text


# ---------------------------------------------------------------------------
# El turno completo, por el workflow real y sin red.
# ---------------------------------------------------------------------------


def _workflow(calls: list[str] | None = None) -> SecopalWorkflow:
    workflow = SecopalWorkflow(Settings())

    def _dispatch(dataset_id: str, soql: str) -> list[dict]:
        if calls is not None:
            calls.append(soql)
        if "SUM(" in soql:
            return [{"total_value": "1396701750307.83", "total_count": "4230"}]
        if "date_min" in soql:
            return [{"date_min": "2016-12-06T00:00:00.000",
                     "date_max": "2026-08-14T00:00:00.000"}]
        if "date_trunc_y" in soql:
            return [{"yr": "2026-01-01T00:00:00.000", "cnt": "511"}]
        if "GROUP BY nombre_entidad" in soql:
            return [{"nombre_entidad": "Universidad del Atlántico", "cnt": "36"}]
        if "GROUP BY modalidad_de_contratacion" in soql:
            return [{"modalidad_de_contratacion": "Mínima cuantía", "cnt": "2115"}]
        if "GROUP BY departamento" in soql:
            return [{"departamento": "Distrito Capital de Bogotá", "cnt": "959"}]
        if "GROUP BY proveedor_adjudicado" in soql:
            return [{"proveedor_adjudicado": "LA PREVISORA S.A. COMPAÑÍA DE SEGUROS",
                     "documento_proveedor": "860002400",
                     "tipodocproveedor": "NIT", "cnt": "4211"}]
        return list(CONTRACT["frozen_rows"])

    workflow.secop_client.query = _dispatch          # type: ignore[method-assign]
    workflow.secop_client.aggregate = _dispatch      # type: ignore[method-assign]
    workflow.secop_client.count = lambda dataset_id, soql: 4230  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None  # type: ignore[method-assign]
    return workflow


def test_an_actor_query_returns_the_profile_through_the_normal_flow():
    result = _workflow().run_query("contratos del NIT 860002400", channel="streamlit")

    assert result["route_reason"] == ROUTE_REASON
    assert "Huella contractual observada" in result["response"]
    assert result["total_count"] == 4230
    assert result["needs_clarification"] is False


def test_the_profile_response_is_not_rewritten_by_the_advisor_layer():
    """El texto que se construyó es el que se entrega: sin secciones añadidas."""
    result = _workflow().run_query("contratos del NIT 860002400", channel="streamlit")
    assert result["response"].startswith("LA PREVISORA")
    assert "RUES" in result["response"]


def test_every_profile_query_filters_by_the_exact_document():
    calls: list[str] = []
    _workflow(calls).run_query("contratos del NIT 860002400", channel="streamlit")
    assert calls
    assert all("documento_proveedor = '860002400'" in soql for soql in calls)


def test_an_actor_on_an_unsupported_dataset_asks_instead_of_answering():
    """INV-NO-SILENT-DROP de extremo a extremo: pregunta, no inventa."""
    result = _workflow().run_query("procesos del NIT 860002400", channel="streamlit")

    assert result["needs_clarification"] is True
    assert result["route_reason"] != ROUTE_REASON
    assert "no publica quién contrata" in result["response"]


def test_an_unresolved_name_asks_for_the_document_instead_of_profiling():
    result = _workflow().run_query("contratos del proveedor FUNDACARIBE", channel="streamlit")

    assert result["needs_clarification"] is True
    assert "NIT" in result["response"] or "cédula" in result["response"]
    assert "Huella contractual" not in result["response"]


def test_a_topical_query_never_enters_the_profile_route():
    result = _workflow().run_query("contratos de alimentación", channel="streamlit")
    assert result["route_reason"] != ROUTE_REASON


def test_a_specific_aggregate_question_still_gets_its_specific_answer():
    """Los analyzers conservan la precedencia: preguntar la suma da la suma."""
    result = _workflow().run_query(
        "cuánto suma lo contratado por el NIT 860002400", channel="streamlit",
    )
    assert result["route_reason"] != ROUTE_REASON
    assert result["analytical_intent"] == "aggregate_sum"


def test_an_actor_plus_a_topic_is_a_filtered_search_not_a_profile():
    """El actor como FILTRO no convierte la consulta en un perfil.

    "Contratos de alimentación del NIT X" pide esa lista. Contestarla con la
    huella completa del actor respondería una pregunta distinta de la que se
    hizo — y el camino tabular ya la sirve bien, con el filtro exacto de
    documento que AP2-B corrigió.
    """
    result = _workflow().run_query(
        "contratos de alimentación del NIT 860002400", channel="streamlit",
    )
    assert result["route_reason"] != ROUTE_REASON
    assert "documento_proveedor = '860002400'" in result["soql_query"]


def test_is_actor_profile_turn_requires_a_document_never_a_name():
    assert is_actor_profile_turn({
        "actor_resolution": "explicit_document",
        "contratista": ["860002400"], "dataset": "contratos",
    }) is True
    assert is_actor_profile_turn({
        "actor_resolution": "name_unresolved", "dataset": "contratos",
    }) is False
    assert is_actor_profile_turn({
        "actor_resolution": "explicit_document",
        "contratista": ["860002400"], "dataset": "procesos",
    }) is False
    assert is_actor_profile_turn({
        "actor_resolution": "explicit_document", "contratista": ["860002400"],
        "dataset": "contratos", "objeto": ["alimentacion"],
    }) is False
