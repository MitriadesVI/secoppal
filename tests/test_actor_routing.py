"""AP2-B — el routing de un actor contractual, y sus invariantes.

Cada prueba de este archivo defiende una forma concreta de mentir que el
diagnóstico AP0 encontró viva en producción (§11). No son pruebas de
cobertura: son las que impiden que el sistema afirme algo falso sobre un
tercero identificable.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.actor_profile import build_actor_profile
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder

router = QueryRouter()
builder = SoQLBuilder()


def parse(text: str) -> dict:
    return dict(router.parse(text).params)


def soql_for(text: str) -> str:
    params = parse(text)
    return builder.build(builder.dataset_id_for(params.get("dataset")), params)


def objeto_tokens(params: dict) -> set[str]:
    out: set[str] = set()
    for item in params.get("objeto") or []:
        if isinstance(item, (list, tuple)):
            out.update(str(x).lower() for x in item)
        else:
            out.add(str(item).lower())
    return out


# ---------------------------------------------------------------------------
# INV-ACTOR-ID — un documento explícito produce identidad TIPADA.
# ---------------------------------------------------------------------------


def test_inv_actor_id_an_explicit_document_becomes_a_typed_identity():
    params = parse("contratos del NIT 860002400")
    scope = params["actor_scope"]

    assert params["actor_resolution"] == "explicit_document"
    assert scope["document_type"] == "NIT"
    assert scope["base_number"] == "860002400"
    assert scope["role"] == "contractor"
    assert scope["requires_resolution"] is False


def test_inv_actor_id_the_declared_type_comes_from_the_words_not_the_length():
    """"cédula" declara CC; el mismo número sin marcador queda UNDETERMINED.

    Si la longitud decidiera, ambos caerían en el mismo tipo y la actividad
    de una empresa podría atribuirse a un ciudadano.
    """
    as_cedula = parse("contratos de la cedula 1140835135")["actor_scope"]
    bare = parse("contratos de 1140835135")["actor_scope"]

    assert as_cedula["document_type"] == "CC"
    assert as_cedula["identity_confidence"] == "declared"
    assert bare["document_type"] == "UNKNOWN"
    assert bare["identity_confidence"] == "undetermined"


def test_inv_id_type_the_same_number_declared_differently_is_not_one_actor():
    """13848603 es cédula de una persona Y NIT de una UT (verificado en vivo)."""
    as_cedula = parse("contratos de la cedula 13848603")["actor_scope"]
    as_nit = parse("contratos del NIT 13848603")["actor_scope"]

    assert as_cedula["document_type"] != as_nit["document_type"]
    assert (as_cedula["document_type"], as_cedula["base_number"]) != (
        as_nit["document_type"], as_nit["base_number"],
    )


def test_the_raw_value_survives_verbatim_for_auditing():
    scope = parse("contratos del NIT 860.034.594-1")["actor_scope"]
    assert scope["raw_value"] == "860.034.594-1"
    assert scope["base_number"] == "860034594"
    assert scope["check_digit"] == "1"


@pytest.mark.parametrize(
    ("marker", "expected_type"),
    [
        ("cédula", "CC"),
        ("CC", "CC"),
        ("NIT", "NIT"),
    ],
)
def test_short_explicit_documents_use_the_actor_path_without_length_inference(
    marker,
    expected_type,
):
    params = parse(f"contratos del proveedor {marker} 4265582")

    assert params["actor_resolution"] == "explicit_document"
    assert params["actor_scope"]["document_type"] == expected_type
    assert params["actor_scope"]["base_number"] == "4265582"
    assert params["contratista"] == ["4265582"]


def test_a_bare_short_number_without_actor_or_document_evidence_is_not_an_actor():
    params = parse("contratos de 4265582")

    assert params.get("actor_resolution") is None
    assert params.get("actor_scope") is None
    assert params.get("contratista") is None


def test_short_cc_profile_matches_the_independent_4265582_oracle():
    oracle = json.loads(
        Path("tests/fixtures/actor_profile_short_document_oracle.json").read_text(
            encoding="utf-8"
        )
    )
    calls: list[str] = []

    class OracleClient:
        def derived_for_timeout(self, timeout_s):
            return self

        def aggregate(self, dataset_id, soql):
            calls.append(soql)
            if "SUM(" in soql:
                return [{"total_count": str(oracle["contract_count"]), "total_value": "0"}]
            if "GROUP BY proveedor_adjudicado" in soql:
                return [{
                    "proveedor_adjudicado": oracle["observed_name"],
                    "documento_proveedor": oracle["document"],
                    "tipodocproveedor": "Cédula de Ciudadanía",
                    "cnt": str(oracle["contract_count"]),
                }]
            return []

        def query(self, dataset_id, soql):
            calls.append(soql)
            return [
                {"id_contrato": contract_id}
                for contract_id in oracle["recent_contract_ids"]
            ]

    params = parse("contratos de la cédula 4265582")
    profile = build_actor_profile(
        params,
        builder.dataset_id_for(params["dataset"]),
        OracleClient(),
        builder,
    )

    assert profile.contract_count == oracle["contract_count"]
    assert [row["id_contrato"] for row in profile.recent_contracts] == oracle[
        "recent_contract_ids"
    ]
    assert calls
    assert all("documento_proveedor = '4265582'" in soql for soql in calls)
    assert all("tipodocproveedor NOT IN ('NIT')" in soql for soql in calls)


@pytest.mark.parametrize(
    "text",
    [
        "contratos del proveedor ASILO DE SAN ANTONIO NIT 890.102.896-1",
        "qué ha contratado ASILO DE SAN ANTONIO con NIT 890.102.896-1",
        "historial contractual de ASILO DE SAN ANTONIO NIT 890.102.896-1",
        (
            "contratos del proveedor AGENCIA LOGÍSTICA DE GESTIÓN "
            "INMOBILIARIA Y DE SERVICIOS DE CUNDINAMARCA NIT 900221051"
        ),
    ],
)
def test_inv_actor_mention_atomicity_name_plus_document_cannot_leak(text):
    params = parse(text)

    assert params["actor_resolution"] == "explicit_document"
    assert params["actor_scope"]["asserted_name"]
    assert params.get("objeto") is None
    assert params.get("entidad") is None
    assert params.get("entidad_resolved") is None
    assert params.get("departamento") is None
    assert params.get("departamento_resolved") is None
    assert params.get("ciudad") is None


def test_name_plus_document_filters_secop_by_document_only():
    query = "contratos del proveedor ASILO DE SAN ANTONIO NIT 890.102.896-1"
    soql = soql_for(query)

    assert "documento_proveedor = '890102896'" in soql
    assert "ASILO" not in soql.upper()
    assert "HOSPITAL SAN ANTONIO" not in soql.upper()


# ---------------------------------------------------------------------------
# INV-ACTOR-NOT-TOPIC — el actor nunca se degrada a búsqueda de texto.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "forbidden"),
    [
        ("contratos de la persona con cedula 79482763", {"persona", "cedula"}),
        ("qué ha celebrado la persona 1140835135 en 2025", {"persona", "celebrado"}),
        ("contratos de la empresa NIT 830053105", {"empresa", "nit"}),
        ("qué ha contratado el contratista 800197268", {"contratista", "contratado"}),
        ("historial contractual del NIT 860002400", {"historial"}),
    ],
)
def test_inv_actor_not_topic_actor_vocabulary_never_reaches_objeto(text, forbidden):
    """Las palabras con que se nombra al actor no son el tema del contrato.

    Dejarlas en ``objeto`` produce un ``LIKE '%persona%'`` que devuelve
    contratos ajenos y los presenta como la actividad de esa persona.
    """
    assert not (objeto_tokens(parse(text)) & forbidden)


def test_inv_actor_not_topic_the_document_digits_never_become_a_topic():
    assert not (objeto_tokens(parse("contratos del NIT 860002400")) & {"860002400"})


# ---------------------------------------------------------------------------
# INV-EXACT-DOC — igualdad exacta sobre el documento, jamás LIKE.
# ---------------------------------------------------------------------------


def test_inv_exact_doc_the_where_filters_the_provider_document_by_equality():
    soql = soql_for("contratos del NIT 860002400")
    assert "documento_proveedor = '860002400'" in soql
    assert "LIKE" not in soql.split("WHERE", 1)[1].split("ORDER BY")[0].upper() or (
        "documento_proveedor" not in soql.upper().split("LIKE")[0][-60:]
    )


def test_inv_exact_doc_all_published_forms_of_the_document_are_reached():
    """La fuente publica el NIT de tres maneras; el filtro cubre las tres.

    El contrato de AP2-A encontró el base solo (90%), el base con DV pegado y
    el base con DV separado por guion conviviendo en la misma columna. Un
    filtro contra una sola forma pierde las otras dos sin decirlo.
    """
    soql = soql_for("contratos del NIT 8600345941")
    for variant in ("860034594", "8600345941", "860034594-1"):
        assert f"documento_proveedor = '{variant}'" in soql


def test_the_check_digit_is_not_expanded_when_nothing_supports_it():
    """Sin DV conocido no se inventan variantes.

    Expandir un número desnudo a base+DV cuando el módulo 11 no lo respalda
    añadiría al OR el documento de OTRO actor.
    """
    params = parse("contratos del NIT 860002400")
    assert params["contratista"] == ["860002400"]


# ---------------------------------------------------------------------------
# INV-MONEY — una cifra de dinero no es un actor (ADR-059).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "contratos de más de 900123456 pesos",
        "contratos de alimentacion de mas de 900123456 COP",
        "contratos mayores a 2000 millones",
    ],
)
def test_inv_money_an_amount_never_produces_an_actor(text):
    params = parse(text)
    assert params.get("contratista") is None
    assert params.get("actor_resolution") is None
    assert params.get("actor_scope") is None


def test_inv_money_an_actor_and_an_amount_coexist_without_eating_each_other():
    params = parse("contratos del NIT 860002400 mayores a 500 millones")
    assert params["actor_scope"]["base_number"] == "860002400"
    assert params["valor_min"] == 500_000_000


# ---------------------------------------------------------------------------
# INV-NAME-NO-GUESS — detectar un actor no autoriza a resolverlo.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "contratos del proveedor FUNDACARIBE",
        "contratos del contratista FUNDACARIBE",
        "contratos de la empresa FUNDACARIBE",
        "qué contratos tiene FUNDACARIBE",
        "qué ha contratado FUNSOCOM",
    ],
)
def test_inv_name_no_guess_a_named_actor_is_declared_never_resolved(text):
    params = parse(text)
    assert params["actor_resolution"] == "name_unresolved"
    assert params["actor_scope"]["requires_resolution"] is True
    assert params.get("contratista") is None


def test_historial_contractual_de_nombre_is_strong_actor_syntax():
    params = parse("historial contractual de FUNDACARIBE")
    assert params["actor_resolution"] == "name_unresolved"
    assert params["actor_scope"]["observed_name"] == "fundacaribe"
    assert "fundacaribe" not in objeto_tokens(params)


@pytest.mark.parametrize(
    ("text", "expected_name"),
    [
        (
            "contratos del proveedor FUNDACIÓN MANOS EN ACCIÓN",
            "fundacion manos en accion",
        ),
        (
            "contratos del proveedor CORPORACIÓN PARA EL DESARROLLO SOCIAL",
            "corporacion para el desarrollo social",
        ),
        (
            "contratos del proveedor FUNDACIÓN CON AMOR",
            "fundacion con amor",
        ),
        (
            "contratos del proveedor ASOCIACIÓN POR COLOMBIA",
            "asociacion por colombia",
        ),
        (
            "contratos del proveedor UNIÓN PARA EL PROGRESO",
            "union para el progreso",
        ),
        (
            "contratos del proveedor CORPORACIÓN DE LA COSTA",
            "corporacion de la costa",
        ),
    ],
)
def test_actor_name_internal_prepositions_are_not_boundaries(text, expected_name):
    params = parse(text)

    assert params["actor_scope"]["observed_name"] == expected_name


@pytest.mark.parametrize(
    ("text", "expected_name", "expected_param", "expected_value"),
    [
        (
            "contratos del proveedor FUNDACIÓN MANOS EN ACCIÓN en 2025",
            "fundacion manos en accion",
            "fecha_desde",
            "2025-01-01",
        ),
        (
            "contratos del proveedor FUNDACIÓN CON AMOR por mayor valor",
            "fundacion con amor",
            "ordering_signal",
            "valor_desc",
        ),
        (
            "contratos del proveedor CORPORACIÓN PARA EL DESARROLLO en Bolívar",
            "corporacion para el desarrollo",
            "departamento_resolved",
            "Bolívar",
        ),
        (
            "contratos del proveedor FUNDACARIBE en 2025 por mayor valor",
            "fundacaribe",
            "ordering_signal",
            "valor_desc",
        ),
    ],
)
def test_actor_name_cuts_only_independently_recognized_modifier_tails(
    text,
    expected_name,
    expected_param,
    expected_value,
):
    params = parse(text)

    assert params["actor_scope"]["observed_name"] == expected_name
    assert params[expected_param] == expected_value


@pytest.mark.parametrize(
    ("text", "expected_name"),
    [
        ("contratos del proveedor UNIVERSIDAD X", "universidad x"),
        ("contratos del contratista HOSPITAL X", "hospital x"),
        ("contratos adjudicados a UNIVERSIDAD X", "universidad x"),
    ],
)
def test_explicit_contractor_role_outweighs_buyer_like_name(text, expected_name):
    params = parse(text)

    assert params["actor_resolution"] == "name_unresolved"
    assert params["actor_scope"]["observed_name"] == expected_name


@pytest.mark.parametrize(
    "text",
    [
        "contratos del proveedor FUNDACARIBE",
        "qué ha contratado FUNSOCOM",
    ],
)
def test_inv_no_topic_fallback_an_unresolved_name_does_not_fall_into_objeto(text):
    """El doble error de AP0/§11-P2, cerrado.

    Un nombre en ``objeto`` pierde los contratos del actor cuyo objeto no lo
    nombra y le atribuye los de terceros que sí lo mencionan.
    """
    tokens = objeto_tokens(parse(text))
    assert not (tokens & {"fundacaribe", "funsocom"})


def test_inv_name_no_guess_an_unresolved_name_never_executes():
    text = "contratos del proveedor FUNDACARIBE"
    plan = compute_query_plan(parse(text), text)
    assert plan.should_execute is False
    assert plan.reason == "actor_name_requires_resolution"
    assert "NIT" in (plan.clarification_question or "")


def test_the_bare_name_stays_out_of_scope_until_ap3():
    """"contratos de FUNDACARIBE" no se clasifica como actor, a propósito.

    Es indistinguible de "contratos de alimentación" sin consultar un índice
    de actores. Clasificarlo con una heurística agresiva rompería las ocho
    consultas temáticas de más abajo.
    """
    params = parse("contratos de FUNDACARIBE")
    assert params.get("actor_resolution") is None
    assert params.get("contratista") is None


# ---------------------------------------------------------------------------
# INV-ACTOR-PRECISION — una señal débil nunca secuestra un tema.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "contratos de alimentación",
        "contratos de mantenimiento",
        "contratos de adulto mayor",
        "contratos de vigilancia",
        "contratos de software",
        "contratos de obra en Boyacá",
        "contratos de la alcaldía de Medellín",
        "procesos de aseo en Cali",
        "contratos de consultoría de la gobernación del Huila",
        "contratos del proveedor de alimentos",
        "contratos del contratista de la obra",
    ],
)
def test_inv_actor_precision_topical_queries_stay_topical(text):
    params = parse(text)
    assert params.get("actor_resolution") is None, params.get("actor_scope")
    assert params.get("contratista") is None


def test_inv_actor_precision_a_buyer_name_is_never_read_as_a_contractor():
    """ADR-040: ante un nombre que puede ser comprador y proveedor, se pregunta.

    Aquí ni siquiera se declara actor — la consulta sigue su camino de
    entidad compradora como siempre.
    """
    params = parse("qué contratos tiene la alcaldía de Medellín")
    assert params.get("actor_resolution") is None


# ---------------------------------------------------------------------------
# INV-NO-SILENT-DROP / INV-DATASET-CAPABILITY.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "procesos del NIT 860002400",
        "licitaciones abiertas del NIT 860002400",
        "oportunidades para el proveedor con NIT 900123456",
        "convocatorias donde participa la cedula 79482763",
    ],
)
def test_inv_no_silent_drop_the_actor_survives_a_dataset_that_cannot_serve_it(text):
    """El actor NUNCA desaparece del frame. Ese borrado es el defecto entero."""
    params = parse(text)
    assert params["contratista"], params
    assert params["actor_resolution"] == "explicit_document"


@pytest.mark.parametrize(
    "text",
    [
        "procesos del NIT 860002400",
        "licitaciones abiertas del NIT 860002400",
        "oportunidades para el proveedor con NIT 900123456",
    ],
)
def test_inv_dataset_capability_an_unsupported_dataset_blocks_instead_of_executing(text):
    params = parse(text)
    plan = compute_query_plan(params, text)
    assert plan.should_execute is False
    assert plan.reason == "actor_dataset_unsupported"


def test_inv_dataset_capability_blocking_runs_before_the_opportunity_scope_rule():
    """P17 antes que P15, y por una razón de honestidad, no de orden.

    "oportunidades para el proveedor con NIT X" no tiene tema, así que P15
    preguntaría qué producto busca el usuario. Esa pregunta esconde el
    problema real: ese conjunto no publica quién contrata.
    """
    text = "oportunidades para el proveedor con NIT 900123456"
    plan = compute_query_plan(parse(text), text)
    assert plan.reason == "actor_dataset_unsupported"


def test_the_where_never_executes_without_the_actor_it_was_asked_about():
    """La prueba directa del bug de AP0: el SoQL de procesos no lleva el documento.

    Por eso la ejecución se bloquea antes. Si algún día el plan dejara pasar
    esta consulta, este SoQL —sin rastro de la cédula— se ejecutaría y su
    resultado se presentaría como el historial de esa persona.
    """
    text = "procesos del NIT 860002400 en 2025"
    params = parse(text)
    assert params["dataset"] == "procesos"

    soql = builder.build(SoQLBuilder.PROCESOS_DATASET, params)
    assert "860002400" not in soql

    assert compute_query_plan(params, text).should_execute is False


def test_a_supported_actor_query_executes_normally():
    text = "contratos del NIT 860002400"
    plan = compute_query_plan(parse(text), text)
    assert plan.should_execute is True


# ---------------------------------------------------------------------------
# Serialización: el frame viaja a Radar y al store sin dataclasses.
# ---------------------------------------------------------------------------


def test_the_actor_scope_is_json_serializable_for_radar_and_the_store():
    import json

    from app.core.query_frame import frame_from_params, params_from_frame

    params = parse("contratos del NIT 860.034.594-1")
    frame = frame_from_params(params)
    restored = params_from_frame(frame)

    assert json.loads(json.dumps(restored["actor_scope"])) == params["actor_scope"]
    assert restored["contratista"] == params["contratista"]


def test_the_contratista_key_stays_the_transport_the_pipeline_already_knows():
    """``contratista`` sigue siendo list[str]: Radar, follow-up y el guard
    anti-``WHERE 1=1`` lo consumen sin saber nada de AP2."""
    from app.core._followup_constants import SCOPE_TOPIC_KEYS

    params = parse("contratos del NIT 860002400")
    assert isinstance(params["contratista"], list)
    assert all(isinstance(v, str) for v in params["contratista"])
    assert "contratista" in SCOPE_TOPIC_KEYS
