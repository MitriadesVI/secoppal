from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.config import Settings
from app.core import llm_handler as llm_module
from app.core.formatter import Formatter, build_no_results_message
from app.core.llm_handler import LLMHandler
from app.core.modality_dataset_projection import (
    ModalityDatasetProjectionError,
)
from app.core.modality_resolution_models import ResolutionStatus
from app.core.orchestrator import SecopalWorkflow
from app.core.query_fingerprint import query_fingerprint
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError
from app.core.workflow.dual_dataset import params_for_dataset

_INCLUDE = "modalidad_include_procedure_ids"
_EXCLUDE = "modalidad_exclude_procedure_ids"
_LEGACY = "modalidad"


def _parse(query: str):
    return QueryRouter().parse(query)


def _where(parsed) -> str:
    builder = SoQLBuilder()
    soql = builder.build(
        builder.dataset_id_for(parsed.params.get("dataset")),
        parsed.params,
    )
    return soql.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]


def test_absence_preserves_object_scope_dates_and_emits_no_modality_params():
    parsed = _parse(
        "contratos de alimentación en Barranquilla en 2025"
    )
    assert parsed.modality_interpretation.resolution_status is (
        ResolutionStatus.NO_MATCH
    )
    assert parsed.params["objeto"] == ["alimentacion"]
    assert parsed.params["ciudad"] == "Barranquilla"
    assert parsed.params["fecha_desde"] == "2025-01-01"
    assert parsed.params["fecha_hasta"] == "2025-12-31"
    assert not ({_INCLUDE, _EXCLUDE, _LEGACY, "issues", "diagnostics"} & parsed.params.keys())


def test_family_specific_alternatives_exclusion_amp_and_state_collision():
    family = _parse(
        "selecciones abreviadas de alimentación en Barranquilla"
    )
    assert family.params[_INCLUDE] == [
        "seleccion_abreviada_menor_cuantia",
        "seleccion_abreviada_menor_cuantia_sin_manifestacion",
        "seleccion_abreviada_subasta_inversa",
    ]
    assert family.params["objeto"] == ["alimentacion"]
    assert "modalidad_de_contratacion IN (" in _where(family)

    specific = _parse("subastas inversas de dotación")
    assert specific.params[_INCLUDE] == [
        "seleccion_abreviada_subasta_inversa"
    ]
    assert "modalidad_de_contratacion = " in _where(specific)

    alternatives = _parse(
        "contratación directa con o sin ofertas de alimentación"
    )
    assert alternatives.params[_INCLUDE] == [
        "contratacion_directa",
        "contratacion_directa_con_ofertas",
    ]
    assert _where(alternatives).count(
        "modalidad_de_contratacion IN"
    ) == 1

    excluded = _parse(
        "contratos de alimentación excepto contratación directa"
    )
    assert _INCLUDE not in excluded.params
    assert excluded.params[_EXCLUDE] == [
        "contratacion_directa",
        "contratacion_directa_con_ofertas",
    ]
    assert excluded.params["objeto"] == ["alimentacion"]
    assert "modalidad_de_contratacion NOT IN" in _where(excluded)

    amp = _parse("acuerdos marco de precios de alimentación")
    assert amp.params[_INCLUDE] == ["acuerdo_marco_precio_secop"]
    assert "seleccion_abreviada" not in str(amp.params[_INCLUDE])

    collision = _parse(
        "concurso de méritos abierto de interventoría"
    )
    assert collision.params[_INCLUDE] == ["concurso_meritos_abierto"]
    assert not collision.params.get("estado")
    assert collision.params["objeto"] == ["interventoria"]

    separate_state = _parse(
        "concurso de méritos abierto de interventoría, publicados"
    )
    assert separate_state.params[_INCLUDE] == [
        "concurso_meritos_abierto"
    ]
    assert separate_state.params["estado"] == "Publicado"
    assert separate_state.params["objeto"] == ["interventoria"]

    public_tender = _parse("licitación pública de alimentación")
    assert public_tender.params[_INCLUDE] == [
        "licitacion_publica",
        "licitacion_publica_obra",
    ]
    assert not public_tender.params.get("estado")
    assert public_tender.params["objeto"] == ["alimentacion"]

    published_tender = _parse(
        "licitación pública de alimentación, publicada"
    )
    assert published_tender.params[_INCLUDE] == [
        "licitacion_publica",
        "licitacion_publica_obra",
    ]
    assert published_tender.params["estado"] == "Publicado"
    assert published_tender.params["objeto"] == ["alimentacion"]


def test_adversarial_phrases_do_not_invent_modality():
    generic_public = _parse("contratación pública")
    assert generic_public.modality_interpretation.resolution_status is (
        ResolutionStatus.NO_MATCH
    )
    assert _INCLUDE not in generic_public.params

    supplier_request = _parse("solicitud a proveedor")
    assert supplier_request.modality_interpretation.resolution_status is (
        ResolutionStatus.NO_MATCH
    )
    assert _INCLUDE not in supplier_request.params


def test_open_tender_precedence_preserves_intent_state_and_topic():
    generic = _parse("licitaciones abiertas")
    assert generic.modality_interpretation.resolution_status is (
        ResolutionStatus.NO_MATCH
    )
    assert _INCLUDE not in generic.params
    assert generic.params["intent_type"] == "opportunity_search"
    assert generic.params["estado_family"] == "oferta_abierta"
    assert "objeto" not in generic.params

    generic_topic = _parse(
        "licitaciones abiertas de alimentación"
    )
    assert generic_topic.modality_interpretation.resolution_status is (
        ResolutionStatus.NO_MATCH
    )
    assert _INCLUDE not in generic_topic.params
    assert generic_topic.params["intent_type"] == "opportunity_search"
    assert generic_topic.params["estado_family"] == "oferta_abierta"
    assert generic_topic.params["objeto"] == ["alimentacion"]

    singular = _parse("licitación abierta de alimentación")
    assert singular.modality_interpretation.resolution_status is (
        ResolutionStatus.NO_MATCH
    )
    assert _INCLUDE not in singular.params
    assert singular.params["intent_type"] == "opportunity_search"
    assert singular.params["estado_family"] == "oferta_abierta"
    assert singular.params["objeto"] == ["alimentacion"]

    processes = _parse("procesos abiertos de alimentación")
    assert processes.modality_interpretation.resolution_status is (
        ResolutionStatus.NO_MATCH
    )
    assert _INCLUDE not in processes.params
    assert processes.params["intent_type"] == "opportunity_search"
    assert processes.params["estado_family"] == "oferta_abierta"
    assert processes.params["objeto"] == ["alimentacion"]


def test_public_tender_plus_open_state_keeps_both_semantics():
    for query in (
        "licitación pública abierta de alimentación",
        "licitaciones públicas abiertas de alimentación",
    ):
        parsed = _parse(query)
        assert parsed.modality_interpretation.resolution_status is (
            ResolutionStatus.RESOLVED
        )
        assert parsed.params[_INCLUDE] == [
            "licitacion_publica",
            "licitacion_publica_obra",
        ]
        assert parsed.params["intent_type"] == "opportunity_search"
        assert parsed.params["estado_family"] == "oferta_abierta"
        assert parsed.params["objeto"] == ["alimentacion"]


def test_merits_open_qualifier_remains_modality_specific():
    parsed = _parse(
        "concurso de méritos abierto de interventoría"
    )
    assert parsed.params[_INCLUDE] == ["concurso_meritos_abierto"]
    assert not parsed.params.get("estado")
    assert not parsed.params.get("estado_family")
    assert parsed.params["objeto"] == ["interventoria"]

    published = _parse(
        "concurso de méritos abierto publicado de interventoría"
    )
    assert published.params[_INCLUDE] == ["concurso_meritos_abierto"]
    assert published.params["estado_family"] == "oferta_abierta"
    assert published.params["objeto"] == ["interventoria"]


@pytest.mark.parametrize(
    "query",
    [
        "contratación pública abierta",
        "concurso público de méritos abierto",
        "concursos abiertos",
        "procesos de licitación abiertos",
        "convocatoria abierta",
        "convocatorias abiertas",
    ],
)
def test_generic_open_language_does_not_invent_modality(query):
    parsed = _parse(query)
    assert parsed.modality_interpretation.resolution_status is (
        ResolutionStatus.NO_MATCH
    )
    assert _INCLUDE not in parsed.params


def test_real_publication_state_survives_explicit_modality_and_open_state():
    for query in (
        "licitación publicada",
        "licitación pública publicada",
        "licitación pública abierta publicada",
    ):
        parsed = _parse(query)
        assert parsed.params[_INCLUDE] == [
            "licitacion_publica",
            "licitacion_publica_obra",
        ]
        assert parsed.params.get("estado") in {"Publicado", "Abierto"}
        assert "publicada" not in parsed.params.get("objeto", [])


def _blocked_workflow(tmp_path: Path):
    workflow = SecopalWorkflow(
        Settings(
            secop_feedback_path=tmp_path / "feedback.jsonl",
            secop_feedback_db_path=tmp_path / "secoppal.db",
        )
    )
    calls: list[tuple] = []
    workflow.secop_client.query = lambda *args, **kwargs: calls.append(
        ("query", args)
    )
    workflow.secop_client.count = lambda *args, **kwargs: calls.append(
        ("count", args)
    )
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None
    return workflow, calls


@pytest.mark.parametrize(
    ("query", "status", "reason"),
    [
        (
            "licitación pública o subasta de alimentación",
            ResolutionStatus.PARTIAL,
            "modality_partial_requires_clarification",
        ),
        (
            "subasta de alimentación",
            ResolutionStatus.AMBIGUOUS,
            "modality_ambiguous",
        ),
        (
            "mínima cuantía con o sin ofertas",
            ResolutionStatus.CONFLICT,
            "modality_conflict",
        ),
        (
            "subasta de prueba",
            ResolutionStatus.NOT_SEARCHABLE,
            "modality_not_searchable",
        ),
        (
            "solicitud de información a proveedores",
            ResolutionStatus.DATASET_INAPPLICABLE,
            "modality_dataset_inapplicable",
        ),
    ],
)
def test_non_executable_mentions_block_before_soql_and_http(
    query, status, reason, tmp_path
):
    parsed = _parse(query)
    assert parsed.modality_interpretation.resolution_status is status
    if status is ResolutionStatus.DATASET_INAPPLICABLE:
        assert parsed.params["dataset"] == "contratos"
        assert parsed.params.get("dataset_scope") is None
    if status is ResolutionStatus.PARTIAL:
        assert parsed.modality_interpretation.candidate_filter.include_procedure_ids
        assert parsed.modality_interpretation.executable_filter.is_empty()

    workflow, calls = _blocked_workflow(tmp_path)
    result = workflow.run_query(query, channel="streamlit")
    assert result["query_plan"]["should_execute"] is False
    assert result["query_plan"]["reason"] == reason
    assert result["soql_query"] == ""
    assert calls == []


def test_dual_resolution_keeps_id_and_each_child_projects_independently():
    parsed = _parse(
        "procesos y contratos de solicitud de información a proveedores"
    )
    assert parsed.modality_interpretation.resolution_status is (
        ResolutionStatus.RESOLVED
    )
    assert parsed.params[_INCLUDE] == [
        "solicitud_informacion_proveedores"
    ]

    builder = SoQLBuilder()
    process_params = params_for_dataset(parsed.params, "procesos")
    contract_params = params_for_dataset(parsed.params, "contratos")
    process_where = builder.build(
        builder.PROCESOS_DATASET, process_params
    ).split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]
    contract_where = builder.build(
        builder.CONTRATOS_DATASET, contract_params
    ).split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]

    assert "Solicitud de información a los Proveedores" in process_where
    assert contract_where == "1=0"
    assert process_params[_INCLUDE] == contract_params[_INCLUDE]


def test_soql_canonical_clauses_zero_set_unknown_id_and_global_guard():
    builder = SoQLBuilder()
    include = builder.build(
        builder.PROCESOS_DATASET,
        {_INCLUDE: ["licitacion_publica"]},
    )
    assert (
        "modalidad_de_contratacion = 'Licitación pública'" in include
    )

    include_many = builder.build(
        builder.PROCESOS_DATASET,
        {_INCLUDE: ["licitacion_publica", "minima_cuantia"]},
    )
    assert "modalidad_de_contratacion IN (" in include_many

    exclude = builder.build(
        builder.PROCESOS_DATASET,
        {_EXCLUDE: ["contratacion_directa"]},
    )
    assert (
        "modalidad_de_contratacion NOT IN ('Contratación directa')"
        in exclude
    )

    combined = builder.build(
        builder.PROCESOS_DATASET,
        {
            _INCLUDE: ["licitacion_publica"],
            _EXCLUDE: ["contratacion_directa"],
        },
    )
    assert (
        "(modalidad_de_contratacion = 'Licitación pública') AND "
        "(modalidad_de_contratacion NOT IN ('Contratación directa'))"
        in combined
    )

    zero = builder.build(
        builder.CONTRATOS_DATASET,
        {_INCLUDE: ["solicitud_informacion_proveedores"]},
    )
    assert " WHERE 1=0 " in zero

    with pytest.raises(ModalityDatasetProjectionError):
        builder.build(
            builder.PROCESOS_DATASET,
            {_INCLUDE: ["no_existe"]},
        )

    with pytest.raises(UnsafeGlobalQueryError):
        builder.build(
            builder.CONTRATOS_DATASET,
            {_EXCLUDE: ["solicitud_informacion_proveedores"]},
        )

    with pytest.raises(ValueError, match="legacy modality field"):
        builder.build(
            builder.PROCESOS_DATASET,
            {_LEGACY: "Licitación pública"},
        )


class _HostileOpenAI:
    def __init__(self, **kwargs):
        args = {
            "modalidad": "Contratación directa",
            _INCLUDE: ["inventado"],
            _EXCLUDE: ["tambien_inventado"],
            "objeto": ["alimentacion"],
        }
        tool_call = SimpleNamespace(
            function=SimpleNamespace(
                name="buscar_procesos",
                arguments=json.dumps(args),
            )
        )
        response = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(tool_calls=[tool_call])
                )
            ]
        )
        self.chat = SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **request: response
            )
        )


def test_llm_hostile_modality_output_is_ignored_and_parser_filter_survives(
    monkeypatch,
):
    monkeypatch.setattr(llm_module, "OpenAI", _HostileOpenAI)
    existing = {
        "dataset": "procesos",
        _INCLUDE: ["licitacion_publica"],
    }
    result = LLMHandler(api_key="test").parse(
        "alimentación", existing_params=existing
    )

    assert result[_INCLUDE] == ["licitacion_publica"]
    assert _EXCLUDE not in result
    assert _LEGACY not in result


def test_formatter_uses_canonical_filter_and_suppresses_tautological_dominance():
    for key in (_INCLUDE, _EXCLUDE):
        message = build_no_results_message(
            {key: ["contratacion_directa"], "objeto": ["alimentacion"]},
            "p6dx-8zbt",
        )
        assert "quita o cambia el filtro de modalidad" in message

    inapplicable = build_no_results_message(
        {
            "dataset": "contratos",
            _EXCLUDE: ["solicitud_informacion_proveedores"],
        },
        "jbjy-vk9h",
    )
    assert "filtro de modalidad" not in inapplicable

    insights = SimpleNamespace(
        modality_dominance_name="Contratación directa",
        modality_dominance_pct=75.0,
    )
    assert "domina" not in Formatter._insights_summary(
        insights, {_INCLUDE: ["contratacion_directa"]}
    )
    assert "domina" in Formatter._insights_summary(
        insights, {_EXCLUDE: ["licitacion_publica"]}
    )


# M1A.1: four Barranquilla fingerprints now carry accredited municipal
# city+department scope and never an inferred BuyerUnit; modality stays NO_MATCH.
_NO_MODALITY_GOLDEN = {
    "contratos de alimentación": (
        "1084437227dc22cc853225828a8a7996bb8f46ba7a1ce2eba5b3febc6ba87f4f",
        ("objeto_del_contrato", "fecha_de_firma DESC"),
    ),
    "contratos de alimentación del SENA": (
        "2cc2481aa110cb9e4f2741466f290b07bccb156ec0a4259efa3c563b1abe0a26",
        ("SENA SECRETARIA GENERAL", "objeto_del_contrato"),
    ),
    "procesos de alimentación en el departamento del Atlántico": (
        "892d649522c8e113d38ac67088b2110993acc5f9d99b4bbf3cf90f8ef3baf048",
        ("departamento_entidad = 'Atlántico'",),
    ),
    "contratos de alimentación en Barranquilla": (
        "49217e5c57215bc84b71ad8ea9f50516a24afb272bb4a9f72cedec72e1183978",
        ("ciudad", "Barranquilla"),
    ),
    "contratos de alimentación por más de 500 millones": (
        "9a8a07cfe52b75ac9f5b7519bf8c0abbd86d47b2f0bd10e7e2caad3658c460f5",
        ("valor_del_contrato >= 500000000",),
    ),
    "contratos de alimentación entre 2024 y 2025": (
        "8bc90f440f7bfda2938525825fd5f8b2e1bb5c0cf970d72050f2a6c45f9b8eb5",
        ("fecha_de_firma >= '2024-01-01'", "fecha_de_firma <= '2025-12-31'"),
    ),
    # ADR-067: contrato_activo se proyecta completo. El marcador anterior era
    # el escalar truncado ``estado_contrato = 'En ejecución'``, que perdía
    # Modificado y Prorrogado (ADR-003). Igual que en el caso AP2-B de abajo,
    # el fingerprint cambia porque cambió el FILTRO, no la interpretación de
    # modalidad, que sigue siendo NO_MATCH y sin cláusula propia.
    "contratos en ejecución de alimentación": (
        "efe365c825462125cec0541db2ed2b7040ebdaf3d71e86d5dedd90519c889aef",
        ("estado_contrato IN ('En ejecución', 'Modificado', 'Prorrogado')",),
    ),
    "oportunidades de alimentación en Barranquilla": (
        "c43b2ace8e26758d3c7e583a225d26647ade4d7c43f85b04f5e4c60d08890fd9",
        ("estado_del_procedimiento IN", "estado_de_apertura_del_proceso = 'Abierto'"),
    ),
    "procesos y contratos de alimentación en Barranquilla": (
        "ea53a138053dd38d78c87f83d6956d692a4321ac2da1ce13852ca032ca0ae94a",
        ("id_del_proceso", "Barranquilla"),
    ),
    # R2D.1: "sena bolivar" dejó de descartarse como topónimo desnudo. Antes la
    # consulta se partía en un comprador genérico —SENA— más el territorio
    # Bolívar; ahora nombra a la regional, que es lo que el usuario pidió. El
    # fingerprint cambia porque cambió el WHERE, no la modalidad.
    "contratos de alimentación de la regional SENA Bolívar": (
        "a55f462444d86196ea012f4eb8ff5f0636b75a9d022b264f4c91015ac2901bd1",
        ("SENA REGIONAL BOLIVAR",),
    ),
    "contratos de manual de contratación en Barranquilla": (
        "41ac6afc1db80deea8ee1ef09afda50255d110539a9adea424dbfd74f1246c00",
        ("manual", "LIMIT 200"),
    ),
    # AP2-B: el filtro ganó una tercera variante, "800091140-4". La fuente
    # publica el DV de las tres formas —pegado, separado y ausente— y el
    # filtro de igualdad solo alcanzaba dos. El fingerprint cambia porque
    # cambió el filtro, no la interpretación de modalidad.
    "contratos del proveedor NIT 800.091.140-4": (
        "32b6c4fad81841570b88e3d38ca4824c733fa2ec63660e59b1a16dfd3308d669",
        ("documento_proveedor = '800091140'", "documento_proveedor = '8000911404'",
         "documento_proveedor = '800091140-4'"),
    ),
}


@pytest.mark.parametrize(
    ("query", "golden"), _NO_MODALITY_GOLDEN.items()
)
def test_no_modality_pre_post_golden_parity(query, golden):
    expected_fingerprint, soql_markers = golden
    parsed = _parse(query)
    builder = SoQLBuilder()
    soql = builder.build(
        builder.dataset_id_for(parsed.params.get("dataset")),
        parsed.params,
    )

    assert parsed.modality_interpretation.resolution_status is (
        ResolutionStatus.NO_MATCH
    )
    assert query_fingerprint(parsed.params) == expected_fingerprint
    assert parsed.needs_llm is False
    assert parsed.route_reason == "heuristic_only"
    assert all(marker in soql for marker in soql_markers)
    where = soql.split(" WHERE ", 1)[1].rsplit(" ORDER BY ", 1)[0]
    assert not re.search(
        r"modalidad_de_contratacion\s+(?:=|IN|NOT IN)", where
    )
    assert not ({_INCLUDE, _EXCLUDE, _LEGACY, "issues", "diagnostics"} & parsed.params.keys())


def test_canonical_lists_make_fingerprint_order_stable_without_envelope():
    left = {
        "dataset": "procesos",
        "objeto": ["alimentacion"],
        _INCLUDE: ["licitacion_publica", "minima_cuantia"],
    }
    right = {
        "objeto": ["alimentacion"],
        _INCLUDE: ["minima_cuantia", "licitacion_publica"],
        "dataset": "procesos",
    }
    assert query_fingerprint(left) == query_fingerprint(right)


def test_production_has_no_legacy_modality_authority_patterns():
    root = Path(__file__).resolve().parents[1] / "app"
    patterns = (
        r"MODALIDAD_KEYWORDS",
        r"_extract_modality",
        r'params\.get\("modalidad"\)',
        r'params\["modalidad"\]',
        r'"modalidad"\s*:\s*\{"type"',
    )
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        if any(re.search(pattern, source) for pattern in patterns):
            offenders.append(str(path.relative_to(root.parent)))
    assert offenders == []


def test_new_modules_respect_layering_and_line_budgets():
    core = Path(__file__).resolve().parents[1] / "app/core"
    budgets = {
        "modality_parser_integration.py": 180,
        "modality_dataset_projection.py": 240,
        "modality_execution_policy.py": 180,
    }
    forbidden = (
        "app.core.radar",
        "app.warehouse",
        "app.core.query_router",
        "app.core.soql_builder",
    )
    for filename, budget in budgets.items():
        source = (core / filename).read_text(encoding="utf-8")
        assert len(source.splitlines()) <= budget
        if filename != "modality_parser_integration.py":
            assert not any(name in source for name in forbidden)
