"""Etapa 1: lookup por referencia acotado por qualifier de entidad."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.formatter import Formatter
from app.core.direct_lookup import (
    DirectLookupRequest,
    parse_direct_lookup_request,
    resolve_reference_candidates,
)
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.context import HandlerContext
from app.core.workflow.handlers.direct_lookup import handle_direct_lookup


PROCESOS = SoQLBuilder.PROCESOS_DATASET
CONTRATOS = SoQLBuilder.CONTRATOS_DATASET


def _candidate(
    uid: str,
    reference: str,
    entity: str,
    *,
    city: str = "",
    department: str = "",
    description: str = "",
) -> dict:
    return {
        "_dataset_id": "p6dx-8zbt",
        "id_del_proceso": uid,
        "referencia_del_proceso": reference,
        "entidad": entity,
        "ciudad_entidad": city,
        "departamento_entidad": department,
        "descripci_n_del_procedimiento": description,
    }


def test_lp005_leticia_matches_candidate_entity_without_resolver():
    request = parse_direct_lookup_request(
        "busca el proceso LP-005-2026 de la Alcaldía de Leticia"
    )
    resolution = resolve_reference_candidates(request, [
        _candidate("CO1.NTC.OTHER", "LP-005-2026", "ALCALDIA DE OTRA CIUDAD"),
        _candidate(
            "CO1.NTC.LETICIA",
            "LP-005-2026",
            "ALCALDIA MUNICIPIO DE LETICIA",
            city="Leticia",
            department="Amazonas",
        ),
    ])

    assert request.entity_qualifier_raw == "Alcaldía de Leticia"
    assert resolution.outcome == "exact"
    assert resolution.candidates[0]["id_del_proceso"] == "CO1.NTC.LETICIA"


def test_samc_la_estrella_matches_candidate_entity():
    request = parse_direct_lookup_request(
        "busca el proceso SAMC 001 2026 de La Estrella"
    )
    resolution = resolve_reference_candidates(request, [
        _candidate("CO1.NTC.OTHER", "SAMC-001-2026", "MUNICIPIO DE ITAGUI"),
        _candidate("CO1.NTC.STAR", "SAMC-001-2026", "MUNICIPIO DE LA ESTRELLA"),
    ])

    assert request.reference_normalized == "SAMC-001-2026"
    assert request.entity_qualifier_raw == "La Estrella"
    assert resolution.outcome == "exact"
    assert resolution.candidates[0]["entity_match"] == "normalized_qualifier"


def test_qualifier_can_select_tolerant_variant_after_wrong_exact_entity():
    request = parse_direct_lookup_request("SAMC 001 2026 de La Estrella")
    resolution = resolve_reference_candidates(request, [
        _candidate("CO1.NTC.WRONG", "SAMC-001-2026", "MUNICIPIO DE COTA"),
        _candidate(
            "CO1.NTC.STAR",
            "SAMC 1 2026",
            "MUNICIPIO DE LA ESTRELLA",
        ),
    ])

    assert resolution.outcome == "approximate"
    assert resolution.candidates[0]["id_del_proceso"] == "CO1.NTC.STAR"


def test_same_reference_in_two_entities_is_ambiguous():
    request = parse_direct_lookup_request("muéstrame LP-005-2026")
    resolution = resolve_reference_candidates(request, [
        _candidate("CO1.NTC.ONE", "LP-005-2026", "ALCALDIA DE LETICIA"),
        _candidate("CO1.NTC.TWO", "LP-005-2026", "ALCALDIA DE MEDELLIN"),
    ])

    assert resolution.outcome == "ambiguous"
    assert len(resolution.candidates) == 2


def test_duplicate_rows_with_same_uid_are_one_process():
    request = parse_direct_lookup_request("LP-005-2026")
    duplicate = _candidate("CO1.NTC.ONE", "LP-005-2026", "ALCALDIA DE LETICIA")
    resolution = resolve_reference_candidates(request, [duplicate, dict(duplicate)])

    assert resolution.outcome == "exact"
    assert len(resolution.candidates) == 1


def test_qualifier_in_description_does_not_count_as_entity_match():
    request = parse_direct_lookup_request("LP-005-2026 de La Estrella")
    resolution = resolve_reference_candidates(request, [
        _candidate(
            "CO1.NTC.OTHER",
            "LP-005-2026",
            "MUNICIPIO DE ITAGUI",
            description="Obra vial hacia La Estrella",
        ),
    ])

    assert resolution.outcome == "not_found"
    assert resolution.candidates == []


def test_reference_without_qualifier_and_one_uid_is_exact():
    request = parse_direct_lookup_request("quiero ver LP-005-2026")
    resolution = resolve_reference_candidates(request, [
        _candidate("CO1.NTC.ONE", "LP-005-2026", "ALCALDIA DE LETICIA"),
    ])

    assert request.entity_qualifier_raw is None
    assert resolution.outcome == "exact"
    assert resolution.candidates[0]["match_type"] == "exact_reference"


def test_reference_without_match_is_honest_not_found():
    request = parse_direct_lookup_request("LP-005-2026 de Leticia")
    resolution = resolve_reference_candidates(request, [])

    assert resolution.outcome == "not_found"
    assert resolution.outcome_reason == "reference_not_found"


def test_notice_uid_url_keeps_exact_lookup_route():
    url = (
        "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index"
        "?noticeUID=CO1.NTC.2597221"
    )
    request = parse_direct_lookup_request(url)

    assert request.uid == "CO1.NTC.2597221"
    assert request.url == url
    assert request.match_mode == "url"


def test_direct_lookup_request_is_hashable():
    request = parse_direct_lookup_request("LP-005-2026 de Leticia")
    assert request in {request}


def test_conversational_suffix_is_not_an_entity_qualifier():
    request = parse_direct_lookup_request("el proceso LP-005-2026 que pediste")
    assert request.entity_qualifier_raw is None


def test_numeric_segment_001_matches_only_segment_1():
    request = parse_direct_lookup_request("SAMC-001-2026")
    resolution = resolve_reference_candidates(request, [
        _candidate("CO1.NTC.ONE", "SAMC-1-2026", "MUNICIPIO DE LA ESTRELLA"),
    ])
    assert resolution.outcome == "approximate"


@pytest.mark.parametrize(
    "reference",
    [
        "SAMC-0010-2026",
        "SAMC-101-2026",
        "SAMC-1001-2026",
        "LP-001-2026",
        "SAMC-001-2025",
        "SAMC-001-2026-2",
    ],
)
def test_numeric_segment_does_not_match_nearby_or_longer_reference(reference):
    request = parse_direct_lookup_request("SAMC-001-2026")
    resolution = resolve_reference_candidates(request, [
        _candidate("CO1.NTC.WRONG", reference, "MUNICIPIO DE LA ESTRELLA"),
    ])
    assert resolution.outcome == "not_found"


class _Feedback:
    def __init__(self):
        self.last_result = None

    def log_trace(self, _query, _channel, result, **_kwargs):
        self.last_result = result
        return "trace-test"


class _Formatter:
    def format_for_channel(self, results, _dataset, _channel, **_kwargs):
        return "detalle", results


class _RecordingFormatter(Formatter):
    def __init__(self):
        super().__init__(max_results=25)
        self.to_rows_calls = []

    def to_rows(self, results, dataset_id, params=None, *, reference_instant=None):
        self.to_rows_calls.append((list(results), dataset_id, reference_instant))
        return super().to_rows(
            results, dataset_id, params, reference_instant=reference_instant,
        )


class _Client:
    """Respuestas por (dataset, mode); las listas respetan LIMIT."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def query(self, dataset, soql):
        if "id_del_proceso =" in soql or "id_contrato =" in soql:
            mode = "uid"
        elif " = UPPER(" in soql:
            mode = "exact"
        else:
            mode = "tolerant"
        limit = int(soql.rsplit("LIMIT ", 1)[1].split()[0])
        self.calls.append((dataset, mode, limit))
        response = self.responses.get((dataset, mode), [])
        if isinstance(response, BaseException):
            raise response
        return [dict(row) for row in response[:limit]]


def _run(query, responses, *, formatter=None):
    feedback = _Feedback()
    deps = SimpleNamespace(
        secop_client=_Client(responses),
        formatter=formatter or _Formatter(),
        feedback=feedback,
    )
    result = handle_direct_lookup(deps, HandlerContext(user_query=query))
    return result, deps.secop_client


def test_multi_hit_disambiguation_uses_operational_process_state_and_keeps_contract():
    selected = {
        **_candidate("CO1.NTC.SELECTED", "LP-005-2026", "ALCALDIA UNO"),
        "nombre_del_procedimiento": "PROCESO SELECCIONADO",
        "estado_de_apertura_del_proceso": "Abierto",
        "estado_del_procedimiento": "Seleccionado",
        "urlproceso": {"url": "https://example.test/selected"},
    }
    evaluation = {
        **_candidate("CO1.NTC.EVALUATION", "LP-005-2026", "ALCALDIA DOS"),
        "nombre_del_procedimiento": "PROCESO EN EVALUACION",
        "estado_de_apertura_del_proceso": "Abierto",
        "estado_del_procedimiento": "Evaluación",
        "urlproceso": {"url": "https://example.test/evaluation"},
    }
    contract = {
        "id_contrato": "CO1.PCCNTR.1",
        "referencia_del_contrato": "LP-005-2026",
        "nombre_entidad": "ENTIDAD CONTRATO",
        "objeto_del_contrato": "CONTRATO SIN CAMBIOS",
        "estado_contrato": "En ejecución",
        "urlproceso": "https://example.test/contract",
    }
    responses = {
        (PROCESOS, "exact"): [selected, evaluation],
        (CONTRATOS, "exact"): [contract],
    }

    formatter = _RecordingFormatter()
    result, _client = _run("LP-005-2026", responses, formatter=formatter)

    response = result["response"]
    assert result["total_count"] == 3
    assert "📌 ⚫ Seleccionado" in response
    assert "SECOP reporta apertura: Abierto" in response
    assert "📌 🟠 En evaluación" in response
    assert "[Contrato] `CO1.PCCNTR.1`" in response
    assert "📌 En ejecución" in response
    assert response.index("CO1.NTC.SELECTED") < response.index("CO1.NTC.EVALUATION")
    assert response.index("CO1.NTC.EVALUATION") < response.index("CO1.PCCNTR.1")
    # Solo los dos procesos visibles se enriquecen; el contrato no pasa por M2.
    assert [call[1] for call in formatter.to_rows_calls] == [PROCESOS, PROCESOS]
    # Ambas llamadas reciben exactamente el mismo objeto temporal.
    instants = [call[2] for call in formatter.to_rows_calls]
    assert instants[0] is instants[1]


@pytest.mark.parametrize(
    ("failed_dataset", "completed_dataset"),
    [(CONTRATOS, PROCESOS), (PROCESOS, CONTRATOS)],
)
def test_reference_partial_timeout_without_match_is_not_confirmed(
    failed_dataset, completed_dataset,
):
    responses = {
        (failed_dataset, "exact"): TimeoutError("timed out"),
        (failed_dataset, "tolerant"): TimeoutError("timed out"),
        (completed_dataset, "exact"): [],
        (completed_dataset, "tolerant"): [],
    }
    result, _client = _run("LP-005-2026 de Leticia", responses)

    assert result["direct_lookup_status"] == "not_confirmed"
    assert result["direct_lookup_reason"] == "partial_source_failure"
    assert result["source_partial"] is True
    assert any(a["timeout"] for a in result["search_attempts"] if a["status"] == "failed")


def test_reference_all_sources_complete_without_match_is_not_found():
    result, _client = _run("LP-005-2026 de Leticia", {})
    assert result["direct_lookup_status"] == "not_found"
    assert result["failed_sources"] == []


def test_reference_all_sources_fail_is_source_unavailable():
    failure = TimeoutError("timed out")
    responses = {
        (dataset, mode): failure
        for dataset in (PROCESOS, CONTRATOS)
        for mode in ("exact", "tolerant")
    }
    result, _client = _run("LP-005-2026 de Leticia", responses)
    assert result["direct_lookup_status"] == "source_unavailable"
    assert result["successful_sources"] == []


def test_wrapped_timeout_is_recorded_as_timeout():
    wrapped = RuntimeError("source query failed")
    wrapped.__cause__ = TimeoutError("read timed out")
    result, _client = _run("LP-005-2026 de Leticia", {
        (PROCESOS, "exact"): wrapped,
    })
    failed = [a for a in result["search_attempts"] if a["status"] == "failed"]
    assert failed[0]["error_type"] == "RuntimeError"
    assert failed[0]["timeout"] is True


def test_reference_match_survives_other_source_failure_with_partial_trace():
    match = _candidate("CO1.NTC.LETICIA", "LP-005-2026", "ALCALDIA DE LETICIA")
    responses = {
        (PROCESOS, "exact"): [match],
        (CONTRATOS, "exact"): TimeoutError("timed out"),
    }
    result, _client = _run("LP-005-2026 de Leticia", responses)
    assert result["direct_lookup_status"] == "exact"
    assert result["results"][0]["id_del_proceso"] == "CO1.NTC.LETICIA"
    assert result["source_partial"] is True
    assert CONTRATOS in result["failed_sources"]


@pytest.mark.parametrize(
    "query",
    [
        "CO1.NTC.2597221",
        (
            "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index"
            "?noticeUID=CO1.NTC.2597221"
        ),
    ],
)
@pytest.mark.parametrize(
    ("responses", "expected"),
    [
        ({}, "not_found"),
        ({(PROCESOS, "uid"): TimeoutError(), (CONTRATOS, "uid"): TimeoutError()}, "source_unavailable"),
        ({(PROCESOS, "uid"): TimeoutError()}, "not_confirmed"),
    ],
)
def test_uid_and_url_share_source_failure_semantics(query, responses, expected):
    result, _client = _run(query, responses)
    assert result["direct_lookup_status"] == expected


@pytest.mark.parametrize(
    "query",
    [
        "CO1.NTC.2597221",
        (
            "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index"
            "?noticeUID=CO1.NTC.2597221"
        ),
    ],
)
def test_uid_and_url_return_match_despite_other_source_failure(query):
    match = _candidate("CO1.NTC.2597221", "LP-005-2026", "ALCALDIA DE LETICIA")
    responses = {
        (PROCESOS, "uid"): [match],
        (CONTRATOS, "uid"): TimeoutError("timed out"),
    }
    result, _client = _run(query, responses)
    assert result["direct_lookup_status"] == "exact"
    assert result["source_partial"] is True


def test_exact_complete_but_required_tolerant_failure_is_not_confirmed():
    responses = {
        (PROCESOS, "tolerant"): TimeoutError("timed out"),
        (CONTRATOS, "tolerant"): [],
    }
    result, _client = _run("LP-005-2026 de Leticia", responses)
    assert result["direct_lookup_status"] == "not_confirmed"
    assert result["direct_lookup_reason"] == "partial_source_failure"


def test_qualified_pool_expands_past_first_ten_with_stable_absolute_limit():
    wrong = [
        _candidate(f"CO1.NTC.{i}", "LP-005-2026", f"ALCALDIA DE MUNICIPIO {i}")
        for i in range(10)
    ]
    correct = _candidate("CO1.NTC.LETICIA", "LP-005-2026", "ALCALDIA DE LETICIA")
    responses = {(PROCESOS, "exact"): [*wrong, correct]}
    result, client = _run("LP-005-2026 de Leticia", responses)

    assert result["direct_lookup_status"] == "exact"
    assert result["results"][0]["id_del_proceso"] == "CO1.NTC.LETICIA"
    assert (PROCESOS, "exact", 10) in client.calls
    assert (PROCESOS, "exact", 20) in client.calls
    assert max(limit for _dataset, _mode, limit in client.calls) <= 40


# ── H2 follow-up: direct_lookup acotado por el deadline del turno ─────────────

class _SlowClock:
    """Reloj falso monotónico; cada probe avanza el tiempo (probe lenta)."""

    def __init__(self, step_s: float):
        self.t = 0.0
        self.step_s = step_s

    def __call__(self):
        return self.t


class _SlowClient:
    """Cada query avanza el reloj `step_s` segundos y devuelve [] (sin match)."""

    timeout = 15

    def __init__(self, clock: _SlowClock):
        self.clock = clock
        self.calls = []

    def query(self, dataset, soql):
        self.calls.append(dataset)
        self.clock.t += self.clock.step_s
        return []


def test_direct_lookup_cuts_serial_probes_when_deadline_exhausted():
    """Antes: hasta 12 probes en serie sin deadline (un turno llegó a 67s). Ahora: cada
    probe se acota al presupuesto restante y se CORTA honestamente al agotarse — sin colgar.
    """
    from app.core import deadline as dl_mod

    clock = _SlowClock(step_s=5.0)           # cada probe "tarda" 5s
    dl_mod.set_clock(clock)
    feedback = _Feedback()
    deps = SimpleNamespace(
        secop_client=_SlowClient(clock), formatter=_Formatter(), feedback=feedback,
    )
    try:
        tokens = dl_mod.begin_deadline(0.0)   # budget 15, reserve 4 → 11s para fetch
        result = handle_direct_lookup(
            deps, HandlerContext(user_query="busca el proceso LP-005-2026 de la Alcaldía de Leticia"),
        )
    finally:
        dl_mod.end_deadline(tokens)
        dl_mod.reset_clock()

    # SIN el corte habría hasta 12 probes; con presupuesto 11s y probes de 5s → ~3 y corta.
    assert len(deps.secop_client.calls) <= 4
    assert clock.t <= 15.0                     # el turno no se pasa del techo
    assert result["direct_lookup_status"] == "not_confirmed"
    assert result["direct_lookup_reason"] == "deadline_exhausted"
    assert any(a.get("skipped_due_to_budget") for a in result["search_attempts"])
    assert "tiempo disponible" in result["response"].lower()


def test_direct_lookup_unbounded_without_deadline():
    """Sin deadline activo (legacy/tests) el comportamiento previo se conserva: proba todo."""
    result, client = _run("LP-005-2026 de Leticia", {})   # sin deadline activo
    assert result["direct_lookup_status"] == "not_found"
    assert not any(a.get("skipped_due_to_budget") for a in result["search_attempts"])
