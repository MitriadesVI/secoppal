"""La semántica conversacional obligatoria, de extremo a extremo.

Cada prueba de aquí mira la ENTRADA, el Frame publicado, el SQL realmente
enviado y la respuesta visible. Un objeto serializado correcto no acredita
nada si la consulta que salió decía otra cosa: esa disociación es el hallazgo
que originó todo este trabajo.
"""
from __future__ import annotations

import json

from app.core.conversation_store import ConversationStore
from app.core.entity_profile.frame_values import (
    SourceKind,
    SourceOrigin,
    TemporalEventKind,
)
from entity_profile_frame_b_harness import (  # noqa: F401 — fixture por nombre
    BUYER,
    Conversation,
    FailingClient,
    UnreadableValueClient,
    frame_v12,
)


_AUGUST, _JULY = "2026-08-01", "2026-07-01"


# ── Origen explícito ───────────────────────────────────────────────────────

def test_an_explicit_origin_is_still_explicit_after_a_reload(frame_v12):
    chat = Conversation("sem-origin")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    chat.say("recientes")
    selection = chat.profile_state().confirmed_frame.source_selection
    assert selection.origin is SourceOrigin.EXPLICIT
    assert selection.requested == frozenset({SourceKind.CONTRACTS})


# ── Fechas visibles en TODAS las consultas ─────────────────────────────────

def test_the_displayed_period_is_in_every_executed_query(frame_v12):
    chat = Conversation("sem-dates")
    overview = chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    assert "Agosto de 2026" in overview.response
    assert overview.sql
    for soql in overview.sql:
        assert _AUGUST in soql, soql


def test_two_sources_with_different_periods_are_not_globalised(frame_v12):
    chat = Conversation("sem-two-periods")
    turn = chat.say(
        f"analiza procesos publicados en julio y contratos firmados en agosto "
        f"de {BUYER}",
    )
    assert not turn.clarifies
    frame = chat.profile_state().confirmed_frame
    processes = frame.predicates_for(SourceKind.PROCESSES)
    contracts = frame.predicates_for(SourceKind.CONTRACTS)
    assert processes.temporal.period.lower == _JULY
    assert contracts.temporal.period.lower == _AUGUST
    assert processes.temporal.event is TemporalEventKind.PROCESS_PUBLICATION
    assert contracts.temporal.event is TemporalEventKind.CONTRACT_SIGNATURE

    # Y el SQL de cada fuente lleva SU ventana, no una común.
    for _, dataset, soql in turn.calls:
        if dataset == SourceKind.PROCESSES.dataset_id:
            assert _JULY in soql and _AUGUST not in soql
        else:
            assert _AUGUST in soql and _JULY not in soql

    # La respuesta rotula las dos cohortes por separado.
    assert "Procesos — Julio de 2026" in turn.response
    assert "Contratos — Agosto de 2026" in turn.response


def test_the_order_of_two_unambiguous_clauses_does_not_change_the_universe(frame_v12):
    first = Conversation("sem-order-a").say(
        f"analiza procesos publicados en julio y contratos firmados en agosto "
        f"de {BUYER}",
    )
    second = Conversation("sem-order-b").say(
        f"analiza contratos firmados en agosto y procesos publicados en julio "
        f"de {BUYER}",
    )
    assert sorted(first.sql) == sorted(second.sql)


# ── Pendiente separado de lo confirmado ────────────────────────────────────

def test_a_pending_never_touches_the_confirmed_frame(frame_v12):
    chat = Conversation("sem-pending")
    chat.say(f"analiza procesos de {BUYER} en julio")
    before = chat.profile_state().confirmed_frame

    chat.say("proveedores en agosto")
    state = chat.profile_state()
    assert state.has_pending
    assert state.confirmed_frame.fingerprint() == before.fingerprint()
    assert state.confirmed_frame.revision == before.revision


def test_a_valid_selection_keeps_the_focus_and_filters_it_proposed(frame_v12):
    chat = Conversation("sem-selection")
    chat.say(f"analiza procesos de {BUYER} en agosto")
    chat.say("proveedores")
    chosen = chat.say("solo contratos")
    frame = chat.profile_state().confirmed_frame
    assert frame.focus == "suppliers"
    assert frame.predicates_for(SourceKind.CONTRACTS).temporal.period.lower == _AUGUST
    assert chosen.datasets == (SourceKind.CONTRACTS.dataset_id,)


def test_an_ambiguous_yes_does_not_select_on_its_own(frame_v12):
    chat = Conversation("sem-yes")
    chat.say(f"analiza procesos de {BUYER} en agosto")
    asked = chat.say("proveedores")
    repeated = chat.say("sí")
    assert repeated.clarifies and repeated.calls == ()
    assert repeated.response == asked.response
    assert chat.profile_state().has_pending


# ── CLEAR durable ──────────────────────────────────────────────────────────

def test_a_cleared_period_is_not_resurrected_by_a_later_turn(frame_v12):
    chat = Conversation("sem-clear")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    chat.say("sin filtro de fecha")
    later = chat.say("proveedores")
    assert later.sql
    for soql in later.sql:
        assert _AUGUST not in soql
    period = chat.profile_state().confirmed_frame.predicates_for(
        SourceKind.CONTRACTS,
    ).temporal.period
    assert period.is_unbounded


# ── Cambio de fuente sin evento incompatible ───────────────────────────────

def test_a_source_switch_does_not_inherit_the_other_event(frame_v12):
    chat = Conversation("sem-switch")
    chat.say(f"analiza procesos publicados de {BUYER} en agosto")
    chat.say("proveedores")
    chat.say("solo contratos")
    contracts = chat.profile_state().confirmed_frame.predicates_for(
        SourceKind.CONTRACTS,
    )
    # La fuente que entra NO recibe el hecho de la que sale: usa su base propia.
    assert contracts.temporal.event is not TemporalEventKind.PROCESS_PUBLICATION
    for soql in chat.turns[-1].sql:
        assert "fecha_de_publicacion_del" not in soql
        assert "fecha_de_firma" in soql


# ── Params y trazas legacy sin autoridad ───────────────────────────────────

def test_tampering_with_legacy_params_changes_neither_frame_nor_sql(frame_v12):
    chat = Conversation("sem-legacy")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")

    store = ConversationStore()
    poisoned = {
        "entidad_resolved": "OTRA ENTIDAD",
        "fecha_desde": _JULY, "fecha_hasta": "2026-07-31",
        "_entity_profile_request": {"focus": "values", "sources": "PROCESSES"},
        "_entity_profile_state": {"version": 1, "confirmed": {"focus": "values"}},
        "objeto": ["pavimentacion"],
    }
    store.append_turn(
        chat_id="sem-legacy", user_query="ruido", response="ruido",
        parsed_params=poisoned,
    )

    followup = chat.say("proveedores")
    assert followup.route == "entity_profile_suppliers"
    for soql in followup.sql:
        assert _AUGUST in soql and _JULY not in soql
        assert "pavimentacion" not in soql
        assert "OTRA ENTIDAD" not in soql
    frame = chat.profile_state().confirmed_frame
    assert frame.buyer_scope.canonical_buyers == ("ALCALDIA MUNICIPAL DE BARANOA",)
    assert frame.focus == "suppliers"


def test_a_forged_execution_record_does_not_change_the_next_turn(frame_v12):
    chat = Conversation("sem-forged")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    store = ConversationStore()
    path = store._envelope_path_for("sem-forged")
    payload = json.loads(path.read_text().splitlines()[-1])
    payload["execution_record"]["focus"] = "values"
    payload["execution_record"]["expected_sections"] = ["values"]
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    followup = chat.say("proveedores")
    # El registro de ejecución acredita lo OBSERVADO; no gobierna el próximo
    # universo, que sale del Frame.
    assert followup.route == "entity_profile_suppliers"
    assert followup.planned_jobs == 2


# ── Fallo total y parcial ──────────────────────────────────────────────────

def test_a_total_source_failure_keeps_the_confirmed_intent(frame_v12):
    chat = Conversation("sem-fail")
    failed = chat.say(
        f"analiza contratos firmados de {BUYER} en agosto",
        client=FailingClient(),
    )
    assert not failed.clarifies
    assert "Agosto de 2026, según fecha de firma." in failed.response
    assert "no interpreto ese silencio" in failed.response.lower()
    # Ningún falso cero y ningún rescate tabular.
    assert "Se observan 0 contratos" not in failed.response
    assert failed.result.get("results") == []

    # El significado aceptado sobrevive al fallo de transporte...
    frame = chat.profile_state().confirmed_frame
    assert frame is not None and frame.focus == "overview"

    # ...y permite un refinamiento honesto después.
    recovered = chat.say("proveedores")
    assert not recovered.clarifies
    for soql in recovered.sql:
        assert _AUGUST in soql


def test_an_unreadable_amount_never_invalidates_its_count(frame_v12):
    chat = Conversation("sem-unreadable")
    turn = chat.say(
        f"analiza contratos firmados de {BUYER} en agosto",
        client=UnreadableValueClient(),
    )
    assert "4 contratos" in turn.response
    assert "$0" not in turn.response
    assert "no puedo afirmar un total" in turn.response.lower()


def test_a_partial_failure_names_what_is_missing_without_zeroing_it(frame_v12):
    from test_entity_profile_workflow import WorkflowClient

    chat = Conversation("sem-partial")
    turn = chat.say(
        f"analiza contratos firmados de {BUYER} en agosto",
        client=WorkflowClient(fail_supplier_value=True),
    )
    assert "no pude" in turn.response.lower()
    assert turn.result["partial"] is True


# ── Estructura del universo visible ────────────────────────────────────────

def test_an_incompatible_subject_and_event_clarifies_without_swapping_them(frame_v12):
    chat = Conversation("sem-incompatible")
    turn = chat.say(
        f"analiza contratos publicados y procesos firmados en agosto de {BUYER}",
    )
    assert turn.clarifies
    assert turn.calls == ()


def test_or_and_and_are_not_rendered_or_queried_alike(frame_v12):
    disjunction = Conversation("sem-or").say(
        f"analiza contratos de mantenimiento o vias de {BUYER}",
    )
    conjunction = Conversation("sem-and").say(
        f"analiza contratos de mantenimiento y vias de {BUYER}",
    )
    assert not disjunction.clarifies and not conjunction.clarifies
    assert " o " in disjunction.response
    assert disjunction.sql != conjunction.sql
    # El conector entre los DOS grupos de términos es lo que distingue ANY de
    # ALL; dentro de cada grupo siempre hay un OR entre columnas de texto.
    assert all(") OR (" in soql for soql in disjunction.sql)
    assert all(") AND (" in soql for soql in conjunction.sql)
    assert not any(") OR (" in soql for soql in conjunction.sql)
    assert "Tema: (mantenimiento o vias)." in disjunction.response
    assert "Tema: mantenimiento y vias." in conjunction.response


def test_a_modality_exclusion_stays_in_the_sql_and_is_declared(frame_v12):
    chat = Conversation("sem-exclusion")
    turn = chat.say(f"analiza contratos excepto contratacion directa de {BUYER}")
    assert not turn.clarifies
    assert "excepto" in turn.response.lower()
    assert any("NOT IN" in soql for soql in turn.sql)


def test_a_non_finite_metric_is_never_accredited(frame_v12):
    from app.core.entity_profile.frame_evidence import MetricStatus

    chat = Conversation("sem-nonfinite")
    chat.say(
        f"analiza contratos firmados de {BUYER} en agosto",
        client=UnreadableValueClient(),
    )
    record = chat.execution_record()
    statuses = {
        (call["call_id"], metric["name"]): metric["status"]
        for call in record["calls"] for metric in call["metrics"]
    }
    assert statuses, record
    unreadable = [
        key for key, status in statuses.items()
        if key[1] == "total_contract_value"
    ]
    assert unreadable
    for key in unreadable:
        assert statuses[key] == MetricStatus.INVALID.value
    # El conteo del mismo agregado SÍ queda acreditado.
    counts = [
        status for key, status in statuses.items() if key[1] == "contract_count"
    ]
    assert counts and all(status == MetricStatus.VALID.value for status in counts)


def test_planned_jobs_attempted_calls_and_metrics_are_three_different_numbers(
    frame_v12,
):
    chat = Conversation("sem-three-numbers")
    turn = chat.say(
        f"analiza contratos firmados de {BUYER} en agosto", client=FailingClient(),
    )
    timings = turn.result["timings_ms"]
    assert timings["entity_profile_planned_jobs"] == 4
    assert timings["entity_profile_calls_total"] == 4
    # Cuatro llamadas intentadas y CERO cifras acreditadas: un plan no prueba
    # que una llamada ocurriera, y una llamada no prueba una cifra.
    assert timings["entity_profile_accredited_metrics"] == 0
