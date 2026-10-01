"""M1 wave 1 R1 (ZEUS P1-1): every consumer of the temporal authority either applies
the period the authority resolved, exactly, or asks. Never another year, never no date.

D-TIME made ``resolve_temporal_scope`` resolve forms that used to go unrecognised
(open bounds «antes/después de», «a partir de», «a la fecha», relative windows
«últimos 3 años», an inferred year, quarters). A consumer that read only a year
with its own regex, or that was saved by the leftover words of an unrecognised
phrase, started executing «antes de 2024» as 2024 and «últimos 3 años» with no date.

The oracle (``FORMS``) is written by hand from the Spanish meaning, with the civil
date fixed at 2026-10-01; it never calls the implementation to compute an expected
value. Consumers covered here: query_router / dialogue_policy (params), Contract
Activity (legacy reader and the authority-bound filters), SUPPLIERS A2 (decision and
final SoQL), Entity Profile (scope, claims, rendering), the accreditation time
authority, the SoQL builder, the actor-profile scope and the follow-up merge.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from app.core.accreditation.authorities import time_authority
from app.core.actor_profile.scope import build_actor_profile_scope
from app.core.actor_profile.scope_render import render_actor_profile_scope
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.filters import accredit_filters
from app.core.analytics.contract_activity.models import AdmissionStatus, Operation
from app.core.analytics.contract_activity.planning import build_plan
from app.core.capability_binding.contract_activity_binding import ContractActivityBinding
from app.core.capability_binding.supplier_contract import Outcome, decide
from app.core.entity_profile.claim_adapters import collect_claims
from app.core.entity_profile.claims import ClaimDimension
from app.core.entity_profile.effective_scope import humanize_period
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_resolver import EntityResolver
from app.core.followup_engine import _merge_modifiers
from app.core.natural_alpha.models import GroundedJob
from app.core.period_consistency import (
    PERIOD_AMBIGUOUS,
    PERIOD_NOT_REPRESENTED,
    TEMPORAL_UNRESOLVED,
    period_conflict,
)
from app.core.query_router import QueryRouter
from app.core.response_policy import _format_temporal_scope
from app.core.soql_builder import SoQLBuilder

TODAY = date(2026, 10, 1)
NONE = None

# (label, text, (fecha_desde, fecha_hasta)); None = open bound.  Hand-written.
FORMS = (
    ("year", "en 2025", ("2025-01-01", "2025-12-31")),
    ("month", "en marzo de 2025", ("2025-03-01", "2025-03-31")),
    ("month_del", "septiembre del 2025", ("2025-09-01", "2025-09-30")),
    ("day_range", "del 1 al 15 de marzo de 2025", ("2025-03-01", "2025-03-15")),
    ("quarter", "primer trimestre de 2025", ("2025-01-01", "2025-03-31")),
    ("semester", "segundo semestre de 2025", ("2025-07-01", "2025-12-31")),
    ("before", "antes de 2024", (NONE, "2023-12-31")),
    ("after", "después de 2022", ("2023-01-01", NONE)),
    ("from_partir", "a partir de 2023", ("2023-01-01", NONE)),
    ("from_adelante", "2023 en adelante", ("2023-01-01", NONE)),
    ("from_fecha", "de 2024 a la fecha", ("2024-01-01", NONE)),
    ("from_hoy", "desde 2024 hasta hoy", ("2024-01-01", NONE)),
    ("relative_years", "en los últimos 3 años", ("2023-10-01", "2026-10-01")),
    ("relative_months", "en los últimos 6 meses", ("2026-04-01", "2026-10-01")),
    ("relative_ago_years", "hace 2 años", ("2024-01-01", "2024-12-31")),
    ("relative_week", "la última semana", ("2026-09-24", "2026-10-01")),
    ("inferred_year", "en marzo", ("2026-03-01", "2026-03-31")),
)
FORM_IDS = [label for label, _, _ in FORMS]
# The only forms the Contract Activity legacy reader can represent exactly: it reads
# one written year or one written month-year.  Everything else must ask.
CA_EXACT = {"year", "month"}
BUYER = "la Alcaldía de Galapa"


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _parse(router: QueryRouter, text: str):
    return router.parse(text, today=TODAY)


def _period(params: dict) -> tuple[str | None, str | None]:
    return params.get("fecha_desde") or None, params.get("fecha_hasta") or None


def _binding(raw: str, aspect: str = "CONTRACT_COUNT") -> ContractActivityBinding:
    job = GroundedJob("ANALYZE", aspect, 0, len(raw))
    binding = ContractActivityBinding.from_job(job)
    assert binding is not None
    return binding


# ── the parse (query_router + dialogue_policy): the reference every consumer reads ──

@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
@pytest.mark.parametrize("template", [
    "contratos de {buyer} {t}",
    "proveedores de {buyer} {t}",
    "procesos abiertos de la Gobernación del Atlántico {t}",
])
def test_the_parse_carries_exactly_the_period_the_spanish_says(router, label, text, expected, template):
    query = template.format(buyer=BUYER, t=text)
    assert _period(_parse(router, query).params) == expected, query


# ── Contract Activity ─────────────────────────────────────────────────────────────

CA_TEMPLATES = (
    "¿Cuántos contratos firmó {buyer} {t}?",
    "Desglosa por modalidad los contratos firmados por {buyer} {t}",
)


def _ca_legacy(router: QueryRouter, raw: str):
    parsed = _parse(router, raw)
    return admit_contract_activity(
        raw, dict(parsed.params), today=TODAY, semantic_evidence=parsed.semantic_evidence.to_state(),
    )


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
@pytest.mark.parametrize("template", CA_TEMPLATES)
def test_contract_activity_legacy_reader_executes_the_resolved_period_or_asks(
    router, label, text, expected, template,
):
    raw = template.format(buyer=BUYER, t=text)
    admission = _ca_legacy(router, raw)
    if admission.status is AdmissionStatus.READY:
        request = admission.request
        assert (request.date_from, request.date_to) == expected, raw
        assert None not in expected, raw
    if label not in CA_EXACT:
        assert admission.status is AdmissionStatus.CLARIFY, (raw, admission.status)
        assert admission.request is None


@pytest.mark.parametrize("label,text,expected", [f for f in FORMS if f[0] in CA_EXACT], ids=sorted(CA_EXACT, key=str))
def test_contract_activity_legacy_reader_still_executes_the_exact_year_and_month(router, label, text, expected):
    raw = f"¿Cuántos contratos firmó {BUYER} {text}?"
    admission = _ca_legacy(router, raw)
    assert admission.status is AdmissionStatus.READY, (raw, admission.reason)
    plan = build_plan(admission.request)
    assert (admission.request.date_from, admission.request.date_to) == expected
    assert f"fecha_de_firma >= '{expected[0]}' AND fecha_de_firma <= '{expected[1]}'" in plan.where


def test_zeus_p1_1_probe_antes_de_2024_is_not_executed_as_2024(router):
    admission = _ca_legacy(router, f"¿Cuántos contratos firmó {BUYER} antes de 2024?")
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.request is None


def test_zeus_p1_1_probe_ultimos_3_anios_is_not_executed_without_a_date(router):
    admission = _ca_legacy(router, f"¿Cuántos contratos firmó {BUYER} en los últimos 3 años?")
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.request is None


def test_a_parse_that_carries_another_period_than_the_written_year_is_asked(router):
    raw = f"¿Cuántos contratos firmó {BUYER} en 2025?"
    parsed = _parse(router, raw)
    bare = {k: v for k, v in parsed.params.items() if k not in ("fecha_desde", "fecha_hasta")}
    for tampered in (
        {"fecha_desde": "2024-01-01", "fecha_hasta": "2024-12-31"},
        {"fecha_desde": "2025-01-01"},
        {"fecha_hasta": "2025-12-31"},
    ):
        admission = admit_contract_activity(
            raw, {**bare, **tampered}, today=TODAY,
            semantic_evidence=parsed.semantic_evidence.to_state(),
        )
        assert admission.status is AdmissionStatus.CLARIFY, tampered
        assert admission.reason.startswith("SIGNATURE_PERIOD"), (tampered, admission.reason)


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_contract_activity_asks_on_its_own_even_if_the_parse_dropped_the_period(router, label, text, expected):
    """The text-vs-authority check does not lean on the parse: with no dates in params it still asks."""
    raw = f"¿Cuántos contratos firmó {BUYER} {text}?"
    parsed = _parse(router, raw)
    bare = {k: v for k, v in parsed.params.items() if k not in ("fecha_desde", "fecha_hasta")}
    admission = admit_contract_activity(
        raw, bare, today=TODAY, semantic_evidence=parsed.semantic_evidence.to_state(),
    )
    if label in CA_EXACT:
        assert admission.status is AdmissionStatus.READY
        assert (admission.request.date_from, admission.request.date_to) == expected
    else:
        assert admission.status is AdmissionStatus.CLARIFY, (raw, admission.reason)
        assert admission.request is None


def test_a_temporal_error_flagged_by_the_parse_is_asked(router):
    raw = f"¿Cuántos contratos firmó {BUYER} en 2025?"
    parsed = _parse(router, raw)
    admission = admit_contract_activity(
        raw, {**parsed.params, "_temporal_error": "invalid_date"}, today=TODAY,
        semantic_evidence=parsed.semantic_evidence.to_state(),
    )
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.reason == "SIGNATURE_PERIOD_UNRESOLVED"


def test_a_period_carried_by_the_parse_without_one_in_the_text_is_asked_not_dropped(router):
    raw = f"¿Cuántos contratos firmó {BUYER}?"
    parsed = _parse(router, raw)
    admission = admit_contract_activity(
        raw, {**parsed.params, "fecha_desde": "2025-01-01", "fecha_hasta": "2025-12-31"},
        today=TODAY, semantic_evidence=parsed.semantic_evidence.to_state(),
    )
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.reason == "SIGNATURE_PERIOD_AUTHORITY_CONFLICT"


def test_two_periods_in_one_turn_are_ambiguous_not_the_first_one(router):
    raw = f"¿Cuántos contratos firmó {BUYER} en 2025 y en los últimos 3 años?"
    admission = _ca_legacy(router, raw)
    assert admission.status is AdmissionStatus.CLARIFY


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_contract_activity_bound_filters_represent_the_whole_range_or_ask(router, label, text, expected):
    raw = f"¿Cuántos contratos firmó {BUYER} {text}?"
    parsed = _parse(router, raw)
    filters = accredit_filters(raw, dict(parsed.params), Operation.TOTAL, today=TODAY)
    if filters.ok:
        assert (filters.date_from, filters.date_to) == expected, raw
        assert None not in expected
    elif None in expected:
        assert filters.reason, raw


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_contract_activity_with_a_binding_never_executes_another_period(router, label, text, expected):
    raw = f"¿Cuántos contratos firmó {BUYER} {text}?"
    parsed = _parse(router, raw)
    admission = admit_contract_activity(
        raw, dict(parsed.params), binding=_binding(raw), today=TODAY,
        semantic_evidence=parsed.semantic_evidence.to_state(),
    )
    if admission.status is AdmissionStatus.READY:
        assert (admission.request.date_from, admission.request.date_to) == expected, raw
        assert None not in expected


_CA_KINDS = {
    "year": (lambda y: f"en {y}", lambda y: (f"{y}-01-01", f"{y}-12-31")),
    "month": (lambda y: f"en marzo de {y}", lambda y: (f"{y}-03-01", f"{y}-03-31")),
    "before": (lambda y: f"antes de {y}", lambda y: (None, f"{y - 1}-12-31")),
    "after": (lambda y: f"después de {y}", lambda y: (f"{y + 1}-01-01", None)),
    "from": (lambda y: f"a partir de {y}", lambda y: (f"{y}-01-01", None)),
    "onward": (lambda y: f"{y} en adelante", lambda y: (f"{y}-01-01", None)),
    "until": (lambda y: f"hasta {y}", lambda y: (None, f"{y}-12-31")),
    "since": (lambda y: f"desde {y}", lambda y: (f"{y}-01-01", None)),
}


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(kind=st.sampled_from(sorted(_CA_KINDS)), year=st.integers(2005, 2025),
       template=st.sampled_from(CA_TEMPLATES))
def test_property_contract_activity_never_runs_a_period_other_than_the_resolved_one(router, kind, year, template):
    write, oracle = _CA_KINDS[kind]
    raw = template.format(buyer=BUYER, t=write(year))
    expected = oracle(year)
    admission = _ca_legacy(router, raw)
    if admission.status is AdmissionStatus.READY:
        assert (admission.request.date_from, admission.request.date_to) == expected, raw
    if None in expected:
        assert admission.status is AdmissionStatus.CLARIFY, raw


# ── SUPPLIERS A2 ──────────────────────────────────────────────────────────────────

A2_TEMPLATES = ("proveedores de {buyer} {t}", "5 proveedores de {buyer} {t}")


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
@pytest.mark.parametrize("template", A2_TEMPLATES)
def test_suppliers_executes_the_resolved_period_exactly_or_asks(router, label, text, expected, template):
    query = template.format(buyer=BUYER, t=text)
    decision = decide(query, dict(_parse(router, query).params), today=TODAY)
    if decision.outcome is Outcome.EXECUTE:
        assert _period(decision.admitted_params) == expected, query
    else:
        assert decision.outcome is Outcome.CLARIFY, (query, decision.outcome)


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_suppliers_final_soql_carries_exactly_the_resolved_bounds(router, label, text, expected):
    """A2 evidence: the SoQL that would be sent applies both bounds, or the one that exists."""
    query = f"proveedores de {BUYER} {text}"
    decision = decide(query, dict(_parse(router, query).params), today=TODAY)
    assert decision.outcome is Outcome.EXECUTE, (query, decision.reason)
    soql = _suppliers_soql(decision.admitted_params, query)
    assert soql, query
    desde, hasta = expected
    for statement in soql:
        assert (f"fecha_de_firma >= '{desde}'" in statement) is (desde is not None), (query, statement)
        assert (f"fecha_de_firma <= '{hasta}'" in statement) is (hasta is not None), (query, statement)


def _suppliers_soql(admitted: dict, query: str) -> list[str]:
    from app.core.entity_profile.execution_plan import build_execution_plan
    from app.core.entity_profile.query_runner import execute_jobs
    from app.core.entity_profile.semantic_request import build_semantic_request
    from app.core.entity_profile.turn import profile_query_params
    from test_entity_profile_queries import FakeClient

    params = profile_query_params(admitted)
    scope = resolve_profile_scope(params).scope
    assert scope is not None
    request = build_semantic_request(user_query=query, params=params, scope=scope, focus="suppliers")
    carried = {k: v for k, v in admitted.items() if k.startswith(("supplier", "ranking"))}
    plan = build_execution_plan(request, {**params, **carried})
    assert plan.clarification is None, plan.clarification
    _, calls = execute_jobs(plan.jobs, FakeClient(), SoQLBuilder())
    return [call.soql for call in calls.values()]


@pytest.mark.parametrize("tampered", [
    {"fecha_desde": "2024-01-01", "fecha_hasta": "2024-12-31"},   # the year of «antes de 2024»
    {"fecha_desde": "2024-01-01"},                               # the open end dropped
    {},                                                          # the whole period dropped
])
def test_suppliers_asks_when_the_parse_period_is_not_the_one_in_the_text(router, tampered):
    query = f"proveedores de {BUYER} antes de 2024"
    params = {k: v for k, v in _parse(router, query).params.items() if k not in ("fecha_desde", "fecha_hasta")}
    decision = decide(query, {**params, **tampered}, today=TODAY)
    assert decision.outcome is Outcome.CLARIFY
    assert decision.reason == "PERIOD_NOT_REPRESENTED"


def test_suppliers_still_executes_without_a_period_when_the_text_names_none(router):
    query = f"proveedores de {BUYER}"
    decision = decide(query, dict(_parse(router, query).params), today=TODAY)
    assert decision.outcome is Outcome.EXECUTE
    assert _period(decision.admitted_params) == (None, None)


# ── Entity Profile (scope, claims, rendering) ────────────────────────────────────

@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_entity_profile_scope_and_rendering_keep_both_bounds(router, label, text, expected):
    query = f"perfil de {BUYER} {text}"
    params = _parse(router, query).params
    scope = resolve_profile_scope(params).scope
    assert scope is not None
    assert (scope.time_from, scope.time_to) == expected, query
    said = humanize_period(scope.time_from, scope.time_to)
    desde, hasta = expected
    if desde is None:
        assert said == f"hasta {hasta}"
    elif hasta is None:
        assert said == f"desde {desde}"
    else:
        assert (desde in said and hasta in said) or said == str(desde[:4]) or "del 1 al" in said, said


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_entity_profile_claims_carry_the_resolved_period(router, label, text, expected):
    query = f"contratos de {BUYER} {text}"
    claims = collect_claims(query, dict(_parse(router, query).params), turn_id="t1", today=TODAY)
    periods = [c for c in claims.claims if c.dimension is ClaimDimension.PERIOD]
    assert len(periods) == 1, query
    value = periods[0].canonical_value
    assert (value.lower, value.upper) == expected, query


@pytest.mark.parametrize("text,said", [
    ("antes de 2024", "hasta 2023-12-31"),
    ("después de 2022", "desde 2023-01-01"),
    ("en los últimos 3 años", "del 2023-10-01 al 2026-10-01"),
    ("de 2024 a la fecha", "desde 2024-01-01"),
])
def test_the_profile_the_user_reads_states_the_period_it_applied(router, text, said):
    """End to end (fake source): the rendered answer names the open bound or window, never «histórico»."""
    from app.core.entity_profile.assembler import build_entity_profile
    from app.core.entity_profile.render import render_entity_profile
    from app.core.entity_profile.turn import classify_entity_profile_turn
    from test_entity_profile_queries import FakeClient

    query = f"perfil de {BUYER} {text}"
    turn = classify_entity_profile_turn(query, _parse(router, query).params)
    assert turn.plan is not None and not turn.clarification
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params, focus=turn.focus,
        secop_client=FakeClient(), soql_builder=SoQLBuilder(), plan=turn.plan,
    )
    rendered = render_entity_profile(profile)
    assert said in rendered, rendered
    assert "todo el histórico" not in rendered


# ── accreditation, SoQL, actor profile, follow-up ────────────────────────────────

@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_the_accreditation_time_authority_reports_exactly_the_resolved_bounds(label, text, expected):
    report = time_authority(f"contratos de {BUYER} {text}", today=TODAY)
    assert dict(report.params) == {
        key: value for key, value in zip(("fecha_desde", "fecha_hasta"), expected) if value
    }


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_the_soql_builder_applies_each_existing_bound_and_only_those(label, text, expected):
    desde, hasta = expected
    params = {"entidad_resolved": "MUNICIPIO DE GALAPA.", "fecha_desde": desde, "fecha_hasta": hasta}
    where = SoQLBuilder()._build_where(SoQLBuilder.SPECS["jbjy-vk9h"], {k: v for k, v in params.items() if v})
    assert (f"fecha_de_firma >= '{desde}'" in where) is (desde is not None)
    assert (f"fecha_de_firma <= '{hasta}'" in where) is (hasta is not None)


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_response_policy_names_each_existing_bound(label, text, expected):
    desde, hasta = expected
    said = _format_temporal_scope(desde or "", hasta or "")
    assert said
    for bound in (desde, hasta):
        if bound:
            assert bound[:4] in said or bound[:7] in said or bound[8:] in said, (said, bound)


@pytest.mark.parametrize("label,text,expected", FORMS, ids=FORM_IDS)
def test_the_actor_profile_scope_keeps_and_renders_the_period(label, text, expected):
    params = {"fecha_desde": expected[0], "fecha_hasta": expected[1]}
    scope = build_actor_profile_scope(params)
    assert (scope.date_from, scope.date_to) == expected
    rendered = render_actor_profile_scope(scope)
    for bound in expected:
        if bound:
            assert bound[:4] in rendered or bound in rendered


@pytest.mark.parametrize("label,text,expected", [f for f in FORMS if None in f[2]], ids=lambda x: x if isinstance(x, str) and "-" not in x else None)
def test_a_new_open_scope_replaces_both_bounds_of_the_previous_turn_atomically(label, text, expected):
    previous = {"fecha_desde": "2020-01-01", "fecha_hasta": "2020-12-31", "entidad_resolved": "X"}
    current = {k: v for k, v in zip(("fecha_desde", "fecha_hasta"), expected) if v}
    merged = _merge_modifiers(previous, {**current, "_temporal_replace_scope": True})
    assert _period(merged) == expected


# ── the shared guard itself ─────────────────────────────────────────────────────

def test_period_conflict_names_the_three_disagreements():
    assert period_conflict("contratos antes de 2024", ("2024-01-01", "2024-12-31"), today=TODAY) == PERIOD_NOT_REPRESENTED
    assert period_conflict("contratos antes de 2024", (None, "2023-12-31"), today=TODAY) is None
    assert period_conflict("contratos en los últimos 3 años", (None, None), today=TODAY) == PERIOD_NOT_REPRESENTED
    assert period_conflict("contratos del 30 de febrero de 2025", (None, None), today=TODAY) == TEMPORAL_UNRESOLVED
    assert period_conflict("contratos de la alcaldia", (None, None), today=TODAY) is None


@pytest.mark.parametrize("represented", [
    ("2024-01-01", "2023-12-31"),   # a wrong lower bound, right upper
    (None, "2024-12-31"),           # right lower (open), wrong upper
    ("2023-01-01", "2023-12-31"),   # both present, right upper, invented lower
    ("2023-12-31", None),           # lower bound taken from the upper
])
def test_period_conflict_compares_each_bound_on_its_own(represented):
    assert period_conflict("contratos antes de 2024", represented, today=TODAY) == PERIOD_NOT_REPRESENTED
    assert period_conflict("contratos desde 2024 a la fecha", ("2024-01-01", None), today=TODAY) is None


@pytest.mark.parametrize("represented,text", [
    (("2025-01-01", "2025-12-31"), "contratos en 2025 y en 2024"),
    # R2: a relative WINDOW plus another period is now an error of the authority itself
    # (relative_window_explicit_anchor); a calendar period like «el mes pasado» is not a window.
    (("2026-09-01", "2026-09-30"), "contratos el mes pasado y en 2025"),
    (("2025-03-01", "2025-03-31"), "contratos en marzo de 2025 y en abril de 2025"),
])
def test_a_second_period_in_the_text_is_ambiguous_even_if_the_first_is_represented_exactly(represented, text):
    assert period_conflict(text, represented, today=TODAY) in {PERIOD_AMBIGUOUS, PERIOD_NOT_REPRESENTED}
    # whichever the authority reads first, the consumer that applied one of them must be asked
    assert period_conflict(text, represented, today=TODAY) is not None


def test_a_second_period_survives_the_first_one_only_when_the_first_is_the_represented_one():
    first = "contratos en 2025 y en 2024"
    assert period_conflict(first, ("2025-01-01", "2025-12-31"), today=TODAY) == PERIOD_AMBIGUOUS
