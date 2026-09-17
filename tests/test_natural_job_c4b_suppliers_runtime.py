"""C4B — the accredited SUPPLIERS lane inside the real turn.

The eight mandatory cases of the C4B plan, with the residual verifier scripted by
the C4A.5/live measurements (no network) and the real witnesses for the structural
zeros: the QueryRouter and the legacy dispatcher are wrapped and delegating, and
the source is a stub that records every call.

Order under test: strong actor -> deterministic authorities -> residual -> B ->
weak actor exact guard. A veto from B never reaches the guard, and an accredited
turn that reaches READY compiles the Frame with ``focus = suppliers``.
"""
import json

import pytest

from app.config import get_settings
from app.core.accreditation import SUPPLIERS
from app.core.natural_alpha.provider import ProviderResult
from tests.test_natural_alpha_contract import payload
from tests.test_natural_alpha_runtime import make_flow, run  # noqa: F401
from tests.test_natural_job_credentialed_admission import counting_dispatch, counting_router

GALAPA_2025 = "con quien ha contratado la alcaldia de galapa en 2025"
BARRANQUILLA = "cuales son los proveedores con contratos mas grandes de alcaldia de barranquilla"
CALI_EDUCACION = "proveedores de educación de Secretaría de Educación de Cali"
GALAPA_LICITACION = "proveedores de licitación pública de Galapa"
GALAPA_MILLONES = "proveedores de Galapa por más de 500 millones"
OBRAS = "proveedores de galapa en obras civiles"
SOLEDAD = "¿Con qué empresas suele contratar la Alcaldía de Soledad?"
A01 = "cuanto le ha vendido fundacaribe a galapa"
A02 = "que le ha contratado la alcaldia de galapa a fundacaribe"
PARAPHRASES = (
    ("con quien ha contratado la alcaldia de galapa", "con quien ha contratado"),
    ("a quien le ha comprado mas el municipio de galapa", "a quien le ha comprado mas"),
    ("quien le vende mas a galapa", "quien le vende mas"),
)


def suppliers_payload(quote):
    return json.dumps({"jobs": [{
        "action": "ANALYZE", "aspect": "SUPPLIERS", "evidence_quote": quote, "occurrence": 1,
    }]})


class ScriptedVerifier:
    """B's frozen contract, scripted: one label per unit text, and every call recorded."""

    def __init__(self, labels):
        self.labels = labels
        self.calls = []

    def observe(self, call_key, request):
        body = json.loads(json.loads(request["input"])["text"].split("\n\nENTRADA\n", 1)[1])
        units = [line.split(":", 1)[1].strip().strip('"') for line in body["residual_units"]]
        self.calls.append(units)
        return ProviderResult(payload=json.dumps({"labels": [
            {"id": line.split(":", 1)[0],
             "label": self.labels if isinstance(self.labels, str)
             else self.labels.get(line.split(":", 1)[1].strip().strip('"'), "JOB_LANGUAGE")}
            for line in body["residual_units"]]}), provider_calls=1)


class FrameSource:
    """The Frame executes against this: no network, every call recorded, never errors."""

    timeout = 15

    def __init__(self):
        self.calls = []

    def with_timeout(self, timeout):
        return self

    derived_for_timeout = with_timeout

    def query(self, dataset, soql):
        self.calls.append((dataset, soql))
        return []

    aggregate = query

    def count(self, dataset, soql):
        self.calls.append((dataset, soql))
        return 0


@pytest.fixture
def frame_on(monkeypatch):
    def enable():
        monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true")
        get_settings.cache_clear()

    yield enable
    get_settings.cache_clear()


@pytest.fixture(scope="module")
def actor_index(tmp_path_factory):
    """A small index with the aliases the C4A.5 audit measured in production.

    ``fundacaribe`` is the real actor of A01/A02; ``obras`` and ``contratar`` are
    the collisions that audit recorded (a real alias that is also material
    vocabulary). With them in the index, "the weak guard was not called" is a
    claim with teeth: were the order reversed, the guard would fire.
    """
    from app.data.actor_alias_index import build_actor_alias_index

    path = tmp_path_factory.mktemp("c4b-actors") / "actors.sqlite"
    build_actor_alias_index(
        [
            {"tipodocproveedor": "NIT", "documento_proveedor": "900123456",
             "proveedor_adjudicado": "FUNDACARIBE", "occurrences": 12,
             "first_seen": "2023-01-01", "last_seen": "2026-01-01"},
            {"tipodocproveedor": "NIT", "documento_proveedor": "900777111",
             "proveedor_adjudicado": "OBRAS", "occurrences": 4,
             "first_seen": "2023-01-01", "last_seen": "2026-01-01"},
            {"tipodocproveedor": "NIT", "documento_proveedor": "900777222",
             "proveedor_adjudicado": "CONTRATAR", "occurrences": 3,
             "first_seen": "2023-01-01", "last_seen": "2026-01-01"},
        ],
        path, source_dataset="c4b-fixture", built_at="2026-09-17T12:00:00+00:00",
    )
    return path


def accredited_flow(make_flow, frame_on, quote, *, labels="JOB_LANGUAGE", actor_index=None):
    flow = make_flow("alpha", ProviderResult(suppliers_payload(quote), provider_calls=1))
    flow.settings.secoppal_a2_suppliers_binding_enabled = True
    frame_on()
    flow.secop_client = FrameSource()
    verifier = ScriptedVerifier(labels)
    flow.residual_verifier_provider = verifier
    if actor_index is not None:
        from app.data.actor_alias_resolver import ActorNameResolver

        flow.actor_name_resolver = ActorNameResolver(actor_index)
    return flow, verifier


# ── 1-2: B called with material residual, and zero calls without it ───────────

def test_galapa_2025_calls_b_then_executes_the_suppliers_frame(frame_on, make_flow):
    flow, verifier = accredited_flow(make_flow, frame_on, "con quien ha contratado")
    router_calls = counting_router(flow)
    dispatch_calls = counting_dispatch(flow)

    result = run(flow, GALAPA_2025, chat_id="c4b-x01")

    assert result["authority_status"] == "READY"
    assert result["admission_reason"] == "accredited"
    assert result["residual_verifier_called"] is True
    assert result["residual_verifier_status"] == "OK"
    assert verifier.calls == [["contratado"]]
    assert result["residual_units"] == [{"id": "R1", "text": "contratado"}]
    assert result["residual_labels"] == {"R1": "JOB_LANGUAGE"}
    assert result["weak_actor_guard_called"] is True
    # The accredited turn runs the existing Frame, and the focus is the credential's.
    assert result["route_reason"] == "entity_profile_suppliers"
    assert result["entity_profile_view"]["focus"] == "suppliers"
    assert result["needs_clarification"] is False
    assert flow.secop_client.calls, "READY executes"
    assert router_calls == [] and dispatch_calls == []
    assert result["query_router_calls"] == 0 and result["legacy_bypassed"] is True


def test_barranquilla_has_no_residual_and_never_calls_b(frame_on, make_flow):
    flow, verifier = accredited_flow(make_flow, frame_on, "cuales son los proveedores")

    result = run(flow, BARRANQUILLA, chat_id="c4b-x02")

    assert result["authority_status"] == "READY"
    assert verifier.calls == [], "an empty residual is zero calls"
    assert result["residual_verifier_called"] is False
    assert result["residual_verifier_status"] == "NOT_CALLED"
    assert result["residual_units"] == [] and result["residual_labels"] == {}
    assert result["route_reason"] == "entity_profile_suppliers"
    assert flow.secop_client.calls


# ── 3: B sees the material and the turn stops ────────────────────────────────

def test_cali_education_is_material_and_stops_the_turn(frame_on, make_flow):
    # Measured in the C4A.5 transfer and re-measured in the C4B replay: "educación" is MATERIAL.
    flow, verifier = accredited_flow(make_flow, frame_on, "proveedores",
                                     labels={"educación": "MATERIAL"})
    router_calls = counting_router(flow)

    result = run(flow, CALI_EDUCACION, chat_id="c4b-x03")

    assert result["authority_status"] == "CLARIFY"
    assert result["route_reason"] == "admission_UNACCREDITED_RESIDUAL"
    assert result["residual_labels"] == {"R1": "MATERIAL"}
    assert result["weak_actor_guard_called"] is False
    assert result["needs_clarification"] is True
    assert verifier.calls == [["educación"]]
    assert flow.secop_client.calls == [] and router_calls == []


# ── 4-5: a deterministic guard stops the turn before B is asked ──────────────

@pytest.mark.parametrize("raw,quote,reason", [
    (GALAPA_LICITACION, "proveedores", "UNSUPPORTED_MODALITY"),
    (GALAPA_MILLONES, "proveedores", "UNSUPPORTED_MONEY"),
])
def test_deterministic_guards_stop_the_turn_without_calling_b(frame_on, make_flow, raw, quote,
                                                              reason):
    flow, verifier = accredited_flow(make_flow, frame_on, quote)

    result = run(flow, raw, chat_id=f"c4b-{reason}")

    assert result["authority_status"] == "CLARIFY"
    assert result["authority_reason"] == reason
    assert verifier.calls == [], "B is only asked at the residual point"
    assert result["residual_verifier_called"] is False
    assert result["weak_actor_guard_called"] is False
    assert flow.secop_client.calls == []


# ── 6: actor subject, both protection branches, never READY ──────────────────

@pytest.mark.parametrize("raw,quote,labels", [
    # Measured live in the C4B replay: the actor unit is UNKNOWN to B.
    (A01, "le ha vendido", {"fundacaribe": "UNKNOWN"}),
    # Measured live (run 0): B sees MATERIAL in the actor unit.
    (A02, "le ha contratado", {"fundacaribe": "MATERIAL"}),
    # Measured live (run 1): B approves both units, and the weak guard catches it.
    (A02, "le ha contratado", {}),
])
def test_actor_subject_never_reaches_ready(frame_on, make_flow, actor_index, raw, quote, labels):
    flow, verifier = accredited_flow(make_flow, frame_on, quote, labels=labels,
                                     actor_index=actor_index)
    router_calls = counting_router(flow)

    result = run(flow, raw, chat_id=f"c4b-actor-{abs(hash((raw, str(labels))))}")

    assert result.get("authority_status") != "READY"
    assert result["needs_clarification"] is True
    assert result["authority_reason"] in ("UNACCREDITED_RESIDUAL", "ACTOR_MENTION_PRESENT")
    assert verifier.calls == [[unit for unit in ("vendido", "contratado") if unit in raw]
                              + ["fundacaribe"]]
    assert flow.secop_client.calls == [] and router_calls == []
    if result["authority_reason"] == "ACTOR_MENTION_PRESENT":
        assert result["weak_actor_guard_called"] is True
    else:
        assert result["weak_actor_guard_called"] is False


# ── 7-8: the two false blocks the reordering removes ─────────────────────────

def test_obras_civiles_is_blocked_by_material_not_by_the_weak_guard(frame_on, make_flow,
                                                                   actor_index):
    flow, verifier = accredited_flow(make_flow, frame_on, "proveedores",
                                     labels={"obras": "MATERIAL", "civiles": "MATERIAL"},
                                     actor_index=actor_index)

    result = run(flow, OBRAS, chat_id="c4b-d01")

    assert result["authority_status"] == "CLARIFY"
    assert result["authority_reason"] == "UNACCREDITED_RESIDUAL"
    assert "obras" in result["residual_material"] and "civiles" in result["residual_material"]
    assert result["weak_actor_guard_called"] is False, (
        "the alias 'obras' is in the index: the guard would have fired had it been asked")
    assert verifier.calls == [["obras", "civiles"]]
    assert flow.secop_client.calls == []


def test_suele_is_unknown_and_the_weak_guard_is_not_asked(frame_on, make_flow, actor_index):
    flow, verifier = accredited_flow(make_flow, frame_on, "empresas",
                                     labels={"suele": "UNKNOWN"}, actor_index=actor_index)

    result = run(flow, SOLEDAD, chat_id="c4b-h05")

    assert result["authority_status"] == "CLARIFY"
    assert result["authority_reason"] == "UNACCREDITED_RESIDUAL"
    assert result["residual_labels"]["R2"] == "UNKNOWN"
    assert result["weak_actor_guard_called"] is False, (
        "the alias 'contratar' is in the index: the guard would have fired had it been asked")
    assert verifier.calls == [["empresas", "suele", "contratar"]]
    assert flow.secop_client.calls == []


# ── the invariant and the structural zeros ───────────────────────────────────

@pytest.mark.parametrize("raw,quote", PARAPHRASES)
def test_an_accredited_suppliers_turn_compiles_focus_suppliers(frame_on, make_flow, raw, quote):
    flow, verifier = accredited_flow(make_flow, frame_on, quote)

    result = run(flow, raw, chat_id=f"c4b-focus-{len(raw)}")

    assert result["authority_status"] == "READY", result.get("authority_reason")
    assert verifier.calls, "these paraphrases have a material unit"
    assert result["route_reason"] == "entity_profile_suppliers"
    assert result["entity_profile_view"]["focus"] == "suppliers"
    assert result["natural_alpha_jobs"][0]["aspect"] == "SUPPLIERS"


def test_a_ready_credentialed_turn_never_falls_back_to_the_legacy_chain(
        frame_on, make_flow, monkeypatch):
    """Without a profile route the credentialed turn clarifies; it never dispatches."""
    from app.core.workflow import profile_route

    flow, verifier = accredited_flow(make_flow, frame_on, "con quien ha contratado")
    router_calls = counting_router(flow)
    dispatch_calls = counting_dispatch(flow)
    monkeypatch.setattr(profile_route, "run_profile_route", lambda *args, **kwargs: None)

    result = run(flow, GALAPA_2025, chat_id="c4b-no-route")

    assert result["route_reason"] == "admission_accredited_profile_route_unavailable"
    assert result["needs_clarification"] is True
    assert result["authority_status"] == "READY", "the assembly was not the problem"
    assert router_calls == [] and dispatch_calls == []
    assert flow.secop_client.calls == []


def test_a_non_credentialed_turn_keeps_its_legacy_lane(frame_on, make_flow):
    """The door is the credential, not the route: without it nothing changes."""
    flow, verifier = accredited_flow(make_flow, frame_on, "con quien ha contratado")
    flow.natural_alpha_provider.observe.return_value = ProviderResult(
        payload(action="OTHER", aspect="OTHER"), provider_calls=1)
    router_calls = counting_router(flow)

    result = run(flow, GALAPA_2025, chat_id="c4b-legacy")

    assert "credential_capability" not in result
    assert verifier.calls == []
    assert router_calls, "the legacy parser still runs without a credential"


def test_the_accredited_capability_is_suppliers(frame_on, make_flow):
    flow, _verifier = accredited_flow(make_flow, frame_on, "con quien ha contratado")

    result = run(flow, GALAPA_2025, chat_id="c4b-capability")

    assert result["credential_capability"] == SUPPLIERS
    assert result["credential_source"] == "NATURAL_ALPHA"
    assert result["admission_owner"] == "ENTITY_PROFILE"
    assert len(flow.feedback.get_recent()) == 1


# ── C4B-R5: a provider that raises is contained, never propagated ────────────

def test_a_provider_that_raises_ends_the_turn_in_clarify(frame_on, make_flow):
    class Exploding:
        def observe(self, call_key, request):
            raise RuntimeError("boom")

    flow = make_flow("alpha", ProviderResult(suppliers_payload("con quien ha contratado"),
                                             provider_calls=1))
    flow.settings.secoppal_a2_suppliers_binding_enabled = True
    frame_on()
    flow.secop_client = FrameSource()
    flow.residual_verifier_provider = Exploding()
    router_calls = counting_router(flow)
    dispatch_calls = counting_dispatch(flow)

    result = run(flow, GALAPA_2025, chat_id="c4b-exploding")

    assert result["authority_status"] == "CLARIFY"
    assert result["authority_reason"] == "UNACCREDITED_RESIDUAL"
    assert result["residual_verifier_called"] is True
    assert result["residual_verifier_status"] == "PROVIDER_ERROR:EXCEPTION"
    assert result["weak_actor_guard_called"] is False
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []
    assert router_calls == [] and dispatch_calls == []
