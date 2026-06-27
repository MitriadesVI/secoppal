"""Etapa 4 — el Turn como ExecutionSnapshot: outcome persistido + last_successful_turn."""
from __future__ import annotations

from app.core.conversation_store import ConversationStore
from app.core.outcome import Outcome, OutcomeReason


def _store(tmp_path):
    return ConversationStore(base_path=tmp_path / "conv")


def test_turn_persists_outcome_roundtrip(tmp_path):
    store = _store(tmp_path)
    store.append_turn(
        chat_id="c1", user_query="ascensores en medellin", response="...",
        parsed_params={"objeto": ["ascensores"], "ciudad": "Medellin"},
        result_ids=["CO1.NTC.1"], outcome=Outcome.SUCCESS,
        outcome_reason=OutcomeReason.RESULTS_FOUND, result_count=25,
        query_fingerprint="abc123",
    )
    turn = store.get_history("c1", last_n=1)[0]
    assert turn.outcome == Outcome.SUCCESS
    assert turn.result_count == 25
    assert turn.query_fingerprint == "abc123"
    assert turn.had_results() is True


def test_empty_turn_did_not_have_results(tmp_path):
    store = _store(tmp_path)
    store.append_turn(
        chat_id="c1", user_query="entre 100 y 150 millones", response="...",
        parsed_params={"objeto": ["ascensores"], "valor_min": 100},
        outcome=Outcome.EMPTY, outcome_reason=OutcomeReason.ZERO_AFTER_REFINEMENT,
    )
    assert store.get_history("c1", last_n=1)[0].had_results() is False


def test_last_successful_turn_skips_empty(tmp_path):
    store = _store(tmp_path)
    store.append_turn(chat_id="c1", user_query="t1", response="ok",
                      parsed_params={"objeto": ["ascensores"], "ciudad": "Medellin"},
                      outcome=Outcome.SUCCESS, outcome_reason=OutcomeReason.RESULTS_FOUND,
                      result_count=10)
    store.append_turn(chat_id="c1", user_query="entre 100 y 150 millones", response="vacio",
                      parsed_params={"objeto": ["ascensores"], "valor_min": 100},
                      outcome=Outcome.EMPTY, outcome_reason=OutcomeReason.ZERO_AFTER_REFINEMENT)

    last_ok = store.last_successful_turn("c1")
    assert last_ok is not None
    assert last_ok.user_query == "t1"
    assert "ciudad" in last_ok.parsed_params


def test_last_successful_turn_none_when_all_empty(tmp_path):
    store = _store(tmp_path)
    store.append_turn(chat_id="c1", user_query="t1", response="vacio",
                      parsed_params={"objeto": ["x"]}, outcome=Outcome.EMPTY,
                      outcome_reason=OutcomeReason.ZERO_RESULTS)
    assert store.last_successful_turn("c1") is None


def test_legacy_turn_without_outcome_falls_back_to_result_ids(tmp_path):
    store = _store(tmp_path)
    # Turno previo a Etapa 4: sin outcome, pero con result_ids.
    store.append_turn(chat_id="c1", user_query="t1", response="ok",
                      parsed_params={"objeto": ["x"]}, result_ids=["CO1.NTC.9"])
    assert store.last_successful_turn("c1") is not None


# ── Inmutabilidad del snapshot (prueba agresiva pedida por el asesor) ─────────

def test_snapshot_unaffected_by_mutating_original_frame(tmp_path):
    store = _store(tmp_path)
    params = {"objeto": ["mantenimiento", "ascensores"], "nested": {"ciudades": ["Medellin"]}}
    store.append_turn(chat_id="c1", user_query="t1", response="ok",
                      parsed_params=params, outcome=Outcome.SUCCESS,
                      outcome_reason=OutcomeReason.RESULTS_FOUND, result_count=5)
    # Mutar las estructuras ORIGINALES después de persistir.
    params["objeto"].append("CONTAMINADO")
    params["nested"]["ciudades"].append("Bogota")
    params["dataset"] = "contratos"

    turn = store.get_history("c1", last_n=1)[0]
    assert turn.parsed_params["objeto"] == ["mantenimiento", "ascensores"]
    assert turn.parsed_params["nested"]["ciudades"] == ["Medellin"]
    assert "dataset" not in turn.parsed_params


def test_persisted_history_unaffected_by_mutating_read_copy(tmp_path):
    store = _store(tmp_path)
    store.append_turn(chat_id="c1", user_query="t1", response="ok",
                      parsed_params={"objeto": ["x"], "filtros": {"valor": [1]}},
                      outcome=Outcome.SUCCESS, outcome_reason=OutcomeReason.RESULTS_FOUND)
    # Leer, mutar la copia local agresivamente.
    first = store.get_history("c1", last_n=1)[0]
    first.parsed_params["objeto"].append("CONTAMINADO")
    first.parsed_params["filtros"]["valor"].append(999)
    first.outcome = Outcome.EMPTY

    again = store.get_history("c1", last_n=1)[0]
    assert again.parsed_params["objeto"] == ["x"]
    assert again.parsed_params["filtros"]["valor"] == [1]
    assert again.outcome == Outcome.SUCCESS
