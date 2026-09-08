"""Publicación, recarga y rollback: lo que sobrevive y lo que se invalida.

El diseño no acepta «se afirma atomicidad por leer el código». Aquí se escribe
en disco de verdad, con dos escritores, con revisiones intercaladas y con la
cola rota a mano — y se comprueba qué queda publicado y qué se niega a
continuar.

Dos invariantes gobiernan el archivo:

* un fallo de publicación NO avanza el Frame durable ni entrega un cursor nuevo;
* una cola corrupta marca continuidad INVÁLIDA y no retrocede en silencio a una
  línea vieja para reactivar su cursor.
"""
from __future__ import annotations

import json
import threading

import pytest

from app.core.conversation_envelope import (
    ConversationEnvelope,
    EnvelopeError,
    EnvelopeStatus,
)
from app.core.conversation_store import ConversationStore, StaleRevisionError
from app.core.entity_profile.profile_state import (
    ProfileConversationState,
    ProfileStateError,
)
from app.core.workflow.continuation import ContinuationError, ContinuationOwner
from entity_profile_frame_b_harness import (  # noqa: F401 — fixture por nombre
    BUYER,
    Conversation,
    frame_v12,
)


# ── Recarga real en otra instancia ─────────────────────────────────────────

def test_the_frame_travels_only_through_what_was_published(frame_v12):
    chat = Conversation("pub-reload")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    # Otra instancia del almacén, como la leería otro proceso.
    read = ConversationStore().load_envelope("pub-reload")
    assert read.status is EnvelopeStatus.OK
    state = ProfileConversationState.from_dict(read.envelope.profile_state)
    assert state.confirmed_frame is not None
    assert read.envelope.execution_record["plan_id"]
    assert read.envelope.continuation["owner"] == ContinuationOwner.ENTITY_PROFILE.value


def test_two_independent_conversations_do_not_share_a_universe(frame_v12):
    first = Conversation("pub-a")
    second = Conversation("pub-b")
    first.say(f"analiza contratos firmados de {BUYER} en agosto")
    second.say(f"analiza procesos de {BUYER} en julio")
    assert first.profile_state().confirmed_frame.frame_id != (
        second.profile_state().confirmed_frame.frame_id
    )
    followup = second.say("recientes")
    for soql in followup.sql:
        assert "2026-07-01" in soql


# ── Revisiones intercaladas ────────────────────────────────────────────────

def test_an_interleaved_writer_loses_instead_of_overwriting(tmp_path):
    store = ConversationStore(base_path=tmp_path)
    first = store.reserve_turn("chat")
    second = store.reserve_turn("chat")
    store.publish_turn(first, user_query="a", response="A")
    with pytest.raises(StaleRevisionError):
        store.publish_turn(second, user_query="b", response="B")
    read = store.load_envelope("chat")
    assert read.envelope.response == "A"
    assert read.revision == 1


def test_concurrent_publishers_never_produce_two_turns_with_one_revision(tmp_path):
    store = ConversationStore(base_path=tmp_path)
    barrier = threading.Barrier(4)
    published: list[int] = []
    refused: list[Exception] = []

    def publish(index: int) -> None:
        reservation = store.reserve_turn("chat")
        barrier.wait()
        try:
            envelope = store.publish_turn(
                reservation, user_query=f"q{index}", response=f"r{index}",
            )
            published.append(envelope.revision)
        except StaleRevisionError as exc:
            refused.append(exc)

    threads = [threading.Thread(target=publish, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(published) == len(set(published))
    assert len(published) + len(refused) == 4
    lines = (tmp_path / "chat.envelopes.jsonl").read_text().splitlines()
    assert len(lines) == len(published)


# ── Cola corrupta y escritura truncada ─────────────────────────────────────

def test_a_truncated_tail_invalidates_continuity_and_never_falls_back(tmp_path):
    store = ConversationStore(base_path=tmp_path)
    store.publish_turn(
        store.reserve_turn("chat"), user_query="a", response="A",
        continuation={"owner": "TABULAR"},
    )
    path = tmp_path / "chat.envelopes.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"schema_version": 1, "revi')
    read = store.load_envelope("chat")
    assert read.status is EnvelopeStatus.CORRUPT
    assert read.envelope is None  # NO se retrocede a la línea válida anterior


def test_a_corrupt_queue_stops_a_pagination_instead_of_continuing(frame_v12):
    chat = Conversation("pub-corrupt")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    path = ConversationStore()._envelope_path_for("pub-corrupt")
    with path.open("a", encoding="utf-8") as handle:
        handle.write("{not json")
    more = chat.say("mas")
    assert more.clarifies
    assert more.calls == ()
    assert more.route == "admission_unreadable_continuation"


def test_a_publication_failure_does_not_advance_the_durable_frame(
    frame_v12, monkeypatch,
):
    chat = Conversation("pub-fail")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    before = chat.profile_state().confirmed_frame

    workflow, _ = chat.workflow()

    def refuse(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(workflow.conv_store, "publish_turn", refuse)
    from entity_profile_frame_b_harness import TODAY

    result = workflow.run_query("proveedores", chat_id="pub-fail", today=TODAY)

    assert result.get("continuation_published") is False
    assert "no pude guardar el contexto" in result["response"].lower()
    after = chat.profile_state().confirmed_frame
    assert after.frame_id == before.frame_id and after.revision == before.revision
    assert after.focus == "overview"


# ── Formas ilegibles ───────────────────────────────────────────────────────

def test_a_non_finite_metric_never_becomes_a_published_envelope():
    envelope = ConversationEnvelope(
        revision=1, turn_id="t", result_id="r", chat_id="c",
        user_query="q", response="a", timestamp="now",
        execution_record={"value": float("inf")},
    )
    with pytest.raises(ValueError):
        envelope.to_json()


def test_nan_in_a_stored_line_is_refused_on_read():
    line = json.dumps({
        "schema_version": 1, "revision": 1, "turn_id": "t", "result_id": "r",
        "chat_id": "c", "user_query": "q", "response": "a", "timestamp": "now",
        "profile_state": None, "continuation": None, "execution_record": None,
    })
    broken = line.replace('"response": "a"', '"response": NaN')
    with pytest.raises(EnvelopeError):
        ConversationEnvelope.from_json(broken)


def test_an_unknown_envelope_version_is_refused_not_guessed():
    with pytest.raises(EnvelopeError):
        ConversationEnvelope.from_dict({"schema_version": 99, "revision": 1})


def test_an_unknown_profile_state_version_is_refused_not_emptied():
    with pytest.raises(ProfileStateError):
        ProfileConversationState.from_dict({"schema_version": 2, "confirmed_frame": None})


def test_a_broken_frame_reference_invalidates_the_whole_state():
    with pytest.raises(ProfileStateError):
        ProfileConversationState.from_dict({
            "schema_version": 1,
            "confirmed_frame": {"schema_version": 1, "frame_id": "f"},
            "pending": None,
        })


def test_a_pending_anchored_to_a_foreign_revision_is_refused(frame_v12):
    chat = Conversation("pub-anchor")
    chat.say(f"analiza procesos de {BUYER} en agosto")
    chat.say("proveedores")
    raw = ConversationStore().load_envelope("pub-anchor").envelope.profile_state
    raw["pending"]["base_frame_ref"] = ["otro-frame", 9]
    with pytest.raises(ProfileStateError):
        ProfileConversationState.from_dict(raw)


def test_an_unknown_continuation_version_is_refused():
    from app.core.workflow.continuation import ContinuationContract

    with pytest.raises(ContinuationError):
        ContinuationContract.from_dict({"schema_version": 7})


def test_an_envelope_whose_state_is_unreadable_yields_no_partial_context(frame_v12):
    chat = Conversation("pub-partial")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    store = ConversationStore()
    path = store._envelope_path_for("pub-partial")
    payload = json.loads(path.read_text().splitlines()[-1])
    payload["profile_state"]["schema_version"] = 99
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    more = chat.say("mas")
    assert more.clarifies and more.calls == ()
    assert more.route == "admission_unreadable_continuation"


# ── Sidecar huérfano ───────────────────────────────────────────────────────

def test_an_orphan_pagination_sidecar_does_not_resurrect_a_cursor(frame_v12):
    from app.core.pagination_buffer import PaginationBuffer, pagination_fingerprint

    chat = Conversation("pub-orphan")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    store = ConversationStore()
    params = {"entidad_resolved": "ALCALDIA MUNICIPAL DE BARANOA"}
    store.save_pagination_buffer("pub-orphan", PaginationBuffer.create(
        query_fingerprint=pagination_fingerprint(params),
        group="contratos", dataset_id="jbjy-vk9h", query_params=params,
        candidate_buffer=[{"id_contrato": "X"}], validated_ranked_buffer=[],
        display_cursor=0, source_cursor=50, source_candidate_total=100,
    ))
    more = chat.say("mas")
    assert more.clarifies
    assert more.calls == ()
    assert more.route == "admission_profile_is_not_paginable"


# ── Rollback ───────────────────────────────────────────────────────────────

def test_the_flag_is_off_by_default_and_writes_no_envelope():
    from app.config import Settings, get_settings

    get_settings.cache_clear()
    assert Settings().secoppal_entity_profile_frame_enabled is False
    chat = Conversation("pub-off")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    assert chat.published().status is EnvelopeStatus.EMPTY


def test_a_reader_that_does_not_understand_the_new_envelope_refuses_it(frame_v12):
    """Rollback: un lector viejo RECHAZA la continuidad; no la reinterpreta.

    El sobre publicado vive en su propio archivo y lleva su versión. Un binario
    anterior no lo lee —no conoce el sufijo— y el que sí lo lee comprueba la
    versión antes de usarlo. La prueba simula el lector antiguo publicando un
    sobre de una versión que este binario no conoce.
    """
    chat = Conversation("pub-rollback")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    store = ConversationStore()
    path = store._envelope_path_for("pub-rollback")
    payload = json.loads(path.read_text().splitlines()[-1])
    payload["schema_version"] = 2
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    read = store.load_envelope("pub-rollback")
    assert read.status is EnvelopeStatus.CORRUPT
    assert read.envelope is None

    more = chat.say("mas")
    assert more.clarifies and more.calls == ()
    assert more.route == "admission_unreadable_continuation"


def test_the_legacy_history_is_untouched_by_the_new_envelope(frame_v12):
    """El JSONL de turnos sigue siendo el de siempre: no se le añade el sobre."""
    chat = Conversation("pub-legacy")
    chat.say(f"analiza contratos de {BUYER} en agosto")
    store = ConversationStore()
    history = store.get_history("pub-legacy", last_n=5)
    # El perfil no escribe un turno legacy: el sobre es la autoridad, y un
    # turno con params heredables reabriría la puerta que V1.2 cierra.
    assert history == []
    for turn in history:
        assert "profile_state" not in turn.parsed_params


# ── Exclusión ENTRE PROCESOS ───────────────────────────────────────────────

_PUBLISHER = """
import json, os, sys, time
sys.path.insert(0, {root!r})
from app.core.conversation_store import ConversationStore, StaleRevisionError

store = ConversationStore(base_path={base!r})
start = float(sys.argv[2])
reservation = store.reserve_turn("chat")
while time.time() < start:
    time.sleep(0.002)
try:
    envelope = store.publish_turn(
        reservation, user_query=sys.argv[1], response=sys.argv[1],
    )
    print(json.dumps({{"published": envelope.revision}}))
except StaleRevisionError:
    print(json.dumps({{"refused": True}}))
"""


def test_the_exclusion_holds_between_processes_not_only_threads(tmp_path):
    """El diseño lo dice: «el RLock de un objeto de buffer no acredita esto».

    Cuatro PROCESOS distintos comparten el mismo almacén y publican a la vez.
    Un candado en memoria no los excluye; `flock` sí. La comprobación es que el
    archivo nunca contiene dos turnos con la misma revisión.
    """
    import json
    import subprocess
    import sys
    import time
    from pathlib import Path

    root = str(Path(__file__).resolve().parents[1])
    script = _PUBLISHER.format(root=root, base=str(tmp_path))
    start = time.time() + 1.0
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, f"q{index}", str(start)],
            stdout=subprocess.PIPE, text=True,
        )
        for index in range(4)
    ]
    outputs = [process.communicate(timeout=60)[0] for process in processes]
    verdicts = [json.loads(line.strip().splitlines()[-1]) for line in outputs]

    published = sorted(v["published"] for v in verdicts if "published" in v)
    assert published, verdicts
    assert published == sorted(set(published))  # ninguna revisión repetida

    lines = (tmp_path / "chat.envelopes.jsonl").read_text().splitlines()
    revisions = [json.loads(line)["revision"] for line in lines if line.strip()]
    assert revisions == list(range(1, len(revisions) + 1))
    assert revisions == published


def test_a_record_from_another_catalog_does_not_govern_the_next_turn(frame_v12):
    """El catálogo es evidencia de lo ejecutado, no autoridad del universo."""
    import json

    from app.core.entity_profile.frame_compiler import CATALOG_VERSION

    chat = Conversation("pub-catalog")
    chat.say(f"analiza contratos firmados de {BUYER} en agosto")
    record = chat.execution_record()
    assert record["catalog_version"] == CATALOG_VERSION

    store = ConversationStore()
    path = store._envelope_path_for("pub-catalog")
    payload = json.loads(path.read_text().splitlines()[-1])
    payload["execution_record"]["catalog_version"] = "entity-profile-frame-999"
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    followup = chat.say("proveedores")
    assert not followup.clarifies
    assert followup.planned_jobs == 2
    # El plan del turno nuevo se compila con el catálogo VIGENTE.
    assert chat.execution_record()["catalog_version"] == CATALOG_VERSION


_HOLDER = """
import sys, time
sys.path.insert(0, {root!r})
from pathlib import Path
from app.core.store_lock import conversation_lock

with conversation_lock(Path(sys.argv[1])):
    Path(sys.argv[2]).write_text("held")
    time.sleep(float(sys.argv[3]))
"""


def test_a_second_process_cannot_hold_the_same_conversation_lock(tmp_path):
    """La exclusión que se prueba es la del MECANISMO, no la de la carrera.

    La prueba de cuatro publicadores comprueba el resultado; ésta comprueba la
    causa, y de forma determinista: mientras un PROCESO tiene el candado de este
    chat, otro no puede tomarlo. Sin `flock` —sólo con el candado en memoria—
    el segundo entraría de inmediato.
    """
    import subprocess
    import sys
    import time
    from pathlib import Path

    import pytest as _pytest

    from app.core.store_lock import ConversationLockTimeout, conversation_lock

    root = str(Path(__file__).resolve().parents[1])
    target = tmp_path / "chat.envelopes.jsonl"
    marker = tmp_path / "held.marker"
    script = _HOLDER.format(root=root)
    holder = subprocess.Popen(
        [sys.executable, "-c", script, str(target), str(marker), "3"],
    )
    try:
        deadline = time.time() + 10
        while not marker.exists() and time.time() < deadline:
            time.sleep(0.02)
        assert marker.exists(), "el proceso que sostiene el candado no arrancó"

        with _pytest.raises(ConversationLockTimeout):
            with conversation_lock(target, timeout=0.5):
                raise AssertionError("dos procesos sostuvieron el mismo candado")
    finally:
        holder.terminate()
        holder.wait(timeout=10)

    # Liberado el otro proceso, el candado se vuelve a tomar sin problema.
    with conversation_lock(target, timeout=5):
        pass
