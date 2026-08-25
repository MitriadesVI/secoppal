"""HCA-R4-S4: symmetric lifecycle for persisted dimensional exclusions."""
from __future__ import annotations

from pathlib import Path

import pytest

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow


def _rows() -> list[dict]:
    return [{
        "nombre_del_procedimiento": "Objeto sintético",
        "objeto_del_contrato": "Objeto sintético",
        "descripcion_del_proceso": "Descripción sintética",
        "descripci_n_del_procedimiento": "Descripción sintética",
        "entidad": "ENTIDAD SINTÉTICA",
        "nombre_entidad": "ENTIDAD SINTÉTICA",
        "departamento": "Atlántico",
        "departamento_entidad": "Atlántico",
        "ciudad": "Barranquilla",
        "ciudad_entidad": "Barranquilla",
        "precio_base": "100000000",
        "valor_del_contrato": "100000000",
        "fecha_de_publicacion_del": "2026-01-01",
        "fecha_de_firma": "2026-01-01",
        "urlproceso": "https://example.com",
    }]


class Dialogue:
    def __init__(self, tmp_path: Path):
        self.workflow = SecopalWorkflow(Settings(
            secop_feedback_path=tmp_path / "feedback.jsonl",
            secop_feedback_db_path=tmp_path / "secoppal.db",
        ))
        self.source_calls = 0

        def query(*_args, **_kwargs):
            self.source_calls += 1
            return _rows()

        self.workflow.secop_client.query = query
        self.workflow.secop_client.count = lambda *_a, **_k: 1
        self.workflow.narrator.narrate_with_grounding = lambda **_k: None
        self.workflow.conv_store = ConversationStore(base_path=tmp_path / "conv")
        self.chat_id = "negation-lifecycle"

    def turn(self, text: str) -> dict:
        return self.workflow.run_query(
            text,
            channel="telegram",
            chat_id=self.chat_id,
        )


@pytest.fixture
def dialogue(tmp_path: Path) -> Dialogue:
    return Dialogue(tmp_path)


def _exclusions(result: dict) -> list[str]:
    return result.get("parsed_params", {}).get("objeto_exclude") or []


def _lifecycle(result: dict) -> dict:
    return result["followup_mutation"]["negation_lifecycle"]


def test_object_remove_is_dimensional_and_traced(dialogue: Dialogue) -> None:
    dialogue.turn(
        "contratos de mantenimiento sin alimentacion en barranquilla"
    )
    result = dialogue.turn("quita la exclusion de alimentacion")

    params = result["parsed_params"]
    assert params["objeto"] == ["mantenimiento"]
    assert params["ciudad"] == "Barranquilla"
    assert not _exclusions(result)
    assert _lifecycle(result) == {
        "operation": "REMOVE_EXCLUSION",
        "previous_exclusions": ["alimentacion"],
        "requested_exclusions": ["alimentacion"],
        "resulting_exclusions": [],
        "reason": "mutation_applied",
        "no_op": False,
    }


@pytest.mark.parametrize(
    "command",
    [
        "quita alimentacion",
        "quita lo de alimentacion",
        "quita la exclusion de alimentacion",
        "ya no excluyas alimentacion",
        "deja de excluir alimentacion",
    ],
)
def test_targeted_remove_variants(command: str, dialogue: Dialogue) -> None:
    dialogue.turn("contratos de mantenimiento sin alimentacion")
    result = dialogue.turn(command)
    assert result.get("needs_clarification") is False
    assert not _exclusions(result)
    assert result["parsed_params"]["objeto"] == ["mantenimiento"]


@pytest.mark.parametrize(
    "command",
    ["ahora incluye alimentacion", "permite alimentacion", "vuelve a incluir alimentacion"],
)
def test_object_reinclude_only_removes_exclusion(
    command: str,
    dialogue: Dialogue,
) -> None:
    dialogue.turn("contratos de mantenimiento sin alimentacion")
    result = dialogue.turn(command)
    assert not _exclusions(result)
    assert result["parsed_params"]["objeto"] == ["mantenimiento"]
    assert _lifecycle(result)["operation"] == "REINCLUDE"


def test_replace_and_add_exclusion_have_distinct_semantics(dialogue: Dialogue) -> None:
    dialogue.turn("contratos de mantenimiento sin alimentacion")
    replaced = dialogue.turn("mejor sin transporte")
    assert _exclusions(replaced) == ["transporte"]
    assert _lifecycle(replaced)["operation"] == "REPLACE_EXCLUSION"

    added = dialogue.turn("tambien sin alimentacion")
    assert _exclusions(added) == ["transporte", "alimentacion"]
    assert _lifecycle(added)["operation"] == "ADD_EXCLUSION"


def test_partial_and_plural_clear_are_idempotent(dialogue: Dialogue) -> None:
    dialogue.turn("contratos de mantenimiento sin alimentacion ni transporte")
    partial = dialogue.turn("quita transporte")
    assert _exclusions(partial) == ["alimentacion"]

    duplicate = dialogue.turn("tambien sin alimentacion")
    assert _exclusions(duplicate) == ["alimentacion"]

    cleared = dialogue.turn("quita esas exclusiones")
    assert not _exclusions(cleared)

    calls_before = dialogue.source_calls
    repeated = dialogue.turn("quita la exclusion de alimentacion")
    assert repeated["needs_clarification"] is True
    assert "no tenía" in repeated["clarification_reason"]
    assert dialogue.source_calls == calls_before


@pytest.mark.parametrize(
    "command",
    ["quita la exclusion de transporte", "quita eso"],
)
def test_failed_clear_is_transactional_and_truthful(
    command: str,
    dialogue: Dialogue,
) -> None:
    first = dialogue.turn("contratos de mantenimiento sin alimentacion")
    calls_before = dialogue.source_calls
    blocked = dialogue.turn(command)

    assert blocked["needs_clarification"] is True
    assert blocked["query_plan"]["should_execute"] is False
    assert blocked["parsed_params"]["objeto"] == first["parsed_params"]["objeto"]
    assert _exclusions(blocked) == ["alimentacion"]
    assert dialogue.source_calls == calls_before


def test_ambiguous_replace_and_contradiction_do_not_mutate(dialogue: Dialogue) -> None:
    first = dialogue.turn("contratos de mantenimiento sin alimentacion")
    calls_before = dialogue.source_calls
    ambiguous = dialogue.turn("excepto transporte, mejor software")
    assert ambiguous["needs_clarification"] is True
    assert ambiguous["parsed_params"]["objeto"] == first["parsed_params"]["objeto"]
    assert _exclusions(ambiguous) == ["alimentacion"]
    assert dialogue.source_calls == calls_before

    contradiction = dialogue.turn("ahora sin mantenimiento")
    assert contradiction["needs_clarification"] is True
    assert contradiction["parsed_params"] == ambiguous["parsed_params"]
    assert dialogue.source_calls == calls_before


def test_sino_replaces_positive_topic_without_artificial_conflict(
    dialogue: Dialogue,
) -> None:
    dialogue.turn("contratos de alimentacion")
    result = dialogue.turn("no alimentacion sino transporte")
    assert result.get("needs_clarification") is False
    assert result["parsed_params"]["objeto"] == ["transporte"]
    assert _exclusions(result) == ["alimentacion"]


def test_complete_new_search_drops_all_previous_negation_context(
    dialogue: Dialogue,
) -> None:
    dialogue.turn(
        "contratos de mantenimiento sin alimentacion en atlantico"
    )
    result = dialogue.turn("muestrame contratos de software en bogota")
    params = result["parsed_params"]
    assert result["followup_intent_type"] == "new_search"
    assert params["objeto"] == ["software"]
    assert "mantenimiento" not in str(params)
    assert "alimentacion" not in str(params)
    assert "Atlántico" not in str(params)


def test_new_search_can_have_its_own_exclusion_without_inheriting_old_one(
    dialogue: Dialogue,
) -> None:
    dialogue.turn("contratos de mantenimiento sin alimentacion en atlantico")
    result = dialogue.turn(
        "busca contratos de software sin transporte en bogota"
    )
    assert result["followup_intent_type"] == "new_search"
    assert result["parsed_params"]["objeto"] == ["software"]
    assert _exclusions(result) == ["transporte"]


def test_complete_new_search_rebuilds_same_region_after_subtraction(
    dialogue: Dialogue,
) -> None:
    dialogue.turn("contratos de aseo en la costa caribe menos atlantico")
    result = dialogue.turn(
        "muestrame contratos de software en la costa caribe"
    )
    assert result["followup_intent_type"] == "new_search"
    assert result["parsed_params"]["objeto"] == ["software"]
    assert "Atlántico" in result["parsed_params"]["departamentos_in"]


def test_unsupported_clarification_leaves_no_zombie_state(
    dialogue: Dialogue,
) -> None:
    first = dialogue.turn("contratos de aseo en atlantico")
    calls_after_first = dialogue.source_calls
    blocked = dialogue.turn("menos barranquilla")
    assert blocked["needs_clarification"] is True
    assert dialogue.source_calls == calls_after_first

    confirmed = dialogue.turn("entonces dejalo asi")
    assert confirmed.get("needs_clarification") is False
    assert confirmed["parsed_params"]["objeto"] == first["parsed_params"]["objeto"]
    assert confirmed["parsed_params"]["departamento_resolved"] == "Atlántico"
    assert "barranquilla" not in str(confirmed["parsed_params"]).lower()
    assert "entonces" not in str(confirmed["parsed_params"]).lower()
    assert dialogue.source_calls == calls_after_first + 1


def test_modality_reinclude_uses_specialized_authority(dialogue: Dialogue) -> None:
    dialogue.turn("procesos de obra sin contratacion directa")
    result = dialogue.turn("ahora incluye contratacion directa")
    params = result["parsed_params"]
    assert result.get("needs_clarification") is False
    assert not params.get("modalidad_exclude_procedure_ids")
    assert not params.get("modalidad_include_procedure_ids")
    assert result["followup_mutation"]["modality"]["operation"] == "restore_include"


def test_department_reinclude_requires_and_uses_region_provenance(
    dialogue: Dialogue,
) -> None:
    dialogue.turn("contratos de aseo en la costa caribe menos atlantico")
    restored = dialogue.turn("vuelve a incluir atlantico")
    assert "Atlántico" in restored["parsed_params"]["departamentos_in"]
    assert restored["parsed_params"]["region"] == "Caribe"
    assert _lifecycle(restored)["dimension"] == "DEPARTMENT"

    dialogue.turn("menos bolivar")
    calls_before = dialogue.source_calls
    blocked = dialogue.turn("vuelve a incluir bolivar")
    assert blocked["needs_clarification"] is True
    assert blocked["query_plan"]["should_execute"] is False
    assert dialogue.source_calls == calls_before


@pytest.mark.parametrize(
    "command",
    ["reset", "empezar de nuevo", "nueva búsqueda"],
)
def test_reset_clears_negative_state_without_source_call(
    command: str,
    dialogue: Dialogue,
) -> None:
    dialogue.turn("contratos de mantenimiento sin alimentacion")
    calls_before = dialogue.source_calls
    result = dialogue.turn(command)
    assert result["route_reason"] == "reset"
    assert result["parsed_params"] == {}
    assert dialogue.source_calls == calls_before
    assert dialogue.workflow.conv_store.get_history(dialogue.chat_id) == []
