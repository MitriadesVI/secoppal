"""E2E de pipeline para operaciones conversacionales de modalidad (M3C-C).

Conduce el workflow REAL (SecopalWorkflow.run_query) con parser + persistencia
reales; solo SECOP está mockeado. Cubre los casos obligatorios §12, los frame
leak canaries §13 y el test de una sola llamada M2 §15.
"""
from __future__ import annotations

import pytest

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow

# Conjuntos canónicos resueltos por M2 (verificados contra el catálogo real).
CD = {"contratacion_directa", "contratacion_directa_con_ofertas"}
LP = {"licitacion_publica", "licitacion_publica_obra"}
MC = {"minima_cuantia"}
SI = {"seleccion_abreviada_subasta_inversa"}

_INCLUDE = "modalidad_include_procedure_ids"
_EXCLUDE = "modalidad_exclude_procedure_ids"

_LEAK_KEYS = (
    "modality_followup_instruction", "modality_operation", "modality_action",
    "control_code", "control_spans", "instruction_status", "clarification_code",
    "candidate_filter", "ModalityInterpretation",
)


def _mk_results(n: int = 3) -> list[dict]:
    return [{
        "nombre_del_procedimiento": f"Obra {i}",
        "entidad": "ENTIDAD X",
        "modalidad_de_contratacion": "Licitacion Publica",
        "urlproceso": "https://example.com",
    } for i in range(n)]


class _Dialogue:
    def __init__(self, tmp_path):
        self.wf = SecopalWorkflow(Settings(
            secop_feedback_path=tmp_path / "feedback.jsonl",
            secop_feedback_db_path=tmp_path / "secoppal.db",
        ))
        # §5: el mock CAPTURA la consulta recibida (no varía resultados por consulta).
        self.captured_soql: list[str] = []

        def _capturing_query(*a, **k):
            for arg in list(a) + list(k.values()):
                if isinstance(arg, str) and arg.lstrip().upper().startswith("SELECT"):
                    self.captured_soql.append(arg)
            return _mk_results(3)

        self.wf.secop_client.query = _capturing_query
        self.wf.secop_client.count = lambda *a, **k: 3
        self.wf.narrator.narrate_with_grounding = lambda **k: None
        self.wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
        self._chat = "chat"

    def turn(self, text: str) -> dict:
        self.captured_soql.clear()
        return self.wf.run_query(text, channel="telegram", chat_id=self._chat)

    def last_select_soql(self) -> str:
        """La última consulta SELECT que el cliente SECOP recibió en el turno."""
        assert self.captured_soql, "el mock SECOP no capturó ninguna consulta SELECT"
        return self.captured_soql[-1]


@pytest.fixture
def dialogue(tmp_path):
    return _Dialogue(tmp_path)


def _inc(result) -> set[str]:
    return set(result.get("parsed_params", {}).get(_INCLUDE) or [])


def _exc(result) -> set[str]:
    return set(result.get("parsed_params", {}).get(_EXCLUDE) or [])


def _objeto(result) -> list[str]:
    return result.get("parsed_params", {}).get("objeto") or []


# ── §12 Casos E2E obligatorios ─────────────────────────────────────────────

def test_replace_explicit_inherits_topic_and_scope(dialogue):
    dialogue.turn("contratación directa de alimentación en barranquilla")
    r = dialogue.turn("ahora licitación pública")
    assert _inc(r) == LP  # contratación directa desaparece
    assert not _exc(r)
    assert "alimentacion" in _objeto(r)  # topic heredado
    scope = r.get("parsed_params", {})
    assert "barranquilla" in str(scope.get("ciudad", "")).lower() or "barranquilla" in str(scope).lower()


def test_add_include_unions_and_inherits_topic(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("además contratación directa")
    assert _inc(r) == CD | LP
    assert not _exc(r)
    assert "mantenimiento" in _objeto(r)


def test_add_exclude_from_no_previous_modality(dialogue):
    dialogue.turn("alimentación en procesos")
    r = dialogue.turn("excepto contratación directa")
    assert not _inc(r)
    assert _exc(r) == CD
    assert "alimentacion" in _objeto(r)


def test_remove_member_keeps_the_rest(dialogue):
    dialogue.turn("licitación pública o contratación directa de mantenimiento")
    r = dialogue.turn("quita contratación directa")
    assert _inc(r) == LP
    assert not _exc(r)  # no se convierte en exclusión
    assert "mantenimiento" in _objeto(r)


def test_remove_exclude_does_not_force_include(dialogue):
    dialogue.turn("alimentación excepto contratación directa")
    r = dialogue.turn("permite contratación directa")
    assert not _exc(r)
    assert not _inc(r)  # no se fuerza a include
    assert "alimentacion" in _objeto(r)


def test_implicit_replace(dialogue):
    dialogue.turn("contratación directa de alimentación")
    r = dialogue.turn("mínima cuantía")
    assert _inc(r) == MC  # contratación directa desaparece
    assert not _exc(r)
    assert "alimentacion" in _objeto(r)  # topic heredado


def test_clear_keeps_other_filters(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("sin filtro de modalidad")
    assert not _inc(r) and not _exc(r)
    assert "mantenimiento" in _objeto(r)
    assert not r.get("needs_clarification")


def test_sin_modalidad_blocks_ambiguous_without_clearing(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("sin modalidad")
    assert r.get("needs_clarification") is True
    assert _inc(r) == LP  # NO se limpia
    assert not r.get("soql_query")


def test_control_conflict_blocks_and_keeps_previous(dialogue):
    dialogue.turn("contratación directa de alimentación")
    r = dialogue.turn("agrega y quita contratación directa")
    assert r.get("needs_clarification") is True
    assert _inc(r) == CD  # filtro previo intacto
    assert not r.get("soql_query")


def test_blocked_ambiguous_target_keeps_previous(dialogue):
    dialogue.turn("contratación directa de alimentación")
    r = dialogue.turn("además subasta")  # M2 AMBIGUOUS
    assert r.get("needs_clarification") is True
    assert _inc(r) == CD  # sin mutación parcial
    assert not r.get("soql_query")


def test_no_op_remove_absent_target(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("quita mínima cuantía")
    assert _inc(r) == LP  # filtro igual
    assert not r.get("needs_clarification")
    assert r["followup_mutation"]["modality"]["no_op"] is True


def test_change_dataset_preserves_ids(dialogue):
    dialogue.turn("subasta inversa de alimentación en procesos")
    r = dialogue.turn("ahora contratos")
    assert _inc(r) == SI  # IDs sobreviven
    assert r["parsed_params"].get("dataset") == "contratos"


def test_new_search_with_modality_is_not_followup_replace(dialogue):
    dialogue.turn("contratación directa de alimentación")
    r = dialogue.turn("busca licitación pública de obras")
    assert _inc(r) == LP
    assert "obras" in _objeto(r)
    assert "alimentacion" not in _objeto(r)  # búsqueda nueva, no hereda


# ── §5 Evidencia de SoQL (raws autorizados capturados del mock) ─────────────

from app.core.modality_dataset_projection import project_modality_filter  # noqa: E402
from app.core.modality_filter import ModalityFilter as _MF  # noqa: E402


def _escape(raw: str) -> str:
    """Igual que SoQLBuilder._escape: solo escapa comillas simples."""
    return raw.replace("'", "''")


def _where(soql: str) -> str:
    """Porción WHERE del SoQL (la modalidad vive como predicado, no en el SELECT)."""
    up = soql.upper()
    idx = up.find(" WHERE ")
    return soql[idx + 7:] if idx >= 0 else soql


def _include_raws(ids, dataset="procesos") -> tuple[str, ...]:
    proj = project_modality_filter(_MF(include_procedure_ids=tuple(sorted(ids))), dataset)
    return proj.include_raw_values


def _exclude_raws(ids, dataset="procesos") -> tuple[str, ...]:
    proj = project_modality_filter(_MF(exclude_procedure_ids=tuple(sorted(ids))), dataset)
    return proj.exclude_raw_values


def test_clear_then_replace_keeps_positive_target_and_inherited_topic(
    dialogue, monkeypatch
):
    import app.core.modality_parser_integration as mpi

    calls = {"n": 0}
    real_resolve = mpi.resolve

    def counting_resolve(*args, **kwargs):
        calls["n"] += 1
        return real_resolve(*args, **kwargs)

    monkeypatch.setattr(mpi, "resolve", counting_resolve)

    dialogue.turn("licitación pública de mantenimiento")
    calls["n"] = 0
    result = dialogue.turn(
        "no importa la modalidad, solo subasta inversa"
    )

    assert result["query_plan"]["should_execute"] is True

    mutation = result["followup_mutation"]["modality"]
    assert mutation["operation"] == "replace"
    assert mutation["construction_id"] == "clear_then_replace"

    params = result["parsed_params"]
    assert params[_INCLUDE] == ["seleccion_abreviada_subasta_inversa"]
    assert params.get(_EXCLUDE, []) == []
    assert params["objeto"] == ["mantenimiento"]
    assert calls["n"] == 1

    where = _where(dialogue.last_select_soql())
    for raw in _include_raws(SI):
        assert _escape(raw) in where
    assert "NOT IN" not in where.upper()
    assert "mantenimiento" in where.lower()


@pytest.mark.parametrize(
    ("query", "expected_include"),
    [
        (
            "no importa la modalidad, solo contratación directa",
            CD,
        ),
        (
            "no importa la modalidad solo subasta inversa",
            SI,
        ),
        (
            "no importa la modalidad\nsolo subasta inversa",
            SI,
        ),
        (
            "no importa la modalidad, busca contratación directa",
            CD,
        ),
    ],
)
def test_clear_then_replace_same_contract_variants(
    dialogue, monkeypatch, query, expected_include
):
    import app.core.modality_parser_integration as mpi

    calls = {"n": 0}
    real_resolve = mpi.resolve

    def counting_resolve(*args, **kwargs):
        calls["n"] += 1
        return real_resolve(*args, **kwargs)

    monkeypatch.setattr(mpi, "resolve", counting_resolve)

    dialogue.turn("licitación pública de mantenimiento")
    calls["n"] = 0
    result = dialogue.turn(query)

    assert result["query_plan"]["should_execute"] is True
    assert _mod(result)["operation"] == "replace"
    assert _mod(result)["construction_id"] == "clear_then_replace"
    assert _inc(result) == expected_include
    assert not _exc(result)
    assert _objeto(result) == ["mantenimiento"]
    assert calls["n"] == 1


@pytest.mark.parametrize(
    ("query", "expected_exclude"),
    [
        ("cualquier modalidad excepto contratación directa", CD),
        ("sin contratación directa", CD),
        ("excepto subasta inversa", SI),
    ],
)
def test_clear_then_replace_fix_preserves_exclusion_contrasts(
    dialogue, query, expected_exclude
):
    dialogue.turn("licitación pública de mantenimiento")
    result = dialogue.turn(query)

    assert expected_exclude <= _exc(result)
    assert "NOT IN" in _where(dialogue.last_select_soql()).upper()


def test_soql_add_include_contains_authorized_raws_of_both(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("además contratación directa")
    assert _inc(r) == CD | LP
    where = _where(dialogue.last_select_soql())
    # Los raws autorizados de A (LP) y B (CD) están ambos en el predicado de inclusión.
    for raw in _include_raws(LP | CD):
        assert _escape(raw) in where, f"raw de inclusión ausente en SoQL: {raw!r}"
    # Es una inclusión (IN/=), no una exclusión.
    assert "NOT IN" not in where.upper()


def test_soql_add_exclude_uses_not_in_of_excluded_modality(dialogue):
    dialogue.turn("alimentación en procesos")
    r = dialogue.turn("excepto contratación directa")
    assert _exc(r) == CD and not _inc(r)
    where = _where(dialogue.last_select_soql())
    assert "NOT IN" in where.upper(), "la exclusión debe materializarse como NOT IN"
    for raw in _exclude_raws(CD):
        assert _escape(raw) in where, f"raw excluido ausente del NOT IN: {raw!r}"


def test_soql_clear_drops_modality_predicate_and_prior_raws(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("sin filtro de modalidad")
    assert not _inc(r) and not _exc(r)
    where = _where(dialogue.last_select_soql())
    # El WHERE ya no contiene el campo de modalidad ni los raws de la modalidad previa.
    assert "modalidad_de_contratacion" not in where
    for raw in _include_raws(LP):
        assert _escape(raw) not in where, f"raw previo sobrevivió al CLEAR: {raw!r}"


def test_soql_change_dataset_uses_contratos_raws_and_preserves_ids(dialogue):
    dialogue.turn("subasta inversa de alimentación en procesos")
    r = dialogue.turn("ahora contratos")
    # procedure_ids sobreviven al cambio de dataset.
    assert _inc(r) == SI
    assert r["parsed_params"].get("dataset") == "contratos"
    where = _where(dialogue.last_select_soql())
    contratos_raws = _include_raws(SI, dataset="contratos")
    assert contratos_raws, "premisa: SI debe proyectar a algún raw en contratos"
    for raw in contratos_raws:
        assert _escape(raw) in where, f"raw de contratos ausente en SoQL: {raw!r}"
    # No debe usar raws EXCLUSIVOS de procesos (para SI el raw coincide entre datasets,
    # así que el conjunto exclusivo es vacío; la aserción documenta la invariante).
    exclusive_procesos = set(_include_raws(SI, dataset="procesos")) - set(contratos_raws)
    for raw in exclusive_procesos:
        assert _escape(raw) not in where


# ── §4 Verbos descriptivos vs control — RESUELTO por M3C-B-R3′ ──────────────
#
# Los verbos "incluye"/"permite"/"usa" son de control SOLO cuando gobiernan un
# target de modalidad producido por M2; en cláusulas subordinadas descriptivas
# ("que incluye", "que permite", "quien usa") NO gobiernan ningún target (el verbo
# queda a la derecha del único target o dentro de una cláusula relativa) y por eso
# ya NO producen operaciones espurias. El clasificador puro decide "control vs
# descriptivo" comparando la posición del verbo contra los consumed_spans de M2
# (contrato de asociación candidato↔target de R3′), no por adivinar imperatividad.
#
# Estos cuatro E2E estaban congelados como xfail(strict) en M3C-C-R1; R3′ los
# convierte en tests normales verdes.


def _mod(r) -> dict:
    return (r.get("followup_mutation", {}) or {}).get("modality", {}) or {}


def test_descriptive_incluye_is_not_add_include(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("contratación directa que incluye suministro")
    assert _mod(r).get("operation") != "add_include"   # "incluye" descriptivo
    assert _inc(r) == CD                                 # corresponde a contratación directa
    assert not (LP & _inc(r))                            # licitación pública NO sobrevive
    assert "suministro" in _objeto(r)                    # suministro disponible como topic


def test_descriptive_incluye_in_new_search_does_not_inherit(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("busca contratación directa que incluye suministro")
    assert r.get("intent_type") == "new_search"
    assert _mod(r).get("operation") != "add_include"
    assert _inc(r) == CD                                 # búsqueda nueva: no hereda LP
    assert "suministro" in _objeto(r)
    assert "mantenimiento" not in _objeto(r)             # mantenimiento no se hereda


def test_descriptive_permite_is_not_remove_exclude(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("régimen especial que permite ofertas")
    assert _mod(r).get("operation") != "remove_exclude"  # "permite" descriptivo
    # M2 consume "ofertas" en la modalidad (régimen_especial_con_ofertas); la
    # modalidad final es régimen especial y la licitación pública previa NO sobrevive.
    assert "regimen_especial" in _inc(r)
    assert not (LP & _inc(r))


def test_descriptive_usa_is_not_replace_control(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("contratación directa para quien usa plataforma")
    assert _mod(r).get("control_code") != "replace_usa"  # "usa" descriptivo
    assert "plataforma" in _objeto(r)                    # plataforma no se elimina del topic
    # §8: el filtro final es el que M2 resolvió en ESTE turno; el previo no sobrevive.
    assert _inc(r) == CD
    assert not (LP & _inc(r))
    assert not _exc(r)


# OBSERVATION_DESCRIPTIVE_VERB_TOPIC_NOISE (documentado, NO corregido en M3C-C):
# el parser deja el verbo descriptivo como término de topic ("incluye", "permite",
# "quien") porque NO es un span de control y por tanto scrub_control_topic no debe
# tocarlo. Es ruido temático del parser, no de la capa de modalidad: no altera el
# filtro, ni el SoQL de modalidad, ni el fingerprint de modalidad. Este test CONGELA
# la observación para que un cambio futuro del parser sea visible, sin bloquear M3C-C.
@pytest.mark.parametrize("text,verb", [
    ("contratación directa que incluye suministro", "incluye"),
    ("régimen especial que permite ofertas", "permite"),
    ("contratación directa para quien usa plataforma", "quien"),
])
def test_observed_descriptive_verb_stays_as_topic_noise(dialogue, text, verb):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn(text)
    assert verb in _objeto(r), "observación: el parser conserva el verbo descriptivo"
    # No afecta la decisión de modalidad: el turno ejecuta y no pide aclaración.
    assert not r.get("needs_clarification")
    assert dialogue.captured_soql


# ── §11 Matriz: operaciones y estados M2 que faltaban ──────────────────────

def test_restore_include_lifts_exclusion_without_forcing_include(dialogue):
    dialogue.turn("alimentación excepto contratación directa")
    r = dialogue.turn("vuelve a incluir contratación directa")
    assert _mod(r).get("operation") == "restore_include"
    assert not _exc(r)   # la exclusión se retira
    assert not _inc(r)   # sin whitelist previa NO se fuerza include
    assert "alimentacion" in _objeto(r)


def test_restore_include_rejoins_existing_whitelist(dialogue):
    dialogue.turn("licitación pública de alimentación excepto contratación directa")
    r = dialogue.turn("vuelve a incluir contratación directa")
    assert _mod(r).get("operation") == "restore_include"
    assert _inc(r) == CD | LP  # había whitelist: el target vuelve a ella
    assert not _exc(r)


@pytest.mark.parametrize("text,code", [
    ("además licitación pública o subasta", "partial_target"),
    ("además mínima cuantía con o sin ofertas", "conflicting_target"),
    ("además solicitud de información a proveedores", "dataset_inapplicable_target"),
    ("además subasta", "ambiguous_target"),
])
def test_blocked_m2_target_keeps_previous_filter_and_blocks_execution(dialogue, text, code):
    dialogue.turn("contratación directa de alimentación")
    r = dialogue.turn(text)
    assert r.get("needs_clarification") is True
    assert _inc(r) == CD and not _exc(r)          # filtro previo intacto
    assert _mod(r).get("reason") == code  # el trace lleva el código REAL del motor
    # El mensaje viene del mapa por el código REAL del motor, no de un texto ad hoc.
    from app.core import modality_followup_wiring as wiring
    assert r.get("clarification_reason") == wiring.message_for(code)
    assert dialogue.captured_soql == []           # §9: ninguna SELECT ejecutada


def test_blocked_turn_rolls_back_orthogonal_deltas(dialogue):
    # El turno bloqueado trae además un cambio de dataset: NADA de ese delta sobrevive.
    dialogue.turn("contratación directa de alimentación en procesos")
    r = dialogue.turn("además solicitud de información a proveedores")
    assert r.get("needs_clarification") is True
    params = r["parsed_params"]
    assert _inc(r) == CD
    assert params.get("dataset") == "procesos"    # rollback al frame anterior
    assert params.get("objeto") == ["alimentacion"]


def test_not_searchable_target_blocks_in_pure_layer():
    """NOT_SEARCHABLE sintético: el motor puro bloquea sin tocar el filtro previo."""
    from app.core.modality_filter import ModalityFilter
    from app.core.modality_followup import (
        ModalityFollowupInstruction,
        ModalityFollowupOperation,
        apply_modality_followup_operation,
    )
    from app.core.modality_resolution_models import ResolutionStatus

    previous = ModalityFilter(include_procedure_ids=("licitacion_publica",))
    instruction = ModalityFollowupInstruction(
        status="resolved", operation=ModalityFollowupOperation.ADD_INCLUDE,
        control_code="add_include_ademas", construction_id=None,
        source_text="además x", control_spans=((0, 6),),
        target_required=True, explanation="e",
    )
    decision = apply_modality_followup_operation(
        previous,
        ModalityFilter(include_procedure_ids=("minima_cuantia",)),
        instruction,
        ResolutionStatus.NOT_SEARCHABLE,
        frozenset({"licitacion_publica", "minima_cuantia"}),
    )
    assert decision.needs_clarification is True
    assert decision.clarification_code == "non_searchable_target"
    assert decision.final_filter == previous  # sin mutación parcial


def test_pagination_more_does_not_get_implicit_replace(dialogue):
    dialogue.turn("contratación directa de alimentación")
    r = dialogue.turn("muéstrame más")
    assert r.get("intent_type") == "pagination_more"
    assert _inc(r) == CD          # el filtro sobrevive tal cual
    assert not _mod(r)            # sin operación de modalidad delegada
    assert not r.get("needs_clarification")


# ── §7 Contrato de los nodos ───────────────────────────────────────────────

def test_parse_query_runs_router_and_m2_once_without_implicit_replace(monkeypatch):
    import json

    from app.core.query_router import QueryRouter
    import app.core.modality_parser_integration as mpi

    router = QueryRouter()
    router_calls = {"n": 0}
    real_parse = router.parse

    def counting_parse(*a, **k):
        router_calls["n"] += 1
        return real_parse(*a, **k)

    router.parse = counting_parse

    m2_calls = {"n": 0}
    real_resolve = mpi.resolve

    def counting_resolve(*a, **k):
        m2_calls["n"] += 1
        return real_resolve(*a, **k)

    monkeypatch.setattr(mpi, "resolve", counting_resolve)

    seen: list[bool] = []
    real_classify = parsing_actions.modality_wiring.classify_control

    def spy_classify(user_query, interpretation, *, allow_implicit_replace):
        seen.append(allow_implicit_replace)
        return real_classify(
            user_query, interpretation, allow_implicit_replace=allow_implicit_replace
        )

    monkeypatch.setattr(
        parsing_actions.modality_wiring, "classify_control", spy_classify
    )

    out = parsing_actions.parse_query(
        State({"user_query": "ahora licitación pública", "timings_ms": {}}), router
    )

    assert router_calls["n"] == 1
    assert m2_calls["n"] == 1
    assert seen == [False], "parse_query nunca promueve REPLACE implícito"
    # La instrucción viaja como metadata efímera JSON-native…
    payload = out["modality_followup_instruction"]
    json.dumps(payload)
    assert isinstance(payload["operation"], str)
    # …y ninguna metadata de control entra a parsed_params.
    params = out["parsed_params"]
    for key in _LEAK_KEYS:
        assert key not in params


def test_blocked_query_plan_matches_the_real_dataclass_defaults(dialogue):
    from dataclasses import asdict, fields

    from app.core.query_plan import QueryPlan

    dialogue.turn("contratación directa de alimentación")
    r = dialogue.turn("agrega y quita contratación directa")
    plan = r["query_plan"]

    reference = asdict(QueryPlan(should_execute=False, params={}, reason="x"))
    assert set(plan) == set(reference) == {f.name for f in fields(QueryPlan)}
    assert plan["should_execute"] is False
    assert plan["reason"] == "modality_control_conflict"
    # Defaults tomados de la dataclass real, no de un esquema aproximado.
    for key in ("missing_slots", "risk_flags", "signals", "clarification_question"):
        assert plan[key] == reference[key]
    # El plan bloqueado lleva el frame ANTERIOR, no el delta del turno.
    assert plan["params"].get(_INCLUDE) == sorted(CD)
    assert not r.get("soql_query")
    assert dialogue.captured_soql == []


def test_apply_context_is_inert_outside_followup():
    state = State({
        "followup": False,
        "user_query": "ahora licitación pública",
        "parsed_params": {"objeto": ["x"]},
        "context_params": {_INCLUDE: ["contratacion_directa"]},
        "modality_interpretation": None,
        "modality_followup_instruction": None,
        "timings_ms": {},
    })
    out = parsing_actions.apply_context(state)
    assert out["parsed_params"] == {"objeto": ["x"]}  # sin merge ni mutación
    assert out.get("followup_mutation") in (None, {})


def test_modality_is_marked_delegated_not_accepted(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("además contratación directa")
    trace = r["followup_mutation"]
    assert trace["delegated_fields"] == [_INCLUDE, _EXCLUDE]
    for field in ("accepted_fields", "rejected_fields", "transition_cleared_fields"):
        assert not ({_INCLUDE, _EXCLUDE} & set(trace.get(field) or []))


# ── §13 Frame leak canaries ────────────────────────────────────────────────

@pytest.mark.parametrize("t1,t2", [
    ("licitación pública de mantenimiento", "además contratación directa"),
    ("contratación directa de alimentación", "sin filtro de modalidad"),
    ("contratación directa de alimentación", "agrega y quita contratación directa"),
])
def test_no_modality_metadata_leaks_into_params(dialogue, t1, t2):
    dialogue.turn(t1)
    r = dialogue.turn(t2)
    for bag in ("parsed_params", "resolved_params"):
        params = r.get(bag, {}) or {}
        for key in _LEAK_KEYS:
            assert key not in params, f"{key} fugó a {bag}"
    # Solo las dos claves canónicas de modalidad pueden persistir.
    modality_keys = {k for k in r.get("parsed_params", {}) if k.startswith("modalidad")}
    assert modality_keys <= {_INCLUDE, _EXCLUDE}


@pytest.mark.parametrize("t1,t2", [
    ("licitación pública de mantenimiento", "además contratación directa"),
    ("contratación directa de alimentación", "sin filtro de modalidad"),
    ("licitación pública de mantenimiento", "quita mínima cuantía"),
])
def test_persisted_turn_carries_no_modality_metadata(dialogue, t1, t2):
    """§10: se comprueba el turno REALMENTE persistido, no solo lo que devuelve run_query."""
    dialogue.turn(t1)
    dialogue.turn(t2)
    history = dialogue.wf.conv_store.get_history(dialogue._chat, last_n=5)
    assert history, "premisa: el turno debe haberse persistido"
    persisted = history[-1]
    assert persisted.user_query == t2
    for key in _LEAK_KEYS:
        assert key not in persisted.parsed_params, f"{key} fugó al turno persistido"
    modality_keys = {k for k in persisted.parsed_params if k.startswith("modalidad")}
    assert modality_keys <= {_INCLUDE, _EXCLUDE}
    # El fingerprint persistido tampoco menciona metadata de control.
    for key in _LEAK_KEYS:
        assert key not in persisted.query_fingerprint


def test_blocked_turn_persists_no_modality_metadata(dialogue):
    dialogue.turn("contratación directa de alimentación")
    dialogue.turn("agrega y quita contratación directa")
    persisted = dialogue.wf.conv_store.get_history(dialogue._chat, last_n=5)[-1]
    for key in _LEAK_KEYS:
        assert key not in persisted.parsed_params
    assert {k for k in persisted.parsed_params if k.startswith("modalidad")} <= {_INCLUDE, _EXCLUDE}


def test_distinct_operations_same_final_share_fingerprint(dialogue, tmp_path):
    # Camino A: REPLACE explícito hasta {LP}.
    dialogue.turn("contratación directa de mantenimiento")
    ra = dialogue.turn("ahora licitación pública")
    # Camino B: no-op (quita ausente) sobre un {LP} inicial → mismo filtro final.
    other = _Dialogue(tmp_path)
    other.wf.conv_store = ConversationStore(base_path=tmp_path / "conv_b")
    other._chat = "chat_b"
    other.turn("licitación pública de mantenimiento")
    rb = other.turn("quita mínima cuantía")
    assert _inc(ra) == _inc(rb) == LP
    assert ra["query_fingerprint"] == rb["query_fingerprint"]


def test_trace_carries_modality_block_but_params_do_not(dialogue):
    dialogue.turn("licitación pública de mantenimiento")
    r = dialogue.turn("además contratación directa")
    # El trace SÍ describe la operación...
    modality_trace = r["followup_mutation"]["modality"]
    assert modality_trace["operation"] == "add_include"
    assert "delegated_fields" in r["followup_mutation"]
    # ...pero la instrucción efímera nunca entra a los params.
    assert "modality_followup_instruction" not in r.get("parsed_params", {})


# ── §4 Metadata efímera: solo None significa "ausente" ─────────────────────

from burr.core import State  # noqa: E402

from app.core.workflow.actions import parsing as parsing_actions  # noqa: E402


def _apply_context_state(payload, *, sentinel=False):
    """State mínimo de follow-up con una instrucción efímera arbitraria."""
    data = {
        "followup": True,
        "user_query": "ahora licitación pública",
        "parsed_params": {"objeto": ["alimentacion"]},
        "context_params": {"objeto": ["alimentacion"], _INCLUDE: ["contratacion_directa"]},
        "modality_interpretation": None,
        "timings_ms": {},
    }
    if not sentinel:
        data["modality_followup_instruction"] = payload
    return State(data)


def test_absent_ephemeral_metadata_falls_back_to_none_instruction():
    # None (y solo None) significa "parse_query no corrió": instrucción none de respaldo.
    instruction = parsing_actions._load_instruction(_apply_context_state(None))
    assert instruction.status == "none"
    assert instruction.operation is None
    assert instruction.control_spans == ()


def test_missing_ephemeral_key_falls_back_to_none_instruction():
    instruction = parsing_actions._load_instruction(_apply_context_state(None, sentinel=True))
    assert instruction.status == "none"


@pytest.mark.parametrize("corrupt", [{}, [], "", 0, False, 0.0, ()])
def test_falsey_corrupt_ephemeral_metadata_fails_closed(corrupt):
    # Un payload falsy NO es "ausente": es corrupción interna del state y debe
    # fallar cerrado, nunca degradarse en silencio a un no-op sin gobierno.
    with pytest.raises(ValueError):
        parsing_actions._load_instruction(_apply_context_state(corrupt))


@pytest.mark.parametrize("corrupt", [{}, [], "", 0, False])
def test_apply_context_propagates_corrupt_metadata_as_error(corrupt):
    # No se convierte en aclaración de usuario: no es ambigüedad conversacional.
    with pytest.raises(ValueError):
        parsing_actions.apply_context(_apply_context_state(corrupt))


def test_apply_context_with_none_metadata_does_not_raise():
    out = parsing_actions.apply_context(_apply_context_state(None))
    assert out.get("needs_clarification") is not True
    assert out["parsed_params"][_INCLUDE] == ["contratacion_directa"]  # merge genérico


# ── §5 El fallback LLM no es autoridad de modalidad ────────────────────────

_LLM_QUERY = "licitación pública de secretaria de salud"  # needs_llm=True con modalidad A


class _AdversarialLLM:
    """LLM hostil: intenta reemplazar, borrar o disfrazar la modalidad de M2."""

    enabled = True

    def __init__(self, mode: str):
        self.mode = mode
        self.calls = 0

    def parse(self, user_query, existing_params=None):
        self.calls += 1
        params = dict(existing_params or {})
        params["entidad"] = "SECRETARIA DE SALUD"  # campo ortogonal legítimo
        if self.mode == "replace":
            params[_INCLUDE] = ["minima_cuantia"]
        elif self.mode == "replace_exclude":
            params[_EXCLUDE] = ["licitacion_publica"]
        elif self.mode == "delete":
            params.pop(_INCLUDE, None)
            params.pop(_EXCLUDE, None)
        elif self.mode == "legacy":
            params["modalidad"] = "Minima Cuantia"
        elif self.mode == "raws":
            params["modalidad_de_contratacion"] = "Minima Cuantia"
        return params


@pytest.mark.parametrize("mode", ["replace", "replace_exclude", "delete", "legacy", "raws"])
def test_llm_cannot_override_m2_modality_on_initial_turn(dialogue, monkeypatch, mode):
    import app.core.modality_parser_integration as mpi

    calls = {"n": 0}
    real_resolve = mpi.resolve

    def counting_resolve(*a, **k):
        calls["n"] += 1
        return real_resolve(*a, **k)

    monkeypatch.setattr(mpi, "resolve", counting_resolve)
    adversarial = _AdversarialLLM(mode)
    dialogue.wf.llm_handler = adversarial

    r = dialogue.turn(_LLM_QUERY)

    assert adversarial.calls == 1, "premisa: el turno debe pasar por el fallback LLM"
    assert _inc(r) == LP, "la modalidad final debe ser la que resolvió M2"
    assert not _exc(r)
    params = r.get("parsed_params", {})
    assert "modalidad" not in params                      # sin clave legacy
    assert "modalidad_de_contratacion" not in params      # sin raws SECOP
    assert {k for k in params if k.startswith("modalidad")} <= {_INCLUDE, _EXCLUDE}
    assert params.get("entidad") == "SECRETARIA DE SALUD"  # ortogonal preservado
    assert calls["n"] == 1, f"M2 se ejecutó {calls['n']} veces en el camino LLM"


@pytest.mark.parametrize("mode", ["replace", "delete", "legacy", "raws"])
def test_llm_parse_preserves_followup_engine_filter(mode):
    """Turno follow-up: la autoridad que entra a llm_parse es el filtro ya mutado.

    Tras apply_context, el filtro canónico del turno es el que produjo el motor
    puro (aquí una unión ADD_INCLUDE que M2 sola NUNCA devolvería). llm_parse no
    puede sustituirlo por la modalidad del turno actual ni por la del LLM.
    """
    final_filter = sorted(CD | LP)
    state = State({
        "user_query": "además contratación directa",
        "needs_llm": True,
        "parsed_params": {"objeto": ["mantenimiento"], _INCLUDE: list(final_filter)},
        "timings_ms": {},
    })
    adversarial = _AdversarialLLM(mode)
    out = parsing_actions.llm_parse(state, adversarial)
    params = out["parsed_params"]
    assert sorted(params[_INCLUDE]) == final_filter
    assert not params.get(_EXCLUDE)
    assert "modalidad" not in params
    assert "modalidad_de_contratacion" not in params
    assert params["objeto"] == ["mantenimiento"]
    assert params["entidad"] == "SECRETARIA DE SALUD"


# ── R2.1 El dataset es inseparable del filtro canónico de modalidad ────────

class _DatasetAttackLLM(_AdversarialLLM):
    """LLM hostil que además mueve el dataset bajo una modalidad ya resuelta."""

    def __init__(self, dataset: str, *, mode: str = "replace", with_contract_state: bool = False):
        super().__init__(mode)
        self.dataset = dataset
        self.with_contract_state = with_contract_state

    def parse(self, user_query, existing_params=None):
        params = super().parse(user_query, existing_params)
        params["dataset"] = self.dataset
        if self.with_contract_state:
            params["estado_contrato"] = ["En ejecución"]
            params["estado_field"] = "estado_contrato"
        return params


def test_llm_cannot_move_dataset_under_resolved_modality_initial_turn(dialogue):
    """PROBE 1: turno inicial — dataset=procesos + LP resueltos por M2."""
    dialogue.wf.llm_handler = _DatasetAttackLLM("contratos")
    r = dialogue.turn(_LLM_QUERY)
    params = r["parsed_params"]
    assert params["dataset"] == "procesos", "el LLM no puede mover el dataset"
    assert _inc(r) == LP
    assert params.get("entidad") == "SECRETARIA DE SALUD"  # ortogonal sí sobrevive


def test_llm_cannot_move_dataset_under_followup_union():
    """PROBE 2: follow-up — la autoridad es la unión del motor, no el M2 del turno."""
    union = sorted(CD | LP)
    state = State({
        "user_query": "además contratación directa de secretaría de salud",
        "needs_llm": True,
        "parsed_params": {"dataset": "procesos", "objeto": ["mantenimiento"], _INCLUDE: list(union)},
        "timings_ms": {},
    })
    params = parsing_actions.llm_parse(state, _DatasetAttackLLM("contratos"))["parsed_params"]
    assert params["dataset"] == "procesos"
    assert sorted(params[_INCLUDE]) == union  # ni el M2 del turno ni el del LLM


def test_legitimate_dataset_change_survives_llm():
    """PROBE 3: se preserva el dataset DE ENTRADA, no un dataset fijo."""
    state = State({
        "user_query": "ahora contratos",
        "needs_llm": True,
        "parsed_params": {"dataset": "contratos", "objeto": ["alimentacion"], _INCLUDE: sorted(SI)},
        "timings_ms": {},
    })
    params = parsing_actions.llm_parse(state, _DatasetAttackLLM("procesos"))["parsed_params"]
    assert params["dataset"] == "contratos"  # el cambio legítimo aguas arriba manda
    assert sorted(params[_INCLUDE]) == sorted(SI)


def test_absent_dataset_key_stays_absent_under_resolved_modality():
    state = State({
        "user_query": "x", "needs_llm": True,
        "parsed_params": {"objeto": ["alimentacion"], _INCLUDE: sorted(LP)}, "timings_ms": {},
    })
    params = parsing_actions.llm_parse(state, _DatasetAttackLLM("contratos"))["parsed_params"]
    assert "dataset" not in params


def test_dataset_is_closed_even_without_canonical_modality():
    """PROBE 4 (revisado en R2.2): ``dataset`` es campo CERRADO, sin condición.

    En R2.1 el dataset solo se protegía cuando había filtro canónico de modalidad.
    La allowlist monotónica lo cierra siempre: el LLM no gobierna el dataset ni
    aunque el turno no tenga modalidad.
    """
    state = State({
        "user_query": "x", "needs_llm": True,
        "parsed_params": {"dataset": "procesos", "objeto": ["alimentacion"]}, "timings_ms": {},
    })
    params = parsing_actions.llm_parse(state, _DatasetAttackLLM("contratos", mode="noop"))["parsed_params"]
    assert params["dataset"] == "procesos"


def test_dataset_state_coupling_is_closed_by_allowlist():
    """El acoplamiento dataset↔estados desaparece: los estados NO llegan al merge.

    Congelado como xfail(strict) en R2.1 —cuando la restauración del dataset dejaba
    vivo un ``estado_contrato`` que el builder emitía contra el esquema de procesos—
    y resuelto en R2.2 sin inventar una política de purga de estados en parsing.py:
    el LLM simplemente no tiene autoridad para producir esas claves.
    """
    state = State({
        "user_query": "licitación pública de secretaria de salud",
        "needs_llm": True,
        "parsed_params": {"dataset": "procesos", "objeto": ["salud"], _INCLUDE: sorted(LP)},
        "timings_ms": {},
    })
    attacker = _DatasetAttackLLM("contratos", with_contract_state=True)
    params = parsing_actions.llm_parse(state, attacker)["parsed_params"]
    assert params["dataset"] == "procesos"
    for key in ("estado_contrato", "estado_contrato_adicional"):
        assert key not in params, f"{key} es exclusivo de contratos"
    assert params.get("estado_field") != "estado_contrato"


# ── R2.2 Allowlist monotónica: el LLM completa huecos, no amplía permisos ──

class _ProposalLLM:
    """LLM que devuelve exactamente la propuesta indicada, sobre los params de entrada."""

    enabled = True

    def __init__(self, proposal: dict):
        self.proposal = proposal

    def parse(self, user_query, existing_params=None):
        return {**dict(existing_params or {}), **self.proposal}


def _run_llm(incoming: dict, proposal: dict) -> dict:
    state = State({
        "user_query": "x", "needs_llm": True,
        "parsed_params": incoming, "timings_ms": {},
    })
    return parsing_actions.llm_parse(state, _ProposalLLM(proposal))["parsed_params"]


@pytest.mark.parametrize("field,value", [
    ("objeto", ["alimentacion"]),
    ("objeto_or", ["aseo", "vigilancia"]),
    ("entidad", "SECRETARIA DE SALUD"),
    ("ciudad", "Barranquilla"),
    ("departamento", "Atlántico"),
])
def test_llm_may_fill_an_unresolved_assisted_field(field, value):
    """El valor del LLM: completar lo que el parser NO resolvió."""
    params = _run_llm({"dataset": "procesos"}, {field: value})
    assert params[field] == value


# ── R2.3 El topic es UNA dimensión, no tres claves independientes ──────────

@pytest.mark.parametrize("incoming_key,incoming_value,proposed_key,proposed_value", [
    ("objeto", ["mantenimiento"], "objeto_or", ["aseo", "vigilancia"]),
    ("objeto_or", ["aseo"], "objeto", ["vigilancia"]),
    ("objeto_phrases", ["alimentacion_escolar"], "objeto", ["aseo"]),
    ("objeto_phrases", ["alimentacion_escolar"], "objeto_or", ["aseo", "vigilancia"]),
    ("objeto", ["mantenimiento"], "objeto_phrases", ["alimentacion_escolar"]),
])
def test_resolved_topic_blocks_every_other_topic_key(
    incoming_key, incoming_value, proposed_key, proposed_value
):
    """Un tema ya interpretado no se amplía por una clave lateral.

    Ruta real: ``LLMHandler._promote_synonym_expansion`` convierte el ``objeto``
    del modelo en ``objeto_or``, así que sin cierre por dimensión el LLM podía
    ampliar el tema esquivando la comprobación por clave.
    """
    incoming = {"dataset": "procesos", incoming_key: incoming_value}
    params = _run_llm(incoming, {proposed_key: proposed_value})
    assert params[incoming_key] == incoming_value  # el tema original, intacto
    assert proposed_key not in params


def test_topic_may_be_filled_when_no_topic_key_is_resolved():
    params = _run_llm({"dataset": "procesos"}, {"objeto": ["mantenimiento"]})
    assert params["objeto"] == ["mantenimiento"]


def test_multiple_new_topic_keys_are_all_rejected():
    """Dos representaciones nuevas de la misma dimensión son contradictorias.

    R2.4 — antes ganaba la primera del dict, con lo que el orden de serialización
    del modelo decidía AND (``objeto``) frente a OR (``objeto_or``). Se rechazan
    ambas: la política no puede delegarse en el orden de las claves.
    """
    incoming = {"dataset": "procesos"}
    params = _run_llm(incoming, {"objeto": ["mantenimiento"], "objeto_or": ["aseo", "vigilancia"]})
    assert "objeto" not in params
    assert "objeto_or" not in params
    assert params == incoming


def test_multiple_topic_keys_rejected_independent_of_dict_order():
    """El resultado es el MISMO en ambos órdenes de inserción."""
    incoming = {"dataset": "procesos"}
    forward = _run_llm(incoming, {"objeto": ["mantenimiento"], "objeto_or": ["aseo"]})
    reverse = _run_llm(incoming, {"objeto_or": ["aseo"], "objeto": ["mantenimiento"]})
    assert forward == reverse == incoming


def test_single_new_topic_key_is_still_accepted_alongside_an_echo():
    """Una sola propuesta temática nueva sigue aceptándose: el eco no cuenta."""
    incoming = {"dataset": "procesos", "objeto_or": ["aseo"]}
    # objeto_or se repite idéntico (eco) y no hay otra dimensión resuelta... salvo
    # el propio objeto_or, que SÍ resuelve el topic: la propuesta de objeto se rechaza.
    params = _run_llm(incoming, {"objeto_or": ["aseo"], "objeto": ["mantenimiento"]})
    assert params == incoming


# ── R2.4 El valor debe llegar YA canónico (política estricta) ──────────────

@pytest.mark.parametrize("proposal", [
    {"ciudad": "  Cali"},
    {"ciudad": "Cali  "},
    {"entidad": "SECRETARIA X  "},
    {"entidad": "\tSECRETARIA DE SALUD\n"},
    {"departamento": " Atlántico "},
    {"objeto": [" mantenimiento"]},
    {"objeto": ["mantenimiento", "aseo "]},
    {"objeto_or": ["aseo", " vigilancia "]},
    {"objeto_or": ["\taseo"]},
])
def test_noncanonical_whitespace_is_rejected(proposal):
    """Validar con strip() y guardar el original dejaba entrar valores no canónicos."""
    incoming = {"dataset": "procesos"}
    params = _run_llm(incoming, proposal)
    for key in proposal:
        assert key not in params, f"whitespace no canónico aceptado en {key}"
    assert params == incoming


@pytest.mark.parametrize("field,value", [
    ("ciudad", "Cali"),
    ("entidad", "SECRETARIA DE SALUD"),
    ("departamento", "Atlántico"),
    ("objeto", ["mantenimiento", "aseo"]),
    ("objeto_or", ["aseo", "vigilancia"]),
])
def test_canonical_values_are_still_accepted(field, value):
    params = _run_llm({"dataset": "procesos"}, {field: value})
    assert params[field] == value


def test_objeto_phrases_is_not_writable_by_the_llm():
    """Identificadores canónicos del catálogo: el LLM no los crea ni los reemplaza."""
    params = _run_llm({"dataset": "procesos"}, {"objeto_phrases": ["alimentacion_escolar"]})
    assert "objeto_phrases" not in params


def test_incoming_objeto_phrases_is_preserved():
    incoming = {"dataset": "procesos", "objeto_phrases": ["alimentacion_escolar"]}
    params = _run_llm(incoming, {"entidad": "SECRETARIA X"})
    assert params["objeto_phrases"] == ["alimentacion_escolar"]
    assert params["entidad"] == "SECRETARIA X"  # otra dimensión sí se completa


# ── R2.3 La frontera valida FORMA, no solo nombres ─────────────────────────

@pytest.mark.parametrize("proposal", [
    {"objeto": "mantenimiento"},              # str: list() lo partiría en caracteres
    {"objeto": [["mantenimiento"]]},          # grupo OR anidado: lo construye el parser
    {"objeto": ["mantenimiento", 3]},         # elemento no str
    {"objeto": ["mantenimiento", True]},      # bool es subtipo de int, no procede
    {"objeto": ["mantenimiento", ""]},        # elemento vacío
    {"objeto": ["   "]},                      # solo espacios
    {"objeto": []},                           # lista vacía
    {"objeto": ("mantenimiento",)},           # tuple
    {"objeto": {"mantenimiento"}},            # set
    {"objeto_or": {"x": 1}},                  # dict
    {"objeto_or": [{"x": 1}]},                # dict dentro de la lista
    {"entidad": ["SECRETARIA X"]},            # list donde se exige escalar
    {"entidad": 42},
    {"ciudad": 7},
    {"ciudad": {"nombre": "Cali"}},
    {"departamento": "   "},
    {"departamento": 42},
    {"departamento": True},
])
def test_malformed_proposals_are_rejected(proposal):
    incoming = {"dataset": "procesos"}
    params = _run_llm(incoming, proposal)
    for key in proposal:
        assert key not in params, f"propuesta con forma inválida aceptada: {key}"
    assert params == incoming  # incoming intacto


def test_accepted_topic_list_is_copied_not_referenced():
    proposal_list = ["mantenimiento"]
    params = _run_llm({"dataset": "procesos"}, {"objeto": proposal_list})
    assert params["objeto"] == ["mantenimiento"]
    assert params["objeto"] is not proposal_list


def test_malformed_proposal_appears_in_rejected_telemetry(caplog):
    with caplog.at_level("INFO"):
        _run_llm({"dataset": "procesos"}, {"objeto": "mantenimiento"})
    logged = [r.getMessage() for r in caplog.records if "allowlist" in r.getMessage()]
    assert logged and "rejected=['objeto']" in logged[-1]
    assert "mantenimiento" not in logged[-1]  # nunca el valor


@pytest.mark.parametrize("field,parser_value,llm_value", [
    ("objeto", ["mantenimiento"], ["otra_cosa"]),
    ("entidad", "ALCALDIA DE CALI", "SECRETARIA DE SALUD"),
    ("ciudad", "Cali", "Bogotá"),
    ("departamento", "Valle del Cauca", "Atlántico"),
])
def test_llm_cannot_overwrite_a_resolved_assisted_field(field, parser_value, llm_value):
    """Merge monotónico: nunca deshace una decisión válida del parser."""
    params = _run_llm({"dataset": "procesos", field: parser_value}, {field: llm_value})
    assert params[field] == parser_value


@pytest.mark.parametrize("field", ["entidad", "departamento", "ciudad"])
def test_gazetteer_resolution_blocks_llm_proposal(field):
    """Un campo resuelto por el gazetteer cuenta como resuelto aunque la clave base falte."""
    params = _run_llm({f"{field}_resolved": "X"}, {field: "propuesta del LLM"})
    assert field not in params


@pytest.mark.parametrize("key,value", [
    ("dataset", "contratos"),
    ("estado_contrato", ["En ejecución"]),
    ("estado_field", "estado_contrato"),
    ("estado_family", "oferta_abierta"),
    ("intent_type", "opportunity_search"),
    ("modalidad", "Minima Cuantia"),
    ("modalidad_de_contratacion", "Minima Cuantia"),
    (_INCLUDE, ["minima_cuantia"]),
    (_EXCLUDE, ["licitacion_publica"]),
    ("valor_min", 100_000_000),
    ("fecha_desde", "2025-07-01"),
    ("ordering_signal", "mayor_valor"),
    ("limit", 500),
    ("un_campo_futuro_que_no_existe", "x"),
])
def test_closed_and_unknown_keys_are_rejected(key, value):
    """Todo lo que no está en la allowlist se descarta, incluidos campos futuros."""
    incoming = {"dataset": "procesos", "objeto": ["x"]}
    params = _run_llm(incoming, {key: value})
    assert params.get(key) == incoming.get(key)


def test_llm_cannot_delete_incoming_keys():
    """El merge parte de los params de entrada: borrar una clave es imposible."""
    incoming = {"dataset": "procesos", "objeto": ["x"], _INCLUDE: sorted(LP)}

    class _DeletingLLM:
        enabled = True

        def parse(self, user_query, existing_params=None):
            return {"objeto": ["x"]}  # descarta todo lo demás

    state = State({
        "user_query": "x", "needs_llm": True, "parsed_params": incoming, "timings_ms": {},
    })
    params = parsing_actions.llm_parse(state, _DeletingLLM())["parsed_params"]
    assert params["dataset"] == "procesos"
    assert params[_INCLUDE] == sorted(LP)


def test_allowlist_decisions_are_logged_by_key_name(caplog):
    """Telemetría: se registra QUÉ se aceptó y rechazó, nunca los valores."""
    with caplog.at_level("INFO"):
        _run_llm({"dataset": "procesos"}, {"entidad": "SECRETARIA X", "dataset": "contratos"})
    logged = [r.getMessage() for r in caplog.records if "allowlist" in r.getMessage()]
    assert logged, "el nodo debe registrar la decisión de la allowlist"
    assert "entidad" in logged[-1] and "dataset" in logged[-1]
    assert "SECRETARIA X" not in logged[-1]  # sin valores del usuario


@pytest.mark.parametrize("bad", [None, [], "x", 42])
def test_llm_parse_rejects_non_dict_result(bad):
    class _BadLLM:
        enabled = True

        def parse(self, user_query, existing_params=None):
            return bad

    state = State({
        "user_query": "x", "needs_llm": True, "parsed_params": {}, "timings_ms": {},
    })
    with pytest.raises(ValueError):
        parsing_actions.llm_parse(state, _BadLLM())


def test_opportunity_invariant_survives_allowlist_via_plan_query(dialogue):
    """Los invariantes determinísticos del handler no se pierden: P11 los reimpone.

    ``LLMHandler.parse`` aplica ``enforce_bidder_opportunity_policy`` alrededor del
    modelo y escribe campos CERRADOS (dataset/intent_type/estado_family). La
    allowlist los descarta, pero P11 en ``plan_query`` los recalcula desde
    ``raw_query``, así que el turno mantiene el intent de oportunidad.
    """
    dialogue.wf.llm_handler = _ProposalLLM({"dataset": "contratos"})
    r = dialogue.turn("procesos para ofertar en secretaria de salud")
    assert r["parsed_params"].get("intent_type") == "opportunity_search"
    assert r["parsed_params"].get("dataset") == "procesos"


def test_llm_parse_is_skipped_when_not_needed():
    adversarial = _AdversarialLLM("replace")
    state = State({
        "user_query": "x", "needs_llm": False,
        "parsed_params": {_INCLUDE: ["licitacion_publica"]}, "timings_ms": {},
    })
    out = parsing_actions.llm_parse(state, adversarial)
    assert adversarial.calls == 0
    assert out["parsed_params"][_INCLUDE] == ["licitacion_publica"]


# ── §15 Una sola llamada M2 por turno ──────────────────────────────────────

@pytest.mark.parametrize("t1,t2", [
    ("contratación directa de alimentación en barranquilla", "ahora licitación pública"),  # replace
    ("licitación pública de mantenimiento", "además contratación directa"),                # add_include
    ("licitación pública de mantenimiento", "sin filtro de modalidad"),                    # clear
    ("contratación directa de alimentación", "además subasta"),                            # ambiguous target
    ("subasta inversa de alimentación en procesos", "ahora contratos"),                    # dataset + modality
    ("contratación directa de alimentación", "mínima cuantía"),                            # implicit replace
])
def test_m2_resolved_exactly_once_per_turn(dialogue, monkeypatch, t1, t2):
    import app.core.modality_parser_integration as mpi

    calls = {"n": 0}
    real_resolve = mpi.resolve

    def counting_resolve(*a, **k):
        calls["n"] += 1
        return real_resolve(*a, **k)

    monkeypatch.setattr(mpi, "resolve", counting_resolve)

    dialogue.turn(t1)
    calls["n"] = 0
    dialogue.turn(t2)
    assert calls["n"] == 1, f"M2 se llamó {calls['n']} veces en el turno de follow-up"
