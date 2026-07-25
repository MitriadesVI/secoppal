"""Tests del adaptador de wiring de modalidad (M3C-C).

Cubren las cinco enmiendas: transporte de la instrucción (serialización estricta
JSON-native), fuente cacheada de known_procedure_ids, mapa de mensajes por los
códigos REALES del motor, elegibilidad del REPLACE implícito y masking con
identidad de offsets sobre la MISMA cadena clasificada.
"""
from __future__ import annotations

import copy
import json
from decimal import Decimal
from enum import Enum

import pytest

from app.core import modality_followup_wiring as wiring
from app.core.modality_filter import ModalityFilter
from app.core.modality_followup import (
    ModalityFollowupInstruction,
    ModalityFollowupOperation,
    apply_modality_followup_operation,
    classify_modality_followup_control,
)
from app.core.modality_resolution_models import ResolutionStatus


def _instr(**kw):
    base = dict(
        status="none", operation=None, control_code=None, construction_id=None,
        source_text="", control_spans=(), target_required=False, explanation="",
    )
    base.update(kw)
    return ModalityFollowupInstruction(**base)


class _Interp:
    def __init__(self, executable, status):
        self.executable_filter = executable
        self.resolution_status = status


# ── Serialización estricta (enmienda #3) ───────────────────────────────────

@pytest.mark.parametrize("instruction", [
    _instr(),
    _instr(status="resolved", operation=ModalityFollowupOperation.ADD_INCLUDE,
           control_code="add_include_ademas", source_text="además x",
           control_spans=((0, 6),), target_required=True, explanation="atomic"),
    _instr(status="conflict", control_code="control_conflict",
           source_text="agrega y quita x", control_spans=((0, 6), (9, 14)),
           explanation="conflict"),
    _instr(status="resolved", operation=ModalityFollowupOperation.CLEAR,
           control_code="clear_phrase", source_text="sin filtro de modalidad",
           control_spans=((0, 23),), target_required=False, explanation="clear"),
])
def test_instruction_roundtrip(instruction):
    payload = wiring.instruction_to_state(instruction)
    json.dumps(payload)  # debe ser JSON-native
    restored = wiring.instruction_from_state(payload)
    assert restored == instruction


def test_serialized_payload_has_no_enum_objects():
    ins = _instr(status="resolved", operation=ModalityFollowupOperation.REPLACE,
                 control_code="replace_ahora", source_text="ahora x",
                 control_spans=((0, 5),), target_required=True, explanation="e")
    payload = wiring.instruction_to_state(ins)
    assert payload["operation"] == "replace"
    assert isinstance(payload["operation"], str)
    assert payload["control_spans"] == [[0, 5]]


@pytest.mark.parametrize("bad", [
    None, [], "x", 42,
    {"status": "none"},  # faltan campos
])
def test_from_state_rejects_malformed_payload(bad):
    with pytest.raises(ValueError):
        wiring.instruction_from_state(bad)


def test_from_state_rejects_extra_field():
    payload = wiring.instruction_to_state(_instr())
    payload["surprise"] = 1
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_from_state_rejects_unknown_operation():
    payload = wiring.instruction_to_state(_instr())
    payload["operation"] = "obliterate"
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_from_state_rejects_bad_span_shape():
    payload = wiring.instruction_to_state(
        _instr(status="resolved", operation=ModalityFollowupOperation.REPLACE,
               control_code="c", source_text="ahora", control_spans=((0, 5),),
               target_required=True, explanation="e"))
    payload["control_spans"] = [[0, 5, 9]]
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_from_state_rejects_inconsistent_status_operation():
    # status resolved sin operación → lo rechaza el __post_init__ de la dataclass.
    payload = wiring.instruction_to_state(_instr())
    payload["status"] = "resolved"
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


# ── Deserialización ESTRICTA hostil (§2): sin coerciones int()/bool() ───────

def _valid_resolved_payload() -> dict:
    """Payload JSON-native válido (resolved ADD_INCLUDE con un span)."""
    return wiring.instruction_to_state(_instr(
        status="resolved", operation=ModalityFollowupOperation.ADD_INCLUDE,
        control_code="add_include_ademas", source_text="además x",
        control_spans=((0, 6),), target_required=True, explanation="atomic",
    ))


def test_bool_span_index_rejected():
    # True/False NO son índices válidos aunque bool sea subtipo de int.
    payload = _valid_resolved_payload()
    payload["control_spans"] = [[True, 6]]
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)
    payload["control_spans"] = [[0, False]]
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_float_span_index_rejected():
    payload = _valid_resolved_payload()
    payload["control_spans"] = [[0.0, 6.0]]
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_numeric_string_span_index_rejected():
    # "0"/"6" NO se coercionan con int(): deben rechazarse.
    payload = _valid_resolved_payload()
    payload["control_spans"] = [["0", "6"]]
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_decimal_span_index_rejected():
    payload = _valid_resolved_payload()
    payload["control_spans"] = [[Decimal(0), Decimal(6)]]
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_mapping_inside_span_rejected():
    # Un mapping como span (o dentro de él) no es una lista [int, int].
    payload = _valid_resolved_payload()
    payload["control_spans"] = [{"start": 0, "end": 6}]
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_tuple_span_rejected_representation_must_be_list():
    # La proyección JSON-native emite listas; una tupla no es representación válida.
    payload = _valid_resolved_payload()
    payload["control_spans"] = [(0, 6)]
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_control_spans_container_must_be_list():
    payload = _valid_resolved_payload()
    payload["control_spans"] = ((0, 6),)  # tupla externa, no lista
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


@pytest.mark.parametrize("bad", [0, 1, "false", "true", "1", None, 1.0, Decimal(1)])
def test_non_bool_target_required_rejected(bad):
    # target_required debe ser bool EXACTO: ni 0/1, ni "false", ni None.
    payload = _valid_resolved_payload()
    payload["target_required"] = bad
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


@pytest.mark.parametrize("field", ["status", "source_text", "explanation"])
def test_required_str_fields_reject_non_str(field):
    payload = _valid_resolved_payload()
    payload[field] = 42
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


@pytest.mark.parametrize("field", ["control_code", "construction_id"])
def test_optional_str_fields_reject_non_str_non_none(field):
    payload = _valid_resolved_payload()
    payload[field] = 7  # ni None ni str
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_optional_str_fields_accept_none():
    payload = _valid_resolved_payload()
    payload["construction_id"] = None  # None sí es válido para campos opcionales
    restored = wiring.instruction_from_state(payload)
    assert restored.construction_id is None


class _ForeignOp(str, Enum):
    ADD_INCLUDE = "add_include"


def test_operation_rejects_foreign_enum_even_if_str_subtype():
    # Un str-Enum foráneo con el MISMO valor no debe colarse como operación.
    payload = _valid_resolved_payload()
    payload["operation"] = _ForeignOp.ADD_INCLUDE
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_operation_rejects_non_str_scalar():
    payload = _valid_resolved_payload()
    payload["operation"] = 3
    with pytest.raises(ValueError):
        wiring.instruction_from_state(payload)


def test_valid_payload_still_roundtrips_after_hardening():
    payload = _valid_resolved_payload()
    restored = wiring.instruction_from_state(payload)
    assert restored.operation is ModalityFollowupOperation.ADD_INCLUDE
    assert restored.control_spans == ((0, 6),)
    assert restored.target_required is True


# ── known_procedure_ids (enmienda #4) ──────────────────────────────────────

def test_known_ids_non_empty_and_cached():
    a = wiring.known_procedure_ids()
    b = wiring.known_procedure_ids()
    assert a is b  # cacheado, no relee YAML por turno
    assert isinstance(a, frozenset)
    assert a
    assert all(isinstance(x, str) and x for x in a)


# ── Mapa de mensajes: cubre TODOS los códigos reales del motor (enmienda #3) ─

def _all_engine_clarification_codes() -> set[str]:
    """Ejecuta el motor por caminos bloqueados y recolecta clarification_codes reales."""
    known = {"proc_a", "proc_b"}
    A = ModalityFilter(include_procedure_ids=("proc_a",))
    codes: set[str] = set()

    def run(instr, current, status, kn=known, prev=A):
        d = apply_modality_followup_operation(prev, current, instr, status, kn)
        if d.clarification_code:
            codes.add(d.clarification_code)

    add = _instr(status="resolved", operation=ModalityFollowupOperation.ADD_INCLUDE,
                 control_code="c", source_text="x", target_required=True, explanation="e")
    cur_b = ModalityFilter(include_procedure_ids=("proc_b",))
    # Estados M2 bloqueados dirigidos.
    for st in (ResolutionStatus.PARTIAL, ResolutionStatus.AMBIGUOUS,
               ResolutionStatus.CONFLICT, ResolutionStatus.NOT_SEARCHABLE,
               ResolutionStatus.DATASET_INAPPLICABLE):
        run(add, cur_b, st)
    run(add, cur_b, ResolutionStatus.NO_MATCH)  # target_missing
    # unknown_procedure_id: current con ID fuera del universo conocido.
    run(add, ModalityFilter(include_procedure_ids=("proc_unknown",)), ResolutionStatus.RESOLVED)
    # mixed_target_polarity: current con include Y exclude en una op dirigida.
    run(add, ModalityFilter(include_procedure_ids=("proc_a",), exclude_procedure_ids=("proc_b",)),
        ResolutionStatus.RESOLVED)
    # control ambiguo y conflicto (status de la instrucción, no de M2).
    amb = _instr(status="ambiguous", control_code="ambiguous_clear_or_null_value",
                 source_text="sin modalidad", explanation="e")
    con = _instr(status="conflict", control_code="control_conflict",
                 source_text="agrega y quita", explanation="e")
    run(amb, cur_b, ResolutionStatus.NO_MATCH)
    run(con, cur_b, ResolutionStatus.NO_MATCH)
    return codes


def test_message_map_covers_every_real_engine_code():
    codes = _all_engine_clarification_codes()
    # Sanity: el motor sí emite los códigos que la orden §17 no listaba.
    assert "mixed_target_polarity" in codes
    assert "ambiguous_clear_or_null_value" in codes
    # Todo código real del motor debe estar mapeado explícitamente (no caer al default).
    missing = codes - wiring.KNOWN_CLARIFICATION_CODES
    assert not missing, f"códigos del motor sin mensaje mapeado: {sorted(missing)}"
    for code in codes:
        assert wiring.message_for(code), f"mensaje vacío para {code}"


def test_ambiguous_clear_message_matches_semantics():
    # La corrección de la enmienda #3: la clave real es ambiguous_clear_or_null_value,
    # NO 'ambiguous_control'; el mensaje debe ofrecer quitar vs dato vacío.
    msg = wiring.message_for("ambiguous_clear_or_null_value").lower()
    assert "quitar" in msg and "dato" in msg


def test_message_map_never_leaks_ids_or_enums():
    for code in _all_engine_clarification_codes():
        msg = wiring.message_for(code)
        assert "procedure_id" not in msg
        assert "proc_" not in msg


# ── Masking con identidad de offsets (enmienda #5) ─────────────────────────

def _spans_for(text: str) -> tuple[tuple[int, int], ...]:
    """Spans reales de las modalidades presentes (para el clasificador R3′)."""
    low = text.lower()
    out = []
    for name in ("licitación pública", "contratación directa", "subasta inversa"):
        i = low.find(name)
        if i >= 0:
            out.append((i, i + len(name)))
    return tuple(sorted(out))


@pytest.mark.parametrize("text", [
    "ahora licitación pública",
    "AHORA Licitación Pública",
    "excepto contratación directa en la región del cañón",
    "además, subasta inversa",
])
def test_mask_uses_exact_same_string_and_preserves_length(text):
    ins = classify_modality_followup_control(
        text,
        current_filter=ModalityFilter(exclude_procedure_ids=("proc_a",)),
        resolution_status=ResolutionStatus.RESOLVED,
        target_spans=_spans_for(text),
    )
    masked = wiring.mask_control_spans(text, ins)
    assert len(masked) == len(text)  # identidad de longitud/offsets
    # Cada char enmascarado corresponde exactamente a un span de control.
    for i, (a, b) in enumerate(zip(text, masked)):
        if a != b:
            assert b == " "
            assert any(s <= i < e for s, e in ins.control_spans)


def test_mask_noop_when_no_control_spans():
    ins = _instr()  # sin spans
    assert wiring.mask_control_spans("hola mundo", ins) == "hola mundo"


# ── Elegibilidad de REPLACE implícito (enmienda #1) ────────────────────────

def _executable_interp():
    return _Interp(ModalityFilter(include_procedure_ids=("proc_a",)), ResolutionStatus.RESOLVED)


@pytest.mark.parametrize("intent,expected", [
    ("refine_filter", True),
    ("change_scope", True),
    ("change_dataset", True),
    ("new_search", False),
    ("pagination_more", False),
])
def test_implicit_replace_eligibility_by_intent(intent, expected):
    ins = _instr()  # status none
    assert wiring.is_eligible_for_implicit_replace(ins, _executable_interp(), intent) is expected


def test_implicit_replace_requires_none_status():
    ins = _instr(status="resolved", operation=ModalityFollowupOperation.ADD_INCLUDE,
                 control_code="c", source_text="x", target_required=True, explanation="e")
    assert not wiring.is_eligible_for_implicit_replace(ins, _executable_interp(), "refine_filter")


def test_implicit_replace_requires_executable_nonempty():
    empty = _Interp(ModalityFilter(), ResolutionStatus.NO_MATCH)
    assert not wiring.is_eligible_for_implicit_replace(_instr(), empty, "refine_filter")


def test_implicit_replace_rejects_non_executable_status():
    partial = _Interp(ModalityFilter(), ResolutionStatus.PARTIAL)
    assert not wiring.is_eligible_for_implicit_replace(_instr(), partial, "refine_filter")


# ── Plumbing de target_spans + validación estricta (R3′ §8) ────────────────

class _InterpSpans:
    def __init__(self, spans):
        self.executable_filter = ModalityFilter(include_procedure_ids=("proc_a",))
        self.resolution_status = ResolutionStatus.RESOLVED
        self.consumed_spans = spans


def test_target_spans_passthrough_from_interpretation():
    interp = _InterpSpans(((0, 5), (10, 14)))
    assert wiring.target_spans(interp) == ((0, 5), (10, 14))


def test_target_spans_empty_when_no_interpretation():
    assert wiring.target_spans(None) == ()


@pytest.mark.parametrize("bad_spans", [
    [(0, 5)],                 # list, no tuple
    ((0, 5, 9),),             # span de 3 elementos
    ((True, 5),),             # bool como índice
    ((0, False),),            # bool como índice
    ((0.0, 5.0),),            # floats
    (("0", "5"),),            # strings numéricos
    ((5, 5),),                # start == end
    ((5, 3),),                # invertido
    ((0, 100),),              # fuera del texto
    ((5, 10), (0, 3)),        # desordenados
    ((0, 6), (4, 9)),         # solapados
])
def test_classifier_rejects_malformed_target_spans(bad_spans):
    with pytest.raises(ValueError):
        classify_modality_followup_control(
            "ahora contratacion directa",
            current_filter=ModalityFilter(),
            resolution_status=ResolutionStatus.RESOLVED,
            target_spans=bad_spans,
        )


def test_classifier_accepts_wellformed_target_spans():
    ins = classify_modality_followup_control(
        "ahora contratacion directa",
        current_filter=ModalityFilter(include_procedure_ids=("proc_a",)),
        resolution_status=ResolutionStatus.RESOLVED,
        target_spans=((6, 26),),
    )
    assert ins.operation is ModalityFollowupOperation.REPLACE


# ── Helpers de params ──────────────────────────────────────────────────────

def test_filter_from_params_roundtrip_and_coerces_tuples():
    params = {"modalidad_include_procedure_ids": ("proc_a", "proc_b"), "objeto": ["x"]}
    f = wiring.filter_from_params(params)
    assert f.include_procedure_ids == ("proc_a", "proc_b")
    assert f.exclude_procedure_ids == ()


def test_filter_from_params_absent_keys_yield_empty():
    f = wiring.filter_from_params({"objeto": ["x"]})
    assert f.is_empty()


def test_filter_from_params_accepts_list():
    f = wiring.filter_from_params({"modalidad_include_procedure_ids": ["proc_a"]})
    assert f.include_procedure_ids == ("proc_a",)


def test_filter_from_params_empty_list_is_empty():
    f = wiring.filter_from_params({"modalidad_include_procedure_ids": []})
    assert f.is_empty()


# ── Reconstrucción ESTRICTA hostil (§3): solo list/tuple ────────────────────

@pytest.mark.parametrize("bad", [
    "proc_a",                       # str: NO debe partirse char-a-char con list()
    {"proc_a"},                     # set
    frozenset({"proc_a"}),          # frozenset
    (x for x in ["proc_a"]),        # generator
    {"proc_a": 1},                  # dict/mapping
    True,                           # bool
    5,                              # int
    None,                           # None con la clave PRESENTE
])
def test_filter_from_params_rejects_invalid_representations(bad):
    params = {"modalidad_include_procedure_ids": bad}
    with pytest.raises(ValueError):
        wiring.filter_from_params(params)


def test_filter_from_params_string_is_not_split_into_chars():
    # Refuerzo explícito del defecto que list(value) introduciría.
    with pytest.raises(ValueError):
        wiring.filter_from_params({"modalidad_include_procedure_ids": "abc"})


def test_filter_from_params_rejects_none_only_when_present():
    # Ausente ⇒ vacío (válido); presente con None ⇒ rechazado.
    assert wiring.filter_from_params({}).is_empty()
    with pytest.raises(ValueError):
        wiring.filter_from_params({"modalidad_exclude_procedure_ids": None})


def test_filter_from_params_does_not_mutate_input():
    original_list = ["proc_a", "proc_b"]
    params = {
        "modalidad_include_procedure_ids": original_list,
        "modalidad_exclude_procedure_ids": ("proc_c",),
        "objeto": ["x"],
    }
    snapshot = copy.deepcopy(params)
    f = wiring.filter_from_params(params)
    assert f.include_procedure_ids == ("proc_a", "proc_b")
    assert f.exclude_procedure_ids == ("proc_c",)
    # El dict y sus contenedores no se tocan.
    assert params == snapshot
    assert params["modalidad_include_procedure_ids"] is original_list


# ── §6 scrub_control_topic sobre las formas REALES del contrato ────────────
#
# Formas verificadas contra QueryRouter._extract_object_terms / detect_phrases:
#   objeto         → list[str | list[str]]  (un nivel: el grupo OR de "X o Y")
#   objeto_or      → list[str]              (plano; lo emite el LLM)
#   objeto_phrases → list[str]              (plano; canonicals del catálogo)
# Los spans NO se inventan: salen del clasificador real alimentado con los
# consumed_spans de M2 del mismo turno.

@pytest.fixture(scope="module")
def router():
    from app.core.query_router import QueryRouter
    return QueryRouter()


def _parse_and_classify(router, text: str):
    """(params reales, spans de control reales) para ``text``."""
    parsed = router.parse(text)
    instruction = wiring.classify_control(
        text, parsed.modality_interpretation, allow_implicit_replace=False
    )
    return parsed.params, instruction.control_spans


CONTROL_QUERIES = [
    "ahora licitación pública",
    "cambia a mínima cuantía",
    "además contratación directa",
    "deja de excluir mínima cuantía",
    "quita contratación directa",
]


@pytest.mark.parametrize("text", CONTROL_QUERIES)
def test_scrub_removes_every_control_word_from_flat_topic(router, text):
    params, spans = _parse_and_classify(router, text)
    assert spans, f"premisa: {text!r} debe producir spans de control"
    scrubbed = wiring.scrub_control_topic(params, text, spans)
    control_words = {
        tok for start, end in spans
        for tok in wiring._normalize_text(text[start:end]).split()
    }
    for key in ("objeto", "objeto_or", "objeto_phrases"):
        for item in scrubbed.get(key) or []:
            terms = item if isinstance(item, list) else [item]
            for term in terms:
                assert wiring._normalize_text(str(term)) not in control_words, (
                    f"control {term!r} sobrevivió en {key} para {text!r}"
                )


def test_scrub_removes_control_inside_nested_or_group(router):
    # "aseo o quita ..." → el parser agrupa ['aseo', 'quita'] como grupo OR y el
    # control queda ANIDADO: la limpieza debe entrar al grupo, no solo al nivel 1.
    text = "aseo o quita contratación directa"
    params, spans = _parse_and_classify(router, text)
    assert params["objeto"] == [["aseo", "quita"]], "premisa: forma anidada real"
    scrubbed = wiring.scrub_control_topic(params, text, spans)
    # Grupo de un solo término → escalar, exactamente como lo representa el parser.
    assert scrubbed["objeto"] == ["aseo"]


def test_scrub_keeps_multi_term_group_shape(router):
    text = "alimentación o además aseo en contratación directa"
    params, spans = _parse_and_classify(router, text)
    assert params["objeto"] == [["alimentacion", "ademas"], "aseo"], "premisa"
    scrubbed = wiring.scrub_control_topic(params, text, spans)
    assert scrubbed["objeto"] == ["alimentacion", "aseo"]


def test_scrub_drops_group_that_only_contained_control():
    # Grupo cuyos dos términos son control → el grupo desaparece por completo.
    params = {"objeto": [["quita", "ademas"], "alimentacion"]}
    text = "quita o además alimentación"
    spans = ((0, 5), (8, 14))  # "quita", "además"
    scrubbed = wiring.scrub_control_topic(params, text, spans)
    assert scrubbed["objeto"] == ["alimentacion"]


def test_scrub_drops_key_when_everything_was_control():
    params = {"objeto": [["quita", "ademas"]], "ciudad": "barranquilla"}
    text = "quita o además"
    spans = ((0, 5), (8, 14))
    scrubbed = wiring.scrub_control_topic(params, text, spans)
    assert "objeto" not in scrubbed
    assert scrubbed["ciudad"] == "barranquilla"  # otros campos intactos


def test_scrub_multiword_control_removes_only_its_own_tokens(router):
    # "deja de excluir" es UN span multi-palabra: sus tokens se retiran del topic,
    # pero los sustantivos del turno siguen ahí.
    text = "alimentación deja de excluir mínima cuantía"
    params, spans = _parse_and_classify(router, text)
    assert [text[a:b] for a, b in spans] == ["deja de excluir"], "premisa: span multi-palabra"
    assert params["objeto"] == ["alimentacion", "excluir"], "premisa: el control cayó en topic"
    scrubbed = wiring.scrub_control_topic(params, text, spans)
    assert "alimentacion" in (scrubbed.get("objeto") or [])
    assert "excluir" not in (scrubbed.get("objeto") or [])


def test_scrub_does_not_delete_substantive_tokens_sharing_the_turn():
    # Una señal multi-palabra ("cambia a") no autoriza a borrar términos sustantivos.
    params = {"objeto": ["cambia", "alimentacion", "aseo"]}
    text = "cambia a mínima cuantía de alimentación o aseo"
    spans = ((0, 8),)  # "cambia a"
    scrubbed = wiring.scrub_control_topic(params, text, spans)
    assert scrubbed["objeto"] == ["alimentacion", "aseo"]


def test_scrub_does_not_mutate_input(router):
    text = "aseo o quita contratación directa"
    params, spans = _parse_and_classify(router, text)
    params["objeto_or"] = ["quita", "aseo"]
    params["objeto_phrases"] = ["alimentacion_escolar"]
    snapshot = copy.deepcopy(params)
    inner = params["objeto"][0]
    scrubbed = wiring.scrub_control_topic(params, text, spans)
    assert params == snapshot                    # ni el dict ni sus listas cambian
    assert params["objeto"][0] is inner
    assert scrubbed is not params
    assert scrubbed["objeto_or"] == ["aseo"]     # objeto_or es plano por contrato
    assert scrubbed["objeto_phrases"] == ["alimentacion_escolar"]


def test_scrub_is_noop_without_control_spans():
    params = {"objeto": ["quita", "alimentacion"]}
    assert wiring.scrub_control_topic(params, "quita alimentación", ()) is params


def test_flat_topic_shape_canary(router):
    """Canary: objeto_or/objeto_phrases son planos; solo objeto admite anidamiento."""
    params, _ = _parse_and_classify(router, "licitación pública o contratación directa de alimentación o aseo")
    assert all(isinstance(x, (str, list)) for x in params.get("objeto") or [])
    for key in ("objeto_or", "objeto_phrases"):
        for item in params.get(key) or []:
            assert isinstance(item, str), f"{key} debe ser plano por contrato"


def test_strip_and_write_final_filter():
    params = {"objeto": ["x"], "modalidad_include_procedure_ids": ["old"]}
    stripped = wiring.strip_modality(params)
    assert "modalidad_include_procedure_ids" not in stripped
    assert stripped["objeto"] == ["x"]
    final = ModalityFilter(exclude_procedure_ids=("new",))
    written = wiring.write_final_filter(params, final)
    assert written["objeto"] == ["x"]
    assert "modalidad_include_procedure_ids" not in written
    assert written["modalidad_exclude_procedure_ids"] == ["new"]


def test_write_empty_filter_removes_all_modality_keys():
    params = {"objeto": ["x"], "modalidad_include_procedure_ids": ["a"],
              "modalidad_exclude_procedure_ids": ["b"]}
    written = wiring.write_final_filter(params, ModalityFilter())
    assert "modalidad_include_procedure_ids" not in written
    assert "modalidad_exclude_procedure_ids" not in written
    assert written["objeto"] == ["x"]
